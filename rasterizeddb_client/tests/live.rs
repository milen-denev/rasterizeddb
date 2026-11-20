//! Against a real server: the row format, the type tags and the literal rules
//! this crate copies from the engine, read back through the native protocol on
//! a unix socket.
//!
//! Needs a server binary with `--rdb-socket` and `--rdb-tcp`: set
//! `RDB_SERVER_BIN` to it. Without it each test says so and passes, so the
//! crate's own `cargo test` stays cheap; CI and anyone changing the wire format
//! must set it.
// The server it starts is unix-only, and it waits on the server's unix
// socket, so on other platforms there is nothing to run against.
#![cfg(unix)]

use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use rasterizeddb_client::{Client, Error, Login, Options, Param, tag};
use tokio::process::{Child, Command};

struct Server {
    child: Child,
    dir: PathBuf,
    socket: String,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

async fn server(name: &str, auth: &str) -> Option<Server> {
    let Ok(bin) = std::env::var("RDB_SERVER_BIN") else {
        eprintln!("RDB_SERVER_BIN is not set: skipping {name}");
        return None;
    };
    let dir = std::env::temp_dir().join(format!("rdb_client_{name}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("db")).unwrap();
    let socket = dir.join("rdb.sock");
    let child = Command::new(bin)
        .arg("--location")
        .arg(dir.join("db"))
        .arg("--rdb-socket")
        .arg(&socket)
        .args([
            "--rdb-tcp",
            "off",
            "--rdb-socket-auth",
            auth,
            "--socket-mode",
            "600",
        ])
        .args(["--log-level", "warn"])
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .expect("start the server");
    let server = Server {
        child,
        dir,
        socket: format!("unix:{}", socket.display()),
    };
    for _ in 0..300 {
        if std::os::unix::net::UnixStream::connect(&socket).is_ok() {
            return Some(server);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("the server did not open {} within 30 s", socket.display());
}

async fn connect(server: &Server, connections: usize) -> Client {
    Client::connect(
        &server.socket,
        Options {
            connections,
            timeout: Some(Duration::from_secs(20)),
            ..Options::default()
        },
    )
    .await
    .expect("connect")
}

fn bounded<F: std::future::Future<Output = ()>>(f: F) -> impl std::future::Future<Output = ()> {
    async move {
        tokio::time::timeout(Duration::from_secs(180), f)
            .await
            .expect("the test did not finish within 180 s")
    }
}

const TEXTS: &[&str] = &[
    "",
    "plain",
    "it's",
    "''",
    "a\\b",
    "\\x41",
    "-- not a comment",
    "/* nor this */",
    "$1 and $2",
    "'); DROP TABLE t; --",
    "ж€😀",
    "line\nbreak\ttab\r",
    "E'\\n'",
];

#[tokio::test(flavor = "multi_thread")]
async fn values_round_trip_through_parameters() {
    bounded(async {
        let Some(server) = server("roundtrip", "trust").await else {
            return;
        };
        let client = connect(&server, 1).await;

        client
            .run(
                "CREATE TABLE t (id BIGINT NOT NULL, s SMALLINT, i INTEGER, b BOOLEAN, \
                 txt TEXT, raw BYTEA, PRIMARY KEY (id))",
            )
            .await
            .expect("create");

        let all_bytes: Vec<u8> = (0..=255u8).collect();
        let bytes_cases: Vec<&[u8]> = vec![&[], &[0], &all_bytes, b"\\x00", b"'"];

        let mut id = 0i64;
        let mut expected = Vec::new();
        for text in TEXTS {
            for raw in &bytes_cases {
                id += 1;
                expected.push((id, (id % 3 == 0).then_some(*text), Some(*raw)));
            }
        }
        // Integer extremes and NULLs in every nullable column.
        let extremes = [
            (i64::MAX, i16::MAX, i32::MAX, true),
            (i64::MIN, i16::MIN, i32::MIN, false),
            (-1, -1, -1, true),
            (0, 0, 0, false),
        ];

        for &(id, text, raw) in &expected {
            let n = client
                .execute(
                    "INSERT INTO t (id, s, i, b, txt, raw) VALUES ($1, $2, $3, $4, $5, $6)",
                    &[
                        Param::I64(id),
                        Param::I16(-(id as i16)),
                        Param::I32(id as i32 * 1000),
                        Param::Bool(id % 2 == 0),
                        text.into(),
                        raw.into(),
                    ],
                )
                .await
                .unwrap_or_else(|e| panic!("insert {id}: {e}"));
            assert_eq!(n, 1, "insert {id}");
        }
        for &(id, s, i, b) in &extremes {
            if id > 0 && id < 10_000 {
                continue;
            }
            client
                .execute(
                    "INSERT INTO t (id, s, i, b, txt, raw) VALUES ($1, $2, $3, $4, $5, $6)",
                    &[
                        Param::I64(id),
                        Param::I16(s),
                        Param::I32(i),
                        Param::Bool(b),
                        Param::Null,
                        Param::Null,
                    ],
                )
                .await
                .unwrap_or_else(|e| panic!("insert extreme {id}: {e}"));
        }

        let mut tags_seen = std::collections::BTreeSet::new();
        for &(id, text, raw) in &expected {
            let rows = client
                .query(
                    "SELECT id, s, i, b, txt, raw FROM t WHERE id = $1",
                    &[Param::I64(id)],
                )
                .await
                .unwrap_or_else(|e| panic!("select {id}: {e}"));
            assert_eq!(rows.len(), 1, "row {id}");
            let row = rows.get(0).unwrap();
            for c in 0..row.len() {
                tags_seen.insert((c, row.tag(c).unwrap()));
            }
            assert_eq!(row.get_i64(0).unwrap(), id);
            assert_eq!(row.get_i16(1).unwrap(), -(id as i16));
            assert_eq!(row.get_i32(2).unwrap(), id as i32 * 1000);
            assert_eq!(row.get_bool(3).unwrap(), id % 2 == 0);
            assert_eq!(row.get_opt_text(4).unwrap(), text, "text of row {id}");
            assert_eq!(row.is_null(4).unwrap(), text.is_none());
            assert_eq!(row.get_opt_bytes(5).unwrap(), raw, "bytes of row {id}");
        }
        eprintln!("(column, tag) pairs seen: {tags_seen:?}");

        for &(id, s, i, b) in &extremes {
            if id > 0 && id < 10_000 {
                continue;
            }
            let rows = client
                .query(
                    "SELECT s, i, b, txt, raw FROM t WHERE id = $1",
                    &[Param::I64(id)],
                )
                .await
                .unwrap();
            let row = rows
                .get(0)
                .unwrap_or_else(|| panic!("extreme {id} missing"));
            assert_eq!(row.get_i16(0).unwrap(), s);
            assert_eq!(row.get_i32(1).unwrap(), i);
            assert_eq!(row.get_bool(2).unwrap(), b);
            assert!(row.is_null(3).unwrap());
            assert!(row.is_null(4).unwrap());
            assert_eq!(row.tag(3), Some(tag::NULL));
        }

        // A negative parameter after a minus sign: rendered bare it would be
        // `x--1 = 7`, a comment that drops the comparison and returns every
        // row. Its own table, since `t` holds i64::MAX, where `id - (-1)`
        // overflows.
        client
            .run("CREATE TABLE m (x BIGINT NOT NULL, PRIMARY KEY (x))")
            .await
            .unwrap();
        client
            .run("INSERT INTO m (x) VALUES (5), (6), (8)")
            .await
            .unwrap();
        let rows = client
            .query("SELECT x FROM m WHERE x-$1 = 7", &[Param::I64(-1)])
            .await
            .unwrap();
        let xs: Vec<i64> = rows.iter().map(|r| r.get_i64(0).unwrap()).collect();
        assert_eq!(xs, vec![6], "a WHERE after x-$1 must still filter");

        let count = client.query("SELECT count(*) FROM t", &[]).await.unwrap();
        let total = count.get(0).unwrap().get_i64(0).unwrap();
        assert_eq!(total as usize, expected.len() + 4);

        let updated = client
            .execute(
                "UPDATE t SET b = $1 WHERE id <= $2",
                &[Param::Bool(true), Param::I64(5)],
            )
            .await
            .unwrap();
        assert_eq!(
            updated,
            5 + 3,
            "ids 1..=5 plus the three non-positive extremes"
        );

        let deleted = client
            .query(
                "DELETE FROM t WHERE id = $1 RETURNING id, txt",
                &[Param::I64(3)],
            )
            .await
            .unwrap();
        assert_eq!(deleted.len(), 1);
        assert_eq!(deleted.get(0).unwrap().get_i64(0).unwrap(), 3);
        assert_eq!(
            deleted.get(0).unwrap().get_opt_text(1).unwrap(),
            Some(TEXTS[0])
        );

        match client.run("SELECT nope FROM no_such_table").await {
            Err(Error::Server(message)) => assert!(!message.is_empty()),
            other => panic!("expected a server error, got {other:?}"),
        }
        client.close().await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn kv_statements_take_text_parameters() {
    bounded(async {
        let Some(server) = server("kv", "trust").await else {
            return;
        };
        let client = connect(&server, 1).await;
        for value in ["eyJhIjoxfQ==", "", "it's", "a\\b"] {
            let set = client
                .query(
                    "KV SET $1 $2 EX 60",
                    &[Param::Text("c:ab01"), Param::Text(value)],
                )
                .await
                .unwrap_or_else(|e| panic!("KV SET {value:?}: {e}"));
            assert_eq!(set.get(0).unwrap().get_text(0).unwrap(), "OK");
            let got = client
                .query("KV GET $1", &[Param::Text("c:ab01")])
                .await
                .unwrap();
            assert_eq!(got.get(0).unwrap().get_text(0).unwrap(), value);
        }
        let missing = client
            .query("KV GET $1", &[Param::Text("c:none")])
            .await
            .unwrap();
        assert!(missing.is_empty());
        let unlinked = client
            .query(
                "KV UNLINK $1 $2",
                &[Param::Text("c:ab01"), Param::Text("c:none")],
            )
            .await
            .unwrap();
        assert_eq!(unlinked.get(0).unwrap().get_i64(0).unwrap(), 1);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn many_tasks_share_one_connection() {
    bounded(async {
        let Some(server) = server("shared", "trust").await else {
            return;
        };
        let client = std::sync::Arc::new(connect(&server, 1).await);
        client
            .run("CREATE TABLE p (id BIGINT NOT NULL, v TEXT, PRIMARY KEY (id))")
            .await
            .unwrap();
        for id in 0..64i64 {
            client
                .execute(
                    "INSERT INTO p (id, v) VALUES ($1, $2)",
                    &[Param::I64(id), Param::Text(&format!("v{id}"))],
                )
                .await
                .unwrap();
        }
        let mut tasks = Vec::new();
        for task in 0..32i64 {
            let client = client.clone();
            tasks.push(tokio::spawn(async move {
                for round in 0..20i64 {
                    let id = (task * 7 + round) % 64;
                    let rows = client
                        .query("SELECT v FROM p WHERE id = $1", &[Param::I64(id)])
                        .await
                        .unwrap();
                    assert_eq!(rows.get(0).unwrap().get_text(0).unwrap(), format!("v{id}"));
                }
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn peer_login_names_this_process_user() {
    bounded(async {
        let Some(server) = server("peer", "peer").await else {
            return;
        };
        // No role is named after this user, so the server refuses; what is
        // checked is that the refusal is the server's answer to our hello,
        // reported as a transport error at connect, not a hang.
        let result = Client::connect(
            &server.socket,
            Options {
                login: Login::Peer,
                timeout: Some(Duration::from_secs(10)),
                ..Options::default()
            },
        )
        .await;
        match result {
            Ok(_) => eprintln!("peer login accepted (a role named after this user exists)"),
            Err(e) => {
                assert!(e.is_transport(), "{e}");
                eprintln!("peer login refused as expected: {e}");
            }
        }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_answer_over_the_message_limit_fails_whole() {
    bounded(async {
        let Some(server) = server("limit", "trust").await else {
            return;
        };
        let client = connect(&server, 1).await;
        client
            .run("CREATE TABLE big (id BIGINT NOT NULL, body BYTEA, PRIMARY KEY (id))")
            .await
            .unwrap();
        let body = vec![0xabu8; 128 * 1024];
        for id in 0..80i64 {
            client
                .execute(
                    "INSERT INTO big (id, body) VALUES ($1, $2)",
                    &[Param::I64(id), Param::Bytes(&body)],
                )
                .await
                .unwrap();
        }
        // 80 x 128 KiB = 10 MiB of answer, past the default 8 MiB.
        match client.run("SELECT id, body FROM big").await {
            Err(e) => {
                assert!(e.is_transport(), "{e}");
                eprintln!("over the default limit: {e}");
            }
            Ok(rows) => panic!(
                "a 10 MiB answer under an 8 MiB limit returned {} rows",
                rows.len()
            ),
        }
        // The refusal costs the connection (the client drops it mid-answer),
        // and the next statement goes out on a new one.
        let rows = client.query("SELECT count(*) FROM big", &[]).await.unwrap();
        assert_eq!(rows.get(0).unwrap().get_i64(0).unwrap(), 80);

        // The limit is the client's: the server sends an answer of any size,
        // and a client that accepts more gets all of it.
        let roomy = Client::connect(
            &server.socket,
            Options {
                max_message_size: 32 * 1024 * 1024,
                ..Options::default()
            },
        )
        .await
        .unwrap();
        let rows = roomy.run("SELECT id, body FROM big").await.unwrap();
        assert_eq!(rows.len(), 80);
        assert!(
            rows.iter()
                .all(|r| r.get_bytes(1).unwrap() == body.as_slice())
        );
    })
    .await;
}

/// The simple-query form end to end: a session whose block outlives a call,
/// a row cap that still counts every row, and a cancel from the shared
/// client that stops a statement waiting for a lock.
#[tokio::test(flavor = "multi_thread")]
async fn a_simple_session_keeps_its_block_and_is_cancellable() {
    use rasterizeddb_client::{CancelToken, SimpleOptions, SimpleResult, TransactionStatus};
    bounded(async {
        let Some(server) = server("simple_session", "trust").await else { return };
        let client = std::sync::Arc::new(connect(&server, 1).await);
        client
            .simple_query(
                "CREATE TABLE simple_t (id BIGINT, label TEXT); \
                 INSERT INTO simple_t (id, label) VALUES (1,'a'),(2,'b'),(3,'c')",
            )
            .await
            .unwrap();

        let capped = client
            .simple_query_with(
                "SELECT id FROM simple_t ORDER BY id",
                &SimpleOptions { cancel: None, max_rows: Some(2) },
            )
            .await
            .unwrap();
        let SimpleResult::Rows { rows, .. } = &capped.results[0] else { panic!("{capped:?}") };
        assert_eq!(rows.len(), 2);
        assert_eq!(capped.results[0].row_count(), Some(3));

        let session = client.simple_session().await.unwrap();
        session.query("BEGIN; INSERT INTO simple_t (id, label) VALUES (4, 'd')").await.unwrap();
        assert_eq!(session.transaction(), TransactionStatus::InBlock);
        session.ping().await.unwrap();
        let count = |answer: &rasterizeddb_client::SimpleAnswer| match &answer.results[0] {
            SimpleResult::Rows { rows, .. } => rows[0][0].clone().unwrap(),
            other => panic!("{other:?}"),
        };
        let inside = session.query("SELECT count(*) FROM simple_t").await.unwrap();
        assert_eq!(count(&inside), "4");
        let outside = client.simple_query("SELECT count(*) FROM simple_t").await.unwrap();
        assert_eq!(count(&outside), "3", "the shared connection saw into the block");

        // A failed statement leaves the block failed, and the session says so.
        assert!(session.query("SELECT * FROM no_such_table").await.is_err());
        assert_eq!(session.transaction(), TransactionStatus::Failed);
        session.query("ROLLBACK").await.unwrap();
        assert_eq!(session.transaction(), TransactionStatus::Idle);

        // The session takes the table; the client's read waits until its
        // cancel stops it.
        session
            .query("BEGIN; LOCK TABLE simple_t IN ACCESS EXCLUSIVE MODE")
            .await
            .unwrap();
        let token = CancelToken::new();
        let waiting = {
            let (client, token) = (client.clone(), token.clone());
            tokio::spawn(async move {
                client
                    .simple_query_with(
                        "SELECT id FROM simple_t",
                        &SimpleOptions { cancel: Some(token), max_rows: None },
                    )
                    .await
            })
        };
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(!waiting.is_finished(), "the read did not wait for the lock");
        assert!(client.cancel(&token).await.unwrap());
        let stopped = tokio::time::timeout(Duration::from_secs(10), waiting)
            .await
            .expect("the cancelled read kept waiting")
            .unwrap();
        match stopped {
            Err(Error::Server(message)) => assert!(message.contains("57014"), "{message}"),
            other => panic!("{other:?}"),
        }
        assert!(!client.cancel(&token).await.unwrap(), "a finished query was cancelled");
        session.query("ROLLBACK").await.unwrap();
    })
    .await;
}
