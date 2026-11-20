//! Transaction blocks over the native protocol (rastcp), end to end.
//!
//! A real server (the native listener on a unix socket, and pgwire on
//! loopback TCP, both over one on-disk database) runs on a runtime of its own
//! with the binary's 32 MiB stacks. Clients speak the raw protocols, so what
//! is tested is the server, not a client library: a transaction connection
//! is a plain `rastcp/1` connection.
//!
//! Every wait is bounded, and every test's directory is a `TempDir`.

// The native listener runs on a unix socket.
#![cfg(unix)]

use std::future::Future;
use std::time::Duration;

use rastcp::client::{TcpClient, TcpClientBuilder};
use rastcp::server::TcpServerBuilder;
use rasterizeddb_core::core::database::catalog::DatabaseCatalog;
use rasterizeddb_core::core::database::{Database, serve_native};
use rasterizeddb_core::pgwire::{
    AuthConfig, ConnectionLimits, TlsConfig, TlsMode, start_pgwire_on_listener,
};
use rclite::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const TEST_LIMIT: Duration = Duration::from_secs(120);

async fn bounded<F: Future>(test: F) -> F::Output {
    tokio::time::timeout(TEST_LIMIT, test)
        .await
        .expect("the test wedged")
}

/// The server: its own thread and runtime, ended when this is dropped.
struct Harness {
    socket: String,
    pg_port: u16,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
    // Last: removed once the server thread has ended.
    _dir: tempfile::TempDir,
}

impl Harness {
    async fn start(request_timeout: Duration) -> Harness {
        let dir = tempfile::Builder::new()
            .prefix("rdb_ntx")
            .tempdir()
            .expect("a temp dir");
        let socket_path = dir.path().join("n.sock");
        let data = dir.path().join("data");
        std::fs::create_dir_all(&data).unwrap();
        let (shutdown, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let (bound_tx, bound_rx) = tokio::sync::oneshot::channel::<u16>();
        let path = socket_path.clone();
        let thread = std::thread::Builder::new()
            .name("native-tx-server".to_string())
            .stack_size(32 * 1024 * 1024)
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(4)
                    .thread_stack_size(32 * 1024 * 1024)
                    .enable_all()
                    .build()
                    .expect("server runtime");
                runtime.block_on(async move {
                    let database = Arc::new(Database::new(data.to_str().unwrap()).await);
                    let server = TcpServerBuilder::unix(&path)
                        .request_timeout(request_timeout)
                        .build()
                        .await
                        .expect("the native socket binds");
                    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                    let _ = bound_tx.send(listener.local_addr().unwrap().port());
                    let catalog = DatabaseCatalog::single(database.clone());
                    let pg = start_pgwire_on_listener(
                        catalog,
                        listener,
                        TlsConfig {
                            mode: TlsMode::Disable,
                            cert_path: None,
                            key_path: None,
                        },
                        AuthConfig {
                            method: rasterizeddb_core::auth::AuthMethod::Trust,
                            bootstrap_user: "rasterized".to_string(),
                            bootstrap_password: None,
                        },
                        ConnectionLimits::default(),
                    );
                    tokio::select! {
                        _ = serve_native(&server, database.clone(), false) => {}
                        _ = pg => {}
                        _ = shutdown_rx => {}
                    }
                });
                runtime.shutdown_timeout(Duration::from_secs(2));
            })
            .expect("the server thread");
        let pg_port = tokio::time::timeout(Duration::from_secs(30), bound_rx)
            .await
            .expect("the server started in time")
            .expect("the server thread bound its listeners");
        // The unix socket is bound before the TCP listener.
        Harness {
            socket: format!("unix:{}", socket_path.display()),
            pg_port,
            shutdown: Some(shutdown),
            thread: Some(thread),
            _dir: dir,
        }
    }

    /// A plain (`rastcp/1`) connection: one caller, requests in order. What
    /// a transaction runs on.
    async fn conn(&self) -> TcpClient {
        let mut client = TcpClientBuilder::new(&self.socket)
            .timeout(Duration::from_secs(30))
            .build()
            .await
            .expect("a native connection");
        client.connect().await.expect("connects");
        client
    }

    async fn pg(&self) -> Pg {
        Pg::connect(self.pg_port).await
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// One cell of a native row, as far as these tests read them.
#[derive(Debug, Clone, PartialEq)]
enum Cell {
    Int(i64),
    Text(String),
    Bool(bool),
    Null,
    Other(u8),
}

#[derive(Debug, Clone, PartialEq)]
enum Answer {
    Ack(Option<u64>),
    Rows(Vec<Vec<Cell>>),
}

fn decode(response: &[u8]) -> Result<Answer, String> {
    match response.first() {
        Some(0) => Ok(Answer::Ack(
            response
                .get(1..9)
                .map(|b| u64::from_le_bytes(b.try_into().unwrap())),
        )),
        Some(1) => Ok(Answer::Ack(Some(u64::from_le_bytes(
            response[1..9].try_into().unwrap(),
        )))),
        Some(2) => {
            let mut rows = Vec::new();
            let mut at = 1usize;
            while at < response.len() {
                let len = u32::from_le_bytes(response[at..at + 4].try_into().unwrap()) as usize;
                at += 4;
                let end = at + len;
                let mut row = Vec::new();
                while at < end {
                    let size =
                        u64::from_le_bytes(response[at..at + 8].try_into().unwrap()) as usize;
                    at += 16;
                    let data = &response[at..at + size];
                    at += size;
                    let tag = response[at];
                    at += 1;
                    row.push(match tag {
                        2 => Cell::Int(i16::from_le_bytes(data.try_into().unwrap()) as i64),
                        3 => Cell::Int(i32::from_le_bytes(data.try_into().unwrap()) as i64),
                        4 => Cell::Int(i64::from_le_bytes(data.try_into().unwrap())),
                        14 => Cell::Text(String::from_utf8_lossy(data).into_owned()),
                        16 => Cell::Bool(data.first() == Some(&1)),
                        253 => Cell::Null,
                        other => Cell::Other(other),
                    });
                }
                rows.push(row);
            }
            Ok(Answer::Rows(rows))
        }
        Some(3) => Err(String::from_utf8_lossy(&response[1..]).into_owned()),
        other => panic!("unknown answer tag {other:?}"),
    }
}

/// The SQLSTATE a native-session error carries.
fn code(message: &str) -> Option<String> {
    let at = message.rfind("(SQLSTATE ")?;
    Some(message[at + 10..].trim_end_matches(')').to_string())
}

async fn send(conn: &mut TcpClient, sql: &str) -> Result<Answer, String> {
    let response = conn
        .send(sql.as_bytes().to_vec())
        .await
        .unwrap_or_else(|e| panic!("{sql}: transport error {e}"));
    decode(&response)
}

async fn ok(conn: &mut TcpClient, sql: &str) -> Answer {
    send(conn, sql)
        .await
        .unwrap_or_else(|e| panic!("{sql} failed: {e}"))
}

async fn fails(conn: &mut TcpClient, sql: &str) -> String {
    match send(conn, sql).await {
        Err(message) => message,
        Ok(answer) => panic!("{sql} should have failed, answered {answer:?}"),
    }
}

/// The single integer a statement returned.
async fn int(conn: &mut TcpClient, sql: &str) -> i64 {
    match ok(conn, sql).await {
        Answer::Rows(rows) if rows.len() == 1 && rows[0].len() == 1 => match &rows[0][0] {
            Cell::Int(v) => *v,
            other => panic!("{sql}: not an integer: {other:?}"),
        },
        other => panic!("{sql}: expected one value, got {other:?}"),
    }
}

async fn count(conn: &mut TcpClient, table: &str) -> i64 {
    match ok(conn, &format!("SELECT id FROM {table}")).await {
        Answer::Rows(rows) => rows.len() as i64,
        other => panic!("{other:?}"),
    }
}

/// A table `name (id BIGINT PRIMARY KEY, v BIGINT)` holding `rows` rows,
/// ids 1..=rows with v = 0.
async fn table(h: &Harness, name: &str, rows: i64) {
    let mut c = h.conn().await;
    ok(
        &mut c,
        &format!("CREATE TABLE {name} (id BIGINT PRIMARY KEY, v BIGINT)"),
    )
    .await;
    for id in 1..=rows {
        ok(
            &mut c,
            &format!("INSERT INTO {name} (id, v) VALUES ({id}, 0)"),
        )
        .await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_commit_is_seen_by_other_connections_and_a_rollback_is_not() {
    bounded(async {
        let h = Harness::start(Duration::from_secs(30)).await;
        table(&h, "t", 0).await;
        let mut tx = h.conn().await;
        let mut other = h.conn().await;

        ok(&mut tx, "BEGIN").await;
        assert_eq!(
            ok(&mut tx, "INSERT INTO t (id, v) VALUES (1, 10)").await,
            Answer::Ack(Some(1)),
            "an insert in a block reports its count"
        );
        assert_eq!(count(&mut tx, "t").await, 1, "the block sees its own write");
        assert_eq!(
            count(&mut other, "t").await,
            0,
            "nobody else sees it before COMMIT"
        );
        assert_eq!(ok(&mut tx, "COMMIT").await, Answer::Ack(None));
        assert_eq!(count(&mut other, "t").await, 1, "COMMIT made it visible");

        ok(&mut tx, "BEGIN").await;
        ok(&mut tx, "INSERT INTO t (id, v) VALUES (2, 20)").await;
        ok(&mut tx, "UPDATE t SET v = 11 WHERE id = 1").await;
        ok(&mut tx, "ROLLBACK").await;
        assert_eq!(
            count(&mut other, "t").await,
            1,
            "the rolled-back insert is gone"
        );
        assert_eq!(
            int(&mut other, "SELECT v FROM t WHERE id = 1").await,
            10,
            "so is the update"
        );

        // START TRANSACTION and END are the same block.
        ok(&mut tx, "START TRANSACTION").await;
        ok(&mut tx, "INSERT INTO t (id, v) VALUES (3, 30)").await;
        ok(&mut tx, "END").await;
        assert_eq!(count(&mut other, "t").await, 2);
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_statement_outside_a_block_still_commits_on_its_own() {
    bounded(async {
        let h = Harness::start(Duration::from_secs(30)).await;
        table(&h, "t", 0).await;
        let mut conn = h.conn().await;
        let mut other = h.conn().await;

        ok(&mut conn, "INSERT INTO t (id, v) VALUES (1, 1)").await;
        assert_eq!(
            count(&mut other, "t").await,
            1,
            "visible at once, no COMMIT needed"
        );

        // Before and after a block on the same connection too.
        ok(&mut conn, "BEGIN").await;
        ok(&mut conn, "INSERT INTO t (id, v) VALUES (2, 2)").await;
        ok(&mut conn, "COMMIT").await;
        ok(&mut conn, "INSERT INTO t (id, v) VALUES (3, 3)").await;
        assert_eq!(count(&mut other, "t").await, 3);

        // COMMIT and ROLLBACK with no block open are warnings in PostgreSQL,
        // and change nothing.
        ok(&mut conn, "COMMIT").await;
        ok(&mut conn, "ROLLBACK").await;
        ok(&mut conn, "INSERT INTO t (id, v) VALUES (4, 4)").await;
        assert_eq!(count(&mut other, "t").await, 4);

        // LOCK outside a block is refused, with pgwire's code.
        let m = fails(&mut conn, "LOCK TABLE t IN ACCESS EXCLUSIVE MODE").await;
        assert_eq!(code(&m).as_deref(), Some("25P01"), "{m}");
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_concurrent_reader_sees_nothing_uncommitted_and_repeatable_read_holds_its_snapshot() {
    bounded(async {
        let h = Harness::start(Duration::from_secs(30)).await;
        table(&h, "t", 1).await;
        let mut writer = h.conn().await;
        let mut committed = h.conn().await;
        let mut repeatable = h.conn().await;
        let mut plain = h.conn().await;

        ok(&mut repeatable, "BEGIN ISOLATION LEVEL REPEATABLE READ").await;
        assert_eq!(count(&mut repeatable, "t").await, 1);
        ok(&mut committed, "BEGIN ISOLATION LEVEL READ COMMITTED").await;
        assert_eq!(count(&mut committed, "t").await, 1);

        ok(&mut writer, "BEGIN").await;
        ok(&mut writer, "INSERT INTO t (id, v) VALUES (2, 0)").await;
        ok(&mut writer, "UPDATE t SET v = 5 WHERE id = 1").await;
        assert_eq!(
            count(&mut plain, "t").await,
            1,
            "an autocommit reader sees nothing uncommitted"
        );
        assert_eq!(
            count(&mut committed, "t").await,
            1,
            "neither does a block's"
        );
        assert_eq!(int(&mut committed, "SELECT v FROM t WHERE id = 1").await, 0);
        ok(&mut writer, "COMMIT").await;

        assert_eq!(
            count(&mut committed, "t").await,
            2,
            "READ COMMITTED sees the commit"
        );
        assert_eq!(
            count(&mut repeatable, "t").await,
            1,
            "REPEATABLE READ keeps its snapshot"
        );
        assert_eq!(
            int(&mut repeatable, "SELECT v FROM t WHERE id = 1").await,
            0
        );
        match ok(&mut repeatable, "SHOW transaction_isolation").await {
            Answer::Rows(rows) => {
                assert_eq!(rows, vec![vec![Cell::Text("repeatable read".to_string())]])
            }
            other => panic!("{other:?}"),
        }
        ok(&mut repeatable, "COMMIT").await;
        ok(&mut committed, "COMMIT").await;
        assert_eq!(
            count(&mut repeatable, "t").await,
            2,
            "after the block, the latest"
        );
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_error_aborts_the_block_until_rollback() {
    bounded(async {
        let h = Harness::start(Duration::from_secs(30)).await;
        table(&h, "t", 1).await;
        let mut tx = h.conn().await;
        let mut other = h.conn().await;

        ok(&mut tx, "BEGIN").await;
        ok(&mut tx, "INSERT INTO t (id, v) VALUES (2, 0)").await;
        let m = fails(&mut tx, "INSERT INTO t (id, v) VALUES (1, 0)").await;
        assert_eq!(code(&m).as_deref(), Some("23505"), "{m}");
        for sql in [
            "SELECT id FROM t",
            "INSERT INTO t (id, v) VALUES (3, 0)",
            "SAVEPOINT a",
        ] {
            let m = fails(&mut tx, sql).await;
            assert_eq!(code(&m).as_deref(), Some("25P02"), "{sql}: {m}");
        }
        // COMMIT of a failed block rolls it back, and says so: the native
        // protocol has no command tag to carry pgwire's "ROLLBACK".
        let m = fails(&mut tx, "COMMIT").await;
        assert!(m.contains("rolled back"), "{m}");
        assert_eq!(code(&m).as_deref(), Some("25P02"), "{m}");
        assert_eq!(
            count(&mut other, "t").await,
            1,
            "nothing of the failed block landed"
        );

        // The block is closed: the connection is usable, and autocommits.
        ok(&mut tx, "INSERT INTO t (id, v) VALUES (4, 0)").await;
        assert_eq!(count(&mut other, "t").await, 2);

        // ROLLBACK ends an aborted block too.
        ok(&mut tx, "BEGIN").await;
        fails(&mut tx, "SELECT nosuchcolumn FROM t").await;
        fails(&mut tx, "SELECT id FROM t").await;
        ok(&mut tx, "ROLLBACK").await;
        assert_eq!(count(&mut tx, "t").await, 2);
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn savepoints_roll_back_part_of_a_block() {
    bounded(async {
        let h = Harness::start(Duration::from_secs(30)).await;
        table(&h, "t", 1).await;
        let mut tx = h.conn().await;
        let mut other = h.conn().await;

        ok(&mut tx, "BEGIN").await;
        ok(&mut tx, "INSERT INTO t (id, v) VALUES (10, 0)").await;
        ok(&mut tx, "SAVEPOINT a").await;
        ok(&mut tx, "INSERT INTO t (id, v) VALUES (11, 0)").await;
        ok(&mut tx, "ROLLBACK TO SAVEPOINT a").await;
        ok(&mut tx, "INSERT INTO t (id, v) VALUES (12, 0)").await;
        ok(&mut tx, "RELEASE SAVEPOINT a").await;

        // A failure under a savepoint fails the block until ROLLBACK TO it.
        ok(&mut tx, "SAVEPOINT b").await;
        let m = fails(&mut tx, "INSERT INTO t (id, v) VALUES (1, 0)").await;
        assert_eq!(code(&m).as_deref(), Some("23505"), "{m}");
        let m = fails(&mut tx, "SELECT id FROM t").await;
        assert_eq!(code(&m).as_deref(), Some("25P02"), "{m}");
        ok(&mut tx, "ROLLBACK TO b").await;
        ok(&mut tx, "INSERT INTO t (id, v) VALUES (13, 0)").await;
        // An unknown savepoint fails the block, as in PostgreSQL; ROLLBACK
        // TO the one still open revives it (and undoes what came after it).
        let m = fails(&mut tx, "RELEASE nosuch").await;
        assert_eq!(code(&m).as_deref(), Some("3B001"), "{m}");
        let m = fails(&mut tx, "SELECT id FROM t").await;
        assert_eq!(code(&m).as_deref(), Some("25P02"), "{m}");
        ok(&mut tx, "ROLLBACK TO b").await;
        ok(&mut tx, "INSERT INTO t (id, v) VALUES (13, 0)").await;
        ok(&mut tx, "COMMIT").await;

        let ids = match ok(&mut other, "SELECT id FROM t ORDER BY id").await {
            Answer::Rows(rows) => rows.into_iter().map(|r| r[0].clone()).collect::<Vec<_>>(),
            other => panic!("{other:?}"),
        };
        assert_eq!(
            ids,
            vec![Cell::Int(1), Cell::Int(10), Cell::Int(12), Cell::Int(13)]
        );

        let m = fails(&mut tx, "SAVEPOINT outside").await;
        assert_eq!(code(&m).as_deref(), Some("25P01"), "{m}");
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ddl_in_a_block_rolls_back_with_it() {
    bounded(async {
        let h = Harness::start(Duration::from_secs(30)).await;
        table(&h, "t", 1).await;
        let mut tx = h.conn().await;

        ok(&mut tx, "BEGIN").await;
        ok(&mut tx, "CREATE TABLE gone (id BIGINT PRIMARY KEY)").await;
        ok(&mut tx, "INSERT INTO gone (id) VALUES (1)").await;
        ok(&mut tx, "ROLLBACK").await;
        fails(&mut tx, "SELECT id FROM gone").await;

        ok(&mut tx, "BEGIN").await;
        ok(&mut tx, "CREATE TABLE kept (id BIGINT PRIMARY KEY)").await;
        ok(&mut tx, "INSERT INTO kept (id) VALUES (1)").await;
        ok(&mut tx, "COMMIT").await;
        assert_eq!(count(&mut tx, "kept").await, 1);

        // DROP TABLE in a block is deferred to COMMIT, so ROLLBACK keeps it.
        ok(&mut tx, "BEGIN").await;
        ok(&mut tx, "DROP TABLE kept").await;
        ok(&mut tx, "ROLLBACK").await;
        assert_eq!(count(&mut tx, "kept").await, 1);

        // A database cannot be created in a block (25001), as in pgwire.
        ok(&mut tx, "BEGIN").await;
        let m = fails(&mut tx, "CREATE DATABASE nope").await;
        assert_eq!(code(&m).as_deref(), Some("25001"), "{m}");
        ok(&mut tx, "ROLLBACK").await;

        // A read-only block refuses a write (25006).
        ok(&mut tx, "BEGIN READ ONLY").await;
        let m = fails(&mut tx, "INSERT INTO t (id, v) VALUES (9, 0)").await;
        assert_eq!(code(&m).as_deref(), Some("25006"), "{m}");
        ok(&mut tx, "ROLLBACK").await;
    })
    .await
}

/// The client went away mid-block: the block is rolled back and its row
/// lock released, so another connection writes the row at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dropped_connection_rolls_back_and_releases_its_locks() {
    bounded(async {
        let h = Harness::start(Duration::from_secs(30)).await;
        table(&h, "t", 1).await;
        let mut other = h.conn().await;

        let mut tx = h.conn().await;
        ok(&mut tx, "BEGIN").await;
        ok(&mut tx, "UPDATE t SET v = 99 WHERE id = 1").await;
        ok(&mut tx, "INSERT INTO t (id, v) VALUES (2, 0)").await;
        drop(tx);

        // Waits on the dropped block's row lock until the rollback releases
        // it; bounded well under what a lock left behind would cost.
        let write = tokio::time::timeout(
            Duration::from_secs(10),
            send(&mut other, "UPDATE t SET v = 5 WHERE id = 1"),
        )
        .await
        .expect("the dropped block's lock was released");
        assert_eq!(write, Ok(Answer::Ack(Some(1))));
        assert_eq!(int(&mut other, "SELECT v FROM t WHERE id = 1").await, 5);
        assert_eq!(
            count(&mut other, "t").await,
            1,
            "the dropped block's insert is gone"
        );
    })
    .await
}

/// A statement still running inside the block when its connection goes: the
/// rollback waits for it, so what it wrote is rolled back too, and nothing
/// of it is left holding a lock.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_statement_in_flight_when_the_connection_drops_is_rolled_back() {
    bounded(async {
        let h = Harness::start(Duration::from_secs(30)).await;
        table(&h, "t", 1).await;
        let mut holder = h.conn().await;
        let mut other = h.conn().await;

        // The holder's block holds the row, so the doomed block's UPDATE
        // waits on it: a statement in flight for as long as the test likes.
        ok(&mut holder, "BEGIN").await;
        ok(&mut holder, "UPDATE t SET v = 1 WHERE id = 1").await;

        let mut doomed = h.conn().await;
        ok(&mut doomed, "BEGIN").await;
        ok(&mut doomed, "INSERT INTO t (id, v) VALUES (2, 0)").await;
        let in_flight = tokio::spawn(async move {
            let _ = doomed
                .send(b"UPDATE t SET v = 2 WHERE id = 1".to_vec())
                .await;
        });
        tokio::time::sleep(Duration::from_millis(500)).await;
        // The connection drops with the UPDATE waiting on the server.
        in_flight.abort();
        let _ = in_flight.await;
        tokio::time::sleep(Duration::from_millis(200)).await;

        // The holder commits: the doomed UPDATE then runs, writes v = 2 in
        // its block, and finds no one to answer.
        ok(&mut holder, "COMMIT").await;
        tokio::time::sleep(Duration::from_millis(500)).await;

        assert_eq!(
            int(&mut other, "SELECT v FROM t WHERE id = 1").await,
            1,
            "the statement in flight wrote into a block that was rolled back"
        );
        assert_eq!(
            count(&mut other, "t").await,
            1,
            "the dropped block's insert is gone"
        );
        let write = tokio::time::timeout(
            Duration::from_secs(10),
            send(&mut other, "UPDATE t SET v = 3 WHERE id = 1"),
        )
        .await
        .expect("no lock of the dropped block outlived it");
        assert_eq!(write, Ok(Answer::Ack(Some(1))));
        assert_eq!(int(&mut other, "SELECT v FROM t WHERE id = 1").await, 3);
    })
    .await
}

/// The server closes a connection idle past its request timeout (30 s by
/// default; 1 s here): a block left open on it is rolled back with it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_idle_close_rolls_back_an_open_block() {
    bounded(async {
        let h = Harness::start(Duration::from_secs(1)).await;
        table(&h, "t", 1).await;

        let mut tx = h.conn().await;
        ok(&mut tx, "BEGIN").await;
        ok(&mut tx, "UPDATE t SET v = 7 WHERE id = 1").await;
        ok(&mut tx, "INSERT INTO t (id, v) VALUES (2, 0)").await;
        tokio::time::sleep(Duration::from_millis(2500)).await;

        // The next statement finds the connection gone: an error, never a
        // statement run outside the block.
        let after = tx.send(b"COMMIT".to_vec()).await;
        assert!(
            after.is_err(),
            "the idle connection should be closed, got {after:?}"
        );

        let mut other = h.conn().await;
        let write = tokio::time::timeout(
            Duration::from_secs(10),
            send(&mut other, "UPDATE t SET v = 8 WHERE id = 1"),
        )
        .await
        .expect("the idle block's lock was released");
        assert_eq!(write, Ok(Answer::Ack(Some(1))));
        assert_eq!(
            count(&mut other, "t").await,
            1,
            "the idle block's insert is gone"
        );
    })
    .await
}

/// A multiplexed connection is shared by unrelated callers, so it cannot hold
/// a block; a plain one handles its requests in order, so statements a
/// client pipelines into its block run there, one after another.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn blocks_need_a_plain_connection_whose_requests_run_in_order() {
    bounded(async {
        let h = Harness::start(Duration::from_secs(30)).await;
        table(&h, "t", 0).await;

        let mux = TcpClientBuilder::new(&h.socket)
            .timeout(Duration::from_secs(30))
            .build_multiplexed()
            .await
            .unwrap();
        for sql in [
            "BEGIN",
            "START TRANSACTION",
            "SAVEPOINT a",
            "COMMIT",
            "ROLLBACK",
        ] {
            let m = decode(&mux.send(sql.as_bytes()).await.unwrap()).expect_err(sql);
            assert!(m.contains("connection of its own"), "{sql}: {m}");
            assert_eq!(code(&m).as_deref(), Some("0A000"), "{m}");
        }
        // Ordinary statements on it are untouched.
        decode(
            &mux.send(b"INSERT INTO t (id, v) VALUES (1, 0)")
                .await
                .unwrap(),
        )
        .unwrap();

        let mut tx = h.conn().await;
        ok(&mut tx, "BEGIN").await;
        let answers = tx
            .send_batch(&[
                "INSERT INTO t (id, v) VALUES (2, 0)",
                "INSERT INTO t (id, v) VALUES (3, 0)",
                "SELECT id FROM t",
                "INSERT INTO t (id, v) VALUES (2, 0)",
                "SELECT id FROM t",
                "ROLLBACK",
            ])
            .await
            .unwrap();
        let answers: Vec<_> = answers.iter().map(|a| decode(a)).collect();
        assert_eq!(answers[0], Ok(Answer::Ack(Some(1))));
        assert_eq!(answers[1], Ok(Answer::Ack(Some(1))));
        match &answers[2] {
            Ok(Answer::Rows(rows)) => assert_eq!(
                rows.len(),
                3,
                "both pipelined inserts ran first, in the block"
            ),
            other => panic!("{other:?}"),
        }
        assert_eq!(
            code(answers[3].as_ref().unwrap_err()).as_deref(),
            Some("23505")
        );
        assert_eq!(
            code(answers[4].as_ref().unwrap_err()).as_deref(),
            Some("25P02")
        );
        assert_eq!(answers[5], Ok(Answer::Ack(None)));
        assert_eq!(count(&mut tx, "t").await, 1);
    })
    .await
}

// ---- the same scripts over both listeners -----------------------------------

/// A minimal PostgreSQL simple-query client, enough to read each statement's
/// outcome.
struct Pg {
    stream: tokio::net::TcpStream,
}

impl Pg {
    async fn connect(port: u16) -> Pg {
        let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .unwrap();
        let mut body = Vec::new();
        body.extend_from_slice(&196608i32.to_be_bytes());
        body.extend_from_slice(b"user\0rasterized\0\0");
        let mut message = ((body.len() + 4) as i32).to_be_bytes().to_vec();
        message.extend_from_slice(&body);
        stream.write_all(&message).await.unwrap();
        let mut pg = Pg { stream };
        pg.until_ready().await.expect("pgwire startup");
        pg
    }

    /// Reads to `ReadyForQuery`: the first error's code, or the last tag.
    async fn until_ready(&mut self) -> Result<String, String> {
        let mut outcome: Result<String, String> = Ok(String::new());
        loop {
            let mut header = [0u8; 5];
            self.stream.read_exact(&mut header).await.unwrap();
            let len = i32::from_be_bytes(header[1..5].try_into().unwrap()) as usize - 4;
            let mut body = vec![0u8; len];
            self.stream.read_exact(&mut body).await.unwrap();
            match header[0] {
                b'E' if outcome.is_ok() => {
                    let mut at = 0;
                    let mut code = String::new();
                    while at < body.len() && body[at] != 0 {
                        let field = body[at];
                        let end = at + 1 + body[at + 1..].iter().position(|b| *b == 0).unwrap();
                        if field == b'C' {
                            code = String::from_utf8_lossy(&body[at + 1..end]).into_owned();
                        }
                        at = end + 1;
                    }
                    outcome = Err(code);
                }
                b'C' if outcome.is_ok() => {
                    outcome = Ok(String::from_utf8_lossy(&body[..body.len() - 1]).into_owned());
                }
                b'Z' => return outcome,
                _ => {}
            }
        }
    }

    async fn query(&mut self, sql: &str) -> Result<String, String> {
        let mut message = vec![b'Q'];
        message.extend_from_slice(&((sql.len() + 5) as i32).to_be_bytes());
        message.extend_from_slice(sql.as_bytes());
        message.push(0);
        self.stream.write_all(&message).await.unwrap();
        self.until_ready().await
    }
}

enum Conn {
    Native(TcpClient),
    Pg(Pg),
}

impl Conn {
    /// "ok", "rolled back" (a COMMIT that could not commit), or the SQLSTATE.
    async fn outcome(&mut self, sql: &str) -> String {
        match self {
            Conn::Native(c) => match send(c, sql).await {
                Ok(_) => "ok".to_string(),
                Err(m) if m.contains("rolled back, not committed") => "rolled back".to_string(),
                Err(m) => code(&m).unwrap_or_else(|| format!("no code: {m}")),
            },
            Conn::Pg(p) => match p.query(sql).await {
                Ok(tag) if tag == "ROLLBACK" && sql.trim().eq_ignore_ascii_case("COMMIT") => {
                    "rolled back".to_string()
                }
                Ok(_) => "ok".to_string(),
                Err(code) => code,
            },
        }
    }
}

async fn conns(h: &Harness, pg: bool, n: usize) -> Vec<Conn> {
    let mut out = Vec::new();
    for _ in 0..n {
        out.push(if pg {
            Conn::Pg(h.pg().await)
        } else {
            Conn::Native(h.conn().await)
        });
    }
    out
}

/// Runs `script` (session, statement; `{t}` is the table) and returns each
/// statement's outcome.
async fn run_script(h: &Harness, pg: bool, t: &str, script: &[(usize, &str)]) -> Vec<String> {
    let sessions = script.iter().map(|(s, _)| *s).max().unwrap_or(0) + 1;
    let mut c = conns(h, pg, sessions).await;
    let mut out = Vec::new();
    for (session, sql) in script {
        let sql = sql.replace("{t}", t);
        out.push(c[*session].outcome(&sql).await);
    }
    out
}

/// Every script runs over pgwire and over native against tables of their
/// own, and must give the same outcome, statement for statement.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_native_listener_answers_a_script_as_pgwire_does() {
    bounded(async {
        let h = Harness::start(Duration::from_secs(30)).await;
        let scripts: Vec<(&str, Vec<(usize, &str)>, Vec<&str>)> = vec![
            (
                "conflict",
                vec![
                    (0, "BEGIN ISOLATION LEVEL REPEATABLE READ"),
                    (1, "BEGIN ISOLATION LEVEL REPEATABLE READ"),
                    (0, "SELECT v FROM {t} WHERE id = 1"),
                    (1, "SELECT v FROM {t} WHERE id = 1"),
                    (0, "UPDATE {t} SET v = v + 1 WHERE id = 1"),
                    (0, "COMMIT"),
                    (1, "UPDATE {t} SET v = v + 1 WHERE id = 1"),
                    (1, "SELECT 1"),
                    (1, "COMMIT"),
                    // SERIALIZABLE is accepted and runs as REPEATABLE READ.
                    (1, "BEGIN ISOLATION LEVEL SERIALIZABLE"),
                    (1, "SELECT v FROM {t} WHERE id = 1"),
                    (0, "UPDATE {t} SET v = v + 1 WHERE id = 1"),
                    (1, "UPDATE {t} SET v = v + 1 WHERE id = 1"),
                    (1, "ROLLBACK"),
                ],
                vec![
                    "ok",
                    "ok",
                    "ok",
                    "ok",
                    "ok",
                    "ok",
                    "40001",
                    "25P02",
                    "rolled back",
                    "ok",
                    "ok",
                    "ok",
                    "40001",
                    "ok",
                ],
            ),
            (
                "aborted",
                vec![
                    (0, "BEGIN"),
                    (0, "INSERT INTO {t} (id, v) VALUES (1, 0)"),
                    (0, "SELECT v FROM {t}"),
                    (0, "SAVEPOINT a"),
                    (0, "ROLLBACK"),
                    (0, "BEGIN"),
                    (0, "SAVEPOINT a"),
                    (0, "INSERT INTO {t} (id, v) VALUES (1, 0)"),
                    (0, "ROLLBACK TO a"),
                    (0, "INSERT INTO {t} (id, v) VALUES (3, 0)"),
                    (0, "RELEASE a"),
                    (0, "ROLLBACK TO a"),
                    (0, "COMMIT"),
                    (0, "SAVEPOINT b"),
                    (0, "LOCK TABLE {t} IN ACCESS EXCLUSIVE MODE"),
                    (0, "BEGIN READ ONLY"),
                    (0, "INSERT INTO {t} (id, v) VALUES (4, 0)"),
                    (0, "ROLLBACK"),
                    (0, "BEGIN"),
                    (0, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"),
                    (0, "SELECT v FROM {t} WHERE id = 1"),
                    (0, "SET TRANSACTION ISOLATION LEVEL READ COMMITTED"),
                    (0, "ROLLBACK"),
                ],
                vec![
                    // A ROLLBACK TO an unknown savepoint (3B001) fails the
                    // block, as in PostgreSQL, so the COMMIT after it rolls
                    // back.
                    "ok",
                    "23505",
                    "25P02",
                    "25P02",
                    "ok",
                    "ok",
                    "ok",
                    "23505",
                    "ok",
                    "ok",
                    "ok",
                    "3B001",
                    "rolled back",
                    "25P01",
                    "25P01",
                    "ok",
                    "25006",
                    "ok",
                    "ok",
                    "ok",
                    "ok",
                    "25001",
                    "ok",
                ],
            ),
            (
                "ddl",
                vec![
                    (0, "BEGIN"),
                    (0, "CREATE TABLE {t}_x (id BIGINT PRIMARY KEY)"),
                    (0, "INSERT INTO {t}_x (id) VALUES (1)"),
                    (0, "ROLLBACK"),
                    (0, "BEGIN"),
                    (0, "SELECT id FROM {t}_x"),
                    (0, "ROLLBACK"),
                    (0, "BEGIN"),
                    (0, "DROP TABLE {t}"),
                    (0, "ROLLBACK"),
                    (0, "BEGIN"),
                    (0, "SELECT v FROM {t} WHERE id = 1"),
                    (0, "COMMIT"),
                ],
                vec![
                    "ok", "ok", "ok", "ok", "ok", "42P01", "ok", "ok", "ok", "ok", "ok", "ok", "ok",
                ],
            ),
            (
                // A key the block itself wrote, written again under a
                // savepoint and after ROLLBACK TO it: 23505, as in
                // PostgreSQL, on both listeners (it was 40001, "being
                // written by another transaction").
                "own key under a savepoint",
                vec![
                    (0, "BEGIN"),
                    (0, "INSERT INTO {t} (id, v) VALUES (10, 0)"),
                    (0, "SAVEPOINT a"),
                    (0, "INSERT INTO {t} (id, v) VALUES (10, 0)"),
                    (0, "ROLLBACK TO a"),
                    (0, "INSERT INTO {t} (id, v) VALUES (10, 0)"),
                    (0, "ROLLBACK"),
                ],
                vec!["ok", "ok", "ok", "23505", "ok", "23505", "ok"],
            ),
            (
                "lock timeout",
                vec![
                    (0, "BEGIN"),
                    (0, "LOCK TABLE {t} IN ACCESS EXCLUSIVE MODE"),
                    (1, "BEGIN"),
                    (1, "SET lock_timeout = '200ms'"),
                    (1, "SELECT v FROM {t} WHERE id = 1"),
                    (1, "ROLLBACK"),
                    (0, "ROLLBACK"),
                ],
                vec!["ok", "ok", "ok", "ok", "55P03", "ok", "ok"],
            ),
        ];

        for (name, script, expected) in &scripts {
            let mut results = Vec::new();
            for pg in [true, false] {
                let t = format!(
                    "s_{}_{}",
                    name.replace(' ', "_"),
                    if pg { "pg" } else { "native" }
                );
                table(&h, &t, 2).await;
                results.push(run_script(&h, pg, &t, script).await);
            }
            assert_eq!(
                results[0], results[1],
                "{name}: pgwire (left) and native (right) differ"
            );
            assert_eq!(&results[1], expected, "{name}");
        }
    })
    .await
}

/// Two blocks each waiting for the other's row: one is chosen and fails with
/// 40P01, the other goes on, on both listeners.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_deadlock_fails_one_block_with_40p01_as_pgwire_does() {
    bounded(async {
        let h = Harness::start(Duration::from_secs(30)).await;
        for pg in [true, false] {
            let t = format!("dl_{}", if pg { "pg" } else { "native" });
            table(&h, &t, 2).await;
            let mut c = conns(&h, pg, 2).await;
            let mut second = c.pop().unwrap();
            let mut first = c.pop().unwrap();
            assert_eq!(first.outcome("BEGIN").await, "ok");
            assert_eq!(second.outcome("BEGIN").await, "ok");
            assert_eq!(
                first
                    .outcome(&format!("UPDATE {t} SET v = 1 WHERE id = 1"))
                    .await,
                "ok"
            );
            assert_eq!(
                second
                    .outcome(&format!("UPDATE {t} SET v = 2 WHERE id = 2"))
                    .await,
                "ok"
            );
            let waiting = {
                let sql = format!("UPDATE {t} SET v = 1 WHERE id = 2");
                tokio::spawn(async move {
                    let outcome = first.outcome(&sql).await;
                    (first, outcome)
                })
            };
            tokio::time::sleep(Duration::from_millis(300)).await;
            let second_outcome = second
                .outcome(&format!("UPDATE {t} SET v = 2 WHERE id = 1"))
                .await;
            // The victim's error ends its block's transaction (R3), so the
            // other is let through; a victim that is the first is answered
            // first, then the second goes on.
            let (mut first, first_outcome) = tokio::time::timeout(Duration::from_secs(30), waiting)
                .await
                .expect("the deadlock was resolved")
                .unwrap();
            let mut outcomes = vec![first_outcome.clone(), second_outcome.clone()];
            outcomes.sort();
            assert_eq!(
                outcomes,
                vec!["40P01".to_string(), "ok".to_string()],
                "pg = {pg}"
            );
            first.outcome("ROLLBACK").await;
            second.outcome("ROLLBACK").await;
        }
    })
    .await
}

/// A correlated scalar subquery over native, on a plain and a multiplexed
/// connection and inside a block: the LATERAL join it is rewritten into
/// needs the statement transaction pgwire opens, which the native path does
/// not have, so it failed with `missing FROM-clause entry for table "__cs0"`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_correlated_scalar_subquery_runs_over_native() {
    bounded(async {
        let h = Harness::start(Duration::from_secs(30)).await;
        let mut conn = h.conn().await;
        for sql in [
            "CREATE TABLE u7 (id BIGINT PRIMARY KEY)",
            "CREATE TABLE p7 (id BIGINT PRIMARY KEY, user_id BIGINT)",
            "INSERT INTO u7 (id) VALUES (1), (2), (3)",
            "INSERT INTO p7 (id, user_id) VALUES (1, 1), (2, 1), (3, 2)",
        ] {
            ok(&mut conn, sql).await;
        }
        let counts = "SELECT id, (SELECT COUNT(*) FROM p7 WHERE p7.user_id = u7.id) FROM u7 ORDER BY id";
        let rows = |counts: [i64; 3]| {
            Answer::Rows(vec![
                vec![Cell::Int(1), Cell::Int(counts[0])],
                vec![Cell::Int(2), Cell::Int(counts[1])],
                vec![Cell::Int(3), Cell::Int(counts[2])],
            ])
        };
        assert_eq!(ok(&mut conn, counts).await, rows([2, 1, 0]));
        assert_eq!(
            ok(&mut conn, "SELECT id FROM u7 WHERE (SELECT COUNT(*) FROM p7 WHERE p7.user_id = u7.id) > 0 ORDER BY id")
                .await,
            Answer::Rows(vec![vec![Cell::Int(1)], vec![Cell::Int(2)]])
        );
        let m = fails(&mut conn, "SELECT id, (SELECT p7.id FROM p7 WHERE p7.user_id = u7.id) FROM u7").await;
        assert_eq!(code(&m).as_deref(), Some("21000"), "{m}");

        let mux = TcpClientBuilder::new(&h.socket)
            .timeout(Duration::from_secs(30))
            .build_multiplexed()
            .await
            .unwrap();
        assert_eq!(decode(&mux.send(counts.as_bytes()).await.unwrap()).unwrap(), rows([2, 1, 0]));

        // In a block it reads the block's own rows, and the block goes on.
        ok(&mut conn, "BEGIN").await;
        ok(&mut conn, "INSERT INTO p7 (id, user_id) VALUES (9, 3)").await;
        assert_eq!(ok(&mut conn, counts).await, rows([2, 1, 1]));
        ok(&mut conn, "ROLLBACK").await;
        assert_eq!(ok(&mut conn, counts).await, rows([2, 1, 0]));
    })
    .await
}

/// An autocommit `SELECT` over native learns a parse for its template, as a
/// pgwire simple query does, and then runs from it. Its answers must be the
/// plain path's for every value, learned with or not, and a schema change
/// after it was learned must be seen.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_learned_native_select_answers_as_the_plain_path() {
    use rasterizeddb_core::pgwire::{TEXT_RUNS, set_text_shapes};
    use std::sync::atomic::Ordering;
    bounded(async {
        let h = Harness::start(Duration::from_secs(30)).await;
        table(&h, "lt", 60).await;
        let mut c = h.conn().await;
        ok(&mut c, "UPDATE lt SET v = id * 10").await;

        let shapes: [(&str, &[&[&str]]); 4] = [
            ("SELECT id, v FROM lt WHERE id = $1", &[&["3"], &["41"], &["60"], &["999"]]),
            ("SELECT id FROM lt WHERE id >= $1 AND id < $2 ORDER BY id", &[&["5", "9"], &["55", "70"], &["9", "9"]]),
            ("SELECT COUNT(*) FROM lt WHERE v > $1", &[&["100"], &["590"], &["0"]]),
            ("SELECT id FROM lt WHERE id = $1 OR id = 2 ORDER BY id", &[&["7"], &["8"]]),
        ];
        let render = |shape: &str, values: &[&str]| {
            let mut sql = shape.to_string();
            for (i, v) in values.iter().enumerate().rev() {
                sql = sql.replace(&format!("${}", i + 1), v);
            }
            sql
        };
        set_text_shapes(false);
        let mut expected = Vec::new();
        for (shape, sets) in &shapes {
            for values in *sets {
                expected.push(send(&mut c, &render(shape, values)).await);
            }
        }
        set_text_shapes(true);
        let before = TEXT_RUNS.load(Ordering::Relaxed);
        for _round in 0..3 {
            let mut want = expected.iter();
            for (shape, sets) in &shapes {
                for values in *sets {
                    let sql = render(shape, values);
                    assert_eq!(&send(&mut c, &sql).await, want.next().unwrap(), "{sql}");
                }
            }
        }
        assert!(TEXT_RUNS.load(Ordering::Relaxed) > before, "no native statement ran from a learned parse");

        // A column added after the template was learned is read.
        for id in [1, 2, 3] {
            ok(&mut c, &format!("SELECT * FROM lt WHERE id = {id}")).await;
        }
        ok(&mut c, "ALTER TABLE lt ADD COLUMN w BIGINT").await;
        ok(&mut c, "UPDATE lt SET w = id + 1000").await;
        match ok(&mut c, "SELECT * FROM lt WHERE id = 4").await {
            Answer::Rows(rows) => assert_eq!(rows, vec![vec![Cell::Int(4), Cell::Int(40), Cell::Int(1004)]]),
            other => panic!("{other:?}"),
        }

        // In a block a learned template reads the block's own rows.
        for id in [5, 6, 7] {
            ok(&mut c, &format!("SELECT id, v FROM lt WHERE id = {id}")).await;
        }
        ok(&mut c, "BEGIN").await;
        ok(&mut c, "INSERT INTO lt (id, v) VALUES (5000, 1)").await;
        assert_eq!(int(&mut c, "SELECT v FROM lt WHERE id = 5000").await, 1);
        ok(&mut c, "ROLLBACK").await;
        match ok(&mut c, "SELECT id, v FROM lt WHERE id = 5000").await {
            Answer::Rows(rows) => assert!(rows.is_empty(), "{rows:?}"),
            other => panic!("{other:?}"),
        }
    })
    .await;
}
