//! The statements of the objects kept above the engine (views, sequences,
//! routines, triggers, MERGE, temporary tables) and CTEs and derived tables,
//! over the native protocol. Each was refused there ("not supported over
//! the native protocol yet"); they now run through the PostgreSQL listener's
//! statement path, in a session made for each statement, with the
//! connection's sequence values and temporary tables carried across.
//!
//! Needs `RDB_SERVER_BIN` (a server binary); skipped without it.

use std::process::Stdio;
use std::time::Duration;

use rasterizeddb_client::{Client, Options, params};
use tokio::process::Command;

#[tokio::test(flavor = "multi_thread")]
async fn object_statements_run_over_the_native_protocol() {
    let Ok(bin) = std::env::var("RDB_SERVER_BIN") else {
        eprintln!("RDB_SERVER_BIN is not set: skipping");
        return;
    };
    let dir = std::env::temp_dir().join(format!("rdb_native_objects_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("db")).unwrap();
    let socket = dir.join("s.sock");
    let mut child = Command::new(&bin)
        .arg("--location")
        .arg(dir.join("db"))
        .arg("--rdb-socket")
        .arg(&socket)
        .args([
            "--rdb-tcp",
            "off",
            "--rdb-socket-auth",
            "trust",
            "--socket-mode",
            "600",
            "--log-level",
            "warn",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .expect("start the server");
    for _ in 0..600 {
        if std::os::unix::net::UnixStream::connect(&socket).is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let address = format!("unix:{}", socket.display());
    let c = Client::connect(
        &address,
        Options {
            timeout: Some(Duration::from_secs(30)),
            ..Options::default()
        },
    )
    .await
    .expect("connect");
    let run = |sql: &'static str| {
        let c = &c;
        async move { c.run(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}")) }
    };
    run("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)").await;
    run("INSERT INTO t (id, v) VALUES (1, 10), (2, 20), (3, 30)").await;
    run("CREATE VIEW big AS SELECT id, v FROM t WHERE v >= 20").await;
    let n: i64 = c
        .fetch_scalar("SELECT count(*) FROM big", params![])
        .await
        .expect("view");
    assert_eq!(n, 2);
    let s: i64 = c
        .fetch_scalar(
            "WITH q AS (SELECT v FROM t) SELECT sum(v) FROM q",
            params![],
        )
        .await
        .expect("cte");
    assert_eq!(s, 60);
    let m: i64 = c
        .fetch_scalar("SELECT max(x) FROM (SELECT v * 2 AS x FROM t) d", params![])
        .await
        .expect("derived");
    assert_eq!(m, 60);
    run("CREATE SEQUENCE s START 5").await;
    let a: i64 = c
        .fetch_scalar("SELECT nextval('s')", params![])
        .await
        .expect("nextval");
    let b: i64 = c
        .fetch_scalar("SELECT currval('s')", params![])
        .await
        .expect("currval");
    assert_eq!((a, b), (5, 5));
    // A column's own sequence, taken by a plain INSERT on the native path:
    // the value is the connection's for `currval` and `lastval`.
    run("CREATE TABLE ser (id BIGSERIAL PRIMARY KEY, g BIGINT GENERATED ALWAYS AS IDENTITY, v BIGINT)").await;
    run("INSERT INTO ser (v) VALUES (1)").await;
    run("INSERT INTO ser (v) VALUES (2)").await;
    let (id, g): (i64, i64) = c
        .fetch_one("SELECT id, g FROM ser WHERE v = $1", params![2i64])
        .await
        .expect("serial row");
    assert_eq!((id, g), (2, 2));
    let cur: i64 = c
        .fetch_scalar("SELECT currval('ser_id_seq')", params![])
        .await
        .expect("currval of a serial");
    assert_eq!(cur, 2);
    let last: i64 = c
        .fetch_scalar("SELECT lastval()", params![])
        .await
        .expect("lastval");
    assert_eq!(last, 2);
    run("CREATE FUNCTION dbl(x bigint) RETURNS bigint LANGUAGE sql AS 'SELECT x * 2'").await;
    let d: i64 = c
        .fetch_scalar("SELECT dbl(v) FROM t WHERE id = $1", params![3i64])
        .await
        .expect("function");
    assert_eq!(d, 60);
    run("CREATE TABLE log (id BIGINT, v BIGINT)").await;
    run("CREATE FUNCTION audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO log VALUES (NEW.id, NEW.v); RETURN NEW; END $$").await;
    run("CREATE TRIGGER t_audit AFTER INSERT ON t FOR EACH ROW EXECUTE FUNCTION audit()").await;
    run("INSERT INTO t (id, v) VALUES (4, 40)").await;
    let logged: i64 = c
        .fetch_scalar("SELECT count(*) FROM log", params![])
        .await
        .expect("trigger");
    assert_eq!(logged, 1);
    run("MERGE INTO t USING (VALUES (1, 11), (9, 99)) AS s(id, v) ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = s.v WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.v)").await;
    let merged: i64 = c
        .fetch_scalar("SELECT v FROM t WHERE id = 9", params![])
        .await
        .expect("merge");
    assert_eq!(merged, 99);
    run("CREATE TEMP TABLE scratch (x BIGINT)").await;
    run("INSERT INTO scratch VALUES (7)").await;
    let x: i64 = c
        .fetch_scalar("SELECT x FROM scratch", params![])
        .await
        .expect("temp table across statements");
    assert_eq!(x, 7);
    let _ = child.kill().await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// Cells typed by their columns over the native protocol: a `jsonb` column
/// as its JSON text, a `bytea` as bytes, an array as an array, a
/// `NUMERIC(10,2)` and a `sum()` with their scales. Each arrived under its
/// storage type (a document as engine bytes, a NUMERIC with no point) and was
/// refused or misread by the client.
#[tokio::test(flavor = "multi_thread")]
async fn columns_arrive_as_their_declared_types() {
    let Ok(bin) = std::env::var("RDB_SERVER_BIN") else {
        eprintln!("RDB_SERVER_BIN is not set: skipping");
        return;
    };
    let dir = std::env::temp_dir().join(format!("rdb_native_types_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("db")).unwrap();
    let socket = dir.join("s.sock");
    let mut child = Command::new(&bin)
        .arg("--location")
        .arg(dir.join("db"))
        .arg("--rdb-socket")
        .arg(&socket)
        .args([
            "--rdb-tcp",
            "off",
            "--rdb-socket-auth",
            "trust",
            "--socket-mode",
            "600",
            "--log-level",
            "warn",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .expect("start the server");
    for _ in 0..600 {
        if std::os::unix::net::UnixStream::connect(&socket).is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let address = format!("unix:{}", socket.display());
    let c = Client::connect(
        &address,
        Options {
            timeout: Some(Duration::from_secs(30)),
            ..Options::default()
        },
    )
    .await
    .expect("connect");
    c.run("CREATE TABLE d (id BIGINT PRIMARY KEY, doc JSONB, raw BYTEA, tags BIGINT[], price NUMERIC(10,2))").await.expect("create");
    c.run("INSERT INTO d VALUES (1, '{\"a\": [1, 2]}', '\\x0102ff', '{3,4}', 12.50), (2, NULL, NULL, NULL, 0.25)").await.expect("insert");
    let doc: rasterizeddb_client::Json = c
        .fetch_scalar("SELECT doc FROM d WHERE id = $1", params![1i64])
        .await
        .expect("jsonb as json");
    assert_eq!(doc.as_str(), "{\"a\": [1, 2]}");
    let text: String = c
        .fetch_scalar("SELECT doc FROM d WHERE id = $1", params![1i64])
        .await
        .expect("jsonb as text");
    assert_eq!(text, "{\"a\": [1, 2]}");
    let raw: Vec<u8> = c
        .fetch_scalar("SELECT raw FROM d WHERE id = $1", params![1i64])
        .await
        .expect("bytea");
    assert_eq!(raw, [1, 2, 255]);
    let tags: Vec<i64> = c
        .fetch_scalar("SELECT tags FROM d WHERE id = $1", params![1i64])
        .await
        .expect("array");
    assert_eq!(tags, [3, 4]);
    let price: rasterizeddb_client::Numeric = c
        .fetch_scalar("SELECT price FROM d WHERE id = $1", params![1i64])
        .await
        .expect("numeric(10,2)");
    assert_eq!(price.to_string(), "12.50");
    let total: rasterizeddb_client::Numeric = c
        .fetch_scalar("SELECT sum(id) FROM d", params![])
        .await
        .expect("sum");
    assert_eq!(total.to_string(), "3");
    // The same inside a block, which runs through the session's path.
    {
        use rasterizeddb_client::Executor;
        let tx = c.begin().await.expect("begin");
        let price: rasterizeddb_client::Numeric = tx
            .fetch_scalar("SELECT price FROM d WHERE id = $1", params![2i64])
            .await
            .expect("numeric in a block");
        assert_eq!(price.to_string(), "0.25");
        let doc: String = tx
            .fetch_scalar("SELECT doc FROM d WHERE id = $1", params![1i64])
            .await
            .expect("jsonb in a block");
        assert_eq!(doc, "{\"a\": [1, 2]}");
        let raw: Vec<u8> = tx
            .fetch_scalar("SELECT raw FROM d WHERE id = $1", params![1i64])
            .await
            .expect("bytea in a block");
        assert_eq!(raw, [1, 2, 255]);
        tx.commit().await.expect("commit");
    }
    let _ = child.kill().await;
    let _ = std::fs::remove_dir_all(&dir);
}
