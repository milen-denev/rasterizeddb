//! The driver matrix: sqlx and tokio-postgres through every surface of the
//! table-lock manager (slice B8, `docs/persistent-indexes.md` §7 "Driver
//! gate", §13 V10 case 14). pgjdbc and npgsql are replayed frame by frame
//! in `tests/wire_replay/`.
//!
//! - **Shape change mid-prepare, both modes** (B7): each driver prepares
//!   and describes a statement, another session changes the table's shape
//!   (ADD COLUMN, DROP COLUMN, INT to BIGINT), and the statement is executed
//!   four times; each execute is classed `ok` (the twin's names and values),
//!   `E<sqlstate>`, `protocol` (the driver refused the reply), or `WRONG`
//!   (rows decoded under the old columns: a silent wrong answer, which no
//!   cell may ever show). The table is what the owner decides
//!   `rdb.shape_change`'s default from (finding L10, open question Q1).
//! - **Lock waits** under `lock_timeout` (55P03) and unbounded (`0`, the
//!   default: the statement waits and then runs).
//! - **Cancellation** of a lock wait (tokio-postgres's CancelRequest,
//!   sqlx's `statement_timeout`): 57014.
//! - **Deadlock** across the two drivers: exactly one 40P01, and the
//!   victim's block is over at once (R3).
//! - **A block error** (R3): 25P02 after it, and the block's locks are gone
//!   before its ROLLBACK, so a DDL waiting on it goes on.
//! - **DDL vs DML**: a block's prepared write holds off another driver's
//!   ALTER, which then runs; the write's cached statement works after it.
//!
//! Every test is bounded (`BOUND`), so a hang fails rather than hangs.

use std::time::{Duration, Instant};

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Column, Connection, Executor, PgConnection, Row, TypeInfo};

const BOUND: Duration = Duration::from_secs(10);

async fn bounded<F: std::future::Future>(what: &str, f: F) -> F::Output {
    tokio::time::timeout(BOUND, f)
        .await
        .unwrap_or_else(|_| panic!("{what}: did not finish within {BOUND:?}"))
}

fn url_with(server: &TestServer, options: &[&str]) -> String {
    if options.is_empty() {
        return server.url();
    }
    let joined = options.iter().map(|o| format!("-c%20{}", o.replace('=', "%3D"))).collect::<Vec<_>>().join("%20");
    format!("{}&options={joined}", server.url())
}

async fn tp(url: &str) -> tokio_postgres::Client {
    let (client, conn) = tokio_postgres::connect(url, tokio_postgres::NoTls)
        .await
        .expect("tokio-postgres connects");
    tokio::spawn(conn);
    client
}

fn tp_code(e: &tokio_postgres::Error) -> Option<String> {
    e.as_db_error().map(|d| d.code().code().to_string())
}

fn sqlx_code(e: &sqlx::Error) -> Option<String> {
    e.as_database_error().and_then(|d| d.code()).map(|c| c.to_string())
}

// ---------------------------------------------------------------------------
// Shape change mid-prepare.
// ---------------------------------------------------------------------------

struct Change {
    name: &'static str,
    setup: &'static [&'static str],
    alter: &'static [&'static str],
}

const CHANGES: &[Change] = &[
    Change {
        name: "add column",
        setup: &[
            "CREATE TABLE t (id INT, a TEXT, b INT)",
            "INSERT INTO t (id, a, b) VALUES (1, 'x', 10)",
            "INSERT INTO t (id, a, b) VALUES (2, 'y', 20)",
        ],
        alter: &["ALTER TABLE t ADD COLUMN c INT", "UPDATE t SET c = id * 7"],
    },
    Change {
        name: "drop column",
        setup: &[
            "CREATE TABLE t (id INT, a INT, b INT)",
            "INSERT INTO t (id, a, b) VALUES (1, 10, 100)",
            "INSERT INTO t (id, a, b) VALUES (2, 20, 200)",
        ],
        alter: &["ALTER TABLE t DROP COLUMN a"],
    },
    // INT to BIGINT: `ALTER COLUMN … TYPE` to another type is refused here,
    // so the column is dropped and added back under its name, wider.
    Change {
        name: "int to bigint",
        setup: &[
            "CREATE TABLE t (id INT, v INT)",
            "INSERT INTO t (id, v) VALUES (1, 5)",
            "INSERT INTO t (id, v) VALUES (2, 6)",
        ],
        alter: &[
            "ALTER TABLE t DROP COLUMN v",
            "ALTER TABLE t ADD COLUMN v BIGINT",
            "UPDATE t SET v = id * 5000000000",
        ],
    },
];

const STAR: &str = "SELECT * FROM t ORDER BY id";

fn sqlx_render(row: &sqlx::postgres::PgRow) -> String {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        row.columns()
            .iter()
            .map(|c| {
                let i = c.ordinal();
                let v = match c.type_info().name() {
                    "INT4" => row.try_get::<Option<i32>, _>(i).map(|v| format!("{v:?}")),
                    "INT8" => row.try_get::<Option<i64>, _>(i).map(|v| format!("{v:?}")),
                    _ => row.try_get::<Option<String>, _>(i).map(|v| format!("{v:?}")),
                };
                format!("{}={}", c.name(), v.unwrap_or_else(|e| format!("<{e}>")))
            })
            .collect::<Vec<_>>()
            .join(" ")
    }))
    .unwrap_or_else(|_| "<panicked decoding>".to_string())
}

fn tp_render(row: &tokio_postgres::Row) -> String {
    use tokio_postgres::types::Type;
    row.columns()
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let v = match *c.type_() {
                Type::INT4 => row.try_get::<_, Option<i32>>(i).map(|v| format!("{v:?}")),
                Type::INT8 => row.try_get::<_, Option<i64>>(i).map(|v| format!("{v:?}")),
                _ => row.try_get::<_, Option<String>>(i).map(|v| format!("{v:?}")),
            };
            format!("{}={}", c.name(), v.unwrap_or_else(|e| format!("<{e}>")))
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// The twin: the text as a simple query, nothing cached, through sqlx.
async fn twin(conn: &mut PgConnection) -> Vec<String> {
    sqlx::raw_sql(AssertSqlSafe(STAR.to_string()))
        .fetch_all(&mut *conn)
        .await
        .expect("twin")
        .iter()
        .map(sqlx_render)
        .collect()
}

fn class(got: Result<Vec<String>, (Option<String>, String)>, twin: &[String]) -> String {
    match got {
        Ok(rows) if rows == twin => "ok".to_string(),
        Ok(_) => "WRONG".to_string(),
        Err((Some(code), _)) => format!("E{code}"),
        Err((None, _)) => "protocol".to_string(),
    }
}

/// One execute of [`STAR`] through sqlx's statement cache.
async fn run(conn: &mut PgConnection) -> Result<Vec<String>, (Option<String>, String)> {
    sqlx::query(AssertSqlSafe(STAR.to_string()))
        .fetch_all(conn)
        .await
        .map(|rows| rows.iter().map(sqlx_render).collect::<Vec<_>>())
        .map_err(|e| (sqlx_code(&e), e.to_string()))
}

/// One driver, one mode, one change: four executes after the change, then
/// what the driver's own recovery gives (sqlx: the same cached statement
/// again; tokio-postgres: a fresh `prepare`).
async fn shape_cell(driver: &str, mode: &str, change: &Change) -> (Vec<String>, String, Vec<String>) {
    let server = TestServer::start().await.expect("server");
    // `default` sends nothing: the server's default (`auto` since B8b).
    let option = format!("rdb.shape_change={mode}");
    let options: Vec<&str> = if mode == "default" { Vec::new() } else { vec![option.as_str()] };
    let url = url_with(&server, &options);
    let mut admin = PgConnection::connect(&server.url()).await.expect("admin");
    for sql in change.setup {
        admin.execute(*sql).await.expect("setup");
    }
    let mut outcomes = Vec::new();
    let mut detail = Vec::new();
    let healed;
    match driver {
        "sqlx" => {
            let mut conn = PgConnection::connect(&url).await.expect("sqlx connects");
            run(&mut conn).await.expect("first execute");
            for sql in change.alter {
                admin.execute(*sql).await.expect("alter");
            }
            let expected = twin(&mut admin).await;
            for _ in 0..4 {
                let got = run(&mut conn).await;
                if let Err((_, e)) = &got {
                    detail.push(e.clone());
                }
                outcomes.push(class(got, &expected));
            }
            healed = class(run(&mut conn).await, &expected);
        }
        _ => {
            let client = tp(&url).await;
            let stmt = client.prepare(STAR).await.expect("prepare");
            client.query(&stmt, &[]).await.expect("first execute");
            for sql in change.alter {
                admin.execute(*sql).await.expect("alter");
            }
            let expected = twin(&mut admin).await;
            for _ in 0..4 {
                let got = client
                    .query(&stmt, &[])
                    .await
                    .map(|rows| rows.iter().map(tp_render).collect::<Vec<_>>())
                    .map_err(|e| (tp_code(&e), e.to_string()));
                if let Err((_, e)) = &got {
                    detail.push(e.clone());
                }
                outcomes.push(class(got, &expected));
            }
            let fresh = match client.prepare(STAR).await {
                Ok(s) => client
                    .query(&s, &[])
                    .await
                    .map(|rows| rows.iter().map(tp_render).collect::<Vec<_>>())
                    .map_err(|e| (tp_code(&e), e.to_string())),
                Err(e) => Err((tp_code(&e), e.to_string())),
            };
            healed = class(fresh, &expected);
        }
    }
    (outcomes, healed, detail)
}

/// What each driver gets, per mode and change: the L10 input. Asserted
/// exactly, so a driver upgrade or a server change that moves a cell is
/// seen; `WRONG` must appear nowhere.
///
/// - sqlx needs `rowdescription`: it applies a RowDescription that arrives
///   with the rows to that execution, and never re-prepares on 0A000 (its
///   cached statement fails every time, `sqlx-ignores-0a000`).
/// - tokio-postgres works in neither mode by itself: a RowDescription after
///   Bind is "unexpected message" to it, and 0A000 is returned to the
///   application on every execute of the kept `Statement`; in both, the
///   connection stays usable and a fresh `prepare` reads the new shape.
///   Error mode is the PostgreSQL-compatible one (an application already
///   written for PostgreSQL handles it).
/// - the default, `auto` (slice B8b, Q1 as the owner decided it, finding
///   L10), sent as nothing: each stale statement's first use answers 0A000,
///   as PostgreSQL does; executed again without a new Parse, it answers with
///   the RowDescription. sqlx re-executes its cached statement and is healed
///   from the second execute on; tokio-postgres gets PostgreSQL's 0A000
///   first, and its fresh `prepare` (a new Parse) reads the new shape, as in
///   both fixed modes. pgjdbc and npgsql re-Parse after the 0A000 (the
///   replays' `…_under_the_default_answers_as_postgresql_17`).
const EXPECTED: &[(&str, &str, [&str; 4], &str)] = &[
    ("sqlx", "rowdescription", ["ok", "ok", "ok", "ok"], "ok"),
    ("sqlx", "error", ["E0A000", "E0A000", "E0A000", "E0A000"], "E0A000"),
    ("sqlx", "default", ["E0A000", "ok", "ok", "ok"], "ok"),
    ("tokio-postgres", "rowdescription", ["protocol", "protocol", "protocol", "protocol"], "ok"),
    ("tokio-postgres", "error", ["E0A000", "E0A000", "E0A000", "E0A000"], "ok"),
    ("tokio-postgres", "default", ["E0A000", "protocol", "protocol", "protocol"], "ok"),
];

#[tokio::test]
async fn shape_change_mid_prepare_through_each_driver_in_both_modes() {
    let mut table = Vec::new();
    let mut bad = Vec::new();
    for (driver, mode, want, want_healed) in EXPECTED {
        for change in CHANGES {
            let (got, healed, detail) =
                bounded(&format!("{driver}/{mode}/{}", change.name), shape_cell(driver, mode, change)).await;
            table.push(format!("{driver:15} {mode:15} {:14} {:?} then {healed}", change.name, got));
            if got.iter().any(|c| c == "WRONG") {
                bad.push(format!("{driver}/{mode}/{}: rows decoded under the old columns: {got:?}", change.name));
            }
            if got != *want || healed != *want_healed {
                bad.push(format!(
                    "{driver}/{mode}/{}: got {got:?} then {healed}, want {want:?} then {want_healed} ({detail:?})",
                    change.name
                ));
            }
        }
    }
    println!("driver matrix (four executes after the change, then the driver's recovery):\n{}", table.join("\n"));
    assert!(bad.is_empty(), "\n{}", bad.join("\n"));
}

// ---------------------------------------------------------------------------
// Lock waits, cancellation, deadlock, block errors, DDL vs DML.
// ---------------------------------------------------------------------------

async fn seeded() -> TestServer {
    let server = TestServer::start().await.expect("server");
    let admin = tp(&server.url()).await;
    for sql in [
        "CREATE TABLE a (id INT PRIMARY KEY, x INT)",
        "CREATE TABLE b (id INT PRIMARY KEY, x INT)",
        "INSERT INTO a (id, x) VALUES (1, 10)",
        "INSERT INTO b (id, x) VALUES (1, 10)",
    ] {
        admin.batch_execute(sql).await.expect("seed");
    }
    server
}

/// A holder's ACCESS EXCLUSIVE: under `lock_timeout` each driver's read
/// fails with 55P03; under the default `0` it waits as long as the holder
/// holds and then runs.
#[tokio::test]
async fn lock_waits_through_each_driver() {
    bounded("lock waits", async {
        let server = seeded().await;
        let holder = tp(&server.url()).await;
        holder.batch_execute("BEGIN").await.unwrap();
        holder.batch_execute("LOCK TABLE a IN ACCESS EXCLUSIVE MODE").await.unwrap();

        let mut sx = PgConnection::connect(&url_with(&server, &["lock_timeout=300"])).await.unwrap();
        let e = sqlx::query("SELECT x FROM a WHERE id = $1").bind(1i32).fetch_all(&mut sx).await.unwrap_err();
        assert_eq!(sqlx_code(&e).as_deref(), Some("55P03"), "sqlx: {e}");
        let t = tp(&server.url()).await;
        t.batch_execute("SET lock_timeout = '300ms'").await.unwrap();
        let e = t.query("SELECT x FROM a WHERE id = $1", &[&1i32]).await.unwrap_err();
        assert_eq!(tp_code(&e).as_deref(), Some("55P03"), "tokio-postgres: {e}");

        // Unbounded: both wait, then run once the holder ends.
        let mut sx0 = PgConnection::connect(&server.url()).await.unwrap();
        let t0 = tp(&server.url()).await;
        let sqlx_wait = tokio::spawn(async move {
            sqlx::query("SELECT x FROM a WHERE id = $1").bind(1i32).fetch_one(&mut sx0).await.map(|r| r.get::<i32, _>(0))
        });
        let tp_wait = tokio::spawn(async move { t0.query_one("SELECT x FROM a WHERE id = $1", &[&1i32]).await.map(|r| r.get::<_, i32>(0)) });
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(!sqlx_wait.is_finished() && !tp_wait.is_finished(), "a read ran beside ACCESS EXCLUSIVE");
        holder.batch_execute("COMMIT").await.unwrap();
        assert_eq!(sqlx_wait.await.unwrap().expect("sqlx after the wait"), 10);
        assert_eq!(tp_wait.await.unwrap().expect("tokio-postgres after the wait"), 10);
    })
    .await;
}

/// A lock wait cancelled: tokio-postgres's CancelRequest and sqlx's
/// `statement_timeout` each end it with 57014, and the session goes on.
#[tokio::test]
async fn a_lock_wait_is_cancelled_through_each_driver() {
    bounded("cancellation", async {
        let server = seeded().await;
        let holder = tp(&server.url()).await;
        holder.batch_execute("BEGIN").await.unwrap();
        holder.batch_execute("LOCK TABLE a IN ACCESS EXCLUSIVE MODE").await.unwrap();

        let t = std::sync::Arc::new(tp(&server.url()).await);
        let token = t.cancel_token();
        let waiter = {
            let t = t.clone();
            tokio::spawn(async move { t.query("SELECT x FROM a", &[]).await })
        };
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(!waiter.is_finished(), "the read did not wait");
        token.cancel_query(tokio_postgres::NoTls).await.expect("cancel request");
        let e = waiter.await.unwrap().expect_err("a cancelled wait answered rows");
        assert_eq!(tp_code(&e).as_deref(), Some("57014"), "tokio-postgres: {e}");
        assert_eq!(t.query_one("SELECT x FROM b WHERE id = 1", &[]).await.unwrap().get::<_, i32>(0), 10);

        let mut sx = PgConnection::connect(&url_with(&server, &["statement_timeout=400"])).await.unwrap();
        let started = Instant::now();
        let e = sqlx::query("SELECT x FROM a").fetch_all(&mut sx).await.unwrap_err();
        assert_eq!(sqlx_code(&e).as_deref(), Some("57014"), "sqlx: {e}");
        assert!(started.elapsed() < Duration::from_secs(5), "statement_timeout ended the wait late");
        holder.batch_execute("COMMIT").await.unwrap();
    })
    .await;
}

/// A cycle across the drivers: sqlx's block holds `a` and waits for `b`,
/// tokio-postgres's holds `b` and asks for `a`. Exactly one gets 40P01; its
/// block is over at once (25P02 for its next statement, its ROLLBACK
/// succeeds), so the other finishes without waiting for that ROLLBACK.
#[tokio::test]
async fn a_deadlock_across_the_drivers_fails_exactly_one() {
    bounded("deadlock", async {
        let server = seeded().await;
        let mut sx = PgConnection::connect(&server.url()).await.unwrap();
        let t = tp(&server.url()).await;
        sx.execute("BEGIN").await.unwrap();
        sx.execute("LOCK TABLE a IN ACCESS EXCLUSIVE MODE").await.unwrap();
        t.batch_execute("BEGIN").await.unwrap();
        t.batch_execute("LOCK TABLE b IN ACCESS EXCLUSIVE MODE").await.unwrap();
        let sqlx_side = tokio::spawn(async move {
            let r = sx.execute("LOCK TABLE b IN ACCESS EXCLUSIVE MODE").await.map(|_| ()).map_err(|e| sqlx_code(&e));
            (r, sx)
        });
        tokio::time::sleep(Duration::from_millis(200)).await;
        let tp_result = t.batch_execute("LOCK TABLE a IN ACCESS EXCLUSIVE MODE").await.map_err(|e| tp_code(&e));
        let (sqlx_result, mut sx) = sqlx_side.await.unwrap();
        let victims = [&sqlx_result, &tp_result].iter().filter(|r| r.is_err()).count();
        assert_eq!(victims, 1, "sqlx {sqlx_result:?}, tokio-postgres {tp_result:?}");
        for r in [&sqlx_result, &tp_result] {
            if let Err(code) = r {
                assert_eq!(code.as_deref(), Some("40P01"));
            }
        }
        if sqlx_result.is_err() {
            let e = sx.execute("SELECT 1").await.unwrap_err();
            assert_eq!(sqlx_code(&e).as_deref(), Some("25P02"));
            sx.execute("ROLLBACK").await.expect("the victim's ROLLBACK");
            t.batch_execute("COMMIT").await.expect("the survivor commits");
        } else {
            let e = t.batch_execute("SELECT 1").await.unwrap_err();
            assert_eq!(tp_code(&e).as_deref(), Some("25P02"));
            t.batch_execute("ROLLBACK").await.expect("the victim's ROLLBACK");
            sx.execute("COMMIT").await.expect("the survivor commits");
        }
    })
    .await;
}

/// R3 through each driver's transaction API: an error ends the block at
/// once. The next statement gets 25P02, a CREATE INDEX waiting on the
/// block's ROW EXCLUSIVE runs before the client's ROLLBACK, and the
/// ROLLBACK succeeds.
#[tokio::test]
async fn a_block_error_ends_the_block_through_each_driver() {
    bounded("block error", async {
        let server = seeded().await;
        let ddl = tp(&server.url()).await;

        // sqlx
        let mut sx = PgConnection::connect(&server.url()).await.unwrap();
        let mut tx = sx.begin().await.unwrap();
        sqlx::query("INSERT INTO a (id, x) VALUES ($1, $2)").bind(2i32).bind(20i32).execute(&mut *tx).await.unwrap();
        let index = {
            let url = server.url();
            tokio::spawn(async move { tp(&url).await.batch_execute("CREATE INDEX a_x ON a (x)").await })
        };
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(!index.is_finished(), "CREATE INDEX ran beside the block's write");
        let e = sqlx::query("INSERT INTO a (id, x) VALUES ($1, $2)").bind(1i32).bind(0i32).execute(&mut *tx).await.unwrap_err();
        assert_eq!(sqlx_code(&e).as_deref(), Some("23505"), "{e}");
        index.await.unwrap().expect("CREATE INDEX ran once the failed block let go, before its ROLLBACK");
        let e = sqlx::query("SELECT 1").execute(&mut *tx).await.unwrap_err();
        assert_eq!(sqlx_code(&e).as_deref(), Some("25P02"), "{e}");
        tx.rollback().await.expect("rollback");
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM a").fetch_one(&mut sx).await.unwrap();
        assert_eq!(n, 1, "the failed block's first INSERT survived");

        // tokio-postgres
        let mut t = tp(&server.url()).await;
        let tx = t.transaction().await.unwrap();
        tx.execute("INSERT INTO b (id, x) VALUES ($1, $2)", &[&2i32, &20i32]).await.unwrap();
        let index = {
            let url = server.url();
            tokio::spawn(async move { tp(&url).await.batch_execute("CREATE INDEX b_x ON b (x)").await })
        };
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(!index.is_finished(), "CREATE INDEX ran beside the block's write");
        let e = tx.execute("INSERT INTO b (id, x) VALUES ($1, $2)", &[&1i32, &0i32]).await.unwrap_err();
        assert_eq!(tp_code(&e).as_deref(), Some("23505"), "{e}");
        index.await.unwrap().expect("CREATE INDEX ran once the failed block let go");
        let e = tx.execute("SELECT 1", &[]).await.unwrap_err();
        assert_eq!(tp_code(&e).as_deref(), Some("25P02"), "{e}");
        tx.rollback().await.expect("rollback");
        let n: i64 = ddl.query_one("SELECT count(*) FROM b", &[]).await.unwrap().get(0);
        assert_eq!(n, 1);
    })
    .await;
}

/// DDL against DML across the drivers: sqlx's block INSERTs through a
/// cached statement (ROW EXCLUSIVE); tokio-postgres's ALTER waits for it,
/// runs after its COMMIT, and sqlx's cached INSERT and tokio-postgres's
/// prepared point read both work after the change.
#[tokio::test]
async fn ddl_waits_for_dml_and_both_drivers_go_on() {
    bounded("ddl vs dml", async {
        let server = seeded().await;
        let mut sx = PgConnection::connect(&server.url()).await.unwrap();
        let t = tp(&server.url()).await;
        let read = t.prepare("SELECT x FROM a WHERE id = $1").await.unwrap();
        let insert = "INSERT INTO a (id, x) VALUES ($1, $2)";
        sx.execute("BEGIN").await.unwrap();
        sqlx::query(insert).bind(2i32).bind(20i32).execute(&mut sx).await.unwrap();
        let alter = {
            let url = server.url();
            tokio::spawn(async move { tp(&url).await.batch_execute("ALTER TABLE a ADD COLUMN note TEXT").await })
        };
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert!(!alter.is_finished(), "the ALTER ran beside the block's write");
        sqlx::query(insert).bind(3i32).bind(30i32).execute(&mut sx).await.expect("the block's next write");
        sx.execute("COMMIT").await.unwrap();
        alter.await.unwrap().expect("the ALTER after the block");
        sqlx::query(insert).bind(4i32).bind(40i32).execute(&mut sx).await.expect("the cached INSERT after the ALTER");
        let x: i32 = t.query_one(&read, &[&3i32]).await.expect("the kept read after the ALTER").get(0);
        assert_eq!(x, 30);
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM a WHERE note IS NULL").fetch_one(&mut sx).await.unwrap();
        assert_eq!(n, 4);
    })
    .await;
}
