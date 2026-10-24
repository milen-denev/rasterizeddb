//! A prepared `SELECT` runs from a cached parse after two executions have
//! shown the text path makes it (see `core::rql::prepared`). The cache is only
//! ever allowed to be faster, never different: every query here runs both
//! prepared and as plain text with the value written in, and the rows must
//! match exactly.
//!
//! What must keep the text path is pinned too, through the engine's count of
//! cached runs: a `LIKE` (a value rewritten by what it holds), a `NULL`, a
//! string holding a quote, and a schema change, after which the cache has to
//! be learned again rather than reused.

use rasterizeddb_compat::TestServer;
use rasterizeddb_core::pgwire::PREPARED_RUNS;
use sqlx::{AssertSqlSafe, Connection, Executor, PgConnection, Row};
use std::sync::atomic::Ordering;

/// The count of cached runs is global to the process, so tests that read it
/// do not run beside tests that move it.
static COUNTED: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn runs() -> u64 {
    PREPARED_RUNS.load(Ordering::Relaxed)
}

/// A row as text, the same whichever path produced it.
fn render(row: &sqlx::postgres::PgRow) -> String {
    (0..row.len())
        .map(|i| {
            if let Ok(v) = row.try_get::<Option<i64>, _>(i) {
                return format!("{v:?}");
            }
            if let Ok(v) = row.try_get::<Option<i32>, _>(i) {
                return format!("{v:?}");
            }
            if let Ok(v) = row.try_get::<Option<f64>, _>(i) {
                return format!("{v:?}");
            }
            if let Ok(v) = row.try_get::<Option<String>, _>(i) {
                return format!("{v:?}");
            }
            "?".to_string()
        })
        .collect::<Vec<_>>()
        .join("|")
}

/// The same statement with the values written in, run as a simple query.
fn literal(sql: &str, values: &[Value]) -> String {
    let mut out = sql.to_string();
    for (i, v) in values.iter().enumerate().rev() {
        let text = match v {
            Value::I(n) => n.to_string(),
            Value::F(f) => format!("{f:?}"),
            Value::S(s) => format!("'{}'", s.replace('\'', "''")),
            Value::Null => "NULL".to_string(),
        };
        out = out.replace(&format!("${}", i + 1), &text);
    }
    out
}

#[derive(Clone, Debug)]
enum Value {
    I(i64),
    F(f64),
    S(String),
    Null,
}

async fn prepared_rows(conn: &mut PgConnection, sql: &str, values: &[Value]) -> Vec<String> {
    let mut q = sqlx::query(AssertSqlSafe(sql.to_string()));
    for v in values {
        q = match v {
            Value::I(n) => q.bind(*n),
            Value::F(f) => q.bind(*f),
            Value::S(s) => q.bind(s.clone()),
            Value::Null => q.bind(None::<i64>),
        };
    }
    let rows = q.fetch_all(&mut *conn).await.unwrap_or_else(|e| panic!("{sql} {values:?}: {e}"));
    let mut out: Vec<String> = rows.iter().map(render).collect();
    out.sort();
    out
}

async fn text_rows(conn: &mut PgConnection, sql: &str, values: &[Value]) -> Vec<String> {
    let text = literal(sql, values);
    // Through the extended protocol with no parameters: rows decode exactly
    // as the prepared ones do, and nothing is cached for a statement with no
    // parameters.
    let rows = sqlx::query(AssertSqlSafe(text.clone()))
        .persistent(false)
        .fetch_all(&mut *conn)
        .await
        .unwrap_or_else(|e| panic!("{text}: {e}"));
    let mut out: Vec<String> = rows.iter().map(render).collect();
    out.sort();
    out
}

async fn same(conn: &mut PgConnection, sql: &str, values: &[Value]) {
    let p = prepared_rows(conn, sql, values).await;
    let t = text_rows(conn, sql, values).await;
    assert_eq!(p, t, "prepared and text answers differ for {sql} with {values:?}");
}

async fn setup() -> (TestServer, PgConnection) {
    let server = TestServer::start().await.expect("server should start");
    let mut conn = PgConnection::connect(&server.url()).await.expect("connect");
    conn.execute(
        "CREATE TABLE items (k BIGINT PRIMARY KEY, s TEXT NOT NULL, f DOUBLE PRECISION NOT NULL, \
         price NUMERIC(10, 2) NOT NULL, n INT)",
    )
    .await
    .unwrap();
    let mut values = Vec::new();
    for k in 0..200i64 {
        let n = if k % 7 == 0 { "NULL".to_string() } else { (k % 13).to_string() };
        values.push(format!(
            "({k}, 'name{}', {}.5, {}.{:02}, {n})",
            k % 50,
            k % 9,
            k % 30,
            k % 100
        ));
    }
    conn.execute(AssertSqlSafe(format!(
        "INSERT INTO items (k, s, f, price, n) VALUES {}",
        values.join(", ")
    )))
    .await
    .unwrap();
    (server, conn)
}

#[tokio::test]
async fn cached_answers_match_the_text_path() {
    let _counted = COUNTED.lock().await;
    let (_server, mut conn) = setup().await;

    let point = "SELECT k, s, f, n FROM items WHERE k = $1";
    for k in [5i64, 17, 0, 199, 200, -3, 77, 77, 140] {
        same(&mut conn, point, &[Value::I(k)]).await;
    }

    let by_text = "SELECT k, s FROM items WHERE s = $1";
    for s in ["name3", "name49", "nobody", "", "name3", "名前"] {
        same(&mut conn, by_text, &[Value::S(s.to_string())]).await;
    }

    let range = "SELECT k, n FROM items WHERE k >= $1 AND k < $2 ORDER BY k";
    for (a, b) in [(10i64, 20i64), (0, 1), (150, 400), (30, 30), (-5, 5), (90, 120)] {
        same(&mut conn, range, &[Value::I(a), Value::I(b)]).await;
    }

    let decimal = "SELECT k FROM items WHERE price > $1 AND k < $2";
    for (p, k) in [(10.5f64, 50i64), (29.99, 200), (0.01, 10), (15.0, 100)] {
        same(&mut conn, decimal, &[Value::F(p), Value::I(k)]).await;
    }

    let float = "SELECT k FROM items WHERE f <= $1";
    for f in [2.5f64, 0.5, 8.5, 100.0] {
        same(&mut conn, float, &[Value::F(f)]).await;
    }

    let nullable = "SELECT k FROM items WHERE n = $1";
    for n in [3i64, 0, 12, 5] {
        same(&mut conn, nullable, &[Value::I(n)]).await;
    }
}

/// Counter-sensitive, so all in one test: the count is global to the process.
#[tokio::test]
async fn the_cache_is_used_and_refused_where_it_should_be() {
    let _counted = COUNTED.lock().await;
    let (_server, mut conn) = setup().await;

    // Learned in two executions with different values, used from the third.
    let point = "SELECT k, s FROM items WHERE k = $1";
    let before = runs();
    for k in 1..=20i64 {
        same(&mut conn, point, &[Value::I(k)]).await;
    }
    assert!(runs() - before >= 18, "a point query never ran from its parse ({} runs)", runs() - before);

    // A statement with no parameters is the same text every time: cached,
    // LIKE and IN included, and still answering exactly as the text path.
    let fixed = [
        "SELECT k, s FROM items WHERE k = 7",
        "SELECT k FROM items WHERE s LIKE 'name1%' AND k < 150",
        "SELECT k FROM items WHERE k IN (3, 30, 199)",
    ];
    let before = runs();
    for sql in fixed {
        for _ in 0..4 {
            same(&mut conn, sql, &[]).await;
        }
    }
    assert!(runs() - before >= 6, "parameter-less statements never ran from a cached parse");

    // `doc @> $1` on a document column.
    conn.execute("CREATE TABLE docs (id BIGINT PRIMARY KEY, doc JSONB NOT NULL)").await.unwrap();
    conn.execute(
        "INSERT INTO docs (id, doc) VALUES (1, '{\"a\":1,\"t\":[\"x\"]}'), (2, '{\"a\":2}'), (3, '{\"a\":1.0}')",
    )
    .await
    .unwrap();
    let before = runs();
    for lit in [r#"{"a":1}"#, r#"{"a":2}"#, r#"{"t":["x"]}"#, r#"{"a":1}"#, r#"{}"#, r#"{"a":3}"#] {
        let p: Vec<String> = sqlx::query("SELECT id FROM docs WHERE doc @> $1")
            .bind(lit)
            .fetch_all(&mut conn)
            .await
            .unwrap()
            .iter()
            .map(render)
            .collect();
        let t = text_rows(&mut conn, "SELECT id FROM docs WHERE doc @> $1", &[Value::S(lit.to_string())]).await;
        let mut p = p;
        p.sort();
        assert_eq!(p, t, "doc @> {lit}");
    }
    assert!(runs() - before >= 2, "`doc @> $1` never ran from a cached parse");

    // A value read from the clock is never cached. The text path evaluates
    // it into the statement before parsing, so a cached parse would keep the
    // first execution's date for every later one, the next day included.
    conn.execute("CREATE TABLE events (k BIGINT NOT NULL, at TIMESTAMP NOT NULL)").await.unwrap();
    conn.execute("INSERT INTO events (k, at) VALUES (777, '2001-01-01 00:00:00'), (888, '2999-01-01 00:00:00')")
        .await
        .unwrap();
    for clocked in [
        "SELECT k FROM events WHERE k = $1 AND at < CURRENT_DATE",
        "SELECT k FROM events WHERE k = $1 AND at < now()",
    ] {
        let before = runs();
        // Digits no date contains, so each value's place in the folded clause
        // is unambiguous and a template could be learned from it.
        for k in [777i64, 888].repeat(3) {
            same(&mut conn, clocked, &[Value::I(k)]).await;
        }
        assert_eq!(runs(), before, "{clocked} ran from a cached parse");
    }

    // A NULL value is answered by the text path, and correctly.
    same(&mut conn, "SELECT k FROM items WHERE n = $1", &[Value::I(4)]).await;
    same(&mut conn, "SELECT k FROM items WHERE n = $1", &[Value::I(5)]).await;
    same(&mut conn, "SELECT k FROM items WHERE n = $1", &[Value::Null]).await;

    // A string holding a quote is not a class the template is reused for.
    same(&mut conn, "SELECT k FROM items WHERE s = $1", &[Value::S("name1".into())]).await;
    same(&mut conn, "SELECT k FROM items WHERE s = $1", &[Value::S("name2".into())]).await;
    same(&mut conn, "SELECT k FROM items WHERE s = $1", &[Value::S("it's".into())]).await;
    same(&mut conn, "SELECT k FROM items WHERE s = $1", &[Value::S("name4".into())]).await;

    // LIKE rewrites a value by what it holds: `'name1'` is an equality,
    // `'name1%'` a prefix test. Two wildcard-free values agreeing must not
    // make a third with a wildcard wrong.
    let like = "SELECT k FROM items WHERE s LIKE $1";
    for s in ["name1", "name2", "name1%", "name4", "%9", "name"] {
        same(&mut conn, like, &[Value::S(s.to_string())]).await;
    }

    // A schema change: the cached parse is learned again, and answers match
    // the new shape.
    for k in [1i64, 2, 3] {
        same(&mut conn, point, &[Value::I(k)]).await;
    }
    conn.execute("ALTER TABLE items ADD COLUMN extra TEXT").await.unwrap();
    conn.execute("UPDATE items SET extra = 'x' WHERE k < 10").await.unwrap();
    let wide = "SELECT k, extra FROM items WHERE k = $1";
    for k in [1i64, 2, 3, 4, 15, 9] {
        same(&mut conn, wide, &[Value::I(k)]).await;
    }
    // A fresh connection after the change, so no client cache is involved.
    let mut fresh = PgConnection::connect(&_server.url()).await.unwrap();
    for k in [1i64, 2, 3, 4, 15] {
        same(&mut fresh, point, &[Value::I(k)]).await;
    }
    // A change that moves columns: the old parse would read the wrong bytes.
    conn.execute("ALTER TABLE items DROP COLUMN f").await.unwrap();
    for k in [1i64, 2, 3, 4, 15, 150] {
        same(&mut conn, point, &[Value::I(k)]).await;
        same(&mut fresh, point, &[Value::I(k)]).await;
        same(&mut fresh, "SELECT k, s, n FROM items WHERE k = $1", &[Value::I(k)]).await;
    }
}

#[tokio::test]
async fn a_cached_parse_reads_through_the_transaction() {
    let _counted = COUNTED.lock().await;
    let (_server, mut conn) = setup().await;
    let point = "SELECT k, s FROM items WHERE k = $1";
    for k in [1i64, 2, 3, 4] {
        same(&mut conn, point, &[Value::I(k)]).await;
    }
    conn.execute("BEGIN").await.unwrap();
    conn.execute("INSERT INTO items (k, s, f, price) VALUES (1000, 'mine', 1.0, 1.00)")
        .await
        .unwrap();
    conn.execute("UPDATE items SET s = 'changed' WHERE k = 5").await.unwrap();
    // Its own uncommitted writes are visible to it, through the cache.
    assert_eq!(prepared_rows(&mut conn, point, &[Value::I(1000)]).await, vec!["Some(1000)|Some(\"mine\")".to_string()]);
    assert_eq!(prepared_rows(&mut conn, point, &[Value::I(5)]).await, vec!["Some(5)|Some(\"changed\")".to_string()]);
    // ... and not to anyone else.
    let mut other = PgConnection::connect(&_server.url()).await.unwrap();
    for k in [1i64, 2, 3] {
        prepared_rows(&mut other, point, &[Value::I(k)]).await;
    }
    assert!(prepared_rows(&mut other, point, &[Value::I(1000)]).await.is_empty());
    assert_eq!(prepared_rows(&mut other, point, &[Value::I(5)]).await, vec!["Some(5)|Some(\"name5\")".to_string()]);
    conn.execute("ROLLBACK").await.unwrap();
    assert!(prepared_rows(&mut conn, point, &[Value::I(1000)]).await.is_empty());
    assert_eq!(prepared_rows(&mut conn, point, &[Value::I(5)]).await, vec!["Some(5)|Some(\"name5\")".to_string()]);

    // An aborted block refuses the cached statement as it refuses any other.
    conn.execute("BEGIN").await.unwrap();
    assert!(conn.execute("SELECT * FROM missing_table").await.is_err());
    let refused = sqlx::query(point).bind(1i64).fetch_all(&mut conn).await;
    let code = match refused {
        Err(sqlx::Error::Database(d)) => d.code().map(|c| c.to_string()),
        other => panic!("expected the aborted block to refuse it, got {other:?}"),
    };
    assert_eq!(code.as_deref(), Some("25P02"));
    conn.execute("ROLLBACK").await.unwrap();
}

/// A partitioned table learns its parse like a plain one (S15) and every
/// cached execution routes its own value (I11): point reads and counts for
/// tenants in different partitions, alternating, and an unrouted fan-out,
/// each matching the text path and the plain twin, with the cache used.
/// File-backed, as a partitioned table needs a directory; on tmpfs, small.
#[tokio::test(flavor = "multi_thread")]
async fn a_partitioned_table_learns_its_parse_and_routes_each_execution() {
    let _counted = COUNTED.lock().await;
    struct Root(std::path::PathBuf);
    impl Drop for Root {
        fn drop(&mut self) {
            rasterizeddb_core::core::wal::remove_for(&self.0.to_string_lossy());
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let root = Root(std::env::temp_dir().join(format!("prepared_select_cache_part_{}", std::process::id())));
    let _ = std::fs::remove_dir_all(&root.0);
    let server = TestServer::start_file_backed(&root.0.to_string_lossy()).await.expect("server should start");
    let mut conn = PgConnection::connect(&server.url()).await.expect("connect");
    let columns = "(tenant_id BIGINT NOT NULL, id BIGINT NOT NULL, s TEXT NOT NULL, n INT, PRIMARY KEY (tenant_id, id))";
    conn.execute(AssertSqlSafe(format!("CREATE TABLE tp {columns} PARTITION BY HASH (tenant_id) PARTITIONS 16")))
        .await
        .unwrap();
    conn.execute(AssertSqlSafe(format!("CREATE TABLE tq {columns}"))).await.unwrap();
    let mut values = Vec::new();
    for t in 1..=40i64 {
        for id in 1..=3i64 {
            let n = if (t + id) % 5 == 0 { "NULL".to_string() } else { ((t * 3 + id) % 17).to_string() };
            values.push(format!("({t}, {id}, 'tenant{t}/{id}', {n})"));
        }
    }
    for table in ["tp", "tq"] {
        conn.execute(AssertSqlSafe(format!("INSERT INTO {table} (tenant_id, id, s, n) VALUES {}", values.join(", "))))
            .await
            .unwrap();
    }

    // The first two of each statement's values teach it, so they differ in
    // every value and no literal occurs inside another (`WhereTemplate`).
    let tenants: Vec<i64> = [34i64, 5, 21, 17, 40, 3, 26, 9, 33, 12, 999].to_vec();
    let mut cached = 0;
    for table in ["tp", "tq"] {
        let point = format!("SELECT tenant_id, id, s, n FROM {table} WHERE tenant_id = $1");
        let pair = format!("SELECT s FROM {table} WHERE tenant_id = $1 AND id = $2");
        let count = format!("SELECT COUNT(*) FROM {table} WHERE tenant_id = $1");
        let fan_out = format!("SELECT COUNT(*), SUM(n) FROM {table} WHERE n > $1");
        let before = runs();
        for t in &tenants {
            same(&mut conn, &point, &[Value::I(*t)]).await;
            same(&mut conn, &pair, &[Value::I(*t), Value::I(t % 3 + 1)]).await;
            same(&mut conn, &count, &[Value::I(*t)]).await;
        }
        for v in [4i64, 11, 0, 16, 7, 30] {
            same(&mut conn, &fan_out, &[Value::I(v)]).await;
        }
        let moved = runs() - before;
        if table == "tp" {
            cached = moved;
        }
        // Each statement learns from its first two executions.
        assert!(moved >= 30, "{table}: only {moved} executions ran from a cached parse");
    }
    // The partitioned answers are the twin's, row for row.
    for t in &tenants {
        let p = prepared_rows(&mut conn, "SELECT id, s, n FROM tp WHERE tenant_id = $1", &[Value::I(*t)]).await;
        let q = prepared_rows(&mut conn, "SELECT id, s, n FROM tq WHERE tenant_id = $1", &[Value::I(*t)]).await;
        assert_eq!(p, q, "tenant {t}: partitioned against plain");
    }
    assert!(cached >= 30, "the partitioned table never ran from a cached parse");
    drop(conn);
    drop(server);
}
