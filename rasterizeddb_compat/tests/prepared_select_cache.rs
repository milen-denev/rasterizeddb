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
    let (_server, mut conn) = setup().await;

    // Learned in two executions with different values, used from the third.
    let point = "SELECT k, s FROM items WHERE k = $1";
    let before = runs();
    for k in 1..=20i64 {
        same(&mut conn, point, &[Value::I(k)]).await;
    }
    assert!(runs() - before >= 18, "a point query never ran from its parse ({} runs)", runs() - before);

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
