//! Three gaps found by the PostgreSQL 17 differential (`.claude/reviews/pgdiff`):
//!
//! - A literal cast to `regclass` / `regtype` outside a catalogue statement
//!   and `to_regclass` were refused ("cast to 'regclass' is not supported
//!   yet").
//! - `random()` in a `WHERE` was folded once for the statement, so
//!   `WHERE random() < 0.5` kept every row or none.
//! - A condition over aggregates (`count(*) > 0`, `sum(v) IS NULL`) failed:
//!   "'__agg_0 > 0' is neither a column nor a value".

use rasterizeddb_compat::TestServer;
use tokio_postgres::SimpleQueryMessage;

async fn rows(c: &tokio_postgres::Client, sql: &str) -> Vec<String> {
    c.simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some(
                (0..r.len())
                    .map(|i| r.get(i).unwrap_or("NULL").to_string())
                    .collect::<Vec<_>>()
                    .join("|"),
            ),
            _ => None,
        })
        .collect()
}

async fn names(c: &tokio_postgres::Client, sql: &str) -> Vec<String> {
    let st = c.prepare(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    st.columns().iter().map(|c| c.name().to_string()).collect()
}

async fn connect(server: &TestServer) -> tokio_postgres::Client {
    let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(conn);
    c
}

#[tokio::test(flavor = "multi_thread")]
async fn regclass_and_regtype_literals() {
    let server = TestServer::start().await.expect("server");
    let c = connect(&server).await;
    c.batch_execute("CREATE TABLE rc (id int, name text); CREATE VIEW rcv AS SELECT id FROM rc; CREATE SEQUENCE rcs")
        .await
        .expect("setup");
    let cases: &[(&str, &[&str])] = &[
        ("SELECT 'rc'::regclass", &["rc"]),
        ("SELECT 'public.RC'::regclass::text", &["rc"]),
        ("SELECT CAST('rc' AS regclass)", &["rc"]),
        ("SELECT 'rc'::regclass::oid = (SELECT oid FROM pg_class WHERE relname = 'rc')", &["t"]),
        ("SELECT 'rc'::regclass = 'rc'::regclass", &["t"]),
        ("SELECT to_regclass('rc'), to_regclass('nosuch'), to_regclass('rcv')", &["rc|NULL|rcv"]),
        ("SELECT relname FROM pg_class WHERE oid = 'rc'::regclass", &["rc"]),
        ("SELECT attname FROM pg_attribute WHERE attrelid = 'rc'::regclass AND attnum > 0 ORDER BY attnum", &["id", "name"]),
        ("SELECT 'int4'::regtype, 'integer'::regtype::oid", &["integer|23"]),
        // Left to the calls that read the literal themselves.
        ("SELECT nextval('rcs'::regclass)", &["1"]),
    ];
    for (sql, want) in cases {
        assert_eq!(rows(&c, sql).await, *want, "{sql}");
    }
    assert_eq!(
        names(&c, "SELECT 'rc'::regclass, 'rc'::regclass::text, 'int4'::regtype").await,
        ["regclass", "text", "regtype"]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn random_in_a_where_is_drawn_for_every_row() {
    let server = TestServer::start().await.expect("server");
    let c = connect(&server).await;
    c.batch_execute("CREATE TABLE rr (id int); INSERT INTO rr SELECT g FROM generate_series(1, 2000) g")
        .await
        .expect("setup");
    // Folded once, each of these was 0 or 2000. Drawn per row, 2000 draws at
    // one half land in [800, 1200] but for a chance below 1e-18.
    for _ in 0..3 {
        let n: i64 = rows(&c, "SELECT count(*) FROM rr WHERE random() < 0.5").await[0].parse().expect("count");
        assert!((800..=1200).contains(&n), "{n}");
    }
    assert_eq!(rows(&c, "SELECT count(*) FROM rr WHERE random() < 0.0").await, ["0"]);
    assert_eq!(rows(&c, "SELECT count(*) FROM rr WHERE id <= 10 AND random() < 1.0").await, ["10"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn conditions_over_aggregates() {
    let server = TestServer::start().await.expect("server");
    let c = connect(&server).await;
    c.batch_execute("CREATE TABLE ra (id int, v int); INSERT INTO ra SELECT g, g % 7 FROM generate_series(1, 2000) g")
        .await
        .expect("setup");
    let cases: &[(&str, &[&str])] = &[
        ("SELECT count(*) > 0, count(*) = 2000, max(v) >= 6, sum(v) IS NULL FROM ra", &["t|t|t|f"]),
        ("SELECT sum(v) IS NULL, max(v) IS NOT NULL FROM ra WHERE id < 0", &["t|f"]),
        ("SELECT count(*) > 0 AS any FROM ra WHERE id < 0", &["f"]),
        ("SELECT v, count(*) > 285 FROM ra GROUP BY v ORDER BY v LIMIT 2", &["0|f", "1|t"]),
        ("SELECT min(v) = max(v) FROM ra WHERE id = 7", &["t"]),
        ("SELECT count(*) FILTER (WHERE v = 1) > 100 FROM ra", &["t"]),
    ];
    for (sql, want) in cases {
        assert_eq!(rows(&c, sql).await, *want, "{sql}");
    }
}
