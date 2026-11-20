//! `TABLESAMPLE BERNOULLI|SYSTEM (p)`, refused before ("TABLESAMPLE is not
//! supported"), and a condition over aggregates of a join (`count(*) > 0`,
//! `sum(t.v) IS NULL`), which failed as "'__agg_0 > 0' is neither a column
//! nor a value". Checked against PostgreSQL 17 (`.claude/reviews/pgdiff`).

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

async fn code(c: &tokio_postgres::Client, sql: &str) -> String {
    let err = c.simple_query(sql).await.expect_err(sql);
    err.as_db_error().unwrap_or_else(|| panic!("{sql}: {err}")).code().code().to_string()
}

async fn connect(server: &TestServer) -> tokio_postgres::Client {
    let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(conn);
    c
}

#[tokio::test(flavor = "multi_thread")]
async fn tablesample_draws_a_sample_of_the_size_asked() {
    let server = TestServer::start().await.expect("server");
    let c = connect(&server).await;
    c.batch_execute("CREATE TABLE ts (id int, v int); INSERT INTO ts SELECT g, g % 3 FROM generate_series(1, 4000) g")
        .await
        .expect("setup");
    assert_eq!(rows(&c, "SELECT count(*) FROM ts TABLESAMPLE BERNOULLI (100)").await, ["4000"]);
    assert_eq!(rows(&c, "SELECT count(*) FROM ts TABLESAMPLE SYSTEM (0)").await, ["0"]);
    // 4000 draws at a tenth land in [300, 500] but for a chance below 1e-6.
    let n: i64 = rows(&c, "SELECT count(*) FROM ts TABLESAMPLE BERNOULLI (10)").await[0].parse().expect("count");
    assert!((300..=500).contains(&n), "{n}");
    assert_eq!(rows(&c, "SELECT x.v FROM ts AS x TABLESAMPLE BERNOULLI (100) WHERE x.id = 7").await, ["1"]);
    assert_eq!(
        rows(&c, "SELECT count(*) FROM ts t TABLESAMPLE SYSTEM (100) JOIN ts u ON t.id = u.id").await,
        ["4000"]
    );
    assert_eq!(code(&c, "SELECT * FROM ts TABLESAMPLE foo (1)").await, "42704");
    assert_eq!(code(&c, "SELECT * FROM ts TABLESAMPLE BERNOULLI (101)").await, "2202H");
    assert_eq!(code(&c, "SELECT * FROM ts TABLESAMPLE BERNOULLI (5) REPEATABLE (1)").await, "0A000");
}

#[tokio::test(flavor = "multi_thread")]
async fn conditions_over_a_join_s_aggregates() {
    let server = TestServer::start().await.expect("server");
    let c = connect(&server).await;
    c.batch_execute("CREATE TABLE tj (id int, v int); INSERT INTO tj SELECT g, g % 3 FROM generate_series(1, 4000) g")
        .await
        .expect("setup");
    let cases: &[(&str, &[&str])] = &[
        ("SELECT max(t.id) <= 4000, count(*) > 0 FROM tj t JOIN tj u ON t.id = u.id", &["t|t"]),
        ("SELECT t.v, count(*) > 1000 FROM tj t JOIN tj u ON t.id = u.id GROUP BY t.v ORDER BY 1", &["0|t", "1|t", "2|t"]),
        ("SELECT sum(t.v) IS NULL FROM tj t JOIN tj u ON t.id = u.id", &["f"]),
        // A computed argument: the condition is parsed again once its type is known.
        ("SELECT sum(t.v + 1) > 7000, sum(t.v + u.v) FROM tj t JOIN tj u ON t.id = u.id", &["t|8000"]),
    ];
    for (sql, want) in cases {
        assert_eq!(rows(&c, sql).await, *want, "{sql}");
    }
}
