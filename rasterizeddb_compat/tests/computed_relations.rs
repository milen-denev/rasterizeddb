//! Relations a `FROM` list computes (`rql::table_functions`): `(VALUES …)
//! v(x)`, `generate_series`, `unnest`, `WITH ORDINALITY`. Each was
//! "relation '…' does not exist", though `generate_series` is how
//! applications fill a calendar and fixtures make rows. Every expected answer
//! is PostgreSQL 17's (`.claude/reviews/srf/p1.sql`).

use rasterizeddb_compat::TestServer;
use sqlx::{Connection, Row};
use tokio_postgres::SimpleQueryMessage;

async fn rows(c: &tokio_postgres::Client, sql: &str) -> Vec<String> {
    c.simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => {
                Some((0..r.len()).map(|i| r.get(i).unwrap_or("NULL").to_string()).collect::<Vec<_>>().join("|"))
            }
            _ => None,
        })
        .collect()
}

async fn code(c: &tokio_postgres::Client, sql: &str) -> String {
    let e = c.simple_query(sql).await.expect_err(sql);
    e.code().map(|s| s.code().to_string()).unwrap_or_else(|| panic!("{sql}: {e}"))
}

#[tokio::test(flavor = "multi_thread")]
async fn computed_relations_answer_as_postgresql_does() {
    let server = TestServer::start().await.expect("server");
    let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(conn);
    c.simple_query("CREATE TABLE t (id INT PRIMARY KEY, n INT)").await.expect("create");
    c.simple_query("INSERT INTO t VALUES (1, 2), (2, 3)").await.expect("rows");
    let cases: &[(&str, &[&str])] = &[
        ("SELECT * FROM (VALUES (1), (2), (3)) AS v(x)", &["1", "2", "3"]),
        ("SELECT x * 2 FROM (VALUES (1), (2)) v(x) ORDER BY 1 DESC", &["4", "2"]),
        ("SELECT * FROM (VALUES (1, 'a'), (2, NULL)) v", &["1|a", "2|NULL"]),
        ("SELECT t.id, v.x FROM t, (VALUES (10), (20)) v(x) ORDER BY 1, 2", &["1|10", "1|20", "2|10", "2|20"]),
        ("SELECT * FROM generate_series(1, 3)", &["1", "2", "3"]),
        ("SELECT g FROM generate_series(1, 10, 4) g", &["1", "5", "9"]),
        ("SELECT generate_series FROM generate_series(5, 1, -2)", &["5", "3", "1"]),
        ("SELECT * FROM generate_series(1, 0)", &[]),
        ("SELECT * FROM generate_series(1, NULL)", &[]),
        ("SELECT * FROM generate_series(1.5, 3, 0.5)", &["1.5", "2.0", "2.5", "3.0"]),
        (
            "SELECT * FROM generate_series(timestamp '2024-01-01', timestamp '2024-01-03', interval '1 day')",
            &["2024-01-01 00:00:00", "2024-01-02 00:00:00", "2024-01-03 00:00:00"],
        ),
        (
            "SELECT d::date FROM generate_series(date '2024-01-30', date '2024-02-02', interval '1 day') d",
            &["2024-01-30", "2024-01-31", "2024-02-01", "2024-02-02"],
        ),
        ("SELECT t.id, g FROM t CROSS JOIN generate_series(1, 2) AS g ORDER BY 1, 2", &["1|1", "1|2", "2|1", "2|2"]),
        ("SELECT count(*), sum(g) FROM generate_series(1, 1000) g", &["1000|500500"]),
        ("SELECT g, g * g AS sq FROM generate_series(1, 5) g WHERE g % 2 = 1 ORDER BY g DESC", &["5|25", "3|9", "1|1"]),
        ("SELECT * FROM unnest(ARRAY[1, 2, 3])", &["1", "2", "3"]),
        ("SELECT * FROM unnest(ARRAY[1, 2], ARRAY['x', 'y']) AS z(a, b)", &["1|x", "2|y"]),
        ("SELECT * FROM unnest('{3,4}'::int[]) WITH ORDINALITY AS z(v, i)", &["3|1", "4|2"]),
        ("SELECT * FROM generate_series(1, 3) WITH ORDINALITY", &["1|1", "2|2", "3|3"]),
        ("SELECT generate_series(1, 3)", &["1", "2", "3"]),
        ("WITH s AS (SELECT g FROM generate_series(1, 3) g) SELECT sum(g) FROM s", &["6"]),
        ("SELECT max(x) FROM (SELECT g * 10 AS x FROM generate_series(1, 4) g) q", &["40"]),
    ];
    for (sql, want) in cases {
        assert_eq!(rows(&c, sql).await, *want, "{sql}");
    }
    c.simple_query("INSERT INTO t SELECT g, g * 10 FROM generate_series(3, 5) g").await.expect("insert select");
    assert_eq!(rows(&c, "SELECT id, n FROM t WHERE id > 2 ORDER BY id").await, ["3|30", "4|40", "5|50"]);

    for (sql, sqlstate) in [
        ("SELECT * FROM generate_series(1, 5, 0)", "22023"),
        ("SELECT t.id, g FROM t, generate_series(1, t.n) g", "0A000"),
        ("SELECT 1 + generate_series(1, 2)", "0A000"),
        ("SELECT * FROM generate_series(1, 2000000)", "0A000"),
    ] {
        assert_eq!(code(&c, sql).await, sqlstate, "{sql}");
    }
}

/// Over the extended protocol a computed relation is described from its
/// arguments, and its values decode by those types.
#[tokio::test(flavor = "multi_thread")]
async fn drivers_read_computed_relations_by_type() {
    let server = TestServer::start().await.expect("server");
    let mut x = sqlx::PgConnection::connect(&server.url()).await.expect("sqlx");
    let rows = sqlx::query("SELECT g, g::text AS s FROM generate_series(1, 3) g WHERE g > $1 ORDER BY g")
        .bind(1i32)
        .fetch_all(&mut x)
        .await
        .expect("extended");
    let got: Vec<(i32, String)> = rows.iter().map(|r| (r.get(0), r.get(1))).collect();
    assert_eq!(got, [(2, "2".to_string()), (3, "3".to_string())]);
    let row = sqlx::query("SELECT x, y FROM (VALUES (1, 'a')) v(x, y)").fetch_one(&mut x).await.expect("values");
    assert_eq!((row.get::<i32, _>(0), row.get::<String, _>(1)), (1, "a".to_string()));
}
