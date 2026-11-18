//! `MERGE`, as PostgreSQL 17 answers it (its `WHEN NOT MATCHED BY SOURCE`
//! and `merge_action()` included). It was "syntax error or unsupported
//! query"; the expected answers are PostgreSQL 17's (`.claude/reviews/pgdiff`).

use rasterizeddb_compat::TestServer;
use sqlx::Row;
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
async fn merge_answers_as_postgresql_does() {
    let server = TestServer::start().await.expect("server");
    let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(conn);
    c.batch_execute(
        "CREATE TABLE stock (id INT PRIMARY KEY, qty INT, note TEXT); \
         INSERT INTO stock VALUES (1, 10, NULL), (2, 20, NULL), (3, 30, NULL); \
         CREATE TABLE incoming (id INT, qty INT); INSERT INTO incoming VALUES (1, 5), (2, 0), (4, 7)",
    )
    .await
    .expect("setup");
    let merged = c
        .simple_query(
            "MERGE INTO stock t USING incoming s ON t.id = s.id \
             WHEN MATCHED AND s.qty = 0 THEN DELETE \
             WHEN MATCHED THEN UPDATE SET qty = t.qty + s.qty, note = 'added' \
             WHEN NOT MATCHED THEN INSERT (id, qty, note) VALUES (s.id, s.qty, 'new')",
        )
        .await
        .expect("merge");
    let tag = merged.iter().find_map(|m| match m {
        SimpleQueryMessage::CommandComplete(n) => Some(*n),
        _ => None,
    });
    assert_eq!(tag, Some(3), "MERGE 3");
    assert_eq!(rows(&c, "SELECT * FROM stock ORDER BY id").await, ["1|15|added", "3|30|NULL", "4|7|new"]);

    let mut returned = rows(
        &c,
        "MERGE INTO stock AS t USING (VALUES (1, 100), (9, 9)) AS s(id, qty) ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET qty = s.qty \
         WHEN NOT MATCHED BY TARGET THEN INSERT VALUES (s.id, s.qty, 'v') \
         WHEN NOT MATCHED BY SOURCE THEN UPDATE SET note = 'stale' \
         RETURNING merge_action(), t.id, t.qty, t.note",
    )
    .await;
    returned.sort();
    assert_eq!(returned, ["INSERT|9|9|v", "UPDATE|1|100|added", "UPDATE|3|30|stale", "UPDATE|4|7|stale"]);
    assert_eq!(rows(&c, "SELECT * FROM stock ORDER BY id").await, ["1|100|added", "3|30|stale", "4|7|stale", "9|9|v"]);

    // Unqualified, by table name, and a subquery source.
    c.batch_execute("MERGE INTO stock USING incoming ON stock.id = incoming.id WHEN MATCHED THEN UPDATE SET qty = incoming.qty")
        .await
        .expect("by name");
    assert_eq!(rows(&c, "SELECT qty FROM stock WHERE id = 1").await, ["5"]);
    c.batch_execute(
        "MERGE INTO stock t USING (SELECT id, qty FROM incoming WHERE id = 4) s ON t.id = s.id \
         WHEN MATCHED THEN DO NOTHING WHEN NOT MATCHED THEN DO NOTHING",
    )
    .await
    .expect("do nothing");

    for (sql, expected) in [
        ("MERGE INTO stock t USING (VALUES (1), (1)) AS s(id) ON t.id = s.id WHEN MATCHED THEN UPDATE SET qty = 0", "21000"),
        ("MERGE INTO stock t USING incoming s ON t.id = s.id WHEN NOT MATCHED THEN DELETE", "42601"),
        ("MERGE INTO nosuch t USING incoming s ON t.id = s.id WHEN MATCHED THEN DELETE", "42P01"),
        ("MERGE INTO stock t USING incoming s ON id = s.id WHEN MATCHED THEN DELETE", "42702"),
        ("MERGE INTO stock t USING incoming s ON t.id = s.id WHEN MATCHED THEN UPDATE SET nosuch = 1", "42703"),
    ] {
        assert_eq!(code(&c, sql).await, expected, "{sql}");
    }
    // One action fails: the whole MERGE is taken back.
    assert_eq!(
        code(
            &c,
            "MERGE INTO stock t USING (VALUES (1, 1), (50, NULL)) AS s(id, qty) ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET qty = 999 WHEN NOT MATCHED THEN INSERT (id, qty) VALUES (CAST(NULL AS integer), 1)",
        )
        .await,
        "23502"
    );
    assert_eq!(rows(&c, "SELECT qty FROM stock WHERE id = 1").await, ["5"]);

    // The extended protocol, with RETURNING in the client's formats.
    let pool = server.connect_sqlx().await.expect("sqlx");
    let row = sqlx::query(
        "MERGE INTO stock t USING (VALUES ($1::int, $2::int)) AS s(id, qty) ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET qty = s.qty RETURNING t.id, t.qty",
    )
    .bind(3)
    .bind(33)
    .fetch_one(&pool)
    .await
    .expect("bound merge");
    assert_eq!((row.get::<i32, _>(0), row.get::<i32, _>(1)), (3, 33));
}
