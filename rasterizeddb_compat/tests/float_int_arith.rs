//! Arithmetic and comparisons across numeric widths.
//!
//! `0.5 * 2` multiplied an eight-byte float by a four-byte integer and failed
//! with "operand size does not match the column type being evaluated"; so did
//! an integer column against a bigint column, and an arithmetic result against
//! a literal of another type. The operands now meet at a common type.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};

#[tokio::test(flavor = "multi_thread")]
async fn mixed_numeric_widths_compute_and_compare() {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    pool.execute(
        "CREATE TABLE m (id INT NOT NULL, i INT NOT NULL, b BIGINT NOT NULL, f DOUBLE PRECISION NOT NULL, r REAL NOT NULL)",
    )
    .await
    .unwrap();
    pool.execute("INSERT INTO m (id, i, b, f, r) VALUES (1, 2, 10, 1.5, 0.5), (2, 3, 20, 2.5, 4.0)")
        .await
        .unwrap();
    let mut wrong = Vec::new();
    for (predicate, expected) in [
        ("0.5 * 2 = 1", vec![1, 2]),
        ("0.5 * 2 = 2", vec![]),
        ("f = 0.5 * 3", vec![1]),
        ("f * 2 = 3", vec![1]),
        ("i * 2.5 > 6", vec![2]),
        ("i + 0.5 = 2.5", vec![1]),
        ("i = b / 10 + 1", vec![1, 2]),
        ("i < b", vec![1, 2]),
        ("b > i * 6", vec![2]),
        ("r * 2 = 1", vec![1]),
        ("f + r = 2", vec![1]),
        ("i * 2 = 4", vec![1]),
        ("i > 2.5", vec![2]),
        ("2.5 < i", vec![2]),
        ("i = 2.0", vec![1]),
        ("i <> 2.5", vec![1, 2]),
    ] {
        let got = sqlx::query(AssertSqlSafe(format!("SELECT id FROM m WHERE {predicate} ORDER BY id")))
            .fetch_all(&pool)
            .await
            .map(|rows| rows.iter().map(|r| r.get::<i32, _>(0)).collect::<Vec<_>>());
        match got {
            Ok(ids) if ids == expected => {}
            Ok(ids) => wrong.push(format!("{predicate}: got {ids:?}, want {expected:?}")),
            Err(e) => wrong.push(format!("{predicate}: error {e}")),
        }
    }
    assert!(wrong.is_empty(), "{} wrong answers:\n{}", wrong.len(), wrong.join("\n"));
}
