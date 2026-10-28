//! Arithmetic over a NUMERIC column with more places than the engine's nine.
//!
//! Expressions compute at nine places, and a NUMERIC(20,12) value lost its
//! last three digits on the way in and a literal past nine places was
//! rounded to nine: `UPDATE … SET a = a + 0.000000000001` stored
//! 0.123456789000 where PostgreSQL stores 0.123456789124, and `SET d = d *
//! 0.00000000011` stored 0. Every one reported success.
//!
//! Now a value stored in a NUMERIC column is computed exactly and rounded
//! once to the column's scale, and what still cannot be computed exactly is
//! refused (0A000), never answered without its digits.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};

struct Root(String);

impl Root {
    fn new(tag: &str) -> Self {
        let r = std::env::temp_dir().join(format!("numeric_wide_arith_{}_{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&r);
        std::fs::create_dir_all(&r).expect("root");
        Root(r.to_string_lossy().into_owned())
    }
}

impl Drop for Root {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn code(e: &sqlx::Error) -> String {
    e.as_database_error().and_then(|d| d.code()).map(|c| c.to_string()).unwrap_or_else(|| e.to_string())
}

async fn run(pool: &sqlx::PgPool, sql: &str) {
    pool.execute(sqlx::raw_sql(AssertSqlSafe(sql.to_string()))).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
}

async fn refused(pool: &sqlx::PgPool, sql: &str) -> String {
    let e = pool.execute(sqlx::raw_sql(AssertSqlSafe(sql.to_string()))).await.expect_err(sql);
    code(&e)
}

async fn rows(pool: &sqlx::PgPool) -> Vec<String> {
    sqlx::query("SELECT id, a::text AS a, n::text AS n FROM w ORDER BY id")
        .fetch_all(pool)
        .await
        .expect("read back")
        .iter()
        .map(|r| {
            let show = |v: Option<String>| v.unwrap_or_else(|| "NULL".into());
            format!("{} {} {}", r.get::<i32, _>("id"), show(r.get("a")), show(r.get("n")))
        })
        .collect()
}

async fn wide_scale(partitioning: &str) {
    let dir = Root::new(if partitioning.is_empty() { "plain" } else { "hash" });
    let want = [
        "1 0.123456789124 1.8333",
        "2 0.011000000000 2.0000",
        "3 -0.500000000001 3.0000",
        "4 0.000000000002 4.0000",
        "5 0.100000000000 5.0000",
        "6 -0.666666666667 NULL",
        "7 NULL 7.0000",
        "8 0.333333333333 0.0000",
    ];
    {
        let server = TestServer::start_file_backed(&dir.0).await.expect("server");
        let pool = server.connect_sqlx().await.expect("connect");
        run(&pool, &format!("CREATE TABLE w (id INT PRIMARY KEY, a NUMERIC(20,12), n NUMERIC(10,4)){partitioning}")).await;
        run(
            &pool,
            "INSERT INTO w VALUES (1, 0.123456789123, 1.5), (2, 99999999.999999999998, 2), (3, -0.000000000001, 3), \
             (4, 0.000000000005, 4), (5, 1, 5), (6, NULL, NULL), (7, NULL, 7)",
        )
        .await;
        // An INSERT's constant expression, at the column's scale: it was
        // 0.333333333000. Integer division stays integer division.
        run(&pool, "INSERT INTO w VALUES (8, 1 / 3.0, 1 / 3)").await;
        for sql in [
            // A literal past nine places, the column's own last digits kept.
            "UPDATE w SET a = a + 0.000000000001 WHERE id = 1",
            "UPDATE w SET a = a * 0.00000000011 WHERE id = 2",
            "UPDATE w SET a = a - 0.5 WHERE id = 3",
            // A quotient, rounded once at the column's twelve places.
            "UPDATE w SET a = a / 3 WHERE id = 4",
            // Integer division is still integer division: 1 / 3 is 0.
            "UPDATE w SET a = 1 / 3 + 0.1 * a WHERE id = 5",
            "UPDATE w SET a = COALESCE(a, 0) - 2 / 3.0 WHERE id = 6",
            // NULL stays NULL.
            "UPDATE w SET a = a + 0.000000000001 WHERE id = 7",
            // A literal past a float's digits against NUMERIC(10,4), which
            // PostgreSQL stores rounded to four places.
            "UPDATE w SET n = n + 0.3333333333333333333333333333 WHERE id = 1",
        ] {
            run(&pool, sql).await;
        }
        assert_eq!(rows(&pool).await, want, "{partitioning}");

        // RETURNING answers what was stored.
        let back: Vec<String> = sqlx::query("UPDATE w SET a = a * 1.000000000001 WHERE id = 1 RETURNING a::text")
            .fetch_all(&pool)
            .await
            .expect("returning")
            .iter()
            .map(|r| r.get::<String, _>(0))
            .collect();
        assert_eq!(back, ["0.123456789124"], "{partitioning}");

        // What cannot be carried exactly is refused, not cut: the column's
        // digits past nine in a SELECT or a WHERE, a literal past nine in
        // WHERE arithmetic, and an assignment the float a long literal is
        // carried as cannot settle (PostgreSQL: 0.000000000001).
        for sql in [
            "SELECT a + 1 FROM w WHERE id = 1",
            "SELECT id FROM w WHERE a + 1 > 1",
            "SELECT id FROM w WHERE a + 0.000000000001 > 1",
            "UPDATE w SET a = 0.0000000000004999999999999999999 + a * 0 WHERE id = 5",
        ] {
            assert_eq!(refused(&pool, sql).await, "0A000", "{partitioning}: {sql}");
        }
        assert_eq!(rows(&pool).await, want, "{partitioning}: a refused statement changed a row");

        // A value the engine scale holds still computes in a SELECT, and a
        // long literal against NUMERIC is the float nearest the exact answer.
        let r = sqlx::query("SELECT (a + 1)::text, n * 0.00000000011 FROM w WHERE id = 5")
            .fetch_one(&pool)
            .await
            .expect("select");
        assert_eq!(r.get::<String, _>(0), "1.100000000000", "{partitioning}");
        assert_eq!(r.get::<f64, _>(1), 5.5e-10, "{partitioning}");
    }
    let server = TestServer::start_file_backed(&dir.0).await.expect("restart");
    let pool = server.connect_sqlx().await.expect("connect");
    assert_eq!(rows(&pool).await, want, "{partitioning}: after a restart");
}

#[tokio::test(flavor = "multi_thread")]
async fn wide_scale_numeric_arithmetic_on_a_plain_table() {
    wide_scale("").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn wide_scale_numeric_arithmetic_on_a_hash_partitioned_table() {
    wide_scale(" PARTITION BY HASH (id) PARTITIONS 4").await;
}
