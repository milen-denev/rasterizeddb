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
use sqlx::{AssertSqlSafe, Executor, Row, ValueRef};

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

        // What cannot be carried exactly is refused, not cut: WHERE-clause
        // arithmetic over the column's digits past nine, a literal past nine
        // there, a quotient that does not end, and an assignment the float a
        // long literal is carried as cannot settle (PostgreSQL:
        // 0.000000000001).
        for sql in [
            "SELECT id FROM w WHERE a + 1 > 1",
            "SELECT a / 3 FROM w WHERE id = 1",
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

        // Reads of the digits past nine that are exact are answered exactly
        // (they were refused): a comparison, casts, unary minus, COALESCE,
        // + - * between NUMERICs, an ORDER BY key, a join's projection.
        let text = |sql: &'static str| {
            let pool = pool.clone();
            async move {
                sqlx::raw_sql(AssertSqlSafe(sql))
                    .fetch_all(&pool)
                    .await
                    .unwrap_or_else(|e| panic!("{sql}: {e}"))
                    .iter()
                    .map(|r| {
                        (0..r.len())
                            .map(|i| r.try_get_raw(i).ok().and_then(|v| v.as_str().ok().map(str::to_string)).unwrap_or("NULL".into()))
                            .collect::<Vec<_>>()
                            .join(" ")
                    })
                    .collect::<Vec<_>>()
            }
        };
        assert_eq!(
            text("SELECT a + 1, -a, COALESCE(a, 0), a * 2, a - a FROM w WHERE id = 1").await,
            ["1.123456789124 -0.123456789124 0.123456789124 0.246913578248 0.000000000000"],
            "{partitioning}"
        );
        assert_eq!(
            text("SELECT CASE WHEN a > 0.1234567891239 THEN 'big' ELSE 'small' END, a::numeric(10,2), a::float8, a::text FROM w WHERE id = 1").await,
            ["big 0.12 0.123456789124 0.123456789124"],
            "{partitioning}"
        );
        assert_eq!(
            text("SELECT id FROM w WHERE a IS NOT NULL ORDER BY a + 1 DESC LIMIT 3").await,
            ["8", "1", "5"],
            "{partitioning}"
        );
        run(&pool, "CREATE TABLE wj (id INT PRIMARY KEY, b NUMERIC(20,12))").await;
        run(&pool, "INSERT INTO wj VALUES (1, 0.000000000001)").await;
        assert_eq!(
            text("SELECT w.a + wj.b FROM w JOIN wj ON wj.id = w.id").await,
            ["0.123456789125"],
            "{partitioning}"
        );
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

/// Two paths that still cut to nine places: an INSERT … SELECT (its rows
/// were rounded to the engine scale on the way out of the SELECT, so `a + 1`
/// over a NUMERIC(20,12) stored 1.123456789000), and a quotient whose exact
/// form passed an i128 (`a / 7` on a NUMERIC(38,20) of 1e13 fell back to nine
/// places and stored …714285714000000000000). Exact now, or refused.
#[tokio::test(flavor = "multi_thread")]
async fn no_path_cuts_a_wide_numeric_to_nine_places() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    run(
        &pool,
        "CREATE TABLE w (id INT PRIMARY KEY, a NUMERIC(20,12)); CREATE TABLE t2 (id INT PRIMARY KEY, a NUMERIC(20,12)); \
         CREATE TABLE t3 (id INT PRIMARY KEY, a NUMERIC(10,2)); INSERT INTO w VALUES (1, 0.123456789123), (2, 1.5); \
         CREATE TABLE z (id INT PRIMARY KEY, a NUMERIC(38,20)); INSERT INTO z VALUES (1, 10000000000000), (2, 1.5)",
    )
    .await;
    run(&pool, "INSERT INTO t2 SELECT id, a + 1 FROM w").await;
    run(&pool, "INSERT INTO t3 SELECT id, a + 1 FROM w").await;
    let text = |sql: &'static str| {
        let pool = pool.clone();
        async move {
            sqlx::query(sql)
                .fetch_all(&pool)
                .await
                .expect(sql)
                .iter()
                .map(|r| r.get::<String, _>(0))
                .collect::<Vec<_>>()
        }
    };
    assert_eq!(text("SELECT a::text FROM t2 ORDER BY id").await, ["1.123456789123", "2.500000000000"]);
    assert_eq!(text("SELECT a::text FROM t3 ORDER BY id").await, ["1.12", "2.50"]);
    assert_eq!(
        text("WITH c AS (SELECT id, a + 1 AS b FROM w) SELECT b::text FROM c ORDER BY id").await,
        ["1.123456789123", "2.500000000000"]
    );

    run(&pool, "UPDATE z SET a = a / 7").await;
    assert_eq!(
        text("SELECT a::text FROM z ORDER BY id").await,
        ["1428571428571.42857142857142857143", "0.21428571428571428571"]
    );
    // Past what even the exact reading holds: refused, the row unchanged.
    assert_eq!(refused(&pool, "UPDATE z SET a = a * 100000000000000000 + a / 3 WHERE id = 1").await, "0A000");
    assert_eq!(text("SELECT a::text FROM z WHERE id = 1").await, ["1428571428571.42857142857142857143"]);
    // A value with no digits past nine, whose exact quotient is past an
    // i128: through the engine it would be computed at nine places and
    // stored cut; refused instead.
    run(&pool, "INSERT INTO z VALUES (3, 900000000000000000)").await;
    assert_eq!(refused(&pool, "UPDATE z SET a = a / 7 WHERE id = 3").await, "0A000");
    assert_eq!(text("SELECT a::text FROM z WHERE id = 3").await, ["900000000000000000.00000000000000000000"]);
}
