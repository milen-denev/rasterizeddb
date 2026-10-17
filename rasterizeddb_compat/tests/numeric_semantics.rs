//! Numeric and arithmetic semantics, against PostgreSQL 17's own answers.
//!
//! Every expected value here was produced by running the same statement on a
//! PostgreSQL 17 server. The failures this pins were all silent: a product that
//! wrapped at 32 bits, a decimal literal evaluated as a binary float, a
//! `NUMERIC(p, s)` assignment that truncated instead of rounding, a generated
//! column computed from the unrounded input, a date plus a day count that added
//! microseconds, and a scale that came back as nine places through a cast or a
//! CTE. Each one returned a plausible number and reported success.
//!
//! Text is compared as the server rendered it (the simple protocol's text
//! format), because a scale is part of the answer: `4.50` and `4.500000000` are
//! equal numbers and different results.

#[path = "support/raw_wire.rs"]
mod raw_wire;

use rasterizeddb_compat::TestServer;
use raw_wire::RawConn;
use rust_decimal::Decimal;
use sqlx::{AssertSqlSafe, Column, Executor, Row, TypeInfo};

struct Db {
    _server: TestServer,
    pool: sqlx::PgPool,
    raw: RawConn,
}

async fn db(setup: &[&str]) -> Db {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    for sql in setup {
        pool.execute(AssertSqlSafe(sql.to_string()))
            .await
            .unwrap_or_else(|e| panic!("setup `{}` failed: {}", sql, e));
    }
    let raw = RawConn::connect(server.port()).await;
    Db {
        _server: server,
        pool,
        raw,
    }
}

const DIGITS: &[&str] = &[
    "CREATE TABLE dg (n INT, one INT)",
    "INSERT INTO dg (n, one) VALUES (0,1),(1,1),(2,1),(3,1),(4,1),(5,1),(6,1),(7,1),(8,1),(9,1)",
    "CREATE TABLE lim (id INT, i INT, s SMALLINT, b BIGINT)",
    "INSERT INTO lim (id, i, s, b) VALUES (1, 2147483647, 32767, 9223372036854775807), \
     (2, -2147483648, -32768, 3000000000), (3, 9, 3, 3)",
];

impl Db {
    /// The rows a statement returned, as text, with `NULL` for a null.
    async fn text(&mut self, sql: &str) -> Result<Vec<String>, String> {
        let shape = self.raw.simple(sql).await?;
        Ok(shape
            .rows
            .iter()
            .map(|r| {
                r.iter()
                    .map(|f| f.clone().unwrap_or_else(|| "NULL".to_string()))
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect())
    }

    /// The SQLSTATE a statement failed with, or a description of its success.
    async fn sqlstate(&self, sql: &str) -> String {
        match sqlx::query(AssertSqlSafe(sql.to_string())).fetch_all(&self.pool).await {
            Ok(rows) => format!("succeeded with {} row(s)", rows.len()),
            Err(sqlx::Error::Database(e)) => {
                format!("{} {}", e.code().unwrap_or_default(), e.message())
            }
            Err(e) => format!("client error {}", e),
        }
    }

    /// The type names the server described a one-row result with.
    async fn types(&self, sql: &str) -> Result<Vec<String>, String> {
        let row = sqlx::query(AssertSqlSafe(sql.to_string()))
            .fetch_one(&self.pool)
            .await
            .map_err(|e| e.to_string())?;
        Ok(row
            .columns()
            .iter()
            .map(|c| c.type_info().name().to_string())
            .collect())
    }
}

/// Runs `(sql, expected rows)` pairs and collects every disagreement, so one
/// run reports all of them.
async fn expect_rows(db: &mut Db, wrong: &mut Vec<String>, cases: &[(&str, &[&str])]) {
    for (sql, expected) in cases {
        match db.text(sql).await {
            Ok(got) if got == *expected => {}
            Ok(got) => wrong.push(format!("{}\n    got  {:?}\n    want {:?}", sql, got, expected)),
            Err(e) => wrong.push(format!("{}\n    error {}\n    want {:?}", sql, e, expected)),
        }
    }
}

/// Runs statements that PostgreSQL refuses with `22003`, and the message it
/// gives.
async fn expect_out_of_range(db: &Db, wrong: &mut Vec<String>, cases: &[(&str, &str)]) {
    for (sql, message) in cases {
        let got = db.sqlstate(sql).await;
        if !got.starts_with("22003") || !got.contains(message) {
            wrong.push(format!("{}\n    got  {}\n    want 22003 {}", sql, got, message));
        }
    }
}

fn verdict(wrong: Vec<String>) {
    assert!(wrong.is_empty(), "{} wrong answer(s):\n{}", wrong.len(), wrong.join("\n"));
}

// ---------------------------------------------------------------- bug #2

/// A literal past `int4` is `int8`, and arithmetic never wraps.
#[tokio::test(flavor = "multi_thread")]
async fn integer_literals_are_typed_and_arithmetic_never_wraps() {
    let mut db = db(DIGITS).await;
    let mut wrong = Vec::new();
    expect_rows(
        &mut db,
        &mut wrong,
        &[
            // `3000000000` is a bigint, so the product is one. It wrapped to
            // 1230196224 at 32 bits.
            ("SELECT n * 3000000000 FROM dg WHERE n = 9", &["27000000000"]),
            ("SELECT 2147483648 + n FROM dg WHERE n = 2", &["2147483650"]),
            // A smallint times an integer literal is an integer. It came back
            // as 0: 65534 wrapped at 16 bits.
            ("SELECT s * 2 FROM lim WHERE id = 1", &["65534"]),
            ("SELECT n FROM dg WHERE n * 3000000000 > 20000000000 ORDER BY n", &["7", "8", "9"]),
            ("SELECT -n, abs(n - 5) FROM dg WHERE n = 3", &["-3|2"]),
            ("SELECT i % -1, i / 2 FROM lim WHERE id = 2", &["0|-1073741824"]),
        ],
    )
    .await;
    for (sql, want) in [
        ("SELECT n * 3000000000 FROM dg WHERE n = 9", vec!["INT8"]),
        ("SELECT s * 2 FROM lim WHERE id = 1", vec!["INT4"]),
        ("SELECT n + 1, 2147483648 + n FROM dg WHERE n = 2", vec!["INT4", "INT8"]),
        ("SELECT -n FROM dg WHERE n = 3", vec!["INT4"]),
    ] {
        match db.types(sql).await {
            Ok(got) if got == want => {}
            other => wrong.push(format!("{}: described as {:?}, want {:?}", sql, other, want)),
        }
    }
    expect_out_of_range(
        &db,
        &mut wrong,
        &[
            ("SELECT n * 1000000000 FROM dg WHERE n = 9", "integer out of range"),
            ("SELECT s + s FROM lim WHERE id = 1", "smallint out of range"),
            ("SELECT i + 1 FROM lim WHERE id = 1", "integer out of range"),
            ("SELECT b * 2 FROM lim WHERE id = 1", "bigint out of range"),
            ("SELECT -i FROM lim WHERE id = 2", "integer out of range"),
            ("SELECT abs(i) FROM lim WHERE id = 2", "integer out of range"),
            ("SELECT b::int FROM lim WHERE id = 2", "integer out of range"),
            // `-2147483648 / -1` panicked the scan worker.
            ("SELECT id FROM lim WHERE i / -1 > 0", "integer out of range"),
        ],
    )
    .await;
    verdict(wrong);
}

/// The write paths: an assignment that overflows its column is refused and
/// leaves the row alone, rather than storing the low bytes.
#[tokio::test(flavor = "multi_thread")]
async fn integer_assignments_refuse_overflow_instead_of_wrapping() {
    let mut db = db(DIGITS).await;
    let mut wrong = Vec::new();
    expect_out_of_range(
        &db,
        &mut wrong,
        &[
            // Stored 705032704 and reported success.
            ("UPDATE lim SET i = i * 1000000000 WHERE id = 3", "integer out of range"),
            // Stored -32768.
            ("UPDATE lim SET s = s + 1 WHERE id = 1", "smallint out of range"),
            ("INSERT INTO lim (id, i, s, b) VALUES (4, 2147483647 + 1, 0, 0)", "integer out of range"),
        ],
    )
    .await;
    for sql in [
        "UPDATE lim SET b = i * 3000000000 WHERE id = 3",
        "INSERT INTO lim (id, i, s, b) VALUES (5, 7, 0, 3000000000 * 3)",
    ] {
        if let Err(e) = db.pool.execute(sql).await {
            wrong.push(format!("{}: {}", sql, e));
        }
    }
    expect_rows(
        &mut db,
        &mut wrong,
        &[(
            "SELECT id, i, s, b FROM lim ORDER BY id",
            &[
                "1|2147483647|32767|9223372036854775807",
                "2|-2147483648|-32768|3000000000",
                "3|9|3|27000000000",
                "5|7|0|9000000000",
            ],
        )],
    )
    .await;
    verdict(wrong);
}

// ---------------------------------------------------------------- bug #3

/// A decimal literal is `numeric`, so arithmetic on it is exact decimal: int
/// with numeric is numeric, and only a float operand makes it a float.
#[tokio::test(flavor = "multi_thread")]
async fn decimal_literals_are_numeric_not_float() {
    let mut db = db(&[
        DIGITS[0],
        DIGITS[1],
        "CREATE TABLE nq (id INT, v NUMERIC(10,2))",
        "INSERT INTO nq (id, v) VALUES (1, 1.10), (2, 2.25), (3, 0.30)",
        "CREATE TABLE fq (id INT, i INT, f FLOAT8, u NUMERIC(10,2))",
        "INSERT INTO fq (id, i, f, u) VALUES (1, 3, 0, 0)",
        // A NUMERIC result assigned to a float column, and an integer one to a
        // NUMERIC column, each in the column's own type.
        "UPDATE fq SET f = i * 1.5, u = i * 2 WHERE id = 1",
    ])
    .await;
    let mut wrong = Vec::new();
    expect_rows(
        &mut db,
        &mut wrong,
        &[
            ("SELECT f, u FROM fq", &["4.5|6.00"]),
            // NUMERIC against a float compares as floats.
            ("SELECT id FROM fq WHERE f = i * 1.5", &["1"]),
            ("SELECT id FROM fq WHERE f * 2 = i * 3", &["1"]),
            ("SELECT 'r' || 1.5, 'r' || (i * 0.5) FROM fq", &["r1.5|r1.5"]),
        ],
    )
    .await;
    expect_rows(
        &mut db,
        &mut wrong,
        &[
            // 20.169999999999998 as a float8.
            ("SELECT 14 * 1.37 + 0.99 FROM dg WHERE n = 1", &["20.17"]),
            (
                "SELECT n * 1.37 + 0.99, n * 1.5, n + 0.5, 1.5 * 2.25 FROM dg WHERE n = 9",
                &["13.32|13.5|9.5|3.375"],
            ),
            ("SELECT 1.37::float8 * 2, n::float8 * 0.1 FROM dg WHERE n = 3", &["2.74|0.30000000000000004"]),
            ("SELECT n FROM dg WHERE n * 1.37 = 12.33", &["9"]),
            ("SELECT n FROM dg WHERE n * 0.1 = 0.3", &["3"]),
            // A NUMERIC(10,2) column against a literal with more places
            // compares exactly: the literal used to be truncated to the
            // column's scale, so 2.25 = 2.251 held and 2.25 < 2.251 did not.
            ("SELECT id FROM nq WHERE v < 2.251 ORDER BY id", &["1", "2", "3"]),
            ("SELECT id FROM nq WHERE v = 2.251", &[]),
            ("SELECT id FROM nq WHERE v >= 2.249 ORDER BY id", &["2"]),
            (
                "SELECT id, v * 2, v + 1, v * 1.5, v - 0.125 FROM nq ORDER BY id",
                &["1|2.20|2.10|1.650|0.975", "2|4.50|3.25|3.375|2.125", "3|0.60|1.30|0.450|0.175"],
            ),
        ],
    )
    .await;
    for (sql, want) in [
        ("SELECT 14 * 1.37 + 0.99 FROM dg WHERE n = 1", vec!["NUMERIC"]),
        ("SELECT n * 1.5 FROM dg WHERE n = 9", vec!["NUMERIC"]),
        ("SELECT 1.37::float8 * 2, n::float8 * 0.1 FROM dg WHERE n = 3", vec!["FLOAT8", "FLOAT8"]),
    ] {
        match db.types(sql).await {
            Ok(got) if got == want => {}
            other => wrong.push(format!("{}: described as {:?}, want {:?}", sql, other, want)),
        }
    }
    verdict(wrong);
}

// ---------------------------------------------------------------- bug #4

/// Assignment to `NUMERIC(p, s)` rounds half away from zero and refuses a value
/// past the precision.
#[tokio::test(flavor = "multi_thread")]
async fn numeric_assignment_rounds_and_checks_precision() {
    let mut db = db(&["CREATE TABLE np (id INT, p NUMERIC(12,2))"]).await;
    let mut wrong = Vec::new();
    for sql in [
        "INSERT INTO np (id, p) VALUES (1, 20.169), (2, 20.165), (3, -20.165), (4, 20.164), (5, 0.005), (6, -0.005)",
        "INSERT INTO np (id, p) VALUES (9, 9999999999.994)",
        "INSERT INTO np (id, p) VALUES (10, 14 * 1.37 + 0.99)",
        "UPDATE np SET p = p * 1.005 WHERE id = 1",
        "UPDATE np SET p = 2.345 WHERE id = 2",
    ] {
        if let Err(e) = db.pool.execute(sql).await {
            wrong.push(format!("{}: {}", sql, e));
        }
    }
    expect_out_of_range(
        &db,
        &mut wrong,
        &[
            ("INSERT INTO np (id, p) VALUES (7, 9999999999.995)", "numeric field overflow"),
            ("INSERT INTO np (id, p) VALUES (8, 100000000000)", "numeric field overflow"),
            ("UPDATE np SET p = p * 1000000000 WHERE id = 9", "numeric field overflow"),
            ("UPDATE np SET p = 100000000000 WHERE id = 9", "numeric field overflow"),
        ],
    )
    .await;
    expect_rows(
        &mut db,
        &mut wrong,
        &[(
            "SELECT id, p FROM np ORDER BY id",
            &[
                // 20.169 * 1.005 = 20.27085
                "1|20.27",
                "2|2.35",
                "3|-20.17",
                "4|20.16",
                "5|0.01",
                "6|-0.01",
                "9|9999999999.99",
                "10|20.17",
            ],
        )],
    )
    .await;
    verdict(wrong);
}

// ---------------------------------------------------------------- bug #5

/// A generated column reads its inputs as stored, after they are rounded to
/// their own columns' types.
#[tokio::test(flavor = "multi_thread")]
async fn generated_columns_read_the_rounded_inputs() {
    let mut db = db(&[
        "CREATE TABLE gen (id INT, p NUMERIC(12,2), g NUMERIC(14,2) GENERATED ALWAYS AS (p * 15) STORED)",
        "INSERT INTO gen (id, p) VALUES (1, 20.169), (2, 20.165), (3, 0.333)",
    ])
    .await;
    let mut wrong = Vec::new();
    expect_rows(
        &mut db,
        &mut wrong,
        &[
            // g was 302.53, computed from 20.169 while p stored 20.16.
            ("SELECT id, p, g FROM gen ORDER BY id", &["1|20.17|302.55", "2|20.17|302.55", "3|0.33|4.95"]),
            ("SELECT id FROM gen WHERE g <> p * 15", &[]),
        ],
    )
    .await;
    verdict(wrong);
}

/// The reporter's shape at scale: 1,000 random three-place prices, each of
/// which has to round, into `NUMERIC(12,2)` beside a generated `p * 15`. Not
/// one row may disagree with its own inputs.
#[tokio::test(flavor = "multi_thread")]
async fn a_thousand_generated_rows_agree_with_their_inputs() {
    let db = db(&[
        "CREATE TABLE stress (id INT, p NUMERIC(12,2), g NUMERIC(14,2) GENERATED ALWAYS AS (p * 15) STORED)",
    ])
    .await;
    // A fixed-seed LCG: the same thousand values every run.
    let mut state: u64 = 0x2545_F491_4F6C_DD1D;
    let mut next = || {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        state >> 33
    };
    let mut written: Vec<(i32, Decimal)> = Vec::with_capacity(1000);
    for batch in 0..10 {
        let mut values = Vec::with_capacity(100);
        for k in 0..100 {
            let id = batch * 100 + k;
            // -99999.999 ..= 99999.999, three places so most values round.
            let thousandths = (next() % 200_000_000) as i64 - 99_999_999;
            let p = Decimal::new(thousandths, 3);
            written.push((id, p));
            values.push(format!("({}, {})", id, p));
        }
        let sql = format!("INSERT INTO stress (id, p) VALUES {}", values.join(", "));
        db.pool
            .execute(AssertSqlSafe(sql))
            .await
            .unwrap_or_else(|e| panic!("batch {} failed: {}", batch, e));
    }

    let disagreeing = sqlx::query("SELECT id FROM stress WHERE g <> p * 15")
        .fetch_all(&db.pool)
        .await
        .expect("WHERE g <> p * 15");
    assert!(
        disagreeing.is_empty(),
        "{} of 1000 rows have g <> p * 15, first id {}",
        disagreeing.len(),
        disagreeing[0].get::<i32, _>(0)
    );

    // And each stored pair is PostgreSQL's: p rounded half away from zero, g
    // exactly fifteen times that.
    let rows = sqlx::query("SELECT id, p, g FROM stress ORDER BY id")
        .fetch_all(&db.pool)
        .await
        .expect("read back");
    assert_eq!(rows.len(), 1000);
    let mut wrong = Vec::new();
    for (row, (id, written)) in rows.iter().zip(&written) {
        let p: Decimal = row.get(1);
        let g: Decimal = row.get(2);
        let want_p =
            written.round_dp_with_strategy(2, rust_decimal::RoundingStrategy::MidpointAwayFromZero);
        let want_g = want_p * Decimal::from(15);
        if row.get::<i32, _>(0) != *id || p != want_p || g != want_g {
            wrong.push(format!("id {}: wrote {}, stored p={} g={}, want p={} g={}", id, written, p, g, want_p, want_g));
        }
    }
    assert!(wrong.is_empty(), "{} rows stored wrongly:\n{}", wrong.len(), wrong.join("\n"));
}

// ---------------------------------------------------------------- bug #14

/// A cast to `numeric(p, s)` rounds to `s` and renders at it, and a
/// `NUMERIC(p, s)` column keeps its scale through a CTE or a derived table.
#[tokio::test(flavor = "multi_thread")]
async fn cast_and_cte_keep_the_declared_scale() {
    let mut db = db(&[
        DIGITS[0],
        DIGITS[1],
        "CREATE TABLE cte_src (id INT, gross NUMERIC(12,2))",
        "INSERT INTO cte_src (id, gross) VALUES (1, 552645.5), (2, 10)",
    ])
    .await;
    let mut wrong = Vec::new();
    expect_rows(
        &mut db,
        &mut wrong,
        &[
            // 4.500000000.
            (
                "SELECT (3 * 1.5)::numeric(10,2), CAST(3 * 1.5 AS numeric(10,3)), 2.5::numeric(10,0), \
                 3.5::numeric(10,0), (-2.5)::numeric(10,0) FROM dg WHERE n = 1",
                &["4.50|4.500|3|4|-3"],
            ),
            // 552645.500000000.
            (
                "WITH c AS (SELECT id, gross FROM cte_src) SELECT id, gross FROM c ORDER BY id",
                &["1|552645.50", "2|10.00"],
            ),
            (
                "SELECT id, gross FROM (SELECT id, gross FROM cte_src) s ORDER BY id",
                &["1|552645.50", "2|10.00"],
            ),
            (
                "WITH c AS (SELECT id, gross * 2 AS g2 FROM cte_src) SELECT id, g2 FROM c ORDER BY id",
                &["1|1105291.00", "2|20.00"],
            ),
        ],
    )
    .await;
    expect_out_of_range(
        &db,
        &mut wrong,
        &[("SELECT 123.456::numeric(4,2) FROM dg WHERE n = 1", "numeric field overflow")],
    )
    .await;
    verdict(wrong);
}
