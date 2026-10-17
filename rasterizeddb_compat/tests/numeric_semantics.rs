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
