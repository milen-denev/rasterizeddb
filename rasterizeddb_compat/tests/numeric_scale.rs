//! Predicates over `NUMERIC(p, s)` columns.
//!
//! A `DECIMAL` is stored as an `i128` of units scaled by the column's own `s`,
//! while a literal used to be encoded at the engine's default scale of 9. Every
//! comparison over a column declared with any other scale was therefore off by
//! a power of ten — and *answered* rather than failing: `WHERE price > 50` over
//! a `NUMERIC(10,2)` column holding 100 and 250 returned no rows at all.
//!
//! The same query is run against four columns whose only difference is the
//! declared scale, and all four must agree. Testing one scale would have passed
//! throughout: the bug is invisible at `NUMERIC` and at `NUMERIC(p, 9)`, which
//! are exactly the shapes a hand-written test reaches for.

use rasterizeddb_compat::TestServer;
use rust_decimal::Decimal;
use sqlx::{AssertSqlSafe, Row};

/// The declared types under test, all holding the same two values.
const DECLARATIONS: &[(&str, &str)] = &[
    ("plain", "NUMERIC"),
    ("scale0", "NUMERIC(12,0)"),
    ("scale2", "NUMERIC(10,2)"),
    ("scale4", "NUMERIC(12,4)"),
    ("scale9", "NUMERIC(20,9)"),
];

async fn seeded() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    for (table, declared) in DECLARATIONS {
        sqlx::query(AssertSqlSafe(format!(
            "CREATE TABLE {} (id INTEGER NOT NULL, v {} NOT NULL)",
            table, declared
        )))
        .execute(&pool)
        .await
        .unwrap_or_else(|e| panic!("CREATE {} failed: {}", declared, e));
        for (id, value) in [(1, "100"), (2, "250")] {
            sqlx::query(AssertSqlSafe(format!(
                "INSERT INTO {} (id, v) VALUES ({}, {})",
                table, id, value
            )))
            .execute(&pool)
            .await
            .unwrap_or_else(|e| panic!("INSERT into {} failed: {}", table, e));
        }
    }
    (server, pool)
}

async fn ids(pool: &sqlx::PgPool, table: &str, predicate: &str) -> Vec<i32> {
    sqlx::query(AssertSqlSafe(format!(
        "SELECT id FROM {} WHERE {} ORDER BY id",
        table, predicate
    )))
    .fetch_all(pool)
    .await
    .unwrap_or_else(|e| panic!("{} / {} failed: {}", table, predicate, e))
    .iter()
    .map(|r| r.get::<i32, _>(0))
    .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn every_declared_scale_answers_the_same_predicate_alike() {
    let (_s, pool) = seeded().await;

    for (predicate, expected) in [
        ("v > 50", vec![1, 2]),
        ("v >= 100", vec![1, 2]),
        ("v = 100", vec![1]),
        ("v > 100", vec![2]),
        ("v < 200", vec![1]),
        ("v < 1000", vec![1, 2]),
        ("v <> 100", vec![2]),
        ("v BETWEEN 90 AND 150", vec![1]),
    ] {
        for (table, declared) in DECLARATIONS {
            assert_eq!(
                ids(&pool, table, predicate).await,
                expected,
                "{} over {}",
                predicate,
                declared
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_fractional_literal_compares_at_the_column_scale() {
    let (_s, pool) = seeded().await;
    // 100 is not above 100.5, and is above 99.5 — at every declared scale that
    // can represent the distinction. `NUMERIC(12,0)` rounds the literal, as
    // PostgreSQL does, so it is left out of the fractional cases.
    for (table, declared) in DECLARATIONS.iter().filter(|(t, _)| *t != "scale0") {
        assert_eq!(ids(&pool, table, "v > 100.5").await, vec![2], "{}", declared);
        assert_eq!(ids(&pool, table, "v > 99.5").await, vec![1, 2], "{}", declared);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_value_read_back_is_the_value_written() {
    let (_s, pool) = seeded().await;
    for (table, declared) in DECLARATIONS {
        let value: Decimal = sqlx::query(AssertSqlSafe(format!(
            "SELECT v FROM {} WHERE id = 1",
            table
        )))
        .fetch_one(&pool)
        .await
        .expect("select")
        .get(0);
        assert_eq!(value, Decimal::from(100), "{}", declared);
    }
}

/// The text protocol (`psql`, simple queries) renders through a different
/// formatter than the binary one sqlx's prepared statements use, and scale 0
/// used to print `100` as `0` there.
#[tokio::test(flavor = "multi_thread")]
async fn the_text_protocol_reads_back_the_value_written() {
    let (_s, pool) = seeded().await;
    for (table, declared) in DECLARATIONS {
        let row = sqlx::raw_sql(AssertSqlSafe(format!(
            "SELECT v FROM {} WHERE id = 1",
            table
        )))
        .fetch_one(&pool)
        .await
        .expect("select");
        let value: Decimal = row.get(0);
        assert_eq!(value, Decimal::from(100), "{}", declared);
    }
}

/// Arithmetic reads the column's stored integer, which is scaled by the
/// column's own `s`, not by the engine default — every declared scale must
/// give the same answer, whichever position the value takes in the row and
/// whichever protocol carries it.
#[tokio::test(flavor = "multi_thread")]
async fn expressions_agree_at_every_declared_scale() {
    let (_s, pool) = seeded().await;
    // (select list, expected value of its *last* item for id = 1)
    let cases = [
        ("v", "100"),
        ("v * 2", "200"),
        ("v + 1", "101"),
        ("v - 1", "99"),
        ("v / 4", "25"),
        ("v * 1.5", "150"),
        ("v + 0.25", "100.25"),
        ("v + v", "200"),
        ("COALESCE(v, 0)", "100"),
        ("id, v", "100"),
        ("id, v + 1", "101"),
        ("v + 1, id, v", "100"),
        ("SUM(v)", "100"),
        ("MAX(v)", "100"),
        ("MIN(v) + 1", "101"),
        ("SUM(v) * 2", "200"),
        ("AVG(v)", "100"),
        ("COUNT(*), MAX(v) - 1", "99"),
        ("v * 1000000000000", "100000000000000"),
    ];
    let mut wrong = Vec::new();
    for (list, expected) in cases {
        let expected: Decimal = expected.parse().unwrap();
        for (table, declared) in DECLARATIONS {
            let sql = format!("SELECT {} FROM {} WHERE id = 1", list, table);
            for (protocol, row) in [
                ("binary", sqlx::query(AssertSqlSafe(sql.clone())).fetch_one(&pool).await),
                ("text", sqlx::raw_sql(AssertSqlSafe(sql.clone())).fetch_one(&pool).await),
            ] {
                match row {
                    Ok(row) => {
                        let last = row.len() - 1;
                        match row.try_get::<Decimal, _>(last) {
                            Ok(v) if v == expected => {}
                            Ok(v) => wrong.push(format!("{declared:14} {protocol:6} {sql}: got {v}")),
                            Err(e) => wrong.push(format!("{declared:14} {protocol:6} {sql}: decode {e}")),
                        }
                    }
                    Err(e) => wrong.push(format!("{declared:14} {protocol:6} {sql}: error {e}")),
                }
            }
        }
    }
    assert!(wrong.is_empty(), "{} wrong answers:\n{}", wrong.len(), wrong.join("\n"));
}

/// An expression in `WHERE` compares at one scale too.
///
/// Arithmetic on a NUMERIC in `WHERE` runs in the scan's predicate engine
/// (`perform_math_operation`), which had no decimal arithmetic and refused
/// every such predicate — at every scale, including plain `NUMERIC` — with
/// "operand size does not match the column type being evaluated". It now
/// computes at the engine scale, lifting each column from its declared one,
/// exactly as the expression evaluator does.
#[tokio::test(flavor = "multi_thread")]
async fn an_expression_predicate_agrees_at_every_declared_scale() {
    let (_s, pool) = seeded().await;
    let mut wrong = Vec::new();
    for (predicate, expected) in [
        ("v + 1 > 101", vec![2]),
        ("v * 2 = 200", vec![1]),
        ("v - 0.5 < 100", vec![1]),
        ("101 < v + 1", vec![2]),
        ("2 * v = 200", vec![1]),
        ("v / 4 = 25", vec![1]),
        ("v * 1.5 > 200", vec![2]),
        ("v + v = 200", vec![1]),
        ("v + 1 > 100.5", vec![1, 2]),
        ("v + 0.25 = 100.25", vec![1]),
        // An arithmetic result against the column itself, at its own scale.
        ("v + 1 > v", vec![1, 2]),
        ("v - 1 >= v", vec![]),
        ("v > v - 1", vec![1, 2]),
        // An integer column joining the arithmetic.
        ("v + id > 101", vec![2]),
        ("v + id = 101", vec![1]),
        // Several steps, and precedence.
        ("v * 2 - 1 = 199", vec![1]),
        ("v + 1 > 101 OR v * 2 = 200", vec![1, 2]),
        ("v + 1 > 101 AND id = 2", vec![2]),
    ] {
        for (table, declared) in DECLARATIONS {
            let got = sqlx::query(AssertSqlSafe(format!(
                "SELECT id FROM {} WHERE {} ORDER BY id",
                table, predicate
            )))
            .fetch_all(&pool)
            .await
            .map(|rows| rows.iter().map(|r| r.get::<i32, _>(0)).collect::<Vec<_>>());
            match got {
                Ok(ids) if ids == expected => {}
                Ok(ids) => wrong.push(format!("{declared:14} {predicate}: got {ids:?}")),
                Err(e) => wrong.push(format!("{declared:14} {predicate}: error {e}")),
            }
        }
    }
    assert!(wrong.is_empty(), "{} wrong answers:\n{}", wrong.len(), wrong.join("\n"));
}

/// Two columns of different declared scales compare as numbers, not as the
/// integers they are stored as. `a > b` over `NUMERIC(10,2)` and
/// `NUMERIC(12,4)` compared 10000 (100.00) against 1000000 (100.0000).
#[tokio::test(flavor = "multi_thread")]
async fn columns_of_different_scales_compare_as_numbers() {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    sqlx::query("CREATE TABLE mix (id INTEGER NOT NULL, a NUMERIC(10,2) NOT NULL, b NUMERIC(12,4) NOT NULL, c NUMERIC NOT NULL)")
        .execute(&pool)
        .await
        .expect("create");
    for (id, a, b, c) in [(1, "100", "100", "100"), (2, "100.5", "100.25", "99"), (3, "1", "2.5", "2.5")] {
        sqlx::query(AssertSqlSafe(format!(
            "INSERT INTO mix (id, a, b, c) VALUES ({}, {}, {}, {})",
            id, a, b, c
        )))
        .execute(&pool)
        .await
        .expect("insert");
    }
    let mut wrong = Vec::new();
    for (predicate, expected) in [
        ("a = b", vec![1]),
        ("a > b", vec![2]),
        ("a < b", vec![3]),
        ("b = c", vec![1, 3]),
        ("a > c", vec![2]),
        ("a + b > 200", vec![2]),
        ("a * 2 = b * 2", vec![1]),
    ] {
        match sqlx::query(AssertSqlSafe(format!(
            "SELECT id FROM mix WHERE {} ORDER BY id",
            predicate
        )))
        .fetch_all(&pool)
        .await
        {
            Ok(rows) => {
                let got: Vec<i32> = rows.iter().map(|r| r.get::<i32, _>(0)).collect();
                if got != expected {
                    wrong.push(format!("{}: got {:?}, want {:?}", predicate, got, expected));
                }
            }
            Err(e) => wrong.push(format!("{}: error {}", predicate, e)),
        }
    }
    assert!(wrong.is_empty(), "{} wrong answers:\n{}", wrong.len(), wrong.join("\n"));
}

/// The filter index bounds a predicate's literal at the column's declared
/// scale, as the evaluator compares it.
///
/// Only a table large enough for the index to be consulted shows this, which
/// is why the small fixtures above passed throughout: over 5000 rows of a
/// `NUMERIC(10,2)` column, `v > 50` returned **no rows**. The index encoded 50
/// at the engine scale, ten million times the stored 50.00, and pruned every
/// zone; the evaluator never saw a row.
#[tokio::test(flavor = "multi_thread")]
async fn the_filter_index_prunes_at_the_declared_scale() {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    for (table, declared) in DECLARATIONS {
        sqlx::query(AssertSqlSafe(format!(
            "CREATE TABLE big_{} (id INTEGER NOT NULL, v {} NOT NULL)",
            table, declared
        )))
        .execute(&pool)
        .await
        .expect("create");
        for chunk in 0..10 {
            let values: Vec<String> = (0..500)
                .map(|i| {
                    let id = chunk * 500 + i;
                    format!("({}, {})", id, id % 300)
                })
                .collect();
            sqlx::raw_sql(AssertSqlSafe(format!(
                "INSERT INTO big_{} (id, v) VALUES {}",
                table,
                values.join(",")
            )))
            .execute(&pool)
            .await
            .expect("insert");
        }
    }
    let mut wrong = Vec::new();
    // 5000 rows, v = id % 300: each value 0..299 appears 16 or 17 times.
    let expected = |pred: &dyn Fn(i64) -> bool| (0..5000i64).filter(|id| pred(id % 300)).count() as i64;
    for (predicate, want) in [
        ("v > 50", expected(&|v| v > 50)),
        ("v = 100", expected(&|v| v == 100)),
        ("v >= 299", expected(&|v| v >= 299)),
        ("v < 10", expected(&|v| v < 10)),
        ("v BETWEEN 100 AND 110", expected(&|v| (100..=110).contains(&v))),
        ("v > 50 AND v < 60", expected(&|v| v > 50 && v < 60)),
        ("v = 7 OR v = 250", expected(&|v| v == 7 || v == 250)),
    ] {
        for (table, declared) in DECLARATIONS {
            let sql = format!("SELECT id FROM big_{} WHERE {}", table, predicate);
            match sqlx::query(AssertSqlSafe(sql)).fetch_all(&pool).await {
                Ok(rows) if rows.len() as i64 == want => {}
                Ok(rows) => wrong.push(format!("{declared:14} {predicate}: {} rows, want {}", rows.len(), want)),
                Err(e) => wrong.push(format!("{declared:14} {predicate}: error {e}")),
            }
        }
    }
    assert!(wrong.is_empty(), "{} wrong answers:\n{}", wrong.len(), wrong.join("\n"));
}

/// `UPDATE … SET v = <expression>` computes at the engine scale and must store
/// at the column's own, and `RETURNING` must report what was stored.
#[tokio::test(flavor = "multi_thread")]
async fn an_update_expression_stores_at_the_declared_scale() {
    let (_s, pool) = seeded().await;
    for (table, declared) in DECLARATIONS {
        let returned: Decimal = sqlx::query(AssertSqlSafe(format!(
            "UPDATE {} SET v = v + 1 WHERE id = 1 RETURNING v",
            table
        )))
        .fetch_one(&pool)
        .await
        .unwrap_or_else(|e| panic!("UPDATE {} failed: {}", declared, e))
        .get(0);
        assert_eq!(returned, Decimal::from(101), "RETURNING over {}", declared);

        let stored: Decimal = sqlx::query(AssertSqlSafe(format!("SELECT v FROM {} WHERE id = 1", table)))
            .fetch_one(&pool)
            .await
            .expect("select")
            .get(0);
        assert_eq!(stored, Decimal::from(101), "stored over {}", declared);

        sqlx::query(AssertSqlSafe(format!("UPDATE {} SET v = v * 2 WHERE id = 2", table)))
            .execute(&pool)
            .await
            .unwrap_or_else(|e| panic!("UPDATE {} failed: {}", declared, e));
        let doubled: Decimal = sqlx::query(AssertSqlSafe(format!("SELECT v FROM {} WHERE id = 2", table)))
            .fetch_one(&pool)
            .await
            .expect("select")
            .get(0);
        assert_eq!(doubled, Decimal::from(500), "v * 2 stored over {}", declared);
    }
}

/// The seeded production schema uses `NUMERIC(12,4)` throughout, so this is the
/// shape the bug was found in.
#[tokio::test(flavor = "multi_thread")]
async fn the_wide_schema_filters_on_its_decimal_columns() {
    use rasterizeddb_compat::wide_types;
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    wide_types::apply(&pool).await.expect("schema should apply");

    // Subtotals are 100, 250 and 80.
    let mut over = sqlx::query(AssertSqlSafe(
        "SELECT order_number FROM test_ecommerce_orders WHERE subtotal > 90 ORDER BY order_number",
    ))
    .fetch_all(&pool)
    .await
    .expect("select")
    .iter()
    .map(|r| r.get::<Option<String>, _>(0).unwrap_or_default())
    .collect::<Vec<_>>();
    over.sort();
    assert_eq!(over, vec!["ORD-0001", "ORD-0002"]);
}
