//! `GENERATED ALWAYS AS (…) STORED` columns.
//!
//! The clause used to be recognised and discarded: the table was created, the
//! column existed, and every row read NULL for it with nothing reported. A
//! `SUM` over such a column returned nothing and looked like an empty table,
//! which is why the value is asserted here rather than only the absence of an
//! error.

use rasterizeddb_compat::TestServer;
use rust_decimal::Decimal;
use sqlx::{AssertSqlSafe, Row};

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    (server, pool)
}

async fn run(pool: &sqlx::PgPool, sql: &str) -> Result<(), String> {
    sqlx::query(AssertSqlSafe(sql.to_string()))
        .execute(pool)
        .await
        .map(|_| ())
        .map_err(|e| e.to_string().lines().next().unwrap_or("").to_string())
}

/// The schema the tests share: a total computed from three inputs, one of
/// which has a default so an omitted column still reaches the expression.
async fn orders(pool: &sqlx::PgPool) {
    run(
        pool,
        "CREATE TABLE billing (
            id INTEGER NOT NULL,
            subtotal NUMERIC(12,4) NOT NULL,
            tax NUMERIC(12,4) NOT NULL,
            fee NUMERIC(12,4) DEFAULT 1.0000,
            total NUMERIC(12,4) GENERATED ALWAYS AS (subtotal + tax + fee) STORED
        )",
    )
    .await
    .expect("create should succeed");
}

async fn total_of(pool: &sqlx::PgPool, id: i32) -> Option<Decimal> {
    sqlx::query(AssertSqlSafe(format!("SELECT total FROM billing WHERE id = {}", id)))
        .fetch_one(pool)
        .await
        .expect("select should succeed")
        .get(0)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_generated_column_is_computed_from_the_row() {
    let (_s, pool) = server().await;
    orders(&pool).await;

    run(&pool, "INSERT INTO billing (id, subtotal, tax) VALUES (1, 2.5, 3.5)")
        .await
        .expect("insert should succeed");

    // 2.5 + 3.5 + the default fee of 1.
    assert_eq!(total_of(&pool, 1).await, Some(Decimal::new(70, 1)));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_supplied_input_is_used_in_place_of_the_default() {
    let (_s, pool) = server().await;
    orders(&pool).await;

    run(&pool, "INSERT INTO billing (id, subtotal, tax, fee) VALUES (2, 10.0, 20.0, 5.0)")
        .await
        .expect("insert should succeed");

    assert_eq!(total_of(&pool, 2).await, Some(Decimal::new(350, 1)));
}

#[tokio::test(flavor = "multi_thread")]
async fn the_computed_value_is_stored_and_aggregates_over_it() {
    let (_s, pool) = server().await;
    orders(&pool).await;

    run(&pool, "INSERT INTO billing (id, subtotal, tax) VALUES (1, 2.5, 3.5)")
        .await
        .expect("insert");
    run(&pool, "INSERT INTO billing (id, subtotal, tax, fee) VALUES (2, 10.0, 20.0, 5.0)")
        .await
        .expect("insert");

    // `STORED` means the value is a column like any other, so an aggregate
    // reads it without re-evaluating anything. This is the assertion that
    // failed before: the sum was NULL.
    let sum: Decimal = sqlx::query(AssertSqlSafe("SELECT SUM(total) FROM billing"))
        .fetch_one(&pool)
        .await
        .expect("sum should succeed")
        .get(0);
    assert_eq!(sum, Decimal::new(420, 1));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_client_supplied_value_is_refused() {
    let (_s, pool) = server().await;
    orders(&pool).await;

    // PostgreSQL's own error. Accepting the value and overruling it would let a
    // client believe it had set the column.
    let error = run(
        &pool,
        "INSERT INTO billing (id, subtotal, tax, total) VALUES (3, 1.0, 1.0, 99.0)",
    )
    .await
    .expect_err("a value for a generated column should be refused");
    assert!(error.contains("generated column"), "unexpected error: {}", error);
}

#[tokio::test(flavor = "multi_thread")]
async fn updating_an_input_recomputes_the_total() {
    let (_s, pool) = server().await;
    orders(&pool).await;
    run(&pool, "INSERT INTO billing (id, subtotal, tax) VALUES (1, 2.5, 3.5)")
        .await
        .expect("insert");

    // This used to be refused, since the UPDATE could not recompute. PG
    // recomputes: 100 + 3.5 + the default fee of 1.
    run(&pool, "UPDATE billing SET subtotal = 100.0 WHERE id = 1")
        .await
        .expect("updating an input of a generated column recomputes it");
    assert_eq!(total_of(&pool, 1).await, Some(Decimal::new(1045, 1)));

    // Assigning the generated column itself is still refused, as in PG, and
    // with its SQLSTATE.
    let error = sqlx::query(AssertSqlSafe("UPDATE billing SET total = 5 WHERE id = 1"))
        .execute(&pool)
        .await
        .expect_err("a generated column cannot be assigned");
    let database_error = error.as_database_error().expect("a database error");
    assert!(
        database_error.message().contains("can only be updated to DEFAULT"),
        "unexpected error: {}",
        database_error.message()
    );
    assert_eq!(database_error.code().as_deref(), Some("428C9"));
    assert_eq!(total_of(&pool, 1).await, Some(Decimal::new(1045, 1)));
}

#[tokio::test(flavor = "multi_thread")]
async fn updating_a_column_the_expression_does_not_name_still_works() {
    let (_s, pool) = server().await;
    run(
        &pool,
        "CREATE TABLE notes (
            id INTEGER NOT NULL,
            a NUMERIC(12,4) NOT NULL,
            b NUMERIC(12,4) NOT NULL,
            memo TEXT,
            total NUMERIC(12,4) GENERATED ALWAYS AS (a + b) STORED
        )",
    )
    .await
    .expect("create");
    run(&pool, "INSERT INTO notes (id, a, b, memo) VALUES (1, 2.0, 3.0, 'before')")
        .await
        .expect("insert");

    run(&pool, "UPDATE notes SET memo = 'after' WHERE id = 1")
        .await
        .expect("a column the expression does not name is not an input");

    let row = sqlx::query(AssertSqlSafe("SELECT memo, total FROM notes WHERE id = 1"))
        .fetch_one(&pool)
        .await
        .expect("select");
    assert_eq!(row.get::<Option<String>, _>(0).as_deref(), Some("after"));
    assert_eq!(row.get::<Option<Decimal>, _>(1), Some(Decimal::new(50, 1)));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_generated_column_survives_adding_another_column() {
    let (_s, pool) = server().await;
    orders(&pool).await;
    run(&pool, "INSERT INTO billing (id, subtotal, tax) VALUES (1, 2.5, 3.5)")
        .await
        .expect("insert");

    // `ADD COLUMN` rewrites the table. A generation expression lost in the
    // rewrite would leave the column present and permanently NULL from then on.
    run(&pool, "ALTER TABLE billing ADD COLUMN note TEXT").await.expect("add column");
    run(&pool, "INSERT INTO billing (id, subtotal, tax) VALUES (2, 1.0, 1.0)")
        .await
        .expect("insert after the rewrite");

    assert_eq!(total_of(&pool, 1).await, Some(Decimal::new(70, 1)), "the existing row");
    assert_eq!(total_of(&pool, 2).await, Some(Decimal::new(30, 1)), "a row written after");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_null_input_makes_the_whole_expression_null() {
    let (_s, pool) = server().await;
    run(
        &pool,
        "CREATE TABLE partial (
            id INTEGER NOT NULL,
            a NUMERIC(12,4),
            b NUMERIC(12,4) NOT NULL,
            total NUMERIC(12,4) GENERATED ALWAYS AS (a + b) STORED
        )",
    )
    .await
    .expect("create");

    // As in PostgreSQL: NULL propagates rather than being treated as zero,
    // which would report a total that looks right and is not.
    run(&pool, "INSERT INTO partial (id, b) VALUES (1, 5.0)").await.expect("insert");
    let total: Option<Decimal> =
        sqlx::query(AssertSqlSafe("SELECT total FROM partial WHERE id = 1"))
            .fetch_one(&pool)
            .await
            .expect("select")
            .get(0);
    assert_eq!(total, None);
}

async fn fetch(pool: &sqlx::PgPool, sql: &str) -> Vec<sqlx::postgres::PgRow> {
    sqlx::query(AssertSqlSafe(sql.to_string()))
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| panic!("{} failed: {}", sql, e))
}

/// `(id, tot)` pairs, from a statement whose first two columns are a BIGSERIAL
/// and a NUMERIC, sorted by id because RETURNING's order is unspecified.
async fn id_and_total(pool: &sqlx::PgPool, sql: &str) -> Vec<(i64, Option<Decimal>)> {
    let mut out: Vec<(i64, Option<Decimal>)> = fetch(pool, sql)
        .await
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    out.sort_by_key(|(id, _)| *id);
    out
}

fn dec(units: i64, scale: u32) -> Option<Decimal> {
    Some(Decimal::new(units, scale))
}

fn s(text: &str) -> Option<String> {
    Some(text.to_string())
}

/// The table of bug #8. Each test names its own table: a sequence is keyed by
/// name across every server in the process, so two tests sharing a name would
/// see each other's ids.
async fn serial_table(pool: &sqlx::PgPool, name: &str) {
    run(
        pool,
        &format!(
            "CREATE TABLE {} (id BIGSERIAL PRIMARY KEY, q INT, p NUMERIC(10,2),
                tot NUMERIC(12,2) GENERATED ALWAYS AS (q * p) STORED)",
            name
        ),
    )
    .await
    .expect("create should succeed");
}

#[tokio::test(flavor = "multi_thread")]
async fn returning_reports_the_row_and_the_serial_advances_once() {
    let (_s, pool) = server().await;
    serial_table(&pool, "gc_ret").await;

    // PostgreSQL 17's answers. This returned no rows while the tag said
    // INSERT 0 1, and the ids came out 2 and 4: the default was evaluated once
    // to compute the generated value and again to write the row.
    assert_eq!(
        id_and_total(&pool, "INSERT INTO gc_ret (q, p) VALUES (2, 1.5) RETURNING id, tot").await,
        vec![(1, dec(300, 2))]
    );
    let row = fetch(&pool, "INSERT INTO gc_ret (q, p) VALUES (3, 2.25) RETURNING *").await;
    assert_eq!(row.len(), 1);
    assert_eq!(row[0].get::<i64, _>(0), 2);
    assert_eq!(row[0].get::<Option<i32>, _>(1), Some(3));
    assert_eq!(row[0].get::<Option<Decimal>, _>(2), dec(225, 2));
    assert_eq!(row[0].get::<Option<Decimal>, _>(3), dec(675, 2));

    // The simple query protocol, which psql uses and the report came from,
    // takes its own path to the same statement.
    let simple = sqlx::raw_sql(AssertSqlSafe(
        "INSERT INTO gc_ret (q, p) VALUES (4, 1.00) RETURNING id, tot",
    ))
    .fetch_all(&pool)
    .await
    .expect("simple-protocol insert");
    assert_eq!(simple.len(), 1);
    assert_eq!(simple[0].get::<i64, _>(0), 3);
    assert_eq!(simple[0].get::<Option<Decimal>, _>(1), dec(400, 2));

    // Exactly one row per insert, with the id RETURNING reported.
    assert_eq!(
        id_and_total(&pool, "SELECT id, tot FROM gc_ret").await,
        vec![(1, dec(300, 2)), (2, dec(675, 2)), (3, dec(400, 2))]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_generated_column_may_read_a_serial_column() {
    let (_s, pool) = server().await;
    run(
        &pool,
        "CREATE TABLE gc_ser (id BIGSERIAL PRIMARY KEY, q INT,
            tag BIGINT GENERATED ALWAYS AS (id * 10 + q) STORED)",
    )
    .await
    .expect("create");

    // The id the expression reads has to be the id the row is written with.
    let row = fetch(&pool, "INSERT INTO gc_ser (q) VALUES (1) RETURNING id, q, tag").await;
    assert_eq!(row.len(), 1);
    assert_eq!(row[0].get::<i64, _>(0), 1);
    assert_eq!(row[0].get::<Option<i64>, _>(2), Some(11));
    let mut pairs: Vec<(i64, Option<i64>)> =
        fetch(&pool, "INSERT INTO gc_ser (q) VALUES (2), (3) RETURNING id, tag")
            .await
            .iter()
            .map(|r| (r.get(0), r.get(1)))
            .collect();
    pairs.sort();
    assert_eq!(pairs, vec![(2, Some(22)), (3, Some(33))]);

    let row = fetch(&pool, "UPDATE gc_ser SET q = 7 WHERE id = 2 RETURNING id, q, tag").await;
    assert_eq!(row.len(), 1);
    assert_eq!(row[0].get::<Option<i64>, _>(2), Some(27));

    let mut all: Vec<(i64, Option<i32>, Option<i64>)> =
        fetch(&pool, "SELECT id, q, tag FROM gc_ser")
            .await
            .iter()
            .map(|r| (r.get(0), r.get(1), r.get(2)))
            .collect();
    all.sort();
    assert_eq!(
        all,
        vec![(1, Some(1), Some(11)), (2, Some(7), Some(27)), (3, Some(3), Some(33))]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn multi_row_insert_and_insert_select_compute_every_row() {
    let (_s, pool) = server().await;
    serial_table(&pool, "gc_multi").await;
    run(&pool, "INSERT INTO gc_multi (q, p) VALUES (2, 1.5)").await.expect("insert");
    run(&pool, "INSERT INTO gc_multi (q, p) VALUES (3, 2.25)").await.expect("insert");

    assert_eq!(
        id_and_total(
            &pool,
            "INSERT INTO gc_multi (q, p) VALUES (4, 1.00), (5, 2.00) RETURNING id, tot"
        )
        .await,
        vec![(3, dec(400, 2)), (4, dec(1000, 2))]
    );
    // PG gives ids 5 and 6 to the two copied rows, in the order it reads them.
    let mut copied: Vec<Option<Decimal>> = id_and_total(
        &pool,
        "INSERT INTO gc_multi (q, p) SELECT q + 10, p FROM gc_multi WHERE id <= 2 \
         RETURNING id, tot",
    )
    .await
    .into_iter()
    .map(|(_, tot)| tot)
    .collect();
    copied.sort();
    assert_eq!(copied, vec![dec(1800, 2), dec(2925, 2)]);

    let all = id_and_total(&pool, "SELECT id, tot FROM gc_multi").await;
    assert_eq!(all.iter().map(|(id, _)| *id).collect::<Vec<_>>(), vec![1, 2, 3, 4, 5, 6]);
    let mut totals: Vec<Option<Decimal>> = all.into_iter().map(|(_, t)| t).collect();
    totals[4..].sort();
    assert_eq!(
        totals,
        vec![dec(300, 2), dec(675, 2), dec(400, 2), dec(1000, 2), dec(1800, 2), dec(2925, 2)]
    );
}

type KeyRow = (String, Option<i32>, Option<i32>);

fn key_rows(rows: Vec<sqlx::postgres::PgRow>) -> Vec<KeyRow> {
    let mut v: Vec<KeyRow> = rows.iter().map(|r| (r.get(0), r.get(1), r.get(2))).collect();
    v.sort();
    v
}

fn key(k: &str, n: i32, d: i32) -> KeyRow {
    (k.to_string(), Some(n), Some(d))
}

#[tokio::test(flavor = "multi_thread")]
async fn on_conflict_on_a_table_with_a_generated_column() {
    let (_s, pool) = server().await;
    run(
        &pool,
        "CREATE TABLE gc_conf (k TEXT PRIMARY KEY, n INT,
            d INT GENERATED ALWAYS AS (n * 2) STORED)",
    )
    .await
    .expect("create");

    assert_eq!(
        key_rows(fetch(&pool, "INSERT INTO gc_conf (k, n) VALUES ('a', 1) RETURNING k, n, d").await),
        vec![key("a", 1, 2)]
    );
    // DO NOTHING returns no row and leaves the stored one alone.
    assert!(
        fetch(
            &pool,
            "INSERT INTO gc_conf (k, n) VALUES ('a', 5) ON CONFLICT (k) DO NOTHING RETURNING k, n, d"
        )
        .await
        .is_empty()
    );
    run(&pool, "INSERT INTO gc_conf (k, n) VALUES ('a', 5) ON CONFLICT DO NOTHING")
        .await
        .expect("do nothing");
    assert_eq!(key_rows(fetch(&pool, "SELECT k, n, d FROM gc_conf").await), vec![key("a", 1, 2)]);

    // DO UPDATE recomputes from the row as updated.
    assert_eq!(
        key_rows(
            fetch(
                &pool,
                "INSERT INTO gc_conf (k, n) VALUES ('a', 5) \
                 ON CONFLICT (k) DO UPDATE SET n = EXCLUDED.n RETURNING k, n, d"
            )
            .await
        ),
        vec![key("a", 5, 10)]
    );
    assert_eq!(
        key_rows(
            fetch(
                &pool,
                "INSERT INTO gc_conf (k, n) VALUES ('a', 9) \
                 ON CONFLICT (k) DO UPDATE SET n = gc_conf.n + 1 RETURNING k, n, d"
            )
            .await
        ),
        vec![key("a", 6, 12)]
    );
    assert_eq!(
        key_rows(
            fetch(
                &pool,
                "INSERT INTO gc_conf (k, n) VALUES ('b', 4) \
                 ON CONFLICT (k) DO UPDATE SET n = EXCLUDED.n RETURNING k, n, d"
            )
            .await
        ),
        vec![key("b", 4, 8)]
    );
    assert_eq!(
        key_rows(fetch(&pool, "SELECT k, n, d FROM gc_conf").await),
        vec![key("a", 6, 12), key("b", 4, 8)]
    );

    // An UPDATE over several rows recomputes each from its own inputs.
    assert_eq!(
        key_rows(fetch(&pool, "UPDATE gc_conf SET n = n + 10 RETURNING k, n, d").await),
        vec![key("a", 16, 32), key("b", 14, 28)]
    );
    // `DEFAULT` is the one value PG lets a generated column be assigned.
    assert_eq!(
        key_rows(
            fetch(&pool, "UPDATE gc_conf SET n = 3, d = DEFAULT WHERE k = 'a' RETURNING k, n, d")
                .await
        ),
        vec![key("a", 3, 6)]
    );
    assert_eq!(
        key_rows(fetch(&pool, "SELECT k, n, d FROM gc_conf").await),
        vec![key("a", 3, 6), key("b", 14, 28)]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn update_recomputes_and_returns_the_new_value() {
    let (_s, pool) = server().await;
    serial_table(&pool, "gc_upd").await;
    for (q, p) in [(2, "1.5"), (3, "2.25"), (4, "1.00"), (5, "2.00")] {
        run(&pool, &format!("INSERT INTO gc_upd (q, p) VALUES ({}, {})", q, p))
            .await
            .expect("insert");
    }

    // A constant, an expression over the column itself, and NULL.
    let row = fetch(&pool, "UPDATE gc_upd SET q = 10 WHERE id = 3 RETURNING id, q, p, tot").await;
    assert_eq!(row.len(), 1);
    assert_eq!(row[0].get::<Option<Decimal>, _>(3), dec(1000, 2));
    assert_eq!(
        id_and_total(&pool, "UPDATE gc_upd SET p = p * 2 WHERE id = 4 RETURNING id, tot").await,
        vec![(4, dec(2000, 2))]
    );
    assert_eq!(
        id_and_total(&pool, "UPDATE gc_upd SET q = NULL WHERE id = 1 RETURNING id, tot").await,
        vec![(1, None)]
    );

    // PG: 1 NULL, 2 6.75, 3 10.00, 4 20.00.
    assert_eq!(
        id_and_total(&pool, "SELECT id, tot FROM gc_upd").await,
        vec![(1, None), (2, dec(675, 2)), (3, dec(1000, 2)), (4, dec(2000, 2))]
    );
}

/// One row of the expression table after its id.
type ExprRow = (
    Option<bool>,
    Option<bool>,
    Option<bool>,
    Option<bool>,
    Option<bool>,
    Option<bool>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<i32>,
    Option<i32>,
);

const EXPR_COLUMNS: &str = "id, b, band, bor, bnot, bnull, bnn, c, co, cat, ar, ar2";

async fn expression_rows(pool: &sqlx::PgPool, sql: &str) -> Vec<(i32, ExprRow)> {
    let mut out: Vec<(i32, ExprRow)> = fetch(pool, sql)
        .await
        .iter()
        .map(|r| {
            (
                r.get(0),
                (
                    r.get(1),
                    r.get(2),
                    r.get(3),
                    r.get(4),
                    r.get(5),
                    r.get(6),
                    r.get(7),
                    r.get(8),
                    r.get(9),
                    r.get(10),
                    r.get(11),
                ),
            )
        })
        .collect();
    out.sort_by_key(|(id, _)| *id);
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn boolean_and_other_expression_kinds_match_postgres() {
    let (_s, pool) = server().await;
    // With the first of these columns every insert failed, with "'(7) > 5' is
    // neither a column nor a value", although CREATE TABLE had accepted it.
    run(
        &pool,
        "CREATE TABLE gc_expr (id INT, q INT, r INT, s TEXT, t TEXT,
            b BOOL GENERATED ALWAYS AS (q > 5) STORED,
            band BOOL GENERATED ALWAYS AS (q > 1 AND r < 10) STORED,
            bor BOOL GENERATED ALWAYS AS (q > 100 OR r = 3) STORED,
            bnot BOOL GENERATED ALWAYS AS (NOT (q = r)) STORED,
            bnull BOOL GENERATED ALWAYS AS (s IS NULL) STORED,
            bnn BOOL GENERATED ALWAYS AS (s IS NOT NULL) STORED,
            c TEXT GENERATED ALWAYS AS (CASE WHEN q > 5 THEN 'big' WHEN q > 0 THEN 'small' ELSE 'none' END) STORED,
            co TEXT GENERATED ALWAYS AS (COALESCE(s, t, 'dflt')) STORED,
            cat TEXT GENERATED ALWAYS AS (s || '-' || t) STORED,
            ar INT GENERATED ALWAYS AS (q * 10 + r - 1) STORED,
            ar2 INT GENERATED ALWAYS AS ((q + r) * (q - r)) STORED)",
    )
    .await
    .expect("create");

    for values in [
        "1, 7, 3, 'x', 'y'",
        "2, 2, 2, NULL, 'y'",
        "3, NULL, 3, NULL, NULL",
        "4, 0, NULL, 'x', NULL",
        "5, 101, 50, 'a''b', 'c'",
    ] {
        run(&pool, &format!("INSERT INTO gc_expr (id, q, r, s, t) VALUES ({})", values))
            .await
            .unwrap_or_else(|e| panic!("insert ({}) failed: {}", values, e));
    }

    // PostgreSQL 17's rows, with its three-valued answers for NULL inputs:
    // `NULL > 100 OR 3 = 3` is true, `0 > 1 AND NULL < 10` is false, and
    // COALESCE and IS NULL are not NULL merely because an input is.
    let expected: Vec<(i32, ExprRow)> = vec![
        (1, (Some(true), Some(true), Some(true), Some(true), Some(false), Some(true), s("big"), s("x"), s("x-y"), Some(72), Some(40))),
        (2, (Some(false), Some(true), Some(false), Some(false), Some(true), Some(false), s("small"), s("y"), None, Some(21), Some(0))),
        (3, (None, None, Some(true), None, Some(true), Some(false), s("none"), s("dflt"), None, None, None)),
        (4, (Some(false), Some(false), None, None, Some(false), Some(true), s("none"), s("x"), None, None, None)),
        (5, (Some(true), Some(false), Some(true), Some(true), Some(false), Some(true), s("big"), s("a'b"), s("a'b-c"), Some(1059), Some(7701))),
    ];
    assert_eq!(
        expression_rows(&pool, &format!("SELECT {} FROM gc_expr", EXPR_COLUMNS)).await,
        expected
    );

    // The boolean is a real stored boolean a predicate can read.
    let count: i64 = fetch(&pool, "SELECT count(*) FROM gc_expr WHERE b = true").await[0].get(0);
    assert_eq!(count, 2);

    let row = fetch(&pool, "INSERT INTO gc_expr (id, q, r) VALUES (6, 8, 1) RETURNING b, c, ar").await;
    assert_eq!(row.len(), 1);
    assert_eq!(row[0].get::<Option<bool>, _>(0), Some(true));
    assert_eq!(row[0].get::<Option<String>, _>(1), s("big"));
    assert_eq!(row[0].get::<Option<i32>, _>(2), Some(80));

    // An UPDATE recomputes every kind, from the row as updated.
    let row = fetch(&pool, "UPDATE gc_expr SET q = 1 WHERE id = 1 RETURNING b, band, c, ar, ar2").await;
    assert_eq!(row.len(), 1);
    assert_eq!(row[0].get::<Option<bool>, _>(0), Some(false));
    assert_eq!(row[0].get::<Option<bool>, _>(1), Some(false));
    assert_eq!(row[0].get::<Option<String>, _>(2), s("small"));
    assert_eq!(row[0].get::<Option<i32>, _>(3), Some(12));
    assert_eq!(row[0].get::<Option<i32>, _>(4), Some(-8));
    let row = fetch(&pool, "UPDATE gc_expr SET s = NULL, t = 'z' WHERE id = 1 RETURNING bnull, co, cat").await;
    assert_eq!(row.len(), 1);
    assert_eq!(row[0].get::<Option<bool>, _>(0), Some(true));
    assert_eq!(row[0].get::<Option<String>, _>(1), s("z"));
    assert_eq!(row[0].get::<Option<String>, _>(2), None);

    assert_eq!(
        expression_rows(&pool, &format!("SELECT {} FROM gc_expr WHERE id = 1", EXPR_COLUMNS)).await,
        vec![(1, (Some(false), Some(false), Some(true), Some(true), Some(true), Some(false), s("small"), s("z"), None, Some(12), Some(-8)))]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn expressions_postgres_refuses_are_refused_at_create_table() {
    let (_s, pool) = server().await;

    // The subquery below reads a table that exists, so the refusal cannot be
    // an accident of a missing one. Flattened like any other subquery, the
    // statement was created with max(a) baked in as a constant.
    run(&pool, "CREATE TABLE gc_x (a INT)").await.expect("create");
    run(&pool, "INSERT INTO gc_x (a) VALUES (4)").await.expect("insert");

    // PostgreSQL 17's refusals and SQLSTATEs, each made before any table
    // exists. Accepted, the first computes from a value that is itself still
    // being computed, and the others store a different answer for the same
    // inputs.
    for (sql, expected, code) in [
        (
            "CREATE TABLE gc_ref (a INT, g1 INT GENERATED ALWAYS AS (a + 1) STORED, \
             g2 INT GENERATED ALWAYS AS (g1 + 1) STORED)",
            "cannot use generated column \"g1\" in column generation expression",
            "42P17",
        ),
        (
            "CREATE TABLE gc_now (a INT, g TIMESTAMPTZ GENERATED ALWAYS AS (now()) STORED)",
            "generation expression is not immutable",
            "42P17",
        ),
        (
            "CREATE TABLE gc_rand (a INT, g FLOAT8 GENERATED ALWAYS AS (a + random()) STORED)",
            "generation expression is not immutable",
            "42P17",
        ),
        (
            "CREATE TABLE gc_ts (a INT, g TIMESTAMPTZ GENERATED ALWAYS AS (current_timestamp) STORED)",
            "generation expression is not immutable",
            "42P17",
        ),
        (
            "CREATE TABLE gc_uuid (a INT, g TEXT GENERATED ALWAYS AS (gen_random_uuid()::text) STORED)",
            "generation expression is not immutable",
            "42P17",
        ),
        (
            "CREATE TABLE gc_sub1 (a INT, g INT GENERATED ALWAYS AS ((SELECT 1)) STORED)",
            "cannot use subquery in column generation expression",
            "0A000",
        ),
        (
            "CREATE TABLE gc_sub2 (a INT, g INT GENERATED ALWAYS AS (a + (SELECT max(a) FROM gc_x)) STORED)",
            "cannot use subquery in column generation expression",
            "0A000",
        ),
    ] {
        let error = sqlx::query(AssertSqlSafe(sql.to_string()))
            .execute(&pool)
            .await
            .expect_err(&format!("should be refused: {}", sql));
        let database_error = error.as_database_error().expect("a database error");
        assert!(
            database_error.message().contains(expected),
            "{}: unexpected error: {}",
            sql,
            database_error.message()
        );
        assert_eq!(database_error.code().as_deref(), Some(code), "{}", sql);
    }

    // None of them left a table behind.
    for name in ["gc_ref", "gc_now", "gc_rand", "gc_ts", "gc_uuid", "gc_sub1", "gc_sub2"] {
        run(&pool, &format!("CREATE TABLE {} (a INT)", name))
            .await
            .unwrap_or_else(|e| panic!("{} should not exist: {}", name, e));
    }
}
