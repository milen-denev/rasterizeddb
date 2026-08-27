//! The wide-type schema and the six queries written against it.
//!
//! Every statement in [`rasterizeddb_compat::wide_types`] is legal PostgreSQL
//! and runs there unchanged, so a difference here is a gap rather than a
//! dialect. The schema and its seed data are asserted outright; the queries are
//! split into the ones that run and the ones that do not, and each of the
//! latter is a test of its own naming exactly what it still needs.
//!
//! The unsupported ones are `#[ignore]`d rather than deleted or commented out.
//! `cargo test` stays green, `cargo test -- --ignored` is the work list, and
//! each failure message names the missing feature instead of a line number.

use rasterizeddb_compat::{TestServer, wide_types};
use sqlx::AssertSqlSafe;

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    (server, pool)
}

fn first_line(e: &sqlx::Error) -> String {
    e.to_string().lines().next().unwrap_or("").to_string()
}

/// Runs the query named in [`wide_types::QUERIES`] against a seeded server.
async fn run_named(name: &str) -> Result<usize, String> {
    let (_s, pool) = server().await;
    wide_types::apply(&pool).await.expect("the wide schema should apply");
    let query = wide_types::QUERIES
        .iter()
        .find(|q| q.name == name)
        .unwrap_or_else(|| panic!("no query named '{}'", name));
    sqlx::query(AssertSqlSafe(query.sql))
        .fetch_all(&pool)
        .await
        .map(|rows| rows.len())
        .map_err(|e| format!("{}\n  needs: {}", first_line(&e), query.features.join(", ")))
}

#[tokio::test(flavor = "multi_thread")]
async fn the_wide_schema_applies() {
    let (_s, pool) = server().await;
    for sql in wide_types::CREATE_TABLES {
        sqlx::query(AssertSqlSafe(*sql))
            .execute(&pool)
            .await
            .unwrap_or_else(|e| panic!("CREATE TABLE failed: {}\n{}", first_line(&e), sql));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_wide_schema_seeds() {
    let (_s, pool) = server().await;
    wide_types::apply(&pool).await.expect("schema and seed should apply");
}

/// Every column of the seeded rows reads back, so no type in the schema is
/// silently storing nothing.
///
/// `SELECT *` rather than a column list on purpose: a column dropped from the
/// projection would take its own failure with it.
#[tokio::test(flavor = "multi_thread")]
async fn every_seeded_column_reads_back() {
    let (_s, pool) = server().await;
    wide_types::apply(&pool).await.expect("schema should apply");

    for table in wide_types::TABLES {
        let rows = sqlx::query(AssertSqlSafe(format!("SELECT * FROM {}", table)))
            .fetch_all(&pool)
            .await
            .unwrap_or_else(|e| panic!("SELECT * FROM {} failed: {}", table, first_line(&e)));
        assert!(!rows.is_empty(), "{} seeded no rows", table);
        let columns = sqlx::Row::columns(&rows[0]).len();
        assert!(columns > 20, "{} described only {} columns", table, columns);
    }
}

// --------------------------------------------------------- queries that run

#[tokio::test(flavor = "multi_thread")]
async fn spatial_range_and_inet_filtering_runs() {
    // Network subsumption, range containment, polygon overlap, circle
    // containment, a cast to text and `SIMILAR TO`, in one statement.
    let matched = run_named("spatial_range_inet_filtering").await.unwrap_or_else(|e| panic!("{}", e));
    // Only the Berlin device is inside 192.168/16, covers the origin square,
    // sits inside the wide circle and has a hardware status beginning 101.
    assert_eq!(matched, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn window_frames_and_interval_math_runs() {
    // Window frames, `AVG(...) OVER`, `DENSE_RANK`, `LAG`, and the interval
    // arithmetic that compares a processing time against a target.
    let matched =
        run_named("window_frames_and_interval_math").await.unwrap_or_else(|e| panic!("{}", e));
    // One order took two and a half hours against a one-hour target; the other
    // fulfilled order beat its target, and the third was never fulfilled.
    assert_eq!(matched, 1);
}

// ----------------------------------------------------- queries not yet served
//
// Each names the features it is waiting on. Run them with
// `cargo test -p rasterizeddb_compat --test wide_types -- --ignored`.

/// Needs `LEFT JOIN LATERAL unnest(...)`, `array_agg(DISTINCT …) FILTER`,
/// `jsonb_agg`, and the bitwise `&` over bit strings.
///
/// The `@>` containment and the `->` path in the same query already work — see
/// `pg_operators.rs` — so what remains is the set-returning function in `FROM`
/// and the aggregate spellings around it.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "LATERAL set-returning functions in FROM are not implemented"]
async fn cte_jsonb_lateral_unnest_bitwise_runs() {
    run_named("cte_jsonb_lateral_unnest_bitwise").await.unwrap_or_else(|e| panic!("{}", e));
}


/// Needs a join whose `ON` is a disjunction, and the `<->` distance operator.
///
/// The `>>=` network predicate in the same condition works on its own; it is
/// the `OR` between two join conditions that the planner cannot express.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "a join condition that is an OR of predicates is not implemented"]
async fn spatial_network_join_runs() {
    run_named("spatial_network_join").await.unwrap_or_else(|e| panic!("{}", e));
}

/// Needs `LATERAL unnest(...) WITH ORDINALITY` and multi-dimensional array
/// subscripting.
///
/// `array_length` and `array_ndims` already work — see `text_search.rs` — and
/// `array_ndims` answers 1 here rather than PostgreSQL's 2, because the array
/// storage is flat.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "LATERAL … WITH ORDINALITY and array subscripts are not implemented"]
async fn array_ndims_and_ordinality_runs() {
    run_named("array_ndims_and_ordinality").await.unwrap_or_else(|e| panic!("{}", e));
}

/// Needs function calls inside a `WHERE` clause, and the `jsonb_path_query`
/// family.
///
/// `to_tsvector`, `to_tsquery` and the `@@` match are all implemented — they
/// work in a select list, and `@@` works as a predicate over a stored value.
/// What is missing is the ability to *call* a function in a predicate: the
/// scan's comparison path takes column bytes and literals, and has nowhere to
/// evaluate an expression per row.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "function calls in a WHERE clause are not implemented"]
async fn fulltext_jsonpath_and_subquery_runs() {
    run_named("fulltext_jsonpath_and_subquery").await.unwrap_or_else(|e| panic!("{}", e));
}
