//! What a prepared statement says it returns.
//!
//! The extended protocol asks `Describe` what a statement returns *before*
//! running it. When that cannot be answered the server replies `NoData` and
//! `Execute` then sends `DataRow`s anyway — a protocol violation. sqlx tolerates
//! it far enough to hand back rows with an empty column set, so the values are
//! right and every `try_get("name")` is `ColumnNotFound`. An application reads
//! that as a row whose every field is missing.
//!
//! A catalogue view is materialised into a temp table at execute time, so it
//! has no entry in `database.tables` and a join over one described as returning
//! nothing. Its column list is fixed and known, so the join is planned against
//! that instead — describe still materialises nothing, which is the constraint
//! that matters.

use sqlx::AssertSqlSafe;
use rasterizeddb_compat::TestServer;
use sqlx::{Column, Row};

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    (server, pool)
}

/// The columns a statement's rows actually carry, by name.
async fn described(pool: &sqlx::PgPool, sql: &str) -> Vec<String> {
    let rows = sqlx::query(AssertSqlSafe(sql))
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| panic!("{} failed: {}", sql, e));
    let first = rows.first().unwrap_or_else(|| panic!("{} returned no rows", sql));
    first.columns().iter().map(|c| c.name().to_string()).collect()
}

#[tokio::test]
async fn a_join_over_catalogue_views_announces_its_columns() {
    let (_s, pool) = server().await;
    sqlx::query("CREATE TABLE t (id INT NOT NULL)").execute(&pool).await.expect("create");

    // The shape an application uses to size its relations.
    let names = described(
        &pool,
        "SELECT c.relname AS relname FROM pg_class c \
         JOIN pg_namespace n ON n.oid = c.relnamespace",
    )
    .await;
    assert_eq!(names, vec!["relname"], "the join must announce its columns");

    // And the field is readable by name, which is the thing that was broken.
    let row = sqlx::query(
        "SELECT c.relname AS relname FROM pg_class c \
         JOIN pg_namespace n ON n.oid = c.relnamespace",
    )
    .fetch_all(&pool)
    .await
    .expect("select");
    let _: String = row[0].try_get("relname").expect("readable by name");
}

/// A plain statement and a top-level `UNION ALL` were already described; this
/// pins them so the catalogue-view change cannot regress them.
#[tokio::test]
async fn ordinary_statements_still_announce_their_columns() {
    let (_s, pool) = server().await;
    sqlx::query("CREATE TABLE t (id INT NOT NULL, n BIGINT)").execute(&pool).await.expect("create");
    sqlx::query("INSERT INTO t (id, n) VALUES (1, 5)").execute(&pool).await.expect("insert");

    assert_eq!(described(&pool, "SELECT id FROM t").await, vec!["id"]);
    assert_eq!(
        described(&pool, "SELECT COALESCE(SUM(n), 0) AS total FROM t").await,
        vec!["total"]
    );
    assert_eq!(
        described(&pool, "SELECT n FROM t UNION ALL SELECT n FROM t").await,
        vec!["n"]
    );
}

/// A single-relation query over a catalogue view, which is the other half of
/// the join case above — the view is equally absent from `database.tables`.
#[tokio::test]
async fn a_single_relation_catalogue_query_announces_its_columns() {
    let (_s, pool) = server().await;
    sqlx::query("CREATE TABLE t (id INT NOT NULL)").execute(&pool).await.expect("create");

    assert_eq!(
        described(&pool, "SELECT n.nspname AS nspname FROM pg_namespace n").await,
        vec!["nspname"]
    );
    assert_eq!(described(&pool, "SELECT c.relname FROM pg_class c").await, vec!["relname"]);
    // An aggregate's columns are its select items, not the fields it happens
    // to read. This reported `oid` — the view's first field, because that is
    // what COUNT was scanning — so the count was right and its name was another
    // column's.
    assert_eq!(described(&pool, "SELECT COUNT(*) AS n FROM pg_class c").await, vec!["n"]);
    assert_eq!(
        described(&pool, "SELECT COALESCE(SUM(c.oid), 0) AS total FROM pg_class c").await,
        vec!["total"]
    );
    assert_eq!(
        described(&pool, "SELECT c.oid, c.relname FROM pg_class c").await,
        vec!["oid", "relname"]
    );
}

/// An aggregate over a derived table — the shape every paginated rollup uses.
///
/// Only `COUNT` was answered before, because its type is fixed regardless of
/// the inner relation; everything else handed the client *no columns*. The rows
/// were right the whole time, so a driver read correct values under no
/// resolvable names and every `try_get` failed. The alias's schema now comes
/// from describing the inner query — the same question asked one level down —
/// and nothing is materialised, which is the constraint describe must respect.
#[tokio::test]
async fn an_aggregate_over_a_derived_table_announces_its_columns() {
    let (_s, pool) = server().await;
    sqlx::query("CREATE TABLE su (id INT NOT NULL, k TEXT, total_bytes BIGINT)")
        .execute(&pool)
        .await
        .expect("create");
    for i in 1..=3 {
        sqlx::query(AssertSqlSafe(format!(
            "INSERT INTO su (id, k, total_bytes) VALUES ({}, 'a', {})",
            i,
            i * 100
        )))
        .execute(&pool)
        .await
        .expect("insert");
    }

    // Names, for each shape the application emits.
    assert_eq!(
        described(&pool, "SELECT COUNT(*) AS n FROM (SELECT id FROM su) AS q").await,
        vec!["n"]
    );
    assert_eq!(
        described(&pool, "SELECT COALESCE(SUM(q.total_bytes), 0) AS n FROM (SELECT total_bytes FROM su) AS q").await,
        vec!["n"]
    );
    assert_eq!(
        described(&pool, "SELECT q.total_bytes FROM (SELECT total_bytes FROM su) AS q").await,
        vec!["total_bytes"]
    );

    // The billing rollup: an aggregate with a cast over a *grouped* derived
    // table. This one fired about ten times a minute and failed every time.
    assert_eq!(
        described(
            &pool,
            "SELECT COALESCE(AVG(q.t)::bigint, 0) AS avg_bytes \
             FROM (SELECT SUM(total_bytes) AS t FROM su GROUP BY k) AS q"
        )
        .await,
        vec!["avg_bytes"]
    );

    // A `UNION ALL` inside the derived table, which is how the storage rollup
    // sums across five relations.
    assert_eq!(
        described(
            &pool,
            "SELECT COALESCE(SUM(p.total_bytes), 0) AS total_bytes \
             FROM (SELECT total_bytes FROM su UNION ALL SELECT total_bytes FROM su) p"
        )
        .await,
        vec!["total_bytes"]
    );

    // And the values, because a description that does not match the rows it
    // precedes is the bug in a different costume.
    // `sum(bigint)` is a numeric, as in PostgreSQL (it used to be an int8).
    let n: rust_decimal::Decimal = sqlx::query("SELECT COALESCE(SUM(q.total_bytes), 0) AS n FROM (SELECT total_bytes FROM su) AS q")
        .fetch_one(&pool)
        .await
        .expect("select")
        .try_get("n")
        .expect("readable by name");
    assert_eq!(n, rust_decimal::Decimal::from(600));

    let avg: i64 = sqlx::query(
        "SELECT COALESCE(AVG(q.t)::bigint, 0) AS avg_bytes \
         FROM (SELECT SUM(total_bytes) AS t FROM su GROUP BY k) AS q",
    )
    .fetch_one(&pool)
    .await
    .expect("billing rollup")
    .try_get("avg_bytes")
    .expect("avg_bytes readable by name");
    assert_eq!(avg, 600, "one group summing 100+200+300");
}
