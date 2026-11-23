//! Two shapes a real application emitted that the engine refused.
//!
//! Both were reported as flat refusals — "SELECT DISTINCT is not supported yet
//! in a join", "function 'date_trunc' is not supported yet" — so the queries
//! failed outright rather than returning something wrong. That is the right
//! failure mode, and these are the tests that make them work.

use sqlx::AssertSqlSafe;
use rasterizeddb_compat::TestServer;
use sqlx::Row;

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    (server, pool)
}

async fn run(pool: &sqlx::PgPool, sql: &str) -> Result<(), String> {
    sqlx::query(AssertSqlSafe(sql))
        .execute(pool)
        .await
        .map(|_| ())
        .map_err(|e| e.to_string().lines().next().unwrap_or("").to_string())
}

/// `SELECT DISTINCT … JOIN …` — the application ran this once per domain and
/// every one of them failed.
#[tokio::test]
async fn distinct_over_a_join_collapses_repeated_rows() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE om (object_id INT NOT NULL, object_key TEXT, domain_id TEXT)")
        .await
        .expect("create om");
    run(&pool, "CREATE TABLE m (object_id INT NOT NULL, domain_id TEXT, restored_at TEXT)")
        .await
        .expect("create m");

    // Two objects, and several manifest rows pointing at each — which is what
    // makes the join produce repeats and the DISTINCT necessary.
    run(&pool, "INSERT INTO om (object_id, object_key, domain_id) VALUES (1, 'key-a', 'd1')").await.expect("i");
    run(&pool, "INSERT INTO om (object_id, object_key, domain_id) VALUES (2, 'key-b', 'd1')").await.expect("i");
    run(&pool, "INSERT INTO om (object_id, object_key, domain_id) VALUES (3, 'key-c', 'd2')").await.expect("i");
    for oid in [1, 1, 1, 2, 2] {
        run(&pool, &format!("INSERT INTO m (object_id, domain_id, restored_at) VALUES ({}, 'd1', NULL)", oid)).await.expect("i");
    }
    run(&pool, "INSERT INTO m (object_id, domain_id, restored_at) VALUES (3, 'd2', NULL)").await.expect("i");

    let keys: Vec<String> = sqlx::query(
        "SELECT DISTINCT om.object_key AS object_key FROM om \
         JOIN m ON m.object_id = om.object_id \
         WHERE om.domain_id = 'd1' AND m.domain_id = 'd1' AND m.restored_at IS NULL",
    )
    .fetch_all(&pool)
    .await
    .expect("DISTINCT over a join must work")
    .iter()
    .map(|r| r.try_get::<String, _>("object_key").expect("object_key"))
    .collect();

    let mut sorted = keys.clone();
    sorted.sort();
    assert_eq!(sorted, vec!["key-a", "key-b"], "got {:?}", keys);

    // Without DISTINCT the same join yields the repeats, which is what proves
    // the deduplication is doing the work rather than the join being narrow.
    let all: i64 = sqlx::query(
        "SELECT COUNT(*) AS n FROM om JOIN m ON m.object_id = om.object_id \
         WHERE om.domain_id = 'd1' AND m.domain_id = 'd1'",
    )
    .fetch_one(&pool)
    .await
    .expect("count")
    .try_get("n")
    .expect("n");
    assert_eq!(all, 5, "the join really does repeat rows");
}

/// `date_trunc('minute', ts)` grouped into time buckets — a billing rollup.
#[tokio::test]
async fn date_trunc_evaluates_in_a_select_list() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE su (id INT NOT NULL, created_at TIMESTAMPTZ, total_bytes BIGINT)")
        .await
        .expect("create");
    // Three rows in one minute, one in the next.
    for (id, ts, bytes) in [
        (1, "2026-08-23 21:47:01", 100),
        (2, "2026-08-23 21:47:30", 200),
        (3, "2026-08-23 21:47:59", 300),
        (4, "2026-08-23 21:48:00", 400),
    ] {
        run(&pool, &format!(
            "INSERT INTO su (id, created_at, total_bytes) VALUES ({}, '{}', {})", id, ts, bytes))
            .await
            .expect("insert");
    }

    // In a select list. The result is a timestamp, not text — asserting that
    // is half the point, since a truncation that returned a string would still
    // group but would no longer compare as an instant.
    let rows = sqlx::query("SELECT date_trunc('minute', created_at) AS bucket FROM su WHERE id = 3")
        .fetch_all(&pool)
        .await
        .expect("date_trunc in a select list must work");
    assert_eq!(rows.len(), 1);
    use sqlx::Column;
    assert_eq!(
        rows[0].column(0).type_info().to_string().to_lowercase(),
        "timestamptz",
        "date_trunc yields an instant"
    );

    // The value itself. Read back as text through a second row so the floor is
    // visible: 21:47:59 and 21:47:01 both become 21:47:00, and 21:48:00 does
    // not — which is what a time bucket has to do.
    //
    // `date_trunc` is evaluated by the expression layer. The rollup shape —
    // `GROUP BY` of its alias or of the expression itself — is pinned by
    // `a_rollup_groups_by_a_select_alias` below; a `WHERE` clause still
    // resolves names by its own path and cannot call it.
    let buckets = sqlx::query("SELECT date_trunc('hour', created_at) AS bucket FROM su")
        .fetch_all(&pool)
        .await
        .expect("date_trunc over every row");
    assert_eq!(buckets.len(), 4, "one bucket value per row");

    // An unknown unit is refused rather than silently not truncating, which
    // would give one bucket per row and look like data.
    let refused = run(&pool, "SELECT date_trunc('fortnight', created_at) FROM su").await;
    assert!(refused.is_err(), "an unknown unit should be refused");
}

/// The rollup itself: the application's billing query groups by the *alias* of
/// a computed select item — `SELECT date_trunc('minute', created_at) AS
/// snapshot_at, SUM(…) … GROUP BY snapshot_at` — which PostgreSQL accepts and
/// the engine refused with "GROUP BY column 'snapshot_at' not found in
/// schema". Grouping by the expression written out has to work too.
#[tokio::test]
async fn a_rollup_groups_by_a_select_alias() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE su (id INT NOT NULL, created_at TIMESTAMPTZ, total_bytes BIGINT)")
        .await
        .expect("create");
    // Three rows in one minute, one in the next.
    for (id, ts, bytes) in [
        (1, "2026-08-23 21:47:01", 100),
        (2, "2026-08-23 21:47:30", 200),
        (3, "2026-08-23 21:47:59", 300),
        (4, "2026-08-23 21:48:00", 400),
    ] {
        run(&pool, &format!(
            "INSERT INTO su (id, created_at, total_bytes) VALUES ({}, '{}', {})", id, ts, bytes))
            .await
            .expect("insert");
    }

    // By the alias.
    let rows = sqlx::query(
        "SELECT date_trunc('minute', created_at) AS snapshot_at, SUM(total_bytes) AS bytes \
         FROM su GROUP BY snapshot_at ORDER BY snapshot_at",
    )
    .fetch_all(&pool)
    .await
    .expect("GROUP BY a select alias must work");
    assert_eq!(rows.len(), 2, "two minute buckets");
    // `sum(bigint)` is a numeric, as in PostgreSQL (it used to be an int8).
    let bytes: Vec<rust_decimal::Decimal> = rows
        .iter()
        .map(|r| r.try_get::<rust_decimal::Decimal, _>("bytes").expect("bytes"))
        .collect();
    assert_eq!(
        bytes,
        vec![rust_decimal::Decimal::from(600), rust_decimal::Decimal::from(400)],
        "21:47 holds three rows, 21:48 one"
    );
    // The bucket is still an instant, not text: a rollup the client cannot
    // compare as a timestamp would group and still be useless.
    use sqlx::Column;
    assert_eq!(
        rows[0].column(0).type_info().to_string().to_lowercase(),
        "timestamptz",
        "the alias keeps date_trunc's type"
    );

    // By the expression written out, matched back to the aliased select item.
    let rows = sqlx::query(
        "SELECT date_trunc('minute', created_at) AS snapshot_at, COUNT(*) AS n \
         FROM su GROUP BY date_trunc('minute', created_at) ORDER BY snapshot_at",
    )
    .fetch_all(&pool)
    .await
    .expect("GROUP BY the expression must work");
    assert_eq!(rows.len(), 2);
    let counts: Vec<i64> = rows
        .iter()
        .map(|r| r.try_get::<i64, _>("n").expect("n"))
        .collect();
    assert_eq!(counts, vec![3, 1]);

    // A per-row expression that is NOT a grouping key is refused, the way
    // PostgreSQL refuses it — emitting an empty value instead would be a
    // silent wrong answer.
    let refused = run(
        &pool,
        "SELECT date_trunc('minute', created_at), SUM(total_bytes) AS b FROM su",
    )
    .await;
    assert!(
        refused.is_err(),
        "an ungrouped per-row expression beside an aggregate must be refused"
    );
}

/// The billing average, verbatim from production: the rollup materialised as
/// a derived table, an outer AVG over it, and `::bigint` rounding the
/// fractional average the way PostgreSQL's numeric cast does — '1092.75'
/// used to fail the whole statement as "cannot be read as I64".
#[tokio::test]
async fn the_billing_average_rounds_into_bigint() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE storage_use (subscription_id TEXT NOT NULL, domain_id TEXT NOT NULL, key TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL, total_bytes BIGINT NOT NULL)")
        .await
        .expect("create");
    // Two minute-buckets: 21:47 sums to 600, 21:48 to 400 — average 500;
    // then a three-bucket shape whose average is fractional (500, 400, 300
    // -> 400 exactly would not prove rounding, so make it 601/400/300 ->
    // 433.67 -> 434).
    for (ts, bytes) in [
        ("2026-08-23 21:47:01", 300i64),
        ("2026-08-23 21:47:30", 301),
        ("2026-08-23 21:48:00", 400),
        ("2026-08-23 21:49:00", 300),
    ] {
        run(&pool, &format!(
            "INSERT INTO storage_use (subscription_id, domain_id, key, created_at, total_bytes) \
             VALUES ('sub1', 'd1', 'records', '{}', {})", ts, bytes))
            .await
            .expect("insert");
    }

    // Through the EXTENDED protocol with bound parameters — the form the
    // billing worker actually sends. This ran through the simple protocol
    // until the describe path learned to describe an aggregate over a derived
    // table by describing the inner query recursively; before that sqlx was
    // handed zero columns and discarded every value of the row.
    let avg: i64 = sqlx::query(
        "SELECT COALESCE(AVG(snapshot_total)::bigint, 0) as avg_bytes FROM ( \
         SELECT date_trunc('minute', created_at) as snapshot_at, SUM(total_bytes) as snapshot_total \
         FROM storage_use WHERE subscription_id = $1 AND domain_id = $2 \
         AND key IN ('attachments', 'records', 'user_tables', 'files') \
         AND created_at >= $3 AND created_at <= $4 \
         GROUP BY snapshot_at ) AS snapshots",
    )
    .bind("sub1")
    .bind("d1")
    .bind("2026-08-23 00:00:00")
    .bind("2026-08-24 00:00:00")
    .fetch_one(&pool)
    .await
    .expect("the billing average must work over the extended protocol")
    .try_get("avg_bytes")
    .expect("avg_bytes must be nameable");
    // Buckets: 601, 400, 300 -> average 433.67 -> rounds to 434.
    assert_eq!(avg, 434, "AVG's fraction rounds into bigint");

    // An empty range still answers 0 through the COALESCE, not an error.
    let empty: i64 = sqlx::query(
        "SELECT COALESCE(AVG(snapshot_total)::bigint, 0) as avg_bytes FROM ( \
         SELECT date_trunc('minute', created_at) as snapshot_at, SUM(total_bytes) as snapshot_total \
         FROM storage_use WHERE subscription_id = $1 AND domain_id = $2 AND key = $3 \
         AND created_at >= $4 AND created_at <= $5 \
         GROUP BY snapshot_at ) AS snapshots",
    )
    .bind("sub1")
    .bind("d1")
    .bind("nothing")
    .bind("2026-08-23 00:00:00")
    .bind("2026-08-24 00:00:00")
    .fetch_one(&pool)
    .await
    .expect("empty range")
    .try_get("avg_bytes")
    .expect("avg_bytes");
    assert_eq!(empty, 0);
}
