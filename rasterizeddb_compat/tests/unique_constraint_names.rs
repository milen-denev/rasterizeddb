//! `DROP CONSTRAINT <table>_<column>_key` drops that column's inline UNIQUE.
//!
//! The name was matched by suffix and the first column it fit was taken:
//! `ALTER TABLE sx DROP CONSTRAINT sx_user_id_key` dropped `id`'s uniqueness,
//! duplicate ids went in, and `user_id` stayed unique. And two long column
//! names that agree up to where a name is cut derive one constraint name.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor};

fn code(e: &sqlx::Error) -> String {
    e.as_database_error().and_then(|d| d.code()).map(|c| c.to_string()).unwrap_or_else(|| e.to_string())
}

async fn ok(pool: &sqlx::PgPool, sql: &str) {
    pool.execute(sqlx::raw_sql(AssertSqlSafe(sql.to_string()))).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
}

async fn refused(pool: &sqlx::PgPool, sql: &str, want: &str) {
    let e = pool.execute(AssertSqlSafe(sql.to_string())).await.expect_err(sql);
    assert_eq!(code(&e), want, "{sql}: {e}");
}

#[tokio::test(flavor = "multi_thread")]
async fn drop_constraint_drops_the_column_it_names() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    ok(&pool, "CREATE TABLE sx (id BIGINT UNIQUE, user_id BIGINT UNIQUE); INSERT INTO sx VALUES (1, 1), (2, 2)").await;
    ok(&pool, "ALTER TABLE sx DROP CONSTRAINT sx_user_id_key").await;
    refused(&pool, "INSERT INTO sx VALUES (1, 3)", "23505").await;
    ok(&pool, "INSERT INTO sx VALUES (3, 1)").await;
    // And the other one by its own name.
    ok(&pool, "ALTER TABLE sx DROP CONSTRAINT sx_id_key").await;
    ok(&pool, "INSERT INTO sx VALUES (1, 4)").await;
    // A renamed table's inline UNIQUE answers to the name the table derives
    // now; the old table's name (PostgreSQL's, which keeps it) is 42704.
    ok(&pool, "CREATE TABLE ry (id BIGINT UNIQUE, user_id BIGINT UNIQUE); ALTER TABLE ry RENAME TO rz").await;
    refused(&pool, "ALTER TABLE rz DROP CONSTRAINT ry_user_id_key", "42704").await;
    ok(&pool, "ALTER TABLE rz DROP CONSTRAINT rz_user_id_key").await;
    ok(&pool, "INSERT INTO rz VALUES (1, 1), (2, 1)").await;
    refused(&pool, "INSERT INTO rz VALUES (1, 9)", "23505").await;
}

/// A name the table does not derive is never matched by its tail. On
/// `order_items (id, item_id)` renamed from `order_item`, `order_item_id_key`
/// is `id`'s name in PostgreSQL and ends like `item_id`'s here: the longest
/// column that fit was dropped, `item_id`, and `bogus_id_key` dropped `id`.
#[tokio::test(flavor = "multi_thread")]
async fn a_name_the_table_does_not_derive_drops_nothing() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    let both_unique = |pool: &sqlx::PgPool, t: &'static str| {
        let pool = pool.clone();
        async move {
            refused(&pool, &format!("INSERT INTO {t} VALUES (1, 99)"), "23505").await;
            refused(&pool, &format!("INSERT INTO {t} VALUES (99, 1)"), "23505").await;
        }
    };
    ok(&pool, "CREATE TABLE order_item (id BIGINT UNIQUE, item_id BIGINT UNIQUE); INSERT INTO order_item VALUES (1, 1)").await;
    ok(&pool, "ALTER TABLE order_item RENAME TO order_items").await;
    refused(&pool, "ALTER TABLE order_items DROP CONSTRAINT order_item_id_key", "42704").await;
    refused(&pool, "ALTER TABLE order_items DROP CONSTRAINT bogus_id_key", "42704").await;
    refused(&pool, "ALTER TABLE order_items DROP CONSTRAINT bogus_item_id_key", "42704").await;
    both_unique(&pool, "order_items").await;
    // Each by the name the table derives, and only that column.
    ok(&pool, "ALTER TABLE order_items DROP CONSTRAINT order_items_item_id_key").await;
    ok(&pool, "INSERT INTO order_items VALUES (2, 1)").await;
    refused(&pool, "INSERT INTO order_items VALUES (1, 98)", "23505").await;

    // The other way round: `item_id`'s old name ends like nothing else, and
    // `id`'s old name ends like `item_id`'s.
    ok(&pool, "CREATE TABLE line_item (item_id BIGINT UNIQUE, id BIGINT UNIQUE); INSERT INTO line_item VALUES (1, 1)").await;
    ok(&pool, "ALTER TABLE line_item RENAME TO line_items").await;
    refused(&pool, "ALTER TABLE line_items DROP CONSTRAINT line_item_item_id_key", "42704").await;
    refused(&pool, "ALTER TABLE line_items DROP CONSTRAINT line_item_id_key", "42704").await;
    both_unique(&pool, "line_items").await;
    ok(&pool, "ALTER TABLE line_items DROP CONSTRAINT line_items_id_key").await;
    ok(&pool, "INSERT INTO line_items VALUES (2, 1)").await;
    refused(&pool, "INSERT INTO line_items VALUES (1, 97)", "23505").await;
    // IF EXISTS on a name that is not derived is a no-op, not a guess.
    ok(&pool, "ALTER TABLE line_items DROP CONSTRAINT IF EXISTS line_item_item_id_key").await;
    refused(&pool, "INSERT INTO line_items VALUES (1, 50)", "23505").await;
}

/// Two columns whose implicit names cut alike are refused at CREATE, plain
/// and partitioned: a DROP could not tell their constraints apart.
#[tokio::test(flavor = "multi_thread")]
async fn two_inline_unique_names_that_cut_alike_are_refused() {
    let dir = std::env::temp_dir().join(format!("unique_constraint_names_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    {
        let server = TestServer::start_file_backed(dir.to_str().unwrap()).await.expect("server");
        let pool = server.connect_sqlx().await.expect("connect");
        let stem = "c".repeat(120);
        let (a, b) = (format!("{stem}_a"), format!("{stem}_b"));
        refused(&pool, &format!("CREATE TABLE ln (id BIGINT, {a} BIGINT UNIQUE, {b} BIGINT UNIQUE)"), "0A000").await;
        refused(
            &pool,
            &format!(
                "CREATE TABLE lp (tenant BIGINT NOT NULL, {a} BIGINT UNIQUE, {b} BIGINT UNIQUE) PARTITION BY HASH (tenant) PARTITIONS 2"
            ),
            "0A000",
        )
        .await;
        // Distinct when cut: accepted, and each dropped by its own name.
        ok(&pool, "CREATE TABLE ok2 (id BIGINT UNIQUE, user_id BIGINT UNIQUE)").await;
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// On a partitioned table, the key column's inline UNIQUE is dropped by its
/// own name.
#[tokio::test(flavor = "multi_thread")]
async fn drop_constraint_names_the_column_on_a_partitioned_table() {
    let dir = std::env::temp_dir().join(format!("unique_constraint_names_p_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    {
        let server = TestServer::start_file_backed(dir.to_str().unwrap()).await.expect("server");
        let pool = server.connect_sqlx().await.expect("connect");
        ok(&pool, "CREATE TABLE ps (tenant_id BIGINT NOT NULL UNIQUE, v BIGINT) PARTITION BY HASH (tenant_id) PARTITIONS 2").await;
        ok(&pool, "INSERT INTO ps VALUES (1, 1), (2, 2)").await;
        refused(&pool, "INSERT INTO ps VALUES (1, 3)", "23505").await;
        refused(&pool, "ALTER TABLE ps DROP CONSTRAINT ps_id_key", "42704").await;
        ok(&pool, "ALTER TABLE ps DROP CONSTRAINT ps_tenant_id_key").await;
        ok(&pool, "INSERT INTO ps VALUES (1, 3)").await;
    }
    let _ = std::fs::remove_dir_all(&dir);
}
