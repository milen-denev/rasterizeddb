//! Composite (multi-column) foreign keys, over a real client.
//!
//! A key over `(a, b)` holds on the tuple: a row references the parent row
//! holding both of its values at once. Every expectation is PostgreSQL 17's
//! answer to the same statements (run against `postgres:17`).

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Row};

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    (server, pool)
}

async fn run(pool: &sqlx::PgPool, sql: &str) -> Result<(), (String, String)> {
    sqlx::raw_sql(AssertSqlSafe(sql.to_string())).execute(pool).await.map(|_| ()).map_err(|e| {
        let code = e.as_database_error().and_then(|d| d.code()).map(|c| c.to_string()).unwrap_or_default();
        (code, e.to_string())
    })
}

async fn ok(pool: &sqlx::PgPool, sql: &str) {
    if let Err((code, e)) = run(pool, sql).await {
        panic!("`{sql}` must succeed, failed {code}: {e}");
    }
}

async fn refused(pool: &sqlx::PgPool, sql: &str, code: &str) -> String {
    match run(pool, sql).await {
        Ok(()) => panic!("`{sql}` was accepted; PostgreSQL refuses it with {code}"),
        Err((got, e)) => {
            assert_eq!(got, code, "`{sql}`: {e}");
            e
        }
    }
}

/// A directory removed when dropped.
struct Root(std::path::PathBuf);

impl Drop for Root {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A file-backed server: partitioned tables need one.
async fn file_backed(name: &str) -> (Root, TestServer, sqlx::PgPool) {
    let root = Root(std::env::temp_dir().join(format!("composite_fk_{name}_{}", std::process::id())));
    let _ = std::fs::remove_dir_all(&root.0);
    std::fs::create_dir_all(&root.0).expect("root");
    let server = TestServer::start_file_backed(&root.0.to_string_lossy()).await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    (root, server, pool)
}

/// Every row of `sql` as `a|b` text, `NULL` for a NULL.
async fn rows(pool: &sqlx::PgPool, sql: &str) -> Vec<String> {
    sqlx::raw_sql(AssertSqlSafe(sql.to_string()))
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| panic!("`{sql}` should succeed: {e}"))
        .iter()
        .map(|row| {
            (0..row.len())
                .map(|i| row.try_get_unchecked::<Option<String>, _>(i).expect("text").unwrap_or_else(|| "NULL".into()))
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_composite_key_holds_on_the_tuple() {
    let (_s, pool) = server().await;
    ok(&pool, "CREATE TABLE p (x int, y int, z int, PRIMARY KEY (x, y))").await;
    ok(&pool, "INSERT INTO p VALUES (1, 1, 0), (1, 2, 0), (2, 1, 0)").await;
    ok(&pool, "CREATE TABLE c (id int PRIMARY KEY, a int, b int, FOREIGN KEY (a, b) REFERENCES p)").await;
    ok(&pool, "INSERT INTO c VALUES (1, 1, 1), (2, NULL, 9), (3, 1, NULL)").await;
    let e = refused(&pool, "INSERT INTO c VALUES (4, 2, 2)", "23503").await;
    assert!(e.contains("c_a_b_fkey") && e.contains("(a, b)=(2, 2)"), "{e}");
    // Prepared, with parameters: the same check.
    let e = sqlx::query("INSERT INTO c VALUES ($1, $2, $3)")
        .bind(5_i32)
        .bind(2_i32)
        .bind(2_i32)
        .execute(&pool)
        .await
        .expect_err("no parent (2, 2)");
    assert_eq!(e.as_database_error().and_then(|d| d.code()).as_deref(), Some("23503"), "{e}");
    sqlx::query("INSERT INTO c VALUES ($1, $2, $3)").bind(5_i32).bind(2_i32).bind(1_i32).execute(&pool).await.unwrap();
    refused(&pool, "UPDATE c SET b = 2 WHERE id = 5", "23503").await;
    ok(&pool, "UPDATE c SET a = 1, b = 2 WHERE id = 5").await;
    refused(&pool, "DELETE FROM p WHERE x = 1 AND y = 2", "23503").await;
    refused(&pool, "UPDATE p SET y = 3 WHERE x = 1 AND y = 2", "23503").await;
    ok(&pool, "DELETE FROM p WHERE x = 2 AND y = 1").await;
    assert_eq!(
        rows(&pool, "SELECT conname, conkey::text, confkey::text, confmatchtype::text FROM pg_constraint WHERE contype = 'f'").await,
        ["c_a_b_fkey|{2,3}|{1,2}|s"]
    );
    refused(&pool, "CREATE TABLE d (a int, b int, FOREIGN KEY (a, b) REFERENCES p (x))", "42830").await;
    refused(&pool, "CREATE TABLE d (a int, b int, FOREIGN KEY (a, b) REFERENCES p (x, z))", "42830").await;
    refused(&pool, "CREATE TABLE d (a int, b int, FOREIGN KEY (a, b) REFERENCES p MATCH PARTIAL)", "0A000").await;
    ok(&pool, "CREATE TABLE f (a int, b int, FOREIGN KEY (a, b) REFERENCES p MATCH FULL)").await;
    let e = refused(&pool, "INSERT INTO f VALUES (1, NULL)", "23503").await;
    assert!(e.contains("MATCH FULL does not allow mixing of null and nonnull key values"), "{e}");
    ok(&pool, "INSERT INTO f VALUES (NULL, NULL)").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_composite_keys_actions_and_catalogues() {
    let (_s, pool) = server().await;
    ok(&pool, "CREATE TABLE p (x int, y int, PRIMARY KEY (x, y))").await;
    ok(&pool, "INSERT INTO p VALUES (1, 1), (1, 2), (2, 1)").await;
    ok(&pool, "CREATE TABLE cc (id int PRIMARY KEY, a int, b int, FOREIGN KEY (a, b) REFERENCES p ON DELETE CASCADE ON UPDATE CASCADE)").await;
    ok(&pool, "CREATE TABLE cs (id int PRIMARY KEY, a int, b int, FOREIGN KEY (a, b) REFERENCES p (x, y) ON DELETE SET NULL (b))").await;
    ok(&pool, "INSERT INTO cc VALUES (1, 1, 1), (2, 1, 2), (3, 2, 1)").await;
    ok(&pool, "INSERT INTO cs VALUES (1, 1, 2), (2, 2, 1)").await;
    ok(&pool, "DELETE FROM p WHERE x = 1 AND y = 2").await;
    assert_eq!(rows(&pool, "SELECT id, a, b FROM cc ORDER BY id").await, ["1|1|1", "3|2|1"]);
    assert_eq!(rows(&pool, "SELECT id, a, b FROM cs ORDER BY id").await, ["1|1|NULL", "2|2|1"]);
    // cs's (2, 1) blocks the parent's move (NO ACTION); cc would follow it.
    refused(&pool, "UPDATE p SET y = 5 WHERE x = 2 AND y = 1", "23503").await;
    ok(&pool, "DELETE FROM cs WHERE id = 2").await;
    ok(&pool, "UPDATE p SET y = 5 WHERE x = 2 AND y = 1").await;
    assert_eq!(rows(&pool, "SELECT id, a, b FROM cc ORDER BY id").await, ["1|1|1", "3|2|5"]);
    assert_eq!(
        rows(
            &pool,
            "SELECT constraint_name, column_name, ordinal_position::text, position_in_unique_constraint::text \
             FROM information_schema.key_column_usage WHERE table_name = 'cc' ORDER BY constraint_name, ordinal_position"
        )
        .await,
        ["cc_a_b_fkey|a|1|1", "cc_a_b_fkey|b|2|2", "cc_pkey|id|1|NULL"]
    );
    assert_eq!(
        rows(
            &pool,
            "SELECT constraint_name, unique_constraint_name, match_option, update_rule, delete_rule \
             FROM information_schema.referential_constraints ORDER BY constraint_name"
        )
        .await,
        ["cc_a_b_fkey|p_pkey|NONE|CASCADE|CASCADE", "cs_a_b_fkey|p_pkey|NONE|NO ACTION|SET NULL"]
    );
    assert_eq!(
        rows(&pool, "SELECT constraint_name, constraint_type FROM information_schema.table_constraints WHERE table_name = 'cs' AND constraint_type <> 'CHECK' ORDER BY 1").await,
        ["cs_a_b_fkey|FOREIGN KEY", "cs_pkey|PRIMARY KEY"]
    );
}

/// The `pms` layout over a real client: `step` and `field` hash-partitioned
/// by `(domain_id, case_id)`, `field` → `step` (partitioned onto
/// partitioned, co-located) and `step` → `case` (partitioned onto plain).
/// Every answer is PostgreSQL 17's with declarative partitions (`PARTITION
/// BY HASH`, `MODULUS 4` children); the catalogue rows are its rows for the
/// partitioned parents (`conparentid = 0`), which hold the keys there.
#[tokio::test(flavor = "multi_thread")]
async fn composite_keys_on_and_onto_partitioned_tables() {
    let (_root, _s, pool) = file_backed("onto").await;
    ok(&pool, "CREATE TABLE \"case\" (domain_id BIGINT NOT NULL, id BIGINT NOT NULL, PRIMARY KEY (domain_id, id))").await;
    ok(
        &pool,
        "CREATE TABLE step (domain_id BIGINT NOT NULL, case_id BIGINT NOT NULL, id BIGINT NOT NULL, \
         PRIMARY KEY (domain_id, case_id, id)) PARTITION BY HASH (domain_id, case_id) PARTITIONS 4",
    )
    .await;
    ok(
        &pool,
        "CREATE TABLE field (domain_id BIGINT NOT NULL, case_id BIGINT NOT NULL, step_id BIGINT NOT NULL, \
         id BIGINT NOT NULL, PRIMARY KEY (domain_id, case_id, step_id, id)) PARTITION BY HASH (domain_id, case_id) PARTITIONS 4",
    )
    .await;
    ok(
        &pool,
        "ALTER TABLE field ADD CONSTRAINT field_step_fk FOREIGN KEY (domain_id, case_id, step_id) \
         REFERENCES step (domain_id, case_id, id)",
    )
    .await;
    ok(&pool, "ALTER TABLE step ADD CONSTRAINT step_case_fk FOREIGN KEY (domain_id, case_id) REFERENCES \"case\" (domain_id, id)").await;
    ok(&pool, "INSERT INTO \"case\" VALUES (1, 10), (1, 11), (2, 10), (-3, 7)").await;
    ok(&pool, "INSERT INTO step VALUES (1, 10, 100), (1, 10, 101), (2, 10, 100), (-3, 7, 1)").await;
    let e = refused(&pool, "INSERT INTO step VALUES (2, 11, 1)", "23503").await;
    assert!(e.contains("step_case_fk") && e.contains("(domain_id, case_id)=(2, 11)"), "{e}");
    ok(&pool, "INSERT INTO field VALUES (1, 10, 100, 1), (-3, 7, 1, 1)").await;
    let e = refused(&pool, "INSERT INTO field VALUES (2, 10, 101, 1)", "23503").await;
    assert!(e.contains("field_step_fk") && e.contains("(domain_id, case_id, step_id)=(2, 10, 101)"), "{e}");
    // Prepared, with parameters, into the partitioned child: the same check,
    // on every execution (a learned one included).
    for _ in 0..3 {
        let e = sqlx::query("INSERT INTO field VALUES ($1, $2, $3, $4)")
            .bind(1_i64)
            .bind(10_i64)
            .bind(102_i64)
            .bind(1_i64)
            .execute(&pool)
            .await
            .expect_err("no step (1, 10, 102)");
        assert_eq!(e.as_database_error().and_then(|d| d.code()).as_deref(), Some("23503"), "{e}");
    }
    for id in 2..5_i64 {
        sqlx::query("INSERT INTO field VALUES ($1, $2, $3, $4)")
            .bind(1_i64)
            .bind(10_i64)
            .bind(101_i64)
            .bind(id)
            .execute(&pool)
            .await
            .unwrap();
    }
    // COPY into the partitioned child: a row without a parent fails it.
    let mut conn = pool.acquire().await.unwrap();
    let mut copy = conn.copy_in_raw("COPY field (domain_id, case_id, step_id, id) FROM STDIN").await.unwrap();
    copy.send(b"1\t10\t100\t7\n2\t10\t999\t7\n".as_slice()).await.unwrap();
    let e = copy.finish().await.expect_err("(2, 10, 999) has no step");
    assert_eq!(e.as_database_error().and_then(|d| d.code()).as_deref(), Some("23503"), "{e}");
    let mut copy = conn.copy_in_raw("COPY field (domain_id, case_id, step_id, id) FROM STDIN").await.unwrap();
    copy.send(b"1\t10\t100\t7\n2\t10\t100\t7\n".as_slice()).await.unwrap();
    assert_eq!(copy.finish().await.unwrap(), 2);
    drop(conn);
    assert_eq!(rows(&pool, "SELECT count(*) FROM field").await, ["7"]);
    // The parent side, on the partitioned parent and on the plain one.
    let e = refused(&pool, "DELETE FROM step WHERE domain_id = 1 AND case_id = 10 AND id = 100", "23503").await;
    assert!(e.contains("violates foreign key constraint \"field_step_fk\" on table \"field\""), "{e}");
    refused(&pool, "UPDATE step SET id = 300 WHERE domain_id = 1 AND case_id = 10 AND id = 101", "23503").await;
    refused(&pool, "DELETE FROM \"case\" WHERE domain_id = 1 AND id = 10", "23503").await;
    refused(&pool, "CREATE TABLE bad (a BIGINT, b BIGINT, FOREIGN KEY (a, b) REFERENCES step (case_id, id))", "42830").await;
    assert_eq!(
        rows(
            &pool,
            "SELECT conrelid::regclass::text, conname, conkey::text, confkey::text, confmatchtype::text, \
             confupdtype::text, confdeltype::text, pg_get_constraintdef(oid) FROM pg_constraint \
             WHERE contype = 'f' ORDER BY 1, 2"
        )
        .await,
        // Every name quoted, where PostgreSQL quotes only `"case"` (the
        // documented divergence of `pg_get_constraintdef`; the same text).
        [
            "field|field_step_fk|{1,2,3}|{1,2,3}|s|a|a|FOREIGN KEY (\"domain_id\", \"case_id\", \"step_id\") REFERENCES \"step\"(\"domain_id\", \"case_id\", \"id\")",
            "step|step_case_fk|{1,2}|{1,2}|s|a|a|FOREIGN KEY (\"domain_id\", \"case_id\") REFERENCES \"case\"(\"domain_id\", \"id\")",
        ]
    );
    assert_eq!(
        rows(
            &pool,
            "SELECT constraint_name, unique_constraint_name, match_option, update_rule, delete_rule \
             FROM information_schema.referential_constraints ORDER BY constraint_name"
        )
        .await,
        ["field_step_fk|step_pkey|NONE|NO ACTION|NO ACTION", "step_case_fk|case_pkey|NONE|NO ACTION|NO ACTION"]
    );
    assert_eq!(
        rows(
            &pool,
            "SELECT table_name, constraint_name, column_name, ordinal_position::text, position_in_unique_constraint::text \
             FROM information_schema.key_column_usage WHERE constraint_name IN ('field_step_fk', 'step_case_fk') \
             ORDER BY 1, 2, 4"
        )
        .await,
        [
            "field|field_step_fk|domain_id|1|1",
            "field|field_step_fk|case_id|2|2",
            "field|field_step_fk|step_id|3|3",
            "step|step_case_fk|domain_id|1|1",
            "step|step_case_fk|case_id|2|2",
        ]
    );
}

/// A plain table's composite key onto a partitioned table, declared inline,
/// with its actions, and a partitioned child's `ON DELETE SET NULL (c)` and
/// `MATCH FULL` (PostgreSQL 17's answers).
#[tokio::test(flavor = "multi_thread")]
async fn composite_key_actions_across_partitioned_tables() {
    let (_root, _s, pool) = file_backed("actions").await;
    ok(&pool, "CREATE TABLE \"case\" (domain_id BIGINT NOT NULL, id BIGINT NOT NULL, PRIMARY KEY (domain_id, id))").await;
    ok(
        &pool,
        "CREATE TABLE step (domain_id BIGINT NOT NULL, case_id BIGINT NOT NULL, id BIGINT NOT NULL, \
         PRIMARY KEY (domain_id, case_id, id)) PARTITION BY HASH (domain_id, case_id) PARTITIONS 4",
    )
    .await;
    ok(&pool, "INSERT INTO step VALUES (1, 10, 100), (1, 11, 300), (1, 11, 6)").await;
    ok(
        &pool,
        "CREATE TABLE note (n INT PRIMARY KEY, domain_id BIGINT, case_id BIGINT, step_id BIGINT, \
         FOREIGN KEY (domain_id, case_id, step_id) REFERENCES step ON DELETE CASCADE ON UPDATE CASCADE)",
    )
    .await;
    ok(&pool, "INSERT INTO note VALUES (1, 1, 10, 100), (2, 1, 11, 300), (3, NULL, 99, 99), (5, 1, 11, 6)").await;
    refused(&pool, "INSERT INTO note VALUES (4, 1, 11, 100)", "23503").await;
    ok(&pool, "UPDATE step SET id = 301 WHERE domain_id = 1 AND case_id = 11 AND id = 300").await;
    assert_eq!(rows(&pool, "SELECT n, step_id FROM note ORDER BY n").await, ["1|100", "2|301", "3|99", "5|6"]);
    ok(&pool, "DELETE FROM step WHERE domain_id = 1 AND case_id = 11").await;
    assert_eq!(rows(&pool, "SELECT n FROM note ORDER BY n").await, ["1", "3"]);

    ok(&pool, "INSERT INTO \"case\" VALUES (5, 5), (5, 6), (4, 1)").await;
    ok(
        &pool,
        "CREATE TABLE sd (k BIGINT NOT NULL, id BIGINT NOT NULL, d BIGINT, c BIGINT, PRIMARY KEY (k, id), \
         FOREIGN KEY (d, c) REFERENCES \"case\" (domain_id, id) MATCH FULL ON DELETE SET NULL (c)) \
         PARTITION BY HASH (k) PARTITIONS 4",
    )
    .await;
    let e = refused(&pool, "INSERT INTO sd VALUES (1, 1, 5, NULL)", "23503").await;
    assert!(e.contains("MATCH FULL does not allow mixing of null and nonnull key values"), "{e}");
    ok(&pool, "INSERT INTO sd VALUES (1, 1, 5, 5), (2, 2, 5, 6)").await;
    // SET NULL (c) leaves (5, NULL), which MATCH FULL refuses: PostgreSQL
    // fails the parent's DELETE.
    refused(&pool, "DELETE FROM \"case\" WHERE domain_id = 5 AND id = 5", "23503").await;
    ok(
        &pool,
        "CREATE TABLE ev (domain_id BIGINT NOT NULL, case_id BIGINT NOT NULL, e BIGINT NOT NULL, \
         PRIMARY KEY (domain_id, case_id, e), CONSTRAINT ev_case_fk FOREIGN KEY (domain_id, case_id) \
         REFERENCES \"case\" (domain_id, id) ON DELETE CASCADE ON UPDATE CASCADE) \
         PARTITION BY HASH (domain_id, case_id) PARTITIONS 4",
    )
    .await;
    ok(&pool, "INSERT INTO ev VALUES (4, 1, 1), (4, 1, 2)").await;
    ok(&pool, "UPDATE \"case\" SET id = 20 WHERE domain_id = 4 AND id = 1").await;
    assert_eq!(rows(&pool, "SELECT domain_id, case_id, e FROM ev ORDER BY e").await, ["4|20|1", "4|20|2"]);
    ok(&pool, "DELETE FROM \"case\" WHERE domain_id = 4").await;
    assert!(rows(&pool, "SELECT e FROM ev").await.is_empty());
}
