//! `DO` blocks as migrations write them.
//!
//! A `DO $$ … $$` block ran its straight-line statements and its `IF`s.
//! The rest of what the pms migrations use was refused, each with its own
//! "PL/pgSQL … is not supported yet": a nested `BEGIN … EXCEPTION WHEN …
//! END`, `FOR … IN <query> LOOP`, a `record` variable, `EXECUTE … INTO`,
//! `SELECT … INTO`, and a `--` comment inside the body was read as part of
//! the statement after it. Beside them, three statements the blocks run:
//! `pg_partition_tree(…)`, `ALTER TABLE … RENAME CONSTRAINT` and `ALTER
//! INDEX … RENAME TO`.
//!
//! Every log a block leaves in the first three tests is the one PostgreSQL
//! 17 leaves for it. The migration test differs from it in one place, by
//! design: a partitioned table's partitions are not relations here, so
//! `pg_partition_tree` answers the table itself as its one leaf.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};

async fn rows_on<'c, E: sqlx::Executor<'c, Database = sqlx::Postgres>>(on: E, sql: &str) -> Vec<String> {
    let rows = sqlx::raw_sql(AssertSqlSafe(sql.to_string()))
        .fetch_all(on)
        .await
        .unwrap_or_else(|e| panic!("`{sql}` should succeed: {e}"));
    rows.iter()
        .map(|row| {
            (0..row.len())
                .map(|i| row.try_get_unchecked::<Option<String>, _>(i).expect("text").unwrap_or_else(|| "NULL".into()))
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

async fn rows(pool: &sqlx::PgPool, sql: &str) -> Vec<String> {
    rows_on(pool, sql).await
}

async fn run(pool: &sqlx::PgPool, sql: &str) {
    pool.execute(sqlx::raw_sql(AssertSqlSafe(sql.to_string()))).await.unwrap_or_else(|e| panic!("`{sql}` should succeed: {e}"));
}

/// The SQLSTATE and message `sql` fails with.
async fn refused(pool: &sqlx::PgPool, sql: &str) -> (String, String) {
    let e = sqlx::raw_sql(AssertSqlSafe(sql.to_string())).fetch_all(pool).await.expect_err(sql);
    let db = e.as_database_error().unwrap_or_else(|| panic!("{sql}: not a server error: {e}"));
    (db.code().map(|c| c.to_string()).unwrap_or_default(), db.message().to_string())
}

/// A server with `lg`, which each block writes what it did into, `src` to
/// loop over and `uq` to collide on.
async fn start() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    run(
        &pool,
        "CREATE TABLE lg (n serial PRIMARY KEY, m text);
         CREATE TABLE src (id int PRIMARY KEY, g text, v int);
         INSERT INTO src VALUES (1, 'a', 10), (2, 'a', 20), (3, 'b', 30), (4, 'b', NULL);
         CREATE TABLE uq (id int PRIMARY KEY, v int UNIQUE);
         INSERT INTO uq VALUES (1, 1);",
    )
    .await;
    (server, pool)
}

/// What the blocks run so far logged, oldest first, and the log emptied.
async fn logged(pool: &sqlx::PgPool) -> Vec<String> {
    let found = rows(pool, "SELECT m FROM lg ORDER BY n").await;
    run(pool, "DELETE FROM lg").await;
    found
}

#[tokio::test(flavor = "multi_thread")]
async fn an_exception_handler_takes_the_error_it_names() {
    let (_server, pool) = start().await;
    for (block, expected) in [
        // OTHERS, and what the handler runs instead.
        (
            "DO $$ BEGIN BEGIN INSERT INTO uq VALUES (2, 1); EXCEPTION WHEN OTHERS THEN \
             INSERT INTO lg (m) VALUES ('others: ' || SQLSTATE); END; END $$",
            vec!["others: 23505"],
        ),
        // A condition by name, the second of two handlers, either of its names.
        (
            "DO $$ DECLARE x int := 0; BEGIN BEGIN x := 1 / x; EXCEPTION WHEN unique_violation THEN \
             INSERT INTO lg (m) VALUES ('wrong'); WHEN division_by_zero OR foreign_key_violation THEN \
             INSERT INTO lg (m) VALUES ('div ' || SQLSTATE); END; END $$",
            vec!["div 22012"],
        ),
        // By its SQLSTATE.
        (
            "DO $$ BEGIN BEGIN INSERT INTO uq VALUES (1, 5); EXCEPTION WHEN SQLSTATE '23505' THEN \
             INSERT INTO lg (m) VALUES ('by code'); END; END $$",
            vec!["by code"],
        ),
        // The body's own handler, of an error the body raises; SQLERRM.
        (
            "DO $$ BEGIN RAISE EXCEPTION 'boom %', 7 USING ERRCODE = '22012'; EXCEPTION WHEN division_by_zero THEN \
             INSERT INTO lg (m) VALUES ('outer ' || SQLERRM); END $$",
            vec!["outer boom 7"],
        ),
        // What follows a handled block runs, and variables keep what they
        // were given before the error.
        (
            "DO $$ DECLARE c int := 0; BEGIN c := c + 1; BEGIN c := c + 1; INSERT INTO uq VALUES (1, 5); c := c + 100; \
             EXCEPTION WHEN OTHERS THEN c := c + 10; END; INSERT INTO lg (m) VALUES ('c=' || c); END $$",
            vec!["c=12"],
        ),
        // A handler that does nothing.
        (
            "DO $$ BEGIN BEGIN EXECUTE 'ALTER TABLE nosuch SET (fillfactor = 90)'; EXCEPTION WHEN OTHERS THEN NULL; END; \
             INSERT INTO lg (m) VALUES ('went on'); END $$",
            vec!["went on"],
        ),
    ] {
        run(&pool, block).await;
        assert_eq!(logged(&pool).await, expected, "{block}");
    }
    // A handler for another condition leaves the error the block's.
    let (code, _) = refused(
        &pool,
        "DO $$ BEGIN BEGIN INSERT INTO uq VALUES (1, 5); EXCEPTION WHEN division_by_zero THEN \
         INSERT INTO lg (m) VALUES ('wrong'); END; END $$",
    )
    .await;
    assert_eq!(code, "23505");
    // A handler may raise its own.
    let (code, message) = refused(
        &pool,
        "DO $$ BEGIN BEGIN INSERT INTO uq VALUES (1, 5); EXCEPTION WHEN OTHERS THEN \
         RAISE EXCEPTION 'again: %', SQLSTATE; END; END $$",
    )
    .await;
    assert_eq!((code.as_str(), message.as_str()), ("P0001", "again: 23505"));
    assert_eq!(logged(&pool).await, Vec::<String>::new());
    assert_eq!(rows(&pool, "SELECT id, v FROM uq ORDER BY id").await, ["1|1"]);
}

/// Inside a transaction block a handled error is rolled back to where its
/// block began, and the transaction goes on: PostgreSQL's subtransaction.
#[tokio::test(flavor = "multi_thread")]
async fn a_handled_error_keeps_its_transaction() {
    let (_server, pool) = start().await;
    let mut conn = pool.acquire().await.expect("connection");
    for sql in [
        "BEGIN",
        "INSERT INTO lg (m) VALUES ('tx outer')",
        "DO $$ BEGIN BEGIN INSERT INTO lg (m) VALUES ('tx inner, rolled back'); INSERT INTO uq VALUES (1, 5); \
         EXCEPTION WHEN unique_violation THEN INSERT INTO lg (m) VALUES ('tx handled'); END; END $$",
        "INSERT INTO lg (m) VALUES ('tx after')",
        "COMMIT",
    ] {
        conn.execute(sqlx::raw_sql(AssertSqlSafe(sql))).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    drop(conn);
    assert_eq!(logged(&pool).await, ["tx outer", "tx handled", "tx after"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn loops_run_over_queries_ranges_and_conditions() {
    let (_server, pool) = start().await;
    for (block, expected) in [
        // A record over a query, CONTINUE and EXIT.
        (
            "DO $$ DECLARE r record; BEGIN FOR r IN SELECT id, g, v FROM src ORDER BY id LOOP CONTINUE WHEN r.v IS NULL; \
             EXIT WHEN r.id > 3; INSERT INTO lg (m) VALUES ('row ' || r.id || ' ' || r.g || ' ' || r.v); END LOOP; END $$",
            vec!["row 1 a 10", "row 2 a 20", "row 3 b 30"],
        ),
        // Scalars, one per column.
        (
            "DO $$ DECLARE a int; b text; BEGIN FOR a, b IN SELECT id, g FROM src WHERE id < 3 ORDER BY id LOOP \
             INSERT INTO lg (m) VALUES ('ab ' || a || b); END LOOP; END $$",
            vec!["ab 1a", "ab 2a"],
        ),
        // Integer ranges: up, down by a step, and an empty one.
        (
            "DO $$ BEGIN FOR i IN 1..3 LOOP INSERT INTO lg (m) VALUES ('i' || i); END LOOP; \
             FOR i IN REVERSE 5..1 BY 2 LOOP INSERT INTO lg (m) VALUES ('r' || i); END LOOP; \
             FOR i IN 3..1 LOOP INSERT INTO lg (m) VALUES ('never'); END LOOP; END $$",
            vec!["i1", "i2", "i3", "r5", "r3", "r1"],
        ),
        // WHILE, and a bare LOOP left by EXIT WHEN.
        (
            "DO $$ DECLARE n int := 0; BEGIN WHILE n < 3 LOOP n := n + 1; END LOOP; \
             LOOP n := n + 10; EXIT WHEN n > 30; END LOOP; INSERT INTO lg (m) VALUES ('n=' || n); END $$",
            vec!["n=33"],
        ),
        // An EXIT leaves the loop it is in, not the one around it.
        (
            "DO $$ DECLARE r record; BEGIN FOR r IN SELECT DISTINCT g FROM src ORDER BY g LOOP FOR i IN 1..3 LOOP \
             IF i = 2 THEN EXIT; END IF; INSERT INTO lg (m) VALUES (r.g || i); END LOOP; END LOOP; END $$",
            vec!["a1", "b1"],
        ),
        // A dynamic query's rows.
        (
            "DO $$ DECLARE r record; BEGIN FOR r IN EXECUTE 'SELECT g, count(*) AS c FROM src GROUP BY g ORDER BY g' LOOP \
             INSERT INTO lg (m) VALUES ('grp ' || r.g || r.c); END LOOP; END $$",
            vec!["grp a2", "grp b2"],
        ),
        // EXECUTE … INTO and SELECT … INTO.
        (
            "DO $$ DECLARE has_row boolean; total int; label text; BEGIN \
             EXECUTE format('SELECT EXISTS (SELECT 1 FROM %I WHERE %s)', 'src', 'v > 25') INTO has_row; \
             SELECT sum(v), max(g) INTO total, label FROM src; \
             INSERT INTO lg (m) VALUES ('has=' || has_row::text || ' total=' || total || ' ' || label); END $$",
            vec!["has=true total=60 b"],
        ),
        // RETURN ends the block.
        (
            "DO $$ BEGIN INSERT INTO lg (m) VALUES ('before return'); RETURN; INSERT INTO lg (m) VALUES ('never'); END $$",
            vec!["before return"],
        ),
        // Comments in the body, with a `;` in them.
        (
            "DO $$\nBEGIN\n    -- a comment; with a semicolon\n    INSERT INTO lg (m) VALUES ('after -- comment'); \
             /* block ; comment */\n    INSERT INTO lg (m) VALUES ('second');\nEND $$",
            vec!["after -- comment", "second"],
        ),
    ] {
        run(&pool, block).await;
        assert_eq!(logged(&pool).await, expected, "{block}");
    }
    let (code, message) = refused(
        &pool,
        "DO $$ DECLARE r record; BEGIN FOR r IN SELECT id FROM src LOOP INSERT INTO lg (m) VALUES (r.nosuch); END LOOP; END $$",
    )
    .await;
    assert_eq!((code.as_str(), message.as_str()), ("42703", "record \"r\" has no field \"nosuch\""));
    assert_eq!(refused(&pool, "DO $$ BEGIN EXIT; END $$").await.0, "42601");
}

/// A file-backed server's directory, which a partitioned table needs.
struct OnDisk(std::path::PathBuf);

impl Drop for OnDisk {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// The pms migrations' own blocks, in their order, over tables shaped like
/// theirs: what they rename, add and drop, and the rows they copy.
#[tokio::test(flavor = "multi_thread")]
async fn the_blocks_migrations_write_run() {
    let root = std::env::temp_dir().join(format!("plpgsql_do_blocks_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let server = TestServer::start_file_backed(&root.to_string_lossy()).await.expect("server");
    let _root = OnDisk(root);
    let pool = server.connect_sqlx().await.expect("connect");
    run(
        &pool,
        r#"CREATE TABLE users (id uuid PRIMARY KEY);
           CREATE TABLE "role" (id uuid PRIMARY KEY, name text);
           CREATE TABLE user_role (id uuid PRIMARY KEY, user_id uuid, role_id uuid);
           CREATE TABLE meta (id bigint PRIMARY KEY, metadata jsonb, user_table_id uuid);
           CREATE TABLE domain_invite (id uuid PRIMARY KEY);
           CREATE TABLE licenses (id uuid PRIMARY KEY, reserved_for_invite_id uuid);
           CREATE TABLE user_table_row (id uuid NOT NULL, domain_id uuid NOT NULL, data jsonb, PRIMARY KEY (domain_id, id))
               PARTITION BY HASH (domain_id) PARTITIONS 2;
           CREATE TABLE crm_task (case_id uuid NOT NULL, space_id uuid, number int, type text, company_row_id uuid,
                                  contact_row_id uuid, CONSTRAINT crm_task_pkey PRIMARY KEY (case_id));
           CREATE UNIQUE INDEX uq_crm_task_space_number ON crm_task (space_id, number);
           CREATE INDEX idx_crm_task_space_type ON crm_task (space_id, type);
           CREATE TABLE app_task_link (case_id uuid NOT NULL, role text NOT NULL, row_id uuid NOT NULL,
                                       PRIMARY KEY (case_id, role, row_id));
           CREATE TABLE lg (n serial PRIMARY KEY, m text);
           INSERT INTO crm_task VALUES
               ('aaaaaaaa-0000-4000-8000-000000000001', NULL, 1, 't', 'bbbbbbbb-0000-4000-8000-000000000001', NULL),
               ('aaaaaaaa-0000-4000-8000-000000000002', NULL, 2, 't', NULL, 'dddddddd-0000-4000-8000-000000000002');"#,
    )
    .await;
    for block in [
        // to_regclass, and a rename run as dynamic SQL; twice, the second a no-op.
        r#"DO $$ BEGIN IF to_regclass('"role"') IS NOT NULL AND to_regclass('roles') IS NULL THEN
               EXECUTE 'ALTER TABLE "role" RENAME TO roles'; END IF; END $$"#,
        r#"DO $$ BEGIN IF to_regclass('"role"') IS NOT NULL AND to_regclass('roles') IS NULL THEN
               EXECUTE 'ALTER TABLE "role" RENAME TO roles'; END IF; END $$"#,
        // A foreign key added once, by pg_constraint; as dynamic SQL and as a statement.
        "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_user_role_role') THEN
             EXECUTE 'ALTER TABLE user_role ADD CONSTRAINT fk_user_role_role FOREIGN KEY (role_id) REFERENCES roles(id) ON DELETE CASCADE ON UPDATE CASCADE';
         END IF; END $$",
        "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_licenses_reserved_for_invite') THEN
             ALTER TABLE licenses ADD CONSTRAINT fk_licenses_reserved_for_invite FOREIGN KEY (reserved_for_invite_id)
                 REFERENCES domain_invite (id) ON DELETE SET NULL ON UPDATE CASCADE;
         END IF; END $$",
        "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_licenses_reserved_for_invite') THEN
             ALTER TABLE licenses ADD CONSTRAINT fk_licenses_reserved_for_invite FOREIGN KEY (reserved_for_invite_id)
                 REFERENCES domain_invite (id) ON DELETE SET NULL ON UPDATE CASCADE;
         END IF; END $$",
        // A storage setting tried, with a fallback, and one whose failure is ignored.
        "DO $$ BEGIN BEGIN EXECUTE 'ALTER TABLE meta ALTER COLUMN metadata SET COMPRESSION lz4';
             EXCEPTION WHEN OTHERS THEN EXECUTE 'ALTER TABLE meta ALTER COLUMN metadata SET COMPRESSION pglz'; END; END $$",
        "DO $$ BEGIN BEGIN EXECUTE 'ALTER TABLE meta SET (autovacuum_vacuum_scale_factor = 0.02)';
             EXCEPTION WHEN OTHERS THEN NULL; END; END $$",
        "DO $$ BEGIN BEGIN EXECUTE 'ALTER TABLE meta RESET (toast.autovacuum_vacuum_scale_factor)';
             EXCEPTION WHEN OTHERS THEN NULL; END; END $$",
        // A column renamed when information_schema says it is there and its new name is not.
        "DO $$ BEGIN IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'meta' AND column_name = 'user_table_id')
             AND NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'meta' AND column_name = 'parent_object_id')
             THEN ALTER TABLE meta RENAME COLUMN user_table_id TO parent_object_id; END IF; END $$",
        // A sequence created once, by pg_class.
        "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'meta_object_id_seq') THEN
             CREATE SEQUENCE meta_object_id_seq; END IF; END $$",
        // Every leaf of a partitioned table, by pg_partition_tree, into a regclass.
        "DO $$ DECLARE part regclass; BEGIN
             FOR part IN SELECT relid FROM pg_partition_tree('user_table_row'::regclass) WHERE isleaf LOOP
                 EXECUTE format('ALTER TABLE %s SET (' || 'autovacuum_vacuum_scale_factor = 0.05, ' || 'fillfactor = 90)', part);
                 INSERT INTO lg (m) VALUES ('leaf ' || part);
             END LOOP; END $$",
        // Nested handlers by condition name, RETURN, PERFORM, and a record over the catalogue.
        "DO $$ DECLARE part record; lz4_ok boolean := true; applied int := 0; skipped int := 0; BEGIN
             -- Fail fast on lock waits rather than block live traffic.
             PERFORM set_config('lock_timeout', '5s', true);
             BEGIN EXECUTE 'ALTER TABLE ONLY user_table_row ALTER COLUMN data SET COMPRESSION lz4';
             EXCEPTION WHEN feature_not_supported OR invalid_parameter_value THEN lz4_ok := false;
                       WHEN lock_not_available OR deadlock_detected THEN RAISE NOTICE 'parent attribute not set'; END;
             IF NOT lz4_ok THEN RAISE NOTICE 'lz4 not supported'; RETURN; END IF;
             FOR part IN SELECT c.relname AS name FROM pg_inherits i JOIN pg_class p ON p.oid = i.inhparent
                         JOIN pg_class c ON c.oid = i.inhrelid JOIN pg_namespace n ON n.oid = p.relnamespace
                         WHERE p.relname = 'user_table_row' AND n.nspname = current_schema() LOOP
                 BEGIN EXECUTE format('ALTER TABLE %I ALTER COLUMN data SET COMPRESSION lz4', part.name); applied := applied + 1;
                 EXCEPTION WHEN lock_not_available OR deadlock_detected THEN skipped := skipped + 1; END;
             END LOOP;
             INSERT INTO lg (m) VALUES ('lz4 done, skipped ' || skipped);
         END $$",
        // A table, its key and its indexes renamed together.
        "DO $$ BEGIN IF to_regclass('public.crm_task') IS NOT NULL AND to_regclass('public.app_task') IS NULL THEN
             ALTER TABLE crm_task RENAME TO app_task;
             ALTER TABLE app_task RENAME CONSTRAINT crm_task_pkey TO app_task_pkey;
             ALTER INDEX IF EXISTS uq_crm_task_space_number RENAME TO uq_app_task_space_number;
             ALTER INDEX IF EXISTS idx_crm_task_space_type RENAME TO idx_app_task_space_type;
             ALTER INDEX IF EXISTS idx_crm_task_gone RENAME TO idx_app_task_gone;
         END IF; END $$",
        // Rows copied out of columns that are then dropped; a second run finds nothing to do.
        "DO $$ BEGIN IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public'
                                 AND table_name = 'app_task' AND column_name = 'company_row_id') THEN
             INSERT INTO app_task_link (case_id, role, row_id)
                 SELECT case_id, 'company', company_row_id FROM app_task WHERE company_row_id IS NOT NULL ON CONFLICT DO NOTHING;
             INSERT INTO app_task_link (case_id, role, row_id)
                 SELECT case_id, 'contact', contact_row_id FROM app_task WHERE contact_row_id IS NOT NULL ON CONFLICT DO NOTHING;
             ALTER TABLE app_task DROP COLUMN IF EXISTS company_row_id, DROP COLUMN IF EXISTS contact_row_id;
         END IF; END $$",
        "DO $$ BEGIN IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public'
                                 AND table_name = 'app_task' AND column_name = 'company_row_id') THEN
             INSERT INTO lg (m) VALUES ('ran twice'); END IF; END $$",
    ] {
        run(&pool, block).await;
    }
    assert_eq!(logged(&pool).await, ["leaf user_table_row", "lz4 done, skipped 0"]);
    assert_eq!(
        rows(&pool, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name IN ('role', 'roles', 'crm_task', 'app_task') ORDER BY 1").await,
        ["app_task", "roles"]
    );
    assert_eq!(
        rows(&pool, "SELECT column_name FROM information_schema.columns WHERE table_name IN ('meta', 'app_task') ORDER BY table_name, ordinal_position").await,
        ["case_id", "space_id", "number", "type", "id", "metadata", "parent_object_id"]
    );
    assert_eq!(
        rows(&pool, "SELECT conname, contype FROM pg_constraint WHERE conname IN ('fk_user_role_role', 'fk_licenses_reserved_for_invite', 'app_task_pkey', 'crm_task_pkey') ORDER BY 1").await,
        ["app_task_pkey|p", "fk_licenses_reserved_for_invite|f", "fk_user_role_role|f"]
    );
    assert_eq!(
        rows(&pool, "SELECT indexname FROM pg_indexes WHERE tablename = 'app_task' ORDER BY 1").await,
        ["app_task_pkey", "idx_app_task_space_type", "uq_app_task_space_number"]
    );
    assert_eq!(
        rows(&pool, "SELECT role, row_id FROM app_task_link ORDER BY 1").await,
        ["company|bbbbbbbb-0000-4000-8000-000000000001", "contact|dddddddd-0000-4000-8000-000000000002"]
    );
    assert_eq!(rows(&pool, "SELECT count(*) FROM pg_class WHERE relname = 'meta_object_id_seq'").await, ["1"]);
    // The renamed unique index still keeps its columns unique, and the
    // renamed key is the one a duplicate names.
    assert_eq!(
        refused(&pool, "INSERT INTO app_task VALUES ('aaaaaaaa-0000-4000-8000-000000000001', NULL, 9, 't')").await.0,
        "23505"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn constraints_and_indexes_are_renamed() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    run(
        &pool,
        "CREATE TABLE a1 (id int, v int, w int, CONSTRAINT a1_pkey PRIMARY KEY (id), CONSTRAINT a1_v_u UNIQUE (v),
                          CONSTRAINT a1_w_pos CHECK (w > 0));
         CREATE TABLE a2 (id int PRIMARY KEY, a int REFERENCES a1 (id));
         CREATE INDEX a1_w_idx ON a1 (w);
         INSERT INTO a1 SELECT g, g, g FROM generate_series(1, 50) g;",
    )
    .await;
    for sql in [
        "ALTER TABLE a1 RENAME CONSTRAINT a1_v_u TO a1_v_unique",
        "ALTER TABLE a1 RENAME CONSTRAINT a1_w_pos TO a1_w_positive",
        "ALTER TABLE a2 RENAME CONSTRAINT a2_a_fkey TO a2_parent",
        "ALTER INDEX a1_w_idx RENAME TO a1_w_index",
        // A unique constraint's index, by ALTER INDEX.
        "ALTER INDEX a1_v_unique RENAME TO a1_v_uq",
        "ALTER INDEX IF EXISTS gone RENAME TO x",
    ] {
        run(&pool, sql).await;
    }
    assert_eq!(
        rows(&pool, "SELECT conname, contype FROM pg_constraint WHERE conrelid IN ('a1'::regclass, 'a2'::regclass) ORDER BY 1").await,
        ["a1_pkey|p", "a1_v_uq|u", "a1_w_positive|c", "a2_parent|f", "a2_pkey|p"]
    );
    assert_eq!(
        rows(&pool, "SELECT indexname FROM pg_indexes WHERE tablename = 'a1' ORDER BY 1").await,
        ["a1_pkey", "a1_v_uq", "a1_w_index"]
    );
    // Each still holds, under its new name.
    let (code, message) = refused(&pool, "INSERT INTO a1 VALUES (99, 1, 1)").await;
    assert!(code == "23505" && message.contains("\"a1_v_uq\""), "{code}: {message}");
    let (code, message) = refused(&pool, "INSERT INTO a1 VALUES (99, 99, -1)").await;
    assert!(code == "23514" && message.contains("\"a1_w_positive\""), "{code}: {message}");
    let (code, message) = refused(&pool, "INSERT INTO a2 VALUES (1, 777)").await;
    assert!(code == "23503" && message.contains("\"a2_parent\""), "{code}: {message}");
    assert_eq!(rows(&pool, "SELECT count(*) FROM a1 WHERE w = 7").await, ["1"]);

    // What PostgreSQL refuses too.
    assert_eq!(refused(&pool, "ALTER TABLE a1 RENAME CONSTRAINT nosuch TO x").await.0, "42704");
    assert_eq!(refused(&pool, "ALTER TABLE a1 RENAME CONSTRAINT a1_v_uq TO a1_w_positive").await.0, "42710");
    assert_eq!(refused(&pool, "ALTER TABLE a1 RENAME CONSTRAINT a1_v_uq TO a2_pkey").await.0, "42P07");
    assert_eq!(refused(&pool, "ALTER INDEX a1_w_index RENAME TO a1_v_uq").await.0, "42P07");
    assert_eq!(refused(&pool, "ALTER INDEX gone RENAME TO x").await.0, "42P01");
    // A primary key takes any name, declared with its column or not
    // (`primary_key_rename.rs` has the rest).
    run(&pool, "ALTER TABLE a2 RENAME CONSTRAINT a2_pkey TO a2_pk").await;
    run(&pool, "ALTER TABLE a1 RENAME CONSTRAINT a1_pkey TO a1_pk").await;
    // And keeps it when its table is renamed, as in PostgreSQL.
    run(&pool, "ALTER TABLE a1 RENAME TO b1; ALTER TABLE b1 RENAME CONSTRAINT a1_pk TO b1_pkey").await;
    assert_eq!(
        rows(&pool, "SELECT conname, contype FROM pg_constraint WHERE conrelid IN ('b1'::regclass, 'a2'::regclass) AND contype = 'p' ORDER BY 1").await,
        ["a2_pk|p", "b1_pkey|p"]
    );
    assert_eq!(refused(&pool, "INSERT INTO b1 VALUES (1, 99, 1)").await.0, "23505");
}
