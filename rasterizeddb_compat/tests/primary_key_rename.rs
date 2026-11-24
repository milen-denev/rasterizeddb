//! A primary key may be called anything, as in PostgreSQL.
//!
//! It was told from a UNIQUE by its name, `<table>_pkey`, alone. So it
//! could be renamed to that and to nothing else, one declared with its
//! column (`id INT PRIMARY KEY`) could not be renamed at all (0A000), and a
//! composite one could not be declared under a name of its own (0A000). The
//! key's record now says it is the primary key, and every reader asks one
//! function for it (`unique_key::primary_key_of`): the catalogues, the
//! indexes, a duplicate's message, `ON CONFLICT ON CONSTRAINT`, the
//! foreign-key and partitioning rules, `DROP CONSTRAINT`.
//!
//! Every expectation in the first test is PostgreSQL 17's answer to the
//! same statements.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};

async fn rows(pool: &sqlx::PgPool, sql: &str) -> Vec<String> {
    let rows = sqlx::raw_sql(AssertSqlSafe(sql.to_string()))
        .fetch_all(pool)
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

async fn run(pool: &sqlx::PgPool, sql: &str) {
    pool.execute(sqlx::raw_sql(AssertSqlSafe(sql.to_string()))).await.unwrap_or_else(|e| panic!("`{sql}` should succeed: {e}"));
}

/// The SQLSTATE and message `sql` fails with.
async fn refused(pool: &sqlx::PgPool, sql: &str) -> (String, String) {
    let e = sqlx::raw_sql(AssertSqlSafe(sql.to_string())).fetch_all(pool).await.expect_err(sql);
    let db = e.as_database_error().unwrap_or_else(|| panic!("{sql}: not a server error: {e}"));
    (db.code().map(|c| c.to_string()).unwrap_or_default(), db.message().to_string())
}

/// A duplicate is 23505, naming `constraint`.
async fn duplicate_of(pool: &sqlx::PgPool, sql: &str, constraint: &str) {
    let (code, message) = refused(pool, sql).await;
    assert!(code == "23505" && message.contains(&format!("\"{constraint}\"")), "{sql}: {code}: {message}");
}

const KEYS: &str = "SELECT conrelid::regclass::text, conname, contype FROM pg_constraint \
                    WHERE conrelid IN ('p1'::regclass, 'p2'::regclass, 'p3'::regclass, 'p4'::regclass) \
                    AND contype IN ('p', 'u') ORDER BY 1, 2";

const SETUP: &str = "
    CREATE TABLE p1 (id int PRIMARY KEY, email text UNIQUE, v int);
    CREATE TABLE p2 (a int NOT NULL, b int NOT NULL, v int, PRIMARY KEY (a, b));
    CREATE TABLE p3 (id int, v int, CONSTRAINT p3_custom PRIMARY KEY (id));
    CREATE TABLE p4 (a int, b int, CONSTRAINT p4_ab PRIMARY KEY (a, b));
    CREATE TABLE c1 (id int PRIMARY KEY, p int REFERENCES p1 (id), q int REFERENCES p3 (id));
    INSERT INTO p1 VALUES (1, 'a', 1), (2, 'b', 2);
    INSERT INTO p2 VALUES (1, 1, 1);
    INSERT INTO p3 VALUES (1, 1);
    INSERT INTO p4 VALUES (1, 1);
    INSERT INTO c1 VALUES (1, 1, 1);";

#[tokio::test(flavor = "multi_thread")]
async fn a_primary_key_takes_any_name() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    run(&pool, SETUP).await;
    // A composite key declared under a name of its own is the primary key.
    assert_eq!(rows(&pool, KEYS).await, ["p1|p1_email_key|u", "p1|p1_pkey|p", "p2|p2_pkey|p", "p3|p3_custom|p", "p4|p4_ab|p"]);

    for sql in [
        // Declared with its column; and the UNIQUE beside it.
        "ALTER TABLE p1 RENAME CONSTRAINT p1_pkey TO p1_primary",
        "ALTER TABLE p1 RENAME CONSTRAINT p1_email_key TO p1_email_unique",
        // Composite, unnamed; table-level, named; composite, named.
        "ALTER TABLE p2 RENAME CONSTRAINT p2_pkey TO p2_pk",
        "ALTER TABLE p3 RENAME CONSTRAINT p3_custom TO p3_pk",
        "ALTER INDEX p4_ab RENAME TO p4_pk",
        // And again, by its index.
        "ALTER INDEX p1_primary RENAME TO p1_pk",
    ] {
        run(&pool, sql).await;
    }
    assert_eq!(rows(&pool, KEYS).await, ["p1|p1_email_unique|u", "p1|p1_pk|p", "p2|p2_pk|p", "p3|p3_pk|p", "p4|p4_pk|p"]);
    assert_eq!(
        rows(&pool, "SELECT tablename, indexname FROM pg_indexes WHERE tablename IN ('p1', 'p2', 'p3', 'p4') ORDER BY 1, 2").await,
        ["p1|p1_email_unique", "p1|p1_pk", "p2|p2_pk", "p3|p3_pk", "p4|p4_pk"]
    );
    assert_eq!(
        rows(
            &pool,
            "SELECT c.relname, ix.indisprimary, ix.indisunique FROM pg_index ix JOIN pg_class c ON c.oid = ix.indexrelid \
             JOIN pg_class t ON t.oid = ix.indrelid WHERE t.relname IN ('p1', 'p2', 'p3', 'p4') ORDER BY 1"
        )
        .await,
        ["p1_email_unique|f|t", "p1_pk|t|t", "p2_pk|t|t", "p3_pk|t|t", "p4_pk|t|t"]
    );

    // Each still holds, and a duplicate names the key as it is now called.
    duplicate_of(&pool, "INSERT INTO p1 VALUES (1, 'z', 9)", "p1_pk").await;
    duplicate_of(&pool, "INSERT INTO p1 VALUES (9, 'a', 9)", "p1_email_unique").await;
    duplicate_of(&pool, "INSERT INTO p2 VALUES (1, 1, 9)", "p2_pk").await;
    duplicate_of(&pool, "INSERT INTO p3 VALUES (1, 9)", "p3_pk").await;
    duplicate_of(&pool, "INSERT INTO p4 VALUES (1, 1)", "p4_pk").await;
    assert_eq!(refused(&pool, "INSERT INTO p1 VALUES (NULL, 'n', 9)").await.0, "23502");
    // The foreign keys onto them.
    assert_eq!(refused(&pool, "INSERT INTO c1 VALUES (2, 77, 1)").await.0, "23503");
    run(&pool, "INSERT INTO c1 VALUES (3, 2, 1)").await;
    assert_eq!(refused(&pool, "DELETE FROM p1 WHERE id = 1").await.0, "23503");
    // By its new name, and by its columns.
    for (sql, expected) in [
        ("INSERT INTO p1 VALUES (1, 'q', 1) ON CONFLICT ON CONSTRAINT p1_pk DO UPDATE SET v = 100 RETURNING v", "100"),
        ("INSERT INTO p1 VALUES (1, 'q', 1) ON CONFLICT (id) DO UPDATE SET v = 101 RETURNING v", "101"),
        ("INSERT INTO p2 VALUES (1, 1, 5) ON CONFLICT ON CONSTRAINT p2_pk DO UPDATE SET v = excluded.v RETURNING v", "5"),
    ] {
        assert_eq!(rows(&pool, sql).await, [expected], "{sql}");
    }

    // It is still the one primary key; its old name is no constraint; a
    // name another table's index has is taken.
    for (sql, code) in [
        ("ALTER TABLE p1 ADD PRIMARY KEY (v)", "42P16"),
        ("ALTER TABLE p2 ADD CONSTRAINT again PRIMARY KEY (a)", "42P16"),
        ("ALTER TABLE p1 RENAME CONSTRAINT p1_pkey TO x", "42704"),
        ("ALTER TABLE p1 RENAME CONSTRAINT p1_pk TO p2_pk", "42P07"),
        // A foreign key references it.
        ("ALTER TABLE p1 DROP CONSTRAINT p1_pk", "2BP01"),
    ] {
        assert_eq!(refused(&pool, sql).await.0, code, "{sql}");
    }
    // Dropped by its new name, and declared again under another.
    for sql in [
        "ALTER TABLE p2 DROP CONSTRAINT p2_pk",
        "ALTER TABLE p2 ADD CONSTRAINT p2_named PRIMARY KEY (a, b)",
        "ALTER TABLE p1 DROP CONSTRAINT p1_pk CASCADE",
        "ALTER TABLE p1 ADD CONSTRAINT p1_again PRIMARY KEY (id)",
    ] {
        run(&pool, sql).await;
    }
    assert_eq!(refused(&pool, "ALTER TABLE p2 ADD PRIMARY KEY (a)").await.0, "42P16");
    assert_eq!(
        rows(&pool, "SELECT conrelid::regclass::text, conname, contype FROM pg_constraint WHERE conrelid IN ('p1'::regclass, 'p2'::regclass, 'c1'::regclass) ORDER BY 1, 2").await,
        ["c1|c1_pkey|p", "c1|c1_q_fkey|f", "p1|p1_again|p", "p1|p1_email_unique|u", "p2|p2_named|p"]
    );
    // A key that has a name of its own keeps it when its table is renamed.
    run(&pool, "ALTER TABLE p3 RENAME TO p3r; ALTER TABLE p4 RENAME TO p4r").await;
    assert_eq!(
        rows(&pool, "SELECT conrelid::regclass::text, conname, contype FROM pg_constraint WHERE conrelid IN ('p3r'::regclass, 'p4r'::regclass) ORDER BY 1, 2").await,
        ["p3r|p3_pk|p", "p4r|p4_pk|p"]
    );
    duplicate_of(&pool, "INSERT INTO p3r VALUES (1, 9)", "p3_pk").await;
    duplicate_of(&pool, "INSERT INTO p4r VALUES (1, 1)", "p4_pk").await;
}

/// A file-backed server's directory; removed on drop.
struct OnDisk(std::path::PathBuf);

impl Drop for OnDisk {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Renamed, then restarted: still the primary key, still enforced, still
/// what foreign keys and the conversion to partitions find.
#[tokio::test(flavor = "multi_thread")]
async fn a_renamed_primary_key_survives_a_restart_and_a_conversion() {
    let root = OnDisk(std::env::temp_dir().join(format!("primary_key_rename_{}", std::process::id())));
    let _ = std::fs::remove_dir_all(&root.0);
    let root_text = root.0.to_string_lossy().into_owned();
    let mut server = TestServer::start_file_backed(&root_text).await.expect("server");
    let mut pool = server.connect_sqlx().await.expect("connect");
    run(
        &pool,
        "CREATE TABLE p1 (id int PRIMARY KEY, email text UNIQUE, v int);
         CREATE TABLE p2 (a int NOT NULL, b int NOT NULL, v int, PRIMARY KEY (a, b));
         CREATE TABLE c1 (id int PRIMARY KEY, p int REFERENCES p1 (id));
         CREATE TABLE pc (id uuid NOT NULL PRIMARY KEY, k int NOT NULL, v text);
         INSERT INTO p1 SELECT g, 'e' || g, g FROM generate_series(1, 200) g;
         INSERT INTO p2 SELECT g, g, g FROM generate_series(1, 50) g;
         INSERT INTO c1 VALUES (1, 1);
         INSERT INTO pc SELECT gen_random_uuid(), g, 'v' FROM generate_series(1, 40) g;
         ALTER TABLE p1 RENAME CONSTRAINT p1_pkey TO p1_pk;
         ALTER TABLE p1 RENAME CONSTRAINT p1_email_key TO p1_mail;
         ALTER INDEX p2_pkey RENAME TO p2_pk;
         ALTER TABLE pc RENAME CONSTRAINT pc_pkey TO pc_primary;
         ALTER TABLE p2 RENAME COLUMN b TO bb;
         CREATE UNIQUE INDEX p1_v_idx ON p1 (v);",
    )
    .await;
    let keys = "SELECT conrelid::regclass::text, conname, contype FROM pg_constraint \
                WHERE conrelid IN ('p1'::regclass, 'p2'::regclass, 'pc'::regclass, 'c1'::regclass) ORDER BY 1, 2";
    for pass in ["before", "after a restart"] {
        assert_eq!(
            rows(&pool, keys).await,
            ["c1|c1_p_fkey|f", "c1|c1_pkey|p", "p1|p1_mail|u", "p1|p1_pk|p", "p2|p2_pk|p", "pc|pc_primary|p"],
            "{pass}"
        );
        // A key's record is written again when one of its columns is
        // renamed, and its mark with it: p2's is still the primary key. And
        // a unique index beside a renamed primary key is an index, not a
        // second primary key: the two marks share a byte of the record.
        assert_eq!(
            rows(&pool, "SELECT tablename, indexname FROM pg_indexes WHERE tablename IN ('p1', 'p2', 'pc') ORDER BY 1, 2").await,
            ["p1|p1_mail", "p1|p1_pk", "p1|p1_v_idx", "p2|p2_pk", "pc|pc_primary"],
            "{pass}"
        );
        assert_eq!(
            rows(
                &pool,
                "SELECT c.relname, ix.indisprimary FROM pg_index ix JOIN pg_class c ON c.oid = ix.indexrelid \
                 JOIN pg_class t ON t.oid = ix.indrelid WHERE t.relname IN ('p1', 'p2') ORDER BY 1"
            )
            .await,
            ["p1_mail|f", "p1_pk|t", "p1_v_idx|f", "p2_pk|t"],
            "{pass}"
        );
        assert_eq!(rows(&pool, "SELECT bb FROM p2 WHERE a = 3").await, ["3"], "{pass}");
        duplicate_of(&pool, "INSERT INTO p1 VALUES (1, 'zz', 9)", "p1_pk").await;
        duplicate_of(&pool, "INSERT INTO p1 VALUES (999, 'e5', 9)", "p1_mail").await;
        duplicate_of(&pool, "INSERT INTO p2 VALUES (1, 1, 9)", "p2_pk").await;
        assert_eq!(refused(&pool, "INSERT INTO c1 VALUES (2, 7777)").await.0, "23503", "{pass}");
        assert_eq!(refused(&pool, "DELETE FROM p1 WHERE id = 1").await.0, "23503", "{pass}");
        // The key's index finds its row.
        assert_eq!(rows(&pool, "SELECT email FROM p1 WHERE id = 150").await, ["e150"], "{pass}");
        if pass == "before" {
            pool.close().await;
            drop(server);
            server = TestServer::start_file_backed(&root_text).await.expect("restart");
            pool = server.connect_sqlx().await.expect("connect again");
        }
    }

    // The conversion to partitions reads the key by what it is. One over
    // `id` alone cannot hold the partition key, whatever it is called.
    let (code, message) = refused(&pool, "ALTER TABLE pc PARTITION BY HASH (k) PARTITIONS 4").await;
    assert!(code != "XX000" && message.contains("PRIMARY KEY (id)"), "{code}: {message}");
    // Its old name names nothing; its own drops it.
    assert_eq!(refused(&pool, "ALTER TABLE pc DROP CONSTRAINT pc_pkey").await.0, "42704");
    for sql in [
        "ALTER TABLE pc DROP CONSTRAINT pc_primary",
        "ALTER TABLE pc ADD CONSTRAINT pc_key PRIMARY KEY (id, k)",
        "ALTER TABLE pc PARTITION BY HASH (k) PARTITIONS 4",
        // A renamed composite key, converted as it is.
        "ALTER TABLE p2 PARTITION BY HASH (a) PARTITIONS 4",
        // And renamed once partitioned.
        "ALTER TABLE pc RENAME CONSTRAINT pc_key TO pc_key2",
    ] {
        run(&pool, sql).await;
    }
    pool.close().await;
    drop(server);
    let server = TestServer::start_file_backed(&root_text).await.expect("second restart");
    let pool = server.connect_sqlx().await.expect("connect");
    assert_eq!(
        rows(&pool, "SELECT conrelid::regclass::text, conname, contype FROM pg_constraint WHERE conrelid IN ('p2'::regclass, 'pc'::regclass) ORDER BY 1, 2").await,
        ["p2|p2_pk|p", "pc|pc_key2|p"]
    );
    assert_eq!(rows(&pool, "SELECT count(*) FROM pc").await, ["40"]);
    assert_eq!(rows(&pool, "SELECT v FROM p2 WHERE a = 7").await, ["7"]);
    duplicate_of(&pool, "INSERT INTO p2 VALUES (7, 7, 0)", "p2_pk").await;
    duplicate_of(&pool, "INSERT INTO pc SELECT id, k, 'dup' FROM pc LIMIT 1", "pc_key2").await;
}
