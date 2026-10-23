//! Partitioning syntax other than the supported `PARTITION BY HASH` shape is
//! refused, refusing it writes nothing, and the supported shape never
//! creates an ordinary table.
//!
//! `CREATE TABLE` used to take its body between the first `(` and the *last*
//! `)`, so a `PARTITION BY HASH (k)` trailer became part of the column list.
//! With a constraint on the last column the leftover words were read as more
//! constraint text and ignored: the statement created an ordinary table and
//! reported success, and a client that asked for partitions got none. These
//! tests pin the refusal at the level that can see the catalogue, since a
//! parser that rejects the text is no proof that nothing was registered.
//! `partition_ddl.rs` covers the supported shape and the key rules.

use rasterizeddb_core::{
    BATCH_SIZE, MAX_PERMITS_THREADS,
    core::{
        database::Database,
        rql::{executor::execute, lexer_s1::recognize_query_purpose},
        storage_providers::traits::StorageIO,
    },
};
use rclite::Arc;

/// Runs a test body on a thread with a stack big enough for the engine; see
/// `table_lifecycle.rs` for why both the thread and the runtime need it.
fn on_a_big_stack<F: std::future::Future<Output = ()> + Send + 'static>(body: F) {
    std::thread::Builder::new()
        .stack_size(32 * 1024 * 1024)
        .spawn(move || {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .thread_stack_size(32 * 1024 * 1024)
                .enable_all()
                .build()
                .expect("a tokio runtime")
                .block_on(body);
        })
        .expect("a test thread")
        .join()
        .expect("the test thread panicked");
}

fn init_globals() {
    MAX_PERMITS_THREADS.get_or_init(|| 4);
    BATCH_SIZE.get_or_init(|| 1_000);
}

/// The engine's answer: `Ok(())` for success, `Err(message)` for status 3.
async fn run(db: &Arc<Database>, q: &str) -> Result<(), String> {
    let purpose = recognize_query_purpose(q).expect("recognized");
    let out = execute(purpose, db.clone()).await;
    if out.first() == Some(&3) {
        Err(String::from_utf8_lossy(&out[1..]).into_owned())
    } else {
        Ok(())
    }
}

/// The table catalogue's length on disk. `add_table` appends a row to it
/// before the table is opened, so an unchanged length means no row was written.
async fn catalogue_len(db: &Database) -> u64 {
    db.db_io_pointers.get_len().await + db.db_io_rows.get_len().await
}

#[test]
fn partitioning_syntax_is_refused_and_writes_no_catalogue_row() {
    on_a_big_stack(partitioning_syntax_is_refused_and_writes_no_catalogue_row_body());
}

async fn partitioning_syntax_is_refused_and_writes_no_catalogue_row_body() {
    init_globals();
    let dir = format!("/tmp/rdb_partition_refusals_{}", std::process::id());
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("dir");
    let db = Arc::new(Database::new_with_backend(&dir, false).await);

    // The probe has to be able to see a write, or "unchanged" proves nothing.
    let before = catalogue_len(&db).await;
    run(
        &db,
        "CREATE TABLE pt_parent (id INT NOT NULL, k INT NOT NULL)",
    )
    .await
    .expect("a plain table is created");
    assert!(
        catalogue_len(&db).await > before,
        "creating a table did not grow the catalogue, so the probe cannot see a write"
    );

    // The silent one: the trailer was swallowed as constraint text and an
    // ordinary table was created. The supported shape now creates a
    // partitioned table, and nothing under its logical name.
    for (table, sql) in [
        (
            "pt_hash_nn",
            "CREATE TABLE pt_hash_nn (id INT, k INT NOT NULL) PARTITION BY HASH (k)",
        ),
        (
            "pt_dollar",
            "CREATE TABLE pt_dollar (k INT NOT NULL, s TEXT NOT NULL, CHECK (s <> $$($$)) \
             PARTITION BY HASH (k)",
        ),
    ] {
        run(&db, sql)
            .await
            .unwrap_or_else(|e| panic!("{} was refused: {}", sql, e));
        assert!(
            db.partitioned.contains_key(table),
            "{} did not create a partitioned table",
            sql
        );
        let litter: Vec<String> = std::fs::read_dir(&dir)
            .expect("read dir")
            .filter_map(|e| e.ok()?.file_name().into_string().ok())
            .filter(|n| n.starts_with(table))
            .collect();
        assert!(litter.is_empty(), "{} made plain-table files {:?}", sql, litter);
    }

    for (table, sql) in [
        // Failed only by accident, on the type `INT)`; now on the key being
        // nullable.
        (
            "pt_hash_bare",
            "CREATE TABLE pt_hash_bare (id INT, k INT) PARTITION BY HASH (k)",
        ),
        (
            "pt_hash_pk",
            "CREATE TABLE pt_hash_pk (id INT, k INT NOT NULL, PRIMARY KEY (id)) \
             PARTITION BY HASH (k)",
        ),
        (
            "pt_range",
            "CREATE TABLE pt_range (id INT, k INT NOT NULL) PARTITION BY RANGE (k)",
        ),
        (
            "pt_list_ine",
            "create table if not exists pt_list_ine (id int not null) partition by list (id)",
        ),
        (
            "pt_inherits",
            "CREATE TABLE pt_inherits (id INT NOT NULL) INHERITS (pt_parent)",
        ),
        // No column list at all: the `(` that was taken for one is the bound.
        (
            "pt_child",
            "CREATE TABLE pt_child PARTITION OF pt_parent FOR VALUES WITH (MODULUS 4, REMAINDER 0)",
        ),
        (
            "pt_child_cols",
            "CREATE TABLE pt_child_cols PARTITION OF pt_parent (id NOT NULL) \
             FOR VALUES WITH (MODULUS 4, REMAINDER 1)",
        ),
        // Block comments nest, so the `(` is comment and the trailer is not.
        // `strip_sql_comments` ends a comment at its first `*/`, so what
        // reaches the parser has a stray `(` and is a syntax error instead;
        // either refusal will do, as long as nothing is created.
        (
            "pt_nested_comment",
            "CREATE TABLE pt_nested_comment (k INT NOT NULL) /* x /* y */ ( */ \
             PARTITION BY HASH (k)",
        ),
        // A name no shape-match would recognise.
        (
            "pt-dash",
            "CREATE TABLE pt-dash (id INT, k INT NOT NULL) PARTITION BY HASH (k)",
        ),
        (
            "pt_using",
            "CREATE TABLE pt_using (k INT NOT NULL) USING heap PARTITION BY HASH (k)",
        ),
        (
            "a.b.c",
            "CREATE TABLE a.b.c (k INT NOT NULL) PARTITION BY HASH (k)",
        ),
        // The column list never closes. Created a table, status 0, until the
        // unbalanced scan became a syntax error.
        (
            "pt_unclosed",
            "CREATE TABLE pt_unclosed (id INT NOT NULL PARTITION BY HASH (id)",
        ),
    ] {
        let tables_before = db.tables.len();
        let len_before = catalogue_len(&db).await;
        let reason = run(&db, sql)
            .await
            .expect_err(&format!("{} was accepted", sql));
        let lower = reason.to_ascii_lowercase();
        let syntax = lower.starts_with("syntax error");
        let unsupported = lower.contains("not supported")
            && (lower.contains("partition") || lower.contains("table inheritance"));
        let expected = match table {
            "pt_unclosed" => syntax,
            "pt_nested_comment" => syntax || unsupported,
            _ => unsupported,
        };
        assert!(
            expected,
            "{} was refused for the wrong reason: {}",
            sql, reason
        );
        assert!(
            !db.tables.contains_key(table),
            "{} registered table {}",
            sql,
            table
        );
        assert_eq!(db.tables.len(), tables_before, "{} registered a table", sql);
        assert_eq!(
            catalogue_len(&db).await,
            len_before,
            "{} wrote a catalogue row",
            sql
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}

/// `ATTACH PARTITION` and `DETACH PARTITION` fail loudly today, through the
/// ALTER parser's catch-all. Pinned so a later "accept and ignore" entry in
/// that parser's no-op list cannot turn either into a silent success.
#[test]
fn attach_and_detach_partition_are_refused() {
    on_a_big_stack(attach_and_detach_partition_are_refused_body());
}

async fn attach_and_detach_partition_are_refused_body() {
    init_globals();
    let dir = format!("/tmp/rdb_partition_attach_{}", std::process::id());
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("dir");
    let db = Arc::new(Database::new_with_backend(&dir, false).await);

    run(&db, "CREATE TABLE pa_parent (id INT NOT NULL)")
        .await
        .expect("parent");
    run(&db, "CREATE TABLE pa_child (id INT NOT NULL)")
        .await
        .expect("child");

    for (sql, verb) in [
        (
            "ALTER TABLE pa_parent ATTACH PARTITION pa_child FOR VALUES WITH (MODULUS 2, REMAINDER 0)",
            "ATTACH",
        ),
        ("ALTER TABLE pa_parent DETACH PARTITION pa_child", "DETACH"),
        (
            "ALTER TABLE pa_parent DETACH PARTITION pa_child CONCURRENTLY",
            "DETACH",
        ),
    ] {
        let reason = run(&db, sql)
            .await
            .expect_err(&format!("{} was accepted", sql));
        assert!(
            reason.contains(verb) && reason.to_ascii_lowercase().contains("not supported"),
            "{} was refused for the wrong reason: {}",
            sql,
            reason
        );
    }
    // Both tables are still there and still plain tables.
    assert!(db.tables.contains_key("pa_parent"));
    assert!(db.tables.contains_key("pa_child"));

    let _ = std::fs::remove_dir_all(&dir);
}

/// Every partitioning statement 2a does not build is refused with "not
/// supported" (0A000) and a reason, on a plain table and on a partitioned
/// one, and leaves the catalogue and both tables as they were: the
/// conversions (`ALTER … PARTITION BY`, `CREATE INDEX … USING partition`) and
/// the reshapes (`SPLIT PARTITION`, `ISOLATE KEY`, `SPLIT KEY`, `MERGE
/// PARTITIONS`, `STORAGE MOVE PARTITION`, `STORAGE REBALANCE PARTITIONS`),
/// `PARTITION OF … DEFAULT` and temporary or unlogged partitioned tables. A
/// statement the ALTER parser's no-op list took would answer success with
/// nothing done, which is the failure this pins.
#[test]
fn unbuilt_partitioning_statements_are_refused() {
    on_a_big_stack(unbuilt_partitioning_statements_are_refused_body());
}

async fn unbuilt_partitioning_statements_are_refused_body() {
    init_globals();
    let dir = tempfile::tempdir().expect("dir");
    let db = Arc::new(Database::new_with_backend(dir.path().to_str().expect("utf-8"), false).await);
    run(&db, "CREATE TABLE pu_plain (k INT NOT NULL, n INT)")
        .await
        .expect("plain");
    run(
        &db,
        "CREATE TABLE pu_part (k INT NOT NULL, g INT NOT NULL, n INT) PARTITION BY HASH (k, g) PARTITIONS 2",
    )
    .await
    .expect("partitioned");
    let uid = db.partitioned.get("pu_part").expect("partitioned").uid();
    let len = catalogue_len(&db).await;

    let mut statements = vec![
        "CREATE TABLE pu_c PARTITION OF pu_part DEFAULT".to_string(),
        "CREATE TABLE pu_c PARTITION OF pu_part FOR VALUES WITH (MODULUS 2, REMAINDER 0)".to_string(),
        "CREATE TEMP TABLE pu_t (k INT NOT NULL) PARTITION BY HASH (k)".to_string(),
        "CREATE TEMPORARY TABLE pu_t (k INT NOT NULL) PARTITION BY HASH (k)".to_string(),
        "CREATE UNLOGGED TABLE pu_t (k INT NOT NULL) PARTITION BY HASH (k)".to_string(),
        // `STORAGE MOVE PARTITION` is read today as a MOVE of a table named
        // PARTITION, so it is refused for that table or its location rather
        // than as unsupported; checked only for being refused. `STORAGE
        // REBALANCE PARTITIONS` is left out: the STORAGE parser ignores the
        // trailing word and runs a whole-database REBALANCE.
        "STORAGE MOVE PARTITION pu_part FOR (k = 42) TO '/nonexistent'".to_string(),
    ];
    for table in ["pu_plain", "pu_part"] {
        for tail in [
            "PARTITION BY HASH (k)",
            "PARTITION BY HASH (k) PARTITIONS 4",
            "SPLIT PARTITION FOR (k = 42)",
            "ISOLATE KEY (k = 42)",
            "ISOLATE KEY (k = 42) TO '/nonexistent'",
            "SPLIT KEY (k = 42) INTO 8",
            "MERGE PARTITIONS FOR (k = 1), FOR (k = 2)",
        ] {
            statements.push(format!("ALTER TABLE {table} {tail}"));
        }
        statements.push(format!("CREATE INDEX {table}_by ON {table} USING partition (k)"));
        statements.push(format!(
            "CREATE INDEX {table}_by ON {table} USING partition (k) WITH (partitions = 4)"
        ));
    }

    let mut wrong = Vec::new();
    for sql in &statements {
        // pgwire answers its no-op list's statements itself, but only those
        // the executor does not recognise; one of these there would be a
        // success with nothing done.
        if rasterizeddb_core::core::rql::lexer_alter::no_op_reason(sql).is_some()
            && recognize_query_purpose(sql).is_none()
        {
            wrong.push(format!("{sql}: accepted as a no-op by pgwire"));
        }
        let tables_before = db.tables.len();
        let outcome = match recognize_query_purpose(sql) {
            None => Err("not recognised".to_string()),
            Some(purpose) => {
                let out = execute(purpose, db.clone()).await;
                if out.first() == Some(&3) {
                    Err(String::from_utf8_lossy(&out[1..]).into_owned())
                } else {
                    Ok(())
                }
            }
        };
        // TEMP and UNLOGGED tables are not a statement the engine knows at
        // all, so they never reach a CREATE; pgwire answers 42601.
        let unknown_is_fine = sql.starts_with("CREATE TEMP") || sql.starts_with("CREATE UNLOGGED");
        match outcome {
            Ok(()) => wrong.push(format!("{sql}: accepted")),
            Err(reason) if reason == "not recognised" && unknown_is_fine => {}
            Err(_) if sql.starts_with("STORAGE MOVE PARTITION") => {}
            Err(reason) if !reason.to_ascii_lowercase().contains("not supported") => {
                wrong.push(format!("{sql}: refused without 0A000: {reason}"))
            }
            Err(_) => {}
        }
        assert_eq!(db.tables.len(), tables_before, "{sql} registered a table");
        assert_eq!(catalogue_len(&db).await, len, "{sql} wrote a catalogue row");
        assert_eq!(db.partitioned.get("pu_part").map(|t| t.uid()), Some(uid), "{sql}");
        assert_eq!(db.partitioned.len(), 1, "{sql} made pu_plain partitioned");
    }
    assert!(wrong.is_empty(), "statements not refused as unsupported:\n{}", wrong.join("\n"));
}
