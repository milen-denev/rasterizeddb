//! Table partitioning syntax is refused, and refusing it writes nothing.
//!
//! `CREATE TABLE` used to take its body between the first `(` and the *last*
//! `)`, so a `PARTITION BY HASH (k)` trailer became part of the column list.
//! With a constraint on the last column the leftover words were read as more
//! constraint text and ignored: the statement created an ordinary table and
//! reported success, and a client that asked for partitions got none. These
//! tests pin the refusal at the level that can see the catalogue, since a
//! parser that rejects the text is no proof that nothing was registered.

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

    for (table, sql) in [
        // The silent one: the trailer was swallowed as constraint text.
        (
            "pt_hash_nn",
            "CREATE TABLE pt_hash_nn (id INT, k INT NOT NULL) PARTITION BY HASH (k)",
        ),
        // Failed only by accident, on the type `INT)`.
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
        (
            "pt_dollar",
            "CREATE TABLE pt_dollar (k INT NOT NULL, s TEXT NOT NULL, CHECK (s <> $$($$)) \
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
            && (lower.contains("table partitioning") || lower.contains("table inheritance"));
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
