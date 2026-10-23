//! The partition shell's guarantees (design §3.3, invariant I8).
//!
//! A shell is a `Table` with a schema and no rows. Anything that reads or
//! writes rows must go through `Table::data`, which refuses on a shell, so a
//! path not yet taught about partitioning fails loudly instead of answering
//! from an empty table. These tests pin the three parts of that: every data
//! method checks first (structurally), every one of them really errs on a
//! shell (behaviourally), and a shell never holds forgetting back.

use rclite::Arc;
use smallvec::SmallVec;

use crate::core::db_type::DbType;
use crate::core::mvcc::TransactionManager;
use crate::core::mvcc::Visibility;
use crate::core::mvcc::status::StatusLog;
use crate::core::mvcc::transaction::FROZEN_TX;
use crate::core::row::row::{ColumnWritePayload, RowFetch, RowWrite};
use crate::core::row::table::Table;
use crate::core::rql::lexer_ct::CreateColumnData;
use crate::core::storage_providers::file_sync::LocalStorageProvider;
use crate::memory_pool::MEMORY_POOL;

/// `pub`/`pub(crate)` methods of `impl Table` that touch only the schema,
/// the transaction wiring or a counter, and so may run on a shell.
const SCHEMA_ONLY: &[&str] = &[
    "scope_cache_to_database",
    "enable_versioning",
    "visibility_for",
    "visibility_now_for",
    "version_horizon",
    "forget_horizon",
    "register_unique_key",
    "dead_stamps",
    "in_place_column_updates",
    "is_shell",
    "data",
];

/// Every data method `a_shell_refuses_every_data_method` calls. The
/// structural test requires this to cover every data method it finds, so a
/// new one cannot be added without also being proven to refuse.
const CALLED_ON_SHELL: &[&str] = &[
    "io_rows",
    "io_pointers",
    "io_filters",
    "io_sequence",
    "filters",
    "indexes",
    "last_row_id",
    "ids_reserved_upto",
    "reserve_insert_id",
    "neutralize_aborted_now",
    "vacuum_now",
    "compact",
    "vacuum_stats",
    "reclaimable_bytes",
    "vacuum_enabled",
    "set_vacuum_enabled",
    "locate_row_by_id",
    "filters_ready",
    "wait_filters_ready",
    "backfill_filters",
    "backfill_filters_from",
    "checkpoint_filters",
    "build_candidates",
    "insert_row",
    "insert_row_in_txn",
    "query_row",
    "count_rows_in_txn",
    "query_row_in_txn",
    "query_row_with_visibility",
    "query_row_bytes",
    "query_row_bytes_in_txn",
    "query_row_bytes_reporting_in_txn",
    "query_row_bytes_with_visibility",
    "delete_rows",
    "delete_rows_in_txn",
    "delete_rows_as_of",
    "update_rows",
    "update_rows_in_txn",
    "update_rows_with_expressions",
    "update_rows_as_of",
];

// ---------------------------------------------------------------------------
// A small reader for table.rs: enough Rust lexing to find method bodies.
// ---------------------------------------------------------------------------

/// `src` with comments removed and string and char literal contents blanked,
/// byte for byte the same length, so braces and `fn` inside them are not
/// mistaken for code.
fn strip(src: &str) -> String {
    let b = src.as_bytes();
    let mut out = b.to_vec();
    let mut i = 0;
    let blank = |out: &mut Vec<u8>, from: usize, to: usize| {
        for c in out.iter_mut().take(to).skip(from) {
            if *c != b'\n' {
                *c = b' ';
            }
        }
    };
    while i < b.len() {
        if b[i..].starts_with(b"//") {
            let end = b[i..].iter().position(|&c| c == b'\n').map_or(b.len(), |p| i + p);
            blank(&mut out, i, end);
            i = end;
        } else if b[i..].starts_with(b"/*") {
            let end = src[i + 2..].find("*/").map_or(b.len(), |p| i + 2 + p + 2);
            blank(&mut out, i, end);
            i = end;
        } else if b[i] == b'r'
            && (i == 0 || !(b[i - 1].is_ascii_alphanumeric() || b[i - 1] == b'_'))
            && matches!(b.get(i + 1), Some(b'"') | Some(b'#'))
        {
            let hashes = b[i + 1..].iter().take_while(|&&c| c == b'#').count();
            if b.get(i + 1 + hashes) != Some(&b'"') {
                i += 1;
                continue;
            }
            let close = format!("\"{}", "#".repeat(hashes));
            let body = i + 2 + hashes;
            let end = src[body..].find(&close).map_or(b.len(), |p| body + p + close.len());
            blank(&mut out, body, end - close.len());
            i = end;
        } else if b[i] == b'"' {
            let mut j = i + 1;
            while j < b.len() && b[j] != b'"' {
                j += if b[j] == b'\\' { 2 } else { 1 };
            }
            blank(&mut out, i + 1, j);
            i = j + 1;
        } else if b[i] == b'\'' {
            // A char literal, or a lifetime (`'a`, `'_`), which has no
            // closing quote.
            if b.get(i + 1) == Some(&b'\\') {
                let end = b[i + 2..].iter().position(|&c| c == b'\'').map_or(b.len(), |p| i + 2 + p + 1);
                blank(&mut out, i + 1, end - 1);
                i = end;
            } else if b.get(i + 2) == Some(&b'\'') {
                blank(&mut out, i + 1, i + 2);
                i += 3;
            } else {
                i += 1;
            }
        } else {
            i += 1;
        }
    }
    String::from_utf8(out).expect("blanking keeps the text ASCII-safe")
}

/// The index of the bracket closing the one at `open`.
fn matching(code: &[u8], open: usize) -> usize {
    let (o, c) = (code[open], if code[open] == b'{' { b'}' } else { b')' });
    let mut depth = 0usize;
    for (k, &ch) in code.iter().enumerate().skip(open) {
        if ch == o {
            depth += 1;
        } else if ch == c {
            depth -= 1;
            if depth == 0 {
                return k;
            }
        }
    }
    panic!("unbalanced bracket at {}", open);
}

struct Method {
    name: String,
    /// The body's first statement, comments stripped.
    first: String,
}

/// Every `pub`/`pub(..)` method with a `self` receiver in the
/// `impl<S: StorageIO> Table<S>` blocks of `src`.
fn public_methods(src: &str) -> Vec<Method> {
    let code = strip(src);
    let bytes = code.as_bytes();
    let header = "impl<S: StorageIO> Table<S> {";
    let mut methods = Vec::new();
    let mut from = 0;
    while let Some(at) = code[from..].find(header) {
        let open = from + at + header.len() - 1;
        let close = matching(bytes, open);
        // Items at depth 1 of this block.
        let mut k = open + 1;
        while k < close {
            match bytes[k] {
                b'{' => {
                    k = matching(bytes, k) + 1;
                    continue;
                }
                b'f' if code[k..].starts_with("fn ")
                    && !(bytes[k - 1].is_ascii_alphanumeric() || bytes[k - 1] == b'_') =>
                {
                    let line_start = code[..k].rfind('\n').map_or(0, |p| p + 1);
                    // Visibility is the first token: `pub`, `pub(crate)`,
                    // `pub(super)` or `pub(in ..)`, whatever follows it.
                    let public = code[line_start..k]
                        .split_whitespace()
                        .next()
                        .is_some_and(|t| t == "pub" || t.starts_with("pub("));
                    let name_end = k + 3 + code[k + 3..].find(|c: char| !(c.is_alphanumeric() || c == '_')).unwrap();
                    let name = code[k + 3..name_end].to_string();
                    let params_open = name_end + code[name_end..].find('(').unwrap();
                    let params_close = matching(bytes, params_open);
                    let params = code[params_open + 1..params_close].trim_start();
                    let body_open = params_close + code[params_close..].find('{').unwrap();
                    let body_close = matching(bytes, body_open);
                    let has_self = params.starts_with("&self")
                        || params.starts_with("&mut self")
                        || params.starts_with("self")
                        || params.starts_with("mut self");
                    if public && has_self {
                        let body = &code[body_open + 1..body_close];
                        let first = body.split(';').next().unwrap_or("").trim().to_string();
                        methods.push(Method { name, first });
                    }
                    k = body_close + 1;
                    continue;
                }
                _ => {}
            }
            k += 1;
        }
        from = close;
    }
    methods
}

/// I8, structurally: each `pub`/`pub(crate)` `self` method of `impl Table`
/// begins with `self.data()` or is named schema-only. A method that read a
/// data field without checking would answer from emptiness on a shell.
#[test]
fn every_data_method_checks_first() {
    let methods = public_methods(include_str!("table.rs"));

    // A reader that found nothing would pass vacuously.
    assert!(methods.len() >= 40, "found only {} methods; the reader is broken", methods.len());
    // Including non-async `pub(crate) fn`s, which a reader matching on the
    // text before `fn` once dropped.
    for known in [
        "count_rows_in_txn",
        "insert_row",
        "query_row_bytes_reporting_in_txn",
        "visibility_for",
        "data",
        "is_shell",
        "indexes",
    ] {
        assert!(methods.iter().any(|m| m.name == known), "the reader missed `{}`", known);
    }

    let unchecked: Vec<&str> = methods
        .iter()
        .filter(|m| !SCHEMA_ONLY.contains(&m.name.as_str()) && !m.first.contains("self.data()"))
        .map(|m| m.name.as_str())
        .collect();
    assert!(
        unchecked.is_empty(),
        "these methods do not start with `self.data()` and are not on SCHEMA_ONLY: {:?}",
        unchecked
    );

    // Every data method is also exercised on a shell.
    let untested: Vec<&str> = methods
        .iter()
        .map(|m| m.name.as_str())
        .filter(|n| !SCHEMA_ONLY.contains(n) && !CALLED_ON_SHELL.contains(n))
        .collect();
    assert!(
        untested.is_empty(),
        "data methods `a_shell_refuses_every_data_method` does not call: {:?}",
        untested
    );
}

/// The reader itself: a `pub fn` that reads a field without the check is
/// found, a checked one is not, and a private one is out of scope.
#[test]
fn the_reader_finds_an_unchecked_method() {
    let src = r#"
impl<S: StorageIO> Table<S> {
    /// fn hidden(&self) { in a comment }
    pub fn probe(&self) -> u64 {
        let s = "{ not a brace";
        self.len.load(Relaxed)
    }
    pub(crate) async fn checked(&self) -> Result<()> {
        let d = self.data()?;
        Ok(())
    }
    fn private(&self) {}
    pub fn associated(x: u8) -> u8 { x }
    pub(crate) fn plain(&self) -> u64 { self.len.load(Relaxed) }
    pub(super) fn sibling(&self) -> u64 { self.len.load(Relaxed) }
    pub(in crate::core) const fn scoped(&self) -> u64 { 0 }
}
"#;
    let methods = public_methods(src);
    let names: Vec<&str> = methods.iter().map(|m| m.name.as_str()).collect();
    assert_eq!(names, ["probe", "checked", "plain", "sibling", "scoped"]);
    assert!(!methods[0].first.contains("self.data()"));
    assert!(methods[1].first.contains("self.data()"));
    assert!(!methods[2].first.contains("self.data()"));
}

// ---------------------------------------------------------------------------
// Behaviour.
// ---------------------------------------------------------------------------

fn init() {
    let _ = crate::MAX_PERMITS_THREADS.set(4);
    let _ = crate::BATCH_SIZE.set(512);
    let _ = crate::core::processor::concurrent_processor::ENABLE_CACHE.set(false);
}

async fn plain_table(dir: &std::path::Path, name: &str) -> Table<LocalStorageProvider> {
    init();
    let io = Arc::new(LocalStorageProvider::new(dir.to_str().unwrap(), Some(name)).await);
    Table::new(
        name,
        io,
        vec![
            CreateColumnData {
                name: "id".into(),
                data_type: DbType::U64,
            },
            CreateColumnData {
                name: "label".into(),
                data_type: DbType::STRING,
            },
        ],
    )
    .await
}

/// A shell with `source`'s schema, renamed so its errors name it.
fn shell_of(source: &Table<LocalStorageProvider>, name: &str) -> Table<LocalStorageProvider> {
    let mut schema = source.schema.clone();
    schema.name = name.to_string();
    Table::partition_shell(source.io_schema.clone(), schema)
}

fn payload(bytes: &[u8], write_order: u32, column_type: DbType) -> ColumnWritePayload {
    let mut mb = MEMORY_POOL.acquire(bytes.len());
    mb.into_slice_mut().copy_from_slice(bytes);
    let size = column_type.get_size();
    ColumnWritePayload {
        data: mb,
        write_order,
        column_type,
        size,
    }
}

fn row(id: u64) -> RowWrite {
    let mut rw = RowWrite {
        columns_writing_data: SmallVec::new(),
    };
    rw.columns_writing_data.push(payload(&id.to_le_bytes(), 0, DbType::U64));
    rw.columns_writing_data.push(payload(b"v", 1, DbType::STRING));
    rw
}

fn fetch(t: &Table<LocalStorageProvider>) -> RowFetch {
    Table::<LocalStorageProvider>::build_full_row_fetch(&t.schema.fields)
}

/// I8, behaviourally: every data method errs on a shell, saying the table is
/// partitioned and that the operation is not supported (pgwire's 0A000).
/// Catches a method whose check exists but whose error is swallowed into an
/// empty answer.
#[tokio::test(flavor = "multi_thread")]
async fn a_shell_refuses_every_data_method() {
    let dir = tempfile::tempdir().unwrap();
    let source = plain_table(dir.path(), "seam_src").await;
    let shell = shell_of(&source, "seam_shell");
    assert!(shell.is_shell());
    assert!(!source.is_shell());

    let mut failures: Vec<String> = Vec::new();
    let mut called: Vec<&str> = Vec::new();
    let mut expect = |name: &'static str, outcome: Result<(), std::io::Error>| {
        called.push(name);
        match outcome {
            Ok(()) => failures.push(format!("{} succeeded on a shell", name)),
            Err(e) => {
                let text = e.to_string();
                if !(text.contains("partitioned") && text.contains("not supported") && text.contains("seam_shell")) {
                    failures.push(format!("{} erred without the refusal: {}", name, text));
                }
            }
        }
    };
    fn unit<T>(r: std::io::Result<T>) -> std::io::Result<()> {
        r.map(|_| ())
    }

    let fields = shell.schema.fields.to_vec();
    let vis = || Visibility::All;

    expect("io_rows", unit(shell.io_rows()));
    expect("io_pointers", unit(shell.io_pointers()));
    expect("io_filters", unit(shell.io_filters()));
    expect("io_sequence", unit(shell.io_sequence()));
    expect("filters", unit(shell.filters()));
    expect("indexes", unit(shell.indexes()));
    expect("last_row_id", unit(shell.last_row_id()));
    expect("ids_reserved_upto", unit(shell.ids_reserved_upto()));
    expect("reserve_insert_id", unit(shell.reserve_insert_id().await));
    expect("neutralize_aborted_now", unit(shell.neutralize_aborted_now().await));
    expect("vacuum_now", unit(shell.vacuum_now().await));
    expect("compact", unit(shell.compact().await));
    expect("vacuum_stats", unit(shell.vacuum_stats()));
    expect("reclaimable_bytes", unit(shell.reclaimable_bytes()));
    expect("vacuum_enabled", unit(shell.vacuum_enabled()));
    expect("set_vacuum_enabled", unit(shell.set_vacuum_enabled(true)));
    expect("locate_row_by_id", unit(shell.locate_row_by_id(1).await));
    expect("filters_ready", unit(shell.filters_ready()));
    expect("wait_filters_ready", unit(shell.wait_filters_ready().await));
    expect("backfill_filters", unit(shell.backfill_filters().await));
    expect("backfill_filters_from", unit(shell.backfill_filters_from(0).await));
    expect("checkpoint_filters", unit(shell.checkpoint_filters().await));
    expect("build_candidates", unit(shell.build_candidates("id = 1", None).await));
    expect("insert_row", unit(shell.insert_row(row(1)).await));
    expect("insert_row_in_txn", unit(shell.insert_row_in_txn(row(1), FROZEN_TX).await));
    expect(
        "query_row",
        unit(shell.query_row("", &fields, fetch(&shell), fetch(&shell), None, None).await),
    );
    expect(
        "count_rows_in_txn",
        unit(shell.count_rows_in_txn("", &fields, fetch(&shell), FROZEN_TX).await),
    );
    expect(
        "query_row_in_txn",
        unit(shell.query_row_in_txn("", &fields, fetch(&shell), fetch(&shell), None, None, FROZEN_TX).await),
    );
    expect(
        "query_row_with_visibility",
        unit(shell.query_row_with_visibility("", &fields, fetch(&shell), fetch(&shell), None, None, vis()).await),
    );
    expect(
        "query_row_bytes_with_visibility",
        unit(
            shell
                .query_row_bytes_with_visibility("", &fields, fetch(&shell), fetch(&shell), None, None, vis(), false)
                .await,
        ),
    );
    expect(
        "query_row_bytes",
        unit(shell.query_row_bytes("", &fields, fetch(&shell), fetch(&shell), None, None).await),
    );
    expect(
        "query_row_bytes_in_txn",
        unit(shell.query_row_bytes_in_txn("", &fields, fetch(&shell), fetch(&shell), None, None, FROZEN_TX).await),
    );
    expect(
        "query_row_bytes_reporting_in_txn",
        unit(
            shell
                .query_row_bytes_reporting_in_txn("", &fields, fetch(&shell), fetch(&shell), None, None, FROZEN_TX)
                .await,
        ),
    );
    expect("delete_rows", unit(shell.delete_rows("id = 1", fetch(&shell), None).await));
    expect(
        "delete_rows_in_txn",
        unit(shell.delete_rows_in_txn("id = 1", fetch(&shell), None, FROZEN_TX).await),
    );
    expect(
        "delete_rows_as_of",
        unit(shell.delete_rows_as_of("id = 1", fetch(&shell), None, FROZEN_TX, Some(vis()), None).await),
    );
    let set = || {
        let mut s = SmallVec::new();
        s.push(payload(b"w", 1, DbType::STRING));
        s
    };
    expect("update_rows", unit(shell.update_rows("id = 1", fetch(&shell), set(), None).await));
    expect(
        "update_rows_in_txn",
        unit(shell.update_rows_in_txn("id = 1", fetch(&shell), set(), None, FROZEN_TX).await),
    );
    expect(
        "update_rows_with_expressions",
        unit(shell.update_rows_with_expressions("id = 1", fetch(&shell), set(), &[], None, FROZEN_TX).await),
    );
    expect(
        "update_rows_as_of",
        unit(shell.update_rows_as_of("id = 1", fetch(&shell), set(), &[], None, FROZEN_TX, Some(vis()), None).await),
    );

    assert!(failures.is_empty(), "{:#?}", failures);
    let mut missing: Vec<&&str> = CALLED_ON_SHELL.iter().filter(|n| !called.contains(n)).collect();
    missing.sort();
    assert!(missing.is_empty(), "listed in CALLED_ON_SHELL but never called: {:?}", missing);

    // The schema-only surface still answers.
    assert!(matches!(shell.visibility_for(FROZEN_TX), Visibility::All));
    assert_eq!(shell.dead_stamps(), 0);
    assert_eq!(shell.in_place_column_updates(), 0);
    assert_eq!(shell.schema.fields.len(), source.schema.fields.len());

    // And the plain table it was cloned from is untouched by all of it.
    source.insert_row(row(7)).await.unwrap();
    let n = source.count_rows_in_txn("", &source.schema.fields.to_vec(), fetch(&source), FROZEN_TX).await.unwrap();
    assert_eq!(n, 1);
}

/// A shell takes no place in the forget registry. A registered handle that
/// no sweep ever advances holds the database's forget horizon at its first
/// value for good, and the status bitmap then grows without limit.
#[tokio::test(flavor = "multi_thread")]
async fn a_shell_does_not_pin_forgetting() {
    let dir = tempfile::tempdir().unwrap();
    let manager = std::sync::Arc::new(TransactionManager::new(StatusLog::in_memory_with(true)));
    let plain = plain_table(dir.path(), "seam_forget_plain").await;
    plain.enable_versioning(manager.clone());
    let shell = shell_of(&plain, "seam_forget_shell");
    shell.enable_versioning(manager.clone());
    assert_eq!(shell.forget_horizon(), None, "a shell registered for forgetting");
    // Versioning still reaches the shell's snapshot rule.
    assert!(!matches!(shell.visibility_for(FROZEN_TX), Visibility::All));

    // History for the plain table to sweep: some aborted inserts among
    // committed ones, then enough later transactions to settle past them.
    for id in 0..300u64 {
        let tx = manager.begin();
        plain.insert_row_in_txn(row(id), tx).await.unwrap();
        if id % 3 == 0 {
            manager.abort(tx);
        } else {
            manager.commit(tx);
        }
    }
    for _ in 0..9_000 {
        let tx = manager.begin();
        manager.commit(tx);
    }
    manager.settle_now();

    let swept = plain.neutralize_aborted_now().await.unwrap();
    assert!(swept.advanced_to.is_some(), "the plain table's sweep did not advance");
    assert!(
        manager.status().forgotten_below() > 0,
        "the forget horizon is pinned: something registered that never sweeps"
    );
}

/// The sites S1 converted, reached with a shell in the live map: each errs
/// with the refusal rather than answering from an empty table, and the shell
/// is still there afterwards. Nothing creates a shell through SQL yet, so it
/// is put into the map by hand, as phase 2a's `CREATE … PARTITION BY` will.
#[tokio::test(flavor = "multi_thread")]
async fn a_shell_in_a_database_errs_rather_than_answering_empty() {
    use crate::core::database::Database;
    use crate::core::introspect::{execute as introspect, parse as parse_introspect};

    init();
    let dir = tempfile::tempdir().unwrap();
    let database = Arc::new(Database::new(dir.path().to_str().unwrap()).await);
    let sql = |text: &'static str| {
        let database = database.clone();
        async move {
            let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(text)
                .unwrap_or_else(|| panic!("unrecognised: {text}"));
            crate::core::rql::executor::execute(purpose, database).await
        }
    };
    // Failures are collected rather than asserted one by one, so one run
    // reports every site that answered.
    let mut failures: Vec<String> = Vec::new();
    // Checks other than a refusal, kept apart while `refused` holds `failures`.
    let mut extra: Vec<String> = Vec::new();
    let mut refused = |what: &str, response: &[u8]| {
        if response.first() != Some(&3u8) {
            failures.push(format!("{} answered on a shell", what));
            return;
        }
        let text = String::from_utf8_lossy(&response[1..]).to_string();
        if !(text.contains("partitioned") && text.contains("not supported")) {
            failures.push(format!("{} erred without the refusal: {}", what, text));
        }
    };

    // A schema and a schema file of its own, with one index of each kind so
    // there is something to drop, then the table is taken out of the map (its
    // files stay) and a shell named `pt` takes its place. `plain_side` stays,
    // for the statements that walk every table.
    for statement in [
        "CREATE TABLE pt_src (id INT, label VARCHAR, doc JSONB)",
        "CREATE INDEX ix_src ON pt_src (id)",
        "CREATE INDEX px_src ON pt_src ((doc->>'a'))",
        "CREATE TABLE plain_side (id INT)",
    ] {
        let response = sql(statement).await;
        assert_ne!(response.first(), Some(&3u8), "{statement}: {}", String::from_utf8_lossy(&response));
    }
    let (_, source) = database.tables.remove("pt_src").unwrap();
    let mut schema = source.schema.clone();
    schema.name = "pt".to_string();
    let io_schema = source.io_schema.clone();
    database
        .tables
        .insert("pt".to_string(), Arc::new(Table::partition_shell(io_schema.clone(), schema)));
    // What the shell's schema file holds, to show a refused DDL wrote nothing.
    let on_disk = || {
        let io_schema = io_schema.clone();
        async move {
            let s = crate::core::row::schema::TableSchema::load(io_schema).await.unwrap();
            let live = |names: Vec<(String, bool)>| {
                let mut v: Vec<String> = names.into_iter().filter(|(_, d)| !d).map(|(n, _)| n).collect();
                v.sort();
                v
            };
            (
                s.fields.iter().filter(|f| !f.is_deleted).count(),
                live(s.secondary_indexes.iter().map(|i| (i.name.clone(), i.is_deleted)).collect()),
                live(s.path_indexes.iter().map(|i| (i.name.clone(), i.is_deleted)).collect()),
            )
        }
    };
    let disk_before = on_disk().await;
    assert_eq!(disk_before.1, ["ix_src"]);
    assert_eq!(disk_before.2, ["px_src"]);

    match database.drop_table("pt").await {
        Ok(_) => refused("drop_table", &[0u8]),
        Err(e) => refused("drop_table", format!("\u{3}{}", e).as_bytes()),
    }
    let shell_kept = database.tables.get("pt").is_some_and(|t| t.is_shell());

    for statement in ["SHOW TABLES", "SHOW DATABASE STATS", "SHOW TABLE STATS pt"] {
        let parsed = parse_introspect(statement).unwrap().unwrap();
        refused(statement, &introspect(&parsed, &database).await);
    }
    for statement in [
        "SELECT * FROM pg_index",
        "SELECT * FROM pg_indexes",
        "SELECT * FROM pg_class",
        "SELECT * FROM pg_stat_user_tables",
    ] {
        // Built here as pgwire builds them before running the statement.
        match crate::core::rql::catalog_views::materialize(statement, &database, FROZEN_TX, None).await {
            Ok(_) => refused(statement, &[0u8]),
            Err(e) => refused(statement, format!("\u{3}{}", e).as_bytes()),
        }
    }

    // DDL that writes the schema file and then reopens the table: refused
    // before the write, so the file and the shell are as they were.
    for statement in [
        "CREATE INDEX ix ON pt (id)",
        "CREATE INDEX px ON pt ((doc->>'b'))",
        "DROP INDEX ix_src",
        "DROP INDEX px_src",
        "ALTER TABLE pt ADD COLUMN x INT",
    ] {
        refused(statement, &sql(statement).await);
    }
    let disk_after = on_disk().await;
    if disk_after != disk_before {
        extra.push(format!("refused DDL changed the schema file: {:?} -> {:?}", disk_before, disk_after));
    }
    if database.tables.get("pt").map(|t| t.schema.fields.len()) != Some(source.schema.fields.len()) {
        extra.push("refused DDL changed the shell's schema".to_string());
    }

    // `SET CONFIG vacuum` walks every table: refused before it touches any.
    // The plain table is entered under many names so that, were the shell
    // checked only on reaching it, some name would almost surely come first
    // (the map's order is random; 63 aliases leave a 1 in 64 miss).
    let plain = database.tables.get("plain_side").unwrap().clone();
    let vacuum_before = plain.vacuum_enabled().unwrap();
    for i in 0..63 {
        database.tables.insert(format!("plain_alias_{i}"), plain.clone());
    }
    let set = format!("SET CONFIG vacuum.enabled = {}", !vacuum_before);
    let parsed = parse_introspect(&set).unwrap().unwrap();
    refused("SET CONFIG vacuum", &introspect(&parsed, &database).await);
    for i in 0..63 {
        database.tables.remove(&format!("plain_alias_{i}"));
    }
    if plain.vacuum_enabled().unwrap() != vacuum_before {
        extra.push("SET CONFIG vacuum changed a plain table before refusing".to_string());
    }

    #[cfg(feature = "storage_pools")]
    match crate::core::storage::sql::execute_structured("SHOW STORAGE PLACEMENTS", &database) {
        Ok(_) => refused("SHOW STORAGE PLACEMENTS", &[0u8]),
        Err(e) => refused("SHOW STORAGE PLACEMENTS", format!("\u{3}{}", e).as_bytes()),
    }

    // The name-keyed `Database` methods that rebuild the map entry from file
    // names with `Table::new`: on a shell each would leave an empty plain
    // table in its place. Last, since one that got through replaces the shell.
    match database.rename_table("plain_side", "pt").await {
        Ok(_) => refused("rename onto pt", &[0u8]),
        Err(e) => refused("rename onto pt", format!("\u{3}{}", e).as_bytes()),
    }
    match database.reload_table("pt").await {
        Ok(_) => refused("reload_table", &[0u8]),
        Err(e) => refused("reload_table", format!("\u{3}{}", e).as_bytes()),
    }
    #[cfg(feature = "storage_pools")]
    match database.move_table_to("pt", "memory").await {
        Ok(_) => refused("move_table_to", &[0u8]),
        Err(e) => refused("move_table_to", format!("\u{3}{}", e).as_bytes()),
    }
    match database.rename_table("pt", "pt_renamed").await {
        Ok(_) => refused("rename of pt", &[0u8]),
        Err(e) => refused("rename of pt", format!("\u{3}{}", e).as_bytes()),
    }

    failures.extend(extra);
    if !shell_kept {
        failures.push("the refused DROP TABLE removed the shell from the map".to_string());
    }
    if !database.tables.get("pt").is_some_and(|t| t.is_shell()) {
        failures.push("a refused statement removed the shell".to_string());
    }
    assert!(failures.is_empty(), "{:#?}", failures);
}
