//! Tables whose files would be another table's sidecars.
//!
//! A table's files are its name plus each of [`RENAMED_SUFFIXES`], so `x` and
//! `x_pointers` share `x_pointers.db`: `x`'s pointers, `x_pointers`'s rows.
//! These tests walk the list itself, so a sidecar added to it is covered here
//! the day it is added, and read the source for every `.db` name the engine
//! builds, so a sidecar built without being listed fails instead of slipping
//! past the guard.

use super::*;
use crate::core::rql::lexer_ct::CreateColumnData;

async fn open_db(dir: &std::path::Path) -> Database {
    Database::new_named(&dir.to_string_lossy(), false, "sidecarclash").await
}

fn create_stmt(name: &str) -> CreateTable {
    CreateTable {
        table_name: name.to_string(),
        columns: vec![CreateColumnData {
            name: "id".to_string(),
            data_type: DbType::I32,
        }],
        unique_columns: Vec::new(),
        foreign_keys: Vec::new(),
        defaults: Vec::new(),
        json_columns: Vec::new(),
        unique_keys: Vec::new(),
        type_modifiers: Vec::new(),
        generated: Vec::new(),
        checks: Vec::new(),
        primary_key: None,
        if_not_exists: false,
        is_immutable: false,
    }
}

async fn create(db: &Database, name: &str) {
    db.add_table(create_stmt(name))
        .await
        .unwrap_or_else(|e| panic!("create {name}: {e}"));
}

async fn insert_id(db: &Database, table: &str, value: i32) {
    let table = db.tables.get(table).expect("table").clone();
    let mut mb = MEMORY_POOL.acquire(4);
    mb.into_slice_mut().copy_from_slice(&value.to_le_bytes());
    let row = RowWrite {
        columns_writing_data: smallvec![ColumnWritePayload {
            column_type: DbType::I32,
            size: 4,
            data: mb,
            write_order: 0,
        }],
    };
    table.insert_row(row).await.expect("insert");
}

fn fetch_all(fields: &SmallVec<[SchemaField; 20]>) -> crate::core::row::row::RowFetch {
    crate::core::row::row::RowFetch {
        columns_fetching_data: fields
            .iter()
            .map(|f| crate::core::row::row::ColumnFetchingData {
                column_offset: f.offset as u32,
                column_type: f.db_type.clone(),
                size: f.size,
                schema_id: f.write_order,
            })
            .collect(),
    }
}

/// The table's ids, sorted, or `None` when it did not come back.
async fn ids(db: &Database, table: &str) -> Option<Vec<i32>> {
    let table = db.tables.get(table)?.clone();
    let rows = table
        .query_row(
            "",
            &table.schema.fields.to_vec(),
            fetch_all(&table.schema.fields),
            fetch_all(&table.schema.fields),
            None,
            None,
        )
        .await
        .ok()?;
    let mut ids: Vec<i32> = rows
        .iter()
        .filter_map(|r| r.columns.first())
        .filter_map(|c| c.data.into_slice().try_into().ok().map(i32::from_le_bytes))
        .collect();
    ids.sort();
    Some(ids)
}

/// Runs `text` through the executor: the reply's status byte and its text.
async fn sql(db: &rclite::Arc<Database>, text: &str) -> (u8, String) {
    let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(text).expect("parse");
    let reply = crate::core::rql::executor::execute(purpose, db.clone()).await;
    let status = *reply.first().expect("a reply");
    (status, String::from_utf8_lossy(&reply[1..]).into_owned())
}

fn files_in(dir: &std::path::Path) -> Vec<String> {
    let mut files: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    files.sort();
    files
}

/// The suffixes a real table has on disk once it has been reopened (several
/// sidecars are created only when a schema is loaded), `.tmp` siblings folded
/// into the file they stage.
async fn observed_suffixes() -> Vec<String> {
    const PROBE: &str = "probe";
    let dir = tempfile::tempdir().unwrap();
    {
        let db = open_db(dir.path()).await;
        create(&db, PROBE).await;
        insert_id(&db, PROBE, 1).await;
    }
    let db = open_db(dir.path()).await;
    assert_eq!(ids(&db, PROBE).await, Some(vec![1]));
    drop(db);
    let mut seen: Vec<String> = files_in(dir.path())
        .into_iter()
        .filter_map(|f| f.strip_prefix(PROBE).map(str::to_string))
        .filter(|rest| rest.starts_with('_') || rest.starts_with('.'))
        .map(|rest| rest.strip_suffix(".tmp").map(str::to_string).unwrap_or(rest))
        .collect();
    seen.sort();
    seen.dedup();
    seen
}

/// What the walk covers: the list, and whatever a real table makes that the
/// list does not have. The second part is what makes a suffix dropped from the
/// list fail here rather than merely go untested.
async fn suffix_walk() -> Vec<String> {
    let mut walk: Vec<String> = RENAMED_SUFFIXES.iter().map(|s| s.to_string()).collect();
    for seen in observed_suffixes().await {
        if !walk.contains(&seen) {
            walk.push(seen);
        }
    }
    walk
}

/// Every `ext` for which `t` and `t<ext>` share a file: `t + long` is
/// `t<ext> + short` whenever `short` ends `long`. Since every suffix ends in
/// `.db`, that is at least one per suffix other than `.db` itself.
fn clashing_extensions(walk: &[String]) -> Vec<String> {
    let mut exts = Vec::new();
    for long in walk {
        for short in walk {
            if long != short && long.ends_with(short.as_str()) {
                let ext = long[..long.len() - short.len()].to_string();
                if !exts.contains(&ext) {
                    exts.push(ext);
                }
            }
        }
    }
    exts
}

/// The files both names would own, as either would spell them.
fn shared_files(a: &str, b: &str) -> Vec<String> {
    let of_b: Vec<String> = table_file_names(b).collect();
    table_file_names(a)
        .filter(|f| of_b.iter().any(|g| g.eq_ignore_ascii_case(f)))
        .collect()
}

/// A refusal of `newcomer` because of `owner`, naming one file they share.
fn assert_names_the_file(err: &str, newcomer: &str, owner: &str, context: &str) {
    let shared = shared_files(newcomer, owner);
    assert!(!shared.is_empty(), "{context}: {newcomer} and {owner} share nothing");
    assert!(
        shared.iter().any(|f| err.contains(f.as_str())),
        "{context}: the refusal should name a shared file ({shared:?}): {err}"
    );
    assert!(
        err.contains(&format!("table \"{owner}\"")),
        "{context}: the refusal should name the table that owns it: {err}"
    );
    assert_eq!(
        crate::pgwire::sqlstate_for(err),
        "42P07",
        "{context}: reported as duplicate_table: {err}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn every_sidecar_name_is_refused_in_both_orders_on_create_and_rename() {
    let walk = suffix_walk().await;
    let exts = clashing_extensions(&walk);
    assert!(
        exts.len() + 1 >= walk.len(),
        "every suffix but `.db` should give a clashing name: {exts:?} from {walk:?}"
    );

    for ext in &exts {
        let short = "t".to_string();
        let long = format!("t{ext}");
        for (existing, newcomer) in [(&short, &long), (&long, &short)] {
            let context = format!("{newcomer} beside {existing}");
            let dir = tempfile::tempdir().unwrap();
            {
                let db = rclite::Arc::new(open_db(dir.path()).await);
                create(&db, existing).await;
                insert_id(&db, existing, 7).await;
                create(&db, "bystander").await;

                let err = db.add_table(create_stmt(newcomer)).await.unwrap_err();
                assert_eq!(err.kind(), io::ErrorKind::AlreadyExists, "create {context}");
                assert_names_the_file(&err.to_string(), newcomer, existing, &context);
                assert!(!db.tables.contains_key(newcomer.as_str()), "{context}");
                assert!(!db.catalogue_contains(newcomer).await, "{context}: catalogue row");

                let err = db.rename_table("bystander", newcomer).await.unwrap_err();
                assert_eq!(err.kind(), io::ErrorKind::AlreadyExists, "rename {context}");
                assert_names_the_file(&err.to_string(), newcomer, existing, &context);
                assert!(db.tables.contains_key("bystander"), "{context}: lost the bystander");
                assert!(!db.tables.contains_key(newcomer.as_str()), "{context}");

                // And what a client sees: the statement, not just the call.
                let (status, text) = sql(&db, &format!("CREATE TABLE {newcomer} (id INT)")).await;
                assert_eq!(status, 3, "SQL create {context}: {text}");
                assert_names_the_file(&text, newcomer, existing, &context);
                let (status, text) =
                    sql(&db, &format!("ALTER TABLE bystander RENAME TO {newcomer}")).await;
                assert_eq!(status, 3, "SQL rename {context}: {text}");
                assert_names_the_file(&text, newcomer, existing, &context);
            }

            let db = open_db(dir.path()).await;
            assert_eq!(ids(&db, existing).await, Some(vec![7]), "{context}: damaged");
            assert!(!db.tables.contains_key(newcomer.as_str()), "{context}: after reopen");
        }
    }
}

#[test]
fn the_name_arithmetic_finds_both_directions_for_every_suffix() {
    // The same walk without a database, over every suffix pair and a name
    // carrying an underscore of its own: the guard's arithmetic, both ways.
    let walk: Vec<String> = RENAMED_SUFFIXES.iter().map(|s| s.to_string()).collect();
    for ext in clashing_extensions(&walk) {
        for base in ["t", "my_table", "a_schema"] {
            let long = format!("{base}{ext}");
            let one = shared_table_file(&long, [base]).unwrap_or_else(|| panic!("{long} vs {base}"));
            assert_eq!(one.owner, base);
            let other = shared_table_file(base, [long.as_str()])
                .unwrap_or_else(|| panic!("{base} vs {long}"));
            assert_eq!(other.owner, long);
            assert_eq!(one.file, other.file, "the same file from both sides");
        }
    }
}

#[test]
fn names_sharing_only_a_prefix_share_no_file() {
    // Not a `starts_with`: these are neighbours, not sidecars.
    for (a, b) in [
        ("t", "ts"),
        ("t", "t_point"),
        ("t", "t_pointerss"),
        ("t", "t_schema_"),
        ("t", "t_schemas"),
        ("t", "t_schema_FIELD"),
        ("t", "t_sequel"),
        ("t", "tflt"),
        ("t_seq", "t_seqs"),
        ("role", "roles"),
    ] {
        assert_eq!(shared_table_file(a, [b]), None, "{a} vs {b}");
        assert_eq!(shared_table_file(b, [a]), None, "{b} vs {a}");
    }
}

#[test]
fn every_suffix_is_a_db_file_name_tail() {
    // What the arithmetic and `reserved_file_owner` lean on: a table file
    // always ends in `.db` (so `.tmp` siblings and the engine's other files
    // are nobody's), and the cut a suffix makes is at `_` or `.`.
    for suffix in RENAMED_SUFFIXES {
        assert!(suffix.ends_with(".db"), "{suffix}");
        assert!(suffix.starts_with('_') || *suffix == ".db", "{suffix}");
        assert!(suffix.is_ascii(), "{suffix}");
    }
    let mut sorted = RENAMED_SUFFIXES.to_vec();
    sorted.sort();
    sorted.dedup();
    assert_eq!(sorted.len(), RENAMED_SUFFIXES.len(), "a suffix is listed twice");
}

#[tokio::test(flavor = "multi_thread")]
async fn no_table_name_reaches_a_file_the_engine_keeps() {
    // Everything a fresh directory holds that is not a table's: each file is
    // either no table's by its shape, or its owner is reserved. Listing the
    // directory, not a list of names, so a file the engine starts keeping is
    // checked without anyone remembering to add it.
    let dir = tempfile::tempdir().unwrap();
    {
        let db = open_db(dir.path()).await;
        create(&db, "t").await;
        insert_id(&db, "t", 1).await;
    }
    let db = open_db(dir.path()).await;
    let own: Vec<String> = table_file_names("t")
        .flat_map(|f| [f.clone(), format!("{f}.tmp")])
        .collect();
    let mut engine = 0;
    for file in files_in(dir.path()) {
        if own.contains(&file) {
            continue;
        }
        engine += 1;
        for (stem, _) in possible_owners(&file) {
            assert!(
                table_name_refusal(stem).is_some() || reserved_file_owner(&file).is_some(),
                "a table named \"{stem}\" would own the engine's {file}"
            );
            let err = db.add_table(create_stmt(stem)).await;
            assert!(err.is_err(), "\"{stem}\" was created over the engine's {file}");
        }
    }
    assert!(engine >= 3, "the catalogue's files at least should be here");
}

#[tokio::test(flavor = "multi_thread")]
async fn file_names_are_compared_ignoring_case() {
    // Names keep their case on disk — `CREATE TABLE X` makes `X.db` — and the
    // Windows and macOS filesystems fold it, so `X`'s pointers and
    // `x_POINTERS`'s rows are one file there, and `X` and `x` are one table's.
    let dir = tempfile::tempdir().unwrap();
    let db = rclite::Arc::new(open_db(dir.path()).await);
    assert_eq!(sql(&db, "CREATE TABLE X (id INT)").await.0, 0);
    assert!(
        files_in(dir.path()).contains(&"X.db".to_string()),
        "the premise: file names keep the table name's case"
    );
    for newcomer in ["x_POINTERS", "x_pointers", "X_POINTERS", "x", "x_Schema_fields"] {
        let err = db.add_table(create_stmt(newcomer)).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists, "{newcomer}");
        assert_names_the_file(&err.to_string(), newcomer, "X", newcomer);
        let (status, text) = sql(&db, &format!("CREATE TABLE {newcomer} (id INT)")).await;
        assert_eq!(status, 3, "{newcomer}: {text}");
        assert_eq!(crate::pgwire::sqlstate_for(&text), "42P07", "{text}");
    }
    let err = db.add_table(create_stmt("x_POINTERS")).await.unwrap_err().to_string();
    assert!(
        err.contains("x_POINTERS.db") && err.contains("X_pointers.db") && err.contains("ignoring case"),
        "both spellings, and why they are one file: {err}"
    );

    // `x` is not `X` here (names are matched exactly), so `IF NOT EXISTS`
    // must refuse rather than skip: a skip would report a table that every
    // later statement on `x` says does not exist.
    let (status, text) = sql(&db, "CREATE TABLE IF NOT EXISTS x (id INT)").await;
    assert_eq!(status, 3, "IF NOT EXISTS x beside X: {text}");
    assert_eq!(crate::pgwire::sqlstate_for(&text), "42P07", "{text}");
    assert!(
        text.contains("table \"X\"") && text.contains("case-insensitive filesystem"),
        "names X and why: {text}"
    );
    assert!(!db.tables.contains_key("x") && db.tables.contains_key("X"));

    // The other order: `x_POINTERS` first, then `X`.
    let dir = tempfile::tempdir().unwrap();
    let db = rclite::Arc::new(open_db(dir.path()).await);
    create(&db, "x_POINTERS").await;
    create(&db, "other").await;
    let err = db.add_table(create_stmt("X")).await.unwrap_err();
    assert_names_the_file(&err.to_string(), "X", "x_POINTERS", "X after x_POINTERS");
    let err = db.rename_table("other", "X").await.unwrap_err();
    assert_names_the_file(&err.to_string(), "X", "x_POINTERS", "rename to X");
    // Renaming a table onto its own name in another case is the same file
    // copied onto itself on those filesystems.
    let err = db.rename_table("other", "OTHER").await.unwrap_err();
    assert_names_the_file(&err.to_string(), "OTHER", "other", "other -> OTHER");
    assert!(db.tables.contains_key("other"));
    // Unrelated names are unaffected.
    create(&db, "Y").await;
}

#[test]
fn a_catalogue_an_older_build_filled_names_its_sharing_pairs() {
    let names: Vec<String> = ["x", "x_pointers", "X", "keep", "keep2", "a_seq", "a"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    let pairs: Vec<(String, String, String)> = shared_file_pairs(&names)
        .into_iter()
        .map(|(a, shared)| (a, shared.owner, shared.file))
        .collect();
    assert_eq!(
        pairs,
        vec![
            ("X".to_string(), "x".to_string(), "X.db".to_string()),
            ("X".to_string(), "x_pointers".to_string(), "X_pointers.db".to_string()),
            ("a".to_string(), "a_seq".to_string(), "a_seq.db".to_string()),
            ("x".to_string(), "x_pointers".to_string(), "x_pointers.db".to_string()),
        ]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_database_that_already_holds_a_sharing_pair_still_opens() {
    // What an older build left: `x` and `x_pointers` both in the catalogue,
    // sharing `x_pointers.db`. The open keeps both, as it always did — the
    // bytes are mixed already and nothing can tell whose are whose — and says
    // so in the log. What the guard adds is refusing the operations that would
    // spread it: a third table onto either's files, and a rewriting ALTER of
    // either, whose drop would delete the other's file.
    let dir = tempfile::tempdir().unwrap();
    {
        let db = open_db(dir.path()).await;
        create(&db, "x").await;
        for i in 1..=3 {
            insert_id(&db, "x", i).await;
        }
        db.add_table_unguarded(create_stmt("x_pointers"))
            .await
            .expect("the pair as an older build made it");
        create(&db, "keep").await;
        insert_id(&db, "keep", 9).await;
    }

    let db = rclite::Arc::new(open_db(dir.path()).await);
    for name in ["x", "x_pointers", "keep"] {
        assert!(db.tables.contains_key(name), "{name} did not open");
    }
    // `x` never had `x_pointers` write a row into its pointer file, so it reads
    // as it did: the guard took nothing away.
    assert_eq!(ids(&db, "x").await, Some(vec![1, 2, 3]));
    assert_eq!(ids(&db, "keep").await, Some(vec![9]));
    insert_id(&db, "keep", 10).await;
    assert_eq!(ids(&db, "keep").await, Some(vec![9, 10]));

    let names: Vec<String> = db.tables.iter().map(|e| e.key().clone()).collect();
    let pairs = shared_file_pairs(&names);
    assert_eq!(pairs.len(), 1, "{pairs:?}");
    assert_eq!((pairs[0].0.as_str(), pairs[0].1.owner.as_str()), ("x", "x_pointers"));

    for name in ["x", "x_pointers"] {
        let err = db.check_recreatable(name).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists, "{name}");
    }
    db.check_recreatable("keep").expect("an unrelated table rewrites as before");
    // A rewriting ALTER on `x` is refused while its rows are still there.
    let (status, text) = sql(&db, "ALTER TABLE x ADD COLUMN extra INT").await;
    assert_eq!(status, 3, "{text}");
    assert_eq!(ids(&db, "x").await, Some(vec![1, 2, 3]));
    // Nor can a third name join the pair.
    for name in ["x_pointers_pointers", "x_seq"] {
        assert!(db.add_table(create_stmt(name)).await.is_err(), "{name}");
    }
}

// ---------------------------------------------------------------------------
// The source agrees with the list
// ---------------------------------------------------------------------------

/// Files under `src/` that are test code by name; their `.db` names are
/// fixtures, not files the engine makes.
fn is_test_file(rel: &str) -> bool {
    rel.ends_with("_tests.rs")
        || rel.ends_with("/tests.rs")
        || rel.contains("/tests/")
        || rel.ends_with("_test.rs")
        || rel.starts_with("ext_cov_tests")
}

/// Sources excluded from the scan, each with why its `.db` names are no
/// table's. Kept short and named, so an exclusion is a decision someone made.
const NOT_TABLE_FILES: &[(&str, &str)] = &[
    // An in-memory stand-in for a table's storage used by benches: its
    // `POINTERS.db` and `ROWS.db` names never reach a directory.
    ("core/mock_table.rs", "bench mock, never on disk"),
    // A test provider rooted in a `TempDir` of its own, under `TEMP.db`.
    ("core/storage_providers/mock_file_sync.rs", "test provider in its own TempDir"),
];

/// Whether `attrs` mark an item as test-only: `#[test]`, `#[tokio::test]`
/// and the like, or `#[cfg(test)]`.
fn is_test_only(attrs: &[syn::Attribute]) -> bool {
    use quote::ToTokens;
    attrs.iter().any(|a| {
        let path = a.path();
        if path.segments.last().is_some_and(|s| s.ident == "test") {
            return true;
        }
        path.is_ident("cfg")
            && a.meta.to_token_stream().to_string().replace(' ', "") == "cfg(test)"
    })
}

/// Every string literal in `file` outside test-only items, macro bodies
/// included.
fn literals(file: &syn::File) -> Vec<String> {
    use quote::ToTokens;
    fn walk(tokens: proc_macro2::TokenStream, out: &mut Vec<String>) {
        for t in tokens {
            match t {
                // Doc comments are `#[doc = "..."]` once tokenized: prose about
                // files, not names built.
                proc_macro2::TokenTree::Group(g)
                    if g.delimiter() == proc_macro2::Delimiter::Bracket
                        && g.stream().into_iter().next().is_some_and(
                            |first| matches!(first, proc_macro2::TokenTree::Ident(i) if i == "doc"),
                        ) => {}
                proc_macro2::TokenTree::Group(g) => walk(g.stream(), out),
                proc_macro2::TokenTree::Literal(l) => {
                    if let Ok(syn::Lit::Str(s)) = syn::parse_str::<syn::Lit>(&l.to_string()) {
                        out.push(s.value());
                    }
                }
                _ => {}
            }
        }
    }
    fn strip(items: &mut Vec<syn::Item>) {
        items.retain(|item| {
            let attrs: &[syn::Attribute] = match item {
                syn::Item::Fn(f) => &f.attrs,
                syn::Item::Mod(m) => &m.attrs,
                syn::Item::Impl(i) => &i.attrs,
                syn::Item::Const(c) => &c.attrs,
                syn::Item::Static(s) => &s.attrs,
                syn::Item::Use(u) => &u.attrs,
                syn::Item::Struct(s) => &s.attrs,
                syn::Item::Enum(e) => &e.attrs,
                syn::Item::Macro(m) => &m.attrs,
                _ => &[],
            };
            !is_test_only(attrs)
        });
        for item in items.iter_mut() {
            if let syn::Item::Mod(m) = item {
                if let Some((_, inner)) = &mut m.content {
                    strip(inner);
                }
            }
        }
    }
    let mut file = file.clone();
    strip(&mut file.items);
    let mut out = Vec::new();
    walk(file.into_token_stream(), &mut out);
    out
}

/// The per-table suffix a `.db` name literal implies, or why it is not one.
///
/// * `"{name}_pointers.db"`, `"{}.db"`: whatever follows the last placeholder.
/// * `"_pointers.db"`, `".db"`: a suffix written out, as in a list.
/// * `"FIELDS.db"`, `"IDX.db"`: a sidecar part, joined to the schema file's
///   stem as `<table>_schema_<part>`.
/// * anything else (`"db_data_rows.db"`): a whole file name, which must be the
///   engine's and reserved — `Err` carries it.
fn implied_suffix(literal: &str) -> Result<String, String> {
    let name = literal.strip_suffix(".tmp").unwrap_or(literal);
    if let Some(at) = name.rfind('}') {
        return Ok(name[at + 1..].to_string());
    }
    if name.starts_with('_') || name == ".db" {
        return Ok(name.to_string());
    }
    let part = name.strip_suffix(".db").unwrap_or(name);
    if !part.is_empty() && part.bytes().all(|b| b.is_ascii_uppercase()) {
        return Ok(format!("_schema_{name}"));
    }
    Err(name.to_string())
}

#[test]
fn every_db_file_name_built_in_src_is_listed() {
    let src = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    fn rust_files(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
        for e in std::fs::read_dir(dir).unwrap() {
            let path = e.unwrap().path();
            if path.is_dir() {
                rust_files(&path, out);
            } else if path.extension().is_some_and(|x| x == "rs") {
                out.push(path);
            }
        }
    }
    rust_files(&src, &mut files);
    assert!(files.len() > 100, "found only {} files", files.len());

    let mut problems = Vec::new();
    let mut suffixes_built: Vec<String> = Vec::new();
    for path in files {
        let rel = path.strip_prefix(&src).unwrap().to_string_lossy().replace('\\', "/");
        if is_test_file(&rel) || NOT_TABLE_FILES.iter().any(|(f, _)| *f == rel) {
            continue;
        }
        let text = std::fs::read_to_string(&path).unwrap();
        let file = syn::parse_file(&text).unwrap_or_else(|e| panic!("{rel}: {e}"));
        for literal in literals(&file) {
            let shape = literal.strip_suffix(".tmp").unwrap_or(&literal);
            if !shape.ends_with(".db") {
                continue;
            }
            // A partitioned table's own files (`__pt_<uid>_ids.db`, its map
            // log, its partitions' rows) live in the `__pt_` namespace, which
            // `table_name_refusal` refuses as a table name: no table can be
            // named onto them, and partition code renames and drops them, not
            // RENAMED_SUFFIXES. Only the prefixed literals: anything else in
            // core/partition is scanned like the rest.
            if shape.starts_with("{PREFIX}") {
                continue;
            }
            match implied_suffix(&literal) {
                Ok(suffix) if RENAMED_SUFFIXES.contains(&suffix.as_str()) => {
                    suffixes_built.push(suffix);
                }
                Ok(suffix) => problems.push(format!(
                    "{rel}: \"{literal}\" builds a table's `{suffix}` file, which \
                     RENAMED_SUFFIXES does not list: the name guard cannot see it, and \
                     rename and drop leave it behind"
                )),
                Err(whole) if reserved_file_owner(&whole).is_some() => {}
                Err(whole) => problems.push(format!(
                    "{rel}: \"{literal}\" names {whole}, a file the engine keeps beside the \
                     tables, but not one RESERVED_FILES reserves: a table could be named onto it"
                )),
            }
        }
    }
    assert!(problems.is_empty(), "{}", problems.join("\n"));

    // The other way: a listed suffix nothing builds is dead weight at best,
    // and at worst a sign the scan stopped seeing how names are built.
    for suffix in RENAMED_SUFFIXES {
        assert!(
            suffixes_built.iter().any(|s| s == suffix),
            "{suffix} is listed but the scan found nothing that builds it"
        );
    }
}

#[test]
fn the_source_scan_reads_the_shapes_it_must() {
    // The classifier, on the shapes the engine uses.
    assert_eq!(implied_suffix("{table_name}_pointers.db"), Ok("_pointers.db".into()));
    assert_eq!(implied_suffix("{}.db"), Ok(".db".into()));
    assert_eq!(implied_suffix("{}_flt.db"), Ok("_flt.db".into()));
    assert_eq!(implied_suffix("FIELDS.db"), Ok("_schema_FIELDS.db".into()));
    assert_eq!(implied_suffix("IDX.db"), Ok("_schema_IDX.db".into()));
    assert_eq!(implied_suffix("_schema_UK.db"), Ok("_schema_UK.db".into()));
    assert_eq!(implied_suffix("x.db.tmp"), Err("x.db".into()));
    assert_eq!(implied_suffix("db_data_rows.db"), Err("db_data_rows.db".into()));
    // And a new sidecar would be caught: built, not listed.
    assert!(!RENAMED_SUFFIXES.contains(&implied_suffix("{}_zzz.db").unwrap().as_str()));
    assert!(!RENAMED_SUFFIXES.contains(&implied_suffix("ZZZ.db").unwrap().as_str()));

    // Test-only items are left out, and only they.
    let file: syn::File = syn::parse_str(
        r#"
        /// Prose naming "{}_doc.db" is not a name built.
        fn real() { let _ = "{}_kept.db"; }
        #[cfg(test)]
        mod tests { fn fixture() { let _ = "{}_fixture.db"; } }
        #[test]
        fn a_test() { let _ = "{}_t.db"; }
        "#,
    )
    .unwrap();
    assert_eq!(literals(&file), vec!["{}_kept.db".to_string()]);
}

/// A partitioned table holds its name's files even when its shell is not in
/// `tables` (its schema could not be read at open): an older binary would
/// read `<name>_schema.db` as that schema, so `acct_schema` or
/// `acct_pointers` must still be refused beside it.
#[tokio::test(flavor = "multi_thread")]
async fn a_partitioned_table_without_its_shell_still_holds_its_files() {
    let dir = tempfile::tempdir().unwrap();
    let db = rclite::Arc::new(open_db(dir.path()).await);
    let (status, text) =
        sql(&db, "CREATE TABLE acct (tenant_id BIGINT NOT NULL, n INT) PARTITION BY HASH (tenant_id)").await;
    assert_eq!(status, 0, "{text}");
    assert!(db.partitioned.contains_key("acct"));
    db.tables.remove("acct");
    for newcomer in ["acct_schema", "acct_pointers", "ACCT_schema_FIELDS"] {
        let err = db.add_table(create_stmt(newcomer)).await.unwrap_err();
        assert_names_the_file(&err.to_string(), newcomer, "acct", newcomer);
    }
}
