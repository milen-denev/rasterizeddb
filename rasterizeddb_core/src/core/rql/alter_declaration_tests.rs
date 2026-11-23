//! Declarations filed by *name* — composite `UNIQUE` keys, foreign keys on
//! either side, `CHECK`s, generation expressions, JSON registrations, the
//! `PRIMARY KEY` header and path indexes — and what `DROP COLUMN`,
//! `RENAME COLUMN`, `ADD COLUMN` and `RENAME TABLE` do to them.
//!
//! A dropped column keeps its slot and is only tombstoned, and its name can be
//! taken again. A declaration that still named it was then picked up by the
//! next column to take the name, which turned a dropped foreign key into a
//! live one on an unrelated column, with `ON DELETE CASCADE` deleting rows by
//! whatever values that column held. All of it reported success.
//!
//! Driven through the executor, as the server drives it, with a reopen of the
//! database where durability is the question.

use crate::core::database::Database;
use crate::core::row::row::{Row, vec_into_rows};
use crate::core::rql::executor::execute;
use crate::core::rql::lexer_s1::recognize_query_purpose;
use rclite::Arc;

fn init_globals() {
    let _ = crate::MAX_PERMITS_THREADS.set(4);
    let _ = crate::BATCH_SIZE.set(1024);
}

async fn open(dir: &std::path::Path) -> Arc<Database> {
    Arc::new(Database::new_named(&dir.to_string_lossy(), false, "alterdecltests").await)
}

async fn run(database: &Arc<Database>, sql: &str) -> Vec<u8> {
    let purpose = recognize_query_purpose(sql).unwrap_or_else(|| panic!("unrecognised: {}", sql));
    execute(purpose, database.clone()).await
}

fn is_error(response: &[u8]) -> bool {
    response.first() == Some(&3)
}

fn message(response: &[u8]) -> String {
    String::from_utf8_lossy(response.get(1..).unwrap_or_default()).into_owned()
}

#[track_caller]
fn expect_ok(response: &[u8], sql: &str) {
    if is_error(response) {
        panic!("`{}` failed: {}", sql, message(response));
    }
}

async fn ok(database: &Arc<Database>, sql: &str) {
    expect_ok(&run(database, sql).await, sql);
}

async fn refused(database: &Arc<Database>, sql: &str) -> String {
    let response = run(database, sql).await;
    assert!(is_error(&response), "`{}` was accepted", sql);
    message(&response)
}

async fn rows(database: &Arc<Database>, sql: &str) -> Vec<Row> {
    let response = run(database, sql).await;
    expect_ok(&response, sql);
    vec_into_rows(&response[1..]).expect("a decodable result set")
}

/// An integer cell, widened; `None` for SQL NULL.
fn int(row: &Row, column: usize) -> Option<i64> {
    let cell = &row.columns[column];
    if cell.column_type == crate::core::db_type::DbType::NULL {
        return None;
    }
    let bytes = cell.data.into_slice();
    Some(match bytes.len() {
        0 => return None,
        2 => i16::from_le_bytes(bytes.try_into().unwrap()) as i64,
        4 => i32::from_le_bytes(bytes.try_into().unwrap()) as i64,
        8 => i64::from_le_bytes(bytes.try_into().unwrap()),
        n => panic!("an integer cell of {} bytes", n),
    })
}

async fn ints(database: &Arc<Database>, sql: &str) -> Vec<Option<i64>> {
    rows(database, sql).await.iter().map(|r| int(r, 0)).collect()
}

fn schema_of(db: &Arc<Database>, table: &str) -> crate::core::row::schema::TableSchema {
    db.table_for_tests(table)
        .unwrap_or_else(|| panic!("no table {table}"))
        .schema
        .clone()
}

fn io_of(
    db: &Arc<Database>,
    table: &str,
) -> Arc<crate::core::storage_providers::backend::DbStorageProvider> {
    db.table_for_tests(table).expect("table").io_schema.clone()
}

// ---------------------------------------------------------------------------
// A dead column's constraints do not pass to the next column of its name.
// ---------------------------------------------------------------------------

/// `DROP COLUMN pid` left the foreign key naming `pid` live. Renaming another
/// column to `pid` then bound it: unrelated values were refused, and a parent
/// delete cascaded into every child row whose *former* `other` matched.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_column_renamed_onto_a_dropped_fk_column_is_not_bound_by_it() {
    init_globals();
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;

    ok(&db, "CREATE TABLE p (id INT PRIMARY KEY)").await;
    ok(
        &db,
        "CREATE TABLE c (id INT, pid INT REFERENCES p(id) ON DELETE CASCADE, other INT)",
    )
    .await;
    ok(&db, "INSERT INTO p (id) VALUES (5)").await;
    ok(&db, "ALTER TABLE c DROP COLUMN pid").await;
    assert!(schema_of(&db, "c").foreign_keys.is_empty(), "DROP COLUMN left its foreign key");
    ok(&db, "ALTER TABLE c RENAME COLUMN other TO pid").await;

    ok(&db, "INSERT INTO c (id, pid) VALUES (1, 99)").await;
    ok(&db, "INSERT INTO c (id, pid) VALUES (2, 5)").await;
    ok(&db, "DELETE FROM p WHERE id = 5").await;
    assert_eq!(
        ints(&db, "SELECT id FROM c ORDER BY id").await,
        vec![Some(1), Some(2)],
        "a parent delete cascaded through the dropped column's foreign key"
    );

    drop(db);
    let db = open(dir.path()).await;
    ok(&db, "INSERT INTO c (id, pid) VALUES (3, 98)").await;
    assert!(
        schema_of(&db, "c").foreign_keys.is_empty(),
        "the dropped column's foreign key is still live: {:?}",
        schema_of(&db, "c").foreign_keys
    );
}

/// The same through `ADD COLUMN`, on the empty table's append path and the
/// populated table's rewrite, which copies the table's declarations across.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_column_added_onto_a_dropped_fk_column_is_not_bound_by_it() {
    init_globals();
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;

    ok(&db, "CREATE TABLE p (id INT PRIMARY KEY)").await;
    ok(&db, "INSERT INTO p (id) VALUES (5)").await;
    ok(&db, "CREATE TABLE e (id INT, pid INT REFERENCES p(id) ON DELETE CASCADE)").await;
    ok(&db, "CREATE TABLE f (id INT, pid INT REFERENCES p(id) ON DELETE CASCADE)").await;
    ok(&db, "INSERT INTO f (id, pid) VALUES (0, NULL)").await;
    for table in ["e", "f"] {
        ok(&db, &format!("ALTER TABLE {table} DROP COLUMN pid")).await;
        ok(&db, &format!("ALTER TABLE {table} ADD COLUMN pid INT")).await;
        ok(&db, &format!("INSERT INTO {table} (id, pid) VALUES (1, 99)")).await;
        ok(&db, &format!("INSERT INTO {table} (id, pid) VALUES (2, 5)")).await;
    }
    ok(&db, "DELETE FROM p WHERE id = 5").await;
    assert_eq!(ints(&db, "SELECT id FROM e ORDER BY id").await, vec![Some(1), Some(2)]);
    assert_eq!(
        ints(&db, "SELECT id FROM f ORDER BY id").await,
        vec![Some(0), Some(1), Some(2)]
    );
}

/// `UNIQUE (a, b)`, then `DROP COLUMN b` and `RENAME x TO b`: the key had been
/// skipped while no column was called `b`, and was enforced again on `x`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dropped_columns_composite_unique_does_not_bind_its_successor() {
    init_globals();
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;

    ok(&db, "CREATE TABLE u (a INT, b INT, x INT, k INT, UNIQUE (a, b))").await;
    ok(&db, "INSERT INTO u (a, b, x, k) VALUES (1, 1, 1, 1)").await;
    ok(&db, "ALTER TABLE u DROP COLUMN b").await;
    assert!(schema_of(&db, "u").unique_keys.is_empty(), "DROP COLUMN left its UNIQUE key");
    ok(&db, "ALTER TABLE u RENAME COLUMN x TO b").await;
    ok(&db, "INSERT INTO u (a, b, k) VALUES (1, 1, 2)").await;
    assert!(schema_of(&db, "u").unique_keys.is_empty());

    drop(db);
    let db = open(dir.path()).await;
    ok(&db, "INSERT INTO u (a, b, k) VALUES (1, 1, 3)").await;
    assert_eq!(
        ints(&db, "SELECT k FROM u ORDER BY k").await,
        vec![Some(1), Some(2), Some(3)]
    );
}

/// A `CHECK (b > 0)` on a dropped column was skipped as uncompilable, and
/// compiled again against the next column called `b`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dropped_columns_check_does_not_bind_its_successor() {
    init_globals();
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;

    ok(&db, "CREATE TABLE k1 (a INT, b INT CHECK (b > 0))").await;
    ok(&db, "ALTER TABLE k1 DROP COLUMN b").await;
    assert!(schema_of(&db, "k1").checks.is_empty(), "DROP COLUMN left its CHECK");
    ok(&db, "ALTER TABLE k1 ADD COLUMN b INT").await;
    ok(&db, "INSERT INTO k1 (a, b) VALUES (1, -1)").await;

    ok(&db, "CREATE TABLE k2 (a INT, b INT, x INT, CHECK (b > 0))").await;
    ok(&db, "ALTER TABLE k2 DROP COLUMN b").await;
    ok(&db, "ALTER TABLE k2 RENAME COLUMN x TO b").await;
    ok(&db, "INSERT INTO k2 (a, b) VALUES (1, -1)").await;

    drop(db);
    let db = open(dir.path()).await;
    ok(&db, "INSERT INTO k1 (a, b) VALUES (2, -2)").await;
    ok(&db, "INSERT INTO k2 (a, b) VALUES (2, -2)").await;
    assert!(schema_of(&db, "k1").checks.is_empty());
    assert!(schema_of(&db, "k2").checks.is_empty());
}

/// Records left naming a column that no longer exists — by an older engine's
/// `DROP COLUMN`, which retired none of them — are retired by the `RENAME` or
/// `ADD` that takes the name, rather than bound to the new column.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn leftover_constraints_naming_a_free_name_are_retired_when_it_is_taken() {
    use crate::core::rql::{check, foreign_key, unique_key};
    init_globals();
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;

    ok(&db, "CREATE TABLE p (id INT PRIMARY KEY)").await;
    ok(&db, "INSERT INTO p (id) VALUES (5)").await;
    ok(&db, "CREATE TABLE l (a INT, x INT, k INT)").await;
    ok(&db, "CREATE TABLE m (a INT, k INT)").await;
    for table in ["l", "m"] {
        let io = io_of(&db, table);
        unique_key::append(
            io.clone(),
            &unique_key::UniqueKey::new(format!("{table}_a_g_key"), vec!["a".into(), "g".into()]),
        )
        .await
        .unwrap();
        foreign_key::append(
            io.clone(),
            &foreign_key::ForeignKey {
                name: format!("{table}_g_fkey"),
                column: "g".into(),
                parent_table: "p".into(),
                parent_column: "id".into(),
                on_delete: foreign_key::ReferentialAction::Cascade,
                on_update: foreign_key::ReferentialAction::NoAction,
                is_deleted: false,
            },
        )
        .await
        .unwrap();
        check::append(
            io,
            &check::CheckConstraint {
                name: format!("{table}_g_check"),
                expression: "g > 0".into(),
                is_deleted: false,
            },
        )
        .await
        .unwrap();
        db.reload_table(table).await.unwrap();
    }
    ok(&db, "INSERT INTO l (a, x, k) VALUES (1, 1, 1)").await;
    ok(&db, "INSERT INTO m (a, k) VALUES (1, 1)").await;
    // And as a parent: another table's key naming l.g / m.g, which no
    // column is, with a cascade that would follow the new column.
    ok(&db, "CREATE TABLE o (id INT, ref_l INT, ref_m INT)").await;
    for (column, parent) in [("ref_l", "l"), ("ref_m", "m")] {
        foreign_key::append(
            io_of(&db, "o"),
            &foreign_key::ForeignKey {
                name: format!("o_{column}_fkey"),
                column: column.into(),
                parent_table: parent.into(),
                parent_column: "g".into(),
                on_delete: foreign_key::ReferentialAction::Cascade,
                on_update: foreign_key::ReferentialAction::NoAction,
                is_deleted: false,
            },
        )
        .await
        .unwrap();
    }
    db.reload_table("o").await.unwrap();

    ok(&db, "ALTER TABLE l RENAME COLUMN x TO g").await;
    ok(&db, "ALTER TABLE m ADD COLUMN g INT").await;
    for table in ["l", "m"] {
        // The leftover UNIQUE (a, g), CHECK (g > 0) and FK g -> p(id) each
        // refuse one of these.
        ok(&db, &format!("INSERT INTO {table} (a, g, k) VALUES (1, 1, 2)")).await;
        ok(&db, &format!("INSERT INTO {table} (a, g, k) VALUES (1, -7, 3)")).await;
        ok(&db, &format!("INSERT INTO {table} (a, g, k) VALUES (2, 5, 4)")).await;
    }
    ok(&db, "INSERT INTO o (id, ref_l, ref_m) VALUES (1, 5, 5)").await;
    ok(&db, "DELETE FROM p WHERE id = 5").await;
    // Deleting the parent rows whose new column holds 5 cascades nowhere.
    ok(&db, "DELETE FROM l WHERE k = 4").await;
    ok(&db, "DELETE FROM m WHERE k = 4").await;
    assert_eq!(ints(&db, "SELECT id FROM o").await, vec![Some(1)], "a leftover parent-side key cascaded");
    assert!(schema_of(&db, "o").foreign_keys.is_empty(), "{:?}", schema_of(&db, "o").foreign_keys);
    for table in ["l", "m"] {
        assert_eq!(
            ints(&db, &format!("SELECT k FROM {table} ORDER BY k")).await,
            vec![Some(1), Some(2), Some(3)],
            "{table}: a leftover cascade deleted a row"
        );
        let schema = schema_of(&db, table);
        assert!(schema.unique_keys.is_empty(), "{table}: {:?}", schema.unique_keys);
        assert!(schema.foreign_keys.is_empty(), "{table}: {:?}", schema.foreign_keys);
        assert!(schema.checks.is_empty(), "{table}: {:?}", schema.checks);
    }
}

/// PostgreSQL refuses to drop a column another table's foreign key references
/// (it needs `CASCADE`). Dropping it here left the child's key naming a column
/// that no longer existed, which failed every later delete from the parent —
/// and bound itself to the next column given the name.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_column_a_foreign_key_references_cannot_be_dropped() {
    init_globals();
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;

    ok(&db, "CREATE TABLE p (id INT PRIMARY KEY, v INT)").await;
    ok(&db, "CREATE TABLE c (id INT, pid INT REFERENCES p(id))").await;
    ok(&db, "INSERT INTO p (id, v) VALUES (1, 1)").await;
    ok(&db, "INSERT INTO c (id, pid) VALUES (1, 1)").await;
    let why = refused(&db, "ALTER TABLE p DROP COLUMN id").await;
    assert!(why.contains("depends on"), "unexpected refusal: {why}");
    assert!(is_error(&run(&db, "DELETE FROM p WHERE id = 1").await));

    // A self-reference: the child column goes with its key, the parent column
    // is refused while the key stands.
    ok(&db, "CREATE TABLE t (id INT PRIMARY KEY, parent INT REFERENCES t(id), w INT)").await;
    refused(&db, "ALTER TABLE t DROP COLUMN id").await;
    ok(&db, "ALTER TABLE t DROP COLUMN parent").await;
    assert!(schema_of(&db, "t").foreign_keys.is_empty());
    ok(&db, "ALTER TABLE t DROP COLUMN id").await;

    // Once the key is gone, so is the objection.
    let fk = schema_of(&db, "c").foreign_keys[0].name.clone();
    ok(&db, &format!("ALTER TABLE c DROP CONSTRAINT {fk}")).await;
    ok(&db, "ALTER TABLE p DROP COLUMN id").await;
}

// ---------------------------------------------------------------------------
// RENAME TABLE: the children's foreign keys follow their parent.
// ---------------------------------------------------------------------------

/// `ALTER TABLE p RENAME TO q` left every child's key naming `p`: deletes from
/// `q` found no referencing key and orphaned the children, and a table later
/// created as `p` took the children's keys over.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_renamed_parent_keeps_its_childrens_foreign_keys() {
    init_globals();
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;

    ok(&db, "CREATE TABLE p (id INT PRIMARY KEY)").await;
    ok(&db, "CREATE TABLE c (id INT, pid INT REFERENCES p(id))").await;
    ok(&db, "CREATE TABLE d (id INT, pid INT REFERENCES p(id) ON DELETE CASCADE)").await;
    for id in [1, 2, 3] {
        ok(&db, &format!("INSERT INTO p (id) VALUES ({id})")).await;
    }
    ok(&db, "INSERT INTO c (id, pid) VALUES (1, 1)").await;
    ok(&db, "INSERT INTO d (id, pid) VALUES (1, 2)").await;

    ok(&db, "ALTER TABLE p RENAME TO q").await;
    for table in ["c", "d"] {
        assert_eq!(schema_of(&db, table).foreign_keys[0].parent_table, "q", "{table}");
    }
    refused(&db, "DELETE FROM q WHERE id = 1").await;
    ok(&db, "DELETE FROM q WHERE id = 2").await;
    assert!(ints(&db, "SELECT id FROM d").await.is_empty(), "the cascade did not follow the rename");
    ok(&db, "INSERT INTO c (id, pid) VALUES (2, 3)").await;
    refused(&db, "INSERT INTO c (id, pid) VALUES (3, 99)").await;

    drop(db);
    let db = open(dir.path()).await;
    assert_eq!(schema_of(&db, "c").foreign_keys[0].parent_table, "q");
    refused(&db, "DELETE FROM q WHERE id = 1").await;
    // A new table under the old name is not the children's parent.
    ok(&db, "CREATE TABLE p (id INT PRIMARY KEY)").await;
    ok(&db, "INSERT INTO p (id) VALUES (99)").await;
    refused(&db, "INSERT INTO c (id, pid) VALUES (4, 99)").await;
    ok(&db, "DELETE FROM p WHERE id = 99").await;
    assert_eq!(ints(&db, "SELECT id FROM c ORDER BY id").await, vec![Some(1), Some(2)]);
}

/// A table referencing itself names itself as the parent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_renamed_self_referencing_table_keeps_its_foreign_key() {
    init_globals();
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;

    ok(&db, "CREATE TABLE t (id INT PRIMARY KEY, parent INT REFERENCES t(id))").await;
    ok(&db, "INSERT INTO t (id, parent) VALUES (1, NULL)").await;
    ok(&db, "INSERT INTO t (id, parent) VALUES (2, 1)").await;
    ok(&db, "ALTER TABLE t RENAME TO t2").await;
    assert_eq!(schema_of(&db, "t2").foreign_keys[0].parent_table, "t2");
    refused(&db, "DELETE FROM t2 WHERE id = 1").await;
    refused(&db, "INSERT INTO t2 (id, parent) VALUES (3, 99)").await;
    ok(&db, "INSERT INTO t2 (id, parent) VALUES (3, 2)").await;

    drop(db);
    let db = open(dir.path()).await;
    assert_eq!(schema_of(&db, "t2").foreign_keys[0].parent_table, "t2");
    refused(&db, "INSERT INTO t2 (id, parent) VALUES (4, 98)").await;
}

// ---------------------------------------------------------------------------
// JSON registrations, the PRIMARY KEY header and path indexes.
// ---------------------------------------------------------------------------

/// A JSON column's registration names it. Left behind by `RENAME COLUMN`, the
/// renamed column read back its raw reference; left by `DROP COLUMN`, a plain
/// column added under the name was read as a reference.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_json_registration_follows_rename_and_drop() {
    use crate::core::rql::json_map::{self, JsonColumn};
    init_globals();
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;

    // As a table created with shadow tables on left it.
    ok(&db, "CREATE TABLE j (id INT, doc UUID, k INT)").await;
    for (column, shadow) in [("doc", "j_doc_shadow"), ("zz", "j_zz_shadow")] {
        json_map::append(
            io_of(&db, "j"),
            &JsonColumn {
                column: column.into(),
                shadow_table: shadow.into(),
                is_deleted: false,
            },
        )
        .await
        .unwrap();
    }
    db.reload_table("j").await.unwrap();

    ok(&db, "ALTER TABLE j RENAME COLUMN doc TO body").await;
    let registered = schema_of(&db, "j").json_columns;
    assert_eq!(json_map::shadow_for("body", &registered), Some("j_doc_shadow"));
    assert_eq!(json_map::shadow_for("doc", &registered), None);

    drop(db);
    let db = open(dir.path()).await;
    let registered = schema_of(&db, "j").json_columns;
    assert_eq!(json_map::shadow_for("body", &registered), Some("j_doc_shadow"));
    assert_eq!(json_map::shadow_for("doc", &registered), None);

    ok(&db, "ALTER TABLE j DROP COLUMN body").await;
    assert_eq!(json_map::shadow_for("body", &schema_of(&db, "j").json_columns), None);
    ok(&db, "ALTER TABLE j ADD COLUMN body TEXT").await;
    // A leftover registration under a free name is retired by the column
    // that takes it.
    ok(&db, "ALTER TABLE j ADD COLUMN zz TEXT").await;
    let registered = schema_of(&db, "j").json_columns;
    assert_eq!(json_map::shadow_for("body", &registered), None);
    assert_eq!(json_map::shadow_for("zz", &registered), None);
    drop(db);
    let db = open(dir.path()).await;
    assert!(schema_of(&db, "j").json_columns.is_empty());
}

/// The header names the `PRIMARY KEY` column. After `RENAME COLUMN id TO
/// ident` it still said `id`: introspection reported a key on a column that
/// does not exist, and none on the one that does.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_primary_key_header_follows_rename_and_drop() {
    init_globals();
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;

    ok(&db, "CREATE TABLE pk (id INT PRIMARY KEY, v INT)").await;
    ok(&db, "ALTER TABLE pk RENAME COLUMN id TO ident").await;
    assert_eq!(schema_of(&db, "pk").primary_key.as_deref(), Some("ident"));
    ok(&db, "INSERT INTO pk (ident, v) VALUES (1, 1)").await;
    refused(&db, "INSERT INTO pk (ident, v) VALUES (1, 2)").await;

    drop(db);
    let db = open(dir.path()).await;
    assert_eq!(schema_of(&db, "pk").primary_key.as_deref(), Some("ident"));
    ok(&db, "ALTER TABLE pk DROP COLUMN ident").await;
    assert_eq!(schema_of(&db, "pk").primary_key, None);
    ok(&db, "ALTER TABLE pk ADD COLUMN ident INT").await;
    drop(db);
    let db = open(dir.path()).await;
    assert_eq!(schema_of(&db, "pk").primary_key, None);
}

/// A path index names its document column inside its path. After a rename it
/// named nothing and was not built; after a drop it attached itself to the
/// next jsonb column given the name.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_path_index_follows_rename_and_drop() {
    init_globals();
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;

    ok(&db, "CREATE TABLE d (id INT, doc JSONB)").await;
    ok(&db, "CREATE INDEX d_status ON d ((doc->>'status'))").await;
    ok(&db, "ALTER TABLE d RENAME COLUMN doc TO body").await;
    let column_of = |db: &Arc<Database>| {
        schema_of(db, "d")
            .path_indexes
            .iter()
            .map(|p| {
                crate::core::jsonb::parse_path_expression(&p.path)
                    .expect("a readable path")
                    .column
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(column_of(&db), vec!["body".to_string()]);

    drop(db);
    let db = open(dir.path()).await;
    assert_eq!(column_of(&db), vec!["body".to_string()]);
    ok(&db, "ALTER TABLE d DROP COLUMN body").await;
    assert!(column_of(&db).is_empty(), "DROP COLUMN left its path index");
    ok(&db, "ALTER TABLE d ADD COLUMN body JSONB").await;
    assert!(column_of(&db).is_empty(), "the dropped column's path index survived");
    drop(db);
    let db = open(dir.path()).await;
    assert!(column_of(&db).is_empty());
}

// ---------------------------------------------------------------------------
// The expression rewrite: type names, literal prefixes, EXTRACT fields.
// ---------------------------------------------------------------------------

/// A word that matches the column but is a type name or part of a literal is
/// not a reference. Renamed, the check stopped compiling and was skipped.
#[test]
fn the_expression_rewrite_leaves_type_names_and_literal_prefixes_alone() {
    use crate::core::rql::executor::rename_column_references as rewrite;
    // Bit-string, hex, Unicode and national literal prefixes.
    assert_eq!(rewrite("x <> X'00'", "t", "x", "y"), "y <> X'00'");
    assert_eq!(rewrite("b <> B'0101'", "t", "b", "c"), "c <> B'0101'");
    assert_eq!(rewrite("u <> U&'d\\0061t'", "t", "u", "v"), "v <> U&'d\\0061t'");
    assert_eq!(rewrite("n <> N'abc'", "t", "n", "m"), "m <> N'abc'");
    // A typed literal: the type is followed by the string.
    assert_eq!(
        rewrite("date > date '2000-01-01'", "t", "date", "d"),
        "d > date '2000-01-01'"
    );
    // CAST's target type, in any spelling of AS.
    assert_eq!(
        rewrite("CAST(n AS text) <> '' AND text <> ''", "t", "text", "body"),
        "CAST(n AS text) <> '' AND body <> ''"
    );
    assert_eq!(rewrite("cast(text as\ttext) <> ''", "t", "text", "body"), "cast(body as\ttext) <> ''");
    // EXTRACT's field.
    assert_eq!(
        rewrite("EXTRACT(epoch FROM ts) > epoch", "t", "epoch", "e"),
        "EXTRACT(epoch FROM ts) > e"
    );
    assert_eq!(rewrite("extract ( epoch from epoch ) > 0", "t", "epoch", "e"), "extract ( epoch from e ) > 0");
    // A column that is merely followed by FROM elsewhere is still a reference.
    assert_eq!(
        rewrite("SUBSTRING(s FROM 2) <> ''", "t", "s", "z"),
        "SUBSTRING(z FROM 2) <> ''"
    );
}

/// The integration of the above, through a generation expression (a CHECK
/// does not compile `CAST`, typed literals, bit strings or `EXTRACT` at all,
/// so it cannot show this): a column computed as `CAST(n AS text)` on a table
/// with a column called `text`, renamed to `body`. Rewritten to
/// `CAST(n AS body)`, every insert failed.
///
/// No insert before the rename: while a column is called `text`, the insert
/// path's own substitution reads the cast's type as that column (a separate
/// defect, in the generated-column evaluation).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cast_to_a_type_named_like_the_column_survives_its_rename() {
    init_globals();
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;

    ok(
        &db,
        "CREATE TABLE tx (n INT, text TEXT, g TEXT GENERATED ALWAYS AS (CAST(n AS text)) STORED)",
    )
    .await;
    ok(&db, "ALTER TABLE tx RENAME COLUMN text TO body").await;
    ok(&db, "INSERT INTO tx (n, body) VALUES (6, 'b')").await;
    let generated = schema_of(&db, "tx").generated;
    assert_eq!(generated.len(), 1);
    assert_eq!(generated[0].expression, "CAST(n AS text)");
}

// ---------------------------------------------------------------------------
// A stale generated record under the target name.
// ---------------------------------------------------------------------------

/// A generated record left live under a dead name (an older engine's drop, or
/// a crash between a drop's writes), then `RENAME a TO b`: the rewrite of every
/// expression mentioning `a` reached that record too, and made the renamed
/// column `GENERATED AS (b * 2)` — reading itself.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stale_generated_record_under_the_target_does_not_capture_the_renamed_column() {
    use crate::core::rql::generated::{self, GeneratedColumn};
    init_globals();
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;

    ok(&db, "CREATE TABLE g (a INT, k INT)").await;
    generated::append(
        io_of(&db, "g"),
        &GeneratedColumn {
            column: "b".into(),
            expression: "a * 2".into(),
            is_deleted: false,
        },
    )
    .await
    .unwrap();
    db.reload_table("g").await.unwrap();

    ok(&db, "ALTER TABLE g RENAME COLUMN a TO b").await;
    assert!(
        schema_of(&db, "g").generated.is_empty(),
        "the renamed column became generated: {:?}",
        schema_of(&db, "g").generated
    );
    ok(&db, "INSERT INTO g (b, k) VALUES (3, 1)").await;
    assert_eq!(ints(&db, "SELECT b FROM g WHERE k = 1").await, vec![Some(3)]);
    drop(db);
    let db = open(dir.path()).await;
    assert!(schema_of(&db, "g").generated.is_empty());
}


// ---------------------------------------------------------------------------
// A partial unique key's predicate.
// ---------------------------------------------------------------------------

/// (`CREATE UNIQUE INDEX … WHERE` reaches the executor as the `ADD CONSTRAINT`
/// form below; pgwire rewrites it.)
///
/// `RENAME COLUMN` of the column a partial index's predicate reads rewrites
/// the predicate record, which follows the key's own record in the sidecar.
/// Left naming the old column the predicate stopped compiling, and a key that
/// cannot read its predicate is not enforced.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_partial_keys_predicate_follows_a_rename_of_the_column_it_reads() {
    init_globals();
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;

    ok(&db, "CREATE TABLE p (k INT, email TEXT, deleted_at TEXT)").await;
    ok(&db, "ALTER TABLE p ADD CONSTRAINT p_live UNIQUE (email) WHERE deleted_at IS NULL").await;
    ok(&db, "INSERT INTO p (k, email) VALUES (1, 'a')").await;
    ok(&db, "ALTER TABLE p RENAME COLUMN deleted_at TO gone_at").await;
    let key = schema_of(&db, "p").unique_keys[0].clone();
    assert_eq!(key.predicate.as_deref(), Some("gone_at IS NULL"));
    refused(&db, "INSERT INTO p (k, email) VALUES (2, 'a')").await;
    ok(&db, "INSERT INTO p (k, email, gone_at) VALUES (3, 'a', 'x')").await;

    drop(db);
    let db = open(dir.path()).await;
    let key = schema_of(&db, "p").unique_keys[0].clone();
    assert_eq!(key.predicate.as_deref(), Some("gone_at IS NULL"));
    refused(&db, "INSERT INTO p (k, email) VALUES (4, 'a')").await;
    assert_eq!(ints(&db, "SELECT k FROM p ORDER BY k").await, vec![Some(1), Some(3)]);
}

/// A rename that would grow a predicate past what its record holds is refused
/// whole, as an over-long CHECK or generation expression is: storing it cut
/// short would change what the index covers.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_rename_that_overflows_a_partial_keys_predicate_is_refused() {
    use crate::core::rql::unique_key::MAX_PREDICATE_LEN;
    init_globals();
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;

    let predicate = vec!["d IS NULL"; 60].join(" AND ");
    assert!(predicate.len() <= MAX_PREDICATE_LEN);
    ok(&db, "CREATE TABLE q (k INT, email TEXT, d TEXT)").await;
    ok(&db, &format!("ALTER TABLE q ADD CONSTRAINT q_live UNIQUE (email) WHERE {predicate}")).await;
    let long = "x".repeat(100);
    let error = refused(&db, &format!("ALTER TABLE q RENAME COLUMN d TO {long}")).await;
    assert!(
        error.contains("the predicate of unique index q_live would exceed"),
        "{error}"
    );
    let schema = schema_of(&db, "q");
    assert!(schema.fields.iter().any(|f| f.name == "d" && !f.is_deleted));
    assert_eq!(schema.unique_keys[0].predicate.as_deref(), Some(predicate.as_str()));
    ok(&db, "INSERT INTO q (k, email) VALUES (1, 'a')").await;
    refused(&db, "INSERT INTO q (k, email) VALUES (2, 'a')").await;
}
