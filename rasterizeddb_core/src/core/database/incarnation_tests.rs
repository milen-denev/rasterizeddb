//! C7: a table's incarnation, recorded in its `_schema.db`, is new whenever
//! the table under a name is a different table — created, dropped and created
//! again, or rewritten by an `ALTER` that drops and recreates it — and the same
//! whenever it is the same table: a rename, a restart.
//!
//! What it is for: anything derived from a table and kept on disk beside it (an
//! index snapshot) records the incarnation it was taken from, and a file left
//! behind by an earlier table of the same name never matches the next one.

use super::*;
use crate::core::row::schema::TableSchema;
use crate::core::rql::lexer_ct::CreateColumnData;

async fn open_db(dir: &std::path::Path) -> Arc<Database> {
    Arc::new(Database::new_named(&dir.to_string_lossy(), false, "incarnationtests").await)
}

async fn create(db: &Database, name: &str) {
    let create = CreateTable {
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
    };
    db.add_table(create).await.expect("create table");
}

async fn exec_sql(db: &Arc<Database>, sql: &str) {
    use crate::core::rql::{executor, lexer_s1::recognize_query_purpose};
    let purpose = recognize_query_purpose(sql).expect("parse");
    executor::execute(purpose, db.clone()).await;
}

/// The incarnation `name`'s `_schema.db` records, read back from the file.
async fn on_disk(db: &Database, name: &str) -> u128 {
    let schema_io = Arc::new(
        db.db_io_pointers
            .create_new(format!("{name}_schema.db"))
            .await,
    );
    TableSchema::load(schema_io).await.expect("load schema").incarnation
}

fn in_memory(db: &Database, name: &str) -> u128 {
    db.table_for_tests(name).expect("open table").schema.incarnation
}

#[tokio::test(flavor = "multi_thread")]
async fn a_table_gets_a_new_incarnation_exactly_when_it_is_a_new_table() {
    let dir = tempfile::tempdir().unwrap();
    let rewritten;
    {
        let db = open_db(dir.path()).await;
        create(&db, "inc_t").await;
        let created = on_disk(&db, "inc_t").await;
        assert_ne!(created, 0, "CREATE TABLE recorded no incarnation");
        assert_eq!(in_memory(&db, "inc_t"), created);

        db.rename_table("inc_t", "inc_u").await.expect("rename");
        let renamed = on_disk(&db, "inc_u").await;
        assert_eq!(renamed, created, "a rename is the same table and keeps its incarnation");

        db.drop_table("inc_u").await.expect("drop");
        create(&db, "inc_u").await;
        let recreated = on_disk(&db, "inc_u").await;
        assert_ne!(recreated, 0);
        assert_ne!(recreated, renamed, "a table dropped and created again kept the old one's");

        // A populated table's ADD COLUMN drops and recreates it.
        exec_sql(&db, "INSERT INTO inc_u (id) VALUES (1)").await;
        exec_sql(&db, "ALTER TABLE inc_u ADD COLUMN extra INT").await;
        assert!(
            db.table_for_tests("inc_u")
                .unwrap()
                .schema
                .fields
                .iter()
                .any(|f| f.name == "extra" && !f.is_deleted),
            "the ALTER did not run, so the test checks nothing"
        );
        rewritten = on_disk(&db, "inc_u").await;
        assert_ne!(rewritten, 0);
        assert_ne!(rewritten, recreated, "an ALTER rewrite kept the dropped table's incarnation");
        assert_eq!(in_memory(&db, "inc_u"), rewritten);
    }

    // A restart is the same table.
    let db = open_db(dir.path()).await;
    assert_eq!(in_memory(&db, "inc_u"), rewritten, "a restart changed the incarnation");
}
