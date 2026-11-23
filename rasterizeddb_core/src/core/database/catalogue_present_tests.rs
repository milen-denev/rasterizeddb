//! `Database::has_table` answers "absent" for an engine catalogue table from a
//! bitmask, without the map. The bitmask must follow every way such a table
//! comes and goes: created, dropped, created again, and found on open.

use rclite::Arc;

use super::*;
use crate::core::rql::executor;
use crate::core::rql::lexer_s1::recognize_query_purpose;
use crate::core::rql::views::VIEW_TABLE;

async fn run(db: &Arc<Database>, sql: &str) -> Vec<u8> {
    let purpose = recognize_query_purpose(sql).unwrap_or_else(|| panic!("parse: {sql}"));
    let reply = executor::execute(purpose, db.clone()).await;
    assert_ne!(reply.first(), Some(&3), "{sql}: {}", String::from_utf8_lossy(&reply));
    reply
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_catalogue_table_is_present_exactly_while_it_exists() {
    let dir = tempfile::tempdir().unwrap();
    let path = relation_dispatch_tests::path_str(dir.path());
    let create = format!("CREATE TABLE {VIEW_TABLE} (name TEXT NOT NULL, definition TEXT)");
    {
        let db = Arc::new(relation_dispatch_tests::open(dir.path()).await);
        assert!(!db.has_table(VIEW_TABLE), "a new database has no view catalogue");
        run(&db, &create).await;
        assert!(db.has_table(VIEW_TABLE), "created, and not seen");
        run(&db, &format!("DROP TABLE {VIEW_TABLE}")).await;
        assert!(!db.has_table(VIEW_TABLE), "dropped, and still seen");
        run(&db, &create).await;
        assert!(db.has_table(VIEW_TABLE), "created again, and not seen");
        let db = Arc::try_unwrap(db).unwrap_or_else(|_| panic!("a statement still holds the database"));
        relation_dispatch_tests::close(db, dir.path());
    }
    // Found on open, where no install runs.
    let db = Database::new_named(&path, false, "dispatchtests").await;
    assert!(db.has_table(VIEW_TABLE), "reopened, and not seen");
}
