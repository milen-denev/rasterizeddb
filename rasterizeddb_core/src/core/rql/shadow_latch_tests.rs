//! The shadow row latch against a shadow rewrite in progress (slice B5,
//! `shadow_lock`): a document whose keys need no DDL, written while another
//! writer's ADD COLUMN rewrites the shadow, waits for the rewrite and lands
//! in the new table, never in the retired one.

use super::*;
use crate::core::power_loss::failpoint;

/// Writer A's new key rewrites the shadow, and the rewrite is parked after
/// its first row; writer B stores a document with an existing key meanwhile.
/// B waits for the rewrite, and after both commit the shadow holds all three
/// documents.
///
/// Mutation: `write_shadow_row` takes no latch; B's row goes to the retired
/// instance (or fails there) and is not in the shadow afterwards.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_shadow_row_written_during_a_shadow_rewrite_is_kept() {
    let base = tempfile::tempdir().unwrap();
    let root = base.path();
    let _ = crate::MAX_PERMITS_THREADS.set(4);
    let _ = crate::BATCH_SIZE.set(512);
    let db = Arc::new(Database::new_named(&root.to_string_lossy(), false, "shadow_latch").await);
    let shadow = "__json_sl_doc";
    let commit = |db: &Arc<Database>, tx: TxId| {
        db.sync_durable().unwrap();
        db.commit_transaction(tx);
        db.release_owner(crate::core::mvcc::owner::LockOwner::block(tx).unwrap());
    };
    let store = |db: &Arc<Database>, doc: &'static str, tx: TxId| {
        let db = db.clone();
        async move {
            let value: serde_json::Value = serde_json::from_str(doc).unwrap();
            crate::core::mvcc::owner::under(
                &db.transactions.clone(),
                tx,
                crate::core::mvcc::owner::OwnerKind::Block,
                crate::core::rql::shadow_lock::scoped("sl", false, Box::pin(store_json_document(&db, shadow, &value, 0, tx))),
            )
            .await
        }
    };
    let tx0 = db.transactions.begin();
    store(&db, r#"{"a":1}"#, tx0).await.unwrap();
    commit(&db, tx0);

    // Parks the rewrite after its first row until released.
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let release_rx = std::sync::Mutex::new(release_rx);
    let (parked_tx, parked_rx) = tokio::sync::oneshot::channel::<()>();
    let parked_tx = std::sync::Mutex::new(Some(parked_tx));
    let _park = failpoint::arm(
        "test.rewrite.mid_fill",
        root,
        1,
        std::sync::Arc::new(move |_hit: &failpoint::Hit<'_>| {
            if let Some(tx) = parked_tx.lock().unwrap().take() {
                let _ = tx.send(());
            }
            let _ = release_rx.lock().unwrap().recv_timeout(std::time::Duration::from_secs(20));
            failpoint::Outcome::Continue
        }),
    );
    // A: a new key, so an ADD COLUMN rewrite of a shadow holding a row.
    let tx1 = db.transactions.begin();
    let a = tokio::spawn(store(&db, r#"{"b":2}"#, tx1));
    tokio::time::timeout(std::time::Duration::from_secs(20), parked_rx)
        .await
        .expect("the shadow rewrite never reached its fill")
        .unwrap();
    // B: an existing key, no DDL; its row write meets the rewrite.
    let tx2 = db.transactions.begin();
    let b = tokio::spawn(store(&db, r#"{"a":5}"#, tx2));
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert!(!b.is_finished(), "a shadow row was written beside the shadow's rewrite");
    release_tx.send(()).unwrap();
    a.await.unwrap().expect("writer A failed");
    b.await.unwrap().expect("writer B failed");
    commit(&db, tx1);
    commit(&db, tx2);

    let table = db.table_for_tests(shadow).unwrap().clone();
    let fetch = crate::core::row::table::Table::<crate::core::storage_providers::backend::DbStorageProvider>::build_full_row_fetch(
        &table.schema.fields,
    );
    let rows = table
        .query_row("", &table.schema.fields.to_vec(), crate::core::row::table::Table::<crate::core::storage_providers::backend::DbStorageProvider>::build_fetch_for(&table.schema.fields, &[]), fetch, None, None)
        .await
        .unwrap();
    assert_eq!(rows.len(), 3, "the shadow lost a document written during its rewrite");
    drop(table);
    drop(db);
    crate::core::wal::remove_for(&root.to_string_lossy());
}
