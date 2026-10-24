//! Slots and schema views (slice B3a): a swap is atomic for readers and a
//! reader keeps the instance it loaded; a schema view is one instance's
//! schema and figures, taken without blocking a writer or a DDL; a
//! partitioned table's view and slot answer for its logical name and never
//! hand out its shell as data. The wire-level stand-in for V10 case 6
//! (`pg_class` during a held ACCESS EXCLUSIVE) is in
//! `pgwire/schema_view_tests.rs`.

use std::time::{Duration, Instant};

use rclite::Arc;

use super::relation_dispatch_tests::{
    acct, close, create, insert, insert_plain, open, partitioned,
};
use super::*;
use crate::core::rql::lexer_ct::CreateColumnData;

/// `name` from [`acct`], plus `extra` INT columns.
fn wider(name: &str, extra: &[&str]) -> CreateTable {
    let mut ct = acct(name);
    for column in extra {
        ct.columns.push(CreateColumnData {
            name: column.to_string(),
            data_type: DbType::I32,
        });
    }
    ct
}

fn live_columns(schema: &TableSchema) -> Vec<String> {
    let mut v: Vec<String> = schema
        .fields
        .iter()
        .filter(|f| !f.is_deleted)
        .map(|f| f.name.clone())
        .collect();
    v.sort();
    v
}

/// A slot swap: readers on other threads see the old instance or the new
/// one, never anything else; one that loaded the old keeps it, working,
/// after the swap; the swap returns exactly the instance it replaced; and
/// the counts arc_swap borrows and owes come back level.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_slot_swap_is_atomic_for_readers() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let db = open(root).await;
    db.add_table(acct("sw_a")).await.unwrap();
    db.add_table(wider("sw_b", &["extra"])).await.unwrap();
    for n in 0..3 {
        insert_plain(&db, "sw_a", 1, n).await;
    }
    let a = db.table_for_tests("sw_a").unwrap();
    let b = db.table_for_tests("sw_b").unwrap();
    let slot = db.slot("sw_a").unwrap();
    assert!(slot.is_live());
    let (a_before, b_before) = (Arc::strong_count(&a), Arc::strong_count(&b));

    // A reader that loaded the old instance before the swap.
    let held = slot.load();
    assert!(Arc::ptr_eq(&held, &a));

    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let readers: Vec<_> = (0..3)
        .map(|_| {
            let slot = slot.clone();
            let (a, b) = (a.clone(), b.clone());
            let stop = stop.clone();
            std::thread::spawn(move || {
                let mut loads = 0u64;
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    let got = slot.load();
                    assert!(
                        Arc::ptr_eq(&got, &a) || Arc::ptr_eq(&got, &b),
                        "a reader saw neither instance: {}",
                        got.schema.name
                    );
                    loads += 1;
                }
                loads
            })
        })
        .collect();
    for i in 0..20_000 {
        let (new, old) = if i % 2 == 0 { (&b, &a) } else { (&a, &b) };
        let replaced = slot.swap(new.clone());
        assert!(
            Arc::ptr_eq(&replaced, old),
            "swap {i} returned the wrong instance"
        );
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let loads: u64 = readers.into_iter().map(|r| r.join().unwrap()).sum();
    assert!(loads > 0, "the readers never ran");

    // Even swaps: the slot holds `a` again; the held reader still works.
    assert!(Arc::ptr_eq(&slot.load(), &a));
    assert_eq!(held.schema.name, "sw_a");
    assert_eq!(
        super::relation_dispatch_tests::count_plain(&db, "sw_a").await,
        3
    );
    drop(held);
    // Every borrowed reference was paid back, none dropped twice.
    assert_eq!(
        (Arc::strong_count(&a), Arc::strong_count(&b)),
        (a_before, b_before)
    );
    drop((a, b, slot));
    close(db, root);
}

/// A view is one instance: after its slot swaps to another table, a view
/// taken before still answers with the first instance's schema and figures
/// together, and a view taken after with the second's.
#[tokio::test(flavor = "multi_thread")]
async fn a_schema_view_keeps_one_instance_across_a_swap() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let db = open(root).await;
    db.add_table(acct("sv_a")).await.unwrap();
    db.add_table(wider("sv_b", &["extra"])).await.unwrap();
    for n in 0..3 {
        insert_plain(&db, "sv_a", 1, n).await;
    }
    for n in 0..5 {
        insert_plain(&db, "sv_b", 1, n).await;
    }
    let view = db.schema_view("sv_a").unwrap();
    assert!(!view.is_partitioned());
    let before = view.stats().totals().await.unwrap();
    assert_eq!((before.ids_handed_out, before.slots), (3, 3), "{before:?}");

    let slot = db.slot("sv_a").unwrap();
    let a = slot.swap(db.table_for_tests("sv_b").unwrap());

    assert_eq!(view.schema().name, "sv_a");
    assert_eq!(live_columns(view.schema()), live_columns(&a.schema));
    assert_eq!(
        view.stats().totals().await.unwrap(),
        before,
        "the view's figures moved to the new instance"
    );

    let fresh = db.schema_view("sv_a").unwrap();
    assert!(live_columns(fresh.schema()).contains(&"extra".to_string()));
    assert_eq!(fresh.stats().totals().await.unwrap().ids_handed_out, 5);

    slot.swap(a);
    drop((view, fresh, slot));
    close(db, root);
}

/// Writers and DDL never wait for a schema view: with a view held, an
/// INSERT, an ALTER and a CREATE INDEX (which reloads the table) finish
/// promptly; the held view
/// still reads the old shape, and a new view the new one.
#[tokio::test(flavor = "multi_thread")]
async fn a_held_schema_view_never_blocks_writers_or_ddl() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let db = Arc::new(open(root).await);
    db.add_table(acct("hv")).await.unwrap();
    insert_plain(&db, "hv", 1, 1).await;
    let view = db.schema_view("hv").unwrap();

    let bound = Duration::from_secs(10);
    let run = |sql: &'static str| {
        let db = db.clone();
        async move {
            let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(sql).unwrap();
            let out = crate::core::rql::executor::execute_with_sync(purpose, db, false).await;
            assert!(
                !out.is_empty() && out[0] != 3,
                "{sql}: {}",
                String::from_utf8_lossy(&out)
            );
        }
    };
    let started = Instant::now();
    tokio::time::timeout(
        bound,
        run("INSERT INTO hv (tenant_id, n, label) VALUES (2, 2, 'y')"),
    )
    .await
    .expect("an INSERT waited for a schema view");
    // The reload first, while the view holds the instance it replaces.
    tokio::time::timeout(bound, run("CREATE INDEX hv_n ON hv (n)"))
        .await
        .expect("a CREATE INDEX (a reload) waited for a schema view");
    tokio::time::timeout(bound, run("ALTER TABLE hv ADD COLUMN later INT"))
        .await
        .expect("an ALTER waited for a schema view");
    assert!(started.elapsed() < bound);

    assert!(
        !live_columns(view.schema()).contains(&"later".to_string()),
        "the held view changed shape"
    );
    let fresh = db.schema_view("hv").unwrap();
    assert!(live_columns(fresh.schema()).contains(&"later".to_string()));
    drop((view, fresh));
    let db = Arc::try_unwrap(db)
        .ok()
        .expect("the database is still shared");
    close(db, root);
}

/// A partitioned table: its logical name has a slot (the shell's) and a
/// view; the view is the partitioned table's, with the declared schema and
/// the partitions' summed figures, never the shell's data (whose every data
/// call refuses).
#[tokio::test(flavor = "multi_thread")]
async fn a_partitioned_tables_view_covers_its_logical_name() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let db = open(root).await;
    create(&db, "pv", 8).await;
    let t = partitioned(&db, "pv");
    for tenant in 0..12 {
        insert(&t, tenant, 1).await;
    }

    let slot = db.slot("pv").expect("the logical name has no slot");
    assert!(slot.load().is_shell());
    assert!(db.has_table("pv") && db.is_partitioned("pv"));
    assert_eq!(db.relation_names().iter().filter(|n| *n == "pv").count(), 1);

    let view = db.schema_view("pv").expect("no view of the logical name");
    assert!(view.is_partitioned(), "the view is the shell's");
    assert_eq!(live_columns(view.schema()), live_columns(&t.shell().schema));
    let totals = view
        .stats()
        .totals()
        .await
        .expect("the view's figures read the shell");
    let expect = t.totals().await.unwrap();
    assert_eq!(
        (totals.bytes, totals.slots, totals.ids_handed_out),
        (expect.bytes, expect.slots, expect.ids_handed_out)
    );
    assert_eq!(totals.ids_handed_out, 12);
    assert!(totals.slots >= 12, "{totals:?}");

    drop((t, slot, view));
    close(db, root);
}

/// Not a check: how long a slot lookup takes against the map of
/// `Arc<Table>` it replaced. `cargo test -p rasterizeddb_core --lib
/// slot_lookup_cost -- --ignored --nocapture`.
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn slot_lookup_cost() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let db = open(root).await;
    for i in 0..32 {
        db.add_table(acct(&format!("lk_{i}"))).await.unwrap();
    }
    let old: DashMap<String, Arc<Table<DbStorageProvider>>, ahash::RandomState> =
        DashMap::with_hasher(ahash::RandomState::new());
    for name in db.table_names() {
        old.insert(name.clone(), db.table_for_tests(&name).unwrap());
    }
    const N: u32 = 2_000_000;
    for round in 0..3 {
        let t = Instant::now();
        for i in 0..N {
            let t = old
                .get(if i % 2 == 0 { "lk_7" } else { "lk_19" })
                .map(|t| t.value().clone());
            std::hint::black_box(t);
        }
        let map = t.elapsed();
        let t = Instant::now();
        for i in 0..N {
            // The same body as `table_unlocked`, which this file may not call.
            std::hint::black_box(db.table_for_tests(if i % 2 == 0 { "lk_7" } else { "lk_19" }));
        }
        let slots = t.elapsed();
        let t = Instant::now();
        for i in 0..N {
            std::hint::black_box(db.schema_view(if i % 2 == 0 { "lk_7" } else { "lk_19" }));
        }
        let views = t.elapsed();
        println!(
            "round {round}: map get+clone {:.1} ns, slot lookup {:.1} ns, schema_view {:.1} ns",
            map.as_nanos() as f64 / N as f64,
            slots.as_nanos() as f64 / N as f64,
            views.as_nanos() as f64 / N as f64
        );
    }
    drop(old);
    close(db, root);
}
