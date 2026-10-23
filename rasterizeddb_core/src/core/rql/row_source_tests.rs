//! The fan-out read (S10, `docs/partitioning.md` §4.2, §4.4, §8): one
//! snapshot for every partition of a statement (I7), and partitions read
//! only when the route cannot rule them out (I3), seen through the fan-out's
//! test hooks rather than timing.

use std::path::Path;
use std::sync::Arc as StdArc;
use std::sync::atomic::{AtomicI64, Ordering};

use rclite::Arc;

use super::test_hooks;
use crate::core::database::Database;
use crate::core::database::relation::Relation;
use crate::core::partition::Route;
use crate::core::partition::table::PartitionedTable;
use crate::core::row::row::{Row, vec_into_rows};
use crate::core::rql::lexer_s1::recognize_query_purpose;

const DDL: &str = "(tenant_id BIGINT NOT NULL, seq BIGINT NOT NULL, amt BIGINT NOT NULL, note TEXT, \
                   PRIMARY KEY (tenant_id, seq))";
const TENANTS: i64 = 16;
const OPENING: i64 = 100;

fn init() {
    let _ = crate::MAX_PERMITS_THREADS.set(4);
    let _ = crate::BATCH_SIZE.set(512);
    let _ = crate::core::processor::concurrent_processor::ENABLE_CACHE.set(false);
}

fn path_str(p: &Path) -> String {
    p.to_string_lossy().into_owned()
}

async fn open(root: &Path, name: &str) -> Arc<Database> {
    init();
    Arc::new(Database::new_named(&path_str(root), false, name).await)
}

fn close(db: Arc<Database>, root: &Path) {
    drop(db);
    crate::core::wal::remove_for(&path_str(root));
}

async fn exec(db: &Arc<Database>, sql: &str) -> Vec<u8> {
    let purpose = recognize_query_purpose(sql).unwrap_or_else(|| panic!("unrecognised: {sql}"));
    crate::core::rql::executor::execute(purpose, db.clone()).await
}

async fn exec_ok(db: &Arc<Database>, sql: &str) {
    let out = exec(db, sql).await;
    assert!(out.first().is_some_and(|b| *b != 3), "{sql} failed: {}", String::from_utf8_lossy(&out));
}

async fn rows(db: &Arc<Database>, sql: &str) -> Vec<Row> {
    let out = exec(db, sql).await;
    assert_eq!(out.first(), Some(&2u8), "{sql}: {}", String::from_utf8_lossy(&out));
    vec_into_rows(&out[1..]).unwrap()
}

/// The first column of every row, as the BIGINT (or COUNT) it holds.
async fn ints(db: &Arc<Database>, sql: &str) -> Vec<i64> {
    rows(db, sql)
        .await
        .iter()
        .map(|r| i64::from_le_bytes(r.columns[0].data.into_slice().try_into().unwrap()))
        .collect()
}

fn partitioned(db: &Database, name: &str) -> Arc<PartitionedTable> {
    match db.relation(name) {
        Some(Relation::Partitioned(t)) => t,
        _ => panic!("{name} is not a partitioned table"),
    }
}

fn pid_of(pt: &PartitionedTable, tenant: i64) -> u32 {
    let point = pt.spec().unwrap().point_of(&[&tenant.to_le_bytes()[..]]).unwrap();
    pt.epoch().unwrap().map.pid_of(point)
}

fn materialized(pt: &PartitionedTable) -> Vec<u32> {
    let epoch = pt.epoch().unwrap();
    pt.materialized(&epoch, &Route::All).unwrap().into_iter().map(|(pid, _)| pid).collect()
}

/// `name`, partitioned by tenant into `partitions`, with one opening row of
/// `OPENING` per tenant `1..=TENANTS`.
async fn ledger(db: &Arc<Database>, name: &str, partitions: u32) -> Arc<PartitionedTable> {
    exec_ok(db, &format!("CREATE TABLE {name} {DDL} PARTITION BY HASH (tenant_id) PARTITIONS {partitions}")).await;
    let values: Vec<String> = (1..=TENANTS).map(|t| format!("({t}, 0, {OPENING}, 'opening')")).collect();
    exec_ok(db, &format!("INSERT INTO {name} (tenant_id, seq, amt, note) VALUES {}", values.join(", "))).await;
    partitioned(db, name)
}

/// I7. A transfer between two tenants in different partitions commits after
/// a fan-out has read the first partition and before it reads the other. The
/// statement's one snapshot predates the commit, so it sees none of the
/// transfer: the sum stays what it was, the row count even, and exactly the
/// transfers committed before the statement began. A snapshot per partition
/// would see the credit and not the debit.
#[tokio::test(flavor = "multi_thread")]
async fn a_transfer_between_two_partition_reads_is_seen_whole_or_not_at_all() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "fanout_i7").await;
    let pt = ledger(&db, "acct", 7).await;
    let parts = materialized(&pt);
    assert!(parts.len() >= 2, "the tenants reach one partition only: {parts:?}");
    // The debit lands in the partition every fan-out reads first, the credit
    // in one it reads after the hook.
    let first = parts[0];
    let debit = (1..=TENANTS).find(|t| pid_of(&pt, *t) == first).unwrap();
    let credit = (1..=TENANTS).find(|t| pid_of(&pt, *t) != first).unwrap();

    let committed = StdArc::new(AtomicI64::new(0));
    let hook: test_hooks::Hook = {
        let db = db.clone();
        let committed = committed.clone();
        StdArc::new(move || {
            let db = db.clone();
            let committed = committed.clone();
            Box::pin(async move {
                let n = committed.load(Ordering::SeqCst) + 1;
                // One statement, so one transaction, across both partitions.
                exec_ok(
                    &db,
                    &format!(
                        "INSERT INTO acct (tenant_id, seq, amt, note) VALUES ({debit}, {n}, -{n}, 'out'), \
                         ({credit}, {n}, {n}, 'in')"
                    ),
                )
                .await;
                committed.store(n, Ordering::SeqCst);
            })
        })
    };
    test_hooks::set_after_first(pt.uid(), Some(hook));

    let total = TENANTS * OPENING;
    for round in 0..8i64 {
        // Row mode: every row's amount, summed here.
        let before = committed.load(Ordering::SeqCst);
        let seen: i64 = ints(&db, "SELECT amt FROM acct").await.iter().sum();
        assert_eq!(seen, total, "round {round}: a transfer was seen half applied (rows)");
        assert_eq!(committed.load(Ordering::SeqCst), before + 1, "round {round}: the hook did not run");

        // The count path: per-partition counts summed under one snapshot.
        let before = committed.load(Ordering::SeqCst);
        let count = ints(&db, "SELECT COUNT(*) FROM acct").await;
        assert_eq!(count, vec![TENANTS + 2 * before], "round {round}: COUNT(*) saw a transfer in part");
        assert_eq!(committed.load(Ordering::SeqCst), before + 1, "round {round}: the hook did not run");

        // The aggregate fold over the merged stream.
        let before = committed.load(Ordering::SeqCst);
        let mins = ints(&db, "SELECT MIN(amt) FROM acct").await;
        let expected_min = if before == 0 { OPENING } else { -before };
        assert_eq!(mins, vec![expected_min], "round {round}: MIN saw a transfer committed after the snapshot");
    }
    test_hooks::set_after_first(pt.uid(), None);

    // With the hook gone, every transfer is visible, and still balanced.
    let n = committed.load(Ordering::SeqCst);
    let seen: i64 = ints(&db, "SELECT amt FROM acct").await.iter().sum();
    assert_eq!(seen, total);
    assert_eq!(ints(&db, "SELECT COUNT(*) FROM acct").await, vec![TENANTS + 2 * n]);
    drop(pt);
    close(db, dir.path());
}

/// I3, seen from the reads: a fully keyed point read and an `IN` list read
/// only their partitions; a never-written tenant's partition and a
/// constant-false clause read none, and still answer; anything else reads
/// every materialized partition, never an unmaterialized one.
#[tokio::test(flavor = "multi_thread")]
async fn a_routed_read_touches_only_its_partitions() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "fanout_i3").await;
    let pt = ledger(&db, "wide", 64).await;
    let uid = pt.uid();
    let parts = materialized(&pt);
    assert!(parts.len() < 64, "every partition is materialized");
    test_hooks::take_reads(uid);

    // A point read.
    let got = ints(&db, "SELECT tenant_id FROM wide WHERE tenant_id = 5").await;
    assert_eq!(got, vec![5]);
    assert_eq!(test_hooks::take_reads(uid), vec![pid_of(&pt, 5)]);

    // The same through the count path, and every spelling of the value.
    for clause in ["tenant_id = 5", "tenant_id = '5'", "tenant_id = 5::bigint", "5 = tenant_id"] {
        assert_eq!(ints(&db, &format!("SELECT COUNT(*) FROM wide WHERE {clause}")).await, vec![1], "{clause}");
        assert_eq!(test_hooks::take_reads(uid), vec![pid_of(&pt, 5)], "{clause}");
    }

    // An IN list: its tenants' partitions, by pid, once each.
    let got = ints(&db, "SELECT tenant_id FROM wide WHERE tenant_id IN (3, 9, 3) ORDER BY tenant_id").await;
    assert_eq!(got, vec![3, 9]);
    let mut want = vec![pid_of(&pt, 3), pid_of(&pt, 9)];
    want.sort();
    want.dedup();
    assert_eq!(test_hooks::take_reads(uid), want);

    // A never-written tenant whose partition was never materialized: no
    // read, and still one row of 0 and one of NULL.
    let never = (1000..)
        .find(|t| !parts.contains(&pid_of(&pt, *t)))
        .unwrap();
    assert_eq!(ints(&db, &format!("SELECT COUNT(*) FROM wide WHERE tenant_id = {never}")).await, vec![0]);
    let max = rows(&db, &format!("SELECT MAX(amt) FROM wide WHERE tenant_id = {never}")).await;
    assert_eq!(max.len(), 1, "MAX over an empty route is one row");
    assert!(rows(&db, &format!("SELECT * FROM wide WHERE tenant_id = {never}")).await.is_empty());
    assert_eq!(test_hooks::take_reads(uid), Vec::<u32>::new());

    // A contradiction: no partition.
    assert_eq!(ints(&db, "SELECT COUNT(*) FROM wide WHERE tenant_id = 1 AND tenant_id = 2 AND 1 = 0").await, vec![0]);
    assert_eq!(test_hooks::take_reads(uid), Vec::<u32>::new());

    // No proof: every materialized partition, and the right rows.
    for clause in ["amt > 0", "tenant_id = 5 OR amt > 0", "tenant_id = seq + 5", "tenant_id::text = '5'"] {
        let got = ints(&db, &format!("SELECT COUNT(*) FROM wide WHERE {clause}")).await;
        let want = if clause == "tenant_id = seq + 5" || clause == "tenant_id::text = '5'" { 1 } else { TENANTS };
        assert_eq!(got, vec![want], "{clause}");
        assert_eq!(test_hooks::take_reads(uid), parts, "{clause}");
    }
    drop(pt);
    close(db, dir.path());
}

/// I3, at the router: predicates that prove nothing about the key route to
/// every partition. Each would prune a matching row if read as a key
/// equality: a key compared with another column or an expression, through a
/// cast that changes equality, a function, a negation, a range, or an `OR`
/// with a branch on another column.
#[tokio::test(flavor = "multi_thread")]
async fn predicates_without_proof_route_everywhere() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "fanout_noproof").await;
    let pt = ledger(&db, "np", 7).await;
    for clause in [
        "tenant_id = seq",
        "tenant_id = seq + 3",
        "tenant_id + 0 = 3",
        "tenant_id::text = '3'",
        "CAST(tenant_id AS TEXT) = '03'",
        "COALESCE(tenant_id, 0) = 3",
        "tenant_id = 3 OR amt > 50",
        "tenant_id = 3 OR note = 'x'",
        "tenant_id <> 3",
        "NOT (tenant_id = 3)",
        "tenant_id > 3",
        "tenant_id BETWEEN 3 AND 4",
        "note LIKE '3%'",
    ] {
        assert_eq!(pt.route_where(clause).unwrap(), Route::All, "{clause} pruned without proof");
    }
    // And the proofs route narrowly. (An `IN` list reaches the router as the
    // parse rewrote it; the SQL test above routes one.)
    for clause in ["tenant_id = 3", "tenant_id = 3 AND amt > 0", "(tenant_id = 3 OR tenant_id = 4)"] {
        assert!(matches!(pt.route_where(clause).unwrap(), Route::Points(_)), "{clause}");
    }
    drop(pt);
    close(db, dir.path());
}
