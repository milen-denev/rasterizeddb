//! Partitioned `UPDATE`, `DELETE` and `VACUUM` (S11, `docs/partitioning.md`
//! §4.4, §4.5, §9): one snapshot and one transaction across partitions, a
//! failure that leaves no partition changed (after a restart and in a crash
//! image too), conflicts retried only while nothing was applied (I17), a key
//! change refused before anything is written (I4), routed writes that touch
//! only their partitions (I3), and VACUUM over the partitions (finding F2).
//!
//! Seen through the fan-out's test hooks and the engine's API rather than
//! timing. Bounded: fixed row and statement counts.

use std::sync::Arc as StdArc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use rclite::Arc;

use super::test_hooks;
use super::tests::{
    OPENING, TENANTS, close, exec, exec_ok, ints, ledger, materialized, open, partitioned, pid_of, rows,
};
use crate::core::database::Database;
use crate::core::mvcc::transaction::TxId;
use crate::core::partition::guard::KEY_CHANGE_REFUSAL;
use crate::core::partition::table::PartitionedTable;
use crate::core::rql::lexer_s1::recognize_query_purpose;
use crate::core::storage_providers::traits::StorageIO;

fn purpose(sql: &str) -> crate::core::rql::lexer_s1::QueryPurpose {
    recognize_query_purpose(sql).unwrap_or_else(|| panic!("unrecognised: {sql}"))
}

/// An autocommit statement whose commit waits for its sync, as a client's
/// default does: what a crash image then must keep.
async fn exec_sync(db: &Arc<Database>, sql: &str) -> Vec<u8> {
    crate::core::rql::executor::execute_with_sync(purpose(sql), db.clone(), true).await
}

/// The failure text of a statement that must fail.
async fn exec_err(db: &Arc<Database>, sql: &str) -> String {
    let out = exec(db, sql).await;
    assert_eq!(out.first(), Some(&3u8), "{sql} succeeded: {:?}", &out[..out.len().min(16)]);
    String::from_utf8_lossy(&out[1..]).into_owned()
}

/// Rows affected, from a status-0 reply.
fn affected(out: &[u8], sql: &str) -> u64 {
    assert_eq!(out.first(), Some(&0u8), "{sql}: {}", String::from_utf8_lossy(out));
    u64::from_le_bytes(out[1..9].try_into().unwrap())
}

/// Runs `sql` in the caller's transaction, as a statement of a block does.
async fn exec_in(db: &Arc<Database>, tx: TxId, sql: &str) -> Vec<u8> {
    crate::core::rql::executor::execute_in_txn(purpose(sql), db.clone(), tx).await
}

fn commit(db: &Arc<Database>, tx: TxId) {
    db.commit_written_transaction(tx).expect("commit");
    db.transactions.flush().expect("flush the outcome");
}

fn rollback(db: &Arc<Database>, tx: TxId) {
    db.abort_transaction(tx);
    let _ = db.transactions.flush();
}

/// Every `(tenant_id, seq, amt)` of `table`, sorted.
async fn ledger_rows(db: &Arc<Database>, table: &str) -> Vec<(i64, i64, i64)> {
    let mut out: Vec<(i64, i64, i64)> = rows(db, &format!("SELECT tenant_id, seq, amt FROM {table}"))
        .await
        .iter()
        .map(|r| {
            let i = |k: usize| i64::from_le_bytes(r.columns[k].data.into_slice().try_into().unwrap());
            (i(0), i(1), i(2))
        })
        .collect();
    out.sort();
    out
}

/// Each materialized partition's pointer slots, by pid: a version written
/// anywhere, committed or not, adds one.
async fn slots_by_partition(pt: &PartitionedTable) -> Vec<(u32, u64)> {
    pt.partitions()
        .await
        .unwrap()
        .into_iter()
        .filter(|p| p.state == "materialized")
        .map(|p| (p.pid, p.slots))
        .collect()
}

/// Bytes of pointer records over every materialized partition: what VACUUM
/// FULL gives back.
async fn pointer_bytes(pt: &PartitionedTable) -> u64 {
    let epoch = pt.epoch().unwrap();
    let mut total = 0;
    for (_, part) in pt.materialized(&epoch, &crate::core::partition::Route::All).unwrap() {
        total += part.io_pointers().unwrap().get_len().await;
    }
    total
}

/// Two tenants of `pt` in different partitions, the first in the lower pid.
fn two_partitions(pt: &PartitionedTable) -> (i64, i64) {
    let first = materialized(pt)[0];
    let low = (1..=TENANTS).find(|t| pid_of(pt, *t) == first).unwrap();
    let high = (1..=TENANTS).find(|t| pid_of(pt, *t) != first).unwrap();
    (low, high)
}

/// A one-shot hook: runs `body` the first time a fan-out over the table has
/// visited its first partition, and nothing on later fan-outs (the body's
/// own statements included).
fn once(body: impl Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send + Sync + 'static)
-> (test_hooks::Hook, StdArc<AtomicBool>) {
    let fired = StdArc::new(AtomicBool::new(false));
    let f = fired.clone();
    let body = StdArc::new(body);
    let hook: test_hooks::Hook = StdArc::new(move || {
        let (f, body) = (f.clone(), body.clone());
        Box::pin(async move {
            if !f.swap(true, Ordering::SeqCst) {
                body().await;
            }
        })
    });
    (hook, fired)
}

/// I7 for a write. A fan-out `UPDATE … WHERE amt > 100` visits the first
/// partition, where tenant `a` holds 50 (no match); a transfer then commits
/// moving 100 from `b` (150, in a later partition) to `a`; then the fan-out
/// visits `b`'s partition. Under any serial order exactly one of the two
/// rows has more than 100 when the UPDATE runs, so exactly one gets the
/// bonus. With the statement's one snapshot, `b`'s old version is still
/// visible and retired by a committed transaction: a write conflict, raised
/// with nothing applied, so the autocommit replays the statement, which then
/// matches `a`. A snapshot taken per partition would read `b` after the
/// transfer, match neither, and report `UPDATE 0`.
#[tokio::test(flavor = "multi_thread")]
async fn a_fan_out_update_reads_every_partition_under_one_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "write_i7").await;
    let pt = ledger(&db, "acct", 7).await;
    let (a, b) = two_partitions(&pt);
    exec_ok(&db, &format!("UPDATE acct SET amt = 50 WHERE tenant_id = {a}")).await;
    exec_ok(&db, &format!("UPDATE acct SET amt = 150 WHERE tenant_id = {b}")).await;

    let (hook, fired) = {
        let db = db.clone();
        once(move || {
            let db = db.clone();
            Box::pin(async move {
                exec_ok(
                    &db,
                    &format!(
                        "UPDATE acct SET amt = CASE WHEN tenant_id = {a} THEN amt + 100 ELSE amt - 100 END \
                         WHERE tenant_id IN ({a}, {b})"
                    ),
                )
                .await;
            })
        })
    };
    test_hooks::set_after_first(pt.uid(), Some(hook));
    let sql = "UPDATE acct SET note = 'bonus' WHERE amt > 100";
    let n = affected(&exec(&db, sql).await, sql);
    test_hooks::set_after_first(pt.uid(), None);
    assert!(fired.load(Ordering::SeqCst), "the transfer never ran between two partition visits");

    assert_eq!(n, 1, "exactly one row had more than 100 in any serial order");
    let got = ints(&db, "SELECT tenant_id FROM acct WHERE note = 'bonus'").await;
    assert_eq!(got, vec![a], "the bonus went to the wrong row");
    // The transfer itself landed whole.
    let amt = |t: i64| format!("SELECT amt FROM acct WHERE tenant_id = {t}");
    assert_eq!(ints(&db, &amt(a)).await, vec![150]);
    assert_eq!(ints(&db, &amt(b)).await, vec![50]);
    drop(pt);
    close(db, dir.path());
}

/// I7 for a read, with a real UPDATE as the transfer (S10's test used an
/// INSERT pair): a cross-partition `UPDATE` moving value between two tenants
/// commits after a fan-out read has seen the first partition. The read sees
/// none of it: the sum stays put, and the debited tenant's balance is what
/// it was when the read began.
#[tokio::test(flavor = "multi_thread")]
async fn an_update_transfer_between_two_partition_reads_is_seen_whole_or_not_at_all() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "write_read_i7").await;
    let pt = ledger(&db, "acct", 7).await;
    let (debit, credit) = two_partitions(&pt);
    let moved = StdArc::new(AtomicU64::new(0));
    let hook: test_hooks::Hook = {
        let (db, moved) = (db.clone(), moved.clone());
        StdArc::new(move || {
            let (db, moved) = (db.clone(), moved.clone());
            Box::pin(async move {
                // Nested: the transfer's own fan-out comes through here too.
                if moved.load(Ordering::SeqCst) % 2 == 1 {
                    return;
                }
                moved.fetch_add(1, Ordering::SeqCst);
                exec_ok(
                    &db,
                    &format!(
                        "UPDATE acct SET amt = CASE WHEN tenant_id = {debit} THEN amt - 7 ELSE amt + 7 END \
                         WHERE tenant_id IN ({debit}, {credit})"
                    ),
                )
                .await;
                moved.fetch_add(1, Ordering::SeqCst);
            })
        })
    };
    test_hooks::set_after_first(pt.uid(), Some(hook));
    let total = TENANTS * OPENING;
    for round in 0..6i64 {
        let done = moved.load(Ordering::SeqCst) as i64 / 2;
        let all = ledger_rows(&db, "acct").await;
        assert_eq!(all.iter().map(|r| r.2).sum::<i64>(), total, "round {round}: a transfer seen half applied");
        let mine = all.iter().find(|r| r.0 == debit).unwrap().2;
        assert_eq!(mine, OPENING - 7 * done, "round {round}: the read saw a transfer committed after it began");
        assert_eq!(moved.load(Ordering::SeqCst) as i64 / 2, done + 1, "round {round}: the hook did not run");
    }
    test_hooks::set_after_first(pt.uid(), None);
    let all = ledger_rows(&db, "acct").await;
    assert_eq!(all.iter().map(|r| r.2).sum::<i64>(), total);
    drop(pt);
    close(db, dir.path());
}

/// I3 for writes: a fully keyed or tenant-keyed UPDATE and DELETE reach
/// only their partitions, an `IN` list its tenants' partitions, a
/// never-written tenant and a contradiction none; a clause without proof
/// reaches every materialized partition and never an unmaterialized one.
/// And what was not routed to was not changed.
#[tokio::test(flavor = "multi_thread")]
async fn a_routed_write_touches_only_its_partitions() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "write_i3").await;
    let pt = ledger(&db, "wide", 64).await;
    let uid = pt.uid();
    let parts = materialized(&pt);
    assert!(parts.len() < 64);
    test_hooks::take_writes(uid);

    let before = slots_by_partition(&pt).await;
    let sql = "UPDATE wide SET amt = amt + 1 WHERE tenant_id = 5 AND seq = 0";
    assert_eq!(affected(&exec(&db, sql).await, sql), 1);
    assert_eq!(test_hooks::take_writes(uid), vec![pid_of(&pt, 5)], "{sql}");
    let after = slots_by_partition(&pt).await;
    for ((pid, a), (_, b)) in before.iter().zip(&after) {
        if *pid != pid_of(&pt, 5) {
            assert_eq!(a, b, "partition {pid} was written by a write routed elsewhere");
        }
    }

    let sql = "UPDATE wide SET note = 'x' WHERE tenant_id = 5";
    assert_eq!(affected(&exec(&db, sql).await, sql), 1);
    assert_eq!(test_hooks::take_writes(uid), vec![pid_of(&pt, 5)], "{sql}");

    let sql = "DELETE FROM wide WHERE tenant_id IN (3, 9, 3)";
    assert_eq!(affected(&exec(&db, sql).await, sql), 2);
    let mut want = vec![pid_of(&pt, 3), pid_of(&pt, 9)];
    want.sort();
    want.dedup();
    assert_eq!(test_hooks::take_writes(uid), want, "{sql}");

    let never = (1000..).find(|t| !parts.contains(&pid_of(&pt, *t))).unwrap();
    for sql in [
        format!("UPDATE wide SET amt = 0 WHERE tenant_id = {never}"),
        format!("DELETE FROM wide WHERE tenant_id = {never}"),
        "UPDATE wide SET amt = 0 WHERE tenant_id = 1 AND tenant_id = 2 AND 1 = 0".to_string(),
    ] {
        assert_eq!(affected(&exec(&db, &sql).await, &sql), 0);
        assert_eq!(test_hooks::take_writes(uid), Vec::<u32>::new(), "{sql}");
    }

    for sql in ["UPDATE wide SET amt = amt + 1 WHERE amt > 0", "DELETE FROM wide WHERE note = 'nothing'"] {
        exec_ok(&db, sql).await;
        assert_eq!(test_hooks::take_writes(uid), parts, "{sql}");
    }

    // A function call in WHERE (evaluated since upstream's 4301045) proves
    // nothing about the key, even wrapping it: every partition, and the
    // rows it matches. Tenants 3 and 9 were deleted above; 5's note is 'x'.
    for (sql, want) in [
        ("UPDATE wide SET amt = amt + 1 WHERE ABS(tenant_id) = 5", 1),
        ("UPDATE wide SET amt = amt + 1 WHERE COALESCE(tenant_id, 0) = 6", 1),
        ("UPDATE wide SET amt = amt + 1 WHERE UPPER(note) = 'X'", 1),
        ("DELETE FROM wide WHERE LOWER(note) = 'nothing' OR ABS(tenant_id) = 7", 1),
    ] {
        assert_eq!(affected(&exec(&db, sql).await, sql), want, "{sql}");
        assert_eq!(test_hooks::take_writes(uid), parts, "{sql}: pruned without proof");
    }
    assert_eq!(materialized(&pt), parts, "a write materialized a partition");
    drop(pt);
    close(db, dir.path());
}

/// A multi-partition UPDATE or DELETE that fails before its `k`-th partition,
/// the earlier ones written, changes no partition: in autocommit, in a
/// block, after a restart, and in every crash image cut after the failure.
/// A crash cut in the middle of a statement that then succeeds shows none of
/// it either; one cut after it, all of it.
#[tokio::test(flavor = "multi_thread")]
async fn a_multi_partition_write_that_fails_midway_changes_no_partition() {
    use crate::core::power_loss::image::Recorder;

    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().join("db");
    let name = "write_atomic";
    let db = open(&base, name).await;
    let pt = ledger(&db, "acct", 7).await;
    let uid = pt.uid();
    let parts = materialized(&pt);
    assert!(parts.len() >= 3, "too few partitions to fail midway: {parts:?}");
    let original = ledger_rows(&db, "acct").await;
    drop(pt);

    // Autocommit, an UPDATE and a DELETE, failing before the third partition.
    test_hooks::fail_before(uid, Some(2));
    for sql in ["UPDATE acct SET amt = amt + 1", "DELETE FROM acct WHERE amt > 0"] {
        let e = exec_err(&db, sql).await;
        assert!(e.contains("injected failure"), "{sql}: {e}");
        assert_eq!(ledger_rows(&db, "acct").await, original, "{sql} changed a partition before failing");
    }
    // In a block: the statement fails, the block rolls back.
    let tx = db.transactions.begin();
    exec_ok_in(&db, tx, "UPDATE acct SET note = 'in block' WHERE tenant_id = 1").await;
    let out = exec_in(&db, tx, "UPDATE acct SET amt = amt - 1").await;
    assert_eq!(out.first(), Some(&3u8), "the injected failure did not fail the statement in a block");
    rollback(&db, tx);
    assert_eq!(ledger_rows(&db, "acct").await, original);
    test_hooks::fail_before(uid, None);

    // After a restart.
    db.sync_durable().unwrap();
    close(db, &base);
    let db = open(&base, name).await;
    assert_eq!(ledger_rows(&db, "acct").await, original, "a failed write came back after a restart");

    // Crash images: after a failed statement, in the middle of one that
    // succeeds, and after it.
    db.sync_durable().unwrap();
    let rec = Recorder::start(&base);
    let pt = partitioned(&db, "acct");
    test_hooks::fail_before(uid, Some(2));
    let sql = "UPDATE acct SET amt = amt + 5";
    let e = String::from_utf8_lossy(&exec_sync(&db, sql).await).into_owned();
    assert!(e.contains("injected failure"), "{e}");
    test_hooks::fail_before(uid, None);
    let mut cuts = vec![(rec.cut_quiescent("after the failed UPDATE"), original.clone())];

    let mid = StdArc::new(parking_lot::Mutex::new(Vec::new()));
    let (hook, fired) = {
        let (rec, mid) = (rec.clone(), mid.clone());
        once(move || {
            let (rec, mid) = (rec.clone(), mid.clone());
            Box::pin(async move {
                mid.lock().push(rec.cut_quiescent("between two partitions of a committing UPDATE"));
            })
        })
    };
    test_hooks::set_after_first(uid, Some(hook));
    let out = exec_sync(&db, sql).await;
    test_hooks::set_after_first(uid, None);
    assert_eq!(affected(&out, sql), TENANTS as u64);
    assert!(fired.load(Ordering::SeqCst));
    for cut in std::mem::take(&mut *mid.lock()) {
        cuts.push((cut, original.clone()));
    }
    let updated: Vec<(i64, i64, i64)> = original.iter().map(|&(t, s, a)| (t, s, a + 5)).collect();
    assert_eq!(ledger_rows(&db, "acct").await, updated);
    cuts.push((rec.cut_quiescent("after the committed UPDATE"), updated.clone()));
    drop(pt);
    close(db, &base);
    drop(rec);

    let img = dir.path().join("image");
    let mut imaged = 0;
    for (cut, want) in cuts {
        let cut = match cut {
            Ok(cut) => cut,
            // Another thread recorded a sync during the cut: no image.
            Err(why) => {
                eprintln!("disturbed: {why}");
                continue;
            }
        };
        for seed in 0..5 {
            cut.write_image(seed, &img);
            let db = open(&img, name).await;
            assert_eq!(ledger_rows(&db, "acct").await, want, "{} seed {seed}", cut.label);
            close(db, &img);
            let _ = std::fs::remove_dir_all(&img);
            imaged += 1;
        }
    }
    assert!(imaged >= 10, "only {imaged} images were checked");
}

async fn exec_ok_in(db: &Arc<Database>, tx: TxId, sql: &str) {
    let out = exec_in(db, tx, sql).await;
    assert!(out.first().is_some_and(|b| *b != 3), "{sql} failed: {}", String::from_utf8_lossy(&out));
}

/// I17. Another transaction holds an uncommitted update of tenant `b`'s row.
/// A fan-out `UPDATE … SET amt = amt + 1` applies tenant `a`'s row in the
/// first partition, then conflicts on `b`. Having applied a row, the
/// conflict is not retryable: replayed in the same transaction (an explicit
/// block's `execute_in_txn`), the statement would see its own new version of
/// `a` and add 1 again. So the statement fails at once, and once the blocker
/// has ended, `a` has been incremented at most once.
#[tokio::test(flavor = "multi_thread")]
async fn a_conflict_after_a_partition_applied_rows_is_not_replayed() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "write_i17").await;
    let pt = ledger(&db, "acct", 7).await;
    let (a, b) = two_partitions(&pt);
    let sql = format!("UPDATE acct SET amt = amt + 1 WHERE tenant_id IN ({a}, {b})");
    let amt = |t: i64| format!("SELECT amt FROM acct WHERE tenant_id = {t}");

    // The block first: its replay, the one that would double-apply, is I17's.
    for in_block in [true, false] {
        let start = ints(&db, &amt(a)).await[0];
        let blocker = db.transactions.begin();
        exec_ok_in(&db, blocker, &format!("UPDATE acct SET note = 'held' WHERE tenant_id = {b}")).await;
        // The blocker ends while a replay, if there were one, would still be
        // backing off.
        let ender = {
            let db = db.clone();
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(40)).await;
                rollback(&db, blocker);
            })
        };
        let out = if in_block {
            let tx = db.transactions.begin();
            let out = exec_in(&db, tx, &sql).await;
            if out.first() == Some(&3) {
                rollback(&db, tx);
            } else {
                commit(&db, tx);
            }
            out
        } else {
            exec(&db, &sql).await
        };
        ender.await.unwrap();
        let text = String::from_utf8_lossy(&out[1..]).into_owned();
        let now = ints(&db, &amt(a)).await[0];
        assert!(now - start <= 1, "in_block {in_block}: `{sql}` applied tenant {a} {} times", now - start);
        assert_eq!(out.first(), Some(&3u8), "in_block {in_block}: the conflict was replayed into a success");
        assert!(text.contains("could not serialize"), "in_block {in_block}: {text}");
        assert!(!text.ends_with(" [retryable]"), "in_block {in_block}: the marker reached the client: {text}");
        assert_eq!(now, start, "in_block {in_block}: a failed statement left a change");
    }
    // Nothing held: the statement goes through, once.
    let start = ints(&db, &amt(a)).await[0];
    assert_eq!(affected(&exec(&db, &sql).await, &sql), 2);
    assert_eq!(ints(&db, &amt(a)).await, vec![start + 1]);
    drop(pt);
    close(db, dir.path());
}

/// I4 through SQL: an UPDATE assigning a key a value the WHERE does not pin
/// is refused with 0A000 before anything is written, in any partition: the
/// rows, every partition's slot count and in-place counter are unchanged.
/// The statement below reaches two partitions, the lower holding only rows
/// the assignment leaves where they are; the range guard alone would refuse
/// it only after writing that partition's new versions. Assigning a key the
/// value its WHERE pins it to is ordinary.
#[tokio::test(flavor = "multi_thread")]
async fn a_key_change_is_refused_before_any_partition_is_written() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "write_i4").await;
    let pt = ledger(&db, "acct", 7).await;
    let (low, high) = two_partitions(&pt);
    let original = ledger_rows(&db, "acct").await;
    let slots = slots_by_partition(&pt).await;
    let epoch = pt.epoch().unwrap();
    let in_place = |pt: &PartitionedTable| -> Vec<u64> {
        pt.materialized(&epoch, &crate::core::partition::Route::All)
            .unwrap()
            .iter()
            .map(|(_, t)| t.in_place_column_updates())
            .collect()
    };
    let in_place_before = in_place(&pt);
    for sql in [
        format!("UPDATE acct SET tenant_id = {low} WHERE tenant_id IN ({low}, {high}) AND amt > 0"),
        format!("UPDATE acct SET tenant_id = {low}, amt = 1 WHERE amt > 0"),
        format!("UPDATE acct SET tenant_id = 999 WHERE tenant_id = {low}"),
        format!("UPDATE acct SET tenant_id = tenant_id + 0 WHERE tenant_id = {low}"),
        format!("UPDATE acct SET tenant_id = {high} WHERE tenant_id = {low}"),
        format!("UPDATE acct SET tenant_id = {low} WHERE tenant_id = {low} OR tenant_id = {high}"),
    ] {
        let e = exec_err(&db, &sql).await;
        assert!(e.contains(KEY_CHANGE_REFUSAL), "{sql}: {e}");
        assert_eq!(ledger_rows(&db, "acct").await, original, "{sql} changed a row");
        assert_eq!(slots_by_partition(&pt).await, slots, "{sql} wrote a version");
        assert_eq!(in_place(&pt), in_place_before, "{sql} overwrote in place");
    }
    // The same value, spelled as the WHERE spells it or otherwise.
    for sql in [
        format!("UPDATE acct SET tenant_id = {low}, amt = amt + 1 WHERE tenant_id = {low}"),
        format!("UPDATE acct SET tenant_id = '{low}', note = 'k' WHERE seq = 0 AND tenant_id = {low}::bigint"),
    ] {
        assert_eq!(affected(&exec(&db, &sql).await, &sql), 1, "{sql}");
    }
    assert_eq!(ints(&db, &format!("SELECT amt FROM acct WHERE tenant_id = {low}")).await, vec![OPENING + 1]);
    drop(pt);
    close(db, dir.path());
}

/// Finding F2: a bare VACUUM vacuumed the shell, which has no rows, and
/// failed in any database holding a partitioned table. Now it vacuums every
/// plain table and every materialized partition, and VACUUM FULL gives the
/// partitions' dead space back.
#[tokio::test(flavor = "multi_thread")]
async fn a_bare_vacuum_with_a_partitioned_table_present_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "write_f2").await;
    let pt = ledger(&db, "acct", 7).await;
    exec_ok(&db, "CREATE TABLE plain_t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT)").await;
    exec_ok(&db, "INSERT INTO plain_t (id, v) VALUES (1, 1), (2, 2), (3, 3)").await;
    // Dead versions in every partition, and in the plain table.
    for _ in 0..3 {
        exec_ok(&db, "UPDATE acct SET note = 'churn', amt = amt + 1").await;
    }
    exec_ok(&db, "DELETE FROM plain_t WHERE id = 2").await;
    exec_ok(&db, "DELETE FROM acct WHERE tenant_id IN (2, 5, 9)").await;
    let live = ledger_rows(&db, "acct").await;
    let before = pointer_bytes(&pt).await;

    let out = exec(&db, "VACUUM").await;
    assert_eq!(out.first(), Some(&0u8), "bare VACUUM: {}", String::from_utf8_lossy(&out));
    let out = exec(&db, "VACUUM FULL").await;
    assert_eq!(out.first(), Some(&0u8), "bare VACUUM FULL: {}", String::from_utf8_lossy(&out));
    assert!(pointer_bytes(&pt).await < before, "VACUUM FULL left the partitions' pointer files at {before} bytes");
    assert_eq!(ledger_rows(&db, "acct").await, live);
    assert_eq!(ints(&db, "SELECT id FROM plain_t ORDER BY id").await, vec![1, 3]);
    drop(pt);
    close(db, dir.path());
}

/// VACUUM FULL on a partitioned table compacts each partition, and the table
/// then answers like its plain twin through every path that could see a
/// stale slot: a fan-out, a routed point read (index and filters), a count,
/// writes after it, and a restart.
#[tokio::test(flavor = "multi_thread")]
async fn vacuum_full_on_a_partitioned_table_keeps_it_like_its_twin() {
    let dir = tempfile::tempdir().unwrap();
    let name = "write_vacuum_full";
    let db = open(dir.path(), name).await;
    let pt = ledger(&db, "p", 7).await;
    exec_ok(&db, &format!("CREATE TABLE q {}", super::tests::DDL)).await;
    let opening: Vec<String> = (1..=TENANTS).map(|t| format!("({t}, 0, {OPENING}, 'opening')")).collect();
    exec_ok(&db, &format!("INSERT INTO q (tenant_id, seq, amt, note) VALUES {}", opening.join(", "))).await;

    let both = |sql: &str| [sql.replace("{t}", "p"), sql.replace("{t}", "q")];
    let mut script: Vec<String> = Vec::new();
    for k in 1..=4 {
        let values: Vec<String> = (1..=TENANTS).map(|t| format!("({t}, {k}, {}, 'n{k}')", t * k)).collect();
        script.push(format!("INSERT INTO {{t}} (tenant_id, seq, amt, note) VALUES {}", values.join(", ")));
    }
    script.push("UPDATE {t} SET note = 'a much longer note that relocates the row' WHERE seq = 2".into());
    script.push("DELETE FROM {t} WHERE seq = 3 OR tenant_id IN (4, 11)".into());
    script.push("UPDATE {t} SET amt = amt * 2 WHERE amt > 20".into());
    for sql in &script {
        for s in both(sql) {
            exec_ok(&db, &s).await;
        }
    }
    let compare = |label: &'static str| {
        let db = db.clone();
        async move {
            assert_eq!(ledger_rows(&db, "p").await, ledger_rows(&db, "q").await, "{label}: rows");
            for t in 1..=TENANTS {
                for s in 0..=4 {
                    let sql = format!("SELECT amt, note FROM {{t}} WHERE tenant_id = {t} AND seq = {s}");
                    let [a, b] = both(&sql);
                    let (a, b) = (rows(&db, &a).await, rows(&db, &b).await);
                    let cols = |r: &Vec<crate::core::row::row::Row>| {
                        r.iter().map(|r| r.columns.iter().map(|c| c.data.into_slice().to_vec()).collect::<Vec<_>>()).collect::<Vec<_>>()
                    };
                    assert_eq!(cols(&a), cols(&b), "{label}: point read of ({t}, {s})");
                }
            }
            let [a, b] = both("SELECT COUNT(*) FROM {t} WHERE amt > 10");
            assert_eq!(ints(&db, &a).await, ints(&db, &b).await, "{label}: count");
        }
    };
    compare("before").await;
    let before = pointer_bytes(&pt).await;
    for s in both("VACUUM FULL {t}") {
        exec_ok(&db, &s).await;
    }
    assert!(pointer_bytes(&pt).await < before, "VACUUM FULL p compacted no partition ({before} bytes of pointers)");
    compare("after VACUUM FULL").await;

    for sql in [
        "INSERT INTO {t} (tenant_id, seq, amt, note) VALUES (4, 9, 1, 'back'), (13, 9, 2, 'new')",
        "UPDATE {t} SET amt = amt + 1 WHERE tenant_id IN (1, 4, 13)",
        "DELETE FROM {t} WHERE seq = 1 AND tenant_id > 8",
    ] {
        for s in both(sql) {
            exec_ok(&db, &s).await;
        }
    }
    compare("writes after VACUUM FULL").await;
    for s in both("VACUUM {t}") {
        exec_ok(&db, &s).await;
    }
    drop(pt);
    close(db, dir.path());
    let db = open(dir.path(), name).await;
    let pt = partitioned(&db, "p");
    let compare = |label: &'static str| {
        let db = db.clone();
        async move {
            assert_eq!(ledger_rows(&db, "p").await, ledger_rows(&db, "q").await, "{label}: rows");
        }
    };
    compare("after a restart").await;
    for sql in ["UPDATE {t} SET amt = 0 WHERE tenant_id = 1", "DELETE FROM {t} WHERE tenant_id = 2"] {
        for s in both(sql) {
            exec_ok(&db, &s).await;
        }
    }
    compare("writes after the restart").await;
    drop(pt);
    close(db, dir.path());
}

/// Concurrent increments of one row in one partition behave as on a plain
/// table (memory "point updates and key stripes"): each success is counted
/// once, the row never forks into two live versions, and a conflict that
/// escapes the replays is a 40001 the client sees, no more often than on the
/// plain twin (with slack: the replay loop's backoff is timing).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_updates_of_one_row_behave_as_on_a_plain_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "write_same_row").await;
    let pt = ledger(&db, "p", 7).await;
    exec_ok(&db, &format!("CREATE TABLE q {}", super::tests::DDL)).await;
    exec_ok(&db, &format!("INSERT INTO q (tenant_id, seq, amt, note) VALUES (1, 0, {OPENING}, 'opening')")).await;
    const TASKS: usize = 16;
    const EACH: usize = 40;
    let mut outcome = Vec::new();
    for table in ["p", "q"] {
        let started = std::time::Instant::now();
        let mut handles = Vec::new();
        for _ in 0..TASKS {
            let db = db.clone();
            let sql = format!("UPDATE {table} SET amt = amt + 1 WHERE tenant_id = 1 AND seq = 0");
            handles.push(tokio::spawn(async move {
                let (mut ok, mut conflict) = (0u64, 0u64);
                for _ in 0..EACH {
                    let out = exec(&db, &sql).await;
                    match out.first() {
                        Some(0) => ok += u64::from_le_bytes(out[1..9].try_into().unwrap()),
                        _ => {
                            let text = String::from_utf8_lossy(&out[1..]).into_owned();
                            assert!(text.contains("could not serialize"), "{sql}: {text}");
                            conflict += 1;
                        }
                    }
                }
                (ok, conflict)
            }));
        }
        let (mut ok, mut conflict) = (0, 0);
        for h in handles {
            let (o, c) = h.await.unwrap();
            ok += o;
            conflict += c;
        }
        let amt = ints(&db, &format!("SELECT amt FROM {table} WHERE tenant_id = 1 AND seq = 0")).await;
        assert_eq!(amt, vec![OPENING + ok as i64], "{table}: the row's increments are not its successes");
        assert_eq!(
            ints(&db, &format!("SELECT COUNT(*) FROM {table} WHERE tenant_id = 1")).await,
            vec![1],
            "{table}: the row forked"
        );
        eprintln!("{table}: {ok} increments, {conflict} conflicts in {:?}", started.elapsed());
        outcome.push((ok, conflict));
    }
    let (p, q) = (outcome[0], outcome[1]);
    assert!(
        p.1 <= q.1 + (TASKS * EACH / 20) as u64,
        "the partitioned table raised {} conflicts, the plain one {}",
        p.1,
        q.1
    );
    drop(pt);
    close(db, dir.path());
}

/// `LOCK TABLE` on the logical name holds off a partitioned UPDATE and
/// DELETE from another transaction, exactly as it does a plain table's: the
/// writer's ROW EXCLUSIVE is asked by the logical name, before any partition
/// is routed to, and waits (here bounded by `lock_timeout`, so it ends in
/// 55P03 instead of waiting for the holder).
#[tokio::test(flavor = "multi_thread")]
async fn a_table_lock_on_the_logical_name_refuses_other_writers() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "write_lock").await;
    let pt = ledger(&db, "p", 7).await;
    exec_ok(&db, &format!("CREATE TABLE q {}", super::tests::DDL)).await;
    exec_ok(&db, &format!("INSERT INTO q (tenant_id, seq, amt, note) VALUES (1, 0, {OPENING}, 'opening')")).await;
    for table in ["p", "q"] {
        let holder = db.transactions.begin();
        exec_ok_in(&db, holder, &format!("LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE")).await;
        for sql in [
            format!("UPDATE {table} SET amt = amt + 1 WHERE tenant_id = 1"),
            format!("UPDATE {table} SET amt = amt + 1"),
            format!("DELETE FROM {table} WHERE tenant_id = 1"),
        ] {
            let e = crate::core::cancel::with_lock_timeout(150, exec_err(&db, &sql)).await;
            assert!(e.contains("lock timeout"), "{sql}: {e}");
        }
        rollback(&db, holder);
        let sql = format!("UPDATE {table} SET amt = amt + 1 WHERE tenant_id = 1");
        assert_eq!(affected(&exec(&db, &sql).await, &sql), 1, "{table}: after the lock ended");
    }
    drop(pt);
    close(db, dir.path());
}
