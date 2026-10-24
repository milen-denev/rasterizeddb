//! `RETURNING` on a partitioned `UPDATE` and `DELETE` (S17,
//! `docs/partitioning.md` §9): the rows come from the statement's one
//! snapshot, a column the statement did not set is reported as stored, and a
//! statement that fails in a later partition returns nothing and changes
//! nothing.
//!
//! Seen through the fan-out's test hooks and the engine's API rather than
//! timing. Bounded: sixteen tenants, a handful of statements.

use std::sync::Arc as StdArc;
use std::sync::atomic::{AtomicBool, Ordering};

use rclite::Arc;

use super::test_hooks;
use super::tests::{OPENING, TENANTS, close, exec, exec_ok, ints, ledger, materialized, open, pid_of, rows};
use crate::core::database::Database;
use crate::core::db_type::DbType;
use crate::core::mvcc::transaction::TxId;
use crate::core::partition::table::PartitionedTable;
use crate::core::row::row::{Row, vec_into_rows};
use crate::core::rql::lexer_s1::recognize_query_purpose;

/// One returned or stored cell: `None` for NULL, else its bytes.
type Cell = Option<Vec<u8>>;

fn cells(row: &Row) -> Vec<Cell> {
    row.columns
        .iter()
        .map(|c| (c.column_type != DbType::NULL).then(|| c.data.into_slice().to_vec()))
        .collect()
}

fn int(cell: &Cell) -> i64 {
    i64::from_le_bytes(cell.as_ref().expect("not NULL").as_slice().try_into().expect("eight bytes"))
}

fn text(cell: &Cell) -> Option<String> {
    cell.as_ref().map(|b| String::from_utf8_lossy(b).into_owned())
}

/// The rows of a status-2 reply; panics on anything else.
fn returned(out: &[u8], sql: &str) -> Vec<Vec<Cell>> {
    assert_eq!(out.first(), Some(&2u8), "{sql}: {}", String::from_utf8_lossy(out));
    vec_into_rows(&out[1..]).unwrap().iter().map(cells).collect()
}

/// Every `(tenant_id, seq, amt)`, sorted.
async fn ledger_rows(db: &Arc<Database>, table: &str) -> Vec<(i64, i64, i64)> {
    let mut out: Vec<(i64, i64, i64)> = rows(db, &format!("SELECT tenant_id, seq, amt FROM {table}"))
        .await
        .iter()
        .map(|r| {
            let c = cells(r);
            (int(&c[0]), int(&c[1]), int(&c[2]))
        })
        .collect();
    out.sort();
    out
}

/// `(tenant_id, seq, amt)` of every returned row, sorted.
fn triples(rows: &[Vec<Cell>]) -> Vec<(i64, i64, i64)> {
    let mut out: Vec<(i64, i64, i64)> = rows.iter().map(|r| (int(&r[0]), int(&r[1]), int(&r[2]))).collect();
    out.sort();
    out
}

async fn exec_in(db: &Arc<Database>, tx: TxId, sql: &str) -> Vec<u8> {
    let purpose = recognize_query_purpose(sql).unwrap_or_else(|| panic!("unrecognised: {sql}"));
    crate::core::rql::executor::execute_in_txn(purpose, db.clone(), tx).await
}

fn rollback(db: &Arc<Database>, tx: TxId) {
    db.abort_transaction(tx);
    let _ = db.transactions.flush();
}

/// A tenant whose partition every fan-out over `pt` visits after the first.
fn late_tenant(pt: &PartitionedTable) -> i64 {
    let first = materialized(pt)[0];
    (1..=TENANTS).find(|t| pid_of(pt, *t) != first).expect("tenants in two partitions")
}

/// A one-shot hook: runs `body` the first time a fan-out over the table has
/// visited its first partition, and nothing on later fan-outs.
fn once(
    body: impl Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send + Sync + 'static,
) -> (test_hooks::Hook, StdArc<AtomicBool>) {
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

/// (a) One snapshot per statement, with `RETURNING`. A fan-out `UPDATE …
/// RETURNING` (and then a `DELETE … RETURNING`) over every tenant visits its
/// first partition; a writer then commits a new matching row in a partition
/// visited later. The statement's snapshot predates that commit, so the new
/// row is neither changed nor returned: the returned rows are exactly the
/// rows the statement changed, each once, with the post-image storage holds
/// (the pre-image, for a DELETE). A snapshot taken per partition (or a
/// re-read of the returned rows) would change and report the new row too.
#[tokio::test(flavor = "multi_thread")]
async fn returning_across_partitions_comes_from_the_statements_one_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "returning_i7").await;
    let pt = ledger(&db, "acct", 7).await;
    let late = late_tenant(&pt);
    let opening: Vec<(i64, i64, i64)> = (1..=TENANTS).map(|t| (t, 0, OPENING)).collect();

    for (round, sql, seq) in [
        (0, "UPDATE acct SET amt = amt + 1 WHERE amt >= 100 RETURNING tenant_id, seq, amt", 1),
        (1, "DELETE FROM acct WHERE amt >= 100 RETURNING tenant_id, seq, amt", 2),
    ] {
        let before = ledger_rows(&db, "acct").await;
        let (hook, fired) = {
            let db = db.clone();
            once(move || {
                let db = db.clone();
                Box::pin(async move {
                    exec_ok(
                        &db,
                        &format!("INSERT INTO acct (tenant_id, seq, amt, note) VALUES ({late}, {seq}, 500, 'late')"),
                    )
                    .await;
                })
            })
        };
        test_hooks::set_after_first(pt.uid(), Some(hook));
        let out = exec(&db, sql).await;
        test_hooks::set_after_first(pt.uid(), None);
        assert!(fired.load(Ordering::SeqCst), "round {round}: the writer never ran between two partition visits");
        let got = triples(&returned(&out, sql));
        let after = ledger_rows(&db, "acct").await;

        let late_row = (late, seq, 500);
        assert!(
            !got.iter().any(|r| r.0 == late && r.1 == seq),
            "round {round}: `{sql}` returned the row committed after its snapshot: {got:?}"
        );
        assert!(after.contains(&late_row), "round {round}: the row committed after the snapshot was changed: {after:?}");
        if round == 0 {
            // Every row the statement saw matched; each was changed once and
            // is reported with what storage now holds.
            let want: Vec<(i64, i64, i64)> = before.iter().map(|&(t, s, a)| (t, s, a + 1)).collect();
            assert_eq!(got, want, "round {round}: returned rows are not the rows changed");
            let mut stored = want.clone();
            stored.push(late_row);
            stored.sort();
            assert_eq!(after, stored, "round {round}: storage");
            assert_eq!(before, opening, "round {round}: setup");
        } else {
            // A DELETE reports each removed row once, as it was; what is left
            // is the row the statement could not see, and round 0's late row
            // (amt 500, matching, but visible: deleted and returned).
            let removed: Vec<(i64, i64, i64)> = before.iter().filter(|r| !after.contains(r)).copied().collect();
            assert_eq!(got, removed, "round {round}: returned rows are not the rows deleted");
            assert_eq!(after, vec![late_row], "round {round}: storage");
            assert_eq!(got.len(), TENANTS as usize + 1, "round {round}: {got:?}");
        }
    }
    drop(pt);
    close(db, dir.path());
}

/// (b) The overlay trap (memory "RETURNING overlay placeholder trap"): an
/// `UPDATE … RETURNING` over columns the statement did not set, echoed and
/// inside expressions, reports what storage holds — never the empty string
/// an evaluation without its column would give. Routed to one partition (the
/// point path), to two, and over every partition; storage is asserted on its
/// own, since RETURNING and storage can disagree.
#[tokio::test(flavor = "multi_thread")]
async fn returning_reports_unset_columns_as_stored() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "returning_overlay").await;
    let pt = ledger(&db, "acct", 7).await;
    let first = materialized(&pt)[0];
    let a = (1..=TENANTS).find(|t| pid_of(&pt, *t) == first).unwrap();
    let b = late_tenant(&pt);
    exec_ok(&db, &format!("UPDATE acct SET note = NULL WHERE tenant_id = {b}")).await;

    let list = "RETURNING tenant_id, note, UPPER(note) AS u, COALESCE(note, 'none') AS c, amt, amt * 2 AS twice";
    let mut bumps: Vec<(i64, i64)> = (1..=TENANTS).map(|t| (t, 0)).collect();
    for (label, clause, tenants) in [
        ("one partition, the point path", format!("tenant_id = {a} AND seq = 0"), vec![a]),
        ("two partitions", format!("tenant_id IN ({a}, {b})"), vec![a, b]),
        ("every partition", "amt > 0".to_string(), (1..=TENANTS).collect::<Vec<_>>()),
    ] {
        let sql = format!("UPDATE acct SET amt = amt + 1 WHERE {clause} {list}");
        for t in &tenants {
            bumps.iter_mut().find(|(x, _)| x == t).unwrap().1 += 1;
        }
        let mut got = returned(&exec(&db, &sql).await, &sql);
        got.sort_by_key(|r| int(&r[0]));
        assert_eq!(got.len(), tenants.len(), "{label}: {sql}");
        for row in &got {
            let t = int(&row[0]);
            let amt = OPENING + bumps.iter().find(|(x, _)| *x == t).unwrap().1;
            let (note, upper, coalesced) = if t == b {
                (None, None, Some("none".to_string()))
            } else {
                (Some("opening".to_string()), Some("OPENING".to_string()), Some("opening".to_string()))
            };
            assert_eq!(text(&row[1]), note, "{label}: tenant {t}'s note");
            assert_eq!(text(&row[2]), upper, "{label}: tenant {t}'s UPPER(note)");
            assert_eq!(text(&row[3]), coalesced, "{label}: tenant {t}'s COALESCE(note, 'none')");
            assert_eq!(int(&row[4]), amt, "{label}: tenant {t}'s amt");
            assert_eq!(int(&row[5]), amt * 2, "{label}: tenant {t}'s amt * 2");
        }
        // Storage, read independently of RETURNING.
        for t in &tenants {
            let stored = rows(&db, &format!("SELECT note, amt FROM acct WHERE tenant_id = {t}")).await;
            let c = cells(&stored[0]);
            let want_note = if *t == b { None } else { Some("opening".to_string()) };
            assert_eq!(text(&c[0]), want_note, "{label}: tenant {t}'s stored note");
            assert_eq!(int(&c[1]), OPENING + bumps.iter().find(|(x, _)| x == t).unwrap().1, "{label}: stored amt");
        }
    }
    drop(pt);
    close(db, dir.path());
}

/// (c) A fan-out `UPDATE … RETURNING` (and `DELETE … RETURNING`) that fails
/// before its third partition, the first two written, returns no rows and
/// changes nothing: in autocommit and in a block. Then, unhindered, the same
/// statement returns every row it changed.
#[tokio::test(flavor = "multi_thread")]
async fn a_returning_write_that_fails_midway_returns_nothing_and_changes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "returning_atomic").await;
    let pt = ledger(&db, "acct", 7).await;
    let uid = pt.uid();
    assert!(materialized(&pt).len() >= 3, "too few partitions to fail midway");
    let original = ledger_rows(&db, "acct").await;

    test_hooks::fail_before(uid, Some(2));
    for sql in [
        "UPDATE acct SET amt = amt + 1 RETURNING tenant_id, seq, amt",
        "UPDATE acct SET amt = amt * 3 WHERE amt > 0 RETURNING amt - 1 AS a, note",
        "DELETE FROM acct WHERE amt > 0 RETURNING *",
    ] {
        let out = exec(&db, sql).await;
        assert_eq!(out.first(), Some(&3u8), "`{sql}` failed midway and answered status {:?}", out.first());
        let e = String::from_utf8_lossy(&out[1..]).into_owned();
        assert!(e.contains("injected failure"), "{sql}: {e}");
        assert_eq!(ledger_rows(&db, "acct").await, original, "{sql} changed a partition before failing");
    }
    // In a block: the statement fails with no rows, the block rolls back.
    let tx = db.transactions.begin();
    let out = exec_in(&db, tx, "UPDATE acct SET note = 'in block' WHERE tenant_id = 1 RETURNING note").await;
    assert_eq!(returned(&out, "in block").len(), 1);
    let out = exec_in(&db, tx, "UPDATE acct SET amt = amt - 1 RETURNING tenant_id, seq, amt").await;
    assert_eq!(out.first(), Some(&3u8), "the injected failure did not fail the statement in a block");
    rollback(&db, tx);
    assert_eq!(ledger_rows(&db, "acct").await, original);
    test_hooks::fail_before(uid, None);

    let sql = "UPDATE acct SET amt = amt + 1 RETURNING tenant_id, seq, amt";
    let got = triples(&returned(&exec(&db, sql).await, sql));
    let want: Vec<(i64, i64, i64)> = original.iter().map(|&(t, s, a)| (t, s, a + 1)).collect();
    assert_eq!(got, want);
    assert_eq!(ledger_rows(&db, "acct").await, want);
    drop(pt);
    close(db, dir.path());
}

/// I17 with `RETURNING`: another transaction holds an uncommitted update of
/// tenant `b`'s row. A fan-out `UPDATE … RETURNING` applies tenant `a`'s row
/// in the first partition, then conflicts on `b`. It fails at once, with no
/// rows and the retry marker gone, and `a` is incremented at most once
/// (exactly zero times, the statement having failed).
#[tokio::test(flavor = "multi_thread")]
async fn a_returning_conflict_after_a_partition_applied_rows_is_not_replayed() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "returning_i17").await;
    let pt = ledger(&db, "acct", 7).await;
    let first = materialized(&pt)[0];
    let a = (1..=TENANTS).find(|t| pid_of(&pt, *t) == first).unwrap();
    let b = late_tenant(&pt);
    let sql = format!("UPDATE acct SET amt = amt + 1 WHERE tenant_id IN ({a}, {b}) RETURNING tenant_id, amt");
    let amt = |t: i64| format!("SELECT amt FROM acct WHERE tenant_id = {t}");

    let start = ints(&db, &amt(a)).await[0];
    let blocker = db.transactions.begin();
    let held = exec_in(&db, blocker, &format!("UPDATE acct SET note = 'held' WHERE tenant_id = {b}")).await;
    assert_ne!(held.first(), Some(&3u8), "{}", String::from_utf8_lossy(&held));
    let ender = {
        let db = db.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(40)).await;
            rollback(&db, blocker);
        })
    };
    let tx = db.transactions.begin();
    let out = exec_in(&db, tx, &sql).await;
    rollback(&db, tx);
    ender.await.unwrap();
    assert_eq!(out.first(), Some(&3u8), "the conflict was replayed into a reply: {:?}", out.first());
    let text = String::from_utf8_lossy(&out[1..]).into_owned();
    assert!(text.contains("could not serialize"), "{text}");
    assert!(!text.ends_with(" [retryable]"), "the marker reached the client: {text}");
    assert_eq!(ints(&db, &amt(a)).await, vec![start], "a failed statement left a change");

    // Nothing held: both rows, once each.
    let mut got: Vec<(i64, i64)> = returned(&exec(&db, &sql).await, &sql).iter().map(|r| (int(&r[0]), int(&r[1]))).collect();
    got.sort();
    let mut want = vec![(a, start + 1), (b, OPENING + 1)];
    want.sort();
    assert_eq!(got, want);
    drop(pt);
    close(db, dir.path());
}
