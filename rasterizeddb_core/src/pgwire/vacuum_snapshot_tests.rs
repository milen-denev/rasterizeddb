//! Vacuum against open snapshots, end to end through sessions.
//!
//! A snapshot keeps every transaction that was running when it was taken
//! invisible after it commits, so a version such a transaction deleted is
//! still visible to the snapshot after the deleter retires. Vacuum's horizon
//! is bounded by the oldest live snapshot as well as the oldest running
//! transaction (`VersionHorizon::sample`, `SnapshotRegistry`); bounded by the
//! transaction alone it freed those versions, and the space went to the next
//! insert while a reader could still read it (`vacuum-frees-visible-versions`).
//! The unit tests next to the horizon cover the rule; these cover what a
//! client sees: a `REPEATABLE READ` reader across `VACUUM` and `VACUUM FULL`,
//! and transfers, vacuum and readers at once against a serial oracle.

use std::time::Duration;

use tokio::io::AsyncReadExt;

use super::owner_tests::{Session, ok, send, session};
use super::*;

async fn disk_db(dir: &std::path::Path, name: &str) -> Arc<Database> {
    crate::replication::test_support::init_globals();
    Arc::new(Database::new_named(&dir.to_string_lossy(), false, name).await)
}

/// The first row of a statement's answer, its cells joined by `|`.
async fn first_row(db: &Arc<Database>, s: &mut Session, sql: &str) -> String {
    Box::pin(execute_recorded(db, &mut s.state, &mut s.server, sql, &[], false, None))
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let mut first: Option<String> = None;
    loop {
        let kind = s.client.read_u8().await.unwrap();
        let len = s.client.read_i32().await.unwrap() as usize;
        let mut body = vec![0u8; len - 4];
        s.client.read_exact(&mut body).await.unwrap();
        match kind {
            b'D' if first.is_none() => {
                let n = i16::from_be_bytes([body[0], body[1]]) as usize;
                let mut at = 2;
                let mut cells = Vec::with_capacity(n);
                for _ in 0..n {
                    let l = i32::from_be_bytes(body[at..at + 4].try_into().unwrap());
                    at += 4;
                    if l < 0 {
                        cells.push("NULL".to_string());
                    } else {
                        cells.push(String::from_utf8_lossy(&body[at..at + l as usize]).into_owned());
                        at += l as usize;
                    }
                }
                first = Some(cells.join("|"));
            }
            b'C' => return first.unwrap_or_default(),
            b'E' => panic!("{sql}: {}", String::from_utf8_lossy(&body)),
            _ => {}
        }
    }
}

fn values(range: std::ops::RangeInclusive<i64>, tag: &str) -> String {
    range.map(|i| format!("({i}, '{tag}-{i:06}')")).collect::<Vec<_>>().join(", ")
}

/// The order the bug needs: a writer's transaction deletes half the rows and
/// updates a quarter; a `REPEATABLE READ` reader takes its snapshot while
/// that transaction is still open (so the writer's id is below the reader's,
/// and the snapshot counts it as running); the writer commits and retires,
/// which moves the oldest running transaction past it to the reader. Then
/// `VACUUM` (or `VACUUM FULL`) runs, and new rows of the same width go in —
/// what reuses space vacuum freed. The reader reads exactly what it read
/// first, every value of it; after it commits, the vacuum has run and the
/// table is what the writes made it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_repeatable_read_keeps_its_rows_across_vacuum() {
    for full in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let db = disk_db(dir.path(), "vacuum_snapshot").await;
        let mut writer = session(&db);
        let mut reader = session(&db);
        ok(&db, &mut writer, "CREATE TABLE vs (id INT PRIMARY KEY, v TEXT)").await;
        ok(&db, &mut writer, &format!("INSERT INTO vs (id, v) VALUES {}", values(1..=200, "old"))).await;

        ok(&db, &mut writer, "BEGIN").await;
        ok(&db, &mut writer, "DELETE FROM vs WHERE id <= 100").await;
        ok(&db, &mut writer, "UPDATE vs SET v = 'upd' WHERE id > 150").await;

        ok(&db, &mut reader, "BEGIN ISOLATION LEVEL REPEATABLE READ").await;
        let seen = "SELECT count(*), sum(id), count(v), min(v), max(v) FROM vs WHERE v LIKE 'old-%'";
        let before = first_row(&db, &mut reader, seen).await;
        assert_eq!(before, "200|20100|200|old-000001|old-000200");

        // The writer retires: the oldest running transaction is now the
        // reader, whose snapshot still sees every row the writer changed.
        ok(&db, &mut writer, "COMMIT").await;
        // In a task of its own: `VACUUM FULL` may wait for the reader.
        let statement = if full { "VACUUM FULL vs" } else { "VACUUM vs" };
        let vacuum = {
            let db = db.clone();
            tokio::spawn(async move {
                let r = send(&db, &mut writer, statement).await;
                (writer, r)
            })
        };
        tokio::time::sleep(Duration::from_millis(400)).await;
        // New rows of the same width, through a third session, into whatever
        // space the vacuum made.
        let mut filler = session(&db);
        let fill = {
            let db = db.clone();
            tokio::spawn(async move {
                send(&db, &mut filler, &format!("INSERT INTO vs (id, v) VALUES {}", values(1001..=1200, "new"))).await
            })
        };
        tokio::time::sleep(Duration::from_millis(400)).await;

        // The reader's snapshot: every deleted and updated row still as it
        // was, none of the new ones.
        assert_eq!(first_row(&db, &mut reader, seen).await, before, "full {full}: the snapshot lost rows");
        assert_eq!(
            first_row(&db, &mut reader, "SELECT count(*) FROM vs").await,
            "200",
            "full {full}: the snapshot sees rows written after it"
        );
        ok(&db, &mut reader, "COMMIT").await;

        let (mut writer, vacuumed) = tokio::time::timeout(Duration::from_secs(30), vacuum)
            .await
            .expect("the vacuum finishes once the reader is gone")
            .unwrap();
        vacuumed.unwrap_or_else(|e| panic!("full {full}: {statement} failed: {e}"));
        tokio::time::timeout(Duration::from_secs(30), fill)
            .await
            .expect("the insert finishes")
            .unwrap()
            .expect("the insert succeeds");
        assert_eq!(
            first_row(&db, &mut writer, "SELECT count(*), sum(id) FROM vs").await,
            format!("{}|{}", 100 + 200, (101..=200).sum::<i64>() + (1001..=1200).sum::<i64>()),
        );
        assert_eq!(first_row(&db, &mut writer, "SELECT count(*) FROM vs WHERE v = 'upd'").await, "50");
        drop(writer);
        drop(reader);
        drop(db);
        crate::core::wal::remove_for(&dir.path().to_string_lossy());
    }
}

/// Transfers between accounts, vacuum in a loop, and `REPEATABLE READ`
/// readers that read the total several times each, all at once. Every
/// reader's every read finds the total the transfers keep, and the accounts
/// end exactly as the committed transfers, replayed one after another, leave
/// them.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn transfers_vacuum_and_readers_at_once_agree_with_a_serial_replay() {
    const ACCOUNTS: i64 = 40;
    const START: i64 = 100;
    const WRITERS: usize = 3;
    const READERS: usize = 2;
    let run_for = Duration::from_millis(2500);

    let dir = tempfile::tempdir().unwrap();
    let db = disk_db(dir.path(), "vacuum_stress").await;
    let mut setup = session(&db);
    ok(&db, &mut setup, "CREATE TABLE acct (id INT PRIMARY KEY, bal BIGINT NOT NULL)").await;
    let rows: Vec<String> = (1..=ACCOUNTS).map(|id| format!("({id}, {START})")).collect();
    ok(&db, &mut setup, &format!("INSERT INTO acct (id, bal) VALUES {}", rows.join(", "))).await;
    let total = (ACCOUNTS * START).to_string();
    let deadline = tokio::time::Instant::now() + run_for;

    let mut writers = Vec::new();
    for w in 0..WRITERS {
        let db = db.clone();
        writers.push(tokio::spawn(async move {
            let mut s = session(&db);
            let mut seed = 0x9E37_79B9_7F4A_7C15u64 ^ (w as u64 + 1);
            let mut next = |bound: u64| {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                seed % bound
            };
            // The transfers that committed, in this writer's order.
            let mut committed: Vec<(i64, i64, i64)> = Vec::new();
            while tokio::time::Instant::now() < deadline {
                let from = next(ACCOUNTS as u64) as i64 + 1;
                let to = next(ACCOUNTS as u64) as i64 + 1;
                let amount = next(5) as i64 + 1;
                if from == to {
                    continue;
                }
                let steps = [
                    "BEGIN".to_string(),
                    format!("UPDATE acct SET bal = bal - {amount} WHERE id = {from}"),
                    format!("UPDATE acct SET bal = bal + {amount} WHERE id = {to}"),
                ];
                let mut failed = false;
                for step in &steps {
                    if send(&db, &mut s, step).await.is_err() {
                        failed = true;
                        break;
                    }
                }
                if failed {
                    let _ = send(&db, &mut s, "ROLLBACK").await;
                    continue;
                }
                if send(&db, &mut s, "COMMIT").await.is_ok() {
                    committed.push((from, to, amount));
                } else {
                    let _ = send(&db, &mut s, "ROLLBACK").await;
                }
            }
            committed
        }));
    }

    let vacuum = {
        let db = db.clone();
        tokio::spawn(async move {
            let mut s = session(&db);
            let mut runs = 0u32;
            while tokio::time::Instant::now() < deadline {
                send(&db, &mut s, "VACUUM acct").await.expect("VACUUM");
                runs += 1;
                tokio::time::sleep(Duration::from_millis(15)).await;
            }
            runs
        })
    };

    let mut readers = Vec::new();
    for _ in 0..READERS {
        let db = db.clone();
        let total = total.clone();
        readers.push(tokio::spawn(async move {
            let mut s = session(&db);
            let mut reads = 0u32;
            while tokio::time::Instant::now() < deadline {
                ok(&db, &mut s, "BEGIN ISOLATION LEVEL REPEATABLE READ").await;
                let first = first_row(&db, &mut s, "SELECT sum(bal), count(*) FROM acct").await;
                assert_eq!(first, format!("{total}|{ACCOUNTS}"), "a snapshot's total");
                for _ in 0..3 {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    let again = first_row(&db, &mut s, "SELECT sum(bal), count(*) FROM acct").await;
                    assert_eq!(again, first, "a repeatable read changed under vacuum");
                    reads += 1;
                }
                ok(&db, &mut s, "COMMIT").await;
            }
            reads
        }));
    }

    let mut model: Vec<i64> = vec![START; ACCOUNTS as usize + 1];
    let mut transfers = 0usize;
    for writer in writers {
        for (from, to, amount) in writer.await.unwrap() {
            model[from as usize] -= amount;
            model[to as usize] += amount;
            transfers += 1;
        }
    }
    let vacuums = vacuum.await.unwrap();
    let mut reads = 0;
    for reader in readers {
        reads += reader.await.unwrap();
    }
    assert!(transfers > 20 && vacuums > 5 && reads > 5, "too little ran: {transfers} transfers, {vacuums} vacuums, {reads} reads");

    for id in 1..=ACCOUNTS {
        let bal = first_row(&db, &mut setup, &format!("SELECT bal FROM acct WHERE id = {id}")).await;
        assert_eq!(bal, model[id as usize].to_string(), "account {id} against the serial replay");
    }
    assert_eq!(first_row(&db, &mut setup, "SELECT sum(bal), count(*) FROM acct").await, format!("{total}|{ACCOUNTS}"));
    drop(setup);
    drop(db);
    crate::core::wal::remove_for(&dir.path().to_string_lossy());
}
