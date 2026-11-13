//! Replication through a power cut.
//!
//! Two promises, each judged on every image of a cut at every log sync, under
//! every seed kind:
//!
//! - **The origin loses nothing and invents nothing.** A transaction's
//!   replication record is written ahead of its commit and rides the commit's
//!   one sync. After the cut, the rows a client can read and the records a
//!   peer would be sent are the same set: no committed row without its
//!   record (a peer would never hear of it), and no record for a row that
//!   did not commit (a peer would hold a write this node disowned).
//! - **A peer applies each transaction once.** How far a stream has been
//!   applied is written ahead of the applying commit, as "this far if that
//!   transaction committed". After the cut the position and the rows agree,
//!   so sending the stream again from that position neither skips a
//!   transaction nor applies one twice. The table has no unique key: a second
//!   apply would be a second row.

use super::*;
use parking_lot::Mutex;
use std::sync::Arc as StdArc;

use crate::replication::TxnRecord;
use crate::replication::config::ReplicationConfig;

async fn open_node(dir: &Path, node_id: u64) -> Result<Arc<Database>, String> {
    open_node_keeping(dir, node_id, ReplicationConfig::default().log_retention_bytes).await
}

/// [`open_node`] with a retention budget, which also sets how soon the log
/// moves on to its next file.
async fn open_node_keeping(dir: &Path, node_id: u64, retention: u64) -> Result<Arc<Database>, String> {
    let db = open(dir, "repl_cut").await?;
    db.replication
        .set_config(ReplicationConfig {
            node_id,
            log_retention_bytes: retention,
            ..Default::default()
        })
        .map_err(|e| e.to_string())?;
    db.open_replication_storage().map_err(|e| format!("replication storage: {e}"))?;
    db.replication.set_active(true);
    Ok(db)
}

async fn ids(db: &Arc<Database>) -> Result<Vec<i64>, String> {
    let rows = decode(&exec(db, "SELECT id FROM t WHERE 1 = 1").await?)?;
    let mut ids: Vec<i64> = rows
        .iter()
        .map(|row| i64::from_le_bytes(row.columns[0].data.into_slice()[..8].try_into().unwrap()))
        .collect();
    ids.sort_unstable();
    Ok(ids)
}

/// The id an `INSERT INTO t (id) VALUES (n)` writes.
fn inserted_id(sql: &str) -> Option<i64> {
    let rest = sql.strip_prefix("INSERT INTO t (id) VALUES (")?;
    rest.trim_end_matches(')').parse().ok()
}

fn arm_log_syncs(
    rec: &StdArc<Recorder>,
    scope: &Path,
    sink: &StdArc<Mutex<Vec<Result<CrashPoint, String>>>>,
    n: u64,
) -> Vec<failpoint::Guard> {
    (1..=n)
        .map(|nth| failpoint::arm("sync:wal.log", scope, nth, rec.cut_action(format!("sync:wal.log #{nth}"), sink.clone())))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_origin_cut_anywhere_keeps_its_rows_and_its_records_the_same_set() {
    origin_cuts(8, ReplicationConfig::default().log_retention_bytes, false).await;
}

/// The same with a log that moves on to a new file every few records, so
/// cuts fall before, inside and after the start of a file. Every file is
/// appended through the write-ahead log from its first byte, and a cut must
/// leave each of them whole up to its last committed record.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_origin_cut_while_its_log_moves_to_new_files_keeps_the_same_set() {
    origin_cuts(40, 8 * 1024, true).await;
}

async fn origin_cuts(writes: i64, retention: u64, rotates: bool) {
    let seeds = env_count("RDB_POWER_LOSS_SEEDS", 5, 1024);
    #[allow(non_snake_case)]
    let WRITES = writes;
    let base = tempfile::tempdir().unwrap();
    let rec_dir = base.path().join("rec");
    let dir = rec_dir.join("db");
    {
        let db = open_node_keeping(&dir, 1, retention).await.unwrap();
        exec(&db, "CREATE TABLE t (id BIGINT)").await.unwrap();
        db.sync_durable().unwrap();
        db.replication.flush();
        close(&dir, db);
    }
    let db = open_node_keeping(&dir, 1, retention).await.unwrap();
    assert_eq!(db.replication.local_seq(), 1, "the CREATE is the stream's first record");
    assert!(db.replication.log().unwrap().is_logged(), "the log does not ride the write-ahead log");

    let rec = Recorder::start(&rec_dir);
    let sink: StdArc<Mutex<Vec<Result<CrashPoint, String>>>> = StdArc::new(Mutex::new(Vec::new()));
    let guards = arm_log_syncs(&rec, &rec_dir, &sink, WRITES as u64 + 2);
    for id in 1..=WRITES {
        exec(&db, &format!("INSERT INTO t (id) VALUES ({id})")).await.unwrap();
        // A cut between two statements, where nothing is in flight.
        if id % 3 == 0 {
            sink.lock().push(rec.cut_quiescent(format!("after insert {id}")));
        }
    }
    let fired = guards.iter().filter(|g| g.fired() > 0).count();
    drop(guards);
    sink.lock().push(rec.cut_quiescent("after the last insert"));
    let log_files = std::fs::read_dir(&dir)
        .unwrap()
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().starts_with("db_replication.log"))
        .count();
    assert_eq!(log_files > 1, rotates, "{log_files} log file(s)");
    close(&dir, db);
    drop(rec);
    assert!(fired >= WRITES as usize / 2, "only {fired} cuts fell inside a log sync");

    let points: Vec<CrashPoint> = sink.lock().drain(..).filter_map(|p| p.map_err(|why| eprintln!("{why}")).ok()).collect();
    assert!(points.len() >= 4, "too many cuts were disturbed: {}", points.len());

    let img_dir = base.path().join("image");
    let img = img_dir.join("db");
    let mut problems = Vec::new();
    let (mut partial, mut voided, mut whole) = (0usize, 0usize, 0usize);
    for point in &points {
        for seed in 0..seeds {
            point.write_image(seed, &img_dir);
            let label = format!("{} seed {seed}", point.label);
            let db = match open_node_keeping(&img, 1, retention).await {
                Ok(db) => db,
                Err(e) => {
                    problems.push(format!("{label}: does not open: {e}"));
                    continue;
                }
            };
            let rows = ids(&db).await.unwrap();
            let log = db.replication.log().unwrap();
            let (records, more) = log.read_after(1, 0, 1000, 1 << 30).unwrap();
            assert!(!more);
            // No holes: a peer could be sent all of it in order.
            for (at, record) in records.iter().enumerate() {
                if record.seq != at as u64 + 1 {
                    problems.push(format!("{label}: the stream has a hole at {} (found {})", at + 1, record.seq));
                }
            }
            let mut recorded: Vec<i64> = records
                .iter()
                .filter(|r| !r.is_void())
                .flat_map(|r| r.statements.iter().filter_map(|s| inserted_id(s)))
                .collect();
            recorded.sort_unstable();
            if recorded != rows {
                problems.push(format!("{label}: rows {rows:?}, but the records a peer would get insert {recorded:?}"));
            }
            if db.replication.released_through() != db.replication.local_seq() {
                problems.push(format!(
                    "{label}: released {} of {} after the open",
                    db.replication.released_through(),
                    db.replication.local_seq()
                ));
            }
            voided += records.iter().filter(|r| r.is_void()).count();
            if rows.len() as i64 == WRITES {
                whole += 1;
            } else {
                partial += 1;
            }
            // The stream goes on from there without reusing a number.
            let next = db.replication.local_seq() + 1;
            exec(&db, "INSERT INTO t (id) VALUES (1000)").await.unwrap();
            let (after, _) = log.read_after(1, next - 1, 10, 1 << 20).unwrap();
            if after.len() != 1 || after[0].seq != next || after[0].is_void() {
                problems.push(format!("{label}: the next write did not take sequence {next}"));
            }
            close(&img, db);
            let _ = std::fs::remove_dir_all(&img_dir);
        }
    }
    assert!(problems.is_empty(), "{} problem(s):\n{}", problems.len(), problems.join("\n"));
    assert!(partial > 0 && whole > 0, "the cuts tell nothing apart: {partial} partial, {whole} whole");
    eprintln!("origin cuts: {partial} partial, {whole} whole, {voided} voided record(s)");
}

fn stream(seqs: std::ops::RangeInclusive<u64>) -> Vec<TxnRecord> {
    seqs.map(|seq| TxnRecord {
        origin: 1,
        seq,
        commit_hlc: seq * 1000,
        flags: 0,
        statements: vec![format!("INSERT INTO t (id) VALUES ({seq})")],
        tables: vec!["t".to_string()],
    })
    .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_peer_cut_anywhere_has_applied_each_transaction_once() {
    let seeds = env_count("RDB_POWER_LOSS_SEEDS", 5, 1024);
    const TOTAL: u64 = 12;
    let base = tempfile::tempdir().unwrap();
    let rec_dir = base.path().join("rec");
    let dir = rec_dir.join("db");
    {
        let db = open_node(&dir, 2).await.unwrap();
        exec(&db, "CREATE TABLE t (id BIGINT)").await.unwrap();
        db.sync_durable().unwrap();
        db.replication.flush();
        close(&dir, db);
    }
    let db = open_node(&dir, 2).await.unwrap();

    let rec = Recorder::start(&rec_dir);
    let sink: StdArc<Mutex<Vec<Result<CrashPoint, String>>>> = StdArc::new(Mutex::new(Vec::new()));
    let guards = arm_log_syncs(&rec, &rec_dir, &sink, 8);
    // Batches of several records and of one: one sync a batch either way.
    for batch in [1..=4u64, 5..=5, 6..=9, 10..=TOTAL] {
        let last = *batch.end();
        let outcomes = crate::replication::server::apply_batch(&db, &db.replication, stream(batch), 1).await;
        assert!(
            outcomes.iter().all(|o| o.status == crate::replication::protocol::ApplyStatus::Applied),
            "{outcomes:?}"
        );
        sink.lock().push(rec.cut_quiescent(format!("after the batch ending at {last}")));
    }
    let fired = guards.iter().filter(|g| g.fired() > 0).count();
    drop(guards);
    assert_eq!(ids(&db).await.unwrap(), (1..=TOTAL as i64).collect::<Vec<_>>());
    close(&dir, db);
    drop(rec);
    assert!(fired >= 3, "only {fired} cuts fell inside a log sync");

    let points: Vec<CrashPoint> = sink.lock().drain(..).filter_map(|p| p.map_err(|why| eprintln!("{why}")).ok()).collect();
    assert!(points.len() >= 4, "too many cuts were disturbed: {}", points.len());

    let img_dir = base.path().join("image");
    let img = img_dir.join("db");
    let mut problems = Vec::new();
    let mut positions = std::collections::BTreeSet::new();
    for point in &points {
        for seed in 0..seeds {
            point.write_image(seed, &img_dir);
            let label = format!("{} seed {seed}", point.label);
            let db = match open_node(&img, 2).await {
                Ok(db) => db,
                Err(e) => {
                    problems.push(format!("{label}: does not open: {e}"));
                    continue;
                }
            };
            let position = db.replication.applied().high_water(1);
            positions.insert(position);
            let rows = ids(&db).await.unwrap();
            let expected: Vec<i64> = (1..=position as i64).collect();
            if rows != expected {
                problems.push(format!("{label}: the position is {position}, the rows are {rows:?}"));
            }
            // What the origin does next: the whole stream again. Everything
            // up to the position is already held; the rest is applied.
            let outcomes = crate::replication::server::apply_batch(&db, &db.replication, stream(1..=TOTAL), 1).await;
            for outcome in &outcomes {
                let want = if outcome.seq <= position {
                    crate::replication::protocol::ApplyStatus::Duplicate
                } else {
                    crate::replication::protocol::ApplyStatus::Applied
                };
                if outcome.status != want {
                    problems.push(format!("{label}: {}:{} came back {}", outcome.origin, outcome.seq, outcome.status));
                }
            }
            let rows = ids(&db).await.unwrap();
            if rows != (1..=TOTAL as i64).collect::<Vec<_>>() {
                problems.push(format!("{label}: after the stream was sent again the rows are {rows:?}"));
            }
            close(&img, db);
            let _ = std::fs::remove_dir_all(&img_dir);
        }
    }
    assert!(problems.is_empty(), "{} problem(s):\n{}", problems.len(), problems.join("\n"));
    assert!(positions.len() >= 3, "the cuts all landed on the same position(s): {positions:?}");
    eprintln!("peer cuts: positions seen {positions:?}");
}
