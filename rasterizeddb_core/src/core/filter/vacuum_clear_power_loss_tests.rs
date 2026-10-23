//! Can vacuum's filter clear reach the disk before the outcome that justifies
//! it?
//!
//! Vacuum clears a version's fingerprints once it decides no snapshot can see
//! the version. That decision reads transaction outcomes from memory. For a
//! table on a second storage location, the filter sidecar's checkpoint is
//! logged and synced in *that* location's log, while the deleter's outcome
//! record waits in the root log for its sync. A power cut in between undoes
//! the delete (the row is visible again) but keeps the sidecar, which trusts
//! its cleared slot: the row is pruned from every filtered query for good.
//!
//! No process is killed (a killed process leaves the page cache, which is not
//! a power cut). Crash images are built from the live directories, as in
//! `database::drop_recreate_replay_tests` (ported from feat/partitioning's
//! `cross_dir_commit_tests`), extended to two directories:
//! - each log cut at the longest length any `fdatasync` of it was issued at,
//!   as `sync_audit` recorded it, plus, under a seed, some of the unsynced
//!   tail, since writeback can persist any of it;
//! - seed 0: every data file as it is now (the page cache survived whole);
//! - other seeds: each 4 KiB page of a data file that existed, fsynced, at the
//!   baseline is kept either as it is now or as it was then, by the seed.
//!   Files created since are taken whole.
//!
//! Every image's filtered query is compared with a whole-table scan of the
//! same image (no predicate, so no filter): the twin.
//!
//! The process-wide log writer syncs every log on a timer and can land inside
//! the scenario, making the deleter's outcome durable before the checkpoint
//! and hiding the window. A sync issued from any thread the scenario did not
//! drive is detected and the scenario rerun.

use std::collections::HashMap;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::thread::ThreadId;

use rclite::Arc;

use crate::core::database::Database;
use crate::core::mvcc::TxId;
use crate::core::storage::LocationRole;
use crate::core::sync_audit::{self, SyncEvent};
use crate::core::wal::DEFAULT_WAL_NAME;

const TABLE: &str = "vc_disk";
/// In the database's own directory, beside the outcome records.
const ROOT_TABLE: &str = "vc_root";
/// Also in the database's own directory, with labels: the half of a
/// two-directory transaction that lands beside the outcome records.
const ROOT_MARK: &str = "vc_mark";
/// Over the filter's default `min_table_rows` (4096): the filter a reopened
/// table loads from its checkpoint uses the default config.
const ROWS: i32 = 4400;
/// The row the transaction under test deletes.
const VICTIM: i32 = 10;
const SEEDS: u64 = 6;
const PAGE: usize = 4096;
const MAX_ATTEMPTS: usize = 6;
/// Ceiling on what one scenario may put on disk, directories and images
/// together. Checked, not assumed: the images land in the shared tmpfs.
const MAX_SCENARIO_BYTES: u64 = 96 * 1024 * 1024;

fn path_str(p: &Path) -> String {
    p.to_string_lossy().into_owned()
}

fn label(id: i32) -> String {
    format!("L{id:05}")
}

async fn open(root: &Path) -> Arc<Database> {
    let _ = crate::core::processor::concurrent_processor::ENABLE_CACHE.set(false);
    Arc::new(Database::new_named(&path_str(root), false, "vacuumclear").await)
}

/// An autocommit statement, durable on return whatever the server default.
async fn run(db: &Arc<Database>, sql: &str) -> Vec<u8> {
    let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(sql)
        .unwrap_or_else(|| panic!("could not parse {sql}"));
    let out = crate::core::rql::executor::execute_with_sync(purpose, db.clone(), true).await;
    assert!(
        !out.is_empty() && out[0] != 3,
        "{sql} failed: {}",
        String::from_utf8_lossy(&out)
    );
    out
}

/// One statement inside `tx`, not committed, as an explicit block runs it.
async fn write_in(db: &Arc<Database>, tx: TxId, sql: &str) {
    let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(sql)
        .unwrap_or_else(|| panic!("could not parse {sql}"));
    let out = Box::pin(crate::core::rql::executor::execute_with_context(
        purpose,
        db.clone(),
        0,
        tx,
    ))
    .await;
    assert!(
        !out.is_empty() && out[0] != 3,
        "{sql} failed: {}",
        String::from_utf8_lossy(&out)
    );
}

/// Every `L#####` label in a result, sorted, duplicates kept.
fn labels_in(out: &[u8]) -> Vec<String> {
    let mut found = Vec::new();
    let mut i = 0;
    while i + 6 <= out.len() {
        if out[i] == b'L'
            && out[i + 1..i + 6].iter().all(u8::is_ascii_digit)
            && out.get(i + 6).is_none_or(|b| !b.is_ascii_digit())
        {
            found.push(String::from_utf8_lossy(&out[i..i + 6]).into_owned());
            i += 6;
        } else {
            i += 1;
        }
    }
    found.sort();
    found
}

fn regular_files(dir: &Path) -> Vec<PathBuf> {
    let mut out: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("cannot list {}: {e}", dir.display()))
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().map(|t| t.is_file()).unwrap_or(false))
        .map(|e| e.path())
        .collect();
    out.sort();
    out
}

fn dir_bytes(dir: &Path) -> u64 {
    if !dir.exists() {
        return 0;
    }
    regular_files(dir)
        .iter()
        .map(|p| std::fs::metadata(p).map(|m| m.len()).unwrap_or(0))
        .sum()
}

/// xorshift64*, so a failing seed reproduces without a dependency.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Rng {
        Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn below(&mut self, n: u64) -> u64 {
        if n == 0 { 0 } else { self.next() % n }
    }
}

type Baseline = HashMap<PathBuf, (u64, Vec<u8>)>;

/// Every file of `dir`, fsynced, with its inode and bytes.
fn durable_baseline(dir: &Path) -> Baseline {
    let mut out = HashMap::new();
    for path in regular_files(dir) {
        let f = std::fs::File::options().read(true).write(true).open(&path).unwrap();
        f.sync_all().unwrap();
        let ino = f.metadata().unwrap().ino();
        out.insert(path.clone(), (ino, std::fs::read(&path).unwrap()));
    }
    if let Ok(d) = std::fs::File::open(dir) {
        let _ = d.sync_all();
    }
    out
}

/// Each page as it is now or as it was at the baseline, for a file the
/// baseline saw; a file created since is taken whole.
fn mixed_pages(path: &Path, current: &[u8], baseline: &Baseline, rng: &mut Rng) -> Vec<u8> {
    let ino = std::fs::metadata(path).unwrap().ino();
    let Some((base_ino, base)) = baseline.get(path) else {
        return current.to_vec();
    };
    if *base_ino != ino {
        return current.to_vec();
    }
    let mut out = current.to_vec();
    for (i, page) in out.chunks_mut(PAGE).enumerate() {
        if rng.below(2) == 0 {
            continue;
        }
        let start = i * PAGE;
        for (j, b) in page.iter_mut().enumerate() {
            *b = base.get(start + j).copied().unwrap_or(0);
        }
    }
    out
}

fn synced_log_len(events: &[SyncEvent], wal_path: &Path, current: usize) -> usize {
    events
        .iter()
        .filter(|e| e.tag == "wal.log" && e.path == wal_path)
        .map(|e| e.len)
        .max()
        .unwrap_or(0)
        .min(current as u64) as usize
}

/// Writes `src`'s image for `seed` into `img`. With `data_whole`, every data
/// file is taken as it is now whatever the seed (writeback persisted all of
/// it), and only the log tails vary: for a directory whose data files a
/// checkpoint synced after the baseline, so a page mixed back from the
/// baseline there describes a state no power cut can leave.
fn image_dir(
    src: &Path,
    img: &Path,
    baseline: &Baseline,
    events: &[SyncEvent],
    seed: u64,
    rng: &mut Rng,
    data_whole: bool,
) {
    std::fs::create_dir_all(img).unwrap();
    let wal_path = src.join(DEFAULT_WAL_NAME);
    let base_log = baseline.get(&wal_path).map(|(_, b)| b.len()).unwrap_or(0);
    for path in regular_files(src) {
        let name = path.file_name().unwrap();
        let now = std::fs::read(&path).unwrap();
        let bytes = if path == wal_path {
            let synced = synced_log_len(events, &wal_path, now.len()).max(base_log.min(now.len()));
            let cut = if seed == 0 {
                synced
            } else {
                synced + rng.below((now.len() - synced) as u64 + 1) as usize
            };
now[..cut].to_vec()
        } else if seed == 0 || data_whole {
            now
        } else {
            mixed_pages(&path, &now, baseline, rng)
        };
        std::fs::write(img.join(name), bytes).unwrap();
    }
}

/// A database with `TABLE` on a second location, filled and checkpointed, at
/// a fsynced baseline, with both directories watched for syncs.
struct Fixture {
    base: tempfile::TempDir,
    root: PathBuf,
    disk: PathBuf,
    db: Arc<Database>,
    root_base: Baseline,
    disk_base: Baseline,
    root_watch: sync_audit::Watch,
    disk_watch: sync_audit::Watch,
}

impl Fixture {
    async fn new() -> Fixture {
        let base = tempfile::tempdir().unwrap();
        let root = base.path().join("root");
        let disk = base.path().join("disk");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::create_dir_all(&disk).unwrap();

        let db = open(&root).await;
        db.storage
            .add_location(&path_str(&disk), LocationRole::Primary, 1, true)
            .unwrap();
        db.storage.pin(TABLE, &path_str(&disk)).unwrap();
        run(&db, &format!("CREATE TABLE {TABLE} (id INTEGER, k INTEGER, label TEXT)")).await;
        run(&db, &format!("CREATE TABLE {ROOT_TABLE} (id INTEGER)")).await;
        run(&db, &format!("CREATE TABLE {ROOT_MARK} (id INTEGER, label TEXT)")).await;
        assert!(
            regular_files(&disk)
                .iter()
                .any(|p| p.file_name().unwrap().to_string_lossy().starts_with(TABLE)),
            "the table's files are not on the second location"
        );
        assert!(
            crate::core::wal::wal_for(&path_str(&disk)).is_some(),
            "the second location has no log of its own, so there is nothing to test"
        );

        let mut id = 0;
        while id < ROWS {
            let end = (id + 200).min(ROWS);
            let values: Vec<String> = (id..end)
                .map(|i| format!("({i}, {}, '{}')", i * 10, label(i)))
                .collect();
            run(
                &db,
                &format!("INSERT INTO {TABLE} (id, k, label) VALUES {}", values.join(", ")),
            )
            .await;
            id = end;
        }
        let t = db.tables.get(TABLE).unwrap().clone();
        t.wait_filters_ready().await;
        t.checkpoint_filters().await;
        drop(t);
        db.sync_durable().unwrap();

        let root_base = durable_baseline(&root);
        let disk_base = durable_baseline(&disk);
        let root_watch = sync_audit::watch(&root);
        let disk_watch = sync_audit::watch(&disk);
        let _ = root_watch.take_events();
        let _ = disk_watch.take_events();

        Fixture {
            base,
            root,
            disk,
            db,
            root_base,
            disk_base,
            root_watch,
            disk_watch,
        }
    }

    /// Cuts power now and, for every seed, opens what the disks would hold.
    /// Returns per seed the mismatches between the filtered queries and the
    /// whole-table scan, and every sync the scenario saw.
    async fn crash_and_check(self) -> (Vec<String>, Vec<SyncEvent>) {
        self.crash_and_check_with(Expect::default()).await
    }

    /// [`Fixture::crash_and_check`], also checking that the transaction under
    /// test survived whole or not at all, as `expect` describes it.
    async fn crash_and_check_with(self, expect: Expect) -> (Vec<String>, Vec<SyncEvent>) {
        let mut events = self.root_watch.take_events();
        events.extend(self.disk_watch.take_events());

        let mut images = Vec::new();
        for seed in 0..SEEDS {
            let mut rng = Rng::new(seed);
            let img_root = self.base.path().join(format!("img_root_{seed}"));
            let img_disk = self.base.path().join(format!("img_disk_{seed}"));
            image_dir(&self.root, &img_root, &self.root_base, &events, seed, &mut rng, false);
            image_dir(&self.disk, &img_disk, &self.disk_base, &events, seed, &mut rng, expect.disk_data_whole);
            let pool = img_root.join(crate::core::storage::POOL_FILE);
            let text = std::fs::read_to_string(&pool).expect("the pool file should be in the image");
            let text = text
                .replace(&path_str(&self.disk), &path_str(&img_disk))
                .replace(&path_str(&self.root), &path_str(&img_root));
            std::fs::write(&pool, text).unwrap();
            images.push((seed, img_root, img_disk));
        }
        let mut total = dir_bytes(&self.root) + dir_bytes(&self.disk);
        for (_, r, d) in &images {
            total += dir_bytes(r) + dir_bytes(d);
        }
        assert!(total < MAX_SCENARIO_BYTES, "the scenario wrote {total} bytes, past its bound");

        // The originals go before an image opens, so nothing an image answers
        // can have come from them.
        let Fixture {
            base,
            root,
            disk,
            db,
            root_watch,
            disk_watch,
            ..
        } = self;
        drop(root_watch);
        drop(disk_watch);
        drop(db);
        crate::core::wal::remove_for(&path_str(&root));
        crate::core::wal::remove_for(&path_str(&disk));
        std::fs::remove_dir_all(&root).unwrap();
        std::fs::remove_dir_all(&disk).unwrap();

        let mut mismatches = Vec::new();
        for (seed, img_root, img_disk) in images {
            let db = open(&img_root).await;
            assert_eq!(
                db.storage.location_of(TABLE).as_deref(),
                Some(path_str(&img_disk).as_str()),
                "seed {seed}: the image resolves the table somewhere else"
            );
            let all = labels_in(&run(&db, &format!("SELECT label FROM {TABLE}")).await);
            // Every row durable before the scenario, exactly once, with the
            // victim present or not (its delete may or may not have survived)
            // and nothing of the uncommitted insert. A row reading another
            // row's bytes shows up here.
            let marker = expect.disk_marker.map(label);
            let rest: Vec<String> = all
                .iter()
                .filter(|l| **l != label(VICTIM) && Some(*l) != marker.as_ref())
                .cloned()
                .collect();
            let durable: Vec<String> = (0..ROWS).filter(|i| *i != VICTIM).map(label).collect();
            if rest != durable || all.len() > ROWS as usize + usize::from(marker.is_some()) {
                let extra: Vec<&String> = all.iter().filter(|l| !durable.contains(l)).collect();
                mismatches.push(format!(
                    "seed {seed}: the whole-table scan holds {} rows, not the {} durable ones \
                     (plus perhaps the victim); not among them: {extra:?}",
                    all.len(),
                    durable.len()
                ));
            }
            // The transaction under test is one unit: each of its effects in
            // the image says whether it committed, and they must all agree.
            let victim_present = all.contains(&label(VICTIM));
            let mut committed: Vec<(&str, bool)> = Vec::new();
            if expect.deletes_victim {
                committed.push(("the victim's delete", !victim_present));
            } else if !victim_present {
                mismatches.push(format!("seed {seed}: the victim, which nothing deleted, is gone"));
            }
            if let Some(m) = marker.as_ref() {
                committed.push((
                    "its insert on the second location",
                    all.iter().filter(|l| *l == m).count() == 1,
                ));
            }
            if let Some(m) = expect.root_marker {
                let marks = labels_in(&run(&db, &format!("SELECT label FROM {ROOT_MARK}")).await);
                committed.push((
                    "its insert in the root directory",
                    marks.iter().filter(|l| **l == label(m)).count() == 1,
                ));
            }
            if committed.iter().any(|c| c.1 != committed[0].1) {
                mismatches.push(format!(
                    "seed {seed}: the transaction survived in part: {committed:?}"
                ));
            }
            let twin: Vec<String> = all.iter().filter(|l| **l == label(VICTIM)).cloned().collect();
            for predicate in [
                format!("label = '{}'", label(VICTIM)),
                format!("k = {}", VICTIM * 10),
            ] {
                let got = labels_in(&run(&db, &format!("SELECT label FROM {TABLE} WHERE {predicate}")).await);
                if got != twin {
                    mismatches.push(format!(
                        "seed {seed}: `WHERE {predicate}` returned {got:?}, the whole-table scan {twin:?}"
                    ));
                }
            }
            drop(db);
            crate::core::wal::remove_for(&path_str(&img_root));
            crate::core::wal::remove_for(&path_str(&img_disk));
            std::fs::remove_dir_all(&img_root).unwrap();
            std::fs::remove_dir_all(&img_disk).unwrap();
        }
        drop(base);
        (mismatches, events)
    }
}

/// What else an image must show about the transaction under test.
#[derive(Debug, Clone, Copy)]
struct Expect {
    /// It deleted `VICTIM`.
    deletes_victim: bool,
    /// An id it inserted into `TABLE` (on the second location).
    disk_marker: Option<i32>,
    /// An id it inserted into `ROOT_MARK` (beside the outcome records).
    root_marker: Option<i32>,
    /// The second location's data files were synced after the baseline (a
    /// checkpoint of its log ran); see [`image_dir`]. The root directory's
    /// never are here, so its pages always mix.
    disk_data_whole: bool,
}

impl Default for Expect {
    fn default() -> Expect {
        Expect {
            deletes_victim: true,
            disk_marker: None,
            root_marker: None,
            disk_data_whole: false,
        }
    }
}

/// The first sync noted of `log` at a length of at least `len` by `thread`,
/// as an index into `events` (one watch's, so in the order they were noted).
fn first_sync(events: &[SyncEvent], log: &Path, len: u64, thread: Option<ThreadId>) -> Option<usize> {
    events
        .iter()
        .position(|e| e.tag == "wal.log" && e.path == log && e.len >= len && thread.is_none_or(|t| e.thread == t))
}

/// Whether a thread other than `ours` noted a sync of either log before
/// index `before` (or at all, with `None`): such a sync may have ordered
/// things on the scenario's behalf, so an order the scenario's own syncs
/// show proves nothing.
fn disturbed(events: &[SyncEvent], logs: [&Path; 2], ours: ThreadId, before: Option<usize>) -> bool {
    events[..before.unwrap_or(events.len())]
        .iter()
        .any(|e| e.thread != ours && e.tag == "wal.log" && logs.contains(&e.path.as_path()))
}

/// T deletes a row on the second location and commits in memory, its outcome
/// staged in the root log and not yet synced (an asynchronous commit, or the
/// window before its sync). Vacuum then finds the version reclaimable and
/// clears it from the filter; two checkpoints follow (the first records a
/// frontier below the cleared slot, the second, after a later uncommitted
/// insert, the full one), each synced in the second location's log. Power is
/// cut before the root log syncs on its own. Whatever the image holds, a
/// filtered query must agree with the whole-table scan.
///
/// The later insert's row is too long for the freed extent, so it is
/// appended and the case isolates the filter.
#[tokio::test(flavor = "multi_thread")]
async fn a_vacuum_clear_never_outlives_an_undone_delete() {
    scenario(&format!("L99999{}", "x".repeat(400))).await;
}

/// The same, with a later insert short enough to take the extent vacuum
/// freed. Handing the victim's bytes out before its deleter's outcome is
/// durable lets the insert overwrite them; the power cut then brings the
/// victim back, reading the insert's bytes.
#[tokio::test(flavor = "multi_thread")]
async fn freed_space_is_not_reused_before_the_delete_is_durable() {
    scenario("L99999").await;
}

async fn scenario(later_label: &str) {
    for attempt in 1..=MAX_ATTEMPTS {
        let fx = Fixture::new().await;
        let db = fx.db.clone();
        let t = db.tables.get(TABLE).unwrap().clone();

        let tx = db.transactions.begin();
        // The later writer, begun now (it only writes after the vacuum).
        let later = db.transactions.begin();
        write_in(&db, tx, &format!("DELETE FROM {TABLE} WHERE id = {VICTIM}")).await;
        // Another session commits durably while both run, so their ids are on
        // disk as begun: recovery reads them as unfinished and hands neither
        // id out again. Without this the image's own first statements are
        // given those ids and commit, which makes T's surviving stamp read as
        // a committed delete and the later insert as committed: a separate
        // bug (ids reused after a power cut), outside this test.
        run(&db, &format!("INSERT INTO {ROOT_TABLE} (id) VALUES (1)")).await;
        db.commit_transaction(tx);
        db.transactions.flush_deferred().unwrap();

        let report = t.vacuum_now().await.unwrap();
        assert!(
            report.fingerprints_cleared >= 1,
            "vacuum cleared nothing, so the scenario tests nothing: {report:?}"
        );
        t.checkpoint_filters().await;

        // A later write, left uncommitted, touches the filter so the next
        // checkpoint has something to append.
        write_in(
            &db,
            later,
            &format!("INSERT INTO {TABLE} (id, k, label) VALUES (99999, 999990, '{later_label}')"),
        )
        .await;
        t.checkpoint_filters().await;
        drop(t);
        drop(db);

        let ours: Vec<ThreadId> = vec![std::thread::current().id()];
        let (mismatches, events) = fx.crash_and_check().await;
        // Any image is a state a power cut can leave, so a mismatch is a wrong
        // answer whoever issued the syncs. Only a clean pass is rerun when a
        // background sync landed, since that sync may have closed the window.
        assert!(
            mismatches.is_empty(),
            "a filtered query lost a row the whole-table scan sees after a power cut:\n{}\n\
             syncs inside the scenario: {events:#?}",
            mismatches.join("\n")
        );
        let stray: Vec<&SyncEvent> = events.iter().filter(|e| !ours.contains(&e.thread)).collect();
        if !stray.is_empty() {
            eprintln!("attempt {attempt} disturbed by a background sync: {stray:?}");
            continue;
        }
        return;
    }
    panic!("every attempt had a background sync land inside it");
}

/// Review S6: `VACUUM FULL` decides what is dead from deleters' outcomes held
/// in memory, then moves live rows over those versions' bytes and slots in
/// durable batches of the table's own log. T deletes the victim and inserts a
/// marker row, both on the second location, and commits in memory, its
/// outcome staged in the root log and not synced. The compaction runs, and
/// power is cut before the root log syncs on its own. An image where T did
/// not commit must show the victim live and the marker absent; the
/// compaction has by then given the victim's slot and bytes to other rows.
#[tokio::test(flavor = "multi_thread")]
async fn a_vacuum_full_never_moves_rows_over_an_undone_delete() {
    const MARKER: i32 = 88888;
    for attempt in 1..=MAX_ATTEMPTS {
        let fx = Fixture::new().await;
        let root_wal = fx.root.join(DEFAULT_WAL_NAME);
        let disk_wal = fx.disk.join(DEFAULT_WAL_NAME);
        let db = fx.db.clone();
        let t = db.tables.get(TABLE).unwrap().clone();

        let tx = db.transactions.begin();
        write_in(&db, tx, &format!("DELETE FROM {TABLE} WHERE id = {VICTIM}")).await;
        write_in(
            &db,
            tx,
            &format!(
                "INSERT INTO {TABLE} (id, k, label) VALUES ({MARKER}, {}, '{}')",
                MARKER * 10,
                label(MARKER)
            ),
        )
        .await;
        // See `scenario`: records T's id as begun on disk.
        run(&db, &format!("INSERT INTO {ROOT_TABLE} (id) VALUES (1)")).await;
        db.commit_transaction(tx);
        db.transactions.flush_deferred().unwrap();
        // T's outcome is in the root log up to here, unsynced; anything the
        // second location's log holds past its length now is the compaction's.
        let outcome_len = std::fs::metadata(&root_wal).unwrap().len();
        let rows_len = std::fs::metadata(&disk_wal).unwrap().len();

        // Both directories in one watch, so its events are in one order.
        let order = sync_audit::watch(fx.base.path());
        let report = t.compact().await.expect("the compaction failed");
        let ordered = order.take_events();
        drop(order);
        assert!(
            report.dead_rows >= 1 && report.slots_moved > 0,
            "the compaction reclaimed or moved nothing, so the scenario tests nothing: {report:?}"
        );
        drop(t);
        drop(db);

        let me = std::thread::current().id();
        let ours: Vec<ThreadId> = vec![me];
        // The images are built only after the last sync, so they cannot see
        // a barrier taken too late: by then the outcome log is synced either
        // way. The order of the syncs can. Nothing may move durably (a sync
        // of the table's log past T's records) before T's outcome is durable.
        let barrier = first_sync(&ordered, &root_wal, outcome_len, Some(me));
        let first_move = ordered
            .iter()
            .position(|e| e.tag == "wal.log" && e.path == disk_wal && e.len > rows_len);
        let order_disturbed = disturbed(&ordered, [&root_wal, &disk_wal], me, barrier);
        let (mismatches, events) = fx
            .crash_and_check_with(Expect {
                deletes_victim: true,
                disk_marker: Some(MARKER),
                root_marker: None,
                disk_data_whole: true,
            })
            .await;
        assert!(
            mismatches.is_empty(),
            "a power cut after VACUUM FULL left a transaction applied in part:\n{}\n\
             syncs inside the scenario: {events:#?}",
            mismatches.join("\n")
        );
        // The compaction's checkpoint syncs the second location's files from
        // a blocking thread; only a foreign sync of the outcome log can have
        // closed the window.
        let stray: Vec<&SyncEvent> = events
            .iter()
            .filter(|e| !ours.contains(&e.thread) && e.tag == "wal.log" && e.path == root_wal)
            .collect();
        if !stray.is_empty() || order_disturbed {
            eprintln!("attempt {attempt} disturbed by a background sync: {stray:?}");
            continue;
        }
        let first_move = first_move.expect("no sync of the table's log carried a move, so the order proves nothing");
        assert!(
            barrier.is_some_and(|b| b < first_move),
            "rows moved durably (sync {first_move}) before the deleters' outcomes were (sync {barrier:?}): \
             a power cut between the two undoes the delete under moved rows\nsyncs: {ordered:#?}"
        );
        return;
    }
    panic!("every attempt had a background sync land inside it");
}

/// Review S13: a sweep's outcome barrier synced the swept table's directory
/// and then the outcome log, which makes every staged outcome durable, not
/// only the deleters'. T inserts one row beside the outcome records and one
/// on the second location and commits in memory. A sweep of an unrelated
/// root-directory table then runs its barrier, and power is cut before the
/// second location's log syncs. T must survive whole or not at all.
#[tokio::test(flavor = "multi_thread")]
async fn a_sweep_never_makes_a_transaction_durable_in_part() {
    const DISK_MARKER: i32 = 88888;
    const ROOT_MARKER: i32 = 77777;
    for attempt in 1..=MAX_ATTEMPTS {
        let fx = Fixture::new().await;
        let root_wal = fx.root.join(DEFAULT_WAL_NAME);
        let disk_wal = fx.disk.join(DEFAULT_WAL_NAME);
        let db = fx.db.clone();

        // Dead versions for the sweep to find, in a root-directory table.
        let values: Vec<String> = (1..=50).map(|i| format!("({i}, 'D{i:05}')")).collect();
        run(&db, &format!("INSERT INTO {ROOT_MARK} (id, label) VALUES {}", values.join(", "))).await;
        run(&db, &format!("DELETE FROM {ROOT_MARK} WHERE id <= 50")).await;

        let tx = db.transactions.begin();
        // Before T writes anything, so this sync does not carry T's rows:
        // records T's id as begun on disk (see `scenario`).
        run(&db, &format!("INSERT INTO {ROOT_TABLE} (id) VALUES (1)")).await;
        write_in(
            &db,
            tx,
            &format!(
                "INSERT INTO {TABLE} (id, k, label) VALUES ({DISK_MARKER}, {}, '{}')",
                DISK_MARKER * 10,
                label(DISK_MARKER)
            ),
        )
        .await;
        write_in(
            &db,
            tx,
            &format!(
                "INSERT INTO {ROOT_MARK} (id, label) VALUES ({ROOT_MARKER}, '{}')",
                label(ROOT_MARKER)
            ),
        )
        .await;
        db.commit_transaction(tx);
        db.transactions.flush_deferred().unwrap();
        // T's outcome, and its rows on the second location, end here.
        let outcome_len = std::fs::metadata(&root_wal).unwrap().len();
        let rows_len = std::fs::metadata(&disk_wal).unwrap().len();

        // Both directories in one watch, so its events are in one order.
        let order = sync_audit::watch(fx.base.path());
        let q = db.tables.get(ROOT_MARK).unwrap().clone();
        let report = q.vacuum_now().await.unwrap();
        let ordered = order.take_events();
        drop(order);
        assert!(
            report.dead_rows >= 50,
            "the sweep found nothing reclaimable, so no barrier ran: {report:?}"
        );
        drop(q);
        drop(db);

        let me = std::thread::current().id();
        let ours: Vec<ThreadId> = vec![me];
        // The images are built only after the last sync of each log, so they
        // cannot tell the barrier's order from its reverse. The syncs can: T's
        // outcome must not be synced before its rows on the second location.
        let outcome_synced = first_sync(&ordered, &root_wal, outcome_len, Some(me));
        let rows_synced = first_sync(&ordered, &disk_wal, rows_len, None);
        let order_disturbed = disturbed(&ordered, [&root_wal, &disk_wal], me, outcome_synced);
        let (mismatches, events) = fx
            .crash_and_check_with(Expect {
                deletes_victim: false,
                disk_marker: Some(DISK_MARKER),
                root_marker: Some(ROOT_MARKER),
                disk_data_whole: false,
            })
            .await;
        assert!(
            mismatches.is_empty(),
            "a power cut after a sweep left a transaction applied in part:\n{}\n\
             syncs inside the scenario: {events:#?}",
            mismatches.join("\n")
        );
        let stray: Vec<&SyncEvent> = events
            .iter()
            .filter(|e| {
                !ours.contains(&e.thread) && e.tag == "wal.log" && (e.path == root_wal || e.path == disk_wal)
            })
            .collect();
        if !stray.is_empty() || order_disturbed {
            eprintln!("attempt {attempt} disturbed by a background sync: {stray:?}");
            continue;
        }
        let outcome_synced = outcome_synced.expect("the sweep's barrier never synced the outcome log");
        assert!(
            rows_synced.is_some_and(|r| r < outcome_synced),
            "T's outcome was synced (sync {outcome_synced}) before its rows on the second location \
             (sync {rows_synced:?}): a power cut between the two commits half of it\nsyncs: {ordered:#?}"
        );
        return;
    }
    panic!("every attempt had a background sync land inside it");
}

/// Review follow-up to S13: the barrier's list of logs is process-wide. A
/// sweep in database A must not sync database B's outcome log. It cannot tell
/// which directories are B's, so it cannot order B's outcomes after B's rows
/// on its other locations, and syncing them first commits half of a B
/// transaction. T, in B, inserts beside B's outcomes and on B's second
/// location and commits in memory; A then sweeps a table with dead rows.
#[tokio::test(flavor = "multi_thread")]
async fn a_sweep_never_syncs_another_databases_outcomes() {
    const DISK_MARKER: i32 = 88888;
    const ROOT_MARKER: i32 = 77777;
    for attempt in 1..=MAX_ATTEMPTS {
        let fx = Fixture::new().await;
        let root_wal = fx.root.join(DEFAULT_WAL_NAME);
        let disk_wal = fx.disk.join(DEFAULT_WAL_NAME);
        let db = fx.db.clone();

        let other = fx.base.path().join("other");
        std::fs::create_dir_all(&other).unwrap();
        let a = Arc::new(Database::new_named(&path_str(&other), false, "vacuumclear_a").await);
        run(&a, "CREATE TABLE va (id INTEGER, label TEXT)").await;
        let values: Vec<String> = (1..=50).map(|i| format!("({i}, 'D{i:05}')")).collect();
        run(&a, &format!("INSERT INTO va (id, label) VALUES {}", values.join(", "))).await;
        run(&a, "DELETE FROM va WHERE id <= 50").await;

        let tx = db.transactions.begin();
        // See `a_sweep_never_makes_a_transaction_durable_in_part`.
        run(&db, &format!("INSERT INTO {ROOT_TABLE} (id) VALUES (1)")).await;
        write_in(
            &db,
            tx,
            &format!(
                "INSERT INTO {TABLE} (id, k, label) VALUES ({DISK_MARKER}, {}, '{}')",
                DISK_MARKER * 10,
                label(DISK_MARKER)
            ),
        )
        .await;
        write_in(
            &db,
            tx,
            &format!(
                "INSERT INTO {ROOT_MARK} (id, label) VALUES ({ROOT_MARKER}, '{}')",
                label(ROOT_MARKER)
            ),
        )
        .await;
        db.commit_transaction(tx);
        db.transactions.flush_deferred().unwrap();
        let outcome_len = std::fs::metadata(&root_wal).unwrap().len();
        let _ = fx.root_watch.take_events();
        let _ = fx.disk_watch.take_events();

        let va = a.tables.get("va").unwrap().clone();
        let report = va.vacuum_now().await.unwrap();
        assert!(report.dead_rows >= 50, "A's sweep found nothing reclaimable, so no barrier ran: {report:?}");
        drop(va);

        let me = std::thread::current().id();
        let mut events = fx.root_watch.take_events();
        events.extend(fx.disk_watch.take_events());
        let ours: Vec<&SyncEvent> = events
            .iter()
            .filter(|e| e.thread == me && e.tag == "wal.log" && e.path == root_wal && e.len >= outcome_len)
            .collect();

        drop(a);
        crate::core::wal::remove_for(&path_str(&other));
        drop(db);
        let Fixture { base, root, disk, db, .. } = fx;
        drop(db);
        crate::core::wal::remove_for(&path_str(&root));
        crate::core::wal::remove_for(&path_str(&disk));
        drop(base);

        // A sweep of A's that found B's logs already synced by the log writer
        // has nothing to show either way.
        if disturbed(&events, [&root_wal, &disk_wal], me, None) {
            eprintln!("attempt {attempt} disturbed by a background sync");
            continue;
        }
        assert!(
            ours.is_empty(),
            "a sweep in database A synced database B's outcome log: {ours:?}\nB's syncs: {events:#?}"
        );
        return;
    }
    panic!("every attempt had a background sync land inside it");
}

/// Review follow-up to S13: the swept table's own log holds the deleters'
/// stamps the decisions rest on. When it cannot be synced, the sweep clears
/// and publishes nothing, and `VACUUM FULL` moves nothing. A later sweep, with
/// the log healthy again, finds the same rows.
#[tokio::test(flavor = "multi_thread")]
async fn a_barrier_that_cannot_sync_the_tables_log_acts_on_nothing() {
    use crate::core::vacuum::barrier_test_hooks::FAIL_SYNC_IN;
    let fx = Fixture::new().await;
    let db = fx.db.clone();
    run(&db, &format!("DELETE FROM {TABLE} WHERE id < 50")).await;
    let t = db.tables.get(TABLE).unwrap().clone();

    FAIL_SYNC_IN.with(|f| *f.borrow_mut() = Some(fx.disk.clone()));
    let swept = t.vacuum_now().await;
    let compacted = t.compact().await;
    FAIL_SYNC_IN.with(|f| *f.borrow_mut() = None);
    let swept = swept.expect("the sweep itself failed");
    assert!(swept.dead_rows >= 50, "the sweep found nothing reclaimable: {swept:?}");
    assert!(swept.deferred, "a sweep whose barrier failed was not deferred: {swept:?}");
    assert_eq!(swept.fingerprints_cleared, 0, "a sweep whose barrier failed cleared fingerprints");
    assert_eq!(t.reclaimable_bytes(), 0, "a sweep whose barrier failed published free space");
    assert!(compacted.is_err(), "a compaction whose barrier failed went ahead");

    let all = labels_in(&run(&db, &format!("SELECT label FROM {TABLE}")).await);
    let want: Vec<String> = (50..ROWS).map(label).collect();
    assert_eq!(all, want, "the failed sweep or compaction changed the table");

    let again = t.vacuum_now().await.unwrap();
    assert!(!again.deferred && again.dead_rows >= 50, "the next sweep did not find the rows again: {again:?}");
    drop(t);

    drop(db);
    let Fixture { base, root, disk, db, .. } = fx;
    drop(db);
    crate::core::wal::remove_for(&path_str(&root));
    crate::core::wal::remove_for(&path_str(&disk));
    drop(base);
}

/// The other half of the barrier's error policy: another directory's log
/// failing to sync is logged and does not stop the sweep. A failed sync
/// poisons that log, and the log writer syncs every outcome log on its next
/// round whatever failed before it, so stopping here orders nothing; it would
/// only stop reclamation everywhere while one directory is broken.
#[tokio::test(flavor = "multi_thread")]
async fn another_directorys_log_failing_does_not_stop_a_sweep() {
    use crate::core::vacuum::barrier_test_hooks::FAIL_SYNC_IN;
    let fx = Fixture::new().await;
    let db = fx.db.clone();
    let values: Vec<String> = (1..=50).map(|i| format!("({i}, 'D{i:05}')")).collect();
    run(&db, &format!("INSERT INTO {ROOT_MARK} (id, label) VALUES {}", values.join(", "))).await;
    run(&db, &format!("DELETE FROM {ROOT_MARK} WHERE id <= 50")).await;

    let q = db.tables.get(ROOT_MARK).unwrap().clone();
    FAIL_SYNC_IN.with(|f| *f.borrow_mut() = Some(fx.disk.clone()));
    let swept = q.vacuum_now().await;
    FAIL_SYNC_IN.with(|f| *f.borrow_mut() = None);
    let swept = swept.expect("the sweep itself failed");
    assert!(swept.dead_rows >= 50, "the sweep found nothing reclaimable: {swept:?}");
    assert!(!swept.deferred, "another directory's log stopped the sweep: {swept:?}");
    drop(q);

    drop(db);
    let Fixture { base, root, disk, db, .. } = fx;
    drop(db);
    crate::core::wal::remove_for(&path_str(&root));
    crate::core::wal::remove_for(&path_str(&disk));
    drop(base);
}

/// Review follow-up to S13, in the log writer: a round used to visit the
/// databases one by one and sync "every log but this one's own" for each, so
/// the first database visited synced every *other* database's outcome log in
/// registry (hash) order, possibly before that database's second location.
/// T, in B, writes rows on B's second location and commits in memory; A is
/// registered before B, so A (or an older database) is visited first. Every
/// round must sync B's rows before B's outcomes. The hash order differs per
/// directory, so the old round fails about one round in two; twelve
/// undisturbed rounds are required.
#[tokio::test(flavor = "multi_thread")]
async fn a_log_writer_round_syncs_each_databases_outcomes_after_its_rows() {
    const ROUNDS: usize = 12;
    let mut checked = 0;
    for _ in 0..ROUNDS * 4 {
        let base = tempfile::tempdir().unwrap();
        let a_root = base.path().join("a");
        let b_root = base.path().join("b_root");
        let b_disk = base.path().join("b_disk");
        for d in [&a_root, &b_root, &b_disk] {
            std::fs::create_dir_all(d).unwrap();
        }
        let _ = crate::core::processor::concurrent_processor::ENABLE_CACHE.set(false);
        let a = Arc::new(Database::new_named(&path_str(&a_root), false, "logwriter_a").await);
        let b = Arc::new(Database::new_named(&path_str(&b_root), false, "logwriter_b").await);
        b.storage
            .add_location(&path_str(&b_disk), LocationRole::Primary, 1, true)
            .unwrap();
        b.storage.pin("lw_disk", &path_str(&b_disk)).unwrap();
        run(&b, "CREATE TABLE lw_disk (id INTEGER)").await;
        run(&b, "CREATE TABLE lw_root (id INTEGER)").await;
        run(&a, "CREATE TABLE lw_a (id INTEGER)").await;

        let tx = b.transactions.begin();
        write_in(&b, tx, "INSERT INTO lw_disk (id) VALUES (1)").await;
        write_in(&b, tx, "INSERT INTO lw_root (id) VALUES (1)").await;
        b.commit_transaction(tx);
        b.transactions.flush_deferred().unwrap();
        let ta = a.transactions.begin();
        write_in(&a, ta, "INSERT INTO lw_a (id) VALUES (1)").await;
        a.commit_transaction(ta);
        a.transactions.flush_deferred().unwrap();
        let b_root_wal = b_root.join(DEFAULT_WAL_NAME);
        let b_disk_wal = b_disk.join(DEFAULT_WAL_NAME);
        let outcome_len = std::fs::metadata(&b_root_wal).unwrap().len();
        let rows_len = std::fs::metadata(&b_disk_wal).unwrap().len();

        let watch = sync_audit::watch(base.path());
        let me = std::thread::current().id();
        // On this thread, as the writer's task runs it on a blocking one.
        crate::core::log_writer::flush_now();
        let events = watch.take_events();
        drop(watch);

        drop(a);
        drop(b);
        for d in [&a_root, &b_root, &b_disk] {
            crate::core::wal::remove_for(&path_str(d));
        }
        drop(base);

        let outcome_synced = first_sync(&events, &b_root_wal, outcome_len, Some(me));
        let rows_synced = first_sync(&events, &b_disk_wal, rows_len, Some(me));
        // The background round got there first, or in between: this round
        // shows nothing about its own order.
        let (Some(outcome_synced), Some(rows_synced)) = (outcome_synced, rows_synced) else {
            continue;
        };
        if disturbed(&events, [&b_root_wal, &b_disk_wal], me, Some(outcome_synced)) {
            continue;
        }
        assert!(
            rows_synced < outcome_synced,
            "a log writer round synced B's outcomes (sync {outcome_synced}) before B's rows on its \
             second location (sync {rows_synced}): a power cut between the two commits half of T\n\
             syncs: {events:#?}"
        );
        checked += 1;
        if checked == ROUNDS {
            return;
        }
    }
    panic!("only {checked} of {ROUNDS} rounds ran undisturbed by the background log writer");
}
