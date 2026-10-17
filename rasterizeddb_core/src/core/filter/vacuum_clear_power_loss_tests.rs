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

/// Writes `src`'s image for `seed` into `img`.
fn image_dir(src: &Path, img: &Path, baseline: &Baseline, events: &[SyncEvent], seed: u64, rng: &mut Rng) {
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
        } else if seed == 0 {
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
        let mut events = self.root_watch.take_events();
        events.extend(self.disk_watch.take_events());

        let mut images = Vec::new();
        for seed in 0..SEEDS {
            let mut rng = Rng::new(seed);
            let img_root = self.base.path().join(format!("img_root_{seed}"));
            let img_disk = self.base.path().join(format!("img_disk_{seed}"));
            image_dir(&self.root, &img_root, &self.root_base, &events, seed, &mut rng);
            image_dir(&self.disk, &img_disk, &self.disk_base, &events, seed, &mut rng);
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
            let rest: Vec<String> = all.iter().filter(|l| **l != label(VICTIM)).cloned().collect();
            let durable: Vec<String> = (0..ROWS).filter(|i| *i != VICTIM).map(label).collect();
            if rest != durable || all.len() > ROWS as usize {
                let extra: Vec<&String> = all.iter().filter(|l| !durable.contains(l)).collect();
                mismatches.push(format!(
                    "seed {seed}: the whole-table scan holds {} rows, not the {} durable ones \
                     (plus perhaps the victim); not among them: {extra:?}",
                    all.len(),
                    durable.len()
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
