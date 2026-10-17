//! Is a transaction that writes to two storage-pool directories all-or-nothing
//! across a power cut?
//!
//! `Database::sync_durable` syncs every non-root location's log and then the
//! root's, because the root's log also carries the transaction outcome. That
//! orders one session's own rows before its own outcome, and nothing else:
//! another session's syncs used to commit a transaction half-durably (the two
//! gap tests below, which failed before `Database::commit_written_transaction`
//! started syncing the other locations ahead of the in-memory commit).
//!
//! No process is killed. A crash is simulated by building the durable state at
//! a chosen instant and opening a fresh database from it:
//! - every non-log file as of a baseline the harness itself fsyncs, before the
//!   interleaving starts. The write path with a log never fsyncs a data file,
//!   which is checked (`assert_no_data_file_sync`), so this is what the disk is
//!   allowed to hold after a power cut;
//! - each log cut at the longest length any `fdatasync` of it was issued at, as
//!   `sync_audit` recorded it. Bytes written after that may be lost.
//!
//! The image is opened from new directories, with the originals deleted first,
//! because `db_storage.cfg` names absolute paths and a copy would otherwise
//! read the source (see `storage::repoint_pool_config`).
//!
//! The interleavings are driven by hand: the transaction under test is run with
//! `execute_with_context` and its commit steps called one by one, as the
//! executor and pgwire run them (`commit_written_transaction`,
//! `flush_deferred`, `sync_own_location`), and a concurrent `sync_durable` is
//! held between its two halves by `test_hooks::AFTER_POOL_SYNC`.
//!
//! The process-wide log writer (`log_writer.rs`) syncs every log on a timer
//! and can land inside a scenario, which would hide the window being tested. A
//! sync issued from any thread the scenario did not drive is detected and the
//! scenario rerun, so the result does not depend on the timer.
//!
//! Failure paths use `test_hooks::FAIL_POOL_SYNC`, which fails the next
//! non-root log sync on the calling thread: a commit whose other locations
//! could not be made durable must commit nothing, and that is also what pins
//! the order inside `commit_written_transaction` — committed first, the
//! transaction would already be visible when the sync failed.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::thread::ThreadId;
use std::time::Duration;

use rclite::Arc;

use crate::core::database::Database;
use crate::core::db_type::DbType;
use crate::core::mvcc::{TxId, TxOutcome};
use crate::core::rql::lexer_ct::{CreateColumnData, CreateTable};
use crate::core::storage::LocationRole;
use crate::core::sync_audit::{self, SyncEvent};
use crate::core::wal::DEFAULT_WAL_NAME;

/// In the database's own directory, next to the outcome records.
const ROOT_TABLE: &str = "atom_root";
/// Pinned to the second location.
const DISK_TABLE: &str = "atom_disk";

/// Ceiling on what one scenario may put on disk, both directories and the
/// image together. Checked, not assumed: the image lands in the shared tmpfs.
const MAX_SCENARIO_BYTES: u64 = 256 * 1024 * 1024;

/// Reruns allowed when a background sync lands inside a scenario.
const MAX_ATTEMPTS: usize = 5;

fn create_statement(name: &str) -> CreateTable {
    CreateTable {
        table_name: name.to_string(),
        columns: vec![
            CreateColumnData {
                name: "id".to_string(),
                data_type: DbType::I32,
            },
            CreateColumnData {
                name: "label".to_string(),
                data_type: DbType::STRING,
            },
        ],
        unique_columns: Vec::new(),
        foreign_keys: Vec::new(),
        defaults: Vec::new(),
        json_columns: Vec::new(),
        unique_keys: Vec::new(),
        type_modifiers: Vec::new(),
        generated: Vec::new(),
        checks: Vec::new(),
        primary_key: None,
        if_not_exists: false,
        is_immutable: false,
    }
}

fn path_str(p: &Path) -> String {
    p.to_string_lossy().into_owned()
}

async fn open(root: &Path) -> Arc<Database> {
    Arc::new(Database::new_named(&path_str(root), false, "crossdircommit").await)
}

/// Runs one statement inside `tx` without committing it, as an explicit block
/// does. Panics on failure: a scenario whose write did not happen tests nothing.
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

/// An autocommit insert, durable on return.
async fn insert_autocommit(db: &Arc<Database>, table: &str, id: i32, label: &str) {
    let sql = format!("INSERT INTO {table} (id, label) VALUES ({id}, '{label}')");
    let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(&sql)
        .unwrap_or_else(|| panic!("could not parse {sql}"));
    let out = crate::core::rql::executor::execute(purpose, db.clone()).await;
    assert!(
        !out.is_empty() && out[0] != 3,
        "{sql} failed: {}",
        String::from_utf8_lossy(&out)
    );
}

fn insert_sql(table: &str, id: i32, label: &str) -> String {
    format!("INSERT INTO {table} (id, label) VALUES ({id}, '{label}')")
}

/// The labels a fresh snapshot sees in `table`.
async fn labels(db: &Arc<Database>, table: &str, candidates: &[&str]) -> HashSet<String> {
    let sql = format!("SELECT label FROM {table}");
    let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(&sql)
        .unwrap_or_else(|| panic!("could not parse {sql}"));
    let out = crate::core::rql::executor::execute(purpose, db.clone()).await;
    assert!(
        !out.is_empty() && out[0] != 3,
        "{sql} failed after recovery: {}",
        String::from_utf8_lossy(&out)
    );
    let text = String::from_utf8_lossy(&out);
    candidates
        .iter()
        .filter(|c| text.contains(*c))
        .map(|c| c.to_string())
        .collect()
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
    regular_files(dir)
        .iter()
        .map(|p| std::fs::metadata(p).map(|m| m.len()).unwrap_or(0))
        .sum()
}

/// One directory's durable state at the baseline.
struct Baseline {
    src: PathBuf,
    /// Where the image of `src` is built. Data files go in at the baseline.
    img: PathBuf,
    /// The log's bytes at the baseline, which the harness fsynced.
    wal: Vec<u8>,
    /// Every file present at the baseline, so files appearing later can be
    /// reported rather than silently left out.
    files: HashSet<PathBuf>,
}

/// Fsyncs every file in `src` and copies it into `img`: the state a power cut
/// at this instant is guaranteed to leave. Subdirectories are not expected
/// here, and a non-file entry would be one this harness does not model.
fn take_baseline(src: &Path, img: &Path) -> Baseline {
    std::fs::create_dir_all(img).unwrap();
    let mut wal = Vec::new();
    let mut files = HashSet::new();
    for path in regular_files(src) {
        let f = std::fs::File::options()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap_or_else(|e| panic!("cannot open {}: {e}", path.display()));
        f.sync_all().unwrap();
        let bytes = std::fs::read(&path).unwrap();
        let name = path.file_name().unwrap();
        if name == DEFAULT_WAL_NAME {
            wal = bytes.clone();
        }
        std::fs::write(img.join(name), &bytes).unwrap();
        files.insert(path);
    }
    let dir = std::fs::File::open(src).unwrap();
    dir.sync_all().unwrap();
    Baseline {
        src: src.to_path_buf(),
        img: img.to_path_buf(),
        wal,
        files,
    }
}

/// Cuts the log into the image at the longest length a sync of it was issued
/// at, never below the baseline. Returns that length and the source log's.
fn finish_image(b: &Baseline, events: &[SyncEvent]) -> (u64, u64) {
    let wal_path = b.src.join(DEFAULT_WAL_NAME);
    let current = std::fs::read(&wal_path).unwrap_or_default();
    assert!(
        current.starts_with(&b.wal),
        "{} no longer begins with its baseline bytes: it was truncated or replaced \
         inside the scenario, which this harness does not model",
        wal_path.display()
    );
    let synced = events
        .iter()
        .filter(|e| e.tag == "wal.log" && e.path == wal_path)
        .map(|e| e.len)
        .max()
        .unwrap_or(0);
    let durable = (b.wal.len() as u64).max(synced).min(current.len() as u64);
    std::fs::write(b.img.join(DEFAULT_WAL_NAME), &current[..durable as usize]).unwrap();
    (durable, current.len() as u64)
}

/// Every non-log sync would make the baseline copy too pessimistic. None is
/// expected on this path; one appearing means the harness no longer models it.
fn assert_no_data_file_sync(events: &[SyncEvent]) {
    let others: Vec<&SyncEvent> = events.iter().filter(|e| e.tag != "wal.log").collect();
    assert!(
        others.is_empty(),
        "a sync other than a log fdatasync happened inside the scenario, so the \
         baseline no longer stands for the data files' durable state: {others:#?}"
    );
}

/// Rewrites the pool file's absolute paths to the image's directories.
fn repoint(root_img: &Path, map: &[(String, String)]) {
    let path = root_img.join(crate::core::storage::POOL_FILE);
    let text = std::fs::read_to_string(&path).expect("the pool file should be in the image");
    let mut out = String::new();
    for line in text.lines() {
        let fields: Vec<String> = line
            .split('\t')
            .map(|f| {
                map.iter()
                    .find(|(from, _)| from.trim_end_matches('/') == f.trim_end_matches('/'))
                    .map(|(_, to)| to.clone())
                    .unwrap_or_else(|| f.to_string())
            })
            .collect();
        out.push_str(&fields.join("\t"));
        out.push('\n');
    }
    for (from, _) in map {
        assert!(
            !out.contains(from.as_str()),
            "the image's pool file still names {from}"
        );
    }
    std::fs::write(&path, out).unwrap();
}

/// A database with a table in its own directory and one on a second location,
/// each holding one durable row, at a fsynced baseline.
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
        db.storage.pin(DISK_TABLE, &path_str(&disk)).unwrap();
        db.add_table(create_statement(ROOT_TABLE)).await.unwrap();
        db.add_table(create_statement(DISK_TABLE)).await.unwrap();
        assert!(
            regular_files(&disk)
                .iter()
                .any(|p| p.file_name().unwrap() == "atom_disk.db"),
            "the disk table's files are not on the second location"
        );
        assert!(
            crate::core::wal::wal_for(&path_str(&disk)).is_some(),
            "the second location has no log of its own, so there is nothing to test"
        );

        // A durable row in each, so every lazily created file exists before the
        // baseline and the image can be checked to be the real tables.
        insert_autocommit(&db, ROOT_TABLE, 1, "base_root").await;
        insert_autocommit(&db, DISK_TABLE, 1, "base_disk").await;
        db.sync_durable().unwrap();

        let root_base = take_baseline(&root, &base.path().join("img_root"));
        let disk_base = take_baseline(&disk, &base.path().join("img_disk"));
        let root_watch = sync_audit::watch(&root);
        let disk_watch = sync_audit::watch(&disk);
        // Anything already recorded predates the baseline.
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

    /// Cuts power now, and opens what the disks would hold. Returns, per table,
    /// which of `candidates` a fresh snapshot sees, the syncs that happened
    /// inside the scenario, and where the second location's log was cut.
    async fn crash_and_recover(self, candidates: &[&str]) -> Seen {
        let mut events = self.root_watch.take_events();
        events.extend(self.disk_watch.take_events());
        assert_no_data_file_sync(&events);

        let (root_len, _) = finish_image(&self.root_base, &events);
        let (disk_len, disk_source_len) = finish_image(&self.disk_base, &events);

        let new_files: Vec<PathBuf> = regular_files(&self.root)
            .into_iter()
            .chain(regular_files(&self.disk))
            .filter(|p| !self.root_base.files.contains(p) && !self.disk_base.files.contains(p))
            .collect();
        assert!(
            new_files.is_empty(),
            "files appeared inside the scenario; the image would leave them out and \
             could fail for that reason instead: {new_files:?}"
        );

        let total = dir_bytes(&self.root)
            + dir_bytes(&self.disk)
            + dir_bytes(&self.root_base.img)
            + dir_bytes(&self.disk_base.img);
        assert!(
            total < MAX_SCENARIO_BYTES,
            "the scenario wrote {total} bytes, past its bound"
        );

        repoint(
            &self.root_base.img,
            &[
                (path_str(&self.root), path_str(&self.root_base.img)),
                (path_str(&self.disk), path_str(&self.disk_base.img)),
            ],
        );

        // The originals go before the image opens, so nothing it answers can
        // have come from them.
        drop(self.root_watch);
        drop(self.disk_watch);
        drop(self.db);
        crate::core::wal::remove_for(&path_str(&self.root));
        crate::core::wal::remove_for(&path_str(&self.disk));
        std::fs::remove_dir_all(&self.root).unwrap();
        std::fs::remove_dir_all(&self.disk).unwrap();

        let db = open(&self.root_base.img).await;
        assert_eq!(
            db.storage.location_of(DISK_TABLE).as_deref(),
            Some(path_str(&self.disk_base.img).as_str()),
            "the image resolves the disk table somewhere else"
        );
        let in_root = labels(&db, ROOT_TABLE, candidates).await;
        let in_disk = labels(&db, DISK_TABLE, candidates).await;
        assert!(
            in_root.contains("base_root") && in_disk.contains("base_disk"),
            "the image lost rows that were durable before the scenario began \
             (root {in_root:?}, disk {in_disk:?}; logs cut at {root_len} and {disk_len} bytes)"
        );

        drop(db);
        crate::core::wal::remove_for(&path_str(&self.root_base.img));
        crate::core::wal::remove_for(&path_str(&self.disk_base.img));
        drop(self.base);
        Seen {
            in_root,
            in_disk,
            events,
            disk_cut: disk_len,
            disk_source_len,
        }
    }
}

/// What one run of a scenario saw after the crash.
struct Seen {
    in_root: HashSet<String>,
    in_disk: HashSet<String>,
    events: Vec<SyncEvent>,
    /// Length of the second location's log in the image.
    disk_cut: u64,
    /// Length of that log in the source at the crash.
    disk_source_len: u64,
}

/// Syncs the scenario did not issue: the log writer's, landing in the window.
fn foreign(events: &[SyncEvent], ours: &[ThreadId]) -> Vec<SyncEvent> {
    events
        .iter()
        .filter(|e| !ours.contains(&e.thread))
        .cloned()
        .collect()
}

/// Runs `scenario` until no background sync lands inside it.
async fn run_undisturbed<F, Fut>(name: &str, scenario: F) -> Seen
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = (Seen, Vec<ThreadId>)>,
{
    for attempt in 1..=MAX_ATTEMPTS {
        let (seen, ours) = scenario().await;
        let stray = foreign(&seen.events, &ours);
        if stray.is_empty() {
            return seen;
        }
        eprintln!("{name}: attempt {attempt} disturbed by a background sync: {stray:?}");
    }
    panic!("{name}: every attempt had a background sync land inside it");
}

const CANDIDATES: &[&str] = &[
    "base_root",
    "base_disk",
    "alpha_root",
    "alpha_disk",
    "bravo_root",
    "charlie_disk",
];

/// Transaction A: one row in each directory, uncommitted.
async fn a_writes_both(db: &Arc<Database>) -> TxId {
    let tx = db.transactions.begin();
    write_in(db, tx, &insert_sql(DISK_TABLE, 2, "alpha_disk")).await;
    write_in(db, tx, &insert_sql(ROOT_TABLE, 2, "alpha_root")).await;
    tx
}

fn assert_all_or_nothing(name: &str, seen: &Seen) {
    let root = seen.in_root.contains("alpha_root");
    let disk = seen.in_disk.contains("alpha_disk");
    assert_eq!(
        root,
        disk,
        "{name}: transaction A is partly committed after the crash: its root row \
         {} and its second-location row {}. Syncs inside the scenario: {:#?}",
        if root { "survived" } else { "is gone" },
        if disk { "survived" } else { "is gone" },
        seen.events
    );
}

// ---------------------------------------------------------------------------
// Controls: the harness must keep what was synced and drop what was not.
// ---------------------------------------------------------------------------

/// A finishes its whole commit, as the executor and pgwire run it. Both rows
/// survive.
#[tokio::test(flavor = "multi_thread")]
async fn control_a_committed_and_synced_keeps_both_rows() {
    let seen = run_undisturbed("control_synced", || async {
        let fx = Fixture::new().await;
        let db = fx.db.clone();
        let tx = a_writes_both(&db).await;
        db.commit_written_transaction(tx).unwrap();
        db.transactions.flush_deferred().unwrap();
        db.sync_own_location().unwrap();
        drop(db);
        (
            fx.crash_and_recover(CANDIDATES).await,
            vec![std::thread::current().id()],
        )
    })
    .await;
    assert!(
        seen.in_root.contains("alpha_root") && seen.in_disk.contains("alpha_disk"),
        "a commit that completed lost rows: root {:?}, disk {:?}; syncs: {:#?}",
        seen.in_root,
        seen.in_disk,
        seen.events
    );
}

/// A stages its outcome and nobody syncs anything. Neither row survives: the
/// image really does drop what was only in the page cache. A harness check, so
/// it commits with the bare `commit_transaction`, which syncs nothing.
#[tokio::test(flavor = "multi_thread")]
async fn control_a_never_synced_keeps_neither_row() {
    let seen = run_undisturbed("control_unsynced", || async {
        let fx = Fixture::new().await;
        let db = fx.db.clone();
        let tx = a_writes_both(&db).await;
        db.commit_transaction(tx);
        db.transactions.flush_deferred().unwrap();
        drop(db);
        (
            fx.crash_and_recover(CANDIDATES).await,
            vec![std::thread::current().id()],
        )
    })
    .await;
    assert!(
        seen.events.is_empty(),
        "nothing was synced in this scenario, yet: {:?}",
        seen.events
    );
    assert!(
        !seen.in_root.contains("alpha_root") && !seen.in_disk.contains("alpha_disk"),
        "rows that were never synced survived the crash image: root {:?}, disk {:?}",
        seen.in_root,
        seen.in_disk
    );
}

/// The second location's log is synced once, then written to again. The image
/// keeps the synced prefix and drops the tail: the partial cut that variant 1
/// relies on, where a log is neither untouched since the baseline nor synced
/// to its end. Y commits with the bare `commit_transaction` and only the root
/// is synced after it, so its outcome is durable and its row is not — a
/// harness that kept the whole log once any sync was seen would show Y.
#[tokio::test(flavor = "multi_thread")]
async fn control_the_unsynced_tail_of_a_synced_log_is_dropped() {
    const SEEN: &[&str] = &["base_root", "base_disk", "xray_disk", "yankee_disk"];
    let seen = run_undisturbed("control_tail", || async {
        let fx = Fixture::new().await;
        let db = fx.db.clone();

        // X: a complete autocommit, waiting for its sync whatever another
        // test has set the server default to.
        let sql = insert_sql(DISK_TABLE, 7, "xray_disk");
        let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(&sql)
            .unwrap_or_else(|| panic!("could not parse {sql}"));
        let out = crate::core::rql::executor::execute_with_sync(purpose, db.clone(), true).await;
        assert!(
            !out.is_empty() && out[0] != 3,
            "{sql} failed: {}",
            String::from_utf8_lossy(&out)
        );

        // Y: appended after X's sync, committed, and only the root synced.
        let tx = db.transactions.begin();
        write_in(&db, tx, &insert_sql(DISK_TABLE, 8, "yankee_disk")).await;
        db.commit_transaction(tx);
        db.transactions.flush_deferred().unwrap();
        db.sync_own_location().unwrap();
        drop(db);
        (
            fx.crash_and_recover(SEEN).await,
            vec![std::thread::current().id()],
        )
    })
    .await;
    assert!(
        seen.in_disk.contains("xray_disk"),
        "X was synced and acknowledged, yet the image lost it: {:?}; syncs: {:#?}",
        seen.in_disk,
        seen.events
    );
    assert!(
        seen.disk_cut < seen.disk_source_len,
        "the image kept the second location's whole log ({} of {} bytes) although \
         Y was appended after its last sync",
        seen.disk_cut,
        seen.disk_source_len
    );
    assert!(
        !seen.in_disk.contains("yankee_disk"),
        "Y's row was never synced, yet it survived the crash image: the cut kept \
         bytes written after the last sync; syncs: {:#?}",
        seen.events
    );
}

// ---------------------------------------------------------------------------
// The suspected gap.
// ---------------------------------------------------------------------------

/// Variant 1: session C's `sync_durable` has synced the second location and not
/// yet the root when A writes both rows and stages its outcome. C's root sync
/// then covers everything issued to the root log, A's outcome included. Before
/// the fix A's second-location row was still only in the page cache. The same
/// split is in every full `sync_durable` and in the log writer's round.
#[tokio::test(flavor = "multi_thread")]
async fn another_sessions_root_sync_cannot_commit_a_half_durable_transaction() {
    let seen = run_undisturbed("variant_1", || async {
        let fx = Fixture::new().await;
        let db = fx.db.clone();

        // C has its own write on the second location and its outcome staged
        // in the root log.
        let tx_c = db.transactions.begin();
        write_in(&db, tx_c, &insert_sql(DISK_TABLE, 3, "charlie_disk")).await;
        db.commit_written_transaction(tx_c).unwrap();
        db.transactions.flush_deferred().unwrap();

        // C's sync_durable, held after its non-root half.
        let (paused_tx, paused_rx) = std::sync::mpsc::channel::<()>();
        let (resume_tx, resume_rx) = std::sync::mpsc::channel::<()>();
        let db_c = db.clone();
        let c = std::thread::spawn(move || {
            super::test_hooks::AFTER_POOL_SYNC.with(|h| {
                *h.borrow_mut() = Some(Box::new(move || {
                    paused_tx.send(()).unwrap();
                    resume_rx
                        .recv_timeout(Duration::from_secs(60))
                        .expect("the scenario never resumed C");
                }))
            });
            let r = db_c.sync_durable();
            super::test_hooks::AFTER_POOL_SYNC.with(|h| *h.borrow_mut() = None);
            r
        });
        let c_thread = c.thread().id();
        paused_rx
            .recv_timeout(Duration::from_secs(60))
            .expect("C never reached the point between its two syncs");

        // A, entirely inside C's window, up to staging its outcome.
        let tx_a = a_writes_both(&db).await;
        db.commit_written_transaction(tx_a).unwrap();
        db.transactions.flush_deferred().unwrap();

        resume_tx.send(()).unwrap();
        c.join()
            .expect("C panicked")
            .expect("C's sync_durable failed");
        // Power is cut here: A has not reached its own sync_durable.
        drop(db);

        (
            fx.crash_and_recover(CANDIDATES).await,
            vec![std::thread::current().id(), c_thread],
        )
    })
    .await;
    // C returned from sync_durable, so its row must be there whatever A's
    // outcome: this is what proves the image kept the second location's synced
    // prefix.
    assert!(
        seen.in_disk.contains("charlie_disk"),
        "C's acknowledged row is missing from the image: {:?}; syncs: {:#?}",
        seen.in_disk,
        seen.events
    );
    assert_all_or_nothing("variant_1", &seen);
}

/// Variant 2: A's outcome is recorded in memory at the commit, before its own
/// root sync. Session B then rolls back a write — pgwire's ROLLBACK and the
/// executor's failed-statement path both call `transactions.flush()`, which
/// stages the whole dirty range of the outcome bitmap, A's bit included, and
/// syncs the root log only. Before the fix no second-location sync happened at
/// all. This is also A under `synchronous_commit = off`, which never takes its
/// own sync.
#[tokio::test(flavor = "multi_thread")]
async fn a_root_only_outcome_flush_cannot_commit_a_half_durable_transaction() {
    let seen = run_undisturbed("variant_2", || async {
        let fx = Fixture::new().await;
        let db = fx.db.clone();

        // A: rows in both directories, and the commit. Nothing staged.
        let tx_a = a_writes_both(&db).await;
        db.commit_written_transaction(tx_a).unwrap();

        // B: a write rolled back, as pgwire's ROLLBACK runs it.
        let tx_b = db.transactions.begin();
        write_in(&db, tx_b, &insert_sql(ROOT_TABLE, 4, "bravo_root")).await;
        let wrote = db.transactions.wrote_family(tx_b);
        assert!(wrote, "B's insert did not register as a write");
        db.abort_transaction(tx_b);
        db.transactions.flush().unwrap();
        // Power is cut here: A has not staged or synced anything itself.
        drop(db);

        (
            fx.crash_and_recover(CANDIDATES).await,
            vec![std::thread::current().id()],
        )
    })
    .await;
    assert!(
        !seen.in_root.contains("bravo_root"),
        "B rolled back, yet its row is visible after the crash"
    );
    assert_all_or_nothing("variant_2", &seen);
}

// ---------------------------------------------------------------------------
// The call site: the executor commits through the fixed path.
// ---------------------------------------------------------------------------

/// Log syncs among `events`: those issued from thread `me`, and the rest.
fn log_syncs(events: Vec<SyncEvent>, me: ThreadId) -> (Vec<SyncEvent>, Vec<SyncEvent>) {
    events
        .into_iter()
        .filter(|e| e.tag == "wal.log")
        .partition(|e| e.thread == me)
}

/// Some sync among `events`, whoever issued it, covered the whole of `wal` as
/// it is now: what a statement that wrote there and committed must leave.
fn assert_log_covered(wal: &Path, events: &[SyncEvent]) {
    let len = std::fs::metadata(wal).map(|m| m.len()).unwrap_or(0);
    let synced = events
        .iter()
        .filter(|e| e.tag == "wal.log" && e.path.as_path() == wal)
        .map(|e| e.len)
        .max();
    assert!(
        synced.is_some_and(|s| s >= len),
        "the statement committed with the end of {} not durable: synced through \
         {synced:?} of {len} bytes",
        wal.display()
    );
}

/// An autocommit statement syncs the second location before it commits, also
/// with `wait = false`. Nothing else on this thread syncs that log then (the
/// root's sync is skipped, which is checked), so the sync seen here can only
/// be `commit_written_transaction`'s; the variants above test what it buys.
///
/// The log writer syncs every log in the process on a timer, and a round
/// landing between the row and the commit leaves the commit's sync nothing to
/// do — so it records nothing. Such an attempt proves nothing either way and
/// is rerun rather than read as a missing sync.
#[tokio::test(flavor = "multi_thread")]
async fn the_executor_syncs_other_locations_before_committing_without_waiting() {
    let fx = Fixture::new().await;
    let me = std::thread::current().id();
    let disk_wal = fx.disk.join(DEFAULT_WAL_NAME);
    let mut confirmed = false;
    for attempt in 1..=MAX_ATTEMPTS {
        let _ = fx.root_watch.take_events();
        let _ = fx.disk_watch.take_events();
        let sql = insert_sql(DISK_TABLE, 100 + attempt as i32, "delta_disk");
        let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(&sql)
            .unwrap_or_else(|| panic!("could not parse {sql}"));
        let out =
            crate::core::rql::executor::execute_with_sync(purpose, fx.db.clone(), false).await;
        assert!(
            !out.is_empty() && out[0] != 3,
            "{sql} failed: {}",
            String::from_utf8_lossy(&out)
        );

        let (root_here, _) = log_syncs(fx.root_watch.take_events(), me);
        assert_eq!(
            root_here.len(),
            0,
            "wait = false still synced the root log, so this test cannot tell the \
             pre-commit sync from the old post-commit one"
        );
        let disk = fx.disk_watch.take_events();
        assert_log_covered(&disk_wal, &disk);
        let (disk_here, disk_elsewhere) = log_syncs(disk, me);
        if !disk_here.is_empty() {
            confirmed = true;
            break;
        }
        assert!(
            !disk_elsewhere.is_empty(),
            "the executor committed a write to the second location without syncing \
             that location's log first; any root flush can now make the outcome \
             durable ahead of the row"
        );
        eprintln!("executor call site: attempt {attempt} disturbed by a background sync");
    }
    if !confirmed {
        // Every attempt had its sync taken by the log writer, and each still
        // committed only a durable row. Inconclusive, not wrong.
        eprintln!("executor call site: every attempt was disturbed by a background sync");
    }
    let _ = fx.crash_and_recover(CANDIDATES).await;
}

// ---------------------------------------------------------------------------
// A failed pre-commit sync commits nothing.
// ---------------------------------------------------------------------------

/// Arms `FAIL_POOL_SYNC` for `f` on this thread and reports whether it fired.
async fn with_failed_pool_sync<T, Fut: std::future::Future<Output = T>>(f: Fut) -> (T, bool) {
    super::test_hooks::FAIL_POOL_SYNC.with(|h| h.set(true));
    let out = f.await;
    let fired = !super::test_hooks::FAIL_POOL_SYNC.with(|h| h.replace(false));
    (out, fired)
}

/// A's rows on the second location cannot be made durable, so the commit
/// fails — and A must not have been committed when it did. In the other order
/// A would already be visible to every snapshot and its outcome stageable by
/// anyone's root flush, the gap this method exists to close. Nor may a crash
/// bring A back.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_pre_commit_sync_leaves_the_transaction_uncommitted() {
    let fx = Fixture::new().await;
    let db = fx.db.clone();
    let tx = a_writes_both(&db).await;

    let (result, fired) = with_failed_pool_sync(async { db.commit_written_transaction(tx) }).await;
    assert!(
        fired,
        "no non-root log was synced, so the commit never tried"
    );
    assert!(
        result.is_err(),
        "the commit succeeded although its rows on the second location were not durable"
    );
    assert_ne!(
        db.transactions.outcome(tx),
        TxOutcome::Committed,
        "the transaction was committed before its rows on the second location were \
         durable: any root flush could now put the outcome on disk ahead of them"
    );

    // What every caller does next.
    db.abort_transaction(tx);
    db.transactions.flush().unwrap();
    let live_root = labels(&db, ROOT_TABLE, CANDIDATES).await;
    let live_disk = labels(&db, DISK_TABLE, CANDIDATES).await;
    assert!(
        !live_root.contains("alpha_root") && !live_disk.contains("alpha_disk"),
        "the failed commit left rows visible: root {live_root:?}, disk {live_disk:?}"
    );
    drop(db);

    let seen = fx.crash_and_recover(CANDIDATES).await;
    assert!(
        !seen.in_root.contains("alpha_root") && !seen.in_disk.contains("alpha_disk"),
        "the failed commit's rows came back after a crash: root {:?}, disk {:?}",
        seen.in_root,
        seen.in_disk
    );
}

/// The executor's side of the same failure: the client is told the commit
/// failed, and the row is invisible.
#[tokio::test(flavor = "multi_thread")]
async fn the_executor_reports_a_failed_pre_commit_sync() {
    let fx = Fixture::new().await;
    let sql = insert_sql(DISK_TABLE, 9, "echo_disk");
    let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(&sql)
        .unwrap_or_else(|| panic!("could not parse {sql}"));
    let (out, fired) = with_failed_pool_sync(crate::core::rql::executor::execute_with_sync(
        purpose,
        fx.db.clone(),
        true,
    ))
    .await;
    assert!(
        fired,
        "no non-root log was synced, so the commit never tried"
    );
    let text = String::from_utf8_lossy(&out).into_owned();
    assert!(
        out.first() == Some(&3)
            && text.contains("could not commit: the rows written could not be made durable"),
        "the statement did not report the failed commit: {text}"
    );
    let live = labels(&fx.db, DISK_TABLE, &["echo_disk"]).await;
    assert!(
        live.is_empty(),
        "a statement reported as failed left its row visible"
    );
    let _ = fx.crash_and_recover(CANDIDATES).await;
}

// ---------------------------------------------------------------------------
// Replication apply commits through the same path.
// ---------------------------------------------------------------------------

/// Turns replication on for `db` with nothing on disk, so the image sees no
/// new files.
fn replicate(db: &Arc<Database>) {
    db.replication
        .set_config(crate::replication::config::ReplicationConfig {
            node_id: 1,
            ..Default::default()
        })
        .unwrap();
    db.replication.open_storage(None).unwrap();
    db.replication.set_active(true);
}

/// A peer's transaction writing one row in each directory.
fn two_location_record() -> crate::replication::protocol::TxnRecord {
    crate::replication::protocol::TxnRecord {
        origin: 2,
        seq: 1,
        commit_hlc: 1000,
        flags: 0,
        statements: vec![
            insert_sql(DISK_TABLE, 2, "alpha_disk"),
            insert_sql(ROOT_TABLE, 2, "alpha_root"),
        ],
        tables: vec![DISK_TABLE.to_string(), ROOT_TABLE.to_string()],
    }
}

/// An applied record is whole after a power cut. The apply used to commit
/// bare and flush the root log only, which made the outcome durable with the
/// second-location row still in the page cache — and with the position
/// already recorded, the record was never applied again.
#[tokio::test(flavor = "multi_thread")]
async fn replication_apply_commits_a_two_location_record_whole() {
    let seen = run_undisturbed("replication_apply", || async {
        let fx = Fixture::new().await;
        let db = fx.db.clone();
        replicate(&db);
        let replication = db.replication.clone();
        let (applied, refused) = crate::replication::server::apply_pulled(
            &db,
            &replication,
            vec![two_location_record()],
        )
        .await;
        assert_eq!((applied, refused), (1, 0), "the record was not applied");
        assert_eq!(replication.applied().high_water(2), 1);
        drop(replication);
        drop(db);
        (
            fx.crash_and_recover(CANDIDATES).await,
            vec![std::thread::current().id()],
        )
    })
    .await;
    assert!(
        seen.in_root.contains("alpha_root") && seen.in_disk.contains("alpha_disk"),
        "an applied, recorded transaction is not whole after the crash: root {:?}, \
         disk {:?}; syncs: {:#?}",
        seen.in_root,
        seen.in_disk,
        seen.events
    );
}

/// An apply whose rows cannot be made durable is refused, applies nothing,
/// and leaves the position where it was, so the record arrives again and is
/// applied then rather than skipped for good.
#[tokio::test(flavor = "multi_thread")]
async fn replication_apply_does_not_record_what_it_could_not_make_durable() {
    let fx = Fixture::new().await;
    let db = fx.db.clone();
    replicate(&db);
    let replication = db.replication.clone();

    let ((applied, refused), fired) = with_failed_pool_sync(
        crate::replication::server::apply_pulled(&db, &replication, vec![two_location_record()]),
    )
    .await;
    assert!(
        fired,
        "no non-root log was synced, so the commit never tried"
    );
    assert_eq!(
        (applied, refused),
        (0, 1),
        "a record whose rows were not durable was reported as applied"
    );
    assert_eq!(
        replication.applied().high_water(2),
        0,
        "the position moved past a record that was not applied, so it is never \
         applied again"
    );
    let live_root = labels(&db, ROOT_TABLE, CANDIDATES).await;
    let live_disk = labels(&db, DISK_TABLE, CANDIDATES).await;
    assert!(
        !live_root.contains("alpha_root") && !live_disk.contains("alpha_disk"),
        "the refused record left rows visible: root {live_root:?}, disk {live_disk:?}"
    );

    // It comes again, and this time it lands.
    let (applied, refused) =
        crate::replication::server::apply_pulled(&db, &replication, vec![two_location_record()])
            .await;
    assert_eq!(
        (applied, refused),
        (1, 0),
        "the resent record was not applied"
    );
    assert_eq!(replication.applied().high_water(2), 1);
    let live_root = labels(&db, ROOT_TABLE, CANDIDATES).await;
    let live_disk = labels(&db, DISK_TABLE, CANDIDATES).await;
    assert!(
        live_root.contains("alpha_root") && live_disk.contains("alpha_disk"),
        "the resent record did not apply whole: root {live_root:?}, disk {live_disk:?}"
    );
    drop(replication);
    drop(db);
    let _ = fx.crash_and_recover(CANDIDATES).await;
}
