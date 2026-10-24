//! Does a table dropped (or renamed away) and created again under the same
//! name get the old table's records replayed into the new table's files?
//!
//! A log record names its file by `file_id_for_name(file name)`, so two
//! incarnations of one table name share every file id and every path. Nothing
//! on the drop path checkpoints, so the old table's committed records stay in
//! the log after its files are deleted, and a replay used to write them into
//! whatever file holds that name by then: the new table's. Wherever the new
//! table had written less than the old one, the old bytes came back, which
//! resurrected dropped rows. Replay runs at every open, not only after a
//! crash, so a clean restart did it too. A rename deletes the old name's files
//! the same way, and an `ALTER TABLE … ADD COLUMN` on a populated table drops
//! and recreates it, so both took the same path.
//!
//! Ported from feat/partitioning 4dd3be0 ("void dropped logs"), with the clean
//! reopen cases and a seeded page model added.
//!
//! No process is killed (a killed process leaves the page cache, which is not a
//! power cut). Crash images are built from the live directory:
//! - the log cut at the longest length any `fdatasync` of it was issued at, as
//!   `sync_audit` recorded it (see `cross_dir_commit_tests` on feat/partitioning),
//!   plus, under a seed, some of the unsynced tail, since writeback can persist
//!   any of it;
//! - seed 0: every data file as it is now (the page cache survived whole);
//! - seed 1, the log only: every file that existed, fsynced, when the old
//!   table's rows were durable as it was then, and every file created since
//!   empty, so whatever the image holds past the baseline came from replay;
//! - other seeds: every data file that existed at that point keeps each 4 KiB
//!   page either as it is now or as it was then, by the seed, and a file
//!   created since (a new inode, which every recreated table file is) keeps
//!   each page or loses it, and may be cut short. Writeback may have persisted
//!   any unsynced page, or none. A new table's rows then have to come from
//!   replaying its own records, so a barrier that voided those too fails here;
//! - a file deleted since the baseline comes back, by the seed, unless a sync
//!   of its directory was issued after the deletion: an unlink is a directory
//!   entry, durable only with the directory. Back with its baseline bytes when
//!   no directory sync followed its first deletion, else empty (a sweep
//!   creates a missing file before deleting it, and only that empty one can
//!   return).
//!
//! Each scenario also checks the image's log still begins with the bytes it
//! held once the old table's rows were durable: a checkpoint in between would
//! have discarded them, and the scenario would then pass without testing
//! anything.

use std::collections::HashMap;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

use rclite::Arc;

use crate::core::database::Database;
use crate::core::db_type::DbType;
use crate::core::rql::lexer_ct::{CreateColumnData, CreateTable};
use crate::core::sync_audit::{self, SyncEvent};
use crate::core::wal::DEFAULT_WAL_NAME;

const TABLE: &str = "reborn";

/// Ceiling on what one scenario may put on disk, directory and images together.
const MAX_SCENARIO_BYTES: u64 = 32 * 1024 * 1024;

/// Seeds per crash scenario: seed 0 keeps every page, the rest mix.
const SEEDS: u64 = 6;

const PAGE: usize = 4096;

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

/// Every file a table of `name` owns.
fn table_file_names(name: &str) -> impl Iterator<Item = String> + '_ {
    super::RENAMED_SUFFIXES
        .iter()
        .map(move |suffix| format!("{}{}", name, suffix))
}

async fn open(root: &Path) -> Arc<Database> {
    Arc::new(Database::new_named(&path_str(root), false, "droprecreate").await)
}

/// Runs one autocommit statement, waiting for its sync whatever another test
/// has set the server default to. Panics on failure: a scenario whose write
/// did not happen tests nothing.
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

async fn insert(db: &Arc<Database>, id: i32, label: &str) {
    insert_into(db, TABLE, id, label).await;
}

async fn insert_into(db: &Arc<Database>, table: &str, id: i32, label: &str) {
    run(
        db,
        &format!("INSERT INTO {table} (id, label) VALUES ({id}, '{label}')"),
    )
    .await;
}

/// Which of `candidates` a fresh snapshot of the table shows, each as many
/// times as it appears: a replayed row can come back beside its own copy.
async fn labels(db: &Arc<Database>, candidates: &[String]) -> Vec<String> {
    labels_of(db, TABLE, candidates).await
}

/// [`labels`] for any table.
async fn labels_of(db: &Arc<Database>, table: &str, candidates: &[String]) -> Vec<String> {
    let out = run(db, &format!("SELECT label FROM {table}")).await;
    let text = String::from_utf8_lossy(&out);
    let mut seen: Vec<String> = candidates
        .iter()
        .flat_map(|c| std::iter::repeat_n(c.clone(), text.matches(c.as_str()).count()))
        .collect();
    seen.sort();
    seen
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

fn names(prefix: &str, n: usize) -> Vec<String> {
    (0..n).map(|i| format!("{prefix}_{i:02}")).collect()
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

/// A file as the baseline saw it. The handle stays open for the scenario's
/// life, so its inode cannot be freed and handed to a file created later
/// under the same name (ext4 reuses inode numbers at once; tmpfs does not).
struct Baseline {
    _held: std::fs::File,
    ino: u64,
    bytes: Vec<u8>,
}

/// Every file of `dir`, fsynced, with its inode and bytes: what the disk is
/// sure to hold from here on until the file is written again.
fn durable_baseline(dir: &Path) -> HashMap<PathBuf, Baseline> {
    let mut out = HashMap::new();
    for path in regular_files(dir) {
        let f = std::fs::File::options().read(true).write(true).open(&path).unwrap();
        f.sync_all().unwrap();
        let ino = f.metadata().unwrap().ino();
        let bytes = std::fs::read(&path).unwrap();
        out.insert(path.clone(), Baseline { _held: f, ino, bytes });
    }
    if let Ok(d) = std::fs::File::open(dir) {
        let _ = d.sync_all();
    }
    out
}

/// The baseline's bytes for `path` when the file there now is the one the
/// baseline saw, `None` for a file created since.
fn baseline_of<'a>(path: &Path, baseline: &'a HashMap<PathBuf, Baseline>) -> Option<&'a [u8]> {
    let ino = std::fs::metadata(path).unwrap().ino();
    baseline
        .get(path)
        .filter(|b| b.ino == ino)
        .map(|b| b.bytes.as_slice())
}

/// How much of the file now at `path` a data-file sync made durable: the
/// longest length a sync of it was issued at since the baseline, counting
/// only syncs after its last deletion, which were of another file.
fn synced_prefix(path: &Path, events: &[SyncEvent]) -> usize {
    let since = events
        .iter()
        .rposition(|e| e.tag == "unlink" && e.path == path)
        .map_or(0, |at| at + 1);
    events[since..]
        .iter()
        .filter(|e| {
            e.path == path
                && matches!(e.tag, "file.data" | "wal.checkpoint.data_file" | "wal.recover.data_file")
        })
        .map(|e| e.len as usize)
        .max()
        .unwrap_or(0)
}

/// The bytes a file created since the baseline may hold after a power cut
/// under `rng`: its synced prefix, then each page kept or lost (zeros), and
/// the file perhaps cut back to a page boundary past that prefix.
fn lost_pages(current: &[u8], synced: usize, rng: &mut Rng) -> Vec<u8> {
    let synced = synced.min(current.len());
    let pages = (current.len() - synced).div_ceil(PAGE) as u64;
    let keep_len = if rng.below(2) == 0 {
        current.len()
    } else {
        (synced + rng.below(pages + 1) as usize * PAGE).min(current.len())
    };
    let mut out = current[..keep_len].to_vec();
    for page in out[synced..].chunks_mut(PAGE) {
        if rng.below(2) == 0 {
            page.fill(0);
        }
    }
    out
}

/// What a power cut may leave of a baseline file that is gone now.
enum Gone {
    /// A directory sync followed its last deletion: gone for good.
    Durably,
    /// A directory sync followed its first deletion but not its last: only a
    /// file created and deleted again since can come back, empty.
    MaybeEmpty,
    /// No directory sync followed its first deletion: it may come back whole.
    MaybeWhole,
}

fn gone(path: &Path, events: &[SyncEvent]) -> Gone {
    let dir = path.parent().unwrap();
    let unlinks: Vec<usize> = events
        .iter()
        .enumerate()
        .filter(|(_, e)| e.tag == "unlink" && e.path == path)
        .map(|(i, _)| i)
        .collect();
    let (Some(&first), Some(&last)) = (unlinks.first(), unlinks.last()) else {
        // Deleted some other way, unrecorded: not modelled.
        return Gone::Durably;
    };
    let dir_synced_after = |at: usize| {
        events[at..]
            .iter()
            .any(|e| e.tag == "dir" && e.path.parent() == Some(dir))
    };
    if dir_synced_after(last) {
        Gone::Durably
    } else if dir_synced_after(first) {
        Gone::MaybeEmpty
    } else {
        Gone::MaybeWhole
    }
}

/// The bytes a data file the baseline saw may hold after a power cut under
/// `rng`: past its synced prefix, each page as it is now or as it was at the
/// baseline (every page, when `all_old`).
fn mixed_pages(base: &[u8], current: &[u8], synced: usize, all_old: bool, rng: &mut Rng) -> Vec<u8> {
    let mut out = current.to_vec();
    for (i, page) in out.chunks_mut(PAGE).enumerate() {
        if !all_old && rng.below(2) == 0 {
            continue;
        }
        let start = i * PAGE;
        for (j, b) in page.iter_mut().enumerate() {
            if start + j >= synced {
                *b = base.get(start + j).copied().unwrap_or(0);
            }
        }
    }
    out
}

/// How far the log reached durably: the longest length any of its syncs was
/// issued at, never past what the file holds.
fn synced_log_len(events: &[SyncEvent], wal_path: &Path, current: usize) -> usize {
    events
        .iter()
        .filter(|e| e.tag == "wal.log" && e.path == wal_path)
        .map(|e| e.len)
        .max()
        .unwrap_or(0)
        .min(current as u64) as usize
}

/// A database directory being watched for log syncs.
struct Scenario {
    base: tempfile::TempDir,
    root: PathBuf,
    db: Arc<Database>,
    watch: sync_audit::Watch,
    /// The log once the old table's rows were durable.
    old_log: Vec<u8>,
    /// Every file, fsynced, at that point.
    baseline: HashMap<PathBuf, Baseline>,
}

impl Scenario {
    /// A table of `old` durable rows labelled `old_<n>`.
    async fn new(old: usize) -> Scenario {
        let base = tempfile::tempdir().unwrap();
        let root = base.path().join("root");
        std::fs::create_dir_all(&root).unwrap();
        let watch = sync_audit::watch(&root);
        let db = open(&root).await;
        assert!(
            crate::core::wal::wal_for(&path_str(&root)).is_some(),
            "the database has no log, so there is nothing to replay"
        );
        db.add_table(create_statement(TABLE)).await.unwrap();
        for n in 0..old {
            insert(&db, n as i32, &format!("old_{n:02}")).await;
        }
        db.sync_durable().unwrap();
        let old_log = std::fs::read(root.join(DEFAULT_WAL_NAME)).unwrap();
        let baseline = durable_baseline(&root);
        // What a crash image needs from here on is what happened since: the
        // baseline is durable whole.
        let _ = watch.take_events();
        Scenario {
            base,
            root,
            db,
            watch,
            old_log,
            baseline,
        }
    }

    /// Writes the crash image for `seed` into a fresh directory.
    fn image(&self, events: &[SyncEvent], seed: u64) -> PathBuf {
        let mut rng = Rng::new(seed);
        let wal_path = self.root.join(DEFAULT_WAL_NAME);
        let current = std::fs::read(&wal_path).unwrap();
        // The baseline synced the log too, before its events were dropped.
        let synced = synced_log_len(events, &wal_path, current.len()).max(self.old_log.len());
        let cut = if seed == 0 {
            synced
        } else {
            synced + rng.below((current.len() - synced) as u64 + 1) as usize
        };
        let image_log = &current[..cut];
        assert!(
            image_log.starts_with(&self.old_log),
            "the image's log no longer holds the old table's records ({} of {} bytes \
             durable, {} before the drop): a checkpoint or truncation ran inside the \
             scenario, so it tests nothing",
            cut,
            current.len(),
            self.old_log.len()
        );

        let img = self.base.path().join(format!("img_{seed}"));
        std::fs::create_dir_all(&img).unwrap();
        for path in regular_files(&self.root) {
            let name = path.file_name().unwrap();
            if name == DEFAULT_WAL_NAME {
                std::fs::write(img.join(name), image_log).unwrap();
            } else {
                let now = std::fs::read(&path).unwrap();
                let synced = synced_prefix(&path, events);
                let bytes = match (seed, baseline_of(&path, &self.baseline)) {
                    (0, _) => now,
                    (1, Some(base)) => mixed_pages(base, &now, synced, true, &mut rng),
                    (1, None) => now[..synced.min(now.len())].to_vec(),
                    (_, Some(base)) => mixed_pages(base, &now, synced, false, &mut rng),
                    (_, None) => lost_pages(&now, synced, &mut rng),
                };
                std::fs::write(img.join(name), bytes).unwrap();
            }
        }
        // Deletions whose directory entries were not synced may be undone.
        // Seed 0 keeps the page cache's view, deletions included.
        if seed != 0 {
            let mut gone_paths: Vec<&PathBuf> =
                self.baseline.keys().filter(|p| !p.exists()).collect();
            gone_paths.sort();
            for path in gone_paths {
                let back = match gone(path, events) {
                    Gone::Durably => None,
                    Gone::MaybeEmpty => Some(Vec::new()),
                    Gone::MaybeWhole => Some(self.baseline[path].bytes.clone()),
                };
                if let Some(bytes) = back
                    && (seed == 1 || rng.below(2) == 0)
                {
                    std::fs::write(img.join(path.file_name().unwrap()), bytes).unwrap();
                }
            }
        }
        // `db_storage.cfg` names absolute paths, which would send the image
        // back to the source directory. Only a pooled build writes one.
        #[cfg(feature = "storage_pools")]
        {
            let pool = img.join(crate::core::storage::POOL_FILE);
            if let Ok(text) = std::fs::read_to_string(&pool) {
                std::fs::write(&pool, text.replace(&path_str(&self.root), &path_str(&img))).unwrap();
            }
        }
        img
    }

    /// Cuts power now, opens each seed's image of what the disk would hold,
    /// and returns which of `candidates` each shows in `table`.
    async fn crash_and_recover_all(
        self,
        table: &str,
        candidates: &[String],
    ) -> Vec<(u64, Vec<String>)> {
        let events: Vec<SyncEvent> = self.watch.take_events();
        let images: Vec<(u64, PathBuf)> = (0..SEEDS).map(|s| (s, self.image(&events, s))).collect();
        let mut total = dir_bytes(&self.root);
        for (_, img) in &images {
            total += dir_bytes(img);
        }
        assert!(
            total < MAX_SCENARIO_BYTES,
            "the scenario wrote {total} bytes, past its bound"
        );

        // The original goes before an image opens, so nothing an image
        // answers can have come from it.
        let Scenario {
            base,
            root,
            db,
            watch,
            ..
        } = self;
        drop(watch);
        drop(db);
        crate::core::wal::remove_for(&path_str(&root));
        std::fs::remove_dir_all(&root).unwrap();

        let mut out = Vec::new();
        for (seed, img) in images {
            let db = open(&img).await;
            let seen = if db.is_partitioned(table) {
                // Read through the API, partition by partition, below SQL.
                partitioned::labels_of(&db, table, candidates).await
            } else if db.has_table(table) {
                labels_of(&db, table, candidates).await
            } else {
                vec![format!("<no table {table}>")]
            };
            drop(db);
            crate::core::wal::remove_for(&path_str(&img));
            std::fs::remove_dir_all(&img).unwrap();
            out.push((seed, seen));
        }
        drop(base);
        out
    }

    /// Closes the database and opens the same directory again, as a clean
    /// restart does: the log is replayed whether or not anything crashed.
    async fn reopen(self) -> (tempfile::TempDir, PathBuf, Arc<Database>) {
        let Scenario {
            base,
            root,
            db,
            watch,
            old_log,
            ..
        } = self;
        drop(watch);
        drop(db);
        crate::core::wal::remove_for(&path_str(&root));
        let log = std::fs::read(root.join(DEFAULT_WAL_NAME)).unwrap();
        assert!(
            log.starts_with(&old_log),
            "the log no longer holds the old table's records ({} bytes, {} before the \
             drop): a checkpoint or truncation ran inside the scenario, so it tests nothing",
            log.len(),
            old_log.len()
        );
        let db = open(&root).await;
        (base, root, db)
    }
}

fn close(root: &Path, db: Arc<Database>) {
    drop(db);
    crate::core::wal::remove_for(&path_str(root));
}

fn assert_every_seed(seen: &[(u64, Vec<String>)], expected: &[String], what: &str) {
    for (seed, labels) in seen {
        assert_eq!(labels, expected, "seed {seed}: {what}");
    }
}

// ---------------------------------------------------------------------------
// Clean restarts: no crash at all. Bugs (e) and (f).
// ---------------------------------------------------------------------------

/// (e) Dropped, created again with fewer rows, and the database closed and
/// opened again: the new table holds exactly its own rows.
#[tokio::test(flavor = "multi_thread")]
async fn a_recreated_table_holds_only_its_rows_after_a_clean_reopen() {
    let sc = Scenario::new(12).await;
    let db = sc.db.clone();
    assert!(db.drop_table(TABLE).await.unwrap(), "the drop found no table");
    db.add_table(create_statement(TABLE)).await.unwrap();
    insert(&db, 100, "new_00").await;
    let mut candidates = names("old", 12);
    candidates.extend(names("new", 1));
    assert_eq!(labels(&db, &candidates).await, names("new", 1), "the live table is wrong");
    drop(db);

    let (_base, root, db) = sc.reopen().await;
    assert_eq!(
        labels(&db, &candidates).await,
        names("new", 1),
        "after a clean reopen the recreated table shows rows the dropped one held"
    );
    close(&root, db);
}

/// (e) The same, recreated twice before the restart: only the last
/// incarnation's rows, so the latest void is the one that counts.
#[tokio::test(flavor = "multi_thread")]
async fn a_table_recreated_twice_holds_only_the_last_rows_after_a_clean_reopen() {
    let sc = Scenario::new(10).await;
    let db = sc.db.clone();
    assert!(db.drop_table(TABLE).await.unwrap());
    db.add_table(create_statement(TABLE)).await.unwrap();
    for n in 0..6 {
        insert(&db, 50 + n, &format!("mid_{n:02}")).await;
    }
    assert!(db.drop_table(TABLE).await.unwrap());
    db.add_table(create_statement(TABLE)).await.unwrap();
    insert(&db, 100, "new_00").await;
    let mut candidates = names("old", 10);
    candidates.extend(names("mid", 6));
    candidates.extend(names("new", 1));
    drop(db);

    let (_base, root, db) = sc.reopen().await;
    assert_eq!(
        labels(&db, &candidates).await,
        names("new", 1),
        "after a clean reopen a twice-recreated table shows an earlier incarnation's rows"
    );
    close(&root, db);
}

/// (f) `RENAME src TO dst; CREATE TABLE src`, then a clean restart: `src`
/// holds only its own rows and `dst` every one of the renamed table's.
#[tokio::test(flavor = "multi_thread")]
async fn a_table_created_under_a_renamed_tables_old_name_holds_only_its_rows_after_a_clean_reopen()
{
    let sc = Scenario::new(8).await;
    let db = sc.db.clone();
    db.rename_table(TABLE, "moved_away").await.unwrap();
    db.add_table(create_statement(TABLE)).await.unwrap();
    insert(&db, 100, "new_00").await;
    let mut candidates = names("old", 8);
    candidates.extend(names("new", 1));
    assert_eq!(labels(&db, &candidates).await, names("new", 1), "the live table is wrong");
    drop(db);

    let (_base, root, db) = sc.reopen().await;
    assert_eq!(
        labels(&db, &candidates).await,
        names("new", 1),
        "after a clean reopen the table under the old name shows the renamed table's rows"
    );
    assert_eq!(
        labels_of(&db, "moved_away", &candidates).await,
        names("old", 8),
        "after a clean reopen the renamed table lost rows or took the new table's"
    );
    close(&root, db);
}

/// (f) A rename and back: `src -> dst`, `dst -> src`, nothing created in
/// between. The table under its first name again holds exactly its rows: the
/// first incarnation's records must not replay over the second's copy.
#[tokio::test(flavor = "multi_thread")]
async fn a_table_renamed_away_and_back_holds_exactly_its_rows_after_a_clean_reopen() {
    let sc = Scenario::new(8).await;
    let db = sc.db.clone();
    run(&db, &format!("DELETE FROM {TABLE} WHERE id < 6")).await;
    db.rename_table(TABLE, "moved_away").await.unwrap();
    insert_into(&db, "moved_away", 100, "new_00").await;
    db.rename_table("moved_away", TABLE).await.unwrap();
    let mut candidates = names("old", 8);
    candidates.extend(names("new", 1));
    let expected: Vec<String> = vec!["new_00".into(), "old_06".into(), "old_07".into()];
    assert_eq!(labels(&db, &candidates).await, expected, "the live table is wrong");
    drop(db);

    let (_base, root, db) = sc.reopen().await;
    assert_eq!(
        labels(&db, &candidates).await,
        expected,
        "after a clean reopen a table renamed away and back does not hold exactly its rows"
    );
    close(&root, db);
}

// ---------------------------------------------------------------------------
// Power cuts.
// ---------------------------------------------------------------------------

/// Dropped and created again under the same name, with fewer rows than the
/// old table had: the new table holds exactly its own rows after a crash.
#[tokio::test(flavor = "multi_thread")]
async fn a_recreated_table_does_not_take_the_dropped_tables_rows() {
    let sc = Scenario::new(12).await;
    let db = sc.db.clone();
    assert!(db.drop_table(TABLE).await.unwrap(), "the drop found no table");
    db.add_table(create_statement(TABLE)).await.unwrap();
    insert(&db, 100, "new_00").await;
    db.sync_durable().unwrap();
    // Before the crash, as a control: the live table is right.
    let mut candidates = names("old", 12);
    candidates.extend(names("new", 1));
    assert_eq!(labels(&db, &candidates).await, names("new", 1));
    drop(db);

    let seen = sc.crash_and_recover_all(TABLE, &candidates).await;
    assert_every_seed(
        &seen,
        &names("new", 1),
        "after the crash the recreated table shows rows the dropped one held",
    );
}

/// Dropped and not created again: the name is free after a crash, rather than
/// held by files a replay recreated from the dropped table's records. Replay
/// resolves ids from the directory listing, so it never could find a deleted
/// file; pinned here because a barrier that resolved them would.
#[tokio::test(flavor = "multi_thread")]
async fn a_dropped_tables_files_do_not_come_back_after_a_crash() {
    let sc = Scenario::new(4).await;
    let db = sc.db.clone();
    assert!(db.drop_table(TABLE).await.unwrap(), "the drop found no table");
    // Anything after the drop, so the log is synced past it.
    db.add_table(create_statement("bystander")).await.unwrap();
    run(&db, "INSERT INTO bystander (id, label) VALUES (1, 'b')").await;
    db.sync_durable().unwrap();
    drop(db);

    let events: Vec<SyncEvent> = sc.watch.take_events();
    let img = sc.image(&events, 0);
    let Scenario {
        base, root, db, watch, ..
    } = sc;
    drop(watch);
    drop(db);
    crate::core::wal::remove_for(&path_str(&root));
    std::fs::remove_dir_all(&root).unwrap();

    let db = open(&img).await;
    let leftovers: Vec<PathBuf> = regular_files(&img)
        .into_iter()
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with(TABLE))
        })
        .collect();
    assert!(
        leftovers.is_empty(),
        "recovery recreated the dropped table's files: {leftovers:?}"
    );
    // And so the name can be used again, empty.
    db.add_table(create_statement(TABLE)).await.unwrap();
    assert!(labels(&db, &names("old", 4)).await.is_empty());
    close(&img, db);
    drop(base);
}

/// `ALTER TABLE … ADD COLUMN` on a populated table drops it and creates it
/// again under the same name. Rows deleted before the rewrite are not carried
/// across, so the new files are shorter than the old ones were. The survivors
/// are the *last* rows: the old pointers past the new file's end are then
/// theirs, live, and a replay that kept them showed each survivor twice.
#[tokio::test(flavor = "multi_thread")]
async fn an_add_column_rewrite_survives_a_crash_with_exactly_its_rows() {
    let sc = Scenario::new(12).await;
    let db = sc.db.clone();
    run(&db, &format!("DELETE FROM {TABLE} WHERE id < 10")).await;
    run(&db, &format!("ALTER TABLE {TABLE} ADD COLUMN extra INT")).await;
    insert(&db, 100, "new_00").await;
    db.sync_durable().unwrap();
    let candidates: Vec<String> = names("old", 12).into_iter().chain(names("new", 1)).collect();
    let expected: Vec<String> = vec!["new_00".into(), "old_10".into(), "old_11".into()];
    assert_eq!(labels(&db, &candidates).await, expected, "the live table is wrong");
    drop(db);

    let seen = sc.crash_and_recover_all(TABLE, &candidates).await;
    assert_every_seed(
        &seen,
        &expected,
        "after the crash the rewritten table does not hold exactly its rows",
    );
}

/// The same rewrite with nothing deleted first: every old row is carried
/// across and widened, so the new files cover every offset the old records
/// name. Coverage of the rewrite under a crash rather than a regression test:
/// this shape replayed correctly even before the barrier.
#[tokio::test(flavor = "multi_thread")]
async fn an_add_column_rewrite_of_every_row_survives_a_crash() {
    let sc = Scenario::new(6).await;
    let db = sc.db.clone();
    run(&db, &format!("ALTER TABLE {TABLE} ADD COLUMN extra INT")).await;
    db.sync_durable().unwrap();
    let candidates = names("old", 6);
    assert_eq!(labels(&db, &candidates).await, candidates, "the live table is wrong");
    drop(db);

    let seen = sc.crash_and_recover_all(TABLE, &candidates).await;
    assert_every_seed(
        &seen,
        &candidates,
        "after the crash the rewritten table does not hold exactly its rows",
    );
}

/// (f) Renamed away and created again under the old name: the rename deletes
/// the old name's files, so the new table under it is the same case as a drop.
#[tokio::test(flavor = "multi_thread")]
async fn a_table_created_under_a_renamed_tables_old_name_holds_only_its_rows() {
    let sc = Scenario::new(8).await;
    let db = sc.db.clone();
    db.rename_table(TABLE, "moved_away").await.unwrap();
    db.add_table(create_statement(TABLE)).await.unwrap();
    insert(&db, 100, "new_00").await;
    db.sync_durable().unwrap();
    let mut candidates = names("old", 8);
    candidates.extend(names("new", 1));
    assert_eq!(labels(&db, &candidates).await, names("new", 1), "the live table is wrong");
    drop(db);

    let seen = sc.crash_and_recover_all(TABLE, &candidates).await;
    assert_every_seed(
        &seen,
        &names("new", 1),
        "after the crash the table under the old name shows the renamed table's rows",
    );
}

/// (f) And the renamed table itself keeps every row through the same crash.
#[tokio::test(flavor = "multi_thread")]
async fn a_renamed_table_keeps_its_rows_after_a_crash() {
    let sc = Scenario::new(8).await;
    let db = sc.db.clone();
    db.rename_table(TABLE, "moved_away").await.unwrap();
    db.add_table(create_statement(TABLE)).await.unwrap();
    insert(&db, 100, "new_00").await;
    db.sync_durable().unwrap();
    let mut candidates = names("old", 8);
    candidates.extend(names("new", 1));
    drop(db);

    let seen = sc.crash_and_recover_all("moved_away", &candidates).await;
    assert_every_seed(
        &seen,
        &names("old", 8),
        "after the crash the renamed table lost rows or took the new table's",
    );
}

// ---------------------------------------------------------------------------
// When the barrier cannot be written.
// ---------------------------------------------------------------------------

/// A drop whose barrier cannot be logged is refused, and the table is left as
/// it was: listed, readable, and still there after a restart. Deleting the
/// files anyway would leave their records live for the next table of the name.
#[tokio::test(flavor = "current_thread")]
async fn a_drop_that_cannot_void_the_log_leaves_the_table_whole() {
    let base = tempfile::tempdir().unwrap();
    let root = base.path().join("root");
    std::fs::create_dir_all(&root).unwrap();
    let db = open(&root).await;
    db.add_table(create_statement(TABLE)).await.unwrap();
    insert(&db, 1, "old_00").await;
    insert(&db, 2, "old_01").await;
    let candidates = names("old", 2);

    crate::core::wal::test_hooks::FAIL_FORGET.with(|f| f.set(true));
    let dropped = db.drop_table(TABLE).await;
    let reached = !crate::core::wal::test_hooks::FAIL_FORGET.with(|f| f.replace(false));
    assert!(reached, "the drop never logged a barrier");
    assert!(dropped.is_err(), "a drop that could not void the log reported success");
    assert!(db.has_table(TABLE), "the refused drop took the table out of the map");
    assert!(root.join(format!("{TABLE}.db")).exists(), "the refused drop deleted files");
    assert_eq!(labels(&db, &candidates).await, candidates);

    close(&root, db);
    let db = open(&root).await;
    assert_eq!(
        labels(&db, &candidates).await,
        candidates,
        "after a restart the table a refused drop left is gone or lost rows"
    );
    // And a drop that can void the log still works.
    assert!(db.drop_table(TABLE).await.unwrap());
    close(&root, db);
}

/// A drop that fails at the catalogue writes no barrier. The barrier comes
/// after the tombstone, never before: written first, a crash between the two
/// would void the records of a table the catalogue still names.
#[tokio::test(flavor = "current_thread")]
async fn a_drop_that_fails_at_the_catalogue_writes_no_barrier() {
    let base = tempfile::tempdir().unwrap();
    let root = base.path().join("root");
    std::fs::create_dir_all(&root).unwrap();
    let db = open(&root).await;
    db.add_table(create_statement(TABLE)).await.unwrap();
    insert(&db, 1, "old_00").await;
    let wal_path = root.join(DEFAULT_WAL_NAME);
    assert_eq!(crate::core::wal::barriers_in(&wal_path), 0);

    super::test_hooks::FAIL_TOMBSTONE.with(|f| f.set(true));
    let dropped = db.drop_table(TABLE).await;
    let reached = !super::test_hooks::FAIL_TOMBSTONE.with(|f| f.replace(false));
    assert!(reached, "the drop never tombstoned the catalogue row");
    assert!(dropped.is_err(), "a drop that failed at the catalogue reported success");
    assert_eq!(
        crate::core::wal::barriers_in(&wal_path),
        0,
        "a barrier went into the log before the catalogue row was tombstoned"
    );
    assert!(db.has_table(TABLE), "the failed drop took the table out of the map");
    assert_eq!(labels(&db, &names("old", 1)).await, names("old", 1));
    // A drop that succeeds writes one barrier per file the table can own.
    assert!(db.drop_table(TABLE).await.unwrap());
    assert_eq!(
        crate::core::wal::barriers_in(&wal_path),
        super::RENAMED_SUFFIXES.len(),
        "a drop did not void every file the table can own"
    );
    close(&root, db);
}

/// A drop whose barrier failed after reaching the log (a failed `fdatasync`:
/// marker written, log poisoned) cannot put the table back: the barrier may
/// be durable, and a restart would open the table with its unflushed records
/// void. It stays dropped, and its files go too, so a later table of the name
/// does not open them. The database's own log is the poisoned one, so not
/// even the tombstone is known durable, and the drop reports that.
/// Opened from both power-loss outcomes: the barrier kept, and the barrier
/// lost (the log cut at its last issued sync).
#[tokio::test(flavor = "current_thread")]
async fn a_drop_whose_barrier_may_be_durable_does_not_bring_the_table_back() {
    let base = tempfile::tempdir().unwrap();
    let root = base.path().join("root");
    std::fs::create_dir_all(&root).unwrap();
    let watch = sync_audit::watch(&root);
    let db = open(&root).await;
    db.add_table(create_statement(TABLE)).await.unwrap();
    for n in 0..6 {
        insert(&db, n, &format!("old_{n:02}")).await;
    }
    db.sync_durable().unwrap();

    crate::core::wal::test_hooks::FAIL_BARRIER_SYNC.with(|f| f.set(true));
    let dropped = db.drop_table(TABLE).await;
    let reached = !crate::core::wal::test_hooks::FAIL_BARRIER_SYNC.with(|f| f.replace(false));
    assert!(reached, "the drop never logged a barrier");
    assert!(dropped.is_err(), "a drop whose catalogue log failed its sync reported success");
    assert!(
        !db.has_table(TABLE),
        "the table came back although its records may be void"
    );
    assert!(
        !root.join(format!("{TABLE}.db")).exists(),
        "the files were kept, and a later table of this name would open them"
    );

    // The two disks a power cut can leave: the barrier lost (the log as far as
    // a sync reached) and kept (the whole file).
    let events = watch.take_events();
    drop(watch);
    let wal_path = root.join(DEFAULT_WAL_NAME);
    let current = std::fs::read(&wal_path).unwrap();
    let synced = synced_log_len(&events, &wal_path, current.len());
    assert!(synced < current.len(), "the barrier was synced, so nothing is undecided");
    let mut images = Vec::new();
    for (tag, cut) in [("lost", synced), ("kept", current.len())] {
        let img = base.path().join(format!("img_{tag}"));
        std::fs::create_dir_all(&img).unwrap();
        for path in regular_files(&root) {
            let name = path.file_name().unwrap();
            if name == DEFAULT_WAL_NAME {
                std::fs::write(img.join(name), &current[..cut]).unwrap();
            } else {
                std::fs::copy(&path, img.join(name)).unwrap();
            }
        }
        images.push((tag, img));
    }
    close(&root, db);

    let candidates: Vec<String> = names("old", 6).into_iter().chain(names("new", 1)).collect();
    for (tag, img) in images {
        let db = open(&img).await;
        assert!(!db.has_table(TABLE), "barrier {tag}: the dropped table is back");
        db.add_table(create_statement(TABLE)).await.unwrap();
        insert(&db, 100, "new_00").await;
        assert_eq!(labels(&db, &candidates).await, names("new", 1), "barrier {tag}: live");
        close(&img, db);
        // And through another restart, which replays whatever the log kept.
        let db = open(&img).await;
        assert_eq!(
            labels(&db, &candidates).await,
            names("new", 1),
            "barrier {tag}: a table created after the failed drop took the dropped rows"
        );
        close(&img, db);
    }
}

/// A rename that cannot void the old name's records keeps the old files, and
/// with them the intent: the name stays refused instead of a later table
/// under it taking the files and records, and the next open finishes the job.
#[tokio::test(flavor = "current_thread")]
async fn a_rename_that_cannot_void_the_log_keeps_the_old_files() {
    let base = tempfile::tempdir().unwrap();
    let root = base.path().join("root");
    std::fs::create_dir_all(&root).unwrap();
    let db = open(&root).await;
    db.add_table(create_statement(TABLE)).await.unwrap();
    insert(&db, 1, "old_00").await;

    crate::core::wal::test_hooks::FAIL_FORGET.with(|f| f.set(true));
    db.rename_table(TABLE, "moved_away").await.unwrap();
    let reached = !crate::core::wal::test_hooks::FAIL_FORGET.with(|f| f.replace(false));
    assert!(reached, "the rename never logged a barrier");
    assert!(
        root.join(format!("{TABLE}.db")).exists(),
        "the old files went with their records still live"
    );
    assert!(
        db.add_table(create_statement(TABLE)).await.is_err(),
        "a table was created under a name whose records are still live"
    );
    db.add_table(create_statement("other")).await.unwrap();
    assert!(
        db.rename_table("other", TABLE).await.is_err(),
        "a table was renamed onto a name whose records are still live"
    );
    assert_eq!(labels_of(&db, "moved_away", &names("old", 1)).await, names("old", 1));
    db.sync_durable().unwrap();
    close(&root, db);

    // The next open finds the intent and discards the old files.
    let db = open(&root).await;
    assert!(
        !root.join(format!("{TABLE}.db")).exists(),
        "the next open left the old files behind"
    );
    assert_eq!(labels_of(&db, "moved_away", &names("old", 1)).await, names("old", 1));
    db.add_table(create_statement(TABLE)).await.unwrap();
    insert(&db, 100, "new_00").await;
    close(&root, db);
    let db = open(&root).await;
    let candidates: Vec<String> = names("old", 1).into_iter().chain(names("new", 1)).collect();
    assert_eq!(
        labels(&db, &candidates).await,
        names("new", 1),
        "the table created once the rename finished shows the renamed table's rows"
    );
    close(&root, db);
}


// ---------------------------------------------------------------------------
// Review round: the rename intent, late writers, directory entries, holds.
// ---------------------------------------------------------------------------

/// Runs the scenario's crash images and checks each with `check`, which gets
/// the open image and its directory.
async fn for_each_image<F>(sc: Scenario, mut check: F)
where
    F: AsyncFnMut(u64, &Arc<Database>, &Path),
{
    let events: Vec<SyncEvent> = sc.watch.take_events();
    let images: Vec<(u64, PathBuf)> = (0..SEEDS).map(|s| (s, sc.image(&events, s))).collect();
    let Scenario {
        base, root, db, watch, ..
    } = sc;
    drop(watch);
    drop(db);
    crate::core::wal::remove_for(&path_str(&root));
    std::fs::remove_dir_all(&root).unwrap();
    for (seed, img) in images {
        let db = open(&img).await;
        check(seed, &db, &img).await;
        close(&img, db);
        std::fs::remove_dir_all(&img).unwrap();
    }
    drop(base);
}

/// The non-empty files under `dir` a table named `name` would take.
fn leftovers(dir: &Path, name: &str) -> Vec<String> {
    table_file_names(name)
        .flat_map(|f| [f.clone(), format!("{f}.tmp")])
        .filter(|f| std::fs::metadata(dir.join(f)).is_ok_and(|m| m.len() > 0))
        .collect()
}

/// A rename after an earlier one with longer names, cut off between its intent
/// and the catalogue flip. The intent file is rewritten at offset 0 by each
/// rename, so the earlier intent's records, left live, replayed under the
/// later one's and garbled it: recovery then acted on a name neither rename
/// had, kept the abandoned copy, and a table created under the copy's name
/// opened it and showed the source table's rows.
async fn interrupted_rename_after_a_longer_one() -> Scenario {
    let sc = Scenario::new(4).await;
    let db = sc.db.clone();
    db.add_table(create_statement("a_rather_long_table_name_one")).await.unwrap();
    db.rename_table("a_rather_long_table_name_one", "a_rather_long_table_name_two")
        .await
        .unwrap();
    db.record_rename_intent(TABLE, "copy_u").await.unwrap();
    db.copy_table_files(TABLE, "copy_u").await.unwrap();
    db.sync_durable().unwrap();
    assert!(!leftovers(&sc.root, "copy_u").is_empty(), "the copy was never made");
    drop(db);
    sc
}

async fn assert_the_copy_was_abandoned(db: &Arc<Database>, dir: &Path, what: &str) {
    assert_eq!(leftovers(dir, "copy_u"), Vec::<String>::new(), "{what}: the abandoned copy was kept");
    assert_eq!(labels(db, &names("old", 4)).await, names("old", 4), "{what}: the source lost rows");
    db.add_table(create_statement("copy_u")).await.unwrap();
    assert!(
        labels_of(db, "copy_u", &names("old", 4)).await.is_empty(),
        "{what}: a table created under the abandoned copy's name shows the source's rows"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_interrupted_rename_after_a_longer_one_is_abandoned_on_a_clean_reopen() {
    let sc = interrupted_rename_after_a_longer_one().await;
    let (_base, root, db) = sc.reopen().await;
    assert_the_copy_was_abandoned(&db, &root, "clean reopen").await;
    close(&root, db);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_interrupted_rename_after_a_longer_one_is_abandoned_after_a_crash() {
    let sc = interrupted_rename_after_a_longer_one().await;
    for_each_image(sc, async |seed, db, img| {
        assert_the_copy_was_abandoned(db, img, &format!("seed {seed}")).await;
    })
    .await;
}

/// An intent holding anything but exactly one intent is refused rather than
/// acted on: stale records showing through a later one, a torn write.
#[test]
fn a_rename_intent_parses_only_whole() {
    let intent = super::encode_intent("t", "u");
    assert_eq!(super::parse_intent(&intent), Some(("t".into(), "u".into())));
    // An older build's intent.
    assert_eq!(super::parse_intent("t\nu"), Some(("t".into(), "u".into())));
    let longer = super::encode_intent("a_rather_long_table_name_one", "a_rather_long_table_name_two");
    let mut overlaid = longer.into_bytes();
    overlaid[..intent.len()].copy_from_slice(intent.as_bytes());
    assert_eq!(super::parse_intent(&String::from_utf8(overlaid).unwrap()), None);
    assert_eq!(super::parse_intent(&intent[..intent.len() - 3]), None);
    assert_eq!(super::parse_intent(&intent.replace('u', "v")), None);
}

/// A statement still holding a dropped table's handle writes after the drop:
/// an append and an in-place write, both before the name is taken again and
/// after. Their records would follow the barrier, so it would not void them,
/// and the next open wrote them into the new table's file. The log refuses
/// them instead.
async fn late_writes_through_a_dropped_handle() -> (Scenario, Vec<String>) {
    use crate::core::storage_providers::traits::StorageIO;
    let sc = Scenario::new(6).await;
    let db = sc.db.clone();
    let held = db.table_for_tests(TABLE).unwrap();
    assert!(db.drop_table(TABLE).await.unwrap());
    held.io_rows().unwrap().append_data(b"GHOST-APPEND-BEFORE", true).await;
    held.io_rows().unwrap().write_data(0, b"GHOST-INPLACE-BEFORE").await;
    db.add_table(create_statement(TABLE)).await.unwrap();
    insert(&db, 100, "new_00").await;
    held.io_rows().unwrap().append_data(b"GHOST-APPEND-AFTER", true).await;
    held.io_rows().unwrap().write_data(0, b"GHOST-INPLACE-AFTER").await;
    drop(held);
    insert(&db, 101, "new_01").await;
    db.sync_durable().unwrap();
    let candidates: Vec<String> = names("old", 6).into_iter().chain(names("new", 2)).collect();
    drop(db);
    (sc, candidates)
}

fn ghosts_in(dir: &Path) -> bool {
    let bytes = std::fs::read(dir.join(format!("{TABLE}.db"))).unwrap_or_default();
    bytes.windows(6).any(|w| w == b"GHOST-")
}

#[tokio::test(flavor = "multi_thread")]
async fn a_dropped_tables_late_writes_do_not_reach_its_successor_on_a_reopen() {
    let (sc, candidates) = late_writes_through_a_dropped_handle().await;
    assert!(!ghosts_in(&sc.root), "a late write reached the recreated table's file live");
    let (_base, root, db) = sc.reopen().await;
    assert!(!ghosts_in(&root), "a late write replayed into the recreated table's file");
    assert_eq!(labels(&db, &candidates).await, names("new", 2));
    close(&root, db);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_dropped_tables_late_writes_do_not_reach_its_successor_after_a_crash() {
    let (sc, candidates) = late_writes_through_a_dropped_handle().await;
    for_each_image(sc, async |seed, db, img| {
        assert!(!ghosts_in(img), "seed {seed}: a late write replayed into the recreated table");
        assert_eq!(labels(db, &candidates).await, names("new", 2), "seed {seed}");
    })
    .await;
}

/// A statement holding the dropped table's handle, with an in-place write it
/// logged before the drop, commits after the name is taken again. The fence
/// refuses only records framed after the barrier. This one was framed
/// before, so the log held it, and the commit applied it by path into the
/// new table's file. Replay voids it, since it precedes the barrier, so the
/// running database and a restarted one disagreed about the new table.
#[tokio::test(flavor = "multi_thread")]
async fn a_dropped_tables_held_write_does_not_reach_its_successor() {
    use crate::core::storage_providers::traits::StorageIO;
    let sc = Scenario::new(6).await;
    let db = sc.db.clone();
    let held = db.table_for_tests(TABLE).unwrap();
    let txn = held.io_rows().unwrap().begin_txn();
    assert_ne!(txn, 0, "no log took the transaction, so nothing is held");
    held.io_rows().unwrap().write_data_txn(txn, 0, b"GHOST-HELD").await;
    assert!(db.drop_table(TABLE).await.unwrap());
    db.add_table(create_statement(TABLE)).await.unwrap();
    insert(&db, 100, "new_00").await;
    let committed = held.io_rows().unwrap().commit_durable(txn).await;
    assert!(!ghosts_in(&sc.root), "a held write reached the recreated table's file");
    assert!(
        committed.is_err(),
        "a commit whose write went nowhere reported success"
    );
    drop(held);
    let candidates: Vec<String> = names("old", 6).into_iter().chain(names("new", 1)).collect();
    assert_eq!(labels(&db, &candidates).await, names("new", 1), "live");
    drop(db);
    let (_base, root, db) = sc.reopen().await;
    assert!(!ghosts_in(&root), "a held write replayed into the recreated table's file");
    assert_eq!(labels(&db, &candidates).await, names("new", 1), "after a reopen");
    close(&root, db);
}

/// The same with nothing created under the name before the commit: the held
/// write created the deleted file again, holding its bytes, and every later
/// `CREATE TABLE` of the name was refused for the leftover file.
#[tokio::test(flavor = "multi_thread")]
async fn a_dropped_tables_held_write_does_not_bring_its_file_back() {
    use crate::core::storage_providers::traits::StorageIO;
    let sc = Scenario::new(3).await;
    let db = sc.db.clone();
    let held = db.table_for_tests(TABLE).unwrap();
    let txn = held.io_rows().unwrap().begin_txn();
    assert_ne!(txn, 0, "no log took the transaction, so nothing is held");
    held.io_rows().unwrap().write_data_txn(txn, 0, b"GHOST-HELD").await;
    assert!(db.drop_table(TABLE).await.unwrap());
    let _ = held.io_rows().unwrap().commit_durable(txn).await;
    drop(held);
    assert!(
        !sc.root.join(format!("{TABLE}.db")).exists(),
        "a held write created the dropped table's file again"
    );
    db.add_table(create_statement(TABLE))
        .await
        .expect("the name was refused after a held write of the dropped table committed");
    insert(&db, 100, "new_00").await;
    let candidates: Vec<String> = names("old", 3).into_iter().chain(names("new", 1)).collect();
    assert_eq!(labels(&db, &candidates).await, names("new", 1));
    drop(db);
    let (_base, root, db) = sc.reopen().await;
    assert_eq!(labels(&db, &candidates).await, names("new", 1), "after a reopen");
    close(&root, db);
}

/// The snapshot gate, held as a snapshot holds it while it fixes where its
/// copy begins. Bounded: a peer test holding it for long must fail this one,
/// not hang it.
#[cfg(feature = "s3_backup")]
async fn hold_snapshot_start() -> tokio::sync::RwLockWriteGuard<'static, ()> {
    tokio::time::timeout(
        std::time::Duration::from_secs(60),
        crate::core::backup::hold_snapshot_gate(),
    )
    .await
    .expect("the snapshot gate was held elsewhere for a minute")
}

/// Returns once a deletion whose first log is `log` is waiting for its
/// permit: a positive signal, where a sleep let a slow drop that had not yet
/// reached its barrier pass for one that waited.
#[cfg(feature = "s3_backup")]
async fn parked_on(log: &Path) {
    tokio::time::timeout(std::time::Duration::from_secs(60), async {
        while !super::PARKED_DELETIONS.lock().iter().any(|p| p == log) {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the deletion never reached the snapshot gate");
}

/// A drop holds the snapshot gate from its barrier through its deletions, so
/// both are before the point a snapshot's copy begins or both after it. A
/// restore empties a file at its barrier only when the barrier committed
/// after the copy began, and a barrier committed just before it, with the
/// file deleted once the copy had read it, left the dropped table's bytes in
/// a copy the restore took for the next table's. Taken before the table
/// leaves the map: a drop waiting with the table half gone hid it, and a
/// `CREATE TABLE` of the name was refused for the files still there.
#[cfg(feature = "s3_backup")]
#[tokio::test(flavor = "multi_thread")]
async fn a_drop_waits_for_a_snapshot_start_in_progress() {
    let sc = Scenario::new(2).await;
    let db = sc.db.clone();
    let wal_path = sc.root.join(DEFAULT_WAL_NAME);
    let barriers = crate::core::wal::barriers_in(&wal_path);
    let gate = hold_snapshot_start().await;
    let dropping = tokio::spawn({
        let db = db.clone();
        async move { db.drop_table(TABLE).await }
    });
    parked_on(&wal_path).await;
    assert_eq!(
        crate::core::wal::barriers_in(&wal_path),
        barriers,
        "a drop logged its barrier while a snapshot was fixing its start"
    );
    assert!(
        sc.root.join(format!("{TABLE}.db")).exists(),
        "a drop deleted its files while a snapshot was fixing its start"
    );
    assert!(
        db.has_table(TABLE),
        "a drop waiting for the snapshot had already taken the table out of the map"
    );
    drop(gate);
    let dropped = tokio::time::timeout(std::time::Duration::from_secs(60), dropping)
        .await
        .expect("the drop never finished once the snapshot let go")
        .unwrap();
    assert!(dropped.unwrap(), "the drop found no table");
    assert!(!sc.root.join(format!("{TABLE}.db")).exists());
    drop(db);
    let Scenario {
        base, root, db, watch, ..
    } = sc;
    drop(watch);
    close(&root, db);
    drop(base);
}

/// A snapshot's upload, which can take minutes, holds only the compaction
/// gate. A drop sharing that gate waited for the whole upload, every DDL that
/// drops (an `ALTER` rewrite, a rename, a move) with it.
#[cfg(feature = "s3_backup")]
#[tokio::test(flavor = "multi_thread")]
async fn a_drop_does_not_wait_for_a_snapshot_upload() {
    let sc = Scenario::new(2).await;
    let db = sc.db.clone();
    let uploading = tokio::time::timeout(
        std::time::Duration::from_secs(60),
        crate::core::backup::hold_compaction_gate(),
    )
    .await
    .expect("the compaction gate was held elsewhere for a minute");
    let dropped = tokio::time::timeout(std::time::Duration::from_secs(20), db.drop_table(TABLE))
        .await;
    drop(uploading);
    let dropped = dropped.expect("a drop waited for a snapshot's upload");
    assert!(dropped.unwrap(), "the drop found no table");
    drop(db);
    let Scenario {
        base, root, db, watch, ..
    } = sc;
    drop(watch);
    close(&root, db);
    drop(base);
}

/// The same for a rename, whose old files go through `discard_files`: its
/// barrier and its deletions wait for a snapshot fixing its start, then go.
#[cfg(feature = "s3_backup")]
#[tokio::test(flavor = "multi_thread")]
async fn a_rename_waits_for_a_snapshot_start_in_progress() {
    let sc = Scenario::new(2).await;
    let db = sc.db.clone();
    let wal_path = sc.root.join(DEFAULT_WAL_NAME);
    let barriers = crate::core::wal::barriers_in(&wal_path);
    let gate = hold_snapshot_start().await;
    let renaming = tokio::spawn({
        let db = db.clone();
        async move { db.rename_table(TABLE, "renamed_away").await }
    });
    parked_on(&wal_path).await;
    assert_eq!(
        crate::core::wal::barriers_in(&wal_path),
        barriers,
        "a rename logged its barrier while a snapshot was fixing its start"
    );
    assert!(
        sc.root.join(format!("{TABLE}.db")).exists(),
        "a rename deleted the old files while a snapshot was fixing its start"
    );
    drop(gate);
    tokio::time::timeout(std::time::Duration::from_secs(60), renaming)
        .await
        .expect("the rename never finished once the snapshot let go")
        .unwrap()
        .unwrap();
    assert!(!sc.root.join(format!("{TABLE}.db")).exists());
    assert_eq!(labels_of(&db, "renamed_away", &names("old", 2)).await, names("old", 2));
    drop(db);
    let Scenario {
        base, root, db, watch, ..
    } = sc;
    drop(watch);
    close(&root, db);
    drop(base);
}

/// The rename intent is a logged file too, and clearing it is a barrier and
/// a deletion: they wait for a snapshot fixing its start, then go. Split
/// across that point, the copy kept the intent while the restore took its
/// barrier for one before the copy, and the restored database acted on a
/// rename it had already finished.
#[cfg(feature = "s3_backup")]
#[tokio::test(flavor = "multi_thread")]
async fn clearing_a_rename_intent_waits_for_a_snapshot_start_in_progress() {
    let sc = Scenario::new(1).await;
    let db = sc.db.clone();
    let wal_path = sc.root.join(DEFAULT_WAL_NAME);
    let intent = sc.root.join(super::RENAME_INTENT_FILE);
    db.record_rename_intent(TABLE, "renamed_away").await.unwrap();
    assert!(intent.exists(), "no intent was written, so there is nothing to clear");
    let barriers = crate::core::wal::barriers_in(&wal_path);
    let gate = hold_snapshot_start().await;
    let clearing = tokio::spawn({
        let db = db.clone();
        async move { super::clear_intent(&db.db_io_pointers).await }
    });
    parked_on(&wal_path).await;
    assert_eq!(
        crate::core::wal::barriers_in(&wal_path),
        barriers,
        "clearing the intent logged its barrier while a snapshot was fixing its start"
    );
    assert!(intent.exists(), "the intent was deleted while a snapshot was fixing its start");
    drop(gate);
    tokio::time::timeout(std::time::Duration::from_secs(60), clearing)
        .await
        .expect("clearing the intent never finished once the snapshot let go")
        .unwrap();
    assert!(!intent.exists());
    assert!(crate::core::wal::barriers_in(&wal_path) > barriers);
    drop(db);
    let Scenario {
        base, root, db, watch, ..
    } = sc;
    drop(watch);
    close(&root, db);
    drop(base);
}

/// A drop whose barrier reached the log but whose commit marker did not (a
/// full disk, the marker cut back cleanly, the log not poisoned) is refused
/// and the table put back. The fence the barrier took stayed, so the table's
/// own handles, born before it, had every record refused until a restart:
/// the drop said nothing happened, and the table could no longer be written.
#[tokio::test(flavor = "current_thread")]
async fn a_drop_whose_barrier_did_not_commit_leaves_the_table_writable() {
    let base = tempfile::tempdir().unwrap();
    let root = base.path().join("root");
    std::fs::create_dir_all(&root).unwrap();
    let db = open(&root).await;
    db.add_table(create_statement(TABLE)).await.unwrap();
    insert(&db, 1, "old_00").await;
    insert(&db, 2, "old_01").await;
    let wal_path = root.join(DEFAULT_WAL_NAME);
    let barriers = crate::core::wal::barriers_in(&wal_path);

    crate::core::wal::test_hooks::FAIL_BARRIER_MARKER.with(|f| f.set(true));
    let dropped = db.drop_table(TABLE).await;
    let reached = !crate::core::wal::test_hooks::FAIL_BARRIER_MARKER.with(|f| f.replace(false));
    crate::core::wal::test_hooks::FAIL_COMMIT_MARKER.with(|f| f.set(false));
    assert!(reached, "the drop never logged a barrier");
    assert!(
        crate::core::wal::barriers_in(&wal_path) > barriers,
        "no barrier reached the log, so this is not the case under test"
    );
    assert!(dropped.is_err(), "a drop whose barrier never committed reported success");
    assert!(db.has_table(TABLE), "the refused drop took the table out of the map");

    // In-place writes through the table's own handles, and an append.
    run(&db, &format!("UPDATE {TABLE} SET label = 'upd_00' WHERE id = 1")).await;
    run(&db, &format!("DELETE FROM {TABLE} WHERE id = 2")).await;
    insert(&db, 3, "new_00").await;
    let candidates: Vec<String> = names("old", 2)
        .into_iter()
        .chain(names("upd", 1))
        .chain(names("new", 1))
        .collect();
    let expected: Vec<String> = names("new", 1).into_iter().chain(names("upd", 1)).collect();
    let mut seen = labels(&db, &candidates).await;
    seen.sort();
    assert_eq!(seen, expected, "live");
    close(&root, db);
    let db = open(&root).await;
    let mut seen = labels(&db, &candidates).await;
    seen.sort();
    assert_eq!(seen, expected, "after a restart");
    close(&root, db);
}

/// Cut off right after a drop or a rename returns, before anything else
/// syncs the directory: the deleted files must not come back holding the old
/// table's bytes, and the name can be used again.
async fn crash_right_after(rename: bool) {
    let sc = Scenario::new(8).await;
    let db = sc.db.clone();
    if rename {
        db.rename_table(TABLE, "moved_away").await.unwrap();
    } else {
        assert!(db.drop_table(TABLE).await.unwrap());
    }
    db.sync_durable().unwrap();
    drop(db);
    let candidates: Vec<String> = names("old", 8).into_iter().chain(names("new", 1)).collect();
    for_each_image(sc, async |seed, db, img| {
        assert_eq!(
            leftovers(img, TABLE),
            Vec::<String>::new(),
            "seed {seed}: the deleted files came back with the old table's bytes"
        );
        assert!(!db.has_table(TABLE), "seed {seed}: the old name is a table again");
        db.add_table(create_statement(TABLE)).await.unwrap();
        insert(db, 100, "new_00").await;
        assert_eq!(labels(db, &candidates).await, names("new", 1), "seed {seed}");
        if rename {
            assert_eq!(
                labels_of(db, "moved_away", &candidates).await,
                names("old", 8),
                "seed {seed}: the renamed table lost rows"
            );
        }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_renamed_tables_old_files_stay_deleted_after_a_crash() {
    crash_right_after(true).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_dropped_tables_files_stay_deleted_after_a_crash() {
    crash_right_after(false).await;
}

/// A file a crash brought back, which no catalogue row names, is refused to
/// a new table of its name instead of being opened as that table's own.
#[tokio::test(flavor = "current_thread")]
async fn a_table_is_not_created_over_a_leftover_file() {
    let base = tempfile::tempdir().unwrap();
    let root = base.path().join("root");
    std::fs::create_dir_all(&root).unwrap();
    let db = open(&root).await;
    db.add_table(create_statement(TABLE)).await.unwrap();
    insert(&db, 1, "old_00").await;
    db.sync_durable().unwrap();
    let saved: Vec<(PathBuf, Vec<u8>)> = table_file_names(TABLE)
        .map(|f| root.join(f))
        .filter(|p| p.exists())
        .map(|p| {
            let bytes = std::fs::read(&p).unwrap();
            (p, bytes)
        })
        .collect();
    assert!(db.drop_table(TABLE).await.unwrap());
    close(&root, db);
    for (p, bytes) in &saved {
        std::fs::write(p, bytes).unwrap();
    }
    let db = open(&root).await;
    let created = db.add_table(create_statement(TABLE)).await;
    assert!(created.is_err(), "a table was created over the dropped table's files");
    assert!(!db.has_table(TABLE));
    // Nor renamed onto them.
    db.add_table(create_statement("other")).await.unwrap();
    assert!(db.rename_table("other", TABLE).await.is_err(), "a table was renamed onto leftovers");
    // Empty files are no table's, and are reused.
    for (p, _) in &saved {
        std::fs::write(p, b"").unwrap();
    }
    db.add_table(create_statement(TABLE)).await.unwrap();
    assert!(labels(&db, &names("old", 1)).await.is_empty());
    close(&root, db);
}

/// A rename whose old files are kept holds both its names until the next
/// open finishes it. Dropping the renamed table meanwhile left the catalogue
/// naming neither, so the open kept the old files and cleared the intent, and
/// a table created under the old name took their rows. Refused instead.
#[tokio::test(flavor = "current_thread")]
async fn a_held_renames_table_cannot_be_dropped_before_the_next_open() {
    let base = tempfile::tempdir().unwrap();
    let root = base.path().join("root");
    std::fs::create_dir_all(&root).unwrap();
    let db = open(&root).await;
    db.add_table(create_statement(TABLE)).await.unwrap();
    for n in 0..4 {
        insert(&db, n, &format!("old_{n:02}")).await;
    }
    crate::core::wal::test_hooks::FAIL_FORGET.with(|f| f.set(true));
    db.rename_table(TABLE, "moved_away").await.unwrap();
    assert!(!crate::core::wal::test_hooks::FAIL_FORGET.with(|f| f.replace(false)));
    assert!(root.join(format!("{TABLE}.db")).exists(), "the rename's old files were not kept");
    assert!(
        db.drop_table("moved_away").await.is_err(),
        "the held rename's table was dropped before the open that settles it"
    );
    assert!(db.has_table("moved_away"));
    db.sync_durable().unwrap();
    close(&root, db);

    let db = open(&root).await;
    assert!(!root.join(format!("{TABLE}.db")).exists(), "the next open kept the old files");
    assert!(db.drop_table("moved_away").await.unwrap(), "the hold outlived the open");
    db.add_table(create_statement(TABLE)).await.unwrap();
    assert!(labels(&db, &names("old", 4)).await.is_empty());
    close(&root, db);
}

/// Tables on a second storage location, and on a backup mirror: the barrier
/// then goes to a log other than the catalogue's.
#[cfg(feature = "storage_pools")]
mod pools {
    use std::collections::HashSet;

    use super::*;
    use crate::core::storage::LocationRole;

    /// A database whose `TABLE` lives on a second location, with one durable
    /// row, and a fsynced copy of both directories taken after it.
    struct Pooled {
        base: tempfile::TempDir,
        root: PathBuf,
        disk: PathBuf,
        db: Arc<Database>,
        /// Watches both directories, so their syncs are in one order.
        watch: sync_audit::Watch,
        /// File name and directory for every file present at the baseline.
        baseline: HashSet<PathBuf>,
        /// Each log's bytes at the baseline.
        logs: Vec<(PathBuf, Vec<u8>)>,
        /// The pool file at the baseline.
        pool: String,
    }

    fn img_of(base: &Path, dir: &Path) -> PathBuf {
        base.join(format!("img_{}", dir.file_name().unwrap().to_string_lossy()))
    }

    impl Pooled {
        async fn new() -> Pooled {
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
            db.add_table(create_statement(TABLE)).await.unwrap();
            assert!(
                disk.join(format!("{TABLE}.db")).exists(),
                "the table's files are not on the second location"
            );
            assert!(
                crate::core::wal::wal_for(&path_str(&disk)).is_some(),
                "the second location has no log, so there is nothing to test"
            );
            insert(&db, 0, "base_00").await;
            db.sync_durable().unwrap();

            // The baseline: everything fsynced and copied, so a data file in
            // the image holds no more than the disk is sure to.
            let mut baseline = HashSet::new();
            let mut logs = Vec::new();
            for dir in [&root, &disk] {
                let img = img_of(base.path(), dir);
                std::fs::create_dir_all(&img).unwrap();
                for path in regular_files(dir) {
                    let f = std::fs::File::options().read(true).write(true).open(&path).unwrap();
                    f.sync_all().unwrap();
                    let bytes = std::fs::read(&path).unwrap();
                    if path.file_name().unwrap() == DEFAULT_WAL_NAME {
                        logs.push((path.clone(), bytes.clone()));
                    }
                    std::fs::write(img.join(path.file_name().unwrap()), &bytes).unwrap();
                    baseline.insert(path);
                }
            }
            let pool = std::fs::read_to_string(root.join(crate::core::storage::POOL_FILE)).unwrap();
            let watch = sync_audit::watch(base.path());
            let _ = watch.take_events();
            Pooled {
                base,
                root,
                disk,
                db,
                watch,
                baseline,
                logs,
                pool,
            }
        }

        /// Cuts power right after the last sync of the second location's log
        /// and opens what the disks would hold then: each log as far as a
        /// sync issued up to that instant reached, the data files as of the
        /// baseline, and any file created since as it is now.
        async fn crash_after_last_disk_sync(self) -> Image {
            let events: Vec<SyncEvent> = self.watch.take_events();
            let disk_log = self.disk.join(DEFAULT_WAL_NAME);
            let instant = events
                .iter()
                .rposition(|e| e.tag == "wal.log" && e.path == disk_log)
                .expect("the second location's log was never synced");
            let synced = &events[..=instant];

            for (log, at_baseline) in &self.logs {
                let current = std::fs::read(log).unwrap();
                assert!(
                    current.starts_with(at_baseline),
                    "{} was truncated inside the scenario, which this harness does not model",
                    log.display()
                );
                let reached = synced
                    .iter()
                    .filter(|e| e.tag == "wal.log" && &e.path == log)
                    .map(|e| e.len)
                    .max()
                    .unwrap_or(0);
                let cut = (at_baseline.len() as u64).max(reached).min(current.len() as u64);
                let img = img_of(self.base.path(), log.parent().unwrap());
                std::fs::write(img.join(DEFAULT_WAL_NAME), &current[..cut as usize]).unwrap();
            }
            for dir in [&self.root, &self.disk] {
                let img = img_of(self.base.path(), dir);
                for path in regular_files(dir) {
                    let name = path.file_name().unwrap();
                    if !self.baseline.contains(&path) && name != crate::core::storage::POOL_FILE {
                        std::fs::copy(&path, img.join(name)).unwrap();
                    }
                }
            }
            let (root_img, disk_img) = (
                img_of(self.base.path(), &self.root),
                img_of(self.base.path(), &self.disk),
            );
            // The pool file is replaced whole, so at the instant it was some
            // version between the baseline's and the current one. A drop
            // unpins only after its barrier and a rename pins the new name
            // before its own, so the union of the two holds every placement
            // the instant did; the current file alone would send a table the
            // catalogue still names to the wrong directory.
            let pool = root_img.join(crate::core::storage::POOL_FILE);
            let current =
                std::fs::read_to_string(self.root.join(crate::core::storage::POOL_FILE)).unwrap();
            let mut text = self.pool.clone();
            for line in current.lines() {
                if !self.pool.lines().any(|l| l == line) {
                    text.push_str(line);
                    text.push('\n');
                }
            }
            std::fs::write(
                &pool,
                text.replace(&path_str(&self.root), &path_str(&root_img))
                    .replace(&path_str(&self.disk), &path_str(&disk_img)),
            )
            .unwrap();
            let total = dir_bytes(&self.root)
                + dir_bytes(&self.disk)
                + dir_bytes(&root_img)
                + dir_bytes(&disk_img);
            assert!(total < MAX_SCENARIO_BYTES, "the scenario wrote {total} bytes, past its bound");

            drop(self.watch);
            drop(self.db);
            crate::core::wal::remove_for(&path_str(&self.root));
            crate::core::wal::remove_for(&path_str(&self.disk));
            std::fs::remove_dir_all(&self.root).unwrap();
            std::fs::remove_dir_all(&self.disk).unwrap();

            let db = open(&root_img).await;
            Image {
                base: self.base,
                root_img,
                disk_img,
                db,
            }
        }
    }

    impl Pooled {
        /// Cuts power now, every log synced whole, with each deletion since
        /// the baseline undone unless a sync of its directory came after it:
        /// the image directories hold the baseline's copies already, so a file
        /// gone now stays in them unless its deletion was durable.
        async fn crash_undoing_unsynced_deletions(self) -> Image {
            let events: Vec<SyncEvent> = self.watch.take_events();
            for dir in [&self.root, &self.disk] {
                let img = img_of(self.base.path(), dir);
                for path in regular_files(dir) {
                    let name = path.file_name().unwrap();
                    if name != crate::core::storage::POOL_FILE {
                        std::fs::copy(&path, img.join(name)).unwrap();
                    }
                }
                for path in self.baseline.iter().filter(|p| p.parent() == Some(dir.as_path())) {
                    if path.exists() {
                        continue;
                    }
                    let in_img = img.join(path.file_name().unwrap());
                    match gone(path, &events) {
                        Gone::Durably => std::fs::remove_file(&in_img).unwrap(),
                        Gone::MaybeEmpty => std::fs::write(&in_img, b"").unwrap(),
                        Gone::MaybeWhole => {}
                    }
                }
            }
            let (root_img, disk_img) = (
                img_of(self.base.path(), &self.root),
                img_of(self.base.path(), &self.disk),
            );
            let current =
                std::fs::read_to_string(self.root.join(crate::core::storage::POOL_FILE)).unwrap();
            std::fs::write(
                root_img.join(crate::core::storage::POOL_FILE),
                current
                    .replace(&path_str(&self.root), &path_str(&root_img))
                    .replace(&path_str(&self.disk), &path_str(&disk_img)),
            )
            .unwrap();
            drop(self.watch);
            drop(self.db);
            crate::core::wal::remove_for(&path_str(&self.root));
            crate::core::wal::remove_for(&path_str(&self.disk));
            std::fs::remove_dir_all(&self.root).unwrap();
            std::fs::remove_dir_all(&self.disk).unwrap();
            let db = open(&root_img).await;
            Image {
                base: self.base,
                root_img,
                disk_img,
                db,
            }
        }
    }

    struct Image {
        base: tempfile::TempDir,
        root_img: PathBuf,
        disk_img: PathBuf,
        db: Arc<Database>,
    }

    impl Image {
        fn finish(self) {
            drop(self.db);
            crate::core::wal::remove_for(&path_str(&self.root_img));
            crate::core::wal::remove_for(&path_str(&self.disk_img));
            drop(self.base);
        }
    }

    /// Rows written after the baseline, durable only in the second location's
    /// log, so a barrier voiding them in a table that still exists loses them.
    async fn late_rows(db: &Arc<Database>) -> Vec<String> {
        for n in 0..4 {
            insert(db, 10 + n, &format!("late_{n:02}")).await;
        }
        let mut all = names("base", 1);
        all.extend(names("late", 4));
        all
    }

    /// Crashed right after the drop's barrier reached the second location's
    /// log, the table is gone or whole: never there without its rows, which a
    /// barrier durable ahead of the catalogue tombstone produced.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_drop_on_another_location_is_never_half_done_after_a_crash() {
        let p = Pooled::new().await;
        let db = p.db.clone();
        let all = late_rows(&db).await;
        assert!(db.drop_table(TABLE).await.unwrap(), "the drop found no table");
        drop(db);

        let img = p.crash_after_last_disk_sync().await;
        if img.db.has_table(TABLE) {
            assert_eq!(
                labels(&img.db, &all).await,
                all,
                "the table survived the crash without the rows its log held"
            );
        }
        img.finish();
    }

    /// Crashed right after the rename voided the old name's records on the
    /// second location, whichever name the catalogue holds has every row.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_rename_on_another_location_keeps_every_row_after_a_crash() {
        let p = Pooled::new().await;
        let db = p.db.clone();
        let all = late_rows(&db).await;
        db.rename_table(TABLE, "moved_away").await.unwrap();
        drop(db);

        let img = p.crash_after_last_disk_sync().await;
        let named: Vec<&str> = [TABLE, "moved_away"]
            .into_iter()
            .filter(|n| img.db.has_table(*n))
            .collect();
        assert_eq!(named.len(), 1, "the image holds {named:?}");
        assert_eq!(
            labels_of(&img.db, named[0], &all).await,
            all,
            "after the crash {} lost rows its log held",
            named[0]
        );
        img.finish();
    }

    /// Cut off right after a drop or rename of a table on a second location.
    /// The database's directory is synced by the placement file's rewrite,
    /// but nothing syncs the second location's, where the files were: their
    /// deletions could be undone by a power cut, and the next table of the
    /// name opened them.
    async fn crash_right_after_on_another_location(rename: bool) {
        let p = Pooled::new().await;
        let db = p.db.clone();
        if rename {
            db.rename_table(TABLE, "moved_away").await.unwrap();
        } else {
            assert!(db.drop_table(TABLE).await.unwrap());
        }
        db.sync_durable().unwrap();
        drop(db);
        let img = p.crash_undoing_unsynced_deletions().await;
        assert_eq!(
            leftovers(&img.disk_img, TABLE),
            Vec::<String>::new(),
            "the deleted files came back on the second location"
        );
        img.db.storage.pin(TABLE, &path_str(&img.disk_img)).unwrap();
        img.db.add_table(create_statement(TABLE)).await.unwrap();
        insert(&img.db, 100, "new_00").await;
        let candidates: Vec<String> = names("base", 1).into_iter().chain(names("new", 1)).collect();
        assert_eq!(labels(&img.db, &candidates).await, names("new", 1));
        img.finish();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_drop_on_another_location_stays_done_after_a_crash() {
        crash_right_after_on_another_location(false).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_rename_on_another_location_leaves_no_old_files_after_a_crash() {
        crash_right_after_on_another_location(true).await;
    }

    /// The invariant under the two tests above, which the drop meets only
    /// incidentally there (its sweep creates and syncs each missing file
    /// before deleting it): when a drop or rename returns, every deletion it
    /// made on the second location is followed by a sync of that directory.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_drop_or_rename_syncs_every_directory_it_deleted_in() {
        for rename in [false, true] {
            let p = Pooled::new().await;
            let db = p.db.clone();
            if rename {
                db.rename_table(TABLE, "moved_away").await.unwrap();
            } else {
                assert!(db.drop_table(TABLE).await.unwrap());
            }
            let events = p.watch.take_events();
            let disk = p.disk.clone();
            let unsynced: Vec<String> = events
                .iter()
                .enumerate()
                .filter(|(_, e)| e.tag == "unlink" && e.path.parent() == Some(disk.as_path()))
                .filter(|(i, _)| {
                    !events[*i..]
                        .iter()
                        .any(|e| e.tag == "dir" && e.path.parent() == Some(disk.as_path()))
                })
                .map(|(_, e)| e.path.file_name().unwrap().to_string_lossy().into_owned())
                .collect();
            assert!(
                events.iter().any(|e| e.tag == "unlink" && e.path.parent() == Some(disk.as_path())),
                "nothing was deleted on the second location, so this tests nothing"
            );
            assert_eq!(
                unsynced,
                Vec::<String>::new(),
                "{} returned with deletions its directory was never synced after",
                if rename { "a rename" } else { "a drop" }
            );
            drop(db);
            let Pooled {
                base, root, disk, db, watch, ..
            } = p;
            drop(watch);
            drop(db);
            crate::core::wal::remove_for(&path_str(&root));
            crate::core::wal::remove_for(&path_str(&disk));
            drop(base);
        }
    }

    /// (e) on a second location: dropped, recreated there, clean reopen.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_recreated_table_on_another_location_holds_only_its_rows_after_a_reopen() {
        let p = Pooled::new().await;
        let db = p.db.clone();
        let mut all = late_rows(&db).await;
        assert!(db.drop_table(TABLE).await.unwrap());
        db.storage.pin(TABLE, &path_str(&p.disk)).unwrap();
        db.add_table(create_statement(TABLE)).await.unwrap();
        assert!(p.disk.join(format!("{TABLE}.db")).exists(), "recreated elsewhere");
        insert(&db, 100, "new_00").await;
        all.extend(names("new", 1));
        drop(db);
        let Pooled {
            base,
            root,
            disk,
            db,
            watch,
            ..
        } = p;
        drop(watch);
        drop(db);
        crate::core::wal::remove_for(&path_str(&root));
        crate::core::wal::remove_for(&path_str(&disk));

        let db = open(&root).await;
        assert_eq!(
            labels(&db, &all).await,
            names("new", 1),
            "after a reopen the table recreated on a second location shows the dropped one's rows"
        );
        drop(db);
        crate::core::wal::remove_for(&path_str(&root));
        crate::core::wal::remove_for(&path_str(&disk));
        drop(base);
    }

    /// An `ALTER … ADD COLUMN` rewrite of a mirrored table whose drop got its
    /// barrier into the database's log but not into the backup's (a full
    /// backup disk, the log not poisoned). The drop happened, table and files,
    /// and used to report an error anyway, so the rewrite stopped before its
    /// create and lost every row it was carrying.
    #[tokio::test(flavor = "current_thread")]
    async fn a_rewrite_keeps_its_rows_when_only_a_backups_barrier_fails() {
        let base = tempfile::tempdir().unwrap();
        let root = base.path().join("root");
        let backup = base.path().join("backup");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::create_dir_all(&backup).unwrap();
        let db = open(&root).await;
        db.storage
            .add_location(&path_str(&backup), LocationRole::Backup, 1, true)
            .unwrap();
        db.add_table(create_statement(TABLE)).await.unwrap();
        for n in 0..5 {
            insert(&db, n, &format!("old_{n:02}")).await;
        }
        let wal_path = backup.join(DEFAULT_WAL_NAME);
        let before = crate::core::wal::barriers_in(&wal_path);

        crate::core::wal::test_hooks::FORGETS_BEFORE_FAILING.with(|n| n.set(1));
        crate::core::wal::test_hooks::FAIL_FORGET.with(|f| f.set(true));
        let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(&format!(
            "ALTER TABLE {TABLE} ADD COLUMN extra INT"
        ))
        .unwrap();
        let out = crate::core::rql::executor::execute_with_sync(purpose, db.clone(), true).await;
        let reached = !crate::core::wal::test_hooks::FAIL_FORGET.with(|f| f.replace(false));
        crate::core::wal::test_hooks::FORGETS_BEFORE_FAILING.with(|n| n.set(0));
        assert!(reached, "the backup's barrier was never attempted");
        assert_eq!(
            crate::core::wal::barriers_in(&wal_path),
            before,
            "the backup's log took a barrier, so this is not the case under test"
        );
        assert!(
            !out.is_empty() && out[0] != 3,
            "the rewrite failed: {}",
            String::from_utf8_lossy(&out)
        );
        assert_eq!(
            labels(&db, &names("old", 5)).await,
            names("old", 5),
            "the rewrite lost rows when only the backup's barrier failed"
        );
        drop(db);
        crate::core::wal::remove_for(&path_str(&root));
        crate::core::wal::remove_for(&path_str(&backup));
    }

    /// A backup location keeps its own log, under the same file ids, for its
    /// copy of a mirrored table. Dropped and created again, the old table's
    /// records there must be void too, or a replay writes them into the new
    /// table's backup copy.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_backup_copy_replays_none_of_a_dropped_tables_records() {
        let base = tempfile::tempdir().unwrap();
        let root = base.path().join("root");
        let backup = base.path().join("backup");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::create_dir_all(&backup).unwrap();
        let db = open(&root).await;
        db.storage
            .add_location(&path_str(&backup), LocationRole::Backup, 1, true)
            .unwrap();
        assert!(
            crate::core::wal::wal_for(&path_str(&backup)).is_some(),
            "the backup location has no log, so there is nothing to test"
        );
        db.add_table(create_statement(TABLE)).await.unwrap();
        for n in 0..6 {
            insert(&db, n, &format!("old_{n:02}")).await;
        }
        assert!(
            std::fs::read(backup.join(format!("{TABLE}.db")))
                .unwrap()
                .windows(4)
                .any(|w| w == b"old_"),
            "the backup copy never received the rows, so it tests nothing"
        );
        assert!(db.drop_table(TABLE).await.unwrap());
        db.add_table(create_statement(TABLE)).await.unwrap();
        insert(&db, 100, "new_00").await;
        db.sync_durable().unwrap();

        // Replay the backup's log, as a crash now would, into empty copies of
        // the table's files.
        let scratch = base.path().join("scratch");
        std::fs::create_dir_all(&scratch).unwrap();
        let log = scratch.join(DEFAULT_WAL_NAME);
        std::fs::copy(backup.join(DEFAULT_WAL_NAME), &log).unwrap();
        let targets: Vec<(u64, PathBuf)> = table_file_names(TABLE)
            .map(|f| (crate::core::wal::file_id_for_name(&f), scratch.join(&f)))
            .collect();
        crate::core::wal::Wal::recover(&log, move |id| {
            targets.iter().find(|(t, _)| *t == id).map(|(_, p)| p.clone())
        })
        .unwrap();
        let rows = std::fs::read(scratch.join(format!("{TABLE}.db"))).unwrap_or_default();
        assert!(
            rows.windows(6).any(|w| w == b"new_00"),
            "the backup's log does not hold the new table's rows, so it tests nothing"
        );
        assert!(
            !rows.windows(4).any(|w| w == b"old_"),
            "replaying the backup's log wrote the dropped table's rows into the new copy"
        );

        drop(db);
        crate::core::wal::remove_for(&path_str(&root));
        crate::core::wal::remove_for(&path_str(&backup));
    }

    /// When a rename returns, every deletion it made in the database's own
    /// directory is followed by a sync of that directory, the rename intent's
    /// included. A power cut that undid the intent's unlink brought a valid,
    /// checksummed intent back, and the next open "finished" the rename again,
    /// over whatever table had taken the old name since. On a second location
    /// nothing else syncs the database's directory after the intent goes: the
    /// renamed table's lazily created files are made there.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_rename_syncs_its_directory_after_clearing_its_intent() {
        let p = Pooled::new().await;
        p.db.rename_table(TABLE, "moved_away").await.unwrap();
        let events = p.watch.take_events();
        let root = p.root.clone();
        let intent = root.join(super::super::RENAME_INTENT_FILE);
        assert!(
            events.iter().any(|e| e.tag == "unlink" && e.path == intent),
            "the rename never deleted its intent, so this tests nothing"
        );
        let unsynced: Vec<String> = events
            .iter()
            .enumerate()
            .filter(|(_, e)| e.tag == "unlink" && e.path.parent() == Some(root.as_path()))
            .filter(|(i, _)| {
                !events[*i..]
                    .iter()
                    .any(|e| e.tag == "dir" && e.path.parent() == Some(root.as_path()))
            })
            .map(|(_, e)| e.path.file_name().unwrap().to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            unsynced,
            Vec::<String>::new(),
            "a rename returned with deletions in the database's directory never synced after"
        );
        assert!(matches!(gone(&intent, &events), Gone::Durably));
        let Pooled {
            base, root, disk, db, watch, ..
        } = p;
        drop(watch);
        drop(db);
        crate::core::wal::remove_for(&path_str(&root));
        crate::core::wal::remove_for(&path_str(&disk));
        drop(base);
    }

    /// A move whose source files could not be released (the source's log
    /// would not take the barrier: a full disk there) kept them, and their
    /// records stayed live in the source's log. Reported as a plain success,
    /// nothing stood in the way of a later move back: its copy went into
    /// those files, and the next open replayed the old incarnation's records
    /// over it, bringing back a row deleted in between.
    #[tokio::test(flavor = "current_thread")]
    async fn a_move_back_after_a_move_that_kept_its_source_brings_no_row_back() {
        let base = tempfile::tempdir().unwrap();
        let root = base.path().join("root");
        let disk = base.path().join("disk");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::create_dir_all(&disk).unwrap();
        let db = open(&root).await;
        db.storage
            .add_location(&path_str(&disk), LocationRole::Primary, 1, true)
            .unwrap();
        db.storage.pin(TABLE, &path_str(&root)).unwrap();
        db.add_table(create_statement(TABLE)).await.unwrap();
        assert!(root.join(format!("{TABLE}.db")).exists(), "the table is not on the root");
        for n in 0..6 {
            insert(&db, n, &format!("old_{n:02}")).await;
        }
        let root_log = root.join(DEFAULT_WAL_NAME);
        let logged = std::fs::metadata(&root_log).unwrap().len();

        crate::core::wal::test_hooks::FAIL_FORGET_IN.with(|p| *p.borrow_mut() = Some(root_log.clone()));
        let moved = db.move_table_to(TABLE, &path_str(&disk)).await;
        let reached = crate::core::wal::test_hooks::FAIL_FORGET_IN.with(|p| p.borrow_mut().take()).is_none();
        assert!(reached, "the move never tried to void the source's records");
        assert!(disk.join(format!("{TABLE}.db")).exists(), "the table did not reach the disk");
        assert_eq!(labels(&db, &names("old", 6)).await, names("old", 6), "after the move");

        run(&db, &format!("DELETE FROM {TABLE} WHERE id = 0")).await;
        db.move_table_to(TABLE, &path_str(&root)).await.unwrap();
        let mut kept = names("old", 6);
        kept.remove(0);
        assert_eq!(labels(&db, &names("old", 6)).await, kept, "after the move back");
        assert!(
            std::fs::metadata(&root_log).unwrap().len() >= logged,
            "the root's log was truncated, so it no longer holds the old records to replay"
        );

        drop(db);
        crate::core::wal::remove_for(&path_str(&root));
        crate::core::wal::remove_for(&path_str(&disk));
        let db = open(&root).await;
        assert_eq!(
            labels(&db, &names("old", 6)).await,
            kept,
            "after a reopen a row deleted before the move back came back"
        );
        assert!(
            moved.is_err(),
            "a move that kept its source's files and records reported success"
        );
        drop(db);
        crate::core::wal::remove_for(&path_str(&root));
        crate::core::wal::remove_for(&path_str(&disk));
    }

    /// A database with `TABLE` on the root, four durable rows, and a second
    /// location `disk` it can move to.
    async fn on_root_with_a_disk() -> (tempfile::TempDir, PathBuf, PathBuf, Arc<Database>) {
        let base = tempfile::tempdir().unwrap();
        let root = base.path().join("root");
        let disk = base.path().join("disk");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::create_dir_all(&disk).unwrap();
        let db = open(&root).await;
        db.storage
            .add_location(&path_str(&disk), LocationRole::Primary, 1, true)
            .unwrap();
        db.storage.pin(TABLE, &path_str(&root)).unwrap();
        db.add_table(create_statement(TABLE)).await.unwrap();
        assert!(root.join(format!("{TABLE}.db")).exists(), "the table is not on the root");
        for n in 0..4 {
            insert(&db, n, &format!("old_{n:02}")).await;
        }
        (base, root, disk, db)
    }

    /// A move whose destination's log will not void its old records for the
    /// name (a full disk there) copies nothing and leaves the table where it
    /// was, readable and writable, and says so.
    #[tokio::test(flavor = "current_thread")]
    async fn a_move_whose_destination_will_not_void_its_records_stays_where_it_was() {
        let (base, root, disk, db) = on_root_with_a_disk().await;
        let disk_log = disk.join(DEFAULT_WAL_NAME);
        assert!(
            crate::core::wal::wal_for(&path_str(&disk)).is_some(),
            "the destination has no log, so it has no barrier to refuse"
        );
        let backend = db.storage.backend_of(TABLE);

        crate::core::wal::test_hooks::FAIL_FORGET_IN.with(|p| *p.borrow_mut() = Some(disk_log.clone()));
        let moved = db.move_table_to(TABLE, &path_str(&disk)).await;
        let reached = crate::core::wal::test_hooks::FAIL_FORGET_IN.with(|p| p.borrow_mut().take()).is_none();
        assert!(reached, "the move never asked the destination's log for a barrier");
        assert!(moved.is_err(), "a move whose destination refused its barrier reported success");
        assert!(db.has_table(TABLE), "the refused move took the table out of the map");
        assert_eq!(db.storage.backend_of(TABLE), backend, "the refused move flipped the placement");
        // Empty files may be there (opening the destination creates some),
        // and a later table of the name takes those over harmlessly.
        let copied: Vec<String> = table_file_names(TABLE)
            .filter(|f| std::fs::metadata(disk.join(f)).is_ok_and(|m| m.len() > 0))
            .collect();
        assert_eq!(copied, Vec::<String>::new(), "the refused move copied files to the destination");
        assert_eq!(labels(&db, &names("old", 4)).await, names("old", 4), "after the refused move");
        insert(&db, 4, "old_04").await;
        assert_eq!(labels(&db, &names("old", 5)).await, names("old", 5), "written after it");

        drop(db);
        crate::core::wal::remove_for(&path_str(&root));
        crate::core::wal::remove_for(&path_str(&disk));
        let db = open(&root).await;
        assert_eq!(labels(&db, &names("old", 5)).await, names("old", 5), "after a reopen");
        drop(db);
        crate::core::wal::remove_for(&path_str(&root));
        crate::core::wal::remove_for(&path_str(&disk));
        drop(base);
    }

    /// A move voids the destination's old records for the name before its
    /// copy, under the snapshot gate like every barrier and deletion: it
    /// waits for a snapshot fixing its start, then goes.
    #[cfg(feature = "s3_backup")]
    #[tokio::test(flavor = "multi_thread")]
    async fn a_move_waits_for_a_snapshot_start_in_progress() {
        let (base, root, disk, db) = on_root_with_a_disk().await;
        let disk_log = disk.join(DEFAULT_WAL_NAME);
        let barriers = crate::core::wal::barriers_in(&disk_log);
        let gate = super::hold_snapshot_start().await;
        let moving = tokio::spawn({
            let db = db.clone();
            let disk = path_str(&disk);
            async move { db.move_table_to(TABLE, &disk).await }
        });
        super::parked_on(&disk_log).await;
        assert_eq!(
            crate::core::wal::barriers_in(&disk_log),
            barriers,
            "a move logged its barrier while a snapshot was fixing its start"
        );
        assert!(!disk.join(format!("{TABLE}.db")).exists(), "a move copied while the gate was held");
        drop(gate);
        tokio::time::timeout(std::time::Duration::from_secs(60), moving)
            .await
            .expect("the move never finished once the snapshot let go")
            .unwrap()
            .unwrap();
        assert!(disk.join(format!("{TABLE}.db")).exists());
        assert!(!root.join(format!("{TABLE}.db")).exists());
        assert_eq!(labels(&db, &names("old", 4)).await, names("old", 4));
        drop(db);
        crate::core::wal::remove_for(&path_str(&root));
        crate::core::wal::remove_for(&path_str(&disk));
        drop(base);
    }

    /// How far an interrupted rename got before the crash.
    #[derive(Clone, Copy, PartialEq)]
    enum RenameStage {
        /// The intent and the copy: the next open abandons the copy.
        Copied,
        /// The new catalogue row too: the next open finishes the rename.
        NewRow,
        /// The old row tombstoned as well: only the old files remain.
        OldRowGone,
    }

    /// A rename interrupted at `stage`, settled by the next open, forgets the
    /// placement of the name whose files it discards: the old name once it
    /// finishes the rename, the copy's name once it abandons it. Left
    /// recorded, the placement outlives its table, and the next table created
    /// under that name inherits it, since `place` keeps a recorded placement.
    async fn interrupted_rename_forgets_the_discarded_placement(stage: RenameStage) {
        let base = tempfile::tempdir().unwrap();
        let root = base.path().join("root");
        std::fs::create_dir_all(&root).unwrap();
        let db = open(&root).await;
        db.add_table(create_statement("src_t")).await.unwrap();
        insert_into(&db, "src_t", 1, "old_00").await;
        db.record_rename_intent("src_t", TABLE).await.unwrap();
        super::super::pool_rename(&db.storage, "src_t", TABLE);
        db.copy_table_files("src_t", TABLE).await.unwrap();
        if stage != RenameStage::Copied {
            db.remember_in_catalogue(TABLE).await.unwrap();
        }
        if stage == RenameStage::OldRowGone {
            assert!(db.forget_in_catalogue("src_t").await.unwrap(), "no src_t row to tombstone");
        }
        assert!(
            db.storage.spec_of("src_t").is_some() && db.storage.spec_of(TABLE).is_some(),
            "both names should have a placement before the reopen, or this tests nothing"
        );
        db.sync_durable().unwrap();
        close(&root, db);

        let db = open(&root).await;
        let (freed, kept) = if stage == RenameStage::Copied { (TABLE, "src_t") } else { ("src_t", TABLE) };
        assert!(!db.has_table(freed), "{freed} is still a table");
        assert!(db.has_table(kept), "{kept} is gone");
        assert_eq!(db.storage.spec_of(freed), None, "the settled rename kept {freed}'s placement");
        assert!(db.storage.spec_of(kept).is_some(), "the settled rename forgot {kept}'s placement");
        close(&root, db);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn an_abandoned_interrupted_rename_forgets_the_copys_placement() {
        interrupted_rename_forgets_the_discarded_placement(RenameStage::Copied).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_finished_interrupted_rename_forgets_the_old_names_placement() {
        interrupted_rename_forgets_the_discarded_placement(RenameStage::NewRow).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn an_interrupted_rename_past_its_tombstone_forgets_the_old_names_placement() {
        interrupted_rename_forgets_the_discarded_placement(RenameStage::OldRowGone).await;
    }
}

/// The same questions for a partitioned table (`docs/partitioning.md` §12):
/// dropped and created again under its name, as a partitioned or a plain
/// table, after a clean reopen and in every crash image, the new table holds
/// only its own rows; and a transaction writing several partitions is whole
/// in every crash image.
///
/// A partitioned table's files are `__pt_<uid>_*`, and a new table takes a new
/// uid, so its files never share a log id with the dropped one's. What can
/// still go wrong is the drop's own order: the barrier voiding the old
/// files' records, the deletion and its directory sync, and late writes
/// through a partition handle a statement still holds, which must be refused
/// rather than replayed into anything.
mod partitioned {
    use super::*;
    use crate::core::partition::Route;
    use crate::core::partition::files::PREFIX;

    const PARTITIONS: u32 = 8;

    async fn create_partitioned(db: &Arc<Database>) -> u64 {
        db.create_partitioned_table(create_statement(TABLE), &["id"], PARTITIONS)
            .await
            .unwrap_or_else(|e| panic!("creating partitioned {TABLE}: {e}"));
        db.partitioned_table(TABLE).expect("partitioned").uid()
    }

    /// [`super::labels_of`] for a partitioned table, read through the API
    /// below SQL (written when its `SELECT` was refused): every stored label that is one of
    /// `candidates`, as often as stored, and any other label too, marked, so
    /// replayed garbage fails the comparison.
    pub(super) async fn labels_of(db: &Arc<Database>, table: &str, candidates: &[String]) -> Vec<String> {
        let pt = db.partitioned_table(table).expect("partitioned");
        let schema = pt.shell().schema.clone();
        let label = schema.fields.iter().find(|f| f.name == "label").expect("a label column").clone();
        let fetch = crate::core::rql::join::fetch_for(&[label], &schema.fields);
        let empty = crate::core::rql::join::fetch_for(&[], &schema.fields);
        let epoch = pt.epoch().expect("the table opens");
        let mut seen = Vec::new();
        for (pid, p) in pt.materialized(&epoch, &Route::All).expect("its partitions") {
            let rows = p
                .query_row("", &schema.fields.to_vec(), empty.clone(), fetch.clone(), None, None)
                .await
                .unwrap_or_else(|e| panic!("reading partition {pid}: {e}"));
            for row in rows {
                for c in row.columns.iter() {
                    let text = String::from_utf8_lossy(c.data.into_slice()).into_owned();
                    if candidates.contains(&text) {
                        seen.push(text);
                    } else {
                        seen.push(format!("<unexpected {text:?}>"));
                    }
                }
            }
        }
        seen.sort();
        seen
    }

    /// A partitioned [`TABLE`] of `old` durable rows labelled `old_<n>`,
    /// written through SQL.
    async fn scenario(old: usize) -> (Scenario, u64) {
        let base = tempfile::tempdir().unwrap();
        let root = base.path().join("root");
        std::fs::create_dir_all(&root).unwrap();
        let watch = sync_audit::watch(&root);
        let db = open(&root).await;
        assert!(crate::core::wal::wal_for(&path_str(&root)).is_some(), "no log, nothing to replay");
        let uid = create_partitioned(&db).await;
        for n in 0..old {
            insert(&db, n as i32, &format!("old_{n:02}")).await;
        }
        db.sync_durable().unwrap();
        assert_eq!(labels_of(&db, TABLE, &names("old", old)).await, names("old", old));
        let old_log = std::fs::read(root.join(DEFAULT_WAL_NAME)).unwrap();
        let baseline = durable_baseline(&root);
        let _ = watch.take_events();
        (
            Scenario {
                base,
                root,
                db,
                watch,
                old_log,
                baseline,
            },
            uid,
        )
    }

    fn files_of_uid(dir: &Path, uid: u64) -> Vec<String> {
        let prefix = format!("{PREFIX}{uid:016x}_");
        let mut out: Vec<String> = std::fs::read_dir(dir)
            .unwrap()
            .filter_map(|e| e.ok()?.file_name().into_string().ok())
            .filter(|n| n.starts_with(&prefix))
            .collect();
        out.sort();
        out
    }

    /// Any file in `dir` holding a late write's marker.
    fn ghosts(dir: &Path) -> Vec<String> {
        regular_files(dir)
            .into_iter()
            .filter(|p| std::fs::read(p).unwrap_or_default().windows(6).any(|w| w == b"GHOST-"))
            .map(|p| p.file_name().unwrap().to_string_lossy().into_owned())
            .collect()
    }

    /// `old_*` and `new_*` labels, sorted as the readers return them.
    fn all(old: usize, new: usize) -> Vec<String> {
        let mut out: Vec<String> = names("old", old).into_iter().chain(names("new", new)).collect();
        out.sort();
        out
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_recreated_partitioned_table_holds_only_its_rows_after_a_clean_reopen() {
        let (sc, old_uid) = scenario(12).await;
        let db = sc.db.clone();
        assert!(db.drop_table(TABLE).await.unwrap(), "the drop found no table");
        assert_eq!(files_of_uid(&sc.root, old_uid), Vec::<String>::new(), "the dropped files are still there");
        let new_uid = create_partitioned(&db).await;
        assert_ne!(new_uid, old_uid);
        insert(&db, 100, "new_00").await;
        let candidates = all(12, 1);
        assert_eq!(labels_of(&db, TABLE, &candidates).await, names("new", 1), "the live table is wrong");
        drop(db);

        let (_base, root, db) = sc.reopen().await;
        assert_eq!(
            labels_of(&db, TABLE, &candidates).await,
            names("new", 1),
            "after a clean reopen the recreated partitioned table shows rows the dropped one held"
        );
        assert_eq!(db.partitioned_table(TABLE).map(|t| t.uid()), Some(new_uid));
        assert_eq!(files_of_uid(&root, old_uid), Vec::<String>::new(), "the dropped files came back");
        close(&root, db);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_partitioned_table_recreated_twice_holds_only_the_last_rows_after_a_clean_reopen() {
        let (sc, _) = scenario(10).await;
        let db = sc.db.clone();
        assert!(db.drop_table(TABLE).await.unwrap());
        create_partitioned(&db).await;
        for n in 0..6 {
            insert(&db, 50 + n, &format!("mid_{n:02}")).await;
        }
        assert!(db.drop_table(TABLE).await.unwrap());
        create_partitioned(&db).await;
        insert(&db, 100, "new_00").await;
        let mut candidates = all(10, 1);
        candidates.extend(names("mid", 6));
        drop(db);

        let (_base, root, db) = sc.reopen().await;
        assert_eq!(labels_of(&db, TABLE, &candidates).await, names("new", 1));
        close(&root, db);
    }

    /// Dropped and created again under the same name, with fewer rows than the
    /// old table had: the new table holds exactly its rows in every image.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_recreated_partitioned_table_does_not_take_the_dropped_tables_rows() {
        let (sc, _) = scenario(12).await;
        let db = sc.db.clone();
        assert!(db.drop_table(TABLE).await.unwrap());
        create_partitioned(&db).await;
        insert(&db, 100, "new_00").await;
        db.sync_durable().unwrap();
        drop(db);
        let seen = sc.crash_and_recover_all(TABLE, &all(12, 1)).await;
        assert_every_seed(&seen, &names("new", 1), "after the crash the recreated table shows dropped rows");
    }

    /// Cut off right after the drop returns: the old uid's files are gone once
    /// the image opens (deleted durably, or reaped as no table's), the name is
    /// free, and a partitioned table created under it holds only its rows.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_dropped_partitioned_tables_files_stay_deleted_after_a_crash() {
        let (sc, old_uid) = scenario(8).await;
        let db = sc.db.clone();
        assert!(db.drop_table(TABLE).await.unwrap());
        db.sync_durable().unwrap();
        drop(db);
        let candidates = all(8, 1);
        for_each_image(sc, async |seed, db, img| {
            assert_eq!(files_of_uid(img, old_uid), Vec::<String>::new(), "seed {seed}: the dropped files survived");
            assert!(!db.has_table(TABLE) && !db.is_partitioned(TABLE), "seed {seed}");
            create_partitioned(db).await;
            insert(db, 100, "new_00").await;
            assert_eq!(labels_of(db, TABLE, &candidates).await, names("new", 1), "seed {seed}");
        })
        .await;
    }

    /// A plain table dropped and created again as a partitioned one, and the
    /// reverse: each successor holds only its own rows in every image, and no
    /// file under the logical name serves the partitioned one (I8).
    #[tokio::test(flavor = "multi_thread")]
    async fn a_plain_table_recreated_as_partitioned_holds_only_its_rows_after_a_crash() {
        let sc = Scenario::new(12).await;
        let db = sc.db.clone();
        assert!(db.drop_table(TABLE).await.unwrap());
        create_partitioned(&db).await;
        insert(&db, 100, "new_00").await;
        db.sync_durable().unwrap();
        drop(db);
        let candidates = all(12, 1);
        for_each_image(sc, async |seed, db, img| {
            assert_eq!(leftovers(img, TABLE), Vec::<String>::new(), "seed {seed}: plain files under the name");
            assert_eq!(labels_of(db, TABLE, &candidates).await, names("new", 1), "seed {seed}");
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_partitioned_table_recreated_as_plain_holds_only_its_rows_after_a_crash() {
        let (sc, old_uid) = scenario(12).await;
        let db = sc.db.clone();
        assert!(db.drop_table(TABLE).await.unwrap());
        db.add_table(create_statement(TABLE)).await.unwrap();
        insert(&db, 100, "new_00").await;
        db.sync_durable().unwrap();
        drop(db);
        let candidates = all(12, 1);
        for_each_image(sc, async |seed, db, img| {
            assert!(!db.is_partitioned(TABLE), "seed {seed}: the partitioned table came back");
            assert_eq!(files_of_uid(img, old_uid), Vec::<String>::new(), "seed {seed}");
            assert_eq!(super::labels_of(db, TABLE, &candidates).await, names("new", 1), "seed {seed}");
        })
        .await;
    }

    /// A statement still holding a partition of the dropped table writes after
    /// the drop, before and after the name is taken again: the log refuses
    /// those records (the barrier's fence), so nothing replays them, and no
    /// file anywhere holds them after a reopen or in any crash image.
    async fn late_writes_through_a_dropped_partition() -> (Scenario, Vec<String>) {
        use crate::core::storage_providers::traits::StorageIO;
        let (sc, _) = scenario(6).await;
        let db = sc.db.clone();
        let held = {
            let pt = db.partitioned_table(TABLE).unwrap();
            let epoch = pt.epoch().unwrap();
            let (_, p) = pt.materialized(&epoch, &Route::All).unwrap().into_iter().next().expect("a partition");
            p
        };
        assert!(db.drop_table(TABLE).await.unwrap());
        held.io_rows().unwrap().append_data(b"GHOST-APPEND-BEFORE", true).await;
        held.io_rows().unwrap().write_data(0, b"GHOST-INPLACE-BEFORE").await;
        create_partitioned(&db).await;
        insert(&db, 100, "new_00").await;
        held.io_rows().unwrap().append_data(b"GHOST-APPEND-AFTER", true).await;
        held.io_rows().unwrap().write_data(0, b"GHOST-INPLACE-AFTER").await;
        drop(held);
        insert(&db, 101, "new_01").await;
        db.sync_durable().unwrap();
        drop(db);
        (sc, all(6, 2))
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_dropped_partitions_late_writes_do_not_reach_anything_on_a_reopen() {
        let (sc, candidates) = late_writes_through_a_dropped_partition().await;
        let (_base, root, db) = sc.reopen().await;
        assert_eq!(ghosts(&root), Vec::<String>::new(), "a late write replayed into a file");
        assert_eq!(labels_of(&db, TABLE, &candidates).await, names("new", 2));
        close(&root, db);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_dropped_partitions_late_writes_do_not_reach_anything_after_a_crash() {
        let (sc, candidates) = late_writes_through_a_dropped_partition().await;
        for_each_image(sc, async |seed, db, img| {
            assert_eq!(ghosts(img), Vec::<String>::new(), "seed {seed}: a late write replayed into a file");
            assert_eq!(labels_of(db, TABLE, &candidates).await, names("new", 2), "seed {seed}");
        })
        .await;
    }

    /// A name a rename still holds (its old files kept, their records live)
    /// is refused to a partitioned CREATE as to a plain one, for that reason,
    /// and is free once the next open has settled the rename; the partitioned
    /// table created then holds only its own rows.
    #[tokio::test(flavor = "current_thread")]
    async fn a_held_name_is_refused_to_a_partitioned_create_until_the_next_open() {
        let base = tempfile::tempdir().unwrap();
        let root = base.path().join("root");
        std::fs::create_dir_all(&root).unwrap();
        let db = open(&root).await;
        db.add_table(create_statement(TABLE)).await.unwrap();
        insert(&db, 1, "old_00").await;
        crate::core::wal::test_hooks::FAIL_FORGET.with(|f| f.set(true));
        db.rename_table(TABLE, "moved_away").await.unwrap();
        let reached = !crate::core::wal::test_hooks::FAIL_FORGET.with(|f| f.replace(false));
        assert!(reached, "the rename never logged a barrier");

        let e = db
            .create_partitioned_table(create_statement(TABLE), &["id"], PARTITIONS)
            .await
            .expect_err("a partitioned table was created under a held name");
        assert!(e.to_string().contains("formerly named"), "refused for another reason: {e}");
        assert!(!db.is_partitioned(TABLE));
        db.sync_durable().unwrap();
        close(&root, db);

        let db = open(&root).await;
        create_partitioned(&db).await;
        insert(&db, 100, "new_00").await;
        assert_eq!(labels_of(&db, TABLE, &all(1, 1)).await, names("new", 1));
        close(&root, db);
    }

    /// A rename interrupted with `stage` done, settled by the next open: the
    /// name whose files it discarded is free for a partitioned CREATE, so its
    /// placement went with them. `committed`: both catalogue rows were
    /// written (the open finishes the rename and frees the old name); else
    /// only the intent and the copy (it abandons the copy, freeing the new).
    async fn interrupted_rename_frees_the_discarded_name(committed: bool) {
        let base = tempfile::tempdir().unwrap();
        let root = base.path().join("root");
        std::fs::create_dir_all(&root).unwrap();
        let db = open(&root).await;
        db.add_table(create_statement("src_t")).await.unwrap();
        insert_into(&db, "src_t", 1, "old_00").await;
        db.record_rename_intent("src_t", TABLE).await.unwrap();
        super::super::pool_rename(&db.storage, "src_t", TABLE);
        db.copy_table_files("src_t", TABLE).await.unwrap();
        if committed {
            db.remember_in_catalogue(TABLE).await.unwrap();
        }
        db.sync_durable().unwrap();
        close(&root, db);

        let db = open(&root).await;
        let freed = if committed { "src_t" } else { TABLE };
        assert!(!db.has_table(freed), "{freed} is still a table");
        let mut ct = create_statement(freed);
        ct.table_name = freed.to_string();
        db.create_partitioned_table(ct, &["id"], PARTITIONS)
            .await
            .unwrap_or_else(|e| panic!("the settled rename kept {freed} from a partitioned CREATE: {e}"));
        close(&root, db);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_finished_interrupted_rename_frees_the_old_name() {
        interrupted_rename_frees_the_discarded_name(true).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn an_abandoned_interrupted_rename_frees_the_copys_name() {
        interrupted_rename_frees_the_discarded_name(false).await;
    }

    /// A partitioned drop whose barrier cannot be logged is past its commit
    /// point (the tombstone, then `DROPPED`), so the table stays dropped and
    /// the DROP reports success, as a plain drop whose barrier may be durable
    /// does: the database's own log is healthy. Its files are kept with their
    /// records live, the name is free (a successor takes a new uid), and the
    /// next start voids and deletes them. The successor holds only its rows.
    #[tokio::test(flavor = "current_thread")]
    async fn a_partitioned_drop_that_cannot_void_the_log_is_finished_at_the_next_start() {
        let base = tempfile::tempdir().unwrap();
        let root = base.path().join("root");
        std::fs::create_dir_all(&root).unwrap();
        let db = open(&root).await;
        let old_uid = create_partitioned(&db).await;
        for n in 0..4 {
            insert(&db, n, &format!("old_{n:02}")).await;
        }
        let candidates = all(4, 1);

        crate::core::wal::test_hooks::FAIL_FORGET.with(|f| f.set(true));
        let dropped = db.drop_table(TABLE).await;
        let reached = !crate::core::wal::test_hooks::FAIL_FORGET.with(|f| f.replace(false));
        assert!(reached, "the drop never logged a barrier");
        assert!(dropped.unwrap(), "the drop past its commit point reported failure");
        assert!(!db.is_partitioned(TABLE) && !db.has_table(TABLE));
        assert!(!files_of_uid(&root, old_uid).is_empty(), "files with live records were deleted");

        let new_uid = create_partitioned(&db).await;
        assert_ne!(new_uid, old_uid, "the kept files' uid was reused");
        insert(&db, 100, "new_00").await;
        assert_eq!(labels_of(&db, TABLE, &candidates).await, names("new", 1));
        close(&root, db);

        let db = open(&root).await;
        assert_eq!(files_of_uid(&root, old_uid), Vec::<String>::new(), "the next start kept the dropped files");
        assert_eq!(labels_of(&db, TABLE, &candidates).await, names("new", 1), "the successor took dropped rows");
        close(&root, db);
        let db = open(&root).await;
        assert_eq!(labels_of(&db, TABLE, &candidates).await, names("new", 1));
        close(&root, db);
    }

    /// A partitioned drop whose barrier failed after reaching the log (its
    /// sync failed, the database's own log is poisoned) reports the error, as
    /// a plain drop does: not even the tombstone is known to be durable. The
    /// table does not come back, and from both outcomes of a power cut (the
    /// barrier lost, the barrier kept) a table created under the name holds
    /// only its own rows, through a second restart too.
    #[tokio::test(flavor = "current_thread")]
    async fn a_partitioned_drop_whose_barrier_may_be_durable_reports_it() {
        let base = tempfile::tempdir().unwrap();
        let root = base.path().join("root");
        std::fs::create_dir_all(&root).unwrap();
        let watch = sync_audit::watch(&root);
        let db = open(&root).await;
        let old_uid = create_partitioned(&db).await;
        for n in 0..6 {
            insert(&db, n, &format!("old_{n:02}")).await;
        }
        db.sync_durable().unwrap();

        crate::core::wal::test_hooks::FAIL_BARRIER_SYNC.with(|f| f.set(true));
        let dropped = db.drop_table(TABLE).await;
        let reached = !crate::core::wal::test_hooks::FAIL_BARRIER_SYNC.with(|f| f.replace(false));
        assert!(reached, "the drop never logged a barrier");
        assert!(dropped.is_err(), "a drop whose catalogue log failed its sync reported success");
        assert!(!db.is_partitioned(TABLE) && !db.has_table(TABLE), "the table came back");

        let events = watch.take_events();
        drop(watch);
        let wal_path = root.join(DEFAULT_WAL_NAME);
        let current = std::fs::read(&wal_path).unwrap();
        let synced = synced_log_len(&events, &wal_path, current.len());
        assert!(synced < current.len(), "the barrier was synced, so nothing is undecided");
        let mut images = Vec::new();
        for (tag, cut) in [("lost", synced), ("kept", current.len())] {
            let img = base.path().join(format!("img_{tag}"));
            std::fs::create_dir_all(&img).unwrap();
            for path in regular_files(&root) {
                let name = path.file_name().unwrap();
                if name == DEFAULT_WAL_NAME {
                    std::fs::write(img.join(name), &current[..cut]).unwrap();
                } else {
                    std::fs::copy(&path, img.join(name)).unwrap();
                }
            }
            images.push((tag, img));
        }
        close(&root, db);

        let candidates = all(6, 1);
        for (tag, img) in images {
            let db = open(&img).await;
            assert!(!db.is_partitioned(TABLE), "barrier {tag}: the dropped table is back");
            assert_eq!(files_of_uid(&img, old_uid), Vec::<String>::new(), "barrier {tag}: dropped files kept");
            create_partitioned(&db).await;
            insert(&db, 100, "new_00").await;
            assert_eq!(labels_of(&db, TABLE, &candidates).await, names("new", 1), "barrier {tag}: live");
            close(&img, db);
            let db = open(&img).await;
            assert_eq!(
                labels_of(&db, TABLE, &candidates).await,
                names("new", 1),
                "barrier {tag}: a table created after the failed drop took the dropped rows"
            );
            close(&img, db);
        }
    }

    /// A partitioned drop holds the snapshot gate from before its catalogue
    /// tombstone through its deletions, as a plain drop does (see
    /// `a_drop_waits_for_a_snapshot_start_in_progress`): its barrier and its
    /// deletions land both before the point a snapshot's copy begins or both
    /// after it. A restore empties a file at its barrier only when the barrier
    /// committed after the copy began, so a barrier just before it, with the
    /// file deleted once the copy had read it, would leave the dropped
    /// partitions' bytes in the snapshot. While it waits, the table is whole
    /// and still named.
    #[cfg(feature = "s3_backup")]
    #[tokio::test(flavor = "multi_thread")]
    async fn a_partitioned_drop_waits_for_a_snapshot_start_in_progress() {
        let (sc, uid) = scenario(4).await;
        let db = sc.db.clone();
        let wal_path = sc.root.join(DEFAULT_WAL_NAME);
        let barriers = crate::core::wal::barriers_in(&wal_path);
        let files = files_of_uid(&sc.root, uid);
        assert!(files.len() > 3, "no partition materialized, so the drop deletes too little: {files:?}");
        let gate = super::hold_snapshot_start().await;
        let dropping = tokio::spawn({
            let db = db.clone();
            async move { db.drop_table(TABLE).await }
        });
        super::parked_on(&wal_path).await;
        assert_eq!(
            crate::core::wal::barriers_in(&wal_path),
            barriers,
            "a partitioned drop logged its barrier while a snapshot was fixing its start"
        );
        assert_eq!(
            files_of_uid(&sc.root, uid),
            files,
            "a partitioned drop deleted files while a snapshot was fixing its start"
        );
        assert!(
            db.is_partitioned(TABLE) && db.has_table(TABLE),
            "a partitioned drop waiting for the snapshot had already taken the table out of the maps"
        );
        drop(gate);
        let dropped = tokio::time::timeout(std::time::Duration::from_secs(60), dropping)
            .await
            .expect("the drop never finished once the snapshot let go")
            .unwrap();
        assert!(dropped.unwrap(), "the drop found no table");
        assert!(crate::core::wal::barriers_in(&wal_path) > barriers, "the drop logged no barrier");
        assert_eq!(files_of_uid(&sc.root, uid), Vec::<String>::new());
        drop(db);
        let Scenario {
            base, root, db, watch, ..
        } = sc;
        drop(watch);
        close(&root, db);
        drop(base);
    }

    /// A partitioned CREATE that fails at its catalogue row discards the files
    /// it wrote under the same gate: the barrier voiding them and their
    /// deletion wait together for a snapshot fixing its start. On one thread,
    /// so the injected failure reaches the CREATE.
    #[cfg(feature = "s3_backup")]
    #[tokio::test(flavor = "current_thread")]
    async fn a_failed_partitioned_create_discards_its_files_under_the_snapshot_gate() {
        let base = tempfile::tempdir().unwrap();
        let root = base.path().join("root");
        std::fs::create_dir_all(&root).unwrap();
        let db = open(&root).await;
        let wal_path = root.join(DEFAULT_WAL_NAME);
        let partition_files = |dir: &Path| -> Vec<String> {
            let mut out: Vec<String> = std::fs::read_dir(dir)
                .unwrap()
                .filter_map(|e| e.ok()?.file_name().into_string().ok())
                .filter(|n| n.starts_with(PREFIX))
                .collect();
            out.sort();
            out
        };
        assert_eq!(partition_files(&root), Vec::<String>::new());
        let barriers = crate::core::wal::barriers_in(&wal_path);

        let gate = super::hold_snapshot_start().await;
        crate::core::database::test_hooks::FAIL_CATALOGUE_ROW.with(|f| f.set(true));
        let creating = tokio::spawn({
            let db = db.clone();
            async move { db.create_partitioned_table(create_statement(TABLE), &["id"], PARTITIONS).await }
        });
        super::parked_on(&wal_path).await;
        let reached = !crate::core::database::test_hooks::FAIL_CATALOGUE_ROW.with(|f| f.replace(false));
        assert!(reached, "the CREATE never wrote its catalogue row");
        let written = partition_files(&root);
        assert!(!written.is_empty(), "the CREATE wrote no files, so it discards nothing");
        assert_eq!(
            crate::core::wal::barriers_in(&wal_path),
            barriers,
            "a failed CREATE logged its barrier while a snapshot was fixing its start"
        );
        drop(gate);
        let created = tokio::time::timeout(std::time::Duration::from_secs(60), creating)
            .await
            .expect("the CREATE never finished once the snapshot let go")
            .unwrap();
        assert!(created.is_err(), "a CREATE whose catalogue row failed succeeded");
        assert!(crate::core::wal::barriers_in(&wal_path) > barriers, "the discard logged no barrier");
        assert_eq!(partition_files(&root), Vec::<String>::new(), "the failed CREATE kept {written:?}");
        assert!(!db.is_partitioned(TABLE));
        close(&root, db);
    }

    // -----------------------------------------------------------------------
    // S9: one transaction across partitions is whole in every crash image.
    // -----------------------------------------------------------------------

    /// Six rows, one per id, inserted under one transaction id; the ids reach
    /// several partitions.
    async fn insert_in_one_transaction(db: &Arc<Database>) -> crate::core::mvcc::TxId {
        let tx = db.transactions.begin();
        for n in 0..6 {
            let sql = format!("INSERT INTO {TABLE} (id, label) VALUES ({}, 'new_{n:02}')", 100 + n * 7);
            let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(&sql).unwrap();
            let out = crate::core::rql::executor::execute_in_txn(purpose, db.clone(), tx).await;
            assert!(!out.is_empty() && out[0] != 3, "{sql}: {}", String::from_utf8_lossy(&out));
        }
        let pt = db.partitioned_table(TABLE).unwrap();
        let epoch = pt.epoch().unwrap();
        let reached: std::collections::BTreeSet<u32> = (0..6i32)
            .map(|n| epoch.map.pid_of(pt.spec().unwrap().point_of(&[&(100 + n * 7).to_le_bytes()[..]]).unwrap()))
            .collect();
        assert!(reached.len() > 1, "the transaction reaches one partition only: {reached:?}");
        tx
    }

    /// Never committed: no image shows any of its rows, nor loses an old one.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_uncommitted_transaction_across_partitions_leaves_nothing_after_a_crash() {
        let (sc, _) = scenario(4).await;
        let db = sc.db.clone();
        let _tx = insert_in_one_transaction(&db).await;
        db.sync_durable().unwrap();
        drop(db);
        let seen = sc.crash_and_recover_all(TABLE, &all(4, 6)).await;
        assert_every_seed(&seen, &names("old", 4), "an uncommitted transaction left rows");
    }

    /// Committed and synced: every image shows all of it.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_committed_transaction_across_partitions_is_whole_after_a_crash() {
        let (sc, _) = scenario(4).await;
        let db = sc.db.clone();
        let tx = insert_in_one_transaction(&db).await;
        // As a COMMIT does: the rows on other locations, the commit, then the
        // outcome record, which rides on the next sync of the log.
        db.commit_written_transaction(tx).unwrap();
        db.transactions.flush_deferred().unwrap();
        db.sync_durable().unwrap();
        drop(db);
        let seen = sc.crash_and_recover_all(TABLE, &all(4, 6)).await;
        assert_every_seed(&seen, &all(4, 6), "a committed transaction lost rows");
    }

    /// Cut off before anything synced the transaction: an image may keep it
    /// or lose it, never part of it, and the table opens either way. With
    /// `commit`, the commit and its outcome record were written (as under
    /// `synchronous_commit = off`); without, the crash came mid-transaction.
    ///
    /// The transaction's first write materialized partitions. Had their
    /// `MATERIALIZE` records waited on the log's next sync like the rows, an
    /// image could lose them while the new partitions' files kept row pages
    /// the kernel wrote back, and the open would refuse the whole table as a
    /// torn map with unexplained data (`refuse_torn_leftovers`), until an
    /// operator removed the evidence.
    async fn unsynced_transaction_is_all_or_nothing(commit: bool) {
        let (sc, _) = scenario(4).await;
        let db = sc.db.clone();
        let tx = insert_in_one_transaction(&db).await;
        if commit {
            db.commit_written_transaction(tx).unwrap();
            db.transactions.flush_deferred().unwrap();
        }
        drop(db);
        let seen = sc.crash_and_recover_all(TABLE, &all(4, 6)).await;
        for (seed, labels) in seen {
            assert!(
                labels == names("old", 4) || labels == all(4, 6),
                "seed {seed}: part of a transaction survived: {labels:?}"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_unsynced_commit_across_partitions_is_all_or_nothing_after_a_crash() {
        unsynced_transaction_is_all_or_nothing(true).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_crash_mid_transaction_across_new_partitions_leaves_the_table_whole() {
        unsynced_transaction_is_all_or_nothing(false).await;
    }
}
