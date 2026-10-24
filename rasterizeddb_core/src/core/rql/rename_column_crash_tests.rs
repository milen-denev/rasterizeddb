//! Power cut in the middle of `RENAME COLUMN` (and of `DROP COLUMN`, which
//! writes the same way).
//!
//! A rename touches the field record, its `__null_` companion's record, and
//! every by-name declaration of the column: its DEFAULT, its CHECKs, the
//! composite UNIQUE keys and foreign keys that name it. Each used to be its own
//! durable write, so a power cut between two of them left a mix — the field
//! renamed while its companion was not (every NULL then read as 0), or the
//! constraints still naming a column that no longer existed (skipped as "not
//! enforced", for good: nothing reconciles them on open).
//!
//! The test pauses the statement at every crash point it reaches and builds,
//! right there, what the disk could hold after a power cut, the way
//! `drop_recreate_replay_tests` does (whose model this follows): the log cut at
//! the longest length an `fdatasync` of it was issued at, and past that some of
//! the unsynced tail by seed; every data file page kept as it is now or put
//! back as it was at the durable baseline, by seed. No process is killed. Each
//! image is then opened and must show the rename either not done at all or
//! done completely.

use std::collections::HashMap;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

use parking_lot::Mutex;
use rclite::Arc;

use crate::core::database::Database;
use crate::core::row::row::{Row, vec_into_rows};
use crate::core::sync_audit::{self, SyncEvent};
use crate::core::wal::DEFAULT_WAL_NAME;

const TABLE: &str = "crash_rename_r";
const CHILD: &str = "crash_rename_c";

/// Seeds per crash point: seed 0 keeps every page, 1 every page old, the rest mix.
const SEEDS: u64 = 4;
const PAGE: usize = 4096;
/// Ceiling on what the scenario may put on disk, directory and images together.
const MAX_SCENARIO_BYTES: u64 = 96 * 1024 * 1024;

fn init_globals() {
    let _ = crate::MAX_PERMITS_THREADS.set(4);
    let _ = crate::BATCH_SIZE.set(1024);
}

fn path_str(p: &Path) -> String {
    p.to_string_lossy().into_owned()
}

async fn open(root: &Path) -> Arc<Database> {
    Arc::new(Database::new_named(&path_str(root), false, "renamecrash").await)
}

async fn run(db: &Arc<Database>, sql: &str) -> Vec<u8> {
    let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(sql)
        .unwrap_or_else(|| panic!("could not parse {sql}"));
    crate::core::rql::executor::execute_with_sync(purpose, db.clone(), true).await
}

fn is_error(out: &[u8]) -> bool {
    out.first() == Some(&3)
}

async fn ok(db: &Arc<Database>, sql: &str) {
    let out = run(db, sql).await;
    assert!(!is_error(&out), "{sql} failed: {}", String::from_utf8_lossy(&out[1..]));
}

fn int(row: &Row, column: usize) -> Option<i64> {
    let cell = &row.columns[column];
    if cell.column_type == crate::core::db_type::DbType::NULL {
        return None;
    }
    let bytes = cell.data.into_slice();
    Some(match bytes.len() {
        0 => return None,
        4 => i32::from_le_bytes(bytes.try_into().unwrap()) as i64,
        8 => i64::from_le_bytes(bytes.try_into().unwrap()),
        n => panic!("an integer cell of {} bytes", n),
    })
}

fn regular_files(dir: &Path) -> Vec<PathBuf> {
    let mut out: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.is_file())
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

/// A file as the baseline saw it; the handle pins its inode.
struct Baseline {
    _held: std::fs::File,
    ino: u64,
    bytes: Vec<u8>,
}

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

/// How much of the file at `path` a data-file sync made durable since the
/// baseline.
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

/// A baseline file after a power cut: past its synced prefix, each page as it
/// is now or as it was at the baseline (every page, when `all_old`).
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

/// A file created since the baseline after a power cut: its synced prefix,
/// then each page kept or lost, perhaps cut back to a page boundary.
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

fn synced_log_len(events: &[SyncEvent], wal_path: &Path, current: usize) -> usize {
    events
        .iter()
        .filter(|e| e.tag == "wal.log" && e.path == wal_path)
        .map(|e| e.len)
        .max()
        .unwrap_or(0)
        .min(current as u64) as usize
}

/// What a crash image is built from.
struct Harness {
    base: PathBuf,
    root: PathBuf,
    baseline: HashMap<PathBuf, Baseline>,
    old_log: Vec<u8>,
    watch: sync_audit::Watch,
    /// Every sync since the baseline, accumulated across crash points.
    events: Vec<SyncEvent>,
    /// `(point, seed, image directory)`.
    images: Vec<(String, u64, PathBuf)>,
}

impl Harness {
    /// Builds every seed's image of the disk as it could be if power were cut
    /// now.
    fn cut_power(&mut self, point: &str) {
        self.events.extend(self.watch.take_events());
        let wal_path = self.root.join(DEFAULT_WAL_NAME);
        let current_log = std::fs::read(&wal_path).unwrap();
        let synced = synced_log_len(&self.events, &wal_path, current_log.len()).max(self.old_log.len());
        for seed in 0..SEEDS {
            let mut rng = Rng::new(seed + 1000 * self.images.len() as u64);
            let cut = if seed == 0 {
                synced
            } else {
                synced + rng.below((current_log.len() - synced) as u64 + 1) as usize
            };
            let image_log = &current_log[..cut];
            assert!(
                image_log.starts_with(&self.old_log),
                "a checkpoint ran inside the scenario, so the image tests nothing"
            );
            let img = self.base.join(format!("img_{}_{seed}", self.images.len()));
            std::fs::create_dir_all(&img).unwrap();
            for path in regular_files(&self.root) {
                let name = path.file_name().unwrap();
                if name == DEFAULT_WAL_NAME {
                    std::fs::write(img.join(name), image_log).unwrap();
                    continue;
                }
                let now = std::fs::read(&path).unwrap();
                let synced = synced_prefix(&path, &self.events);
                let ino = std::fs::metadata(&path).unwrap().ino();
                let base = self
                    .baseline
                    .get(&path)
                    .filter(|b| b.ino == ino)
                    .map(|b| b.bytes.as_slice());
                let bytes = match (seed, base) {
                    (0, _) => now,
                    (1, Some(base)) => mixed_pages(base, &now, synced, true, &mut rng),
                    (1, None) => now[..synced.min(now.len())].to_vec(),
                    (_, Some(base)) => mixed_pages(base, &now, synced, false, &mut rng),
                    (_, None) => lost_pages(&now, synced, &mut rng),
                };
                std::fs::write(img.join(name), bytes).unwrap();
            }
            // Only a pooled build writes `db_storage.cfg`, whose absolute
            // paths would send the image back to the source directory.
            #[cfg(feature = "storage_pools")]
            {
                let pool = img.join(crate::core::storage::POOL_FILE);
                if let Ok(text) = std::fs::read_to_string(&pool) {
                    std::fs::write(&pool, text.replace(&path_str(&self.root), &path_str(&img))).unwrap();
                }
            }
            self.images.push((point.to_string(), seed, img));
        }
    }
}

/// What an image shows: which name the column has, and whether everything the
/// column carries behaves as declared under that name.
async fn judge(db: &Arc<Database>) -> Result<String, String> {
    let schema = db
        .tables
        .get(TABLE)
        .ok_or_else(|| "the table is gone".to_string())?
        .schema
        .clone();
    let live = |n: &str| schema.fields.iter().any(|f| !f.is_deleted && f.name.eq_ignore_ascii_case(n));
    let name = match (live("a"), live("z")) {
        (true, false) => "a",
        (false, true) => "z",
        (a, z) => return Err(format!("a live: {a}, z live: {z}")),
    };
    let mut wrong = Vec::new();

    // The NULL stored in row 1 reads as NULL: the companion moved with it.
    let out = run(db, &format!("SELECT {name} FROM {TABLE} WHERE id = 1")).await;
    if is_error(&out) {
        wrong.push(format!("select failed: {}", String::from_utf8_lossy(&out[1..])));
    } else {
        let rows = vec_into_rows(&out[1..]).unwrap();
        let got: Vec<Option<i64>> = rows.iter().map(|r| int(r, 0)).collect();
        if got != vec![None] {
            wrong.push(format!("row 1's NULL reads as {got:?}"));
        }
    }
    // CHECK (a > 0).
    if !is_error(&run(db, &format!("INSERT INTO {TABLE} (id, {name}, b, k) VALUES (10, -5, 9, 10)")).await) {
        wrong.push("the CHECK was not enforced".into());
    }
    // DEFAULT 7.
    let out = run(db, &format!("INSERT INTO {TABLE} (id, b, k) VALUES (11, 50, 11)")).await;
    if is_error(&out) {
        wrong.push(format!("insert failed: {}", String::from_utf8_lossy(&out[1..])));
    } else {
        let out = run(db, &format!("SELECT {name} FROM {TABLE} WHERE id = 11")).await;
        let rows = vec_into_rows(&out[1..]).unwrap();
        let got: Vec<Option<i64>> = rows.iter().map(|r| int(r, 0)).collect();
        if got != vec![Some(7)] {
            wrong.push(format!("the DEFAULT gave {got:?}"));
        }
    }
    // UNIQUE (a, b): row 2 holds (3, 1).
    if !is_error(&run(db, &format!("INSERT INTO {TABLE} (id, {name}, b, k) VALUES (12, 3, 1, 12)")).await) {
        wrong.push("the composite UNIQUE was not enforced".into());
    }
    // The child's foreign key on a: row 2 is referenced.
    if !is_error(&run(db, &format!("DELETE FROM {TABLE} WHERE id = 2")).await) {
        wrong.push("a referenced row was deleted".into());
    }
    if wrong.is_empty() { Ok(name.to_string()) } else { Err(format!("as {name}: {}", wrong.join("; "))) }
}

/// Runs `setup` and makes it durable, then runs `alter` with a crash image cut
/// at every crash point the statement reaches on `table` and once more after
/// it returns (as `"after"`). Returns the images; the source directory is gone.
async fn crash_images(
    base: &Path,
    table: &str,
    setup: &[String],
    alter: &str,
) -> Vec<(String, u64, PathBuf)> {
    let root = base.join("root");
    std::fs::create_dir_all(&root).unwrap();
    let watch = sync_audit::watch(&root);
    let db = open(&root).await;
    assert!(crate::core::wal::wal_for(&path_str(&root)).is_some(), "no log, nothing to test");
    for sql in setup {
        ok(&db, sql).await;
    }
    db.sync_durable().unwrap();

    let old_log = std::fs::read(root.join(DEFAULT_WAL_NAME)).unwrap();
    let baseline = durable_baseline(&root);
    let _ = watch.take_events();
    let harness = std::sync::Arc::new(Mutex::new(Harness {
        base: base.to_path_buf(),
        root: root.clone(),
        baseline,
        old_log,
        watch,
        events: Vec::new(),
        images: Vec::new(),
    }));
    {
        let h = harness.clone();
        let _installed = crate::core::rql::column_declarations::crash_points::install(
            table,
            Box::new(move |point| h.lock().cut_power(point)),
        );
        ok(&db, alter).await;
    }
    harness.lock().cut_power("after");
    drop(db);
    crate::core::wal::remove_for(&path_str(&root));

    let Harness { images, .. } = std::sync::Arc::try_unwrap(harness)
        .ok()
        .expect("the hook is gone")
        .into_inner();
    let mut total = dir_bytes(&root);
    for (_, _, img) in &images {
        total += dir_bytes(img);
    }
    assert!(total < MAX_SCENARIO_BYTES, "the scenario wrote {total} bytes, past its bound");
    // Before any image opens, so nothing an image answers came from it.
    std::fs::remove_dir_all(&root).unwrap();
    images
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_power_cut_inside_rename_column_leaves_it_undone_or_done() {
    init_globals();
    let base = tempfile::tempdir().unwrap();
    let setup = vec![
        format!(
            "CREATE TABLE {TABLE} (id INT, a INT DEFAULT 7, b INT, k INT, CHECK (a > 0), \
             UNIQUE (a, b))"
        ),
        format!("CREATE TABLE {CHILD} (id INT, ra INT REFERENCES {TABLE}(a))"),
        format!("INSERT INTO {TABLE} (id, a, b, k) VALUES (1, NULL, 1, 1)"),
        format!("INSERT INTO {TABLE} (id, a, b, k) VALUES (2, 3, 1, 2)"),
        format!("INSERT INTO {CHILD} (id, ra) VALUES (1, 3)"),
    ];
    let images = crash_images(
        base.path(),
        TABLE,
        &setup,
        &format!("ALTER TABLE {TABLE} RENAME COLUMN a TO z"),
    )
    .await;
    let points: std::collections::BTreeSet<&str> = images.iter().map(|(p, _, _)| p.as_str()).collect();
    assert!(
        points.contains("logged.before_commit") && points.contains("rename.after_commit"),
        "the rename did not reach both sides of its commit ({points:?}), so it was not tested"
    );

    let mut failures = Vec::new();
    let mut seen = Vec::new();
    for (point, seed, img) in images {
        let db = open(&img).await;
        // Before the commit marker is durable nothing of the rename may show;
        // once it is (the commit syncs), all of it must.
        let expected = if point.starts_with("logged.") { "a" } else { "z" };
        match judge(&db).await {
            Ok(name) if name == expected => seen.push(format!("{point}/{seed}: {name}")),
            Ok(name) => failures.push(format!("{point}/seed {seed}: shows {name}, not {expected}")),
            Err(why) => failures.push(format!("{point}/seed {seed}: {why}")),
        }
        drop(db);
        crate::core::wal::remove_for(&path_str(&img));
        std::fs::remove_dir_all(&img).unwrap();
    }
    assert!(
        failures.is_empty(),
        "a power cut inside RENAME COLUMN left it half done:\n{}\n(consistent: {:?})",
        failures.join("\n"),
        seen
    );
}

const DROPPED: &str = "crash_drop_d";

/// What an image of the `DROP COLUMN a` scenario shows: `"kept"` when `a` is
/// live with its CHECK, `"dropped"` when it is gone with every declaration —
/// a column added again under the name is plain — or what is wrong.
async fn judge_drop(db: &Arc<Database>) -> Result<&'static str, String> {
    let live = db
        .tables
        .get(DROPPED)
        .ok_or_else(|| "the table is gone".to_string())?
        .schema
        .fields
        .iter()
        .any(|f| !f.is_deleted && f.name.eq_ignore_ascii_case("a"));
    if live {
        if !is_error(&run(db, &format!("INSERT INTO {DROPPED} (id, a, b) VALUES (9, -1, 9)")).await) {
            return Err("a kept column lost its CHECK".into());
        }
        return Ok("kept");
    }
    let schema = db.tables.get(DROPPED).expect("table").schema.clone();
    let mut wrong = Vec::new();
    if !(schema.defaults.is_empty() && schema.checks.is_empty() && schema.unique_keys.is_empty()) {
        wrong.push(format!(
            "the dropped column's declarations remain: {:?} {:?} {:?}",
            schema.defaults, schema.checks, schema.unique_keys
        ));
    }
    ok(db, &format!("ALTER TABLE {DROPPED} ADD COLUMN a INT")).await;
    for id in [5, 6] {
        let out = run(db, &format!("INSERT INTO {DROPPED} (id, a, b) VALUES ({id}, -1, 1)")).await;
        if is_error(&out) {
            wrong.push(format!("insert {id}: {}", String::from_utf8_lossy(&out[1..])));
        }
    }
    ok(db, &format!("INSERT INTO {DROPPED} (id, b) VALUES (7, 2)")).await;
    let out = run(db, &format!("SELECT a FROM {DROPPED} WHERE id = 7")).await;
    let got: Vec<Option<i64>> = vec_into_rows(&out[1..]).unwrap().iter().map(|r| int(r, 0)).collect();
    if got != vec![None] {
        wrong.push(format!("the new column defaulted to {got:?}"));
    }
    if wrong.is_empty() { Ok("dropped") } else { Err(wrong.join("; ")) }
}

/// The same for `DROP COLUMN`: the field, its companion and its DEFAULT, CHECK
/// and UNIQUE key go in one transaction, or not at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_power_cut_inside_drop_column_leaves_it_undone_or_done() {
    init_globals();
    let base = tempfile::tempdir().unwrap();
    let setup = vec![
        format!("CREATE TABLE {DROPPED} (id INT, a INT DEFAULT 7, b INT, CHECK (a > 0), UNIQUE (a, b))"),
        format!("INSERT INTO {DROPPED} (id, a, b) VALUES (1, 3, 1)"),
    ];
    let images = crash_images(base.path(), DROPPED, &setup, &format!("ALTER TABLE {DROPPED} DROP COLUMN a")).await;
    assert!(
        images.iter().any(|(p, _, _)| p == "logged.before_commit"),
        "the drop never paused before its commit, so it was not tested"
    );
    let mut failures = Vec::new();
    for (point, seed, img) in images {
        let db = open(&img).await;
        let expected = if point.starts_with("logged.") { "kept" } else { "dropped" };
        match judge_drop(&db).await {
            Ok(state) if state == expected => {}
            Ok(state) => failures.push(format!("{point}/seed {seed}: {state}, not {expected}")),
            Err(why) => failures.push(format!("{point}/seed {seed}: {why}")),
        }
        drop(db);
        crate::core::wal::remove_for(&path_str(&img));
        std::fs::remove_dir_all(&img).unwrap();
    }
    assert!(failures.is_empty(), "a power cut inside DROP COLUMN:\n{}", failures.join("\n"));
}
