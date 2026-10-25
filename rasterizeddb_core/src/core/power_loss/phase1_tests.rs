//! Persistent indexes phase 1's power-loss suites (slice B15): the filter
//! and the snapshot lifecycle driven to a crash image at every §11
//! failpoint that touches them, each image reopened and compared with a
//! twin that never restarted.
//!
//! One table with no primary key, so no secondary index and the filter
//! alone prunes (the FILTER-only snapshots of phase 1). It lives either in
//! the database's root, beside the status log, or **on a second storage
//! location with its own WAL and `rdb.ixe`** (§11: in the root the root log
//! covers outcomes by accident). The recorded directory holds the root and
//! the location side by side, so one crash image cuts both.
//!
//! **The twin** is a model of the table fed only the statements the subject
//! acknowledged, never restarted, imaged or filtered. A cut inside a
//! statement may or may not keep it, so such a point carries both states
//! (before and after) and an image must match one of them **as a whole**:
//! its unprunable scan (no `WHERE`) picks the state, and every filtered
//! answer must then be that state's (one state per cut, never per query,
//! P27).
//!
//! **Oracles per image**:
//! - *no lost rows*: the recovered rows (an unprunable scan) equal one of
//!   the point's states exactly;
//! - *answers*: every predicate of a battery (equalities on the touched
//!   rows' old and new values, ranges, `OR`s) answers as that state does;
//! - *superset*: for every visible row and each filtered column, the
//!   filter's candidate mask for an equality on its own value holds the
//!   row's slot, so a load cannot drop codes no query exercised;
//! - *the right state*: whether the open loaded the snapshot (`vouched`)
//!   or rebuilt, as the failpoint's row of §11 says: `Rebuild`, `Load`, or
//!   `Either` where the cut leaves both durable outcomes possible (an
//!   unsynced `CLEAN`, a `DIRTY` in flight).
//!
//! Bounded: ~1,200 rows, `RDB_POWER_LOSS_SEEDS` images per cut (default 5,
//! one per seed kind), `RDB_POWER_LOSS_RUNS` workload seeds (default 1),
//! held bytes and directories checked against `MAX_RUN_BYTES`, temporary
//! directories removed on every path (a panic included, through
//! `TempDir`).

use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc as StdArc;
use std::time::Duration;

use parking_lot::Mutex;
use rclite::Arc;

use super::failpoint::{self, Outcome};
use super::image::{CrashPoint, Recorder, Rng};
use crate::core::database::Database;
use crate::core::index_store::gate_word::GateState;
use crate::core::index_store::seal::SealOutcome;
use crate::core::mvcc::transaction::FROZEN_TX;
use crate::core::row::row::Row;
use crate::core::row::row_pointer::{ROW_POINTER_RECORD_LEN, RowPointerIterator};
use crate::core::row::table::Table;
use crate::core::storage_providers::backend::DbStorageProvider;

type DbTable = Table<DbStorageProvider>;

/// Unique in the crate: the filter's per-table settings are process-wide.
const TABLE: &str = "pl1f";
const DB_NAME: &str = "phase1";
const ROWS: i64 = 1200;
/// Ceiling on what a run may hold: crash points in memory and directories
/// on disk together (the images land in the shared tmpfs).
const MAX_RUN_BYTES: u64 = 384 * 1024 * 1024;
const READY_TIMEOUT: Duration = Duration::from_secs(30);

/// Seal steps, in order, with what a cut there must open as (§11).
const SEAL_STEPS: &[(&str, Expect)] = &[
    ("snapshot_before_fsync", Expect::Rebuild),
    ("snapshot_after_fsync", Expect::Rebuild),
    ("snapshot_after_rename", Expect::Rebuild),
    ("snapshot_after_dir_fsync", Expect::Rebuild),
    ("marker_compaction_after_rename", Expect::Rebuild),
    ("clean_append_before_fsync", Expect::Either),
    ("clean_after_fsync", Expect::Load),
];

pub(super) fn env_count(name: &str, default: u64, max: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
        .clamp(1, max)
}

fn path_str(p: &Path) -> String {
    p.to_string_lossy().into_owned()
}

fn eio() -> io::Error {
    io::Error::from_raw_os_error(libc::EIO)
}

// ---------------------------------------------------------------------------
// The subject's directories
// ---------------------------------------------------------------------------

/// Where the table lives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Place {
    /// The database's root, beside the status log.
    Root,
    /// A second storage location with its own WAL and `rdb.ixe`.
    Second,
}

/// `<base>/root`, and `<base>/disk` for a second location: siblings, since
/// a location may not nest inside the root, under one recorded directory.
#[derive(Clone)]
pub(super) struct Dirs {
    pub base: PathBuf,
    pub place: Place,
}

impl Dirs {
    pub fn root(&self) -> PathBuf {
        self.base.join("root")
    }
    pub fn disk(&self) -> PathBuf {
        self.base.join("disk")
    }
    /// The directory holding the table's files.
    pub fn table_dir(&self) -> PathBuf {
        match self.place {
            Place::Root => self.root(),
            Place::Second => self.disk(),
        }
    }
    pub fn at(&self, base: &Path) -> Dirs {
        Dirs {
            base: base.to_path_buf(),
            place: self.place,
        }
    }
}

pub(super) async fn open(dirs: &Dirs) -> Result<Arc<Database>, String> {
    let _ = crate::core::processor::concurrent_processor::ENABLE_CACHE.set(false);
    std::fs::create_dir_all(dirs.root()).map_err(|e| e.to_string())?;
    Database::try_new_named(&path_str(&dirs.root()), false, DB_NAME).await.map(Arc::new)
}

pub(super) fn close(dirs: &Dirs, db: Arc<Database>) {
    drop(db);
    crate::core::wal::remove_for(&path_str(&dirs.root()));
    crate::core::wal::remove_for(&path_str(&dirs.disk()));
}

/// Opens the subject for the first time: the location added and the table
/// pinned to it before it exists.
async fn create(dirs: &Dirs) -> Arc<Database> {
    let db = open(dirs).await.expect("open the subject");
    if dirs.place == Place::Second {
        #[cfg(feature = "storage_pools")]
        {
            let disk = path_str(&dirs.disk());
            db.storage
                .add_location(&disk, crate::core::storage::LocationRole::Primary, 1, true)
                .expect("add the second location");
            db.storage.pin(TABLE, &disk).expect("pin the table to it");
        }
    }
    // The filter is consulted from the first row, not from 4,096: the
    // suite's table stays small. Set before the table exists, so every
    // open (images included) builds the same configuration, whose hash the
    // snapshot's identity records.
    crate::core::filter::settings::settings().update_table(TABLE, |o| o.min_table_rows = Some(0));
    exec(&db, &format!("CREATE TABLE {TABLE} (id BIGINT, a BIGINT, b BIGINT, s TEXT, f DOUBLE)"))
        .await
        .expect("create the table");
    db
}

pub(super) async fn exec(db: &Arc<Database>, sql: &str) -> Result<Vec<u8>, String> {
    exec_sync(db, sql, true).await
}

/// `wait` is the effective `synchronous_commit`.
pub(super) async fn exec_sync(db: &Arc<Database>, sql: &str, wait: bool) -> Result<Vec<u8>, String> {
    let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(sql).ok_or_else(|| format!("cannot parse {sql}"))?;
    let out = crate::core::rql::executor::execute_with_sync(purpose, db.clone(), wait).await;
    if out.is_empty() || out[0] == 3 {
        return Err(format!("{sql}: {}", String::from_utf8_lossy(&out)));
    }
    Ok(out)
}

async fn exec_in(db: &Arc<Database>, tx: crate::core::mvcc::TxId, sql: &str) -> Result<(), String> {
    let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(sql).ok_or_else(|| format!("cannot parse {sql}"))?;
    let out = Box::pin(crate::core::rql::executor::execute_with_context(purpose, db.clone(), 0, tx)).await;
    if out.is_empty() || out[0] == 3 {
        return Err(format!("{sql}: {}", String::from_utf8_lossy(&out)));
    }
    Ok(())
}

pub(super) fn table(db: &Database) -> Arc<DbTable> {
    db.table_for_tests(TABLE).expect("the table")
}

pub(super) async fn ready(t: &DbTable) -> Result<(), String> {
    let deadline = std::time::Instant::now() + READY_TIMEOUT;
    while !t.filters_ready().map_err(|e| e.to_string())? {
        if std::time::Instant::now() > deadline {
            return Err("the filter never became ready".into());
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    Ok(())
}

async fn seal(db: &Arc<Database>) -> SealOutcome {
    // A rebuilt filter is ineligible until its backfill finished.
    ready(&table(db)).await.expect("the filter before a seal");
    let mut sealed = db.index_checkpoint(TABLE, None).await.expect("rdb_index_checkpoint").sealed;
    assert_eq!(sealed.len(), 1);
    sealed.remove(0).outcome
}

async fn seal_published(db: &Arc<Database>, why: &str) {
    let outcome = seal(db).await;
    assert!(matches!(outcome, SealOutcome::Published), "{why}: the seal did not publish: {outcome:?}");
}

/// A clean point: published now, or already clean and vouched.
async fn clean_point(db: &Arc<Database>, why: &str) {
    let outcome = seal(db).await;
    assert!(
        matches!(outcome, SealOutcome::Published | SealOutcome::AlreadyClean),
        "{why}: no clean point: {outcome:?}"
    );
}

fn dir_bytes(dir: &Path) -> u64 {
    super::image::walk(dir)
        .iter()
        .map(|r| std::fs::metadata(dir.join(r)).map(|m| m.len()).unwrap_or(0))
        .sum()
}

// ---------------------------------------------------------------------------
// The model twin and its predicates
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub(super) struct ModelRow {
    pub a: i64,
    pub b: i64,
    pub s: String,
    pub f: f64,
}

impl ModelRow {
    pub fn for_id(id: i64) -> ModelRow {
        ModelRow {
            a: (id * 7919) % 1000,
            b: id / 10,
            s: format!("s{}", id % 97),
            f: (id % 50) as f64 * 0.5,
        }
    }
    pub fn values(&self, id: i64) -> String {
        format!("({id}, {}, {}, '{}', {:.1})", self.a, self.b, self.s, self.f)
    }
}

/// The table as the subject acknowledged it.
#[derive(Debug, Clone, Default, PartialEq)]
pub(super) struct Model {
    pub rows: BTreeMap<i64, ModelRow>,
}

/// One predicate the battery asks, in SQL and in the model.
#[derive(Debug, Clone)]
pub(super) enum Pred {
    AEq(i64),
    BEq(i64),
    SEq(String),
    FEq(f64),
    ARange(i64, i64),
    BLess(i64),
    AOrB(i64, i64),
    AAndS(i64, String),
}

impl Pred {
    pub fn sql(&self) -> String {
        match self {
            Pred::AEq(v) => format!("a = {v}"),
            Pred::BEq(v) => format!("b = {v}"),
            Pred::SEq(v) => format!("s = '{v}'"),
            Pred::FEq(v) => format!("f = {v:.1}"),
            Pred::ARange(lo, hi) => format!("a BETWEEN {lo} AND {hi}"),
            Pred::BLess(v) => format!("b < {v}"),
            Pred::AOrB(a, b) => format!("a = {a} OR b = {b}"),
            Pred::AAndS(a, s) => format!("a > {a} AND s = '{s}'"),
        }
    }
    pub fn matches(&self, r: &ModelRow) -> bool {
        match self {
            Pred::AEq(v) => r.a == *v,
            Pred::BEq(v) => r.b == *v,
            Pred::SEq(v) => r.s == *v,
            Pred::FEq(v) => r.f == *v,
            Pred::ARange(lo, hi) => r.a >= *lo && r.a <= *hi,
            Pred::BLess(v) => r.b < *v,
            Pred::AOrB(a, b) => r.a == *a || r.b == *b,
            Pred::AAndS(a, s) => r.a > *a && r.s == *s,
        }
    }
}

impl Model {
    pub fn answer(&self, p: &Pred) -> Vec<i64> {
        self.rows.iter().filter(|(_, r)| p.matches(r)).map(|(id, _)| *id).collect()
    }

    /// A seeded battery, plus equalities on every value in `touched` (the
    /// old and new values of the rows the last statements changed).
    pub fn battery(&self, rng: &mut Rng, touched: &[ModelRow]) -> Vec<Pred> {
        let mut out = Vec::new();
        let ids: Vec<i64> = self.rows.keys().copied().collect();
        let pick = |rng: &mut Rng| -> Option<&ModelRow> {
            if ids.is_empty() {
                None
            } else {
                self.rows.get(&ids[rng.below(ids.len() as u64) as usize])
            }
        };
        for _ in 0..14 {
            let Some(r) = pick(rng).cloned() else { break };
            out.push(match rng.below(8) {
                0 => Pred::AEq(r.a),
                1 => Pred::BEq(r.b),
                2 => Pred::SEq(r.s.clone()),
                3 => Pred::FEq(r.f),
                4 => Pred::ARange(r.a, r.a + rng.below(20) as i64),
                5 => Pred::BLess(r.b),
                6 => Pred::AOrB(r.a, r.b + 3),
                _ => Pred::AAndS(r.a.saturating_sub(100), r.s.clone()),
            });
        }
        // Values no row holds: a filter that answers them with rows is
        // wrong the other way, which the answers catch too.
        out.push(Pred::AEq(123_456_789));
        out.push(Pred::SEq("absent".into()));
        for r in touched {
            out.push(Pred::AEq(r.a));
            out.push(Pred::BEq(r.b));
            out.push(Pred::SEq(r.s.clone()));
        }
        out
    }
}

// ---------------------------------------------------------------------------
// Checking one opened database against the twin
// ---------------------------------------------------------------------------

/// What an image must open as.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Expect {
    Load,
    Rebuild,
    Either,
}

/// Every row, decoded, by an unprunable scan.
async fn scan(db: &Arc<Database>) -> Result<BTreeMap<i64, ModelRow>, String> {
    let out = exec(db, &format!("SELECT id, a, b, s, f FROM {TABLE}")).await?;
    let rows = decode(&out)?;
    let mut map = BTreeMap::new();
    for r in rows {
        let col = |i: usize| r.columns[i].data.into_slice().to_vec();
        let i64_of = |b: Vec<u8>| -> Result<i64, String> {
            Ok(i64::from_le_bytes(b.as_slice().try_into().map_err(|_| format!("not an i64: {b:?}"))?))
        };
        let id = i64_of(col(0))?;
        let row = ModelRow {
            a: i64_of(col(1))?,
            b: i64_of(col(2))?,
            s: String::from_utf8_lossy(&col(3)).into_owned(),
            f: f64::from_le_bytes(col(4).as_slice().try_into().map_err(|_| "not an f64".to_string())?),
        };
        if map.insert(id, row).is_some() {
            return Err(format!("id {id} is visible twice"));
        }
    }
    Ok(map)
}

fn decode(out: &[u8]) -> Result<Vec<Row>, String> {
    if out.first() != Some(&2) {
        return Err(format!("not a row set: {}", String::from_utf8_lossy(out)));
    }
    crate::core::row::row::vec_into_rows(&out[1..]).map_err(|e| e.to_string())
}

pub(super) async fn ids(db: &Arc<Database>, clause: &str) -> Result<Vec<i64>, String> {
    let out = exec(db, &format!("SELECT id FROM {TABLE} WHERE {clause}")).await?;
    let mut v: Vec<i64> = decode(&out)?
        .iter()
        .map(|r| i64::from_le_bytes(r.columns[0].data.into_slice().try_into().unwrap_or([0xFF; 8])))
        .collect();
    v.sort_unstable();
    Ok(v)
}

/// Every visible row's slot and values, read under a fresh snapshot.
async fn visible_slots(t: &DbTable) -> Result<Vec<(u64, ModelRow)>, String> {
    let d = t.data().map_err(|e| e.to_string())?;
    let _read = d.readers.enter();
    let visibility = t.visibility_for(FROZEN_TX);
    let cols: Vec<u32> = t.schema.fields.iter().map(|f| f.write_order as u32).collect();
    let fetch = DbTable::build_fetch_for(&t.schema.fields, &cols);
    let wo = |name: &str| t.schema.fields.iter().find(|f| f.name == name).map(|f| f.write_order as u32);
    let (wa, wb, ws, wf) = (wo("a").unwrap(), wo("b").unwrap(), wo("s").unwrap(), wo("f").unwrap());
    let mut it = RowPointerIterator::new(d.io_pointers.clone()).await.map_err(|e| format!("{e:?}"))?;
    let mut row = Row::default();
    let mut out = Vec::new();
    while let Some((offset, pointer)) = it.next_row_pointer_with_offset().await.map_err(|e| format!("{e:?}"))? {
        if !visibility.visible(pointer.deleted, pointer.xmin, pointer.xmax) {
            continue;
        }
        let slot = offset / ROW_POINTER_RECORD_LEN as u64;
        pointer
            .fetch_row_reuse_async(&*d.io_rows, &fetch, &mut row)
            .await
            .map_err(|e| format!("visible slot {slot} could not be read: {e:?}"))?;
        let get = |w: u32| row.columns.iter().find(|c| c.schema_id as u32 == w).map(|c| c.data.into_slice().to_vec());
        let i64_at = |w: u32| get(w).and_then(|b| b.as_slice().try_into().ok()).map(i64::from_le_bytes).unwrap_or(i64::MIN);
        out.push((
            slot,
            ModelRow {
                a: i64_at(wa),
                b: i64_at(wb),
                s: String::from_utf8_lossy(&get(ws).unwrap_or_default()).into_owned(),
                f: get(wf).and_then(|b| b.as_slice().try_into().ok()).map(f64::from_le_bytes).unwrap_or(f64::NAN),
            },
        ));
    }
    Ok(out)
}

/// The superset oracle: for every visible row and each filtered column,
/// an equality on the row's own value keeps its slot a candidate. Returns
/// how many probes the filter narrowed.
async fn superset(t: &DbTable, label: &str, problems: &mut Vec<String>) -> usize {
    let rows = match visible_slots(t).await {
        Ok(r) => r,
        Err(e) => {
            problems.push(format!("{label}: containment: {e}"));
            return 0;
        }
    };
    let mut by: BTreeMap<String, Vec<u64>> = BTreeMap::new();
    for (slot, r) in &rows {
        for clause in [Pred::AEq(r.a).sql(), Pred::BEq(r.b).sql(), Pred::SEq(r.s.clone()).sql()] {
            by.entry(clause).or_default().push(*slot);
        }
    }
    let mut narrowed = 0;
    for (clause, slots) in by {
        let Ok(Some(mask)) = t.build_candidates(&clause, None).await else { continue };
        narrowed += 1;
        for slot in slots {
            if !mask.get(slot) {
                problems.push(format!("{label}: the filter prunes visible slot {slot} from `{clause}`"));
            }
        }
    }
    narrowed
}

/// What one check saw.
#[derive(Debug, Default, Clone, Copy)]
pub(super) struct Seen {
    pub loaded: bool,
    pub narrowed: usize,
}

/// Compares an opened subject with whichever of `states` its rows match.
pub(super) async fn check(
    db: &Arc<Database>,
    label: &str,
    states: &[Model],
    preds: &[Pred],
    expect: Expect,
    problems: &mut Vec<String>,
) -> Seen {
    let Some(t) = db.table_for_tests(TABLE) else {
        problems.push(format!("{label}: the table is gone"));
        return Seen::default();
    };
    let loaded = t.gate().vouched();
    match (expect, loaded) {
        (Expect::Load, false) => problems.push(format!("{label}: expected the snapshot to load, the open rebuilt")),
        (Expect::Rebuild, true) => problems.push(format!("{label}: expected a rebuild, the open loaded the snapshot")),
        _ => {}
    }
    if let Err(e) = ready(&t).await {
        problems.push(format!("{label}: {e}"));
        return Seen { loaded, narrowed: 0 };
    }
    let got = match scan(db).await {
        Ok(r) => r,
        Err(e) => {
            problems.push(format!("{label}: scan: {e}"));
            return Seen { loaded, narrowed: 0 };
        }
    };
    let Some(state) = states.iter().find(|s| s.rows == got) else {
        let diffs: Vec<String> = states
            .iter()
            .map(|s| {
                let extra: Vec<i64> = got.iter().filter(|(k, v)| s.rows.get(k) != Some(v)).map(|(k, _)| *k).take(12).collect();
                let missing: Vec<i64> = s.rows.iter().filter(|(k, v)| got.get(k) != Some(v)).map(|(k, _)| *k).take(12).collect();
                format!("vs {} twin rows: extra or changed {extra:?}, missing or changed {missing:?}", s.rows.len())
            })
            .collect();
        problems.push(format!(
            "{label}: the recovered rows ({}) match no state the twin passed through: {}",
            got.len(),
            diffs.join("; ")
        ));
        return Seen { loaded, narrowed: 0 };
    };
    for p in preds {
        let want = state.answer(p);
        match ids(db, &p.sql()).await {
            Ok(g) if g == want => {}
            Ok(g) => problems.push(format!(
                "{label} ({}): WHERE {}: got {} ids {:?}, twin {} ids {:?}",
                if loaded { "loaded" } else { "rebuilt" },
                p.sql(),
                g.len(),
                g.iter().take(8).collect::<Vec<_>>(),
                want.len(),
                want.iter().take(8).collect::<Vec<_>>()
            )),
            Err(e) => problems.push(format!("{label}: WHERE {}: {e}", p.sql())),
        }
    }
    let narrowed = superset(&t, label, problems).await;
    Seen { loaded, narrowed }
}

// ---------------------------------------------------------------------------
// Points and a run
// ---------------------------------------------------------------------------

pub(super) struct Point {
    pub cp: CrashPoint,
    pub states: Vec<Model>,
    pub preds: Vec<Pred>,
    pub expect: Expect,
}

#[derive(Debug, Default)]
pub(super) struct Tally {
    pub points: usize,
    pub disturbed: usize,
    pub images: usize,
    pub loads: usize,
    pub rebuilds: usize,
    pub narrowed: usize,
    /// Point label prefixes (failpoint or scenario names) that were imaged.
    pub covered: std::collections::BTreeSet<String>,
}

/// Collects crash points with what they must open as.
struct Run {
    dirs: Dirs,
    db: Option<Arc<Database>>,
    rec: StdArc<Recorder>,
    model: Model,
    rng: Rng,
    next_id: i64,
    points: Vec<Point>,
    tally: Tally,
    problems: Vec<String>,
    held: u64,
    run_seed: u64,
}

type Sink = StdArc<Mutex<Vec<Result<CrashPoint, String>>>>;

impl Run {
    fn db(&self) -> &Arc<Database> {
        self.db.as_ref().expect("the subject is open")
    }

    fn label(&self, what: &str) -> String {
        format!("{what} [{:?} run {}]", self.dirs.place, self.run_seed)
    }

    async fn insert(&mut self, n: i64) {
        let from = self.next_id;
        let values: Vec<String> = (from..from + n).map(|i| ModelRow::for_id(i).values(i)).collect();
        exec(self.db(), &format!("INSERT INTO {TABLE} (id, a, b, s, f) VALUES {}", values.join(", ")))
            .await
            .unwrap();
        for i in from..from + n {
            self.model.rows.insert(i, ModelRow::for_id(i));
        }
        self.next_id += n;
    }

    /// A live id, chosen by the run's generator.
    fn some_id(&mut self) -> i64 {
        let ids: Vec<i64> = self.model.rows.keys().copied().collect();
        ids[self.rng.below(ids.len() as u64) as usize]
    }

    /// The statement for an in-place UPDATE (a fixed-width column of one
    /// row, a value no row had), and the model after it.
    fn in_place(&mut self) -> (String, Model, Vec<ModelRow>) {
        let id = self.some_id();
        let old = self.model.rows[&id].clone();
        let fresh = 500_000 + self.rng.below(400_000) as i64;
        let mut after = self.model.clone();
        after.rows.get_mut(&id).unwrap().a = fresh;
        let new = after.rows[&id].clone();
        (format!("UPDATE {TABLE} SET a = {fresh} WHERE id = {id}"), after, vec![old, new])
    }

    fn delete_some(&mut self, n: usize) -> (String, Model, Vec<ModelRow>) {
        let mut ids: Vec<i64> = (0..n).map(|_| self.some_id()).collect();
        ids.sort_unstable();
        ids.dedup();
        let mut after = self.model.clone();
        let touched: Vec<ModelRow> = ids.iter().map(|i| after.rows.remove(i).unwrap()).collect();
        let list: Vec<String> = ids.iter().map(i64::to_string).collect();
        (format!("DELETE FROM {TABLE} WHERE id IN ({})", list.join(", ")), after, touched)
    }

    fn battery(&mut self, states: &[&Model], touched: &[ModelRow]) -> Vec<Pred> {
        let mut out = Vec::new();
        for s in states {
            out.extend(s.battery(&mut self.rng, touched));
        }
        out
    }

    fn arm(&self, name: &str, sink: &Sink) -> failpoint::Guard {
        failpoint::arm(name, &self.dirs.base, 1, self.rec.cut_action(name.to_string(), sink.clone()))
    }

    fn arm_nth(&self, name: &str, nth: u64, label: &str, sink: &Sink) -> failpoint::Guard {
        failpoint::arm(name, &self.dirs.base, nth, self.rec.cut_action(label.to_string(), sink.clone()))
    }

    /// Moves what `sink` collected into points, each with its expectation.
    fn collect(&mut self, sink: &Sink, states: &[&Model], preds: &[Pred], expect_of: impl Fn(&str) -> Expect) {
        for cut in std::mem::take(&mut *sink.lock()) {
            match cut {
                Ok(cp) => {
                    let expect = expect_of(&cp.label);
                    self.push(cp, states, preds, expect);
                }
                Err(why) => {
                    eprintln!("{why}");
                    self.tally.disturbed += 1;
                }
            }
        }
    }

    fn push(&mut self, cp: CrashPoint, states: &[&Model], preds: &[Pred], expect: Expect) {
        self.held += cp.held_bytes();
        self.tally.points += 1;
        self.points.push(Point {
            cp,
            states: states.iter().map(|s| (*s).clone()).collect(),
            preds: preds.to_vec(),
            expect,
        });
        let on_disk = dir_bytes(&self.dirs.base);
        assert!(
            self.held + on_disk < MAX_RUN_BYTES,
            "the run holds {} bytes, past its bound",
            self.held + on_disk
        );
    }

    fn quiescent(&mut self, what: &str, preds: &[Pred], expect: Expect) {
        let label = self.label(what);
        match self.rec.cut_quiescent(label) {
            Ok(cp) => {
                let model = self.model.clone();
                self.push(cp, &[&model], preds, expect);
            }
            Err(why) => {
                eprintln!("{why}");
                self.tally.disturbed += 1;
            }
        }
    }

    /// Closes and reopens the subject (no clean point: an abandoned close).
    async fn reopen(&mut self) {
        let db = self.db.take().unwrap();
        close(&self.dirs, db);
        self.db = Some(open(&self.dirs).await.unwrap_or_else(|e| panic!("reopen: {e}")));
    }

    fn state(&self) -> GateState {
        table(self.db()).gate().state()
    }

    /// Checks the live subject, as an image is checked.
    async fn check_live(&mut self, what: &str, expect: Expect) {
        let preds = self.battery(&[&self.model.clone()], &[]);
        let label = self.label(what);
        let db = self.db().clone();
        let model = self.model.clone();
        let seen = check(&db, &label, &[model], &preds, expect, &mut self.problems).await;
        self.tally.narrowed += seen.narrowed;
    }
}

/// A seal with every step's cut armed, plus a quiescent cut after it.
async fn seal_with_cuts(run: &mut Run, which: &str) {
    let sink: Sink = StdArc::new(Mutex::new(Vec::new()));
    let guards: Vec<failpoint::Guard> = SEAL_STEPS.iter().map(|(n, _)| run.arm(n, &sink)).collect();
    seal_published(run.db(), which).await;
    drop(guards);
    let model = run.model.clone();
    let preds = run.battery(&[&model], &[]);
    let label = run.label(which);
    run.collect(&sink, &[&model], &preds, |cut| {
        SEAL_STEPS
            .iter()
            .find(|(n, _)| cut.starts_with(n))
            .map(|(_, e)| *e)
            .unwrap_or(Expect::Either)
    });
    // Relabel the points just collected with the scenario.
    let n = SEAL_STEPS.len().min(run.points.len());
    for p in run.points.iter_mut().rev().take(n) {
        if !p.cp.label.contains('[') {
            p.cp.label = format!("{} ({label})", p.cp.label);
        }
    }
    run.quiescent(&format!("clean_after_publish, {which}"), &preds, Expect::Load);
}

/// The first write after a clean point, in each of the four variants of
/// §11 (RV26): an in-place UPDATE (length-preserving), a vacuum forget with
/// its filter clear, `neutralize_aborted`, and a DELETE as the control. Cut
/// in the `DIRTY` append (unsynced), after it, at the statement's log sync
/// and after the statement returned.
async fn first_writes(run: &mut Run) {
    for variant in ["update_in_place", "delete", "vacuum_clear", "neutralize_aborted"] {
        // What the variant needs before the clean point.
        match variant {
            "vacuum_clear" => {
                // Versions dead before the snapshot, still coded in it; the
                // vacuum after it clears them.
                let (sql, after, _) = run.delete_some(25);
                exec(run.db(), &sql).await.unwrap();
                run.model = after;
            }
            "neutralize_aborted" => {
                let db = run.db().clone();
                let tx = db.transactions.begin();
                let id = run.next_id + 1_000_000;
                exec_in(&db, tx, &format!("INSERT INTO {TABLE} (id, a, b, s, f) VALUES {}", ModelRow::for_id(id).values(id)))
                    .await
                    .unwrap();
                db.abort_transaction(tx);
                db.transactions.flush().unwrap();
            }
            _ => {}
        }
        clean_point(run.db(), &format!("before {variant}")).await;
        let before = run.model.clone();
        let (after, touched) = match variant {
            "update_in_place" => {
                let (sql, after, touched) = run.in_place();
                (Some((sql, after)), touched)
            }
            "delete" => {
                let (sql, after, touched) = run.delete_some(3);
                (Some((sql, after)), touched)
            }
            _ => (None, Vec::new()),
        };
        let after_model = after.as_ref().map(|(_, m)| m.clone()).unwrap_or_else(|| before.clone());
        let preds = run.battery(&[&before, &after_model], &touched);
        let sink: Sink = StdArc::new(Mutex::new(Vec::new()));
        let guards = vec![
            run.arm("dirty_append_before_fsync", &sink),
            run.arm("dirty_after_fsync", &sink),
            run.arm_nth("sync:wal.log", 1, &format!("first_write_after_snapshot/{variant} at its log sync"), &sink),
        ];
        match variant {
            "vacuum_clear" => {
                let t = table(run.db());
                let report = t.vacuum_now().await.unwrap();
                eprintln!("{}: vacuum cleared {} fingerprints", run.label(variant), report.fingerprints_cleared);
            }
            "neutralize_aborted" => {
                let t = table(run.db());
                let report = t.neutralize_aborted_now().await.unwrap();
                eprintln!("{}: neutralize {report:?}", run.label(variant));
            }
            _ => {
                let (sql, _) = after.as_ref().unwrap();
                exec(run.db(), sql).await.unwrap();
            }
        }
        drop(guards);
        run.model = after_model.clone();
        // A writer's DIRTY is durable before its first write is issued, so
        // every cut a statement's write may precede must rebuild; a cut in
        // the unsynced DIRTY may still prove the CLEAN when nothing was
        // written. Maintenance passes that found nothing take no ticket
        // and may leave the snapshot loadable, which the twin judges.
        let statement = after.is_some();
        let label = run.label(&format!("first_write_after_snapshot/{variant}"));
        run.collect(&sink, &[&before, &after_model], &preds, |cut| {
            if cut.starts_with("dirty_after_fsync") || (statement && cut.contains("log sync")) {
                Expect::Rebuild
            } else {
                Expect::Either
            }
        });
        for p in run.points.iter_mut().rev().take(3) {
            if !p.cp.label.contains('[') {
                p.cp.label = format!("{} ({label})", p.cp.label);
            }
        }
        let wrote = run.state() != GateState::Clean;
        assert!(
            !statement || wrote,
            "{label}: an acknowledged write left the word Clean"
        );
        run.quiescent(
            &format!("first_write_after_snapshot/{variant}, after it returned"),
            &preds,
            if wrote { Expect::Rebuild } else { Expect::Load },
        );
    }
}

/// `clean_fsync_fails` then `unknown_rewrite_after_first_write`: the
/// `CLEAN`'s fdatasync returns EIO (its page may persist), so the word is
/// `Unknown`, and the next write rewrites the log before it is issued.
async fn clean_fsync_fails(run: &mut Run) {
    let (sql, after, touched) = run.in_place();
    // Something to seal: the word must be Dirty.
    exec(run.db(), &format!("UPDATE {TABLE} SET f = f WHERE id = -1")).await.unwrap();
    let fail = failpoint::arm("clean_fsync_fails", &run.dirs.base, 1, StdArc::new(|_: &failpoint::Hit<'_>| Outcome::Fail(eio())));
    let outcome = seal(run.db()).await;
    drop(fail);
    assert!(matches!(outcome, SealOutcome::FailedAfterClean(_)), "clean_fsync_fails: {outcome:?}");
    if run.state() != GateState::Unknown {
        let why = format!("{}: a failed CLEAN sync left {:?}, not Unknown", run.label("clean_fsync_fails"), run.state());
        run.problems.push(why);
    }
    let before = run.model.clone();
    let preds = run.battery(&[&before, &after], &touched);
    let sink: Sink = StdArc::new(Mutex::new(Vec::new()));
    let guard = run.arm("unknown_rewrite_after_first_write", &sink);
    exec(run.db(), &sql).await.unwrap();
    drop(guard);
    run.model = after.clone();
    let label = run.label("clean_fsync_fails");
    // Nothing is issued before the rewrite: an unrenamed rewrite leaves the
    // old log, whose CLEAN (kept or not) still describes the data.
    run.collect(&sink, &[&before, &after], &preds, |_| Expect::Either);
    if let Some(p) = run.points.last_mut()
        && !p.cp.label.contains('[')
    {
        p.cp.label = format!("{} ({label})", p.cp.label);
    }
    run.quiescent("clean_fsync_fails, then a length-preserving write", &preds, Expect::Rebuild);
}

/// `barrier_root_sync_fails` and `barrier_dir_sync_fails`: the step-4
/// barrier fails, nothing is published, the log still ends `DIRTY`.
async fn barrier_fails(run: &mut Run) {
    for name in ["barrier_root_sync_fails", "barrier_dir_sync_fails"] {
        let (sql, after, touched) = run.in_place();
        exec(run.db(), &sql).await.unwrap();
        run.model = after;
        let fail = failpoint::arm(name, &run.dirs.base, 1, StdArc::new(|_: &failpoint::Hit<'_>| Outcome::Fail(eio())));
        let outcome = seal(run.db()).await;
        let fired = fail.fired();
        drop(fail);
        assert_eq!(fired, 1, "{name} never fired");
        assert!(matches!(outcome, SealOutcome::Failed(_)), "{name}: {outcome:?}");
        let preds = run.battery(&[&run.model.clone()], &touched);
        run.quiescent(&format!("{name}, the seal failed"), &preds, Expect::Rebuild);
    }
}

/// `dirty_enospc_after_first_write`: the `DIRTY` append hits ENOSPC, so the
/// slow path unlinks `.ixs` and fsyncs the directory, and the write goes
/// ahead.
async fn dirty_enospc(run: &mut Run) {
    clean_point(run.db(), "before dirty_enospc").await;
    let ixs = run.dirs.table_dir().join(format!("{TABLE}.ixs"));
    assert!(ixs.exists());
    let (sql, after, touched) = run.in_place();
    let fail = failpoint::arm(
        "dirty_enospc_after_first_write",
        &run.dirs.base,
        1,
        StdArc::new(|_: &failpoint::Hit<'_>| Outcome::Fail(io::Error::from_raw_os_error(libc::ENOSPC))),
    );
    exec(run.db(), &sql).await.unwrap();
    assert_eq!(fail.fired(), 1, "the DIRTY append never hit its ENOSPC");
    drop(fail);
    assert!(!ixs.exists(), "ENOSPC on DIRTY left the snapshot in place");
    run.model = after;
    let preds = run.battery(&[&run.model.clone()], &touched);
    run.quiescent("dirty_enospc_after_first_write, the write went ahead", &preds, Expect::Rebuild);
}

/// `rdb.ixe` and `invalidate_after_first_compaction_batch`: a `VACUUM FULL`
/// from a clean point, cut at its log syncs (the epoch record, the move
/// batches) and after it. No row's visibility changes, so one state.
async fn vacuum_full(run: &mut Run) {
    let (sql, after, _) = run.delete_some(200);
    exec(run.db(), &sql).await.unwrap();
    run.model = after;
    // The dead versions are reclaimed by the compaction, not a sweep.
    clean_point(run.db(), "before VACUUM FULL").await;
    let model = run.model.clone();
    let preds = run.battery(&[&model], &[]);
    let sink: Sink = StdArc::new(Mutex::new(Vec::new()));
    let guards: Vec<failpoint::Guard> = [1u64, 2, 4]
        .iter()
        .map(|n| {
            run.arm_nth(
                "sync:wal.log",
                *n,
                &format!("invalidate_after_first_compaction_batch at log sync {n}"),
                &sink,
            )
        })
        .collect();
    exec(run.db(), &format!("VACUUM FULL {TABLE}")).await.unwrap();
    drop(guards);
    let label = run.label("VACUUM FULL");
    // Before the compaction's ticket nothing moved and the snapshot may
    // load; after it, DIRTY and the epoch are durable. The twin judges.
    run.collect(&sink, &[&model], &preds, |_| Expect::Either);
    for p in run.points.iter_mut().rev().take(3) {
        if !p.cp.label.contains('[') {
            p.cp.label = format!("{} ({label})", p.cp.label);
        }
    }
    assert!(
        run.dirs.table_dir().join("rdb.ixe").exists(),
        "the compaction's epoch record is not in the table's own directory"
    );
    run.quiescent("invalidate_after_first_compaction_batch, after VACUUM FULL", &preds, Expect::Rebuild);
    run.check_live("live after VACUUM FULL", Expect::Either).await;
}

/// `ixm_read_eio_at_open` (no power cut, and a cut after): the open cannot
/// read `.ixm`, so the word starts `Unknown`; a length-preserving write
/// rewrites the log first; an abandoned close (no clean point) and a
/// reopen must rebuild.
async fn ixm_read_eio(run: &mut Run) {
    clean_point(run.db(), "before ixm_read_eio_at_open").await;
    let db = run.db.take().unwrap();
    close(&run.dirs, db);
    let fail = failpoint::arm("ixm_read_eio_at_open", &run.dirs.base, 1, StdArc::new(|_: &failpoint::Hit<'_>| Outcome::Fail(eio())));
    run.db = Some(open(&run.dirs).await.unwrap());
    assert_eq!(fail.fired(), 1, "ixm_read_eio_at_open never fired");
    drop(fail);
    if run.state() != GateState::Unknown {
        let why = format!("{}: an unreadable marker log started {:?}, not Unknown", run.label("ixm_read_eio_at_open"), run.state());
        run.problems.push(why);
    }
    assert!(!table(run.db()).gate().vouched(), "an unreadable marker log proved a snapshot");
    let (sql, after, touched) = run.in_place();
    exec(run.db(), &sql).await.unwrap();
    run.model = after;
    let preds = run.battery(&[&run.model.clone()], &touched);
    run.quiescent("ixm_read_eio_at_open, then a length-preserving write", &preds, Expect::Rebuild);
    run.reopen().await;
    run.check_live("ixm_read_eio_at_open, abandoned close and reopen", Expect::Rebuild).await;
}

/// `marker_rot_after_clean`: a durable `DIRTY` after `CLEAN` rots (its
/// bytes no longer read); bytes still follow the `CLEAN`, so nothing
/// proves it. The next write's append overwrites the rot; cut while that
/// append is unsynced and after it.
async fn marker_rot(run: &mut Run) {
    clean_point(run.db(), "before marker_rot_after_clean").await;
    let (sql, after, mut touched) = run.in_place();
    exec(run.db(), &sql).await.unwrap();
    run.model = after;
    let db = run.db.take().unwrap();
    close(&run.dirs, db);
    let ixm = run.dirs.table_dir().join(format!("{TABLE}.ixm"));
    let mut log = std::fs::read(&ixm).unwrap();
    let last = log.len() - 1;
    log[last] ^= 0x5A;
    std::fs::write(&ixm, &log).unwrap();
    std::fs::File::open(&ixm).unwrap().sync_all().unwrap();
    run.db = Some(open(&run.dirs).await.unwrap());
    assert!(!table(run.db()).gate().vouched(), "a CLEAN followed by rotten bytes proved a snapshot");
    let before = run.model.clone();
    let (sql, after, more) = run.in_place();
    touched.extend(more);
    let preds = run.battery(&[&before, &after], &touched);
    let sink: Sink = StdArc::new(Mutex::new(Vec::new()));
    let guard = run.arm_nth("dirty_append_before_fsync", 1, "marker_rot_after_clean, the next append unsynced", &sink);
    exec(run.db(), &sql).await.unwrap();
    drop(guard);
    run.model = after.clone();
    run.collect(&sink, &[&before, &after], &preds, |_| Expect::Rebuild);
    run.quiescent("marker_rot_after_clean, after the next write", &preds, Expect::Rebuild);
}

/// The pair as it stood after a seal, and a filter image as a pre-v1
/// binary's `_flt.db`.
fn stale_pair_and_flt(dir: &Path, pair: &(Vec<u8>, Vec<u8>)) {
    std::fs::write(dir.join(format!("{TABLE}.ixs")), &pair.0).unwrap();
    std::fs::write(dir.join(format!("{TABLE}.ixm")), &pair.1).unwrap();
    let mut flt = b"RDBFLT01".to_vec();
    flt.extend_from_slice(&[1u8; 56]);
    std::fs::write(dir.join(format!("{TABLE}_flt.db")), &flt).unwrap();
    for f in [format!("{TABLE}.ixs"), format!("{TABLE}.ixm"), format!("{TABLE}_flt.db")] {
        std::fs::File::open(dir.join(f)).unwrap().sync_all().unwrap();
    }
    std::fs::File::open(dir).unwrap().sync_all().unwrap();
}

/// `flt_evidence_before_ixs_unlink` and `unknown_flt_delete_order`: an old
/// binary changed rows in place after the clean point and left `_flt.db`
/// beside the old pair, whose `CLEAN` still matches the files' lengths.
/// The evidence must win.
async fn flt_evidence(run: &mut Run) {
    for (step, unlink_fails) in [("flt_evidence_before_ixs_unlink", false), ("unknown_flt_delete_order", true)] {
        clean_point(run.db(), &format!("before {step}")).await;
        let dir = run.dirs.table_dir();
        let pair = (
            std::fs::read(dir.join(format!("{TABLE}.ixs"))).unwrap(),
            std::fs::read(dir.join(format!("{TABLE}.ixm"))).unwrap(),
        );
        // What the old binary did: an in-place UPDATE the marker log never
        // heard of (here it did, and the old pair is put back below).
        let (sql, after, touched) = run.in_place();
        exec(run.db(), &sql).await.unwrap();
        run.model = after;
        let db = run.db.take().unwrap();
        db.sync_durable().unwrap();
        close(&run.dirs, db);
        stale_pair_and_flt(&dir, &pair);
        let preds = run.battery(&[&run.model.clone()], &touched);
        let sink: Sink = StdArc::new(Mutex::new(Vec::new()));
        if unlink_fails {
            // The `.ixs` unlink fails: `_flt.db` is kept, the word starts
            // Unknown, and the first write's slow path rewrites the log
            // before it deletes `_flt.db`.
            let fail = failpoint::arm(
                "flt_evidence_before_ixs_unlink",
                &run.dirs.base,
                1,
                StdArc::new(|_: &failpoint::Hit<'_>| Outcome::Fail(io::Error::from_raw_os_error(libc::EACCES))),
            );
            run.db = Some(open(&run.dirs).await.unwrap());
            drop(fail);
            assert_eq!(run.state(), GateState::Unknown, "{step}: evidence that could not be acted on must start Unknown");
            assert!(dir.join(format!("{TABLE}_flt.db")).exists(), "{step}: the evidence was deleted");
            let (sql, after2, more) = run.in_place();
            let before = run.model.clone();
            let mut touched = touched.clone();
            touched.extend(more);
            let preds = run.battery(&[&before, &after2], &touched);
            let guard = run.arm_nth(step, 1, &format!("{step}, after the Unknown rewrite"), &sink);
            exec(run.db(), &sql).await.unwrap();
            drop(guard);
            run.model = after2.clone();
            run.collect(&sink, &[&before, &after2], &preds, |_| Expect::Rebuild);
            assert!(!dir.join(format!("{TABLE}_flt.db")).exists(), "{step}: the slow path never deleted _flt.db");
            run.quiescent(&format!("{step}, after the write"), &preds, Expect::Rebuild);
        } else {
            let guard = run.arm_nth(step, 1, &format!("{step}, .ixs unlinked and synced, _flt.db not yet deleted"), &sink);
            run.db = Some(open(&run.dirs).await.unwrap());
            drop(guard);
            assert!(!table(run.db()).gate().vouched(), "{step}: a snapshot beside evidence loaded");
            let model = run.model.clone();
            run.collect(&sink, &[&model], &preds, |_| Expect::Rebuild);
            run.quiescent(&format!("{step}, after the open"), &preds, Expect::Rebuild);
        }
        run.check_live(&format!("live after {step}"), Expect::Rebuild).await;
    }
}

/// One run of every phase-1 scenario on one placement.
async fn run(place: Place, run_seed: u64) -> Tally {
    let tmp = tempfile::tempdir().unwrap();
    let dirs = Dirs {
        base: tmp.path().join("subject"),
        place,
    };
    std::fs::create_dir_all(dirs.root()).unwrap();
    let db = create(&dirs).await;
    let mut run = Run {
        dirs: dirs.clone(),
        db: Some(db),
        rec: Recorder::start(&dirs.base),
        model: Model::default(),
        rng: Rng::new(0xB15 ^ run_seed.wrapping_mul(0x9E37_79B9) ^ ((place as u64) << 48)),
        next_id: 0,
        points: Vec::new(),
        tally: Tally::default(),
        problems: Vec::new(),
        held: 0,
        run_seed,
    };
    // The recorder's baseline is the empty table; the load happens under it
    // so its syncs are known.
    drop(std::mem::replace(&mut run.rec, Recorder::start(&dirs.base)));
    for _ in 0..(ROWS / 300) {
        run.insert(300).await;
    }
    let (sql, after, _) = run.delete_some(40);
    exec(run.db(), &sql).await.unwrap();
    run.model = after;
    for _ in 0..10 {
        let (sql, after, _) = run.in_place();
        exec(run.db(), &sql).await.unwrap();
        run.model = after;
    }
    ready(&table(run.db())).await.unwrap();
    table(run.db()).vacuum_now().await.unwrap();
    if place == Place::Second {
        assert_eq!(
            table(run.db()).gate().dir(),
            Some(dirs.disk().as_path()),
            "the table is not on the second location"
        );
    }

    // The first seal: no log yet, so the compaction path's directory sync
    // covers the rename.
    seal_with_cuts(&mut run, "seal 1 (no marker log)").await;
    first_writes(&mut run).await;
    clean_fsync_fails(&mut run).await;
    barrier_fails(&mut run).await;
    dirty_enospc(&mut run).await;
    // A second seal over a log ending DIRTY, with no `.ixs` (the ENOSPC
    // path unlinked it).
    seal_with_cuts(&mut run, "seal 2 (log ends DIRTY)").await;
    // A third over an old `.ixs`.
    let (sql, after, _) = run.in_place();
    exec(run.db(), &sql).await.unwrap();
    run.model = after;
    seal_with_cuts(&mut run, "seal 3 (an old .ixs under the name)").await;
    vacuum_full(&mut run).await;
    ixm_read_eio(&mut run).await;
    marker_rot(&mut run).await;
    flt_evidence(&mut run).await;
    run.check_live("live, end of the run", Expect::Either).await;

    let db = run.db.take().unwrap();
    close(&dirs, db);
    let Run { rec, points, mut tally, mut problems, .. } = run;
    drop(rec);

    let seeds = env_count("RDB_POWER_LOSS_SEEDS", 5, 1024);
    let img = dirs.at(&tmp.path().join("image"));
    for point in &points {
        for seed in 0..seeds {
            let _ = std::fs::remove_dir_all(&img.base);
            point.cp.write_image(seed, &img.base);
            let label = format!("{} seed {seed} (in flight at the cut: {:?})", point.cp.label, point.cp.in_flight);
            match open(&img).await {
                Ok(db) => {
                    let before = problems.len();
                    let seen = check(&db, &label, &point.states, &point.preds, point.expect, &mut problems).await;
                    if problems.len() > before && std::env::var_os("RDB_POWER_LOSS_DESCRIBE").is_some() {
                        eprintln!("{label}:{}", point.cp.describe());
                    }
                    if seen.loaded {
                        tally.loads += 1;
                    } else {
                        tally.rebuilds += 1;
                    }
                    tally.narrowed += seen.narrowed;
                    close(&img, db);
                }
                Err(e) => {
                    problems.push(format!("{label}: the image would not open: {e}"));
                    crate::core::wal::remove_for(&path_str(&img.root()));
                    crate::core::wal::remove_for(&path_str(&img.disk()));
                }
            }
            tally.images += 1;
        }
        let name = point.cp.label.split([',', ' ']).next().unwrap_or_default().to_string();
        tally.covered.insert(name);
    }
    let _ = std::fs::remove_dir_all(&img.base);
    assert!(
        problems.is_empty(),
        "{place:?} run {run_seed}: {} problems ({tally:?}):\n{}",
        problems.len(),
        problems.iter().take(40).cloned().collect::<Vec<_>>().join("\n")
    );
    tally
}

/// Every failpoint name the suite must have imaged.
const COVERED: &[&str] = &[
    "snapshot_before_fsync",
    "snapshot_after_fsync",
    "snapshot_after_rename",
    "snapshot_after_dir_fsync",
    "marker_compaction_after_rename",
    "clean_append_before_fsync",
    "clean_after_fsync",
    "clean_after_publish",
    "dirty_append_before_fsync",
    "dirty_after_fsync",
    "first_write_after_snapshot/update_in_place",
    "first_write_after_snapshot/delete",
    "first_write_after_snapshot/vacuum_clear",
    "first_write_after_snapshot/neutralize_aborted",
    "clean_fsync_fails",
    "unknown_rewrite_after_first_write",
    "barrier_root_sync_fails",
    "barrier_dir_sync_fails",
    "dirty_enospc_after_first_write",
    "invalidate_after_first_compaction_batch",
    "ixm_read_eio_at_open",
    "marker_rot_after_clean",
    "flt_evidence_before_ixs_unlink",
    "unknown_flt_delete_order",
];

async fn suite(place: Place) {
    let runs = env_count("RDB_POWER_LOSS_RUNS", 1, 256);
    let started = std::time::Instant::now();
    let mut total = Tally::default();
    for r in 0..runs {
        let t = run(place, r).await;
        total.points += t.points;
        total.disturbed += t.disturbed;
        total.images += t.images;
        total.loads += t.loads;
        total.rebuilds += t.rebuilds;
        total.narrowed += t.narrowed;
        total.covered.extend(t.covered);
    }
    eprintln!("phase 1 suite {place:?}: {runs} runs in {:.1}s: {total:?}", started.elapsed().as_secs_f64());
    let missing: Vec<&&str> = COVERED.iter().filter(|n| !total.covered.contains(**n)).collect();
    assert!(missing.is_empty(), "{place:?}: never imaged: {missing:?} (covered {:?})", total.covered);
    assert!(total.loads > 0 && total.rebuilds > 0, "{place:?}: the images missed one side: {total:?}");
    assert!(total.narrowed > 20 * total.images.max(1) / 4, "{place:?}: the filter narrowed too few probes: {total:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_filter_survives_power_cuts_at_every_phase_1_failpoint_in_the_root() {
    suite(Place::Root).await;
}

#[cfg(feature = "storage_pools")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_filter_survives_power_cuts_at_every_phase_1_failpoint_on_a_second_location() {
    suite(Place::Second).await;
}

/// Every model reachable by applying any subset of the steps in order,
/// where each step is the model after one statement, applied to the one
/// before it: the rows a step changed relative to its predecessor.
fn subsets(base: &Model, steps: &[Model]) -> Vec<Model> {
    let mut deltas: Vec<(Vec<i64>, BTreeMap<i64, ModelRow>)> = Vec::new();
    let mut prev = base;
    for s in steps {
        let removed: Vec<i64> = prev.rows.keys().filter(|k| !s.rows.contains_key(k)).copied().collect();
        let changed: BTreeMap<i64, ModelRow> =
            s.rows.iter().filter(|(k, v)| prev.rows.get(k) != Some(v)).map(|(k, v)| (*k, v.clone())).collect();
        deltas.push((removed, changed));
        prev = s;
    }
    let mut out = Vec::new();
    for mask in 0u32..(1 << steps.len()) {
        let mut m = base.clone();
        for (i, (removed, changed)) in deltas.iter().enumerate() {
            if mask & (1 << i) != 0 {
                for k in removed {
                    m.rows.remove(k);
                }
                for (k, v) in changed {
                    m.rows.insert(*k, v.clone());
                }
            }
        }
        if !out.contains(&m) {
            out.push(m);
        }
    }
    out
}

/// Bug (c) on a second location (§5, §11, phase 1's gate): a table whose
/// directory has its own WAL and `rdb.ixe`, written under
/// `synchronous_commit = off`, so each statement's outcome waits in the
/// root's log for a sync while its rows are already durable in the
/// location's log (the pre-commit sync). Vacuum clears a deleted version's
/// codes; an in-place UPDATE rewrites a row's codes; a clean point
/// publishes the filter. A power cut anywhere around the seal may undo the
/// statements whose outcomes never reached the root, which brings the rows
/// back as they were. Nothing may then prune them: the oracle is the
/// recovered table read with no predicate at all, and every filtered
/// answer must be that state's. The seal's barrier (the status log staged,
/// then the root's and the location's logs synced, poison checked) is what
/// makes the outcomes durable before the `CLEAN` that vouches for codes
/// derived from them.
///
/// Two layers keep it: vacuum's `outcome_barrier` makes the deleters'
/// outcomes durable before it clears their codes (6bfed18), and the seal's
/// barrier makes every outcome durable before the `CLEAN`. Either alone
/// suffices here (an in-place UPDATE's outcome is durable through the
/// location's commit marker, so the seal's barrier guards nothing an
/// UPDATE could undo), so the mutation removes both: vacuum skips its
/// barrier and the seal's skips the status log and the root's log. Images
/// then load a filter that prunes the undone DELETE's rows (verified: 1,672
/// problems, `WHERE a = 313` answering nothing where the recovered table
/// holds id 527). Each alone passes.
#[cfg(feature = "storage_pools")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bug_c_holds_on_a_second_location_under_synchronous_commit_off() {
    let tmp = tempfile::tempdir().unwrap();
    let dirs = Dirs {
        base: tmp.path().join("subject"),
        place: Place::Second,
    };
    std::fs::create_dir_all(dirs.root()).unwrap();
    let seeds = env_count("RDB_POWER_LOSS_SEEDS", 5, 1024).max(5);
    let runs = env_count("RDB_POWER_LOSS_RUNS", 1, 256);
    let mut problems = Vec::new();
    let (mut images, mut loads, mut points_total) = (0usize, 0usize, 0usize);
    for run_seed in 0..runs {
        let _ = std::fs::remove_dir_all(&dirs.base);
        std::fs::create_dir_all(dirs.root()).unwrap();
        let db = create(&dirs).await;
        let mut run = Run {
            dirs: dirs.clone(),
            db: Some(db),
            rec: Recorder::start(&dirs.base),
            model: Model::default(),
            rng: Rng::new(0xC0C ^ run_seed.wrapping_mul(0x9E37_79B9)),
            next_id: 0,
            points: Vec::new(),
            tally: Tally::default(),
            problems: Vec::new(),
            held: 0,
            run_seed,
        };
        for _ in 0..(ROWS / 300) {
            run.insert(300).await;
        }
        // A compaction first, so the location holds its own `rdb.ixe`.
        let (sql, after, _) = run.delete_some(60);
        exec(run.db(), &sql).await.unwrap();
        run.model = after;
        exec(run.db(), &format!("VACUUM FULL {TABLE}")).await.unwrap();
        ready(&table(run.db())).await.unwrap();
        assert!(dirs.disk().join("rdb.ixe").exists(), "no rdb.ixe on the second location");
        assert!(
            super::image::walk(&dirs.disk())
                .iter()
                .any(|f| f.to_string_lossy().starts_with(crate::core::wal::DEFAULT_WAL_NAME)),
            "the second location has no log of its own"
        );
        clean_point(run.db(), "bug (c): the first clean point").await;
        run.db().sync_durable().unwrap();
        drop(std::mem::replace(&mut run.rec, Recorder::start(&dirs.base)));

        // Under synchronous_commit = off: a DELETE, vacuum's clear of its
        // codes, then in-place UPDATEs to fresh values.
        // Each statement's outcome is durable on its own path (the root's
        // status log, or the location's commit marker for an in-place
        // write), so an image may hold any combination of them: the states
        // are every subset, applied in order.
        let s0 = run.model.clone();
        let mut steps: Vec<Model> = Vec::new();
        let (del, s1, mut touched) = run.delete_some(4);
        exec_sync(run.db(), &del, false).await.unwrap();
        let t = table(run.db());
        let report = t.vacuum_now().await.unwrap();
        drop(t);
        run.model = s1.clone();
        steps.push(s1);
        for _ in 0..3 {
            let (upd, after, more) = run.in_place();
            exec_sync(run.db(), &upd, false).await.unwrap();
            touched.extend(more);
            run.model = after.clone();
            steps.push(after);
        }
        let states = subsets(&s0, &steps);
        let state_refs: Vec<&Model> = states.iter().collect();
        let preds = run.battery(&state_refs, &touched);
        let sink: Sink = StdArc::new(Mutex::new(Vec::new()));
        let guards: Vec<failpoint::Guard> = SEAL_STEPS.iter().map(|(n, _)| run.arm(n, &sink)).collect();
        seal_published(run.db(), "bug (c): the seal over unsynced outcomes").await;
        drop(guards);
        run.collect(&sink, &state_refs, &preds, |cut| {
            SEAL_STEPS.iter().find(|(n, _)| cut.starts_with(n)).map(|(_, e)| *e).unwrap_or(Expect::Either)
        });
        let label = run.label("bug (c), after the seal");
        match run.rec.cut_quiescent(label) {
            Ok(cp) => run.push(cp, &state_refs, &preds, Expect::Load),
            Err(why) => eprintln!("{why}"),
        }
        eprintln!(
            "bug (c) run {run_seed}: vacuum cleared {} fingerprints; {} points",
            report.fingerprints_cleared,
            run.points.len()
        );
        assert!(report.fingerprints_cleared > 0, "the vacuum cleared nothing: the scenario tests nothing");
        let db = run.db.take().unwrap();
        close(&dirs, db);
        let Run { rec, points, .. } = run;
        drop(rec);
        points_total += points.len();
        let img = dirs.at(&tmp.path().join("image"));
        for point in &points {
            for seed in 0..seeds {
                let _ = std::fs::remove_dir_all(&img.base);
                point.cp.write_image(seed, &img.base);
                let label = format!("bug (c): {} seed {seed}", point.cp.label);
                match open(&img).await {
                    Ok(db) => {
                        let seen = check(&db, &label, &point.states, &point.preds, point.expect, &mut problems).await;
                        loads += seen.loaded as usize;
                        close(&img, db);
                    }
                    Err(e) => {
                        problems.push(format!("{label}: the image would not open: {e}"));
                        crate::core::wal::remove_for(&path_str(&img.root()));
                        crate::core::wal::remove_for(&path_str(&img.disk()));
                    }
                }
                images += 1;
            }
        }
        let _ = std::fs::remove_dir_all(&img.base);
    }
    eprintln!("bug (c): {runs} runs, {points_total} points, {images} images, {loads} loaded");
    assert!(
        problems.is_empty(),
        "bug (c): {} problems:\n{}",
        problems.len(),
        problems.iter().take(30).cloned().collect::<Vec<_>>().join("\n")
    );
    assert!(loads > 0, "no image loaded its snapshot: the oracle never met a loaded filter");
}

/// A run of the suite's table at one placement, opened and loaded, with no
/// recorder needed beyond the run's own.
async fn loaded_run(dirs: &Dirs, seed: u64) -> Run {
    let db = create(dirs).await;
    let mut run = Run {
        dirs: dirs.clone(),
        db: Some(db),
        rec: Recorder::start(&dirs.base),
        model: Model::default(),
        rng: Rng::new(seed),
        next_id: 0,
        points: Vec::new(),
        tally: Tally::default(),
        problems: Vec::new(),
        held: 0,
        run_seed: seed,
    };
    for _ in 0..(ROWS / 300) {
        run.insert(300).await;
    }
    ready(&table(run.db())).await.unwrap();
    run
}

fn assert_no_problems(run: &Run, what: &str) {
    assert!(
        run.problems.is_empty(),
        "{what}: {} problems:\n{}",
        run.problems.len(),
        run.problems.iter().take(20).cloned().collect::<Vec<_>>().join("\n")
    );
}

/// `apply_held_fails_then_shutdown` (§11, V7, RV4, P26): on a second
/// location, a held in-place overwrite whose commit marker is durable fails
/// its apply with EIO, which poisons that directory's log; then a clean
/// shutdown. The shutdown seal's barrier is the one poison check: it must
/// refuse the snapshot, so the restart replays the write and rebuilds.
///
/// The doc's overwrite is an in-place UPDATE of an indexed key; since
/// e72a399 no row is overwritten in place outside the outcome log's
/// directory, so the overwrite here is a DELETE's `xmax` stamp on the
/// pointer record, which is still logged and applied at commit. (Measured
/// with the UPDATE before e72a399: under the mutation the restart loaded
/// the stale filter and pruned the new key.)
///
/// Mutation: the barrier syncs the location's log through `sync_pending`,
/// which does not look at the poison: the shutdown seal publishes.
#[cfg(feature = "storage_pools")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn apply_held_fails_then_shutdown() {
    let tmp = tempfile::tempdir().unwrap();
    let dirs = Dirs { base: tmp.path().join("subject"), place: Place::Second };
    std::fs::create_dir_all(dirs.root()).unwrap();
    let mut run = loaded_run(&dirs, 0xA11).await;
    clean_point(run.db(), "apply_held: the clean point").await;
    let before = run.model.clone();
    // Since e72a399 no row overwrite is in place on a second location;
    // a DELETE's `xmax` stamp still is (a logged overwrite of the pointer
    // record, applied from the location's log at its commit).
    let (sql, after, touched) = run.delete_some(1);
    let fail = failpoint::arm(
        "apply_held_fails_then_shutdown",
        &dirs.disk(),
        1,
        StdArc::new(|_: &failpoint::Hit<'_>| Outcome::Fail(eio())),
    );
    let answered = exec(run.db(), &sql).await;
    assert_eq!(fail.fired(), 1, "the in-place apply never ran: {answered:?}");
    drop(fail);
    eprintln!("apply_held: the statement answered {answered:?}");
    let shutdown = run.db().seal_for_shutdown(Duration::from_secs(20)).await;
    let ours: Vec<_> = shutdown.sealed.iter().filter(|s| s.name.contains(TABLE)).collect();
    if ours.iter().any(|s| matches!(s.outcome, SealOutcome::Published | SealOutcome::AlreadyClean)) {
        let why = format!("a seal over a poisoned location log published: {ours:?}");
        run.problems.push(why);
    }
    let db = run.db.take().unwrap();
    close(&dirs, db);
    run.db = Some(open(&dirs).await.expect("reopen after the poisoned apply"));
    let preds = run.battery(&[&before, &after], &touched);
    let label = run.label("apply_held_fails_then_shutdown, after the restart");
    let db = run.db().clone();
    // The statement failed after its commit marker was durable: the
    // replay may keep it.
    check(&db, &label, &[before.clone(), after.clone()], &preds, Expect::Rebuild, &mut run.problems).await;
    let db = run.db.take().unwrap();
    close(&dirs, db);
    assert_no_problems(&run, "apply_held_fails_then_shutdown");
}

/// `staged_tmp_at_snapshot` (§11, V7): staged bytes sit in the rows file's
/// `.tmp` when an optional (idle) seal runs. Step 2 lands them under the
/// sealer's own count before the capture, so the identity's lengths are
/// the landed ones; a clean restart loads the snapshot and answers as the
/// twin.
///
/// Mutation: skip step 2's landing. The bytes then land after the `CLEAN`
/// (the provider's background landing, or at the reopen), so the restart
/// either rebuilds or loads a snapshot whose lengths do not match.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn staged_tmp_at_snapshot() {
    use crate::core::storage_providers::traits::StorageIO;
    let tmp = tempfile::tempdir().unwrap();
    let dirs = Dirs { base: tmp.path().join("subject"), place: Place::Root };
    std::fs::create_dir_all(dirs.root()).unwrap();
    let mut run = loaded_run(&dirs, 0x57A).await;
    let (sql, after, touched) = run.in_place();
    exec(run.db(), &sql).await.unwrap();
    run.model = after;
    let t = table(run.db());
    // Unreferenced bytes past the last row: what a copy's staged append
    // leaves, and nothing a pointer names.
    let rows = t.io_rows().unwrap().clone();
    let len_before = rows.get_len().await;
    rows.append_data(&[0x5Au8; 3 * 4096], false).await;
    let tmp_file = dirs.root().join(format!("{TABLE}.db.tmp"));
    let staged = std::fs::metadata(&tmp_file).map(|m| m.len()).unwrap_or(0);
    assert!(staged > 0, "nothing was staged in {}", tmp_file.display());
    let outcome = t.seal_snapshot(crate::core::index_store::gate_word::SealKind::Optional).await;
    assert!(matches!(outcome, SealOutcome::Published), "the idle seal did not publish: {outcome:?}");
    assert_eq!(
        std::fs::metadata(dirs.root().join(format!("{TABLE}.db"))).unwrap().len(),
        len_before + 3 * 4096,
        "the seal published with the staged bytes still unlanded"
    );
    drop(t);
    let db = run.db.take().unwrap();
    close(&dirs, db);
    run.db = Some(open(&dirs).await.unwrap());
    let preds = run.battery(&[&run.model.clone()], &touched);
    let label = run.label("staged_tmp_at_snapshot, clean restart");
    let db = run.db().clone();
    let model = run.model.clone();
    check(&db, &label, &[model], &preds, Expect::Load, &mut run.problems).await;
    let db = run.db.take().unwrap();
    close(&dirs, db);
    assert_no_problems(&run, "staged_tmp_at_snapshot");
}

/// Retirement cancels a running seal (§7 step 2(a), B13): a mandatory
/// seal (writers queue behind it, so nothing but retirement cancels it)
/// parked after its `.ixs.tmp` fsync holds the file set's maintenance
/// lock; the instance is retired, as a reload or DROP does once its ACCESS
/// EXCLUSIVE is granted. Retirement sets `CANCEL` and waits for the lock;
/// released, the seal sees the cancel at its next step and publishes
/// nothing, so no `CLEAN` vouches for a retired instance's capture. The
/// reopened table rebuilds and answers as the twin.
///
/// Mutation: `retire` does not cancel the seal. The parked seal then
/// publishes over the retired instance and the cancel is never observed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retirement_cancels_a_running_seal() {
    let tmp = tempfile::tempdir().unwrap();
    let dirs = Dirs { base: tmp.path().join("subject"), place: Place::Root };
    std::fs::create_dir_all(dirs.root()).unwrap();
    let mut run = loaded_run(&dirs, 0x8E7).await;
    let (sql, after, touched) = run.in_place();
    exec(run.db(), &sql).await.unwrap();
    run.model = after;
    let t = table(run.db());
    let word = t.gate().word().clone();
    let (reached_tx, reached) = std::sync::mpsc::channel::<()>();
    let (go, go_rx) = std::sync::mpsc::channel::<()>();
    let (reached_tx, go_rx) = (std::sync::Mutex::new(reached_tx), std::sync::Mutex::new(go_rx));
    let park = failpoint::arm(
        "snapshot_after_fsync",
        &dirs.base,
        1,
        StdArc::new(move |_: &failpoint::Hit<'_>| {
            let _ = reached_tx.lock().unwrap().send(());
            let _ = go_rx.lock().unwrap().recv_timeout(Duration::from_secs(30));
            Outcome::Continue
        }),
    );
    let sealing = {
        let t = t.clone();
        tokio::spawn(async move { t.seal_snapshot(crate::core::index_store::gate_word::SealKind::Mandatory).await })
    };
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while reached.try_recv().is_err() {
        assert!(std::time::Instant::now() < deadline, "the seal never reached its park");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let retiring = {
        let t = t.clone();
        tokio::spawn(async move { t.retire().await })
    };
    let mut cancelled = false;
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while std::time::Instant::now() < deadline {
        if word.load().cancelled() {
            cancelled = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let _ = go.send(());
    let outcome = sealing.await.unwrap();
    drop(park);
    tokio::time::timeout(Duration::from_secs(30), retiring)
        .await
        .expect("the retirement never finished")
        .unwrap();
    if !cancelled {
        run.problems.push("the retirement never cancelled the running seal".into());
    }
    if !matches!(outcome, SealOutcome::Skipped(_)) {
        run.problems.push(format!("a seal of an instance being retired ended {outcome:?}"));
    }
    let log = std::fs::read(dirs.root().join(format!("{TABLE}.ixm"))).unwrap_or_default();
    if crate::core::index_store::marker::scan(&log).ok().and_then(|s| s.proven_clean().copied()).is_some() {
        run.problems.push("the cancelled seal left a CLEAN".into());
    }
    drop(t);
    run.reopen().await;
    let preds = run.battery(&[&run.model.clone()], &touched);
    let label = run.label("retirement_cancels_a_running_seal, reopened");
    let db = run.db().clone();
    let model = run.model.clone();
    check(&db, &label, &[model], &preds, Expect::Rebuild, &mut run.problems).await;
    let db = run.db.take().unwrap();
    close(&dirs, db);
    assert_no_problems(&run, "retirement_cancels_a_running_seal");
}
