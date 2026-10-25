//! The index load at open on a real database (slice B17): a loaded index
//! finds every row a rebuilt one finds, and is exactly the one sealed; a
//! version bump rebuilds once, then loads; a bad checksum, a section its
//! index refuses and a stale pair each fall back to a rebuild that answers
//! as the twin; writes wait for a load and readers wait up to `load_wait`;
//! loads queue costliest first under their limit; a partitioned table's
//! partitions all load after a clean shutdown; a table with a gin index
//! writes FILTER only and rebuilds.
//!
//! The twin is a model of the table fed only acknowledged statements.
//! Every directory sets `index_storage = snapshot` for itself only
//! (`storage_override`), so the rest of the suite runs under the default.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::atomic::Ordering as AtomicOrdering;
use std::time::{Duration, Instant};

use rclite::Arc;

use super::settings::IndexStorage;
use super::*;
use crate::core::database::Database;
use crate::core::index::{KEY_LOOKUPS, LiveIndex};
use crate::core::index_store::marker::{self, CleanRecord, MarkerRecord};
use crate::core::index_store::open_proof;
use crate::core::index_store::seal::SealOutcome;
use crate::core::mvcc::transaction::FROZEN_TX;
use crate::core::row::row::Row;
use crate::core::row::row_pointer::{ROW_POINTER_RECORD_LEN, RowPointerIterator};
use crate::core::row::table::Table;
use crate::core::storage_providers::backend::DbStorageProvider;

type DbTable = Table<DbStorageProvider>;

const SETTLE: Duration = Duration::from_secs(60);

async fn open(dir: &Path) -> Arc<Database> {
    let _ = crate::core::processor::concurrent_processor::ENABLE_CACHE.set(false);
    Arc::new(Database::try_new_named(&dir.to_string_lossy(), false, "b17").await.expect("open the database"))
}

fn close(dir: &Path, db: Arc<Database>) {
    drop(db);
    crate::core::wal::remove_for(&dir.to_string_lossy());
}

async fn try_exec(db: &Arc<Database>, sql: &str) -> Result<Vec<u8>, String> {
    let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(sql).ok_or_else(|| format!("parse {sql}"))?;
    let out = crate::core::rql::executor::execute_with_sync(purpose, db.clone(), true).await;
    if out.is_empty() || out[0] == 3 {
        return Err(format!("{sql}: {}", String::from_utf8_lossy(&out)));
    }
    Ok(out)
}

async fn exec(db: &Arc<Database>, sql: &str) -> Vec<u8> {
    try_exec(db, sql).await.unwrap_or_else(|e| panic!("{e}"))
}

fn table(db: &Database, name: &str) -> Arc<DbTable> {
    db.table_for_tests(name).expect("the table")
}

/// The load (or the build) has finished and the filter is ready.
async fn settled(t: &DbTable) {
    let deadline = Instant::now() + SETTLE;
    tokio::time::timeout(SETTLE, t.index_load_settled()).await.expect("the index load never settled");
    while !t.filters_ready().unwrap() || t.indexes().unwrap().is_some_and(|ix| !ix.is_ready()) {
        assert!(Instant::now() < deadline, "the filter or the indexes never became ready");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

async fn checkpoint(db: &Arc<Database>, name: &str) -> SealOutcome {
    let mut sealed = db.index_checkpoint(name, None).await.expect("rdb_index_checkpoint").sealed;
    assert_eq!(sealed.len(), 1);
    sealed.remove(0).outcome
}

fn decode_rows(out: &[u8]) -> Vec<Row> {
    assert_eq!(out.first(), Some(&2), "not a row set: {}", String::from_utf8_lossy(out));
    crate::core::row::row::vec_into_rows(&out[1..]).unwrap_or_default()
}

async fn ids(db: &Arc<Database>, name: &str, clause: &str) -> Vec<i64> {
    let out = exec(db, &format!("SELECT id FROM {name} WHERE {clause}")).await;
    let mut v: Vec<i64> = decode_rows(&out)
        .iter()
        .map(|r| i64::from_le_bytes(r.columns[0].data.into_slice().try_into().expect("an i64 id")))
        .collect();
    v.sort_unstable();
    v
}

// ---------------------------------------------------------------------------
// The twin
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
struct R {
    a: i64,
    b: i64,
    s: String,
    /// `doc->>'k'`; `None` for a document without it (the path index's
    /// empty key).
    k: Option<String>,
}

impl R {
    fn for_id(id: i64) -> R {
        R {
            a: (id * 7919) % 5000,
            b: id % 211,
            s: format!("s{}", id % 997),
            k: (id % 13 != 0).then(|| format!("v{}", id % 300)),
        }
    }

    fn doc(&self) -> String {
        match &self.k {
            Some(k) => format!("'{{\"k\":\"{k}\"}}'"),
            None => "'{\"o\":1}'".to_string(),
        }
    }

    fn values(&self, id: i64) -> String {
        format!("({id}, {}, {}, '{}', {})", self.a, self.b, self.s, self.doc())
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
struct Model {
    rows: BTreeMap<i64, R>,
}

#[derive(Debug, Clone)]
enum P {
    Id(i64),
    A(i64),
    S(String),
    Ba(i64, i64),
    K(String),
    ARange(i64, i64),
    B(i64),
}

impl P {
    fn sql(&self) -> String {
        match self {
            P::Id(v) => format!("id = {v}"),
            P::A(v) => format!("a = {v}"),
            P::S(v) => format!("s = '{v}'"),
            P::Ba(b, a) => format!("b = {b} AND a = {a}"),
            P::K(v) => format!("doc->>'k' = '{v}'"),
            P::ARange(lo, hi) => format!("a >= {lo} AND a <= {hi}"),
            P::B(v) => format!("b = {v}"),
        }
    }

    fn matches(&self, r: &R, id: i64) -> bool {
        match self {
            P::Id(v) => id == *v,
            P::A(v) => r.a == *v,
            P::S(v) => r.s == *v,
            P::Ba(b, a) => r.b == *b && r.a == *a,
            P::K(v) => r.k.as_deref() == Some(v.as_str()),
            P::ARange(lo, hi) => r.a >= *lo && r.a <= *hi,
            P::B(v) => r.b == *v,
        }
    }
}

impl Model {
    fn answer(&self, p: &P) -> Vec<i64> {
        self.rows.iter().filter(|(id, r)| p.matches(r, **id)).map(|(id, _)| *id).collect()
    }

    /// Predicates on sampled rows' own values, every touched row's, and
    /// values no row holds.
    fn battery(&self, rng: &mut fastrand::Rng, touched: &[(i64, R)]) -> Vec<P> {
        let ids: Vec<i64> = self.rows.keys().copied().collect();
        let mut out = Vec::new();
        for _ in 0..24 {
            let id = ids[rng.usize(..ids.len())];
            let r = &self.rows[&id];
            out.push(match rng.u8(0..7) {
                0 => P::Id(id),
                1 => P::A(r.a),
                2 => P::S(r.s.clone()),
                3 => P::Ba(r.b, r.a),
                4 => P::K(r.k.clone().unwrap_or_else(|| "v1".into())),
                5 => P::ARange(r.a, r.a + 3),
                _ => P::B(r.b),
            });
        }
        for (id, r) in touched {
            out.extend([P::Id(*id), P::A(r.a), P::S(r.s.clone()), P::Ba(r.b, r.a)]);
            if let Some(k) = &r.k {
                out.push(P::K(k.clone()));
            }
        }
        out.extend([P::A(77_777_777), P::S("absent".into()), P::K("absent".into()), P::Id(-5)]);
        out
    }
}

async fn create(db: &Arc<Database>, n: &str) {
    exec(db, &format!("CREATE TABLE {n} (id BIGINT PRIMARY KEY, a BIGINT, b BIGINT, s TEXT, doc JSONB)")).await;
    exec(db, &format!("CREATE INDEX {n}_a ON {n} (a)")).await;
    exec(db, &format!("CREATE INDEX {n}_s ON {n} USING hash (s)")).await;
    exec(db, &format!("CREATE INDEX {n}_ba ON {n} (b, a)")).await;
    exec(db, &format!("CREATE INDEX {n}_k ON {n} ((doc->>'k'))")).await;
}

/// The B-tree, hash and path indexes [`create`] declares, the primary key's
/// among them.
const INDEXES: usize = 5;

async fn insert(db: &Arc<Database>, n: &str, model: &mut Model, ids: std::ops::Range<i64>) {
    let all: Vec<i64> = ids.collect();
    for chunk in all.chunks(250) {
        let values: Vec<String> = chunk.iter().map(|id| R::for_id(*id).values(*id)).collect();
        exec(db, &format!("INSERT INTO {n} (id, a, b, s, doc) VALUES {}", values.join(", "))).await;
        for id in chunk {
            model.rows.insert(*id, R::for_id(*id));
        }
    }
}

/// Single-row updates of each indexed column to a value no row had: in
/// place for `a` and `b`, a new version for `s` and `doc`.
async fn update_some(db: &Arc<Database>, n: &str, model: &mut Model, rng: &mut fastrand::Rng, count: usize, tag: i64) -> Vec<(i64, R)> {
    let mut touched = Vec::new();
    let ids: Vec<i64> = model.rows.keys().copied().collect();
    for i in 0..count {
        let id = ids[rng.usize(..ids.len())];
        let r = model.rows.get_mut(&id).unwrap();
        match i % 4 {
            0 => {
                r.a = 1_000_000 + tag * 1000 + i as i64;
                exec(db, &format!("UPDATE {n} SET a = {} WHERE id = {id}", r.a)).await;
            }
            1 => {
                r.s = format!("u{tag}_{i}");
                exec(db, &format!("UPDATE {n} SET s = '{}' WHERE id = {id}", r.s)).await;
            }
            2 => {
                r.b = 5_000 + tag * 100 + i as i64;
                exec(db, &format!("UPDATE {n} SET b = {} WHERE id = {id}", r.b)).await;
            }
            _ => {
                r.k = Some(format!("w{tag}_{i}"));
                exec(db, &format!("UPDATE {n} SET doc = {} WHERE id = {id}", r.doc())).await;
            }
        }
        touched.push((id, r.clone()));
    }
    touched
}

/// Inserts, deletes, updates of every indexed column, vacuum, a `VACUUM
/// FULL`, then more of each.
async fn populate(db: &Arc<Database>, n: &str, model: &mut Model, rng: &mut fastrand::Rng, rows: i64) -> Vec<(i64, R)> {
    insert(db, n, model, 0..rows).await;
    exec(db, &format!("DELETE FROM {n} WHERE id % 37 = 5")).await;
    model.rows.retain(|id, _| id % 37 != 5);
    let mut touched = update_some(db, n, model, rng, 40, 1).await;
    exec(db, &format!("VACUUM {n}")).await;
    exec(db, &format!("VACUUM FULL {n}")).await;
    insert(db, n, model, rows..rows + rows / 5).await;
    touched.extend(update_some(db, n, model, rng, 20, 2).await);
    exec(db, &format!("DELETE FROM {n} WHERE id % 41 = 7")).await;
    model.rows.retain(|id, _| id % 41 != 7);
    exec(db, &format!("VACUUM {n}")).await;
    touched.retain(|(id, _)| model.rows.contains_key(id));
    touched
}

/// Every predicate answers as the model does.
async fn answers(db: &Arc<Database>, n: &str, model: &Model, preds: &[P], label: &str) -> Vec<String> {
    let mut problems = Vec::new();
    for p in preds {
        let want = model.answer(p);
        let got = ids(db, n, &p.sql()).await;
        if got != want {
            problems.push(format!(
                "{label}: WHERE {}: {} ids {:?}, twin {} ids {:?}",
                p.sql(),
                got.len(),
                &got[..got.len().min(6)],
                want.len(),
                &want[..want.len().min(6)]
            ));
        }
    }
    problems
}

/// Every visible row's slot and column bytes by write order, under a fresh
/// snapshot.
async fn visible_rows(t: &DbTable) -> Vec<(u64, BTreeMap<u32, Vec<u8>>)> {
    let d = t.data().unwrap();
    let _read = d.readers.enter();
    let visibility = t.visibility_for(FROZEN_TX);
    let cols: Vec<u32> = t.schema.fields.iter().map(|f| f.write_order as u32).collect();
    let fetch = DbTable::build_fetch_for(&t.schema.fields, &cols);
    let mut it = RowPointerIterator::new(d.io_pointers.clone()).await.unwrap();
    let mut row = Row::default();
    let mut out = Vec::new();
    while let Some((offset, pointer)) = it.next_row_pointer_with_offset().await.unwrap() {
        if !visibility.visible(pointer.deleted, pointer.xmin, pointer.xmax) {
            continue;
        }
        pointer.fetch_row_reuse_async(&*d.io_rows, &fetch, &mut row).await.unwrap();
        let cols = row.columns.iter().map(|c| (c.schema_id as u32, c.data.into_slice().to_vec())).collect();
        out.push((offset / ROW_POINTER_RECORD_LEN as u64, cols));
    }
    out
}

fn list(t: &DbTable) -> Vec<std::sync::Arc<LiveIndex>> {
    t.data().unwrap().indexes.as_ref().map(|ix| ix.list.clone()).unwrap_or_default()
}

/// The superset at the index: every visible row is listed, in every index,
/// under the key its own values give (or unkeyed).
async fn index_superset(t: &DbTable, label: &str) -> Vec<String> {
    let rows = visible_rows(t).await;
    let mut problems = Vec::new();
    for ix in list(t) {
        for (slot, cols) in &rows {
            let found = match ix.key_of(|wo| cols.get(&wo).map(|v| v.as_slice())) {
                Some(key) => ix.slots_for_key(&key).contains(slot),
                None => true,
            };
            if !found && problems.len() < 20 {
                problems.push(format!("{label}: index {} does not find visible slot {slot}", ix.decl.name));
            }
        }
    }
    problems
}

/// One index's stream, owned.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct Captured {
    entries: Vec<(Vec<u8>, Vec<u64>)>,
    unkeyed: Vec<u64>,
    present: Vec<u64>,
}

fn capture_all(t: &DbTable) -> Vec<Captured> {
    list(t)
        .iter()
        .map(|ix| {
            let mut c = Captured::default();
            ix.for_each_sorted(|part| -> Result<(), ()> {
                match part {
                    crate::core::index::sorted::Sorted::Entry(k, s) => c.entries.push((k.to_vec(), s.to_vec())),
                    crate::core::index::sorted::Sorted::Unkeyed(s) => c.unkeyed = s.to_vec(),
                    crate::core::index::sorted::Sorted::Present(w) => c.present = w.to_vec(),
                }
                Ok(())
            })
            .unwrap();
            c
        })
        .collect()
}

fn index_sections(d: &Path, stem: &str) -> usize {
    let bytes = std::fs::read(d.join(format!("{stem}.ixs"))).expect("the snapshot");
    snapshot::decode(&bytes).unwrap().sections.iter().filter(|(e, _)| e.kind == SectionKind::Index).count()
}

/// A table populated through every mutator, sealed under `snapshot`:
/// returns the model, the touched rows and the sealed indexes' streams.
async fn sealed_table(d: &Path, n: &str, rows: i64, seed: u64) -> (Model, Vec<(i64, R)>, Vec<Captured>) {
    let mut model = Model::default();
    let mut rng = fastrand::Rng::with_seed(seed);
    let db = open(d).await;
    create(&db, n).await;
    let touched = populate(&db, n, &mut model, &mut rng, rows).await;
    let t = table(&db, n);
    settled(&t).await;
    let outcome = checkpoint(&db, n).await;
    assert!(matches!(outcome, SealOutcome::Published), "{outcome:?}");
    let sealed = capture_all(&t);
    drop(t);
    close(d, db);
    assert_eq!(index_sections(d, n), INDEXES, "the seal did not write an INDEX section per index");
    (model, touched, sealed)
}

// ---------------------------------------------------------------------------
// The false-negative guard
// ---------------------------------------------------------------------------

/// The false-negative guard. A table goes through inserts, deletes,
/// updates of every indexed column (in place and as new versions), vacuum
/// and `VACUUM FULL`, and is sealed under `snapshot`. The reopen loads (the
/// load counter moves, the instance built nothing), and its indexes are
/// exactly the ones sealed, stream for stream; every visible row is found
/// in every index under its own key; every predicate answers as the twin
/// through the indexes. A rebuild of the same files lists nothing for a
/// visible slot that the loaded indexes lack. A write after the reopen and
/// an abandoned close: the next open builds, and answers as the twin.
///
/// Mutation (drop a section): the load skips one index's `load_sorted` and
/// marks the indexes ready anyway: that index finds nothing.
#[tokio::test(flavor = "multi_thread")]
async fn a_loaded_index_finds_every_row_a_rebuilt_one_finds() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    let snap = storage_override::set(d, IndexStorage::Snapshot);
    let (mut model, touched, sealed) = sealed_table(d, "lx", 2500, 0xb17).await;
    let mut rng = fastrand::Rng::with_seed(0xb17_2);
    let preds = model.battery(&mut rng, &touched);

    let loads = loaded_total();
    let db = open(d).await;
    let t = table(&db, "lx");
    settled(&t).await;
    assert_eq!(t.index_origin(), Origin::Loaded, "a clean reopen did not load its indexes");
    assert!(loaded_total() > loads, "the load counter did not move");
    assert!(t.gate().vouched(), "a load that covered everything did not vouch");
    let loaded = capture_all(&t);
    assert_eq!(loaded.len(), INDEXES);
    assert!(loaded == sealed, "the loaded indexes are not the ones sealed");
    let mut problems = index_superset(&t, "loaded").await;
    let lookups = KEY_LOOKUPS.load(AtomicOrdering::Relaxed);
    problems.extend(answers(&db, "lx", &model, &preds, "loaded").await);
    assert!(KEY_LOOKUPS.load(AtomicOrdering::Relaxed) > lookups, "no predicate was answered through an index");
    let visible: BTreeSet<u64> = visible_rows(&t).await.into_iter().map(|(s, _)| s).collect();
    drop(t);
    close(d, db);

    // The same files, rebuilt.
    drop(snap);
    let rebuild = storage_override::set(d, IndexStorage::Rebuild);
    let db = open(d).await;
    let t = table(&db, "lx");
    settled(&t).await;
    assert_eq!(t.index_origin(), Origin::Built);
    let rebuilt = capture_all(&t);
    for (i, (r, l)) in rebuilt.iter().zip(&loaded).enumerate() {
        let have: BTreeSet<(&[u8], u64)> =
            l.entries.iter().flat_map(|(k, s)| s.iter().map(move |s| (k.as_slice(), *s))).collect();
        for (k, slots) in &r.entries {
            for s in slots.iter().filter(|s| visible.contains(s)) {
                if !have.contains(&(k.as_slice(), *s)) && !l.unkeyed.contains(s) {
                    problems.push(format!("index {i}: the rebuild lists visible slot {s} under {k:02x?}; the load does not"));
                }
            }
        }
    }
    problems.extend(answers(&db, "lx", &model, &preds, "rebuilt").await);
    drop(t);
    close(d, db);
    drop(rebuild);

    // A write after a loaded reopen, then an abandoned close: rebuild.
    let _snap = storage_override::set(d, IndexStorage::Snapshot);
    let db = open(d).await;
    settled(&table(&db, "lx")).await;
    let after = update_some(&db, "lx", &mut model, &mut rng, 8, 3).await;
    close(d, db);
    let db = open(d).await;
    let t = table(&db, "lx");
    settled(&t).await;
    assert_eq!(t.index_origin(), Origin::Built, "an open after a write loaded a snapshot");
    let preds = model.battery(&mut rng, &after);
    problems.extend(answers(&db, "lx", &model, &preds, "after a write").await);
    problems.extend(index_superset(&t, "after a write").await);
    drop(t);
    close(d, db);
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}

// ---------------------------------------------------------------------------
// Rule 2: rebuild once, then load
// ---------------------------------------------------------------------------

/// A version bump followed by two read-only reopens moves the build counter
/// once (§3 rule 2). The seal writes its sections under another version (an
/// older binary's snapshot); the first reopen proves the snapshot, refuses
/// the sections, builds, and stays unvouched; its clean point rewrites the
/// snapshot; the second reopen loads. Each answers as the twin.
///
/// Mutations: the open vouches whatever the load did (the clean point is
/// then "already clean" and the second reopen builds again); a failed load
/// marks the indexes ready without building (they answer nothing).
#[tokio::test(flavor = "multi_thread")]
async fn a_version_bump_rebuilds_once_then_loads() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    let _snap = storage_override::set(d, IndexStorage::Snapshot);
    let older = hooks::write_version(d, index_section::INDEX_SECTION_VER - 1);
    let (model, touched, _) = sealed_table(d, "vb", 1500, 0xb17_3).await;
    drop(older);
    let mut rng = fastrand::Rng::with_seed(0xb17_4);
    let preds = model.battery(&mut rng, &touched);
    let mut problems = Vec::new();
    let mut origins = Vec::new();
    for round in 0..3 {
        let failed = failed_total();
        let db = open(d).await;
        let t = table(&db, "vb");
        settled(&t).await;
        origins.push(t.index_origin());
        if round == 0 {
            assert!(failed_total() > failed, "the older section did not fail its load");
            assert!(!t.gate().vouched(), "an open that rebuilt its indexes vouched for the snapshot");
        }
        problems.extend(answers(&db, "vb", &model, &preds, &format!("reopen {round}")).await);
        problems.extend(index_superset(&t, &format!("reopen {round}")).await);
        // The clean point a read-only partition reaches: shutdown's seal.
        let outcome = checkpoint(&db, "vb").await;
        assert!(outcome.is_clean(), "round {round}: {outcome:?}");
        drop(t);
        close(d, db);
    }
    assert_eq!(origins, vec![Origin::FellBack, Origin::Loaded, Origin::Loaded], "the build counter must move once");
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}

/// A snapshot written under `rebuild` has no INDEX sections. Turning
/// `snapshot` on without a reopen must not let the vouched word look
/// current: the next clean point rewrites the snapshot with its sections,
/// and the reopen loads.
///
/// Mutation: `snapshot_current` always true (the seal answers "already
/// clean" and the reopen builds).
#[tokio::test(flavor = "multi_thread")]
async fn turning_snapshot_on_rewrites_a_vouched_snapshot_at_the_next_clean_point() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    let mut model = Model::default();
    let rebuild = storage_override::set(d, IndexStorage::Rebuild);
    let db = open(d).await;
    create(&db, "on").await;
    insert(&db, "on", &mut model, 0..800).await;
    let t = table(&db, "on");
    settled(&t).await;
    assert!(matches!(checkpoint(&db, "on").await, SealOutcome::Published));
    assert_eq!(index_sections(d, "on"), 0, "a seal under rebuild wrote INDEX sections");
    assert!(matches!(checkpoint(&db, "on").await, SealOutcome::AlreadyClean));
    drop(rebuild);
    let _snap = storage_override::set(d, IndexStorage::Snapshot);
    let outcome = checkpoint(&db, "on").await;
    assert!(matches!(outcome, SealOutcome::Published), "a snapshot without its INDEX sections was kept: {outcome:?}");
    assert_eq!(index_sections(d, "on"), 4 + 1);
    drop(t);
    close(d, db);
    let db = open(d).await;
    let t = table(&db, "on");
    settled(&t).await;
    assert_eq!(t.index_origin(), Origin::Loaded);
    let mut rng = fastrand::Rng::with_seed(9);
    let problems = answers(&db, "on", &model, &model.battery(&mut rng, &[]), "loaded").await;
    drop(t);
    close(d, db);
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}

// ---------------------------------------------------------------------------
// Fallbacks
// ---------------------------------------------------------------------------

/// The INDEX section `ordinal` of a snapshot: its offset and length.
fn section_at(ixs: &[u8], ordinal: u16) -> (usize, usize) {
    let snap = snapshot::decode(ixs).unwrap();
    let e = snap.header.section(SectionKind::Index, ordinal).unwrap();
    (e.offset as usize, e.len as usize)
}

/// A byte of an INDEX section flipped: the proof refuses the snapshot
/// (its crc32c), the open rebuilds everything, and answers as the twin.
#[tokio::test(flavor = "multi_thread")]
async fn a_bad_index_section_checksum_falls_back_and_answers_as_the_twin() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    let _snap = storage_override::set(d, IndexStorage::Snapshot);
    let (model, touched, _) = sealed_table(d, "bc", 1200, 0xb17_5).await;
    let path = d.join("bc.ixs");
    let mut ixs = std::fs::read(&path).unwrap();
    let (at, len) = section_at(&ixs, 2);
    ixs[at + len / 2] ^= 0x10;
    std::fs::write(&path, &ixs).unwrap();
    let corrupt = open_proof::rejected_total("snapshot_corrupt");
    let db = open(d).await;
    let t = table(&db, "bc");
    settled(&t).await;
    assert!(open_proof::rejected_total("snapshot_corrupt") > corrupt, "the bad checksum was not what refused it");
    assert_eq!(t.index_origin(), Origin::Built);
    assert!(!t.gate().vouched());
    let mut rng = fastrand::Rng::with_seed(5);
    let mut problems = answers(&db, "bc", &model, &model.battery(&mut rng, &touched), "rebuilt").await;
    problems.extend(index_superset(&t, "rebuilt").await);
    drop(t);
    close(d, db);
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}

/// Writes `sections` as the table's snapshot with a `CLEAN` that proves it:
/// every checksum valid, the identity the open will expect.
fn rewrite_snapshot(d: &Path, stem: &str, replace: impl Fn(u16, &[u8]) -> Vec<u8>) {
    let ixs = std::fs::read(d.join(format!("{stem}.ixs"))).unwrap();
    let ixm = std::fs::read(d.join(format!("{stem}.ixm"))).unwrap();
    let clean: CleanRecord = *marker::scan(&ixm).unwrap().proven_clean().unwrap();
    let snap = snapshot::decode(&ixs).unwrap();
    let owned: Vec<(SectionKind, u16, Vec<u8>)> = snap
        .sections
        .iter()
        .map(|(e, b)| (e.kind, e.ordinal, if e.kind == SectionKind::Index { replace(e.ordinal, b) } else { b.to_vec() }))
        .collect();
    let borrowed: Vec<(SectionKind, u16, &[u8])> = owned.iter().map(|(k, o, b)| (*k, *o, b.as_slice())).collect();
    let bytes = snapshot::encode(snap.header.identity, &borrowed);
    let footer = u64::from_le_bytes(bytes[bytes.len() - snapshot::FOOTER_LEN..].try_into().unwrap());
    let record = CleanRecord {
        identity: clean.identity,
        snapshot_len: bytes.len() as u64,
        footer,
    };
    let mut log = marker::new_log(clean.identity.incarnation);
    log.extend_from_slice(&marker::encode_record(1, &MarkerRecord::Clean(record)));
    std::fs::write(d.join(format!("{stem}.ixs")), bytes).unwrap();
    std::fs::write(d.join(format!("{stem}.ixm")), log).unwrap();
}

/// A section every checksum vouches for, whose stream its index refuses (a
/// slot past the table): the proof passes and the filter loads, the index
/// load fails, everything it loaded is cleared and the indexes are built;
/// unvouched; answers as the twin. Then a section left out of the
/// snapshot: built instead, unvouched.
#[tokio::test(flavor = "multi_thread")]
async fn a_section_its_index_refuses_falls_back_and_answers_as_the_twin() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    let _snap = storage_override::set(d, IndexStorage::Snapshot);
    let (model, touched, _) = sealed_table(d, "rf", 1200, 0xb17_6).await;
    let n_slots = std::fs::metadata(d.join("rf_pointers.db")).unwrap().len() / ROW_POINTER_RECORD_LEN as u64;
    rewrite_snapshot(d, "rf", |ordinal, bytes| {
        if ordinal != 3 {
            return bytes.to_vec();
        }
        let past = [n_slots + 5];
        index_section::encode_stream([
            crate::core::index::sorted::Sorted::Entry(&b"k"[..], &past[..]),
            crate::core::index::sorted::Sorted::Unkeyed(&[]),
            crate::core::index::sorted::Sorted::Present(&[]),
        ])
        .unwrap()
    });
    let (filters, failed) = (open_proof::loaded_total(), failed_total());
    let db = open(d).await;
    let t = table(&db, "rf");
    settled(&t).await;
    assert!(open_proof::loaded_total() > filters, "the proof did not pass");
    assert!(failed_total() > failed, "the refused section did not fail the load");
    assert_eq!(t.index_origin(), Origin::FellBack);
    assert!(!t.gate().vouched());
    let mut rng = fastrand::Rng::with_seed(6);
    let preds = model.battery(&mut rng, &touched);
    let mut problems = answers(&db, "rf", &model, &preds, "fell back").await;
    problems.extend(index_superset(&t, "fell back").await);
    assert!(matches!(checkpoint(&db, "rf").await, SealOutcome::Published), "an unvouched open was not resealed");
    drop(t);
    close(d, db);

    // One section fewer than indexes.
    let ixs = std::fs::read(d.join("rf.ixs")).unwrap();
    let snap = snapshot::decode(&ixs).unwrap();
    let kept: Vec<(SectionKind, u16, &[u8])> = snap
        .sections
        .iter()
        .filter(|(e, _)| !(e.kind == SectionKind::Index && e.ordinal == 4))
        .map(|(e, b)| (e.kind, e.ordinal, *b))
        .collect();
    let bytes = snapshot::encode(snap.header.identity, &kept);
    std::fs::write(d.join("rf.ixs"), &bytes).unwrap();
    rewrite_snapshot(d, "rf", |_, b| b.to_vec());
    let db = open(d).await;
    let t = table(&db, "rf");
    settled(&t).await;
    assert_eq!(t.index_origin(), Origin::Built, "a snapshot missing a section loaded");
    assert!(!t.gate().vouched());
    problems.extend(answers(&db, "rf", &model, &preds, "missing section").await);
    drop(t);
    close(d, db);
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}

/// A stale pair put back over files that grew and changed since (a
/// restored backup, a half-done copy): its `CLEAN` is last, but it
/// describes other files, so the open builds and every row is found.
///
/// Mutation (skip an identity row): `prove` skips the identity comparison;
/// the stale indexes load, and the rows inserted since are missing.
#[tokio::test(flavor = "multi_thread")]
async fn a_stale_pair_never_loads_indexes_over_changed_files() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    let _snap = storage_override::set(d, IndexStorage::Snapshot);
    let (mut model, _, _) = sealed_table(d, "st", 1000, 0xb17_7).await;
    let pair = (std::fs::read(d.join("st.ixs")).unwrap(), std::fs::read(d.join("st.ixm")).unwrap());
    let db = open(d).await;
    settled(&table(&db, "st")).await;
    insert(&db, "st", &mut model, 5000..5300).await;
    let mut rng = fastrand::Rng::with_seed(7);
    let touched = update_some(&db, "st", &mut model, &mut rng, 12, 4).await;
    close(d, db);
    std::fs::write(d.join("st.ixs"), &pair.0).unwrap();
    std::fs::write(d.join("st.ixm"), &pair.1).unwrap();
    let db = open(d).await;
    let t = table(&db, "st");
    settled(&t).await;
    assert_eq!(t.index_origin(), Origin::Built, "a stale pair loaded");
    let mut preds = model.battery(&mut rng, &touched);
    preds.extend((5000..5300).step_by(29).map(|id| P::A(R::for_id(id).a)));
    preds.extend((5000..5300).step_by(31).map(P::Id));
    let mut problems = answers(&db, "st", &model, &preds, "stale pair").await;
    problems.extend(index_superset(&t, "stale pair").await);
    drop(t);
    close(d, db);
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}

// ---------------------------------------------------------------------------
// Waiting for a load
// ---------------------------------------------------------------------------

/// With the load held: a query waits `load_wait`, then scans and answers;
/// an UPDATE does not finish (its first ticket waits for the load). Once
/// the load is let go, the UPDATE lands, the indexes are the loaded ones,
/// and every answer is the twin's.
///
/// Mutations: `gate::ticket` does not wait for the barrier (the UPDATE
/// finishes while the load is held); the reader does not wait (the query
/// returns before `load_wait`).
#[tokio::test(flavor = "multi_thread")]
async fn writes_wait_for_the_load_and_readers_wait_up_to_load_wait() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    let _snap = storage_override::set(d, IndexStorage::Snapshot);
    let (mut model, _, _) = sealed_table(d, "wt", 1200, 0xb17_8).await;
    let held = hooks::hold(d);
    let db = open(d).await;
    let t = table(&db, "wt");
    assert!(t.gate().loads().loading(), "no load in flight");
    let id = *model.rows.keys().nth(17).unwrap();
    let a = model.rows[&id].a;
    let started = Instant::now();
    let got = ids(&db, "wt", &format!("a = {a}")).await;
    let waited = started.elapsed();
    assert_eq!(got, model.answer(&P::A(a)), "the query during the load");
    let wait = settings::load_wait();
    assert!(waited + Duration::from_millis(50) >= wait, "the query waited {waited:?}, not load_wait ({wait:?})");
    assert_eq!(t.index_origin(), Origin::Pending);

    let writer = {
        let db = db.clone();
        tokio::spawn(async move { exec(&db, &format!("UPDATE wt SET a = 4242424 WHERE id = {id}")).await })
    };
    tokio::time::sleep(wait + Duration::from_millis(900)).await;
    assert!(!writer.is_finished(), "a write finished while the load it must wait for was held");
    drop(held);
    tokio::time::timeout(SETTLE, writer).await.expect("the write never finished").unwrap();
    model.rows.get_mut(&id).unwrap().a = 4_242_424;
    settled(&t).await;
    assert_eq!(t.index_origin(), Origin::Loaded);
    let mut rng = fastrand::Rng::with_seed(8);
    let touched = vec![(id, model.rows[&id].clone())];
    let mut problems = answers(&db, "wt", &model, &model.battery(&mut rng, &touched), "after the load").await;
    problems.extend(answers(&db, "wt", &model, &[P::A(a)], "the old value").await);
    problems.extend(index_superset(&t, "after the load").await);
    drop(t);
    close(d, db);
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}

// ---------------------------------------------------------------------------
// The queue
// ---------------------------------------------------------------------------

/// Freed slots go to the costliest waiter (ties to the first queued), never
/// more than the limit at once, and a waiter dropped while it waits gives
/// back the slot it may have been granted.
///
/// Mutations: the queue grants in arrival order; it ignores its limit.
#[tokio::test(flavor = "multi_thread")]
async fn loads_queue_costliest_first_and_never_past_the_limit() {
    static ONE: LoadQueue = LoadQueue::new(|| 1);
    let first = ONE.admit(1).await;
    let order = std::sync::Arc::new(parking_lot::Mutex::new(Vec::new()));
    let mut tasks = Vec::new();
    for cost in [5u64, 50, 10, 50, 1] {
        let order = order.clone();
        tasks.push(tokio::spawn(async move {
            let slot = ONE.admit(cost).await;
            order.lock().push(cost);
            tokio::time::sleep(Duration::from_millis(5)).await;
            drop(slot);
        }));
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(order.lock().is_empty(), "a load ran past the limit");
    drop(first);
    for t in tasks {
        t.await.unwrap();
    }
    assert_eq!(*order.lock(), vec![50, 50, 10, 5, 1]);
    assert_eq!(ONE.peak_and_reset(), 1);

    static TWO: LoadQueue = LoadQueue::new(|| 2);
    let tasks: Vec<_> = (0..8u64)
        .map(|i| {
            tokio::spawn(async move {
                let slot = TWO.admit(i).await;
                tokio::time::sleep(Duration::from_millis(15)).await;
                drop(slot);
            })
        })
        .collect();
    for t in tasks {
        t.await.unwrap();
    }
    assert_eq!(TWO.peak_and_reset(), 2, "the limit was not used, or was passed");

    // A waiter dropped while it waits: its grant comes back.
    static DROPPED: LoadQueue = LoadQueue::new(|| 1);
    let held = DROPPED.admit(1).await;
    let waiter = tokio::spawn(async { DROPPED.admit(1).await; });
    tokio::time::sleep(Duration::from_millis(20)).await;
    waiter.abort();
    let _ = waiter.await;
    drop(held);
    tokio::time::timeout(Duration::from_secs(5), DROPPED.admit(1)).await.expect("a dropped waiter kept the slot");
}

// ---------------------------------------------------------------------------
// Partitions and gin
// ---------------------------------------------------------------------------

/// A partitioned table's partitions after a clean shutdown (`Database::
/// seal_for_shutdown`): every partition's primary key loads from its own
/// snapshot, point lookups go through it (`KEY_LOOKUPS`), and every answer
/// is the twin's.
#[tokio::test(flavor = "multi_thread")]
async fn every_partition_loads_after_a_clean_shutdown() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    let _snap = storage_override::set(d, IndexStorage::Snapshot);
    let mut rows: BTreeMap<(i64, i64), i64> = BTreeMap::new();
    {
        let db = open(d).await;
        exec(&db, "CREATE TABLE pt (tenant_id BIGINT NOT NULL, id BIGINT NOT NULL, v BIGINT, PRIMARY KEY (tenant_id, id)) PARTITION BY HASH (tenant_id) PARTITIONS 6").await;
        for chunk in (0..1800i64).collect::<Vec<_>>().chunks(300) {
            let values: Vec<String> = chunk.iter().map(|i| format!("({}, {i}, {})", i % 23, i * 3)).collect();
            exec(&db, &format!("INSERT INTO pt (tenant_id, id, v) VALUES {}", values.join(", "))).await;
            for i in chunk {
                rows.insert((i % 23, *i), i * 3);
            }
        }
        exec(&db, "DELETE FROM pt WHERE id % 19 = 4").await;
        rows.retain(|(_, id), _| id % 19 != 4);
        exec(&db, "UPDATE pt SET v = 7 WHERE id % 50 = 1").await;
        for ((_, id), v) in rows.iter_mut() {
            if id % 50 == 1 {
                *v = 7;
            }
        }
        let pt = db.partitioned_table("pt").unwrap();
        let epoch = pt.epoch().unwrap();
        for (_, part) in pt.materialized(&epoch, &crate::core::partition::Route::All).unwrap() {
            settled(&part).await;
        }
        let report = db.seal_for_shutdown(Duration::from_secs(60)).await;
        assert!(report.skipped.is_empty() && report.sealed.iter().all(|s| s.outcome.is_clean()), "the shutdown seal left partitions");
        assert!(report.sealed.len() >= 6, "{} partitions sealed", report.sealed.len());
        close(d, db);
    }
    let loads = loaded_total();
    let db = open(d).await;
    let pt = db.partitioned_table("pt").unwrap();
    let epoch = pt.epoch().unwrap();
    let parts = pt.materialized(&epoch, &crate::core::partition::Route::All).unwrap();
    assert!(parts.len() >= 6);
    for (pid, part) in &parts {
        settled(part).await;
        assert_eq!(part.index_origin(), Origin::Loaded, "partition {pid} did not load");
    }
    assert!(loaded_total() >= loads + parts.len() as u64);
    let mut problems = Vec::new();
    let lookups = KEY_LOOKUPS.load(AtomicOrdering::Relaxed);
    for (tenant, id) in rows.keys().step_by(37).copied().chain([(3, 99_999), (22, 4)]) {
        let out = exec(&db, &format!("SELECT v FROM pt WHERE tenant_id = {tenant} AND id = {id}")).await;
        let got: Vec<i64> = decode_rows(&out).iter().map(|r| i64::from_le_bytes(r.columns[0].data.into_slice().try_into().unwrap())).collect();
        let want: Vec<i64> = rows.get(&(tenant, id)).copied().into_iter().collect();
        if got != want {
            problems.push(format!("tenant {tenant} id {id}: {got:?}, twin {want:?}"));
        }
    }
    assert!(KEY_LOOKUPS.load(AtomicOrdering::Relaxed) > lookups, "no point lookup went through a loaded key");
    for tenant in [0i64, 7, 22] {
        let out = exec(&db, &format!("SELECT id FROM pt WHERE tenant_id = {tenant}")).await;
        let mut got: Vec<i64> = decode_rows(&out).iter().map(|r| i64::from_le_bytes(r.columns[0].data.into_slice().try_into().unwrap())).collect();
        got.sort_unstable();
        let want: Vec<i64> = rows.keys().filter(|(t, _)| *t == tenant).map(|(_, id)| *id).collect();
        if got != want {
            problems.push(format!("tenant {tenant}: {} ids, twin {}", got.len(), want.len()));
        }
    }
    for (_, part) in &parts {
        problems.extend(index_superset(part, "partition").await);
    }
    drop(parts);
    close(d, db);
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}

/// A table with a gin index writes the FILTER section only and rebuilds
/// all its indexes at a clean reopen (gin waits for phase 3), vouched,
/// answering as the twin.
#[tokio::test(flavor = "multi_thread")]
async fn a_table_with_a_gin_index_writes_filter_only_and_rebuilds() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    let _snap = storage_override::set(d, IndexStorage::Snapshot);
    let mut model = Model::default();
    let db = open(d).await;
    create(&db, "gn").await;
    exec(&db, "CREATE INDEX gn_g ON gn USING gin (doc)").await;
    insert(&db, "gn", &mut model, 0..700).await;
    let t = table(&db, "gn");
    settled(&t).await;
    assert!(matches!(checkpoint(&db, "gn").await, SealOutcome::Published));
    assert_eq!(index_sections(d, "gn"), 0, "a table with a gin index wrote INDEX sections");
    drop(t);
    close(d, db);
    let db = open(d).await;
    let t = table(&db, "gn");
    settled(&t).await;
    assert_eq!(t.index_origin(), Origin::Built);
    assert!(t.gate().vouched(), "the FILTER-only snapshot is all such a table writes");
    let mut problems = Vec::new();
    for k in ["v5", "v77", "absent"] {
        let want: Vec<i64> = model.rows.iter().filter(|(_, r)| r.k.as_deref() == Some(k)).map(|(id, _)| *id).collect();
        let got = ids(&db, "gn", &format!("doc @> '{{\"k\":\"{k}\"}}'")).await;
        if got != want {
            problems.push(format!("doc @> k={k}: {} ids, twin {}", got.len(), want.len()));
        }
    }
    let mut rng = fastrand::Rng::with_seed(10);
    problems.extend(answers(&db, "gn", &model, &model.battery(&mut rng, &[]), "gin table").await);
    drop(t);
    close(d, db);
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}

// ---------------------------------------------------------------------------
// Measurement
// ---------------------------------------------------------------------------

/// `RssAnon` and `RssFile` of this process, in MiB.
fn rss_parts() -> (f64, f64) {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    let field = |name: &str| {
        status
            .lines()
            .find(|l| l.starts_with(name))
            .and_then(|l| l.split_whitespace().nth(1))
            .and_then(|v| v.parse::<f64>().ok())
            .map_or(0.0, |kb| kb / 1024.0)
    };
    (field("RssAnon:"), field("RssFile:"))
}

/// `VmRSS` and `VmHWM` of this process, in bytes.
fn rss_and_peak() -> (u64, u64) {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    let field = |name: &str| {
        status
            .lines()
            .find(|l| l.starts_with(name))
            .and_then(|l| l.split_whitespace().nth(1))
            .and_then(|v| v.parse::<u64>().ok())
            .map_or(0, |kb| kb * 1024)
    };
    (field("VmRSS:"), field("VmHWM:"))
}

/// Open time of a table's indexes, loaded from INDEX sections or rebuilt
/// from rows, and the memory each leaves: a bigint primary key, `(tenant_id,
/// id)` and a hash index (B18's three-index set). Not a test; one mode per
/// process, so each round's memory is its own:
///
/// ```text
/// RDB_B17_DIR=<dir> RDB_B17_ROWS=1000000 RDB_B17_MODE=prepare cargo test --release \
///     -p rasterizeddb_core --lib -- --ignored b17_measure_open_time --nocapture
/// RDB_B17_DIR=<dir> RDB_B17_MODE=snapshot|rebuild cargo test ... (alternate)
/// ```
///
/// `prepare` writes the table and seals it, keeping the pair; each
/// measuring run puts the pair back first (a rebuilt open's vacuum may
/// write), opens, waits for the indexes, and prints its numbers.
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn b17_measure_open_time() {
    let Ok(dir) = std::env::var("RDB_B17_DIR") else {
        eprintln!("B17 measure: set RDB_B17_DIR");
        return;
    };
    let d = Path::new(&dir).join("b17");
    let mode = std::env::var("RDB_B17_MODE").unwrap_or_else(|_| "snapshot".into());
    let pair_dir = Path::new(&dir).join("b17_pair");
    if mode == "prepare" {
        let rows: i64 = std::env::var("RDB_B17_ROWS").ok().and_then(|v| v.parse().ok()).unwrap_or(1_000_000);
        let _ = std::fs::remove_dir_all(&d);
        let _ = std::fs::remove_dir_all(&pair_dir);
        std::fs::create_dir_all(&d).unwrap();
        std::fs::create_dir_all(&pair_dir).unwrap();
        let started = Instant::now();
        let _snap = storage_override::set(&d, IndexStorage::Snapshot);
        let db = open(&d).await;
        exec(&db, "CREATE TABLE m (id BIGINT PRIMARY KEY, tenant_id BIGINT, s TEXT)").await;
        exec(&db, "CREATE INDEX m_tid ON m (tenant_id, id)").await;
        exec(&db, "CREATE INDEX m_s ON m USING hash (s)").await;
        let mut id = 0i64;
        while id < rows {
            let end = (id + 5000).min(rows);
            let values: Vec<String> = (id..end).map(|i| format!("({i}, {}, 's{}')", i % 1000, (i * 7919) % 200_003)).collect();
            exec(&db, &format!("INSERT INTO m (id, tenant_id, s) VALUES {}", values.join(", "))).await;
            id = end;
        }
        let t = table(&db, "m");
        settled(&t).await;
        t.quiesce_for_shutdown().await;
        let outcome = checkpoint(&db, "m").await;
        assert!(matches!(outcome, SealOutcome::Published), "{outcome:?}");
        drop(t);
        close(&d, db);
        for f in ["m.ixs", "m.ixm"] {
            std::fs::copy(d.join(f), pair_dir.join(f)).unwrap();
        }
        let size = |n: &str| std::fs::metadata(d.join(n)).map_or(0, |m| m.len());
        let ixs = std::fs::read(d.join("m.ixs")).unwrap();
        let snap = snapshot::decode(&ixs).unwrap();
        let sections: Vec<String> = snap.sections.iter().map(|(e, b)| format!("{:?}{}={}", e.kind, e.ordinal, b.len())).collect();
        eprintln!(
            "B17 measure: {rows} rows written in {:.1}s; rows {} B, pointers {} B, snapshot {} B ({})",
            started.elapsed().as_secs_f64(),
            size("m.db"),
            size("m_pointers.db"),
            size("m.ixs"),
            sections.join(", ")
        );
        return;
    }
    let storage = IndexStorage::parse(&mode).expect("RDB_B17_MODE: prepare, snapshot or rebuild");
    let _ = env_logger::builder().is_test(true).filter_module("rasterizeddb_core::core::index_store::load", log::LevelFilter::Info).try_init();
    for f in ["m.ixs", "m.ixm"] {
        std::fs::copy(pair_dir.join(f), d.join(f)).unwrap();
    }
    let _mode = storage_override::set(&d, storage);
    let (rss0, _) = rss_and_peak();
    let t0 = Instant::now();
    let db = open(&d).await;
    let t = table(&db, "m");
    let opened = t0.elapsed();
    settled(&t).await;
    let ready = t0.elapsed();
    let want = if storage == IndexStorage::Snapshot { Origin::Loaded } else { Origin::Built };
    assert_eq!(t.index_origin(), want);
    let (rss, peak) = rss_and_peak();
    let (anon, file) = rss_parts();
    eprintln!(
        "B17 measure {}: database open {:.3}s, indexes ready {:.3}s, RSS {:.1} MiB (anon {anon:.1}, file {file:.1}; before the open {:.1}), peak {:.1} MiB",
        storage.name(),
        opened.as_secs_f64(),
        ready.as_secs_f64(),
        rss as f64 / 1048576.0,
        rss0 as f64 / 1048576.0,
        peak as f64 / 1048576.0
    );
    drop(t);
    close(&d, db);
}
