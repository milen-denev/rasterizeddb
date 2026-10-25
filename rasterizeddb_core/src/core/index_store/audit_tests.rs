//! The audit and `rdb_index_check` on a real database (slice B18): a
//! dropped entry in a loaded snapshot is found, the indexes rebuild in
//! place, the bad snapshot never loads again, and answers match the twin;
//! `rdb_index_check` finds a dropped index entry and a dropped filter code,
//! reports a healthy table clean, and refuses what is not a table; a write
//! during a check abandons it, never a false mismatch; concurrent writes
//! never make one; a mismatch rebuilds into the live structures, keeping
//! what writes added meanwhile.
//!
//! The twin is a model of the table fed only acknowledged statements. Each
//! test's directory sets `index_storage = snapshot` and its audit rate for
//! itself only, and reads only its own audits' outcomes.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::Ordering as AtomicOrdering;
use std::time::{Duration, Instant};

use rclite::Arc;

use super::hooks;
use super::*;
use crate::core::database::Database;
use crate::core::database::index_checkpoint::CheckRefusal;
use crate::core::index::KEY_LOOKUPS;
use crate::core::index_store::gate::{self, Proof};
use crate::core::index_store::gate_word::GateState;
use crate::core::index_store::index_section;
use crate::core::index_store::load::settings::IndexStorage;
use crate::core::index_store::load::{Origin, built_total, storage_override};
use crate::core::index_store::marker::{self, CleanRecord, MarkerRecord};
use crate::core::index_store::seal::SealOutcome;
use crate::core::index_store::snapshot::{self, SectionKind};
use crate::core::mvcc::transaction::FROZEN_TX;
use crate::core::row::row::Row;
use crate::core::row::row_pointer::{ROW_POINTER_RECORD_LEN, RowPointerIterator};
use crate::core::row::table::Table;
use crate::core::storage_providers::backend::DbStorageProvider;

type DbTable = Table<DbStorageProvider>;

const SETTLE: Duration = Duration::from_secs(60);

async fn open(dir: &Path) -> Arc<Database> {
    let _ = crate::core::processor::concurrent_processor::ENABLE_CACHE.set(false);
    Arc::new(Database::try_new_named(&dir.to_string_lossy(), false, "b18").await.expect("open the database"))
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

/// The outcomes of the checks under `dir`, once there are more than `seen`.
async fn outcome_after(dir: &Path, seen: usize) -> (String, Outcome) {
    let deadline = Instant::now() + SETTLE;
    loop {
        let all = hooks::outcomes(dir);
        if all.len() > seen {
            return all[seen].clone();
        }
        assert!(Instant::now() < deadline, "no audit outcome under {}", dir.display());
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Waits until checks under `dir` have reached `point` more than `before`
/// times.
async fn reached(dir: &Path, point: Point, before: usize) {
    let deadline = Instant::now() + SETTLE;
    while hooks::reached(dir, point) <= before {
        assert!(Instant::now() < deadline, "no check reached {point:?}");
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

// ---------------------------------------------------------------------------
// The twin
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
struct R {
    a: i64,
    b: i64,
    s: String,
    /// Nullable: every ninth row has none.
    n: Option<i64>,
}

impl R {
    fn for_id(id: i64) -> R {
        R {
            a: (id * 7919) % 5000,
            b: id % 211,
            s: format!("s{}", id % 997),
            n: (id % 9 != 0).then_some(id * 3),
        }
    }

    fn values(&self, id: i64) -> String {
        let n = self.n.map_or("NULL".to_string(), |n| n.to_string());
        format!("({id}, {}, {}, '{}', {n})", self.a, self.b, self.s)
    }
}

#[derive(Debug, Clone, Default)]
struct Model {
    rows: BTreeMap<i64, R>,
}

impl Model {
    /// Predicates through every index and filtered column, on sampled rows'
    /// own values and on `extra` rows.
    fn battery(&self, rng: &mut fastrand::Rng, extra: &[i64]) -> Vec<(String, Vec<i64>)> {
        let keys: Vec<i64> = self.rows.keys().copied().collect();
        let mut picks: Vec<i64> = (0..20).map(|_| keys[rng.usize(..keys.len())]).collect();
        picks.extend(extra.iter().filter(|id| self.rows.contains_key(id)));
        let mut out = Vec::new();
        for id in picks {
            let r = &self.rows[&id];
            let mut preds = vec![
                format!("id = {id}"),
                format!("a = {}", r.a),
                format!("s = '{}'", r.s),
                format!("b = {} AND a = {}", r.b, r.a),
            ];
            if let Some(n) = r.n {
                preds.push(format!("n = {n}"));
            }
            for p in preds {
                let want = self.answer(&p);
                out.push((p, want));
            }
        }
        out.push(("a = 77777777".into(), Vec::new()));
        out
    }

    fn answer(&self, pred: &str) -> Vec<i64> {
        let (col, rest) = pred.split_once(" = ").unwrap();
        self.rows
            .iter()
            .filter(|(id, r)| match col {
                "id" => rest.parse::<i64>().unwrap() == **id,
                "a" => rest.parse::<i64>().unwrap() == r.a,
                "s" => rest.trim_matches('\'') == r.s,
                "n" => Some(rest.parse::<i64>().unwrap()) == r.n,
                "b" => {
                    let (b, a) = rest.split_once(" AND a = ").unwrap();
                    b.parse::<i64>().unwrap() == r.b && a.parse::<i64>().unwrap() == r.a
                }
                _ => unreachable!("{pred}"),
            })
            .map(|(id, _)| *id)
            .collect()
    }
}

async fn answers(db: &Arc<Database>, n: &str, battery: &[(String, Vec<i64>)], label: &str) -> Vec<String> {
    let mut problems = Vec::new();
    for (p, want) in battery {
        let got = ids(db, n, p).await;
        if &got != want {
            problems.push(format!("{label}: WHERE {p}: {got:?}, twin {want:?}"));
        }
    }
    problems
}

async fn create(db: &Arc<Database>, n: &str) {
    exec(db, &format!("CREATE TABLE {n} (id BIGINT PRIMARY KEY, a BIGINT, b BIGINT, s TEXT, n BIGINT)")).await;
    exec(db, &format!("CREATE INDEX {n}_a ON {n} (a)")).await;
    exec(db, &format!("CREATE INDEX {n}_s ON {n} USING hash (s)")).await;
    exec(db, &format!("CREATE INDEX {n}_ba ON {n} (b, a)")).await;
}

async fn insert(db: &Arc<Database>, n: &str, model: &mut Model, ids: std::ops::Range<i64>) {
    let all: Vec<i64> = ids.collect();
    for chunk in all.chunks(250) {
        let values: Vec<String> = chunk.iter().map(|id| R::for_id(*id).values(*id)).collect();
        exec(db, &format!("INSERT INTO {n} (id, a, b, s, n) VALUES {}", values.join(", "))).await;
        for id in chunk {
            model.rows.insert(*id, R::for_id(*id));
        }
    }
}

/// A populated table: inserts, a delete, in-place updates of `a`, a new
/// version of `s`, vacuum.
async fn populate(db: &Arc<Database>, n: &str, model: &mut Model, rows: i64) {
    create(db, n).await;
    insert(db, n, model, 0..rows).await;
    exec(db, &format!("DELETE FROM {n} WHERE id % 37 = 5")).await;
    model.rows.retain(|id, _| id % 37 != 5);
    exec(db, &format!("UPDATE {n} SET a = a + 100000 WHERE id % 53 = 1")).await;
    exec(db, &format!("UPDATE {n} SET s = 'v' || s WHERE id % 61 = 2")).await;
    for (id, r) in model.rows.iter_mut() {
        if id % 53 == 1 {
            r.a += 100_000;
        }
        if id % 61 == 2 {
            r.s = format!("v{}", r.s);
        }
    }
    exec(db, &format!("VACUUM {n}")).await;
}

/// Every visible row's slot and column bytes by write order.
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

fn write_order(t: &DbTable, column: &str) -> u32 {
    t.schema.fields.iter().find(|f| f.name == column).unwrap().write_order as u32
}

/// The slot of row `id` and its column bytes.
async fn slot_of(t: &DbTable, id: i64) -> (u64, BTreeMap<u32, Vec<u8>>) {
    let wo = write_order(t, "id");
    visible_rows(t)
        .await
        .into_iter()
        .find(|(_, cols)| cols.get(&wo).is_some_and(|v| v.as_slice() == id.to_le_bytes()))
        .expect("the row is visible")
}

fn live_index(t: &DbTable, name: &str) -> (usize, std::sync::Arc<crate::core::index::LiveIndex>) {
    let list = t.data().unwrap().indexes.as_ref().unwrap().list.clone();
    let at = list.iter().position(|ix| ix.decl.name == name).expect("the index");
    (at, list[at].clone())
}

/// Every visible row is listed in every index under its own key.
async fn index_superset(t: &DbTable, label: &str) -> Vec<String> {
    let rows = visible_rows(t).await;
    let mut problems = Vec::new();
    for ix in t.data().unwrap().indexes.as_ref().unwrap().list.iter() {
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

/// Writes `ixs` (sections replaced through `replace`) with a `CLEAN` that
/// proves it: every checksum valid, the identity the open will expect.
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

/// A section's stream with `(key, slot)` dropped, re-encoded.
fn without_entry(bytes: &[u8], n_slots: u64, key: &[u8], slot: u64) -> Vec<u8> {
    let mut w = index_section::SectionWriter::new();
    let mut dropped = false;
    index_section::decode_into(bytes, n_slots, |part| match part {
        crate::core::index::Sorted::Entry(k, slots) if k == key => {
            let kept: Vec<u64> = slots.iter().copied().filter(|s| *s != slot).collect();
            dropped = kept.len() < slots.len();
            if kept.is_empty() { Ok(()) } else { w.push(crate::core::index::Sorted::Entry(k, &kept)) }
        }
        other => w.push(other),
    })
    .unwrap();
    assert!(dropped, "the entry to drop was not in the section");
    w.finish().unwrap()
}

/// The marker log ends `DIRTY` (or anything but a provable `CLEAN`): the
/// snapshot can never load again.
fn log_refuses_snapshot(d: &Path, stem: &str) -> bool {
    let ixm = std::fs::read(d.join(format!("{stem}.ixm"))).unwrap();
    marker::scan(&ixm).map_or(true, |s| s.proven_clean().is_none())
}

// ---------------------------------------------------------------------------
// The audit after a load
// ---------------------------------------------------------------------------

/// A snapshot whose INDEX section lacks one entry (every checksum valid:
/// the bug the proof cannot see) loads, and answers `a = …` without that
/// row: a silent wrong answer. The audit after the load (rate 1) finds the
/// entry missing, makes `DIRTY` durable, rebuilds the indexes in place,
/// and the answers are the twin's again, through the index. A close
/// without a seal: the reopen builds (the build counter moves) and answers
/// as the twin. A seal, then a reopen: it loads, and its audit is clean.
///
/// Mutations: the audit checks the safe direction (live within scratch:
/// nothing found); a mismatch does not demote and rebuild (the row stays
/// missing).
#[tokio::test(flavor = "multi_thread")]
async fn an_audit_after_a_load_finds_a_dropped_entry_and_rebuilds_in_place() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    let _snap = storage_override::set(d, IndexStorage::Snapshot);
    let _never = hooks::rate(d, 0);
    let mut model = Model::default();
    let victim = 1234i64;
    let (ordinal, key, slot) = {
        let db = open(d).await;
        populate(&db, "au", &mut model, 3000).await;
        let t = table(&db, "au");
        settled(&t).await;
        assert!(matches!(checkpoint(&db, "au").await, SealOutcome::Published));
        let (ordinal, ix) = live_index(&t, "au_a");
        let (slot, cols) = slot_of(&t, victim).await;
        let key = ix.key_of(|wo| cols.get(&wo).map(|v| v.as_slice())).unwrap();
        drop(t);
        close(d, db);
        (ordinal as u16, key, slot)
    };
    let n_slots = std::fs::metadata(d.join("au_pointers.db")).unwrap().len() / ROW_POINTER_RECORD_LEN as u64;
    rewrite_snapshot(d, "au", |o, b| if o == ordinal { without_entry(b, n_slots, &key, slot) } else { b.to_vec() });

    drop(_never);
    let _every = hooks::rate(d, 1);
    let held = hooks::hold(d, Point::Scanned);
    let mut rng = fastrand::Rng::with_seed(0xb18);
    let battery = model.battery(&mut rng, &[victim]);
    let victim_a = format!("a = {}", model.rows[&victim].a);
    let mismatches = mismatch_total();
    let db = open(d).await;
    let t = table(&db, "au");
    settled(&t).await;
    assert_eq!(t.index_origin(), Origin::Loaded, "the rewritten snapshot did not load");
    reached(d, Point::Scanned, 0).await;
    // The silent wrong answer the audit exists for.
    let lookups = KEY_LOOKUPS.load(AtomicOrdering::Relaxed);
    let before = ids(&db, "au", &victim_a).await;
    assert!(KEY_LOOKUPS.load(AtomicOrdering::Relaxed) > lookups, "the lookup did not go through the index");
    assert!(!before.contains(&victim), "the dropped entry was found anyway: {before:?}");
    drop(held);

    let (name, outcome) = outcome_after(d, 0).await;
    assert_eq!(name, "au");
    let Outcome::Mismatch { findings, rebuilt } = &outcome else { panic!("the audit did not find the dropped entry: {outcome:?}") };
    assert!(*rebuilt, "the in-place rebuild did not finish: {outcome:?}");
    assert_eq!(findings.missing_entries, 1, "{findings:?}");
    assert_eq!(findings.filter_misses, 0, "{findings:?}");
    assert!(mismatch_total() > mismatches);
    assert_eq!(t.gate().state(), GateState::Dirty, "the mismatch took no ticket");
    assert!(log_refuses_snapshot(d, "au"), "DIRTY is not durable: the bad snapshot would load again");
    settled(&t).await;
    let mut problems = answers(&db, "au", &battery, "after the audit").await;
    problems.extend(index_superset(&t, "after the audit").await);
    drop(t);
    close(d, db);

    // A close without a seal: the next open builds.
    let builds = built_total();
    let db = open(d).await;
    let t = table(&db, "au");
    settled(&t).await;
    assert_eq!(t.index_origin(), Origin::Built, "a snapshot the audit refused loaded again");
    assert!(built_total() > builds, "the build counter did not move");
    problems.extend(answers(&db, "au", &battery, "rebuilt reopen").await);
    // A clean point writes a good snapshot; the reopen loads it, and its
    // audit is clean.
    assert!(matches!(checkpoint(&db, "au").await, SealOutcome::Published));
    drop(t);
    close(d, db);
    let seen = hooks::outcomes(d).len();
    let db = open(d).await;
    let t = table(&db, "au");
    settled(&t).await;
    assert_eq!(t.index_origin(), Origin::Loaded);
    let (_, outcome) = outcome_after(d, seen).await;
    assert!(matches!(outcome, Outcome::Clean(_)), "the good snapshot's audit: {outcome:?}");
    problems.extend(answers(&db, "au", &battery, "loaded again").await);
    drop(t);
    close(d, db);
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}

// ---------------------------------------------------------------------------
// rdb_index_check
// ---------------------------------------------------------------------------

/// A healthy table checks clean: every visible row compared, through every
/// index and filtered column. Then an index entry and a row's filter codes
/// are dropped in memory, as a bug would (no ticket, the word unmoved), and
/// both rows go missing from their answers; the check finds exactly that,
/// rebuilds in place, and the answers are the twin's. A partitioned table
/// checks each partition. An index, a catalogue view and an unknown name
/// are refused before anything is read.
#[tokio::test(flavor = "multi_thread")]
async fn rdb_index_check_finds_dropped_entries_and_reports_a_healthy_table_clean() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    let _snap = storage_override::set(d, IndexStorage::Snapshot);
    let _never = hooks::rate(d, 0);
    let mut model = Model::default();
    let db = open(d).await;
    populate(&db, "ck", &mut model, 6000).await;
    let t = table(&db, "ck");
    settled(&t).await;
    let visible = visible_rows(&t).await.len() as u64;

    let report = db.index_check("ck", None).await.expect("rdb_index_check");
    assert_eq!(report.checked.len(), 1);
    let Outcome::Clean(f) = &report.checked[0].outcome else { panic!("a healthy table: {:?}", report.checked[0].outcome) };
    assert_eq!(f.rows, visible, "{f:?}");
    assert_eq!(f.indexes, 4, "{f:?}");
    assert_eq!(f.entries, 4 * visible, "{f:?}");
    assert!(f.filter_checked >= 3 * visible, "the filter was not compared: {f:?}");
    assert!(f.skipped.is_empty(), "{f:?}");
    assert_eq!(report.result(), "clean");

    // The dropped entry and the dropped filter codes.
    let (lost_ix, lost_flt) = (2222i64, 3333i64);
    let state = t.gate().state();
    {
        let build = gate::test_mint();
        let (_, ix) = live_index(&t, "ck_s");
        let (slot, cols) = slot_of(&t, lost_ix).await;
        let key = ix.key_of(|wo| cols.get(&wo).map(|v| v.as_slice())).unwrap();
        ix.remove(Proof::from(&build), slot, &key);
        let (slot, _) = slot_of(&t, lost_flt).await;
        t.data().unwrap().filters.as_ref().unwrap().on_delete(Proof::from(&build), slot);
    }
    assert_eq!(t.gate().state(), state);
    let s_pred = format!("s = '{}'", model.rows[&lost_ix].s);
    let n_pred = format!("n = {}", model.rows[&lost_flt].n.unwrap());
    let a_pred = format!("a = {}", model.rows[&lost_flt].a);
    assert!(!ids(&db, "ck", &s_pred).await.contains(&lost_ix), "the dropped index entry was found anyway");
    assert!(
        !ids(&db, "ck", &n_pred).await.contains(&lost_flt) || !ids(&db, "ck", &a_pred).await.contains(&lost_flt),
        "the dropped filter codes prune nothing"
    );

    let mismatches = mismatch_total();
    let report = db.index_check("ck", None).await.expect("rdb_index_check");
    let Outcome::Mismatch { findings, rebuilt } = &report.checked[0].outcome else {
        panic!("the dropped entries were not found: {:?}", report.checked[0].outcome)
    };
    assert!(*rebuilt);
    assert_eq!(findings.missing_entries, 1, "{findings:?}");
    assert!(findings.filter_misses >= 1, "{findings:?}");
    assert!(mismatch_total() > mismatches);
    assert_eq!(report.result(), "mismatch: rebuilt");
    settled(&t).await;
    let mut rng = fastrand::Rng::with_seed(0xb18_2);
    let battery = model.battery(&mut rng, &[lost_ix, lost_flt]);
    let mut problems = answers(&db, "ck", &battery, "after the check").await;
    problems.extend(index_superset(&t, "after the check").await);
    let report = db.index_check("ck", None).await.unwrap();
    assert!(matches!(report.checked[0].outcome, Outcome::Clean(_)), "{:?}", report.checked[0].outcome);

    // A partitioned table: each partition.
    exec(&db, "CREATE TABLE pk (tenant_id BIGINT NOT NULL, id BIGINT NOT NULL, v BIGINT, PRIMARY KEY (tenant_id, id)) PARTITION BY HASH (tenant_id) PARTITIONS 3").await;
    let values: Vec<String> = (0..600i64).map(|i| format!("({}, {i}, {})", i % 17, i * 2)).collect();
    exec(&db, &format!("INSERT INTO pk (tenant_id, id, v) VALUES {}", values.join(", "))).await;
    let report = db.index_check("pk", None).await.expect("a partitioned table");
    assert!(report.checked.len() >= 3, "{report:?}");
    assert!(report.checked.iter().all(|c| matches!(c.outcome, Outcome::Clean(_))), "{report:?}");
    assert_eq!(report.findings().rows, 600);

    // Refusals.
    assert!(matches!(db.index_check("ck_a", None).await, Err(CheckRefusal::NotATable(_))));
    assert!(matches!(db.index_check("pg_class", None).await, Err(CheckRefusal::NotATable(_))));
    assert!(matches!(db.index_check("no_such", None).await, Err(CheckRefusal::NotFound(_))));
    drop(t);
    close(d, db);
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}

/// The stability rule. The audit after a load holds after its scratch scan;
/// meanwhile in-place updates move rows' keys (the live index drops the
/// old keys the scratch read: no other transaction is running, so the
/// in-place path fires), and rows are deleted and vacuumed. The audit is
/// abandoned, never a mismatch, and the table answers as the twin. Then
/// the same on demand, under `rdb_index_check`'s own statement owner.
///
/// Mutation (the verdict ignores the tickets taken): the audit after the
/// load reports a false mismatch.
#[tokio::test(flavor = "multi_thread")]
async fn a_write_during_a_check_abandons_it() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    let _snap = storage_override::set(d, IndexStorage::Snapshot);
    let never = hooks::rate(d, 0);
    let mut model = Model::default();
    {
        let db = open(d).await;
        populate(&db, "wr", &mut model, 2000).await;
        settled(&table(&db, "wr")).await;
        assert!(matches!(checkpoint(&db, "wr").await, SealOutcome::Published));
        close(d, db);
    }
    drop(never);
    let _every = hooks::rate(d, 1);
    let held = hooks::hold(d, Point::Scanned);
    let db = open(d).await;
    let t = table(&db, "wr");
    settled(&t).await;
    assert_eq!(t.index_origin(), Origin::Loaded);
    reached(d, Point::Scanned, 0).await;
    let in_place = t.in_place_column_updates();
    let moved: Vec<i64> = model.rows.keys().copied().filter(|id| id % 50 == 0).collect();
    for id in &moved {
        let r = model.rows.get_mut(id).unwrap();
        r.a += 7;
        exec(&db, &format!("UPDATE wr SET a = {} WHERE id = {id}", r.a)).await;
    }
    assert!(t.in_place_column_updates() > in_place, "the updates were not in place: the test needs keys moved");
    exec(&db, "DELETE FROM wr WHERE id % 11 = 3").await;
    exec(&db, "VACUUM wr").await;
    model.rows.retain(|id, _| id % 11 != 3);
    drop(held);
    let (_, outcome) = outcome_after(d, 0).await;
    assert!(matches!(outcome, Outcome::Abandoned(_)), "an audit a write overlapped: {outcome:?}");

    // On demand.
    let held = hooks::hold(d, Point::Scanned);
    let before = hooks::reached(d, Point::Scanned);
    let check = {
        let db = db.clone();
        tokio::spawn(async move { db.index_check("wr", None).await.unwrap() })
    };
    reached(d, Point::Scanned, before).await;
    exec(&db, "UPDATE wr SET b = b + 1 WHERE id % 4 = 1").await;
    for (id, r) in model.rows.iter_mut() {
        if id % 4 == 1 {
            r.b += 1;
        }
    }
    drop(held);
    let report = tokio::time::timeout(SETTLE, check).await.expect("the check never ended").unwrap();
    let outcome = &report.checked[0].outcome;
    assert!(matches!(outcome, Outcome::Abandoned(_)), "a check a write overlapped: {outcome:?}");
    let mut rng = fastrand::Rng::with_seed(0xb18_3);
    let mut problems = answers(&db, "wr", &model.battery(&mut rng, &[50, 100, 14, 1, 9]), "after").await;
    problems.extend(index_superset(&t, "after").await);
    drop(t);
    close(d, db);
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}

/// Checks run back to back while a writer inserts, updates in place and as
/// new versions, and deletes, in bursts with pauses between. No check is
/// ever a mismatch; some are abandoned (a write overlapped), some clean
/// (between bursts). The table answers as the twin at the end.
#[tokio::test(flavor = "multi_thread")]
async fn checks_under_concurrent_writes_never_report_a_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    let _snap = storage_override::set(d, IndexStorage::Snapshot);
    let _every = hooks::rate(d, 1);
    let mut model = Model::default();
    {
        let db = open(d).await;
        populate(&db, "cw", &mut model, 2500).await;
        settled(&table(&db, "cw")).await;
        assert!(matches!(checkpoint(&db, "cw").await, SealOutcome::Published));
        close(d, db);
    }
    let db = open(d).await;
    let t = table(&db, "cw");
    settled(&t).await;
    assert_eq!(t.index_origin(), Origin::Loaded);
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let checker = {
        let (db, stop) = (db.clone(), stop.clone());
        tokio::spawn(async move {
            while !stop.load(AtomicOrdering::Relaxed) {
                db.index_check("cw", None).await.unwrap();
                tokio::time::sleep(Duration::from_millis(3)).await;
            }
        })
    };
    let mut next = 10_000i64;
    let started = Instant::now();
    let mut burst = 0i64;
    while started.elapsed() < Duration::from_secs(4) {
        burst += 1;
        insert(&db, "cw", &mut model, next..next + 40).await;
        next += 40;
        exec(&db, &format!("UPDATE cw SET a = a + 1 WHERE id % 13 = {}", burst % 13)).await;
        exec(&db, &format!("UPDATE cw SET s = s || 'x' WHERE id % 17 = {}", burst % 17)).await;
        exec(&db, &format!("DELETE FROM cw WHERE id % 29 = {}", burst % 29)).await;
        for (id, r) in model.rows.iter_mut() {
            if id % 13 == burst % 13 {
                r.a += 1;
            }
            if id % 17 == burst % 17 {
                r.s.push('x');
            }
        }
        model.rows.retain(|id, _| id % 29 != burst % 29);
        if burst % 3 == 0 {
            exec(&db, "VACUUM cw").await;
        }
        tokio::time::sleep(Duration::from_millis(60)).await;
    }
    stop.store(true, AtomicOrdering::Relaxed);
    checker.await.unwrap();
    let outcomes = hooks::outcomes(d);
    let count = |f: fn(&Outcome) -> bool| outcomes.iter().filter(|(_, o)| f(o)).count();
    let mismatched: Vec<_> = outcomes.iter().filter(|(_, o)| matches!(o, Outcome::Mismatch { .. })).collect();
    assert!(mismatched.is_empty(), "a check reported a mismatch under concurrent writes: {mismatched:?}");
    assert!(count(|o| matches!(o, Outcome::Clean(_))) > 0, "no check completed: {outcomes:?}");
    assert!(count(|o| matches!(o, Outcome::Abandoned(_))) > 0, "no check overlapped a write: {}", outcomes.len());
    let mut rng = fastrand::Rng::with_seed(0xb18_4);
    let mut problems = answers(&db, "cw", &model.battery(&mut rng, &[next - 1, next - 41]), "after").await;
    problems.extend(index_superset(&t, "after").await);
    drop(t);
    close(d, db);
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}

/// A mismatch rebuilds into the live structures, never swaps the scratch
/// in. The check holds at its verdict while rows are inserted and updated
/// (their writers feed the live indexes); once let go it rebuilds from
/// rows, and every row written meanwhile is found through the indexes.
///
/// Mutation (swap the side build in: the live indexes cleared and filled
/// from the scratch): the rows written during the hold are missing.
#[tokio::test(flavor = "multi_thread")]
async fn a_mismatch_rebuilds_in_place_keeping_what_writes_added_meanwhile() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    let _snap = storage_override::set(d, IndexStorage::Snapshot);
    let _never = hooks::rate(d, 0);
    let mut model = Model::default();
    let db = open(d).await;
    populate(&db, "sw", &mut model, 2000).await;
    let t = table(&db, "sw");
    settled(&t).await;
    {
        let build = gate::test_mint();
        let (_, ix) = live_index(&t, "sw_a");
        let (slot, cols) = slot_of(&t, 777).await;
        let key = ix.key_of(|wo| cols.get(&wo).map(|v| v.as_slice())).unwrap();
        ix.remove(Proof::from(&build), slot, &key);
    }
    let held = hooks::hold(d, Point::Verdict);
    let check = {
        let db = db.clone();
        tokio::spawn(async move { db.index_check("sw", None).await.unwrap() })
    };
    reached(d, Point::Verdict, 0).await;
    insert(&db, "sw", &mut model, 50_000..50_300).await;
    exec(&db, "UPDATE sw SET a = a + 900000 WHERE id % 7 = 3").await;
    for (id, r) in model.rows.iter_mut() {
        if id % 7 == 3 {
            r.a += 900_000;
        }
    }
    drop(held);
    let report = tokio::time::timeout(SETTLE, check).await.expect("the check never ended").unwrap();
    assert!(
        matches!(report.checked[0].outcome, Outcome::Mismatch { rebuilt: true, .. }),
        "{:?}",
        report.checked[0].outcome
    );
    settled(&t).await;
    let mut rng = fastrand::Rng::with_seed(0xb18_5);
    let extra: Vec<i64> = (50_000..50_300).step_by(7).chain([777, 3, 10, 17]).collect();
    let lookups = KEY_LOOKUPS.load(AtomicOrdering::Relaxed);
    let mut problems = answers(&db, "sw", &model.battery(&mut rng, &extra), "after the rebuild").await;
    assert!(KEY_LOOKUPS.load(AtomicOrdering::Relaxed) > lookups, "nothing was answered through an index");
    problems.extend(index_superset(&t, "after the rebuild").await);
    drop(t);
    close(d, db);
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}

/// `missing_from` on its own: the dangerous direction, the unkeyed slots
/// that excuse a pair, a scratch-unkeyed slot the live index holds.
#[test]
fn missing_from_checks_the_dangerous_direction_only() {
    use crate::core::db_type::DbType;
    use crate::core::index::{IndexDecl, KeyBuf, LiveIndex, Method};
    let decl = IndexDecl {
        name: "t_v".into(),
        method: Method::BTree,
        unique: false,
        columns: vec![0],
        is_deleted: false,
    };
    let live = LiveIndex::new(decl, vec![(0, DbType::I64)], false);
    let build = gate::test_mint();
    let k = |v: u8| KeyBuf::from_slice(&[v]);
    for (slot, v) in [(0u64, 1u8), (1, 1), (2, 2), (5, 9)] {
        live.insert(Proof::from(&build), slot, Some(k(v)));
    }
    live.insert(Proof::from(&build), 7, None);
    // Everything the scratch lists is in the live index: nothing missing,
    // though the live index has more (slot 5 under 9).
    let mut s = IndexScratch {
        pairs: vec![(k(2), 2), (k(1), 0), (k(1), 1), (k(4), 7)],
        unkeyed: vec![2],
    };
    assert_eq!(missing_from(&live, &mut s, 8).0, 0);
    // A slot under a key the live index lacks, one under a key it has, and
    // an unkeyed slot it holds nowhere: three missing.
    let mut s = IndexScratch {
        pairs: vec![(k(1), 3), (k(3), 4), (k(0), 6)],
        unkeyed: vec![8],
    };
    let (n, got) = missing_from(&live, &mut s, 8);
    assert_eq!(n, 4, "{got:?}");
    assert_eq!(got, vec![(k(0), 6), (k(1), 3), (k(3), 4)]);
}
