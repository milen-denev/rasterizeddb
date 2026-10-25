//! The open's proof and the FILTER load on a real database (slice B14): a
//! loaded filter answers every query a rebuilt one does, row for row; every
//! identity field, checksum and log shape that does not match forces a
//! rebuild; crash images never load a snapshot that does not match their
//! files; `_flt.db` is deleted at open under the evidence rule; an open that
//! loads writes nothing and its first vacuum takes no ticket; `cleared_below`
//! survives; DROP and the reaper remove the pair.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc as StdArc;
use std::time::Duration;

use parking_lot::Mutex;
use rclite::Arc;

use super::*;
use crate::core::database::Database;
use crate::core::index_store::census::{Census, holds_on};
use crate::core::index_store::identity::VersionTuple;
use crate::core::index_store::marker::{CleanRecord, MarkerRecord};
use crate::core::index_store::seal::SealOutcome;
use crate::core::power_loss::failpoint::{self, Outcome};
use crate::core::power_loss::image::{CrashPoint, Recorder};
use crate::core::row::row::Row;
use crate::core::row::row_pointer::ROW_POINTER_RECORD_LEN;
use crate::core::row::table::Table;
use crate::core::storage_providers::backend::DbStorageProvider;

type DbTable = Table<DbStorageProvider>;

async fn open(dir: &Path, name: &str) -> Arc<Database> {
    let _ = crate::core::processor::concurrent_processor::ENABLE_CACHE.set(false);
    Arc::new(
        Database::try_new_named(&dir.to_string_lossy(), false, name)
            .await
            .expect("open the database"),
    )
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

async fn ready(t: &DbTable) {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while !t.filters_ready().unwrap() {
        assert!(std::time::Instant::now() < deadline, "the filter never became ready");
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    while t.indexes().unwrap().is_some_and(|ix| !ix.is_ready()) {
        assert!(std::time::Instant::now() < deadline, "the index build never finished");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

async fn checkpoint(db: &Arc<Database>, name: &str) -> SealOutcome {
    let mut sealed = db.index_checkpoint(name, None).await.expect("rdb_index_checkpoint").sealed;
    assert_eq!(sealed.len(), 1);
    sealed.remove(0).outcome
}

/// The ids `clause` selects from `name`, sorted.
async fn ids(db: &Arc<Database>, name: &str, clause: &str) -> Vec<i64> {
    let out = exec(db, &format!("SELECT id FROM {name} WHERE {clause}")).await;
    assert_eq!(out.first(), Some(&2), "not a row set: {}", String::from_utf8_lossy(&out));
    let rows: Vec<Row> = crate::core::row::row::vec_into_rows(&out[1..]).unwrap_or_default();
    let mut v: Vec<i64> = rows
        .iter()
        .map(|r| i64::from_le_bytes(r.columns[0].data.into_slice().try_into().expect("an i64 id")))
        .collect();
    v.sort_unstable();
    v
}

fn flt_files(dir: &Path) -> Vec<String> {
    std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok()?.file_name().into_string().ok())
        .filter(|n| n.contains("_flt.db"))
        .collect()
}

/// What the files and this binary say now, as `Table::assemble` builds it.
async fn expected_identity(t: &DbTable) -> Identity {
    let pointers_len = t.io_pointers().unwrap().get_len().await;
    let filters = t.filters().unwrap().clone();
    Identity {
        incarnation: t.schema.incarnation,
        slot_epoch: t.gate().slot_epoch(),
        n_slots: pointers_len / ROW_POINTER_RECORD_LEN as u64,
        pointers_len,
        rows_len: t.io_rows().unwrap().get_len().await,
        versions: VersionTuple::current(crate::core::filter::persist::FORMAT_VER),
        shape_hash: crate::core::index::shape_of(&t.schema),
        filter_config_hash: filters.map_or(0, |f| f.config_hash()),
    }
}

use crate::core::storage_providers::traits::StorageIO;

// ---------------------------------------------------------------------------
// The model the twin checks the database against.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct ModelRow {
    a: i64,
    b: i64,
    s: String,
    f: f64,
}

#[derive(Default)]
struct Model {
    rows: BTreeMap<i64, ModelRow>,
}

impl Model {
    fn row_for(id: i64) -> ModelRow {
        ModelRow {
            a: id * 7 + 3,
            b: id % 211,
            s: format!("s{:06}", (id * 7919) % 1_000_003),
            f: id as f64 * 0.5,
        }
    }

    async fn insert(&mut self, db: &Arc<Database>, name: &str, ids: std::ops::Range<i64>) {
        let all: Vec<i64> = ids.collect();
        for chunk in all.chunks(500) {
            let values: Vec<String> = chunk
                .iter()
                .map(|&id| {
                    let r = Model::row_for(id);
                    format!("({id}, {}, {}, '{}', {:.1})", r.a, r.b, r.s, r.f)
                })
                .collect();
            exec(db, &format!("INSERT INTO {name} (id, a, b, s, f) VALUES {}", values.join(", "))).await;
            for &id in chunk {
                self.rows.insert(id, Model::row_for(id));
            }
        }
    }

    async fn delete_range(&mut self, db: &Arc<Database>, name: &str, lo: i64, hi: i64) {
        exec(db, &format!("DELETE FROM {name} WHERE id >= {lo} AND id < {hi}")).await;
        self.rows.retain(|id, _| !(lo..hi).contains(id));
    }

    /// Single-row updates of each column to values no row had, the case a
    /// stale fingerprint misses.
    async fn update_some(&mut self, db: &Arc<Database>, name: &str, rng: &mut fastrand::Rng, n: usize, tag: i64) {
        let keys: Vec<i64> = self.rows.keys().copied().collect();
        for i in 0..n {
            let id = keys[rng.usize(..keys.len())];
            let row = self.rows.get_mut(&id).unwrap();
            match i % 3 {
                0 => {
                    let v = 50_000_000 + tag * 10_000 + i as i64;
                    exec(db, &format!("UPDATE {name} SET a = {v} WHERE id = {id}")).await;
                    row.a = v;
                }
                1 => {
                    let v = format!("upd{tag}_{i}");
                    exec(db, &format!("UPDATE {name} SET s = '{v}' WHERE id = {id}")).await;
                    row.s = v;
                }
                _ => {
                    let v = 9_000 + tag * 100 + i as i64;
                    exec(db, &format!("UPDATE {name} SET b = {v} WHERE id = {id}")).await;
                    row.b = v;
                }
            }
        }
    }
}

/// One predicate: its SQL and the model's answer.
#[derive(Debug, Clone)]
enum Pred {
    AEq(i64),
    ARange(i64, i64),
    BEq(i64),
    SEq(String),
    FGe(f64),
    AIn(Vec<i64>),
    AOrB(i64, i64),
    SAndA(String, i64),
}

impl Pred {
    fn sql(&self) -> String {
        match self {
            Pred::AEq(v) => format!("a = {v}"),
            Pred::ARange(lo, hi) => format!("a > {lo} AND a < {hi}"),
            Pred::BEq(v) => format!("b = {v}"),
            Pred::SEq(v) => format!("s = '{v}'"),
            Pred::FGe(v) => format!("f >= {v:.1}"),
            Pred::AIn(vs) => format!("a IN ({})", vs.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(", ")),
            Pred::AOrB(a, b) => format!("a = {a} OR b = {b}"),
            Pred::SAndA(s, a) => format!("s = '{s}' AND a > {a}"),
        }
    }

    fn matches(&self, r: &ModelRow) -> bool {
        match self {
            Pred::AEq(v) => r.a == *v,
            Pred::ARange(lo, hi) => r.a > *lo && r.a < *hi,
            Pred::BEq(v) => r.b == *v,
            Pred::SEq(v) => r.s == *v,
            Pred::FGe(v) => r.f >= *v,
            Pred::AIn(vs) => vs.contains(&r.a),
            Pred::AOrB(a, b) => r.a == *a || r.b == *b,
            Pred::SAndA(s, a) => r.s == *s && r.a > *a,
        }
    }
}

impl Model {
    fn answer(&self, p: &Pred) -> Vec<i64> {
        self.rows.iter().filter(|(_, r)| p.matches(r)).map(|(id, _)| *id).collect()
    }

    /// Predicates drawn from the live rows (most of all the updated ones,
    /// whose values only a current filter knows) and from values no row
    /// has, so both a missing row and a wrong extra one would show.
    fn predicates(&self, seed: u64, n: usize) -> Vec<Pred> {
        let mut rng = fastrand::Rng::with_seed(seed);
        let rows: Vec<&ModelRow> = self.rows.values().collect();
        let updated: Vec<&ModelRow> = rows
            .iter()
            .copied()
            .filter(|r| r.a >= 50_000_000 || r.s.starts_with("upd") || r.b >= 9_000)
            .collect();
        let pick = |rng: &mut fastrand::Rng| -> &ModelRow {
            if !updated.is_empty() && rng.bool() {
                updated[rng.usize(..updated.len())]
            } else {
                rows[rng.usize(..rows.len())]
            }
        };
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            let r = pick(&mut rng);
            out.push(match i % 8 {
                0 => Pred::AEq(r.a),
                1 => Pred::ARange(r.a - 30, r.a + 30),
                2 => Pred::BEq(r.b),
                3 => Pred::SEq(r.s.clone()),
                4 => Pred::FGe(r.f + 1000.0),
                5 => Pred::AIn(vec![r.a, pick(&mut rng).a, 1, pick(&mut rng).a]),
                6 => Pred::AOrB(r.a, pick(&mut rng).b),
                _ => Pred::SAndA(r.s.clone(), r.a - 1),
            });
        }
        // Values nothing has.
        out.push(Pred::AEq(-17));
        out.push(Pred::SEq("nobody".into()));
        out
    }
}

/// Every predicate's rows from the database against the model's; returns
/// the database's answers for a row-for-row comparison across opens.
async fn battery(db: &Arc<Database>, name: &str, model: &Model, preds: &[Pred], when: &str) -> Vec<Vec<i64>> {
    let mut answers = Vec::with_capacity(preds.len());
    for p in preds {
        let got = ids(db, name, &p.sql()).await;
        let want = model.answer(p);
        if got != want {
            let missing: Vec<i64> = want.iter().copied().filter(|i| !got.contains(i)).collect();
            let extra: Vec<i64> = got.iter().copied().filter(|i| !want.contains(i)).collect();
            panic!(
                "{when}: `{}` returned {} rows, the model {}: missing {missing:?}, extra {extra:?}",
                p.sql(),
                got.len(),
                want.len()
            );
        }
        answers.push(got);
    }
    answers
}

/// The table every twin uses: no primary key, so no secondary index, and
/// the filter alone prunes.
async fn create_twin(db: &Arc<Database>, name: &str) {
    exec(db, &format!("CREATE TABLE {name} (id BIGINT, a BIGINT, b BIGINT, s TEXT, f DOUBLE)")).await;
}

fn pair_bytes(dir: &Path, stem: &str) -> (Vec<u8>, Vec<u8>) {
    (
        std::fs::read(dir.join(format!("{stem}.ixs"))).expect("the snapshot"),
        std::fs::read(dir.join(format!("{stem}.ixm"))).expect("the marker log"),
    )
}

/// The false-negative guard. A table goes through inserts, deletes, single
/// row updates to fresh values, vacuum, `VACUUM FULL` and more writes, is
/// sealed and reopened: the loaded filter answers every predicate exactly as
/// the model does and exactly as a rebuilt filter does, row for row, and it
/// prunes. Then the table is compacted again and grown past the old size,
/// sealed again, and the first snapshot's pair is put back (a stale pair: a
/// restored backup, a half-done copy): its `CLEAN` describes other files,
/// so the reopen must rebuild, and every row is still found.
///
/// Mutation: `prove` skips the identity comparison. The stale pair then
/// loads (its slot count fits), its codes describe rows that compaction
/// moved, and rows go missing from the second reopen's answers.
#[tokio::test(flavor = "multi_thread")]
async fn a_loaded_filter_answers_every_query_a_rebuilt_one_does() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    let mut model = Model::default();
    let mut rng = fastrand::Rng::with_seed(0xb14);
    let live_mask;
    {
        let db = open(d, "b14_twin").await;
        create_twin(&db, "g").await;
        model.insert(&db, "g", 0..6000).await;
        model.delete_range(&db, "g", 500, 900).await;
        model.update_some(&db, "g", &mut rng, 60, 1).await;
        table(&db, "g").vacuum_now().await.unwrap();
        exec(&db, "VACUUM FULL g").await;
        let t = table(&db, "g");
        ready(&t).await;
        model.insert(&db, "g", 6000..6500).await;
        model.delete_range(&db, "g", 3000, 3100).await;
        model.update_some(&db, "g", &mut rng, 30, 2).await;
        t.vacuum_now().await.unwrap();
        t.vacuum_now().await.unwrap();
        // What the live filter prunes for one equality, which the loaded one
        // must match exactly (it is the same filter).
        let probe = model.rows.values().nth(1234).unwrap().a;
        live_mask = t
            .build_candidates(&format!("a = {probe}"), None)
            .await
            .unwrap()
            .expect("the live filter declined an equality on a distinct column")
            .count();
        // As shutdown does: nothing writes between the seal and the close.
        t.quiesce_for_shutdown().await;
        let outcome = checkpoint(&db, "g").await;
        assert!(matches!(outcome, SealOutcome::Published), "{outcome:?}");
        assert!(flt_files(d).is_empty(), "a _flt.db exists: {:?}", flt_files(d));
        drop(t);
        close(d, db);
    }
    let preds = model.predicates(1, 160);
    let s1 = pair_bytes(d, "g");
    let s1_slots = marker::scan(&s1.1).unwrap().proven_clean().unwrap().identity.n_slots;

    // The load.
    let probe = model.rows.values().nth(1234).unwrap().a;
    let (loaded, loaded_mask) = {
        let db = open(d, "b14_twin").await;
        let t = table(&db, "g");
        assert!(t.gate().vouched(), "the clean reopen did not load its snapshot");
        assert!(t.filters_ready().unwrap(), "a loaded filter has nothing to backfill");
        assert_eq!(t.gate().state(), GateState::Clean);
        let answers = battery(&db, "g", &model, &preds, "loaded").await;
        // Not vacuous: the loaded filter prunes.
        let n_slots = t.io_pointers().unwrap().get_len().await / ROW_POINTER_RECORD_LEN as u64;
        let mask = t
            .build_candidates(&format!("a = {probe}"), None)
            .await
            .unwrap()
            .expect("the loaded filter declined an equality on a distinct column");
        assert!(mask.count() * 2 < n_slots, "the loaded filter barely pruned: {} of {n_slots}", mask.count());
        assert_eq!(mask.count(), live_mask, "the loaded filter prunes differently from the one sealed");
        drop(t);
        close(d, db);
        (answers, mask.count())
    };

    // The rebuild, from the same files: the same answers, row for row.
    std::fs::remove_file(d.join("g.ixs")).unwrap();
    let db = open(d, "b14_twin").await;
    let t = table(&db, "g");
    assert!(!t.gate().vouched(), "a missing snapshot was vouched for");
    ready(&t).await;
    let rebuilt = battery(&db, "g", &model, &preds, "rebuilt").await;
    assert_eq!(loaded, rebuilt, "the loaded and the rebuilt filter answered differently");
    // How a rebuild prunes the same equality, for the report: a long-lived
    // filter after VACUUM FULL may prune worse than a fresh rebuild (its
    // state is what was sealed), never better, and never wrongly.
    let rebuilt_mask = t
        .build_candidates(&format!("a = {probe}"), None)
        .await
        .unwrap()
        .expect("the rebuilt filter declined")
        .count();
    eprintln!("candidates for a = {probe}: live and loaded {loaded_mask}, rebuilt {rebuilt_mask}");

    // A stale pair under newer files.
    model.delete_range(&db, "g", 0, 2000).await;
    exec(&db, "VACUUM FULL g").await;
    let t = table(&db, "g");
    ready(&t).await;
    model.insert(&db, "g", 6500..9500).await;
    model.update_some(&db, "g", &mut rng, 30, 3).await;
    t.quiesce_for_shutdown().await;
    let outcome = checkpoint(&db, "g").await;
    assert!(matches!(outcome, SealOutcome::Published), "{outcome:?}");
    let n_slots = t.io_pointers().unwrap().get_len().await / ROW_POINTER_RECORD_LEN as u64;
    // So the stale filter's own slot-count check cannot be what refuses it.
    assert!(s1_slots <= n_slots, "the stale snapshot claims more slots than exist: {s1_slots} > {n_slots}");
    drop(t);
    close(d, db);
    std::fs::write(d.join("g.ixs"), &s1.0).unwrap();
    std::fs::write(d.join("g.ixm"), &s1.1).unwrap();

    let db = open(d, "b14_twin").await;
    let t = table(&db, "g");
    ready(&t).await;
    let preds = model.predicates(2, 160);
    battery(&db, "g", &model, &preds, "after a stale pair was put back").await;
    assert!(!t.gate().vouched(), "the stale pair was loaded");
    drop(t);
    close(d, db);
}

/// A sealed table, closed: what the proof tests tamper with.
async fn sealed_table(d: &Path, name: &str) {
    let db = open(d, name).await;
    create_twin(&db, "p").await;
    let mut model = Model::default();
    model.insert(&db, "p", 0..600).await;
    let t = table(&db, "p");
    ready(&t).await;
    t.quiesce_for_shutdown().await;
    let outcome = checkpoint(&db, "p").await;
    assert!(matches!(outcome, SealOutcome::Published), "{outcome:?}");
    drop(t);
    close(d, db);
}

/// Each identity field that differs from the files or the binary is named
/// as the reason, and the proof fails: incarnation, slot epoch, slot count,
/// both file lengths, every version, the shape hash and the filter
/// configuration hash.
///
/// Mutations: `prove` skips the identity comparison; `first_mismatch` drops
/// a field (each fails its row).
#[tokio::test(flavor = "multi_thread")]
async fn every_identity_field_that_differs_forces_a_rebuild() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    sealed_table(d, "b14_fields").await;
    let db = open(d, "b14_fields").await;
    let t = table(&db, "p");
    assert!(t.gate().vouched());
    let base = expected_identity(&t).await;
    let flt = FltAtOpen::Absent;
    assert!(prove(t.gate(), &flt, &base, true).is_ok(), "the unchanged identity is refused");
    type Tweak = fn(&mut Identity);
    let tweaks: [(IdentityField, Tweak); 14] = [
        (IdentityField::Incarnation, |i| i.incarnation ^= 1),
        (IdentityField::SlotEpoch, |i| i.slot_epoch += 1),
        (IdentityField::NSlots, |i| i.n_slots += 1),
        (IdentityField::PointersLen, |i| i.pointers_len += 1),
        (IdentityField::RowsLen, |i| i.rows_len += 1),
        (IdentityField::SnapshotFormatVer, |i| i.versions.snapshot_format_ver += 1),
        (IdentityField::EncodingVersion, |i| i.versions.encoding_version += 1),
        (IdentityField::TextRenderVersion, |i| i.versions.text_render_version += 1),
        (IdentityField::ItemDeriveVersion, |i| i.versions.item_derive_version += 1),
        (IdentityField::GinHasherId, |i| i.versions.gin_hasher_id += 1),
        (IdentityField::FilterDeriveVersion, |i| i.versions.filter_derive_version += 1),
        (IdentityField::FilterFormatVer, |i| i.versions.filter_format_ver += 1),
        (IdentityField::ShapeHash, |i| i.shape_hash ^= 1),
        (IdentityField::FilterConfigHash, |i| i.filter_config_hash ^= 1),
    ];
    for (field, tweak) in tweaks {
        let mut e = base;
        tweak(&mut e);
        let got = prove(t.gate(), &flt, &e, true).map(|_| ());
        let want = if field == IdentityField::Incarnation {
            // The log's HEADER names the incarnation before the CLEAN does.
            Err(Reject::ForeignLog)
        } else {
            Err(Reject::Identity(field))
        };
        assert_eq!(got, want, "{field:?}");
    }
    drop(t);
    close(d, db);
}

/// The files themselves: every way `.ixm` or `.ixs` can fail to match is
/// refused, each for its own reason, and a real reopen of each rebuilds.
///
/// Mutations: `prove` trusts a `CLEAN` with bytes after it (the log's state
/// instead of `proven_clean`); skips the length check; skips the footer
/// comparison; ignores `_flt.db` evidence. Each fails its case.
#[tokio::test(flavor = "multi_thread")]
async fn every_mismatch_in_the_files_forces_a_rebuild() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    sealed_table(d, "b14_files").await;
    let (ixs, ixm) = pair_bytes(d, "p");
    let clean: CleanRecord = *marker::scan(&ixm).unwrap().proven_clean().unwrap();
    let snap = snapshot::decode(&ixs).unwrap();
    let header_len = snapshot::header_len(snap.sections.len()) as usize;
    let filter_at = snap.sections[0].0.offset as usize;

    // A valid snapshot of the same length and identity whose footer is not
    // the CLEAN's: a stale snapshot under a newer CLEAN.
    let other = {
        let mut section = snap.sections[0].1.to_vec();
        let last = section.len() - 1;
        section[last] ^= 0x55;
        snapshot::encode(snap.header.identity, &[(SectionKind::Filter, 0, &section)])
    };
    assert_eq!(other.len(), ixs.len());
    // A log whose HEADER is another incarnation's, ending in a CLEAN.
    let foreign = {
        let mut log = marker::new_log(clean.identity.incarnation ^ 0xff);
        log.extend_from_slice(&marker::encode_record(1, &MarkerRecord::Clean(clean)));
        log
    };
    // The log ending DIRTY after the CLEAN.
    let dirty = {
        let scan = marker::scan(&ixm).unwrap();
        let mut log = ixm.clone();
        for op in scan.plan_append(marker::Append::Dirty).ops {
            op.apply(&mut log);
        }
        log
    };
    let flip = |bytes: &[u8], at: usize| {
        let mut b = bytes.to_vec();
        b[at] ^= 0x01;
        b
    };
    let mut appended = ixs.clone();
    appended.push(0);
    let mut trailing = ixm.clone();
    trailing.extend_from_slice(&[0u8; 3]);

    struct Case {
        what: &'static str,
        ixs: Option<Vec<u8>>,
        ixm: Vec<u8>,
        flt: bool,
        want: &'static str,
    }
    let cases = vec![
        Case { what: "bytes after the CLEAN", ixs: Some(ixs.clone()), ixm: trailing, flt: false, want: "trailing_bytes" },
        Case { what: "a DIRTY after the CLEAN", ixs: Some(ixs.clone()), ixm: dirty, flt: false, want: "not_clean" },
        Case { what: "another incarnation's log", ixs: Some(ixs.clone()), ixm: foreign, flt: false, want: "not_clean" },
        Case { what: "no snapshot", ixs: None, ixm: ixm.clone(), flt: false, want: "no_snapshot" },
        Case { what: "a longer snapshot", ixs: Some(appended), ixm: ixm.clone(), flt: false, want: "snapshot_length" },
        Case { what: "a header byte flipped", ixs: Some(flip(&ixs, 20)), ixm: ixm.clone(), flt: false, want: "snapshot_corrupt" },
        Case { what: "a section byte flipped", ixs: Some(flip(&ixs, filter_at + 9)), ixm: ixm.clone(), flt: false, want: "snapshot_corrupt" },
        Case { what: "the footer flipped", ixs: Some(flip(&ixs, ixs.len() - 1)), ixm: ixm.clone(), flt: false, want: "snapshot_corrupt" },
        Case { what: "another snapshot of the same length", ixs: Some(other), ixm: ixm.clone(), flt: false, want: "footer" },
        Case { what: "a _flt.db beside the log", ixs: Some(ixs.clone()), ixm: ixm.clone(), flt: true, want: "flt_evidence" },
    ];
    assert!(header_len <= filter_at);
    for case in cases {
        match &case.ixs {
            Some(b) => std::fs::write(d.join("p.ixs"), b).unwrap(),
            None => {
                let _ = std::fs::remove_file(d.join("p.ixs"));
            }
        }
        std::fs::write(d.join("p.ixm"), &case.ixm).unwrap();
        if case.flt {
            let mut flt = b"RDBFLT01".to_vec();
            flt.extend_from_slice(&[0u8; 56]);
            std::fs::write(d.join("p_flt.db"), flt).unwrap();
        }
        let before = rejected_total(case.want);
        let db = open(d, "b14_files").await;
        let t = table(&db, "p");
        assert!(!t.gate().vouched(), "{}: loaded", case.what);
        assert!(rejected_total(case.want) > before, "{}: not refused as {}", case.what, case.want);
        ready(&t).await;
        assert_eq!(ids(&db, "p", "id >= 0").await.len(), 600, "{}", case.what);
        drop(t);
        close(d, db);
    }
    // And the untouched pair, put back, loads.
    std::fs::write(d.join("p.ixs"), &ixs).unwrap();
    std::fs::write(d.join("p.ixm"), &ixm).unwrap();
    let db = open(d, "b14_files").await;
    let t = table(&db, "p");
    assert!(t.gate().vouched(), "the untouched pair did not load");
    drop(t);
    close(d, db);
}

/// An `rdb.ixe` that cannot be read proves no slot epoch: the reopen
/// refuses its snapshot. Mutation: `prove` skips the `epoch_proven` check.
#[tokio::test(flavor = "multi_thread")]
async fn an_unreadable_epoch_log_refuses_the_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    sealed_table(d, "b14_epoch").await;
    std::fs::write(d.join(crate::core::index_store::epoch_log::EPOCH_LOG_FILE), b"not an epoch log at all, sixteen+").unwrap();
    let db = open(d, "b14_epoch").await;
    let t = table(&db, "p");
    assert!(!t.gate().epoch_proven());
    assert!(!t.gate().vouched(), "a snapshot loaded with no proven slot epoch");
    drop(t);
    close(d, db);
}

/// An open that loads writes nothing: the marker log is byte for byte what
/// it was, no `DIRTY` is made durable, no `_flt.db` appears; the first
/// write then makes `DIRTY` durable once, before anything else (T1). And
/// the first vacuum after the reopen has nothing to change and takes no
/// ticket (RV2; B11's deviation 3 gone), with `cleared_below` restored.
///
/// Mutations: the load is skipped (the open rebuilds: its backfill indexes
/// the dead versions again and the first vacuum takes a ticket);
/// `cleared_below` starts at 0 instead of the snapshot's.
#[tokio::test(flavor = "multi_thread")]
async fn an_open_that_loads_writes_nothing_and_its_first_vacuum_takes_no_ticket() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    let mut model = Model::default();
    let cleared;
    {
        let db = open(d, "b14_quiet").await;
        create_twin(&db, "q").await;
        model.insert(&db, "q", 0..5000).await;
        model.delete_range(&db, "q", 100, 1500).await;
        let mut rng = fastrand::Rng::with_seed(7);
        model.update_some(&db, "q", &mut rng, 30, 1).await;
        let t = table(&db, "q");
        ready(&t).await;
        for _ in 0..3 {
            t.vacuum_now().await.unwrap();
        }
        cleared = t.cleared_below_for_tests();
        assert!(cleared > 0, "vacuum cleared nothing");
        t.quiesce_for_shutdown().await;
        let outcome = checkpoint(&db, "q").await;
        assert!(matches!(outcome, SealOutcome::Published), "{outcome:?}");
        drop(t);
        close(d, db);
    }
    let ixm_before = std::fs::read(d.join("q.ixm")).unwrap();
    let watch = crate::core::sync_audit::watch(d);
    let db = open(d, "b14_quiet").await;
    let t = table(&db, "q");
    let opened = watch.take();
    let cleared_at_reopen = t.cleared_below_for_tests();
    ready(&t).await;

    // The first vacuum, checked first: with the filter loaded it has nothing
    // to clear. (A rebuilt filter indexes the dead versions again, and its
    // first sweep must clear them under a ticket.)
    let census = Census::start();
    let report = t.vacuum_now().await.unwrap();
    let events = census.finish();
    assert_eq!(holds_on(&events, t.gate().census_id()), 0, "the first vacuum took a ticket: {report:?}");
    assert_eq!(t.gate().state(), GateState::Clean, "the first vacuum dirtied the table");
    assert_eq!(std::fs::read(d.join("q.ixm")).unwrap(), ixm_before);

    assert!(t.gate().vouched(), "the reopen did not load");
    assert_eq!(cleared_at_reopen, cleared, "cleared_below did not survive");
    let marker_syncs = opened
        .into_iter()
        .filter(|(tag, p)| tag.starts_with("index_store") || p.to_string_lossy().contains("q.ix"))
        .count();
    assert_eq!(marker_syncs, 0, "the open wrote to the index store");
    assert_eq!(std::fs::read(d.join("q.ixm")).unwrap(), ixm_before, "the open changed the marker log");
    assert_eq!(t.gate().state(), GateState::Clean);
    assert!(flt_files(d).is_empty());

    // The first write: DIRTY durable once, before anything is written.
    let census = Census::start();
    exec(&db, "INSERT INTO q (id, a, b, s, f) VALUES (90000, 1, 1, 'x', 1.0)").await;
    let events = census.finish();
    let summary = crate::core::index_store::census::check(
        &events,
        &crate::core::index_store::census::Scope {
            gate: t.gate().census_id(),
            began_clean: true,
            structures: &t.census_structures(),
            files: &t.census_files(),
        },
    )
    .unwrap_or_else(|e| panic!("census: {e}"));
    assert_eq!(summary.dirty_durable, 1);
    assert_eq!(t.gate().state(), GateState::Dirty);
    drop(t);
    close(d, db);
}

/// Power loss around a seal and around the first write after a load: in
/// every crash image, an open that loads the snapshot answers exactly what
/// a rebuild of the same image answers, so no image loads a snapshot that
/// does not match its files.
///
/// The first write after the load is an UPDATE, then a compaction (rows
/// move below the snapshot's frontier) and inserts past its slot count, so
/// a loaded stale snapshot would prune moved rows.
///
/// Mutation (the doc's V1 "load when the log ends `DIRTY`", with the
/// identity row skipped too, since either alone refuses these images):
/// `prove` takes the last `CLEAN` anywhere in the log whatever follows,
/// ignores the word's state and skips the identity comparison. The images
/// after the compaction then load the old snapshot and answer differently
/// from their rebuilt twins.
#[tokio::test(flavor = "multi_thread")]
async fn no_crash_image_loads_a_snapshot_that_does_not_match_its_files() {
    let base = tempfile::tempdir().unwrap();
    let dir = base.path().join("db");
    std::fs::create_dir(&dir).unwrap();
    let mut model = Model::default();
    let db = open(&dir, "b14_crash").await;
    create_twin(&db, "c").await;
    model.insert(&db, "c", 0..4500).await;
    model.delete_range(&db, "c", 1000, 1200).await;
    let mut rng = fastrand::Rng::with_seed(11);
    model.update_some(&db, "c", &mut rng, 30, 1).await;
    let t = table(&db, "c");
    ready(&t).await;
    t.vacuum_now().await.unwrap();
    t.quiesce_for_shutdown().await;
    db.sync_durable().unwrap();
    let rec = Recorder::start(&dir);
    let sink = StdArc::new(Mutex::new(Vec::new()));
    let names = [
        "snapshot_before_fsync",
        "snapshot_after_fsync",
        "snapshot_after_rename",
        "snapshot_after_dir_fsync",
        "marker_compaction_after_rename",
        "clean_append_before_fsync",
        "clean_after_fsync",
    ];
    let cuts: Vec<failpoint::Guard> = names
        .iter()
        .map(|n| failpoint::arm(n, &dir, 1, rec.cut_action(n.to_string(), sink.clone())))
        .collect();
    let outcome = checkpoint(&db, "c").await;
    assert!(matches!(outcome, SealOutcome::Published), "{outcome:?}");
    drop(cuts);
    let mut points: Vec<CrashPoint> = sink.lock().drain(..).filter_map(Result::ok).collect();
    points.push(rec.cut_quiescent("after the seal").unwrap());
    drop(t);
    close(&dir, db);
    drop(rec);

    // The first write after a load, cut around its DIRTY and its commit.
    let after_seal = points.last().unwrap();
    after_seal.write_image(0, &dir.with_file_name("first_write"));
    let fw = dir.with_file_name("first_write");
    let db = open(&fw, "b14_crash").await;
    let t = table(&db, "c");
    assert!(t.gate().vouched(), "the quiescent image did not load");
    db.sync_durable().unwrap();
    let rec = Recorder::start(&fw);
    let sink2 = StdArc::new(Mutex::new(Vec::new()));
    let cuts: Vec<failpoint::Guard> = ["dirty_append_before_fsync", "dirty_after_fsync"]
        .iter()
        .map(|n| failpoint::arm(n, &fw, 1, rec.cut_action(n.to_string(), sink2.clone())))
        .chain(std::iter::once(failpoint::arm("sync:wal.log", &fw, 1, rec.cut_action("sync:wal.log".into(), sink2.clone()))))
        .collect();
    let probe = model.rows.values().nth(77).unwrap().clone();
    exec(&db, &format!("UPDATE c SET a = 777777777 WHERE a = {}", probe.a)).await;
    // Rows moved below the snapshot's frontier (a compaction), and the table
    // grown past the snapshot's slot count: only the log and the identity
    // can tell this image's files from the snapshot's.
    model.delete_range(&db, "c", 0, 1500).await;
    exec(&db, "VACUUM FULL c").await;
    model.insert(&db, "c", 4500..7000).await;
    drop(cuts);
    let mut fw_points: Vec<CrashPoint> = sink2.lock().drain(..).filter_map(Result::ok).collect();
    fw_points.push(rec.cut_quiescent("after the first write").unwrap());
    drop(t);
    close(&fw, db);
    drop(rec);

    let preds: Vec<String> = {
        let mut p: Vec<String> = model.predicates(5, 40).iter().map(|p| p.sql()).collect();
        p.push(format!("a = {}", probe.a));
        p.push("a = 777777777".into());
        p
    };
    let (mut loads, mut rebuilds) = (0, 0);
    let img = base.path().join("image");
    let twin = base.path().join("twin");
    for p in points.iter().chain(fw_points.iter()) {
        for seed in 0..4 {
            p.write_image(seed, &img);
            p.write_image(seed, &twin);
            let _ = std::fs::remove_file(twin.join("c.ixs"));
            let db = open(&img, "b14_crash").await;
            let t = table(&db, "c");
            let loaded = t.gate().vouched();
            ready(&t).await;
            let mut got = Vec::new();
            for q in &preds {
                got.push(ids(&db, "c", q).await);
            }
            drop(t);
            close(&img, db);
            if loaded {
                loads += 1;
            } else {
                rebuilds += 1;
            }
            // Every image against its rebuilt twin, vouched or not: a word
            // that is not vouched may still hold a loaded filter.
            let db = open(&twin, "b14_crash").await;
            let t = table(&db, "c");
            assert!(!t.gate().vouched());
            ready(&t).await;
            for (q, g) in preds.iter().zip(&got) {
                let want = ids(&db, "c", q).await;
                assert_eq!(g, &want, "{} seed {seed}: the image answered `{q}` differently from its rebuild", p.label);
            }
            drop(t);
            close(&twin, db);
            let _ = std::fs::remove_dir_all(&img);
            let _ = std::fs::remove_dir_all(&twin);
        }
    }
    assert!(loads > 0 && rebuilds > 0, "the images missed one side: {loads} loads, {rebuilds} rebuilds");
    let _ = std::fs::remove_dir_all(&fw);
}

/// `_flt.db` with no marker log beside it: deleted at open (with its
/// staging sibling), the open otherwise as before.
#[tokio::test(flavor = "multi_thread")]
async fn a_flt_db_with_no_marker_log_is_deleted_at_open() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    {
        let db = open(d, "b14_flt_plain").await;
        create_twin(&db, "f").await;
        let mut model = Model::default();
        model.insert(&db, "f", 0..50).await;
        close(d, db);
    }
    assert!(!d.join("f.ixm").exists());
    let mut flt = b"RDBFLT01".to_vec();
    flt.extend_from_slice(&[7u8; 120]);
    std::fs::write(d.join("f_flt.db"), &flt).unwrap();
    std::fs::write(d.join("f_flt.db.tmp"), b"staged").unwrap();
    let db = open(d, "b14_flt_plain").await;
    let t = table(&db, "f");
    assert!(flt_files(d).is_empty(), "left: {:?}", flt_files(d));
    assert_eq!(t.gate().state(), GateState::Dirty);
    assert_eq!(ids(&db, "f", "id >= 0").await.len(), 50);
    drop(t);
    close(d, db);
}

/// `_flt.db` beside a log ending `CLEAN` is evidence (RV5): `.ixs` is
/// unlinked and the directory synced before `_flt.db` goes; the proof
/// fails, the filter rebuilds, the word stays `Clean` (the log's state,
/// N21) and the first write makes `DIRTY` durable.
///
/// Mutations: the evidence is ignored (the snapshot loads); `_flt.db` is
/// deleted before `.ixs` is unlinked (the order assertion fails).
#[tokio::test(flavor = "multi_thread")]
async fn a_flt_db_beside_a_clean_log_is_evidence() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    sealed_table(d, "b14_flt_evidence").await;
    let mut flt = b"RDBFLT01".to_vec();
    flt.extend_from_slice(&[1u8; 56]);
    std::fs::write(d.join("p_flt.db"), &flt).unwrap();
    let watch = crate::core::sync_audit::watch(d);
    let db = open(d, "b14_flt_evidence").await;
    let events = watch.take();
    let t = table(&db, "p");
    assert!(!t.gate().vouched(), "a snapshot beside evidence was loaded");
    assert!(!d.join("p.ixs").exists(), "the snapshot beside the evidence is still there");
    assert!(flt_files(d).is_empty(), "the evidence was not deleted once the snapshot was gone");
    assert_eq!(t.gate().state(), GateState::Clean, "the word follows the log (N21)");
    let at = |pred: &dyn Fn(&(&'static str, std::path::PathBuf)) -> bool| events.iter().position(|e| pred(e));
    let ixs_gone = at(&|(tag, p)| *tag == "unlink" && p.ends_with("p.ixs")).expect("no unlink of p.ixs");
    let flt_gone = at(&|(tag, p)| *tag == "unlink" && p.ends_with("p_flt.db")).expect("no unlink of p_flt.db");
    let synced_between = events[ixs_gone..flt_gone.max(ixs_gone)]
        .iter()
        .any(|(tag, p)| *tag != "unlink" && (p == d || p.ends_with("p.ixs")));
    assert!(ixs_gone < flt_gone && synced_between, "the evidence went before the snapshot was durably gone: {events:?}");
    ready(&t).await;
    assert_eq!(ids(&db, "p", "id >= 0").await.len(), 600);
    exec(&db, "INSERT INTO p (id, a, b, s, f) VALUES (9999, 1, 1, 'x', 1.0)").await;
    assert_eq!(t.gate().state(), GateState::Dirty);
    drop(t);
    close(d, db);
}

/// Evidence that cannot be acted on (the `.ixs` unlink or its directory
/// sync fails): `_flt.db` is kept and the word starts `Unknown`, so the
/// first write rewrites the log before anything is issued, then deletes it.
///
/// Mutation: the failure branch does not start the word `Unknown`.
#[tokio::test(flavor = "multi_thread")]
async fn evidence_that_cannot_be_acted_on_starts_unknown() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    sealed_table(d, "b14_flt_unlink").await;
    let mut flt = b"RDBFLT01".to_vec();
    flt.extend_from_slice(&[3u8; 56]);
    std::fs::write(d.join("p_flt.db"), &flt).unwrap();
    let fail = failpoint::arm(
        "flt_evidence_before_ixs_unlink",
        d,
        1,
        StdArc::new(|_: &failpoint::Hit<'_>| Outcome::Fail(std::io::Error::other("injected: the directory sync failed"))),
    );
    let db = open(d, "b14_flt_unlink").await;
    drop(fail);
    let t = table(&db, "p");
    assert_eq!(t.gate().state(), GateState::Unknown);
    assert!(!t.gate().vouched());
    assert!(d.join("p_flt.db").exists(), "the evidence went though the snapshot's unlink failed");
    ready(&t).await;
    exec(&db, "INSERT INTO p (id, a, b, s, f) VALUES (9999, 1, 1, 'x', 1.0)").await;
    assert_eq!(t.gate().state(), GateState::Dirty);
    assert!(!d.join("p_flt.db").exists());
    assert_eq!(ids(&db, "p", "id >= 0").await.len(), 601);
    drop(t);
    close(d, db);
}

/// A `_flt.db` delete that fails at open starts the word `Unknown` and keeps
/// the file; the first write's slow path rewrites the log durably first,
/// then deletes it, and the word leaves `Unknown` only then (§2 R9, Q12). A
/// slow path whose delete fails fails the write and stays `Unknown`; the
/// next write retries.
///
/// Mutations: the `Unknown` slow path skips the `_flt.db` delete (the file
/// survives the write); the delete runs before the log rewrite (the order
/// assertion fails).
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_flt_delete_starts_unknown_and_the_first_write_retries_it() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    sealed_table(d, "b14_flt_unknown").await;
    let mut flt = b"RDBFLT01".to_vec();
    flt.extend_from_slice(&[2u8; 56]);
    std::fs::write(d.join("p_flt.db"), &flt).unwrap();
    let fail = || {
        StdArc::new(|_: &failpoint::Hit<'_>| Outcome::Fail(std::io::Error::other("injected: the delete failed")))
    };
    let at_open = failpoint::arm("unknown_flt_delete_order", d, 1, fail());
    let db = open(d, "b14_flt_unknown").await;
    drop(at_open);
    let t = table(&db, "p");
    assert_eq!(t.gate().state(), GateState::Unknown);
    assert!(d.join("p_flt.db").exists(), "the file went though its delete failed");
    ready(&t).await;

    // A slow path whose delete fails: the write fails, the word stays Unknown.
    let again = failpoint::arm("unknown_flt_delete_order", d, 1, fail());
    let err = try_exec(&db, "INSERT INTO p (id, a, b, s, f) VALUES (9998, 1, 1, 'x', 1.0)").await;
    drop(again);
    assert!(err.is_err(), "a write went ahead beside a surviving _flt.db");
    assert_eq!(t.gate().state(), GateState::Unknown);
    assert!(d.join("p_flt.db").exists());

    let watch = crate::core::sync_audit::watch(d);
    exec(&db, "INSERT INTO p (id, a, b, s, f) VALUES (9999, 1, 1, 'x', 1.0)").await;
    let events = watch.take();
    assert_eq!(t.gate().state(), GateState::Dirty);
    assert!(!d.join("p_flt.db").exists(), "the slow path left the evidence");
    let rewritten = events
        .iter()
        .position(|(tag, _)| *tag == "index_store.marker_tmp")
        .expect("the log was not rewritten");
    let deleted = events
        .iter()
        .position(|(tag, p)| *tag == "unlink" && p.ends_with("p_flt.db"))
        .expect("no unlink of p_flt.db");
    assert!(rewritten < deleted, "the evidence went before the log was durably DIRTY: {events:?}");
    assert_eq!(ids(&db, "p", "id >= 9999").await, vec![9999]);
    drop(t);
    close(d, db);
}

/// The legacy-pair rule on a file set (P19): a `<t>_flt.db` that may be
/// another table's rows (a table `<t>_flt` has its schema or pointers file
/// beside it, or the bytes are not a filter image) is kept, and refuses the
/// snapshot; a filter image with no such neighbour is deleted.
///
/// Mutation: `flt_may_be_a_table` always answers no (the other table's rows
/// file is deleted).
#[test]
fn a_flt_db_that_may_be_another_tables_rows_is_kept() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    let rows = b"rows of table t_flt, not a filter image".to_vec();
    let mut image = b"RDBFLT01".to_vec();
    image.extend_from_slice(&[0u8; 24]);
    for (case, (neighbour, bytes, kept)) in [
        (Some("t_flt_schema.db"), image.clone(), true),
        (Some("t_flt_pointers.db"), image.clone(), true),
        (None, rows.clone(), true),
        (None, image.clone(), false),
    ]
    .into_iter()
    .enumerate()
    {
        let sub = d.join(format!("case{case}"));
        std::fs::create_dir(&sub).unwrap();
        if let Some(n) = neighbour {
            std::fs::write(sub.join(n), b"x").unwrap();
        }
        std::fs::write(sub.join("t_flt.db"), &bytes).unwrap();
        let gate = FileGate::open(Some(sub.clone()), "t", 42 + case as u128);
        let found = gate.retire_flt();
        assert_eq!(sub.join("t_flt.db").exists(), kept, "case {case}: {found:?}");
        if kept {
            assert_eq!(found, FltAtOpen::LegacyPair, "case {case}");
            assert!(found.refuses_snapshot());
            assert_eq!(std::fs::read(sub.join("t_flt.db")).unwrap(), bytes, "case {case}: the bytes changed");
        } else {
            assert_eq!(found, FltAtOpen::Deleted { evidence: false }, "case {case}");
        }
    }
}

/// DROP of a plain and of a partitioned table removes every index-store
/// file it sealed; the startup reaper removes an unmaterialized partition's
/// and a dead table's (their `.tmp`s too), and keeps the live ones'.
///
/// Mutation: `parse_name` does not read `.ixs`/`.ixm` (the partitioned
/// DROP and the reaper leave them).
#[tokio::test(flavor = "multi_thread")]
async fn drop_and_the_reaper_remove_the_pair() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    let db = open(d, "b14_drop").await;
    exec(&db, "CREATE TABLE plain (id BIGINT PRIMARY KEY, v BIGINT)").await;
    exec(&db, "INSERT INTO plain (id, v) VALUES (1, 1), (2, 2)").await;
    exec(&db, "CREATE TABLE pt (tenant_id BIGINT NOT NULL, id BIGINT, v BIGINT) PARTITION BY HASH (tenant_id) PARTITIONS 8").await;
    let values: Vec<String> = (0..40).map(|i| format!("({i}, {i}, {i})")).collect();
    exec(&db, &format!("INSERT INTO pt (tenant_id, id, v) VALUES {}", values.join(", "))).await;
    exec(&db, "CREATE TABLE keep (tenant_id BIGINT NOT NULL, id BIGINT) PARTITION BY HASH (tenant_id) PARTITIONS 8").await;
    exec(&db, "INSERT INTO keep (tenant_id, id) VALUES (1, 1), (2, 2)").await;
    for name in ["plain", "pt", "keep"] {
        let report = db.index_checkpoint(name, None).await.expect("rdb_index_checkpoint");
        assert!(report.sealed.iter().all(|s| s.outcome.is_clean()), "{name}: {:?}", report.sealed.iter().map(|s| s.outcome.describe()).collect::<Vec<_>>());
    }
    let ix = |pred: &dyn Fn(&str) -> bool| -> Vec<String> {
        let mut v: Vec<String> = std::fs::read_dir(d)
            .unwrap()
            .filter_map(|e| e.ok()?.file_name().into_string().ok())
            .filter(|n| (n.contains(".ixs") || n.contains(".ixm")) && pred(n))
            .collect();
        v.sort();
        v
    };
    let pt_uid = db.partitioned_table("pt").unwrap().uid();
    let keep_uid = db.partitioned_table("keep").unwrap().uid();
    let pt_prefix = format!("__pt_{pt_uid:016x}_");
    let keep_prefix = format!("__pt_{keep_uid:016x}_");
    assert!(ix(&|n| n.starts_with("plain.")).len() == 2);
    assert!(ix(&|n| n.starts_with(&pt_prefix)).len() >= 2);
    let kept_before = ix(&|n| n.starts_with(&keep_prefix));
    assert!(kept_before.len() >= 2);
    exec(&db, "DROP TABLE plain").await;
    exec(&db, "DROP TABLE pt").await;
    assert_eq!(ix(&|n| n.starts_with("plain.")), Vec::<String>::new(), "DROP left the plain table's pair");
    assert_eq!(ix(&|n| n.starts_with(&pt_prefix)), Vec::<String>::new(), "DROP left a partition's pair");
    close(d, db);

    // Orphans: an unmaterialized partition of a live table, a dead uid.
    let keep_pids: Vec<u32> = kept_before
        .iter()
        .filter_map(|n| match crate::core::partition::files::parse_name(n)? {
            crate::core::partition::files::PartFile::Data { pid, .. } => Some(pid),
            _ => None,
        })
        .collect();
    let free = (0..8u32).find(|p| !keep_pids.contains(p)).unwrap();
    let mut orphans = Vec::new();
    for stem in [crate::core::partition::files::data_stem(keep_uid, free), crate::core::partition::files::data_stem(0xdead_beef, 3)] {
        for s in [".ixs", ".ixs.tmp", ".ixm", ".ixm.tmp"] {
            let n = format!("{stem}{s}");
            std::fs::write(d.join(&n), b"orphan").unwrap();
            orphans.push(n);
        }
    }
    let db = open(d, "b14_drop").await;
    for o in &orphans {
        assert!(!d.join(o).exists(), "the reaper kept {o}");
    }
    assert_eq!(ix(&|n| n.starts_with(&keep_prefix)), kept_before, "the reaper touched a live partition's pair");
    close(d, db);
}

/// Open time of a ~1M-row table's filter, loaded from its snapshot against
/// rebuilt (B15's indicative RV7). Not a test: run with
/// `cargo test --release -p rasterizeddb_core --lib -- --ignored
/// b14_measure_open_time --nocapture` (`RDB_B14_ROWS`, `RDB_B14_DIR`).
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn b14_measure_open_time() {
    let rows: i64 = std::env::var("RDB_B14_ROWS").ok().and_then(|v| v.parse().ok()).unwrap_or(1_000_000);
    let base = match std::env::var("RDB_B14_DIR") {
        Ok(d) => tempfile::tempdir_in(d).unwrap(),
        Err(_) => tempfile::tempdir().unwrap(),
    };
    let d = base.path();
    let started = std::time::Instant::now();
    {
        let db = open(d, "b14_measure").await;
        create_twin(&db, "m").await;
        let mut id = 0i64;
        while id < rows {
            let end = (id + 5000).min(rows);
            let values: Vec<String> = (id..end)
                .map(|i| {
                    let r = Model::row_for(i);
                    format!("({i}, {}, {}, '{}', {:.1})", r.a, r.b, r.s, r.f)
                })
                .collect();
            exec(&db, &format!("INSERT INTO m (id, a, b, s, f) VALUES {}", values.join(", "))).await;
            id = end;
        }
        let t = table(&db, "m");
        ready(&t).await;
        t.quiesce_for_shutdown().await;
        let outcome = checkpoint(&db, "m").await;
        assert!(matches!(outcome, SealOutcome::Published), "{outcome:?}");
        drop(t);
        close(d, db);
    }
    let ixs = std::fs::metadata(d.join("m.ixs")).unwrap().len();
    let rows_len = std::fs::metadata(d.join("m.db")).unwrap().len();
    let ptr_len = std::fs::metadata(d.join("m_pointers.db")).unwrap().len();
    eprintln!("B14 measure: {rows} rows loaded in {:.1}s; rows {rows_len} B, pointers {ptr_len} B, snapshot {ixs} B", started.elapsed().as_secs_f64());
    let pair = pair_bytes(d, "m");
    let (mut loads, mut rebuilds) = (Vec::new(), Vec::new());
    for round in 0..3 {
        for rebuild in [false, true] {
            std::fs::write(d.join("m.ixs"), &pair.0).unwrap();
            std::fs::write(d.join("m.ixm"), &pair.1).unwrap();
            if rebuild {
                std::fs::remove_file(d.join("m.ixs")).unwrap();
            }
            let t0 = std::time::Instant::now();
            let db = open(d, "b14_measure").await;
            let t = table(&db, "m");
            let opened = t0.elapsed();
            ready(&t).await;
            let total = t0.elapsed();
            assert_eq!(t.gate().vouched(), !rebuild, "round {round}");
            eprintln!(
                "B14 measure round {round} {}: database open {:.3}s, filter ready {:.3}s",
                if rebuild { "rebuild" } else { "snapshot" },
                opened.as_secs_f64(),
                total.as_secs_f64()
            );
            if rebuild { rebuilds.push(total.as_secs_f64()) } else { loads.push(total.as_secs_f64()) }
            drop(t);
            close(d, db);
        }
    }
    loads.sort_by(f64::total_cmp);
    rebuilds.sort_by(f64::total_cmp);
    eprintln!("B14 measure medians: snapshot {:.3}s (min {:.3}, max {:.3}), rebuild {:.3}s (min {:.3}, max {:.3})",
        loads[1], loads[0], loads[2], rebuilds[1], rebuilds[0], rebuilds[2]);
}
