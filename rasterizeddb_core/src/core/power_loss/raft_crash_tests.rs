//! The shard's Raft store through a power cut (sharding-plan.md D3: "the
//! power-loss harness on every log and vote write").
//!
//! A script of persists (entries, a cut suffix, hard states, a snapshot that
//! keeps the log, one that replaces it, a local compaction) runs with a cut
//! armed at every sync of each step. Every image of every cut must reopen,
//! and hold what the step started from, or that with some prefix of the
//! step's own records applied (records are appended in order and a torn tail
//! is cut at a record), with the snapshot file and the hard state each old
//! or new (each is replaced whole by a rename). Anything else is an entry
//! lost that a finished step made durable, or one invented.

use super::*;
use parking_lot::Mutex;
use std::sync::Arc as StdArc;

use crate::core::shard::raft_store::RaftStore;
use rasterizeddb_raft::{Config, Entry, EntryData, HardState, Index, SnapshotMeta, Term};

/// The log as the store's restore sees it: the snapshot point and the
/// entries after it, as `(index, term)`.
#[derive(Clone, Debug, PartialEq, Eq)]
struct LogModel {
    snap: (Index, Term),
    entries: Vec<(Index, Term)>,
}

enum Rec {
    Reset(Index, Term),
    Truncate(Index),
    Entry(Index, Term),
}

impl LogModel {
    fn apply(&mut self, r: &Rec) {
        match *r {
            Rec::Reset(i, t) => {
                self.snap = (i, t);
                self.entries.clear();
            }
            Rec::Truncate(from) => self.entries.retain(|(i, _)| *i < from),
            Rec::Entry(i, t) => self.entries.push((i, t)),
        }
    }

    /// As `open` reads it beside snapshot file `snap`.
    fn with_snapshot_file(&self, snap: Option<(Index, Term)>) -> LogModel {
        let mut m = self.clone();
        if let Some((i, t)) = snap
            && i > m.snap.0
        {
            m.entries.retain(|(e, _)| *e > i);
            if m.entries.first().is_some_and(|(e, _)| *e != i + 1) {
                m.entries.clear();
            }
            m.snap = (i, t);
        }
        m
    }
}

fn entry(term: Term, index: Index) -> Entry {
    Entry { term, index, data: EntryData::Command(vec![index as u8; 16]) }
}

fn meta(index: Index, term: Term) -> SnapshotMeta {
    SnapshotMeta { index, term, config: Config::new([1, 2, 3]) }
}

/// One step of the script.
enum Step {
    Persist { snapshot: Option<(Index, Term, bool)>, truncate: Option<Index>, entries: Vec<Entry>, hard: Option<HardState> },
    Compact { index: Index, term: Term, keep: Vec<Entry> },
}

fn script() -> Vec<Step> {
    let hs = |term, vote, commit| Some(HardState { term, vote: Some(vote), commit });
    vec![
        Step::Persist { snapshot: None, truncate: None, entries: (1..=5).map(|i| entry(1, i)).collect(), hard: hs(1, 1, 2) },
        Step::Persist { snapshot: None, truncate: None, entries: (6..=8).map(|i| entry(1, i)).collect(), hard: hs(1, 1, 6) },
        Step::Persist { snapshot: None, truncate: Some(7), entries: (7..=9).map(|i| entry(2, i)).collect(), hard: hs(2, 2, 7) },
        Step::Persist { snapshot: Some((5, 1, true)), truncate: None, entries: vec![], hard: None },
        Step::Persist { snapshot: None, truncate: None, entries: (10..=12).map(|i| entry(2, i)).collect(), hard: hs(2, 2, 11) },
        Step::Compact { index: 9, term: 2, keep: (10..=12).map(|i| entry(2, i)).collect() },
        Step::Persist { snapshot: Some((20, 3, false)), truncate: None, entries: vec![entry(3, 21)], hard: hs(3, 3, 21) },
        Step::Persist { snapshot: None, truncate: None, entries: (22..=25).map(|i| entry(3, i)).collect(), hard: hs(3, 3, 24) },
    ]
}

/// What `open` restores, as a model.
fn restored(dir: &Path) -> Result<(LogModel, HardState, Option<(Index, Term)>), String> {
    let (_, r) = RaftStore::open(dir)?;
    let log = &r.log;
    let mut entries = Vec::new();
    for i in log.first_index()..=log.last_index() {
        entries.push((i, log.term(i).ok_or(format!("no term at {i}"))?));
    }
    let snap_file = r.snapshot.as_ref().map(|(m, _)| (m.index, m.term));
    Ok((LogModel { snap: (log.snapshot_index(), log.snapshot_term()), entries }, r.hard, snap_file))
}

#[test]
fn a_cut_at_any_raft_store_sync_keeps_every_finished_step() {
    let dir = tempfile::tempdir().unwrap();
    let (mut store, _) = RaftStore::open(dir.path()).unwrap();
    let rec = Recorder::start(dir.path());

    // The durable state each finished step leaves: the log as records
    // replayed (before the snapshot file filters it), the snapshot file,
    // the hard state.
    let mut log = LogModel { snap: (0, 0), entries: vec![] };
    let mut snap_file: Option<(Index, Term)> = None;
    let mut hard = HardState::default();
    let mut checked = 0usize;

    for (n, step) in script().into_iter().enumerate() {
        let sink: StdArc<Mutex<Vec<Result<CrashPoint, String>>>> = StdArc::new(Mutex::new(Vec::new()));
        let guards: Vec<failpoint::Guard> = ["sync:raft.log", "sync:raft.replace", "sync:dir"]
            .iter()
            .flat_map(|tag| (1..=4).map(move |nth| (*tag, nth)))
            .map(|(tag, nth)| failpoint::arm(tag, dir.path(), nth, rec.cut_action(format!("step {n}, {tag} #{nth}"), sink.clone())))
            .collect();

        // What this step writes, in order, and what it leaves.
        let mut records = Vec::new();
        let mut new_snap = snap_file;
        let mut new_hard = hard;
        match &step {
            Step::Persist { snapshot, truncate, entries, hard: h } => {
                let snap = snapshot.map(|(i, t, keep)| ((meta(i, t), vec![9u8; 32]), keep));
                if let Some(((m, _), keep)) = &snap {
                    new_snap = Some((m.index, m.term));
                    if !keep {
                        records.push(Rec::Reset(m.index, m.term));
                    }
                }
                if let Some(from) = truncate {
                    records.push(Rec::Truncate(*from));
                }
                records.extend(entries.iter().map(|e| Rec::Entry(e.index, e.term)));
                if let Some(h) = h {
                    new_hard = *h;
                }
                store
                    .persist_parts(snap.as_ref().map(|(s, _)| s), snap.as_ref().is_some_and(|(_, k)| *k), *truncate, entries, h.as_ref())
                    .unwrap();
            }
            Step::Compact { index, term, keep } => {
                // Replaced whole by a rename: the old log or the new one.
                records.push(Rec::Reset(*index, *term));
                records.extend(keep.iter().map(|e| Rec::Entry(e.index, e.term)));
                store.compact(*index, *term, keep).unwrap();
            }
        }
        drop(guards);
        let atomic = matches!(step, Step::Compact { .. });

        // Every acceptable outcome of a cut inside this step.
        let mut candidates: Vec<LogModel> = Vec::new();
        for snap in [snap_file, new_snap] {
            let mut m = log.clone();
            candidates.push(m.with_snapshot_file(snap));
            for (k, r) in records.iter().enumerate() {
                m.apply(r);
                if !atomic || k + 1 == records.len() {
                    candidates.push(m.with_snapshot_file(snap));
                }
            }
        }

        for cut in std::mem::take(&mut *sink.lock()) {
            let point = match cut {
                Ok(p) => p,
                Err(why) => panic!("step {n}: a cut could not be taken: {why}"),
            };
            for seed in 0..6u64 {
                let image = tempfile::tempdir().unwrap();
                point.write_image(seed, image.path());
                let (got, got_hard, _) = restored(image.path())
                    .unwrap_or_else(|e| panic!("{} (seed {seed}): the store does not reopen: {e}", point.label));
                assert!(
                    candidates.contains(&got),
                    "{} (seed {seed}): restored {got:?}, not one of {candidates:?}",
                    point.label
                );
                assert!(got_hard == hard || got_hard == new_hard, "{} (seed {seed}): hard state {got_hard:?}", point.label);
                checked += 1;
            }
        }

        for r in &records {
            log.apply(r);
        }
        snap_file = new_snap;
        hard = new_hard;
        // And with no cut, the store holds exactly that.
        let (got, got_hard, _) = restored(dir.path()).unwrap();
        assert_eq!(got, log.with_snapshot_file(snap_file), "step {n} as left");
        assert_eq!(got_hard, hard);
    }
    assert!(checked > 40, "only {checked} images were checked: the cuts did not fire");
}
