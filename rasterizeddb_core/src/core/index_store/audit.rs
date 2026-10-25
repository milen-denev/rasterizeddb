//! The audit of a loaded snapshot, and `rdb_index_check` (docs/persistent-
//! indexes.md §4 Audit, slice B18).
//!
//! The open's proof checks what someone thought of. So 1 in
//! `index_snapshot.audit_rate` loads (64) also builds a scratch copy from
//! rows in the background and checks **containment in the dangerous
//! direction**: everything a build from rows would list must be in the live
//! structures. More is allowed (zone bounds only widen, an in-place update
//! keeps the old gin items, a rebuild re-adds versions vacuum forgot); less
//! is a false negative, a row a query would never see.
//!
//! # What is compared
//!
//! Every **visible** row, under a snapshot taken when the check begins: a
//! version no snapshot can see is never returned, so an entry missing for
//! it misleads nobody, and a build re-adds versions vacuum already forgot,
//! which the live structures rightly lack.
//! - **Indexes** (B-tree, hash and path): the row's key, built as
//!   `build_indexes` builds it (`LiveIndex::key_of` over the read-back
//!   columns), must list the slot in the live index, or the slot must be
//!   among its unkeyed ones. The scratch pairs of each index are sorted and
//!   walked beside the live index's own sorted stream (hook C3), so the
//!   check costs one sort and one merge, never a lookup per row.
//! - **The filter**, on its meaning, never code for code: a zone's width is
//!   chosen from history (the HLL in completion order, in-place updates
//!   that widen bounds), so a scratch backfill picks other widths and its
//!   codes are not comparable. Instead each row's own value must keep its
//!   slot a candidate for `col = value` in the live filter
//!   (`TableFilters::admits_eq`, the probe's bit for one slot, at the
//!   width the snapshot chose for that zone, with its SuperBloom and zone
//!   bounds). No scratch filter is built.
//!
//! # When a verdict counts
//!
//! Only if nothing a write path mutates moved under it: no ticket was held
//! when the check began, and none was taken until the verdict
//! ([`super::gate_word::WriteGate::tickets_taken`]). A write takes its
//! ticket before it writes a row or touches an index (T1), so a row written
//! after the load, before or during the scratch scan, or between the scan
//! and the walk, moves the count, and the check is **abandoned**, counted
//! and logged, never reported as a mismatch. That covers a row the scan read
//! before an in-place update moved its key (the live index no longer lists
//! the old one), a version vacuum forgot after the scan read it, and a slot
//! reused meanwhile. After a load the word is `Clean`, where every first
//! ticket takes the slow path, so "no ticket since the load" is the design's
//! "runs only while the gate stays `Clean`"; on demand, the same test serves
//! a table that is `Dirty`, provided nobody writes it during the check.
//!
//! # A mismatch
//!
//! Never swaps the scratch in: it lacks what writes add once the build has
//! passed their slots, and those writes keep feeding the live structures.
//! The audit instead takes a `WriteTicket`, so `DIRTY` is durable and the
//! bad snapshot never loads again; demotes what failed (indexes unready,
//! the filter's prefix unindexed and `filters_ready` off) and clears it,
//! all under that ticket; then rebuilds in place exactly as the open's
//! build does, into the installed structures (row/`table_audit.rs`). It
//! counts `rdb_index_snapshot_mismatch_total`, and the next clean point
//! rewrites the snapshot (§3 rule 1).
//!
//! Gin indexes are not compared (persistent phase 3 loads none).

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::core::index::sorted::Sorted;
use crate::core::index::{KeyBuf, LiveIndex};

/// The audit's settings (`index_snapshot.*`), process-wide.
pub mod settings {
    use std::sync::atomic::{AtomicU64, Ordering};

    static AUDIT_RATE: AtomicU64 = AtomicU64::new(64);

    /// `index_snapshot.audit_rate`: 1 in this many snapshot loads is
    /// audited; 0 audits none.
    pub fn audit_rate() -> u64 {
        AUDIT_RATE.load(Ordering::Relaxed)
    }

    pub fn set_audit_rate(n: u64) {
        AUDIT_RATE.store(n, Ordering::Relaxed);
    }

    /// The rate for a file set in `dir`: the process's, or a test's override
    /// for its directory.
    pub fn audit_rate_for(dir: Option<&std::path::Path>) -> u64 {
        #[cfg(test)]
        if let Some(d) = dir
            && let Some(n) = super::hooks::rate_for(d)
        {
            return n;
        }
        let _ = dir;
        audit_rate()
    }
}

// ---------------------------------------------------------------------------
// Counters
// ---------------------------------------------------------------------------

static STARTED_TOTAL: AtomicU64 = AtomicU64::new(0);
static PASSED_TOTAL: AtomicU64 = AtomicU64::new(0);
static MISMATCH_TOTAL: AtomicU64 = AtomicU64::new(0);
static ABANDONED_TOTAL: AtomicU64 = AtomicU64::new(0);
static LOADS_SEEN: AtomicU64 = AtomicU64::new(0);

/// `rdb_index_snapshot_audits_total`: checks begun (after loads, and on
/// demand).
pub fn started_total() -> u64 {
    STARTED_TOTAL.load(Ordering::Relaxed)
}

/// Checks whose verdict was clean.
pub fn passed_total() -> u64 {
    PASSED_TOTAL.load(Ordering::Relaxed)
}

/// `rdb_index_snapshot_mismatch_total`: checks that found a missing index
/// entry or a filter that prunes a visible row's own value.
pub fn mismatch_total() -> u64 {
    MISMATCH_TOTAL.load(Ordering::Relaxed)
}

/// `rdb_index_snapshot_audit_abandoned_total`: checks a write overlapped,
/// or that could not run to a verdict.
pub fn abandoned_total() -> u64 {
    ABANDONED_TOTAL.load(Ordering::Relaxed)
}

/// Whether this load is one of the 1 in `rate` audited: the first load of
/// the process is, then every `rate`-th.
pub fn chosen(dir: Option<&Path>) -> bool {
    let rate = settings::audit_rate_for(dir);
    rate != 0 && LOADS_SEEN.fetch_add(1, Ordering::Relaxed) % rate == 0
}

// ---------------------------------------------------------------------------
// Findings
// ---------------------------------------------------------------------------

/// Why a check began: after a load (the 1 in 64), or `rdb_index_check`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    AfterLoad,
    OnDemand,
}

/// What one file set's check found.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Findings {
    /// Visible rows read back and compared.
    pub rows: u64,
    /// Visible rows whose columns could not be read (kept as candidates by
    /// every build, so not compared).
    pub unreadable: u64,
    /// B-tree, hash and path indexes compared.
    pub indexes: usize,
    /// `(key, slot)` pairs a build would list, checked against the live
    /// indexes.
    pub entries: u64,
    /// Of those, missing from the live index: false negatives.
    pub missing_entries: u64,
    /// `(slot, column)` pairs checked against the live filter.
    pub filter_checked: u64,
    /// Of those, a value the filter prunes at its own slot.
    pub filter_misses: u64,
    /// The first few problems, for the log and the notice.
    pub examples: Vec<String>,
    /// What was not compared, and why (a structure still building, gin).
    pub skipped: Vec<String>,
}

impl Findings {
    /// Whether anything would make a query miss a row.
    pub fn mismatch(&self) -> bool {
        self.missing_entries > 0 || self.filter_misses > 0
    }

    pub(crate) fn note(&mut self, what: String) {
        if self.examples.len() < 8 {
            self.examples.push(what);
        }
    }

    /// Adds another file set's findings (a partitioned table's).
    pub fn absorb(&mut self, other: &Findings) {
        self.rows += other.rows;
        self.unreadable += other.unreadable;
        self.indexes = self.indexes.max(other.indexes);
        self.entries += other.entries;
        self.missing_entries += other.missing_entries;
        self.filter_checked += other.filter_checked;
        self.filter_misses += other.filter_misses;
        for e in &other.examples {
            self.note(e.clone());
        }
        for s in &other.skipped {
            if !self.skipped.contains(s) {
                self.skipped.push(s.clone());
            }
        }
    }
}

/// How a check ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// Everything a build lists is in the live structures.
    Clean(Findings),
    /// Something is missing. `rebuilt` says whether the in-place rebuild
    /// ran to ready.
    Mismatch { findings: Findings, rebuilt: bool },
    /// No verdict: a write overlapped the check, or it could not run.
    Abandoned(String),
}

impl Outcome {
    pub fn findings(&self) -> Option<&Findings> {
        match self {
            Outcome::Clean(f) | Outcome::Mismatch { findings: f, .. } => Some(f),
            Outcome::Abandoned(_) => None,
        }
    }

    /// One line, for logs and `rdb_index_check`'s `result` column.
    pub fn describe(&self) -> String {
        match self {
            Outcome::Clean(_) => "clean".into(),
            Outcome::Mismatch { rebuilt: true, .. } => "mismatch: rebuilt".into(),
            Outcome::Mismatch { rebuilt: false, .. } => "mismatch: rebuild did not finish".into(),
            Outcome::Abandoned(why) => format!("abandoned: {why}"),
        }
    }

    /// Counts the outcome of a check that began.
    pub(crate) fn count(&self) {
        match self {
            Outcome::Clean(_) => PASSED_TOTAL.fetch_add(1, Ordering::Relaxed),
            Outcome::Mismatch { .. } => MISMATCH_TOTAL.fetch_add(1, Ordering::Relaxed),
            Outcome::Abandoned(_) => ABANDONED_TOTAL.fetch_add(1, Ordering::Relaxed),
        };
    }
}

/// Counts a check begun.
pub(crate) fn note_started() {
    STARTED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

// ---------------------------------------------------------------------------
// The index comparison
// ---------------------------------------------------------------------------

/// What a build from rows would list in one index, over the visible rows:
/// its `(key, slot)` pairs and the slots it could not key.
#[derive(Debug, Default)]
pub struct IndexScratch {
    pub pairs: Vec<(KeyBuf, u64)>,
    pub unkeyed: Vec<u64>,
}

/// The pairs of `scratch` that `live` does not list: walked beside the live
/// index's sorted stream, so each live key is visited once. A pair is
/// missing unless its slot is listed under its key, or is among the live
/// unkeyed slots (a candidate for every key). A slot the scratch could not
/// key is missing unless the live index lists it as unkeyed or holds it
/// under some key (its presence bit). Returns the missing pairs, at most
/// `keep` of them, and how many there were.
///
/// The dangerous direction only: what the live index has beyond the scratch
/// is the superset the design allows.
pub fn missing_from(live: &LiveIndex, scratch: &mut IndexScratch, keep: usize) -> (u64, Vec<(KeyBuf, u64)>) {
    scratch.pairs.sort_unstable();
    scratch.pairs.dedup();
    scratch.unkeyed.sort_unstable();
    scratch.unkeyed.dedup();
    let pairs = &scratch.pairs;
    let mut j = 0usize;
    let mut misses: Vec<(KeyBuf, u64)> = Vec::new();
    let mut live_unkeyed: Vec<u64> = Vec::new();
    let mut unkeyed_missing = 0u64;
    let scratch_unkeyed = &scratch.unkeyed;
    let _ = live.for_each_sorted(|part| -> Result<(), ()> {
        match part {
            Sorted::Entry(key, slots) => {
                while j < pairs.len() && pairs[j].0.as_slice() < key {
                    misses.push(pairs[j].clone());
                    j += 1;
                }
                while j < pairs.len() && pairs[j].0.as_slice() == key {
                    if slots.binary_search(&pairs[j].1).is_err() {
                        misses.push(pairs[j].clone());
                    }
                    j += 1;
                }
            }
            Sorted::Unkeyed(slots) => {
                // Every key the live index has went by: what is left of the
                // scratch is under keys it does not have.
                misses.extend(pairs[j..].iter().cloned());
                j = pairs.len();
                live_unkeyed = slots.to_vec();
            }
            Sorted::Present(words) => {
                let held = |s: u64| words.get((s / 64) as usize).is_some_and(|w| w & (1 << (s % 64)) != 0);
                unkeyed_missing = scratch_unkeyed
                    .iter()
                    .filter(|s| live_unkeyed.binary_search(s).is_err() && !held(**s))
                    .count() as u64;
            }
        }
        Ok(())
    });
    misses.retain(|(_, s)| live_unkeyed.binary_search(s).is_err());
    let total = misses.len() as u64 + unkeyed_missing;
    misses.truncate(keep);
    (total, misses)
}

/// Every index against its scratch ([`missing_from`]), one thread each, off
/// the runtime's workers: a sort and a merge per index. Adds to `findings`.
pub fn compare_indexes(list: &[std::sync::Arc<LiveIndex>], scratch: Vec<IndexScratch>, findings: &mut Findings) {
    let results: Vec<(usize, u64, u64, Vec<(KeyBuf, u64)>)> = super::file_gate::blocking(|| {
        std::thread::scope(|scope| {
            let handles: Vec<_> = list
                .iter()
                .zip(scratch)
                .enumerate()
                .map(|(i, (ix, mut s))| {
                    scope.spawn(move || {
                        let entries = s.pairs.len() as u64 + s.unkeyed.len() as u64;
                        let (missing, examples) = missing_from(ix, &mut s, 4);
                        (i, entries, missing, examples)
                    })
                })
                .collect();
            handles.into_iter().map(|h| h.join().expect("an index comparison panicked")).collect()
        })
    });
    for (i, entries, missing, examples) in results {
        findings.entries += entries;
        findings.missing_entries += missing;
        for (key, slot) in examples {
            findings.note(format!("index {} ({}) does not list slot {slot} under key {key:02x?}", i, list[i].decl.name));
        }
    }
}

// ---------------------------------------------------------------------------
// Test hooks
// ---------------------------------------------------------------------------

/// Where a test can hold a check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Point {
    /// The scratch is built; nothing compared yet.
    Scanned,
    /// The verdict is a mismatch; nothing done about it yet.
    Verdict,
}

/// Test-only: the audit rate for one directory, holds at [`Point`]s, and
/// every outcome, so a test can wait for an audit it cannot call.
#[cfg(test)]
pub(crate) mod hooks {
    use std::path::{Path, PathBuf};

    use parking_lot::Mutex;

    use super::{Outcome, Point};

    static RATES: Mutex<Vec<(PathBuf, u64)>> = Mutex::new(Vec::new());
    static HELD: Mutex<Vec<(PathBuf, Point)>> = Mutex::new(Vec::new());
    static REACHED: Mutex<Vec<(PathBuf, Point)>> = Mutex::new(Vec::new());
    static OUTCOMES: Mutex<Vec<(PathBuf, String, Outcome)>> = Mutex::new(Vec::new());

    /// Removes the override when dropped, and the directory's records.
    pub(crate) struct Rate(PathBuf);

    impl Drop for Rate {
        fn drop(&mut self) {
            RATES.lock().retain(|(d, _)| *d != self.0);
            OUTCOMES.lock().retain(|(d, _, _)| !d.starts_with(&self.0));
            REACHED.lock().retain(|(d, _)| !d.starts_with(&self.0));
        }
    }

    /// Loads of file sets under `dir` are audited 1 in `n`.
    pub(crate) fn rate(dir: &Path, n: u64) -> Rate {
        RATES.lock().push((dir.to_path_buf(), n));
        Rate(dir.to_path_buf())
    }

    pub(super) fn rate_for(dir: &Path) -> Option<u64> {
        RATES.lock().iter().find(|(d, _)| dir.starts_with(d)).map(|(_, n)| *n)
    }

    /// Checks of file sets under `dir` wait at `point` while this lives.
    pub(crate) struct Hold(PathBuf, Point);

    impl Drop for Hold {
        fn drop(&mut self) {
            HELD.lock().retain(|(d, p)| !(*d == self.0 && *p == self.1));
        }
    }

    pub(crate) fn hold(dir: &Path, point: Point) -> Hold {
        HELD.lock().push((dir.to_path_buf(), point));
        Hold(dir.to_path_buf(), point)
    }

    /// Called by a check at `point`: records it, then waits while held.
    pub(crate) async fn at(dir: Option<&Path>, point: Point) {
        let Some(dir) = dir else { return };
        REACHED.lock().push((dir.to_path_buf(), point));
        while HELD.lock().iter().any(|(d, p)| dir.starts_with(d) && *p == point) {
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
    }

    /// How many times checks under `dir` have reached `point`.
    pub(crate) fn reached(dir: &Path, point: Point) -> usize {
        REACHED.lock().iter().filter(|(d, p)| d.starts_with(dir) && *p == point).count()
    }

    pub(crate) fn record(dir: Option<&Path>, name: &str, outcome: &Outcome) {
        if let Some(dir) = dir {
            OUTCOMES.lock().push((dir.to_path_buf(), name.to_string(), outcome.clone()));
        }
    }

    /// Every check's outcome under `dir`, in order, with its file set's name.
    pub(crate) fn outcomes(dir: &Path) -> Vec<(String, Outcome)> {
        OUTCOMES
            .lock()
            .iter()
            .filter(|(d, _, _)| d.starts_with(dir))
            .map(|(_, n, o)| (n.clone(), o.clone()))
            .collect()
    }
}
