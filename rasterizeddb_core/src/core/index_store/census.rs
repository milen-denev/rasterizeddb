//! The census (docs/persistent-indexes.md §13 V3): a test-only journal of
//! what the write gate is for, in the order it happened.
//!
//! Four kinds of event are recorded, each where it cannot be skipped:
//! - **a count taken or given back on a gate word** ([`Event::Hold`],
//!   [`Event::Release`]), in `gate_word::WriteGate` itself: every ticket,
//!   family-kept or transient, and every landing of staged bytes;
//! - **`DIRTY` made durable** ([`Event::DirtyDurable`]), by the slow path
//!   after its last fdatasync (or the `Unknown` rewrite's directory sync)
//!   returned, before the word moves to `Dirty`;
//! - **an in-memory mutation** ([`Event::Mutation`]), at the raw mutator of
//!   `LiveIndex`, `GinIndex`, `TableIndexes` and `TableFilters`, which has the
//!   proof in hand, with the structure's address and the proof's kind;
//! - **a write issued** ([`Event::Write`]), at the storage provider's write
//!   entry points and where a provider lands staged bytes, with the word it
//!   registered on (`gated_io`, B12).
//!
//! [`check`] then asserts what T1 and T2 promise for one gate: a durable
//! `DIRTY` and a held ticket preceded every ticket-proven mutation of its
//! structures and every write to its files. The proof proves that *a*
//! ticket is held (C10); this is what proves it was this gate's.
//!
//! Production builds compile every hook to nothing: the functions below are
//! empty `#[inline(always)]` bodies unless `cfg(test)`.

use std::path::Path;

use super::gate::Proof;

#[cfg(test)]
pub use recording::*;

/// Records a count taken on the gate word at `gate` (its address).
#[inline(always)]
pub fn hold(gate: usize) {
    #[cfg(test)]
    recording::push(|| Event::Hold { gate });
    #[cfg(not(test))]
    let _ = gate;
}

/// Records a count given back on the gate word at `gate`.
#[inline(always)]
pub fn release(gate: usize) {
    #[cfg(test)]
    recording::push(|| Event::Release { gate });
    #[cfg(not(test))]
    let _ = gate;
}

/// Records that the slow path of the gate word at `gate` made `DIRTY`
/// durable (or the log durably `HEADER` + `DIRTY`, or `.ixs` durably gone).
#[inline(always)]
pub fn dirty_durable(gate: usize) {
    #[cfg(test)]
    recording::push(|| Event::DirtyDurable { gate });
    #[cfg(not(test))]
    let _ = gate;
}

/// Records an in-memory mutation of the structure at `at`, made under
/// `proof`. Called first thing by every raw mutator.
#[inline(always)]
pub fn mutation(proof: Proof<'_>, at: usize, what: &'static str) {
    #[cfg(test)]
    {
        let kind = proof.kind();
        recording::push(|| Event::Mutation { at, kind, what });
    }
    #[cfg(not(test))]
    let _ = (proof, at, what);
}

/// A write issued, as `GatedIo` saw it (slice B12).
#[derive(Debug, Clone, Copy)]
pub struct Written<'a> {
    pub location: &'a str,
    pub file: &'a str,
    /// The word the write registered on (its address), if its provider has
    /// one.
    pub gate: Option<usize>,
    /// For a provider with no word: the live file set the file belongs to,
    /// if one is open.
    pub live: Option<usize>,
    /// The `gated_io::UNGATED` entry the write ran under: its provider's
    /// exempt policy, or the `gated_io::ungated` scope around it.
    pub ungated: Option<super::gated_io::Ungated>,
}

/// Records a write issued.
#[inline(always)]
pub fn write(w: Written<'_>) {
    #[cfg(test)]
    recording::push(|| Event::Write {
        file: Path::new(w.location).join(w.file),
        gate: w.gate,
        live: w.live,
        ungated: w.ungated,
    });
    #[cfg(not(test))]
    let _ = (w, Path::new(""));
}

#[cfg(test)]
mod recording {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use parking_lot::Mutex;

    use crate::core::index_store::gate::ProofKind;

    /// One journal entry.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum Event {
        Hold { gate: usize },
        Release { gate: usize },
        DirtyDurable { gate: usize },
        Mutation { at: usize, kind: ProofKind, what: &'static str },
        /// A write issued: the word it registered on, or for a provider with
        /// no word the live file set it reached; and the `UNGATED` entry it
        /// ran under, if any (B12).
        Write {
            file: PathBuf,
            gate: Option<usize>,
            live: Option<usize>,
            ungated: Option<crate::core::index_store::gated_io::Ungated>,
        },
    }

    static ACTIVE: AtomicUsize = AtomicUsize::new(0);
    static JOURNAL: Mutex<Vec<Event>> = Mutex::new(Vec::new());

    pub(super) fn push(event: impl FnOnce() -> Event) {
        if ACTIVE.load(Ordering::Acquire) == 0 {
            return;
        }
        let e = event();
        JOURNAL.lock().push(e);
    }

    /// Records from now until [`Census::finish`]. Tests running beside it in
    /// the same binary record too; [`check`] keeps only the events of the
    /// gate, structures and files it is given.
    pub struct Census {
        from: usize,
        done: bool,
    }

    impl Census {
        pub fn start() -> Census {
            let mut journal = JOURNAL.lock();
            if ACTIVE.fetch_add(1, Ordering::AcqRel) == 0 {
                journal.clear();
            }
            Census {
                from: journal.len(),
                done: false,
            }
        }

        /// Stops recording and returns what was recorded since the start.
        pub fn finish(mut self) -> Vec<Event> {
            self.done = true;
            let mut journal = JOURNAL.lock();
            let events = journal[self.from.min(journal.len())..].to_vec();
            if ACTIVE.fetch_sub(1, Ordering::AcqRel) == 1 {
                journal.clear();
            }
            events
        }
    }

    impl Drop for Census {
        fn drop(&mut self) {
            if !self.done {
                let mut journal = JOURNAL.lock();
                if ACTIVE.fetch_sub(1, Ordering::AcqRel) == 1 {
                    journal.clear();
                }
            }
        }
    }

    /// What one gate's census holds.
    #[derive(Debug, Default, Clone, PartialEq, Eq)]
    pub struct Summary {
        pub holds: usize,
        pub releases: usize,
        pub dirty_durable: usize,
        pub ticket_mutations: usize,
        pub build_mutations: usize,
        pub writes: usize,
        /// Writes to an exempt file (see [`Scope::exempt`]).
        pub exempt_writes: usize,
        /// The mutators seen, in order of first appearance.
        pub mutators: Vec<&'static str>,
    }

    /// Which events belong to the gate under test.
    pub struct Scope<'a> {
        /// The gate word's address (`FileGate::census_id`).
        pub gate: usize,
        /// Whether the gate began `Clean` or `Unknown`: then a
        /// [`Event::DirtyDurable`] must come before anything is touched.
        /// A gate that began `Dirty` had its `DIRTY` durable already.
        pub began_clean: bool,
        /// The addresses of the gate's structures (`Table::census_structures`).
        pub structures: &'a [usize],
        /// The gate's files.
        pub files: &'a [PathBuf],
        /// File-name suffixes written with no ticket held, each for a reason
        /// the test states: they still need the durable `DIRTY` (T1).
        pub exempt: &'a [&'a str],
    }

    /// Checks T1 and T2 over `events` for `scope`: before every ticket-proven
    /// mutation of the gate's structures and every write to its files, a
    /// durable `DIRTY` (when the gate began clean) and a ticket held on the
    /// gate. `Err` names the first event that broke it.
    pub fn check(events: &[Event], scope: &Scope<'_>) -> Result<Summary, String> {
        let mut s = Summary::default();
        let mut held: i64 = 0;
        let mut dirty = !scope.began_clean;
        for (i, e) in events.iter().enumerate() {
            match e {
                Event::Hold { gate } if *gate == scope.gate => {
                    held += 1;
                    s.holds += 1;
                }
                Event::Release { gate } if *gate == scope.gate => {
                    held -= 1;
                    s.releases += 1;
                    if held < 0 {
                        return Err(format!("event {i}: a release with no hold on the gate"));
                    }
                }
                Event::DirtyDurable { gate } if *gate == scope.gate => {
                    dirty = true;
                    s.dirty_durable += 1;
                }
                Event::Mutation { at, kind, what } if scope.structures.contains(at) => {
                    if !s.mutators.contains(what) {
                        s.mutators.push(what);
                    }
                    match kind {
                        ProofKind::Build => s.build_mutations += 1,
                        ProofKind::Ticket => {
                            s.ticket_mutations += 1;
                            if !dirty {
                                return Err(format!("event {i}: {what} mutated before DIRTY was durable"));
                            }
                            if held <= 0 {
                                return Err(format!("event {i}: {what} mutated with no ticket held on the gate"));
                            }
                        }
                    }
                }
                Event::Write { file, .. } if scope.files.contains(file) => {
                    if !dirty {
                        return Err(format!(
                            "event {i}: a write to {} was issued before DIRTY was durable",
                            file.display()
                        ));
                    }
                    let name = file.file_name().and_then(|n| n.to_str()).unwrap_or("");
                    if scope.exempt.iter().any(|x| name.ends_with(x)) {
                        s.exempt_writes += 1;
                        continue;
                    }
                    s.writes += 1;
                    if held <= 0 {
                        return Err(format!(
                            "event {i}: a write to {} was issued with no ticket held on the gate",
                            file.display()
                        ));
                    }
                }
                _ => {}
            }
        }
        Ok(s)
    }

    /// The holds recorded on `gate`.
    pub fn holds_on(events: &[Event], gate: usize) -> usize {
        events
            .iter()
            .filter(|e| matches!(e, Event::Hold { gate: g } if *g == gate))
            .count()
    }
}
