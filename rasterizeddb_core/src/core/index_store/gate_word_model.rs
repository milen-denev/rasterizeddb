//! The gate word's interleaving model (docs/persistent-indexes.md §2: "a
//! loom model test interleaves `ticket()`, drop, seal, `CANCEL`, a `GatedIo`
//! write and retirement's seal cancel").
//!
//! loom is not available offline here, so this is an exhaustive search
//! instead. Each model thread is one of the real operation machines of
//! `gate_word.rs`, stepped one atomic operation at a time against a shared
//! word; a depth-first search visits every state reachable by any
//! interleaving of those steps (states are memoised: the invariants are
//! predicates on a state, so visiting each once is enough). The word is a
//! single atomic location and every operation on it is `SeqCst`, so these
//! interleavings are every behaviour the hardware can produce.
//!
//! Checked in every reachable state:
//! - **no ticket is held, and no `GatedIo` write is in flight, while the
//!   state is `Clean` or `PUBLISHING` is set**;
//! - **`CANCEL` is never set on an unsealed word**;
//! - the word is well formed (no count wrapped into the reserved bits);
//! - no state is stuck: something can always move until every thread ends;
//! - and, reported after the search so that a semantic violation is shown
//!   first, the count equals the number of threads counted on it.

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering::SeqCst};

use super::*;

/// A writer: `ticket()` with its slow path, one mutation, the drop.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Writer {
    Ticket(TicketOp, u8),
    /// The slow path's I/O succeeded; mark the state `Dirty`.
    Slow(MarkDirtyOp, u8),
    /// Waiting for a mandatory seal or a `PUBLISHING` window to end.
    Blocked(u8),
    /// Held: mutating. The next step drops the ticket.
    Holding,
    Done,
}

/// A `GatedIo` write: register, issue, return.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Io {
    Register(IoOp),
    /// Registered and in flight. The next step returns.
    Issuing,
    Done,
}

/// A seal from start to unseal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Sealer {
    Start,
    /// Mandatory: waiting for the count to reach zero (or `seal_timeout`).
    Drain,
    Publish(PublishOp, u8),
    Unseal(UnsealOp, SealEnd),
    Done,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Thread {
    Writer(Writer),
    Io(Io),
    Sealer { kind: SealKind, outcome: SealEnd, at: Sealer },
    /// Retirement's seal cancel.
    Retirer(Option<CancelSealOp>),
}

impl Thread {
    fn writer(tries: u8) -> Thread {
        Thread::Writer(Writer::Ticket(TicketOp::Add, tries))
    }

    fn io() -> Thread {
        Thread::Io(Io::Register(IoOp::Add))
    }

    fn sealer(kind: SealKind, outcome: SealEnd) -> Thread {
        Thread::Sealer {
            kind,
            outcome,
            at: Sealer::Start,
        }
    }

    fn retirer() -> Thread {
        Thread::Retirer(Some(CancelSealOp::Load))
    }

    fn done(&self) -> bool {
        matches!(
            self,
            Thread::Writer(Writer::Done)
                | Thread::Io(Io::Done)
                | Thread::Sealer { at: Sealer::Done, .. }
                | Thread::Retirer(None)
        )
    }

    /// Whether this thread is counted on the word now.
    fn counted(&self) -> bool {
        matches!(
            self,
            Thread::Writer(Writer::Ticket(TicketOp::Cancel { .. } | TicketOp::BackOut { .. }, _))
                | Thread::Writer(Writer::Holding)
                | Thread::Io(Io::Register(IoOp::Cancel { .. } | IoOp::BackOut { .. }))
                | Thread::Io(Io::Issuing)
        )
    }

    /// Whether the thread holds a ticket or has a write in flight.
    fn mutating(&self) -> bool {
        matches!(self, Thread::Writer(Writer::Holding) | Thread::Io(Io::Issuing))
    }

    /// A drop or a return: explored after every other step, so a violation
    /// that needs a holder to stay is found before the holder leaves.
    fn is_release(&self) -> bool {
        self.mutating()
    }

    /// Whether the thread can step on `word` now.
    fn enabled(&self, word: Word) -> bool {
        match self {
            _ if self.done() => false,
            Thread::Writer(Writer::Blocked(_)) => word.lets_tickets_retry(),
            Thread::Sealer { at: Sealer::Drain, .. } => true, // the count reaching zero, or seal_timeout
            _ => true,
        }
    }

    /// Every next state of this thread, each with the word it leaves.
    fn successors(&self, word: u64) -> Vec<(Thread, u64)> {
        let atom = AtomicU64::new(word);
        let mut me = *self;
        let mut out = Vec::new();
        match &mut me {
            Thread::Writer(w) => {
                *w = match *w {
                    Writer::Ticket(mut op, tries) => match op.step(&atom) {
                        Step::Again => Writer::Ticket(op, tries),
                        Step::Done(TicketEnd::Held) => Writer::Holding,
                        Step::Done(TicketEnd::Slow { reason, .. }) => match reason {
                            SlowReason::Clean | SlowReason::Unknown => Writer::Slow(MarkDirtyOp::Load, tries),
                            SlowReason::SealRunning | SlowReason::Publishing => Writer::Blocked(tries),
                        },
                    },
                    Writer::Slow(mut op, tries) => match op.step(&atom) {
                        Step::Again => Writer::Slow(op, tries),
                        Step::Done(_) if tries == 0 => Writer::Done,
                        Step::Done(_) => Writer::Ticket(TicketOp::Add, tries - 1),
                    },
                    Writer::Blocked(0) => Writer::Done,
                    Writer::Blocked(tries) => Writer::Ticket(TicketOp::Add, tries - 1),
                    Writer::Holding => {
                        release(&atom);
                        Writer::Done
                    }
                    Writer::Done => unreachable!("a finished thread does not step"),
                };
                out.push((me, atom.load(SeqCst)));
            }
            Thread::Io(io) => {
                *io = match *io {
                    Io::Register(mut op) => match op.step(&atom) {
                        Step::Again => Io::Register(op),
                        Step::Done(IoEnd::Issued { .. }) => Io::Issuing,
                        // Release builds take the slow path inline; that is
                        // the writer's path, modelled above.
                        Step::Done(IoEnd::Slow(_)) => Io::Done,
                    },
                    Io::Issuing => {
                        release(&atom);
                        Io::Done
                    }
                    Io::Done => unreachable!("a finished thread does not step"),
                };
                out.push((me, atom.load(SeqCst)));
            }
            Thread::Sealer { kind, outcome, at } => {
                let (kind, outcome) = (*kind, *outcome);
                let next = |at| Thread::Sealer { kind, outcome, at };
                match *at {
                    Sealer::Start => {
                        let at = match seal(&atom, kind) {
                            SealStart::NotDirty => Sealer::Done,
                            SealStart::Sealed { outstanding: 0 } => Sealer::Publish(PublishOp::Load, 2),
                            SealStart::Sealed { .. } => match kind {
                                SealKind::Optional => Sealer::Unseal(UnsealOp::Load, SealEnd::Abandoned),
                                SealKind::Mandatory => Sealer::Drain,
                            },
                        };
                        out.push((next(at), atom.load(SeqCst)));
                    }
                    Sealer::Drain => {
                        // seal_timeout: skip, leaving DIRTY.
                        out.push((next(Sealer::Unseal(UnsealOp::Load, SealEnd::Abandoned)), word));
                        if Word(word).count() == 0 {
                            out.push((next(Sealer::Publish(PublishOp::Load, 2)), word));
                        }
                    }
                    Sealer::Publish(mut op, retries) => {
                        let at = match op.step(&atom) {
                            Step::Again => Sealer::Publish(op, retries),
                            Step::Done(PublishEnd::Publishing) => Sealer::Unseal(UnsealOp::Load, outcome),
                            Step::Done(PublishEnd::Busy { .. })
                                if kind == SealKind::Mandatory && retries > 0 =>
                            {
                                Sealer::Publish(PublishOp::Load, retries - 1)
                            }
                            Step::Done(_) => Sealer::Unseal(UnsealOp::Load, SealEnd::Abandoned),
                        };
                        out.push((next(at), atom.load(SeqCst)));
                    }
                    Sealer::Unseal(mut op, end) => {
                        let at = match op.step(&atom, end) {
                            Step::Again => Sealer::Unseal(op, end),
                            Step::Done(_) => Sealer::Done,
                        };
                        out.push((next(at), atom.load(SeqCst)));
                    }
                    Sealer::Done => unreachable!("a finished thread does not step"),
                }
            }
            Thread::Retirer(op) => {
                let mut o = op.expect("a finished thread does not step");
                *op = match o.step(&atom) {
                    Step::Again => Some(o),
                    Step::Done(_) => None,
                };
                out.push((me, atom.load(SeqCst)));
            }
        }
        out
    }
}

type State = (u64, Vec<Thread>);

struct Search {
    seen: HashSet<State>,
    /// The first count mismatch: reported only if no invariant fails.
    accounting: Option<String>,
    states: usize,
}

/// The semantic invariants of one state, or why it breaks one.
fn violation(word: u64, threads: &[Thread]) -> Option<String> {
    let w = Word(word);
    if !w.well_formed() {
        return Some(format!("the word is malformed: {word:#018x}"));
    }
    if w.cancelled() && !w.sealed() {
        return Some(format!("CANCEL is set on an unsealed word: {w:?}"));
    }
    for (i, t) in threads.iter().enumerate() {
        if t.mutating() && (w.state() == GateState::Clean || w.publishing()) {
            return Some(format!(
                "thread {i} ({t:?}) holds a ticket or has a write in flight while the word is {:?}{}",
                w.state(),
                if w.publishing() { " and PUBLISHING" } else { "" }
            ));
        }
    }
    None
}

impl Search {
    fn visit(&mut self, word: u64, threads: Vec<Thread>, trace: &mut Vec<String>) {
        if !self.seen.insert((word, threads.clone())) {
            return;
        }
        self.states += 1;
        if let Some(v) = violation(word, &threads) {
            panic!("{v}\n  after:\n    {}", trace.join("\n    "));
        }
        let counted = threads.iter().filter(|t| t.counted()).count() as u64;
        if Word(word).count() != counted && self.accounting.is_none() {
            self.accounting = Some(format!(
                "the word counts {} but {counted} threads are counted, after:\n    {}",
                Word(word).count(),
                trace.join("\n    ")
            ));
        }
        let enabled: Vec<usize> = {
            let mut e: Vec<usize> = (0..threads.len()).filter(|i| threads[*i].enabled(Word(word))).collect();
            e.sort_by_key(|i| threads[*i].is_release());
            e
        };
        if enabled.is_empty() {
            assert!(
                threads.iter().all(Thread::done),
                "stuck: nothing can move in {:?} with {threads:?}, after:\n    {}",
                Word(word),
                trace.join("\n    ")
            );
            return;
        }
        for i in enabled {
            for (t, w) in threads[i].successors(word) {
                let mut next = threads.clone();
                next[i] = t;
                trace.push(format!("{i}: {:?} -> {t:?}, word {:?}", threads[i], Word(w)));
                self.visit(w, next, trace);
                trace.pop();
            }
        }
    }
}

/// Explores every interleaving of `threads` from a word in `state`; returns
/// how many states it visited.
fn explore(state: GateState, threads: Vec<Thread>) -> usize {
    let mut s = Search {
        seen: HashSet::new(),
        accounting: None,
        states: 0,
    };
    s.visit(Word::new(state).0, threads, &mut Vec::new());
    if let Some(a) = s.accounting {
        panic!("{a}");
    }
    eprintln!("gate word model from {state:?}: {} states", s.states);
    s.states
}

use SealEnd::{FailedAfterCleanIssued as Failed, Published};
use SealKind::{Mandatory, Optional};

#[test]
fn writers_preempt_an_optional_seal_beside_a_gated_write() {
    let n = explore(
        GateState::Dirty,
        vec![Thread::writer(2), Thread::writer(2), Thread::sealer(Optional, Published), Thread::io()],
    );
    assert!(n > 1000, "{n} states");
}

#[test]
fn a_mandatory_seal_queues_writers_and_retirement_cancels_it() {
    let n = explore(
        GateState::Dirty,
        vec![Thread::writer(2), Thread::sealer(Mandatory, Published), Thread::io(), Thread::retirer()],
    );
    assert!(n > 500, "{n} states");
}

#[test]
fn a_failed_publish_leaves_unknown_and_writers_rewrite_first() {
    explore(
        GateState::Dirty,
        vec![Thread::writer(2), Thread::sealer(Optional, Failed), Thread::retirer(), Thread::io()],
    );
    explore(
        GateState::Dirty,
        vec![Thread::writer(2), Thread::sealer(Mandatory, Failed), Thread::io(), Thread::io()],
    );
}

#[test]
fn writers_from_clean_and_unknown_mark_dirty_before_holding() {
    explore(GateState::Clean, vec![Thread::writer(2), Thread::writer(2), Thread::io()]);
    explore(GateState::Unknown, vec![Thread::writer(2), Thread::sealer(Optional, Published), Thread::io()]);
    explore(GateState::Clean, vec![Thread::writer(2), Thread::sealer(Mandatory, Published), Thread::retirer()]);
}

#[test]
fn three_writers_against_one_seal() {
    explore(
        GateState::Dirty,
        vec![Thread::writer(1), Thread::writer(1), Thread::writer(1), Thread::sealer(Optional, Published)],
    );
    explore(
        GateState::Dirty,
        vec![Thread::writer(1), Thread::writer(1), Thread::sealer(Mandatory, Published), Thread::io()],
    );
}

#[test]
fn retirement_alone_never_cancels_an_unsealed_word() {
    for state in [GateState::Clean, GateState::Dirty, GateState::Unknown] {
        explore(state, vec![Thread::retirer(), Thread::writer(1)]);
    }
}
