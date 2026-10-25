//! The write gate's packed word and its state machine
//! (docs/persistent-indexes.md §2 T1/T2, §3 steps 1, 6 and 7, §7 step 2).
//!
//! # Where it is used
//!
//! Slice B10 built the word, every transition on it, and an exhaustive
//! model of their interleavings. Slice B11 wired it in: each file set's
//! [`WriteGate`] sits in its `index_store::file_gate::FileGate`, which the
//! table's map slot and every instance of the table share, and
//! `gate::ticket` (the `WriteTicket` mint) takes a [`Hold`] through
//! [`WriteGate::acquire`] and wraps it. B12's `GatedIo` calls
//! [`WriteGate::register_io`].
//!
//! # The word
//!
//! One `AtomicU64` per file set:
//!
//! | Bits | Field |
//! |---|---|
//! | 0..56 | the count of outstanding tickets and `GatedIo` registrations |
//! | 56..58 | the state: `Clean` 0, `Dirty` 1, `Unknown` 2 |
//! | 58 | `SEALED` |
//! | 59 | `MANDATORY` |
//! | 60 | `PUBLISHING` |
//! | 61 | `CANCEL` |
//! | 62..64 | zero |
//!
//! The count is in the low bits so that taking a ticket is one
//! `fetch_add(1)` and dropping one a `fetch_sub(1)`. A decrement of a zero
//! count would borrow into the state and flags, so every decrement
//! debug-asserts the count it found was positive.
//!
//! # The invariants
//!
//! - **No ticket is held, and no `GatedIo` write is in flight, while the
//!   state is `Clean`** (nor while `PUBLISHING` is set). A ticket is held
//!   only when the word its `fetch_add` returned (or its `CANCEL`
//!   compare-and-swap replaced) was `Dirty`; the state leaves `Dirty` for
//!   `Clean` only through `PUBLISHING`, whose compare-and-swap requires a
//!   count of zero and `CANCEL` clear; while `PUBLISHING` is set every new
//!   ticket backs out.
//! - **`CANCEL` is never set on an unsealed word.** Writers, `GatedIo` and
//!   retirement set it only by compare-and-swap against a word that still
//!   shows an unpublished seal, never by `fetch_or`, and the unseal clears
//!   it in the same RMW that clears `SEALED`. A stray `CANCEL` would let
//!   later tickets skip a mandatory seal's queue (P7) and make every later
//!   seal skip (R8).
//!
//! # How the operations are written
//!
//! Each multi-step operation is a small state machine ([`TicketOp`],
//! [`IoOp`], [`PublishOp`], [`UnsealOp`], [`CancelSealOp`], [`MarkDirtyOp`])
//! whose every `step` performs at most one atomic operation on the word.
//! [`WriteGate`] drives each to completion in a loop. The model test drives
//! the same machines, one step at a time, through every interleaving of a
//! few threads: loom is not available offline here, and because the word is
//! a single atomic location, on which every operation is `SeqCst` and
//! linearizable, the interleavings of these steps are exactly the behaviours
//! the hardware can produce.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::SeqCst};

/// The count's bits.
pub const COUNT_MASK: u64 = (1 << 56) - 1;
const STATE_SHIFT: u32 = 56;
const STATE_MASK: u64 = 0b11 << STATE_SHIFT;
/// A seal is running: tickets either cancel it (optional) or queue behind it
/// (mandatory).
pub const SEALED: u64 = 1 << 58;
/// The running seal is mandatory: shutdown, `rdb_index_checkpoint`.
pub const MANDATORY: u64 = 1 << 59;
/// The seal is publishing its `CLEAN`; nothing new may start.
pub const PUBLISHING: u64 = 1 << 60;
/// The running seal was cancelled and will not publish.
pub const CANCEL: u64 = 1 << 61;
const FLAGS: u64 = SEALED | MANDATORY | PUBLISHING | CANCEL;
const RESERVED: u64 = !(COUNT_MASK | STATE_MASK | FLAGS);

/// What the marker log says, as the word holds it (§2, N21).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum GateState {
    /// The last good record is `CLEAN`: the next write needs `DIRTY` first.
    Clean = 0,
    /// The log ends `DIRTY` (or `HEADER`, or there is none).
    Dirty = 1,
    /// The log proves nothing: the next write rewrites it or unlinks `.ixs`.
    Unknown = 2,
}

/// A value of the word.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Word(pub u64);

impl Word {
    /// A word in `state` with no flags and no count.
    pub const fn new(state: GateState) -> Word {
        Word((state as u64) << STATE_SHIFT)
    }

    pub const fn state(self) -> GateState {
        match (self.0 & STATE_MASK) >> STATE_SHIFT {
            0 => GateState::Clean,
            1 => GateState::Dirty,
            // 3 is never written; reading it as Unknown keeps it safe.
            _ => GateState::Unknown,
        }
    }

    pub const fn count(self) -> u64 {
        self.0 & COUNT_MASK
    }

    pub const fn sealed(self) -> bool {
        self.0 & SEALED != 0
    }

    pub const fn mandatory(self) -> bool {
        self.0 & MANDATORY != 0
    }

    pub const fn publishing(self) -> bool {
        self.0 & PUBLISHING != 0
    }

    pub const fn cancelled(self) -> bool {
        self.0 & CANCEL != 0
    }

    /// Whether the reserved top bits are clear: a wrap of the count sets
    /// them.
    pub const fn well_formed(self) -> bool {
        self.0 & RESERVED == 0
    }

    const fn dirty(self) -> bool {
        matches!(self.state(), GateState::Dirty)
    }

    const fn with_state(self, state: GateState) -> Word {
        Word((self.0 & !STATE_MASK) | ((state as u64) << STATE_SHIFT))
    }

    /// An optional seal a writer may cancel: `Dirty`, `SEALED`, not
    /// `MANDATORY`, not `PUBLISHING`, not `CANCEL`.
    pub const fn shows_optional_seal(self) -> bool {
        self.dirty() && self.sealed() && !self.mandatory() && !self.publishing() && !self.cancelled()
    }

    /// A seal that can still be cancelled: `SEALED`, not `PUBLISHING`, not
    /// `CANCEL`.
    pub const fn shows_unpublished_seal(self) -> bool {
        self.sealed() && !self.publishing() && !self.cancelled()
    }

    /// The writer's verdict on the word its `fetch_add` returned.
    pub const fn ticket_verdict(self) -> TicketVerdict {
        if self.dirty() && (!self.sealed() || (self.cancelled() && !self.publishing())) {
            TicketVerdict::Hold
        } else if self.shows_optional_seal() {
            TicketVerdict::CancelSeal
        } else {
            TicketVerdict::Slow(self.slow_reason())
        }
    }

    /// Why a ticket or a `GatedIo` write cannot go ahead on this word.
    pub const fn slow_reason(self) -> SlowReason {
        if self.publishing() {
            SlowReason::Publishing
        } else {
            match self.state() {
                GateState::Clean => SlowReason::Clean,
                GateState::Unknown => SlowReason::Unknown,
                GateState::Dirty => SlowReason::SealRunning,
            }
        }
    }

    /// Whether a slow-path writer waiting on [`SlowReason::SealRunning`] or
    /// [`SlowReason::Publishing`] should try again.
    pub const fn lets_tickets_retry(self) -> bool {
        !(self.publishing() || (self.sealed() && self.mandatory() && !self.cancelled()))
    }
}

/// The writer's verdict on the word its `fetch_add` returned.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TicketVerdict {
    /// Held: one RMW, no lock.
    Hold,
    /// An optional seal: keep the count, set `CANCEL` by compare-and-swap.
    CancelSeal,
    /// Back out and take the slow path.
    Slow(SlowReason),
}

/// Why the fast path failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SlowReason {
    /// A mandatory seal is running: wait for it to end.
    SealRunning,
    /// A seal is publishing its `CLEAN`: wait for the window to end.
    Publishing,
    /// The log ends `CLEAN`: append `DIRTY` and fdatasync first (T1).
    Clean,
    /// The log proves nothing: rewrite it, or unlink `.ixs`, first.
    Unknown,
}

/// One step's result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step<T> {
    Again,
    Done(T),
}

/// How taking a ticket ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TicketEnd {
    /// The ticket is held; its count is on the word.
    Held,
    /// Backed out. `last_out` when the back-out took the count to zero,
    /// which wakes a sealer waiting for it.
    Slow { reason: SlowReason, last_out: bool },
}

/// `ticket()`'s fast path (§2 layer 1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TicketOp {
    /// `fetch_add(1)` and judge the returned word.
    Add,
    /// The word showed an optional seal: set `CANCEL` while it still shows
    /// that seal. `seen` is the word the swap expects.
    Cancel { seen: u64 },
    /// `fetch_sub(1)`: then retry from the top (`then: None`), or end on the
    /// slow path.
    BackOut { then: Option<SlowReason> },
}

impl TicketOp {
    pub fn step(&mut self, word: &AtomicU64) -> Step<TicketEnd> {
        match *self {
            TicketOp::Add => {
                let w = Word(word.fetch_add(1, SeqCst));
                debug_assert!(w.count() < COUNT_MASK, "the ticket count would carry into the state");
                match w.ticket_verdict() {
                    TicketVerdict::Hold => return Step::Done(TicketEnd::Held),
                    TicketVerdict::CancelSeal => *self = TicketOp::Cancel { seen: w.0 + 1 },
                    TicketVerdict::Slow(reason) => *self = TicketOp::BackOut { then: Some(reason) },
                }
            }
            TicketOp::Cancel { seen } => match word.compare_exchange(seen, seen | CANCEL, SeqCst, SeqCst) {
                Ok(_) => return Step::Done(TicketEnd::Held),
                Err(actual) if Word(actual).shows_optional_seal() => *self = TicketOp::Cancel { seen: actual },
                // The seal skipped, became mandatory, reached PUBLISHING, or
                // someone else cancelled it: back out and start again, so
                // CANCEL is never left on an unsealed word.
                Err(_) => *self = TicketOp::BackOut { then: None },
            },
            TicketOp::BackOut { then } => {
                let prev = Word(word.fetch_sub(1, SeqCst));
                debug_assert!(prev.count() > 0, "a back-out found no count to give back");
                match then {
                    None => *self = TicketOp::Add,
                    Some(reason) => {
                        return Step::Done(TicketEnd::Slow {
                            reason,
                            last_out: prev.count() == 1,
                        });
                    }
                }
            }
        }
        Step::Again
    }
}

/// How a `GatedIo` registration ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IoEnd {
    /// Registered: the write may be issued and holds the count until it
    /// returns. `ticketless` when no ticket existed anywhere and the
    /// registration cancelled an unpublished seal to go ahead: tests panic
    /// with the call site there; release issues it.
    Issued { ticketless: bool },
    /// Backed out: wait (`Publishing`) or take the slow path inline.
    Slow(SlowReason),
}

/// `GatedIo`'s registration (§2 layer 2): the ticket RMW, judged for a disk
/// write.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IoOp {
    Add,
    /// No ticket anywhere and an unpublished seal: cancel it by
    /// compare-and-swap, as `ticket()` would.
    Cancel { seen: u64 },
    BackOut { reason: SlowReason },
}

impl IoOp {
    pub fn step(&mut self, word: &AtomicU64) -> Step<IoEnd> {
        match *self {
            IoOp::Add => {
                let w = Word(word.fetch_add(1, SeqCst));
                debug_assert!(w.count() < COUNT_MASK, "the registration count would carry into the state");
                if !w.dirty() || w.publishing() {
                    *self = IoOp::BackOut { reason: w.slow_reason() };
                } else if w.shows_unpublished_seal() && w.count() == 0 {
                    *self = IoOp::Cancel { seen: w.0 + 1 };
                } else {
                    // Unsealed; or sealed and cancelled; or sealed with a
                    // count that may be a ticket holder's, which keeps the
                    // seal from publishing until this write returns.
                    return Step::Done(IoEnd::Issued { ticketless: false });
                }
            }
            IoOp::Cancel { seen } => match word.compare_exchange(seen, seen | CANCEL, SeqCst, SeqCst) {
                Ok(_) => return Step::Done(IoEnd::Issued { ticketless: true }),
                Err(actual) if Word(actual).shows_unpublished_seal() => *self = IoOp::Cancel { seen: actual },
                // The seal ended or was cancelled. This registration's count
                // has kept PUBLISHING out throughout, so the state is still
                // Dirty.
                Err(_) => return Step::Done(IoEnd::Issued { ticketless: true }),
            },
            IoOp::BackOut { reason } => {
                let prev = Word(word.fetch_sub(1, SeqCst));
                debug_assert!(prev.count() > 0, "a back-out found no count to give back");
                return Step::Done(IoEnd::Slow(reason));
            }
        }
        Step::Again
    }
}

/// Gives back one count: a dropped ticket or a returned `GatedIo` write.
/// True when it was the last, which wakes a sealer.
pub fn release(word: &AtomicU64) -> bool {
    let prev = Word(word.fetch_sub(1, SeqCst));
    debug_assert!(prev.count() > 0, "a release would borrow into the state and flags");
    prev.count() == 1
}

/// Which seal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SealKind {
    /// The scheduler's idle checkpoint, an idle close: writers preempt it.
    Optional,
    /// Shutdown, `rdb_index_checkpoint`: new tickets queue behind it.
    Mandatory,
}

/// How a seal began.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SealStart {
    /// Sealed; `outstanding` tickets were counted when it looked. An optional
    /// seal with any skips at once; a mandatory one waits for them.
    Sealed { outstanding: u64 },
    /// The state was not `Dirty`; nothing is sealed. A seal starts only from
    /// `Dirty` (§3).
    NotDirty,
}

/// Starts a seal: `fetch_or(SEALED)`, then the count. The caller holds the
/// maintenance lock, so no other seal is running.
pub fn seal(word: &AtomicU64, kind: SealKind) -> SealStart {
    let bits = match kind {
        SealKind::Optional => SEALED,
        SealKind::Mandatory => SEALED | MANDATORY,
    };
    let prev = Word(word.fetch_or(bits, SeqCst));
    debug_assert!(prev.0 & FLAGS == 0, "a seal began while another ran: {prev:?}");
    if !prev.dirty() {
        // Not ours to seal: clear what this set, and any CANCEL set on it
        // meanwhile, in one RMW.
        word.fetch_and(!FLAGS, SeqCst);
        return SealStart::NotDirty;
    }
    SealStart::Sealed {
        outstanding: prev.count(),
    }
}

/// How the `PUBLISHING` compare-and-swap ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PublishEnd {
    /// Set: append `CLEAN` and fdatasync, then unseal.
    Publishing,
    /// The seal was cancelled: skip.
    Cancelled,
    /// Tickets or registrations are counted. An optional seal skips as if
    /// cancelled; a mandatory one retries while the count is only transient
    /// back-outs.
    Busy { count: u64 },
    /// No seal is running, or the state left `Dirty`: a caller bug.
    NotSealed,
}

/// The `PUBLISHING` compare-and-swap (§3 step 6): requires `SEALED`,
/// `Dirty`, `CANCEL` clear **and a count of zero**.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PublishOp {
    Load,
    Cas { seen: u64 },
}

impl PublishOp {
    fn judge(w: Word) -> Result<(), PublishEnd> {
        if !w.sealed() || !w.dirty() || w.publishing() {
            Err(PublishEnd::NotSealed)
        } else if w.cancelled() {
            Err(PublishEnd::Cancelled)
        } else if w.count() != 0 {
            Err(PublishEnd::Busy { count: w.count() })
        } else {
            Ok(())
        }
    }

    pub fn step(&mut self, word: &AtomicU64) -> Step<PublishEnd> {
        let seen = match *self {
            PublishOp::Load => Word(word.load(SeqCst)),
            PublishOp::Cas { seen } => match word.compare_exchange(seen, seen | PUBLISHING, SeqCst, SeqCst) {
                Ok(_) => return Step::Done(PublishEnd::Publishing),
                Err(actual) => Word(actual),
            },
        };
        match PublishOp::judge(seen) {
            Ok(()) => {
                *self = PublishOp::Cas { seen: seen.0 };
                Step::Again
            }
            Err(end) => Step::Done(end),
        }
    }
}

/// How a seal ended, which decides the state the unseal leaves (§3 step 7).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SealEnd {
    /// `CLEAN` is durable: the state becomes `Clean`.
    Published,
    /// Skipped, cancelled, or failed before `PUBLISHING`: the state stays.
    Abandoned,
    /// `CLEAN`'s write was issued and its fdatasync, or anything after it,
    /// failed: the page may persist now or later, so `Unknown`, never
    /// `Dirty`.
    FailedAfterCleanIssued,
}

/// The unseal: the new state and every flag cleared, in one compare-and-swap
/// that keeps the count (transient back-outs may be on it).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum UnsealOp {
    Load,
    Cas { seen: u64 },
}

impl UnsealOp {
    fn target(seen: Word, end: SealEnd) -> Word {
        let state = match end {
            SealEnd::Published => {
                debug_assert!(seen.publishing(), "published without PUBLISHING");
                GateState::Clean
            }
            SealEnd::Abandoned => seen.state(),
            SealEnd::FailedAfterCleanIssued => GateState::Unknown,
        };
        Word((seen.0 & COUNT_MASK) | ((state as u64) << STATE_SHIFT))
    }

    pub fn step(&mut self, word: &AtomicU64, end: SealEnd) -> Step<GateState> {
        let seen = match *self {
            UnsealOp::Load => Word(word.load(SeqCst)),
            UnsealOp::Cas { seen } => {
                let to = UnsealOp::target(Word(seen), end);
                match word.compare_exchange(seen, to.0, SeqCst, SeqCst) {
                    Ok(_) => return Step::Done(to.state()),
                    Err(actual) => Word(actual),
                }
            }
        };
        *self = UnsealOp::Cas { seen: seen.0 };
        Step::Again
    }
}

/// How retirement's seal cancel ended (§7 step 2(a), R8).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CancelEnd {
    Cancelled,
    AlreadyCancelled,
    /// Nothing to cancel.
    NoSeal,
    /// Too late: the seal is publishing. Retirement then waits for the
    /// maintenance lock.
    Publishing,
}

/// Retirement's cancel: `CANCEL` by compare-and-swap, only while the word
/// shows an unpublished seal; never `fetch_or`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CancelSealOp {
    Load,
    Cas { seen: u64 },
}

impl CancelSealOp {
    pub fn step(&mut self, word: &AtomicU64) -> Step<CancelEnd> {
        let seen = match *self {
            CancelSealOp::Load => Word(word.load(SeqCst)),
            CancelSealOp::Cas { seen } => match word.compare_exchange(seen, seen | CANCEL, SeqCst, SeqCst) {
                Ok(_) => return Step::Done(CancelEnd::Cancelled),
                Err(actual) => Word(actual),
            },
        };
        if !seen.sealed() {
            Step::Done(CancelEnd::NoSeal)
        } else if seen.publishing() {
            Step::Done(CancelEnd::Publishing)
        } else if seen.cancelled() {
            Step::Done(CancelEnd::AlreadyCancelled)
        } else {
            *self = CancelSealOp::Cas { seen: seen.0 };
            Step::Again
        }
    }
}

/// How the slow path's state change ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MarkDirtyEnd {
    /// This call moved the state to `Dirty`.
    Marked,
    /// Someone else already had.
    AlreadyDirty,
}

/// The end of the slow path: after `DIRTY` is durable (or the `Unknown`
/// rewrite and `_flt.db` delete returned `Ok`), the state becomes `Dirty`,
/// keeping the count and flags.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MarkDirtyOp {
    Load,
    Cas { seen: u64 },
}

impl MarkDirtyOp {
    pub fn step(&mut self, word: &AtomicU64) -> Step<MarkDirtyEnd> {
        let seen = match *self {
            MarkDirtyOp::Load => Word(word.load(SeqCst)),
            MarkDirtyOp::Cas { seen } => {
                match word.compare_exchange(seen, Word(seen).with_state(GateState::Dirty).0, SeqCst, SeqCst) {
                    Ok(_) => return Step::Done(MarkDirtyEnd::Marked),
                    Err(actual) => Word(actual),
                }
            }
        };
        if seen.dirty() {
            return Step::Done(MarkDirtyEnd::AlreadyDirty);
        }
        *self = MarkDirtyOp::Cas { seen: seen.0 };
        Step::Again
    }
}

fn run<T>(mut step: impl FnMut() -> Step<T>) -> T {
    loop {
        if let Step::Done(t) = step() {
            return t;
        }
    }
}

/// The gate of one file set: the word, and the wakeups around it.
pub struct WriteGate {
    word: AtomicU64,
    /// Woken when a seal ends: slow-path writers waiting on a mandatory seal
    /// or a `PUBLISHING` window.
    unsealed: tokio::sync::Notify,
    /// Woken when the count reaches zero: a mandatory seal waiting for its
    /// outstanding tickets.
    drained: tokio::sync::Notify,
    /// Single-flight for the slow path's I/O.
    slow: tokio::sync::Mutex<()>,
}

/// One count on a [`WriteGate`]: a held ticket (B11 wraps it in
/// `WriteTicket`) or a registered `GatedIo` write. Dropping it gives the
/// count back. Not `Clone`: a copy would give it back twice.
#[derive(Debug)]
pub struct Hold {
    gate: Arc<WriteGate>,
    ticketless: bool,
}

impl Hold {
    /// For a `GatedIo` registration: no ticket existed and it cancelled a
    /// seal to go ahead (tests panic with the call site there).
    pub fn ticketless(&self) -> bool {
        self.ticketless
    }

    /// A second count on the same word, beside this one: one `fetch_add`,
    /// judged on nothing. Sound only because this hold is a ticket: while it
    /// is held the state is `Dirty` and `PUBLISHING` cannot be set (its
    /// compare-and-swap needs a count of zero), so the count added here is
    /// as good as one `ticket()`'s fast path would have taken, and it never
    /// waits. A family's kept ticket and the ticket its statement mutates
    /// under are two such counts (`gate::ticket`).
    pub fn beside(&self) -> Hold {
        debug_assert!(!self.ticketless, "a sibling of a ticketless registration");
        let prev = Word(self.gate.word.fetch_add(1, SeqCst));
        debug_assert!(
            prev.count() > 0 && prev.state() == GateState::Dirty && !prev.publishing(),
            "a sibling count beside no held ticket: {prev:?}"
        );
        super::census::hold(self.gate.census_id());
        Hold {
            gate: self.gate.clone(),
            ticketless: false,
        }
    }

    /// The word this hold counts on.
    pub fn gate(&self) -> &Arc<WriteGate> {
        &self.gate
    }
}

impl Drop for Hold {
    fn drop(&mut self) {
        super::census::release(self.gate.census_id());
        self.gate.give_back();
    }
}

impl std::fmt::Debug for WriteGate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriteGate").field("word", &self.load()).finish()
    }
}

impl WriteGate {
    /// A gate whose log, read at the file set's first open, says `state`.
    pub fn new(state: GateState) -> WriteGate {
        WriteGate {
            word: AtomicU64::new(Word::new(state).0),
            unsealed: tokio::sync::Notify::new(),
            drained: tokio::sync::Notify::new(),
            slow: tokio::sync::Mutex::new(()),
        }
    }

    pub fn load(&self) -> Word {
        Word(self.word.load(SeqCst))
    }

    /// The word's address: who the census attributes a hold to.
    #[inline]
    pub fn census_id(&self) -> usize {
        self as *const WriteGate as usize
    }

    /// Sets the state of a word no ticket, registration or seal is on: the
    /// test-only "force Clean" hook that stands in for a published `CLEAN`
    /// until B13 seals (docs/persistent-indexes.md §13 V3, B11). Waits (up
    /// to ten seconds, then panics) for a moment when nothing is counted and
    /// no flag is set: a background sweep's or landing's transient ticket.
    #[cfg(test)]
    pub fn force_state_for_tests(&self, state: GateState) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let now = self.load();
            if now.count() == 0
                && now.0 & FLAGS == 0
                && self
                    .word
                    .compare_exchange(now.0, Word::new(state).0, SeqCst, SeqCst)
                    .is_ok()
            {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "a forced state change on a word that stayed in use: {now:?}"
            );
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }

    fn give_back(&self) {
        if release(&self.word) {
            self.drained.notify_waiters();
        }
    }

    /// The fast path alone: held, or why not.
    pub fn try_acquire(self: &Arc<Self>) -> Result<Hold, SlowReason> {
        let mut op = TicketOp::Add;
        match run(|| op.step(&self.word)) {
            TicketEnd::Held => {
                super::census::hold(self.census_id());
                Ok(Hold {
                    gate: self.clone(),
                    ticketless: false,
                })
            }
            TicketEnd::Slow { reason, last_out } => {
                if last_out {
                    self.drained.notify_waiters();
                }
                Err(reason)
            }
        }
    }

    /// Takes a ticket, through the slow path when it must: wait out a
    /// mandatory seal or a `PUBLISHING` window; in `Clean` or `Unknown`,
    /// single-flight, run `slow(state)` (append `DIRTY` and fdatasync; or
    /// rewrite the log or unlink `.ixs`, then delete a lingering `_flt.db`)
    /// and mark the state `Dirty` only once it returned `Ok`; then retry. An
    /// error from `slow` fails the ticket and leaves the state as it was.
    pub async fn acquire<F, Fut, E>(self: &Arc<Self>, mut slow: F) -> Result<Hold, E>
    where
        F: FnMut(GateState) -> Fut,
        Fut: Future<Output = Result<(), E>>,
    {
        loop {
            match self.try_acquire() {
                Ok(hold) => return Ok(hold),
                Err(SlowReason::SealRunning | SlowReason::Publishing) => self.wait_for_tickets().await,
                Err(SlowReason::Clean | SlowReason::Unknown) => {
                    let _single_flight = self.slow.lock().await;
                    let w = self.load();
                    if w.state() == GateState::Dirty {
                        continue;
                    }
                    slow(w.state()).await?;
                    let mut op = MarkDirtyOp::Load;
                    run(|| op.step(&self.word));
                }
            }
        }
    }

    /// Waits until the word lets tickets retry: no `PUBLISHING`, and no
    /// uncancelled mandatory seal.
    async fn wait_for_tickets(&self) {
        let notified = self.unsealed.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if self.load().lets_tickets_retry() {
            return;
        }
        notified.await;
    }

    /// Registers a `GatedIo` write: held until the write returns.
    pub fn register_io(self: &Arc<Self>) -> Result<Hold, SlowReason> {
        let mut op = IoOp::Add;
        match run(|| op.step(&self.word)) {
            IoEnd::Issued { ticketless } => {
                super::census::hold(self.census_id());
                Ok(Hold {
                    gate: self.clone(),
                    ticketless,
                })
            }
            IoEnd::Slow(reason) => Err(reason),
        }
    }

    /// §3 step 1. The caller holds the maintenance lock.
    pub fn seal(&self, kind: SealKind) -> SealStart {
        seal(&self.word, kind)
    }

    /// Waits until no ticket or registration is counted: a mandatory seal's
    /// wait (the caller bounds it by `seal_timeout`).
    pub async fn drained(&self) {
        let notified = self.drained.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if self.load().count() == 0 {
            return;
        }
        notified.await;
    }

    /// §3 step 6's compare-and-swap.
    pub fn try_publish(&self) -> PublishEnd {
        let mut op = PublishOp::Load;
        run(|| op.step(&self.word))
    }

    /// §3 step 7: the state `end` leaves, every flag cleared, waiters woken.
    pub fn unseal(&self, end: SealEnd) -> GateState {
        let mut op = UnsealOp::Load;
        let state = run(|| op.step(&self.word, end));
        self.unsealed.notify_waiters();
        state
    }

    /// §7 step 2(a): cancels a running, unpublished seal.
    pub fn cancel_seal(&self) -> CancelEnd {
        let mut op = CancelSealOp::Load;
        run(|| op.step(&self.word))
    }
}

#[cfg(test)]
#[path = "gate_word_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "gate_word_model.rs"]
mod model;
