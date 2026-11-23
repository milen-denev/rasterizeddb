//! A hybrid logical clock, for ordering commits across instances.
//!
//! Multi-primary replication needs an ordering that every node agrees on, and
//! wall-clock time alone does not give one: two nodes whose clocks differ by a
//! second will disagree about which of two commits happened first, and a clock
//! that steps backwards makes a node disagree with *itself*.
//!
//! A hybrid logical clock is a wall-clock reading that is never allowed to go
//! backwards and is bumped whenever a message arrives carrying a higher one. It
//! keeps two properties that matter here:
//!
//! - **Causality.** If this node saw commit `A` before producing commit `B`,
//!   then `A < B`. That is what makes "apply in timestamp order" safe.
//! - **Readability.** The high bits are still milliseconds since the Unix
//!   epoch, so a timestamp printed by `SHOW REPLICATION STATUS` means something
//!   to an operator, unlike an opaque counter.
//!
//! The trade the packing makes: 48 bits of milliseconds runs out in the year
//! 10889, and 16 bits of counter allows 65 536 commits inside one millisecond
//! on one node before the clock has to borrow from the next millisecond — which
//! it does, rather than losing monotonicity.

use cacheguard::CacheGuard;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// A packed hybrid timestamp: 48 bits of milliseconds, 16 bits of counter.
///
/// Ordered by plain integer comparison, which is the whole point of the
/// packing — comparing two of them is one instruction, and the replication
/// apply path does it once per record.
pub type Hlc = u64;

/// Not a timestamp. Sorts below every real one.
pub const NO_HLC: Hlc = 0;

const COUNTER_BITS: u32 = 16;
const COUNTER_MASK: u64 = (1 << COUNTER_BITS) - 1;

/// The wall-clock part, in milliseconds since the Unix epoch.
#[inline]
pub fn physical_ms(hlc: Hlc) -> u64 {
    hlc >> COUNTER_BITS
}

/// The tie-breaking part, which counts commits inside one millisecond.
#[inline]
pub fn counter(hlc: Hlc) -> u16 {
    (hlc & COUNTER_MASK) as u16
}

#[inline]
fn pack(ms: u64, counter: u64) -> Hlc {
    (ms << COUNTER_BITS) | (counter & COUNTER_MASK)
}

/// Milliseconds since the Unix epoch, or 0 if the system clock predates it.
#[inline]
fn wall_clock_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Renders a timestamp as `<millis>.<counter>`, which is what the status views
/// show. Deliberately not a calendar date: the value's job is comparison, and a
/// formatted date invites reading it as one.
pub fn format(hlc: Hlc) -> String {
    if hlc == NO_HLC {
        return "-".to_string();
    }
    format!("{}.{}", physical_ms(hlc), counter(hlc))
}

/// The node's clock. One per database.
///
/// Every operation is a single CAS loop on one word: no lock, and no allocation,
/// so stamping a commit costs the same as an atomic increment in the common
/// case where the wall clock has moved on.
#[derive(Debug)]
pub struct HybridClock {
    /// Padded to a cache line. The clock is embedded by value in
    /// [`Replication`](super::manager::Replication), whose neighbouring fields
    /// are themselves incremented per statement — so without the guard, one
    /// thread taking a capture sequence invalidated the line another thread's
    /// CAS loop was spinning on. `compare_exchange_weak` retries on a lost
    /// line as readily as on a lost race, which turned false sharing into extra
    /// loop iterations rather than just extra latency.
    last: CacheGuard<AtomicU64>,
}

impl Default for HybridClock {
    fn default() -> Self {
        Self::new()
    }
}

impl HybridClock {
    pub fn new() -> Self {
        HybridClock {
            last: AtomicU64::new(pack(wall_clock_ms(), 0)).into(),
        }
    }

    /// Starts the clock at `seed`, so a restart does not hand out timestamps
    /// below ones this node already published. Used when the replication log is
    /// opened and its last record read.
    pub fn seeded(seed: Hlc) -> Self {
        let clock = HybridClock::new();
        clock.observe(seed);
        clock
    }

    /// The last value handed out or observed, without advancing anything.
    #[inline]
    pub fn read(&self) -> Hlc {
        self.last.load(Ordering::Acquire)
    }

    /// Stamps an event happening now, and returns a timestamp strictly greater
    /// than every one this clock has previously returned or observed.
    pub fn now(&self) -> Hlc {
        let wall = wall_clock_ms();
        let mut current = self.last.load(Ordering::Acquire);
        loop {
            let next = if wall > physical_ms(current) {
                // The wall clock moved on; adopt it and restart the counter.
                pack(wall, 0)
            } else {
                // It did not — either because less than a millisecond has
                // passed or because the clock went backwards. Either way the
                // counter carries the ordering, and carrying into the next
                // millisecond is what keeps it from wrapping.
                current + 1
            };

            match self.last.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return next,
                Err(observed) => current = observed,
            }
        }
    }

    /// Folds a timestamp received from another node into this clock, and
    /// returns a timestamp that is above both it and anything issued locally.
    ///
    /// Calling this on every inbound frame is what makes a reply causally later
    /// than the request it answers, even when the two nodes' wall clocks
    /// disagree.
    pub fn observe(&self, remote: Hlc) -> Hlc {
        let wall = wall_clock_ms();
        let mut current = self.last.load(Ordering::Acquire);
        loop {
            let highest = current.max(remote);
            let next = if wall > physical_ms(highest) {
                pack(wall, 0)
            } else {
                highest + 1
            };

            match self.last.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return next,
                Err(observed) => current = observed,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamps_are_strictly_increasing_even_within_a_millisecond() {
        let clock = HybridClock::new();
        let mut previous = clock.now();
        // Far more than one millisecond's worth, so the counter is exercised.
        for _ in 0..10_000 {
            let next = clock.now();
            assert!(next > previous, "{} !> {}", next, previous);
            previous = next;
        }
    }

    #[test]
    fn observing_a_future_timestamp_moves_this_clock_past_it() {
        let clock = HybridClock::new();
        let remote = pack(physical_ms(clock.read()) + 60_000, 7);

        let merged = clock.observe(remote);
        assert!(
            merged > remote,
            "a reply must be later than what it answers"
        );
        assert!(clock.now() > remote, "and stay later afterwards");
    }

    #[test]
    fn observing_an_old_timestamp_does_not_move_the_clock_backwards() {
        let clock = HybridClock::new();
        let before = clock.now();

        clock.observe(1);
        assert!(clock.read() > before);
    }

    #[test]
    fn the_physical_part_survives_the_packing() {
        let clock = HybridClock::new();
        let now = clock.now();

        let wall = wall_clock_ms();
        // Within a second of the real clock: the packing must not shift it.
        assert!(
            physical_ms(now).abs_diff(wall) < 1_000,
            "{} vs {}",
            physical_ms(now),
            wall
        );
    }

    #[test]
    fn a_seeded_clock_never_reissues_what_it_was_seeded_with() {
        let seed = pack(wall_clock_ms() + 5_000, 42);
        let clock = HybridClock::seeded(seed);

        assert!(clock.now() > seed);
    }
}
