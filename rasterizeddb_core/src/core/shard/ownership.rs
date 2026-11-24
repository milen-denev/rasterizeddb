//! Which level-0 block ranges this node owns (sharding-plan.md B1a.4).
//!
//! Installed once a node starts as a shard; until then every check passes
//! with one relaxed load, so a server built with `sharding` but run as a
//! plain database behaves exactly as one built without it.

use std::sync::atomic::{AtomicBool, Ordering};

use arc_swap::ArcSwapOption;

use crate::core::partition::map::PointRange;
use crate::core::shard_hooks::StampOp;

/// The ranges this node may stamp rows in: what it owns and what its own
/// moves are bringing in. Block ranges `[lo, hi)` over level-0 blocks, sorted
/// and disjoint.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Ownership {
    pub shard_id: u32,
    pub map_epoch: u64,
    pub owned: Vec<(u64, u64)>,
    pub incoming: Vec<(u64, u64)>,
}

impl Ownership {
    /// True when every level-0 block `range` reaches is owned or incoming.
    pub fn may_stamp(&self, range: PointRange) -> bool {
        if range.is_empty() {
            return true;
        }
        let lo = range.lo >> 16;
        // The last block the range reaches: hi is exclusive, in points.
        let hi = ((range.hi - 1) >> 16) as u64 + 1;
        covers(&self.owned, &self.incoming, lo, hi)
    }
}

/// True when `[lo, hi)` lies within the union of `a` and `b`.
fn covers(a: &[(u64, u64)], b: &[(u64, u64)], mut lo: u64, hi: u64) -> bool {
    while lo < hi {
        let next = a
            .iter()
            .chain(b)
            .filter(|&&(l, h)| l <= lo && lo < h)
            .map(|&(_, h)| h)
            .max();
        match next {
            Some(h) => lo = h,
            None => return false,
        }
    }
    true
}

static ACTIVE: AtomicBool = AtomicBool::new(false);
static OWNERSHIP: ArcSwapOption<Ownership> = ArcSwapOption::const_empty();

/// Installs (or replaces) this node's ownership; from now on every stamp is
/// checked against it.
pub fn install(o: Ownership) {
    OWNERSHIP.store(Some(std::sync::Arc::new(o)));
    ACTIVE.store(true, Ordering::Release);
}

/// The ownership installed, if this node is a shard.
pub fn current() -> Option<std::sync::Arc<Ownership>> {
    if !ACTIVE.load(Ordering::Relaxed) {
        return None;
    }
    OWNERSHIP.load_full()
}

/// [`crate::core::shard_hooks::check_stamp`] with the feature built in.
#[inline]
pub fn check_stamp(range: Option<PointRange>, op: StampOp) -> std::io::Result<()> {
    if !ACTIVE.load(Ordering::Relaxed) {
        return Ok(());
    }
    check_slow(range, op)
}

#[cold]
fn check_slow(range: Option<PointRange>, op: StampOp) -> std::io::Result<()> {
    let Some(o) = OWNERSHIP.load_full() else {
        return Ok(());
    };
    // A plain table is a global one: its rows live on every shard's copy and
    // are written by the home shard's statements (B1a.8); B1a decides who may
    // write it, so the hook passes it here.
    let Some(range) = range else {
        return Ok(());
    };
    if o.may_stamp(range) {
        return Ok(());
    }
    Err(std::io::Error::other(format!(
        "RDB02: shard {} does not own points {:#018x}..{:#x} ({op:?} refused at map epoch {}); the router's map is stale",
        o.shard_id, range.lo, range.hi, o.map_epoch
    )))
}
