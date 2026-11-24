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
    /// Owned ranges whose writes are held for a move's copy and hand-off
    /// (B2 step 2): read as usual, stamped never (`57P03`, before effect).
    pub fenced: Vec<(u64, u64)>,
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
        let lo = range.lo >> 16;
        let hi = ((range.hi - 1) >> 16) as u64 + 1;
        if let Some(&(flo, fhi)) = o.fenced.iter().find(|&&(flo, fhi)| lo < fhi && flo < hi) {
            return Err(std::io::Error::other(format!(
                "57P03: blocks [{flo:#x}, {fhi:#x}) of shard {} are fenced for a move; retry after it",
                o.shard_id
            )));
        }
        return Ok(());
    }
    Err(std::io::Error::other(format!(
        "RDB02: shard {} does not own points {:#018x}..{:#x} ({op:?} refused at map epoch {}); the router's map is stale",
        o.shard_id, range.lo, range.hi, o.map_epoch
    )))
}

/// The file holding a shard's in-flight ownership (incoming and fenced
/// ranges) across restarts: what the map does not say.
pub const STATE_FILE: &str = "rdb_shard_ownership";

static DATA_DIR: std::sync::OnceLock<std::path::PathBuf> = std::sync::OnceLock::new();

/// Where [`STATE_FILE`] lives; set once by the shard start.
pub fn set_data_dir(dir: std::path::PathBuf) {
    let _ = DATA_DIR.set(dir);
}

/// Reads the in-flight ranges `dir` recorded: `(incoming, fenced)`.
pub fn read_state(dir: &std::path::Path) -> Result<(Vec<(u64, u64)>, Vec<(u64, u64)>), String> {
    let path = dir.join(STATE_FILE);
    let text = match std::fs::read_to_string(&path) {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok((Vec::new(), Vec::new())),
        Err(e) => return Err(format!("cannot read {}: {e}", path.display())),
    };
    let (mut incoming, mut fenced) = (Vec::new(), Vec::new());
    for line in text.lines().filter(|l| !l.trim().is_empty()) {
        let mut w = line.split_whitespace();
        let kind = w.next();
        let lo = w.next().and_then(|v| u64::from_str_radix(v.trim_start_matches("0x"), 16).ok());
        let hi = w.next().and_then(|v| u64::from_str_radix(v.trim_start_matches("0x"), 16).ok());
        match (kind, lo, hi) {
            (Some("incoming"), Some(lo), Some(hi)) => incoming.push((lo, hi)),
            (Some("fenced"), Some(lo), Some(hi)) => fenced.push((lo, hi)),
            _ => return Err(format!("{} has a malformed line {line:?}", path.display())),
        }
    }
    Ok((incoming, fenced))
}

fn write_state(o: &Ownership) -> Result<(), String> {
    use std::io::Write;
    let Some(dir) = DATA_DIR.get() else {
        return Err("internal error: the shard's data directory is not known".to_string());
    };
    let mut text = String::new();
    for (lo, hi) in &o.incoming {
        text.push_str(&format!("incoming {lo:#x} {hi:#x}\n"));
    }
    for (lo, hi) in &o.fenced {
        text.push_str(&format!("fenced {lo:#x} {hi:#x}\n"));
    }
    let path = dir.join(STATE_FILE);
    let tmp = dir.join(format!("{STATE_FILE}.tmp"));
    let io = |e: std::io::Error| format!("cannot record the shard's ownership in {}: {e}", path.display());
    let mut f = std::fs::File::create(&tmp).map_err(io)?;
    f.write_all(text.as_bytes()).map_err(io)?;
    f.sync_all().map_err(io)?;
    drop(f);
    std::fs::rename(&tmp, &path).map_err(io)?;
    crate::core::sync_path::sync_dir(dir).map_err(io)?;
    Ok(())
}

/// Replaces the ownership with `edit` of it, durably first.
fn update(edit: impl FnOnce(&mut Ownership)) -> Result<Ownership, String> {
    let Some(cur) = current() else {
        return Err("this node is not a shard (--role shard)".to_string());
    };
    let mut next = (*cur).clone();
    edit(&mut next);
    for list in [&mut next.incoming, &mut next.fenced] {
        list.sort_unstable();
        list.dedup();
    }
    write_state(&next)?;
    install(next.clone());
    Ok(next)
}

/// Removes `[lo, hi)` from every range of `list`, splitting where needed.
fn subtract(list: &mut Vec<(u64, u64)>, lo: u64, hi: u64) {
    let mut out = Vec::with_capacity(list.len() + 1);
    for &(a, b) in list.iter() {
        if b <= lo || a >= hi {
            out.push((a, b));
            continue;
        }
        if a < lo {
            out.push((a, lo));
        }
        if b > hi {
            out.push((hi, b));
        }
    }
    *list = out;
}

/// Fences owned blocks `[lo, hi)` for a move: their writes are refused.
pub fn fence(lo: u64, hi: u64) -> Result<Ownership, String> {
    update(|o| o.fenced.push((lo, hi)))
}

pub fn unfence(lo: u64, hi: u64) -> Result<Ownership, String> {
    update(|o| subtract(&mut o.fenced, lo, hi))
}

/// Marks `[lo, hi)` as coming in by this shard's own move: it may stamp
/// rows there before the map gives it the range.
pub fn add_incoming(lo: u64, hi: u64) -> Result<Ownership, String> {
    update(|o| o.incoming.push((lo, hi)))
}

/// Takes `[lo, hi)` out of this shard's ownership altogether (a move handed
/// it off): neither owned, incoming nor fenced.
pub fn give_up(lo: u64, hi: u64) -> Result<Ownership, String> {
    update(|o| {
        subtract(&mut o.owned, lo, hi);
        subtract(&mut o.incoming, lo, hi);
        subtract(&mut o.fenced, lo, hi);
    })
}

/// A newer map's ranges become what this shard owns; what they now cover
/// stops being incoming.
pub fn adopt_map(owned: Vec<(u64, u64)>, map_epoch: u64) -> Result<Ownership, String> {
    update(|o| {
        for &(lo, hi) in &owned {
            subtract(&mut o.incoming, lo, hi);
        }
        o.owned = owned;
        o.map_epoch = map_epoch;
    })
}
