//! Auto-balancing between shards: the planner (sharding-plan.md §4.6b).
//!
//! Pure: it reads a shard map and each range's measured load, and proposes
//! moves and splits. Applying a move is [`ShardMap::reassign`] plus the data
//! copy, which the caller (the `rdb-shard` tool, or the authority) does.
//!
//! - A shard's load is `w_bytes × bytes/Σbytes + w_ops × ops/Σops`.
//! - A round starts only when some shard is more than `tolerance` from the
//!   mean, and stops once every shard is within `tolerance / 2`
//!   (hysteresis, so ranges do not flap between rounds).
//! - Greedy: from the heaviest shard to the lightest, the movable range that
//!   best closes their gap without overshooting it.
//! - A range pinned (by policy or operator) never moves, and nothing moves
//!   onto a shard that is not accepting.
//! - A range too big to move without overshooting, but wider than one block,
//!   is proposed for a split at its middle block instead.

use crate::shard_map::{BLOCKS, ShardMap};

/// One range's measured load on its shard.
#[derive(Clone, Debug, PartialEq)]
pub struct RangeLoad {
    /// Level-0 blocks `[lo, hi)`.
    pub lo: u64,
    pub hi: u64,
    /// The shard holding it (must agree with the map).
    pub shard: u32,
    /// Stored bytes, all sharded tables summed.
    pub bytes: u64,
    /// Statements that touched it since the last report.
    pub ops: u64,
}

/// The balancer's knobs.
#[derive(Clone, Debug, PartialEq)]
pub struct BalanceConfig {
    pub w_bytes: f64,
    pub w_ops: f64,
    /// How far from the mean a shard may be before a round starts (0.10).
    pub tolerance: f64,
    /// Moves per round, at most.
    pub max_moves: usize,
    /// Bytes moved per round, at most.
    pub max_bytes: u64,
    /// Block ranges that never move.
    pub pinned: Vec<(u64, u64)>,
    /// Shards that may not receive ranges (draining, full, or excluded by a
    /// policy).
    pub not_accepting: Vec<u32>,
}

impl Default for BalanceConfig {
    fn default() -> Self {
        BalanceConfig {
            w_bytes: 0.5,
            w_ops: 0.5,
            tolerance: 0.10,
            max_moves: 4,
            max_bytes: 64 << 30,
            pinned: Vec::new(),
            not_accepting: Vec::new(),
        }
    }
}

/// A proposed move of blocks `[lo, hi)` from one shard to another.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Move {
    pub lo: u64,
    pub hi: u64,
    pub from: u32,
    pub to: u32,
    pub bytes: u64,
}

/// What one round proposes.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Plan {
    pub moves: Vec<Move>,
    /// Ranges to split at the given block before a later round can move part
    /// of them: `(lo, hi, at)`.
    pub splits: Vec<(u64, u64, u64)>,
    /// Each shard's normalised load before and after the moves, by shard id.
    pub before: Vec<(u32, f64)>,
    pub after: Vec<(u32, f64)>,
    /// True when no move was needed (every shard within tolerance).
    pub balanced: bool,
}

/// Plans one round (module docs). `loads` may omit ranges with no load; a
/// range the map gives another shard than `loads` says is refused, since the
/// report is then stale.
pub fn plan(map: &ShardMap, loads: &[RangeLoad], cfg: &BalanceConfig) -> Result<Plan, String> {
    for l in loads {
        if l.lo >= l.hi || l.hi > BLOCKS {
            return Err(format!("load range [{:#x}, {:#x}) is empty or past the end", l.lo, l.hi));
        }
        if map.shard_of_block(l.lo) != l.shard || map.shard_of_block(l.hi - 1) != l.shard {
            return Err(format!(
                "load range [{:#x}, {:#x}) is reported by shard {} but the map gives it elsewhere; the report is stale",
                l.lo, l.hi, l.shard
            ));
        }
    }
    let total_bytes: u64 = loads.iter().map(|l| l.bytes).sum::<u64>().max(1);
    let total_ops: u64 = loads.iter().map(|l| l.ops).sum::<u64>().max(1);
    let weight = |l: &RangeLoad| cfg.w_bytes * l.bytes as f64 / total_bytes as f64 + cfg.w_ops * l.ops as f64 / total_ops as f64;

    let ids: Vec<u32> = map.shards().iter().map(|s| s.id).collect();
    let mut load: Vec<(u32, f64)> = ids.iter().map(|&id| (id, 0.0)).collect();
    for l in loads {
        if let Some(e) = load.iter_mut().find(|(id, _)| *id == l.shard) {
            e.1 += weight(l);
        }
    }
    let before = load.clone();
    let n = ids.len() as f64;
    let mean = load.iter().map(|(_, w)| w).sum::<f64>() / n;
    let spread = |load: &[(u32, f64)]| load.iter().map(|(_, w)| (w - mean).abs()).fold(0.0, f64::max);
    let mut out = Plan { before, ..Plan::default() };
    if mean <= 0.0 || spread(&load) <= cfg.tolerance * mean {
        out.balanced = true;
        out.after = load;
        return Ok(out);
    }
    let pinned = |l: &RangeLoad| cfg.pinned.iter().any(|&(lo, hi)| l.lo < hi && lo < l.hi);
    let mut movable: Vec<RangeLoad> = loads.iter().filter(|l| !pinned(l)).cloned().collect();
    let mut moved_bytes = 0u64;
    while out.moves.len() < cfg.max_moves && spread(&load) > cfg.tolerance * mean / 2.0 {
        let (heavy, heavy_w) = load.iter().copied().fold((u32::MAX, f64::MIN), |a, b| if b.1 > a.1 { b } else { a });
        let light = load
            .iter()
            .copied()
            .filter(|(id, _)| !cfg.not_accepting.contains(id) && *id != heavy)
            .fold(None, |a: Option<(u32, f64)>, b| match a {
                Some(a) if a.1 <= b.1 => Some(a),
                _ => Some(b),
            });
        let Some((light, light_w)) = light else { break };
        // Moving w closes the gap best at w = gap / 2.
        let half_gap = (heavy_w - light_w) / 2.0;
        let candidates = movable.iter().enumerate().filter(|(_, l)| {
            l.shard == heavy && moved_bytes.saturating_add(l.bytes) <= cfg.max_bytes && weight(l) > 0.0
        });
        let best = candidates
            .clone()
            .filter(|(_, l)| weight(l) <= half_gap * 2.0 * 0.95)
            .min_by(|a, b| {
                (weight(a.1) - half_gap).abs().partial_cmp(&(weight(b.1) - half_gap).abs()).unwrap()
            })
            .map(|(i, _)| i);
        let Some(i) = best else {
            // Nothing fits: the heaviest range of this shard, if wider than
            // a block, is split for a later round.
            if let Some((_, l)) = candidates.max_by(|a, b| weight(a.1).partial_cmp(&weight(b.1)).unwrap())
                && l.hi - l.lo > 1
                && !out.splits.iter().any(|s| s.0 == l.lo)
            {
                out.splits.push((l.lo, l.hi, l.lo + (l.hi - l.lo) / 2));
            }
            break;
        };
        let l = movable.remove(i);
        let w = weight(&l);
        for e in load.iter_mut() {
            if e.0 == heavy {
                e.1 -= w;
            } else if e.0 == light {
                e.1 += w;
            }
        }
        moved_bytes += l.bytes;
        out.moves.push(Move { lo: l.lo, hi: l.hi, from: heavy, to: light, bytes: l.bytes });
    }
    out.after = load;
    Ok(out)
}

/// The map after `plan`'s moves, one epoch per move.
pub fn apply(map: &ShardMap, plan: &Plan) -> Result<ShardMap, String> {
    let mut m = map.clone();
    for mv in &plan.moves {
        m = m.reassign(mv.lo, mv.hi, mv.to)?;
    }
    Ok(m)
}

/// The shard a new per-user-shard user is placed on: the least-loaded one
/// that is accepting, by `loads` (shard id, load); ties go to the lowest id.
pub fn place(loads: &[(u32, f64)], not_accepting: &[u32]) -> Option<u32> {
    loads
        .iter()
        .filter(|(id, _)| !not_accepting.contains(id))
        .min_by(|a, b| a.1.partial_cmp(&b.1).unwrap().then(a.0.cmp(&b.0)))
        .map(|(id, _)| *id)
}

/// One partition's report (`SHOW SHARD LOAD` on `shard`): its blocks and
/// size.
#[derive(Clone, Debug, PartialEq)]
pub struct PartitionReport {
    pub shard: u32,
    pub lo: u64,
    pub hi: u64,
    pub bytes: u64,
    pub ops: u64,
}

/// The movable ranges of a cluster: each shard's map ranges cut at every
/// partition boundary any table reported, so a move carries whole
/// partitions of every table. Each segment's load is the share of every
/// partition overlapping it, in proportion to the blocks they share.
/// Reports a shard made for blocks the map gives elsewhere are left out
/// (stale).
pub fn segments(map: &ShardMap, reports: &[PartitionReport]) -> Vec<RangeLoad> {
    let mut out = Vec::new();
    for s in map.shards() {
        let mine: Vec<&PartitionReport> = reports
            .iter()
            .filter(|r| r.shard == s.id && r.lo < r.hi && r.hi <= BLOCKS)
            .collect();
        for (lo, hi) in map.ranges_of(s.id) {
            let mut cuts: Vec<u64> = vec![lo, hi];
            for r in &mine {
                for c in [r.lo, r.hi] {
                    if c > lo && c < hi {
                        cuts.push(c);
                    }
                }
            }
            cuts.sort_unstable();
            cuts.dedup();
            for w in cuts.windows(2) {
                let (a, b) = (w[0], w[1]);
                let mut bytes = 0f64;
                let mut ops = 0f64;
                for r in &mine {
                    let shared = r.hi.min(b).saturating_sub(r.lo.max(a));
                    if shared > 0 {
                        let frac = shared as f64 / (r.hi - r.lo) as f64;
                        bytes += r.bytes as f64 * frac;
                        ops += r.ops as f64 * frac;
                    }
                }
                out.push(RangeLoad { lo: a, hi: b, shard: s.id, bytes: bytes.round() as u64, ops: ops.round() as u64 });
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shard_map::ShardInfo;

    fn map(n: u32) -> ShardMap {
        let shards = (0..n).map(|id| ShardInfo { id, addr: String::new(), standbys: vec![] }).collect();
        ShardMap::initial(1, shards).unwrap()
    }

    #[test]
    fn a_balanced_cluster_moves_nothing() {
        let m = map(2);
        let loads: Vec<RangeLoad> = m
            .shards()
            .iter()
            .flat_map(|s| m.ranges_of(s.id).into_iter().map(move |(lo, hi)| (s.id, lo, hi)))
            .map(|(shard, lo, hi)| RangeLoad { lo, hi, shard, bytes: 100, ops: 0 })
            .collect();
        let p = plan(&m, &loads, &BalanceConfig::default()).unwrap();
        assert!(p.balanced && p.moves.is_empty());
    }

    #[test]
    fn a_skewed_cluster_moves_toward_the_mean_and_respects_pins() {
        let m = map(2);
        let half = BLOCKS / 2;
        // Shard 0 holds eight equal ranges, shard 1 one small one.
        let mut reports: Vec<PartitionReport> = (0..8)
            .map(|i| PartitionReport { shard: 0, lo: i * (half / 8), hi: (i + 1) * (half / 8), bytes: 100, ops: 0 })
            .collect();
        reports.push(PartitionReport { shard: 1, lo: half, hi: BLOCKS, bytes: 10, ops: 0 });
        let segs = segments(&m, &reports);
        assert_eq!(segs.len(), 9);
        let p = plan(&m, &segs, &BalanceConfig::default()).unwrap();
        assert!(!p.moves.is_empty());
        assert!(p.moves.iter().all(|mv| mv.from == 0 && mv.to == 1));
        let spread = |l: &[(u32, f64)]| (l[0].1 - l[1].1).abs();
        assert!(spread(&p.after) < spread(&p.before));
        let applied = apply(&m, &p).unwrap();
        assert_eq!(applied.epoch(), m.epoch() + p.moves.len() as u64);
        // Pinning everything on shard 0 moves nothing.
        let cfg = BalanceConfig { pinned: vec![(0, half)], ..BalanceConfig::default() };
        assert!(plan(&m, &segs, &cfg).unwrap().moves.is_empty());
        // A stale report is refused.
        let stale = [RangeLoad { lo: half, hi: half + 1, shard: 0, bytes: 1, ops: 0 }];
        assert!(plan(&m, &stale, &BalanceConfig::default()).is_err());
    }

    #[test]
    fn one_huge_range_is_proposed_for_a_split() {
        let m = map(2);
        let half = BLOCKS / 2;
        let loads = [
            RangeLoad { lo: 0, hi: half, shard: 0, bytes: 1000, ops: 0 },
            RangeLoad { lo: half, hi: BLOCKS, shard: 1, bytes: 1, ops: 0 },
        ];
        let p = plan(&m, &loads, &BalanceConfig::default()).unwrap();
        assert!(p.moves.is_empty());
        assert_eq!(p.splits, vec![(0, half, half / 2)]);
    }

    #[test]
    fn users_go_to_the_least_loaded_accepting_shard() {
        assert_eq!(place(&[(0, 0.5), (1, 0.2), (2, 0.2)], &[]), Some(1));
        assert_eq!(place(&[(0, 0.5), (1, 0.2)], &[1]), Some(0));
        assert_eq!(place(&[], &[]), None);
    }
}
