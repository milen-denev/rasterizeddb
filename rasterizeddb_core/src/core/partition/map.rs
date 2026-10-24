//! The range map: which partition owns each routing point (§1.1, §1.2).

use std::collections::HashSet;

/// The routing space is `[0, 2⁶⁴)`; `hi` is a `u128` so the last range can
/// end there.
pub const SPACE_END: u128 = 1u128 << 64;

/// One tenant's block, 2¹⁶ points (§1.2).
pub const BLOCK: u64 = 1 << 16;

/// The most partitions [`RangeMap::initial`] lays out (§15, phase 3).
pub const MAX_INITIAL: u32 = 65_536;

/// A half-open `[lo, hi)` of routing points. `hi` may be [`SPACE_END`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PointRange {
    pub lo: u64,
    pub hi: u128,
}

impl PointRange {
    pub const FULL: PointRange = PointRange { lo: 0, hi: SPACE_END };

    /// The range holding only `p`.
    pub fn point(p: u64) -> Self {
        PointRange { lo: p, hi: p as u128 + 1 }
    }

    pub fn is_empty(&self) -> bool {
        self.lo as u128 >= self.hi
    }

    pub fn contains(&self, p: u64) -> bool {
        p >= self.lo && (p as u128) < self.hi
    }
}

/// Sorted `(lo, pid)` entries covering `[0, 2⁶⁴)`: entry `j` owns
/// `[lo_j, lo_{j+1})`, the last one up to 2⁶⁴. Constructed only through
/// [`RangeMap::new`] or [`RangeMap::initial`], so every instance has passed
/// [`RangeMap::validate`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RangeMap {
    entries: Vec<(u64, u32)>,
}

impl RangeMap {
    /// A map from persisted or reshaped entries; refused unless total and
    /// disjoint.
    pub fn new(entries: Vec<(u64, u32)>) -> Result<Self, String> {
        let map = RangeMap { entries };
        map.validate()?;
        Ok(map)
    }

    /// `n` partitions with pids `0..n` and `lo_i = ⌊i · 2⁴⁸ / n⌋ << 16`, so
    /// every boundary falls between tenant blocks.
    pub fn initial(n: u32) -> Result<Self, String> {
        if n == 0 || n > MAX_INITIAL {
            return Err(format!("partition count must be 1 to {MAX_INITIAL}, not {n}"));
        }
        let entries = (0..n)
            .map(|i| {
                let block = (i as u128 * (1u128 << 48)) / n as u128;
                ((block as u64) << 16, i)
            })
            .collect();
        Self::new(entries)
    }

    /// Checks the map is total and disjoint: it starts at 0, `lo` strictly
    /// increases, and no pid owns two ranges.
    pub fn validate(&self) -> Result<(), String> {
        let Some(&(first, _)) = self.entries.first() else {
            return Err("partition map is empty".to_string());
        };
        if first != 0 {
            return Err(format!("partition map starts at {first:#018x}, not 0"));
        }
        for w in self.entries.windows(2) {
            if w[1].0 <= w[0].0 {
                return Err(format!(
                    "partition map is not strictly increasing: {:#018x} (pid {}) then {:#018x} (pid {})",
                    w[0].0, w[0].1, w[1].0, w[1].1
                ));
            }
        }
        let mut seen = HashSet::with_capacity(self.entries.len());
        for &(lo, pid) in &self.entries {
            if !seen.insert(pid) {
                return Err(format!("partition map names pid {pid} twice (again at {lo:#018x})"));
            }
        }
        Ok(())
    }

    pub fn entries(&self) -> &[(u64, u32)] {
        &self.entries
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Always false; present for the `len` convention.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Index of the entry owning `p`.
    fn index_of(&self, p: u64) -> usize {
        // Entry 0 has lo == 0, so at least one entry is <= p.
        self.entries.partition_point(|&(lo, _)| lo <= p) - 1
    }

    /// The range entry `j` owns.
    fn range_at(&self, j: usize) -> PointRange {
        let hi = self.entries.get(j + 1).map_or(SPACE_END, |&(lo, _)| lo as u128);
        PointRange { lo: self.entries[j].0, hi }
    }

    /// The partition owning point `p`.
    pub fn pid_of(&self, p: u64) -> u32 {
        self.entries[self.index_of(p)].1
    }

    /// The range `pid` owns, or `None` when the map has no such pid.
    pub fn range_of(&self, pid: u32) -> Option<PointRange> {
        self.entries.iter().position(|&(_, p)| p == pid).map(|j| self.range_at(j))
    }

    /// The one partition whose range holds all of `[lo, hi)`, or `None` when
    /// the interval meets several (or is empty): [`Self::pids_overlapping`]
    /// for the common one-partition case, without a `Vec`.
    pub fn sole_pid(&self, lo: u64, hi: u128) -> Option<u32> {
        if lo as u128 >= hi {
            return None;
        }
        let j = self.index_of(lo);
        (self.range_at(j).hi >= hi).then(|| self.entries[j].1)
    }

    /// Every partition whose range meets `[lo, hi)`, sorted by pid. Empty only
    /// for an empty interval.
    pub fn pids_overlapping(&self, lo: u64, hi: u128) -> Vec<u32> {
        if lo as u128 >= hi {
            return Vec::new();
        }
        let first = self.index_of(lo);
        // Entries starting before `hi`; `hi` may be past every lo.
        let end = self.entries.partition_point(|&(l, _)| (l as u128) < hi);
        let mut pids: Vec<u32> = self.entries[first..end].iter().map(|&(_, p)| p).collect();
        pids.sort_unstable();
        pids.dedup();
        pids
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initial_boundaries_fall_between_tenant_blocks() {
        for n in [1u32, 2, 3, 7, 10, 1000, 1023, 1024, 4097, 65_535, 65_536] {
            let map = RangeMap::initial(n).unwrap();
            assert_eq!(map.len(), n as usize);
            for &(lo, _) in map.entries() {
                assert_eq!(lo % BLOCK, 0, "n = {n}: boundary {lo:#x} splits a block");
            }
            // The formula, checked against an independent computation.
            for &(lo, pid) in map.entries().iter().step_by(1 + n as usize / 50) {
                let want = ((pid as u128 * (1u128 << 64)) / n as u128) as u64 & !(BLOCK - 1);
                assert_eq!(lo, want, "n = {n}, pid {pid}");
            }
        }
        assert!(RangeMap::initial(0).is_err());
        assert!(RangeMap::initial(65_537).is_err());
    }

    #[test]
    fn a_tenant_block_never_straddles_an_initial_boundary() {
        // Blocks at the space's two ends, next to boundaries, and spread
        // pseudo-randomly between.
        let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
        for n in [1u32, 3, 7, 1024, 65_536] {
            let map = RangeMap::initial(n).unwrap();
            let mut blocks: Vec<u64> = vec![0, (1 << 48) - 1];
            for &(lo, _) in map.entries().iter().take(64) {
                let b = lo >> 16;
                blocks.extend([b, b.saturating_sub(1), (b + 1).min((1 << 48) - 1)]);
            }
            for _ in 0..2000 {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                blocks.push(x >> 16);
            }
            for b in blocks {
                let lo = b << 16;
                let hi = lo as u128 + BLOCK as u128;
                let pids = map.pids_overlapping(lo, hi);
                assert_eq!(pids.len(), 1, "n = {n}: block {b:#x} spans {pids:?}");
                assert_eq!(pids[0], map.pid_of(lo));
                assert_eq!(pids[0], map.pid_of(lo + (BLOCK - 1)));
            }
        }
    }

    #[test]
    fn validate_refuses_gaps_overlaps_and_reused_pids() {
        assert!(RangeMap::new(vec![]).is_err());
        assert!(RangeMap::new(vec![(1, 0)]).is_err(), "must start at 0");
        assert!(RangeMap::new(vec![(0, 0), (0, 1)]).is_err(), "duplicate lo");
        assert!(RangeMap::new(vec![(0, 0), (10, 1), (5, 2)]).is_err(), "unsorted");
        assert!(RangeMap::new(vec![(0, 0), (10, 0)]).is_err(), "pid twice");
        assert!(RangeMap::new(vec![(0, 7), (10, 3), (u64::MAX, 9)]).is_ok());
    }

    #[test]
    fn lookups_respect_half_open_bounds() {
        let map = RangeMap::new(vec![(0, 5), (100, 2), (200, 9), (u64::MAX, 1)]).unwrap();
        assert_eq!(map.pid_of(0), 5);
        assert_eq!(map.pid_of(99), 5);
        assert_eq!(map.pid_of(100), 2);
        assert_eq!(map.pid_of(u64::MAX - 1), 9);
        assert_eq!(map.pid_of(u64::MAX), 1);

        assert_eq!(map.pids_overlapping(0, 100), vec![5]);
        assert_eq!(map.pids_overlapping(0, 101), vec![2, 5]);
        assert_eq!(map.pids_overlapping(99, 201), vec![2, 5, 9]);
        assert_eq!(map.pids_overlapping(150, 150), Vec::<u32>::new());
        assert_eq!(map.pids_overlapping(200, SPACE_END), vec![1, 9]);
        assert_eq!(map.pids_overlapping(u64::MAX, SPACE_END), vec![1]);
        assert_eq!(map.pids_overlapping(0, SPACE_END), vec![1, 2, 5, 9]);

        assert_eq!(map.range_of(2), Some(PointRange { lo: 100, hi: 200 }));
        assert_eq!(map.range_of(1), Some(PointRange { lo: u64::MAX, hi: SPACE_END }));
        assert_eq!(map.range_of(4), None);

        // `contains` is the range guard's primitive: `hi` is the next owner's.
        let r = map.range_of(2).unwrap();
        assert!(r.contains(100) && r.contains(199) && !r.contains(200) && !r.contains(99));
        assert!(!PointRange::point(5).contains(6) && PointRange::point(5).contains(5));
        assert!(map.range_of(1).unwrap().contains(u64::MAX));
        assert!(PointRange { lo: 7, hi: 7 }.is_empty() && !PointRange { lo: 7, hi: 7 }.contains(7));
        assert!(!PointRange::point(u64::MAX).is_empty());
    }

    #[test]
    fn every_point_belongs_to_exactly_the_range_its_pid_owns() {
        let map = RangeMap::initial(7).unwrap();
        let mut x: u64 = 1;
        for _ in 0..5000 {
            x = x.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1_442_695_040_888_963_407);
            let pid = map.pid_of(x);
            assert!(map.range_of(pid).unwrap().contains(x));
            assert_eq!(map.pids_overlapping(x, x as u128 + 1), vec![pid]);
        }
    }
}
