use std::sync::OnceLock;

use crate::core::{row::row_pointer::ROW_POINTER_RECORD_LEN, sme_v2::rules::{RangeVec, RowRange}};

/// Intersect many `RangeVec`/range-slices by folding pairwise intersections.
///
/// - If `inputs` is empty, returns empty.
/// - If any input is empty, returns empty.
pub fn intersect_many<'a>(inputs: impl IntoIterator<Item = &'a [RowRange]>) -> RangeVec {
    let mut it = inputs.into_iter();
    let Some(first) = it.next() else {
        return RangeVec::new();
    };

    let mut acc = normalize_ranges(first);
    for next in it {
        if acc.is_empty() {
            return acc;
        }

        // Normalize each input once (no repeated normalization of `acc`).
        let next = normalize_ranges(next);
        acc = intersect_two_normalized(&acc, &next);
    }

    acc
}

#[inline]
fn end_inclusive(r: &RowRange) -> u64 {
    r.end_pointer_pos_inclusive()
}

#[inline]
fn is_unbounded_end(end: u64) -> bool {
    end == u64::MAX
}

#[inline]
fn force_scalar_impl() -> bool {
    static FORCE: OnceLock<bool> = OnceLock::new();
    *FORCE.get_or_init(|| std::env::var_os("RANGE_CALC_FORCE_SCALAR").is_some())
}

/// Returns true if the two inclusive pointer ranges overlap (or touch adjacently).
#[inline]
fn overlaps_or_adjacent(a_end: u64, b_start: u64) -> bool {
    if is_unbounded_end(a_end) {
        return true;
    }

    let rec = ROW_POINTER_RECORD_LEN as u64;
    // Adjacent means: b_start == a_end + rec
    let a_end_plus_one_rec = a_end.saturating_add(rec);
    b_start <= a_end_plus_one_rec
}

#[inline]
fn is_sorted_by_start_nonempty(ranges: &[RowRange]) -> bool {
    let mut prev_start = 0u64;
    for (idx, r) in ranges.iter().enumerate() {
        if r.is_empty() {
            return false;
        }
        if idx > 0 && r.start_pointer_pos < prev_start {
            return false;
        }
        prev_start = r.start_pointer_pos;
    }
    true
}

/// Normalize a list of ranges:
/// - drop empty
/// - sort by `start_pointer_pos`
/// - merge overlaps / adjacencies
fn normalize_ranges(ranges: &[RowRange]) -> RangeVec {
    // Fast-path: if input is already sorted, normalize via a single linear merge (no sort).
    // This is common in real code paths and in benchmarks.
    if !ranges.is_empty() && is_sorted_by_start_nonempty(ranges) {
        let mut out: RangeVec = RangeVec::new();
        out.reserve(ranges.len());

        for &r in ranges {
            if out.is_empty() {
                out.push(r);
                continue;
            }

            let last = out.last_mut().expect("non-empty");
            let last_start = last.start_pointer_pos;
            let last_end = end_inclusive(last);

            if overlaps_or_adjacent(last_end, r.start_pointer_pos) {
                let r_end = end_inclusive(&r);
                let merged_end = if is_unbounded_end(last_end) || is_unbounded_end(r_end) {
                    u64::MAX
                } else {
                    last_end.max(r_end)
                };
                *last = RowRange::from_pointer_bounds_inclusive(last_start, merged_end);
            } else {
                out.push(r);
            }
        }

        return out;
    }

    let mut v: Vec<RowRange> = ranges.iter().copied().filter(|r| !r.is_empty()).collect();
    if v.is_empty() {
        return RangeVec::new();
    }

    v.sort_unstable_by_key(|r| r.start_pointer_pos);

    let mut out: RangeVec = RangeVec::new();

    for r in v {
        if out.is_empty() {
            out.push(r);
            continue;
        }

        let last = out.last_mut().expect("non-empty");
        let last_start = last.start_pointer_pos;
        let last_end = end_inclusive(last);

        if overlaps_or_adjacent(last_end, r.start_pointer_pos) {
            // Merge by extending end.
            let r_end = end_inclusive(&r);
            let merged_end = if is_unbounded_end(last_end) || is_unbounded_end(r_end) {
                u64::MAX
            } else {
                last_end.max(r_end)
            };

            *last = RowRange::from_pointer_bounds_inclusive(last_start, merged_end);
        } else {
            out.push(r);
        }
    }

    out
}

/// Intersect two *already-normalized* range lists.
///
/// Assumes `a` and `b` are sorted by `start_pointer_pos` and internally non-overlapping.
#[inline]
fn intersect_two_normalized_scalar(a: &[RowRange], b: &[RowRange]) -> RangeVec {
    if a.is_empty() || b.is_empty() {
        return RangeVec::new();
    }

    let mut out: RangeVec = RangeVec::new();
    out.reserve(a.len().min(b.len()));

    let mut i = 0usize;
    let mut j = 0usize;

    while i < a.len() && j < b.len() {
        let ra = a[i];
        let rb = b[j];

        let start = ra.start_pointer_pos.max(rb.start_pointer_pos);
        let end_a = end_inclusive(&ra);
        let end_b = end_inclusive(&rb);

        let end = if is_unbounded_end(end_a) {
            end_b
        } else if is_unbounded_end(end_b) {
            end_a
        } else {
            end_a.min(end_b)
        };

        if start <= end {
            out.push(RowRange::from_pointer_bounds_inclusive(start, end));
        }

        // Advance the range that ends first.
        let a_ends_first = match (is_unbounded_end(end_a), is_unbounded_end(end_b)) {
            (true, true) => false,
            (true, false) => false,
            (false, true) => true,
            (false, false) => end_a < end_b,
        };

        if a_ends_first {
            i += 1;
        } else {
            j += 1;
        }
    }

    out
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
mod simd_x86 {
    use super::*;
    
    #[cfg(target_arch = "x86")]
    use std::arch::x86 as arch;
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64 as arch;
    use std::cell::RefCell;

    thread_local! {
        static B_ENDS_SCRATCH: RefCell<Vec<u64>> = RefCell::new(Vec::new());
    }

    #[inline]
    fn cmp_u64_lt_mask_avx2(ends: arch::__m256i, threshold: u64) -> u32 {
        unsafe {
            // Unsigned compare: ends < threshold
            // Convert to signed ordering by flipping the sign bit.
            let sign = arch::_mm256_set1_epi64x(i64::MIN);
            let ends_x = arch::_mm256_xor_si256(ends, sign);
            let thr_x = (threshold ^ 0x8000_0000_0000_0000) as i64;
            let thr = arch::_mm256_set1_epi64x(thr_x);

            // cmpgt(thr, ends_x) => thr > ends_x => ends_x < thr
            let cmp = arch::_mm256_cmpgt_epi64(thr, ends_x);
            arch::_mm256_movemask_pd(arch::_mm256_castsi256_pd(cmp)) as u32
        }
    }

    /// Advance `j` while `ends[j] < threshold` using AVX2 (4x u64 at a time).
    #[target_feature(enable = "avx2")]
    pub unsafe fn advance_while_end_lt_avx2(ends: &[u64], mut j: usize, threshold: u64) -> usize {
        while j + 4 <= ends.len() {
            let ptr = unsafe { ends.as_ptr().add(j) as *const arch::__m256i };
            let v = unsafe { arch::_mm256_loadu_si256(ptr) };
            let mask = cmp_u64_lt_mask_avx2(v, threshold) & 0b1111;

            if mask == 0b1111 {
                j += 4;
                continue;
            }

            // Find first lane where ends[lane] >= threshold (bit == 0).
            let not_mask = (!mask) & 0b1111;
            let lane = not_mask.trailing_zeros() as usize;
            j += lane;
            break;
        }

        while j < ends.len() && ends[j] < threshold {
            j += 1;
        }

        j
    }

    #[target_feature(enable = "avx2")]
    pub unsafe fn intersect_two_normalized_avx2(a: &[RowRange], b: &[RowRange]) -> RangeVec {
        if a.is_empty() || b.is_empty() {
            return RangeVec::new();
        }

        // Heuristic: if `b` is small, AVX2 skipping overhead isn't worth it.
        if b.len() < 64 {
            return intersect_two_normalized_scalar(a, b);
        }

        B_ENDS_SCRATCH.with(|cell| {
            let mut b_ends = cell.borrow_mut();
            b_ends.clear();
            b_ends.reserve(b.len());
            for r in b.iter() {
                b_ends.push(end_inclusive(r));
            }

            let mut out: RangeVec = RangeVec::new();

            let mut i = 0usize;
            let mut j = 0usize;
            while i < a.len() && j < b.len() {
                let ra = a[i];
                let a_start = ra.start_pointer_pos;
                let a_end = end_inclusive(&ra);

                // Skip b ranges that end before a_start.
                j = unsafe { advance_while_end_lt_avx2(&b_ends, j, a_start) };
                if j >= b.len() {
                    break;
                }

                let rb = b[j];
                let b_start = rb.start_pointer_pos;
                let b_end = b_ends[j];

                let start = a_start.max(b_start);
                let end = if is_unbounded_end(a_end) {
                    b_end
                } else if is_unbounded_end(b_end) {
                    a_end
                } else {
                    a_end.min(b_end)
                };

                if start <= end {
                    out.push(RowRange::from_pointer_bounds_inclusive(start, end));
                }

                // Advance the range that ends first.
                let a_ends_first = match (is_unbounded_end(a_end), is_unbounded_end(b_end)) {
                    (true, true) => false,
                    (true, false) => false,
                    (false, true) => true,
                    (false, false) => a_end < b_end,
                };

                if a_ends_first {
                    i += 1;
                } else {
                    j += 1;
                }
            }

            out
        })
    }
}

fn intersect_two_normalized(a: &[RowRange], b: &[RowRange]) -> RangeVec {
    if force_scalar_impl() {
        return intersect_two_normalized_scalar(a, b);
    }

    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
        if std::is_x86_feature_detected!("avx2") {
            return unsafe { simd_x86::intersect_two_normalized_avx2(a, b) };
        }
    }

    intersect_two_normalized_scalar(a, b)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_normalized_non_overlapping(out: &[RowRange]) {
        if out.is_empty() {
            return;
        }

        for w in out.windows(2) {
            let a = w[0];
            let b = w[1];
            assert!(!a.is_empty());
            assert!(!b.is_empty());
            assert!(a.start_pointer_pos <= b.start_pointer_pos);

            let a_end = a.end_pointer_pos_inclusive();
            // No overlap or adjacency in normalized output.
            assert!(!overlaps_or_adjacent(a_end, b.start_pointer_pos));
        }
    }

    #[test]
    fn intersects_example() {
        // Example from the prompt, expressed as row-id ranges.
        let a = [
            RowRange::from_row_id_range_inclusive(0, 10),
            RowRange::from_row_id_range_inclusive(40, 50),
        ];
        let b = [
            RowRange::from_row_id_range_inclusive(5, 15),
            RowRange::from_row_id_range_inclusive(35, 45),
        ];

        let out = intersect_many([a.as_slice(), b.as_slice()]);

        let expected = [
            RowRange::from_row_id_range_inclusive(5, 10),
            RowRange::from_row_id_range_inclusive(40, 45),
        ];

        assert_eq!(out.as_slice(), expected);
    }

    #[test]
    fn normalize_merges_overlaps_and_adjacency() {
        let rec = ROW_POINTER_RECORD_LEN as u64;

        // (0..=2) and (3..=5) are adjacent in pointer-space.
        let r1 = RowRange::from_pointer_bounds_inclusive(0 * rec, 2 * rec);
        let r2 = RowRange::from_pointer_bounds_inclusive(3 * rec, 5 * rec);

        let out = normalize_ranges(&[r2, r1]);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0], RowRange::from_pointer_bounds_inclusive(0 * rec, 5 * rec));
    }

    #[test]
    fn intersect_many_works() {
        let a = [RowRange::from_row_id_range_inclusive(0, 100)];
        let b = [RowRange::from_row_id_range_inclusive(10, 20)];
        let c = [RowRange::from_row_id_range_inclusive(15, 25)];

        let out = intersect_many([a.as_slice(), b.as_slice(), c.as_slice()]);
        assert_eq!(out.as_slice(), [RowRange::from_row_id_range_inclusive(15, 20)]);
    }

    #[test]
    fn duplicates_in_single_input_are_merged() {
        // Duplicate and overlapping ranges should normalize into one.
        let a = [
            RowRange::from_row_id_range_inclusive(0, 10),
            RowRange::from_row_id_range_inclusive(0, 10),
            RowRange::from_row_id_range_inclusive(5, 7),
        ];

        let out = intersect_many([a.as_slice()]);
        assert_eq!(out.as_slice(), [RowRange::from_row_id_range_inclusive(0, 10)]);
    }

    #[test]
    fn duplicates_do_not_create_duplicate_output_ranges() {
        let a = [
            RowRange::from_row_id_range_inclusive(0, 10),
            RowRange::from_row_id_range_inclusive(0, 10),
        ];
        let b = [RowRange::from_row_id_range_inclusive(5, 5)];

        let out = intersect_many([a.as_slice(), b.as_slice()]);
        assert_eq!(out.as_slice(), [RowRange::from_row_id_range_inclusive(5, 5)]);
    }

    #[test]
    fn unsorted_inputs_still_work() {
        let a = [
            RowRange::from_row_id_range_inclusive(40, 50),
            RowRange::from_row_id_range_inclusive(0, 10),
        ];
        let b = [
            RowRange::from_row_id_range_inclusive(35, 45),
            RowRange::from_row_id_range_inclusive(5, 15),
        ];

        let out = intersect_many([a.as_slice(), b.as_slice()]);
        let expected = [
            RowRange::from_row_id_range_inclusive(5, 10),
            RowRange::from_row_id_range_inclusive(40, 45),
        ];
        assert_eq!(out.as_slice(), expected);
        assert_normalized_non_overlapping(out.as_slice());
    }

    #[test]
    fn empty_inputs_behavior() {
        let empty: [RowRange; 0] = [];
        let a = [RowRange::from_row_id_range_inclusive(0, 10)];

        // No inputs => empty output.
        let out0 = intersect_many(std::iter::empty::<&[RowRange]>());
        assert!(out0.is_empty());

        // Any empty input => empty output.
        let out1 = intersect_many([a.as_slice(), empty.as_slice()]);
        assert!(out1.is_empty());
    }

    #[test]
    fn adjacency_is_merged_during_normalize() {
        // Adjacent row-id ranges should merge into one normalized range.
        let a = [
            RowRange::from_row_id_range_inclusive(0, 10),
            RowRange::from_row_id_range_inclusive(11, 20),
        ];

        let out = intersect_many([a.as_slice()]);
        assert_eq!(out.as_slice(), [RowRange::from_row_id_range_inclusive(0, 20)]);
        assert_normalized_non_overlapping(out.as_slice());
    }

    #[test]
    fn unbounded_range_intersects_correctly() {
        // [start..=MAX] intersect [x..=y] => [max(start,x)..=y]
        let a = [RowRange::from_row_id_range_inclusive(10, u64::MAX)];
        let b = [RowRange::from_row_id_range_inclusive(0, 20)];

        let out = intersect_many([a.as_slice(), b.as_slice()]);
        assert_eq!(out.as_slice(), [RowRange::from_row_id_range_inclusive(10, 20)]);
        assert_normalized_non_overlapping(out.as_slice());
    }

    #[test]
    fn output_is_normalized_for_multiway_intersection() {
        let a = [
            RowRange::from_row_id_range_inclusive(0, 100),
            RowRange::from_row_id_range_inclusive(0, 100),
        ];
        let b = [
            RowRange::from_row_id_range_inclusive(10, 20),
            RowRange::from_row_id_range_inclusive(21, 30),
        ];
        let c = [RowRange::from_row_id_range_inclusive(15, 25)];

        let out = intersect_many([a.as_slice(), b.as_slice(), c.as_slice()]);
        // Note: `b` has adjacent ranges (20 then 21), and normalization merges adjacency.
        assert_eq!(out.as_slice(), [RowRange::from_row_id_range_inclusive(15, 25)]);
        assert_normalized_non_overlapping(out.as_slice());
    }
}
