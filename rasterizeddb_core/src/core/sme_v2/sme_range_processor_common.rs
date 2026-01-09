use crate::core::{row::row_pointer::ROW_POINTER_RECORD_LEN, sme_v2::rules::RowRange};


/// Sorts by `start_pointer_pos`, then merges overlapping or directly-adjacent ranges.
pub fn merge_row_ranges(
	mut ranges: smallvec::SmallVec<[RowRange; 64]>,
) -> smallvec::SmallVec<[RowRange; 64]> {
	ranges.retain(|r| !r.is_empty());
	if ranges.len() <= 1 {
		return ranges;
	}

	ranges.sort_unstable_by(|a, b| {
		a.start_pointer_pos
			.cmp(&b.start_pointer_pos)
			.then_with(|| a.end_pointer_pos_inclusive().cmp(&b.end_pointer_pos_inclusive()))
	});

	// SAFETY: we just sorted by start/end, and RowRange is Copy.
	unsafe { merge_row_ranges_sorted_in_place_unchecked(&mut ranges) };
	ranges
}

/// Merges overlapping or directly-adjacent ranges **in-place**.
///
/// # Safety
/// Caller must guarantee:
/// - `ranges` is sorted by `(start_pointer_pos, end_pointer_pos_inclusive)` ascending.
/// - every element has `row_count > 0`.
pub unsafe fn merge_row_ranges_sorted_in_place_unchecked(
	ranges: &mut smallvec::SmallVec<[RowRange; 64]>,
) {
	let len = ranges.len();
	if len <= 1 {
		return;
	}

	let ptr = ranges.as_mut_ptr();
	let mut write = 0usize;
	let first = unsafe { *ptr };
	debug_assert!(!first.is_empty());
	let rec = ROW_POINTER_RECORD_LEN as u64;
	debug_assert!(rec > 0);

	let mut cur_start = first.start_pointer_pos;
	let mut cur_end = first.end_pointer_pos_inclusive();

	for read in 1..len {
		let r = unsafe { *ptr.add(read) };
		debug_assert!(!r.is_empty());
		let r_start = r.start_pointer_pos;
		let r_end = r.end_pointer_pos_inclusive();
		let cur_end_plus_rec = cur_end.saturating_add(rec);
		if r_start <= cur_end_plus_rec {
			cur_end = cur_end.max(r_end);
		} else {
			unsafe { *ptr.add(write) = RowRange::from_pointer_bounds_inclusive(cur_start, cur_end) };
			write += 1;
			cur_start = r_start;
			cur_end = r_end;
		}
	}

	unsafe { *ptr.add(write) = RowRange::from_pointer_bounds_inclusive(cur_start, cur_end) };
	write += 1;

	unsafe { ranges.set_len(write) };
}

#[inline]
pub fn row_ranges_overlap(a: RowRange, b: RowRange) -> bool {
    if a.is_empty() || b.is_empty() {
        return false;
    }
    // Inclusive overlap in pointer space: [a.start, a.end] intersects [b.start, b.end]
    let a_start = a.start_pointer_pos;
    let a_end = a.end_pointer_pos_inclusive();
    let b_start = b.start_pointer_pos;
    let b_end = b.end_pointer_pos_inclusive();
    a_start <= b_end && b_start <= a_end
}

/// Scalar overlap check between two vectors of row ranges.
///
/// Returns true if any range in `a` overlaps any range in `b`.
#[inline]
pub fn any_overlap_scalar(a: &[RowRange], b: &[RowRange]) -> bool {
    for &ra in a {
        for &rb in b {
            if row_ranges_overlap(ra, rb) {
                return true;
            }
        }
    }
    false
}

/// AVX2 overlap check between two vectors of row ranges.
///
/// - On non-x86_64 targets, or if AVX2 isn't available at runtime, this falls back to scalar.
/// - For correctness with full `u64` range, comparisons are performed as unsigned.
#[inline]
pub fn any_overlap_avx2(a: &[RowRange], b: &[RowRange]) -> bool {
    // Small inputs are typically faster scalar (less overhead).
    if a.is_empty() || b.is_empty() {
        return false;
    }

    #[cfg(all(target_arch = "x86_64"))]
    {
        if std::is_x86_feature_detected!("avx2") {
            // SAFETY: guarded by runtime feature detection.
            return unsafe { any_overlap_avx2_with_ranges(a, b) };
        }
    }

    any_overlap_scalar(a, b)
}

#[cfg(all(target_arch = "x86_64"))]
#[target_feature(enable = "avx2")]
unsafe fn any_overlap_avx2_with_ranges(a: &[RowRange], b: &[RowRange]) -> bool {
    use std::arch::x86_64::*;

    let n = b.len();
    if n == 0 {
        return false;
    }

    let sign = _mm256_set1_epi64x(i64::MIN);
    let ones = _mm256_set1_epi64x(-1);

    // Helper: unsigned (a > b) per lane mask, using signed compare after XOR with sign bit.
    #[inline(always)]
    unsafe fn cmpgt_u64(a: __m256i, b: __m256i, sign: __m256i) -> __m256i {
        let ax = unsafe { _mm256_xor_si256(a, sign) };
        let bx = unsafe { _mm256_xor_si256(b, sign) };
        unsafe { _mm256_cmpgt_epi64(ax, bx) }
    }

    // Helper: unsigned (a <= b) per lane mask.
    #[inline(always)]
    unsafe fn cmple_u64(a: __m256i, b: __m256i, sign: __m256i, ones: __m256i) -> __m256i {
        let gt = unsafe { cmpgt_u64(a, b, sign) };
        unsafe { _mm256_andnot_si256(gt, ones) }
    }

    #[repr(C)]
    #[derive(Clone, Copy)]
    struct Bounds {
        start: u64,
        end: u64,
    }

    let mut b_bounds: Vec<Bounds> = Vec::with_capacity(b.len());
    for rb in b {
        if rb.is_empty() {
            continue;
        }
        b_bounds.push(Bounds {
            start: rb.start_pointer_pos,
            end: rb.end_pointer_pos_inclusive(),
        });
    }
    if b_bounds.is_empty() {
        return false;
    }

    for ra in a {
        if ra.is_empty() {
            continue;
        }
        let a_start_u = ra.start_pointer_pos;
        let a_end_u = ra.end_pointer_pos_inclusive();
        let a_start = _mm256_set1_epi64x(a_start_u as i64);
        let a_end = _mm256_set1_epi64x(a_end_u as i64);

        let mut i = 0usize;
        // Load 2 ranges at a time (32 bytes): [s0,e0,s1,e1] in 64-bit lanes.
        let n2 = b_bounds.len();
        while i + 2 <= n2 {
            let v = unsafe { _mm256_loadu_si256(b_bounds.as_ptr().add(i) as *const __m256i) };
            // starts = [s0,s1,s0,s1], ends = [e0,e1,e0,e1]
            let b_start = _mm256_permute4x64_epi64(v, 0x88);
            let b_end = _mm256_permute4x64_epi64(v, 0xDD);

            // overlap = (a_start <= b_end) && (b_start <= a_end)
            let c1 = unsafe { cmple_u64(a_start, b_end, sign, ones) };
            let c2 = unsafe { cmple_u64(b_start, a_end, sign, ones) };
            let both = _mm256_and_si256(c1, c2);

            if _mm256_movemask_epi8(both) != 0 {
                return true;
            }
            i += 2;
        }

        // Tail.
        for j in i..n2 {
            let rb = b_bounds[j];
            if a_start_u <= rb.end && rb.start <= a_end_u {
                return true;
            }
        }
    }

    false
}