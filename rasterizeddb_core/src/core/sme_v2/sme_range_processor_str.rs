#[cfg(target_arch = "x86_64")]
use crate::core::sme_v2::rules::STRING_RULE_VALUE_MAX_BYTES;
use crate::core::sme_v2::{rules::{RowRange, StringCorrelationRule, StringRuleOp}, sme_range_processor_common::{merge_row_ranges, merge_row_ranges_clamped, uncovered_row_ranges_from_merged}};


/// Returns merged candidate row ranges for a string query.
///
/// This mirrors the numeric engines: each rule represents a predicate that all row-values
/// inside its `ranges` satisfy. Depending on `query_op`, we either:
/// - `None` (e.g. `=`): treat rule ops as predicates on the *query string* and include rules
///   that the query satisfies (strong filtering).
/// - `Some(op)` (e.g. `STARTSWITH`/`ENDSWITH`/`CONTAINS`): include rules that are *compatible*
///   with the query predicate (conservative superset), i.e. there exists at least one string
///   that could satisfy both the rule predicate and the query predicate.
///
/// Compatibility rules:
/// - `StartsWith(v)`: compatible iff `query` starts with `v`.
/// - `EndsWith(v)`: compatible iff `query` ends with `v`.
/// - `Contains(v, count)`: compatible iff `query` contains `v` at least `count` times.
///
/// Note: this function only *selects candidate ranges*. It does not guarantee that every
/// row within returned ranges matches the query.
pub fn candidate_row_ranges_for_query_string(
	query: &str,
	query_op: Option<StringRuleOp>,
	rules: &[StringCorrelationRule],
	total_rows: Option<u64>,
) -> smallvec::SmallVec<[RowRange; 64]> {
	// For small rule counts, scalar is typically fastest.
	if rules.len() < 16 {
		return candidate_row_ranges_for_query_scalar(query, query_op, rules, total_rows);
	}

	#[cfg(target_arch = "x86_64")]
	{
		if std::is_x86_feature_detected!("avx2") {
			// SAFETY: guarded by runtime feature detection.
			return unsafe { candidate_row_ranges_for_query_avx2(query, query_op, rules, total_rows) };
		}
	}

	candidate_row_ranges_for_query_scalar(query, query_op, rules, total_rows)
}

#[inline]
fn starts_with_bytes(haystack: &[u8], needle: &[u8]) -> bool {
	haystack.starts_with(needle)
}

#[inline]
fn ends_with_bytes(haystack: &[u8], needle: &[u8]) -> bool {
	haystack.ends_with(needle)
}

#[inline]
fn count_occurrences_overlapping(haystack: &[u8], needle: &[u8]) -> usize {
	if needle.is_empty() {
		// We treat empty-needle as non-matchable for counting.
		return 0;
	}
	if needle.len() == 1 {
		let b = needle[0];
		return haystack.iter().filter(|&&x| x == b).count();
	}
	if needle.len() > haystack.len() {
		return 0;
	}

	let mut count = 0usize;
	let mut i = 0usize;
	while i + needle.len() <= haystack.len() {
		if &haystack[i..i + needle.len()] == needle {
			count += 1;
		}
		i += 1;
	}
	count
}

#[inline]
pub fn rule_matches_query(query: &str, rule: &StringCorrelationRule) -> bool {
	let q = query.as_bytes();
	let v = rule.value.as_bytes();
	match rule.op {
		StringRuleOp::StartsWith => starts_with_bytes(q, v),
		StringRuleOp::EndsWith => ends_with_bytes(q, v),
		StringRuleOp::Contains => {
			if rule.count == 0 {
				return true;
			}
			if v.is_empty() {
				return false;
			}
			count_occurrences_overlapping(q, v) >= rule.count as usize
		}
	}
}

#[inline]
fn startswith_compatible(query: &str, rule_value: &str) -> bool {
	// Exists string s that starts with both query and rule_value.
	query.starts_with(rule_value) || rule_value.starts_with(query)
}

#[inline]
fn endswith_compatible(query: &str, rule_value: &str) -> bool {
	// Exists string s that ends with both query and rule_value.
	query.ends_with(rule_value) || rule_value.ends_with(query)
}

#[inline]
pub fn rule_compatible_with_query(query: &str, query_op: StringRuleOp, rule: &StringCorrelationRule) -> bool {
	match query_op {
		StringRuleOp::StartsWith => match rule.op {
			StringRuleOp::StartsWith => startswith_compatible(query, &rule.value),
			// Always satisfiable together: choose a longer string with both constraints.
			StringRuleOp::EndsWith => true,
			StringRuleOp::Contains => {
				if rule.count == 0 {
					true
				} else {
					!rule.value.is_empty()
				}
			}
		},
		StringRuleOp::EndsWith => match rule.op {
			StringRuleOp::EndsWith => endswith_compatible(query, &rule.value),
			StringRuleOp::StartsWith => true,
			StringRuleOp::Contains => {
				if rule.count == 0 {
					true
				} else {
					!rule.value.is_empty()
				}
			}
		},
		StringRuleOp::Contains => match rule.op {
			// A CONTAINS query can be compatible with essentially any other predicate.
			StringRuleOp::Contains => rule.count == 0 || !rule.value.is_empty(),
			StringRuleOp::StartsWith => true,
			StringRuleOp::EndsWith => true,
		},
	}
}

#[inline]
pub fn candidate_row_ranges_for_query_scalar(
	query: &str,
	query_op: Option<StringRuleOp>,
	rules: &[StringCorrelationRule],
	total_rows: Option<u64>,
) -> smallvec::SmallVec<[RowRange; 64]> {
	let mut out = smallvec::SmallVec::<[RowRange; 64]>::new();
	let mut coverage = smallvec::SmallVec::<[RowRange; 64]>::new();
	for rule in rules {
		coverage.extend_from_slice(&rule.ranges);
		match query_op {
			None => {
				if rule_matches_query(query, rule) {
					out.extend_from_slice(&rule.ranges);
				}
			}
			Some(op) => {
				if rule_compatible_with_query(query, op, rule) {
					out.extend_from_slice(&rule.ranges);
				}
			}
		}
	}
	finalize_string_candidates(out, coverage, total_rows)
}

#[inline]
fn finalize_string_candidates(
	matched: smallvec::SmallVec<[RowRange; 64]>,
	coverage: smallvec::SmallVec<[RowRange; 64]>,
	total_rows: Option<u64>,
) -> smallvec::SmallVec<[RowRange; 64]> {
	match total_rows {
		Some(total) => {
			let merged_coverage = merge_row_ranges_clamped(total, &coverage);
			let mut out = merge_row_ranges_clamped(total, &matched);
			let unknown = uncovered_row_ranges_from_merged(total, &merged_coverage);
			out.extend_from_slice(&unknown);
			merge_row_ranges(out)
		}
		None => merge_row_ranges(matched),
	}
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
fn mask_for_prefix_len(len: usize) -> (u64, u64) {
	debug_assert!(len <= 16);
	if len == 0 {
		return (0, 0);
	}
	if len <= 8 {
		let shift = (len as u32) * 8;
		let low = if shift == 64 { u64::MAX } else { (1u64 << shift) - 1 };
		(low, 0)
	} else {
		let hi_len = len - 8;
		let shift = (hi_len as u32) * 8;
		let high = if shift == 64 { u64::MAX } else { (1u64 << shift) - 1 };
		(u64::MAX, high)
	}
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
fn avx2_lane_any(mask: i32, lane: usize) -> bool {
	((mask >> (lane * 8)) & 0xFF) != 0
}

#[cfg(target_arch = "x86_64")]
#[inline]
fn encode_16_padded(bytes: &[u8]) -> [u8; STRING_RULE_VALUE_MAX_BYTES] {
	let mut out = [0u8; STRING_RULE_VALUE_MAX_BYTES];
	let n = bytes.len().min(STRING_RULE_VALUE_MAX_BYTES);
	out[..n].copy_from_slice(&bytes[..n]);
	out
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn candidate_row_ranges_for_query_avx2(
	query: &str,
	query_op: Option<StringRuleOp>,
	rules: &[StringCorrelationRule],
	total_rows: Option<u64>,
) -> smallvec::SmallVec<[RowRange; 64]> {
	use std::arch::x86_64::*;

	let q_bytes = query.as_bytes();

	// Split by op and by value length (0..=16) for the AVX2 paths.
	// Anything not representable within 16 bytes is scalar fallback.
	let mut sw_by_len: [smallvec::SmallVec<[usize; 64]>; 17] = std::array::from_fn(|_| smallvec::SmallVec::new());
	let mut ew_by_len: [smallvec::SmallVec<[usize; 64]>; 17] = std::array::from_fn(|_| smallvec::SmallVec::new());
	let mut scalar_fallback = smallvec::SmallVec::<[usize; 64]>::new();
	let mut coverage = smallvec::SmallVec::<[RowRange; 64]>::new();

	for (i, r) in rules.iter().enumerate() {
		coverage.extend_from_slice(&r.ranges);
		let len = r.value.as_bytes().len();
		if len > STRING_RULE_VALUE_MAX_BYTES {
			scalar_fallback.push(i);
			continue;
		}
		match r.op {
			StringRuleOp::StartsWith => sw_by_len[len].push(i),
			StringRuleOp::EndsWith => ew_by_len[len].push(i),
			StringRuleOp::Contains => scalar_fallback.push(i),
		}
	}

	let mut out = smallvec::SmallVec::<[RowRange; 64]>::new();

	// Apply scalar fallback rules.
	for &i in &scalar_fallback {
		let rule = &rules[i];
		match query_op {
			None => {
				if rule_matches_query(query, rule) {
					out.extend_from_slice(&rule.ranges);
				}
			}
			Some(op) => {
				if rule_compatible_with_query(query, op, rule) {
					out.extend_from_slice(&rule.ranges);
				}
			}
		}
	}

	let zero = _mm256_setzero_si256();

	// STARTS WITH
	if query_op.is_none() || query_op == Some(StringRuleOp::StartsWith) {
		for len in 0..=STRING_RULE_VALUE_MAX_BYTES {
		let idx = &sw_by_len[len];
		if idx.is_empty() {
			continue;
		}
		// For equality mode (None): require query startswith rule.value (len <= q_len).
		// For StartsWith mode: startswith compatibility for StartsWith rules means:
		// - query startswith rule.value (len <= q_len)
		// - OR rule.value startswith query (len >= q_len)
		if query_op.is_none() {
			if len > q_bytes.len() {
				continue;
			}
		} else {
			// In StartsWith mode, when len < q_len we still can match (query startswith rule.value);
			// when len >= q_len we can match if rule.value startswith query.
			// We'll handle both by choosing a compare length `cmp_len`:
			// - if len <= q_len: cmp_len = len, compare query prefix
			// - if len > q_len: cmp_len = q_len, compare rule prefix
			if q_bytes.is_empty() {
				// Empty query prefix is compatible with any startswith constraint.
				for &i in idx {
					out.extend_from_slice(&rules[i].ranges);
				}
				continue;
			}
		}

		let q_len = q_bytes.len().min(STRING_RULE_VALUE_MAX_BYTES);
		let cmp_len = match query_op {
			None => len,
			Some(_) => if len <= q_len { len } else { q_len },
		};
		if cmp_len == 0 {
			for &i in idx {
				out.extend_from_slice(&rules[i].ranges);
			}
			continue;
		}

		let (mask_low, mask_high) = mask_for_prefix_len(cmp_len);
		let mask_low_vec = _mm256_set1_epi64x(mask_low as i64);
		let mask_high_vec = _mm256_set1_epi64x(mask_high as i64);

		let q16 = encode_16_padded(&q_bytes[..cmp_len]);
		let q_lo = u64::from_le_bytes(q16[0..8].try_into().unwrap());
		let q_hi = u64::from_le_bytes(q16[8..16].try_into().unwrap());
		let q_lo_vec = _mm256_set1_epi64x(q_lo as i64);
		let q_hi_vec = _mm256_set1_epi64x(q_hi as i64);

		let mut k = 0usize;
		while k + 4 <= idx.len() {
			let i0 = idx[k];
			let i1 = idx[k + 1];
			let i2 = idx[k + 2];
			let i3 = idx[k + 3];

			let b0 = encode_16_padded(rules[i0].value.as_bytes());
			let b1 = encode_16_padded(rules[i1].value.as_bytes());
			let b2 = encode_16_padded(rules[i2].value.as_bytes());
			let b3 = encode_16_padded(rules[i3].value.as_bytes());

			let lo0 = u64::from_le_bytes(b0[0..8].try_into().unwrap());
			let hi0 = u64::from_le_bytes(b0[8..16].try_into().unwrap());
			let lo1 = u64::from_le_bytes(b1[0..8].try_into().unwrap());
			let hi1 = u64::from_le_bytes(b1[8..16].try_into().unwrap());
			let lo2 = u64::from_le_bytes(b2[0..8].try_into().unwrap());
			let hi2 = u64::from_le_bytes(b2[8..16].try_into().unwrap());
			let lo3 = u64::from_le_bytes(b3[0..8].try_into().unwrap());
			let hi3 = u64::from_le_bytes(b3[8..16].try_into().unwrap());

			let r_lo = _mm256_set_epi64x(lo3 as i64, lo2 as i64, lo1 as i64, lo0 as i64);
			let r_hi = _mm256_set_epi64x(hi3 as i64, hi2 as i64, hi1 as i64, hi0 as i64);

			// Compare either query-prefix (None, or len<=q_len), or rule-prefix (len>q_len in StartsWith-mode).
			let diff_lo = _mm256_and_si256(_mm256_xor_si256(r_lo, q_lo_vec), mask_low_vec);
			let eq_lo = _mm256_cmpeq_epi64(diff_lo, zero);
			let mut ok_mask = _mm256_movemask_epi8(eq_lo);

			if cmp_len > 8 {
				let diff_hi = _mm256_and_si256(_mm256_xor_si256(r_hi, q_hi_vec), mask_high_vec);
				let eq_hi = _mm256_cmpeq_epi64(diff_hi, zero);
				ok_mask &= _mm256_movemask_epi8(eq_hi);
			}

			if avx2_lane_any(ok_mask, 0) {
				out.extend_from_slice(&rules[i0].ranges);
			}
			if avx2_lane_any(ok_mask, 1) {
				out.extend_from_slice(&rules[i1].ranges);
			}
			if avx2_lane_any(ok_mask, 2) {
				out.extend_from_slice(&rules[i2].ranges);
			}
			if avx2_lane_any(ok_mask, 3) {
				out.extend_from_slice(&rules[i3].ranges);
			}

			k += 4;
		}

		// Tail.
		for &i in &idx[k..] {
			let rv = rules[i].value.as_bytes();
			let ok = match query_op {
				None => starts_with_bytes(q_bytes, rv),
				Some(_) => {
					let rvs = rules[i].value.as_str();
					startswith_compatible(query, rvs)
				}
			};
			if ok {
				out.extend_from_slice(&rules[i].ranges);
			}
		}
		}
	}

	// ENDS WITH
	if query_op.is_none() || query_op == Some(StringRuleOp::EndsWith) {
		for len in 0..=STRING_RULE_VALUE_MAX_BYTES {
		let idx = &ew_by_len[len];
		if idx.is_empty() {
			continue;
		}
		if query_op.is_none() {
			if len > q_bytes.len() {
				continue;
			}
		} else {
			if q_bytes.is_empty() {
				for &i in idx {
					out.extend_from_slice(&rules[i].ranges);
				}
				continue;
			}
		}

		let q_len = q_bytes.len().min(STRING_RULE_VALUE_MAX_BYTES);
		let cmp_len = match query_op {
			None => len,
			Some(_) => if len <= q_len { len } else { q_len },
		};
		if cmp_len == 0 {
			for &i in idx {
				out.extend_from_slice(&rules[i].ranges);
			}
			continue;
		}

		let (mask_low, mask_high) = mask_for_prefix_len(cmp_len);
		let mask_low_vec = _mm256_set1_epi64x(mask_low as i64);
		let mask_high_vec = _mm256_set1_epi64x(mask_high as i64);

		// Compare query suffix (None, or len<=q_len), or rule suffix (len>q_len in EndsWith-mode).
		let q_suffix_start = q_bytes.len() - cmp_len;
		let q16 = encode_16_padded(&q_bytes[q_suffix_start..]);
		let q_lo = u64::from_le_bytes(q16[0..8].try_into().unwrap());
		let q_hi = u64::from_le_bytes(q16[8..16].try_into().unwrap());
		let q_lo_vec = _mm256_set1_epi64x(q_lo as i64);
		let q_hi_vec = _mm256_set1_epi64x(q_hi as i64);

		let mut k = 0usize;
		while k + 4 <= idx.len() {
			let i0 = idx[k];
			let i1 = idx[k + 1];
			let i2 = idx[k + 2];
			let i3 = idx[k + 3];

			// For equality mode: compare entire rule value suffix of length `len` against query suffix.
			// For EndsWith-mode with len>q_len: compare rule suffix of length `q_len` against query suffix.
			let b0 = if query_op.is_none() || len <= q_len {
				encode_16_padded(rules[i0].value.as_bytes())
			} else {
				let rv = rules[i0].value.as_bytes();
				let start = rv.len() - cmp_len;
				encode_16_padded(&rv[start..])
			};
			let b1 = if query_op.is_none() || len <= q_len {
				encode_16_padded(rules[i1].value.as_bytes())
			} else {
				let rv = rules[i1].value.as_bytes();
				let start = rv.len() - cmp_len;
				encode_16_padded(&rv[start..])
			};
			let b2 = if query_op.is_none() || len <= q_len {
				encode_16_padded(rules[i2].value.as_bytes())
			} else {
				let rv = rules[i2].value.as_bytes();
				let start = rv.len() - cmp_len;
				encode_16_padded(&rv[start..])
			};
			let b3 = if query_op.is_none() || len <= q_len {
				encode_16_padded(rules[i3].value.as_bytes())
			} else {
				let rv = rules[i3].value.as_bytes();
				let start = rv.len() - cmp_len;
				encode_16_padded(&rv[start..])
			};

			let lo0 = u64::from_le_bytes(b0[0..8].try_into().unwrap());
			let hi0 = u64::from_le_bytes(b0[8..16].try_into().unwrap());
			let lo1 = u64::from_le_bytes(b1[0..8].try_into().unwrap());
			let hi1 = u64::from_le_bytes(b1[8..16].try_into().unwrap());
			let lo2 = u64::from_le_bytes(b2[0..8].try_into().unwrap());
			let hi2 = u64::from_le_bytes(b2[8..16].try_into().unwrap());
			let lo3 = u64::from_le_bytes(b3[0..8].try_into().unwrap());
			let hi3 = u64::from_le_bytes(b3[8..16].try_into().unwrap());

			let r_lo = _mm256_set_epi64x(lo3 as i64, lo2 as i64, lo1 as i64, lo0 as i64);
			let r_hi = _mm256_set_epi64x(hi3 as i64, hi2 as i64, hi1 as i64, hi0 as i64);

			let diff_lo = _mm256_and_si256(_mm256_xor_si256(r_lo, q_lo_vec), mask_low_vec);
			let eq_lo = _mm256_cmpeq_epi64(diff_lo, zero);
			let mut ok_mask = _mm256_movemask_epi8(eq_lo);

			if cmp_len > 8 {
				let diff_hi = _mm256_and_si256(_mm256_xor_si256(r_hi, q_hi_vec), mask_high_vec);
				let eq_hi = _mm256_cmpeq_epi64(diff_hi, zero);
				ok_mask &= _mm256_movemask_epi8(eq_hi);
			}

			if avx2_lane_any(ok_mask, 0) {
				out.extend_from_slice(&rules[i0].ranges);
			}
			if avx2_lane_any(ok_mask, 1) {
				out.extend_from_slice(&rules[i1].ranges);
			}
			if avx2_lane_any(ok_mask, 2) {
				out.extend_from_slice(&rules[i2].ranges);
			}
			if avx2_lane_any(ok_mask, 3) {
				out.extend_from_slice(&rules[i3].ranges);
			}

			k += 4;
		}

		// Tail.
		for &i in &idx[k..] {
			let rv = rules[i].value.as_bytes();
			let ok = match query_op {
				None => ends_with_bytes(q_bytes, rv),
				Some(_) => {
					let rvs = rules[i].value.as_str();
					endswith_compatible(query, rvs)
				}
			};
			if ok {
				out.extend_from_slice(&rules[i].ranges);
			}
		}
		}
	}

	// For CONTAINS query-op, everything compatible (except impossible contains rules).
	if query_op == Some(StringRuleOp::Contains) {
		for rule in rules {
			if rule.op == StringRuleOp::Contains && rule.count > 0 && rule.value.is_empty() {
				continue;
			}
			out.extend_from_slice(&rule.ranges);
		}
	}

	finalize_string_candidates(out, coverage, total_rows)
}

#[cfg(test)]
mod tests {
	use crate::core::{db_type::DbType, row::row_pointer::ROW_POINTER_RECORD_LEN};

	use super::*;
#[test]
	fn candidate_ranges_string_avx2_matches_scalar_when_available() {
		let rules = vec![
			StringCorrelationRule {
				column_schema_id: 1,
				column_type: DbType::STRING,
				op: StringRuleOp::StartsWith,
				count: 0,
				value: "abc".to_string(),
				ranges: smallvec::smallvec![RowRange {
					start_pointer_pos: 10 * ROW_POINTER_RECORD_LEN as u64,
					row_count: 11,
				}],
			},
			StringCorrelationRule {
				column_schema_id: 1,
				column_type: DbType::STRING,
				op: StringRuleOp::EndsWith,
				count: 0,
				value: "xyz".to_string(),
				ranges: smallvec::smallvec![RowRange {
					start_pointer_pos: 100 * ROW_POINTER_RECORD_LEN as u64,
					row_count: 21,
				}],
			},
			StringCorrelationRule {
				column_schema_id: 1,
				column_type: DbType::STRING,
				op: StringRuleOp::Contains,
				count: 2,
				value: "a".to_string(),
				ranges: smallvec::smallvec![RowRange {
					start_pointer_pos: 200 * ROW_POINTER_RECORD_LEN as u64,
					row_count: 21,
				}],
			},
		];

		let query = "abc__a__xyz";
		let total_rows = 400u64;
		let scalar = candidate_row_ranges_for_query_scalar(query, None, &rules, Some(total_rows));

		#[cfg(target_arch = "x86_64")]
		{
			if std::is_x86_feature_detected!("avx2") {
				let simd = unsafe { candidate_row_ranges_for_query_avx2(query, None, &rules, Some(total_rows)) };
				assert_eq!(simd, scalar);
			}
		}
	}

	#[test]
	fn candidate_ranges_startswith_compatibility_includes_contains_rules() {
		let total_rows = 25u64;
		let rules = vec![
			StringCorrelationRule {
				column_schema_id: 1,
				column_type: DbType::STRING,
				op: StringRuleOp::Contains,
				count: 1,
				value: "a".to_string(),
				ranges: smallvec::smallvec![RowRange {
					start_pointer_pos: 1 * ROW_POINTER_RECORD_LEN as u64,
					row_count: 2,
				}],
			},
			StringCorrelationRule {
				column_schema_id: 1,
				column_type: DbType::STRING,
				op: StringRuleOp::StartsWith,
				count: 0,
				value: "zzz".to_string(),
				ranges: smallvec::smallvec![RowRange {
					start_pointer_pos: 10 * ROW_POINTER_RECORD_LEN as u64,
					row_count: 11,
				}],
			},
		];

		let query = "abc";
		let out = candidate_row_ranges_for_query_string(query, Some(StringRuleOp::StartsWith), &rules, Some(total_rows));
		// CONTAINS("a",1) is compatible with STARTSWITH("abc"), but STARTSWITH("zzz") is not.
		let expected: smallvec::SmallVec<[RowRange; 64]> = smallvec::smallvec![
			RowRange::from_row_id_range_inclusive(0, 9),
			RowRange::from_row_id_range_inclusive(21, 24),
		];
		assert_eq!(out, expected);
	}
}