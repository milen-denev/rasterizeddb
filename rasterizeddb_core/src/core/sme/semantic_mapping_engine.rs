use std::sync::{Arc, OnceLock};

use crc::{CRC_64_ECMA_182, Crc};
use dashmap::DashMap;
use smallvec::SmallVec;

use crate::core::{
    db_type::DbType,
    row::{
        row_pointer::{RowPointer, RowPointerIterator},
        schema::SchemaField,
    },
    sme::rule_store::{ScanMeta, SemanticRuleStore},
    storage_providers::traits::StorageIO,
    tokenizer::query_tokenizer::{NumericValue, Token},
};

/// Global singleton instance.
///
/// The engine can be lazily initialized via `SME.get_or_init(...)`.
pub static SME: OnceLock<rclite::Arc<SemanticMappingEngine>> = OnceLock::new();

const CRC_64: Crc<u64> = Crc::<u64>::new(&CRC_64_ECMA_182);

pub struct SemanticMappingEngine {
    // Key: crc64(table_name) ^ crc64(where_query)
    candidates: DashMap<u64, rclite::Arc<Vec<RowPointer>>>,
}

impl SemanticMappingEngine {
    pub fn new() -> Self {
        Self {
            candidates: DashMap::new(),
        }
    }

    #[inline]
    fn key_from_tokens(table_name: &str, tokens: &SmallVec<[Token; 36]>) -> u64 {
        CRC_64.checksum(table_name.as_bytes()) ^ hash_tokens(tokens)
    }

    pub fn get_candidates_for_table_tokens(
        &self,
        table_name: &str,
        tokens: &SmallVec<[Token; 36]>,
    ) -> Option<rclite::Arc<Vec<RowPointer>>> {
        let key = Self::key_from_tokens(table_name, tokens);
        self.candidates.get(&key).map(|v| rclite::Arc::clone(v.value()))
    }

    pub fn put_candidates_for_table_tokens(
        &self,
        table_name: &str,
        tokens: &SmallVec<[Token; 36]>,
        pointers: rclite::Arc<Vec<RowPointer>>,
    ) {
        let key = Self::key_from_tokens(table_name, tokens);
        self.candidates.insert(key, pointers);
    }

    pub fn remove_table_tokens(&self, table_name: &str, tokens: &SmallVec<[Token; 36]>) {
        let key = Self::key_from_tokens(table_name, tokens);
        self.candidates.remove(&key);
    }

    /// Returns cached candidates if present; otherwise attempts to build candidates
    /// from the persisted semantic rules for this table.
    ///
    /// If the query is too complex to safely map (OR, parens, non-numeric ops, etc.)
    /// or no rules are available, returns `None` so the caller can fall back to
    /// scanning all pointers.
    pub async fn get_or_build_candidates_for_table_tokens<S: StorageIO>(
        &self,
        table_name: &str,
        tokens: &SmallVec<[Token; 36]>,
        schema: &[SchemaField],
        io_rows: Arc<S>,
        pointers_io: Arc<S>,
    ) -> Option<rclite::Arc<Vec<RowPointer>>> {
        if let Some(cached) = self.get_candidates_for_table_tokens(table_name, tokens) {
            return Some(cached);
        }

        let rules_io = SemanticRuleStore::open_rules_io(io_rows, table_name).await;
        let (meta, rules) = SemanticRuleStore::load_meta_and_rules(rules_io).await.ok()?;
        if rules.is_empty() {
            return None;
        }

        let plan = RuleTransformerPlan::from_tokens(tokens, schema)?;
        if plan.predicates.is_empty() {
            return None;
        }

        // Index rules by column id for faster evaluation.
        let mut by_col: Vec<Vec<crate::core::sme::rules::SemanticRule>> = vec![Vec::new(); schema.len()];
        for r in rules.into_iter() {
            if r.op == crate::core::sme::rules::SemanticRuleOp::Meta {
                continue;
            }
            let idx = r.column_schema_id as usize;
            if idx < by_col.len() {
                by_col[idx].push(r);
            }
        }

        let intervals = plan.evaluate(&by_col, meta)?;
        if intervals.is_empty() {
            // Still allow scanning only appended rows if we have meta.
            if meta.is_none() {
                return None;
            }
        }
        let built = build_candidates_from_intervals(pointers_io, meta, &intervals)
            .await
            .ok()?;

        let arc = rclite::Arc::new(built);
        self.put_candidates_for_table_tokens(table_name, tokens, rclite::Arc::clone(&arc));
        Some(arc)
    }
}

#[derive(Debug, Clone)]
struct NumericConstraint {
    column_schema_id: u64,
    lower: Option<(NumericScalar, bool)>, // (value, inclusive)
    upper: Option<(NumericScalar, bool)>, // (value, inclusive)
}

#[derive(Debug, Clone, Copy)]
enum NumericScalar {
    Signed(i128),
    Unsigned(u128),
    Float(f64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RuleNext {
    And,
    Or,
}

#[derive(Debug, Clone)]
struct RulePredicate {
    constraint: NumericConstraint,
    next: Option<RuleNext>,
}

/// A transformer-like plan evaluated against semantic rules (not rows).
///
/// Semantics mirror `TransformerProcessor::evaluate_comparison_results`:
/// AND groups are evaluated left-to-right; OR splits groups and any satisfied group returns true.
/// Here, “satisfied” means “produces at least one candidate interval”.
#[derive(Debug, Clone)]
struct RuleTransformerPlan {
    predicates: Vec<RulePredicate>,
}

impl RuleTransformerPlan {
    fn from_tokens(tokens: &SmallVec<[Token; 36]>, schema: &[SchemaField]) -> Option<Self> {
        if tokens.iter().any(|t| matches!(t, Token::LPar | Token::RPar)) {
            return None;
        }

        let mut predicates = Vec::new();
        let mut i = 0usize;
        while i < tokens.len() {
            // Ident Op Number [Next]
            if let (Some(Token::Ident((_name, db_type, write_order))), Some(Token::Op(op)), Some(Token::Number(num))) =
                (tokens.get(i), tokens.get(i + 1), tokens.get(i + 2))
            {
                let schema_idx = schema
                    .iter()
                    .position(|f| f.write_order as u64 == *write_order)? as u64;

                let constraint = constraint_from_parts(schema_idx, db_type.clone(), op, num)?;
                let mut next: Option<RuleNext> = None;
                if let Some(Token::Next(s)) = tokens.get(i + 3) {
                    next = match s.as_str() {
                        "AND" => Some(RuleNext::And),
                        "OR" => Some(RuleNext::Or),
                        _ => None,
                    };
                    i += 4;
                } else {
                    i += 3;
                }
                predicates.push(RulePredicate { constraint, next });
                continue;
            }

            // Number Op Ident [Next]
            if let (Some(Token::Number(num)), Some(Token::Op(op)), Some(Token::Ident((_name, db_type, write_order)))) =
                (tokens.get(i), tokens.get(i + 1), tokens.get(i + 2))
            {
                let schema_idx = schema
                    .iter()
                    .position(|f| f.write_order as u64 == *write_order)? as u64;
                let flipped = flip_op(op);
                let constraint = constraint_from_parts(schema_idx, db_type.clone(), &flipped, num)?;
                let mut next: Option<RuleNext> = None;
                if let Some(Token::Next(s)) = tokens.get(i + 3) {
                    next = match s.as_str() {
                        "AND" => Some(RuleNext::And),
                        "OR" => Some(RuleNext::Or),
                        _ => None,
                    };
                    i += 4;
                } else {
                    i += 3;
                }
                predicates.push(RulePredicate { constraint, next });
                continue;
            }

            // Anything else: unsupported for SME.
            return None;
        }

        Some(Self { predicates })
    }

    fn evaluate(
        &self,
        rules_by_col: &[Vec<crate::core::sme::rules::SemanticRule>],
        meta: Option<ScanMeta>,
    ) -> Option<Vec<(u64, u64)>> {
        if self.predicates.is_empty() {
            return None;
        }

        // Evaluate as OR-of-AND-groups, producing intervals instead of booleans.
        let mut groups: Vec<Vec<(u64, u64)>> = Vec::new();
        let mut current_and: Option<Vec<(u64, u64)>> = None;

        for p in self.predicates.iter() {
            let col = p.constraint.column_schema_id as usize;
            if col >= rules_by_col.len() {
                return None;
            }

            let local = normalize_intervals(collect_matching_intervals(&rules_by_col[col], &p.constraint));
            current_and = Some(match current_and {
                None => local,
                Some(prev) => intersect_intervals(&prev, &local),
            });

            match p.next {
                Some(RuleNext::And) => {}
                Some(RuleNext::Or) => {
                    groups.push(current_and.take().unwrap_or_default());
                    current_and = None;
                }
                None => {
                    groups.push(current_and.take().unwrap_or_default());
                    break;
                }
            }
        }

        // Union all non-empty groups.
        let mut unioned: Vec<(u64, u64)> = Vec::new();
        for g in groups.into_iter() {
            if !g.is_empty() {
                unioned.extend(g);
            }
        }
        unioned = normalize_intervals(unioned);

        // If rules produce nothing, but meta exists, we still want appended rows.
        if unioned.is_empty() && meta.is_none() {
            return None;
        }

        Some(unioned)
    }
}

fn collect_matching_intervals(
    rules: &[crate::core::sme::rules::SemanticRule],
    c: &NumericConstraint,
) -> Vec<(u64, u64)> {
    if rules.is_empty() {
        return Vec::new();
    }

    // SIMD fast paths only apply to constraints that can be represented as i64/u64/f64.
    match &rules[0].column_type {
        DbType::I8 | DbType::I16 | DbType::I32 | DbType::I64 => {
            let lower = c.lower.and_then(|(v, inc)| numeric_scalar_to_i64(v).map(|vv| (vv, inc)));
            let upper = c.upper.and_then(|(v, inc)| numeric_scalar_to_i64(v).map(|vv| (vv, inc)));
            if lower.is_some() || upper.is_some() {
                if let (Some((l, true)), Some((u, true))) = (lower, upper) {
                    if l == u {
                        return simd_or_scalar_filter_eq_i64(rules, l);
                    }
                }
                return simd_or_scalar_filter_i64(rules, lower, upper);
            }
        }
        DbType::U8 | DbType::U16 | DbType::U32 | DbType::U64 => {
            let lower = c.lower.and_then(|(v, inc)| numeric_scalar_to_u64(v).map(|vv| (vv, inc)));
            let upper = c.upper.and_then(|(v, inc)| numeric_scalar_to_u64(v).map(|vv| (vv, inc)));
            if lower.is_some() || upper.is_some() {
                if let (Some((l, true)), Some((u, true))) = (lower, upper) {
                    if l == u {
                        return simd_or_scalar_filter_eq_u64(rules, l);
                    }
                }
                return simd_or_scalar_filter_u64(rules, lower, upper);
            }
        }
        DbType::F32 | DbType::F64 => {
            let lower = c.lower.and_then(|(v, inc)| numeric_scalar_to_f64(v).map(|vv| (vv, inc)));
            let upper = c.upper.and_then(|(v, inc)| numeric_scalar_to_f64(v).map(|vv| (vv, inc)));
            if lower.is_some() || upper.is_some() {
                if let (Some((l, true)), Some((u, true))) = (lower, upper) {
                    if (l - u).abs() < f64::EPSILON {
                        return simd_or_scalar_filter_eq_f64(rules, l);
                    }
                }
                return simd_or_scalar_filter_f64(rules, lower, upper);
            }
        }
        _ => {}
    }

    // Fallback: scalar rule checks.
    let mut out = Vec::new();
    for r in rules.iter() {
        if rule_might_match_constraint(r, c) {
            out.push((r.start_row_id, r.end_row_id));
        }
    }
    out
}

fn numeric_scalar_to_i64(v: NumericScalar) -> Option<i64> {
    match v {
        NumericScalar::Signed(x) => i64::try_from(x).ok(),
        NumericScalar::Unsigned(x) => i64::try_from(x).ok(),
        NumericScalar::Float(x) => {
            if x.is_finite() {
                i64::try_from(x as i128).ok()
            } else {
                None
            }
        }
    }
}

fn numeric_scalar_to_u64(v: NumericScalar) -> Option<u64> {
    match v {
        NumericScalar::Unsigned(x) => u64::try_from(x).ok(),
        NumericScalar::Signed(x) => u64::try_from(x).ok(),
        NumericScalar::Float(x) => {
            if x.is_finite() && x >= 0.0 {
                u64::try_from(x as u128).ok()
            } else {
                None
            }
        }
    }
}

fn numeric_scalar_to_f64(v: NumericScalar) -> Option<f64> {
    match v {
        NumericScalar::Float(x) => Some(x),
        NumericScalar::Signed(x) => Some(x as f64),
        NumericScalar::Unsigned(x) => Some(x as f64),
    }
}

fn decode_rule_i64(rule: &crate::core::sme::rules::SemanticRule) -> Option<(i64, i64)> {
    match rule.column_type {
        DbType::I8 | DbType::I16 | DbType::I32 | DbType::I64 => {
            let lo = i128::from_le_bytes(rule.lower);
            let hi = i128::from_le_bytes(rule.upper);
            Some((i64::try_from(lo).ok()?, i64::try_from(hi).ok()?))
        }
        DbType::U8 | DbType::U16 | DbType::U32 | DbType::U64 => {
            let lo = u128::from_le_bytes(rule.lower);
            let hi = u128::from_le_bytes(rule.upper);
            if lo > i64::MAX as u128 {
                // Entirely above i64 range.
                return Some((i64::MAX, i64::MIN));
            }
            let lo_i = i64::try_from(lo).ok()?;
            let hi_i = if hi > i64::MAX as u128 {
                i64::MAX
            } else {
                i64::try_from(hi).ok()?
            };
            Some((lo_i, hi_i))
        }
        DbType::F32 | DbType::F64 => {
            let lo_bits = u64::from_le_bytes(rule.lower[0..8].try_into().ok()?);
            let hi_bits = u64::from_le_bytes(rule.upper[0..8].try_into().ok()?);
            let lo_f = f64::from_bits(lo_bits);
            let hi_f = f64::from_bits(hi_bits);

            if hi_f < i64::MIN as f64 {
                return Some((i64::MAX, i64::MIN));
            }

            let lo_i = if lo_f <= i64::MIN as f64 {
                i64::MIN
            } else {
                if lo_f > i64::MAX as f64 {
                    return Some((i64::MAX, i64::MIN));
                }
                lo_f.ceil() as i64
            };

            let hi_i = if hi_f > i64::MAX as f64 {
                i64::MAX
            } else {
                hi_f.floor() as i64
            };

            Some((lo_i, hi_i))
        }
        _ => None,
    }
}

fn decode_rule_u64(rule: &crate::core::sme::rules::SemanticRule) -> Option<(u64, u64)> {
    match rule.column_type {
        DbType::U8 | DbType::U16 | DbType::U32 | DbType::U64 => {
            let lo = u128::from_le_bytes(rule.lower);
            let hi = u128::from_le_bytes(rule.upper);
            Some((u64::try_from(lo).ok()?, u64::try_from(hi).ok()?))
        }
        DbType::I8 | DbType::I16 | DbType::I32 | DbType::I64 => {
            let lo = i128::from_le_bytes(rule.lower);
            let hi = i128::from_le_bytes(rule.upper);
            if hi < 0 {
                // Entirely negative, cannot match any u64.
                return Some((u64::MAX, u64::MIN));
            }
            let lo_u = if lo < 0 { 0 } else { u64::try_from(lo).ok()? };
            let hi_u = if hi > u64::MAX as i128 {
                u64::MAX
            } else {
                u64::try_from(hi).ok()?
            };
            Some((lo_u, hi_u))
        }
        DbType::F32 | DbType::F64 => {
            let lo_bits = u64::from_le_bytes(rule.lower[0..8].try_into().ok()?);
            let hi_bits = u64::from_le_bytes(rule.upper[0..8].try_into().ok()?);
            let lo_f = f64::from_bits(lo_bits);
            let hi_f = f64::from_bits(hi_bits);

            if hi_f < 0.0 {
                return Some((u64::MAX, u64::MIN));
            }

            let lo_u = if lo_f <= 0.0 {
                0
            } else {
                if lo_f > u64::MAX as f64 {
                    return Some((u64::MAX, u64::MIN));
                }
                lo_f.ceil() as u64
            };

            let hi_u = if hi_f > u64::MAX as f64 {
                u64::MAX
            } else {
                hi_f.floor() as u64
            };

            Some((lo_u, hi_u))
        }
        _ => None,
    }
}

fn decode_rule_f64(rule: &crate::core::sme::rules::SemanticRule) -> Option<(f64, f64)> {
    match rule.column_type {
        DbType::F32 | DbType::F64 => {
            let lo_bits = u64::from_le_bytes(rule.lower[0..8].try_into().ok()?);
            let hi_bits = u64::from_le_bytes(rule.upper[0..8].try_into().ok()?);
            Some((f64::from_bits(lo_bits), f64::from_bits(hi_bits)))
        }
        _ => None,
    }
}

fn interval_i64_matches(lo: i64, hi: i64, lower: Option<(i64, bool)>, upper: Option<(i64, bool)>) -> bool {
    if let Some((b, inc)) = lower {
        if inc {
            if hi < b {
                return false;
            }
        } else if hi <= b {
            return false;
        }
    }
    if let Some((b, inc)) = upper {
        if inc {
            if lo > b {
                return false;
            }
        } else if lo >= b {
            return false;
        }
    }
    true
}

fn interval_u64_matches(lo: u64, hi: u64, lower: Option<(u64, bool)>, upper: Option<(u64, bool)>) -> bool {
    if let Some((b, inc)) = lower {
        if inc {
            if hi < b {
                return false;
            }
        } else if hi <= b {
            return false;
        }
    }
    if let Some((b, inc)) = upper {
        if inc {
            if lo > b {
                return false;
            }
        } else if lo >= b {
            return false;
        }
    }
    true
}

fn interval_f64_matches(lo: f64, hi: f64, lower: Option<(f64, bool)>, upper: Option<(f64, bool)>) -> bool {
    if let Some((b, inc)) = lower {
        if inc {
            if hi < b {
                return false;
            }
        } else if hi <= b {
            return false;
        }
    }
    if let Some((b, inc)) = upper {
        if inc {
            if lo > b {
                return false;
            }
        } else if lo >= b {
            return false;
        }
    }
    true
}

fn simd_or_scalar_filter_eq_i64(rules: &[crate::core::sme::rules::SemanticRule], val: i64) -> Vec<(u64, u64)> {
    #[cfg(all(target_arch = "x86_64"))]
    {
        if std::arch::is_x86_feature_detected!("avx2") {
            unsafe { return simd_filter_eq_i64_avx2(rules, val) }
        }
    }
    let mut out = Vec::new();
    for r in rules.iter() {
        if let Some((lo, hi)) = decode_rule_i64(r) {
            if val >= lo && val <= hi {
                out.push((r.start_row_id, r.end_row_id));
            }
        } else {
            out.push((r.start_row_id, r.end_row_id));
        }
    }
    out
}

fn simd_or_scalar_filter_eq_u64(rules: &[crate::core::sme::rules::SemanticRule], val: u64) -> Vec<(u64, u64)> {
    #[cfg(all(target_arch = "x86_64"))]
    {
        if std::arch::is_x86_feature_detected!("avx2") {
            unsafe { return simd_filter_eq_u64_avx2(rules, val) }
        }
    }
    let mut out = Vec::new();
    for r in rules.iter() {
        if let Some((lo, hi)) = decode_rule_u64(r) {
            if val >= lo && val <= hi {
                out.push((r.start_row_id, r.end_row_id));
            }
        } else {
            out.push((r.start_row_id, r.end_row_id));
        }
    }
    out
}

fn simd_or_scalar_filter_eq_f64(rules: &[crate::core::sme::rules::SemanticRule], val: f64) -> Vec<(u64, u64)> {
    #[cfg(all(target_arch = "x86_64"))]
    {
        if std::arch::is_x86_feature_detected!("avx2") {
            unsafe { return simd_filter_eq_f64_avx2(rules, val) }
        }
    }
    let mut out = Vec::new();
    for r in rules.iter() {
        if let Some((lo, hi)) = decode_rule_f64(r) {
            if val >= lo && val <= hi {
                out.push((r.start_row_id, r.end_row_id));
            }
        } else {
            out.push((r.start_row_id, r.end_row_id));
        }
    }
    out
}

#[cfg(target_arch = "x86_64")]
#[allow(unsafe_op_in_unsafe_fn)]
#[target_feature(enable = "avx2")]
unsafe fn simd_filter_eq_i64_avx2(
    rules: &[crate::core::sme::rules::SemanticRule],
    val: i64,
) -> Vec<(u64, u64)> {
    use std::arch::x86_64::*;
    let mut out = Vec::new();
    let mut i = 0usize;
    let val_v = _mm256_set1_epi64x(val);

    while i + 4 <= rules.len() {
        let mut lo_arr = [0i64; 4];
        let mut hi_arr = [0i64; 4];
        for lane in 0..4 {
            if let Some((lo, hi)) = decode_rule_i64(&rules[i + lane]) {
                lo_arr[lane] = lo;
                hi_arr[lane] = hi;
            } else {
                lo_arr[lane] = i64::MIN;
                hi_arr[lane] = i64::MAX;
            }
        }
        let lo_v = _mm256_loadu_si256(lo_arr.as_ptr() as *const __m256i);
        let hi_v = _mm256_loadu_si256(hi_arr.as_ptr() as *const __m256i);

        // We want: lo <= val && hi >= val
        // lo <= val  <=>  !(lo > val)  <=>  !cmpgt(lo, val)
        // hi >= val  <=>  !(val > hi)  <=>  !cmpgt(val, hi)
        
        let lo_gt_val = _mm256_cmpgt_epi64(lo_v, val_v);
        let val_gt_hi = _mm256_cmpgt_epi64(val_v, hi_v);
        let bad = _mm256_or_si256(lo_gt_val, val_gt_hi);
        let mask = !(_mm256_movemask_pd(_mm256_castsi256_pd(bad)) as u32);

        for lane in 0..4 {
            if (mask & (1u32 << lane)) != 0 {
                let r = &rules[i + lane];
                out.push((r.start_row_id, r.end_row_id));
            }
        }
        i += 4;
    }
    for r in rules[i..].iter() {
        if let Some((lo, hi)) = decode_rule_i64(r) {
            if val >= lo && val <= hi {
                out.push((r.start_row_id, r.end_row_id));
            }
        } else {
            out.push((r.start_row_id, r.end_row_id));
        }
    }
    out
}

#[cfg(target_arch = "x86_64")]
#[allow(unsafe_op_in_unsafe_fn)]
#[target_feature(enable = "avx2")]
unsafe fn simd_filter_eq_u64_avx2(
    rules: &[crate::core::sme::rules::SemanticRule],
    val: u64,
) -> Vec<(u64, u64)> {
    use std::arch::x86_64::*;
    let mut out = Vec::new();
    let mut i = 0usize;
    let val_i = u64_ordered_to_i64(val);
    let val_v = _mm256_set1_epi64x(val_i);

    while i + 4 <= rules.len() {
        let mut lo_arr = [0i64; 4];
        let mut hi_arr = [0i64; 4];
        for lane in 0..4 {
            if let Some((lo, hi)) = decode_rule_u64(&rules[i + lane]) {
                lo_arr[lane] = u64_ordered_to_i64(lo);
                hi_arr[lane] = u64_ordered_to_i64(hi);
            } else {
                lo_arr[lane] = i64::MIN;
                hi_arr[lane] = i64::MAX;
            }
        }
        let lo_v = _mm256_loadu_si256(lo_arr.as_ptr() as *const __m256i);
        let hi_v = _mm256_loadu_si256(hi_arr.as_ptr() as *const __m256i);

        // We want: lo <= val && hi >= val
        // Using signed comparison on ordered-mapped values.
        let lo_gt_val = _mm256_cmpgt_epi64(lo_v, val_v);
        let val_gt_hi = _mm256_cmpgt_epi64(val_v, hi_v);
        let bad = _mm256_or_si256(lo_gt_val, val_gt_hi);
        let mask = !(_mm256_movemask_pd(_mm256_castsi256_pd(bad)) as u32);

        for lane in 0..4 {
            if (mask & (1u32 << lane)) != 0 {
                let r = &rules[i + lane];
                out.push((r.start_row_id, r.end_row_id));
            }
        }
        i += 4;
    }
    for r in rules[i..].iter() {
        if let Some((lo, hi)) = decode_rule_u64(r) {
            if val >= lo && val <= hi {
                out.push((r.start_row_id, r.end_row_id));
            }
        } else {
            out.push((r.start_row_id, r.end_row_id));
        }
    }
    out
}

#[cfg(target_arch = "x86_64")]
#[allow(unsafe_op_in_unsafe_fn)]
#[target_feature(enable = "avx2")]
unsafe fn simd_filter_eq_f64_avx2(
    rules: &[crate::core::sme::rules::SemanticRule],
    val: f64,
) -> Vec<(u64, u64)> {
    use std::arch::x86_64::*;
    let mut out = Vec::new();
    let mut i = 0usize;
    let val_v = _mm256_set1_pd(val);

    while i + 4 <= rules.len() {
        let mut lo_arr = [0f64; 4];
        let mut hi_arr = [0f64; 4];
        for lane in 0..4 {
            if let Some((lo, hi)) = decode_rule_f64(&rules[i + lane]) {
                lo_arr[lane] = lo;
                hi_arr[lane] = hi;
            } else {
                lo_arr[lane] = f64::NEG_INFINITY;
                hi_arr[lane] = f64::INFINITY;
            }
        }
        let lo_v = _mm256_loadu_pd(lo_arr.as_ptr());
        let hi_v = _mm256_loadu_pd(hi_arr.as_ptr());

        // We want: lo <= val && hi >= val
        // _mm256_cmp_pd with _CMP_LE_OQ (Less-equal, ordered, non-signaling)
        // lo <= val
        let lo_le_val = _mm256_cmp_pd(lo_v, val_v, _CMP_LE_OQ);
        // val <= hi  <=>  hi >= val
        let val_le_hi = _mm256_cmp_pd(val_v, hi_v, _CMP_LE_OQ);
        
        let good = _mm256_and_pd(lo_le_val, val_le_hi);
        let mask = _mm256_movemask_pd(good) as u32;

        for lane in 0..4 {
            if (mask & (1u32 << lane)) != 0 {
                let r = &rules[i + lane];
                out.push((r.start_row_id, r.end_row_id));
            }
        }
        i += 4;
    }
    for r in rules[i..].iter() {
        if let Some((lo, hi)) = decode_rule_f64(r) {
            if val >= lo && val <= hi {
                out.push((r.start_row_id, r.end_row_id));
            }
        } else {
            out.push((r.start_row_id, r.end_row_id));
        }
    }
    out
}

fn simd_or_scalar_filter_i64(
    rules: &[crate::core::sme::rules::SemanticRule],
    lower: Option<(i64, bool)>,
    upper: Option<(i64, bool)>,
) -> Vec<(u64, u64)> {
    #[cfg(all(target_arch = "x86_64"))]
    {
        if std::arch::is_x86_feature_detected!("avx512f") {
            // SAFETY: guarded by runtime feature detection.
            unsafe { return simd_filter_i64_avx512(rules, lower, upper) }
        }
        if std::arch::is_x86_feature_detected!("avx2") {
            // SAFETY: guarded by runtime feature detection.
            unsafe { return simd_filter_i64_avx2(rules, lower, upper) }
        }
    }

    let mut out = Vec::new();
    for r in rules.iter() {
        if let Some((lo, hi)) = decode_rule_i64(r) {
            if interval_i64_matches(lo, hi, lower, upper) {
                out.push((r.start_row_id, r.end_row_id));
            }
        } else {
            // Conservative fallback if a rule can't be decoded in this fast-path.
            out.push((r.start_row_id, r.end_row_id));
        }
    }
    out
}

fn simd_or_scalar_filter_u64(
    rules: &[crate::core::sme::rules::SemanticRule],
    lower: Option<(u64, bool)>,
    upper: Option<(u64, bool)>,
) -> Vec<(u64, u64)> {
    #[cfg(all(target_arch = "x86_64"))]
    {
        if std::arch::is_x86_feature_detected!("avx512f") {
            unsafe { return simd_filter_u64_avx512(rules, lower, upper) }
        }
        if std::arch::is_x86_feature_detected!("avx2") {
            unsafe { return simd_filter_u64_avx2(rules, lower, upper) }
        }
    }

    let mut out = Vec::new();
    for r in rules.iter() {
        if let Some((lo, hi)) = decode_rule_u64(r) {
            if interval_u64_matches(lo, hi, lower, upper) {
                out.push((r.start_row_id, r.end_row_id));
            }
        }
    }
    out
}

fn simd_or_scalar_filter_f64(
    rules: &[crate::core::sme::rules::SemanticRule],
    lower: Option<(f64, bool)>,
    upper: Option<(f64, bool)>,
) -> Vec<(u64, u64)> {
    #[cfg(all(target_arch = "x86_64"))]
    {
        if std::arch::is_x86_feature_detected!("avx512f") {
            unsafe { return simd_filter_f64_avx512(rules, lower, upper) }
        }
        if std::arch::is_x86_feature_detected!("avx2") {
            unsafe { return simd_filter_f64_avx2(rules, lower, upper) }
        }
    }

    let mut out = Vec::new();
    for r in rules.iter() {
        if let Some((lo, hi)) = decode_rule_f64(r) {
            if interval_f64_matches(lo, hi, lower, upper) {
                out.push((r.start_row_id, r.end_row_id));
            }
        }
    }
    out
}

#[cfg(target_arch = "x86_64")]
#[allow(unsafe_op_in_unsafe_fn)]
#[target_feature(enable = "avx2")]
unsafe fn simd_filter_i64_avx2(
    rules: &[crate::core::sme::rules::SemanticRule],
    lower: Option<(i64, bool)>,
    upper: Option<(i64, bool)>,
) -> Vec<(u64, u64)> {
    use std::arch::x86_64::*;

    let mut out = Vec::new();
    let mut i = 0usize;
    while i + 4 <= rules.len() {
        let mut lo_arr = [0i64; 4];
        let mut hi_arr = [0i64; 4];
        for lane in 0..4 {
            if let Some((lo, hi)) = decode_rule_i64(&rules[i + lane]) {
                lo_arr[lane] = lo;
                hi_arr[lane] = hi;
            } else {
                // Force match conservatively.
                lo_arr[lane] = i64::MIN;
                hi_arr[lane] = i64::MAX;
            }
        }

        let lo_v = _mm256_loadu_si256(lo_arr.as_ptr() as *const __m256i);
        let hi_v = _mm256_loadu_si256(hi_arr.as_ptr() as *const __m256i);

        let mut mask = !0u32; // 4 lanes.
        if let Some((b, inc)) = lower {
            let b_v = _mm256_set1_epi64x(b);
            // lower_ok: hi >= b (inclusive) OR hi > b (exclusive)
            let hi_gt_b = _mm256_cmpgt_epi64(hi_v, b_v);
            let hi_eq_b = _mm256_cmpeq_epi64(hi_v, b_v);
            let lower_ok = if inc { _mm256_or_si256(hi_gt_b, hi_eq_b) } else { hi_gt_b };
            mask &= _mm256_movemask_pd(_mm256_castsi256_pd(lower_ok)) as u32;
        }
        if let Some((b, inc)) = upper {
            let b_v = _mm256_set1_epi64x(b);
            // upper_ok: lo <= b (inclusive) OR lo < b (exclusive)
            let lo_gt_b = _mm256_cmpgt_epi64(lo_v, b_v);
            let lo_eq_b = _mm256_cmpeq_epi64(lo_v, b_v);
            let lo_le_b = _mm256_xor_si256(_mm256_set1_epi64x(-1), lo_gt_b);
            let upper_ok = if inc { lo_le_b } else { _mm256_andnot_si256(lo_eq_b, lo_le_b) };
            mask &= _mm256_movemask_pd(_mm256_castsi256_pd(upper_ok)) as u32;
        }

        for lane in 0..4 {
            if (mask & (1u32 << lane)) != 0 {
                let r = &rules[i + lane];
                out.push((r.start_row_id, r.end_row_id));
            }
        }

        i += 4;
    }

    for r in rules[i..].iter() {
        if let Some((lo, hi)) = decode_rule_i64(r) {
            if interval_i64_matches(lo, hi, lower, upper) {
                out.push((r.start_row_id, r.end_row_id));
            }
        }
    }

    out
}

#[cfg(target_arch = "x86_64")]
#[allow(unsafe_op_in_unsafe_fn)]
#[target_feature(enable = "avx512f")]
unsafe fn simd_filter_i64_avx512(
    rules: &[crate::core::sme::rules::SemanticRule],
    lower: Option<(i64, bool)>,
    upper: Option<(i64, bool)>,
) -> Vec<(u64, u64)> {
    use std::arch::x86_64::*;

    let mut out = Vec::new();
    let mut i = 0usize;
    while i + 8 <= rules.len() {
        let mut lo_arr = [0i64; 8];
        let mut hi_arr = [0i64; 8];
        for lane in 0..8 {
            if let Some((lo, hi)) = decode_rule_i64(&rules[i + lane]) {
                lo_arr[lane] = lo;
                hi_arr[lane] = hi;
            } else {
                lo_arr[lane] = i64::MIN;
                hi_arr[lane] = i64::MAX;
            }
        }

        let lo_v = _mm512_loadu_si512(lo_arr.as_ptr() as *const _);
        let hi_v = _mm512_loadu_si512(hi_arr.as_ptr() as *const _);

        let mut mask: u8 = 0xFF;
        if let Some((b, inc)) = lower {
            let b_v = _mm512_set1_epi64(b);
            let gt = _mm512_cmpgt_epi64_mask(hi_v, b_v);
            let eq = _mm512_cmpeq_epi64_mask(hi_v, b_v);
            let lower_ok = if inc { gt | eq } else { gt };
            mask &= lower_ok as u8;
        }
        if let Some((b, inc)) = upper {
            let b_v = _mm512_set1_epi64(b);
            let lt = _mm512_cmpgt_epi64_mask(b_v, lo_v);
            let eq = _mm512_cmpeq_epi64_mask(lo_v, b_v);
            let upper_ok = if inc { lt | eq } else { lt };
            mask &= upper_ok as u8;
        }

        for lane in 0..8 {
            if (mask & (1u8 << lane)) != 0 {
                let r = &rules[i + lane];
                out.push((r.start_row_id, r.end_row_id));
            }
        }

        i += 8;
    }

    for r in rules[i..].iter() {
        if let Some((lo, hi)) = decode_rule_i64(r) {
            if interval_i64_matches(lo, hi, lower, upper) {
                out.push((r.start_row_id, r.end_row_id));
            }
        }
    }

    out
}

#[cfg(target_arch = "x86_64")]
#[inline]
fn u64_ordered_to_i64(x: u64) -> i64 {
    (x ^ 0x8000_0000_0000_0000u64) as i64
}

#[cfg(target_arch = "x86_64")]
#[allow(unsafe_op_in_unsafe_fn)]
#[target_feature(enable = "avx2")]
unsafe fn simd_filter_u64_avx2(
    rules: &[crate::core::sme::rules::SemanticRule],
    lower: Option<(u64, bool)>,
    upper: Option<(u64, bool)>,
) -> Vec<(u64, u64)> {
    use std::arch::x86_64::*;

    let mut out = Vec::new();
    let mut i = 0usize;
    while i + 4 <= rules.len() {
        let mut lo_arr = [0i64; 4];
        let mut hi_arr = [0i64; 4];
        for lane in 0..4 {
            if let Some((lo, hi)) = decode_rule_u64(&rules[i + lane]) {
                lo_arr[lane] = u64_ordered_to_i64(lo);
                hi_arr[lane] = u64_ordered_to_i64(hi);
            } else {
                lo_arr[lane] = i64::MIN;
                hi_arr[lane] = i64::MAX;
            }
        }

        let lo_v = _mm256_loadu_si256(lo_arr.as_ptr() as *const __m256i);
        let hi_v = _mm256_loadu_si256(hi_arr.as_ptr() as *const __m256i);

        let mut mask = !0u32;
        if let Some((b_u, inc)) = lower {
            let b = u64_ordered_to_i64(b_u);
            let b_v = _mm256_set1_epi64x(b);
            let hi_gt_b = _mm256_cmpgt_epi64(hi_v, b_v);
            let hi_eq_b = _mm256_cmpeq_epi64(hi_v, b_v);
            let lower_ok = if inc { _mm256_or_si256(hi_gt_b, hi_eq_b) } else { hi_gt_b };
            mask &= _mm256_movemask_pd(_mm256_castsi256_pd(lower_ok)) as u32;
        }
        if let Some((b_u, inc)) = upper {
            let b = u64_ordered_to_i64(b_u);
            let b_v = _mm256_set1_epi64x(b);
            let lo_gt_b = _mm256_cmpgt_epi64(lo_v, b_v);
            let lo_eq_b = _mm256_cmpeq_epi64(lo_v, b_v);
            let lo_le_b = _mm256_xor_si256(_mm256_set1_epi64x(-1), lo_gt_b);
            let upper_ok = if inc { lo_le_b } else { _mm256_andnot_si256(lo_eq_b, lo_le_b) };
            mask &= _mm256_movemask_pd(_mm256_castsi256_pd(upper_ok)) as u32;
        }

        for lane in 0..4 {
            if (mask & (1u32 << lane)) != 0 {
                let r = &rules[i + lane];
                out.push((r.start_row_id, r.end_row_id));
            }
        }
        i += 4;
    }

    for r in rules[i..].iter() {
        if let Some((lo, hi)) = decode_rule_u64(r) {
            if interval_u64_matches(lo, hi, lower, upper) {
                out.push((r.start_row_id, r.end_row_id));
            }
        }
    }
    out
}

#[cfg(target_arch = "x86_64")]
#[allow(unsafe_op_in_unsafe_fn)]
#[target_feature(enable = "avx512f")]
unsafe fn simd_filter_u64_avx512(
    rules: &[crate::core::sme::rules::SemanticRule],
    lower: Option<(u64, bool)>,
    upper: Option<(u64, bool)>,
) -> Vec<(u64, u64)> {
    use std::arch::x86_64::*;

    let mut out = Vec::new();
    let mut i = 0usize;
    while i + 8 <= rules.len() {
        let mut lo_arr = [0i64; 8];
        let mut hi_arr = [0i64; 8];
        for lane in 0..8 {
            if let Some((lo, hi)) = decode_rule_u64(&rules[i + lane]) {
                lo_arr[lane] = u64_ordered_to_i64(lo);
                hi_arr[lane] = u64_ordered_to_i64(hi);
            } else {
                lo_arr[lane] = i64::MIN;
                hi_arr[lane] = i64::MAX;
            }
        }

        let lo_v = _mm512_loadu_si512(lo_arr.as_ptr() as *const _);
        let hi_v = _mm512_loadu_si512(hi_arr.as_ptr() as *const _);

        let mut mask: u8 = 0xFF;
        if let Some((b_u, inc)) = lower {
            let b = u64_ordered_to_i64(b_u);
            let b_v = _mm512_set1_epi64(b);
            let gt = _mm512_cmpgt_epi64_mask(hi_v, b_v);
            let eq = _mm512_cmpeq_epi64_mask(hi_v, b_v);
            let lower_ok = if inc { gt | eq } else { gt };
            mask &= lower_ok as u8;
        }
        if let Some((b_u, inc)) = upper {
            let b = u64_ordered_to_i64(b_u);
            let b_v = _mm512_set1_epi64(b);
            let lt = _mm512_cmpgt_epi64_mask(b_v, lo_v);
            let eq = _mm512_cmpeq_epi64_mask(lo_v, b_v);
            let upper_ok = if inc { lt | eq } else { lt };
            mask &= upper_ok as u8;
        }

        for lane in 0..8 {
            if (mask & (1u8 << lane)) != 0 {
                let r = &rules[i + lane];
                out.push((r.start_row_id, r.end_row_id));
            }
        }

        i += 8;
    }

    for r in rules[i..].iter() {
        if let Some((lo, hi)) = decode_rule_u64(r) {
            if interval_u64_matches(lo, hi, lower, upper) {
                out.push((r.start_row_id, r.end_row_id));
            }
        }
    }

    out
}

#[cfg(target_arch = "x86_64")]
#[allow(unsafe_op_in_unsafe_fn)]
#[target_feature(enable = "avx2")]
unsafe fn simd_filter_f64_avx2(
    rules: &[crate::core::sme::rules::SemanticRule],
    lower: Option<(f64, bool)>,
    upper: Option<(f64, bool)>,
) -> Vec<(u64, u64)> {
    use std::arch::x86_64::*;
    let mut out = Vec::new();
    let mut i = 0usize;
    while i + 4 <= rules.len() {
        let mut lo_arr = [0f64; 4];
        let mut hi_arr = [0f64; 4];
        for lane in 0..4 {
            if let Some((lo, hi)) = decode_rule_f64(&rules[i + lane]) {
                lo_arr[lane] = lo;
                hi_arr[lane] = hi;
            } else {
                lo_arr[lane] = f64::NEG_INFINITY;
                hi_arr[lane] = f64::INFINITY;
            }
        }

        let lo_v = _mm256_loadu_pd(lo_arr.as_ptr());
        let hi_v = _mm256_loadu_pd(hi_arr.as_ptr());

        let mut mask = !0u32;
        if let Some((b, inc)) = lower {
            let b_v = _mm256_set1_pd(b);
            let lower_ok = if inc {
                _mm256_cmp_pd(hi_v, b_v, _CMP_GE_OQ)
            } else {
                _mm256_cmp_pd(hi_v, b_v, _CMP_GT_OQ)
            };
            mask &= _mm256_movemask_pd(lower_ok) as u32;
        }
        if let Some((b, inc)) = upper {
            let b_v = _mm256_set1_pd(b);
            let upper_ok = if inc {
                _mm256_cmp_pd(lo_v, b_v, _CMP_LE_OQ)
            } else {
                _mm256_cmp_pd(lo_v, b_v, _CMP_LT_OQ)
            };
            mask &= _mm256_movemask_pd(upper_ok) as u32;
        }

        for lane in 0..4 {
            if (mask & (1u32 << lane)) != 0 {
                let r = &rules[i + lane];
                out.push((r.start_row_id, r.end_row_id));
            }
        }
        i += 4;
    }

    for r in rules[i..].iter() {
        if let Some((lo, hi)) = decode_rule_f64(r) {
            if interval_f64_matches(lo, hi, lower, upper) {
                out.push((r.start_row_id, r.end_row_id));
            }
        }
    }
    out
}

#[cfg(target_arch = "x86_64")]
#[allow(unsafe_op_in_unsafe_fn)]
#[target_feature(enable = "avx512f")]
unsafe fn simd_filter_f64_avx512(
    rules: &[crate::core::sme::rules::SemanticRule],
    lower: Option<(f64, bool)>,
    upper: Option<(f64, bool)>,
) -> Vec<(u64, u64)> {
    use std::arch::x86_64::*;
    let mut out = Vec::new();
    let mut i = 0usize;
    while i + 8 <= rules.len() {
        let mut lo_arr = [0f64; 8];
        let mut hi_arr = [0f64; 8];
        for lane in 0..8 {
            if let Some((lo, hi)) = decode_rule_f64(&rules[i + lane]) {
                lo_arr[lane] = lo;
                hi_arr[lane] = hi;
            } else {
                lo_arr[lane] = f64::NEG_INFINITY;
                hi_arr[lane] = f64::INFINITY;
            }
        }

        let lo_v = _mm512_loadu_pd(lo_arr.as_ptr());
        let hi_v = _mm512_loadu_pd(hi_arr.as_ptr());

        let mut mask: u8 = 0xFF;
        if let Some((b, inc)) = lower {
            let b_v = _mm512_set1_pd(b);
            let lower_ok = if inc {
                _mm512_cmp_pd_mask(hi_v, b_v, _CMP_GE_OQ)
            } else {
                _mm512_cmp_pd_mask(hi_v, b_v, _CMP_GT_OQ)
            };
            mask &= lower_ok as u8;
        }
        if let Some((b, inc)) = upper {
            let b_v = _mm512_set1_pd(b);
            let upper_ok = if inc {
                _mm512_cmp_pd_mask(lo_v, b_v, _CMP_LE_OQ)
            } else {
                _mm512_cmp_pd_mask(lo_v, b_v, _CMP_LT_OQ)
            };
            mask &= upper_ok as u8;
        }

        for lane in 0..8 {
            if (mask & (1u8 << lane)) != 0 {
                let r = &rules[i + lane];
                out.push((r.start_row_id, r.end_row_id));
            }
        }

        i += 8;
    }

    for r in rules[i..].iter() {
        if let Some((lo, hi)) = decode_rule_f64(r) {
            if interval_f64_matches(lo, hi, lower, upper) {
                out.push((r.start_row_id, r.end_row_id));
            }
        }
    }
    out
}

fn hash_tokens(tokens: &SmallVec<[Token; 36]>) -> u64 {
    let mut d = CRC_64.digest();

    for t in tokens.iter() {
        match t {
            Token::Ident((name, db_type, write_order)) => {
                d.update(&[0x01]);
                d.update(name.as_bytes());
                d.update(&[0x00]);
                d.update(format!("{}", db_type).as_bytes());
                d.update(&[0x00]);
                d.update(&write_order.to_le_bytes());
            }
            Token::Number(n) => {
                d.update(&[0x02]);
                match n {
                    NumericValue::I8(v) => d.update(&(*v as i128).to_le_bytes()),
                    NumericValue::I16(v) => d.update(&(*v as i128).to_le_bytes()),
                    NumericValue::I32(v) => d.update(&(*v as i128).to_le_bytes()),
                    NumericValue::I64(v) => d.update(&(*v as i128).to_le_bytes()),
                    NumericValue::I128(v) => d.update(&v.to_le_bytes()),
                    NumericValue::U8(v) => d.update(&(*v as u128).to_le_bytes()),
                    NumericValue::U16(v) => d.update(&(*v as u128).to_le_bytes()),
                    NumericValue::U32(v) => d.update(&(*v as u128).to_le_bytes()),
                    NumericValue::U64(v) => d.update(&(*v as u128).to_le_bytes()),
                    NumericValue::U128(v) => d.update(&v.to_le_bytes()),
                    NumericValue::F32(v) => d.update(&(*v as f64).to_le_bytes()),
                    NumericValue::F64(v) => d.update(&v.to_le_bytes()),
                }
            }
            Token::StringLit(s) => {
                d.update(&[0x03]);
                d.update(s.as_bytes());
            }
            Token::Op(s) => {
                d.update(&[0x04]);
                d.update(s.as_bytes());
            }
            Token::Next(s) => {
                d.update(&[0x05]);
                d.update(s.as_bytes());
            }
            Token::LPar => d.update(&[0x06]),
            Token::RPar => d.update(&[0x07]),
        }
        d.update(&[0xFF]);
    }

    d.finalize()
}

fn flip_op(op: &str) -> String {
    match op {
        "<" => ">".to_string(),
        "<=" => ">=".to_string(),
        ">" => "<".to_string(),
        ">=" => "<=".to_string(),
        other => other.to_string(),
    }
}

fn constraint_from_parts(
    column_schema_id: u64,
    db_type: DbType,
    op: &str,
    num: &NumericValue,
) -> Option<NumericConstraint> {
    let scalar = coerce_numeric(db_type.clone(), num)?;

    match op {
        "=" | "==" => Some(NumericConstraint {
            column_schema_id,
            lower: Some((scalar, true)),
            upper: Some((scalar, true)),
        }),
        "<" => Some(NumericConstraint {
            column_schema_id,
            lower: None,
            upper: Some((scalar, false)),
        }),
        "<=" => Some(NumericConstraint {
            column_schema_id,
            lower: None,
            upper: Some((scalar, true)),
        }),
        ">" => Some(NumericConstraint {
            column_schema_id,
            lower: Some((scalar, false)),
            upper: None,
        }),
        ">=" => Some(NumericConstraint {
            column_schema_id,
            lower: Some((scalar, true)),
            upper: None,
        }),
        _ => None,
    }
}

fn coerce_numeric(db_type: DbType, num: &NumericValue) -> Option<NumericScalar> {
    match db_type {
        DbType::I8
        | DbType::I16
        | DbType::I32
        | DbType::I64
        | DbType::I128 => {
            let v = match num {
                NumericValue::I8(v) => *v as i128,
                NumericValue::I16(v) => *v as i128,
                NumericValue::I32(v) => *v as i128,
                NumericValue::I64(v) => *v as i128,
                NumericValue::I128(v) => *v,
                NumericValue::U8(v) => *v as i128,
                NumericValue::U16(v) => *v as i128,
                NumericValue::U32(v) => *v as i128,
                NumericValue::U64(v) => (*v).try_into().ok()?,
                NumericValue::U128(v) => (*v).try_into().ok()?,
                NumericValue::F32(v) => *v as i128,
                NumericValue::F64(v) => *v as i128,
            };
            Some(NumericScalar::Signed(v))
        }
        DbType::U8
        | DbType::U16
        | DbType::U32
        | DbType::U64
        | DbType::U128 => {
            let v = match num {
                NumericValue::I8(v) => u128::try_from(*v as i128).ok()?,
                NumericValue::I16(v) => u128::try_from(*v as i128).ok()?,
                NumericValue::I32(v) => u128::try_from(*v as i128).ok()?,
                NumericValue::I64(v) => u128::try_from(*v as i128).ok()?,
                NumericValue::I128(v) => u128::try_from(*v).ok()?,
                NumericValue::U8(v) => *v as u128,
                NumericValue::U16(v) => *v as u128,
                NumericValue::U32(v) => *v as u128,
                NumericValue::U64(v) => *v as u128,
                NumericValue::U128(v) => *v,
                NumericValue::F32(v) => u128::try_from(*v as i128).ok()?,
                NumericValue::F64(v) => u128::try_from(*v as i128).ok()?,
            };
            Some(NumericScalar::Unsigned(v))
        }
        DbType::F32 | DbType::F64 => {
            let v = match num {
                NumericValue::I8(v) => *v as f64,
                NumericValue::I16(v) => *v as f64,
                NumericValue::I32(v) => *v as f64,
                NumericValue::I64(v) => *v as f64,
                NumericValue::I128(v) => *v as f64,
                NumericValue::U8(v) => *v as f64,
                NumericValue::U16(v) => *v as f64,
                NumericValue::U32(v) => *v as f64,
                NumericValue::U64(v) => *v as f64,
                NumericValue::U128(v) => *v as f64,
                NumericValue::F32(v) => *v as f64,
                NumericValue::F64(v) => *v,
            };
            Some(NumericScalar::Float(v))
        }
        _ => None,
    }
}

fn rule_might_match_constraint(rule: &crate::core::sme::rules::SemanticRule, c: &NumericConstraint) -> bool {
    use crate::core::sme::rules::SemanticRuleOp;

    // Convert rule bounds into a scalar interval.
    let (rule_lo, rule_hi) = match rule.column_type {
        DbType::I8 | DbType::I16 | DbType::I32 | DbType::I64 | DbType::I128 => {
            let lo = i128::from_le_bytes(rule.lower);
            let hi = i128::from_le_bytes(rule.upper);
            (NumericScalar::Signed(lo), NumericScalar::Signed(hi))
        }
        DbType::U8 | DbType::U16 | DbType::U32 | DbType::U64 | DbType::U128 => {
            let lo = u128::from_le_bytes(rule.lower);
            let hi = u128::from_le_bytes(rule.upper);
            (NumericScalar::Unsigned(lo), NumericScalar::Unsigned(hi))
        }
        DbType::F32 | DbType::F64 => {
            let lo_bits = u64::from_le_bytes(rule.lower[0..8].try_into().unwrap());
            let hi_bits = u64::from_le_bytes(rule.upper[0..8].try_into().unwrap());
            (NumericScalar::Float(f64::from_bits(lo_bits)), NumericScalar::Float(f64::from_bits(hi_bits)))
        }
        _ => return true,
    };

    // For now, treat Meta as non-filtering.
    if rule.op == SemanticRuleOp::Meta {
        return true;
    }

    interval_intersects_constraint(rule_lo, rule_hi, c)
}

fn interval_intersects_constraint(lo: NumericScalar, hi: NumericScalar, c: &NumericConstraint) -> bool {
    // Conservative: if any overlap exists between rule interval and constraint interval.
    match (lo, hi) {
        (NumericScalar::Signed(lo), NumericScalar::Signed(hi)) => {
            let (clo, clo_inc) = match c.lower {
                Some((NumericScalar::Signed(v), inc)) => (Some(v), inc),
                Some((NumericScalar::Unsigned(v), inc)) => (Some(v as i128), inc),
                Some((NumericScalar::Float(v), inc)) => (Some(v as i128), inc),
                None => (None, false),
            };
            let (chi, chi_inc) = match c.upper {
                Some((NumericScalar::Signed(v), inc)) => (Some(v), inc),
                Some((NumericScalar::Unsigned(v), inc)) => (Some(v as i128), inc),
                Some((NumericScalar::Float(v), inc)) => (Some(v as i128), inc),
                None => (None, false),
            };

            if let Some(bound) = clo {
                if clo_inc {
                    if hi < bound {
                        return false;
                    }
                } else if hi <= bound {
                    return false;
                }
            }
            if let Some(bound) = chi {
                if chi_inc {
                    if lo > bound {
                        return false;
                    }
                } else if lo >= bound {
                    return false;
                }
            }
            true
        }
        (NumericScalar::Unsigned(lo), NumericScalar::Unsigned(hi)) => {
            let (clo, clo_inc) = match c.lower {
                Some((NumericScalar::Unsigned(v), inc)) => (Some(v), inc),
                Some((NumericScalar::Signed(v), inc)) => (u128::try_from(v).ok(), inc),
                Some((NumericScalar::Float(v), inc)) => (u128::try_from(v as i128).ok(), inc),
                None => (None, false),
            };
            let (chi, chi_inc) = match c.upper {
                Some((NumericScalar::Unsigned(v), inc)) => (Some(v), inc),
                Some((NumericScalar::Signed(v), inc)) => (u128::try_from(v).ok(), inc),
                Some((NumericScalar::Float(v), inc)) => (u128::try_from(v as i128).ok(), inc),
                None => (None, false),
            };

            if let Some(bound) = clo {
                if clo_inc {
                    if hi < bound {
                        return false;
                    }
                } else if hi <= bound {
                    return false;
                }
            }
            if let Some(bound) = chi {
                if chi_inc {
                    if lo > bound {
                        return false;
                    }
                } else if lo >= bound {
                    return false;
                }
            }
            true
        }
        (NumericScalar::Float(lo), NumericScalar::Float(hi)) => {
            let (clo, clo_inc) = match c.lower {
                Some((NumericScalar::Float(v), inc)) => (Some(v), inc),
                Some((NumericScalar::Signed(v), inc)) => (Some(v as f64), inc),
                Some((NumericScalar::Unsigned(v), inc)) => (Some(v as f64), inc),
                None => (None, false),
            };
            let (chi, chi_inc) = match c.upper {
                Some((NumericScalar::Float(v), inc)) => (Some(v), inc),
                Some((NumericScalar::Signed(v), inc)) => (Some(v as f64), inc),
                Some((NumericScalar::Unsigned(v), inc)) => (Some(v as f64), inc),
                None => (None, false),
            };

            if let Some(bound) = clo {
                if clo_inc {
                    if hi < bound {
                        return false;
                    }
                } else if hi <= bound {
                    return false;
                }
            }
            if let Some(bound) = chi {
                if chi_inc {
                    if lo > bound {
                        return false;
                    }
                } else if lo >= bound {
                    return false;
                }
            }
            true
        }
        _ => true,
    }
}

fn normalize_intervals(mut v: Vec<(u64, u64)>) -> Vec<(u64, u64)> {
    if v.is_empty() {
        return v;
    }
    v.sort_unstable_by_key(|x| x.0);
    let mut out = Vec::with_capacity(v.len());
    let mut cur = v[0];
    for (s, e) in v.into_iter().skip(1) {
        if s <= cur.1.saturating_add(1) {
            cur.1 = cur.1.max(e);
        } else {
            out.push(cur);
            cur = (s, e);
        }
    }
    out.push(cur);
    out
}

fn intersect_intervals(a: &[(u64, u64)], b: &[(u64, u64)]) -> Vec<(u64, u64)> {
    let mut out = Vec::new();
    let mut i = 0usize;
    let mut j = 0usize;
    while i < a.len() && j < b.len() {
        let (a0, a1) = a[i];
        let (b0, b1) = b[j];

        let s = a0.max(b0);
        let e = a1.min(b1);
        if s <= e {
            out.push((s, e));
        }

        if a1 < b1 {
            i += 1;
        } else {
            j += 1;
        }
    }
    normalize_intervals(out)
}

async fn build_candidates_from_intervals<S: StorageIO>(
    pointers_io: Arc<S>,
    meta: Option<ScanMeta>,
    intervals: &[(u64, u64)],
) -> crate::core::row::error::Result<Vec<RowPointer>> {
    let mut it = RowPointerIterator::new(pointers_io).await?;
    let mut out = Vec::new();
    let mut interval_idx = 0usize;

    loop {
        let batch = it.next_row_pointers().await?;
        if batch.is_empty() {
            break;
        }

        for pointer in batch.into_iter() {
            if pointer.deleted {
                continue;
            }

            if let Some(meta) = meta {
                // Anything appended after the last scan is conservatively included.
                if pointer.id > meta.last_row_id {
                    out.push(pointer);
                    continue;
                }
            }

            while interval_idx < intervals.len() && pointer.id > intervals[interval_idx].1 {
                interval_idx += 1;
            }
            if interval_idx >= intervals.len() {
                continue;
            }
            let (s, e) = intervals[interval_idx];
            if pointer.id >= s && pointer.id <= e {
                out.push(pointer);
            }
        }
    }

    Ok(out)
}
