//! SME v2 scanner.
//!
//! Scans the whole dataset and builds numeric correlation rules.
//!
//! Current implementation is intentionally minimal:
//! - For each numeric column, compute min/max + a reservoir sample.
//! - Derive a small set of threshold rules (LT/GT) from sample percentiles.
//! - Rescan and build row-id ranges for each rule.

use rclite::Arc;

use cacheguard::CacheGuard;
use log::{error, info};
use smallvec::SmallVec;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    cmp::Ordering as CmpOrdering,
    io,
    sync::{
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use crate::core::{
    db_type::DbType,
    row::{
        row::RowFetch,
        row_pointer::{RowPointer, RowPointerIterator},
        schema::{SchemaCalculator, SchemaField},
    },
    storage_providers::traits::StorageIO,
};

use super::{
    in_memory_rules,
    rule_store::CorrelationRuleStore,
    rules::{
        normalize_ranges_smallvec,
        RangeVec,
        NumericCorrelationRule,
        NumericRuleOp,
        NumericScalar,
        RowRange,
        StringCorrelationRule,
        StringRuleOp,
    },
};

// Numeric rule budget (higher = more candidate thresholds).
const RULES_SIZE_VARIABLE: usize = 48;

// Reservoir sample cap and thresholds scale with the rule budget.
const SAMPLE_CAP: usize = 1024 * 2 * RULES_SIZE_VARIABLE / 8;
const THRESHOLD_COUNT: usize = 4 * RULES_SIZE_VARIABLE; // 192 thresholds → 384 rules per numeric column.

// Numeric sampling stride: larger divisors create fewer percentile anchors.
const NUMERIC_STEP_DIVISOR: usize = 96;

// Cost model weights for numeric candidate scoring (lower prefers more ranges, higher prefers fewer rows).
// Bias selection to favor ranges that exclude more rows (higher row cost).
const NUMERIC_RANGE_WEIGHT: u64 = 24;
const NUMERIC_CANDIDATE_ROWS_WEIGHT: u64 = 64;

// Maximum sampled coverage (0.0-1.0) allowed before a candidate is considered too broad.
const NUMERIC_SAMPLE_BAD_COVERAGE_FRAC: f64 = 0.35;

// How many numeric candidates to keep after scoring (per column, across both LT/GT variants).
const NUMERIC_KEEP_CANDIDATES: usize = 32;

// Candidate search breadth around the base percentile indices.
// Kept intentionally small because this runs during full-table scans.
const CANDIDATE_STEPS: [isize; 11] = [-5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5];

// Scoring weights for choosing thresholds.
// For integer-like columns, we heavily penalize a high number of disjoint ranges.
// For float columns, we penalize huge candidate-row sets more strongly (otherwise
// the optimizer may pick thresholds that match "almost everything" in one big range).
const SCORE_RANGE_COUNT_WEIGHT_INT: u64 = 256;
const SCORE_CANDIDATE_ROWS_WEIGHT_INT: u64 = 1;

// const SCORE_RANGE_COUNT_WEIGHT_FLOAT: u64 = 64;
// const SCORE_CANDIDATE_ROWS_WEIGHT_FLOAT: u64 = 64;

const FLOAT_COARSEN_MAX_CANDIDATE_MULTIPLIER: u64 = 64;
const FLOAT_COARSEN_TARGET_MAX_RANGES: usize = 256;

// Max percentage of the total rows a single rule is allowed to cover (1-100).
// Lowered to bias toward exclusionary, high-precision rules.
const MAX_RULE_COVERAGE_PERCENT: u64 = 5;

// For small tables, a strict percentage cap can discard everything.
// This floor keeps the rule set non-empty while still avoiding "matches everything" rules on large tables.
const MIN_RULE_COVERAGE_ROWS: u64 = 16;

// String histogram/scan sizing and n-gram lengths (kept together for easy tuning).
const STRING_BYTE_FREQ_BINS: usize = 256;
const STRING_NGRAM_MAX_SCAN: usize = STRING_BYTE_FREQ_BINS;
const STRING_BIGRAM_LEN: usize = 2;
const STRING_TRIGRAM_LEN: usize = 3;

const STRING_SAMPLE_CAP: usize = 1024 * RULES_SIZE_VARIABLE / 16;

const STRING_PREFIX_SUFFIX_LENS: [usize; 7] = [1, 2, 3, 4, 6, 8, 12];
const STRING_PREFIX_CANDIDATES: usize = 12;
const STRING_SUFFIX_CANDIDATES: usize = 12;
const STRING_CONTAINS_BYTE_CANDIDATES: usize = 8;
const STRING_CONTAINS_BIGRAM_CANDIDATES: usize = 8;
const STRING_CONTAINS_TRIGRAM_CANDIDATES: usize = 6;
const STRING_CONTAINS_COUNTS: [u8; 5] = [0, 1, 2, 3, 4];
const STRING_CONTAINS_NGRAM_COUNTS: [u8; 2] = [1, 2];
const STRING_FINAL_RULES_MAX: usize = 32 * RULES_SIZE_VARIABLE;

const STRING_RULE_CANDIDATE_CAP: usize = 128;
const STRING_FEATURE_MAP_MAX: usize = 4096;
const STRING_OVERLAP_PENALTY_WEIGHT: u64 = 4;
const STRING_LOCAL_SEARCH_PASSES: usize = 4;

const POINTERS_FINGERPRINT_CHUNK: usize = 16 * 1024;

/// Spawns a background task that rebuilds SME v2 correlation rules on-demand.
///
/// Design:
/// - Scanning is on-demand (triggered by INSERT/UPDATE/DELETE), not periodic.
/// - A debounce window coalesces bursts of mutations.
/// - Optional in-memory change detection can skip rescans when pointers are unchanged.
pub fn spawn_table_rules_scanner_v2<S: StorageIO>(
    base_io: Arc<S>,
    table_name: String,
    table_schema_fields: SmallVec<[SchemaField; 20]>,
    table_pointers_io: Arc<S>,
    table_rows_io: Arc<S>,
    debounce: Duration,
    only_rebuild_if_table_changed: bool,
) -> TableRulesScannerHandleV2 {
    let notify = Arc::new(tokio::sync::Notify::new());
    let pending = Arc::new(CacheGuard::new(std::sync::atomic::AtomicBool::new(false)));
    let last_fingerprint = Arc::new(AtomicU64::new(0));

    let handle = TableRulesScannerHandleV2 {
        notify: Arc::clone(&notify),
        pending: Arc::clone(&pending),
    };

    // If the rules file was deleted (or is empty), schedule a one-time rebuild on startup.
    // Without this, deleting the rules file does nothing until a mutation occurs.
    {
        let base_io = Arc::clone(&base_io);
        let table_name = table_name.clone();
        let notify = Arc::clone(&notify);
        let pending = Arc::clone(&pending);
        tokio::spawn(async move {
            let rules_io = CorrelationRuleStore::open_rules_io(base_io, &table_name).await;
            let len = rules_io.get_len().await;
            if len == 0 {
                log::info!("Rules file empty/missing for table {}, scheduling initial scan", table_name);
                pending.store(true, std::sync::atomic::Ordering::Release);
                notify.notify_one();
            }
        });
    }

    tokio::spawn(async move {
        loop {
            // Wait until a caller requests a scan.
            notify.notified().await;

            // Debounce/coalesce rapid bursts of mutations.
            if debounce != Duration::from_secs(0) {
                tokio::time::sleep(debounce).await;
            }

            // Drain all pending scan requests.
            while pending.swap(false, std::sync::atomic::Ordering::AcqRel) {
                log::info!("Starting SME v2 background scan for table {}", table_name);
                let start = std::time::Instant::now();

                if only_rebuild_if_table_changed {
                    if let Ok(fp) = compute_pointers_fingerprint(table_pointers_io.clone()).await {
                        let prev = last_fingerprint.load(Ordering::Acquire);
                        if prev != 0 && prev == fp {
                            log::info!("Table {} fingerprint unchanged ({}), skipping scan", table_name, fp);
                            continue;
                        }
                        last_fingerprint.store(fp, Ordering::Release);
                    }
                }

                // Best-effort scan; if it fails, we leave pending=false but future mutations can retry.
                if let Err(e) = scan_whole_table_build_rules(
                    base_io.clone(),
                    &table_name,
                    table_schema_fields.as_slice(),
                    table_pointers_io.clone(),
                    table_rows_io.clone(),
                )
                .await
                {
                    error!("SME v2 scan failed for table {}: {}", table_name, e);
                } else {
                    log::info!("SME v2 scan completed for table {} in {:.2?}", table_name, start.elapsed());
                }
            }
        }
    });

    handle
}

#[derive(Clone)]
pub struct TableRulesScannerHandleV2 {
    notify: Arc<tokio::sync::Notify>,
    pending: Arc<CacheGuard<std::sync::atomic::AtomicBool>>,
}

impl TableRulesScannerHandleV2 {
    #[inline]
    pub fn request_scan(&self) {
        self.pending
            .store(true, std::sync::atomic::Ordering::Release);
        self.notify.notify_one();
    }
}

async fn compute_pointers_fingerprint<S: StorageIO>(pointers_io: Arc<S>) -> std::io::Result<u64> {
    use std::hash::Hasher;

    let start = std::time::Instant::now();
    let file_len = pointers_io.get_len().await;
    let mut h = ahash::AHasher::default();
    h.write_u64(file_len);

    let mut pos = 0u64;
    let mut buf = vec![0u8; POINTERS_FINGERPRINT_CHUNK];
    while pos < file_len {
        let remaining = (file_len - pos) as usize;
        let to_read = std::cmp::min(remaining, buf.len());
        let slice = &mut buf[..to_read];
        pointers_io.read_data_into_buffer(&mut pos, slice).await?;
        h.write(slice);
    }

    let fp = h.finish();
    log::trace!("compute_pointers_fingerprint: {} bytes in {:.2?} -> {:x}", file_len, start.elapsed(), fp);
    Ok(fp)
}

/// Accumulates statistics for a numeric column during the first pass.
/// Tracks min/max values and maintains a fixed-size reservoir sample of the data distribution.
#[derive(Debug, Clone)]
enum NumericStats {
    Signed {
        min: i128,
        max: i128,
        seen: u64,
        samples: Vec<(u64, i128)>,
    },
    Unsigned {
        min: u128,
        max: u128,
        seen: u64,
        samples: Vec<(u64, u128)>,
    },
    Float {
        min: f64,
        max: f64,
        seen: u64,
        samples: Vec<(u64, f64)>,
    },
}

impl NumericStats {
    fn new(db_type: &DbType) -> Option<Self> {
        match db_type {
            DbType::F32 | DbType::F64 => Some(Self::Float {
                min: f64::INFINITY,
                max: f64::NEG_INFINITY,
                seen: 0,
                samples: Vec::new(),
            }),
            DbType::I8
            | DbType::I16
            | DbType::I32
            | DbType::I64
            | DbType::I128 => Some(Self::Signed {
                min: i128::MAX,
                max: i128::MIN,
                seen: 0,
                samples: Vec::new(),
            }),
            DbType::U8
            | DbType::U16
            | DbType::U32
            | DbType::U64
            | DbType::U128 => Some(Self::Unsigned {
                min: u128::MAX,
                max: 0,
                seen: 0,
                samples: Vec::new(),
            }),
            _ => None,
        }
    }

    fn push_sample_i128(samples: &mut Vec<(u64, i128)>, seen: u64, pos: u64, v: i128) {
        if samples.len() < SAMPLE_CAP {
            samples.push((pos, v));
            return;
        }
        // Reservoir sampling.
        let j = fastrand::u64(..seen);
        if (j as usize) < samples.len() {
            samples[j as usize] = (pos, v);
        }
    }

    fn push_sample_u128(samples: &mut Vec<(u64, u128)>, seen: u64, pos: u64, v: u128) {
        if samples.len() < SAMPLE_CAP {
            samples.push((pos, v));
            return;
        }
        let j = fastrand::u64(..seen);
        if (j as usize) < samples.len() {
            samples[j as usize] = (pos, v);
        }
    }

    fn push_sample_f64(samples: &mut Vec<(u64, f64)>, seen: u64, pos: u64, v: f64) {
        if samples.len() < SAMPLE_CAP {
            samples.push((pos, v));
            return;
        }
        let j = fastrand::u64(..seen);
        if (j as usize) < samples.len() {
            samples[j as usize] = (pos, v);
        }
    }

    fn update(&mut self, pos: u64, v: NumericScalar) {
        match (self, v) {
            (
                NumericStats::Signed {
                    min,
                    max,
                    seen,
                    samples,
                },
                NumericScalar::Signed(x),
            ) => {
                *seen += 1;
                *min = (*min).min(x);
                *max = (*max).max(x);
                Self::push_sample_i128(samples, *seen, pos, x);
            }
            (
                NumericStats::Unsigned {
                    min,
                    max,
                    seen,
                    samples,
                },
                NumericScalar::Unsigned(x),
            ) => {
                *seen += 1;
                *min = (*min).min(x);
                *max = (*max).max(x);
                Self::push_sample_u128(samples, *seen, pos, x);
            }
            (
                NumericStats::Float {
                    min,
                    max,
                    seen,
                    samples,
                },
                NumericScalar::Float(x),
            ) => {
                if x.is_nan() {
                    return;
                }
                *seen += 1;
                *min = (*min).min(x);
                *max = (*max).max(x);
                Self::push_sample_f64(samples, *seen, pos, x);
            }
            _ => {}
        }
    }

    fn sorted_unique_sample_values(&self) -> Vec<NumericScalar> {
        match self {
            NumericStats::Signed { samples, .. } => {
                if samples.is_empty() {
                    return Vec::new();
                }
                let mut v: Vec<i128> = samples.iter().map(|(_, val)| *val).collect();
                v.sort();
                v.dedup();
                v.into_iter().map(NumericScalar::Signed).collect()
            }
            NumericStats::Unsigned { samples, .. } => {
                if samples.is_empty() {
                    return Vec::new();
                }
                let mut v: Vec<u128> = samples.iter().map(|(_, val)| *val).collect();
                v.sort();
                v.dedup();
                v.into_iter().map(NumericScalar::Unsigned).collect()
            }
            NumericStats::Float { samples, .. } => {
                if samples.is_empty() {
                    return Vec::new();
                }
                let mut v: Vec<f64> = samples.iter().map(|(_, val)| *val).collect();
                v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                v.dedup_by(|a, b| a.to_bits() == b.to_bits());
                v.into_iter().map(NumericScalar::Float).collect()
            }
        }
    }

    fn get_sorted_samples_with_pos(&self) -> Vec<(u64, NumericScalar)> {
         match self {
            NumericStats::Signed { samples, .. } => {
                let mut v = samples.clone();
                v.sort_by_key(|(pos, _)| *pos);
                v.into_iter().map(|(p, v)| (p, NumericScalar::Signed(v))).collect()
            }
            NumericStats::Unsigned { samples, .. } => {
                let mut v = samples.clone();
                v.sort_by_key(|(pos, _)| *pos);
                v.into_iter().map(|(p, v)| (p, NumericScalar::Unsigned(v))).collect()
            }
            NumericStats::Float { samples, .. } => {
                let mut v = samples.clone();
                v.sort_by_key(|(pos, _)| *pos);
                v.into_iter().map(|(p, v)| (p, NumericScalar::Float(v))).collect()
            }
         }
    }
}

#[inline]
fn scalar_key(v: NumericScalar) -> [u8; 16] {
    v.encode_16le()
}

#[inline]
fn ranges_candidate_rows(ranges: &[RowRange]) -> u64 {
    ranges.iter().map(|r| r.row_count).sum()
}

fn build_uncovered_ranges(total_rows: u64, ranges: &[RowRange]) -> Vec<RowRange> {
    if total_rows == 0 {
        return Vec::new();
    }

    use crate::core::row::row_pointer::ROW_POINTER_RECORD_LEN;
    let row_len = ROW_POINTER_RECORD_LEN as u64;

    // Merge and sort existing ranges to compute gaps.
    let mut merged = if ranges.is_empty() {
        Vec::new()
    } else {
        normalize_ranges_smallvec(ranges.to_vec()).into_vec()
    };

    if merged.is_empty() {
        return vec![RowRange {
            start_pointer_pos: 0,
            row_count: total_rows,
        }];
    }

    merged.sort_by_key(|r| r.start_pointer_pos);

    let mut uncovered: Vec<RowRange> = Vec::new();
    let mut cursor = 0u64;
    let table_bytes = total_rows.saturating_mul(row_len);

    for r in merged.into_iter() {
        if r.start_pointer_pos > cursor {
            let gap_rows = (r.start_pointer_pos - cursor) / row_len;
            if gap_rows > 0 {
                uncovered.push(RowRange {
                    start_pointer_pos: cursor,
                    row_count: gap_rows,
                });
            }
        }
        cursor = r.end_pointer_pos_exclusive().max(cursor);
    }

    if cursor < table_bytes {
        let gap_rows = (table_bytes - cursor) / row_len;
        if gap_rows > 0 {
            uncovered.push(RowRange {
                start_pointer_pos: cursor,
                row_count: gap_rows,
            });
        }
    }

    uncovered
}

fn cmp_scalar(a: NumericScalar, b: NumericScalar) -> CmpOrdering {
    match (a, b) {
        (NumericScalar::Signed(x), NumericScalar::Signed(y)) => x.cmp(&y),
        (NumericScalar::Unsigned(x), NumericScalar::Unsigned(y)) => x.cmp(&y),
        (NumericScalar::Float(x), NumericScalar::Float(y)) => x
            .partial_cmp(&y)
            .unwrap_or(std::cmp::Ordering::Equal),
        // Different kinds shouldn't happen within a single numeric column.
        _ => std::cmp::Ordering::Equal,
    }
}

fn numeric_catchall_scalar(db_type: &DbType) -> NumericScalar {
    match db_type {
        DbType::F32 | DbType::F64 => NumericScalar::Float(f64::INFINITY),
        DbType::I8 | DbType::I16 | DbType::I32 | DbType::I64 | DbType::I128 => {
            NumericScalar::Signed(i128::MAX)
        }
        DbType::U8 | DbType::U16 | DbType::U32 | DbType::U64 | DbType::U128 => {
            NumericScalar::Unsigned(u128::MAX)
        }
        _ => NumericScalar::Signed(i128::MAX),
    }
}

fn coarsen_ranges_with_gap(mut ranges: Vec<RowRange>, max_gap: u64) -> Vec<RowRange> {
    if ranges.len() <= 1 {
        return ranges;
    }

    use crate::core::row::row_pointer::ROW_POINTER_RECORD_LEN;
    let len = ROW_POINTER_RECORD_LEN as u64;

    ranges.sort_by_key(|r| r.start_pointer_pos);
    let mut out: Vec<RowRange> = Vec::with_capacity(ranges.len());
    let mut cur = ranges[0];
    for r in ranges.into_iter().skip(1) {
        // Allow merging ranges that are close, to reduce fragmentation.
        let cur_end_excl = cur.end_pointer_pos_exclusive();
        let can_merge = r.start_pointer_pos <= cur_end_excl.saturating_add(max_gap.saturating_mul(len));
        if can_merge {
            let new_end_excl = cur_end_excl.max(r.end_pointer_pos_exclusive());
            cur.row_count = (new_end_excl.saturating_sub(cur.start_pointer_pos)) / len;
        } else {
            out.push(cur);
            cur = r;
        }
    }
    out.push(cur);
    out
}

/// Dynamically optimizes row ranges using an iterative trial-and-error approach.
///
/// This function attempts to find the "perfect" balance between rule metadata size (number of ranges)
/// and query precision (number of candidate rows). It does this by:
/// 1. establishing a baseline cost model based on simulated IO latency.
/// 2. running multiple "simulation" passes with different merging strategies (Gap-based, Clustering).
/// 3. scoring each result against a simulated "random access" workload.
fn optimize_ranges_dynamic(
    ranges: Vec<RowRange>,
    range_weight: u64,
    candidate_rows_weight: u64,
) -> Vec<RowRange> {
    if ranges.len() <= 1 {
        return ranges;
    }

    // 1. Establish Baseline (Exact ranges)
    let exact_candidates = ranges_candidate_rows(&ranges);
    if exact_candidates == 0 {
        return ranges;
    }
    
    // We use a sophisticated cost function that simulates "latency".
    // It penalizes range-seeking (random access) and row-scanning (sequential access).
    let calculate_simulated_latency = |rs: &[RowRange]| -> u64 {
        let count = rs.len() as u64;
        let rows = ranges_candidate_rows(rs);
        // Base Seek Cost + Per-Range Overhead + Scan Cost + False Positive Penalty
        let overhead_cost = count.saturating_mul(range_weight);
        let scan_cost = rows.saturating_mul(candidate_rows_weight);
        overhead_cost.saturating_add(scan_cost)
    };

    let mut best_ranges = ranges.clone();
    let mut min_latency = calculate_simulated_latency(&best_ranges);

    log::trace!(
        "optimize_ranges_dynamic: start. {} ranges, {} rows. exact_latency={}",
        best_ranges.len(), 
        exact_candidates,
        min_latency
    );
    
    // If we are over the hard limit, exact ranges essentially have infinite cost (softly modeled here).
    if best_ranges.len() > FLOAT_COARSEN_TARGET_MAX_RANGES {
        min_latency = u64::MAX;
    }

    // 2. Iterative "Trial and Error" - Global Gap Search 
    // We try a wide spectrum of gap sizes to see how the data responds.
    // Instead of fixed powers of 2, we dynamically probe around promising areas if we had more time,
    // but here we sweep exponentially to cover all scales.
    
    // We also include some prime-ish numbers to catch periodic patterns that powers-of-2 might miss.
    let trial_gaps = [
        1, 2, 3, 4, 6, 8, 12, 16, 24, 32, 48, 64, 96, 128, 192, 256, 
        512, 1024, 2048, 4096, 8192, 16384, 32768, 65536
    ];

    for gap in trial_gaps {
        let candidate = coarsen_ranges_with_gap(ranges.clone(), gap);
        
        // Hard constraint check
        let cand_rows = ranges_candidate_rows(&candidate);
        if cand_rows > exact_candidates.saturating_mul(FLOAT_COARSEN_MAX_CANDIDATE_MULTIPLIER) {
            continue; 
        }

        let latency = calculate_simulated_latency(&candidate);
        
        let valid_count = candidate.len() <= FLOAT_COARSEN_TARGET_MAX_RANGES;
        let best_valid = best_ranges.len() <= FLOAT_COARSEN_TARGET_MAX_RANGES;

        // Logic:
        // - If we were invalid and now are valid: Take it.
        // - If both invalid: Take the one with fewer ranges (closer to valid).
        // - If both valid: Take lower latency.
        if !best_valid && valid_count {
            best_ranges = candidate;
            min_latency = latency;
        } else if !best_valid && !valid_count {
            if candidate.len() < best_ranges.len() {
                best_ranges = candidate;
                min_latency = latency;
            }
        } else if valid_count && latency < min_latency {
            best_ranges = candidate;
            min_latency = latency;
        }
    }

    // 3. "Squash to Perfection" - Greedy Clustering (Hierarchical Agglomerative)
    // If we still have too many ranges, or just to fine-tune, we try to merge the *smallest* gaps first.
    // We allow this to run even if we have many ranges (up to the target max), but we apply aggressive
    // squashing if the count is high to force convergence.
    if best_ranges.len() > 1 {
         let mut clustered = best_ranges.clone();
         // Attempt multiple passes of "merge smallest gap"
         let mut improved = true;
         // Safety: prevent infinite loops or excessively long optimization
         let mut iterations = 0;
         while improved && iterations < 10000 {
             iterations += 1;
             improved = false;
             if clustered.len() <= 1 { break; }

             use crate::core::row::row_pointer::ROW_POINTER_RECORD_LEN;
             let len = ROW_POINTER_RECORD_LEN as u64;
             
             // Find smallest gap
             let mut min_gap = u64::MAX;
             let mut min_gap_idx = 0;
             for i in 0..clustered.len()-1 {
                 let cur_end_excl = clustered[i].end_pointer_pos_exclusive();
                 let next_start = clustered[i + 1].start_pointer_pos;
                 let g = if next_start > cur_end_excl {
                     (next_start - cur_end_excl) / len
                 } else {
                     0
                 };
                 if g < min_gap {
                     min_gap = g;
                     min_gap_idx = i;
                 }
             }
             
             let added_rows = min_gap;
             
             // Dynamic Squashing:
             // If we have a huge number of ranges, we increase the "weight" (value) of removing a range.
             // This effectively makes us more tolerant of larger gaps (more candidate rows) in exchange for
             // reducing the range count.
             let count_penalty_multiplier = if clustered.len() > 1000 {
                 // E.g., at 2000 ranges, multiplier is 4. For floats (weight 256), effective weight becomes 1024.
                 // This allows merging gaps of size ~16 instead of ~4.
                 (clustered.len() as u64) / 500
             } else {
                 1
             };
             // Ensure at least 1
             let count_penalty_multiplier = count_penalty_multiplier.max(1);

             let effective_range_weight = range_weight.saturating_mul(count_penalty_multiplier);
             
             // Benefit: -1 Range (save effective_range_weight)
             // Cost: +added_rows (pay candidate_rows_weight * added_rows)
             
             let benefit = effective_range_weight;
             let cost = added_rows.saturating_mul(candidate_rows_weight);
             
             // Merge if profitable OR if we are mandated to reduce size (hard limit)
             if cost < benefit || clustered.len() > FLOAT_COARSEN_TARGET_MAX_RANGES {
                 // Do merge
                 let start = clustered[min_gap_idx].start_pointer_pos;
                 let new_end_excl = clustered[min_gap_idx + 1].end_pointer_pos_exclusive();
                 clustered[min_gap_idx].row_count = (new_end_excl.saturating_sub(start)) / len;
                 clustered.remove(min_gap_idx+1);
                 improved = true;
             }
         }
         
         let cl_latency = calculate_simulated_latency(&clustered);
         let cl_valid = clustered.len() <= FLOAT_COARSEN_TARGET_MAX_RANGES;
         let best_valid = best_ranges.len() <= FLOAT_COARSEN_TARGET_MAX_RANGES;
         
         // If clustering made us valid when we weren't, OR if it improved latency while staying valid/invalid status quo
         if (!best_valid && cl_valid) || (cl_valid == best_valid && cl_latency < min_latency) {
             best_ranges = clustered;
         }
    }

    best_ranges
}

/// Scans the whole dataset and produces numeric correlation rules.
/// 
/// This function executes a two-pass algorithm to generate data-dependent correlation rules (SME v2):
///
/// 1.  **Pass 1 (Statistics Collection)**:
///     - Scans the table in parallel using a Producer-Consumer model.
///     - Collects min/max values and a reservoir sample of the data distribution for each numeric column.
///     - Uses these partial stats to derive global candidate thresholds (quantiles).
///
/// 2.  **Pass 2 (Range Building)**:
///     - Generates candidate rules (LessThan/GreaterThan) for the chosen thresholds.
///     - Re-scans the table in parallel.
///     - Builds lists of `RowRange`s where each rule evaluates to true.
///     - Merges partial ranges from worker threads.
///
/// 3.  **Optimization**:
///     - Applies a cost-based optimizer to merge fragmented ranges and select the best rules.
///     - Filters out rules that are too broad (cover too much of the table) or redundant.
pub async fn scan_whole_table_build_rules<S: StorageIO>(
    base_io: Arc<S>,
    table_name: &str,
    schema: &[SchemaField],
    pointers_io: Arc<S>,
    rows_io: Arc<S>,
) -> std::io::Result<()> {
    log::info!("scan_whole_table_build_rules: starting for table {}", table_name);
    let total_start = std::time::Instant::now();

    let concurrency = *crate::MAX_PERMITS_THREADS.get().unwrap_or(&16);
    let concurrency = concurrency.max(1);

    info!("Scanner's concurrency: {}", concurrency);

    #[derive(Clone)]
    struct FieldInfo {
        schema_id: u64,
        db_type: DbType,
        fetch_idx: usize,
    }

    // Identify numeric and string columns.
    let mut numeric_fields: Vec<FieldInfo> = Vec::new();
    let mut string_fields: Vec<FieldInfo> = Vec::new();
    let schema_calc = SchemaCalculator::default();

    let mut numeric_columns_fetching_data = SmallVec::new();
    let mut string_columns_fetching_data = SmallVec::new();
    for (idx, f) in schema.iter().enumerate() {
        let (offset, schema_id) = schema_calc.calculate_schema_offset(&f.name, schema);
        let size = schema[idx].size as u32;

        if NumericStats::new(&f.db_type).is_some() {
            numeric_columns_fetching_data.push(crate::core::row::row::ColumnFetchingData {
                column_offset: offset,
                column_type: f.db_type.clone(),
                size,
                schema_id,
            });
            let fetch_idx = numeric_columns_fetching_data.len() - 1;
            numeric_fields.push(FieldInfo {
                schema_id,
                db_type: f.db_type.clone(),
                fetch_idx,
            });
            continue;
        }

        if f.db_type == DbType::STRING {
            string_columns_fetching_data.push(crate::core::row::row::ColumnFetchingData {
                column_offset: offset,
                column_type: f.db_type.clone(),
                size,
                schema_id,
            });
            let fetch_idx = string_columns_fetching_data.len() - 1;
            string_fields.push(FieldInfo {
                schema_id,
                db_type: f.db_type.clone(),
                fetch_idx,
            });
        }
    }

    if numeric_fields.is_empty() && string_fields.is_empty() {
        log::info!(
            "scan_whole_table_build_rules: no numeric or string columns found, writing empty rules"
        );
        in_memory_rules::set_table_rules(table_name, Vec::new(), Vec::new());
        return Ok(());
    }

    log::trace!(
        "scan_whole_table_build_rules: found {} numeric + {} string columns. Using concurrency: {}",
        numeric_fields.len(),
        string_fields.len(),
        concurrency
    );

    // IMPORTANT: Keep numeric and string fetches separate.
    // A single invalid STRING header can cause fetch_row_reuse_async() to error;
    // if we fetched both together, we'd skip the row entirely and end up with zero numeric samples/rules.
    let numeric_row_fetch = Arc::new(RowFetch {
        columns_fetching_data: numeric_columns_fetching_data,
    });
    let string_row_fetch = Arc::new(RowFetch {
        columns_fetching_data: string_columns_fetching_data,
    });
    let numeric_fields = Arc::new(numeric_fields);
    let string_fields = Arc::new(string_fields);

    // Schema maps for pass 2
    let mut numeric_schema_id_to_fetch: BTreeMap<u64, (usize, DbType)> = BTreeMap::new();
    for f in numeric_fields.iter() {
        numeric_schema_id_to_fetch.insert(f.schema_id, (f.fetch_idx, f.db_type.clone()));
    }
    let numeric_schema_id_to_fetch = Arc::new(numeric_schema_id_to_fetch);

    let mut string_schema_id_to_fetch: BTreeMap<u64, usize> = BTreeMap::new();
    for f in string_fields.iter() {
        string_schema_id_to_fetch.insert(f.schema_id, f.fetch_idx);
    }
    let string_schema_id_to_fetch = Arc::new(string_schema_id_to_fetch);

    // PASS 1: collect stats.
    let pass1_start = std::time::Instant::now();

    #[derive(Clone)]
    struct StringStats {
        seen: u64,
        prefix_counts: HashMap<Vec<u8>, u32>,
        suffix_counts: HashMap<Vec<u8>, u32>,
        bigram_counts: HashMap<[u8; 2], u32>,
        trigram_counts: HashMap<[u8; 3], u32>,
        byte_freq: [u32; STRING_BYTE_FREQ_BINS],
    }

    impl StringStats {
        fn new() -> Self {
            Self {
                seen: 0,
                prefix_counts: HashMap::new(),
                suffix_counts: HashMap::new(),
                bigram_counts: HashMap::new(),
                trigram_counts: HashMap::new(),
                byte_freq: [0u32; STRING_BYTE_FREQ_BINS],
            }
        }

        #[inline]
        fn bump_bytes_map(map: &mut HashMap<Vec<u8>, u32>, key: Vec<u8>) {
            if let Some(v) = map.get_mut(&key) {
                *v = v.saturating_add(1);
                return;
            }
            if map.len() < STRING_FEATURE_MAP_MAX {
                map.insert(key, 1);
            }
        }

        #[inline]
        fn bump_2(map: &mut HashMap<[u8; 2], u32>, key: [u8; 2]) {
            if let Some(v) = map.get_mut(&key) {
                *v = v.saturating_add(1);
                return;
            }
            if map.len() < STRING_FEATURE_MAP_MAX {
                map.insert(key, 1);
            }
        }

        #[inline]
        fn bump_3(map: &mut HashMap<[u8; 3], u32>, key: [u8; 3]) {
            if let Some(v) = map.get_mut(&key) {
                *v = v.saturating_add(1);
                return;
            }
            if map.len() < STRING_FEATURE_MAP_MAX {
                map.insert(key, 1);
            }
        }

        fn update(&mut self, bytes: &[u8]) {
            self.seen = self.seen.saturating_add(1);
            if bytes.is_empty() {
                return;
            }

            // Sample after a certain point to avoid pathological feature-map growth.
            if self.seen > STRING_SAMPLE_CAP as u64 {
                let j = fastrand::u64(..self.seen);
                if j >= STRING_SAMPLE_CAP as u64 {
                    return;
                }
            }

            for &b in bytes.iter() {
                self.byte_freq[b as usize] = self.byte_freq[b as usize].saturating_add(1);
            }

            for &len in STRING_PREFIX_SUFFIX_LENS.iter() {
                if let Some(p) = utf8_safe_prefix(bytes, len) {
                    if !p.is_empty() {
                        Self::bump_bytes_map(&mut self.prefix_counts, p);
                    }
                }
                if let Some(s) = utf8_safe_suffix(bytes, len) {
                    if !s.is_empty() {
                        Self::bump_bytes_map(&mut self.suffix_counts, s);
                    }
                }
            }

            // Lightweight n-gram mapping (ASCII-printable only) to seed better CONTAINS rules.
            // Keeping this small and sampled avoids blowing up the scan cost.
            let max_scan = std::cmp::min(bytes.len(), STRING_NGRAM_MAX_SCAN);
            if max_scan >= STRING_BIGRAM_LEN {
                for w in bytes[..max_scan].windows(STRING_BIGRAM_LEN) {
                    if w[0].is_ascii_graphic() && w[1].is_ascii_graphic() {
                        Self::bump_2(&mut self.bigram_counts, [w[0], w[1]]);
                    }
                }
            }
            if max_scan >= STRING_TRIGRAM_LEN {
                for w in bytes[..max_scan].windows(STRING_TRIGRAM_LEN) {
                    if w[0].is_ascii_graphic() && w[1].is_ascii_graphic() && w[2].is_ascii_graphic() {
                        Self::bump_3(&mut self.trigram_counts, [w[0], w[1], w[2]]);
                    }
                }
            }
        }

        fn merge_from(&mut self, other: StringStats) {
            self.seen = self.seen.saturating_add(other.seen);
            for (k, v) in other.prefix_counts.into_iter() {
                *self.prefix_counts.entry(k).or_insert(0) += v;
            }
            for (k, v) in other.suffix_counts.into_iter() {
                *self.suffix_counts.entry(k).or_insert(0) += v;
            }
            for (k, v) in other.bigram_counts.into_iter() {
                *self.bigram_counts.entry(k).or_insert(0) += v;
            }
            for (k, v) in other.trigram_counts.into_iter() {
                *self.trigram_counts.entry(k).or_insert(0) += v;
            }
            for i in 0..STRING_BYTE_FREQ_BINS {
                self.byte_freq[i] = self.byte_freq[i].saturating_add(other.byte_freq[i]);
            }
        }
    }
    
    // Producer: Read RowPointers (with their byte offset in the pointers file).
    let (tx, rx) = tokio::sync::mpsc::channel::<Vec<(u64, RowPointer)>>(concurrency * 4);
    let rx = Arc::new(tokio::sync::Mutex::new(rx));
    
    let pointers_io_clone = pointers_io.clone();
    let producer_handle = tokio::spawn(async move {
        let mut iter = match RowPointerIterator::new(pointers_io_clone).await {
            Ok(i) => i,
            Err(e) => return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("RowPointerIterator: {}", e))),
        };
        loop {
            let mut chunk: Vec<(u64, RowPointer)> = Vec::new();
            // Keep chunk sizes consistent with the iterator batching.
            for _ in 0..*crate::BATCH_SIZE.get().unwrap() {
                match iter.next_row_pointer_with_offset().await {
                    Ok(Some((pos, p))) => chunk.push((pos, p)),
                    Ok(None) => break,
                    Err(e) => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            format!("RowPointerIterator: {}", e),
                        ))
                    }
                }
            }
            if chunk.is_empty() {
                break;
            }
            if tx.send(chunk).await.is_err() {
                break;
            }
        }
        Ok(())
    });

    let mut handles = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        let rx = rx.clone();
        let rows_io = rows_io.clone();
        let numeric_row_fetch = numeric_row_fetch.clone();
        let string_row_fetch = string_row_fetch.clone();
        let numeric_fields = numeric_fields.clone();
        let string_fields = string_fields.clone();
        
        handles.push(tokio::spawn(async move {
            let mut stats: Vec<NumericStats> = numeric_fields
                .iter()
                .map(|f| NumericStats::new(&f.db_type).unwrap())
                .collect();

            let mut string_stats: Vec<StringStats> = (0..string_fields.len()).map(|_| StringStats::new()).collect();

            let mut row_num = crate::core::row::row::Row::default();
            let mut row_str = crate::core::row::row::Row::default();
            
            loop {
                let chunk_opt = {
                    let mut lock = rx.lock().await;
                    lock.recv().await
                };
                
                match chunk_opt {
                    Some(chunk) => {
                        for (_pos, p) in chunk {
                            if p.deleted { continue; }

                            if !numeric_fields.is_empty() {
                                if p
                                    .fetch_row_reuse_async(rows_io.clone(), &numeric_row_fetch, &mut row_num)
                                    .await
                                    .is_ok()
                                {
                                    for (i, f) in numeric_fields.iter().enumerate() {
                                        let col = &row_num.columns[f.fetch_idx];
                                        let db_type = &f.db_type;
                                        if let Some(v) = decode_numeric_value(db_type, col.data.into_slice()) {
                                            stats[i].update(_pos, v);
                                        }
                                    }
                                }
                            }

                            if !string_fields.is_empty() {
                                if p
                                    .fetch_row_reuse_async(rows_io.clone(), &string_row_fetch, &mut row_str)
                                    .await
                                    .is_ok()
                                {
                                    for (i, f) in string_fields.iter().enumerate() {
                                        let col = &row_str.columns[f.fetch_idx];
                                        let bytes = col.data.into_slice();
                                        // Only accept valid UTF-8 source values.
                                        if std::str::from_utf8(bytes).is_ok() {
                                            string_stats[i].update(bytes);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    None => break,
                }
            }
            (stats, string_stats)
        }));
    }

    // Wait for producer and consumers
    if let Err(e) = producer_handle.await.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))? {
        return Err(e);
    }
    
    // Aggregate numeric + string stats
    let mut aggregated_stats: Vec<Option<NumericStats>> = (0..numeric_fields.len()).map(|_| None).collect();
    let mut aggregated_string_stats: Vec<Option<StringStats>> = (0..string_fields.len()).map(|_| None).collect();
    
    for h in handles {
        let (thread_numeric, thread_strings) = h
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

        for (i, s) in thread_numeric.into_iter().enumerate() {
             match &mut aggregated_stats[i] {
                 None => aggregated_stats[i] = Some(s),
                 Some(current) => {
                     // Merge stats
                     // For min/max/seen it is easy. For reservoir samples, we just concat and resample later or just keep all (up to a limit).
                     match (current, s) {
                        (NumericStats::Signed { min: m1, max: x1, seen: s1, samples: v1 },
                         NumericStats::Signed { min: m2, max: x2, seen: s2, samples: v2 }) => {
                             *m1 = (*m1).min(m2);
                             *x1 = (*x1).max(x2);
                             *s1 += s2;
                             v1.extend(v2);
                             // If we have too many, shrink?
                             if v1.len() > SAMPLE_CAP * 2 {
                                 // Simple decimation
                                 let mut new_v = Vec::with_capacity(SAMPLE_CAP);
                                 for (idx, val) in v1.iter().enumerate() {
                                     if idx % 2 == 0 { new_v.push(*val); }
                                 }
                                 *v1 = new_v;
                             }
                        }
                        (NumericStats::Unsigned { min: m1, max: x1, seen: s1, samples: v1 },
                         NumericStats::Unsigned { min: m2, max: x2, seen: s2, samples: v2 }) => {
                             *m1 = (*m1).min(m2);
                             *x1 = (*x1).max(x2);
                             *s1 += s2;
                             v1.extend(v2);
                             if v1.len() > SAMPLE_CAP * 2 {
                                 let mut new_v = Vec::with_capacity(SAMPLE_CAP);
                                 for (idx, val) in v1.iter().enumerate() {
                                     if idx % 2 == 0 { new_v.push(*val); }
                                 }
                                 *v1 = new_v;
                             }
                        }
                        (NumericStats::Float { min: m1, max: x1, seen: s1, samples: v1 },
                         NumericStats::Float { min: m2, max: x2, seen: s2, samples: v2 }) => {
                             *m1 = (*m1).min(m2);
                             *x1 = (*x1).max(x2);
                             *s1 += s2;
                             v1.extend(v2);
                             if v1.len() > SAMPLE_CAP * 2 {
                                 let mut new_v = Vec::with_capacity(SAMPLE_CAP);
                                 for (idx, val) in v1.iter().enumerate() {
                                     if idx % 2 == 0 { new_v.push(*val); }
                                 }
                                 *v1 = new_v;
                             }
                        }
                        _ => {}
                     }
                 }
             }
        }

        for (i, s) in thread_strings.into_iter().enumerate() {
            match &mut aggregated_string_stats[i] {
                None => aggregated_string_stats[i] = Some(s),
                Some(current) => current.merge_from(s),
            }
        }
    }
    
    // Unwrap aggregated stats
    let stats: Vec<NumericStats> = aggregated_stats.into_iter().map(|s| s.unwrap_or_else(|| {
        // Should not happen, but safe fallback
        NumericStats::new(&DbType::I64).unwrap()
    })).collect();

    let string_stats: Vec<StringStats> = aggregated_string_stats
        .into_iter()
        .map(|s| s.unwrap_or_else(StringStats::new))
        .collect();

    log::info!("scan_whole_table_build_rules: Pass 1 (stats) done in {:.2?}", pass1_start.elapsed());

    // Build threshold rules per column.

    struct ColumnCandidatePlan {
        schema_id: u64,
        db_type: DbType,
        range_weight: u64,
        candidate_rows_weight: u64,
        slot_keys: Vec<Vec<[u8; 16]>>, // candidate thresholds per percentile slot
    }

    let mut plans: Vec<ColumnCandidatePlan> = Vec::new();
    let mut rules_by_col: Vec<(u64, Vec<NumericCorrelationRule>)> = Vec::new();

    for (i, f) in numeric_fields.iter().enumerate() {
        let schema_id = &f.schema_id;
        let db_type = &f.db_type;
        let sorted_samples = stats[i].sorted_unique_sample_values();
        if sorted_samples.is_empty() {
            log::trace!("scan_whole_table_build_rules: col {} has no samples, skipping", schema_id);
            continue;
        }

        let full_samples_with_pos = stats[i].get_sorted_samples_with_pos();
        let n = sorted_samples.len();
        
        // Determine step size to spread candidates.
        let step = std::cmp::max(1usize, n / NUMERIC_STEP_DIVISOR);

        // Balanced weights: allow more candidates but still prefer tighter ranges.
        let range_weight = NUMERIC_RANGE_WEIGHT;
        let candidate_rows_weight = NUMERIC_CANDIDATE_ROWS_WEIGHT;

        let mut slot_keys: Vec<Vec<[u8; 16]>> = Vec::with_capacity(THRESHOLD_COUNT);
        let mut key_to_scalar: BTreeMap<[u8; 16], NumericScalar> = BTreeMap::new();
        let mut raw_candidates: HashSet<[u8; 16]> = HashSet::new();

        for k in 1..=THRESHOLD_COUNT {
            let p = (k as f64) / ((THRESHOLD_COUNT + 1) as f64);
            let base_idx = (p * ((n - 1) as f64)).round() as isize;

            let mut keys_for_slot: Vec<[u8; 16]> = Vec::new();
            for mult in CANDIDATE_STEPS {
                let idx = base_idx.saturating_add(mult.saturating_mul(step as isize));
                let idx = idx.clamp(0, (n as isize) - 1) as usize;
                let scalar = sorted_samples[idx];
                let key = scalar_key(scalar);
                key_to_scalar.insert(key, scalar);
                keys_for_slot.push(key);
                raw_candidates.insert(key);
            }
            slot_keys.push(keys_for_slot);
        }

        // Filter candidates using sample-based scoring to select promising ones for Pass 2.
        let mut scored_candidates: Vec<([u8; 16], u64)> = Vec::with_capacity(raw_candidates.len());
        
        for key in raw_candidates {
             let val = key_to_scalar[&key];
             let mut score_sum = 0u64;
             
             for op in [NumericRuleOp::LessThan, NumericRuleOp::GreaterThan] {
                 let mut range_count = 0u64;
                 let mut row_count = 0u64;
                 let mut in_range = false;

                 for (_pos, s_val) in full_samples_with_pos.iter() {
                     if value_satisfies_rule(*s_val, op, val) {
                         row_count += 1;
                         if !in_range {
                             range_count += 1;
                             in_range = true;
                         }
                     } else {
                         in_range = false;
                     }
                 }
                 
                  // If coverage > threshold (sampled), mark as low-quality.
                  if row_count > 0
                     && (row_count as f64) < (full_samples_with_pos.len() as f64 * NUMERIC_SAMPLE_BAD_COVERAGE_FRAC)
                  {
                      let s = range_count * range_weight + row_count * candidate_rows_weight;
                      score_sum += s;
                 } else {
                      score_sum += u64::MAX / 4; 
                 }
             }
             scored_candidates.push((key, score_sum));
        }
        
           // Pick best candidates (larger keep count for more aggressive rule generation).
           scored_candidates.sort_by_key(|(_, s)| *s);
           let keep_count = std::cmp::min(scored_candidates.len(), NUMERIC_KEEP_CANDIDATES);
           let kept_keys: HashSet<[u8; 16]> = scored_candidates.iter().take(keep_count).map(|(k, _)| *k).collect();

        for slot in slot_keys.iter_mut() {
            slot.retain(|k| kept_keys.contains(k));
        }
        
        // Sort keys to allow binary search optimization in Pass 2
        let mut sorted_kept_keys: Vec<[u8; 16]> = kept_keys.into_iter().collect();
        sorted_kept_keys.sort();

        let mut rules: Vec<NumericCorrelationRule> = Vec::with_capacity(sorted_kept_keys.len() * 2);
        for key in sorted_kept_keys.iter() {
            let scalar = key_to_scalar[key];
            rules.push(NumericCorrelationRule {
                column_schema_id: *schema_id,
                column_type: db_type.clone(),
                op: NumericRuleOp::LessThan,
                value: scalar,
                ranges: RangeVec::new(),
            });
             rules.push(NumericCorrelationRule {
                column_schema_id: *schema_id,
                column_type: db_type.clone(),
                op: NumericRuleOp::GreaterThan,
                value: scalar,
                ranges: RangeVec::new(),
            });
        }

        plans.push(ColumnCandidatePlan {
            schema_id: *schema_id,
            db_type: db_type.clone(),
            range_weight,
            candidate_rows_weight,
            slot_keys,
        });
        rules_by_col.push((*schema_id, rules));
    }

    // Build string candidate rules per column from string stats.
    let mut string_candidate_rules_by_col: Vec<(u64, Vec<StringCorrelationRule>)> = Vec::new();
    for (i, f) in string_fields.iter().enumerate() {
        let schema_id = f.schema_id;
        let db_type = f.db_type.clone();
        let st = &string_stats[i];

        let mut seen_rules: HashSet<(u8, u8, Vec<u8>)> = HashSet::new();
        let mut rules: Vec<StringCorrelationRule> = Vec::new();

        // Pick top prefixes and expand around them (shorter/longer) for trial-and-error.
        let mut prefixes: Vec<(Vec<u8>, u32)> = st
            .prefix_counts
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect();
        prefixes.sort_by(|a, b| b.1.cmp(&a.1));
        prefixes.truncate(STRING_PREFIX_CANDIDATES);

        let mut prefix_set: HashSet<Vec<u8>> = prefixes.iter().map(|(k, _)| k.clone()).collect();
        for (p, _) in prefixes.iter() {
            // Add shorter variants.
            for &len in STRING_PREFIX_SUFFIX_LENS.iter() {
                if len < p.len() {
                    if let Some(shorter) = utf8_safe_prefix(p, len) {
                        if !shorter.is_empty() {
                            prefix_set.insert(shorter);
                        }
                    }
                }
            }
            // Add a few longer variants that extend the base prefix.
            let mut ext: Vec<(Vec<u8>, u32)> = st
                .prefix_counts
                .iter()
                .filter(|(k, _v)| k.len() > p.len() && k.starts_with(p.as_slice()))
                .map(|(k, v)| (k.clone(), *v))
                .collect();
            ext.sort_by(|a, b| b.1.cmp(&a.1));
            ext.truncate(4);
            for (k, _) in ext.into_iter() {
                prefix_set.insert(k);
            }
        }

        // Pick top suffixes and expand around them (shorter/longer) for trial-and-error.
        let mut suffixes: Vec<(Vec<u8>, u32)> = st
            .suffix_counts
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect();
        suffixes.sort_by(|a, b| b.1.cmp(&a.1));
        suffixes.truncate(STRING_SUFFIX_CANDIDATES);

        let mut suffix_set: HashSet<Vec<u8>> = suffixes.iter().map(|(k, _)| k.clone()).collect();
        for (s, _) in suffixes.iter() {
            for &len in STRING_PREFIX_SUFFIX_LENS.iter() {
                if len < s.len() {
                    if let Some(shorter) = utf8_safe_suffix(s, len) {
                        if !shorter.is_empty() {
                            suffix_set.insert(shorter);
                        }
                    }
                }
            }
            let mut ext: Vec<(Vec<u8>, u32)> = st
                .suffix_counts
                .iter()
                .filter(|(k, _v)| k.len() > s.len() && k.ends_with(s.as_slice()))
                .map(|(k, v)| (k.clone(), *v))
                .collect();
            ext.sort_by(|a, b| b.1.cmp(&a.1));
            ext.truncate(4);
            for (k, _) in ext.into_iter() {
                suffix_set.insert(k);
            }
        }

        // Pick top ASCII bytes for contains
        let mut bytes: Vec<(u8, u32)> = (0u8..=255u8)
            .map(|b| (b, st.byte_freq[b as usize]))
            .filter(|(b, c)| *c > 0 && (32u8..=126u8).contains(b))
            .collect();
        bytes.sort_by(|a, b| b.1.cmp(&a.1));
        bytes.truncate(STRING_CONTAINS_BYTE_CANDIDATES);

        // Pick contains n-grams (ASCII) to seed stronger CONTAINS/count rules.
        let mut bigrams: Vec<([u8; 2], u32)> = st.bigram_counts.iter().map(|(k, v)| (*k, *v)).collect();
        bigrams.sort_by(|a, b| b.1.cmp(&a.1));
        bigrams.truncate(STRING_CONTAINS_BIGRAM_CANDIDATES);

        let mut trigrams: Vec<([u8; 3], u32)> = st.trigram_counts.iter().map(|(k, v)| (*k, *v)).collect();
        trigrams.sort_by(|a, b| b.1.cmp(&a.1));
        trigrams.truncate(STRING_CONTAINS_TRIGRAM_CANDIDATES);

        // Emit STARTS_WITH/ENDS_WITH candidates.
        for p in prefix_set.into_iter() {
            push_string_candidate_rule(
                &mut rules,
                &mut seen_rules,
                schema_id,
                &db_type,
                StringRuleOp::StartsWith,
                1,
                &p,
            );
            if rules.len() >= STRING_RULE_CANDIDATE_CAP {
                break;
            }
        }

        for s in suffix_set.into_iter() {
            push_string_candidate_rule(
                &mut rules,
                &mut seen_rules,
                schema_id,
                &db_type,
                StringRuleOp::EndsWith,
                1,
                &s,
            );
            if rules.len() >= STRING_RULE_CANDIDATE_CAP {
                break;
            }
        }

        for (b, _) in bytes.into_iter() {
            for &count in STRING_CONTAINS_COUNTS.iter() {
                push_string_candidate_rule(
                    &mut rules,
                    &mut seen_rules,
                    schema_id,
                    &db_type,
                    StringRuleOp::Contains,
                    count,
                    &[b],
                );
                if rules.len() >= STRING_RULE_CANDIDATE_CAP {
                    break;
                }
            }
            if rules.len() >= STRING_RULE_CANDIDATE_CAP {
                break;
            }
        }

        for (bg, _) in bigrams.into_iter() {
            for &count in STRING_CONTAINS_NGRAM_COUNTS.iter() {
                push_string_candidate_rule(
                    &mut rules,
                    &mut seen_rules,
                    schema_id,
                    &db_type,
                    StringRuleOp::Contains,
                    count,
                    &bg,
                );
                if rules.len() >= STRING_RULE_CANDIDATE_CAP {
                    break;
                }
            }
            if rules.len() >= STRING_RULE_CANDIDATE_CAP {
                break;
            }
        }

        for (tg, _) in trigrams.into_iter() {
            for &count in STRING_CONTAINS_NGRAM_COUNTS.iter() {
                push_string_candidate_rule(
                    &mut rules,
                    &mut seen_rules,
                    schema_id,
                    &db_type,
                    StringRuleOp::Contains,
                    count,
                    &tg,
                );
                if rules.len() >= STRING_RULE_CANDIDATE_CAP {
                    break;
                }
            }
            if rules.len() >= STRING_RULE_CANDIDATE_CAP {
                break;
            }
        }

        // Keep a hard cap to avoid pathological candidate explosion.
        if rules.len() > STRING_RULE_CANDIDATE_CAP {
            rules.truncate(STRING_RULE_CANDIDATE_CAP);
        }
        if !rules.is_empty() {
            string_candidate_rules_by_col.push((schema_id, rules));
        }
    }

    log::info!(
        "scan_whole_table_build_rules: Generated {} numeric plans and {} string candidate columns",
        plans.len(),
        string_candidate_rules_by_col.len()
    );
    
    let rules_by_col = Arc::new(rules_by_col);
    let string_rules_by_col = Arc::new(string_candidate_rules_by_col);

    // PASS 2: build pointer-file ranges (RAM-only).
    let pass2_start = std::time::Instant::now();
    
    // Producer for Pass 2
    let (tx2, rx2) = tokio::sync::mpsc::channel::<Vec<(u64, RowPointer)>>(concurrency * 4);
    let rx2 = Arc::new(tokio::sync::Mutex::new(rx2));
    
    let pointers_io_clone_2 = pointers_io.clone();
    let producer_handle_2 = tokio::spawn(async move {
        let mut iter = match RowPointerIterator::new(pointers_io_clone_2).await {
            Ok(i) => i,
            Err(e) => return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("RowPointerIterator 2: {}", e))),
        };
        loop {
            let mut chunk: Vec<(u64, RowPointer)> = Vec::new();
            for _ in 0..*crate::BATCH_SIZE.get().unwrap() {
                match iter.next_row_pointer_with_offset().await {
                    Ok(Some((pos, p))) => chunk.push((pos, p)),
                    Ok(None) => break,
                    Err(e) => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            format!("RowPointerIterator 2: {}", e),
                        ))
                    }
                }
            }
            if chunk.is_empty() { break; }
            if tx2.send(chunk).await.is_err() { break; }
        }
        Ok(())
    });

    let mut handles_2 = Vec::with_capacity(concurrency);

    for _worker_id in 0..concurrency {
        let rx2 = rx2.clone();
        let rows_io = rows_io.clone();
        let numeric_row_fetch = numeric_row_fetch.clone();
        let string_row_fetch = string_row_fetch.clone();
        let rules_by_col = rules_by_col.clone();
        let string_rules_by_col = string_rules_by_col.clone();
        let numeric_schema_id_to_fetch = numeric_schema_id_to_fetch.clone();
        let string_schema_id_to_fetch = string_schema_id_to_fetch.clone();

        handles_2.push(tokio::spawn(async move {
            // Per-worker in-memory range lists per rule.
            let mut numeric_ranges: Vec<Vec<Vec<RowRange>>> = rules_by_col
                .iter()
                .map(|(_, rules)| vec![Vec::new(); rules.len()])
                .collect();
            let mut string_ranges: Vec<Vec<Vec<RowRange>>> = string_rules_by_col
                .iter()
                .map(|(_, rules)| vec![Vec::new(); rules.len()])
                .collect();

            // Maintain open ranges across chunks to reduce fragmentation.
            let mut open_numeric: Vec<Vec<Option<RowRange>>> = rules_by_col
                .iter()
                .map(|(_, rules)| vec![None; rules.len()])
                .collect();
            let mut open_string: Vec<Vec<Option<RowRange>>> = string_rules_by_col
                .iter()
                .map(|(_, rules)| vec![None; rules.len()])
                .collect();

            // Precompute thresholds per numeric column for fast bound searches.
            let numeric_thresholds: Vec<Vec<NumericScalar>> = rules_by_col
                .iter()
                .map(|(_, rules)| {
                    let mut v = Vec::with_capacity(rules.len() / 2);
                    for pair in rules.chunks_exact(2) {
                        v.push(pair[0].value);
                    }
                    v
                })
                .collect();

            let mut prev_lower: Vec<usize> = numeric_thresholds.iter().map(|_| 0usize).collect();
            let mut prev_upper: Vec<usize> = numeric_thresholds.iter().map(|_| 0usize).collect();

            let mut row_num = crate::core::row::row::Row::default();
            let mut row_str = crate::core::row::row::Row::default();

            use crate::core::row::row_pointer::ROW_POINTER_RECORD_LEN;
            let row_len = ROW_POINTER_RECORD_LEN as u64;

            #[inline]
            fn close_range(open: &mut Option<RowRange>, dst: &mut Vec<RowRange>) {
                if let Some(r) = open.take() {
                    dst.push(r);
                }
            }

            #[inline]
            fn append_point(open: &mut Option<RowRange>, dst: &mut Vec<RowRange>, pos: u64, row_len: u64) {
                if let Some(r) = open {
                    let expected_next = r.start_pointer_pos.saturating_add(r.row_count.saturating_mul(row_len));
                    if pos == expected_next {
                        r.row_count = r.row_count.saturating_add(1);
                        return;
                    }
                }

                if let Some(r) = open.take() {
                    dst.push(r);
                }
                *open = Some(RowRange {
                    start_pointer_pos: pos,
                    row_count: 1,
                });
            }
            
            loop {
                let chunk_opt = {
                    let mut lock = rx2.lock().await;
                    lock.recv().await
                };
                let Some(chunk) = chunk_opt else { break; };

                for (pointer_pos, p) in chunk {
                    if p.deleted { continue; }

                    if !rules_by_col.is_empty() {
                        if p
                            .fetch_row_reuse_async(rows_io.clone(), &numeric_row_fetch, &mut row_num)
                            .await
                            .is_ok()
                        {
                            for (col_rules_idx, (schema_id, _rules)) in rules_by_col.iter().enumerate() {
                                let Some((fetch_idx, db_type)) = numeric_schema_id_to_fetch.get(schema_id).cloned() else { continue; };
                                let col = &row_num.columns[fetch_idx];
                                let Some(v) = decode_numeric_value(&db_type, col.data.into_slice()) else { continue; };

                                let thresholds = &numeric_thresholds[col_rules_idx];
                                if thresholds.is_empty() {
                                    continue;
                                }

                                // lower: first index >= v (for GT)
                                // upper: first index > v (for LT)
                                let search = |val: NumericScalar| thresholds.binary_search_by(|t| cmp_scalar(*t, val));
                                let lower = match search(v) {
                                    Ok(idx) => idx,
                                    Err(idx) => idx,
                                };
                                let upper = match search(v) {
                                    Ok(idx) => idx.saturating_add(1),
                                    Err(idx) => idx,
                                };

                                let num_pairs = thresholds.len();
                                let prev_lo = prev_lower[col_rules_idx];
                                let prev_up = prev_upper[col_rules_idx];

                                // GT transitions: active set [0, lower)
                                if lower > prev_lo {
                                    for i in prev_lo..lower {
                                        let idx = i * 2 + 1;
                                        close_range(&mut open_numeric[col_rules_idx][idx], &mut numeric_ranges[col_rules_idx][idx]);
                                    }
                                }

                                // LT transitions: active set [upper, num_pairs)
                                if upper > prev_up {
                                    for i in prev_up..upper {
                                        let idx = i * 2;
                                        close_range(&mut open_numeric[col_rules_idx][idx], &mut numeric_ranges[col_rules_idx][idx]);
                                    }
                                }

                                // Extend currently active GT rules.
                                for i in 0..lower {
                                    let idx = i * 2 + 1;
                                    append_point(
                                        &mut open_numeric[col_rules_idx][idx],
                                        &mut numeric_ranges[col_rules_idx][idx],
                                        pointer_pos,
                                        row_len,
                                    );
                                }

                                // Extend currently active LT rules.
                                for i in upper..num_pairs {
                                    let idx = i * 2;
                                    append_point(
                                        &mut open_numeric[col_rules_idx][idx],
                                        &mut numeric_ranges[col_rules_idx][idx],
                                        pointer_pos,
                                        row_len,
                                    );
                                }

                                prev_lower[col_rules_idx] = lower;
                                prev_upper[col_rules_idx] = upper;
                            }
                        }
                    }

                    if !string_rules_by_col.is_empty() {
                        if p
                            .fetch_row_reuse_async(rows_io.clone(), &string_row_fetch, &mut row_str)
                            .await
                            .is_ok()
                        {
                            for (col_rules_idx, (schema_id, rules)) in string_rules_by_col.iter().enumerate() {
                                let Some(fetch_idx) = string_schema_id_to_fetch.get(schema_id).copied() else { continue; };
                                let col = &row_str.columns[fetch_idx];
                                let bytes = col.data.into_slice();
                                // Only evaluate on valid UTF-8 row values.
                                if std::str::from_utf8(bytes).is_err() {
                                    continue;
                                }

                                for (rule_idx, rule) in rules.iter().enumerate() {
                                    let is_match = string_satisfies_rule(bytes, rule.op, rule.value.as_bytes(), rule.count);
                                    if is_match {
                                        match &mut open_string[col_rules_idx][rule_idx] {
                                            None => {
                                                open_string[col_rules_idx][rule_idx] = Some(RowRange {
                                                    start_pointer_pos: pointer_pos,
                                                    row_count: 1,
                                                });
                                            }
                                            Some(r) => {
                                                let expected_next = r.start_pointer_pos
                                                    .saturating_add(r.row_count.saturating_mul(row_len));
                                                if pointer_pos == expected_next {
                                                    r.row_count = r.row_count.saturating_add(1);
                                                } else {
                                                    string_ranges[col_rules_idx][rule_idx].push(*r);
                                                    *r = RowRange {
                                                        start_pointer_pos: pointer_pos,
                                                        row_count: 1,
                                                    };
                                                }
                                            }
                                        }
                                    } else if let Some(r) = open_string[col_rules_idx][rule_idx].take() {
                                        string_ranges[col_rules_idx][rule_idx].push(r);
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // Flush any open ranges.
            for col_rules_idx in 0..open_numeric.len() {
                for rule_idx in 0..open_numeric[col_rules_idx].len() {
                    if let Some(r) = open_numeric[col_rules_idx][rule_idx].take() {
                        numeric_ranges[col_rules_idx][rule_idx].push(r);
                    }
                }
            }
            for col_rules_idx in 0..open_string.len() {
                for rule_idx in 0..open_string[col_rules_idx].len() {
                    if let Some(r) = open_string[col_rules_idx][rule_idx].take() {
                        string_ranges[col_rules_idx][rule_idx].push(r);
                    }
                }
            }

            (numeric_ranges, string_ranges)
        }));
    }

    if let Err(e) = producer_handle_2.await.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))? {
        return Err(e);
    }
    
    // Merge worker ranges into per-column, per-rule lists.
    let mut numeric_raw_by_col: Vec<Vec<Vec<RowRange>>> = rules_by_col
        .iter()
        .map(|(_, rules)| vec![Vec::new(); rules.len()])
        .collect();
    let mut string_raw_by_col: Vec<Vec<Vec<RowRange>>> = string_rules_by_col
        .iter()
        .map(|(_, rules)| vec![Vec::new(); rules.len()])
        .collect();

    for h in handles_2 {
        let (num_worker, str_worker) = h
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
        for col_idx in 0..numeric_raw_by_col.len() {
            for rule_idx in 0..numeric_raw_by_col[col_idx].len() {
                numeric_raw_by_col[col_idx][rule_idx].extend(num_worker[col_idx][rule_idx].iter().copied());
            }
        }
        for col_idx in 0..string_raw_by_col.len() {
            for rule_idx in 0..string_raw_by_col[col_idx].len() {
                string_raw_by_col[col_idx][rule_idx].extend(str_worker[col_idx][rule_idx].iter().copied());
            }
        }
    }

    #[derive(Clone)]
    struct InMemColumnRuleStore {
        ranges: Vec<Vec<RowRange>>, // normalized per rule_idx
        range_counts: Vec<u32>,
        covered_rows: Vec<u64>,
    }

    fn build_inmem_store(raw_by_rule: Vec<Vec<RowRange>>) -> InMemColumnRuleStore {
        let mut ranges: Vec<Vec<RowRange>> = Vec::with_capacity(raw_by_rule.len());
        let mut range_counts: Vec<u32> = vec![0u32; raw_by_rule.len()];
        let mut covered_rows: Vec<u64> = vec![0u64; raw_by_rule.len()];

        for (idx, raw) in raw_by_rule.into_iter().enumerate() {
            if raw.is_empty() {
                ranges.push(Vec::new());
                continue;
            }
            let norm = normalize_ranges_smallvec(raw).into_vec();
            let count: u32 = norm.len().try_into().unwrap_or(u32::MAX);
            let covered = ranges_candidate_rows(&norm);
            range_counts[idx] = count;
            covered_rows[idx] = covered;
            ranges.push(norm);
        }

        InMemColumnRuleStore {
            ranges,
            range_counts,
            covered_rows,
        }
    }

    let mut numeric_range_stores: Vec<InMemColumnRuleStore> = Vec::with_capacity(numeric_raw_by_col.len());
    for raw in numeric_raw_by_col.into_iter() {
        numeric_range_stores.push(build_inmem_store(raw));
    }

    let mut string_range_stores: Vec<InMemColumnRuleStore> = Vec::with_capacity(string_raw_by_col.len());
    for raw in string_raw_by_col.into_iter() {
        string_range_stores.push(build_inmem_store(raw));
    }

    log::info!("scan_whole_table_build_rules: Pass 2 (range building) done in {:.2?}", pass2_start.elapsed());

    // Choose the best thresholds per column from candidates.
    let mut final_chosen_rules_by_col: Vec<(u64, Vec<NumericCorrelationRule>)> = Vec::new();
    let opt_start = std::time::Instant::now();

    // Build schema_id -> numeric column index for range store access.
    let mut numeric_schema_idx: HashMap<u64, usize> = HashMap::new();
    for (idx, (schema_id, _)) in rules_by_col.iter().enumerate() {
        numeric_schema_idx.insert(*schema_id, idx);
    }

    for plan in plans.iter() {
        let Some(&col_idx) = numeric_schema_idx.get(&plan.schema_id) else {
            continue;
        };
        let candidate_rules = &rules_by_col[col_idx].1;
        let store = &numeric_range_stores[col_idx];

        #[derive(Clone)]
        struct CandidatePacked {
            scalar: NumericScalar,
            lt_rule_idx: Option<usize>,
            gt_rule_idx: Option<usize>,
            lt_score: u64,
            gt_score: u64,
            score: u64,
        }

        let mut candidates: BTreeMap<[u8; 16], CandidatePacked> = BTreeMap::new();
        for (rule_idx, r) in candidate_rules.iter().enumerate() {
            let key = scalar_key(r.value);
            let entry = candidates.entry(key).or_insert_with(|| CandidatePacked {
                scalar: r.value,
                lt_rule_idx: None,
                gt_rule_idx: None,
                lt_score: u64::MAX,
                gt_score: u64::MAX,
                score: u64::MAX,
            });

            let range_count = store.range_counts[rule_idx] as u64;
            let covered = store.covered_rows[rule_idx];
            let s = range_count
                .saturating_mul(plan.range_weight)
                .saturating_add(covered.saturating_mul(plan.candidate_rows_weight));

            match r.op {
                NumericRuleOp::LessThan => {
                    entry.lt_rule_idx = Some(rule_idx);
                    entry.lt_score = s;
                }
                NumericRuleOp::GreaterThan => {
                    entry.gt_rule_idx = Some(rule_idx);
                    entry.gt_score = s;
                }
            }
        }

        for (_k, c) in candidates.iter_mut() {
            let lt = if c.lt_score == u64::MAX { 0 } else { c.lt_score };
            let gt = if c.gt_score == u64::MAX { 0 } else { c.gt_score };
            c.score = lt.saturating_add(gt);
        }

        let mut chosen_keys: Vec<[u8; 16]> = Vec::new();
        for slot in plan.slot_keys.iter() {
            let mut best: Option<([u8; 16], u64)> = None;
            for key in slot.iter().copied() {
                let Some(c) = candidates.get(&key) else { continue; };
                best = Some(match best {
                    None => (key, c.score),
                    Some((best_key, best_score)) => {
                        if c.score < best_score {
                            (key, c.score)
                        } else {
                            (best_key, best_score)
                        }
                    }
                });
            }
            if let Some((k, _)) = best {
                chosen_keys.push(k);
            }
        }

        chosen_keys.sort();
        chosen_keys.dedup();

        // If duplicates collapsed us below THRESHOLD_COUNT, fill with globally best candidates.
        if chosen_keys.len() < THRESHOLD_COUNT {
            let mut all: Vec<([u8; 16], u64)> = candidates
                .iter()
                .map(|(k, c)| (*k, c.score))
                .collect();
            all.sort_by_key(|(_k, score)| *score);
            for (k, _score) in all.into_iter() {
                if chosen_keys.len() >= THRESHOLD_COUNT {
                    break;
                }
                if !chosen_keys.contains(&k) {
                    chosen_keys.push(k);
                }
            }
        }

        // Order thresholds monotonically for stable output.
        let mut chosen: Vec<CandidatePacked> = chosen_keys
            .into_iter()
            .filter_map(|k| candidates.get(&k).cloned())
            .collect();
        chosen.sort_by(|a, b| cmp_scalar(a.scalar, b.scalar));
        if chosen.len() > THRESHOLD_COUNT {
            chosen.truncate(THRESHOLD_COUNT);
        }

        let mut out_rules: Vec<NumericCorrelationRule> = Vec::with_capacity(chosen.len() * 2);
        for c in chosen.into_iter() {
            let mut lt_ranges: Vec<RowRange> = Vec::new();
            let mut gt_ranges: Vec<RowRange> = Vec::new();

            if let Some(rule_idx) = c.lt_rule_idx {
                lt_ranges = store.ranges[rule_idx].clone();
            }
            if let Some(rule_idx) = c.gt_rule_idx {
                gt_ranges = store.ranges[rule_idx].clone();
            }

            let lt_ranges = optimize_ranges_dynamic(lt_ranges, plan.range_weight, plan.candidate_rows_weight);
            let gt_ranges = optimize_ranges_dynamic(gt_ranges, plan.range_weight, plan.candidate_rows_weight);

            out_rules.push(NumericCorrelationRule {
                column_schema_id: plan.schema_id,
                column_type: plan.db_type.clone(),
                op: NumericRuleOp::LessThan,
                value: c.scalar,
                ranges: SmallVec::<[RowRange; 64]>::from_vec(lt_ranges),
            });
            out_rules.push(NumericCorrelationRule {
                column_schema_id: plan.schema_id,
                column_type: plan.db_type.clone(),
                op: NumericRuleOp::GreaterThan,
                value: c.scalar,
                ranges: SmallVec::<[RowRange; 64]>::from_vec(gt_ranges),
            });
        }
        
        // Post-processing: Filter, deduplicate, and backfill coverage gaps.
        {
            use crate::core::row::row_pointer::ROW_POINTER_RECORD_LEN;
            let total_rows = pointers_io.get_len().await / (ROW_POINTER_RECORD_LEN as u64);

            if total_rows > 0 && !out_rules.is_empty() {
                // For small tables, integer division can yield 0 and would discard all rules.
                let threshold = ((total_rows as u128 * MAX_RULE_COVERAGE_PERCENT as u128) / 100) as u64;
                let threshold = threshold.max(MIN_RULE_COVERAGE_ROWS).max(1);
                out_rules.retain(|r| ranges_candidate_rows(&r.ranges) <= threshold);
            }

            if !out_rules.is_empty() {
                let mut seen_coverage = std::collections::HashSet::new();
                out_rules.retain(|r| {
                    let covered = ranges_candidate_rows(&r.ranges);
                    let op_byte = r.op as u8;
                    let key = (op_byte, covered);
                    seen_coverage.insert(key)
                });
            }

            if total_rows > 0 {
                let mut merged: Vec<RowRange> = Vec::new();
                for r in out_rules.iter() {
                    merged.extend_from_slice(&r.ranges);
                }

                let uncovered = build_uncovered_ranges(total_rows, &merged);
                if !uncovered.is_empty() {
                    let sentinel = numeric_catchall_scalar(&plan.db_type);
                    out_rules.push(NumericCorrelationRule {
                        column_schema_id: plan.schema_id,
                        column_type: plan.db_type.clone(),
                        op: NumericRuleOp::LessThan,
                        value: sentinel,
                        ranges: SmallVec::<[RowRange; 64]>::from_vec(uncovered),
                    });
                }
            }
        }

        final_chosen_rules_by_col.push((plan.schema_id, out_rules));
    }
    log::info!("scan_whole_table_build_rules: Plan optimization done in {:.2?}", opt_start.elapsed());
    log::info!("scan_whole_table_build_rules: completed total in {:.2?}", total_start.elapsed());

    // Select best string rules per column (disk-backed, RAM efficient).
    let mut final_string_rules_by_col: Vec<(u64, Vec<StringCorrelationRule>)> = Vec::new();
    use crate::core::row::row_pointer::ROW_POINTER_RECORD_LEN;
    let total_rows = pointers_io.get_len().await / (ROW_POINTER_RECORD_LEN as u64);

    for (col_idx, (schema_id, rules)) in string_rules_by_col.iter().enumerate() {
        let store = &string_range_stores[col_idx];
        let broad_threshold = if total_rows > 0 {
            // For small tables, integer division can yield 0 and would discard all candidates.
            (((total_rows as u128) * (MAX_RULE_COVERAGE_PERCENT as u128)) / 100) as u64
        } else {
            u64::MAX
        };
        let broad_threshold = broad_threshold.max(MIN_RULE_COVERAGE_ROWS).max(1);

        #[derive(Clone)]
        struct CandMeta {
            rule_idx: usize,
            score: u64,
            covered: u64,
            op: StringRuleOp,
            count: u8,
            value: String,
            column_type: DbType,
        }

        // Dedup + prefilter using store stats.
        let mut best: HashMap<(u8, u8, String), CandMeta> = HashMap::new();
        for (rule_idx, r) in rules.iter().enumerate() {
            let covered = store.covered_rows[rule_idx];
            if covered == 0 || covered > broad_threshold {
                continue;
            }
            let rc = store.range_counts[rule_idx] as u64;
            let score = rc
                .saturating_mul(SCORE_RANGE_COUNT_WEIGHT_INT)
                .saturating_add(covered.saturating_mul(SCORE_CANDIDATE_ROWS_WEIGHT_INT));

            let key = (string_rule_op_id(r.op), r.count, r.value.clone());
            let cand = CandMeta {
                rule_idx,
                score,
                covered,
                op: r.op,
                count: r.count,
                value: r.value.clone(),
                column_type: r.column_type.clone(),
            };

            match best.get(&key) {
                None => {
                    best.insert(key, cand);
                }
                Some(prev) => {
                    if cand.score < prev.score {
                        best.insert(key, cand);
                    }
                }
            }
        }

        let mut cands: Vec<CandMeta> = best.into_values().collect();
        if cands.is_empty() {
            continue;
        }
        cands.sort_by(|a, b| {
            a.score
                .cmp(&b.score)
                .then_with(|| a.covered.cmp(&b.covered))
                .then_with(|| b.value.len().cmp(&a.value.len()))
        });
        if cands.len() > STRING_RULE_CANDIDATE_CAP {
            cands.truncate(STRING_RULE_CANDIDATE_CAP);
        }
        let max_out = std::cmp::min(STRING_FINAL_RULES_MAX, cands.len());

        // Small cache for on-demand range loads.
        let mut range_cache: HashMap<usize, RangeVec> = HashMap::new();
        let mut cache_order: Vec<usize> = Vec::new();
        let mut load_ranges = |rule_idx: usize| -> io::Result<RangeVec> {
            if let Some(r) = range_cache.get(&rule_idx) {
                return Ok(r.clone());
            }
            let v = store.ranges[rule_idx].clone();
            let rv = RangeVec::from_vec(v);
            // Tiny LRU-ish cap.
            range_cache.insert(rule_idx, rv.clone());
            cache_order.push(rule_idx);
            if cache_order.len() > 64 {
                if let Some(old) = cache_order.first().copied() {
                    range_cache.remove(&old);
                    cache_order.remove(0);
                }
            }
            Ok(rv)
        };

        // Greedy selection with overlap penalty.
        let mut selected: Vec<usize> = Vec::new();
        let mut selected_ranges: Vec<RangeVec> = Vec::new();
        let mut selected_set: HashSet<usize> = HashSet::new();

        for _ in 0..max_out {
            let mut best_idx: Option<usize> = None;
            let mut best_cost = u64::MAX;

            for idx in 0..cands.len() {
                if selected_set.contains(&idx) {
                    continue;
                }
                let cand = &cands[idx];
                let ranges = match load_ranges(cand.rule_idx) {
                    Ok(r) => r,
                    Err(_) => continue,
                };

                let mut overlap = 0u64;
                for sr in selected_ranges.iter() {
                    overlap = overlap
                        .saturating_add(row_ranges_intersection_count(&ranges, sr));
                }
                let cost = cand
                    .score
                    .saturating_add(overlap.saturating_mul(STRING_OVERLAP_PENALTY_WEIGHT));
                if cost < best_cost {
                    best_cost = cost;
                    best_idx = Some(idx);
                }
            }

            let Some(chosen) = best_idx else { break; };
            selected_set.insert(chosen);
            let rule_idx = cands[chosen].rule_idx;
            let ranges = load_ranges(rule_idx).unwrap_or_else(|_| RangeVec::new());
            selected.push(chosen);
            selected_ranges.push(ranges);
        }

        if selected.is_empty() {
            continue;
        }

        let objective = |sel: &[usize], sel_ranges: &[RangeVec]| -> u64 {
            let mut sum = 0u64;
            for &i in sel.iter() {
                sum = sum.saturating_add(cands[i].score);
            }
            for i in 0..sel_ranges.len() {
                for j in (i + 1)..sel_ranges.len() {
                    let o = row_ranges_intersection_count(&sel_ranges[i], &sel_ranges[j]);
                    sum = sum.saturating_add(o.saturating_mul(STRING_OVERLAP_PENALTY_WEIGHT));
                }
            }
            sum
        };

        // Local search swaps (neighbor-based).
        for _pass in 0..STRING_LOCAL_SEARCH_PASSES {
            let mut improved = false;
            let cur_obj = objective(&selected, &selected_ranges);

            'outer: for pos in 0..selected.len() {
                let cur_idx = selected[pos];
                let cur_meta = &cands[cur_idx];

                let mut best_swap: Option<usize> = None;
                let mut best_obj = cur_obj;

                for idx in 0..cands.len() {
                    if selected_set.contains(&idx) {
                        continue;
                    }
                    // Neighbor heuristic.
                    let cand = &cands[idx];
                    let a = StringCorrelationRule {
                        column_schema_id: *schema_id,
                        column_type: cur_meta.column_type.clone(),
                        op: cur_meta.op,
                        count: cur_meta.count,
                        value: cur_meta.value.clone(),
                        ranges: RangeVec::new(),
                    };
                    let b = StringCorrelationRule {
                        column_schema_id: *schema_id,
                        column_type: cand.column_type.clone(),
                        op: cand.op,
                        count: cand.count,
                        value: cand.value.clone(),
                        ranges: RangeVec::new(),
                    };
                    if !string_rules_neighbor(&a, &b) {
                        continue;
                    }

                    let cand_ranges = match load_ranges(cand.rule_idx) {
                        Ok(r) => r,
                        Err(_) => continue,
                    };

                    let mut test_sel = selected.clone();
                    let mut test_ranges = selected_ranges.clone();
                    test_sel[pos] = idx;
                    test_ranges[pos] = cand_ranges;
                    let obj = objective(&test_sel, &test_ranges);
                    if obj < best_obj {
                        best_obj = obj;
                        best_swap = Some(idx);
                    }
                }

                if let Some(new_idx) = best_swap {
                    selected_set.remove(&cur_idx);
                    selected_set.insert(new_idx);
                    selected[pos] = new_idx;
                    let r = load_ranges(cands[new_idx].rule_idx).unwrap_or_else(|_| RangeVec::new());
                    selected_ranges[pos] = r;
                    improved = true;
                    break 'outer;
                }
            }

            if !improved {
                break;
            }
        }

        // Stable-ish ordering for output.
        selected.sort_by(|&a, &b| {
            string_rule_op_id(cands[a].op)
                .cmp(&string_rule_op_id(cands[b].op))
                .then_with(|| cands[a].score.cmp(&cands[b].score))
                .then_with(|| cands[b].value.len().cmp(&cands[a].value.len()))
                .then_with(|| cands[a].value.cmp(&cands[b].value))
        });

        let mut out_rules: Vec<StringCorrelationRule> = Vec::with_capacity(selected.len());
        for idx in selected.into_iter() {
            let cand = &cands[idx];
            let ranges = RangeVec::from_vec(store.ranges[cand.rule_idx].clone());

            out_rules.push(StringCorrelationRule {
                column_schema_id: *schema_id,
                column_type: cand.column_type.clone(),
                op: cand.op,
                count: cand.count,
                value: cand.value.clone(),
                ranges,
            });
        }

        // Add a catch-all rule to guarantee full coverage even if optimized rules leave gaps.
        if total_rows > 0 {
            let mut merged: Vec<RowRange> = Vec::new();
            for r in out_rules.iter() {
                merged.extend_from_slice(&r.ranges);
            }

            let uncovered = build_uncovered_ranges(total_rows, &merged);
            if !uncovered.is_empty() {
                let column_type = rules
                    .get(0)
                    .map(|r| r.column_type.clone())
                    .unwrap_or(DbType::STRING);

                out_rules.push(StringCorrelationRule {
                    column_schema_id: *schema_id,
                    column_type,
                    op: StringRuleOp::StartsWith,
                    count: 1,
                    value: String::new(),
                    ranges: RangeVec::from_vec(uncovered),
                });
            }
        }

        if !out_rules.is_empty() {
            final_string_rules_by_col.push((*schema_id, out_rules));
        }
    }

    // Persist to disk for durability across restarts.
    let rules_io = CorrelationRuleStore::open_rules_io(base_io.clone(), table_name).await;
    CorrelationRuleStore::save_rules_atomic::<S>(
        rules_io,
        &final_chosen_rules_by_col,
        &final_string_rules_by_col,
    )
    .await?;

    // Publish rules in RAM for fast access.
    in_memory_rules::set_table_rules(table_name, final_chosen_rules_by_col.clone(), final_string_rules_by_col.clone());

    // Rules are now rebuilt successfully; clear the dirty pointer tracker for this table.
    crate::core::sme_v2::dirty_row_tracker::dirty_row_tracker().clear_table(table_name);

    // NOTE: debug JSON writer intentionally disabled by default.

    Ok(())
}

fn string_rule_op_id(op: StringRuleOp) -> u8 {
    match op {
        StringRuleOp::StartsWith => 0,
        StringRuleOp::EndsWith => 1,
        StringRuleOp::Contains => 2,
    }
}

fn push_string_candidate_rule(
    rules: &mut Vec<StringCorrelationRule>,
    seen_rules: &mut HashSet<(u8, u8, Vec<u8>)>,
    schema_id: u64,
    db_type: &DbType,
    op: StringRuleOp,
    count: u8,
    bytes: &[u8],
) {
    if bytes.is_empty() {
        return;
    }
    // Only accept UTF-8 rule values.
    if std::str::from_utf8(bytes).is_err() {
        return;
    }

    let key = (string_rule_op_id(op), count, bytes.to_vec());
    if !seen_rules.insert(key) {
        return;
    }

    let value = unsafe { std::str::from_utf8_unchecked(bytes) }.to_string();
    rules.push(StringCorrelationRule {
        column_schema_id: schema_id,
        column_type: db_type.clone(),
        op,
        count,
        value,
        ranges: RangeVec::new(),
    });
}

fn row_ranges_intersection_count(a: &[RowRange], b: &[RowRange]) -> u64 {
    if a.is_empty() || b.is_empty() {
        return 0;
    }
    use crate::core::row::row_pointer::ROW_POINTER_RECORD_LEN;
    let len = ROW_POINTER_RECORD_LEN as u64;

    let mut i = 0usize;
    let mut j = 0usize;
    let mut total = 0u64;

    while i < a.len() && j < b.len() {
        let a_start = a[i].start_pointer_pos;
        let a_end = a[i].end_pointer_pos_exclusive();
        let b_start = b[j].start_pointer_pos;
        let b_end = b[j].end_pointer_pos_exclusive();

        let start = a_start.max(b_start);
        let end = a_end.min(b_end);
        if end > start {
            total = total.saturating_add((end - start) / len);
        }

        if a_end < b_end {
            i += 1;
        } else {
            j += 1;
        }
    }

    total
}

fn string_rules_neighbor(a: &StringCorrelationRule, b: &StringCorrelationRule) -> bool {
    if a.op != b.op {
        return false;
    }

    let ab = a.value.as_bytes();
    let bb = b.value.as_bytes();
    if ab.is_empty() || bb.is_empty() {
        return false;
    }

    match a.op {
        StringRuleOp::StartsWith => {
            let diff = ab.len().abs_diff(bb.len());
            diff <= 8 && (ab.starts_with(bb) || bb.starts_with(ab))
        }
        StringRuleOp::EndsWith => {
            let diff = ab.len().abs_diff(bb.len());
            diff <= 8 && (ab.ends_with(bb) || bb.ends_with(ab))
        }
        StringRuleOp::Contains => {
            if a.value == b.value {
                return a.count != b.count;
            }
            let diff = ab.len().abs_diff(bb.len());
            diff <= 1 && (ab.starts_with(bb) || ab.ends_with(bb) || bb.starts_with(ab) || bb.ends_with(ab))
        }
    }
}

fn decode_numeric_value(db_type: &DbType, bytes: &[u8]) -> Option<NumericScalar> {
    match db_type {
        DbType::I8 => Some(NumericScalar::Signed(i8::from_le_bytes(bytes[0..1].try_into().ok()?) as i128)),
        DbType::I16 => Some(NumericScalar::Signed(i16::from_le_bytes(bytes[0..2].try_into().ok()?) as i128)),
        DbType::I32 => Some(NumericScalar::Signed(i32::from_le_bytes(bytes[0..4].try_into().ok()?) as i128)),
        DbType::I64 => Some(NumericScalar::Signed(i64::from_le_bytes(bytes[0..8].try_into().ok()?) as i128)),
        DbType::I128 => Some(NumericScalar::Signed(i128::from_le_bytes(bytes[0..16].try_into().ok()?))),

        DbType::U8 => Some(NumericScalar::Unsigned(u8::from_le_bytes(bytes[0..1].try_into().ok()?) as u128)),
        DbType::U16 => Some(NumericScalar::Unsigned(u16::from_le_bytes(bytes[0..2].try_into().ok()?) as u128)),
        DbType::U32 => Some(NumericScalar::Unsigned(u32::from_le_bytes(bytes[0..4].try_into().ok()?) as u128)),
        DbType::U64 => Some(NumericScalar::Unsigned(u64::from_le_bytes(bytes[0..8].try_into().ok()?) as u128)),
        DbType::U128 => Some(NumericScalar::Unsigned(u128::from_le_bytes(bytes[0..16].try_into().ok()?))),

        DbType::F32 => Some(NumericScalar::Float(f32::from_le_bytes(bytes[0..4].try_into().ok()?) as f64)),
        DbType::F64 => Some(NumericScalar::Float(f64::from_le_bytes(bytes[0..8].try_into().ok()?))),
        _ => None,
    }
}

fn value_satisfies_rule(v: NumericScalar, op: NumericRuleOp, threshold: NumericScalar) -> bool {
    match (v, threshold) {
        (NumericScalar::Signed(x), NumericScalar::Signed(t)) => match op {
            NumericRuleOp::LessThan => x < t,
            NumericRuleOp::GreaterThan => x > t,
        },
        (NumericScalar::Unsigned(x), NumericScalar::Unsigned(t)) => match op {
            NumericRuleOp::LessThan => x < t,
            NumericRuleOp::GreaterThan => x > t,
        },
        (NumericScalar::Float(x), NumericScalar::Float(t)) => match op {
            NumericRuleOp::LessThan => x < t,
            NumericRuleOp::GreaterThan => x > t,
        },
        _ => false,
    }
}

fn utf8_safe_prefix(bytes: &[u8], max_len: usize) -> Option<Vec<u8>> {
    let n = std::cmp::min(max_len, bytes.len());
    for k in (0..=n).rev() {
        if std::str::from_utf8(&bytes[..k]).is_ok() {
            return Some(bytes[..k].to_vec());
        }
    }
    None
}

fn utf8_safe_suffix(bytes: &[u8], max_len: usize) -> Option<Vec<u8>> {
    let n = std::cmp::min(max_len, bytes.len());
    let start0 = bytes.len().saturating_sub(n);
    for start in start0..=bytes.len() {
        let slice = &bytes[start..];
        if slice.len() <= max_len && std::str::from_utf8(slice).is_ok() {
            return Some(slice.to_vec());
        }
    }
    None
}

fn count_substring_occurrences(haystack: &[u8], needle: &[u8]) -> u64 {
    if needle.is_empty() {
        return 0;
    }
    if haystack.len() < needle.len() {
        return 0;
    }

    let mut i = 0usize;
    let mut count = 0u64;
    while i + needle.len() <= haystack.len() {
        if &haystack[i..i + needle.len()] == needle {
            count += 1;
            // Non-overlapping by default.
            i += needle.len();
        } else {
            i += 1;
        }
    }
    count
}

fn string_satisfies_rule(value: &[u8], op: StringRuleOp, needle: &[u8], count: u8) -> bool {
    if needle.is_empty() {
        return false;
    }
    match op {
        StringRuleOp::StartsWith => value.starts_with(needle),
        StringRuleOp::EndsWith => value.ends_with(needle),
        StringRuleOp::Contains => {
            if count == 0 {
                !value.windows(needle.len()).any(|w| w == needle)
            } else {
                count_substring_occurrences(value, needle) >= (count as u64)
            }
        }
    }
}
