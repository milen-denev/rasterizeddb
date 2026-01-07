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
#[cfg(debug_assertions)]
use log::warn;
use log::error;
use smallvec::SmallVec;
use std::{
    collections::BTreeMap,
    cmp::Ordering as CmpOrdering,
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
    rule_store::CorrelationRuleStore,
    rules::{normalize_ranges, NumericCorrelationRule, NumericRuleOp, NumericScalar, RowRange},
};

#[cfg(debug_assertions)]
use serde::Serialize;

const RULES_SIZE_VARIABLE: usize = 48;

const SAMPLE_CAP: usize = 1024 * 4 * RULES_SIZE_VARIABLE;
const THRESHOLD_COUNT: usize = 8 * RULES_SIZE_VARIABLE;

// Candidate search breadth around the base percentile indices.
// Kept intentionally small because this runs during full-table scans.
const CANDIDATE_STEPS: [isize; 11] = [-5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5];

// Scoring weights for choosing thresholds.
// For integer-like columns, we heavily penalize a high number of disjoint ranges.
// For float columns, we penalize huge candidate-row sets more strongly (otherwise
// the optimizer may pick thresholds that match "almost everything" in one big range).
const SCORE_RANGE_COUNT_WEIGHT_INT: u64 = 256;
const SCORE_CANDIDATE_ROWS_WEIGHT_INT: u64 = 1;

const SCORE_RANGE_COUNT_WEIGHT_FLOAT: u64 = 64;
const SCORE_CANDIDATE_ROWS_WEIGHT_FLOAT: u64 = 64;

const FLOAT_COARSEN_MAX_CANDIDATE_MULTIPLIER: u64 = 64;
const FLOAT_COARSEN_TARGET_MAX_RANGES: usize = 256;

// Max percentage of the total rows a single rule is allowed to cover (1-100).
// Rules covering more than this are discarded as being "too broad" (essentially full-table scans).
const MAX_RULE_COVERAGE_PERCENT: u64 = 10;

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

const POINTERS_FINGERPRINT_CHUNK: usize = 16 * 1024;

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
        samples: Vec<i128>,
    },
    Unsigned {
        min: u128,
        max: u128,
        seen: u64,
        samples: Vec<u128>,
    },
    Float {
        min: f64,
        max: f64,
        seen: u64,
        samples: Vec<f64>,
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

    fn push_sample_i128(samples: &mut Vec<i128>, seen: u64, v: i128) {
        if samples.len() < SAMPLE_CAP {
            samples.push(v);
            return;
        }
        // Reservoir sampling.
        let j = fastrand::u64(..seen);
        if (j as usize) < samples.len() {
            samples[j as usize] = v;
        }
    }

    fn push_sample_u128(samples: &mut Vec<u128>, seen: u64, v: u128) {
        if samples.len() < SAMPLE_CAP {
            samples.push(v);
            return;
        }
        let j = fastrand::u64(..seen);
        if (j as usize) < samples.len() {
            samples[j as usize] = v;
        }
    }

    fn push_sample_f64(samples: &mut Vec<f64>, seen: u64, v: f64) {
        if samples.len() < SAMPLE_CAP {
            samples.push(v);
            return;
        }
        let j = fastrand::u64(..seen);
        if (j as usize) < samples.len() {
            samples[j as usize] = v;
        }
    }

    fn update(&mut self, v: NumericScalar) {
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
                Self::push_sample_i128(samples, *seen, x);
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
                Self::push_sample_u128(samples, *seen, x);
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
                Self::push_sample_f64(samples, *seen, x);
            }
            _ => {}
        }
    }

    fn sorted_unique_samples(&self) -> Vec<NumericScalar> {
        match self {
            NumericStats::Signed { samples, .. } => {
                if samples.is_empty() {
                    return Vec::new();
                }
                let mut v = samples.clone();
                v.sort();
                v.dedup();
                v.into_iter().map(NumericScalar::Signed).collect()
            }
            NumericStats::Unsigned { samples, .. } => {
                if samples.is_empty() {
                    return Vec::new();
                }
                let mut v = samples.clone();
                v.sort();
                v.dedup();
                v.into_iter().map(NumericScalar::Unsigned).collect()
            }
            NumericStats::Float { samples, .. } => {
                if samples.is_empty() {
                    return Vec::new();
                }
                let mut v = samples.clone();
                v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                v.dedup_by(|a, b| a.to_bits() == b.to_bits());
                v.into_iter().map(NumericScalar::Float).collect()
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
    ranges
        .iter()
        .map(|r| r.end_row_id.saturating_sub(r.start_row_id).saturating_add(1))
        .sum()
}

#[inline]
fn score_rule_ranges(ranges: &[RowRange], range_weight: u64, candidate_rows_weight: u64) -> u64 {
    let range_count = ranges.len() as u64;
    let candidate_rows = ranges_candidate_rows(ranges);
    range_count
    .saturating_mul(range_weight)
    .saturating_add(candidate_rows.saturating_mul(candidate_rows_weight))
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

fn coarsen_ranges_with_gap(mut ranges: Vec<RowRange>, max_gap: u64) -> Vec<RowRange> {
    if ranges.len() <= 1 {
        return ranges;
    }
    ranges.sort_by_key(|r| r.start_row_id);
    let mut out: Vec<RowRange> = Vec::with_capacity(ranges.len());
    let mut cur = ranges[0];
    for r in ranges.into_iter().skip(1) {
        // Allow merging ranges that are close, to reduce fragmentation.
        let can_merge = r.start_row_id <= cur.end_row_id.saturating_add(max_gap).saturating_add(1);
        if can_merge {
            cur.end_row_id = cur.end_row_id.max(r.end_row_id);
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
    if best_ranges.len() > 1 && best_ranges.len() <= FLOAT_COARSEN_TARGET_MAX_RANGES {
         let mut clustered = best_ranges.clone();
         // Attempt multiple passes of "merge smallest gap"
         let mut improved = true;
         // Safety: prevent infinite loops or excessively long optimization
         let mut iterations = 0;
         while improved && iterations < 10000 {
             iterations += 1;
             improved = false;
             if clustered.len() <= 1 { break; }
             
             // Find smallest gap
             let mut min_gap = u64::MAX;
             let mut min_gap_idx = 0;
             for i in 0..clustered.len()-1 {
                 let g = clustered[i+1].start_row_id.saturating_sub(clustered[i].end_row_id).saturating_sub(1);
                 if g < min_gap {
                     min_gap = g;
                     min_gap_idx = i;
                 }
             }
             
             let new_end = clustered[min_gap_idx+1].end_row_id;
             let added_rows = min_gap; // roughly
             
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
                 clustered[min_gap_idx].end_row_id = new_end;
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

    // Identify numeric columns.
    let mut numeric_fields: Vec<(usize, u64, DbType, u32)> = Vec::new();
    let schema_calc = SchemaCalculator::default();
    for (idx, f) in schema.iter().enumerate() {
        if NumericStats::new(&f.db_type).is_none() {
            continue;
        }
        let (offset, schema_id) = schema_calc.calculate_schema_offset(&f.name, schema);
        numeric_fields.push((idx, schema_id, f.db_type.clone(), offset));
    }

    if numeric_fields.is_empty() {
        log::info!("scan_whole_table_build_rules: no numeric columns found, writing empty rules");
        let rules_io = CorrelationRuleStore::open_rules_io(base_io, table_name).await;
        return CorrelationRuleStore::save_numeric_rules_atomic::<S>(rules_io, &[]).await;
    }

    log::trace!("scan_whole_table_build_rules: found {} numeric columns. Using concurrency: {}", numeric_fields.len(), concurrency);

    // Build a RowFetch for just numeric columns.
    let mut columns_fetching_data = SmallVec::new();
    for (idx, schema_id, db_type, offset) in numeric_fields.iter() {
        let size = schema[*idx].size as u32;
        columns_fetching_data.push(crate::core::row::row::ColumnFetchingData {
            column_offset: *offset,
            column_type: db_type.clone(),
            size,
            schema_id: *schema_id,
        });
    }
    let row_fetch = RowFetch { columns_fetching_data };
    let row_fetch = Arc::new(row_fetch);
    let numeric_fields = Arc::new(numeric_fields);

    // PASS 1: collect stats.
    let pass1_start = std::time::Instant::now();
    
    // Producer: Read RowPointers
    let (tx, rx) = tokio::sync::mpsc::channel::<Vec<RowPointer>>(concurrency * 4);
    let rx = Arc::new(tokio::sync::Mutex::new(rx));
    
    let pointers_io_clone = pointers_io.clone();
    let producer_handle = tokio::spawn(async move {
        let mut iter = match RowPointerIterator::new(pointers_io_clone).await {
            Ok(i) => i,
            Err(e) => return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("RowPointerIterator: {}", e))),
        };
        loop {
            let mut chunk = Vec::new(); // RowPointerIterator will fill this based on its internal batch size (usually 64KB)
            if let Err(e) = iter.next_row_pointers_into(&mut chunk).await {
                return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("RowPointerIterator: {}", e)));
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
        let row_fetch = row_fetch.clone();
        let numeric_fields = numeric_fields.clone();
        
        handles.push(tokio::spawn(async move {
            let mut stats: Vec<NumericStats> = numeric_fields
                .iter()
                .map(|(_, _, t, _)| NumericStats::new(t).unwrap())
                .collect();
                
            let mut row = crate::core::row::row::Row::default();
            
            loop {
                let chunk_opt = {
                    let mut lock = rx.lock().await;
                    lock.recv().await
                };
                
                match chunk_opt {
                    Some(chunk) => {
                        for p in chunk {
                            if p.deleted { continue; }
                            if let Err(_) = p.fetch_row_reuse_async(rows_io.clone(), &row_fetch, &mut row).await {
                                // Ignore read errors in background scan
                                continue;
                            }
                            for (i, (_, _, db_type, _)) in numeric_fields.iter().enumerate() {
                                let col = &row.columns[i];
                                if let Some(v) = decode_numeric_value(db_type, col.data.into_slice()) {
                                    stats[i].update(v);
                                }
                            }
                        }
                    }
                    None => break,
                }
            }
            stats
        }));
    }

    // Wait for producer and consumers
    if let Err(e) = producer_handle.await.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))? {
        return Err(e);
    }
    
    // Aggregate stats
    let mut aggregated_stats: Vec<Option<NumericStats>> = (0..numeric_fields.len()).map(|_| None).collect();
    
    for h in handles {
        let thread_stats = h.await.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
        for (i, s) in thread_stats.into_iter().enumerate() {
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
    }
    
    // Unwrap aggregated stats
    let stats: Vec<NumericStats> = aggregated_stats.into_iter().map(|s| s.unwrap_or_else(|| {
        // Should not happen, but safe fallback
        NumericStats::new(&DbType::I64).unwrap()
    })).collect();

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

    for (i, (_, schema_id, db_type, _)) in numeric_fields.iter().enumerate() {
        let sorted_samples = stats[i].sorted_unique_samples();
        if sorted_samples.is_empty() {
            log::trace!("scan_whole_table_build_rules: col {} has no samples, skipping", schema_id);
            continue;
        }

        let n = sorted_samples.len();
        let is_float = matches!(db_type, DbType::F32 | DbType::F64);
        let step = if is_float {
            std::cmp::max(1usize, n / 256)
        } else {
            std::cmp::max(1usize, n / 32)
        };

        let (range_weight, candidate_rows_weight) = if is_float {
            (SCORE_RANGE_COUNT_WEIGHT_FLOAT, SCORE_CANDIDATE_ROWS_WEIGHT_FLOAT)
        } else {
            (SCORE_RANGE_COUNT_WEIGHT_INT, SCORE_CANDIDATE_ROWS_WEIGHT_INT)
        };

        let mut slot_keys: Vec<Vec<[u8; 16]>> = Vec::with_capacity(THRESHOLD_COUNT);
        let mut all_keys: BTreeMap<[u8; 16], NumericScalar> = BTreeMap::new();

        for k in 1..=THRESHOLD_COUNT {
            let p = (k as f64) / ((THRESHOLD_COUNT + 1) as f64);
            let base_idx = (p * ((n - 1) as f64)).round() as isize;

            let mut keys_for_slot: Vec<[u8; 16]> = Vec::new();
            for mult in CANDIDATE_STEPS {
                let idx = base_idx.saturating_add(mult.saturating_mul(step as isize));
                let idx = idx.clamp(0, (n as isize) - 1) as usize;
                let scalar = sorted_samples[idx];
                let key = scalar_key(scalar);
                all_keys.entry(key).or_insert(scalar);
                keys_for_slot.push(key);
            }
            keys_for_slot.sort();
            keys_for_slot.dedup();
            slot_keys.push(keys_for_slot);
        }

        let mut rules: Vec<NumericCorrelationRule> = Vec::with_capacity(all_keys.len() * 2);
        for (_key, scalar) in all_keys.iter() {
            rules.push(NumericCorrelationRule {
                column_schema_id: *schema_id,
                column_type: db_type.clone(),
                op: NumericRuleOp::LessThan,
                value: *scalar,
                ranges: Vec::new(),
            });
            rules.push(NumericCorrelationRule {
                column_schema_id: *schema_id,
                column_type: db_type.clone(),
                op: NumericRuleOp::GreaterThan,
                value: *scalar,
                ranges: Vec::new(),
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

    log::info!("scan_whole_table_build_rules: Generated {} candidate plans", plans.len());
    let rules_by_col = Arc::new(rules_by_col);

    // PASS 2: build row-id ranges.
    let pass2_start = std::time::Instant::now();
    
    // Producer for Pass 2
    let (tx2, rx2) = tokio::sync::mpsc::channel::<Vec<RowPointer>>(concurrency * 4);
    let rx2 = Arc::new(tokio::sync::Mutex::new(rx2));
    
    let pointers_io_clone_2 = pointers_io.clone();
    let producer_handle_2 = tokio::spawn(async move {
        let mut iter = match RowPointerIterator::new(pointers_io_clone_2).await {
            Ok(i) => i,
            Err(e) => return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("RowPointerIterator 2: {}", e))),
        };
        loop {
            let mut chunk = Vec::new();
            if let Err(e) = iter.next_row_pointers_into(&mut chunk).await {
                return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("RowPointerIterator 2: {}", e)));
            }
            if chunk.is_empty() { break; }
            if tx2.send(chunk).await.is_err() { break; }
        }
        Ok(())
    });

    let mut handles_2 = Vec::with_capacity(concurrency);
    
    // Map schema_id -> index into rules_by_col and into row columns.
    // Need this map inside threads.
    let mut schema_id_to_col_idx: BTreeMap<u64, usize> = BTreeMap::new();
    for (col_idx, (_, schema_id, _, _)) in numeric_fields.iter().enumerate() {
        schema_id_to_col_idx.insert(*schema_id, col_idx);
    }
    let schema_id_to_col_idx = Arc::new(schema_id_to_col_idx);

    for _ in 0..concurrency {
        let rx2 = rx2.clone();
        let rows_io = rows_io.clone();
        let row_fetch = row_fetch.clone();
        let rules_by_col = rules_by_col.clone();
        let numeric_fields = numeric_fields.clone();
        let schema_id_to_col_idx = schema_id_to_col_idx.clone();
        
        handles_2.push(tokio::spawn(async move {
            // Local storage for ranges: Vec of (column_idx_in_rules_by_col, rule_idx, Vec<RowRange>)
            // Or simpler: clone the structure of rules_by_col but only keep ranges.
            // But rules_by_col structure is complex.
            // Let's use a flat-ish accumulator: Vec<Vec<Vec<RowRange>>> matched by index to rules_by_col
            let mut local_ranges: Vec<Vec<Vec<RowRange>>> = rules_by_col.iter()
                .map(|(_, rules)| vec![Vec::with_capacity(rules.len()); rules.len()]) // Init with vectors
                .collect();
                
            // Temp buffering for checking "open" ranges within a chunk is useful for compression,
            // but since we receive arbitrary chunks, we can just produce ranges and let global merge handle it.
            // Optimization: Maintain "last open" state for the current chunk processing loop to avoid generating 1-length ranges for contiguous rows within a chunk.
            
            let mut row = crate::core::row::row::Row::default();
            
            loop {
                let chunk_opt = {
                    let mut lock = rx2.lock().await;
                    lock.recv().await
                };
                let Some(chunk) = chunk_opt else { break; };
                
                // Per-chunk open state to compress ranges locally
                let mut open: Vec<Vec<Option<RowRange>>> = rules_by_col
                    .iter()
                    .map(|(_, rules)| vec![None; rules.len()])
                    .collect();

                for p in chunk {
                    if p.deleted { continue; }
                    let row_id: u64 = row_id_u64(p.id);
                    if let Err(_) = p.fetch_row_reuse_async(rows_io.clone(), &row_fetch, &mut row).await { continue; }

                    for (col_rules_idx, (schema_id, rules)) in rules_by_col.iter().enumerate() {
                        let Some(col_idx) = schema_id_to_col_idx.get(schema_id).copied() else { continue; };
                        let db_type = &numeric_fields[col_idx].2;
                        let col = &row.columns[col_idx];
                        let Some(v) = decode_numeric_value(db_type, col.data.into_slice()) else { continue; };

                        for (rule_idx, rule) in rules.iter().enumerate() {
                            let is_match = value_satisfies_rule(v, rule.op, rule.value);
                            if is_match {
                                match &mut open[col_rules_idx][rule_idx] {
                                    None => {
                                        open[col_rules_idx][rule_idx] = Some(RowRange {
                                            start_row_id: row_id,
                                            end_row_id: row_id,
                                        });
                                    }
                                    Some(r) => {
                                        if row_id == r.end_row_id.saturating_add(1) {
                                            r.end_row_id = row_id;
                                        } else {
                                            local_ranges[col_rules_idx][rule_idx].push(*r);
                                            *r = RowRange {
                                                start_row_id: row_id,
                                                end_row_id: row_id,
                                            };
                                        }
                                    }
                                }
                            } else if let Some(r) = open[col_rules_idx][rule_idx].take() {
                                local_ranges[col_rules_idx][rule_idx].push(r);
                            }
                        }
                    }
                }
                
                // Flush open for this chunk
                for (col_rules_idx, rules) in rules_by_col.iter().enumerate() {
                    for (rule_idx, _) in rules.1.iter().enumerate() {
                        if let Some(r) = open[col_rules_idx][rule_idx].take() {
                            local_ranges[col_rules_idx][rule_idx].push(r);
                        }
                    }
                }
            }
            local_ranges
        }));
    }

    if let Err(e) = producer_handle_2.await.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))? {
        return Err(e);
    }
    
    // Collect and merge ranges
    let mut merged_ranges: Vec<Vec<Vec<RowRange>>> = rules_by_col.iter()
        .map(|(_, rules)| vec![Vec::new(); rules.len()])
        .collect();

    for h in handles_2 {
        let thread_ranges = h.await.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
        for (col_idx, rules_ranges) in thread_ranges.into_iter().enumerate() {
            for (rule_idx, ranges) in rules_ranges.into_iter().enumerate() {
                merged_ranges[col_idx][rule_idx].extend(ranges);
            }
        }
    }
    
    // Normalize global ranges
    // Unwrapping the Arc to modify the inner rules is hard. We have to reconstruct rules structure.
    let mut final_rules_by_col: Vec<(u64, Vec<NumericCorrelationRule>)> = Vec::new();

    for (col_idx, (schema_id, rules)) in rules_by_col.iter().enumerate() {
        let mut new_rules = Vec::with_capacity(rules.len());
        for (rule_idx, rule) in rules.iter().enumerate() {
            let raw_ranges = std::mem::take(&mut merged_ranges[col_idx][rule_idx]);
            // This normalize handles sorting and merging touching ranges
            let ranges = normalize_ranges(raw_ranges);
            
            new_rules.push(NumericCorrelationRule {
                column_schema_id: rule.column_schema_id,
                column_type: rule.column_type.clone(),
                op: rule.op,
                value: rule.value,
                ranges,
            });
        }
        rules_by_col_ref_mut_hack(&mut final_rules_by_col).push((*schema_id, new_rules));
    }
    
    // Reassign rules by col for usage in optimization below
    let rules_by_col = final_rules_by_col;

    log::info!("scan_whole_table_build_rules: Pass 2 (range building) done in {:.2?}", pass2_start.elapsed());

    // Choose the best thresholds per column from candidates.
    let mut final_chosen_rules_by_col: Vec<(u64, Vec<NumericCorrelationRule>)> = Vec::new();
    let opt_start = std::time::Instant::now();

    for plan in plans.iter() {
        let Some((_schema_id, candidate_rules)) = rules_by_col
            .iter()
            .find(|(sid, _)| *sid == plan.schema_id)
        else {
            continue;
        };

        #[derive(Clone)]
        struct CandidatePacked {
            scalar: NumericScalar,
            lt: Vec<RowRange>,
            gt: Vec<RowRange>,
            score: u64,
        }

        let mut candidates: BTreeMap<[u8; 16], CandidatePacked> = BTreeMap::new();
        for r in candidate_rules.iter() {
            let key = scalar_key(r.value);
            let entry = candidates.entry(key).or_insert_with(|| CandidatePacked {
                scalar: r.value,
                lt: Vec::new(),
                gt: Vec::new(),
                score: u64::MAX,
            });
            match r.op {
                NumericRuleOp::LessThan => entry.lt = r.ranges.clone(),
                NumericRuleOp::GreaterThan => entry.gt = r.ranges.clone(),
            }
        }
        for (_k, c) in candidates.iter_mut() {
            let s = score_rule_ranges(&c.lt, plan.range_weight, plan.candidate_rows_weight)
                .saturating_add(score_rule_ranges(
                    &c.gt,
                    plan.range_weight,
                    plan.candidate_rows_weight,
                ));
            c.score = s;
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
            let (mut lt, mut gt) = (c.lt, c.gt);
            lt = optimize_ranges_dynamic(lt, plan.range_weight, plan.candidate_rows_weight);
            gt = optimize_ranges_dynamic(gt, plan.range_weight, plan.candidate_rows_weight);

            out_rules.push(NumericCorrelationRule {
                column_schema_id: plan.schema_id,
                column_type: plan.db_type.clone(),
                op: NumericRuleOp::LessThan,
                value: c.scalar,
                ranges: lt,
            });
            out_rules.push(NumericCorrelationRule {
                column_schema_id: plan.schema_id,
                column_type: plan.db_type.clone(),
                op: NumericRuleOp::GreaterThan,
                value: c.scalar,
                ranges: gt,
            });
        }
        
        // Post-processing: Filter out rules that cover too much of the dataset.
        if !out_rules.is_empty() {
             let max_row_id = out_rules.iter()
                 .flat_map(|r| r.ranges.iter().map(|range| range.end_row_id))
                 .max()
                 .unwrap_or(0);
             
             if max_row_id > 0 {
                 let threshold = (max_row_id as u128 * MAX_RULE_COVERAGE_PERCENT as u128 / 100) as u64;
                 out_rules.retain(|r| {
                     let covered = ranges_candidate_rows(&r.ranges);
                     covered <= threshold
                 });
             }
             
             let mut seen_coverage = std::collections::HashSet::new();
             out_rules.retain(|r| {
                 let covered = ranges_candidate_rows(&r.ranges);
                 let op_byte = r.op as u8;
                 let key = (op_byte, covered);
                 seen_coverage.insert(key)
             });
        }

        final_chosen_rules_by_col.push((plan.schema_id, out_rules));
    }
    log::info!("scan_whole_table_build_rules: Plan optimization done in {:.2?}", opt_start.elapsed());
    log::info!("scan_whole_table_build_rules: completed total in {:.2?}", total_start.elapsed());

    let rules_io = CorrelationRuleStore::open_rules_io(base_io.clone(), table_name).await;
    CorrelationRuleStore::save_numeric_rules_atomic::<S>(rules_io, &final_chosen_rules_by_col).await?;

    #[cfg(debug_assertions)]
    {
        let json_io = base_io
            .create_new(format!("{}_rules_v2.json", table_name))
            .await;
        if let Err(e) = save_rules_json_atomic(json_io, table_name, &final_chosen_rules_by_col).await {
            warn!("Failed to write SME v2 rules JSON for table {}: {}", table_name, e);
        }
    }

    Ok(())
}

fn rules_by_col_ref_mut_hack<T>(v: &mut Vec<T>) -> &mut Vec<T> { v }

#[cfg(debug_assertions)]
#[derive(Debug, Serialize)]
struct RulesJsonFile<'a> {
    table: &'a str,
    columns: Vec<RulesJsonColumn>,
}

#[cfg(debug_assertions)]
#[derive(Debug, Serialize)]
struct RulesJsonColumn {
    column_schema_id: u64,
    column_type: String,
    rules: Vec<RulesJsonRule>,
}

#[cfg(debug_assertions)]
#[derive(Debug, Serialize)]
struct RulesJsonRule {
    op: String,
    value: RulesJsonScalar,
    ranges: Vec<(u64, u64)>,
}

#[cfg(debug_assertions)]
#[derive(Debug, Serialize)]
#[serde(tag = "kind", content = "value")]
enum RulesJsonScalar {
    Signed(i128),
    Unsigned(u128),
    Float(f64),
}

#[cfg(debug_assertions)]
async fn save_rules_json_atomic<S: StorageIO>(
    json_io: S,
    table_name: &str,
    rules_by_col: &[(u64, Vec<NumericCorrelationRule>)],
) -> std::io::Result<()> {
    let mut columns: Vec<RulesJsonColumn> = Vec::with_capacity(rules_by_col.len());

    for (schema_id, rules) in rules_by_col.iter() {
        let mut col_type: Option<String> = None;
        let mut jr: Vec<RulesJsonRule> = Vec::with_capacity(rules.len());
        for r in rules.iter() {
            col_type.get_or_insert_with(|| r.column_type.to_string());

            let value = match r.value {
                NumericScalar::Signed(v) => RulesJsonScalar::Signed(v),
                NumericScalar::Unsigned(v) => RulesJsonScalar::Unsigned(v),
                NumericScalar::Float(v) => RulesJsonScalar::Float(v),
            };

            jr.push(RulesJsonRule {
                op: format!("{:?}", r.op),
                value,
                ranges: r
                    .ranges
                    .iter()
                    .map(|rr| (rr.start_row_id, rr.end_row_id))
                    .collect(),
            });
        }

        columns.push(RulesJsonColumn {
            column_schema_id: *schema_id,
            column_type: col_type.unwrap_or_else(|| "UNKNOWN".to_string()),
            rules: jr,
        });
    }

    let dump = RulesJsonFile {
        table: table_name,
        columns,
    };

    let bytes = serde_json::to_vec_pretty(&dump)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("serde_json: {}", e)))?;

    let mut temp = json_io.create_temp().await;
    temp.write_data(0, &bytes).await;
    json_io.swap_temp(&mut temp).await;
    Ok(())
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

#[cfg(feature = "enable_long_row")]
#[inline]
fn row_id_u64(id: u128) -> u64 {
    if id > (u64::MAX as u128) {
        u64::MAX
    } else {
        id as u64
    }
}

#[cfg(not(feature = "enable_long_row"))]
#[inline]
fn row_id_u64(id: u64) -> u64 {
    id
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
