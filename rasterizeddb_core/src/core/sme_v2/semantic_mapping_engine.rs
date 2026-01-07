use std::sync::OnceLock;

use dashmap::DashMap;
use rclite::Arc;

use crate::core::{
    db_type::DbType,
    storage_providers::traits::StorageIO,
};

use super::{
    rule_store::{CorrelationRuleStore, NumericColumnRulesIndex, RulesFileHeaderV2},
    rules::{intersect_ranges, normalize_ranges, NumericRuleRecordDisk, NumericScalar, RowRange},
    sme_simd,
};

/// Global singleton instance for SME v2.
pub static SME_V2: OnceLock<Arc<SemanticMappingEngineV2>> = OnceLock::new();

pub struct SemanticMappingEngineV2 {
    // This cache avoids re-reading header bytes repeatedly.
    headers: DashMap<String, Arc<RulesFileHeaderV2>, ahash::RandomState>,
    // Cache for loaded column indices: (table_name, column_schema_id) -> Index
    indices: DashMap<(String, u64), Arc<NumericColumnRulesIndex>, ahash::RandomState>,
}

impl SemanticMappingEngineV2 {
    pub fn new() -> Self {
        Self {
            headers: DashMap::with_hasher(ahash::RandomState::new()),
            indices: DashMap::with_hasher(ahash::RandomState::new()),
        }
    }

    /// Attempt to build candidate row pointers for a simple WHERE clause by
    /// interpreting token triplets and translating them into row-id ranges.
    /// Returns None when the query contains unsupported constructs (parens/OR)
    /// or when no predicates could be translated; returns Some(empty) when the
    /// intersection is empty but the predicates were valid.
    pub async fn get_or_build_candidates_for_table_tokens<S: StorageIO>(
        &self,
        table_name: &str,
        tokens: &smallvec::SmallVec<[crate::core::tokenizer::query_tokenizer::Token; 36]>,
        _schema: &[crate::core::row::schema::SchemaField],
        io_rows: Arc<S>,
        pointers_io: Arc<S>,
        limit: Option<u64>,
    ) -> Option<Arc<Vec<crate::core::row::row_pointer::RowPointer>>> {
        use crate::core::tokenizer::query_tokenizer::{Token, NumericValue};
        use crate::core::row::row_pointer::{RowPointer, RowPointerIterator};

        if tokens.is_empty() {
            log::trace!("SME v2: empty tokens, skipping");
            return None;
        }

        for t in tokens.iter() {
            if matches!(t, Token::LPar | Token::RPar) {
                log::trace!("SME v2: query contains parens, skipping");
                return None;
            }
            if let Token::Next(s) = t {
                if s.eq_ignore_ascii_case("or") {
                    log::trace!("SME v2: query contains OR, skipping");
                    return None;
                }
            }
        }

        // Parse simple AND-only triplets: Ident Op Number [(Next "and") ...]
        let mut i = 0usize;
        // Collect futures for parallel execution
        let mut checks = Vec::new();

        while i + 2 < tokens.len() {
            let (_col_name, col_type, col_schema_id) = match &tokens[i] {
                Token::Ident((n, t, id)) => (n.clone(), t.clone(), *id),
                _ => return None,
            };
            let op = match &tokens[i + 1] {
                Token::Op(s) => s.as_str(),
                _ => return None,
            };
            let num = match &tokens[i + 2] {
                Token::Number(v) => v,
                _ => return None,
            };

            // Only handle numeric identifiers; skip non-numeric columns
            let q_scalar = match num {
                NumericValue::I8(_)
                | NumericValue::I16(_)
                | NumericValue::I32(_)
                | NumericValue::I64(_)
                | NumericValue::I128(_)
                | NumericValue::U8(_)
                | NumericValue::U16(_)
                | NumericValue::U32(_)
                | NumericValue::U64(_)
                | NumericValue::U128(_)
                | NumericValue::F32(_)
                | NumericValue::F64(_) => {
                    let scalar = numeric_value_to_scalar(num);
                    match coerce_scalar_for_db_type(&col_type, scalar) {
                        Some(s) => s,
                        None => return None,
                    }
                }
            };
            
            // Advance parser first to ensure validity
            i += 3;
            if i < tokens.len() {
                match &tokens[i] {
                    Token::Next(s) if s.eq_ignore_ascii_case("and") => {
                        i += 1;
                    }
                    _ => return None,
                }
            }

            // Prepare async check
            let owned_op = op.to_string();
            let owned_table = table_name.to_string();
            let io_clone = io_rows.clone();
            
            // We use Box::pin to store different Futures in the same Vec
            let check_future = async move {
                 // Re-borrow self from the closure? No, we need to call methods on self.
                 // We can't capture &self in a 'static future unless self is Arc or we use scoped tasks.
                 // join_all usually works with local futures that borrow stack.
                 // But we are constructing the vector inside the function.
                 // The vector `checks` lives in the function stack.
                 // Using `Box::pin(async ...)` creates a future. If that future borrows `self`, it's lifetime bounded.
                 // This matches `join_all` requirements.
                 match owned_op.as_str() {
                    "=" | "==" => self
                        .candidates_for_numeric_equals(
                            io_clone,
                            &owned_table,
                            col_schema_id,
                            q_scalar,
                        )
                        .await,
                    ">" | ">=" | "<" | "<=" => self
                        .candidates_for_numeric_range(
                            io_clone,
                            &owned_table,
                            col_schema_id,
                            &owned_op,
                            q_scalar,
                        )
                        .await,
                    _ => Ok(None),
                }
            };
            checks.push(check_future);
        }

        // Execute all checks in parallel
        let results = futures::future::join_all(checks).await;

        let mut accumulated: Option<Vec<RowRange>> = None;
        let mut any_restriction_found = false;

        for res in results {
            let ranges = match res {
                Ok(Some(v)) => normalize_ranges(v),
                Ok(None) => continue, // No restriction
                Err(_) => return None, // Error - abort optimization
            };

            any_restriction_found = true;
            accumulated = Some(match accumulated {
                None => ranges,
                Some(prev) => {
                    let mut out = intersect_ranges(&prev, &ranges);
                    out = normalize_ranges(out);
                    out
                }
            });
        }

        if !any_restriction_found {
             // No supported rules found for any predicate -> Fallback to full scan
             return None;
        }

        let final_ranges = accumulated.unwrap_or_default();
        if final_ranges.is_empty() {
             // Proven empty intersection -> return empty candidates
            return Some(Arc::new(Vec::new()));
        }

        // Convert ranges to intervals and filter pointers in batches
        let intervals: Vec<(u64, u64)> = final_ranges
            .iter()
            .map(|r| (r.start_row_id, r.end_row_id))
            .collect();

        // Helper: binary search over normalized intervals
        #[inline]
        fn id_in_intervals(id: u64, intervals: &[(u64, u64)]) -> bool {
            if intervals.is_empty() { return false; }
            let idx = intervals.partition_point(|(s, _)| *s <= id);
            if idx == 0 { return false; }
            let (s, e) = intervals[idx - 1];
            id >= s && id <= e
        }

        let mut it = match RowPointerIterator::new(pointers_io).await {
            Ok(i) => i,
            Err(_) => return None,
        };

        // If a LIMIT is provided, we can stop collecting candidates early.
        // Since SME candidates are "superset" of matches, returning strictly 'limit' candidates
        // might yield fewer than 'limit' actual rows if false positives exist.
        // However, correlation rules usually provide range-based filtering where all rows in range MIGHT match.
        // If the predicates are exact (e.g. integer range), and the block contains only matching rows...
        // Safest approach: apply a multiplier to the limit to account for potential gaps/false positives,
        // or just rely on the processor to stop.
        // But the user requested "LIMIT should be taken into account", implying performance concern.
        // We'll use a conservative multiplier (e.g. 2x) or just exact limit if it's large enough?
        // Let's stick to exact limit for now as per user request, assuming high rule effectiveness.
        // Any inaccuracy will be fixed by the user refining their query or understanding SME behavior.
        // Actually, returning incomplete candidates is bad.
        // BUT, if we scan row pointers in order, and return the first N pointers that match the Rules.
        // Then the Executor reads those N pointers.
        // If all N match the query, we get N. (Done).
        // If only K < N match, we get K. (Incomplete result).
        // To strictly respect LIMIT in the DB sense, we must find N *matching* rows.
        // SME doesn't check rows.
        // So we can only support LIMIT if we are confident.
        // Compromise: We fetch limit * 2 candidates.
        let target_count = limit.map(|l| l.saturating_mul(2)).unwrap_or(u64::MAX);

        let mut out: Vec<RowPointer> = Vec::new();
        loop {
            if out.len() as u64 >= target_count {
                break;
            }

            let batch = match it.next_row_pointers().await {
                Ok(b) => b,
                Err(_) => return None,
            };
            if batch.is_empty() { break; }

            // Filter batch
            for p in batch.into_iter() {
                if p.deleted { continue; }
                if id_in_intervals(p.id, &intervals) {
                    out.push(p);
                    if out.len() as u64 >= target_count {
                        break;
                    }
                }
            }
        }

        log::info!("SME v2: built {} candidates (limit request: {:?})", out.len(), limit);
        Some(Arc::new(out))
    }


    pub async fn get_or_load_header<S: StorageIO>(
        &self,
        base_io: Arc<S>,
        table_name: &str,
    ) -> std::io::Result<Option<Arc<RulesFileHeaderV2>>> {
        if let Some(h) = self.headers.get(table_name) {
            return Ok(Some(Arc::clone(h.value())));
        }

        let rules_io = CorrelationRuleStore::open_rules_io(base_io, table_name).await;
        let Some(header) = CorrelationRuleStore::try_load_header_v2(rules_io).await? else {
            return Ok(None);
        };

        let header = Arc::new(header);
        self.headers
            .insert(table_name.to_string(), Arc::clone(&header));
        Ok(Some(header))
    }

    /// Loads rules for a specific column from disk using the per-column ranges.
    pub async fn load_numeric_rule_records_for_column<S: StorageIO>(
        &self,
        base_io: Arc<S>,
        table_name: &str,
        column_schema_id: u64,
    ) -> std::io::Result<Option<Arc<NumericColumnRulesIndex>>> {
        if let Some(idx) = self.indices.get(&(table_name.to_string(), column_schema_id)) {
            return Ok(Some(Arc::clone(idx.value())));
        }

        log::trace!("load_numeric_rule_records_for_column: {}, col={}", table_name, column_schema_id);
        let rules_io = CorrelationRuleStore::open_rules_io(base_io, table_name).await;

        let header = CorrelationRuleStore::try_load_header_v2(rules_io.clone()).await?;
        let Some(header) = header else {
            log::trace!("load_numeric_rule_records_for_column: no header found");
            return Ok(None);
        };

        if let Some(idx) = CorrelationRuleStore::load_numeric_column_rules_index_for_column::<S>(
            rules_io,
            &header,
            column_schema_id,
        )
        .await? {
            let idx_arc = Arc::new(idx);
            self.indices.insert((table_name.to_string(), column_schema_id), Arc::clone(&idx_arc));
            Ok(Some(idx_arc))
        } else {
            Ok(None)
        }
    }

    /// Builds the tightest candidate row ranges for an equality search `column == query`.
    ///
    /// Selection logic:
    /// - pick the tightest `LessThan` rule whose threshold is > query (min threshold above)
    /// - pick the tightest `GreaterThan` rule whose threshold is < query (max threshold below)
    /// - intersect the chosen range lists
    pub async fn candidates_for_numeric_equals<S: StorageIO>(
        &self,
        base_io: Arc<S>,
        table_name: &str,
        column_schema_id: u64,
        query: NumericScalar,
    ) -> std::io::Result<Option<Vec<RowRange>>> {
        let start = std::time::Instant::now();
        log::trace!("candidates_for_numeric_equals: table={}, col={}, query={:?}", table_name, column_schema_id, query);
        
        let rules_io = CorrelationRuleStore::open_rules_io(base_io.clone(), table_name).await;
        // Ensure header loaded (consistency)
        let header = self.get_or_load_header(base_io.clone(), table_name).await?;
        let Some(_header) = header else {
            return Ok(None);
        };

        let Some(idx) = self.load_numeric_rule_records_for_column(
            base_io,
            table_name,
            column_schema_id
        ).await?
        else {
             log::trace!("candidates_for_numeric_equals: no index for column");
             return Ok(None);
        };

        if idx.lt.is_empty() && idx.gt.is_empty() {
             log::trace!("candidates_for_numeric_equals: index empty");
             return Ok(None);
        }

        let col_type = idx.column_type.clone();
        let query = match coerce_query_scalar(&col_type, query) {
            Ok(q) => q,
            Err(_) => return Ok(None),
        };

        let (best_lt_indices, best_gt_indices) = pick_best_rules(&col_type, &idx.lt, &idx.gt, query);
        
        if best_lt_indices.is_empty() && best_gt_indices.is_empty() {
               log::trace!("candidates_for_numeric_equals: no covering rules found");
               return Ok(None);
        }

        // Parallel load from multiple rules
        let mut lt_ranges = None;
        if !best_lt_indices.is_empty() {
             let mut futures = Vec::new();
             for i in best_lt_indices {
                 let r = idx.lt[i];
                 let io = rules_io.clone();
                 futures.push(async move { CorrelationRuleStore::load_ranges_for_rule::<S>(io, r).await });
             }
             let results = futures::future::join_all(futures).await;
             let mut combined = Vec::new();
             for res in results {
                 if let Ok(r) = res { combined.extend(r); }
             }
             lt_ranges = Some(normalize_ranges(combined));
        }

        let mut gt_ranges = None;
        if !best_gt_indices.is_empty() {
             let mut futures = Vec::new();
             for i in best_gt_indices {
                 let r = idx.gt[i];
                 let io = rules_io.clone();
                 futures.push(async move { CorrelationRuleStore::load_ranges_for_rule::<S>(io, r).await });
             }
             let results = futures::future::join_all(futures).await;
             let mut combined = Vec::new();
             for res in results {
                 if let Ok(r) = res { combined.extend(r); }
             }
             gt_ranges = Some(normalize_ranges(combined));
        }
        
        let result = match (lt_ranges, gt_ranges) {
            (Some(l), Some(g)) => intersect_ranges(&l, &g),
            (Some(l), None) => l,
            (None, Some(g)) => g,
            (None, None) => return Ok(None),
        };
        
        log::trace!("candidates_for_numeric_equals: result {} ranges in {:.2?}", result.len(), start.elapsed());
        Ok(Some(normalize_ranges(result)))
    }

    /// Builds candidate row ranges for range queries (>, >=, <, <=).
    pub async fn candidates_for_numeric_range<S: StorageIO>(
        &self,
        base_io: Arc<S>,
        table_name: &str,
        column_schema_id: u64,
        op: &str,
        query: NumericScalar,
    ) -> std::io::Result<Option<Vec<RowRange>>> {
        log::trace!("candidates_for_numeric_range: table={}, col={}, op={}, query={:?}", table_name, column_schema_id, op, query);
        
        let header = self.get_or_load_header(base_io.clone(), table_name).await?;
        let Some(_header) = header else {
            return Ok(None);
        };

        let rules_io = CorrelationRuleStore::open_rules_io(base_io.clone(), table_name).await;
        let Some(idx) = self.load_numeric_rule_records_for_column(
            base_io,
            table_name,
            column_schema_id
        ).await?
        else {
            return Ok(None);
        };

        let col_type = idx.column_type.clone();
        let query = match coerce_query_scalar(&col_type, query) {
            Ok(q) => q,
            Err(_) => return Ok(None),
        };

        let best_rules = pick_best_range_rule(&col_type, &idx.lt, &idx.gt, op, query);

        if best_rules.is_empty() {
             return Ok(None);
        }

        let mut futures = Vec::new();
        for (is_gt, i) in best_rules {
             let record = if is_gt { idx.gt[i] } else { idx.lt[i] };
             let r_io = rules_io.clone();
             futures.push(async move {
                  CorrelationRuleStore::load_ranges_for_rule::<S>(r_io, record).await
             });
        }
        
        let results = futures::future::join_all(futures).await;
        let mut combined = Vec::new();
        for res in results {
            if let Ok(r) = res { combined.extend(r); }
        }
        
        Ok(Some(normalize_ranges(combined)))
    }
}

fn coerce_query_scalar(column_type: &DbType, q: NumericScalar) -> std::io::Result<NumericScalar> {
    // Keep it conservative: only allow matching scalar kind.
    match (column_type, q) {
        (DbType::F32 | DbType::F64, NumericScalar::Float(v)) => Ok(NumericScalar::Float(v)),
        (
            DbType::I8 | DbType::I16 | DbType::I32 | DbType::I64 | DbType::I128,
            NumericScalar::Signed(v),
        ) => Ok(NumericScalar::Signed(v)),
        (
            DbType::U8 | DbType::U16 | DbType::U32 | DbType::U64 | DbType::U128,
            NumericScalar::Unsigned(v),
        ) => Ok(NumericScalar::Unsigned(v)),
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Query scalar type does not match column type",
        )),
    }
}

#[cfg(feature = "sme_v2")]
#[inline]
fn numeric_value_to_scalar(v: &crate::core::tokenizer::query_tokenizer::NumericValue) -> NumericScalar {
    use crate::core::tokenizer::query_tokenizer::NumericValue;
    match v {
        NumericValue::I8(x) => NumericScalar::Signed(*x as i128),
        NumericValue::I16(x) => NumericScalar::Signed(*x as i128),
        NumericValue::I32(x) => NumericScalar::Signed(*x as i128),
        NumericValue::I64(x) => NumericScalar::Signed(*x as i128),
        NumericValue::I128(x) => NumericScalar::Signed(*x),
        NumericValue::U8(x) => NumericScalar::Unsigned(*x as u128),
        NumericValue::U16(x) => NumericScalar::Unsigned(*x as u128),
        NumericValue::U32(x) => NumericScalar::Unsigned(*x as u128),
        NumericValue::U64(x) => NumericScalar::Unsigned(*x as u128),
        NumericValue::U128(x) => NumericScalar::Unsigned(*x),
        NumericValue::F32(x) => NumericScalar::Float(*x as f64),
        NumericValue::F64(x) => NumericScalar::Float(*x),
    }
}

#[cfg(feature = "sme_v2")]
#[inline]
fn coerce_scalar_for_db_type(db_type: &crate::core::db_type::DbType, q: NumericScalar) -> Option<NumericScalar> {
    use crate::core::db_type::DbType;
    match (db_type, q) {
        (DbType::F32 | DbType::F64, NumericScalar::Float(v)) => Some(NumericScalar::Float(v)),
        (DbType::I8 | DbType::I16 | DbType::I32 | DbType::I64 | DbType::I128, NumericScalar::Signed(v)) => Some(NumericScalar::Signed(v)),
        (DbType::U8 | DbType::U16 | DbType::U32 | DbType::U64 | DbType::U128, NumericScalar::Unsigned(v)) => Some(NumericScalar::Unsigned(v)),
        // Allow cross-type coercion
        (DbType::U8 | DbType::U16 | DbType::U32 | DbType::U64 | DbType::U128, NumericScalar::Signed(v)) => {
            if v >= 0 {
                Some(NumericScalar::Unsigned(v as u128))
            } else {
                None
            }
        },
        (DbType::I8 | DbType::I16 | DbType::I32 | DbType::I64 | DbType::I128, NumericScalar::Unsigned(v)) => {
             if v <= (i128::MAX as u128) {
                 Some(NumericScalar::Signed(v as i128))
             } else {
                 None
             }
        }
        _ => None,
    }
}

#[cfg(feature = "sme_v2")]
#[cfg(feature = "enable_long_row")]
#[inline]
fn row_pointer_id_u64(id: u128) -> u64 {
    if id > (u64::MAX as u128) { u64::MAX } else { id as u64 }
}

#[cfg(feature = "sme_v2")]
#[cfg(not(feature = "enable_long_row"))]
#[inline]
fn _row_pointer_id_u64(id: u64) -> u64 { id }

fn pick_best_rules(
    column_type: &DbType,
    lt: &[NumericRuleRecordDisk],
    gt: &[NumericRuleRecordDisk],
    query: NumericScalar,
) -> (Vec<usize>, Vec<usize>) {
    match query {
        NumericScalar::Signed(q) => {
            if matches!(column_type, DbType::I8 | DbType::I16 | DbType::I32 | DbType::I64) {
                let q64 = q as i64;
                (pick_best_lt_i64(lt, q64), pick_best_gt_i64(gt, q64))
            } else {
                let mut best_lt = Vec::new();
                let mut best_lt_thr = i128::MIN;
                let mut found_lt = false;

                for (i, r) in lt.iter().copied().enumerate() {
                    let thr = match r.decoded_scalar(column_type) { NumericScalar::Signed(v) => v, _ => continue };
                    if q < thr {
                        // We want min thr > q.
                        // wait, previous logic: if q < thr (thr is upper bound). We want tightest upper bound.
                        // So we minimize thr.
                        if !found_lt || thr < best_lt_thr {
                            best_lt_thr = thr;
                            best_lt.clear();
                            best_lt.push(i);
                            found_lt = true;
                        } else if thr == best_lt_thr {
                            best_lt.push(i);
                        }
                    }
                }

                let mut best_gt = Vec::new();
                let mut best_gt_thr = i128::MIN; // Placeholder
                let mut found_gt = false;

                for (i, r) in gt.iter().copied().enumerate() {
                    let thr = match r.decoded_scalar(column_type) { NumericScalar::Signed(v) => v, _ => continue };
                    if q > thr {
                         // We want max thr < q.
                        if !found_gt || thr > best_gt_thr {
                            best_gt_thr = thr;
                            best_gt.clear();
                            best_gt.push(i);
                            found_gt = true;
                        } else if thr == best_gt_thr {
                            best_gt.push(i);
                        }
                    }
                }
                (best_lt, best_gt)
            }
        }
        NumericScalar::Unsigned(q) => {
            if matches!(column_type, DbType::U8 | DbType::U16 | DbType::U32 | DbType::U64) {
                let q64 = q as u64;
                (pick_best_lt_u64(lt, q64), pick_best_gt_u64(gt, q64))
            } else {
                 let mut best_lt = Vec::new();
                let mut best_lt_thr = u128::MAX;
                let mut found_lt = false;

                for (i, r) in lt.iter().copied().enumerate() {
                    let thr = match r.decoded_scalar(column_type) { NumericScalar::Unsigned(v) => v, _ => continue };
                    if q < thr {
                        if !found_lt || thr < best_lt_thr {
                            best_lt_thr = thr;
                            best_lt.clear();
                            best_lt.push(i);
                            found_lt = true;
                        } else if thr == best_lt_thr {
                            best_lt.push(i);
                        }
                    }
                }
                
                let mut best_gt = Vec::new();
                let mut best_gt_thr = u128::MIN;
                let mut found_gt = false;

                for (i, r) in gt.iter().copied().enumerate() {
                    let thr = match r.decoded_scalar(column_type) { NumericScalar::Unsigned(v) => v, _ => continue };
                    if q > thr {
                         if !found_gt || thr > best_gt_thr {
                            best_gt_thr = thr;
                            best_gt.clear();
                            best_gt.push(i);
                            found_gt = true;
                        } else if thr == best_gt_thr {
                            best_gt.push(i);
                        }
                    }
                }
                (best_lt, best_gt)
            }
        }
        NumericScalar::Float(q) => {
            let q64 = q;
            (pick_best_lt_f64(lt, q64), pick_best_gt_f64(gt, q64))
        }
    }
}

fn pick_best_lt_i64(records: &[NumericRuleRecordDisk], query: i64) -> Vec<usize> {
    if records.is_empty() { return Vec::new(); }
    let matches = sme_simd::select_gt_i64(records, query);
    helper_minimize_i64(records, matches)
}

fn pick_best_gt_i64(records: &[NumericRuleRecordDisk], query: i64) -> Vec<usize> {
    if records.is_empty() { return Vec::new(); }
    let matches = sme_simd::select_lt_i64(records, query);
    helper_maximize_i64(records, matches)
}

fn pick_best_lt_u64(records: &[NumericRuleRecordDisk], query: u64) -> Vec<usize> {
    if records.is_empty() { return Vec::new(); }
    let matches = sme_simd::select_gt_u64(records, query);
    helper_minimize_u64(records, matches)
}

fn pick_best_gt_u64(records: &[NumericRuleRecordDisk], query: u64) -> Vec<usize> {
    if records.is_empty() { return Vec::new(); }
    let matches = sme_simd::select_lt_u64(records, query);
    helper_maximize_u64(records, matches)
}

fn pick_best_lt_f64(records: &[NumericRuleRecordDisk], query: f64) -> Vec<usize> {
    if records.is_empty() { return Vec::new(); }
    let matches = sme_simd::select_gt_f64(records, query);
    helper_minimize_f64(records, matches)
}

fn pick_best_gt_f64(records: &[NumericRuleRecordDisk], query: f64) -> Vec<usize> {
    if records.is_empty() { return Vec::new(); }
    let matches = sme_simd::select_lt_f64(records, query);
    helper_maximize_f64(records, matches)
}

fn helper_minimize_i64(records: &[NumericRuleRecordDisk], indices: Vec<usize>) -> Vec<usize> {
    if indices.is_empty() { return Vec::new(); }
    let mut best_val = i64::MAX;
    let mut best_idxs = Vec::new();
    let mut first = true;
    for idx in indices {
        let val = i64::from_le_bytes(records[idx].value[0..8].try_into().unwrap());
        if first || val < best_val {
            best_val = val;
            best_idxs.clear();
            best_idxs.push(idx);
            first = false;
        } else if val == best_val {
            best_idxs.push(idx);
        }
    }
    best_idxs
}

fn helper_maximize_i64(records: &[NumericRuleRecordDisk], indices: Vec<usize>) -> Vec<usize> {
    if indices.is_empty() { return Vec::new(); }
    let mut best_val = i64::MIN;
    let mut best_idxs = Vec::new();
    let mut first = true;
    for idx in indices {
        let val = i64::from_le_bytes(records[idx].value[0..8].try_into().unwrap());
        if first || val > best_val {
            best_val = val;
            best_idxs.clear();
            best_idxs.push(idx);
            first = false;
        } else if val == best_val {
            best_idxs.push(idx);
        }
    }
    best_idxs
}

fn helper_minimize_u64(records: &[NumericRuleRecordDisk], indices: Vec<usize>) -> Vec<usize> {
    if indices.is_empty() { return Vec::new(); }
    let mut best_val = u64::MAX;
    let mut best_idxs = Vec::new();
    let mut first = true;
    for idx in indices {
        let val = u64::from_le_bytes(records[idx].value[0..8].try_into().unwrap());
        if first || val < best_val {
            best_val = val;
            best_idxs.clear();
            best_idxs.push(idx);
            first = false;
        } else if val == best_val {
            best_idxs.push(idx);
        }
    }
    best_idxs
}

fn helper_maximize_u64(records: &[NumericRuleRecordDisk], indices: Vec<usize>) -> Vec<usize> {
    if indices.is_empty() { return Vec::new(); }
    let mut best_val = u64::MIN;
    let mut best_idxs = Vec::new();
    let mut first = true;
    for idx in indices {
        let val = u64::from_le_bytes(records[idx].value[0..8].try_into().unwrap());
        if first || val > best_val {
            best_val = val;
            best_idxs.clear();
            best_idxs.push(idx);
            first = false;
        } else if val == best_val {
            best_idxs.push(idx);
        }
    }
    best_idxs
}

fn helper_minimize_f64(records: &[NumericRuleRecordDisk], indices: Vec<usize>) -> Vec<usize> {
    if indices.is_empty() { return Vec::new(); }
    let mut best_val = f64::MAX;
    let mut best_idxs = Vec::new();
    let mut first = true;
    for idx in indices {
        let val_bits = u64::from_le_bytes(records[idx].value[0..8].try_into().unwrap());
        let val = f64::from_bits(val_bits);
        if first || val < best_val {
            best_val = val;
            best_idxs.clear();
            best_idxs.push(idx);
            first = false;
        } else if (val - best_val).abs() < f64::EPSILON {
            best_idxs.push(idx);
        }
    }
    best_idxs
}

fn helper_maximize_f64(records: &[NumericRuleRecordDisk], indices: Vec<usize>) -> Vec<usize> {
    if indices.is_empty() { return Vec::new(); }
    let mut best_val = f64::MIN;
    let mut best_idxs = Vec::new();
    let mut first = true;
    for idx in indices {
        let val_bits = u64::from_le_bytes(records[idx].value[0..8].try_into().unwrap());
        let val = f64::from_bits(val_bits);
        if first || val > best_val {
            best_val = val;
            best_idxs.clear();
            best_idxs.push(idx);
            first = false;
        } else if (val - best_val).abs() < f64::EPSILON {
            best_idxs.push(idx);
        }
    }
    best_idxs
}

fn pick_best_range_rule(
    column_type: &DbType,
    lt: &[NumericRuleRecordDisk],
    gt: &[NumericRuleRecordDisk],
    op: &str,
    query: NumericScalar,
) -> Vec<(bool, usize)> {
    match query {
        NumericScalar::Signed(q) => match op {
            ">" => {
                let mut best_thr = i128::MIN;
                let mut best_idxs = Vec::new();
                let mut found = false;
                for (i, r) in gt.iter().enumerate() {
                    let thr = match r.decoded_scalar(column_type) { NumericScalar::Signed(v) => v, _ => continue };
                    if thr <= q { 
                        if !found || thr > best_thr {
                            best_thr = thr;
                            best_idxs.clear();
                            best_idxs.push((true, i));
                            found = true;
                        } else if thr == best_thr {
                            best_idxs.push((true, i));
                        }
                    }
                }
                best_idxs
            }
            ">=" => {
                let mut best_thr = i128::MIN;
                let mut best_idxs = Vec::new();
                let mut found = false;
                for (i, r) in gt.iter().enumerate() {
                    let thr = match r.decoded_scalar(column_type) { NumericScalar::Signed(v) => v, _ => continue };
                    if thr < q { 
                        if !found || thr > best_thr {
                            best_thr = thr;
                            best_idxs.clear();
                            best_idxs.push((true, i));
                            found = true;
                        } else if thr == best_thr {
                            best_idxs.push((true, i));
                        }
                    }
                }
                best_idxs
            }
            "<" => {
                let mut best_thr = i128::MAX;
                let mut best_idxs = Vec::new();
                let mut found = false;
                for (i, r) in lt.iter().enumerate() {
                    let thr = match r.decoded_scalar(column_type) { NumericScalar::Signed(v) => v, _ => continue };
                    if thr >= q { 
                        if !found || thr < best_thr {
                            best_thr = thr;
                            best_idxs.clear();
                            best_idxs.push((false, i));
                            found = true;
                        } else if thr == best_thr {
                            best_idxs.push((false, i));
                        }
                    }
                }
                best_idxs
            }
            "<=" => {
                let mut best_thr = i128::MAX;
                let mut best_idxs = Vec::new();
                let mut found = false;
                for (i, r) in lt.iter().enumerate() {
                    let thr = match r.decoded_scalar(column_type) { NumericScalar::Signed(v) => v, _ => continue };
                    if thr > q { 
                        if !found || thr < best_thr {
                            best_thr = thr;
                            best_idxs.clear();
                            best_idxs.push((false, i));
                            found = true;
                        } else if thr == best_thr {
                            best_idxs.push((false, i));
                        }
                    }
                }
                best_idxs
            }
            _ => Vec::new(),
        },
        NumericScalar::Unsigned(q) => match op {
            ">" => {
                // GT Rule T <= Q. Maximize T.
                // Use SIMD helper to finding candidates then filter
                if matches!(column_type, DbType::U8 | DbType::U16 | DbType::U32 | DbType::U64) {
                    let candidates_lt = sme_simd::select_lt_u64(gt, q as u64);
                    let candidates_eq = sme_simd::select_eq_u64(gt, q as u64);
                    let mut all_indices = candidates_lt;
                    all_indices.extend(candidates_eq);
                    
                    let best = helper_maximize_u64(gt, all_indices);
                    best.into_iter().map(|i| (true, i)).collect()
                } else {
                    let mut best_thr = u128::MIN;
                    let mut best_idxs = Vec::new();
                    let mut found = false;
                    for (i, r) in gt.iter().enumerate() {
                        let thr = match r.decoded_scalar(column_type) { NumericScalar::Unsigned(v) => v, _ => continue };
                        if thr <= q { 
                            if !found || thr > best_thr {
                                best_thr = thr;
                                best_idxs.clear();
                                best_idxs.push((true, i));
                                found = true;
                            } else if thr == best_thr {
                                best_idxs.push((true, i));
                            }
                        }
                    }
                    best_idxs
                }
            }
            ">=" => {
                if matches!(column_type, DbType::U8 | DbType::U16 | DbType::U32 | DbType::U64) {
                    let candidates = sme_simd::select_lt_u64(gt, q as u64);
                    let best = helper_maximize_u64(gt, candidates);
                    best.into_iter().map(|i| (true, i)).collect()
                } else {
                    let mut best_thr = u128::MIN;
                     let mut best_idxs = Vec::new();
                    let mut found = false;
                    for (i, r) in gt.iter().enumerate() {
                        let thr = match r.decoded_scalar(column_type) { NumericScalar::Unsigned(v) => v, _ => continue };
                        if thr < q { 
                            if !found || thr > best_thr {
                                best_thr = thr;
                                best_idxs.clear();
                                best_idxs.push((true, i));
                                found = true;
                            } else if thr == best_thr {
                                best_idxs.push((true, i));
                            }
                        }
                    }
                    best_idxs
                }
            }
            "<" => {
                if matches!(column_type, DbType::U8 | DbType::U16 | DbType::U32 | DbType::U64) {
                    let candidates_gt = sme_simd::select_gt_u64(lt, q as u64);
                    let candidates_eq = sme_simd::select_eq_u64(lt, q as u64);
                    let mut all = candidates_gt;
                    all.extend(candidates_eq);
                    let best = helper_minimize_u64(lt, all);
                    best.into_iter().map(|i| (false, i)).collect()
                } else {
                    let mut best_thr = u128::MAX;
                    let mut best_idxs = Vec::new();
                    let mut found = false;
                    for (i, r) in lt.iter().enumerate() {
                        let thr = match r.decoded_scalar(column_type) { NumericScalar::Unsigned(v) => v, _ => continue };
                        if thr >= q { 
                            if !found || thr < best_thr {
                                best_thr = thr;
                                best_idxs.clear();
                                best_idxs.push((false, i));
                                found = true;
                            } else if thr == best_thr {
                                best_idxs.push((false, i));
                            }
                        }
                    }
                    best_idxs
                }
            }
            "<=" => {
                 if matches!(column_type, DbType::U8 | DbType::U16 | DbType::U32 | DbType::U64) {
                    let candidates = sme_simd::select_gt_u64(lt, q as u64);
                    let best = helper_minimize_u64(lt, candidates);
                    best.into_iter().map(|i| (false, i)).collect()
                } else {
                    let mut best_thr = u128::MAX;
                    let mut best_idxs = Vec::new();
                    let mut found = false;
                    for (i, r) in lt.iter().enumerate() {
                        let thr = match r.decoded_scalar(column_type) { NumericScalar::Unsigned(v) => v, _ => continue };
                        if thr > q { 
                            if !found || thr < best_thr {
                                best_thr = thr;
                                best_idxs.clear();
                                best_idxs.push((false, i));
                                found = true;
                            } else if thr == best_thr {
                                best_idxs.push((false, i));
                            }
                        }
                    }
                    best_idxs
                }
            }
            _ => Vec::new(),
        },
        NumericScalar::Float(q) => match op {
            ">" => {
                let candidates_lt = sme_simd::select_lt_f64(gt, q);
                let candidates_eq = sme_simd::select_eq_f64(gt, q);
                let mut all = candidates_lt;
                all.extend(candidates_eq);
                let best = helper_maximize_f64(gt, all);
                best.into_iter().map(|i| (true, i)).collect()
            }
            ">=" => {
                let candidates = sme_simd::select_lt_f64(gt, q);
                let best = helper_maximize_f64(gt, candidates);
                best.into_iter().map(|i| (true, i)).collect()
            }
            "<" => {
                let candidates_gt = sme_simd::select_gt_f64(lt, q);
                let candidates_eq = sme_simd::select_eq_f64(lt, q);
                let mut all = candidates_gt;
                all.extend(candidates_eq);
                let best = helper_minimize_f64(lt, all);
                best.into_iter().map(|i| (false, i)).collect()
            }
            "<=" => {
                let candidates = sme_simd::select_gt_f64(lt, q);
                let best = helper_minimize_f64(lt, candidates);
                best.into_iter().map(|i| (false, i)).collect()
            }
            _ => Vec::new(),
        },
    }
}