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
}

impl SemanticMappingEngineV2 {
    pub fn new() -> Self {
        Self {
            headers: DashMap::with_hasher(ahash::RandomState::new()),
        }
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
    ) -> std::io::Result<Option<NumericColumnRulesIndex>> {
        log::trace!("load_numeric_rule_records_for_column: {}, col={}", table_name, column_schema_id);
        let rules_io = CorrelationRuleStore::open_rules_io(base_io, table_name).await;

        let header = CorrelationRuleStore::try_load_header_v2(rules_io.clone()).await?;
        let Some(header) = header else {
            log::trace!("load_numeric_rule_records_for_column: no header found");
            return Ok(None);
        };

        CorrelationRuleStore::load_numeric_column_rules_index_for_column::<S>(
            rules_io,
            &header,
            column_schema_id,
        )
        .await
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
    ) -> std::io::Result<Vec<RowRange>> {
        let start = std::time::Instant::now();
        log::trace!("candidates_for_numeric_equals: table={}, col={}, query={:?}", table_name, column_schema_id, query);
        
        let rules_io = CorrelationRuleStore::open_rules_io(base_io, table_name).await;
        let header = CorrelationRuleStore::try_load_header_v2(rules_io.clone()).await?;
        let Some(header) = header else {
            return Ok(Vec::new());
        };

        let Some(idx) = CorrelationRuleStore::load_numeric_column_rules_index_for_column::<S>(
            rules_io.clone(),
            &header,
            column_schema_id,
        )
        .await?
        else {
             log::trace!("candidates_for_numeric_equals: no index for column");
            return Ok(Vec::new());
        };

        if idx.lt.is_empty() && idx.gt.is_empty() {
             log::trace!("candidates_for_numeric_equals: index empty");
            return Ok(Vec::new());
        }

        let col_type = idx.column_type;
        let query = coerce_query_scalar(&col_type, query)?;

        let (best_lt, best_gt) = pick_best_rules(&col_type, &idx.lt, &idx.gt, query);
        if best_lt.is_none() && best_gt.is_none() {
             log::trace!("candidates_for_numeric_equals: no covering rules found");
        }

        let mut current: Option<Vec<RowRange>> = None;
        if let Some(lt_i) = best_lt {
             log::trace!("candidates_for_numeric_equals: using LT rule idx {}", lt_i);
            let r = CorrelationRuleStore::load_ranges_for_rule::<S>(rules_io.clone(), idx.lt[lt_i]).await?;
            current = Some(normalize_ranges(r));
        }
        if let Some(gt_i) = best_gt {
             log::trace!("candidates_for_numeric_equals: using GT rule idx {}", gt_i);
            let r = CorrelationRuleStore::load_ranges_for_rule::<S>(rules_io.clone(), idx.gt[gt_i]).await?;
            let r = normalize_ranges(r);
            current = Some(match current {
                None => r,
                Some(a) => intersect_ranges(&a, &r),
            });
        }
        
        let result = current.unwrap_or_default();
        log::trace!("candidates_for_numeric_equals: result {} ranges in {:.2?}", result.len(), start.elapsed());

        Ok(result)
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

fn pick_best_rules(
    column_type: &DbType,
    lt: &[NumericRuleRecordDisk],
    gt: &[NumericRuleRecordDisk],
    query: NumericScalar,
) -> (Option<usize>, Option<usize>) {
    match query {
        NumericScalar::Signed(q) => {
            // SIMD path only for signed types representable as i64.
            if matches!(column_type, DbType::I8 | DbType::I16 | DbType::I32 | DbType::I64) {
                let q64 = q as i64;
                let lt_idx = pick_best_lt_i64(lt, q64);
                let gt_idx = pick_best_gt_i64(gt, q64);
                (lt_idx, gt_idx)
            } else {
                // Full-width i128 fallback.
                let mut best_lt: Option<(i128, usize)> = None;
                let mut best_gt: Option<(i128, usize)> = None;

                for (i, r) in lt.iter().copied().enumerate() {
                    let thr = match r.decoded_scalar(column_type) {
                        NumericScalar::Signed(v) => v,
                        _ => continue,
                    };
                    if q < thr {
                        best_lt = Some(match best_lt {
                            None => (thr, i),
                            Some((best_thr, best_i)) => {
                                if thr < best_thr {
                                    (thr, i)
                                } else {
                                    (best_thr, best_i)
                                }
                            }
                        });
                    }
                }

                for (i, r) in gt.iter().copied().enumerate() {
                    let thr = match r.decoded_scalar(column_type) {
                        NumericScalar::Signed(v) => v,
                        _ => continue,
                    };
                    if q > thr {
                        best_gt = Some(match best_gt {
                            None => (thr, i),
                            Some((best_thr, best_i)) => {
                                if thr > best_thr {
                                    (thr, i)
                                } else {
                                    (best_thr, best_i)
                                }
                            }
                        });
                    }
                }

                (best_lt.map(|(_, i)| i), best_gt.map(|(_, i)| i))
            }
        }
        // TODO: add SIMD paths for unsigned + float. For now, scalar selection is still correct.
        NumericScalar::Unsigned(q) => {
            let mut best_lt: Option<(u128, usize)> = None;
            let mut best_gt: Option<(u128, usize)> = None;

            for (i, r) in lt.iter().copied().enumerate() {
                let thr = match r.decoded_scalar(column_type) {
                    NumericScalar::Unsigned(v) => v,
                    _ => continue,
                };
                if q < thr {
                    best_lt = Some(match best_lt {
                        None => (thr, i),
                        Some((best_thr, best_i)) => {
                            if thr < best_thr {
                                (thr, i)
                            } else {
                                (best_thr, best_i)
                            }
                        }
                    });
                }
            }

            for (i, r) in gt.iter().copied().enumerate() {
                let thr = match r.decoded_scalar(column_type) {
                    NumericScalar::Unsigned(v) => v,
                    _ => continue,
                };
                if q > thr {
                    best_gt = Some(match best_gt {
                        None => (thr, i),
                        Some((best_thr, best_i)) => {
                            if thr > best_thr {
                                (thr, i)
                            } else {
                                (best_thr, best_i)
                            }
                        }
                    });
                }
            }

            (best_lt.map(|(_, i)| i), best_gt.map(|(_, i)| i))
        }
        NumericScalar::Float(q) => {
            let mut best_lt: Option<(f64, usize)> = None;
            let mut best_gt: Option<(f64, usize)> = None;

            for (i, r) in lt.iter().copied().enumerate() {
                let thr = match r.decoded_scalar(column_type) {
                    NumericScalar::Float(v) => v,
                    _ => continue,
                };
                if q < thr {
                    best_lt = Some(match best_lt {
                        None => (thr, i),
                        Some((best_thr, best_i)) => {
                            if thr < best_thr {
                                (thr, i)
                            } else {
                                (best_thr, best_i)
                            }
                        }
                    });
                }
            }

            for (i, r) in gt.iter().copied().enumerate() {
                let thr = match r.decoded_scalar(column_type) {
                    NumericScalar::Float(v) => v,
                    _ => continue,
                };
                if q > thr {
                    best_gt = Some(match best_gt {
                        None => (thr, i),
                        Some((best_thr, best_i)) => {
                            if thr > best_thr {
                                (thr, i)
                            } else {
                                (best_thr, best_i)
                            }
                        }
                    });
                }
            }

            (best_lt.map(|(_, i)| i), best_gt.map(|(_, i)| i))
        }
    }
}

fn pick_best_lt_i64(records: &[NumericRuleRecordDisk], query: i64) -> Option<usize> {
    if records.is_empty() {
        return None;
    }
    let matches = sme_simd::select_lt_i64(records, query);
    let mut best: Option<(i64, usize)> = None;
    for local_idx in matches {
        let thr = i64::from_le_bytes(records[local_idx].value[0..8].try_into().unwrap());
        best = Some(match best {
            None => (thr, local_idx),
            Some((best_thr, best_i)) => {
                if thr < best_thr { (thr, local_idx) } else { (best_thr, best_i) }
            }
        });
    }
    best.map(|(_, i)| i)
}

fn pick_best_gt_i64(records: &[NumericRuleRecordDisk], query: i64) -> Option<usize> {
    if records.is_empty() {
        return None;
    }
    let matches = sme_simd::select_gt_i64(records, query);
    let mut best: Option<(i64, usize)> = None;
    for local_idx in matches {
        let thr = i64::from_le_bytes(records[local_idx].value[0..8].try_into().unwrap());
        best = Some(match best {
            None => (thr, local_idx),
            Some((best_thr, best_i)) => {
                if thr > best_thr { (thr, local_idx) } else { (best_thr, best_i) }
            }
        });
    }
    best.map(|(_, i)| i)
}
