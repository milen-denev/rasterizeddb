use std::sync::OnceLock;

use dashmap::DashMap;
use rclite::Arc;

use crate::core::{db_type::DbType, sme_v2::sme_range_processor_comp, storage_providers::traits::StorageIO};

use super::{
    rule_store::{CorrelationRuleStore, RulesFileHeaderV2},
    rules::{
        intersect_ranges, normalize_ranges, NumericCorrelationRule, NumericScalar,
        RowRange,
    },
    sme_range_processor,
};

/// Global singleton instance for SME v2.
pub static SME_V2: OnceLock<Arc<SemanticMappingEngineV2>> = OnceLock::new();

pub struct SemanticMappingEngineV2 {
    // This cache avoids re-reading header bytes repeatedly.
    headers: DashMap<String, Arc<RulesFileHeaderV2>, ahash::RandomState>,
    // Cache for fully loaded per-column rules: (table_name, column_schema_id) -> Rules
    // This is the format expected by `sme_range_processor`.
    rules: DashMap<(String, u64), Arc<Vec<NumericCorrelationRule>>, ahash::RandomState>,
}

impl SemanticMappingEngineV2 {
    pub fn new() -> Self {
        Self {
            headers: DashMap::with_hasher(ahash::RandomState::new()),
            rules: DashMap::with_hasher(ahash::RandomState::new()),
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

        let mut out: Vec<RowPointer> = Vec::new();
        loop {
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

    /// Loads and caches fully materialized numeric rules for one column.
    ///
    /// This matches the data model expected by `sme_range_processor`.
    pub async fn load_numeric_rules_for_column<S: StorageIO>(
        &self,
        base_io: Arc<S>,
        table_name: &str,
        column_schema_id: u64,
    ) -> std::io::Result<Option<Arc<Vec<NumericCorrelationRule>>>> {
        if let Some(rules) = self.rules.get(&(table_name.to_string(), column_schema_id)) {
            return Ok(Some(Arc::clone(rules.value())));
        }

        let header = self.get_or_load_header(base_io.clone(), table_name).await?;
        let Some(header) = header else {
            return Ok(None);
        };

        let rules_io = CorrelationRuleStore::open_rules_io(base_io, table_name).await;
        let rules = CorrelationRuleStore::load_numeric_rules_for_column::<S>(
            rules_io,
            &header,
            column_schema_id,
        )
        .await?;

        if rules.is_empty() {
            return Ok(None);
        }

        let rules = Arc::new(rules);
        self.rules
            .insert((table_name.to_string(), column_schema_id), Arc::clone(&rules));
        Ok(Some(rules))
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
        log::trace!(
            "candidates_for_numeric_equals: table={}, col={}, query={:?}",
            table_name,
            column_schema_id,
            query
        );

        let Some(rules) = self
            .load_numeric_rules_for_column(base_io, table_name, column_schema_id)
            .await?
        else {
            return Ok(None);
        };

        let Some(column_type) = rules.first().map(|r| r.column_type.clone()) else {
            return Ok(None);
        };

        let query = match coerce_query_scalar(&column_type, query) {
            Ok(q) => q,
            Err(_) => return Ok(None),
        };

        let ranges = sme_range_processor::candidate_row_ranges_for_query(
            query,
            rules.as_slice(),
        );

        if ranges.is_empty() {
            return Ok(None);
        };

        if ranges.is_empty() {
            return Ok(None);
        }

        Ok(Some(ranges.to_vec()))
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
        log::trace!(
            "candidates_for_numeric_range: table={}, col={}, op={}, query={:?}",
            table_name,
            column_schema_id,
            op,
            query
        );

        let Some(rules) = self
            .load_numeric_rules_for_column(base_io, table_name, column_schema_id)
            .await?
        else {
            return Ok(None);
        };

        let Some(column_type) = rules.first().map(|r| r.column_type.clone()) else {
            return Ok(None);
        };

        let query = match coerce_query_scalar(&column_type, query) {
            Ok(q) => q,
            Err(_) => return Ok(None),
        };

        let comparison = match op {
            "<" => sme_range_processor_comp::ComparisonType::LessThan,
            "<=" => sme_range_processor_comp::ComparisonType::LessThanOrEqual,
            ">" => sme_range_processor_comp::ComparisonType::GreaterThan,
            ">=" => sme_range_processor_comp::ComparisonType::GreaterThanOrEqual,
            _ => return Ok(None),
        };

        let ranges = 
            sme_range_processor_comp::candidate_row_ranges_for_comparison_query(
            query,
            comparison,
            rules.as_slice());

        if ranges.is_empty() {
            return Ok(None);
        }

        Ok(Some(ranges.to_vec()))
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
