use std::sync::{OnceLock, atomic::{AtomicU64, Ordering}};

use dashmap::DashMap;
use opool::{Pool, PoolAllocator};
use rclite::Arc;
use smallvec::SmallVec;

use crate::core::{db_type::DbType, sme_v2::sme_range_processor_comp, storage_providers::traits::StorageIO};
use cacheguard::CacheGuard;

use super::{
    in_memory_rules,
    rule_store::{CorrelationRuleStore, RulesFileHeaderV2},
    rules::{
        intersect_ranges, normalize_ranges_smallvec, NumericCorrelationRule, NumericScalar,
        RowRange, StringCorrelationRule, StringRuleOp,
    },
    sme_range_processor_common::merge_row_ranges,
    sme_range_processor_str,
};

struct SmeByteVecAllocator;

impl PoolAllocator<Vec<u8>> for SmeByteVecAllocator {
    #[inline]
    fn allocate(&self) -> Vec<u8> {
        // Keep this modest: SME reads in bounded chunks (READ_CHUNK_POINTERS).
        vec![0; 1024 * 1024]
    }

    #[inline]
    fn reset(&self, obj: &mut Vec<u8>) {
        obj.clear();
    }
}

static SME_BYTE_VEC_POOL: OnceLock<Pool<SmeByteVecAllocator, Vec<u8>>> = OnceLock::new();

#[inline]
fn sme_byte_vec_pool() -> &'static Pool<SmeByteVecAllocator, Vec<u8>> {
    SME_BYTE_VEC_POOL.get_or_init(|| Pool::new(64, SmeByteVecAllocator))
}

/// Global singleton instance for SME v2.
pub static SME_V2: OnceLock<Arc<SemanticMappingEngineV2>> = OnceLock::new();

pub struct SemanticMappingEngineV2 {
    // This cache avoids re-reading header bytes repeatedly.
    headers: DashMap<String, Arc<RulesFileHeaderV2>, ahash::RandomState>,
    // Cache for fully loaded per-column rules: (table_name, column_schema_id) -> Rules
    // This is the format expected by `sme_range_processor`.
    rules: DashMap<(String, u64), Arc<Vec<NumericCorrelationRule>>, ahash::RandomState>,
    // Cache for fully loaded per-column string rules.
    string_rules: DashMap<(String, u64), Arc<Vec<StringCorrelationRule>>, ahash::RandomState>,
    // Cached row counts per table (exact rows derived from pointer file length).
    row_counts: DashMap<String, CacheGuard<AtomicU64>, ahash::RandomState>,
}

impl SemanticMappingEngineV2 {
    pub fn new() -> Self {
        Self {
            headers: DashMap::with_hasher(ahash::RandomState::new()),
            rules: DashMap::with_hasher(ahash::RandomState::new()),
            string_rules: DashMap::with_hasher(ahash::RandomState::new()),
            row_counts: DashMap::with_hasher(ahash::RandomState::new()),
        }
    }

    async fn get_total_rows<S: StorageIO>(&self, pointers_io: Arc<S>, table_name: &str) -> std::io::Result<u64> {
        use crate::core::row::row_pointer::ROW_POINTER_RECORD_LEN;
        let rec_len = ROW_POINTER_RECORD_LEN as u64;
        let bytes = pointers_io.get_len().await;
        let rows = bytes / rec_len;

        // Update cached count if changed.
        if let Some(entry) = self.row_counts.get(table_name) {
            let cached = entry.load(Ordering::Acquire);
            if cached != rows {
                entry.store(rows, Ordering::Release);
            }
        } else {
            self.row_counts
                .insert(table_name.to_string(), AtomicU64::new(rows).into());
        }

        Ok(rows)
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
        use crate::core::tokenizer::query_tokenizer::Token;
        use crate::core::row::row_pointer::RowPointer;
        use std::time::Instant;

        #[inline]
        fn log_step(table: &str, step: &str, total_start: Instant, step_start: Instant) {
            log::info!(
                "SME v2: candidates: table={} step={} t_ms={} dt_ms={}",
                table,
                step,
                total_start.elapsed().as_millis(),
                step_start.elapsed().as_millis(),
            );
        }

        let total_start = Instant::now();
        let step_start = Instant::now();

        if tokens.is_empty() {
            log::trace!("SME v2: empty tokens, skipping");
            return None;
        }

        let total_rows = match self.get_total_rows(pointers_io.clone(), table_name).await {
            Ok(v) => v,
            Err(e) => {
                log::warn!("SME v2: failed to load row count: table={} err={:?}", table_name, e);
                return None;
            }
        };

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

        log_step(table_name, "validate_tokens", total_start, step_start);

        // Parse simple AND-only triplets: Ident Op (Number|StringLit) [(Next "and") ...]
        let step_start = Instant::now();
        let mut i = 0usize;
        // Collect futures for parallel execution
        let mut checks = Vec::new();

        while i + 2 < tokens.len() {
            let (_col_name, col_type, col_schema_id) = match &tokens[i] {
                Token::Ident((n, t, id)) => (n.clone(), t.clone(), *id),
                _ => return None,
            };

            // Middle token can be either:
            // - Token::Op("=", "<", ">", ...)
            // - Token::Ident(("CONTAINS"|"STARTSWITH"|"ENDSWITH", DbType::STRING, 0))
            //   (this is how the tokenizer currently emits string ops)
            let op_token = &tokens[i + 1];
            let op_str: &str = match op_token {
                Token::Op(s) => s.as_str(),
                Token::Ident((s, _, _)) => s.as_str(),
                _ => return None,
            };

            let string_query_op: Option<StringRuleOp> = match op_str {
                "CONTAINS" => Some(StringRuleOp::Contains),
                "STARTSWITH" => Some(StringRuleOp::StartsWith),
                "ENDSWITH" => Some(StringRuleOp::EndsWith),
                _ => None,
            };
            let rhs = &tokens[i + 2];

            // Only handle numeric and string columns.
            let q_numeric_scalar: Option<NumericScalar> = match rhs {
                Token::Number(num) => {
                    let scalar = numeric_value_to_scalar(num);
                    match coerce_scalar_for_db_type(&col_type, scalar) {
                        Some(s) => Some(s),
                        None => return None,
                    }
                }
                _ => None,
            };
            let q_string_lit: Option<String> = match rhs {
                Token::StringLit(s) => Some(s.clone()),
                _ => None,
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
            let owned_op = op_str.to_string();
            let owned_table = table_name.to_string();
            let io_clone = io_rows.clone();
            let owned_col_type = col_type.clone();
            let owned_string = q_string_lit.clone();
            let owned_numeric = q_numeric_scalar;
            let owned_string_op = string_query_op;
            let total_rows = total_rows;
            
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
                    "=" | "==" => {
                        if owned_col_type == DbType::STRING {
                            let Some(s) = owned_string else { return Ok(None); };
                            return self
                                .candidates_for_string_predicate(io_clone, &owned_table, col_schema_id, &s, None, total_rows)
                                .await;
                        }

                        let Some(q_scalar) = owned_numeric else { return Ok(None); };
                        self
                            .candidates_for_numeric_equals(io_clone, &owned_table, col_schema_id, q_scalar, total_rows)
                            .await
                    }
                    ">" | ">=" | "<" | "<=" => {
                        let Some(q_scalar) = owned_numeric else { return Ok(None); };
                        self
                            .candidates_for_numeric_range(io_clone, &owned_table, col_schema_id, &owned_op, q_scalar, total_rows)
                            .await
                    }
                    "CONTAINS" | "STARTSWITH" | "ENDSWITH" => {
                        if owned_col_type != DbType::STRING {
                            return Ok(None);
                        }
                        let Some(s) = owned_string else { return Ok(None); };
                        let Some(op) = owned_string_op else { return Ok(None); };
                        self
                            .candidates_for_string_predicate(io_clone, &owned_table, col_schema_id, &s, Some(op), total_rows)
                            .await
                    }
                    _ => Ok(None),
                }
            };
            checks.push(check_future);
        }

        log_step(table_name, "parse_triplets", total_start, step_start);

        // Execute all checks in parallel
        let step_start = Instant::now();
        let results = futures::future::join_all(checks).await;
        log_step(table_name, "execute_predicates", total_start, step_start);

        // println!("Total predicates checked: {}", results.len());

        // for res in results.iter() {
        //     match res {
        //         Ok(Some(ranges)) => {
        //             println!("  Ranges found: {}", ranges.len());
        //             // for rr in ranges.iter() {
        //             //     println!("    Range: start={}, count={}", rr.start_pointer_pos, rr.row_count);
        //             // }
        //         }
        //         Ok(None) => {
        //             println!("  No ranges found for this predicate");
        //         }
        //         Err(e) => {
        //             println!("  Error during predicate processing: {}", e);
        //         }
        //     }
        // }

        let mut accumulated: Option<SmallVec<[RowRange; 64]>> = None;
        let mut any_restriction_found = false;

        let step_start = Instant::now();
        for res in results {
            let mut ranges = match res {
                Ok(Some(v)) => normalize_ranges_smallvec(v),
                Ok(None) => continue, // No restriction
                Err(_) => return None, // Error - abort optimization
            };

            // Skip predicates that produced no candidate ranges to avoid collapsing to zero.
            if ranges.is_empty() {
                continue;
            }

            //println!("ranges after normalization: {}", ranges.len());

            any_restriction_found = true;

            accumulated = Some(match accumulated {
                None => ranges,
                Some(mut prev) => {
                    prev.append(&mut ranges);
                    merge_row_ranges(prev)
                }
            });

            //println!("accumulated ranges after intersection: {}", accumulated.as_ref().unwrap().len());
        }

        log_step(table_name, "intersect_ranges", total_start, step_start);

        if !any_restriction_found {
             // No supported rules found for any predicate -> Fallback to full scan
             return None;
        }

        let mut final_ranges: SmallVec<[RowRange; 64]> = accumulated.unwrap_or_default();

        // Ensure rows affected by recent INSERT/UPDATE/DELETE remain candidates until the next
        // successful rule rebuild, regardless of how multiple predicates intersect.
        {
            let dirty = crate::core::sme_v2::dirty_row_tracker::dirty_row_tracker()
                .dirty_ranges(table_name);
            if !dirty.is_empty() {
                final_ranges.extend(dirty);
                final_ranges = merge_row_ranges(final_ranges);
            }
        }

        if final_ranges.is_empty() {
            // Proven empty intersection -> return empty candidates
            return Some(Arc::new(Vec::new()));
        }

        // Directly read the candidate row pointers from the pointers file.
        // Each RowRange is (start_pointer_pos, row_count), so we can read and decode in-place.
        use crate::core::row::row_pointer::ROW_POINTER_RECORD_LEN;
        let rec_len = ROW_POINTER_RECORD_LEN as u64;

        log::info!(
            "SME v2: candidates: table={} step=read_pointer_ranges t_ms={} ranges={}",
            table_name,
            total_start.elapsed().as_millis(),
            final_ranges.len(),
        );

        let scan_step_start = Instant::now();
        let mut out: Vec<RowPointer> = Vec::new();

        // Chunk reads to avoid huge allocations for broad ranges.
        const READ_CHUNK_POINTERS: u64 = 8192;

        for rr in final_ranges.iter().copied() {
            if rr.row_count == 0 {
                continue;
            }
            if (rr.start_pointer_pos % rec_len) != 0 {
                // Corrupt/mismatched rules file; bail out to full-scan fallback.
                return None;
            }

            let mut remaining = rr.row_count;
            let mut pos = rr.start_pointer_pos;
            while remaining > 0 {
                let take = remaining.min(READ_CHUNK_POINTERS);
                let span_bytes = take.saturating_mul(rec_len);
                let mut buf = sme_byte_vec_pool().get();
                let want = span_bytes as usize;
                let cap = buf.capacity();
                if cap < want {
                    buf.reserve(want - cap);
                }
                buf.resize(want, 0u8);
                let mut read_pos = pos;
                if pointers_io
                    .read_data_into_buffer(&mut read_pos, &mut buf[..])
                    .await
                    .is_err()
                {
                    return None;
                }

                for chunk in buf[..].chunks_exact(ROW_POINTER_RECORD_LEN) {
                    let p = RowPointer::from_slice_unknown(chunk);
                    if p.deleted {
                        continue;
                    }
                    out.push(p);
                }

                pos = pos.saturating_add(span_bytes);
                remaining -= take;
            }
        }

        log_step(table_name, "read_row_pointers", total_start, scan_step_start);
        log::info!(
            "SME v2: candidates: table={} step=done t_ms={} built={} limit={:?}",
            table_name,
            total_start.elapsed().as_millis(),
            out.len(),
            limit,
        );
        Some(Arc::new(out))
    }


    pub async fn get_or_load_header<S: StorageIO>(
        &self,
        base_io: Arc<S>,
        table_name: &str,
    ) -> std::io::Result<Option<Arc<RulesFileHeaderV2>>> {
        use std::time::Instant;
        let start = Instant::now();

        if let Some(h) = self.headers.get(table_name) {
            log::info!(
                "SME v2: get_or_load_header: table={} cache=hit dt_ms={}",
                table_name,
                start.elapsed().as_millis()
            );
            return Ok(Some(Arc::clone(h.value())));
        }

        // RAM-only fast path.
        if let Some(header) = in_memory_rules::get_header(table_name) {
            self.headers
                .insert(table_name.to_string(), Arc::clone(&header));
            return Ok(Some(header));
        }

        let rules_io = CorrelationRuleStore::open_rules_io(base_io, table_name).await;
        let Some(header) = CorrelationRuleStore::try_load_header_v2(rules_io).await? else {
            log::info!(
                "SME v2: get_or_load_header: table={} cache=miss result=none dt_ms={}",
                table_name,
                start.elapsed().as_millis()
            );
            return Ok(None);
        };

        let header = Arc::new(header);
        self.headers
            .insert(table_name.to_string(), Arc::clone(&header));

        log::info!(
            "SME v2: get_or_load_header: table={} cache=miss result=some dt_ms={}",
            table_name,
            start.elapsed().as_millis()
        );
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
        use std::time::Instant;
        let start = Instant::now();

        if let Some(rules) = self.rules.get(&(table_name.to_string(), column_schema_id)) {
            log::info!(
                "SME v2: load_numeric_rules_for_column: table={} col={} cache=hit dt_ms={}",
                table_name,
                column_schema_id,
                start.elapsed().as_millis()
            );
            return Ok(Some(Arc::clone(rules.value())));
        }

        // RAM-only fast path.
        if let Some(rules) = in_memory_rules::get_numeric_rules(table_name, column_schema_id) {
            if rules.is_empty() {
                return Ok(None);
            }
            self.rules
                .insert((table_name.to_string(), column_schema_id), Arc::clone(&rules));
            // Ensure header cache exists too.
            if let Some(h) = in_memory_rules::get_header(table_name) {
                self.headers.insert(table_name.to_string(), h);
            }
            return Ok(Some(rules));
        }

        let header = self.get_or_load_header(base_io.clone(), table_name).await?;
        let Some(header) = header else {
            log::info!(
                "SME v2: load_numeric_rules_for_column: table={} col={} cache=miss header=none dt_ms={}",
                table_name,
                column_schema_id,
                start.elapsed().as_millis()
            );
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
            log::info!(
                "SME v2: load_numeric_rules_for_column: table={} col={} cache=miss rules=empty dt_ms={}",
                table_name,
                column_schema_id,
                start.elapsed().as_millis()
            );
            return Ok(None);
        }

        let rules = Arc::new(rules);
        self.rules
            .insert((table_name.to_string(), column_schema_id), Arc::clone(&rules));

        log::info!(
            "SME v2: load_numeric_rules_for_column: table={} col={} cache=miss rules=some dt_ms={}",
            table_name,
            column_schema_id,
            start.elapsed().as_millis()
        );
        Ok(Some(rules))
    }

    /// Loads and caches fully materialized string rules for one column.
    pub async fn load_string_rules_for_column<S: StorageIO>(
        &self,
        base_io: Arc<S>,
        table_name: &str,
        column_schema_id: u64,
    ) -> std::io::Result<Option<Arc<Vec<StringCorrelationRule>>>> {
        use std::time::Instant;
        let start = Instant::now();

        if let Some(rules) = self
            .string_rules
            .get(&(table_name.to_string(), column_schema_id))
        {
            log::info!(
                "SME v2: load_string_rules_for_column: table={} col={} cache=hit dt_ms={}",
                table_name,
                column_schema_id,
                start.elapsed().as_millis()
            );
            return Ok(Some(Arc::clone(rules.value())));
        }

        // RAM-only fast path.
        if let Some(rules) = in_memory_rules::get_string_rules(table_name, column_schema_id) {
            if rules.is_empty() {
                return Ok(None);
            }
            self.string_rules
                .insert((table_name.to_string(), column_schema_id), Arc::clone(&rules));
            if let Some(h) = in_memory_rules::get_header(table_name) {
                self.headers.insert(table_name.to_string(), h);
            }
            return Ok(Some(rules));
        }

        let header = self.get_or_load_header(base_io.clone(), table_name).await?;
        let Some(header) = header else {
            log::info!(
                "SME v2: load_string_rules_for_column: table={} col={} cache=miss header=none dt_ms={}",
                table_name,
                column_schema_id,
                start.elapsed().as_millis()
            );
            return Ok(None);
        };

        let rules_io = CorrelationRuleStore::open_rules_io(base_io, table_name).await;
        let rules = CorrelationRuleStore::load_string_rules_for_column::<S>(
            rules_io,
            &header,
            column_schema_id,
        )
        .await?;

        if rules.is_empty() {
            log::info!(
                "SME v2: load_string_rules_for_column: table={} col={} cache=miss rules=empty dt_ms={}",
                table_name,
                column_schema_id,
                start.elapsed().as_millis()
            );
            return Ok(None);
        }

        let rules = Arc::new(rules);
        self.string_rules
            .insert((table_name.to_string(), column_schema_id), Arc::clone(&rules));

        log::info!(
            "SME v2: load_string_rules_for_column: table={} col={} cache=miss rules=some dt_ms={}",
            table_name,
            column_schema_id,
            start.elapsed().as_millis()
        );
        Ok(Some(rules))
    }

    pub async fn candidates_for_string_predicate<S: StorageIO>(
        &self,
        base_io: Arc<S>,
        table_name: &str,
        column_schema_id: u64,
        query: &str,
        query_op: Option<StringRuleOp>,
        _total_rows: u64,
    ) -> std::io::Result<Option<Vec<RowRange>>> {
        use std::time::Instant;
        let total_start = Instant::now();
        let step_start = Instant::now();

        let Some(rules) = self
            .load_string_rules_for_column(base_io, table_name, column_schema_id)
            .await?
        else {
            log::info!(
                "SME v2: candidates_for_string_predicate: table={} col={} step=no_rules t_ms={} dt_ms={}",
                table_name,
                column_schema_id,
                total_start.elapsed().as_millis(),
                step_start.elapsed().as_millis()
            );
            return Ok(None);
        };

        let ranges = sme_range_processor_str::candidate_row_ranges_for_query_string(
            query,
            query_op,
            rules.as_slice(),
            None,
        );
        
        if ranges.is_empty() {
            return Ok(None);
        }

        // The range processor already returns merged ranges.
        let ranges = ranges.into_vec();
        if ranges.is_empty() {
            return Ok(None);
        }

        log::info!(
            "SME v2: candidates_for_string_predicate: table={} col={} step=done t_ms={} out_ranges={}",
            table_name,
            column_schema_id,
            total_start.elapsed().as_millis(),
            ranges.len(),
        );
        Ok(Some(ranges))
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
        _total_rows: u64,
    ) -> std::io::Result<Option<Vec<RowRange>>> {
        use std::time::Instant;
        let total_start = Instant::now();
        let mut step_start = Instant::now();

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
            log::info!(
                "SME v2: candidates_for_numeric_equals: table={} col={} step=no_rules t_ms={} dt_ms={}",
                table_name,
                column_schema_id,
                total_start.elapsed().as_millis(),
                step_start.elapsed().as_millis()
            );
            return Ok(None);
        };

        log::info!(
            "SME v2: candidates_for_numeric_equals: table={} col={} step=load_rules t_ms={} dt_ms={}",
            table_name,
            column_schema_id,
            total_start.elapsed().as_millis(),
            step_start.elapsed().as_millis()
        );

        let Some(column_type) = rules.first().map(|r| r.column_type.clone()) else {
            log::info!(
                "SME v2: candidates_for_numeric_equals: table={} col={} step=no_column_type t_ms={}",
                table_name,
                column_schema_id,
                total_start.elapsed().as_millis(),
            );
            return Ok(None);
        };

        step_start = Instant::now();
        let query = match coerce_query_scalar(&column_type, query) {
            Ok(q) => q,
            Err(_) => {
                log::info!(
                    "SME v2: candidates_for_numeric_equals: table={} col={} step=coerce_query failed t_ms={} dt_ms={}",
                    table_name,
                    column_schema_id,
                    total_start.elapsed().as_millis(),
                    step_start.elapsed().as_millis()
                );
                return Ok(None);
            }
        };

        log::info!(
            "SME v2: candidates_for_numeric_equals: table={} col={} step=coerce_query t_ms={} dt_ms={}",
            table_name,
            column_schema_id,
            total_start.elapsed().as_millis(),
            step_start.elapsed().as_millis()
        );

        step_start = Instant::now();

        // Build upper/lower envelopes separately, then intersect to approximate equality.
        let le_ranges = sme_range_processor_comp::candidate_row_ranges_for_comparison_query(
            query,
            sme_range_processor_comp::ComparisonType::LessThanOrEqual,
            rules.as_slice(),
            None,
        );
        let ge_ranges = sme_range_processor_comp::candidate_row_ranges_for_comparison_query(
            query,
            sme_range_processor_comp::ComparisonType::GreaterThanOrEqual,
            rules.as_slice(),
            None,
        );

        let le_len = le_ranges.len();
        let ge_len = ge_ranges.len();

        #[inline]
        fn total_rows_estimate(ranges: &[RowRange]) -> u64 {
            ranges.iter().map(|r| r.row_count).sum()
        }

        let chosen = if !le_ranges.is_empty() && !ge_ranges.is_empty() {
            let intersection = intersect_ranges(&le_ranges, &ge_ranges);
            if !intersection.is_empty() {
                merge_row_ranges(intersection)
            } else {
                // Rules disagree (no overlap); fall back to the tighter side.
                let le_rows = total_rows_estimate(&le_ranges);
                let ge_rows = total_rows_estimate(&ge_ranges);
                if le_rows <= ge_rows {
                    le_ranges
                } else {
                    ge_ranges
                }
            }
        } else if !le_ranges.is_empty() {
            le_ranges
        } else if !ge_ranges.is_empty() {
            ge_ranges
        } else {
            smallvec::SmallVec::new()
        };

        log::info!(
            "SME v2: candidates_for_numeric_equals: table={} col={} step=compute_ranges t_ms={} dt_ms={} le={} ge={} chosen={}",
            table_name,
            column_schema_id,
            total_start.elapsed().as_millis(),
            step_start.elapsed().as_millis(),
            le_len,
            ge_len,
            chosen.len(),
        );

        if chosen.is_empty() {
            return Ok(None);
        };

        log::info!(
            "SME v2: candidates_for_numeric_equals: table={} col={} step=done t_ms={} out_ranges={}",
            table_name,
            column_schema_id,
            total_start.elapsed().as_millis(),
            chosen.len(),
        );
        Ok(Some(chosen.to_vec()))
    }

    /// Builds candidate row ranges for range queries (>, >=, <, <=).
    pub async fn candidates_for_numeric_range<S: StorageIO>(
        &self,
        base_io: Arc<S>,
        table_name: &str,
        column_schema_id: u64,
        op: &str,
        query: NumericScalar,
        _total_rows: u64,
    ) -> std::io::Result<Option<Vec<RowRange>>> {
        use std::time::Instant;
        let total_start = Instant::now();
        let mut step_start = Instant::now();

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
            log::info!(
                "SME v2: candidates_for_numeric_range: table={} col={} op={} step=no_rules t_ms={} dt_ms={}",
                table_name,
                column_schema_id,
                op,
                total_start.elapsed().as_millis(),
                step_start.elapsed().as_millis()
            );
            return Ok(None);
        };

        log::info!(
            "SME v2: candidates_for_numeric_range: table={} col={} op={} step=load_rules t_ms={} dt_ms={}",
            table_name,
            column_schema_id,
            op,
            total_start.elapsed().as_millis(),
            step_start.elapsed().as_millis()
        );

        let Some(column_type) = rules.first().map(|r| r.column_type.clone()) else {
            log::info!(
                "SME v2: candidates_for_numeric_range: table={} col={} op={} step=no_column_type t_ms={}",
                table_name,
                column_schema_id,
                op,
                total_start.elapsed().as_millis(),
            );
            return Ok(None);
        };

        step_start = Instant::now();
        let query = match coerce_query_scalar(&column_type, query) {
            Ok(q) => q,
            Err(_) => {
                log::info!(
                    "SME v2: candidates_for_numeric_range: table={} col={} op={} step=coerce_query failed t_ms={} dt_ms={}",
                    table_name,
                    column_schema_id,
                    op,
                    total_start.elapsed().as_millis(),
                    step_start.elapsed().as_millis()
                );
                return Ok(None);
            }
        };

        log::info!(
            "SME v2: candidates_for_numeric_range: table={} col={} op={} step=coerce_query t_ms={} dt_ms={}",
            table_name,
            column_schema_id,
            op,
            total_start.elapsed().as_millis(),
            step_start.elapsed().as_millis()
        );

        let comparison = match op {
            "<" => sme_range_processor_comp::ComparisonType::LessThan,
            "<=" => sme_range_processor_comp::ComparisonType::LessThanOrEqual,
            ">" => sme_range_processor_comp::ComparisonType::GreaterThan,
            ">=" => sme_range_processor_comp::ComparisonType::GreaterThanOrEqual,
            _ => return Ok(None),
        };

        step_start = Instant::now();

        let ranges = 
            sme_range_processor_comp::candidate_row_ranges_for_comparison_query(
            query,
            comparison,
            rules.as_slice(),
            None);

        log::info!(
            "SME v2: candidates_for_numeric_range: table={} col={} op={} step=compute_ranges t_ms={} dt_ms={} ranges={}",
            table_name,
            column_schema_id,
            op,
            total_start.elapsed().as_millis(),
            step_start.elapsed().as_millis(),
            ranges.len(),
        );

        if ranges.is_empty() {
            return Ok(None);
        }

        log::info!(
            "SME v2: candidates_for_numeric_range: table={} col={} op={} step=done t_ms={} out_ranges={}",
            table_name,
            column_schema_id,
            op,
            total_start.elapsed().as_millis(),
            ranges.len(),
        );
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
