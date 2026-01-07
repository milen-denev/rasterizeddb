use std::{borrow::Cow, cell::UnsafeCell, sync::{OnceLock, atomic::{AtomicBool, AtomicU64}}};

use cacheguard::CacheGuard;
use log::error;
use rclite::Arc;

use opool::{Pool, PoolAllocator};

use crc::{CRC_64_ECMA_182, Crc};
use futures::future::join_all;
use smallvec::SmallVec;

use tokio::{
    sync::Semaphore,
    task,
};

use super::{
    transformer::{ColumnTransformer, Next, TransformerProcessor},
};

use crate::{
    MAX_PERMITS_THREADS, cache::atomic_cache::AtomicGenericCache, core::{
        processor::transformer::RowBlocks, row::{row::{Row, RowFetch}, row_pointer::{RowPointer, RowPointerIterator}, schema::SchemaField}, storage_providers::traits::StorageIO, tokenizer::{query_parser::QueryParser, query_tokenizer::tokenize}
    }, memory_pool::MemoryBlock
};

use crate::{BATCH_SIZE, semantics_enabled};

#[cfg(not(feature = "sme_v2"))]
use crate::core::sme::semantic_mapping_engine::{SME, SemanticMappingEngine};

#[cfg(feature = "sme_v2")]
use crate::core::sme_v2::{
    semantic_mapping_engine::{SemanticMappingEngineV2, SME_V2},
    rules::{intersect_ranges, normalize_ranges, NumericScalar, RowRange},
};

use crate::core::tokenizer::query_tokenizer::Token;

pub static EMPTY_STR: &str = "";
pub static ATOMIC_CACHE: OnceLock<std::sync::Arc<AtomicGenericCache<u64, Arc<Vec<RowPointer>>>>> = OnceLock::new();
pub static ENABLE_CACHE: OnceLock<bool> = OnceLock::new();
const CRC_64: Crc<u64> = Crc::<u64>::new(&CRC_64_ECMA_182);

#[derive(Debug, Clone)]
pub struct RowPointerWithOffset {
    pub pointer_record_pos: u64,
    pub pointer: RowPointer,
}

struct RowPointerVecAllocator;

impl PoolAllocator<Vec<RowPointer>> for RowPointerVecAllocator {
    #[inline]
    fn allocate(&self) -> Vec<RowPointer> {
        let cap = BATCH_SIZE.get().copied().unwrap_or(1024 * 64);
        Vec::with_capacity(cap)
    }

    #[inline]
    fn reset(&self, obj: &mut Vec<RowPointer>) {
        obj.clear();
    }
}

static ROW_POINTER_VEC_POOL: OnceLock<Pool<RowPointerVecAllocator, Vec<RowPointer>>> = OnceLock::new();

#[inline]
fn row_pointer_vec_pool() -> &'static Pool<RowPointerVecAllocator, Vec<RowPointer>> {
    ROW_POINTER_VEC_POOL.get_or_init(|| {
        let max_threads = MAX_PERMITS_THREADS.get().copied().unwrap_or(16);
        // Keep this modest; the pool can grow on demand.
        let slots = (max_threads * 2).clamp(32, 512);
        Pool::new(slots, RowPointerVecAllocator)
    })
}

pub struct ConcurrentProcessor;

impl ConcurrentProcessor {
    pub fn new() -> Self {
        Self {}
    }

    async fn try_collect_cached_rows<S: StorageIO>(
        table_name: &str,
        where_query: &str,
        requested_row_fetch: &RowFetch,
        io_rows: &Arc<S>,
    ) -> Option<(u64, Vec<Row>)> {
        let cache_enabled = *ENABLE_CACHE.get().unwrap();
        if !cache_enabled {
            return None;
        }

        let crc64_hash = CRC_64.checksum(table_name.as_bytes()) ^ CRC_64.checksum(where_query.as_bytes());
        let cache = ATOMIC_CACHE.get().unwrap();
        let entries = cache.get(&crc64_hash)?;

        let mut collected_rows = Vec::with_capacity(50);
        let mut row = Row::default();

        for pointer in entries.iter() {
            if pointer.deleted {
                continue;
            }

            let io_rows_clone = Arc::clone(io_rows);
            if pointer
                .fetch_row_reuse_async(io_rows_clone, requested_row_fetch, &mut row)
                .await
                .is_err()
            {
                continue;
            }

            collected_rows.push(Row::clone_row(&row));
        }

        Some((crc64_hash, collected_rows))
    }

    #[inline]
    fn prepare_query(where_query: &str, table_schema: &SmallVec<[SchemaField; 20]>) -> (bool, Arc<SmallVec<[SchemaField; 20]>>, Arc<SmallVec<[Token; 36]>>) {
        // Empty WHERE means "match all rows"; skip tokenization/parsing entirely.
        let no_filter = where_query.trim().is_empty();
        let tokens: SmallVec<[Token; 36]> = if no_filter {
            SmallVec::new()
        } else {
            tokenize(&where_query, table_schema)
        };

        let schema_arc = Arc::new(table_schema.clone());
        let tokens_arc = Arc::new(tokens);
        (no_filter, schema_arc, tokens_arc)
    }

    #[cfg(not(feature = "sme_v2"))]
    async fn maybe_build_sme_candidates<S: StorageIO>(
        table_name: &str,
        tokens: &Arc<SmallVec<[Token; 36]>>,
        schema: &Arc<SmallVec<[SchemaField; 20]>>,
        io_rows: &Arc<S>,
        iterator: &RowPointerIterator<S>,
    ) -> Option<(Arc<Vec<RowPointer>>, usize)> {
        if !semantics_enabled() {
            return None;
        }

        let pointers_io = iterator.io();
        let sme = SME.get_or_init(|| rclite::Arc::new(SemanticMappingEngine::new()));
        sme.get_or_build_candidates_for_table_tokens(
            table_name,
            &*tokens,
            schema.as_slice(),
            Arc::clone(io_rows),
            pointers_io,
        )
        .await
        .map(|arc| (arc, 0usize))
    }

    #[cfg(feature = "sme_v2")]
    async fn maybe_build_sme_candidates<S: StorageIO>(
        table_name: &str,
        tokens: &Arc<SmallVec<[Token; 36]>>,
        schema: &Arc<SmallVec<[SchemaField; 20]>>,
        io_rows: &Arc<S>,
        iterator: &RowPointerIterator<S>,
    ) -> Option<(Arc<Vec<RowPointer>>, usize)> {
        let _ = schema;
        let _ = io_rows;

        if !semantics_enabled() {
            return None;
        }

        let start = std::time::Instant::now();
        log::trace!("maybe_build_sme_candidates: starting for table {}", table_name);

        // SME v2 currently supports a minimal predicate subset:
        //   <ident> = <number>
        // and combines multiple such predicates by intersecting row-id ranges.
        // If the query contains parentheses or OR, fall back to full scan.
        for t in tokens.iter() {
            if matches!(t, Token::LPar | Token::RPar) {
                log::trace!("maybe_build_sme_candidates: query contains parens, skipping SME");
                return None;
            }
            if let Token::Next(s) = t {
                if s.eq_ignore_ascii_case("or") {
                    log::trace!("maybe_build_sme_candidates: query contains OR, skipping SME");
                    return None;
                }
            }
        }

        let engine = SME_V2.get_or_init(|| rclite::Arc::new(SemanticMappingEngineV2::new()));
        let pointers_io = iterator.io();

        let mut ranges: Option<Vec<RowRange>> = None;

        // Pattern match token triplets: Ident, Op("="), Number
        let toks = &**tokens;
        if toks.len() < 3 {
             log::trace!("maybe_build_sme_candidates: query too short for predicates");
            return None;
        }

        for w in toks.windows(3) {
            let (Token::Ident((_name, db_type, schema_id)), Token::Op(op), Token::Number(num)) =
                (&w[0], &w[1], &w[2])
            else {
                continue;
            };

            if op != "=" {
                 log::trace!("maybe_build_sme_candidates: found non-equality op '{}', ignoring predicate", op);
                continue;
            }

            let query = Self::numeric_value_to_scalar(num);
            let query = match Self::coerce_scalar_for_db_type(db_type, query) {
                Some(q) => q,
                None => {
                    log::trace!("maybe_build_sme_candidates: type coercion failed for {:?} -> {:?}", num, db_type);
                    continue
                },
            };

             log::trace!("maybe_build_sme_candidates: solving predicate col_{} = {:?}", schema_id, query);
            let mut r = engine
                .candidates_for_numeric_equals(
                    Arc::clone(&pointers_io),
                    table_name,
                    *schema_id,
                    query,
                )
                .await
                .ok()?;
            r = normalize_ranges(r);
            log::trace!("maybe_build_sme_candidates: got {} ranges for predicate", r.len());

            ranges = Some(match ranges {
                None => r,
                Some(prev) => intersect_ranges(&prev, &r),
            });
        }

        let Some(mut ranges) = ranges else {
            log::trace!("maybe_build_sme_candidates: no ranges derived from query");
            return None;
        };

        ranges = normalize_ranges(ranges);
        if ranges.is_empty() {
            log::info!("maybe_build_sme_candidates: intersection empty, 0 candidates in {:.2?}", start.elapsed());
            return Some((Arc::new(Vec::new()), 0));
        }

        log::debug!(
            "maybe_build_sme_candidates: resolved {} ranges, scanning pointers...", 
            ranges.len()
        );

        // Convert row-id ranges to candidate RowPointers by scanning pointer records only.
        let mut it = RowPointerIterator::new(pointers_io.clone()).await.ok()?;
        it.reset();

        let mut out: Vec<RowPointer> = Vec::new();
        let mut batch: Vec<RowPointer> = Vec::new();

        let mut range_idx = 0usize;
        loop {
            batch.clear();
            it.next_row_pointers_into(&mut batch).await.ok()?;
            if batch.is_empty() {
                break;
            }

            for p in batch.iter() {
                if p.deleted {
                    continue;
                }
                let id_u64 = Self::row_pointer_id_u64(p.id);

                while range_idx < ranges.len() && id_u64 > ranges[range_idx].end_row_id {
                    range_idx += 1;
                }
                if range_idx >= ranges.len() {
                    break;
                }
                if id_u64 >= ranges[range_idx].start_row_id && id_u64 <= ranges[range_idx].end_row_id {
                    out.push(p.clone());
                }
            }

            if range_idx >= ranges.len() {
                break;
            }
        }
        
        log::info!("maybe_build_sme_candidates: built {} candidates in {:.2?}", out.len(), start.elapsed());

        Some((Arc::new(out), 0))
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
        _ => None,
    }
}

#[cfg(feature = "sme_v2")]
#[cfg(feature = "enable_long_row")]
#[inline]
fn row_pointer_id_u64(id: u128) -> u64 {
    if id > (u64::MAX as u128) {
        u64::MAX
    } else {
        id as u64
    }
}

#[cfg(feature = "sme_v2")]
#[cfg(not(feature = "enable_long_row"))]
#[inline]
fn row_pointer_id_u64(id: u64) -> u64 {
    id
}
    async fn spawn_batch_tasks<S: StorageIO>(
        iterator: &mut RowPointerIterator<S>,
        mut sme_candidates: Option<(Arc<Vec<RowPointer>>, usize)>,
        shared_tuple: Arc<(Semaphore, RowFetch, RowFetch)>,
        schema_arc: Arc<SmallVec<[SchemaField; 20]>>,
        tokens_arc: Arc<SmallVec<[Token; 36]>>,
        io_rows_outer: Arc<S>,
        tx: kanal::AsyncSender<Row>,
        tx_rp: kanal::AsyncSender<RowPointer>,
        limit: Option<u64>,
        enforce_limit_early: bool,
        no_filter: bool,
        limit_counter: Arc<CacheGuard<AtomicU64>>,
        stop_processing: Arc<CacheGuard<AtomicBool>>,
        reading_time_total: Arc<CacheGuard<AtomicU64>>,
    ) -> Vec<tokio::task::JoinHandle<()>> {
        let mut batch_handles = Vec::new();
        iterator.reset();

        // Process batches
        loop {
            if stop_processing.load(std::sync::atomic::Ordering::Relaxed) {
                break;
            }

            let reading_time_total_clone = Arc::clone(&reading_time_total);

            let mut pointers = row_pointer_vec_pool().get();

            if let Some((cands, idx)) = &mut sme_candidates {
                let batch_size = *BATCH_SIZE.get().unwrap();
                if *idx < cands.len() {
                    let end = std::cmp::min(*idx + batch_size, cands.len());
                    pointers.extend(cands[*idx..end].iter().cloned());
                    *idx = end;
                }
            } else {
                if iterator.next_row_pointers_into(&mut *pointers).await.is_err() {
                    pointers.clear();
                }
            }

            if pointers.is_empty() {
                break;
            }

            if stop_processing.load(std::sync::atomic::Ordering::Relaxed) {
                break;
            }

            let tuple_clone = Arc::clone(&shared_tuple);
            let tx_clone = tx.clone();
            let tx_rp_clone = tx_rp.clone();
            let schema_ref = Arc::clone(&schema_arc);
            let tokens_ref = Arc::clone(&tokens_arc);
            let io_rows_batch = Arc::clone(&io_rows_outer);

            let limit_opt = limit;
            let limit_counter_ref = Arc::clone(&limit_counter);
            let stop_ref = Arc::clone(&stop_processing);
            let enforce_limit_early = enforce_limit_early;
            let no_filter = no_filter;

            let batch_handle = task::spawn(async move {
                let reading_time_total_clone_2 = Arc::clone(&reading_time_total_clone);

                // Acquire a permit for this batch
                let _batch_permit = tuple_clone.0.acquire().await.unwrap();

                // Fast-path for queries without WHERE: accept all non-deleted rows.
                if no_filter {
                    let mut row = Row::default();

                    for pointer in pointers.iter() {
                        if stop_ref.load(std::sync::atomic::Ordering::Relaxed) {
                            break;
                        }

                        if pointer.deleted {
                            continue;
                        }

                        // Quick stop check for LIMIT to avoid unnecessary fetch work.
                        if enforce_limit_early {
                            if let Some(limit_val) = limit_opt {
                                if limit_counter_ref.load(std::sync::atomic::Ordering::Relaxed)
                                    >= limit_val
                                {
                                    stop_ref.store(true, std::sync::atomic::Ordering::Relaxed);
                                    break;
                                }
                            }
                        }

                        let reading_time = std::time::Instant::now();
                        let io_rows_clone = Arc::clone(&io_rows_batch);
                        let (_, _, requested_row_fetch) = &*tuple_clone;

                        if pointer
                            .fetch_row_reuse_async(
                                io_rows_clone,
                                requested_row_fetch,
                                &mut row,
                            )
                            .await
                            .is_err()
                        {
                            continue;
                        }

                        let elapsed_ns = reading_time.elapsed().as_nanos() as u64;
                        reading_time_total_clone_2
                            .fetch_add(elapsed_ns, std::sync::atomic::Ordering::Relaxed);

                        // LIMIT is enforced on successfully fetched/emitted rows.
                        if enforce_limit_early {
                            if let Some(limit_val) = limit_opt {
                                let reserved = limit_counter_ref.fetch_update(
                                    std::sync::atomic::Ordering::Relaxed,
                                    std::sync::atomic::Ordering::Relaxed,
                                    |cur| if cur <= limit_val { Some(cur + 1) } else { None },
                                );

                                match reserved {
                                    Ok(prev) => {
                                        if prev + 1 >= limit_val {
                                            stop_ref.store(
                                                true,
                                                std::sync::atomic::Ordering::Relaxed,
                                            );
                                        }
                                    }
                                    Err(_) => {
                                        stop_ref.store(true, std::sync::atomic::Ordering::Relaxed);
                                        break;
                                    }
                                }
                            }
                        }

                        if tx_rp_clone.send(pointer.clone()).await.is_err() {
                            log::error!("Failed to send row pointer to cache inserter");
                            break;
                        }

                        if tx_clone.send(Row::clone_row(&row)).await.is_err() {
                            log::error!("Failed to send row to aggregator");
                            break;
                        }
                    }

                    return;
                }

                // Local row buffer reused across all pointers in this batch
                let row = Row::default();

                let mut buffer = Buffer {
                    hashtable_buffer: UnsafeCell::new(SmallVec::new()),
                    row,
                    transformers: SmallVec::new(),
                    intermediate_results: SmallVec::new(),
                    bool_buffer: SmallVec::new(),
                };

                // Build a processor and parser once per batch
                let processor_cell = UnsafeCell::new(TransformerProcessor::new(
                    &mut buffer.transformers,
                    &mut buffer.intermediate_results,
                ));
                let processor_mut = unsafe { &mut *processor_cell.get() };
                let parser_cell = UnsafeCell::new(QueryParser::new(&tokens_ref, processor_mut));

                // Build the transformer plan only once (first row in this batch)
                let mut plan_built = false;

                for pointer in pointers.iter() {
                    if stop_ref.load(std::sync::atomic::Ordering::Relaxed) {
                        break;
                    }

                    if pointer.deleted {
                        continue;
                    }

                    let io_rows_clone = Arc::clone(&io_rows_batch);
                    let (_, query_row_fetch, requested_row_fetch) = &*tuple_clone;

                    let reading_time = std::time::Instant::now();

                    // Read the minimal set of columns needed for the WHERE evaluation
                    if pointer
                        .fetch_row_reuse_async(io_rows_clone, query_row_fetch, &mut buffer.row)
                        .await
                        .is_err()
                    {
                        error!("Failed to fetch query row for pointer {:?}", pointer);
                        continue;
                    }

                    let elapsed_ns = reading_time.elapsed().as_nanos() as u64;
                    reading_time_total_clone_2
                        .fetch_add(elapsed_ns, std::sync::atomic::Ordering::Relaxed);

                    // On the first row, parse and build the plan using a temporary vector.
                    if !plan_built {
                        let tmp_vec = unsafe { &mut *buffer.hashtable_buffer.get() };
                        tmp_vec.clear();

                        // Populate tmp_vec from the current row for the first parse only.
                        // Subsequent rows use a zero-copy view.
                        {
                            let columns = &buffer.row.columns;
                            let schema = &*schema_ref;

                            // Build an indexed map by write_order to avoid misalignment.
                            // Initialize to schema length so lookups by write_order are valid.
                            tmp_vec.clear();
                            tmp_vec.resize(
                                schema.len(),
                                (Cow::Borrowed(EMPTY_STR), MemoryBlock::default()),
                            );

                            // SAFETY: schema_id and write_order come from the schema; bounds are checked by get_mut
                            for column in columns.iter() {
                                let schema_field = &schema[column.schema_id as usize];
                                let write_order = schema_field.write_order as usize;
                                if let Some(entry) = tmp_vec.get_mut(write_order) {
                                    *entry = (
                                        Cow::Borrowed(schema_field.name.as_str()),
                                        column.data.clone(),
                                    );
                                }
                            }
                        }

                        // Build transformations once
                        let parser_mut = unsafe { &mut *parser_cell.get() };
                        // Do NOT call reset_for_next_execution() per row anymore.
                        if let Err(e) = parser_mut.execute(tmp_vec) {
                            panic!(
                                "Error executing query parser while building plan: {:?}",
                                e
                            );
                        }

                        // Ensure the intermediate buffer has the needed capacity once
                        unsafe { &mut *processor_cell.get() }.replace_row_inputs(&[]);

                        plan_built = true;
                    }

                    // Evaluate against the current row using a zero-copy view
                    let result = {
                        // No need to clear bool_buffer; execute_row clears the output slice itself
                        let processor = unsafe { &mut *processor_cell.get() };
                        processor.execute_row(
                            &RowView::new(&buffer.row, schema_ref.as_slice()),
                            &mut buffer.bool_buffer,
                        )
                    };

                    if result {
                        let io_rows_clone = Arc::clone(&io_rows_batch);

                        let reading_time = std::time::Instant::now();

                        if let Err(e) = pointer
                            .fetch_row_reuse_async(
                                io_rows_clone,
                                requested_row_fetch,
                                &mut buffer.row,
                            )
                            .await
                        {
                            error!(
                                "Failed to fetch requested row for pointer {:?}: kind={:?} err={}",
                                pointer,
                                e.kind(),
                                e
                            );
                            continue;
                        }

                        let elapsed_ns = reading_time.elapsed().as_nanos() as u64;
                        reading_time_total_clone_2
                            .fetch_add(elapsed_ns, std::sync::atomic::Ordering::Relaxed);

                        // LIMIT is enforced on successfully fetched/emitted rows.
                        // For ORDER BY queries, don't stop early; we must sort the full result set.
                        if enforce_limit_early {
                            if let Some(limit_val) = limit_opt {
                                // Quick stop check to avoid extra work.
                                if limit_counter_ref.load(std::sync::atomic::Ordering::Relaxed)
                                    >= limit_val
                                {
                                    stop_ref.store(true, std::sync::atomic::Ordering::Relaxed);
                                    break;
                                }

                                let reserved = limit_counter_ref.fetch_update(
                                    std::sync::atomic::Ordering::Relaxed,
                                    std::sync::atomic::Ordering::Relaxed,
                                    |cur| if cur <= limit_val { Some(cur + 1) } else { None },
                                );

                                match reserved {
                                    Ok(prev) => {
                                        if prev + 1 >= limit_val {
                                            stop_ref.store(
                                                true,
                                                std::sync::atomic::Ordering::Relaxed,
                                            );
                                        }
                                    }
                                    Err(_) => {
                                        stop_ref.store(true, std::sync::atomic::Ordering::Relaxed);
                                        break;
                                    }
                                }
                            }
                        }

                        if tx_rp_clone.send(pointer.clone()).await.is_err() {
                            log::error!("Failed to send row pointer to cache inserter");
                            // Receiver dropped; stop early
                            break;
                        }

                        // Send a clone out to the aggregator
                        if tx_clone.send(Row::clone_row(&buffer.row)).await.is_err() {
                            log::error!("Failed to send row to aggregator");
                            // Receiver dropped; stop early
                            break;
                        }
                    }
                }
            });

            batch_handles.push(batch_handle);
        }

        batch_handles
    }

    #[inline]
    fn drain_rows(rx: kanal::AsyncReceiver<Row>) -> Vec<Row> {
        let mut collected_rows = Vec::with_capacity(50);
        if !rx.is_empty() {
            rx.drain_into(&mut collected_rows).unwrap();
        }
        drop(rx);
        collected_rows
    }

    #[inline]
    fn drain_row_pointers(rx_rp: kanal::AsyncReceiver<RowPointer>) -> Vec<RowPointer> {
        let mut pointers = Vec::with_capacity(50);
        if !rx_rp.is_empty() {
            rx_rp.drain_into(&mut pointers).unwrap();
        }
        drop(rx_rp);
        pointers
    }

    fn sort_rows_if_needed(
        all_rows: &mut Vec<Row>,
        schema_arc: &Arc<SmallVec<[SchemaField; 20]>>,
        order_by: &Option<String>,
    ) {
        // ORDER BY: after collection, sort the resulting rows.
        // NOTE: Row/Column schema_id uses SchemaField.write_order (not vector index).
        if let Some(order_field) = order_by {
            let order_write_order = schema_arc
                .iter()
                .find(|f| f.name.eq_ignore_ascii_case(order_field.as_str()))
                .map(|f| f.write_order as usize);

            if let Some(schema_id) = order_write_order {
                all_rows.sort_by(|a, b| {
                    compare_rows_by_schema_id(a, b, schema_id, schema_arc.as_slice())
                });
            } else {
                log::warn!(
                    "ORDER BY field '{}' not found in schema; skipping sort",
                    order_field
                );
            }
        }
    }

    pub async fn process<'a, S: StorageIO>(
        &self,
        table_name: &str,
        where_query: &str,
        query_row_fetch: RowFetch,
        requested_row_fetch: RowFetch,
        table_schema: &Vec<SchemaField>,
        io_rows: Arc<S>,
        iterator: &mut RowPointerIterator<S>,
        limit: Option<u64>,
        order_by: Option<String>,
    ) -> Vec<Row> {
        let prep_stopwatch = std::time::Instant::now();

        log::info!(
            "Starting concurrent processing for query: {}",
            where_query
        );

        // Cache fast-path (if enabled)
        if let Some((_hash, rows)) =
            Self::try_collect_cached_rows(table_name, where_query, &requested_row_fetch, &io_rows).await
        {
            return rows;
        }

        let (tx, rx) = kanal::unbounded_async::<Row>();
        let (tx_rp, rx_rp) = kanal::unbounded_async::<RowPointer>();

        // LIMIT enforcement: global counter across all tasks.
        // The increment/threshold check is performed inside the `if result { }` section.
        let limit_counter: Arc<CacheGuard<AtomicU64>> = Arc::new(AtomicU64::new(0).into());
        let stop_processing: Arc<CacheGuard<AtomicBool>> = Arc::new(AtomicBool::new(false).into());

        // Important: for ORDER BY queries, we must not stop early based on LIMIT,
        // otherwise we only sort a prefix of the matching rows.
        let enforce_limit_early = limit.is_some() && order_by.is_none();

        let shared_tuple: Arc<(Semaphore, RowFetch, RowFetch)> = Arc::new((
            Semaphore::new(*MAX_PERMITS_THREADS.get().unwrap()),
            query_row_fetch,
            requested_row_fetch,
        ));

        let table_schema = table_schema
            .iter()
            .cloned()
            .collect::<SmallVec<[SchemaField; 20]>>();

        let io_rows_outer = Arc::clone(&io_rows);

        let crc64_hash = if *ENABLE_CACHE.get().unwrap() {
            CRC_64.checksum(table_name.as_bytes()) ^ CRC_64.checksum(where_query.as_bytes())
        } else {
            0
        };

        let (no_filter, schema_arc, tokens_arc) = Self::prepare_query(where_query, &table_schema);

        log::info!(
            "Preperation for query completed in {:.2?}",
            prep_stopwatch.elapsed()
        );

        let semantic_stopwatch = std::time::Instant::now();
        if semantics_enabled() {
            log::info!("Starting semantic processing for query: {}", where_query);
        }

        // If SME semantics are enabled and SME can build candidates for this query/table,
        // only iterate those. Otherwise fall back to scanning all pointers.
        let sme_candidates =
            Self::maybe_build_sme_candidates(table_name, &tokens_arc, &schema_arc, &io_rows_outer, iterator)
                .await;

        if sme_candidates.is_some() {
            log::info!(
                "Semantic processing will use candidate row pointers, with {} candidates for query: {}",
                sme_candidates.as_ref().unwrap().0.len(),
                where_query
            );
        } else {
            if semantics_enabled() {
                log::info!(
                    "Semantic processing will scan all row pointers for query: {}",
                    where_query
                );
            }
        }

        if semantics_enabled() {
            log::info!(
                "Semantic processing setup completed in {:.2?}",
                semantic_stopwatch.elapsed()
            );
        }
        
        let reading_time_total: Arc<CacheGuard<AtomicU64>> = Arc::new(AtomicU64::new(0).into());

        let batch_handles = Self::spawn_batch_tasks(
            iterator,
            sme_candidates,
            Arc::clone(&shared_tuple),
            Arc::clone(&schema_arc),
            Arc::clone(&tokens_arc),
            Arc::clone(&io_rows_outer),
            tx.clone(),
            tx_rp.clone(),
            limit,
            enforce_limit_early,
            no_filter,
            Arc::clone(&limit_counter),
            Arc::clone(&stop_processing),
            Arc::clone(&reading_time_total),
        )
        .await;

        // Wait for all batch tasks to complete
        join_all(batch_handles).await;

        // Drop the sender to signal that no more rows will be sent
        drop(tx);

        let mut all_rows = Self::drain_rows(rx);

        Self::sort_rows_if_needed(&mut all_rows, &schema_arc, &order_by);

        // Safety: even though LIMIT is enforced strictly at send-time, keep a final cap.
        if let Some(limit_val) = limit {
            if all_rows.len() > limit_val as usize {
                all_rows.truncate(limit_val as usize);
            }
        }

        if crc64_hash != 0 {
            let pointers = Self::drain_row_pointers(rx_rp);

            // Store the resulting pointers in the cache
            let cache = ATOMIC_CACHE.get().unwrap();
            if cache.insert(crc64_hash, Arc::new(pointers)).is_err() {
                log::error!("Failed to insert entries into cache for hash {}", crc64_hash);
            }
        }

        let total_reading_ns = reading_time_total.load(std::sync::atomic::Ordering::Relaxed);

        log::info!(
            "Total reading time for query '{}' : {:.2?} over {} rows",
            where_query,
            std::time::Duration::from_nanos(total_reading_ns),
            all_rows.len()
        );

        all_rows
    }

    /// Concurrently evaluates a WHERE clause and returns matching row pointers (with their pointer-record offsets).
    ///
    /// This is intended for UPDATE/DELETE operations where the caller must mutate the pointer record
    /// (tombstone or update-in-place/append).
    pub async fn process_row_pointers<S: StorageIO>(
        &self,
        table_name: &str,
        where_query: &str,
        query_row_fetch: RowFetch,
        table_schema: &SmallVec<[SchemaField; 20]>,
        io_rows: Arc<S>,
        iterator: &mut RowPointerIterator<S>,
        limit: Option<u64>,
    ) -> Vec<RowPointerWithOffset> {
        let prep_stopwatch = std::time::Instant::now();

        log::info!(
            "Starting concurrent pointer processing for query: {}",
            where_query
        );

        let (tx, rx) = kanal::unbounded_async::<RowPointerWithOffset>();

        let limit_counter = Arc::new(AtomicU64::new(0));
        let stop_processing = Arc::new(AtomicBool::new(false));

        let shared_tuple: Arc<(Semaphore, RowFetch)> = Arc::new((
            Semaphore::new(*MAX_PERMITS_THREADS.get().unwrap()),
            query_row_fetch,
        ));

        let table_schema = table_schema
            .iter()
            .cloned()
            .collect::<SmallVec<[SchemaField; 20]>>();

        let io_rows_outer = Arc::clone(&io_rows);

        // Empty WHERE means "match all rows"; skip tokenization/parsing entirely.
        let no_filter = where_query.trim().is_empty();
        let tokens: SmallVec<[Token; 36]> = if no_filter {
            SmallVec::new()
        } else {
            tokenize(&where_query, &table_schema)
        };

        let schema_arc = Arc::new(table_schema);
        let tokens_arc = Arc::new(tokens);

        log::info!(
            "Preperation for pointer query completed in {:.2?}",
            prep_stopwatch.elapsed()
        );

        if semantics_enabled() {
            // NOTE: SME candidates currently don't carry pointer-record offsets.
            // For correctness of UPDATE/DELETE (which must write pointer records), we conservatively
            // fall back to scanning all pointers here.
            log::info!(
                "Semantic processing is enabled, but pointer-record offsets are required; scanning all row pointers for query: {}",
                where_query
            );
        }

        let reading_time_total = Arc::new(AtomicU64::new(0));

        let mut batch_handles = Vec::new();
        iterator.reset();

        loop {
            if stop_processing.load(std::sync::atomic::Ordering::Relaxed) {
                break;
            }

            let mut pointers: Vec<RowPointerWithOffset> = Vec::with_capacity(*BATCH_SIZE.get().unwrap());

            for _ in 0..*BATCH_SIZE.get().unwrap() {
                match iterator.next_row_pointer_with_offset().await {
                    Ok(Some((pointer_record_pos, pointer))) => {
                        pointers.push(RowPointerWithOffset {
                            pointer_record_pos,
                            pointer,
                        });
                    }
                    Ok(None) => break,
                    Err(_) => break,
                }
            }

            if pointers.is_empty() {
                break;
            }

            if stop_processing.load(std::sync::atomic::Ordering::Relaxed) {
                break;
            }

            let tuple_clone = Arc::clone(&shared_tuple);
            let tx_clone = tx.clone();
            let schema_ref = Arc::clone(&schema_arc);
            let tokens_ref = Arc::clone(&tokens_arc);
            let io_rows_batch = Arc::clone(&io_rows_outer);

            let limit_opt = limit;
            let limit_counter_ref = Arc::clone(&limit_counter);
            let stop_ref = Arc::clone(&stop_processing);
            let no_filter = no_filter;
            let reading_time_total_clone = Arc::clone(&reading_time_total);

            let batch_handle = task::spawn(async move {
                // Acquire a permit for this batch
                let _batch_permit = tuple_clone.0.acquire().await.unwrap();

                // Fast-path for empty WHERE: accept all non-deleted pointers.
                if no_filter {
                    for pwo in pointers.into_iter() {
                        if stop_ref.load(std::sync::atomic::Ordering::Relaxed) {
                            break;
                        }

                        if pwo.pointer.deleted {
                            continue;
                        }

                        if let Some(limit_val) = limit_opt {
                            // Stop once we have emitted limit rows.
                            if limit_counter_ref.load(std::sync::atomic::Ordering::Relaxed)
                                >= limit_val
                            {
                                stop_ref.store(true, std::sync::atomic::Ordering::Relaxed);
                                break;
                            }

                            let reserved = limit_counter_ref.fetch_update(
                                std::sync::atomic::Ordering::Relaxed,
                                std::sync::atomic::Ordering::Relaxed,
                                |cur| if cur <= limit_val { Some(cur + 1) } else { None },
                            );

                            if reserved.is_err() {
                                stop_ref.store(true, std::sync::atomic::Ordering::Relaxed);
                                break;
                            }
                        }

                        if tx_clone.send(pwo).await.is_err() {
                            break;
                        }
                    }

                    return;
                }

                // Local row buffer reused across all pointers in this batch
                let row = Row::default();
                let mut buffer = Buffer {
                    hashtable_buffer: UnsafeCell::new(SmallVec::new()),
                    row,
                    transformers: SmallVec::new(),
                    intermediate_results: SmallVec::new(),
                    bool_buffer: SmallVec::new(),
                };

                // Build a processor and parser once per batch
                let processor_cell = UnsafeCell::new(TransformerProcessor::new(
                    &mut buffer.transformers,
                    &mut buffer.intermediate_results,
                ));
                let processor_mut = unsafe { &mut *processor_cell.get() };
                let parser_cell = UnsafeCell::new(QueryParser::new(&tokens_ref, processor_mut));
                let mut plan_built = false;

                for pwo in pointers.into_iter() {
                    if stop_ref.load(std::sync::atomic::Ordering::Relaxed) {
                        break;
                    }

                    if pwo.pointer.deleted {
                        continue;
                    }

                    let io_rows_clone = Arc::clone(&io_rows_batch);
                    let (_, query_row_fetch) = &*tuple_clone;

                    let reading_time = std::time::Instant::now();

                    if pwo
                        .pointer
                        .fetch_row_reuse_async(io_rows_clone, query_row_fetch, &mut buffer.row)
                        .await
                        .is_err()
                    {
                        continue;
                    }

                    let elapsed_ns = reading_time.elapsed().as_nanos() as u64;
                    reading_time_total_clone.fetch_add(
                        elapsed_ns,
                        std::sync::atomic::Ordering::Relaxed,
                    );

                    if !plan_built {
                        let tmp_vec = unsafe { &mut *buffer.hashtable_buffer.get() };
                        tmp_vec.clear();

                        {
                            let columns = &buffer.row.columns;
                            let schema = &*schema_ref;

                            tmp_vec.clear();
                            tmp_vec.resize(
                                schema.len(),
                                (Cow::Borrowed(EMPTY_STR), MemoryBlock::default()),
                            );

                            for column in columns.iter() {
                                // Here `schema_id` is write_order.
                                let write_order = column.schema_id as usize;
                                if let Some(schema_field) = schema.iter().find(|f| f.write_order as usize == write_order) {
                                    if let Some(entry) = tmp_vec.get_mut(write_order) {
                                        *entry = (
                                            Cow::Borrowed(schema_field.name.as_str()),
                                            column.data.clone(),
                                        );
                                    }
                                }
                            }
                        }

                        let parser_mut = unsafe { &mut *parser_cell.get() };
                        if let Err(e) = parser_mut.execute(tmp_vec) {
                            panic!(
                                "Error executing query parser while building plan: {:?}",
                                e
                            );
                        }

                        unsafe { &mut *processor_cell.get() }.replace_row_inputs(&[]);
                        plan_built = true;
                    }

                    let result = {
                        let processor = unsafe { &mut *processor_cell.get() };
                        processor.execute_row(
                            &RowView::new(&buffer.row, schema_ref.as_slice()),
                            &mut buffer.bool_buffer,
                        )
                    };

                    if result {
                        if let Some(limit_val) = limit_opt {
                            if limit_counter_ref.load(std::sync::atomic::Ordering::Relaxed)
                                >= limit_val
                            {
                                stop_ref.store(true, std::sync::atomic::Ordering::Relaxed);
                                break;
                            }

                            let reserved = limit_counter_ref.fetch_update(
                                std::sync::atomic::Ordering::Relaxed,
                                std::sync::atomic::Ordering::Relaxed,
                                |cur| if cur <= limit_val { Some(cur + 1) } else { None },
                            );

                            if reserved.is_err() {
                                stop_ref.store(true, std::sync::atomic::Ordering::Relaxed);
                                break;
                            }
                        }

                        if tx_clone.send(pwo).await.is_err() {
                            break;
                        }
                    }
                }
            });

            batch_handles.push(batch_handle);
        }

        join_all(batch_handles).await;
        drop(tx);

        let mut matches = Vec::with_capacity(50);
        if !rx.is_empty() {
            rx.drain_into(&mut matches).unwrap();
        }
        drop(rx);

        let total_reading_ns = reading_time_total.load(std::sync::atomic::Ordering::Relaxed);
        log::info!(
            "Total reading time for pointer query '{}' : {:.2?} over {} pointers",
            where_query,
            std::time::Duration::from_nanos(total_reading_ns),
            matches.len()
        );

        // Keep SME referenced to avoid unused warnings on some feature combos.
        let _ = table_name;

        matches
    }
}

fn compare_rows_by_schema_id(
    a: &Row,
    b: &Row,
    schema_id: usize,
    schema: &[SchemaField],
) -> std::cmp::Ordering {
    let a_col = a.columns.iter().find(|c| c.schema_id as usize == schema_id);
    let b_col = b.columns.iter().find(|c| c.schema_id as usize == schema_id);

    match (a_col, b_col) {
        (None, None) => std::cmp::Ordering::Equal,
        (None, Some(_)) => std::cmp::Ordering::Less,
        (Some(_), None) => std::cmp::Ordering::Greater,
        (Some(ac), Some(bc)) => {
            // Prefer the schema's declared type (more stable than per-column), fallback to column type.
            // schema_id here is SchemaField.write_order, not necessarily the schema slice index.
            let db_type = schema
                .iter()
                .find(|f| f.write_order as usize == schema_id)
                .map(|f| f.db_type.clone())
                .unwrap_or_else(|| ac.column_type.clone());
            compare_bytes_as_type(db_type, ac.data.into_slice(), bc.data.into_slice())
        }
    }
}

fn compare_bytes_as_type(db_type: crate::core::db_type::DbType, a: &[u8], b: &[u8]) -> std::cmp::Ordering {
    use crate::core::db_type::DbType;

    match db_type {
        DbType::STRING | DbType::UNKNOWN | DbType::START | DbType::END | DbType::DATETIME => a.cmp(b),
        DbType::CHAR => a.cmp(b),
        DbType::I8 => read_num::<1, i8>(a, b, i8::from_le_bytes),
        DbType::I16 => read_num::<2, i16>(a, b, i16::from_le_bytes),
        DbType::I32 => read_num::<4, i32>(a, b, i32::from_le_bytes),
        DbType::I64 => read_num::<8, i64>(a, b, i64::from_le_bytes),
        DbType::I128 => read_num::<16, i128>(a, b, i128::from_le_bytes),
        DbType::U8 => read_num::<1, u8>(a, b, u8::from_le_bytes),
        DbType::U16 => read_num::<2, u16>(a, b, u16::from_le_bytes),
        DbType::U32 => read_num::<4, u32>(a, b, u32::from_le_bytes),
        DbType::U64 => read_num::<8, u64>(a, b, u64::from_le_bytes),
        DbType::U128 => read_num::<16, u128>(a, b, u128::from_le_bytes),
        DbType::BOOL => read_num::<1, u8>(a, b, u8::from_le_bytes),
        DbType::F32 => {
            let av = read_float::<4, f32>(a, f32::from_le_bytes);
            let bv = read_float::<4, f32>(b, f32::from_le_bytes);
            av.partial_cmp(&bv).unwrap_or(std::cmp::Ordering::Equal)
        }
        DbType::F64 => {
            let av = read_float::<8, f64>(a, f64::from_le_bytes);
            let bv = read_float::<8, f64>(b, f64::from_le_bytes);
            av.partial_cmp(&bv).unwrap_or(std::cmp::Ordering::Equal)
        }
        DbType::NULL => std::cmp::Ordering::Equal,
    }
}

fn read_num<const N: usize, T: Ord + Copy>(
    a: &[u8],
    b: &[u8],
    f: fn([u8; N]) -> T,
) -> std::cmp::Ordering {
    let aa = a.get(0..N);
    let bb = b.get(0..N);
    if let (Some(aa), Some(bb)) = (aa, bb) {
        let mut arr_a = [0u8; N];
        let mut arr_b = [0u8; N];
        arr_a.copy_from_slice(aa);
        arr_b.copy_from_slice(bb);
        let av = f(arr_a);
        let bv = f(arr_b);
        av.cmp(&bv)
    } else {
        a.cmp(b)
    }
}

fn read_float<const N: usize, T: Copy>(a: &[u8], f: fn([u8; N]) -> T) -> T {
    if let Some(aa) = a.get(0..N) {
        let mut arr_a = [0u8; N];
        arr_a.copy_from_slice(aa);
        f(arr_a)
    } else {
        // Fallback: treat missing/invalid as 0
        f([0u8; N])
    }
}

pub struct Buffer<'a> {
    pub hashtable_buffer: UnsafeCell<SmallVec<[(Cow<'a, str>, MemoryBlock); 20]>>,
    pub row: Row,
    pub transformers: SmallVec<[ColumnTransformer; 36]>,
    pub intermediate_results: SmallVec<[MemoryBlock; 20]>,
    pub bool_buffer: SmallVec<[(bool, Option<Next>); 20]>,
}

// A zero-copy view over Row's MemoryBlocks for execution, indexed by schema write_order.
// This maps logical indices (write_order) used by the QueryParser/Transformers to the
// actual MemoryBlocks present in the fetched row (which may be a subset of columns).
struct RowView<'a> {
    row: &'a Row,
    schema: &'a [SchemaField],
}

impl<'a> RowView<'a> {
    #[inline]
    fn new(row: &'a Row, schema: &'a [SchemaField]) -> Self {
        Self { row, schema }
    }
}

impl<'a> RowBlocks for RowView<'a> {
    #[inline]
    fn mb_at(&self, idx: usize) -> &MemoryBlock {
        // Map requested write_order index to the corresponding column in this row.
        // Rows fetched for WHERE may contain only a subset of columns, so we search
        // by schema.write_order rather than assuming positional alignment.
        for col in self.row.columns.iter() {
            let schema_field = &self.schema[col.schema_id as usize];
            if schema_field.write_order as usize == idx {
                return &col.data;
            }
        }
        panic!(
            "Requested column write_order {} not present in fetched row (fetched {} cols)",
            idx,
            self.row.columns.len()
        );
    }

    #[inline]
    fn len(&self) -> usize {
        // Return full schema length to reflect logical indexing space.
        self.schema.len()
    }
}