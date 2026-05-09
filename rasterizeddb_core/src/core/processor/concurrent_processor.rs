use std::{borrow::Cow, cell::UnsafeCell, sync::{OnceLock, atomic::{AtomicBool, AtomicU64}}};

use branches::{likely, unlikely};
use cacheguard::CacheGuard;
use log::error;
use rclite::Arc;
use std::sync::Arc as StdArc;

use crc::{CRC_64_ECMA_182, Crc};
use futures::{future::join_all};
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

use crate::BATCH_SIZE;

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

            if pointer
                .fetch_row_reuse_async(&**io_rows, requested_row_fetch, &mut row)
                .await
                .is_err()
            {
                continue;
            }

            collected_rows.push(Row::clone_row(&row));
        }

        Some((crc64_hash, collected_rows))
    }

    /// Bytes-mode cache fast-path. Serializes matched rows directly into a
    /// `Vec<u8>` at the wire format, skipping the `Vec<Row>` intermediate
    /// and the `Row::clone_row` per pointer. This is the cache-hit
    /// counterpart of the worker bytes-mode change in Phase 8.
    async fn try_collect_cached_bytes<S: StorageIO>(
        table_name: &str,
        where_query: &str,
        requested_row_fetch: &RowFetch,
        io_rows: &Arc<S>,
    ) -> Option<Vec<u8>> {
        let cache_enabled = *ENABLE_CACHE.get().unwrap();
        if !cache_enabled {
            return None;
        }

        let crc64_hash = CRC_64.checksum(table_name.as_bytes()) ^ CRC_64.checksum(where_query.as_bytes());
        let cache = ATOMIC_CACHE.get().unwrap();
        let entries = cache.get(&crc64_hash)?;

        // Conservative initial guess; extend_serialized will grow as needed.
        let mut out: Vec<u8> = Vec::with_capacity(entries.len().saturating_mul(64));
        let mut row = Row::default();

        for pointer in entries.iter() {
            if pointer.deleted {
                continue;
            }

            if pointer
                .fetch_row_reuse_async(&**io_rows, requested_row_fetch, &mut row)
                .await
                .is_err()
            {
                continue;
            }

            row.extend_serialized(&mut out);
        }

        Some(out)
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

    /// Long-lived worker pool design.
    ///
    /// Spawns `MAX_PERMITS_THREADS` worker tasks ONCE up front. Each worker
    /// allocates its own `Buffer`, `TransformerProcessor`, and `QueryParser`
    /// once and reuses them across every batch it pulls from `work_rx`.
    /// The query plan is built once per worker on its first WHERE-filtered
    /// batch and reused for the rest of the query.
    ///
    /// The iterator-driving loop runs in this same task (the caller awaits
    /// the returned join handles). Backpressure is provided by the bounded
    /// MPMC `work_tx` channel: when all workers are busy, the iterator stops
    /// reading ahead. Per-batch work submitted to the pool is a single
    /// `Vec<RowPointer>` send — no `tokio::task::spawn`, no per-batch
    /// `JoinHandle`, no Semaphore/futex round-trip per batch.
    async fn spawn_batch_tasks<S: StorageIO>(
        iterator: &mut RowPointerIterator<S>,
        shared_tuple: Arc<(RowFetch, RowFetch)>,
        schema_arc: Arc<SmallVec<[SchemaField; 20]>>,
        tokens_arc: Arc<SmallVec<[Token; 36]>>,
        io_rows_outer: Arc<S>,
        // Exactly ONE of `tx_rows` / `tx_bytes` should be Some, depending on
        // whether the caller wants `Vec<Row>` (e.g. for ORDER BY or row-level
        // consumers like database/mod.rs) or pre-serialized `Vec<u8>` (the
        // hot SELECT path — skips a full Row→Vec<u8> conversion downstream).
        tx_rows: Option<kanal::AsyncSender<Vec<Row>>>,
        tx_bytes: Option<kanal::AsyncSender<Vec<u8>>>,
        tx_rp: Option<kanal::AsyncSender<Vec<RowPointer>>>,
        limit: Option<u64>,
        enforce_limit_early: bool,
        no_filter: bool,
        // True when `query_row_fetch` already contains every column of `requested_row_fetch`,
        // so the second (requested) fetch can be skipped for matched rows.
        query_covers_requested: bool,
        limit_counter: Arc<CacheGuard<AtomicU64>>,
        stop_processing: Arc<CacheGuard<AtomicBool>>,
        reading_time_total: Arc<CacheGuard<AtomicU64>>,
    ) -> Vec<tokio::task::JoinHandle<()>> {
        let n_workers = *MAX_PERMITS_THREADS.get().unwrap();

        // Bounded MPMC channel for batch dispatch. Capacity = n_workers gives
        // natural backpressure: the iterator stops reading ahead when every
        // worker has a batch in-hand.
        let (work_tx, work_rx) =
            kanal::bounded_async::<Vec<RowPointer>>(n_workers);

        let mut worker_handles = Vec::with_capacity(n_workers);

        for _ in 0..n_workers {
            let work_rx = work_rx.clone();
            let tuple_clone = Arc::clone(&shared_tuple);
            let tx_rows_clone = tx_rows.as_ref().map(|s| s.clone());
            let tx_bytes_clone = tx_bytes.as_ref().map(|s| s.clone());
            let bytes_mode = tx_bytes_clone.is_some();
            let tx_rp_clone = tx_rp.as_ref().map(|s| s.clone());
            let schema_ref = Arc::clone(&schema_arc);
            let tokens_ref = Arc::clone(&tokens_arc);
            let io_rows_batch = Arc::clone(&io_rows_outer);
            let limit_counter_ref = Arc::clone(&limit_counter);
            let stop_ref = Arc::clone(&stop_processing);
            let reading_time_ref = Arc::clone(&reading_time_total);
            let limit_opt = limit;

            let h = task::spawn(async move {
                let io_rows_ref: &S = &*io_rows_batch;

                // Worker-local Buffer; allocated ONCE and reused across every
                // batch this worker processes. Same for the processor/parser
                // and the WHERE-evaluation plan (built once per worker on its
                // first batch).
                let mut buffer: Buffer<'_> = Buffer {
                    hashtable_buffer: UnsafeCell::new(SmallVec::new()),
                    row: Row::default(),
                    transformers: SmallVec::new(),
                    intermediate_results: SmallVec::new(),
                    bool_buffer: SmallVec::new(),
                    write_order_to_col: SmallVec::new(),
                };

                let processor_cell = UnsafeCell::new(TransformerProcessor::new(
                    &mut buffer.transformers,
                    &mut buffer.intermediate_results,
                ));
                let processor_mut = unsafe { &mut *processor_cell.get() };
                let parser_cell =
                    UnsafeCell::new(QueryParser::new(&tokens_ref, processor_mut));

                // Plan-build state lives across batches: same tokens + schema
                // produce the same plan for every batch this worker handles.
                let mut plan_built = false;

                // Per-worker result accumulators. Sent ONCE at the end of
                // this worker's lifetime to avoid per-batch channel sends
                // (each kanal send takes a parking_lot mutex). Exactly one
                // of `acc_rows` / `acc_bytes` is populated, governed by
                // `bytes_mode`.
                let mut acc_rows: Vec<Row> = Vec::new();
                let mut acc_bytes: Vec<u8> = Vec::new();
                let mut acc_ptrs: Vec<RowPointer> = Vec::new();

                while let Ok(pointers) = work_rx.recv().await {
                    let batch_read_start = std::time::Instant::now();

                    if no_filter {
                        if !bytes_mode {
                            acc_rows.reserve(pointers.len());
                        } else {
                            // Rough byte budget per row — grows on demand.
                            acc_bytes.reserve(pointers.len() * 64);
                        }
                        if tx_rp_clone.is_some() {
                            acc_ptrs.reserve(pointers.len());
                        }

                        for pointer in pointers.iter() {
                            if unlikely(stop_ref.load(std::sync::atomic::Ordering::Relaxed)) {
                                break;
                            }
                            if unlikely(pointer.deleted) {
                                continue;
                            }

                            if unlikely(enforce_limit_early) {
                                if let Some(limit_val) = limit_opt {
                                    if limit_counter_ref
                                        .load(std::sync::atomic::Ordering::Relaxed)
                                        >= limit_val
                                    {
                                        stop_ref.store(
                                            true,
                                            std::sync::atomic::Ordering::Relaxed,
                                        );
                                        break;
                                    }
                                }
                            }

                            let (_, requested_row_fetch) = &*tuple_clone;

                            if unlikely(pointer
                                .fetch_row_reuse_async(
                                    io_rows_ref,
                                    requested_row_fetch,
                                    &mut buffer.row,
                                )
                                .await
                                .is_err())
                            {
                                continue;
                            }

                            if unlikely(enforce_limit_early) {
                                if let Some(limit_val) = limit_opt {
                                    let prev = limit_counter_ref.fetch_add(
                                        1,
                                        std::sync::atomic::Ordering::Relaxed,
                                    );
                                    if unlikely(prev + 1 >= limit_val) {
                                        stop_ref.store(
                                            true,
                                            std::sync::atomic::Ordering::Relaxed,
                                        );
                                    }
                                    // Skip the emit if we already reached the
                                    // limit on a sibling worker. Without this
                                    // gate, up to N_workers extra rows can be
                                    // emitted past LIMIT (the row-mode path
                                    // papers over this with a final truncate;
                                    // the bytes-mode path cannot).
                                    if unlikely(prev >= limit_val) {
                                        break;
                                    }
                                }
                            }

                            if tx_rp_clone.is_some() {
                                acc_ptrs.push(pointer.clone());
                            }
                            if likely(bytes_mode) {
                                buffer.row.extend_serialized(&mut acc_bytes);
                            } else {
                                acc_rows.push(std::mem::take(&mut buffer.row));
                            }
                        }
                    } else {
                        // WHERE-filtered path
                        let (query_row_fetch, requested_row_fetch) =
                            &*tuple_clone;

                        for pointer in pointers.iter() {
                            if unlikely(stop_ref.load(std::sync::atomic::Ordering::Relaxed)) {
                                break;
                            }
                            if unlikely(pointer.deleted) {
                                continue;
                            }

                            if unlikely(pointer
                                .fetch_row_reuse_async(
                                    io_rows_ref,
                                    query_row_fetch,
                                    &mut buffer.row,
                                )
                                .await
                                .is_err())
                            {
                                error!(
                                    "Failed to fetch query row for pointer {:?}",
                                    pointer
                                );
                                continue;
                            }

                            if !plan_built {
                                let tmp_vec =
                                    unsafe { &mut *buffer.hashtable_buffer.get() };
                                tmp_vec.clear();

                                {
                                    let columns = &buffer.row.columns;
                                    let schema = &*schema_ref;

                                    tmp_vec.resize(
                                        schema.len(),
                                        (
                                            Cow::Borrowed(EMPTY_STR),
                                            MemoryBlock::default(),
                                        ),
                                    );

                                    for column in columns.iter() {
                                        let schema_field =
                                            &schema[column.schema_id as usize];
                                        let write_order =
                                            schema_field.write_order as usize;
                                        if let Some(entry) =
                                            tmp_vec.get_mut(write_order)
                                        {
                                            *entry = (
                                                Cow::Borrowed(
                                                    schema_field.name.as_str(),
                                                ),
                                                column.data.clone(),
                                            );
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

                                unsafe { &mut *processor_cell.get() }
                                    .replace_row_inputs(&[]);

                                build_write_order_map(
                                    &mut buffer.write_order_to_col,
                                    &buffer.row,
                                    schema_ref.as_slice(),
                                );

                                plan_built = true;
                            }

                            let result = {
                                let processor =
                                    unsafe { &mut *processor_cell.get() };
                                processor.execute_row(
                                    &RowView::new(
                                        &buffer.row,
                                        &buffer.write_order_to_col,
                                    ),
                                    &mut buffer.bool_buffer,
                                )
                            };

                            if result {
                                if !query_covers_requested {
                                    if let Err(e) = pointer
                                        .fetch_row_reuse_async(
                                            io_rows_ref,
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
                                }

                                if unlikely(enforce_limit_early) {
                                    if let Some(limit_val) = limit_opt {
                                        let prev = limit_counter_ref.fetch_add(
                                            1,
                                            std::sync::atomic::Ordering::Relaxed,
                                        );
                                        if unlikely(prev + 1 >= limit_val) {
                                            stop_ref.store(
                                                true,
                                                std::sync::atomic::Ordering::Relaxed,
                                            );
                                        }
                                        if unlikely(prev >= limit_val) {
                                            break;
                                        }
                                    }
                                }

                                if tx_rp_clone.is_some() {
                                    acc_ptrs.push(pointer.clone());
                                }
                                if likely(bytes_mode) {
                                    buffer.row.extend_serialized(&mut acc_bytes);
                                } else {
                                    acc_rows.push(std::mem::take(&mut buffer.row));
                                }
                            }
                        }
                    }

                    let elapsed_ns =
                        batch_read_start.elapsed().as_nanos() as u64;
                    reading_time_ref.fetch_add(
                        elapsed_ns,
                        std::sync::atomic::Ordering::Relaxed,
                    );
                }

                // Single send per worker on exit.
                if let Some(tx) = tx_rows_clone.as_ref() {
                    if !acc_rows.is_empty() {
                        let _ = tx.send(acc_rows).await;
                    }
                }
                if let Some(tx) = tx_bytes_clone.as_ref() {
                    if !acc_bytes.is_empty() {
                        let _ = tx.send(acc_bytes).await;
                    }
                }
                if let Some(ref tx_rp) = tx_rp_clone {
                    if !acc_ptrs.is_empty() {
                        let _ = tx_rp.send(acc_ptrs).await;
                    }
                }
            });

            worker_handles.push(h);
        }

        // Drop our copy of the receiver; once the iterator finishes and we
        // drop work_tx, only the workers' clones remain — when they finish
        // their last batch the channel closes and recv() returns Err, exiting
        // the loop.
        drop(work_rx);

        // Drive the iterator inline. No more per-batch task::spawn.
        iterator.reset();
        loop {
            if stop_processing.load(std::sync::atomic::Ordering::Relaxed) {
                break;
            }

            // Direct allocation; with BATCH_SIZE up to 65536 RowPointers per
            // batch, this is one alloc per ~thousands of rows — negligible.
            let mut pointers: Vec<RowPointer> =
                Vec::with_capacity(*BATCH_SIZE.get().unwrap());

            if iterator
                .next_row_pointers_into(&mut pointers)
                .await
                .is_err()
            {
                pointers.clear();
            }

            if pointers.is_empty() {
                break;
            }

            if stop_processing.load(std::sync::atomic::Ordering::Relaxed) {
                break;
            }

            // Bounded send: blocks the iterator when all workers are busy.
            if work_tx.send(pointers).await.is_err() {
                break;
            }
        }

        // Closing work_tx tells workers no more batches are coming.
        drop(work_tx);

        worker_handles
    }

    // Old per-batch spawning loop removed; kept signature-compatible.

    #[inline]
    fn drain_rows(rx: kanal::AsyncReceiver<Vec<Row>>) -> Vec<Row> {
        if rx.is_empty() {
            drop(rx);
            return Vec::new();
        }
        let mut chunks: Vec<Vec<Row>> = Vec::new();
        rx.drain_into(&mut chunks).unwrap();
        drop(rx);

        if chunks.is_empty() {
            return Vec::new();
        }

        // Take the largest chunk as the base. `Vec::append` moves bytes via
        // ptr::copy_nonoverlapping + length update, but the base chunk's own
        // bytes are NEVER copied — they stay in their original allocation.
        // With 8 worker chunks of ~equal size, this saves ~12.5% of the
        // total Row-struct memcpy traffic; with skewed chunks it saves more.
        let mut largest_idx = 0;
        let mut largest_len = 0usize;
        for (i, c) in chunks.iter().enumerate() {
            if c.len() > largest_len {
                largest_len = c.len();
                largest_idx = i;
            }
        }
        let mut collected_rows = chunks.swap_remove(largest_idx);
        let total: usize = collected_rows.len() + chunks.iter().map(|c| c.len()).sum::<usize>();
        collected_rows.reserve(total - collected_rows.len());
        for mut c in chunks {
            collected_rows.append(&mut c);
        }
        collected_rows
    }

    #[inline]
    fn drain_row_pointers(rx_rp: kanal::AsyncReceiver<Vec<RowPointer>>) -> Vec<RowPointer> {
        if rx_rp.is_empty() {
            drop(rx_rp);
            return Vec::new();
        }
        let mut chunks: Vec<Vec<RowPointer>> = Vec::new();
        rx_rp.drain_into(&mut chunks).unwrap();
        drop(rx_rp);

        if chunks.is_empty() {
            return Vec::new();
        }

        let mut largest_idx = 0;
        let mut largest_len = 0usize;
        for (i, c) in chunks.iter().enumerate() {
            if c.len() > largest_len {
                largest_len = c.len();
                largest_idx = i;
            }
        }
        let mut pointers = chunks.swap_remove(largest_idx);
        let total: usize = pointers.len() + chunks.iter().map(|c| c.len()).sum::<usize>();
        pointers.reserve(total - pointers.len());
        for mut c in chunks {
            pointers.append(&mut c);
        }
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

        let (tx, rx) = kanal::unbounded_async::<Vec<Row>>();

        let cache_enabled = *ENABLE_CACHE.get().unwrap();
        let crc64_hash = if cache_enabled {
            CRC_64.checksum(table_name.as_bytes()) ^ CRC_64.checksum(where_query.as_bytes())
        } else {
            0
        };

        // Only allocate the pointer channel when caching is on. When caching is off we
        // can avoid one channel allocation, an Arc clone per batch, an Arc clone per
        // matched row, and a `RowPointer::clone()` + `send().await` per matched row.
        let (tx_rp, rx_rp) = if cache_enabled {
            let (a, b) = kanal::unbounded_async::<Vec<RowPointer>>();
            (Some(a), Some(b))
        } else {
            (None, None)
        };

        // LIMIT enforcement: global counter across all tasks.
        // The increment/threshold check is performed inside the `if result { }` section.
        let limit_counter: Arc<CacheGuard<AtomicU64>> = Arc::new(AtomicU64::new(0).into());
        let stop_processing: Arc<CacheGuard<AtomicBool>> = Arc::new(AtomicBool::new(false).into());

        // Important: for ORDER BY queries, we must not stop early based on LIMIT,
        // otherwise we only sort a prefix of the matching rows.
        let enforce_limit_early = limit.is_some() && order_by.is_none();

        let shared_tuple: Arc<(RowFetch, RowFetch)> = Arc::new((
            query_row_fetch,
            requested_row_fetch,
        ));

        let table_schema = table_schema
            .iter()
            .cloned()
            .collect::<SmallVec<[SchemaField; 20]>>();

        let io_rows_outer = Arc::clone(&io_rows);

        let (no_filter, schema_arc, tokens_arc) = Self::prepare_query(where_query, &table_schema);

        // If the WHERE-evaluation columns already cover the SELECT columns, we can
        // skip the second (requested) fetch for matched rows and emit the WHERE buffer
        // directly. This is common for queries like `SELECT a, b WHERE a > 5 AND b < 10`.
        // Comparison is by `schema_id` (a unique key per schema field).
        let query_covers_requested = {
            let q = &shared_tuple.0.columns_fetching_data;
            let r = &shared_tuple.1.columns_fetching_data;
            !no_filter
                && r.iter().all(|rc| q.iter().any(|qc| qc.schema_id == rc.schema_id))
        };

        log::info!(
            "Preperation for query completed in {:.2?}",
            prep_stopwatch.elapsed()
        );

        let reading_time_total: Arc<CacheGuard<AtomicU64>> = Arc::new(AtomicU64::new(0).into());

        let batch_handles = Self::spawn_batch_tasks(
            iterator,
            Arc::clone(&shared_tuple),
            Arc::clone(&schema_arc),
            Arc::clone(&tokens_arc),
            io_rows_outer.clone(),
            Some(tx.clone()),
            None,
            tx_rp.clone(),
            limit,
            enforce_limit_early,
            no_filter,
            query_covers_requested,
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
            if let Some(rx_rp) = rx_rp {
                let pointers = Self::drain_row_pointers(rx_rp);

                // Store the resulting pointers in the cache
                let cache = ATOMIC_CACHE.get().unwrap();
                if cache.insert(crc64_hash, Arc::new(pointers)).is_err() {
                    log::error!("Failed to insert entries into cache for hash {}", crc64_hash);
                }
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

    /// Bytes-mode variant of [`Self::process`]. Workers serialize matched rows
    /// directly into per-worker `Vec<u8>` buffers (one alloc per worker, not
    /// per row), and the result is the concatenation of those buffers in
    /// undefined order. This skips the `Vec<Row>` accumulation, the merged
    /// `Vec<Row>` allocation in `drain_rows`, and the post-process
    /// `rows_into_vec` pass entirely.
    ///
    /// Falls back to row-mode internally when ORDER BY is requested (rows
    /// must be sorted before serialization).
    pub async fn process_into_bytes<'a, S: StorageIO>(
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
    ) -> Vec<u8> {
        // ORDER BY requires materialized Row structs to sort by column value;
        // fall back to the row-mode path and serialize at the end.
        if order_by.is_some() {
            let rows = self
                .process(
                    table_name,
                    where_query,
                    query_row_fetch,
                    requested_row_fetch,
                    table_schema,
                    io_rows,
                    iterator,
                    limit,
                    order_by,
                )
                .await;
            return crate::core::row::row::rows_into_vec(rows);
        }

        let prep_stopwatch = std::time::Instant::now();

        log::info!(
            "Starting concurrent bytes-mode processing for query: {}",
            where_query
        );

        // Cache fast-path (if enabled): serialize directly to wire bytes
        // without materializing a `Vec<Row>` or cloning rows.
        if let Some(bytes) = Self::try_collect_cached_bytes(
            table_name,
            where_query,
            &requested_row_fetch,
            &io_rows,
        )
        .await
        {
            return bytes;
        }

        let (tx_b, rx_b) = kanal::unbounded_async::<Vec<u8>>();

        let cache_enabled = *ENABLE_CACHE.get().unwrap();
        let crc64_hash = if cache_enabled {
            CRC_64.checksum(table_name.as_bytes()) ^ CRC_64.checksum(where_query.as_bytes())
        } else {
            0
        };

        let (tx_rp, rx_rp) = if cache_enabled {
            let (a, b) = kanal::unbounded_async::<Vec<RowPointer>>();
            (Some(a), Some(b))
        } else {
            (None, None)
        };

        let limit_counter: Arc<CacheGuard<AtomicU64>> = Arc::new(AtomicU64::new(0).into());
        let stop_processing: Arc<CacheGuard<AtomicBool>> = Arc::new(AtomicBool::new(false).into());
        let enforce_limit_early = limit.is_some();

        let shared_tuple: Arc<(RowFetch, RowFetch)> = Arc::new((
            query_row_fetch,
            requested_row_fetch,
        ));

        let table_schema_sv = table_schema
            .iter()
            .cloned()
            .collect::<SmallVec<[SchemaField; 20]>>();

        let io_rows_outer = Arc::clone(&io_rows);

        let (no_filter, schema_arc, tokens_arc) =
            Self::prepare_query(where_query, &table_schema_sv);

        let query_covers_requested = {
            let q = &shared_tuple.0.columns_fetching_data;
            let r = &shared_tuple.1.columns_fetching_data;
            !no_filter
                && r.iter().all(|rc| q.iter().any(|qc| qc.schema_id == rc.schema_id))
        };

        log::info!(
            "Preperation for bytes-mode query completed in {:.2?}",
            prep_stopwatch.elapsed()
        );

        let reading_time_total: Arc<CacheGuard<AtomicU64>> = Arc::new(AtomicU64::new(0).into());

        let batch_handles = Self::spawn_batch_tasks(
            iterator,
            Arc::clone(&shared_tuple),
            Arc::clone(&schema_arc),
            Arc::clone(&tokens_arc),
            io_rows_outer.clone(),
            None,
            Some(tx_b.clone()),
            tx_rp.clone(),
            limit,
            enforce_limit_early,
            no_filter,
            query_covers_requested,
            Arc::clone(&limit_counter),
            Arc::clone(&stop_processing),
            Arc::clone(&reading_time_total),
        )
        .await;

        join_all(batch_handles).await;
        drop(tx_b);

        // Drain bytes chunks; pick the largest as the base to avoid copying it.
        let mut chunks: Vec<Vec<u8>> = Vec::new();
        if !rx_b.is_empty() {
            let _ = rx_b.drain_into(&mut chunks);
        }
        drop(rx_b);

        let bytes = if chunks.is_empty() {
            Vec::new()
        } else {
            let mut largest_idx = 0;
            let mut largest_len = 0usize;
            for (i, c) in chunks.iter().enumerate() {
                if c.len() > largest_len {
                    largest_len = c.len();
                    largest_idx = i;
                }
            }
            let mut base = chunks.swap_remove(largest_idx);
            let total: usize = base.len() + chunks.iter().map(|c| c.len()).sum::<usize>();
            base.reserve(total - base.len());
            for c in chunks {
                base.extend_from_slice(&c);
            }
            base
        };

        if crc64_hash != 0 {
            if let Some(rx_rp) = rx_rp {
                let pointers = Self::drain_row_pointers(rx_rp);
                let cache = ATOMIC_CACHE.get().unwrap();
                if cache.insert(crc64_hash, Arc::new(pointers)).is_err() {
                    log::error!(
                        "Failed to insert entries into cache for hash {}",
                        crc64_hash
                    );
                }
            }
        }

        let total_reading_ns = reading_time_total.load(std::sync::atomic::Ordering::Relaxed);
        log::info!(
            "Total reading time for query '{}' : {:.2?} ({} bytes)",
            where_query,
            std::time::Duration::from_nanos(total_reading_ns),
            bytes.len()
        );

        bytes
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

        let shared_tuple: Arc<RowFetch> = Arc::new(query_row_fetch);
        let sem: StdArc<Semaphore> = StdArc::new(Semaphore::new(*MAX_PERMITS_THREADS.get().unwrap()));

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

            // Acquire batch permit BEFORE spawn for natural backpressure.
            let batch_permit = StdArc::clone(&sem).acquire_owned().await.unwrap();

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
                // Owned permit acquired in the outer loop; held until task completion.
                let _batch_permit = batch_permit;

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
                let io_rows_ref: &S = &*io_rows_batch;
                let batch_read_start = std::time::Instant::now();

                let mut buffer = Buffer {
                    hashtable_buffer: UnsafeCell::new(SmallVec::new()),
                    row,
                    transformers: SmallVec::new(),
                    intermediate_results: SmallVec::new(),
                    bool_buffer: SmallVec::new(),
                    write_order_to_col: SmallVec::new(),
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

                    let query_row_fetch = &*tuple_clone;

                    if pwo
                        .pointer
                        .fetch_row_reuse_async(io_rows_ref, query_row_fetch, &mut buffer.row)
                        .await
                        .is_err()
                    {
                        continue;
                    }

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
                        build_write_order_map(
                            &mut buffer.write_order_to_col,
                            &buffer.row,
                            schema_ref.as_slice(),
                        );
                        plan_built = true;
                    }

                    let result = {
                        let processor = unsafe { &mut *processor_cell.get() };
                        processor.execute_row(
                            &RowView::new(&buffer.row, &buffer.write_order_to_col),
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

                let elapsed_ns = batch_read_start.elapsed().as_nanos() as u64;
                reading_time_total_clone.fetch_add(
                    elapsed_ns,
                    std::sync::atomic::Ordering::Relaxed,
                );
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
    /// Precomputed map: write_order -> position in `row.columns`.
    /// `u8::MAX` denotes an absent column. Computed once per batch.
    pub write_order_to_col: SmallVec<[u8; 32]>,
}

// A zero-copy view over Row's MemoryBlocks for execution, indexed by schema write_order.
// This maps logical indices (write_order) used by the QueryParser/Transformers to the
// actual MemoryBlocks present in the fetched row (which may be a subset of columns).
struct RowView<'a> {
    row: &'a Row,
    /// Map from write_order -> position in `row.columns` (or `u8::MAX` if absent).
    /// Built once per batch, not per row.
    idx_map: &'a [u8],
}

impl<'a> RowView<'a> {
    #[inline]
    fn new(row: &'a Row, idx_map: &'a [u8]) -> Self {
        Self { row, idx_map }
    }
}

impl<'a> RowBlocks for RowView<'a> {
    #[inline]
    fn mb_at(&self, idx: usize) -> &MemoryBlock {
        // O(1) lookup via the precomputed map.
        let pos = unsafe { *self.idx_map.get_unchecked(idx) } as usize;
        debug_assert!(
            pos != u8::MAX as usize,
            "Requested column write_order {} not present in fetched row (fetched {} cols)",
            idx,
            self.row.columns.len()
        );
        unsafe { &self.row.columns.get_unchecked(pos).data }
    }

    #[inline]
    fn len(&self) -> usize {
        self.idx_map.len()
    }
}

/// Build / refresh the `write_order -> row column position` map for the columns currently
/// loaded in `row`. Cheap: O(N) where N is the number of fetched columns (typically 1-8).
#[inline]
fn build_write_order_map(
    map: &mut SmallVec<[u8; 32]>,
    row: &Row,
    schema: &[SchemaField],
) {
    map.clear();
    map.resize(schema.len(), u8::MAX);
    for (pos, col) in row.columns.iter().enumerate() {
        let schema_field = &schema[col.schema_id as usize];
        let wo = schema_field.write_order as usize;
        if wo < map.len() {
            map[wo] = pos as u8;
        }
    }
}