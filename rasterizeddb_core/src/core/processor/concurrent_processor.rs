use std::{borrow::Cow, cell::UnsafeCell, sync::atomic::{AtomicBool, AtomicU64, AtomicU8}};

use branches::{likely, unlikely};
use cacheguard::CacheGuard;
use log::error;
use rclite::Arc;
use std::sync::Arc as StdArc;

use crate::core::mvcc::Visibility;

use futures::{future::join_all};
use smallvec::SmallVec;

use tokio::{
    sync::Semaphore,
    task,
};

use super::{
    transformer::{ColumnTransformer, Next, TransformerPlan, TransformerProcessor},
};

use crate::{
    cache::query_cache, core::{
        processor::transformer::RowBlocks, row::{error::EvalError, row::{Row, RowFetch}, row_pointer::{RowPointer, RowPointerIterator}, schema::SchemaField}, storage_providers::traits::StorageIO, tokenizer::query_parser::QueryParser
    }, memory_pool::MemoryBlock
};

use crate::core::tokenizer::query_tokenizer::Token;

pub static EMPTY_STR: &str = "";

// The query cache proper lives in `crate::cache::query_cache`; these are the
// historical names its globals were reachable under.
pub use crate::cache::query_cache::{ENABLE_CACHE, QUERY_CACHE as ATOMIC_CACHE};

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


    /// Turns a finished worker fleet into a query-level error when something
    /// went wrong.
    ///
    /// Two failure modes are folded together here:
    ///
    ///  * a WHERE expression that is undefined for some row (`eval_error`), and
    ///  * a worker that panicked outright (`JoinError`).
    ///
    /// Both used to be discarded. Workers accumulate their matched rows locally
    /// and send them once at the end, so a worker that dies takes every row it
    /// had already matched with it — and `join_all`'s ignored `Result` meant the
    /// query then returned that truncated set as a success.
    fn check_worker_outcome(
        join_results: Vec<std::result::Result<(), tokio::task::JoinError>>,
        eval_error: &AtomicU8,
    ) -> std::io::Result<()> {
        if let Some(e) = EvalError::from_code(eval_error.load(std::sync::atomic::Ordering::Acquire))
        {
            return Err(e.into());
        }

        for r in join_results {
            if let Err(join_err) = r {
                error!("Query worker terminated abnormally: {}", join_err);
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("query worker terminated abnormally: {}", join_err),
                ));
            }
        }

        Ok(())
    }

    /// Row-mode cache fast-path.
    ///
    /// The cached set is every pointer the clause matched, so `ORDER BY` and
    /// `LIMIT` are applied here exactly as the scan path applies them — the
    /// entry is shared across every limit and ordering of the same clause.
    ///
    /// A pointer that will not read back means the entry describes storage
    /// that has moved. That is a miss, not a short answer: the whole entry is
    /// abandoned and the caller rescans. Skipping the row instead would return
    /// a silently truncated result set.
    async fn try_collect_cached_rows<S: StorageIO>(
        table_name: &str,
        where_query: &str,
        generation: Option<u64>,
        requested_row_fetch: &RowFetch,
        io_rows: &Arc<S>,
        schema_arc: &Arc<SmallVec<[SchemaField; 20]>>,
        limit: Option<u64>,
        order_by: &Option<String>,
        // Applied per pointer, because the cached set is by predicate and does
        // not know who is asking.
        visibility: &Visibility,
    ) -> Option<Vec<Row>> {
        let entry = query_cache::lookup(table_name, where_query, generation?, limit)?;

        let mut collected_rows = Vec::with_capacity(entry.pointers.len());
        let mut row = Row::default();

        for pointer in entry.pointers.iter() {
            // A cached entry holds the pointers matching the *predicate*, not
            // the ones a particular snapshot could see — that is what lets one
            // entry serve every snapshot. So visibility is applied here, per
            // pointer, and it must run **before** the fetch: reclaimed space is
            // reused, so fetch-then-check would read another row's bytes.
            //
            // `deleted` is passed rather than tested separately; `visible`
            // short-circuits on it, and that short-circuit is what covers the
            // reclamation branches that carry no `xmax`. See
            // `mvcc::visibility::reclamation_lemma`.
            if !visibility.visible(pointer.deleted, pointer.xmin, pointer.xmax) {
                continue;
            }

            if pointer
                .fetch_row_reuse_async(&**io_rows, requested_row_fetch, &mut row)
                .await
                .is_err()
            {
                query_cache::record_abandoned_hit();
                return None;
            }

            collected_rows.push(Row::clone_row(&row));
        }

        Self::sort_rows_if_needed(&mut collected_rows, schema_arc, order_by, limit);

        Some(collected_rows)
    }

    /// Bytes-mode cache fast-path. Serializes matched rows directly into a
    /// `Vec<u8>` at the wire format, skipping the `Vec<Row>` intermediate
    /// and the `Row::clone_row` per pointer.
    ///
    /// Only reached with no `ORDER BY` (that case is delegated to row mode),
    /// so `LIMIT` is the only shaping left to apply.
    async fn try_collect_cached_bytes<S: StorageIO>(
        table_name: &str,
        where_query: &str,
        generation: Option<u64>,
        requested_row_fetch: &RowFetch,
        io_rows: &Arc<S>,
        limit: Option<u64>,
        // Applied per pointer, because the cached set is by predicate and does
        // not know who is asking.
        visibility: &Visibility,
    ) -> Option<Vec<u8>> {
        let entry = query_cache::lookup(table_name, where_query, generation?, limit)?;

        // Conservative initial guess; extend_serialized will grow as needed.
        let mut out: Vec<u8> = Vec::with_capacity(entry.pointers.len().saturating_mul(64));
        let mut row = Row::default();
        let mut emitted: u64 = 0;

        for pointer in entry.pointers.iter() {
            if let Some(limit_val) = limit {
                if emitted >= limit_val {
                    break;
                }
            }

            // See `try_collect_cached_rows` for why the slot flag and the
            // visibility rule are both needed here.
            if !visibility.visible(pointer.deleted, pointer.xmin, pointer.xmax) {
                continue;
            }

            // See `try_collect_cached_rows`: an unreadable pointer invalidates
            // the entry rather than shortening the answer.
            if pointer
                .fetch_row_reuse_async(&**io_rows, requested_row_fetch, &mut row)
                .await
                .is_err()
            {
                query_cache::record_abandoned_hit();
                return None;
            }

            row.extend_serialized(&mut out);
            emitted += 1;
        }

        Some(out)
    }

    /// The schema in the shape the workers and the cache fast-path both want.
    ///
    /// Built before the cache is consulted, because serving a cached answer
    /// still needs the schema to apply `ORDER BY`.
    #[inline]
    fn schema_arc(table_schema: &[SchemaField]) -> Arc<SmallVec<[SchemaField; 20]>> {
        Arc::new(table_schema.iter().cloned().collect())
    }

    #[inline]
    /// Tokenizes the predicate, or says why it cannot.
    ///
    /// The error is returned rather than raised: a clause naming something that
    /// is not a column is a client mistake, and it used to panic the worker and
    /// drop the connection with nothing reported.
    pub(crate) fn prepare_query(
        where_query: &str,
        schema_arc: &Arc<SmallVec<[SchemaField; 20]>>,
    ) -> Result<(bool, Arc<SmallVec<[Token; 36]>>), String> {
        // Empty WHERE means "match all rows"; skip tokenization/parsing entirely.
        let no_filter = where_query.trim().is_empty();
        let tokens: SmallVec<[Token; 36]> = if no_filter {
            SmallVec::new()
        } else {
            crate::core::tokenizer::query_tokenizer::try_tokenize(where_query, schema_arc)?
        };

        Ok((no_filter, Arc::new(tokens)))
    }

    #[inline]
    fn build_plan_columns<'a>(
        schema: &'a [SchemaField],
    ) -> SmallVec<[(Cow<'a, str>, MemoryBlock); 20]> {
        let max_write_order = schema
            .iter()
            .map(|field| field.write_order as usize)
            .max()
            .unwrap_or(0);

        let mut columns = SmallVec::new();
        columns.resize_with(max_write_order.saturating_add(1), || {
            (Cow::Borrowed(EMPTY_STR), MemoryBlock::default())
        });

        for field in schema {
            columns[field.write_order as usize] = (
                Cow::Borrowed(field.name.as_str()),
                MemoryBlock::default(),
            );
        }

        columns
    }

    /// Compiles the `WHERE` clause into a transformer plan.
    ///
    /// Returns the compile error rather than panicking on it. It used to
    /// `panic!`, which unwound the tokio task running the connection — so a
    /// client that sent an unsupported operator did not get an error, it got
    /// its **connection dropped** ("SSL error: unexpected eof"). A statement the
    /// engine cannot compile has to be answerable; killing the session is a
    /// denial of service triggerable by one malformed query.
    pub(crate) fn compile_query_plan(
        schema_arc: &Arc<SmallVec<[SchemaField; 20]>>,
        tokens_arc: &Arc<SmallVec<[Token; 36]>>,
    ) -> Result<TransformerPlan, String> {
        let plan_columns = Self::build_plan_columns(schema_arc.as_slice());
        let mut transformers: SmallVec<[ColumnTransformer; 36]> = SmallVec::new();
        let mut intermediate_results: SmallVec<[MemoryBlock; 20]> = SmallVec::new();
        let processor_cell = UnsafeCell::new(TransformerProcessor::new(
            &mut transformers,
            &mut intermediate_results,
        ));

        {
            let processor_mut = unsafe { &mut *processor_cell.get() };
            let mut parser = QueryParser::new(tokens_arc.as_ref(), processor_mut);
            if let Err(e) = parser.execute(&plan_columns) {
                return Err(format!("could not compile the query plan: {}", e));
            }
        }

        Ok(unsafe { &*processor_cell.get() }.snapshot_plan())
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
        compiled_plan: Option<Arc<TransformerPlan>>,
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
        // First `EvalError` any worker hit, as `EvalError::to_code` (0 = none).
        // A WHERE expression that is undefined for some row must fail the whole
        // query; letting workers skip the row would return a short result set
        // that is indistinguishable from a complete one.
        eval_error: Arc<CacheGuard<AtomicU8>>,
        // Which row versions the workers may emit.
        visibility: Visibility,
    ) -> Vec<tokio::task::JoinHandle<()>> {
        // As many workers as there is work to hand them, never more.
        //
        // This used to spawn `max_permits_threads()` — 16 by default — for
        // every scan whatever its size. Pointers are dispatched a batch at a
        // time and a batch is 10,000 of them, so a table under that size is a
        // single batch: one worker took it and the other fifteen were spawned,
        // parked on an empty channel, and joined again having done nothing.
        //
        // Measured on a table of one row, that overhead is the whole cost of
        // the query — a `COUNT(*)` over it took ~0.46 ms against ~0.15 ms for
        // `SELECT 1`. It is worse than it looks in a serial benchmark: a page
        // that fans out dozens of small queries at once multiplies the idle
        // spawns together and they contend for the same runtime as the workers
        // actually doing the reading.
        //
        // Large scans are unaffected: once there are at least as many batches
        // as threads this is exactly what it was.
        let batches = iterator
            .total_records()
            .div_ceil(crate::batch_size().max(1) as u64)
            .max(1);
        let n_workers = crate::max_permits_threads().min(batches as usize).max(1);

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
            let compiled_plan_ref = compiled_plan.as_ref().map(Arc::clone);
            let io_rows_batch = Arc::clone(&io_rows_outer);
            let limit_counter_ref = Arc::clone(&limit_counter);
            let stop_ref = Arc::clone(&stop_processing);
            let reading_time_ref = Arc::clone(&reading_time_total);
            let eval_error_ref = Arc::clone(&eval_error);
            let limit_opt = limit;
            let visibility_ref = visibility.clone();
            // Sampled once per worker rather than per row: it is a bound, not a
            // reading, so a later sample can only be more permissive and the
            // scan stays consistent with itself either way.
            let horizon_ref = crate::core::vacuum::VersionHorizon::for_visibility(&visibility);

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
                if !no_filter {
                    let plan = compiled_plan_ref
                        .as_ref()
                        .expect("compiled plan required for filtered query");
                    processor_mut.load_plan(plan.as_ref());
                }

                // Row layout for fetched WHERE columns is stable within a query.
                let mut row_layout_built = false;

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
                            // Only slot vacancy gates the scan. Visibility is
                            // applied later, at the point a row is *emitted*,
                            // so a pointer that matches the predicate but is not
                            // visible to this snapshot still reaches the cache.
                            //
                            // That is what makes the cache sound across
                            // snapshots: the cached set is "pointers matching
                            // this WHERE clause", which does not depend on who
                            // is asking. Caching the *visible* set instead made
                            // an entry answerable only by the snapshot that
                            // built it — and since a commit changes visibility
                            // without writing anything, the generation counter
                            // could not tell when it had gone stale. The whole
                            // cache was therefore switched off under MVCC, which
                            // meant every repeat of a query re-scanned from
                            // slot 0.
                            if unlikely(pointer.deleted) {
                                continue;
                            }
                            // Cached whether or not this snapshot may see it.
                            if tx_rp_clone.is_some() {
                                acc_ptrs.push(pointer.clone());
                            }
                            // The visibility test runs *before* any fetch, and
                            // that ordering is load-bearing rather than
                            // stylistic: reclaimed space is reused, so
                            // fetch-then-check would read another row's bytes
                            // and only then discard them — one refactor away
                            // from returning them. See
                            // `mvcc::visibility::reclamation_lemma`.
                            if unlikely(!visibility_ref.visible(
                                pointer.deleted,
                                pointer.xmin,
                                pointer.xmax,
                            )) {
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
                            // See the note in the unfiltered branch: the
                            // predicate still has to be evaluated for a row this
                            // snapshot cannot see, because the cached set is by
                            // predicate. So this cannot gate on `visible`.
                            if unlikely(pointer.deleted) {
                                continue;
                            }

                            // But it must gate on *reclaimable*, which is the
                            // strictly weaker test and the one this branch was
                            // missing. `deleted` alone is no gate at all under
                            // MVCC: `RowPointer::delete` stamps `xmax` and
                            // deliberately leaves `deleted` false for a versioned
                            // delete, and nothing sets it afterwards — so every
                            // transactionally-deleted row reached the fetch
                            // below. Once reclamation freed its extent and an
                            // insert reused it, this read returned another row's
                            // bytes; in production it surfaced as
                            // "Invalid string size" on pointers that all had
                            // `xmax` set.
                            //
                            // Skipping here loses nothing: by
                            // `mvcc::visibility::reclamation_lemma` a reclaimable
                            // version is invisible to every snapshot that can
                            // still be taken, so no cache entry built from this
                            // scan could ever need to serve it.
                            if unlikely(horizon_ref.is_reclaimable(
                                pointer.deleted,
                                pointer.xmin,
                                pointer.xmax,
                            )) {
                                continue;
                            }

                            // The reason is logged, not discarded: this fires in
                            // bursts on a hot row and the pointer alone says
                            // nothing about whether the read was short, the
                            // layout disagreed, or the slot had been reused.
                            if let Err(e) = pointer
                                .fetch_row_reuse_async(
                                    io_rows_ref,
                                    query_row_fetch,
                                    &mut buffer.row,
                                )
                                .await
                            {
                                error!(
                                    "Failed to fetch query row for pointer {:?}: {}",
                                    pointer, e
                                );
                                continue;
                            }

                            if !row_layout_built {
                                build_write_order_map(
                                    &mut buffer.write_order_to_col,
                                    &buffer.row,
                                    schema_ref.as_slice(),
                                );
                                row_layout_built = true;
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

                            let result = match result {
                                Ok(matched) => matched,
                                Err(e) => {
                                    // Record the first failure and stop every
                                    // worker. Continuing would emit whatever
                                    // this worker matched so far as if the
                                    // query had succeeded.
                                    let _ = eval_error_ref.compare_exchange(
                                        0,
                                        e.to_code(),
                                        std::sync::atomic::Ordering::AcqRel,
                                        std::sync::atomic::Ordering::Acquire,
                                    );
                                    stop_ref.store(
                                        true,
                                        std::sync::atomic::Ordering::Relaxed,
                                    );
                                    break;
                                }
                            };

                            if result {
                                // Matches the predicate, so it belongs in the
                                // cached set regardless of who can see it.
                                if tx_rp_clone.is_some() {
                                    acc_ptrs.push(pointer.clone());
                                }
                                // The visibility test runs *before* any fetch, and
                                // that ordering is load-bearing rather than
                                // stylistic: reclaimed space is reused, so
                                // fetch-then-check would read another row's bytes
                                // and only then discard them — one refactor away
                                // from returning them. See
                                // `mvcc::visibility::reclamation_lemma`.
                                if unlikely(!visibility_ref.visible(
                                    pointer.deleted,
                                    pointer.xmin,
                                    pointer.xmax,
                                )) {
                                    continue;
                                }

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
                Vec::with_capacity(crate::batch_size());

            // NOTE: a failed pointer read is silently treated as the end of
            // the table here — the batch is cleared and the `is_empty` break
            // below ends the scan, so a truncated result is returned as a
            // success. The bytes-mode dispatcher in `process_row_pointers`
            // now propagates instead; doing the same here needs an error
            // channel out of `spawn_batch_tasks`, which returns handles
            // rather than a `Result`.
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

    /// Orders `all_rows` for `ORDER BY`, applying `limit` as part of the same
    /// step rather than after it.
    ///
    /// A full sort is the wrong algorithm for `ORDER BY … LIMIT k`. The answer
    /// only needs the `k` smallest rows, which is a *selection* problem —
    /// `select_nth_unstable_by` partitions in O(n) and then only those `k` are
    /// sorted, against O(n log n) to order a million rows and throw all but two
    /// hundred of them away. On a 1.18M-row table with `LIMIT 200` that is
    /// roughly 1.2M comparisons instead of 23M.
    ///
    /// It does not reduce what the scan *reads*: `enforce_limit_early` is off
    /// whenever `ORDER BY` is present, because the first `k` rows in scan order
    /// are not the first `k` in sort order. Only an index on the sort column
    /// removes that, and there isn't one.
    ///
    /// One observable difference, and it is within what SQL allows: `sort_by`
    /// is stable while `select_nth_unstable_by` is not, so which of several
    /// *equal* rows survives the limit may change. `ORDER BY` leaves the order
    /// of tied rows unspecified, so this is a licence the language grants —
    /// but a client relying on the previous incidental stability would notice.
    fn sort_rows_if_needed(
        all_rows: &mut Vec<Row>,
        schema_arc: &Arc<SmallVec<[SchemaField; 20]>>,
        order_by: &Option<String>,
        limit: Option<u64>,
    ) {
        // ORDER BY: after collection, sort the resulting rows.
        // NOTE: Row/Column schema_id uses SchemaField.write_order (not vector index).
        let truncate_to = limit.map(|l| l as usize);
        let Some(order_spec) = order_by else {
            // No ordering, but the cap still applies.
            if let Some(k) = truncate_to {
                all_rows.truncate(k);
            }
            return;
        };

        // The spec is a comma-separated list of column names, each optionally
        // followed by ASC or DESC. Carrying the keys inside the existing string
        // keeps every signature between the SQL front end and here unchanged;
        // only this function needs to know the sort has keys and directions.
        let mut keys: Vec<(usize, bool)> = Vec::new();
        for (name, descending) in crate::core::rql::order_by::parse_spec(order_spec) {
            // Tombstones excluded: ordering by a dropped column's write_order
            // sorts by a column no fetch includes, which now reads as absent
            // rather than as another column's bytes — an unordered result
            // reported as ordered.
            match schema_arc
                .iter()
                .find(|f| !f.is_deleted && f.name.eq_ignore_ascii_case(&name))
                .map(|f| f.write_order as usize)
            {
                Some(schema_id) => keys.push((schema_id, descending)),
                // The planner resolved every key against this same schema, so
                // this is unreachable in practice. Dropping just the one key
                // still sorts by the rest, which beats abandoning the sort.
                None => log::warn!("ORDER BY field '{}' not found in schema; skipping key", name),
            }
        }

        if keys.is_empty() {
            // Nothing resolvable to sort by; the cap still applies.
            if let Some(k) = truncate_to {
                all_rows.truncate(k);
            }
            return;
        }

        // Shared by both paths so the partition and the final sort cannot
        // disagree about the order they are producing.
        let compare = |a: &Row, b: &Row| -> std::cmp::Ordering {
            // Later keys only break the ties the earlier ones left.
            for (schema_id, descending) in keys.iter() {
                let ord = compare_rows_by_schema_id(a, b, *schema_id, schema_arc.as_slice());
                let ord = if *descending { ord.reverse() } else { ord };
                if ord != std::cmp::Ordering::Equal {
                    return ord;
                }
            }
            std::cmp::Ordering::Equal
        };

        match truncate_to {
            // Select, then order only the survivors.
            Some(k) if k < all_rows.len() => {
                if k == 0 {
                    all_rows.clear();
                    return;
                }
                // Puts the `k` smallest in `[..k]` in unspecified order, in
                // O(n) rather than sorting everything first.
                all_rows.select_nth_unstable_by(k - 1, compare);
                all_rows.truncate(k);
                all_rows.sort_by(compare);
            }
            // Asking for at least as many rows as exist: the selection step
            // would be pure overhead.
            _ => all_rows.sort_by(compare),
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
        // `generation` is the table's cache generation, read before this scan
        // began. `None` opts the query out of the cache entirely — used by
        // callers with no table handle to read a generation from, whose results
        // must never be mistaken for a user query against the same clause.
        generation: Option<u64>,
        // Which row versions this query may see.
        visibility: Visibility,
    ) -> std::io::Result<Vec<Row>> {
        let prep_stopwatch = std::time::Instant::now();

        log::info!(
            "Starting concurrent processing for query: {}",
            where_query
        );

        let schema_arc = Self::schema_arc(table_schema);

        // Cache fast-path (if enabled)
        if let Some(rows) = Self::try_collect_cached_rows(
            table_name,
            where_query,
            generation,
            &requested_row_fetch,
            &io_rows,
            &schema_arc,
            limit,
            &order_by,
            &visibility,
        )
        .await
        {
            return Ok(rows);
        }

        let (tx, rx) = kanal::unbounded_async::<Vec<Row>>();

        let cache_enabled = query_cache::enabled() && generation.is_some();

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
        let eval_error: Arc<CacheGuard<AtomicU8>> = Arc::new(AtomicU8::new(0).into());

        // Important: for ORDER BY queries, we must not stop early based on LIMIT,
        // otherwise we only sort a prefix of the matching rows.
        let enforce_limit_early = limit.is_some() && order_by.is_none();

        let shared_tuple: Arc<(RowFetch, RowFetch)> = Arc::new((
            query_row_fetch,
            requested_row_fetch,
        ));

        let io_rows_outer = Arc::clone(&io_rows);

        let (no_filter, tokens_arc) = Self::prepare_query(where_query, &schema_arc)
            .map_err(std::io::Error::other)?;
        let compiled_plan = if no_filter {
            None
        } else {
            Some(Arc::new(
                Self::compile_query_plan(&schema_arc, &tokens_arc).map_err(|e| {
                    std::io::Error::new(std::io::ErrorKind::InvalidInput, e)
                })?,
            ))
        };

        // If the WHERE-evaluation columns already cover the SELECT columns, we can
        // skip the second (requested) fetch for matched rows and emit the WHERE buffer
        // directly. This is common for queries like `SELECT a, b WHERE a > 5 AND b < 10`.
        // Comparison is by `schema_id` (a unique key per schema field).
        let query_covers_requested = {
            let q = &shared_tuple.0.columns_fetching_data;
            let r = &shared_tuple.1.columns_fetching_data;
            // The optimisation emits the WHERE buffer *as it is*, so the
            // requested columns must already sit at its front in the requested
            // order. Testing only for set inclusion was wrong: the WHERE
            // columns are gathered through a `HashSet`, so their order varies
            // run to run, and a covered query could return its columns
            // transposed — `SELECT a, b` answered with `b, a`.
            !no_filter
                && q.len() >= r.len()
                && r.iter()
                    .zip(q.iter())
                    .all(|(rc, qc)| qc.schema_id == rc.schema_id)
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
            compiled_plan.clone(),
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
            Arc::clone(&eval_error),
            visibility.clone(),
        )
        .await;

        // Wait for all batch tasks to complete
        let join_results = join_all(batch_handles).await;

        // Drop the sender to signal that no more rows will be sent
        drop(tx);

        Self::check_worker_outcome(join_results, &eval_error)?;

        let mut all_rows = Self::drain_rows(rx);

        // The cap is applied inside, as part of the ordering: `ORDER BY … LIMIT k`
        // is a selection problem, not a sort-then-discard one.
        Self::sort_rows_if_needed(&mut all_rows, &schema_arc, &order_by, limit);

        if let Some(rx_rp) = rx_rp {
            let pointers = Self::drain_row_pointers(rx_rp);

            // A scan that stopped on LIMIT saw only a prefix of the matches,
            // so it is stored under a key that includes that limit. Storing it
            // under the bare clause would answer the *unlimited* form with a
            // truncated set; keying it separately means the prefix can still
            // serve repeats of the same `WHERE ... LIMIT n`, which is the shape
            // a paging client sends over and over.
            //
            // Before this, such a query was simply never cached, so every
            // repeat re-ran the whole scan — including the leading stretch of
            // non-matching rows, which for a selective-late predicate is the
            // entire cost.
            if let Some(generation) = generation {
                let stopped_early =
                    stop_processing.load(std::sync::atomic::Ordering::Acquire);
                if stopped_early {
                    // Only a prefix, and only meaningful if we know its length.
                    if limit.is_some() {
                        query_cache::store(
                            table_name,
                            where_query,
                            generation,
                            limit,
                            pointers,
                        );
                    }
                } else {
                    // Ran to completion: a complete match set, which answers
                    // any limit by truncation.
                    query_cache::store(table_name, where_query, generation, None, pointers);
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

        Ok(all_rows)
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
        // See [`Self::process`].
        generation: Option<u64>,
        visibility: Visibility,
    ) -> std::io::Result<Vec<u8>> {
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
                    generation,
                    visibility,
                )
                .await?;
            return Ok(crate::core::row::row::rows_into_vec(rows));
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
            generation,
            &requested_row_fetch,
            &io_rows,
            limit,
            &visibility,
        )
        .await
        {
            return Ok(bytes);
        }

        let (tx_b, rx_b) = kanal::unbounded_async::<Vec<u8>>();

        let cache_enabled = query_cache::enabled() && generation.is_some();

        let (tx_rp, rx_rp) = if cache_enabled {
            let (a, b) = kanal::unbounded_async::<Vec<RowPointer>>();
            (Some(a), Some(b))
        } else {
            (None, None)
        };

        let limit_counter: Arc<CacheGuard<AtomicU64>> = Arc::new(AtomicU64::new(0).into());
        let stop_processing: Arc<CacheGuard<AtomicBool>> = Arc::new(AtomicBool::new(false).into());
        let eval_error: Arc<CacheGuard<AtomicU8>> = Arc::new(AtomicU8::new(0).into());
        let enforce_limit_early = limit.is_some();

        let shared_tuple: Arc<(RowFetch, RowFetch)> = Arc::new((
            query_row_fetch,
            requested_row_fetch,
        ));

        let schema_arc = Self::schema_arc(table_schema);

        let io_rows_outer = Arc::clone(&io_rows);

        let (no_filter, tokens_arc) = Self::prepare_query(where_query, &schema_arc)
            .map_err(std::io::Error::other)?;
        let compiled_plan = if no_filter {
            None
        } else {
            Some(Arc::new(
                Self::compile_query_plan(&schema_arc, &tokens_arc).map_err(|e| {
                    std::io::Error::new(std::io::ErrorKind::InvalidInput, e)
                })?,
            ))
        };

        let query_covers_requested = {
            let q = &shared_tuple.0.columns_fetching_data;
            let r = &shared_tuple.1.columns_fetching_data;
            // The optimisation emits the WHERE buffer *as it is*, so the
            // requested columns must already sit at its front in the requested
            // order. Testing only for set inclusion was wrong: the WHERE
            // columns are gathered through a `HashSet`, so their order varies
            // run to run, and a covered query could return its columns
            // transposed — `SELECT a, b` answered with `b, a`.
            !no_filter
                && q.len() >= r.len()
                && r.iter()
                    .zip(q.iter())
                    .all(|(rc, qc)| qc.schema_id == rc.schema_id)
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
            compiled_plan.clone(),
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
            Arc::clone(&eval_error),
            visibility.clone(),
        )
        .await;

        let join_results = join_all(batch_handles).await;
        drop(tx_b);

        Self::check_worker_outcome(join_results, &eval_error)?;

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

        if let Some(rx_rp) = rx_rp {
            let pointers = Self::drain_row_pointers(rx_rp);

            // See `process`: a prefix is keyed by its limit, a complete set
            // by the bare clause.
            if let Some(generation) = generation {
                let stopped_early =
                    stop_processing.load(std::sync::atomic::Ordering::Acquire);
                if stopped_early {
                    // Only a prefix, and only meaningful if we know its length.
                    if limit.is_some() {
                        query_cache::store(
                            table_name,
                            where_query,
                            generation,
                            limit,
                            pointers,
                        );
                    }
                } else {
                    // Ran to completion: a complete match set, which answers
                    // any limit by truncation.
                    query_cache::store(table_name, where_query, generation, None, pointers);
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

        Ok(bytes)
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
        // Which row versions this statement may see. `Visibility::All` is the
        // unversioned rule and admits every untombstoned row.
        visibility: Visibility,
    ) -> std::io::Result<Vec<RowPointerWithOffset>> {
        let prep_stopwatch = std::time::Instant::now();

        log::info!(
            "Starting concurrent pointer processing for query: {}",
            where_query
        );

        let (tx, rx) = kanal::unbounded_async::<RowPointerWithOffset>();

        let limit_counter = Arc::new(AtomicU64::new(0));
        let stop_processing = Arc::new(AtomicBool::new(false));
        let eval_error: Arc<CacheGuard<AtomicU8>> = Arc::new(AtomicU8::new(0).into());
        // A WHERE clause the planner refuses. Reported through the result
        // rather than by panicking: the panic unwound the task running the
        // statement and the connection with it, where the SELECT path returns
        // the reason — see `compile_query_plan`.
        let plan_error: Arc<parking_lot::Mutex<Option<String>>> =
            Arc::new(parking_lot::Mutex::new(None));

        let shared_tuple: Arc<RowFetch> = Arc::new(query_row_fetch);
        let sem: StdArc<Semaphore> = StdArc::new(Semaphore::new(crate::max_permits_threads()));

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
            crate::core::tokenizer::query_tokenizer::try_tokenize(&where_query, &table_schema)
                .map_err(std::io::Error::other)?
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

        // Set when a pointer read fails, and returned after the workers are
        // joined so the scan reports the failure instead of a short answer.
        let mut pointer_read_error: Option<crate::core::row::error::RowError> = None;

        loop {
            if stop_processing.load(std::sync::atomic::Ordering::Relaxed) {
                break;
            }

            let mut pointers: Vec<RowPointerWithOffset> = Vec::with_capacity(crate::batch_size());

            for _ in 0..crate::batch_size() {
                match iterator.next_row_pointer_with_offset().await {
                    Ok(Some((pointer_record_pos, pointer))) => {
                        pointers.push(RowPointerWithOffset {
                            pointer_record_pos,
                            pointer,
                        });
                    }
                    Ok(None) => break,
                    // As in the row-mode dispatcher: a read failure must not
                    // be mistaken for the end of the table.
                    Err(e) => {
                        pointer_read_error = Some(e);
                        stop_processing.store(true, std::sync::atomic::Ordering::Relaxed);
                        break;
                    }
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
            let eval_error_ref = Arc::clone(&eval_error);
            let plan_error_ref = Arc::clone(&plan_error);
            // Cheap: the rule is an enum holding at most two `Arc`s, and each
            // batch needs its own so the task can own it.
            let visibility_ref = visibility.clone();

            let batch_handle = task::spawn(async move {
                // Owned permit acquired in the outer loop; held until task completion.
                let _batch_permit = batch_permit;

                // Fast-path for empty WHERE: accept all non-deleted pointers.
                if no_filter {
                    for pwo in pointers.into_iter() {
                        if stop_ref.load(std::sync::atomic::Ordering::Relaxed) {
                            break;
                        }

                        // The visibility test runs *before* any fetch, and
                        // that ordering is load-bearing rather than
                        // stylistic: reclaimed space is reused, so
                        // fetch-then-check would read another row's bytes
                        // and only then discard them — one refactor away
                        // from returning them. See
                        // `mvcc::visibility::reclamation_lemma`.
                        if !visibility_ref.visible(
                            pwo.pointer.deleted,
                            pwo.pointer.xmin,
                            pwo.pointer.xmax,
                        ) {
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
                                // `<`, not `<=`: the counter is the number of rows already
                                // claimed, so admitting `cur == limit` claimed one too many
                                // and `DELETE … LIMIT 100` removed 101.
                                |cur| if cur < limit_val { Some(cur + 1) } else { None },
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

                    // The visibility test runs *before* any fetch, and
                    // that ordering is load-bearing rather than
                    // stylistic: reclaimed space is reused, so
                    // fetch-then-check would read another row's bytes
                    // and only then discard them — one refactor away
                    // from returning them. See
                    // `mvcc::visibility::reclamation_lemma`.
                    if !visibility_ref.visible(
                        pwo.pointer.deleted,
                        pwo.pointer.xmin,
                        pwo.pointer.xmax,
                    ) {
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
                            plan_error_ref.lock().get_or_insert(e);
                            stop_ref.store(true, std::sync::atomic::Ordering::Relaxed);
                            return;
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

                    let result = match result {
                        Ok(matched) => matched,
                        Err(e) => {
                            let _ = eval_error_ref.compare_exchange(
                                0,
                                e.to_code(),
                                std::sync::atomic::Ordering::AcqRel,
                                std::sync::atomic::Ordering::Acquire,
                            );
                            stop_ref.store(true, std::sync::atomic::Ordering::Relaxed);
                            break;
                        }
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
                                // `<`, not `<=`: the counter is the number of rows already
                                // claimed, so admitting `cur == limit` claimed one too many
                                // and `DELETE … LIMIT 100` removed 101.
                                |cur| if cur < limit_val { Some(cur + 1) } else { None },
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

        let join_results = join_all(batch_handles).await;
        drop(tx);

        Self::check_worker_outcome(join_results, &eval_error)?;
        // A pointer read that failed mid-scan is reported rather than being
        // left to look like the end of the table.
        if let Some(e) = pointer_read_error {
            return Err(std::io::Error::other(e.to_string()));
        }
        if let Some(reason) = plan_error.lock().take() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("could not compile the WHERE clause: {}", reason),
            ));
        }

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

        Ok(matches)
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

pub(crate) fn compare_bytes_as_type(db_type: crate::core::db_type::DbType, a: &[u8], b: &[u8]) -> std::cmp::Ordering {
    use crate::core::db_type::DbType;

    match db_type {
        // Variable-width values order by their bytes. For an array that is not
        // a meaningful order, but it is stable, which is what a sort needs.
        // `JSON` is reporting-only and never reaches storage, but a match has
        // to be exhaustive and a byte compare is the safe answer.
        // `JSONB` compares by its encoded bytes, which is a *stable* order and
        // not the one PostgreSQL defines for `jsonb`. Enough for a sort or a
        // join key; a query that needs jsonb's own ordering does not have it.
        DbType::STRING | DbType::BYTEA | DbType::ARRAY | DbType::UNKNOWN | DbType::START
        | DbType::END | DbType::JSON | DbType::JSONB => a.cmp(b),
        // Fixed-width and big-endian by construction, so a plain byte compare
        // already yields canonical UUID order and address order.
        DbType::UUID | DbType::INET => a.cmp(b),
        DbType::CHAR => a.cmp(b),
        // Microseconds since the Unix epoch, stored little-endian. Comparing the
        // raw bytes would order by the low byte first, and would sort negative
        // (pre-1970) instants above positive ones.
        DbType::DATETIME => read_num::<8, i64>(a, b, i64::from_le_bytes),
        // Fixed-scale unscaled units, so the integer order is the numeric order.
        DbType::DECIMAL => read_num::<16, i128>(a, b, i128::from_le_bytes),
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
pub(crate) struct RowView<'a> {
    row: &'a Row,
    /// Map from write_order -> position in `row.columns` (or `u8::MAX` if absent).
    /// Built once per batch, not per row.
    idx_map: &'a [u8],
}

impl<'a> RowView<'a> {
    #[inline]
    pub(crate) fn new(row: &'a Row, idx_map: &'a [u8]) -> Self {
        Self { row, idx_map }
    }
}

/// What a `RowView` yields for a column the fetch did not include.
///
/// Empty is already this engine's stand-in for NULL — `check_unique` skips an
/// empty value on exactly that basis — so an absent column reads as absent
/// rather than as another column's bytes.
fn absent_column() -> &'static MemoryBlock {
    static ABSENT: std::sync::OnceLock<MemoryBlock> = std::sync::OnceLock::new();
    ABSENT.get_or_init(MemoryBlock::default)
}

impl<'a> RowBlocks for RowView<'a> {
    #[inline]
    fn mb_at(&self, idx: usize) -> &MemoryBlock {
        // Both of these were `get_unchecked`, and both were reachable.
        //
        // `idx` is a write_order and `idx_map` was sized by the *field count*,
        // so a write_order past that count read out of bounds — the same
        // mismatch that panicked a pgwire worker, except unchecked here, so
        // undefined behaviour rather than a crash.
        //
        // `u8::MAX` is the "column not in this fetch" sentinel, and the only
        // thing between it and `columns.get_unchecked(255)` was a
        // `debug_assert!` — compiled out of the release build we ship. Any
        // transformer asking for a column the fetch did not include hit that,
        // needing neither a projection nor a gap in the write orders. Whoever
        // wrote the assert knew the case was possible.
        //
        // Two bounds checks against an O(1) map lookup is not a cost worth
        // undefined behaviour.
        let pos = self.idx_map.get(idx).copied().unwrap_or(u8::MAX) as usize;
        if pos == u8::MAX as usize {
            return absent_column();
        }
        match self.row.columns.get(pos) {
            Some(column) => &column.data,
            None => absent_column(),
        }
    }

    #[inline]
    fn len(&self) -> usize {
        self.idx_map.len()
    }
}

/// Build / refresh the `write_order -> row column position` map for the columns currently
/// loaded in `row`. Cheap: O(N) where N is the number of fetched columns (typically 1-8).
#[inline]
pub(crate) fn build_write_order_map(
    map: &mut SmallVec<[u8; 32]>,
    row: &Row,
    schema: &[SchemaField],
) {
    map.clear();
    // Sized by the highest write_order rather than the field count, because that
    // is what indexes it. `RowBlocks::len` is the only thing this changes and it
    // has no non-test consumer, so nothing observes the wider map.
    let needed = schema
        .iter()
        .map(|f| f.write_order as usize + 1)
        .max()
        .unwrap_or(0)
        .max(schema.len());
    map.resize(needed, u8::MAX);
    for (pos, col) in row.columns.iter().enumerate() {
        // `schema_id` *is* the write order — `calculate_schema_offset` returns
        // `field.write_order` — so the old `schema[col.schema_id].write_order`
        // was a round-trip that only returned its own input, and only while the
        // two coincided. Where they don't, it indexed out of bounds: the same
        // mistake in pgwire panicked a worker with "len is 22 but the index is
        // 22" on a projected field list.
        let wo = col.schema_id as usize;
        // `pos` is stored as a `u8` and `u8::MAX` is the absent sentinel, so a
        // row with 255 or more fetched columns cannot be represented. Leaving
        // the sentinel makes such a column read as NULL, which is wrong but
        // consistent and detectable; truncating would silently return a
        // *different* column's bytes. Widening the map to `u16` is the real fix
        // if a table ever approaches that width — the widest in the live
        // deployment is 64 fields including `__null_` companions.
        if wo < map.len() && pos < u8::MAX as usize {
            map[wo] = pos as u8;
        }
    }
}

#[cfg(test)]
mod order_limit_tests {
    use super::*;
    use crate::core::db_type::DbType;
    use crate::core::row::row::{Column, Row};
    use crate::memory_pool::MEMORY_POOL;

    fn row(v: i64) -> Row {
        let bytes = v.to_le_bytes();
        let mut mb = MEMORY_POOL.acquire(bytes.len());
        mb.into_slice_mut().copy_from_slice(&bytes);
        let mut columns = SmallVec::new();
        columns.push(Column { schema_id: 0, data: mb, column_type: DbType::I64 });
        Row { position: v as u64, columns, length: 8 as _ }
    }

    fn schema() -> Arc<SmallVec<[SchemaField; 20]>> {
        let mut f = SmallVec::new();
        f.push(SchemaField::new("v".to_string(), DbType::I64, 8, 0, 0, false));
        Arc::new(f)
    }

    fn values(rows: &[Row]) -> Vec<i64> {
        rows.iter()
            .map(|r| {
                let s = r.columns[0].data.into_slice();
                i64::from_le_bytes(s[..8].try_into().unwrap())
            })
            .collect()
    }

    /// A deterministic shuffle: distinct values, so no tie can make the
    /// unstable selection and the stable sort legitimately disagree.
    fn scrambled(n: i64) -> Vec<Row> {
        let mut state = 0x2545_F491_4F6C_DD1Du64;
        let mut vs: Vec<i64> = (0..n).collect();
        for i in (1..vs.len()).rev() {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            let j = ((state >> 33) as usize) % (i + 1);
            vs.swap(i, j);
        }
        vs.into_iter().map(row).collect()
    }

    /// `ORDER BY … LIMIT k` must return exactly the rows a full sort followed by
    /// a truncate would, which is the whole licence for doing the cheaper thing.
    #[test]
    fn top_k_selection_agrees_with_sort_then_truncate() {
        let sc = schema();
        for k in [0usize, 1, 2, 7, 199, 200, 201, 999, 1000, 1001] {
            let mut fast = scrambled(1000);
            Self_sort(&mut fast, &sc, Some(k as u64));

            let mut reference = scrambled(1000);
            Self_sort(&mut reference, &sc, None);
            reference.truncate(k);

            assert_eq!(values(&fast), values(&reference), "k={}", k);
        }
    }

    /// Descending must select the *largest* k, not the smallest reversed.
    #[test]
    fn descending_selects_from_the_other_end() {
        let sc = schema();
        let mut rows = scrambled(500);
        ConcurrentProcessor::sort_rows_if_needed(
            &mut rows,
            &sc,
            &Some("v DESC".to_string()),
            Some(3),
        );
        assert_eq!(values(&rows), vec![499, 498, 497]);
    }

    /// Without an `ORDER BY` the cap still has to be applied — it used to be a
    /// separate `truncate` at the call site.
    #[test]
    fn a_limit_without_an_order_by_still_caps() {
        let sc = schema();
        let mut rows = scrambled(50);
        ConcurrentProcessor::sort_rows_if_needed(&mut rows, &sc, &None, Some(5));
        assert_eq!(rows.len(), 5);
    }

    #[allow(non_snake_case)]
    fn Self_sort(rows: &mut Vec<Row>, sc: &Arc<SmallVec<[SchemaField; 20]>>, k: Option<u64>) {
        ConcurrentProcessor::sort_rows_if_needed(rows, sc, &Some("v".to_string()), k);
    }
}
