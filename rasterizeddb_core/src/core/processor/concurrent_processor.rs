use std::{borrow::Cow, cell::UnsafeCell, sync::{Arc, OnceLock}};

use crc::{CRC_64_ECMA_182, Crc};
use futures::future::join_all;
use smallvec::SmallVec;
use tokio::{
    sync::{Semaphore, mpsc},
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

pub static EMPTY_STR: &str = "";
pub static ATOMIC_CACHE: OnceLock<Arc<AtomicGenericCache<u64, Arc<Vec<RowPointer>>>>> = OnceLock::new();
pub static ENABLE_CACHE: OnceLock<bool> = OnceLock::new();
const CRC_64: Crc<u64> = Crc::<u64>::new(&CRC_64_ECMA_182);

pub struct ConcurrentProcessor;

impl ConcurrentProcessor {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn process<'a, S: StorageIO>(
        &self,
        where_query: &str,
        query_row_fetch: RowFetch,
        requested_row_fetch: RowFetch,
        table_schema: &Vec<SchemaField>,
        io_rows: Arc<S>,
        iterator: &mut RowPointerIterator<S>,
    ) -> Vec<Row> {
        let (tx, mut rx) = mpsc::unbounded_channel::<Row>();
    
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

        let cache_enabled = *ENABLE_CACHE.get().unwrap();
        let crc64_hash;

        let entries_available = if cache_enabled {
            crc64_hash = CRC_64.checksum(where_query.as_bytes());
            let cache = ATOMIC_CACHE.get().unwrap();
            let entries = cache.get(&crc64_hash);
            if let Some(entries) = entries {
                for pointer in entries.iter() {
                    if pointer.deleted {
                        continue;
                    }

                    let io_rows_clone = Arc::clone(&io_rows_outer);
                    let (_, _, requested_row_fetch) = &*shared_tuple;

                    // Fetch the requested row
                    let mut row = Row::default();

                    pointer
                        .fetch_row_reuse_async(
                            io_rows_clone,
                            requested_row_fetch,
                            &mut row,
                        )
                        .await;

                    // Send a clone out to the aggregator
                    if tx.send(Row::clone_row(&row)).is_err() {
                        // Receiver dropped; stop early
                        break;
                    }
                }

                true
            } else {
                false
            }
        } else {
            crc64_hash = 0;
            false
        };

        if entries_available {
            // If cache entries were found and processed, skip further processing.
            drop(tx);
            let mut collected_rows = Vec::with_capacity(50);

            if !rx.is_empty() {
                rx.recv_many(&mut collected_rows, usize::MAX).await;
            }

            drop(rx);

            return collected_rows;
        }

        let (tx_rp, mut rx_rp) = mpsc::unbounded_channel::<RowPointer>();
        
        let mut batch_handles = Vec::new();
        iterator.reset();

        let tokens = tokenize(&where_query, &table_schema);
        
        let schema_arc = Arc::new(table_schema);
        let tokens_arc = Arc::new(tokens);

        // Process batches
        while let Ok(pointers) = iterator.next_row_pointers().await {
            if pointers.is_empty() {
                break;
            }

            let tuple_clone = Arc::clone(&shared_tuple);
            let tx_clone = tx.clone();
            let tx_rp_clone = tx_rp.clone();
            let schema_ref = Arc::clone(&schema_arc);
            let tokens_ref = Arc::clone(&tokens_arc);
            let io_rows_batch = Arc::clone(&io_rows_outer);

            let batch_handle = task::spawn(async move {
                // Acquire a permit for this batch
                let _batch_permit = tuple_clone.0.acquire().await.unwrap();

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
                    if pointer.deleted {
                        continue;
                    }

                    let io_rows_clone = Arc::clone(&io_rows_batch);
                    let (_, query_row_fetch, requested_row_fetch) = &*tuple_clone;

                    // Read the minimal set of columns needed for the WHERE evaluation
                    pointer
                        .fetch_row_reuse_async(io_rows_clone, query_row_fetch, &mut buffer.row)
                        .await;

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
                            panic!("Error executing query parser while building plan: {:?}", e);
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
                        if tx_rp_clone.send(pointer.clone()).is_err() {
                            log::error!("Failed to send row pointer to cache inserter");
                            // Receiver dropped; stop early
                            break;
                        }

                        let io_rows_clone = Arc::clone(&io_rows_batch);
                        pointer
                            .fetch_row_reuse_async(
                                io_rows_clone,
                                requested_row_fetch,
                                &mut buffer.row,
                            )
                            .await;

                        // Send a clone out to the aggregator
                        if tx_clone.send(Row::clone_row(&buffer.row)).is_err() {
                            log::error!("Failed to send row to aggregator");
                            // Receiver dropped; stop early
                            break;
                        }
                    }
                }
            });

            batch_handles.push(batch_handle);
        }

        // Wait for all batch tasks to complete
        join_all(batch_handles).await;

        // Drop the sender to signal that no more rows will be sent
        drop(tx);

        // Aggregator
        let aggregator_handle = tokio::spawn(async move {
            let mut collected_rows = Vec::with_capacity(50);

            if !rx.is_empty() {
                rx.recv_many(&mut collected_rows, usize::MAX).await;
            }

            drop(rx);

            collected_rows
        });

        // Await the aggregator to finish collecting rows.
        let all_rows = aggregator_handle.await.unwrap();

        if crc64_hash != 0 {
            let mut pointers = Vec::with_capacity(50);
            if !rx_rp.is_empty() {
                rx_rp.recv_many(&mut pointers, usize::MAX).await;
            }

            drop(rx_rp);

            // Store the resulting pointers in the cache
            let cache = ATOMIC_CACHE.get().unwrap();

            if cache.insert(crc64_hash, Arc::new(pointers)).is_err() {
                log::error!("Failed to insert entries into cache for hash {}", crc64_hash);
            }
        }

        all_rows
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
