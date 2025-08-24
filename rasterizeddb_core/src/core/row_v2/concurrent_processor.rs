use std::{borrow::Cow, cell::UnsafeCell, sync::Arc};

use futures::future::join_all;
use smallvec::SmallVec;
use tokio::{sync::{mpsc, Semaphore}, task};

use crate::{core::{row_v2::query_parser::QueryParser, storage_providers::traits::StorageIO}, memory_pool::MemoryBlock, MAX_PERMITS_THREADS};
use super::{row::{Row, RowFetch}, row_pointer::RowPointerIterator, schema::SchemaField, query_tokenizer::tokenize, transformer::{ColumnTransformer, Next, TransformerProcessor}};

pub static EMPTY_STR: &str = "";

pub struct ConcurrentProcessor;

impl ConcurrentProcessor {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn process<'a, S: StorageIO>(&self, 
        where_query: &str,
        query_row_fetch: RowFetch,
        requested_row_fetch: RowFetch,
        table_schema: &Vec<SchemaField>,
        io_rows: Arc<S>,
        iterator: &mut RowPointerIterator<S>) -> Vec<Row> {

        let (tx, mut rx) = mpsc::unbounded_channel::<Row>();

        // Collect handles for all batch tasks
        let mut batch_handles = Vec::new();

        iterator.reset();

        let table_schema = table_schema
            .into_iter()
            .map(|x| x.clone())
            .collect::<SmallVec<[SchemaField; 20]>>();

        let tokens = tokenize(&where_query, &table_schema);

        let arc_tuple: Arc<(Semaphore, RowFetch, RowFetch)> = 
            Arc::new((
                Semaphore::new(MAX_PERMITS_THREADS), 
                query_row_fetch,
                requested_row_fetch
            ));

        let schema_arc = Arc::new(table_schema);
        let token_arc_1 = Arc::new(tokens);

        let io_rows_1 = Arc::clone(&io_rows);

        // Process batches
        while let Ok(pointers) = iterator.next_row_pointers().await {
            if pointers.len() == 0 {
                break;
            }

            let tuple_clone = arc_tuple.clone();
            let tx_clone = tx.clone();

            let schema_ref = schema_arc.clone();
            let token_ref_1 = token_arc_1.clone();

            let io_rows_2 = Arc::clone(&io_rows_1);

            // Spawn a task for this entire batch of pointers
            let batch_handle = task::spawn(async move {

                // Acquire a permit for this batch
                let batch_permit = tuple_clone.0.acquire().await.unwrap();

                let row = Row::default();

                let mut buffer = Buffer {
                    // Cleared in this function
                    hashtable_buffer: UnsafeCell::new(SmallVec::new()),
                    // Cleared in read row functions
                    row,
                    // Cleared in new function
                    transformers: SmallVec::new(),
                    // Cleared in new function
                    intermediate_results: SmallVec::new(),
                    // Cleared in this function
                    bool_buffer: SmallVec::new()
                };

                
                let transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));
                let mut_transformer = unsafe { &mut *transformer.get() };
                let parser = UnsafeCell::new(QueryParser::new(&token_ref_1, mut_transformer));

                // Process each pointer in this batch
                for pointer in pointers.iter() {
                    let io_rows_clone = Arc::clone(&io_rows_2);
                    let tuple_clone_2 = tuple_clone.clone();
                    let (_, query_row_fetch, requested_row_fetch) = &*tuple_clone_2;

                    pointer.fetch_row_reuse_async(io_rows_clone, &query_row_fetch, &mut buffer.row).await;

                    let result = {
                        let mut_hashtable_buffer = unsafe { &mut *buffer.hashtable_buffer.get() };

                        mut_hashtable_buffer.clear();
                        buffer.bool_buffer.clear();

                        // Optimized column population - reduces allocations and function call overhead
                        {
                            let columns = &buffer.row.columns;
                            let schema = &*schema_ref;
                            let len = columns.len();
                            
                            // Pre-reserve capacity to avoid reallocation during push operations
                            mut_hashtable_buffer.reserve(schema.len());
                            
                            // Unrolled loop for better branch prediction and reduced overhead
                            unsafe {
                                for i in 0..len {
                                    let column = columns.get_unchecked(i);
                                    let schema_field = schema.get_unchecked(column.schema_id as usize);

                                    let write_order = schema_field.write_order;
                                    
                                    if i < write_order as usize {
                                        // Fill in any missing fields with empty strings
                                        for _ in i..write_order as usize {
                                            mut_hashtable_buffer.push((Cow::Borrowed(EMPTY_STR), MemoryBlock::default()));
                                        }
                                    }

                                    // Direct push with borrowed string (no allocation)
                                    mut_hashtable_buffer.push((
                                        Cow::Borrowed(schema_field.name.as_str()), 
                                        column.data.clone()
                                    ));
                                }
                            }
                        }

                        // Execute query parsing with reduced indirection
                        {   
                            let mut_parser = unsafe { &mut *parser.get() };

                            mut_parser.reset_for_next_execution();

                            let execute_result = mut_parser.execute(mut_hashtable_buffer);
                            if execute_result.is_err() {
                                panic!("Error executing query parser. Error message: {:?}", execute_result.err());
                            }

                            // Execute the transformer and return result
                            unsafe { &mut *transformer.get() }.execute(&mut buffer.bool_buffer)
                        }
                    };

                    if result {
                        let io_rows_clone = Arc::clone(&io_rows_2);
                        pointer.fetch_row_reuse_async(io_rows_clone, &requested_row_fetch, &mut buffer.row).await;

                        tx_clone.send(Row::clone_row(&buffer.row)).unwrap();
                    }
                }

                // Release the batch semaphore permit
                drop(batch_permit);
            });
            
            batch_handles.push(batch_handle);
        }

        // Wait for all batch tasks to complete
        join_all(batch_handles).await;

        // Drop the sender to signal that no more rows will be sent
        drop(tx);

        // Aggregator remains the same
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

        all_rows
    }
}

pub struct Buffer<'a> {
    pub hashtable_buffer: UnsafeCell<SmallVec<[(Cow<'a, str>, MemoryBlock); 20]>>,
    pub row: Row,
    pub transformers: SmallVec<[ColumnTransformer; 36]>,
    pub intermediate_results: SmallVec<[MemoryBlock; 20]>,
    pub bool_buffer: SmallVec<[(bool, Option<Next>); 20]>
}