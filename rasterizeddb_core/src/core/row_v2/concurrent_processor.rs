use std::{borrow::Cow, cell::UnsafeCell, sync::Arc};

use futures::future::join_all;
use itertools::Either;
use smallvec::SmallVec;
use tokio::{sync::{mpsc, Semaphore}, task};

use crate::{core::{row_v2::{query_tokenizer::{numeric_to_mb, str_to_mb, Token}, transformer::{ColumnTransformerType, ComparerOperation}}, storage_providers::traits::StorageIO}, memory_pool::MemoryBlock, MAX_PERMITS};
use super::{query_parser::parse_query, row::{column_vec_into_vec, Row, RowFetch}, row_pointer::RowPointerIterator, schema::SchemaField, query_tokenizer::tokenize, transformer::{ColumnTransformer, Next, TransformerProcessor}};

pub struct ConcurrentProcessor;

impl ConcurrentProcessor {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn process<'a, S: StorageIO>(&self, 
        where_query: &str,
        row_fetch: RowFetch,
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

        let arc_tuple: Arc<(Semaphore, RowFetch)> = Arc::new((Semaphore::new(MAX_PERMITS), row_fetch));

        let schema_arc = Arc::new(table_schema);
        let token_arc_1 = Arc::new(tokens);

        let io_rows_1 = Arc::clone(&io_rows);

        // Process batches
        while let Ok(pointers) = iterator.next_row_pointers().await {
            //println!("pointers: {:?}", pointers);

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

                let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

                // Process each pointer in this batch
                for pointer in pointers.iter() {
                    let io_rows_clone = Arc::clone(&io_rows_2);
                    let tuple_clone_2 = tuple_clone.clone();
                    let (_, row_fetch) = &*tuple_clone_2;

                    pointer.fetch_row_reuse_async(io_rows_clone, &row_fetch, &mut buffer.row).await;

                    let result = {
                        let mut_hashtable_buffer = unsafe { &mut *buffer.hashtable_buffer.get() };

                        mut_hashtable_buffer.clear();
                        buffer.bool_buffer.clear();

                        column_vec_into_vec(
                            mut_hashtable_buffer,
                            &buffer.row.columns,
                            &*schema_ref
                        );

                        unsafe 
                        {
                            let mut_transformer = &mut *transformer.get();

                            parse_query(
                                &token_ref_1,
                                mut_hashtable_buffer,
                                mut_transformer
                            );
                        }
                    
                        transformer.get_mut().execute(&mut buffer.bool_buffer)
                    };

                    if result {
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