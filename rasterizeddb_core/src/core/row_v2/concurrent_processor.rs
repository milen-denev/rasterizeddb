use std::{borrow::Cow, cell::UnsafeCell, collections::VecDeque, sync::Arc};

use futures::future::join_all;
use tokio::{sync::{mpsc, Semaphore}, task};

use crate::{core::storage_providers::traits::StorageIO, memory_pool::MemoryBlock};
use super::{query_parser::{parse_query, tokenize}, row::{column_vec_into_vec, Row, RowFetch}, row_pointer::RowPointerIterator, schema::SchemaField, transformer::{ColumnTransformer, Next, TransformerProcessor}};

pub struct ConcurrentProcessor;

impl ConcurrentProcessor {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn process<'a, S: StorageIO>(&self, 
        where_query: &str,
        row_fetch: RowFetch,
        table_schema: Vec<SchemaField>,
        io_rows: &'static S,
        iterator: &mut RowPointerIterator<'a, S>) -> Vec<Row> {

        let (tx, mut rx) = mpsc::unbounded_channel::<Row>();
        let arc_tuple = Arc::new((Semaphore::new(16), io_rows, row_fetch));

        // Collect handles for all batch tasks
        let mut batch_handles = Vec::new();

        iterator.reset();

        let schema_arc = Arc::new(table_schema);
        let query_arc = Arc::new(where_query.to_string());

        let tokens = tokenize(&query_arc, &schema_arc);
        let token_arc = Arc::new(tokens);

        // Process batches
        while let Ok(pointers) = iterator.next_row_pointers().await {
            if pointers.is_empty() {
                break;
            }

            let tuple_clone = arc_tuple.clone();
            let tx_clone = tx.clone();

            let schema_ref = schema_arc.clone();
            let token_ref = token_arc.clone();

            // Spawn a task for this entire batch of pointers
            let batch_handle = task::spawn(async move {

                // Acquire a permit for this batch
                let batch_permit = tuple_clone.0.acquire().await.unwrap();

                let row = Row::default();

                let mut buffer = Buffer {
                    // Cleared in this function
                    hashtable_buffer: vec![],
                    // Cleared in read row functions
                    row,
                    // Cleared in new function
                    transformers: VecDeque::new(),
                    // Cleared in new function
                    intermediate_results: vec![],
                    // Cleared in this function
                    bool_buffer: vec![]
                };

                let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

                // Process each pointer in this batch (same as your original code)
                for pointer in pointers {
                    buffer.hashtable_buffer.clear();
                    buffer.bool_buffer.clear();

                    let tuple_clone_2 = tuple_clone.clone();
                    let (_, io_clone, row_fetch) = &*tuple_clone_2;

                    pointer.fetch_row_reuse_async(io_clone, &row_fetch, &mut buffer.row).await;

                    column_vec_into_vec(
                        &mut buffer.hashtable_buffer,
                        &buffer.row.columns,
                        &schema_ref
                    );
                
                    unsafe 
                    {
                        let mut_transformer = &mut *transformer.get();

                        parse_query(
                            &token_ref,
                            &buffer.hashtable_buffer,
                            &schema_ref,
                            mut_transformer
                        );
                    }
                
                    let result = transformer.get_mut().execute(&mut buffer.bool_buffer);

                    if result {
                        tx_clone.send(Row::clone_from_mut_row(&buffer.row)).unwrap();
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
    pub hashtable_buffer: Vec<(Cow<'a, str>, MemoryBlock)>,
    pub row: Row,
    pub transformers: VecDeque<ColumnTransformer>,
    pub intermediate_results: Vec<MemoryBlock>,
    pub bool_buffer: Vec<(bool, Option<Next>)>
}