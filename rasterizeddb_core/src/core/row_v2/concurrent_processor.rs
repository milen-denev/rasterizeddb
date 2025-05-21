use std::sync::Arc;

use futures::future::join_all;
use tokio::{sync::{mpsc, Semaphore}, task};

use crate::{core::storage_providers::traits::StorageIO, memory_pool::MemoryBlock};
use super::{query_parser::{parse_query, tokenize}, row::{column_vec_into_dashmap, update_values_of_dashmap, Column, Row, RowFetch}, row_pointer::RowPointerIterator, schema::SchemaField};

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

        let reference_columns = {
            let mut temp_columns = Vec::new();
            for schema in &table_schema {
                let column = Column {
                    schema_id: schema.write_order,
                    data: MemoryBlock::default(),
                    column_type: schema.db_type.clone(),
                };
                temp_columns.push(column);
            }
            temp_columns
        };

        let reference_columns_arc = Arc::new(reference_columns);
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
            let reference_columns_ref = reference_columns_arc.clone();
            let token_ref = token_arc.clone();

            // Spawn a task for this entire batch of pointers
            let batch_handle = task::spawn(async move {
                let dashmap = column_vec_into_dashmap(
                    &reference_columns_ref,
                    &schema_ref
                );
                
                // Acquire a permit for this batch
                let batch_permit = tuple_clone.0.acquire().await.unwrap();

                let mut row = Row::default();

                // Process each pointer in this batch (same as your original code)
                for pointer in pointers {
                    let tuple_clone_2 = tuple_clone.clone();
                    let (_, io_clone, row_fetch) = &*tuple_clone_2;
                    pointer.fetch_row_reuse_async(io_clone, &row_fetch, &mut row).await;
                    
                    update_values_of_dashmap(
                        &dashmap,
                        &row.columns,
                        &schema_ref
                    );

                    let mut transformer = parse_query(
                        &token_ref,
                        &dashmap,
                        &schema_ref
                    );

                    let result = transformer.execute();

                    if result {
                        tx_clone.send(Row::clone_from_mut_row(&row)).unwrap();
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