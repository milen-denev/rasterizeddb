use std::sync::Arc;

use futures::future::join_all;
use itertools::Either;
use tokio::{sync::{mpsc, Semaphore}, task};

use crate::core::storage_providers::traits::StorageIO;
use super::{row::{Row, RowFetch}, row_pointer::RowPointerIterator, transformer::ColumnTransformer};

pub struct ConcurrentProcessor;

impl ConcurrentProcessor {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn process<'a, S: StorageIO>(&self, 
        row_fetch: RowFetch,
        io_rows: &'static S,
        iterator: &mut RowPointerIterator<'a, S>,
        column_transformer: ColumnTransformer) -> Vec<Row> {

        let (tx, mut rx) = mpsc::unbounded_channel::<Row>();
        let arc_tuple = Arc::new((Semaphore::new(16), io_rows, row_fetch));

        // Collect handles for all batch tasks
        let mut batch_handles = Vec::new();

        iterator.reset();

        // Process batches
        while let Ok(pointers) = iterator.next_row_pointers().await {
            if pointers.is_empty() {
                break;
            }

            let tuple_clone = arc_tuple.clone();
            let tx_clone = tx.clone();

            let column_type = column_transformer.column_type.clone();
            let column_1 = column_transformer.column_1.clone();
            let transformer_type = column_transformer.transformer_type.clone();
            let next = column_transformer.next.clone();

            // Spawn a task for this entire batch of pointers
            let batch_handle = task::spawn(async move {
                // Acquire a permit for this batch
                let batch_permit = tuple_clone.0.acquire().await.unwrap();

                let mut row = Row::default();

                // Process each pointer in this batch (same as your original code)
                for pointer in pointers {
                    let tuple_clone_2 = tuple_clone.clone();
                    let (_, io_clone, row_fetch) = &*tuple_clone_2;
                    pointer.fetch_row_reuse_async(io_clone, &row_fetch, &mut row).await;
                    
                    let mut transformer = ColumnTransformer::new(
                        column_type.clone(),
                        column_1.clone(),
                        transformer_type.clone(),
                        next.clone()
                    );

                    transformer.setup_column_2(row.columns[0].data.clone());
                    
                    let result = transformer.transform_single();
                    
                    if let Either::Right(found) = result {
                        if found {
                            println!("Found row with ID: {}", pointer.id);
                            tx_clone.send(Row::clone_from_mut_row(&row)).unwrap();
                        }
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