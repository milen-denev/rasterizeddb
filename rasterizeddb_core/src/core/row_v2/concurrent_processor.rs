use std::{borrow::Cow, cell::UnsafeCell, sync::Arc};

use futures::future::join_all;
use itertools::Either;
use smallvec::SmallVec;
use tokio::{sync::{mpsc, Semaphore}, task};

use crate::{core::storage_providers::traits::StorageIO, memory_pool::{MemoryBlock, MEMORY_POOL}, MAX_PERMITS};
use super::{query_parser::parse_query, row::{column_vec_into_vec, Row, RowFetch}, row_pointer::RowPointerIterator, schema::SchemaField, tokenizer::{tokenize, NumericValue, Token}, transformer::{ColumnTransformer, ColumnTransformerType, ComparerOperation, Next, TransformerProcessor}};

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

        // Collect handles for all batch tasks
        let mut batch_handles = Vec::new();

        iterator.reset();

        let table_schema = table_schema
            .into_iter()
            .collect::<SmallVec<[SchemaField; 20]>>();

        let tokens = tokenize(&where_query, &table_schema);

        let arc_tuple = Arc::new((Semaphore::new(MAX_PERMITS), io_rows, row_fetch));

        let schema_arc = Arc::new(table_schema);
        let token_arc_1 = Arc::new(tokens);

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
                let mut schema_id: u64 = 0;

                let mut single_transformer_data = if token_ref_1.len() == 3 {
                    let ident = token_ref_1.iter().filter(|x| {
                        match x {
                            Token::Ident(_) => true,
                            _ => false
                        }
                    }).next().unwrap();

                    let column_1 = match ident {
                        Token::Ident(column) => column,
                        _ => unreachable!()
                    };

                    let db_type = column_1.1.clone();
                    schema_id = column_1.2;

                    let transform = token_ref_1.iter().filter(|x| {
                        match x {
                            Token::Op(_) => true,
                            _ => false
                        }
                    }).next().unwrap();
  
                    let operations_type = match transform {
                        Token::Op(op) => op,
                        _ => unreachable!()
                    };

                    let operation = match operations_type.as_str() {
                        ">" => ComparerOperation::Less, // reverse for logical operations
                        "<" => ComparerOperation::Greater, // reverse for logical operations
                        "=" => ComparerOperation::Equals,
                        "!=" => ComparerOperation::NotEquals,
                        "<=" => ComparerOperation::GreaterOrEquals, // reverse for logical operations
                        ">=" => ComparerOperation::LessOrEquals, // reverse for logical operations
                        "CONTAINS" => ComparerOperation::Contains,
                        "STARTSWITH" => ComparerOperation::StartsWith,
                        "ENDSWITH" => ComparerOperation::EndsWith,
                        _ => unreachable!()
                    };

                    let value = token_ref_1.iter().filter(|x| {
                        match x {
                            Token::Number(_) => true,
                            Token::StringLit(_) => true,
                            _ => false
                        }
                    }).map(|x| 
                        match x {
                            Token::Number(val) => {
                                numeric_value_into_memory_block(val)
                            },
                            Token::StringLit(val) => {
                                string_value_into_memory_block(val)
                            },
                            _ => unreachable!()
                        }
                    ).next().unwrap();

                    let transformer = ColumnTransformer::new(
                        db_type,
                        value,
                        ColumnTransformerType::ComparerOperation(operation),
                        None
                    );

                    Some(transformer)
                } else {
                    None
                };

                // Process each pointer in this batch (same as your original code)
                for pointer in pointers.iter() {
                    let tuple_clone_2 = tuple_clone.clone();
                    let (_, io_clone, row_fetch) = &*tuple_clone_2;

                    pointer.fetch_row_reuse_async(io_clone, &row_fetch, &mut buffer.row).await;

                    let result = if let Some(ref mut single_transformer_d) = single_transformer_data {
                        let column = buffer.row.columns.iter().filter(|x| {
                            match x.schema_id {
                                _ if x.schema_id == schema_id => true,
                                _ => false
                            }
                        }).next().unwrap();

                        single_transformer_d.setup_column_2(column.data.clone());

                        if let Either::Right(result) = single_transformer_d.transform_single() {
                            result.0 
                        } else {
                            false
                        }
                    } else {
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
    pub hashtable_buffer: UnsafeCell<SmallVec<[(Cow<'a, str>, MemoryBlock); 20]>>,
    pub row: Row,
    pub transformers: SmallVec<[ColumnTransformer; 36]>,
    pub intermediate_results: SmallVec<[MemoryBlock; 20]>,
    pub bool_buffer: SmallVec<[(bool, Option<Next>); 20]>
}

fn numeric_value_into_memory_block(value: &NumericValue) -> MemoryBlock {
    match value {
        NumericValue::I8(val) => {
            let bytes = val.to_le_bytes();
            let block = MEMORY_POOL.acquire(bytes.len());
            let slice = block.into_slice_mut();
            slice.copy_from_slice(&bytes);
            block
        },
        NumericValue::I16(val) => {
            let bytes = val.to_le_bytes();
            let block = MEMORY_POOL.acquire(bytes.len());
            let slice = block.into_slice_mut();
            slice.copy_from_slice(&bytes);
            block
        },
        NumericValue::I32(val) => {
            let bytes = val.to_le_bytes();
            let block = MEMORY_POOL.acquire(bytes.len());
            let slice = block.into_slice_mut();
            slice.copy_from_slice(&bytes);
            block
        },
        NumericValue::I64(val) => {
            let bytes = val.to_le_bytes();
            let block = MEMORY_POOL.acquire(bytes.len());
            let slice = block.into_slice_mut();
            slice.copy_from_slice(&bytes);
            block
        },
        NumericValue::I128(val) => {
            let bytes = val.to_le_bytes();
            let block = MEMORY_POOL.acquire(bytes.len());
            let slice = block.into_slice_mut();
            slice.copy_from_slice(&bytes);
            block
        },
        NumericValue::U8(val) => {
            let bytes = val.to_le_bytes();
            let block = MEMORY_POOL.acquire(bytes.len());
            let slice = block.into_slice_mut();
            slice.copy_from_slice(&bytes);
            block
        },
        NumericValue::U16(val) => {
            let bytes = val.to_le_bytes();
            let block = MEMORY_POOL.acquire(bytes.len());
            let slice = block.into_slice_mut();
            slice.copy_from_slice(&bytes);
            block
        },
        NumericValue::U32(val) => {
            let bytes = val.to_le_bytes();
            let block = MEMORY_POOL.acquire(bytes.len());
            let slice = block.into_slice_mut();
            slice.copy_from_slice(&bytes);
            block
        },
        NumericValue::U64(val) => {
            let bytes = val.to_le_bytes();
            let block = MEMORY_POOL.acquire(bytes.len());
            let slice = block.into_slice_mut();
            slice.copy_from_slice(&bytes);
            block
        },
        NumericValue::U128(val) => {
            let bytes = val.to_le_bytes();
            let block = MEMORY_POOL.acquire(bytes.len());
            let slice = block.into_slice_mut();
            slice.copy_from_slice(&bytes);
            block
        },
        NumericValue::F32(val) => {
            let bytes = val.to_le_bytes();
            let block = MEMORY_POOL.acquire(bytes.len());
            let slice = block.into_slice_mut();
            slice.copy_from_slice(&bytes);
            block
        },
        NumericValue::F64(val) => {
            let bytes = val.to_le_bytes();
            let block = MEMORY_POOL.acquire(bytes.len());
            let slice = block.into_slice_mut();
            slice.copy_from_slice(&bytes);
            block
        }
    }
}

fn string_value_into_memory_block(value: &str) -> MemoryBlock {
    let bytes = value.as_bytes();
    let block = MEMORY_POOL.acquire(bytes.len());
    let slice = block.into_slice_mut();
    slice.copy_from_slice(bytes);
    block
}