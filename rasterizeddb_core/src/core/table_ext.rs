use std::io::{self, Read, Seek, SeekFrom};
use log::debug;

use super::{db_type::DbType, support_types::CursorVector};
use super::helpers::read_row_cursor_whole;

use crate::{renderers::html::render_rows_to_html, rql::parser::ReturnView};
use super::support_types::ReturnResult;
use smallvec::SmallVec;

#[cfg(feature = "enable_parallelism")]
use std::{arch::x86_64::{_mm_prefetch, _MM_HINT_T0}, sync::{atomic::{AtomicU64, Ordering}, Arc}};

#[cfg(feature = "enable_parallelism")]
use tokio::sync::mpsc;

#[cfg(feature = "enable_index_caching")]
use crate::POSITIONS_CACHE;

#[cfg(feature = "enable_parallelism")]
use super::{column::Column, helpers::row_prefetching_cursor, row::Row, storage_providers::traits::StorageIO, support_types::FileChunk};

#[cfg(feature = "enable_parallelism")]
use crate::{memory_pool::MEMORY_POOL, rql::{models::{Next, Token}, tokenizer::evaluate_column_result}, simds::endianess::read_u32};

#[inline(always)]
pub fn extent_non_string_buffer(
    data_buffer: &'static mut [u8],
    db_type: &DbType,
    cursor_vector: &mut CursorVector,
    position: &mut u64) {
    let db_size = db_type.get_size();

    if db_size == 1 {
        let preset_array: [u8; 1] = [cursor_vector.vector[*position as usize]];
        *position += 1;

        data_buffer.copy_from_slice(&preset_array);
    } else if db_size == 2 {
        let preset_array: [u8; 2] = [
            cursor_vector.vector[*position as usize],
            cursor_vector.vector[(*position + 1) as usize],
        ];
        *position += 2;

        data_buffer.copy_from_slice(&preset_array);
    } else if db_size == 4 {
        let preset_array: [u8; 4] = [
            cursor_vector.vector[*position as usize],
            cursor_vector.vector[(*position + 1) as usize],
            cursor_vector.vector[(*position + 2) as usize],
            cursor_vector.vector[(*position + 3) as usize],
        ];
        *position += 4;

        data_buffer.copy_from_slice(&preset_array);
    } else if db_size == 8 {
        let preset_array: [u8; 8] = [
            cursor_vector.vector[*position as usize],
            cursor_vector.vector[(*position + 1) as usize],
            cursor_vector.vector[(*position + 2) as usize],
            cursor_vector.vector[(*position + 3) as usize],
            cursor_vector.vector[(*position + 4) as usize],
            cursor_vector.vector[(*position + 5) as usize],
            cursor_vector.vector[(*position + 6) as usize],
            cursor_vector.vector[(*position + 7) as usize],
        ];
        *position += 8;

        data_buffer.copy_from_slice(&preset_array);
    } else if db_size == 16 {
        let preset_array: [u8; 16] = [
            cursor_vector.vector[*position as usize],
            cursor_vector.vector[(*position + 1) as usize],
            cursor_vector.vector[(*position + 2) as usize],
            cursor_vector.vector[(*position + 3) as usize],
            cursor_vector.vector[(*position + 4) as usize],
            cursor_vector.vector[(*position + 5) as usize],
            cursor_vector.vector[(*position + 6) as usize],
            cursor_vector.vector[(*position + 7) as usize],
            cursor_vector.vector[(*position + 8) as usize],
            cursor_vector.vector[(*position + 9) as usize],
            cursor_vector.vector[(*position + 10) as usize],
            cursor_vector.vector[(*position + 11) as usize],
            cursor_vector.vector[(*position + 12) as usize],
            cursor_vector.vector[(*position + 13) as usize],
            cursor_vector.vector[(*position + 14) as usize],
            cursor_vector.vector[(*position + 15) as usize],
        ];
        *position += 16;

        data_buffer.copy_from_slice(&preset_array);
    } else {
        let cursor = &mut cursor_vector.cursor;
        cursor.seek(SeekFrom::Start(*position)).unwrap();
        let mut preset_buffer = vec![0; db_size as usize];
        cursor.read(&mut preset_buffer).unwrap();
        data_buffer.copy_from_slice(&mut preset_buffer);
        *position += db_size as u64;
    }
}

#[cfg(feature = "enable_parallelism")]
pub(crate) async fn process_all_chunks(
    table_name: &str,
    return_view: Option<ReturnView>,
    column_indexes: Arc<Vec<u32>>,
    evaluation_tokens: Vec<(Vec<Token>, Option<Next>)>,
    limit: u64,
    select_all: bool,
    mutated: bool,
    io_sync: &Box<impl StorageIO>,
    chunks: Arc<Vec<FileChunk>>,
    parallelism_limit: usize,

    #[cfg(feature = "enable_index_caching")]
    hash: u64

) -> io::Result<Option<ReturnResult>> {
    use futures::future::join_all;
    use tokio::{sync::Semaphore, task};

    let atomic_limit = Arc::new(AtomicU64::new(0));

    let (tx, mut rx) = mpsc::unbounded_channel::<Row>();

    let semaphore = Arc::new(Semaphore::new(parallelism_limit));
    let total_chunks = chunks.len();
    let mut handles = Vec::with_capacity(total_chunks);

    for (i, chunk) in chunks.iter().cloned().enumerate() {
  
        let semaphore_clone = semaphore.clone();

        let atomic_u64_clone = atomic_limit.clone();
        let tx_clone = tx.clone();
        let mut io_sync_inner = io_sync.clone();
        let column_indexes_inner = column_indexes.clone();
        let evaluation_tokens_inner = evaluation_tokens.clone();

        let handle = task::spawn(async move {
            let permit = semaphore_clone.acquire().await.unwrap();

            _ = process_chunk_async(
                chunk.read_chunk_sync(&mut io_sync_inner).await, 
                column_indexes_inner,
                evaluation_tokens_inner, 
                limit,
                select_all,
                mutated,
                chunk.clone(),
                atomic_u64_clone,
                tx_clone,
                i as u32,

                #[cfg(feature = "enable_index_caching")]
                hash

            ).await;

            drop(permit); // Release the permit when task finishes
        });

        handles.push(handle);
    }

    join_all(handles).await;

    let aggregator_handle = tokio::spawn(async move {
        let mut collected_rows = Vec::with_capacity(if limit > 50 { 50 } else { limit as usize });

        if !rx.is_empty() {
            rx.recv_many(&mut collected_rows, usize::MAX).await;
        }

        drop(rx);
        collected_rows
    });

    // Await the aggregator to finish collecting rows.
    let all_rows = aggregator_handle.await.unwrap();

    if all_rows.len() == 0 {
        Ok(None)
    } else {
        if let Some(return_view) = return_view {
            if return_view == ReturnView::Html {
                return  Ok(Some(ReturnResult::HtmlView(render_rows_to_html(Ok(Some(all_rows)), table_name).unwrap())));
            } else {
                return Ok(Some(ReturnResult::Rows(SmallVec::from_vec(all_rows))));
            }
        } else {
            return Ok(Some(ReturnResult::Rows(SmallVec::from_vec(all_rows))));
        }
    }
}

#[cfg(feature = "enable_parallelism")]
#[inline(always)]
pub(crate) async fn process_chunk_async(
    chunk_buffer: Vec<u8>,
    column_indexes: Arc<Vec<u32>>,
    mut evaluation_tokens: Vec<(Vec<Token>, Option<Next>)>,
    limit: u64,
    select_all: bool, 
    mutated: bool,
    file_chunk: FileChunk,
    atomic_limit: Arc<AtomicU64>,
    tx: mpsc::UnboundedSender<Row>,

    _thread_id: u32,

    #[cfg(feature = "enable_index_caching")]
    hash: u64

) -> io::Result<()> {
    debug!("thread: {}", _thread_id);

    #[cfg(feature = "enable_index_caching")]
    let mut result_row_vec: Vec<(u64, u32)> = Vec::default();

    let mut position: u64 = 0;
    let mut cursor_vector = CursorVector::new(&chunk_buffer);
    let mut required_columns: Vec<(u32, Column)> = Vec::default();
    let mut token_results: Vec<(bool, Option<Next>)> = Vec::default();

    loop {
        if let Some(prefetch_result) =
            row_prefetching_cursor(&mut position, &mut cursor_vector, &file_chunk, mutated)
                .unwrap()
        {
            let mut current_column_index: u32 = 0;
            let first_column_index = position.clone();

            loop {
                if column_indexes.iter().any(|x| *x == current_column_index) {
                    let column_type = cursor_vector.vector[position as usize];
                    position += 1;

                    let db_type = DbType::from_byte(column_type.clone());

                    if db_type == DbType::END {
                        break;
                    }

                    if db_type != DbType::STRING {
                        let size = db_type.get_size();
                        let memory_chunk = MEMORY_POOL.acquire(size as usize);

                        let data_buffer = memory_chunk.into_slice_mut();

                        extent_non_string_buffer(
                            data_buffer,
                            &db_type,
                            &mut cursor_vector,
                            &mut position,
                        );

                        let column = Column::from_chunk(column_type, memory_chunk);

                        required_columns.push((current_column_index, column));
                    } else {
                        let str_len_array: [u8; 4] = [
                            cursor_vector.vector[position as usize],
                            cursor_vector.vector[(position + 1) as usize],
                            cursor_vector.vector[(position + 2) as usize],
                            cursor_vector.vector[(position + 3) as usize],
                        ];

                        position += 4;

                        let str_len_array_pointer = str_len_array.as_ptr();

                        #[cfg(target_arch = "x86_64")]
                        {
                            unsafe {
                                _mm_prefetch::<_MM_HINT_T0>(
                                    str_len_array_pointer as *const i8,
                                )
                            };
                        }

                        let str_length = unsafe { read_u32(str_len_array_pointer) };

                        let chunk_slice = cursor_vector.vector.as_slice();

                        let str_memory_chunk = MEMORY_POOL.acquire(str_length as usize);

                        let preset_buffer = str_memory_chunk.into_slice_mut();

                        for (i, byte) in chunk_slice[position as usize..position as usize + str_length as usize].iter().enumerate() {
                            preset_buffer[i] = *byte;
                        }

                        position += str_length as u64;

                        let column =
                            Column::from_chunk(column_type, str_memory_chunk);

                        required_columns
                            .push((current_column_index, column));
                    }
                } else {
                    let column_type = cursor_vector.vector[position as usize];
                    position += 1;

                    let db_type = DbType::from_byte(column_type);

                    if db_type == DbType::END {
                        break;
                    }

                    if db_type != DbType::STRING {
                        let db_size = db_type.get_size();
                        position += db_size as u64;
                    } else {
                        let str_len_array: [u8; 4] = [
                            cursor_vector.vector[position as usize],
                            cursor_vector.vector[(position + 1) as usize],
                            cursor_vector.vector[(position + 2) as usize],
                            cursor_vector.vector[(position + 3) as usize],
                        ];

                        position += 4;

                        let str_len_array_pointer = str_len_array.as_ptr();

                        #[cfg(target_arch = "x86_64")]
                        {
                            unsafe {
                                _mm_prefetch::<_MM_HINT_T0>(
                                    str_len_array_pointer as *const i8,
                                )
                            };
                        }

                        let str_length = unsafe { read_u32(str_len_array_pointer) };

                        position += str_length as u64;
                    }
                }
                current_column_index += 1;
            }

            let evaluation = if !select_all {
                let eval = evaluate_column_result(
                    &required_columns,
                    &mut evaluation_tokens,
                    &mut token_results,
                );

                required_columns.clear();
                token_results.clear();
                eval
            } else {
                true
            };

            if evaluation {
                let mut cursor = &mut cursor_vector.cursor;
                let new_position = first_column_index + prefetch_result.length as u64 + 1;

                let row = read_row_cursor_whole(
                    first_column_index,
                    prefetch_result.found_id,
                    prefetch_result.length,
                    &mut cursor,
                )
                .unwrap();

                let current_limit = atomic_limit.fetch_add(1, Ordering::Relaxed);

                if current_limit < limit {
                    if tx.send(row).is_err() {
                        break;
                    }    
                }
                
                #[cfg(feature = "enable_index_caching")]
                {
                    result_row_vec.push((
                        file_chunk.current_file_position + first_column_index
                            - 1
                            - 8
                            - 4,
                        prefetch_result.length,
                    ));
                }

                if current_limit == limit {
                    #[cfg(feature = "enable_index_caching")]
                    {
                        POSITIONS_CACHE.insert(hash, result_row_vec);
                    }

                    break;
                }

                position = new_position;
            } else {
                position = first_column_index + prefetch_result.length as u64 + 1;
            }
        } else {
            break;
        }
    };

    Ok(())
}