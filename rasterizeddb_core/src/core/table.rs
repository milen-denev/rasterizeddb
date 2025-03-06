use std::{
    arch::x86_64::{_mm_prefetch, _MM_HINT_T0},
    io::{self, Read, Seek, SeekFrom, Write},
    sync::{atomic::{AtomicBool, AtomicU64, Ordering}, Arc},
};

use byteorder::{ByteOrder, LittleEndian, ReadBytesExt, WriteBytesExt};
use itertools::Itertools;
use log::debug;

use super::{
    super::rql::{
        models::Token, 
        parser::ParserResult, 
        tokenizer::evaluate_column_result
    },
    column::Column,
    db_type::DbType,
    helpers::{ 
        columns_cursor_to_row_whole, 
        indexed_row_fetching_file, 
        read_row_columns, 
        read_row_cursor_whole, 
        row_prefetching, 
        row_prefetching_cursor
    },
    row::{InsertOrUpdateRow, Row},
    storage_providers::traits::IOOperationsSync,
    support_types::{CursorVector, FileChunk},
    table_ext::extent_non_string_buffer,
    table_header::TableHeader,
};

use crate::{
    core::helpers::delete_row_file, memory_pool::MEMORY_POOL, rql::models::Next, simds::endianess::read_u32, CHUNK_SIZE, EMPTY_BUFFER, HEADER_SIZE 
};

#[cfg(feature = "enable_parallelism")]
use crate::THREADS;

#[cfg(feature = "enable_parallelism")]
use crate::core::table_ext::process_all_chunks;

#[cfg(feature = "enable_index_caching")]
use crate::POSITIONS_CACHE;

#[allow(dead_code)]
pub struct Table<S: IOOperationsSync> {
    pub(crate) io_sync: Box<S>,
    pub(crate) table_header: Arc<TableHeader>,
    pub(crate) in_memory_index: Arc<Option<Vec<FileChunk>>>,
    pub(crate) current_file_length: AtomicU64,
    pub(crate) current_row_id: AtomicU64,
    pub(crate) immutable: bool,
    pub(crate) locked: AtomicBool,
    pub(crate) mutated: AtomicBool,
}

impl<S: IOOperationsSync> Clone for Table<S> {
    fn clone(&self) -> Self {
        let is_locked = self.locked.load(Ordering::Relaxed);
        let current_file_length = self.current_file_length.load(Ordering::Relaxed);
        let current_row_id = self.current_row_id.load(Ordering::Relaxed);
        let mutated = self.mutated.load(Ordering::Relaxed);

        Self { 
            io_sync: self.io_sync.clone(), 
            table_header: self.table_header.clone(), 
            in_memory_index: self.in_memory_index.clone(), 
            current_file_length: AtomicU64::new(current_file_length), 
            current_row_id: AtomicU64::new(current_row_id), 
            immutable: self.immutable, 
            locked: AtomicBool::new(is_locked),
            mutated: AtomicBool::new(mutated) 
        }
    }
}

unsafe impl<S: IOOperationsSync> Send for Table<S> {}
unsafe impl<S: IOOperationsSync> Sync for Table<S> {}

impl<S: IOOperationsSync> Table<S> {
    /// #### STABILIZED
    /// Initializes a new table. compressed and immutable are not implemented yet.
    pub async fn init(mut io_sync: S, compressed: bool, immutable: bool) -> io::Result<Table<S>> {
        let table_file_len = io_sync.get_len().await;

        if table_file_len >= HEADER_SIZE as u64 {
            
            let buffer = io_sync.read_data(&mut 0, HEADER_SIZE as u32).await;
            let table_header = TableHeader::from_buffer(buffer).unwrap();

            let mutated = table_header.mutated.load(Ordering::Relaxed);
            
            let last_row_id: u64 = table_header.last_row_id.load(Ordering::Relaxed);

            table_header.total_file_length.store(io_sync.get_len().await, Ordering::Relaxed);

            let table = Table {
                io_sync: Box::new(io_sync),
                table_header: Arc::new(table_header),
                in_memory_index: Arc::new(None),
                current_file_length: AtomicU64::new(table_file_len),
                current_row_id: AtomicU64::new(last_row_id),
                immutable: immutable,
                locked: AtomicBool::new(false),
                mutated: AtomicBool::new(mutated),
            };

            Ok(table)
        } else {
            let table_header = TableHeader::new(HEADER_SIZE as u64, 0, 0, compressed, 0, 0, false);

            let mutated = table_header.mutated.load(Ordering::Relaxed);
            
            // Serialize the header and write it to the file
            let header_bytes = table_header.to_bytes().unwrap();

            io_sync.write_data(0, &header_bytes).await;

            let table = Table {
                io_sync: Box::new(io_sync),
                table_header: Arc::new(table_header),
                in_memory_index: Arc::new(None),
                current_file_length: AtomicU64::new(table_file_len),
                current_row_id: AtomicU64::new(0),
                immutable: immutable,
                locked: AtomicBool::new(false),
                mutated: AtomicBool::new(mutated),
            };

            Ok(table)
        }
    }

    /// #### STABILIZED
    /// Initializes a new table. compressed and immutable are not implemented yet.
    pub(crate) async fn init_inner(
        mut io_sync: S,
        compressed: bool,
        immutable: bool,
    ) -> io::Result<Table<S>> {
        let table_file_len = io_sync.get_len().await;

        if table_file_len >= HEADER_SIZE as u64 {
            let buffer = io_sync.read_data(&mut 0, HEADER_SIZE as u32).await;
            let table_header = TableHeader::from_buffer(buffer).unwrap();

            let mutated = table_header.mutated.load(Ordering::Relaxed);
            let last_row_id: u64 = table_header.last_row_id.load(Ordering::Relaxed);

            table_header.total_file_length.store(io_sync.get_len().await, Ordering::Relaxed);

            let table = Table {
                io_sync: Box::new(io_sync),
                table_header: Arc::new(table_header),
                in_memory_index: Arc::new(None),
                current_file_length: AtomicU64::new(table_file_len),
                current_row_id: AtomicU64::new(last_row_id),
                immutable: immutable,
                locked: AtomicBool::new(false),
                mutated: AtomicBool::new(mutated),
            };

            Ok(table)
        } else {
            let table_header = TableHeader::new(HEADER_SIZE as u64, 0, 0, compressed, 0, 0, false);

            let mutated = table_header.mutated.load(Ordering::Relaxed);

            // Serialize the header and write it to the file
            let header_bytes = table_header.to_bytes().unwrap();

            io_sync.write_data(0, &header_bytes).await;

            let table = Table {
                io_sync: Box::new(io_sync),
                table_header: Arc::new(table_header),
                in_memory_index: Arc::new(None),
                current_file_length: AtomicU64::new(table_file_len),
                current_row_id: AtomicU64::new(0),
                immutable: immutable,
                locked: AtomicBool::new(false),
                mutated: AtomicBool::new(mutated),
            };

            Ok(table)
        }
    }

    /// #### STABILIZED
    fn get_current_table_length(&self) -> u64 {
        return self.current_file_length.load(Ordering::Relaxed);
    }

    /// #### STABILIZED
    async fn update_eof_and_id(&mut self, buffer_size: u64) -> u64 {
        // Get current values from atomics
        let end_of_file = self.current_file_length.load(Ordering::SeqCst);
        
        // If end_of_file is 0, set it to HEADER_SIZE
        let new_end_of_file = if end_of_file == 0 {
            HEADER_SIZE as u64 + buffer_size
        } else {
            end_of_file + buffer_size
        };
        
        // Store new end_of_file value
        self.current_file_length.store(new_end_of_file, Ordering::SeqCst);
        
        // Increment and return row ID atomically
        let new_row_id = self.current_row_id.fetch_add(1, Ordering::SeqCst) + 1;
        
        return new_row_id;
    }

    /// #### STABILIZED
    /// Inserts a new row.
    pub async fn insert_row(&mut self, row: InsertOrUpdateRow) {
        loop {
            let table_locked = self.locked.load(Ordering::SeqCst);
            if table_locked {
                continue;
            } else {
                break;
            }
        }

        let columns_len = row.columns_data.len();

        //START (1) + END (1) + u64 ID (8) + u32 LENGTH (4) + VEC SIZE
        let mut buffer: Vec<u8> = Vec::with_capacity(1 + 1 + 8 + 4 + columns_len);

        let total_row_size = buffer.capacity() as u64;

        let row_id = self.update_eof_and_id(total_row_size).await;

        //DbType START
        buffer.push(254);

        WriteBytesExt::write_u64::<LittleEndian>(&mut buffer, row_id).unwrap();
        WriteBytesExt::write_u32::<LittleEndian>(&mut buffer, columns_len as u32).unwrap();

        let mut columns_data = row.columns_data;

        buffer.append(&mut columns_data);

        //DbType END
        buffer.push(255);

        self.io_sync.append_data(&buffer).await;
        // let verify_result = self.io_sync.verify_data_and_sync(current_file_len, &buffer);

        // #[allow(unreachable_code)]
        // if !verify_result {
        //     panic!("DB file contains error.");
        //     todo!("Add rollback.");
        // }

        self.table_header.last_row_id.store(row_id, Ordering::SeqCst);
        let table_bytes = self.table_header.to_bytes().unwrap();
        self.io_sync.write_data_seek(SeekFrom::Start(0), &table_bytes).await;
    }

    /// #### STABILIZED
    /// Inserts a new row.
    pub async fn insert_row_unsync(&mut self, row: InsertOrUpdateRow) {
        loop {
            let table_locked = self.locked.load(Ordering::SeqCst);
            if table_locked {
                continue;
            } else {
                break;
            }
        }

        let columns_len = row.columns_data.len();

        //START (1) + END (1) + u64 ID (8) + u32 LENGTH (4) + VEC SIZE
        let mut buffer: Vec<u8> = Vec::with_capacity(1 + 1 + 8 + 4 + columns_len);

        let total_row_size = buffer.capacity() as u64;

        let row_id = self.update_eof_and_id(total_row_size).await;

        //DbType START
        buffer.push(254);

        WriteBytesExt::write_u64::<LittleEndian>(&mut buffer, row_id).unwrap();
        WriteBytesExt::write_u32::<LittleEndian>(&mut buffer, columns_len as u32).unwrap();

        let mut columns_data = row.columns_data;

        buffer.append(&mut columns_data);

        //DbType END
        buffer.push(255);

        self.io_sync.append_data_unsync(&buffer).await;
        // let verify_result = self.io_sync.verify_data_and_sync(current_file_len, &buffer);

        // #[allow(unreachable_code)]
        // if !verify_result {
        //     panic!("DB file contains error.");
        //     todo!("Add rollback.");
        // }

        self.table_header.last_row_id.store(row_id, Ordering::SeqCst);
        let table_bytes = self.table_header.to_bytes().unwrap();
        self.io_sync.write_data_seek(SeekFrom::Start(0), &table_bytes).await;
    }

    pub async fn execute_query(
        &self,
        parser_result: ParserResult,
    ) -> io::Result<Option<Vec<Row>>> {
        if let ParserResult::CachedHashIndexes(hash_indexes) = parser_result {
            let mut rows: Vec<Row> = Vec::with_capacity(hash_indexes.len());
            for record in hash_indexes {
                let mut position = record.0;
                let length = record.1;
                rows.push(
                    indexed_row_fetching_file(&self.io_sync, &mut position, length)
                        .await
                        .unwrap(),
                );
            }

            return Ok(Some(rows));
        } else if let ParserResult::QueryEvaluationTokens(evaluation_tokens) = parser_result {
            #[cfg(feature = "enable_index_caching")]
            let hash = evaluation_tokens.query_hash;

            let limit = evaluation_tokens.limit;

            let mut rows: Vec<Row> = if limit < 1024 && limit > 0 {
                Vec::with_capacity(limit as usize)
            } else {
                Vec::default()
            };

            let select_all = evaluation_tokens.select_all;
            let mut evaluation_tokens = evaluation_tokens.tokens;
            let file_length = self.get_current_table_length();

            let chunks_arc_clone = self.in_memory_index.clone();
            let chunks_result = chunks_arc_clone.as_ref();

            let mut current_limit: i64 = 0;

            #[cfg(feature = "enable_index_caching")]
            let mut result_row_vec: Vec<(u64, u32)> = Vec::default();

            let mutated = self.mutated.load(Ordering::Relaxed);

            if let Some(chunks) = chunks_result.as_ref() {
                let column_indexes = evaluation_tokens
                    .iter()
                    .flat_map(|(tokens, _)| tokens.iter())
                    .filter(|x| match **x {
                        Token::Column(_) => true,
                        _ => false,
                    })
                    .map(|x| match *x {
                        Token::Column(column_index) => column_index,
                        _ => panic!(),
                    })
                    .collect_vec();

                #[cfg(feature  = "enable_parallelism")]
                {
                    return process_all_chunks(
                        Arc::new(column_indexes),
                        evaluation_tokens,
                        limit as u64,
                        select_all,
                        mutated,
                        &self.io_sync,
                        Arc::new(chunks.clone()),
                        THREADS,
                        
                        #[cfg(feature = "enable_index_caching")]
                        hash

                    ).await;
                }

                #[allow(unreachable_code)]
                let mut required_columns: Vec<(u32, Column)> = Vec::default();
                let mut token_results: Vec<(bool, Option<Next>)> = Vec::with_capacity(evaluation_tokens.len());

                for chunk in chunks {
                    let buffer = chunk.read_chunk_sync(&mut self.io_sync).await;

                    let mut cursor_vector = CursorVector::new(&buffer);
                    let mut position: u64 = 0;

                    loop {
                        if let Some(prefetch_result) = row_prefetching_cursor(&mut position, &mut cursor_vector, chunk, mutated).unwrap() {
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
                                        let memory_chunk = MEMORY_POOL.acquire(size);

                                        let mut data_buffer = unsafe { memory_chunk.into_vec() };

                                        extent_non_string_buffer(
                                            data_buffer.as_vec_mut(),
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
                
                                        let str_memory_chunk = MEMORY_POOL.acquire(str_length);
                
                                        let mut preset_buffer =
                                            unsafe { str_memory_chunk.into_vec() };
                
                                        let preset_buffer_slice = preset_buffer.as_vec_mut();
 
                                        for (i, byte) in chunk_slice[position as usize..position as usize + str_length as usize].iter().enumerate() {
                                            preset_buffer_slice[i] = *byte;
                                        }
                
                                        position += str_length as u64;
                
                                        drop(preset_buffer);                

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
                                debug!("Evaluating column result.");
                                debug!("Required columns: {:?}", required_columns);
                                debug!("Evaluation tokens: {:?}", evaluation_tokens);

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

                                rows.push(row);

                                #[cfg(feature = "enable_index_caching")]
                                {
                                    result_row_vec.push((
                                        chunk.current_file_position + first_column_index
                                            - 1
                                            - 8
                                            - 4,
                                        prefetch_result.length,
                                    ));
                                }

                                current_limit += 1;

                                if current_limit == limit {
                                    #[cfg(feature = "enable_index_caching")]
                                    {
                                        POSITIONS_CACHE.insert(hash, result_row_vec);
                                    }
                                    return Ok(Some(rows));
                                }

                                position = new_position;
                            } else {
                                position = first_column_index + prefetch_result.length as u64 + 1;
                            }
                        } else {
                            break;
                        }
                    }
                }
            } else {
                let column_indexes = evaluation_tokens
                    .iter()
                    .flat_map(|(tokens, _)| tokens.iter())
                    .filter(|x| match **x {
                        Token::Column(_) => true,
                        _ => false,
                    })
                    .map(|x| match *x {
                        Token::Column(column_index) => column_index,
                        _ => panic!(),
                    })
                    .collect_vec();

                let mut token_results: Vec<(bool, Option<Next>)> = Vec::with_capacity(evaluation_tokens.len());
                let mut required_columns: Vec<(u32, Column)> = Vec::default();
                let mut position = HEADER_SIZE as u64;

                loop {
                    if let Some(prefetch_result) =
                        row_prefetching(&self.io_sync, &mut position, file_length, mutated)
                            .await
                            .unwrap()
                    {
                        let mut column_index_inner = 0;

                        #[cfg(feature = "enable_index_caching")]
                        let first_column_index = position;

                        let mut columns_cursor = self
                            .io_sync
                            .read_data_to_cursor(&mut position, prefetch_result.length + 1)
                            .await;

                        loop {
                            if column_indexes.iter().any(|x| *x == column_index_inner) {
                                let memory_chunk = MEMORY_POOL.acquire(prefetch_result.length + 1);

                                let mut data_buffer = unsafe { memory_chunk.into_vec() };

                                let column_type = columns_cursor.read_u8().unwrap();

                                let db_type = DbType::from_byte(column_type);

                                if db_type == DbType::END {
                                    break;
                                }

                                if db_type != DbType::STRING {
                                    let db_size = db_type.get_size();

                                    let mut preset_buffer = vec![0; db_size as usize];
                                    columns_cursor.read(&mut preset_buffer).unwrap();
                                    data_buffer.as_vec_mut().write(&mut preset_buffer).unwrap();
                                } else {
                                    let str_length =
                                        columns_cursor.read_u32::<LittleEndian>().unwrap();

                                    let mut preset_buffer = vec![0; str_length as usize];
                                    columns_cursor.read(&mut preset_buffer).unwrap();
                                    data_buffer.as_vec_mut().write(&mut preset_buffer).unwrap();
                                }

                                let column = Column::from_chunk(column_type, memory_chunk);

                                required_columns.push((column_index_inner, column));
                            } else {
                                let column_type = columns_cursor.read_u8().unwrap();

                                let db_type = DbType::from_byte(column_type);

                                if db_type == DbType::END {
                                    break;
                                }

                                if db_type != DbType::STRING {
                                    let db_size = db_type.get_size();

                                    columns_cursor
                                        .seek(SeekFrom::Current(db_size as i64))
                                        .unwrap();
                                } else {
                                    let str_length =
                                        columns_cursor.read_u32::<LittleEndian>().unwrap();

                                    columns_cursor
                                        .seek(SeekFrom::Current(str_length as i64))
                                        .unwrap();
                                }
                            }

                            column_index_inner += 1;
                        }

                        let evaluation = if !select_all {
                            debug!("Evaluating column result.");
                            debug!("Required columns: {:?}", required_columns);
                            debug!("Evaluation tokens: {:?}", evaluation_tokens);

                            let eval = evaluate_column_result(
                                &required_columns,
                                &mut evaluation_tokens,
                                &mut token_results,
                            );

                            token_results.clear();
                            required_columns.clear();
                            eval
                        } else {
                            true
                        };

                        if evaluation {
                            let row = columns_cursor_to_row_whole(
                                columns_cursor,
                                prefetch_result.found_id,
                                prefetch_result.length,
                            )
                            .unwrap();

                            rows.push(row);

                            #[cfg(feature = "enable_index_caching")]
                            {
                                result_row_vec
                                    .push((first_column_index - 1 - 8 - 4, prefetch_result.length));
                            }

                            current_limit += 1;

                            if current_limit == limit {
                                #[cfg(feature = "enable_index_caching")]
                                {
                                    POSITIONS_CACHE.insert(hash, result_row_vec);
                                }
                                return Ok(Some(rows));
                            }
                        }
                    } else {
                        break;
                    }
                }
            }

            if rows.len() > 0 {
                return Ok(Some(rows));
            } else {
                return Ok(None);
            }
        } else {
            panic!() //Don't show error.
        }
    }

    /// #### STABILIZED
    /// Deletes the row by the given id.
    pub async fn delete_row_by_id(&mut self, id: u64) -> io::Result<()> {
        loop {
            let table_locked = self.locked.load(Ordering::Relaxed);
            if table_locked {
                continue;
            } else {
                break;
            }
        }

        let file_length = self.get_current_table_length();

        let chunks_arc_clone = self.in_memory_index.clone();

        let chunks_result = chunks_arc_clone.as_ref();

        let mutated = self.table_header.mutated.load(Ordering::Relaxed);

        if let Some(chunks) = chunks_result.as_ref() {
            for chunk in chunks {
                let buffer = chunk.read_chunk_sync(&mut self.io_sync).await;

                let mut cursor_vector = CursorVector::new(&buffer);
                let mut position: u64 = 0;

                loop {
                    if let Some(prefetch_result) =
                        row_prefetching_cursor(&mut position, &mut cursor_vector, chunk, mutated).unwrap()
                    {
                        if id == prefetch_result.found_id {
                            let starting_column_position =
                                position.clone() + chunk.current_file_position as u64;

                            #[cfg(feature = "enable_index_caching")]
                            if let Some(record) = POSITIONS_CACHE.iter().find(|x| {
                                x.1.iter().any(|&(key, _)| {
                                    key == (starting_column_position - 1 - 4 - 8) as u64
                                })
                            }) {
                                POSITIONS_CACHE.invalidate(&record.0);
                            }

                            delete_row_file(starting_column_position, &mut self.io_sync)
                                .await
                                .unwrap();

                            self.mutated.store(true, Ordering::Relaxed);
                            self.table_header.mutated.store(true, Ordering::Relaxed);
                            self.io_sync.write_data(0, &self.table_header.to_bytes().unwrap()).await;

                            return Ok(());
                        } else {
                            //+1 because of the END db type.
                            position += prefetch_result.length as u64 + 1;
                        }
                    } else {
                        break;
                    }
                }
            }
        } else {
            let mut position = HEADER_SIZE as u64;
            loop {
                if let Some(prefetch_result) =
                    row_prefetching(&mut self.io_sync, &mut position, file_length, mutated)
                        .await
                        .unwrap()
                {
                    if id == prefetch_result.found_id {
                        let starting_column_position = position;

                        delete_row_file(starting_column_position, &mut self.io_sync)
                            .await
                            .unwrap();

                            self.mutated.store(true, Ordering::Relaxed);
                            self.table_header.mutated.store(true, Ordering::Relaxed);
                            self.io_sync.write_data(0, &self.table_header.to_bytes().unwrap()).await;

                        return Ok(());
                    } else {
                        //+1 because of the END db type.
                        position += (prefetch_result.length + 1) as u64;
                    }
                } else {
                    break;
                }
            }
        }

        return Ok(());
    }

    /// #### STABILIZED
    /// Rebuilds the in-memory indexes.
    pub async fn rebuild_in_memory_indexes(&mut self) {
        // Wait until the table is not locked
        loop {
            let table_locked = self.locked.load(Ordering::Relaxed);
            if table_locked {
                continue;
            } else {
                break;
            }
        }

        let file_length = self.get_current_table_length();
        if file_length <= HEADER_SIZE as u64 {
            // Empty database, nothing to index
            return;
        }

        // Get table header info for mutation status
        let mutated = self.table_header.mutated.load(Ordering::Relaxed);
        
        // Initialize a temporary storage for the chunks we find
        let mut chunks_vec: Vec<FileChunk> = Vec::with_capacity(32);
        let mut position = HEADER_SIZE as u64;
        let mut current_chunk_start = position;
        let mut current_chunk_size: u32 = 0;
        let mut last_row_id: u64 = 0;

        // Use a larger read buffer (4MB) for efficient scanning
        const SCAN_BUFFER_SIZE: u32 = 4_194_304; // 4MB

        debug!("Starting indexing - file_length: {}", file_length);

        let mut scan_count = 0;
        let start_time = std::time::Instant::now();

        while position < file_length {
            scan_count += 1;
            if scan_count % 10 == 0 {
                debug!("Scan progress: {:.2}% ({}/{})", 
                    (position as f64 / file_length as f64) * 100.0, 
                    position, file_length);
            }
        
            // Calculate how much to read (either our buffer size or remaining file)
            let read_size = std::cmp::min(
                SCAN_BUFFER_SIZE, 
                (file_length - position) as u32
            );
            
            if read_size == 0 {
                break;
            }

            // Read a large chunk in one operation
            let mut data_position = position;
            let buffer = self.io_sync.read_data(&mut data_position, read_size).await;
            
            // Safety check - ensure we got some data
            if buffer.is_empty() {
                debug!("Warning: Empty buffer read at position {}", position);
                position += 1024; // Skip ahead to make progress
                continue;
            }
            
            // Process the buffer to find START markers and row information
            let mut buffer_position: usize = 0;
            let buffer_length = buffer.len();
            let mut rows_found_in_buffer = 0;
            
            while buffer_position + 13 <= buffer_length { // Need at least START + ID + LENGTH
                // Skip empty spaces if table was mutated
                if mutated && buffer_position < buffer_length && buffer[buffer_position] == 0 {
                    // Fast-forward through empty spaces
                    let mut empty_pos = buffer_position;
                    while empty_pos + 8 <= buffer_length && 
                          &buffer[empty_pos..empty_pos+8] == EMPTY_BUFFER.as_slice() {
                        empty_pos += 8;
                    }
                    
                    // If we reached the end of the buffer without finding data
                    if empty_pos + 8 > buffer_length {
                        buffer_position = buffer_length;
                        position += buffer_position as u64;
                        break;
                    }
                    
                    // Find START marker
                    while empty_pos < buffer_length && buffer[empty_pos] != DbType::START.to_byte() {
                        empty_pos += 1;
                    }
                    
                    if empty_pos >= buffer_length {
                        buffer_position = buffer_length;
                        position += buffer_position as u64;
                        break;
                    }
                    
                    buffer_position = empty_pos;
                }

                // Check for START marker
                if buffer_position >= buffer_length || buffer[buffer_position] != DbType::START.to_byte() {
                    // If no START marker is found, move to next byte and try again
                    buffer_position += 1;
                    continue;
                }

                // We found a START marker, extract ID and LENGTH
                if buffer_position + 13 > buffer_length {
                    // Not enough data in buffer, move position to the marker and read again
                    position += buffer_position as u64;
                    break;
                }

                // Extract row ID and length
                let row_id_bytes = &buffer[buffer_position+1..buffer_position+9];
                let length_bytes = &buffer[buffer_position+9..buffer_position+13];
                let row_id = LittleEndian::read_u64(row_id_bytes);
                let row_length = LittleEndian::read_u32(length_bytes);
                
                // Validate the row length is reasonable
                if row_length > CHUNK_SIZE || row_length == 0 {
                    // This doesn't look like a valid row, skip this byte
                    buffer_position += 1;
                    continue;
                }
                
                // Calculate full row size including header and footer
                let full_row_size = (1 + 8 + 4 + row_length + 1) as u32; // START + ID + LEN + CONTENT + END
                rows_found_in_buffer += 1;
                
                // Update the last row ID if this one is larger
                if row_id > last_row_id {
                    last_row_id = row_id;
                }
                
                // Check if current row would make chunk exceed CHUNK_SIZE
                if current_chunk_size + full_row_size > CHUNK_SIZE {
                    // Add the current chunk without this row
                    if current_chunk_size > 0 {
                        chunks_vec.push(FileChunk {
                            current_file_position: current_chunk_start,
                            chunk_size: current_chunk_size,
                            next_row_id: row_id,
                        });
                    }
                    
                    // Start a new chunk with this row
                    current_chunk_start = position + buffer_position as u64;
                    current_chunk_size = full_row_size;
                } else {
                    // Add to current chunk
                    current_chunk_size += full_row_size;
                }
                
                // Move to next row - ensure we don't exceed the buffer
                if buffer_position + full_row_size as usize <= buffer_length {
                    buffer_position += full_row_size as usize;
                } else {
                    // Row extends beyond buffer, skip to next read
                    position += buffer_position as u64;
                    break;
                }
            }
            
            // Update position for next read
            if buffer_position > 0 {
                position += buffer_position as u64;
            } else {
                // If we made no progress, skip ahead to avoid an infinite loop
                position += 1024; // Skip ahead by 1KB 
                debug!("Warning: No progress made in buffer scan, skipping ahead");
            }

            if rows_found_in_buffer == 0 && buffer_length > 13 {
                // No rows found in this buffer, which is unexpected
                debug!("Warning: No rows found in buffer of size {}", buffer_length);
            }
        }
        
        // Always add the final chunk if it has data
        if current_chunk_size > 0 {
            chunks_vec.push(FileChunk {
                current_file_position: current_chunk_start,
                chunk_size: file_length as u32 - current_chunk_start as u32,
                next_row_id: last_row_id,
            });
        }

        // Display indexing results
        let elapsed = start_time.elapsed();
        debug!("Indexing complete in {:.2?}", elapsed);
        debug!("Indexed from {} to {} bytes ({:.2}% of file)", 
            HEADER_SIZE, position, (position as f64 / file_length as f64) * 100.0);
        debug!("Found {} chunks with last row_id {}", chunks_vec.len(), last_row_id);
        
        // Debug: Print first and last chunk info
        if !chunks_vec.is_empty() {
            let first = &chunks_vec[0];
            let last = &chunks_vec[chunks_vec.len() - 1];
            debug!("First chunk: start={}, size={}, next_row_id={}", 
                first.current_file_position, first.chunk_size, first.next_row_id);
            debug!("Last chunk: start={}, size={}, next_row_id={}", 
                last.current_file_position, last.chunk_size, last.next_row_id);
        }

        // Update the in_memory_index
        if chunks_vec.is_empty() {
            self.in_memory_index = Arc::new(None);
        } else {
            self.in_memory_index = Arc::new(Some(chunks_vec));
        }
        
        // Update last_row_id in current_row_id atomic if needed
        let current_last_row_id = self.current_row_id.load(Ordering::Relaxed);
        if last_row_id > current_last_row_id {
            self.current_row_id.store(last_row_id, Ordering::Relaxed);
            
            // Update the table header
            self.table_header.last_row_id.store(current_last_row_id, Ordering::Relaxed);
            self.io_sync.write_data(0, &self.table_header.to_bytes().unwrap()).await;
        }
    }
    
    /// #### STABILIZED
    /// Updates the row by the given id.
    pub async fn update_row_by_id(&mut self, id: u64, row: InsertOrUpdateRow) {
        loop {
            let table_locked = self.locked.load(Ordering::Relaxed);
            if table_locked {
                continue;
            } else {
                break;
            }
        }

        let columns_len = row.columns_data.len();

        //START (1) + END (1) + u64 ID (8) + u32 LENGTH (4) + VEC SIZE
        let mut buffer: Vec<u8> = Vec::with_capacity(1 + 1 + 8 + 4 + columns_len);

        let update_row_size = buffer.capacity() as u32;

        // Get current file length using atomic
        let current_file_len = self.current_file_length.load(Ordering::Relaxed);
        let current_file_len = if current_file_len == 0 {
            HEADER_SIZE as u64
        } else {
            current_file_len
        };

        let file_length = self.get_current_table_length();
        let mut position = HEADER_SIZE as u64;

        loop {           
            let mutated = self.table_header.mutated.load(Ordering::Relaxed);

            if let Some(prefetch_result) =
                row_prefetching(&mut self.io_sync, &mut position, file_length, mutated)
                    .await
                    .unwrap()
            {
                if prefetch_result.found_id == id {
                    // START (1) + END (1) + u64 ID (8) + u32 LENGTH (4) + VEC SIZE
                    let current_row_len = (1 + 1 + 8 + 4 + prefetch_result.length) as u32;
                    if update_row_size == current_row_len {
                        //DbType START
                        buffer.push(254);

                        WriteBytesExt::write_u64::<LittleEndian>(
                            &mut buffer,
                            prefetch_result.found_id,
                        )
                        .unwrap();
                        WriteBytesExt::write_u32::<LittleEndian>(&mut buffer, columns_len as u32)
                            .unwrap();

                        let mut columns_data = row.columns_data.clone();

                        buffer.append(&mut columns_data);

                        //DbType END
                        buffer.push(255);

                        let new_position = position - 8 - 4 - 1;

                        self.io_sync.write_data_unsync(new_position, &buffer).await;
                        let verify_result = self.io_sync.verify_data_and_sync(new_position, &buffer).await;

                        #[allow(unreachable_code)]
                        if !verify_result {
                            panic!("DB file contains error.");
                            todo!("Add rollback.");
                        }

                        break;
                    } else if update_row_size < current_row_len {
                        //DbType START
                        buffer.push(254);

                        WriteBytesExt::write_u64::<LittleEndian>(
                            &mut buffer,
                            prefetch_result.found_id,
                        )
                        .unwrap();
                        WriteBytesExt::write_u32::<LittleEndian>(&mut buffer, columns_len as u32)
                            .unwrap();

                        let mut columns_data = row.columns_data.clone();

                        buffer.append(&mut columns_data);

                        //DbType END
                        buffer.push(255);

                        let new_position = position - 8 - 4 - 1;

                        self.io_sync.write_data_unsync(new_position, &buffer).await;
                        let verify_result = self.io_sync.verify_data_and_sync(new_position, &buffer).await;

                        #[allow(unreachable_code)]
                        if !verify_result {
                            panic!("DB file contains error.");
                            todo!("Add rollback.");
                        }

                        let empty_buffer =
                            vec![0 as u8; (current_row_len - update_row_size) as usize];

                        self.io_sync.write_data_unsync(
                            new_position + update_row_size as u64,
                            &empty_buffer,
                        ).await;
                        let clean_up_verify_result = self.io_sync.verify_data_and_sync(
                            new_position + update_row_size as u64,
                            &empty_buffer,
                        ).await;

                        #[allow(unreachable_code)]
                        if !clean_up_verify_result {
                            panic!("DB file contains error.");
                            todo!("Add rollback.");
                        }

                        break;
                    } else {
                        let row_id = self.update_eof_and_id(update_row_size as u64).await;

                        //DbType START
                        buffer.push(254);

                        WriteBytesExt::write_u64::<LittleEndian>(&mut buffer, row_id).unwrap();
                        WriteBytesExt::write_u32::<LittleEndian>(&mut buffer, columns_len as u32)
                            .unwrap();

                        let mut columns_data = row.columns_data.clone();

                        buffer.append(&mut columns_data);

                        //DbType END
                        buffer.push(255);

                        self.io_sync.append_data_unsync(&buffer).await;
                        let verify_result = self.io_sync.verify_data_and_sync(current_file_len, &buffer).await;

                        #[allow(unreachable_code)]
                        if !verify_result {
                            panic!("DB file contains error.");
                            todo!("Add rollback.");
                        }

                        let empty_buffer = vec![0 as u8; (current_row_len) as usize];

                        let new_position = position - 8 - 4 - 1;

                        self.io_sync.write_data_unsync(new_position, &empty_buffer).await;
                        let clean_up_verify_result = self
                            .io_sync
                            .verify_data_and_sync(new_position, &empty_buffer).await;

                        #[allow(unreachable_code)]
                        if !clean_up_verify_result {
                            panic!("DB file contains error.");
                            todo!("Add rollback.");
                        }

                        let chunks_arc_clone = self.in_memory_index.clone();

                        let mut new_chunks = if let Some(existing_chunks) = chunks_arc_clone.as_ref() {
                            existing_chunks.clone()
                        } else {
                            Vec::with_capacity(1)
                        };
                        
                        if !new_chunks.is_empty() {
                            let last_chunk = new_chunks.last_mut().unwrap();
                            
                            if last_chunk.chunk_size + update_row_size <= CHUNK_SIZE as u32 {
                                last_chunk.chunk_size += update_row_size;
                                last_chunk.next_row_id += 1;
                            } else {
                                new_chunks.push(FileChunk {
                                    current_file_position: current_file_len,
                                    chunk_size: update_row_size,
                                    next_row_id: row_id,
                                });
                            }
                        } else {
                            new_chunks.push(FileChunk {
                                current_file_position: current_file_len,
                                chunk_size: update_row_size,
                                next_row_id: row_id,
                            });
                        }
                        
                        self.in_memory_index = Arc::new(Some(new_chunks));

    
                        self.table_header.mutated.store(true, Ordering::Relaxed);
                        self.io_sync.write_data(0, &self.table_header.to_bytes().unwrap()).await;

                        break;
                    }
                } else {
                    position += prefetch_result.length as u64 + 1;
                }
            }
        }
    }

    /// #### STABILIZED
    /// This function will remove all empty spaces from the table and squash the ROW ID pool.
    /// So deleted row ID(3) and following rows ID(4), ID(5), ID(X) will become row ID(3), ID(4), ID(X - 1)
    /// Vacuuming currently locks the table and inserts, updates, rebuild cache indexes, and deletes will be waiting until the vacuum is done.
    pub async fn vacuum_table(&mut self) {
        // Acquire lock on the table
        loop {
            let table_locked = self.locked.compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed).is_ok();
            if !table_locked {
                continue;
            } else {
                break;
            }
        }

        let file_length = self.get_current_table_length();
        
        // Early return for empty tables
        if file_length <= HEADER_SIZE as u64 {
            self.locked.store(false, Ordering::Relaxed);
            return;
        }
        
        const BUFFER_THRESHOLD: usize = 8 * 1024 * 1024; // 8 MB buffer threshold
        
        // Create temporary table
        let temp_io_sync = self.io_sync.create_temp().await;
        let mut temp_table = Table::init(temp_io_sync, false, false).await.unwrap();
        
        // Batch collection for buffering rows before writing to disk
        let mut row_batch: Vec<InsertOrUpdateRow> = Vec::new();
        let mut current_batch_size: usize = 0;
        let mut total_rows_processed: usize = 0;
        
        // Get table mutation status
        let mutated = self.table_header.mutated.load(Ordering::Relaxed);
        
        // Check if in-memory indexes are available
        let chunks_arc_clone = self.in_memory_index.clone();
        
        if let Some(chunks) = chunks_arc_clone.as_ref() {
            debug!("Vacuum: processing with in-memory chunks ({} chunks found)", chunks.len());
            
            for (chunk_idx, chunk) in chunks.iter().enumerate() {
                // Read an entire chunk into memory at once
                let buffer = chunk.read_chunk_sync(&mut self.io_sync).await;
                let mut cursor_vector = CursorVector::new(&buffer);
                let mut position: u64 = 0;
                
                loop {
                    if let Some(prefetch_result) = row_prefetching_cursor(&mut position, &mut cursor_vector, chunk, mutated).unwrap() {
                        // Use the in-memory cursor data directly rather than re-reading from disk
                        let first_column_index = position;
                        let mut cursor = &mut cursor_vector.cursor;
                        
                        let row = read_row_cursor_whole(
                            first_column_index,
                            prefetch_result.found_id,
                            prefetch_result.length,
                            &mut cursor,
                        ).unwrap();
                        
                        // Add row to memory buffer
                        let row_size = row.columns_data.len();
                        row_batch.push(InsertOrUpdateRow {
                            columns_data: row.columns_data,
                        });
                        current_batch_size += row_size;
                        total_rows_processed += 1;
                        
                        // Flush buffer when threshold is reached
                        if current_batch_size >= BUFFER_THRESHOLD {
                            debug!("Vacuum: flushing buffer - chunk {}/{} - {} rows processed", 
                                   chunk_idx + 1, chunks.len(), total_rows_processed);
                            
                            // Write batch to temp table
                            for row_to_insert in row_batch.drain(..) {
                                temp_table.insert_row_unsync(row_to_insert).await;
                            }
                            current_batch_size = 0;
                        }
                        
                        // Move to next row
                        position += prefetch_result.length as u64 + 1;
                    } else {
                        break;
                    }
                }
            }
        } else {
            debug!("Vacuum: processing without in-memory indexes");
            
            // Process the file sequentially when no in-memory indexes exist
            let mut position: u64 = HEADER_SIZE as u64;
            let mut last_log_pos: u64 = position;
            
            loop {
                if let Some(prefetch_result) = row_prefetching(&mut self.io_sync, &mut position, file_length, mutated)
                    .await
                    .unwrap()
                {
                    // Read the entire row at once
                    let row = read_row_columns(
                        &mut self.io_sync,
                        position,
                        prefetch_result.found_id,
                        prefetch_result.length + 1,
                    )
                    .await
                    .unwrap();
                    
                    // Add to memory buffer
                    let row_size = row.columns_data.len();
                    row_batch.push(InsertOrUpdateRow {
                        columns_data: row.columns_data,
                    });
                    current_batch_size += row_size;
                    total_rows_processed += 1;
                    
                    // Flush buffer when threshold is reached
                    if current_batch_size >= BUFFER_THRESHOLD {
                        let progress = ((position - HEADER_SIZE as u64) as f64 / 
                                      (file_length - HEADER_SIZE as u64) as f64) * 100.0;
                        
                        debug!("Vacuum: flushing buffer - {:.2}% complete ({} rows, {:.2}MB processed)",
                               progress, total_rows_processed,
                               (position - last_log_pos) as f64 / 1_048_576.0);
                        
                        // Write batch to temp table
                        for row_to_insert in row_batch.drain(..) {
                            temp_table.insert_row_unsync(row_to_insert).await;
                        }
                        current_batch_size = 0;
                        last_log_pos = position;
                    }
                    
                    // Move to next row
                    position += prefetch_result.length as u64 + 1;
                } else {
                    break;
                }
            }
        }
        
        // Flush any remaining rows in the buffer
        if !row_batch.is_empty() {
            debug!("Vacuum: flushing final buffer - {} rows total", total_rows_processed);
            for row_to_insert in row_batch.drain(..) {
                temp_table.insert_row_unsync(row_to_insert).await;
            }
        }

        debug!("Vacuum: swapping temporary table with main table");
        
        // Swap the temporary table with the main one
        self.io_sync.swap_temp(&mut temp_table.io_sync).await;
        
        drop(temp_table);

        // Reset the in-memory index
        self.in_memory_index = Arc::new(None);

        // Release the table lock and update state
        loop {
            let table_locked = self.locked.compare_exchange(true, false, Ordering::Relaxed, Ordering::Relaxed).is_ok();
            if !table_locked {
                continue;
            } else {
                self.mutated.store(false, Ordering::Relaxed);
                break;
            }
        }

        self.table_header.mutated.store(false, Ordering::Relaxed);
        self.io_sync.write_data(0, &self.table_header.to_bytes().unwrap()).await;
        
        debug!("Vacuum completed: processed {} rows", total_rows_processed);
    }
}
