use std::{
    fmt::Display, io::{self, Read, Seek, SeekFrom}, sync::Arc
};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use itertools::Itertools;
use tokio::sync::{RwLock, RwLockWriteGuard};

use crate::{core::helpers::delete_row_file, CHUNK_SIZE, HEADER_SIZE, POSITIONS_CACHE};
use super::{
    super::rql::{
        models::Token, parser::ParserResult, tokenizer::evaluate_column_result
    }, column::{Column, DowngradeType}, db_type::DbType, helpers::{
        add_in_memory_index, 
        add_last_in_memory_index, 
        columns_cursor_to_row, 
        indexed_row_fetching_file, 
        read_row_columns, 
        read_row_cursor, 
        row_prefetching, 
        row_prefetching_cursor
    }, row::{InsertOrUpdateRow, Row}, storage_providers::traits::IOOperationsSync, support_types::FileChunk, table_header::TableHeader
};

pub struct Table<S: IOOperationsSync> {
    pub(crate) io_sync: S,
    pub(crate) table_header: Arc<RwLock<TableHeader>>,
    pub(crate) in_memory_index: Arc<RwLock<Option<Vec<FileChunk>>>>,
    pub(crate) current_file_length: Arc<RwLock<u64>>,
    pub(crate) current_row_id: Arc<RwLock<u64>>,
    pub(crate) immutable: bool,
    pub(crate) locked: Arc<RwLock<bool>>
}

unsafe impl<S: IOOperationsSync> Send for Table<S> {}
unsafe impl<S: IOOperationsSync> Sync for Table<S> {}

impl<S: IOOperationsSync> Table<S> {
    /// #### STABILIZED
    /// Initializes a new table. compressed and immutable are not implemented yet.
    pub async fn init(
        mut io_sync: S,
        compressed: bool,
        immutable: bool) -> io::Result<Table<S>> {
        let table_file_len = io_sync.get_len().await;

        if table_file_len >= HEADER_SIZE as u64 {
            
            let buffer = io_sync.read_data(&mut 0, HEADER_SIZE as u32).await;
            let mut table_header = TableHeader::from_buffer(buffer).unwrap();

            let last_row_id: u64 = table_header.last_row_id;

            table_header.total_file_length = table_file_len;

            let table = Table {
                io_sync: io_sync,
                table_header: Arc::new(RwLock::const_new(table_header)),
                in_memory_index: Arc::new(RwLock::const_new(None)),
                current_file_length: Arc::new(RwLock::const_new(table_file_len)),
                current_row_id: Arc::new(RwLock::const_new(last_row_id)),
                immutable: immutable,
                locked: Arc::new(RwLock::const_new(false))
            };

            Ok(table)
        } else {
            let table_header = TableHeader::new(HEADER_SIZE as u64, 0, compressed, 0, 0, false);

            // Serialize the header and write it to the file
            let header_bytes = table_header.to_bytes().unwrap();

            io_sync.write_data(0, &header_bytes);

            let table = Table {
                io_sync: io_sync,
                table_header: Arc::new(RwLock::const_new(table_header)),
                in_memory_index: Arc::new(RwLock::const_new(None)),
                current_file_length: Arc::new(RwLock::const_new(table_file_len)),
                current_row_id: Arc::new(RwLock::const_new(0)),
                immutable: immutable,
                locked: Arc::new(RwLock::const_new(false))
            };

            Ok(table)
        }
    }

/// #### STABILIZED
    /// Initializes a new table. compressed and immutable are not implemented yet.
    pub(crate) async fn init_inner(
        mut io_sync: S,
        compressed: bool,
        immutable: bool) -> io::Result<Table<S>> {
        let table_file_len = io_sync.get_len().await;

        if table_file_len >= HEADER_SIZE as u64 {
            
            let buffer = io_sync.read_data(&mut 0, HEADER_SIZE as u32).await;
            let mut table_header = TableHeader::from_buffer(buffer).unwrap();

            let last_row_id: u64 = table_header.last_row_id;

            table_header.total_file_length = table_file_len;

            let table = Table {
                io_sync: io_sync,
                table_header: Arc::new(RwLock::const_new(table_header)),
                in_memory_index: Arc::new(RwLock::const_new(None)),
                current_file_length: Arc::new(RwLock::const_new(table_file_len)),
                current_row_id: Arc::new(RwLock::const_new(last_row_id)),
                immutable: immutable,
                locked: Arc::new(RwLock::const_new(false))
            };

            Ok(table)
        } else {
            let table_header = TableHeader::new(HEADER_SIZE as u64, 0, compressed, 0, 0, false);

            // Serialize the header and write it to the file
            let header_bytes = table_header.to_bytes().unwrap();

            io_sync.write_data(0, &header_bytes);

            let table = Table {
                io_sync: io_sync,
                table_header: Arc::new(RwLock::const_new(table_header)),
                in_memory_index: Arc::new(RwLock::const_new(None)),
                current_file_length: Arc::new(RwLock::const_new(table_file_len)),
                current_row_id: Arc::new(RwLock::const_new(0)),
                immutable: immutable,
                locked: Arc::new(RwLock::const_new(false))
            };

            Ok(table)
        }
    }

    /// #### STABILIZED
    fn get_current_table_length(&mut self) -> u64 {
        let file_length = loop {
            if let Ok(len) = self.current_file_length.try_read() {
                break *len;
            }
        };

        return file_length;
    }

    /// #### STABILIZED
    async fn update_eof_and_id(&mut self, buffer_size: u64) -> u64 {    
        let end_of_file_c = self.current_file_length.clone();
        let last_row_id_c = self.current_row_id.clone();

        let mut end_of_file : RwLockWriteGuard<u64> = loop {
            let result = end_of_file_c.try_write();
            if let Ok(mut end_of_file) = result {
                if *end_of_file == 0 {
                    *end_of_file = HEADER_SIZE as u64;
                    break end_of_file;
                }
                break end_of_file;
            } 
        };

        let mut last_row_id : RwLockWriteGuard<u64> = loop {
            let result = last_row_id_c.try_write();
            if let Ok(last_row_id) = result  {
                break last_row_id;
            } 
        };

        *end_of_file = *end_of_file + buffer_size;
        *last_row_id = *last_row_id + 1;

        let last_row_id = *last_row_id;

        return last_row_id;
    }

    /// #### STABILIZED
    /// Inserts a new row.
    pub async fn insert_row(&mut self, row: InsertOrUpdateRow) {
        loop {
            let table_locked = self.locked.read().await;
            if *table_locked {
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

        self.io_sync.append_data(&buffer);
        // let verify_result = self.io_sync.verify_data_and_sync(current_file_len, &buffer);

        // #[allow(unreachable_code)]
        // if !verify_result {
        //     panic!("DB file contains error.");
        //     todo!("Add rollback.");
        // }

        let table_header = self.table_header.clone();

        let mut table_header_w: RwLockWriteGuard<TableHeader> = loop {
            let result = table_header.try_write();
            if let Ok(table_header) = result  {
                break table_header;
            } 
        };

        table_header_w.last_row_id = row_id;

        let new_header = table_header_w.to_bytes().unwrap();

        self.io_sync.write_data(0, &new_header);
    }

    /// #### STABILIZED
    /// Get first row by columns value.
    pub async fn first_or_default_by_id(&mut self, id: u64) -> io::Result<Option<Row>> {
        let file_length = self.get_current_table_length();

        let chunks_arc_clone = self.in_memory_index.clone();

        let chunks_result = loop {
            let result = chunks_arc_clone.try_read();
            if let Ok(in_memory_index) = result {
                break in_memory_index;
            } 
        };

        if let Some(chunks) = chunks_result.as_ref() {
            for chunk in chunks {
                let mut cursor = chunk.read_chunk_sync(&mut self.io_sync).await;

                loop {
                    if let Some(prefetch_result) = row_prefetching_cursor(&mut cursor, chunk).unwrap() {
                        if id == prefetch_result.found_id {
                            let row = read_row_cursor(
                                cursor.stream_position().unwrap(), 
                                prefetch_result.found_id,
                                prefetch_result.length,
                                &mut cursor).unwrap();
        
                            return Ok(Some(row));
                        } else {
                            //+1 because of the END db type.
                            cursor.seek(SeekFrom::Current((prefetch_result.length + 1) as i64)).unwrap();
                        }
                    } else {
                        break;
                    }
                }
            }
        } else {
            let mut position = HEADER_SIZE as u64;
            loop {       
                if let Some(prefetch_result) = row_prefetching(
                    &mut self.io_sync,
                    &mut position,
                    file_length).await.unwrap() {
                    
                    if id == prefetch_result.found_id {
                        //+1 because of the END db type.
                        let row = read_row_columns(
                            &mut self.io_sync,
                            position,
                            prefetch_result.found_id,
                            prefetch_result.length + 1).await.unwrap();

                        return Ok(Some(row));
                    } else {
                        //+1 because of the END db type.
                        position += (prefetch_result.length + 1) as u64;
                    }
                } else {
                    break;
                }
            }
        }

        return Ok(None);
    }

    /// #### STABILIZED
    /// Get first row by columns value (EQUALS operations performed).
    pub async fn first_or_default_by_column<T>(&mut self, column_index: u32, value: T) -> io::Result<Option<Row>> 
    where
        T: Display {
        let file_length = self.get_current_table_length();
        let value_column = Column::new(value).unwrap();

        let chunks_arc_clone = self.in_memory_index.clone();

        let chunks_result = loop {
            let result = chunks_arc_clone.try_read();
            if let Ok(in_memory_index) = result {
                break in_memory_index;
            } 
        };

        if let Some(chunks) = chunks_result.as_ref() {
            for chunk in chunks {
                let mut cursor = chunk.read_chunk_sync(&mut self.io_sync).await;

                loop {
                    if let Some(prefetch_result) = row_prefetching_cursor(&mut cursor, chunk).unwrap() {
                            
                        let mut index = 0;
                        let first_column_index = cursor.stream_position().unwrap();

                        loop {  
                            if column_index == index {
                                index += 1;

                                let column_type = cursor.read_u8().unwrap();

                                let db_type = DbType::from_byte(column_type);
        
                                if db_type == DbType::END {
                                    break;
                                }
                                
                                let mut data_buffer = Vec::default();

                                if db_type != DbType::STRING {
                                    let db_size = db_type.get_size();
    
                                    let mut preset_buffer = vec![0; db_size as usize];
                                    cursor.read(&mut preset_buffer).unwrap();
                                    data_buffer.append(&mut preset_buffer);
                                } else {
                                    let str_length = cursor.read_u32::<LittleEndian>().unwrap();
                                    let mut preset_buffer = vec![0; str_length as usize];
                                    cursor.read(&mut preset_buffer).unwrap();
                                    data_buffer.append(&mut preset_buffer);
                                }
                                
                                let column = Column::from_raw(column_type, data_buffer, None);

                                if value_column.equals(&column) {
                                    let row = read_row_cursor(
                                        first_column_index, 
                                        prefetch_result.found_id,
                                        prefetch_result.length,
                                        &mut cursor).unwrap();
        
                                    return Ok(Some(row));
                                }
                            } else {
                                index += 1;

                                let column_type = cursor.read_u8().unwrap();

                                let db_type = DbType::from_byte(column_type);

                                if db_type == DbType::END {
                                    break;
                                }

                                if db_type != DbType::STRING {
                                    let db_size = db_type.get_size();
    
                                    cursor.seek(SeekFrom::Current(db_size as i64)).unwrap();
                                } else {
                                    let str_length = cursor.read_u32::<LittleEndian>().unwrap();
                                    cursor.seek(SeekFrom::Current(str_length as i64)).unwrap();
                                }
                            }
                        }
                    } else {
                        break;
                    }
                }
            }
        } else {
            let mut position = HEADER_SIZE as u64;
            loop {
                if let Some(prefetch_result) = row_prefetching(
                    &mut self.io_sync,
                    &mut position,
                    file_length).await.unwrap() {

                    let mut column_index_inner = 0;

                    let mut columns_cursor = self.io_sync.read_data_to_cursor(
                        &mut position, 
                        prefetch_result.length + 1).await;

                    loop {
                        if column_index == column_index_inner {
                            column_index_inner += 1;

                            let column_type =  columns_cursor.read_u8().unwrap();
                          
                            let db_type = DbType::from_byte(column_type);

                            if db_type == DbType::END {
                                break;
                            }

                            let mut data_buffer = Vec::default();

                            if db_type != DbType::STRING {
                                let db_size = db_type.get_size();
                                let mut preset_buffer = vec![0; db_size as usize];
                                columns_cursor.read(&mut preset_buffer).unwrap();
                                data_buffer.append(&mut preset_buffer);
                            } else {
                                let str_length = columns_cursor.read_u32::<LittleEndian>().unwrap();
                                let mut preset_buffer = vec![0; str_length as usize];
                                columns_cursor.read(&mut preset_buffer).unwrap();
                                data_buffer.append(&mut preset_buffer);
                            }
                            
                            let column = Column::from_raw(column_type, data_buffer.clone(), None);

                            if value_column.equals(&column) {
                                let row = columns_cursor_to_row(
                                    columns_cursor, 
                                    prefetch_result.found_id,
                                    prefetch_result.length).unwrap();

                                return Ok(Some(row));
                            } else {
                                continue;
                            }
                        } else {
                            column_index_inner += 1;

                            let column_type = columns_cursor.read_u8().unwrap();
                           
                            let db_type = DbType::from_byte(column_type);

                            if db_type == DbType::END {
                                break;
                            }

                            if db_type != DbType::STRING {
                                let db_size = db_type.get_size();

                                columns_cursor.seek(SeekFrom::Current(db_size as i64)).unwrap();
                               
                            } else {
                                let str_length = columns_cursor.read_u32::<LittleEndian>().unwrap();
                               
                                columns_cursor.seek(SeekFrom::Current(str_length as i64)).unwrap();
                            }
                        }
                    }
                } else {
                    break;
                }

            }
        }

        return Ok(None);    
    }
 
    /// #### STABILIZED
    /// Get first row by query.
    pub async fn first_or_default_by_query(&mut self, parser_result: ParserResult) -> io::Result<Option<Row>> {
        if let ParserResult::HashIndexes(hash_indexes) = parser_result {
            for record in hash_indexes {
                let mut position = record.0;
                let length = record.1;
                return Ok(Some(indexed_row_fetching_file(
                    &mut self.io_sync,
                    &mut position,
                    length).await.unwrap()));
            }
            panic!("hash_indexes is empty.");
        } else if let ParserResult::EvaluationTokens(evaluation_tokens) = parser_result {

            let hash = evaluation_tokens.query_hash;
            let evaluation_tokens = evaluation_tokens.tokens;

            let file_length = self.get_current_table_length();
    
            let chunks_arc_clone = self.in_memory_index.clone();

            let chunks_result = loop {
                let result = chunks_arc_clone.try_read();
                if let Ok(in_memory_index) = result {
                    break in_memory_index;
                } 
            };

            if let Some(chunks) = chunks_result.as_ref() {
                let mut required_columns: Vec<(u32, Column)> = Vec::default();

                let column_indexes = evaluation_tokens.iter()
                    .flat_map(|(tokens, _)| tokens.iter())
                    .filter(|x|  match **x {
                        Token::Column(_) => true,
                        _ => false
                    }).map(|x| match *x {
                        Token::Column(column_index) => column_index,
                        _ => panic!()
                    }).collect_vec();

                for chunk in chunks {
                    let mut cursor = chunk.read_chunk_sync(&mut self.io_sync).await;
                    
                    let mut data_buffer = Vec::default();
        
                    loop {
                        if let Some(prefetch_result) = row_prefetching_cursor(&mut cursor, chunk).unwrap() {
                        
                            let mut current_column_index: u32 = 0;
                            let first_column_index = cursor.stream_position().unwrap();

                            loop {  

                                if column_indexes.iter().any(|x| *x == current_column_index) {
                                    let column_type = cursor.read_u8().unwrap();
        
                                    let db_type = DbType::from_byte(column_type);
        
                                    if db_type == DbType::END {
                                        break;
                                    }
        
                                    if db_type != DbType::STRING {
                                        let db_size = db_type.get_size();
        
                                        let mut preset_buffer = vec![0; db_size as usize];
                                        cursor.read(&mut preset_buffer).unwrap();
                                        data_buffer.append(&mut preset_buffer);
                                    } else {
                                        let str_length = cursor.read_u32::<LittleEndian>().unwrap();
        
                                        let mut preset_buffer = vec![0; str_length as usize];
                                        cursor.read(&mut preset_buffer).unwrap();
                                        data_buffer.append(&mut preset_buffer);
                                    }
                                    
                                    let type_byte = db_type.to_byte();
                                        
                                    let column = if type_byte >= 1 && type_byte <= 10 {
                                        Column::from_raw(column_type, data_buffer.to_vec(), Some(DowngradeType::I128))
                                    } else if type_byte >= 11 && type_byte <= 12 {
                                        Column::from_raw(column_type, data_buffer.to_vec(), Some(DowngradeType::F64))
                                    } else {
                                        Column::from_raw(column_type, data_buffer.to_vec(), None)
                                    };
        
                                    required_columns.push((current_column_index, column));
                                } else {
                                    let column_type = cursor.read_u8().unwrap();
        
                                    let db_type = DbType::from_byte(column_type);
        
                                    if db_type == DbType::END {
                                        break;
                                    }
        
                                    if db_type != DbType::STRING {
                                        let db_size = db_type.get_size();
                                        cursor.seek(SeekFrom::Current(db_size as i64)).unwrap();
                                    } else {
                                        let str_length = cursor.read_u32::<LittleEndian>().unwrap();
                                        cursor.seek(SeekFrom::Current(str_length as i64)).unwrap();
                                    }
                                }

                                current_column_index += 1;
                                data_buffer.clear();
                            }

                            let evaluation = evaluate_column_result(&required_columns, &evaluation_tokens);
                            required_columns.clear();

                            if evaluation {   
                                let row = read_row_cursor(
                                    first_column_index, 
                                    prefetch_result.found_id,
                                    prefetch_result.length,
                                    &mut cursor).unwrap();

                                #[cfg(feature = "enable_index_caching")]
                                {
                                    let mut row_vec: Vec<(u64, u32)> = Vec::with_capacity(1);
                                    row_vec.push((chunk.current_file_position + first_column_index - 1 - 8 - 4, prefetch_result.length));
                                    POSITIONS_CACHE.insert(hash, row_vec);
                                }
                                
                                return Ok(Some(row));
                            }
                        } else {
                            break;
                        }
                    }
                }
            } else {
                let column_indexes = evaluation_tokens.iter()
                    .flat_map(|(tokens, _)| tokens.iter())
                    .filter(|x|  match **x {
                        Token::Column(_) => true,
                        _ => false
                    }).map(|x| match *x {
                        Token::Column(column_index) => column_index,
                        _ => panic!()
                    }).collect_vec();

                let mut required_columns: Vec<(u32, Column)> = Vec::default();
                let mut position = HEADER_SIZE as u64;

                loop {
                    if let Some(prefetch_result) = row_prefetching(
                        &mut self.io_sync,
                        &mut position,
                        file_length).await.unwrap() {
    
                        let mut column_index_inner = 0;

                        let first_column_index = position + 4 + 8 + 1;

                        let mut columns_cursor = self.io_sync.read_data_to_cursor(
                            &mut position, 
                            prefetch_result.length + 1).await;
    
                        loop {
                            if column_indexes.iter().any(|x| *x == column_index_inner) {
                                let column_type = columns_cursor.read_u8().unwrap();

                                let db_type = DbType::from_byte(column_type);

                                if db_type == DbType::END {
                                    break;
                                }

                                let mut data_buffer = Vec::default();

                                if db_type != DbType::STRING {
                                    let db_size = db_type.get_size();

                                    let mut preset_buffer = vec![0; db_size as usize];
                                    columns_cursor.read(&mut preset_buffer).unwrap();
                                    data_buffer.append(&mut preset_buffer);
                                } else {
                                    let str_length = columns_cursor.read_u32::<LittleEndian>().unwrap();

                                    let mut preset_buffer = vec![0; str_length as usize];
                                    columns_cursor.read(&mut preset_buffer).unwrap();
                                    data_buffer.append(&mut preset_buffer);
                                }
                                
                                let type_byte = db_type.to_byte();
                                    
                                let column = if type_byte >= 1 && type_byte <= 10 {
                                    Column::from_raw(column_type, data_buffer, Some(DowngradeType::I128))
                                } else if type_byte >= 11 && type_byte <= 12 {
                                    Column::from_raw(column_type, data_buffer, Some(DowngradeType::F64))
                                } else {
                                    Column::from_raw(column_type, data_buffer, None)
                                };

                                required_columns.push((column_index_inner, column));
                            } else {
                                let column_type = columns_cursor.read_u8().unwrap();

                                let db_type = DbType::from_byte(column_type);

                                if db_type == DbType::END {
                                    break;
                                }

                                if db_type != DbType::STRING {
                                    let db_size = db_type.get_size();

                                    columns_cursor.seek(SeekFrom::Current(db_size as i64)).unwrap();
                                
                                } else {
                                    let str_length = columns_cursor.read_u32::<LittleEndian>().unwrap();


                                    columns_cursor.seek(SeekFrom::Current(str_length as i64)).unwrap();
                                }
                            }

                            column_index_inner += 1;
                        }

                        let evaluation = evaluate_column_result(&required_columns, &evaluation_tokens);
                        required_columns.clear();

                        if evaluation {
                            let row = columns_cursor_to_row(
                                columns_cursor, 
                                prefetch_result.found_id,
                                prefetch_result.length).unwrap();

                            #[cfg(feature = "enable_index_caching")]
                            {  
                                let mut row_vec: Vec<(u64, u32)> = Vec::with_capacity(1);
                                row_vec.push((first_column_index - 1 - 8 - 4, prefetch_result.length));
                                POSITIONS_CACHE.insert(hash, row_vec);
                            }

                            return Ok(Some(row));
                        }
                    } else {
                        break;
                    }
                }
            }

            return Ok(None);    
        } else {
            panic!() //Don't show error.
        }
    }

    pub async fn execute_query(&mut self, parser_result: ParserResult) -> io::Result<Option<Vec<Row>>> {
        if let ParserResult::HashIndexes(hash_indexes) = parser_result {
            let mut rows: Vec<Row> = Vec::with_capacity(hash_indexes.len());
            for record in hash_indexes {
                let mut position = record.0;
                let length = record.1;
                rows.push(indexed_row_fetching_file(
                    &mut self.io_sync,
                    &mut position,
                    length).await.unwrap());
            }

            return Ok(Some(rows));
        } else if let ParserResult::EvaluationTokens(evaluation_tokens) = parser_result {
            let limit = evaluation_tokens.limit;

            let mut rows: Vec<Row> = if limit < 1024 && limit > 0 {
                Vec::with_capacity(limit as usize)
            } else {
                Vec::default()
            };

            let hash = evaluation_tokens.query_hash;
            let evaluation_tokens = evaluation_tokens.tokens;

            let file_length = self.get_current_table_length();
    
            let chunks_arc_clone = self.in_memory_index.clone();

            let chunks_result = loop {
                let result = chunks_arc_clone.try_read();
                if let Ok(in_memory_index) = result {
                    break in_memory_index;
                } 
            };

            let mut current_limit: i64 = 0;

            #[cfg(feature = "enable_index_caching")]
            let mut result_row_vec: Vec<(u64, u32)> = Vec::default();

            if let Some(chunks) = chunks_result.as_ref() {
                let mut required_columns: Vec<(u32, Column)> = Vec::default();

                let column_indexes = evaluation_tokens.iter()
                    .flat_map(|(tokens, _)| tokens.iter())
                    .filter(|x|  match **x {
                        Token::Column(_) => true,
                        _ => false
                    }).map(|x| match *x {
                        Token::Column(column_index) => column_index,
                        _ => panic!()
                    }).collect_vec();

                for chunk in chunks {
                    let mut cursor = chunk.read_chunk_sync(&mut self.io_sync).await;
                    
                    let mut data_buffer = Vec::default();

                    loop {
                        if let Some(prefetch_result) = row_prefetching_cursor(&mut cursor, chunk).unwrap() {
                        
                            let mut current_column_index: u32 = 0;
                            let first_column_index = cursor.stream_position().unwrap();

                            loop {  

                                if column_indexes.iter().any(|x| *x == current_column_index) {
                                    let column_type = cursor.read_u8().unwrap();
        
                                    let db_type = DbType::from_byte(column_type);
        
                                    if db_type == DbType::END {
                                        break;
                                    }
        
                                    if db_type != DbType::STRING {
                                        let db_size = db_type.get_size();
        
                                        let mut preset_buffer = vec![0; db_size as usize];
                                        cursor.read(&mut preset_buffer).unwrap();
                                        data_buffer.append(&mut preset_buffer);
                                    } else {
                                        let str_length = cursor.read_u32::<LittleEndian>().unwrap();
        
                                        let mut preset_buffer = vec![0; str_length as usize];
                                        cursor.read(&mut preset_buffer).unwrap();
                                        data_buffer.append(&mut preset_buffer);
                                    }
                                    
                                    let type_byte = db_type.to_byte();
                                        
                                    let column = if type_byte >= 1 && type_byte <= 10 {
                                        Column::from_raw(column_type, data_buffer.to_vec(), Some(DowngradeType::I128))
                                    } else if type_byte >= 11 && type_byte <= 12 {
                                        Column::from_raw(column_type, data_buffer.to_vec(), Some(DowngradeType::F64))
                                    } else {
                                        Column::from_raw(column_type, data_buffer.to_vec(), None)
                                    };
        
                                    required_columns.push((current_column_index, column));
                                } else {
                                    let column_type = cursor.read_u8().unwrap();
        
                                    let db_type = DbType::from_byte(column_type);
        
                                    if db_type == DbType::END {
                                        break;
                                    }
        
                                    if db_type != DbType::STRING {
                                        let db_size = db_type.get_size();
                                        cursor.seek(SeekFrom::Current(db_size as i64)).unwrap();
                                    } else {
                                        let str_length = cursor.read_u32::<LittleEndian>().unwrap();
                                        cursor.seek(SeekFrom::Current(str_length as i64)).unwrap();
                                    }
                                }

                                current_column_index += 1;
                                data_buffer.clear();
                            }

                            let evaluation = evaluate_column_result(&required_columns, &evaluation_tokens);
                            required_columns.clear();

                            if evaluation {   
                                
                                let row = read_row_cursor(
                                    first_column_index, 
                                    prefetch_result.found_id,
                                    prefetch_result.length,
                                    &mut cursor).unwrap();

                                rows.push(row);

                                #[cfg(feature = "enable_index_caching")]
                                {
                                    result_row_vec.push((chunk.current_file_position + first_column_index - 1 - 8 - 4, prefetch_result.length));
                                }
                                
                                current_limit += 1;

                                if current_limit >= limit {
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
            } else {
                let column_indexes = evaluation_tokens.iter()
                    .flat_map(|(tokens, _)| tokens.iter())
                    .filter(|x|  match **x {
                        Token::Column(_) => true,
                        _ => false
                    }).map(|x| match *x {
                        Token::Column(column_index) => column_index,
                        _ => panic!()
                    }).collect_vec();

                let mut required_columns: Vec<(u32, Column)> = Vec::default();
                let mut position = HEADER_SIZE as u64;

                loop {
                    if let Some(prefetch_result) = row_prefetching(
                        &mut self.io_sync,
                        &mut position,
                        file_length).await.unwrap() {
    
                        let mut column_index_inner = 0;

                        let first_column_index = position + 4 + 8 + 1;

                        let mut columns_cursor = self.io_sync.read_data_to_cursor(
                            &mut position, 
                            prefetch_result.length + 1).await;
    
                        loop {
                            if column_indexes.iter().any(|x| *x == column_index_inner) {
                                let column_type = columns_cursor.read_u8().unwrap();

                                let db_type = DbType::from_byte(column_type);

                                if db_type == DbType::END {
                                    break;
                                }

                                let mut data_buffer = Vec::default();

                                if db_type != DbType::STRING {
                                    let db_size = db_type.get_size();

                                    let mut preset_buffer = vec![0; db_size as usize];
                                    columns_cursor.read(&mut preset_buffer).unwrap();
                                    data_buffer.append(&mut preset_buffer);
                                } else {
                                    let str_length = columns_cursor.read_u32::<LittleEndian>().unwrap();

                                    let mut preset_buffer = vec![0; str_length as usize];
                                    columns_cursor.read(&mut preset_buffer).unwrap();
                                    data_buffer.append(&mut preset_buffer);
                                }
                                
                                let type_byte = db_type.to_byte();
                                    
                                let column = if type_byte >= 1 && type_byte <= 10 {
                                    Column::from_raw(column_type, data_buffer, Some(DowngradeType::I128))
                                } else if type_byte >= 11 && type_byte <= 12 {
                                    Column::from_raw(column_type, data_buffer, Some(DowngradeType::F64))
                                } else {
                                    Column::from_raw(column_type, data_buffer, None)
                                };

                                required_columns.push((column_index_inner, column));
                            } else {
                                let column_type = columns_cursor.read_u8().unwrap();

                                let db_type = DbType::from_byte(column_type);

                                if db_type == DbType::END {
                                    break;
                                }

                                if db_type != DbType::STRING {
                                    let db_size = db_type.get_size();

                                    columns_cursor.seek(SeekFrom::Current(db_size as i64)).unwrap();
                                
                                } else {
                                    let str_length = columns_cursor.read_u32::<LittleEndian>().unwrap();


                                    columns_cursor.seek(SeekFrom::Current(str_length as i64)).unwrap();
                                }
                            }

                            column_index_inner += 1;
                        }

                        let evaluation = evaluate_column_result(&required_columns, &evaluation_tokens);
                        required_columns.clear();

                        if evaluation {
                            let row = columns_cursor_to_row(
                                columns_cursor, 
                                prefetch_result.found_id,
                                prefetch_result.length).unwrap();

                            rows.push(row);

                            #[cfg(feature = "enable_index_caching")]
                            {
                                result_row_vec.push((first_column_index - 1 - 8 - 4, prefetch_result.length));
                            }
                            
                            current_limit += 1;

                            if current_limit >= limit {
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

            return Ok(None);    
        } else {
            panic!() //Don't show error.
        }
    }

    /// #### STABILIZED
    /// Deletes the row by the given id.
    pub async fn delete_row_by_id(&mut self, id: u64) -> io::Result<()> {
        loop {
            let table_locked = self.locked.read().await;
            if *table_locked {
                continue;
            } else {
                break;
            }
        }

        let file_length = self.get_current_table_length();

        let chunks_arc_clone = self.in_memory_index.clone();

        let chunks_result = loop {
            let result = chunks_arc_clone.try_read();
            if let Ok(in_memory_index) = result {
                break in_memory_index;
            } 
        };

        if let Some(chunks) = chunks_result.as_ref() {
            let mut chunks_ended_position: u64 = 0;
            for chunk in chunks {
                let mut cursor = chunk.read_chunk_sync(&mut self.io_sync).await;

                loop {
                    if let Some(prefetch_result) = row_prefetching_cursor(&mut cursor, chunk).unwrap() {
                        if id == prefetch_result.found_id {
                            let starting_column_position = cursor.stream_position().unwrap() as u64 + chunk.current_file_position as u64;
                            
                            if let Some(record) = POSITIONS_CACHE.iter().find(|x| x.1.iter().any(|&(key, _)| key == (starting_column_position - 1 - 4 - 8) as u64)) {
                                POSITIONS_CACHE.invalidate(&record.0);
                            }

                            delete_row_file(
                                starting_column_position, 
                                &mut self.io_sync).await.unwrap();
            
                            return Ok(());
                        } else {
                            //+1 because of the END db type.
                            cursor.seek(SeekFrom::Current((prefetch_result.length + 1) as i64)).unwrap();
                        }
                    } else {
                        break;
                    }

                }

                chunks_ended_position = chunk.current_file_position;
            }

            if chunks_ended_position < file_length {
                let chunk = FileChunk {
                    current_file_position: 0,
                    chunk_size: 0,
                    next_row_id: 0
                };

                let mut cursor = chunk.read_chunk_to_end_sync(file_length - chunks_ended_position, &mut self.io_sync).await;

                loop {
                    if let Some(prefetch_result) = row_prefetching_cursor(&mut cursor, &chunk).unwrap() {
                        if id == prefetch_result.found_id {
                            let starting_column_position = cursor.stream_position().unwrap() as u64 + chunks_ended_position;
                    
                            delete_row_file(
                                starting_column_position, 
                                &mut self.io_sync).await.unwrap();
            
                            return Ok(());
                        } else {
                            cursor.seek(SeekFrom::Current((prefetch_result.length + 1) as i64)).unwrap();
                        }
                    } else {
                        break;
                    }

                }
            }
        } else {
            let mut position = HEADER_SIZE as u64;
            loop {
                if let Some(prefetch_result) = row_prefetching(
                    &mut self.io_sync,
                    &mut position,
                    file_length).await.unwrap() {
                        
                    if id == prefetch_result.found_id {
                        let starting_column_position = position;
                        
                        delete_row_file(
                            starting_column_position, 
                            &mut self.io_sync).await.unwrap();
        
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
        loop {
            let table_locked = self.locked.read().await;
            if *table_locked {
                continue;
            } else {
                break;
            }
        }

        let file_length = self.get_current_table_length();

        let chunks_arc_clone = self.in_memory_index.clone();

        let chunks_result = loop {
            let result = chunks_arc_clone.try_read();
            if let Ok(in_memory_index) = result {
                break in_memory_index;
            } 
        };

        let mut current_chunk_options = if let Some(chunks) = chunks_result.clone() {
            Some(chunks)
        } else {
            None
        };

        drop(chunks_result);

        let mut current_chunk_size: u32 = 0;

        let mut position = HEADER_SIZE as u64;

        loop {
            if let Some(prefetch_result) = row_prefetching(
                &mut self.io_sync,
                &mut position,
                file_length).await.unwrap() {

                position += prefetch_result.length as u64 + 1;
                //START (1), ROWID (8), LEN (4), COLUMNS (X), END (1)
                current_chunk_size += 1 + 8 + 4 + prefetch_result.length + 1;

                add_in_memory_index(&mut current_chunk_size, prefetch_result.found_id, position, &mut current_chunk_options);
                if (position + CHUNK_SIZE as u64) >= file_length {
                    break;
                }
            } else {
                break;
            } 
        }

        if file_length > position {
            add_last_in_memory_index(position, file_length, &mut current_chunk_options);
        }

        let chunks_arc_clone = self.in_memory_index.clone();

        let mut chunks_write_result = loop {
            let result = chunks_arc_clone.try_write();
            if let Ok(in_memory_index) = result {
                break in_memory_index;
            } 
        };

        *chunks_write_result = current_chunk_options;

        drop(chunks_write_result);
    }

    /// #### STABILIZED
    /// Updates the row by the given id.
    pub async fn update_row_by_id(&mut self, id: u64, row: InsertOrUpdateRow) {
        loop {
            let table_locked = self.locked.read().await;
            if *table_locked {
                continue;
            } else {
                break;
            }
        }

        let columns_len = row.columns_data.len();

        //START (1) + END (1) + u64 ID (8) + u32 LENGTH (4) + VEC SIZE 
        let mut buffer: Vec<u8> = Vec::with_capacity(1 + 1 + 8 + 4 + columns_len);

        let update_row_size = buffer.capacity() as u32;

        let current_file_len: u64 = loop {
            let result = self.current_file_length.try_read();
            if let Ok(current_len) = result {
                if *current_len == 0 {
                    break (current_len.clone() + HEADER_SIZE as u64);
                } else {
                    break current_len.clone();
                }
            } 
        };

        let file_length = self.get_current_table_length();
        let mut position = HEADER_SIZE as u64;

        loop {
            if let Some(prefetch_result) = row_prefetching(
                &mut self.io_sync,
                &mut position,
                file_length).await.unwrap() {
                if prefetch_result.found_id == id {
                    // START (1) + END (1) + u64 ID (8) + u32 LENGTH (4) + VEC SIZE 
                    let current_row_len = (1 + 1 + 8 + 4 + prefetch_result.length) as u32; 
                    if update_row_size == current_row_len {
                        //DbType START
                        buffer.push(254);

                        WriteBytesExt::write_u64::<LittleEndian>(&mut buffer, prefetch_result.found_id).unwrap();
                        WriteBytesExt::write_u32::<LittleEndian>(&mut buffer, columns_len as u32).unwrap();

                        let mut columns_data = row.columns_data.clone();

                        buffer.append(&mut columns_data);

                        //DbType END
                        buffer.push(255);

                        let new_position = position - 8 - 4 - 1;

                        self.io_sync.write_data_unsync(new_position, &buffer);
                        let verify_result = self.io_sync.verify_data_and_sync(new_position, &buffer);

                        #[allow(unreachable_code)]
                        if !verify_result {
                            panic!("DB file contains error.");
                            todo!("Add rollback.");
                        }

                        break;
                    } else if update_row_size < current_row_len {
                        //DbType START
                        buffer.push(254);

                        WriteBytesExt::write_u64::<LittleEndian>(&mut buffer, prefetch_result.found_id).unwrap();
                        WriteBytesExt::write_u32::<LittleEndian>(&mut buffer, columns_len as u32).unwrap();

                        let mut columns_data = row.columns_data.clone();

                        buffer.append(&mut columns_data);

                        //DbType END
                        buffer.push(255);

                        let new_position = position - 8 - 4 - 1;

                        self.io_sync.write_data_unsync(new_position, &buffer);
                        let verify_result = self.io_sync.verify_data_and_sync(new_position, &buffer);

                        #[allow(unreachable_code)]
                        if !verify_result {
                            panic!("DB file contains error.");
                            todo!("Add rollback.");
                        }

                        let empty_buffer = vec![0 as u8; (current_row_len - update_row_size) as usize];

                        self.io_sync.write_data_unsync(new_position + update_row_size as u64, &empty_buffer);
                        let clean_up_verify_result = self.io_sync.verify_data_and_sync(new_position + update_row_size as u64, &empty_buffer);

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
                        WriteBytesExt::write_u32::<LittleEndian>(&mut buffer, columns_len as u32).unwrap();

                        let mut columns_data = row.columns_data.clone();

                        buffer.append(&mut columns_data);

                        //DbType END
                        buffer.push(255);

                        self.io_sync.append_data_unsync(&buffer);
                        let verify_result = self.io_sync.verify_data_and_sync(current_file_len, &buffer);

                        #[allow(unreachable_code)]
                        if !verify_result {
                            panic!("DB file contains error.");
                            todo!("Add rollback.");
                        }

                        let empty_buffer = vec![0 as u8; (current_row_len) as usize];

                        let new_position = position - 8 - 4 - 1;

                        self.io_sync.write_data_unsync(new_position, &empty_buffer);
                        let clean_up_verify_result = self.io_sync.verify_data_and_sync(new_position, &empty_buffer);

                        #[allow(unreachable_code)]
                        if !clean_up_verify_result {
                            panic!("DB file contains error.");
                            todo!("Add rollback.");
                        }

                        let chunks_arc_clone = self.in_memory_index.clone();

                        let mut chunks_write_result = loop {
                            let result = chunks_arc_clone.try_write();
                            if let Ok(in_memory_index) = result {
                                break in_memory_index;
                            } 
                        };

                        if let Some(ref mut chunks) = *chunks_write_result {
                            let last_chunk = chunks.last_mut().unwrap();

                            if last_chunk.chunk_size + update_row_size <= CHUNK_SIZE as u32 {
                                last_chunk.chunk_size = last_chunk.chunk_size + update_row_size;
                                last_chunk.next_row_id = last_chunk.next_row_id + 1;
                            } else {
                                let new_chunk = FileChunk {
                                    current_file_position: current_file_len, //+ last_chunk.chunk_size as u64,
                                    chunk_size: update_row_size,
                                    next_row_id: row_id
                                };

                                chunks.push(new_chunk);
                            }
                        }

                        drop(chunks_write_result);

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
        loop {
            let mut table_locked = self.locked.write().await;
            if *table_locked {
                continue;
            } else {
                *table_locked = true;
                break;
            }
        }

        let file_length = self.get_current_table_length();
        let mut position: u64 = HEADER_SIZE as u64;

        let temp_io_sync = self.io_sync.create_temp().await;
        let mut temp_table = Table::init(temp_io_sync, false, false).await.unwrap();

        loop {
            if let Some(prefetch_result) = row_prefetching(
                &mut self.io_sync,
                &mut position,
                file_length).await.unwrap() {

                let row = read_row_columns(
                    &mut self.io_sync,
                    position,
                    prefetch_result.found_id,
                    prefetch_result.length + 1).await.unwrap();
                
                // END (1)
                position += prefetch_result.length as u64 + 1;
                temp_table.insert_row(InsertOrUpdateRow { columns_data: row.columns_data }).await;
            } else {
                break;
            }
        }

        self.io_sync.swap_temp(&mut temp_table.io_sync).await;

        drop(temp_table);

        self.in_memory_index = Arc::new(RwLock::const_new(None));

        let mut table_locked = self.locked.write().await;
        *table_locked = false;
    }
}