use std::{
    fmt::Display, fs::File, io::{self, BufReader, Cursor, Read, Seek, SeekFrom}, 
    os::windows::fs::FileExt, 
    path::Path, 
    sync::Arc
};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use itertools::Itertools;
use tokio::{io::AsyncWriteExt, sync::{RwLock, RwLockWriteGuard}};

use crate::{core::helpers::delete_row_file, HEADER_SIZE, POSITIONS_CACHE};
use super::{
    column::{Column, DowngradeType}, 
    db_type::DbType, helpers::{
        add_in_memory_index, add_last_in_memory_index, indexed_row_fetching_file, read_row_cursor, read_row_file, row_prefetching_cursor, row_prefetching_file, row_prefetching_file_index
    }, 
    row::{InsertRow, Row}, 
    super::rql::{
        models::Token, parser::ParserResult, tokenizer::evaluate_column_result
    }, 
    support_types::FileChunk, 
    table_header::TableHeader
};

pub struct Table {
    pub(crate) location: String,
    pub(crate) table_header: Arc<RwLock<TableHeader>>,
    pub(crate) file_connection_read: File,
    pub(crate) in_memory_index: Option<Vec<FileChunk>>,
    pub(crate) current_file_length: Arc<RwLock<u64>>,
    pub(crate) current_row_id: Arc<RwLock<u64>>
}

impl Table {
    pub fn init(file_name: &str, compressed: bool) -> io::Result<Table> {
        let path = Path::new(file_name);

        if path.exists() && path.is_file() {
            let file = File::options().read(true).open(file_name).unwrap();

            let mut buffer: [u8; HEADER_SIZE as usize] = [0; HEADER_SIZE as usize];

            file.seek_read(&mut buffer, 0).unwrap();

            let mut table_header = TableHeader::from_buffer(buffer.to_vec()).unwrap();

            let last_row_id: u64 = table_header.last_row_id;

            table_header.total_file_length = file.metadata().unwrap().len();

            let table = Table {
                location: file_name.to_string(),
                table_header: Arc::new(RwLock::const_new(table_header)),
                file_connection_read: File::options().read(true).open(&file_name).unwrap(),
                in_memory_index: None,
                current_file_length: Arc::new(RwLock::const_new(file.metadata().unwrap().len())),
                current_row_id: Arc::new(RwLock::const_new(last_row_id))
            };

            Ok(table)
        } else {
            let file = File::create(file_name).unwrap();
            
            let table_header = TableHeader::new(HEADER_SIZE, 0, compressed, 0, 0, false);

            // Serialize the header and write it to the file
            let header_bytes = table_header.to_bytes().unwrap();

            file.seek_write(&header_bytes, 0).unwrap();

            let table = Table {
                location: file_name.to_string(),
                table_header: Arc::new(RwLock::const_new(table_header)),
                file_connection_read: File::options().read(true).open(&file_name).unwrap(),
                in_memory_index: None,
                current_file_length: Arc::new(RwLock::const_new(file.metadata().unwrap().len())),
                current_row_id: Arc::new(RwLock::const_new(0))
            };

            Ok(table)
        }
    }

    /// (End Of File, Row Id)
    async fn update_eof_and_id(&mut self, buffer_size: u64) -> u64 {
        
        let end_of_file_c = self.current_file_length.clone();

        let mut end_of_file : RwLockWriteGuard<u64> = loop {
            let result = end_of_file_c.try_write();
            if let Ok(end_of_file) = result  {
                break end_of_file;
            } 
        };
        
        let last_row_id_c = self.current_row_id.clone();

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

    pub async fn insert_row(&mut self, row: &mut InsertRow) -> io::Result<()>{
        let mut file = tokio::fs::File::options().append(true).open(&self.location).await?;
        
        let columns_len = row.columns_data.len();

        //START (1) + END (1) + u64 ID (8) + u32 LENGTH (4) + VEC SIZE 
        let mut buffer: Vec<u8> = Vec::with_capacity(1 + 1 + 8 + 4 + columns_len);

        let total_row_size = buffer.capacity() as u64;

        let row_id = self.update_eof_and_id(total_row_size).await;

        //DbType START
        buffer.push(254);

        WriteBytesExt::write_u64::<LittleEndian>(&mut buffer, row_id).unwrap();
        WriteBytesExt::write_u32::<LittleEndian>(&mut buffer, columns_len as u32).unwrap();

        buffer.append(&mut row.columns_data);

        //DbType END
        buffer.push(255);

        file.write(&buffer).await?;

        let table_header = self.table_header.clone();

        let mut table_header_w: RwLockWriteGuard<TableHeader> = loop {
            let result = table_header.try_write();
            if let Ok(table_header) = result  {
                break table_header;
            } 
        };

        table_header_w.last_row_id = row_id + 1;
        table_header_w.total_file_length = total_row_size + buffer.len() as u64;

        Ok(())
    }

    pub fn first_or_default_by_id(&mut self, id: u64) -> io::Result<Option<Row>> {
        let mut file = self.file_connection_read.try_clone().unwrap();

        let file_length = loop {
            if let Ok(header) = self.table_header.try_read() {
                break header.total_file_length - HEADER_SIZE;
            }
        };

        file.seek(SeekFrom::Start(HEADER_SIZE)).unwrap();

        if let Some(chunks) = self.in_memory_index.as_ref() {
            let mut chunks_ended_position: u64 = 0;
            for chunk in chunks {
                let mut buffer = vec![0; chunk.chunk_size as usize];
                file.seek_read(&mut buffer, chunk.current_file_position).unwrap();

                if buffer.len() == 0 {
                    panic!("Error reading the file to buffer.");
                }

                let mut cursor = Cursor::new(&buffer);

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
              
                chunks_ended_position = chunk.current_file_position;
            }

            if chunks_ended_position < file_length {
                let mut buffer: Vec<u8> = Vec::default();
                file.read_to_end(&mut buffer).unwrap();

                let cursor_size = buffer.len();

                let mut cursor = Cursor::new(&buffer);

                loop {
                    let chunk = FileChunk {
                        current_file_position: 0,
                        chunk_size: cursor_size as u64,
                        next_row_id: 0
                    };

                    if let Some(prefetch_result) = row_prefetching_cursor(&mut cursor, &chunk).unwrap() {
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
            loop {       
                let file_postion = file.stream_position()?;

                if let Some(prefetch_result) = row_prefetching_file(
                    &mut file,
                    file_postion,
                    file_length).unwrap() {
                      
                    if id == prefetch_result.found_id {
                        let row = read_row_file(
                            file.stream_position().unwrap(), 
                            prefetch_result.found_id,
                            prefetch_result.length,
                            &mut file).unwrap();

                        return Ok(Some(row));
                    } else {
                        //+1 because of the END db type.
                        file.seek(SeekFrom::Current((prefetch_result.length + 1) as i64)).unwrap();
                    }
                } else {
                    break;
                }
            }
        }

        return Ok(None);
    }

    pub fn first_or_default_by_column<T>(&mut self, column_index: u32, value: T) -> io::Result<Option<Row>> 
    where
        T: Display {
        let mut file = self.file_connection_read.try_clone().unwrap();

        let value_column = Column::new(value).unwrap();

        let file_length =  loop {
            if let Ok(header) = self.table_header.try_read() {
                break header.total_file_length - HEADER_SIZE;
            }
        };

        file.seek(SeekFrom::Start(HEADER_SIZE)).unwrap();

        if let Some(chunks) = self.in_memory_index.as_ref() {
            let mut chunks_ended_position: u64 = 0;

            for chunk in chunks {
                let mut buffer = vec![0; chunk.chunk_size as usize];
                file.seek_read(&mut buffer, chunk.current_file_position).unwrap();

                if buffer.len() == 0 {
                    panic!("Error reading the file to buffer.");
                }

                let mut cursor = Cursor::new(&buffer);

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
                                    data_buffer.append(&mut preset_buffer.to_vec());
                                } else {
                                    let str_length = cursor.read_u32::<LittleEndian>().unwrap();
                                    let mut preset_buffer = vec![0; str_length as usize];
                                    cursor.read(&mut preset_buffer).unwrap();
                                    data_buffer.append(&mut preset_buffer.to_vec());
                                }
                                
                                let column = Column::from_raw(column_type, data_buffer, None);

                                if value_column.eq(&column) {
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

                chunks_ended_position = chunk.current_file_position;
            }

            if chunks_ended_position < file_length {
                let mut buffer: Vec<u8> = Vec::default();
                file.read_to_end(&mut buffer).unwrap();

                let cursor_size = buffer.len();

                let mut cursor = Cursor::new(&buffer);

                loop {
                    let chunk = FileChunk {
                        current_file_position: 0,
                        chunk_size: cursor_size as u64,
                        next_row_id: 0
                    };

                    if let Some(prefetch_result) = row_prefetching_cursor(&mut cursor, &chunk).unwrap() {
                        //Current column index
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
                                    data_buffer.append(&mut preset_buffer.to_vec());
                                } else {
                                    let str_length = cursor.read_u32::<LittleEndian>().unwrap();
                                    let mut preset_buffer = vec![0; str_length as usize];
                                    cursor.read(&mut preset_buffer).unwrap();
                                    data_buffer.append(&mut preset_buffer.to_vec());
                                }
                                
                                let column = Column::from_raw(column_type, data_buffer, None);

                                if value_column.eq(&column) {
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

            loop {
                let file_postion = file.stream_position()?;

                if let Some(prefetch_result) = row_prefetching_file(
                    &mut file,
                    file_postion,
                    file_length).unwrap() {

                    let mut column_index_inner = 0;
                    let first_column_index = file.stream_position().unwrap();

                    loop {  
                        if column_index == column_index_inner {
                            column_index_inner += 1;

                            let column_type = file.read_u8().unwrap();
                          
                            let db_type = DbType::from_byte(column_type);

                            if db_type == DbType::END {
                                break;
                            }

                            let mut data_buffer = Vec::default();

                            if db_type != DbType::STRING {
                                let db_size = db_type.get_size();

                                let mut preset_buffer = vec![0; db_size as usize];
                                file.read(&mut preset_buffer).unwrap();
                                data_buffer.append(&mut preset_buffer.to_vec());
                            } else {
                                let str_length = file.read_u32::<LittleEndian>().unwrap();
                               
                                let mut preset_buffer = vec![0; str_length as usize];
                                file.read(&mut preset_buffer).unwrap();
                                data_buffer.append(&mut preset_buffer.to_vec());
                            }
                            
                            let column = Column::from_raw(column_type, data_buffer, None);

                            if value_column.eq(&column) {
                                let row = read_row_file(
                                    first_column_index, 
                                    prefetch_result.found_id,
                                    prefetch_result.length,
                                    &mut file).unwrap();

                                return Ok(Some(row));
                            }
                        } else {
                            column_index_inner += 1;

                            let column_type = file.read_u8().unwrap();
                           
                            let db_type = DbType::from_byte(column_type);

                            if db_type == DbType::END {
                                break;
                            }

                            if db_type != DbType::STRING {
                                let db_size = db_type.get_size();

                                file.seek(SeekFrom::Current(db_size as i64)).unwrap();
                               
                            } else {
                                let str_length = file.read_u32::<LittleEndian>().unwrap();
                               
                                file.seek(SeekFrom::Current(str_length as i64)).unwrap();
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

    pub async fn first_or_default_by_query(&mut self, parser_result: ParserResult) -> io::Result<Option<Row>> {
        if let ParserResult::HashIndexes(hash_indexes) = parser_result {
            let mut file = self.file_connection_read.try_clone().unwrap();

            for record in hash_indexes {
                return Ok(Some(indexed_row_fetching_file(
                    &mut file,
                    record.0,
                    record.1).unwrap()));
            }
            todo!("Support many results.");
        } else if let ParserResult::EvaluationTokens(evaluation_tokens) = parser_result {
            let hash = evaluation_tokens.0;
            let evaluation_tokens = evaluation_tokens.1;

            let file_length = loop {
                if let Ok(header) = self.table_header.try_read() {
                    break header.total_file_length - HEADER_SIZE;
                }
            };
    
            if let Some(chunks) = self.in_memory_index.as_ref() {
                let file = self.file_connection_read.try_clone().unwrap();
                let mut reader = BufReader::new(file);

                reader.seek(SeekFrom::Start(HEADER_SIZE)).unwrap();

                let mut chunks_ended_position: u64 = 0;

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
                    let mut buffer = vec![0; chunk.chunk_size as usize];
                    reader.read(&mut buffer).unwrap();

                    if buffer.len() == 0 {
                        panic!("Error reading the file to buffer.");
                    }

                    let mut cursor = Cursor::new(&buffer);

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
        
                                    let mut data_buffer = Vec::default();
        
                                    if db_type != DbType::STRING {
                                        let db_size = db_type.get_size();
        
                                        let mut preset_buffer = vec![0; db_size as usize];
                                        cursor.read(&mut preset_buffer).unwrap();
                                        data_buffer.append(&mut preset_buffer.to_vec());
                                    } else {
                                        let str_length = cursor.read_u32::<LittleEndian>().unwrap();
        
                                        let mut preset_buffer = vec![0; str_length as usize];
                                        cursor.read(&mut preset_buffer).unwrap();
                                        data_buffer.append(&mut preset_buffer.to_vec());
                                    }
                                    
                                    let type_byte = db_type.to_byte();
                                        
                                    let column = if type_byte >= 1 && type_byte <= 10 {
                                        Column::from_raw(column_type, data_buffer, Some(DowngradeType::I128))
                                    } else if type_byte >= 11 && type_byte <= 12 {
                                        Column::from_raw(column_type, data_buffer, Some(DowngradeType::F64))
                                    } else {
                                        Column::from_raw(column_type, data_buffer, None)
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
                            }

                            let evaluation = evaluate_column_result(&required_columns, &evaluation_tokens);
                            required_columns.clear();

                            if evaluation {
                                let row = read_row_cursor(
                                    first_column_index, 
                                    prefetch_result.found_id,
                                    prefetch_result.length,
                                    &mut cursor).unwrap();

                                let mut row_vec: Vec<(u64, u32)> = Vec::with_capacity(1);
                                row_vec.push((chunk.current_file_position + first_column_index - 1 - 8 - 4, prefetch_result.length));
                                POSITIONS_CACHE.insert(hash, row_vec);

                                return Ok(Some(row));
                            }
                        } else {
                            break;
                        }
                    }

                    chunks_ended_position = chunk.current_file_position;
                }

                if chunks_ended_position < file_length {
                    let mut buffer: Vec<u8> = Vec::default();
                    reader.read_to_end(&mut buffer).unwrap();

                    let cursor_size = buffer.len();

                    let mut cursor = Cursor::new(&buffer);

                    loop {
                        let chunk = FileChunk {
                            current_file_position: 0,
                            chunk_size: cursor_size as u64,
                            next_row_id: 0
                        };

                        if let Some(prefetch_result) = row_prefetching_cursor(&mut cursor, &chunk).unwrap() {
                                
                            let mut current_column_index: u32 = 0;
                            let first_column_index = cursor.stream_position().unwrap();

                            loop {  
                                if column_indexes.iter().any(|x| *x == current_column_index) {
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
                                        data_buffer.append(&mut preset_buffer.to_vec());
                                    } else {
                                        let str_length = cursor.read_u32::<LittleEndian>().unwrap();
        
                                        let mut preset_buffer = vec![0; str_length as usize];
                                        cursor.read(&mut preset_buffer).unwrap();
                                        data_buffer.append(&mut preset_buffer.to_vec());
                                    }
                                    
                                    let type_byte = db_type.to_byte();
                                        
                                    let column = if type_byte >= 1 && type_byte <= 10 {
                                        Column::from_raw(column_type, data_buffer, Some(DowngradeType::I128))
                                    } else if type_byte >= 11 && type_byte <= 12 {
                                        Column::from_raw(column_type, data_buffer, Some(DowngradeType::F64))
                                    } else {
                                        Column::from_raw(column_type, data_buffer, None)
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
                            }

                            let evaluation = evaluate_column_result(&required_columns, &evaluation_tokens);
                            required_columns.clear();

                            if evaluation {
                                let row = read_row_cursor(
                                    first_column_index, 
                                    prefetch_result.found_id,
                                    prefetch_result.length,
                                    &mut cursor).unwrap();

                                let mut row_vec: Vec<(u64, u32)> = Vec::with_capacity(1);
                                row_vec.push((chunks_ended_position + first_column_index - 1 - 8 - 4, prefetch_result.length));
                                POSITIONS_CACHE.insert(hash, row_vec);

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

                loop {
                    let mut file = self.file_connection_read.try_clone().unwrap();

                    let file_postion = file.stream_position()?;

                    if let Some(prefetch_result) = row_prefetching_file(
                        &mut file,
                        file_postion,
                        file_length).unwrap() {

                        let first_column_index = file.stream_position().unwrap();

                        let mut current_column_index: u32 = 0;

                        loop {  
                            if column_indexes.iter().any(|x| *x == current_column_index) {
                                let column_type = file.read_u8().unwrap();

                                let db_type = DbType::from_byte(column_type);

                                if db_type == DbType::END {
                                    break;
                                }

                                let mut data_buffer = Vec::default();

                                if db_type != DbType::STRING {
                                    let db_size = db_type.get_size();

                                    let mut preset_buffer = vec![0; db_size as usize];
                                    file.read(&mut preset_buffer).unwrap();
                                    data_buffer.append(&mut preset_buffer.to_vec());
                                } else {
                                    let str_length = file.read_u32::<LittleEndian>().unwrap();

                                    let mut preset_buffer = vec![0; str_length as usize];
                                    file.read(&mut preset_buffer).unwrap();
                                    data_buffer.append(&mut preset_buffer.to_vec());
                                }
                                
                                let type_byte = db_type.to_byte();
                                    
                                let column = if type_byte >= 1 && type_byte <= 10 {
                                    Column::from_raw(column_type, data_buffer, Some(DowngradeType::I128))
                                } else if type_byte >= 11 && type_byte <= 12 {
                                    Column::from_raw(column_type, data_buffer, Some(DowngradeType::F64))
                                } else {
                                    Column::from_raw(column_type, data_buffer, None)
                                };

                                required_columns.push((current_column_index, column));
                            } else {
                                let column_type = file.read_u8().unwrap();

                                let db_type = DbType::from_byte(column_type);

                                if db_type == DbType::END {
                                    break;
                                }

                                if db_type != DbType::STRING {
                                    let db_size = db_type.get_size();

                                    file.seek(SeekFrom::Current(db_size as i64)).unwrap();
                                
                                } else {
                                    let str_length = file.read_u32::<LittleEndian>().unwrap();


                                    file.seek(SeekFrom::Current(str_length as i64)).unwrap();
                                }
                            }

                            current_column_index += 1;
                        }

                        let evaluation = evaluate_column_result(&required_columns, &evaluation_tokens);
                        required_columns.clear();

                        if evaluation {
                            let row = read_row_file(
                                first_column_index, 
                                prefetch_result.found_id,
                                prefetch_result.length,
                                &mut file).unwrap();

                            let mut row_vec: Vec<(u64, u32)> = Vec::with_capacity(1);
                            row_vec.push((first_column_index - 1 - 8 - 4, prefetch_result.length));
                            POSITIONS_CACHE.insert(hash, row_vec);

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

    pub fn delete_row_by_id(&mut self, id: u64) -> io::Result<()> {
        let mut file = self.file_connection_read.try_clone().unwrap();

        let file_length = loop {
            if let Ok(header) = self.table_header.try_read() {
                break header.total_file_length - HEADER_SIZE;
            }
        };

        file.seek(SeekFrom::Start(HEADER_SIZE)).unwrap();

        if let Some(chunks) = self.in_memory_index.as_ref() {
            let mut chunks_ended_position: u64 = 0;

            for chunk in chunks {
                let mut buffer = vec![0; chunk.current_file_position as usize];
                file.seek_read(&mut buffer, chunk.chunk_size).unwrap();

                if buffer.len() == 0 {
                    panic!("Error reading the file to buffer.");
                }

                let mut cursor = Cursor::new(&buffer);

                loop {
                    if let Some(prefetch_result) = row_prefetching_cursor(&mut cursor, chunk).unwrap() {
                        if id == prefetch_result.found_id {
                            let starting_column_position = cursor.stream_position().unwrap() as u64 + chunk.chunk_size;
                            
                            if let Some(record) = POSITIONS_CACHE.iter().find(|x| x.1.iter().any(|&(key, _)| key == (starting_column_position - 1 - 4 - 8) as u64)) {
                                POSITIONS_CACHE.invalidate(&record.0);
                            }

                            let mut file = File::options().read(true).write(true).open(&self.location).unwrap();

                            delete_row_file(
                                starting_column_position, 
                                &mut file).unwrap();
            
                            return Ok(());
                        } else {
                            //+1 because of the END db type.
                            cursor.seek(SeekFrom::Current((prefetch_result.length + 1) as i64)).unwrap();
                        }
                    } else {
                        break;
                    }

                }

                chunks_ended_position = chunk.chunk_size;
            }

            if chunks_ended_position < file_length {
                let mut buffer: Vec<u8> = Vec::default();
                file.read_to_end(&mut buffer).unwrap();

                let cursor_size = buffer.len();

                let mut cursor = Cursor::new(&buffer);

                loop {
                    let chunk = FileChunk {
                        current_file_position: 0,
                        chunk_size: cursor_size as u64,
                        next_row_id: 0
                    };

                    if let Some(prefetch_result) = row_prefetching_cursor(&mut cursor, &chunk).unwrap() {
                        if id == prefetch_result.found_id {
                            let starting_column_position = cursor.stream_position().unwrap() as u64 + chunks_ended_position;
                    
                            let mut file = File::options().read(true).write(true).open(&self.location).unwrap();

                            delete_row_file(
                                starting_column_position, 
                                &mut file).unwrap();
            
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
            loop {
                let file_postion = file.stream_position()?;
                if let Some(prefetch_result) = row_prefetching_file(
                    &mut file,
                    file_postion,
                    file_length).unwrap() {
                        
                    if id == prefetch_result.found_id {
                        let starting_column_position = file.stream_position().unwrap() as u64;
                        
                        let mut file = File::options().read(true).write(true).open(&self.location).unwrap();
    
                        delete_row_file(
                            starting_column_position, 
                            &mut file).unwrap();
        
                        return Ok(());
                    } else {
                        //+1 because of the END db type.
                        file.seek(SeekFrom::Current((prefetch_result.length + 1) as i64)).unwrap();
                    }
                } else {
                    break;
                }
            }
        }

        return Ok(());    
    }

    pub fn rebuild_in_memory_indexes(&mut self) {
        let mut file = self.file_connection_read.try_clone().unwrap();
        let file_length = loop {
            if let Ok(header) = self.table_header.try_read() {
                break header.total_file_length - HEADER_SIZE;
            }
        };
        file.seek(SeekFrom::Start(HEADER_SIZE)).unwrap();

        self.in_memory_index = None;

        let mut file_position: u64 = HEADER_SIZE;
        let mut current_chunk_size: u64 = 0;

        loop {       
            #[allow(unused_assignments)]
            let mut current_id: u64 = 0;

            if let Some(prefetch_result) = row_prefetching_file_index(
                &mut file,
                &mut file_position,
                &mut current_chunk_size,
                file_length).unwrap() {
                    
                current_id = prefetch_result.found_id;

                //+1 because of the END db type.
                file.seek(SeekFrom::Current((prefetch_result.length + 1) as i64)).unwrap();

                file_position += (prefetch_result.length + 1) as u64;
                current_chunk_size += (prefetch_result.length + 1) as u64;

                add_in_memory_index(&mut current_chunk_size, current_id, file_position, &mut self.in_memory_index);
            } else {
                break;
            }
        }

        add_last_in_memory_index(current_chunk_size, file_position, file_length, &mut self.in_memory_index);
    }
}