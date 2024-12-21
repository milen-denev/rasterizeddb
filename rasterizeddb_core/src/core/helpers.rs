use std::{fs::File, io::{self, Cursor, Read, Seek, SeekFrom, Write}};

use byteorder::{LittleEndian, ReadBytesExt};

use crate::{CHUNK_SIZE, EMPTY_BUFFER, HEADER_SIZE};
use super::{
    column::Column, 
    db_type::DbType, 
    row::Row, 
    storage_providers::traits::IOOperationsSync, 
    support_types::{FileChunk, RowPrefetchResult}
};

pub(crate) fn read_row_columns(
    io_sync: &mut impl IOOperationsSync, 
    first_column_index: u64, 
    id: u64, 
    length: u32) -> io::Result<Row> {
    let mut io_sync = io_sync.clone();

    io_sync.seek(SeekFrom::Start(first_column_index));

    let mut columns: Vec<Column> = Vec::default();
    
    let mut position = first_column_index;

    let mut cursor = io_sync.read_data_to_cursor(&mut position, length);

    loop {
        let column_type = cursor.read_u8().unwrap();
      
        let db_type = DbType::from_byte(column_type);

        if db_type == DbType::END {
            break;
        }

        let mut data_buffer = Vec::default();

        if db_type != DbType::STRING {
            let db_size = db_type.get_size();

            let mut preset_buffer = vec![0; db_size as usize];
            cursor.read_exact(&mut preset_buffer)?;

            data_buffer.append(&mut preset_buffer.to_vec());
        } else {
            let str_length = cursor.read_u32::<LittleEndian>().unwrap();

            let mut preset_buffer = vec![0; str_length as usize];
            cursor.read_exact(&mut preset_buffer)?;
          
            data_buffer.append(&mut preset_buffer.to_vec());
        }
        
        let column = Column::from_raw(column_type, data_buffer, None);

        columns.push(column);
    }

    let mut columns_buffer: Vec<u8> = Vec::default();

    for mut column in columns {
        columns_buffer.append(&mut column.into_vec().unwrap());
    }

    return Ok(Row {
        id: id,
        length: length,
        columns_data: columns_buffer
    });
}

pub(crate) fn read_row_cursor(first_column_index: u64, id: u64, length: u32, cursor: &mut Cursor<Vec<u8>>) -> io::Result<Row> {
    cursor.seek(SeekFrom::Start(first_column_index)).unwrap();

    let mut columns: Vec<Column> = Vec::default();
    
    loop {
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

        columns.push(column);
    }

    let mut columns_buffer: Vec<u8> = Vec::default();

    for mut column in columns {
        columns_buffer.append(&mut column.into_vec().unwrap());
    }

    return Ok(Row {
        id: id,
        length: length,
        columns_data: columns_buffer
    });
}

pub(crate) fn delete_row_file(first_column_index: u64, file: &mut File) -> io::Result<()> {
    file.seek(SeekFrom::Start(first_column_index - 4)).unwrap();

    let columns_length = file.read_u32::<LittleEndian>().unwrap();

    //ID (8) LEN (4) COLUMNS (columns_length) START (1) END (1)
    let empty_buffer = vec![0; (columns_length + 4 + 8 + 1 + 1) as usize];

    //ID (8) LEN (4) START (1)
    file.seek(SeekFrom::Start(first_column_index - 4 - 8 - 1)).unwrap();

    // Write the empty buffer to overwrite the data
    file.write_all(&empty_buffer).unwrap();

    // Flush to ensure changes are written to disk
    file.flush().unwrap();

    return Ok(());
}

pub(crate) fn skip_empty_spaces_file(
    io_sync: &mut impl IOOperationsSync, 
    file_position: &mut u64,
    file_length: u64) -> Result<impl IOOperationsSync, String> {
    let mut io_sync = io_sync.clone();

    if file_length == *file_position || file_length < *file_position {
        return Ok(io_sync);
    }

    let mut check_next_buffer = io_sync.read_data(file_position, 8);

    if check_next_buffer == EMPTY_BUFFER {
        loop {
            io_sync.read_data_into_buffer(file_position, &mut check_next_buffer);

            if file_length == *file_position || file_length < *file_position {
                return Ok(io_sync);
            }

            if check_next_buffer != EMPTY_BUFFER {
                break;
            }
        }

        let index_of_row_start = check_next_buffer.iter().position(|x| *x == DbType::START.to_byte()).unwrap();

        let move_back = -8 as i64 + index_of_row_start as i64;

        io_sync.seek(SeekFrom::Current(move_back));
        *file_position = ((*file_position) as i64 + move_back) as u64;

        return Ok(io_sync);
    } else {
        let index_of_row_start = check_next_buffer.iter().position(|x| *x == DbType::START.to_byte());
       
        if let Some(index_of_row_start) = index_of_row_start {
            let deduct = (index_of_row_start as i64 - 8) * -1;

            io_sync.seek(SeekFrom::Current(deduct * -1));

            *file_position = ((*file_position) as i64 - deduct) as u64;
        } else {
            panic!("FILE -> 254 DbType::START not found: {:?}", check_next_buffer);
        }
    }

    return Ok(io_sync);
}

pub(crate) fn skip_empty_spaces_file_index(
    file: &mut File, 
    file_position: &mut u64, 
    current_chunk_size: &mut u64, 
    file_length: u64) -> io::Result<()> {
    if file_length == *file_position || file_length < *file_position {
        return Ok(());
    }
    
    let mut check_next_buffer = vec![0; 8];

    file.read(&mut check_next_buffer).unwrap();
   
    *file_position += 8;
    *current_chunk_size += 8;

    if check_next_buffer == EMPTY_BUFFER {
        loop {
            file.read(&mut check_next_buffer).unwrap();
            *file_position += 8;
            *current_chunk_size += 8;

            if file_length == *file_position || file_length < *file_position {
                return Ok(());
            }

            if check_next_buffer != EMPTY_BUFFER {
                break;
            }
        }

        let index_of_row_start = check_next_buffer.iter().position(|x| *x == DbType::START.to_byte()).unwrap();

        let move_back = -8 as i64 + index_of_row_start as i64;

        file.seek(SeekFrom::Current(move_back)).unwrap();
        *file_position = *file_position - (move_back * -1) as u64;
        *current_chunk_size = *current_chunk_size - (move_back * -1) as u64;

        return Ok(());
    } else {
        let index_of_row_start = check_next_buffer.iter().position(|x| *x == DbType::START.to_byte());
       
        if let Some(index_of_row_start) = index_of_row_start {
            let deduct = (index_of_row_start as i64 - 8) * -1;

            file.seek(SeekFrom::Current(deduct * -1)).unwrap(); 
            
            *file_position = *file_position - deduct as u64;
            *current_chunk_size = *current_chunk_size - deduct as u64;
        } else {
            panic!("FILE -> 254 DbType::START not found: {:?}. File position: {:?}", 
            check_next_buffer, 
            file.stream_position()?);
        }
    }

    return Ok(());
}

pub(crate) fn skip_empty_spaces_cursor(cursor: &mut Cursor<Vec<u8>>, cursor_length: u32) -> io::Result<()> {
    let cursor_position = cursor.position();
    if cursor_length == cursor_position as u32 || cursor_length < cursor_position as u32 {
        return Ok(());
    }

    let mut check_next_buffer = vec![0; 8];
    cursor.read(&mut check_next_buffer).unwrap();

    if check_next_buffer == EMPTY_BUFFER {
        loop {
            cursor.read(&mut check_next_buffer).unwrap();

            let cursor_position = cursor.position();
            if cursor_length == cursor_position as u32 || cursor_length < cursor_position as u32 {
                return Ok(());
            }

            if check_next_buffer != EMPTY_BUFFER {
                break;
            }
        }

        let index_of_row_start = check_next_buffer.iter().position(|x| *x == DbType::START.to_byte()).unwrap();

        let move_back = -8 as i64 + index_of_row_start as i64;

        cursor.seek(SeekFrom::Current(move_back)).unwrap();
    } else {
        let index_of_row_start = check_next_buffer.iter().position(|x| *x == DbType::START.to_byte());

        if let Some(index_of_row_start) = index_of_row_start {
            let deduct = (index_of_row_start as i64 - 8) * -1;

            cursor.seek(SeekFrom::Current(deduct * -1)).unwrap(); 
        } else {
            panic!("CURSOR -> 254 DbType::START not found: VECTOR {:?}|POSITION {}", check_next_buffer, cursor.position());
        }
    }

    return Ok(());
}

pub(crate) fn row_prefetching(
    io_sync: &mut impl IOOperationsSync, 
    file_position: &mut u64, 
    file_length: u64) -> io::Result<Option<RowPrefetchResult>> {
    let mut io_sync = skip_empty_spaces_file(io_sync, file_position, file_length).unwrap();

    let mut cursor = io_sync.read_data_to_cursor(file_position, 1 + 8 + 4);

    let start_now_byte = cursor.read_u8();

    if start_now_byte.is_err() {
        return Ok(None);
    }

    let start_row = DbType::from_byte(start_now_byte.unwrap());

    if start_row != DbType::START {
        panic!("Start row signal not present.");
    }

    let found_id = cursor.read_u64::<LittleEndian>().unwrap();

    let length = cursor.read_u32::<LittleEndian>().unwrap();

    return Ok(Some(RowPrefetchResult {
        found_id: found_id,
        length: length
    }));
}

pub(crate) fn row_prefetching_file_index(
    file: &mut File, 
    file_position: &mut u64, 
    current_chunk_size: &mut u64, 
    file_length: u64) -> io::Result<Option<RowPrefetchResult>> {
    skip_empty_spaces_file_index(file, file_position, current_chunk_size, file_length).unwrap();

    let start_now_byte = file.read_u8();

    *file_position += 1;
    *current_chunk_size += 1;

    if start_now_byte.is_err() {
        return Ok(None);
    }

    let start_row = DbType::from_byte(start_now_byte.unwrap());

    if start_row != DbType::START {
        panic!("Start row signal not present.");
    }

    let found_id = file.read_u64::<LittleEndian>().unwrap();

    *file_position += 8;
    *current_chunk_size += 8;

    let length = file.read_u32::<LittleEndian>().unwrap();

    *file_position += 4;
    *current_chunk_size += 4;

    return Ok(Some(RowPrefetchResult {
        found_id: found_id,
        length: length
    }));
}

pub(crate) fn row_prefetching_cursor(
    cursor: &mut Cursor<Vec<u8>>, 
    chunk: &FileChunk) -> io::Result<Option<RowPrefetchResult>> {

    skip_empty_spaces_cursor(cursor, chunk.chunk_size).unwrap();
                    
    let start_now_byte = cursor.read_u8();

    if start_now_byte.is_err() {
        return Ok(None);
    }

    let start_row = DbType::from_byte(start_now_byte.unwrap());

    if start_row != DbType::START {
        panic!("Start row signal not present.");
    }

    let found_id = cursor.read_u64::<LittleEndian>().unwrap();

    let length = cursor.read_u32::<LittleEndian>().unwrap();

    return Ok(Some(RowPrefetchResult {
        found_id: found_id,
        length: length
    }));
}

pub(crate) fn add_in_memory_index(
    current_chunk_size: &mut u32,
    current_id: u64,
    file_position: u64,
    in_memory_index: &mut Option<Vec<FileChunk>>) {

    if *current_chunk_size > CHUNK_SIZE {
        if let Some(file_chunks_indexes) = in_memory_index.as_mut() {
            let entry = FileChunk {
                current_file_position: file_position,
                chunk_size: *current_chunk_size,
                next_row_id: current_id
            };
            file_chunks_indexes.push(entry);
        } else {
            let mut chunks_vec: Vec<FileChunk> = Vec::default();

            let first_entry = FileChunk {
                current_file_position: HEADER_SIZE as u64,
                chunk_size: *current_chunk_size,
                next_row_id: current_id
            };
            
            chunks_vec.push(first_entry);
            *in_memory_index = Some(chunks_vec);
        }
        
        *current_chunk_size = 0;
    }
}

pub(crate) fn add_last_in_memory_index(
    current_chunk_size: u32,
    file_position: u64,
    file_length: u64,
    in_memory_index: &mut Option<Vec<FileChunk>>) {

    if file_position < file_length {
        if let Some(file_chunks_indexes) = in_memory_index.as_mut() {
            let entry = FileChunk {
                current_file_position: file_position,
                chunk_size: current_chunk_size,
                next_row_id: 0
            };
            file_chunks_indexes.push(entry);
        } else {
            let mut chunks_vec: Vec<FileChunk> = Vec::default();

            let first_entry = FileChunk {
                current_file_position: HEADER_SIZE as u64,
                chunk_size: current_chunk_size,
                next_row_id: 0
            };
            
            chunks_vec.push(first_entry);
            *in_memory_index = Some(chunks_vec);
        }
    }
}

pub(crate) fn indexed_row_fetching_file( 
    file: &mut File, 
    position: u64, 
    length: u32) -> io::Result<Row> {
    let mut buffer: Vec<u8> = vec![0; (length + 1 + 1 + 8 + 4) as usize];

    file.seek(SeekFrom::Start(position))?;
    file.read(&mut buffer).unwrap();

    let mut cursor = Cursor::new(buffer);

    let start_now_byte = cursor.read_u8();

    let start_row = DbType::from_byte(start_now_byte.unwrap());

    if start_row != DbType::START {
        panic!("Start row signal not present.");
    }

    let found_id = cursor.read_u64::<LittleEndian>().unwrap();

    let length = cursor.read_u32::<LittleEndian>().unwrap();

    let mut columns: Vec<Column> = Vec::default();
    
    loop {
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

        columns.push(column);
    }

    let mut columns_buffer: Vec<u8> = Vec::default();

    for mut column in columns {
        columns_buffer.append(&mut column.into_vec().unwrap());
    }

    return Ok(Row {
        id: found_id,
        length: length,
        columns_data: columns_buffer
    });
}