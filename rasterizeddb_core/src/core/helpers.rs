use std::io::{self, Cursor, Read, Seek, SeekFrom};

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

pub(crate) fn delete_row_file(
    first_column_index: u64, 
    io_sync: &mut impl IOOperationsSync) -> io::Result<()> {
    let mut position = first_column_index.clone();

    let columns_length = io_sync.read_data(&mut position, 4);
    let columns_length = Cursor::new(columns_length).read_u32::<LittleEndian>().unwrap();

    //ID (8) LEN (4) COLUMNS (columns_length) START (1) END (1)
    let empty_buffer = vec![0; (columns_length + 4 + 8 + 1 + 1) as usize];

    // Write the empty buffer to overwrite the data
    //ID (8) LEN (4) START (1)
    io_sync.write_data_seek(SeekFrom::Start(first_column_index - 4 - 8 - 1), &empty_buffer);

    return Ok(());
}

pub(crate) fn skip_empty_spaces_file(
    io_sync: &mut impl IOOperationsSync, 
    file_position: &mut u64,
    file_length: u64) -> u64 {
    if file_length == *file_position || file_length < *file_position {
        return *file_position;
    }

    let mut check_next_buffer = io_sync.read_data(file_position, 8);

    if check_next_buffer == EMPTY_BUFFER {
        loop {
            io_sync.read_data_into_buffer(file_position, &mut check_next_buffer);

            if file_length == *file_position || file_length < *file_position {
                return *file_position;
            }

            if check_next_buffer != EMPTY_BUFFER {
                break;
            }
        }

        let index_of_row_start = check_next_buffer.iter().position(|x| *x == DbType::START.to_byte()).unwrap();

        let move_back = -8 as i64 + index_of_row_start as i64;

        *file_position = ((*file_position) as i64 + move_back) as u64;

        return *file_position;
    } else {
        let index_of_row_start = check_next_buffer.iter().position(|x| *x == DbType::START.to_byte());
       
        if let Some(index_of_row_start) = index_of_row_start {
            let deduct = (index_of_row_start as i64 - 8) * -1;

            *file_position = ((*file_position) as i64 - deduct) as u64;
        } else {
            panic!("FILE -> 254 DbType::START not found: {:?}", check_next_buffer);
        }
    }

    return *file_position;
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
    if *file_position < file_length {
        *file_position = skip_empty_spaces_file(io_sync, file_position, file_length);

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
    } else {
        return Ok(None);
    }
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
                current_file_position: file_position - *current_chunk_size as u64,
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
    file_position: u64,
    file_length: u64,
    in_memory_index: &mut Option<Vec<FileChunk>>) {

    if file_position <= file_length {
        if let Some(file_chunks_indexes) = in_memory_index.as_mut() {
            let entry = FileChunk {
                current_file_position: file_position,
                chunk_size: file_length as u32 - file_position as u32,
                next_row_id: 0
            };
            file_chunks_indexes.push(entry);
        } else {
            let mut chunks_vec: Vec<FileChunk> = Vec::default();

            let first_entry = FileChunk {
                current_file_position: HEADER_SIZE as u64,
                chunk_size: file_length as u32 - file_position as u32,
                next_row_id: 0
            };
            
            chunks_vec.push(first_entry);
            *in_memory_index = Some(chunks_vec);
        }
    }
}

pub(crate) fn indexed_row_fetching_file( 
    io_sync: &mut impl IOOperationsSync, 
    position: &mut u64, 
    length: u32) -> io::Result<Row> {
    let mut cursor = io_sync.read_data_to_cursor(position, (length + 1 + 1 + 8 + 4) as u32);

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

pub(crate) fn columns_cursor_to_row( 
    mut columns_cursor: Cursor<Vec<u8>>, 
    id: u64,
    length: u32) -> io::Result<Row> {
    let mut columns: Vec<Column> = Vec::default();
    
    columns_cursor.set_position(0);

    loop {
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
            data_buffer.append(&mut preset_buffer.to_vec());
        } else {
            let str_length = columns_cursor.read_u32::<LittleEndian>().unwrap();
            let mut preset_buffer = vec![0; str_length as usize];
            columns_cursor.read(&mut preset_buffer).unwrap();
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