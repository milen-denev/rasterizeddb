use std::{
    arch::x86_64::{_mm_prefetch, _MM_HINT_T0},
    io::{self, Cursor, Read, Seek, SeekFrom}
};

use byteorder::{LittleEndian, ReadBytesExt};

use super::{
    column::Column,
    db_type::DbType,
    row::Row,
    storage_providers::traits::IOOperationsSync,
    support_types::{CursorVector, FileChunk, RowPrefetchResult},
};

use crate::{
    simds::endianess::{read_u32, read_u64},
    EMPTY_BUFFER,
};

#[inline(always)]
pub(crate) fn read_row_cursor_whole<'a>(
    first_column_index: u64,
    id: u64,
    length: u32,
    cursor: &mut Cursor<&Vec<u8>>,
) -> io::Result<Row> {
    cursor.seek(SeekFrom::Start(first_column_index)).unwrap();
    let mut columns_buffer: Vec<u8> = vec![0; length as usize];

    cursor.read_exact(&mut columns_buffer).unwrap();

    Ok(Row {
        id,
        length,
        columns_data: columns_buffer,
    })
}

#[inline(always)]
pub(crate) async fn read_row_columns(
    io_sync: &Box<impl IOOperationsSync>,
    first_column_index: u64,
    id: u64,
    length: u32,
) -> io::Result<Row> {
    let mut columns: Vec<Column> = Vec::with_capacity(8); // Typical row has several columns
    let mut position = first_column_index;
    let mut cursor = io_sync.read_data_to_cursor(&mut position, length).await;
    let mut columns_buffer: Vec<u8> = Vec::with_capacity(length as usize);

    loop {
        let column_type = cursor.read_u8().unwrap();
        let db_type = DbType::from_byte(column_type);

        if db_type == DbType::END {
            break;
        }

        let data_buffer = if db_type != DbType::STRING {
            let db_size = db_type.get_size();
            let mut buffer = vec![0; db_size as usize];
            cursor.read_exact(&mut buffer)?;
            buffer
        } else {
            let str_length = cursor.read_u32::<LittleEndian>().unwrap();
            let mut buffer = vec![0; str_length as usize];
            cursor.read_exact(&mut buffer)?;
            buffer
        };

        let column = Column::from_raw_clone(column_type, &data_buffer);
        columns.push(column);
        columns_buffer.extend_from_slice(&data_buffer);
    }

    Ok(Row {
        id,
        length,
        columns_data: columns_buffer,
    })
}

#[inline(always)]
pub(crate) async fn delete_row_file(
    first_column_index: u64,
    io_sync: &mut Box<impl IOOperationsSync>,
) -> io::Result<()> {
    // LEN (4)
    let mut position = first_column_index - 4;

    let columns_length = io_sync.read_data(&mut position, 4).await;
    let columns_length = Cursor::new(columns_length)
        .read_u32::<LittleEndian>()
        .unwrap();

    // Create a buffer of zeros to overwrite the row data
    // ID (8) LEN (4) COLUMNS (columns_length) START (1) END (1)
    let empty_buffer = vec![0; (columns_length + 4 + 8 + 1 + 1) as usize];

    // Write the empty buffer to overwrite the data
    // ID (8) LEN (4) START (1)
    io_sync.write_data_seek(
        SeekFrom::Start(first_column_index - 4 - 8 - 1),
        &empty_buffer,
    ).await;
    io_sync.verify_data_and_sync(first_column_index - 4 - 8 - 1, &empty_buffer).await;

    Ok(())
}

#[inline(always)]
pub(crate) async fn skip_empty_spaces_file(
    io_sync: &Box<impl IOOperationsSync>,
    file_position: &mut u64,
    file_length: u64,
) -> u64 {
    if file_length <= *file_position {
        return *file_position;
    }

    // Use a loop instead of recursion to prevent stack overflow
    let mut continue_search = true;
    
    while continue_search {
        // Don't read past the end of file
        if file_length <= *file_position {
            return *file_position;
        }
        
        let mut check_next_buffer = io_sync.read_data(file_position, 8).await;
        
        if check_next_buffer == EMPTY_BUFFER {
            // Skip through empty spaces in larger blocks when possible
            loop {
                io_sync
                    .read_data_into_buffer(file_position, &mut check_next_buffer)
                    .await;
                
                if file_length <= *file_position {
                    return *file_position;
                }
                
                if check_next_buffer != EMPTY_BUFFER {
                    break;
                }
            }
            
            // Find the exact position of the row start
            if let Some(index_of_row_start) = check_next_buffer
                .iter()
                .position(|x| *x == DbType::START.to_byte())
            {
                let move_back = -8i64 + index_of_row_start as i64;
                *file_position = ((*file_position) as i64 + move_back) as u64;
                continue_search = false; // Found start marker, exit the loop
            } else {
                // If no start marker found, move forward a byte and try again
                // This is safer than moving backward which could cause an infinite loop
                *file_position = *file_position - check_next_buffer.len() as u64 + 1;
                // Continue the outer loop to search again
            }
        } else {
            if let Some(index_of_row_start) = check_next_buffer
                .iter()
                .position(|x| *x == DbType::START.to_byte())
            {
                let deduct = (index_of_row_start as i64 - 8) * -1;
                *file_position = ((*file_position) as i64 - deduct) as u64;
                continue_search = false; // Found start marker, exit the loop
            } else {
                // If no start marker found, move forward and try again
                *file_position += 1; // Move forward just one byte to avoid skipping data
                // Continue the outer loop to search again
            }
        }
        
        // Safety check to prevent infinite loops
        if *file_position >= file_length {
            return file_length;
        }
    }
    
    *file_position
}

#[inline(always)]
pub(crate) fn skip_empty_spaces_cursor(
    position: &mut u64,
    cursor_vector: &mut CursorVector,
    cursor_length: u32,
) -> io::Result<()> {
    if cursor_length as u64 <= *position + 8 {
        return Ok(());
    }

    // Create a slice for direct array access instead of multiple index operations
    let pos = *position as usize;
    let check_next_buffer = &cursor_vector.vector[pos..pos + 8];
    let check_next_buffer_ptr = check_next_buffer.as_ptr();

    #[cfg(target_arch = "x86_64")]
    {
        unsafe { _mm_prefetch::<_MM_HINT_T0>(check_next_buffer_ptr as *const i8) };
    }

    *position += 8;

    if check_next_buffer == EMPTY_BUFFER {
        // Fast-forward through empty spaces
        let mut current_pos = *position as usize;
        let vec_slice = cursor_vector.vector.as_slice();
        let end_pos = (cursor_length as usize).min(vec_slice.len());
        
        while current_pos + 8 <= end_pos {
            let chunk = &vec_slice[current_pos..current_pos + 8];
            
            #[cfg(target_arch = "x86_64")]
            {
                unsafe { _mm_prefetch::<_MM_HINT_T0>(chunk.as_ptr() as *const i8) };
            }
            
            if chunk != EMPTY_BUFFER {
                // Found non-empty data
                break;
            }
            
            current_pos += 8;
        }
        
        *position = current_pos as u64;
        
        if cursor_length as u64 <= *position {
            return Ok(());
        }
        
        // Find the exact position of the row start
        let remaining = &cursor_vector.vector[*position as usize..];
        if let Some(index_of_row_start) = remaining
            .iter()
            .position(|x| *x == DbType::START.to_byte())
        {
            *position += index_of_row_start as u64;
        } else {
            // If we can't find a start marker, return to caller
            return Ok(());
        }
    } else {
        if let Some(index_of_row_start) = check_next_buffer
            .iter()
            .position(|x| *x == DbType::START.to_byte())
        {
            let deduct = (index_of_row_start as i64 - 8) * -1;
            *position -= deduct as u64;
        } else {
            panic!(
                "CURSOR -> 254 DbType::START not found: VECTOR {:?}|POSITION {}",
                check_next_buffer, position
            );
        }
    }

    Ok(())
}

#[inline(always)]
pub(crate) async fn row_prefetching(
    io_sync: &Box<impl IOOperationsSync>,
    file_position: &mut u64,
    file_length: u64,
    is_mutated: bool
) -> io::Result<Option<RowPrefetchResult>> {
    if *file_position >= file_length {
        return Ok(None);
    }

    if is_mutated {
        *file_position = skip_empty_spaces_file(io_sync, file_position, file_length).await;
    }
    
    // Read header data in one operation to minimize I/O
    let mut cursor = io_sync.read_data_to_cursor(file_position, 1 + 8 + 4).await;

    let start_now_byte = cursor.read_u8();
    if start_now_byte.is_err() {
        return Ok(None);
    }

    let start_byte = start_now_byte.unwrap();
    if start_byte != DbType::START.to_byte() {
        panic!("Start row signal not present.");
    }

    let found_id = cursor.read_u64::<LittleEndian>().unwrap();
    let length = cursor.read_u32::<LittleEndian>().unwrap();

    Ok(Some(RowPrefetchResult {
        found_id,
        length,
    }))
}

#[inline(always)]
#[track_caller]
pub fn row_prefetching_cursor(
    position: &mut u64,
    cursor_vector: &mut CursorVector,
    chunk: &FileChunk,
    is_mutated: bool
) -> io::Result<Option<RowPrefetchResult>> {
    if is_mutated {
        skip_empty_spaces_cursor(position, cursor_vector, chunk.chunk_size)?;
    }

    let slice = cursor_vector.vector.as_slice();

    // Check if we have enough data to read
    if chunk.chunk_size <= *position as u32 + 13 {
        return Ok(None);
    }

    // Use direct slice access for better performance
    let pos = *position as usize;
    let start_byte = slice[pos];
    
    // Prefetch the data we're about to read
    #[cfg(target_arch = "x86_64")]
    unsafe {
        _mm_prefetch::<_MM_HINT_T0>(&slice[pos] as *const u8 as *const i8);
    }

    if start_byte != DbType::START.to_byte() {
        panic!("Start row signal not present.");
    }

    // Read ID and length using SIMD-optimized functions
    let id_ptr = &slice[pos + 1] as *const u8;
    let len_ptr = &slice[pos + 9] as *const u8;
    
    let found_id = unsafe { read_u64(id_ptr) };
    let length = unsafe { read_u32(len_ptr) };

    // Update position
    *position += 13;

    Ok(Some(RowPrefetchResult {
        found_id,
        length,
    }))
}

#[inline(always)]
pub(crate) async fn indexed_row_fetching_file(
    io_sync: &Box<impl IOOperationsSync>,
    position: &mut u64,
    length: u32,
) -> io::Result<Row> {
    // Read all the data we need in one operation to reduce I/O overhead
    let full_length = length + 1 + 1 + 8 + 4;
    let mut cursor = io_sync.read_data_to_cursor(position, full_length).await;

    // Process row header
    let start_now_byte = cursor.read_u8();
    if start_now_byte.is_err() || start_now_byte.unwrap() != DbType::START.to_byte() {
        panic!("Start row signal not present.");
    }

    let found_id = cursor.read_u64::<LittleEndian>().unwrap();
    let length = cursor.read_u32::<LittleEndian>().unwrap();

    // Process columns with pre-allocated vectors
    let mut columns: Vec<Column> = Vec::with_capacity(8); // Typical row has several columns
    let mut columns_buffer: Vec<u8> = Vec::with_capacity(length as usize);

    loop {
        let column_type = cursor.read_u8().unwrap();
        let db_type = DbType::from_byte(column_type);

        if db_type == DbType::END {
            break;
        }

        let data_buffer = if db_type != DbType::STRING {
            let db_size = db_type.get_size();
            let mut buffer = vec![0; db_size as usize];
            cursor.read(&mut buffer).unwrap();
            buffer
        } else {
            let str_length = cursor.read_u32::<LittleEndian>().unwrap();
            let mut buffer = vec![0; str_length as usize];
            cursor.read(&mut buffer).unwrap();
            buffer
        };

        let column = Column::from_raw_clone(column_type, &data_buffer);
        columns.push(column);
        columns_buffer.extend_from_slice(&data_buffer);
    }

    Ok(Row {
        id: found_id,
        length,
        columns_data: columns_buffer,
    })
}

#[inline(always)]
pub(crate) fn columns_cursor_to_row_whole(
    mut columns_cursor: Cursor<Vec<u8>>,
    id: u64,
    length: u32,
) -> io::Result<Row> {
    columns_cursor.set_position(0);
    let mut columns_buffer: Vec<u8> = Vec::with_capacity(length as usize);
    let cursor_data = columns_cursor.get_ref();
    
    columns_buffer.extend_from_slice(&cursor_data[..length as usize]);

    Ok(Row {
        id,
        length,
        columns_data: columns_buffer,
    })
}