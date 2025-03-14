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
    simds::endianess::{read_u32, read_u64, read_u8},
    CHUNK_SIZE, EMPTY_BUFFER, HEADER_SIZE,
};

pub(crate) fn read_row_cursor<'a>(
    first_column_index: u64,
    id: u64,
    length: u32,
    cursor: &mut Cursor<&Vec<u8>>,
) -> io::Result<Row> {
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

        let column = Column::from_raw_clone(column_type, &data_buffer);

        columns.push(column);
    }

    let mut columns_buffer: Vec<u8> = Vec::default();

    for column in columns {
        columns_buffer.append(&mut &mut column.content.to_vec());
    }

    return Ok(Row {
        id: id,
        length: length,
        columns_data: columns_buffer,
    });
}

pub(crate) async fn read_row_columns(
    io_sync: &mut Box<impl IOOperationsSync>,
    first_column_index: u64,
    id: u64,
    length: u32,
) -> io::Result<Row> {
    let mut columns: Vec<Column> = Vec::default();

    let mut position = first_column_index;

    let mut cursor = io_sync.read_data_to_cursor(&mut position, length).await;

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

            data_buffer.append(&mut preset_buffer);
        } else {
            let str_length = cursor.read_u32::<LittleEndian>().unwrap();

            let mut preset_buffer = vec![0; str_length as usize];
            cursor.read_exact(&mut preset_buffer)?;

            data_buffer.append(&mut preset_buffer);
        }

        let column = Column::from_raw_clone(column_type, &data_buffer);

        columns.push(column);
    }

    let mut columns_buffer: Vec<u8> = Vec::default();

    for column in columns {
        columns_buffer.append(&mut &mut column.content.to_vec());
    }

    return Ok(Row {
        id: id,
        length: length,
        columns_data: columns_buffer,
    });
}

pub(crate) async fn delete_row_file(
    first_column_index: u64,
    io_sync: &mut Box<impl IOOperationsSync>,
) -> io::Result<()> {
    // LEN (4)
    let mut position = first_column_index.clone() - 4;

    let columns_length = io_sync.read_data(&mut position, 4).await;
    let columns_length = Cursor::new(columns_length)
        .read_u32::<LittleEndian>()
        .unwrap();

    //ID (8) LEN (4) COLUMNS (columns_length) START (1) END (1)
    let empty_buffer = vec![0; (columns_length + 4 + 8 + 1 + 1) as usize];

    // Write the empty buffer to overwrite the data
    //ID (8) LEN (4) START (1)
    io_sync.write_data_seek(
        SeekFrom::Start(first_column_index - 4 - 8 - 1),
        &empty_buffer,
    ).await;
    io_sync.verify_data_and_sync(first_column_index - 4 - 8 - 1, &empty_buffer).await;

    return Ok(());
}

pub(crate) async fn skip_empty_spaces_file(
    io_sync: &mut Box<impl IOOperationsSync>,
    file_position: &mut u64,
    file_length: u64,
) -> u64 {
    if file_length == *file_position || file_length < *file_position {
        return *file_position;
    }

    let mut check_next_buffer = io_sync.read_data(file_position, 8).await;

    if check_next_buffer == EMPTY_BUFFER {
        loop {
            io_sync
                .read_data_into_buffer(file_position, &mut check_next_buffer)
                .await;

            if file_length == *file_position || file_length < *file_position {
                return *file_position;
            }

            if check_next_buffer != EMPTY_BUFFER {
                break;
            }
        }

        let index_of_row_start = check_next_buffer
            .iter()
            .position(|x| *x == DbType::START.to_byte())
            .unwrap();

        let move_back = -8 as i64 + index_of_row_start as i64;

        *file_position = ((*file_position) as i64 + move_back) as u64;

        return *file_position;
    } else {
        let index_of_row_start = check_next_buffer
            .iter()
            .position(|x| *x == DbType::START.to_byte());

        if let Some(index_of_row_start) = index_of_row_start {
            let deduct = (index_of_row_start as i64 - 8) * -1;

            *file_position = ((*file_position) as i64 - deduct) as u64;
        } else {
            panic!(
                "FILE -> 254 DbType::START not found: {:?}",
                check_next_buffer
            );
        }
    }

    return *file_position;
}

pub(crate) fn skip_empty_spaces_cursor(
    position: &mut u64,
    cursor_vector: &mut CursorVector,
    cursor_length: u32,
) -> io::Result<()> {
    if cursor_length <= *position as u32 + 8 {
        return Ok(());
    }

    let check_next_buffer =
        &cursor_vector.vector.as_slice()[*position as usize..*position as usize + 8];

    let check_next_buffer_ptr = check_next_buffer.as_ptr();

    #[cfg(target_arch = "x86_64")]
    {
        unsafe { _mm_prefetch::<_MM_HINT_T0>(check_next_buffer_ptr as *const i8) };
    }

    *position += 8;

    if check_next_buffer == EMPTY_BUFFER {
        loop {
            let check_next_buffer =
                &cursor_vector.vector.as_slice()[*position as usize..*position as usize + 8];

            let check_next_buffer_ptr = check_next_buffer.as_ptr();

            #[cfg(target_arch = "x86_64")]
            {
                unsafe { _mm_prefetch::<_MM_HINT_T0>(check_next_buffer_ptr as *const i8) };
            }

            *position += 8;

            let cursor_position = *position;

            if cursor_length == cursor_position as u32 || cursor_length < cursor_position as u32 {
                return Ok(());
            }

            if !(check_next_buffer == EMPTY_BUFFER) {
                break;
            }
        }

        let index_of_row_start = check_next_buffer
            .iter()
            .position(|x| *x == DbType::START.to_byte())
            .unwrap();

        let move_back = -8 as i64 + index_of_row_start as i64;

        *position -= (move_back * -1) as u64;
    } else {
        let index_of_row_start = check_next_buffer
            .iter()
            .position(|x| *x == DbType::START.to_byte());

        if let Some(index_of_row_start) = index_of_row_start {
            let deduct = (index_of_row_start as i64 - 8) * -1;

            *position -= deduct as u64;
        } else {
            panic!(
                "CURSOR -> 254 DbType::START not found: VECTOR {:?}|POSITION {}",
                check_next_buffer, position
            );
        }
    }

    return Ok(());
}

pub(crate) async fn row_prefetching(
    io_sync: &mut Box<impl IOOperationsSync>,
    file_position: &mut u64,
    file_length: u64,
    is_mutated: bool
) -> io::Result<Option<RowPrefetchResult>> {
    if *file_position < file_length {
        if is_mutated {
            *file_position = skip_empty_spaces_file(io_sync, file_position, file_length).await;
        }
        
        let mut cursor = io_sync.read_data_to_cursor(file_position, 1 + 8 + 4).await;

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
            length: length,
        }));
    } else {
        return Ok(None);
    }
}

#[inline(always)]
pub fn row_prefetching_cursor(
    position: &mut u64,
    cursor_vector: &mut CursorVector,
    chunk: &FileChunk,
    is_mutated: bool
) -> io::Result<Option<RowPrefetchResult>> {
    if is_mutated {
        skip_empty_spaces_cursor(position, cursor_vector, chunk.chunk_size).unwrap();
    }

    let slice = cursor_vector.vector.as_slice();

    if chunk.chunk_size <= *position as u32 + 8 {
        return Ok(None);
    }

    let header_bytes = &slice[*position as usize..*position as usize + 13];

    #[allow(static_mut_refs)]
    unsafe {
        let start_bytes = [header_bytes[0]];

        let found_id_bytes = [
            header_bytes[1],
            header_bytes[2],
            header_bytes[3],
            header_bytes[4],
            header_bytes[5],
            header_bytes[6],
            header_bytes[7],
            header_bytes[8],
        ];

        let length_bytes = [
            header_bytes[9],
            header_bytes[10],
            header_bytes[11],
            header_bytes[12],
        ];

        let start_now_ptr = start_bytes.as_ptr();

        #[cfg(target_arch = "x86_64")]
        {
            _mm_prefetch::<_MM_HINT_T0>(start_now_ptr as *const i8);
        }

        let start_signal = read_u8(start_now_ptr);

        let start_signal_type = DbType::from_byte(start_signal);

        if start_signal_type != DbType::START {
            panic!("Start row signal not present.");
        }

        let found_id_ptr = found_id_bytes.as_ptr();

        #[cfg(target_arch = "x86_64")]
        {
            _mm_prefetch::<_MM_HINT_T0>(found_id_ptr as *const i8);
        }

        let found_id = read_u64(found_id_ptr);

        let length_ptr = length_bytes.as_ptr();

        #[cfg(target_arch = "x86_64")]
        {
            _mm_prefetch::<_MM_HINT_T0>(length_ptr as *const i8);
        }

        let length = read_u32(length_ptr);

        *position += 13;

        return Ok(Some(RowPrefetchResult {
            found_id: found_id,
            length: length,
        }));
    }
}

pub(crate) fn add_in_memory_index(
    current_chunk_size: &mut u32,
    current_id: u64,
    file_position: u64,
    in_memory_index: &mut Option<Vec<FileChunk>>,
) -> bool {
    if *current_chunk_size > CHUNK_SIZE {
        if let Some(file_chunks_indexes) = in_memory_index.as_mut() {
            let entry = FileChunk {
                current_file_position: file_position - *current_chunk_size as u64,
                chunk_size: *current_chunk_size,
                next_row_id: current_id,
            };
            file_chunks_indexes.push(entry);
        } else {
            let mut chunks_vec: Vec<FileChunk> = Vec::default();
            let first_entry = FileChunk {
                current_file_position: HEADER_SIZE as u64,
                chunk_size: *current_chunk_size,
                next_row_id: current_id,
            };

            chunks_vec.push(first_entry);
            *in_memory_index = Some(chunks_vec);
        }

        *current_chunk_size = 0;

        true
    } else {
        false
    }
}

pub(crate) fn add_last_in_memory_index(
    file_position: u64,
    file_length: u64,
    in_memory_index: &mut Option<Vec<FileChunk>>,
) {
    if file_position <= file_length {
        if let Some(file_chunks_indexes) = in_memory_index.as_mut() {
            let entry = FileChunk {
                current_file_position: file_position,
                chunk_size: file_length as u32 - file_position as u32,
                next_row_id: 0,
            };
            file_chunks_indexes.push(entry);
        } else {
            let mut chunks_vec: Vec<FileChunk> = Vec::default();

            let first_entry = FileChunk {
                current_file_position: HEADER_SIZE as u64,
                chunk_size: file_length as u32 - file_position as u32,
                next_row_id: 0,
            };

            chunks_vec.push(first_entry);
            *in_memory_index = Some(chunks_vec);
        }
    }
}

pub(crate) async fn indexed_row_fetching_file(
    io_sync: &mut Box<impl IOOperationsSync>,
    position: &mut u64,
    length: u32,
) -> io::Result<Row> {
    let mut cursor = io_sync
        .read_data_to_cursor(position, (length + 1 + 1 + 8 + 4) as u32)
        .await;

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

        let column = Column::from_raw_clone(column_type, &data_buffer);

        columns.push(column);
    }

    let mut columns_buffer: Vec<u8> = Vec::default();

    for column in columns {
        columns_buffer.append(&mut &mut column.content.to_vec());
    }

    return Ok(Row {
        id: found_id,
        length: length,
        columns_data: columns_buffer,
    });
}

pub(crate) fn columns_cursor_to_row(
    mut columns_cursor: Cursor<Vec<u8>>,
    id: u64,
    length: u32,
) -> io::Result<Row> {
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

        let column = Column::from_raw_clone(column_type, &data_buffer);

        columns.push(column);
    }

    let mut columns_buffer: Vec<u8> = Vec::default();

    for column in columns {
        columns_buffer.append(&mut column.content.to_vec());
    }

    return Ok(Row {
        id: id,
        length: length,
        columns_data: columns_buffer,
    });
}
