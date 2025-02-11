use std::io::{Read, Seek, SeekFrom};

use super::{db_type::DbType, support_types::CursorVector};

pub fn extent_non_string_buffer(
    data_buffer: &mut Vec<u8>, 
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
            cursor_vector.vector[(*position + 1) as usize]
        ];
        *position += 2;

        data_buffer.copy_from_slice(&preset_array);
    } else if db_size == 4 {
        let preset_array: [u8; 4] = [
            cursor_vector.vector[*position as usize],
            cursor_vector.vector[(*position + 1) as usize],
            cursor_vector.vector[(*position + 2) as usize],
            cursor_vector.vector[(*position + 3) as usize]
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
            cursor_vector.vector[(*position + 7) as usize]
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
            cursor_vector.vector[(*position + 15) as usize]
        ];
        *position += 16;

        data_buffer.copy_from_slice(&preset_array);
    } else {
        let cursor = &mut cursor_vector.cursor;
        cursor.seek(SeekFrom::Start(*position)).unwrap();
        let mut preset_buffer = vec![0; db_size as usize];
        cursor.read(&mut preset_buffer).unwrap();
        data_buffer.append(&mut preset_buffer);
        *position += db_size as u64;
    }
}