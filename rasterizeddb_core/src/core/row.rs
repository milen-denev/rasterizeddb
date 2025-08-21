use smallvec::SmallVec;

use super::column::Column;
use super::support_types::ReturnResult;

#[derive(Debug, Clone)]
pub struct Row {
    pub id: u64,
    pub length: u32,
    pub columns_data: Vec<u8>,
}

impl Row {
    /// Serializes the Row into a Vec<u8>
    pub fn into_vec(&self) -> Vec<u8> {
        use byteorder::{LittleEndian, WriteBytesExt};
        
        // Create a buffer with enough capacity
        let mut buffer = Vec::with_capacity(8 + 4 + self.columns_data.len());
        
        // Write row ID
        buffer.write_u64::<LittleEndian>(self.id).unwrap();
        
        // Write length
        buffer.write_u32::<LittleEndian>(self.length).unwrap();
        
        // Write columns data
        buffer.extend_from_slice(&self.columns_data);
        
        buffer
    }
    
    /// Deserializes a Vec<u8> into a Row
    pub fn from_vec(buffer: &[u8]) -> Result<Self, std::io::Error> {
        use byteorder::{LittleEndian, ReadBytesExt};
        use std::io::Cursor;
        
        // Create a cursor to read from the buffer
        let mut cursor = Cursor::new(buffer);
        
        // Read row ID
        let id = cursor.read_u64::<LittleEndian>()?;
        
        // Read length
        let length = cursor.read_u32::<LittleEndian>()?;
        
        // Extract columns data
        let position = cursor.position() as usize;
        let columns_data = buffer[position..].to_vec();
        
        Ok(Row {
            id,
            length,
            columns_data,
        })
    }
    
    /// Serializes Option<ReturnResult> into a Vec<u8>
    pub fn serialize_rows(result: Option<ReturnResult>) -> Vec<u8> {
        use byteorder::{LittleEndian, WriteBytesExt};
        
        match result {
            None => {
                // Return a single byte 0 to indicate None
                vec![0]
            },
            Some(return_result) => {
                match return_result {
                    ReturnResult::Rows(rows) => {
                        // Calculate buffer size (marker + type + row count + all row data)
                        let total_size = 1 + 1 + 4 + rows.iter()
                            .map(|row| 8 + 4 + row.columns_data.len())
                            .sum::<usize>();
                        
                        let mut buffer = Vec::with_capacity(total_size);
                        
                        // Write marker byte 1 to indicate Some
                        buffer.push(1);
                        
                        // Write type marker 0 to indicate Rows
                        buffer.push(0);
                        
                        // Write number of rows as u32
                        buffer.write_u32::<LittleEndian>(rows.len() as u32).unwrap();
                        
                        // Write each row's serialized data
                        for row in rows {
                            let row_data = row.into_vec();
                            buffer.extend_from_slice(&row_data);
                        }
                        
                        buffer
                    },
                    ReturnResult::HtmlView(html) => {
                        // Calculate buffer size (marker + type + string length + string data)
                        let html_bytes = html.as_bytes();
                        let total_size = 1 + 1 + 4 + html_bytes.len();
                        
                        let mut buffer = Vec::with_capacity(total_size);
                        
                        // Write marker byte 1 to indicate Some
                        buffer.push(1);
                        
                        // Write type marker 1 to indicate HtmlView
                        buffer.push(1);
                        
                        // Write string length as u32
                        buffer.write_u32::<LittleEndian>(html_bytes.len() as u32).unwrap();
                        
                        // Write string data
                        buffer.extend_from_slice(html_bytes);
                        
                        buffer
                    }
                }
            }
        }
    }
    
    /// Deserializes a Vec<u8> into Option<ReturnResult>
    pub fn deserialize_rows(buffer: &[u8]) -> Result<Option<ReturnResult>, std::io::Error> {
        use byteorder::{LittleEndian, ReadBytesExt};
        use std::io::{Cursor, Error, ErrorKind};
        
        if buffer.is_empty() {
            return Err(Error::new(ErrorKind::InvalidData, "Empty buffer"));
        }
        
        // Read marker byte
        let marker = buffer[0];
        
        match marker {
            0 => Ok(None), // None marker
            1 => {
                // Read the type marker
                if buffer.len() < 2 {
                    return Err(Error::new(ErrorKind::InvalidData, "Buffer too short for type marker"));
                }
                
                let type_marker = buffer[1];
                
                match type_marker {
                    0 => {
                        // Type 0: Rows
                        // Create cursor starting after the markers
                        let mut cursor = Cursor::new(&buffer[2..]);
                        
                        // Read number of rows
                        let row_count = cursor.read_u32::<LittleEndian>()?;
                        let mut rows = SmallVec::new();
                        
                        // Starting position for reading row data
                        let mut position = cursor.position() as usize + 2; // +2 for the marker bytes
                        
                        // Read each row
                        for _ in 0..row_count {
                            // Read row ID and length
                            if position + 12 > buffer.len() {
                                return Err(Error::new(ErrorKind::UnexpectedEof, "Buffer too short"));
                            }
                            
                            let mut row_cursor = Cursor::new(&buffer[position..]);
                            let id = row_cursor.read_u64::<LittleEndian>()?;
                            let length = row_cursor.read_u32::<LittleEndian>()?;
                            
                            // Calculate row data size
                            let row_header_size = 12; // 8 bytes for id + 4 bytes for length
                            let row_data_end = position + row_header_size + length as usize;
                            
                            if row_data_end > buffer.len() {
                                return Err(Error::new(ErrorKind::UnexpectedEof, "Buffer too short for row data"));
                            }
                            
                            // Extract row data
                            let columns_data = buffer[position+row_header_size..row_data_end].to_vec();
                            
                            // Create and add row
                            rows.push(Row {
                                id,
                                length,
                                columns_data,
                            });
                            
                            // Move to next row position
                            position = row_data_end;
                        }
                        
                        Ok(Some(ReturnResult::Rows(rows)))
                    },
                    1 => {
                        // Type 1: HtmlView
                        // Create cursor starting after the markers
                        let mut cursor = Cursor::new(&buffer[2..]);
                        
                        // Read string length
                        let string_length = cursor.read_u32::<LittleEndian>()?;
                        
                        // Calculate string data position and check buffer length
                        let string_start = cursor.position() as usize + 2; // +2 for the marker bytes
                        let string_end = string_start + string_length as usize;
                        
                        if string_end > buffer.len() {
                            return Err(Error::new(ErrorKind::UnexpectedEof, "Buffer too short for HTML data"));
                        }
                        
                        // Extract and convert string data
                        let html_bytes = &buffer[string_start..string_end];
                        let html_string = String::from_utf8(html_bytes.to_vec())
                            .map_err(|_| Error::new(ErrorKind::InvalidData, "Invalid UTF-8 in HTML data"))?;
                        
                        Ok(Some(ReturnResult::HtmlView(html_string)))
                    },
                    _ => Err(Error::new(ErrorKind::InvalidData, "Invalid type marker"))
                }
            },
            _ => Err(Error::new(ErrorKind::InvalidData, "Invalid marker byte"))
        }
    }

    /// Extract column data into a Vec<Column>
    pub fn columns(&self) -> Result<Vec<Column>, std::io::Error> {
        Column::from_buffer(&self.columns_data)
    }
}

#[derive(Debug)]
pub struct InsertOrUpdateRow {
    pub columns_data: Vec<u8>,
}
