use std::io::{self, Cursor};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

pub struct TableHeader {
    pub(crate) total_file_length: u64, // Total length of the file in bytes
    pub(crate) shard_number: u32,      // Identifier for the shard
    pub(crate) compressed: bool,       // Compression status: true if compressed, false otherwise
    pub(crate) first_row_id: u64,      // ID of the first row
    pub(crate) last_row_id: u64,       // ID of the last row
    pub(crate) immutable: bool,        // Append only table
    pub(crate) mutated: bool
}

impl TableHeader {
    pub(crate) fn new(
        total_file_length: u64, //8
        shard_number: u32,      //4
        compressed: bool,       //1
        first_row_id: u64,      //8
        last_row_id: u64,       //8
        immutable: bool,        //1
    ) -> Self {
        Self {
            total_file_length,
            shard_number,
            compressed,
            first_row_id,
            last_row_id,
            immutable: immutable,
            mutated: false
        }
    }

    /// Serialize the header into bytes
    pub(crate) fn to_bytes(&self) -> io::Result<Vec<u8>> {
        let mut buffer = Vec::new();

        // Write total_file_length as u64
        buffer
            .write_u64::<LittleEndian>(self.total_file_length)
            .unwrap();

        // Write shard_number as u32
        buffer.write_u32::<LittleEndian>(self.shard_number).unwrap();

        // Write compressed as a single byte (0 for false, 1 for true)
        buffer
            .write_u8(if self.compressed { 1 } else { 0 })
            .unwrap();

        // Write first_row_id as u64
        buffer.write_u64::<LittleEndian>(self.first_row_id).unwrap();

        // Write last_row_id as u64
        buffer.write_u64::<LittleEndian>(self.last_row_id).unwrap();

        // Write immutable as u8
        buffer.write_u8(if self.immutable { 1 } else { 0 }).unwrap();

        // Write mutated as u8
        buffer.write_u8(if self.mutated { 1 } else { 0 }).unwrap();

        Ok(buffer)
    }

    pub(crate) fn from_buffer(buffer: Vec<u8>) -> io::Result<TableHeader> {
        let mut cursor = Cursor::new(buffer);

        // Read total_file_length as u64
        let total_file_length = cursor.read_u64::<LittleEndian>().unwrap();

        // Read shard_number as u32
        let shard_number = cursor.read_u32::<LittleEndian>().unwrap();

        // Read compressed as a single byte (0 for false, 1 for true)
        let compressed = cursor.read_u8().unwrap();

        // Read first_row_id as u64
        let first_row_id = cursor.read_u64::<LittleEndian>().unwrap();

        // Read last_row_id as u64
        let last_row_id = cursor.read_u64::<LittleEndian>().unwrap();

        // Read immutable as u8
        let immutable = cursor.read_u8().unwrap();

        // Read mutated as u8
        let mutated = cursor.read_u8().unwrap();

        Ok(TableHeader {
            total_file_length: total_file_length,
            shard_number: shard_number,
            compressed: if compressed == 1 { true } else { false },
            first_row_id: first_row_id,
            last_row_id: last_row_id,
            immutable: if immutable == 1 { true } else { false },
            mutated: if mutated == 1 { true } else { false },
        })
    }
}
