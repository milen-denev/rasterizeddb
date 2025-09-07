use std::{
    io::{self, Cursor},
    sync::atomic::{AtomicBool, AtomicU32, AtomicU64},
};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

pub struct TableHeader {
    pub(crate) total_file_length: AtomicU64, // Total length of the file in bytes
    pub(crate) shard_number: AtomicU32,      // Identifier for the shard
    pub(crate) shard_id: AtomicU64,          // Hash of the shard
    pub(crate) compressed: AtomicBool, // Compression status: true if compressed, false otherwise
    pub(crate) first_row_id: AtomicU64, // ID of the first row
    pub(crate) last_row_id: AtomicU64, // ID of the last row
    pub(crate) immutable: AtomicBool,  // Append only table
    pub(crate) mutated: AtomicBool,
}

impl TableHeader {
    pub(crate) const fn new(
        total_file_length: u64, //8
        shard_number: u32,      //4
        shard_id: u64,          //8
        compressed: bool,       //1
        first_row_id: u64,      //8
        last_row_id: u64,       //8
        immutable: bool,        //1
    ) -> Self {
        Self {
            total_file_length: AtomicU64::new(total_file_length),
            shard_number: AtomicU32::new(shard_number),
            shard_id: AtomicU64::new(shard_id),
            compressed: AtomicBool::new(compressed),
            first_row_id: AtomicU64::new(first_row_id),
            last_row_id: AtomicU64::new(last_row_id),
            immutable: AtomicBool::new(immutable),
            mutated: AtomicBool::new(false),
        }
    }

    /// Serialize the header into bytes
    pub(crate) fn to_bytes(&self) -> io::Result<Vec<u8>> {
        let mut buffer = Vec::new();

        // Write total_file_length as u64
        buffer
            .write_u64::<LittleEndian>(
                self.total_file_length
                    .load(std::sync::atomic::Ordering::Relaxed),
            )
            .unwrap();

        // Write shard_number as u32
        buffer
            .write_u32::<LittleEndian>(self.shard_number.load(std::sync::atomic::Ordering::Relaxed))
            .unwrap();

        // Write shard hash id as u64
        buffer
            .write_u64::<LittleEndian>(self.shard_id.load(std::sync::atomic::Ordering::Relaxed))
            .unwrap();

        // Write compressed as a single byte (0 for false, 1 for true)
        buffer
            .write_u8(
                if self.compressed.load(std::sync::atomic::Ordering::Relaxed) {
                    1
                } else {
                    0
                },
            )
            .unwrap();

        // Write first_row_id as u64
        buffer
            .write_u64::<LittleEndian>(self.first_row_id.load(std::sync::atomic::Ordering::Relaxed))
            .unwrap();

        // Write last_row_id as u64
        buffer
            .write_u64::<LittleEndian>(self.last_row_id.load(std::sync::atomic::Ordering::Relaxed))
            .unwrap();

        // Write immutable as u8
        buffer
            .write_u8(
                if self.immutable.load(std::sync::atomic::Ordering::Relaxed) {
                    1
                } else {
                    0
                },
            )
            .unwrap();

        // Write mutated as u8
        buffer
            .write_u8(if self.mutated.load(std::sync::atomic::Ordering::Relaxed) {
                1
            } else {
                0
            })
            .unwrap();

        Ok(buffer)
    }

    pub(crate) fn from_buffer(buffer: Vec<u8>) -> io::Result<TableHeader> {
        let mut cursor = Cursor::new(buffer);

        // Read total_file_length as u64
        let total_file_length = cursor.read_u64::<LittleEndian>().unwrap();

        // Read shard_number as u32
        let shard_number = cursor.read_u32::<LittleEndian>().unwrap();

        // Read shard_number as u64
        let shard_id = cursor.read_u64::<LittleEndian>().unwrap();

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
            total_file_length: AtomicU64::new(total_file_length),
            shard_number: AtomicU32::new(shard_number),
            shard_id: AtomicU64::new(shard_id),
            compressed: if compressed == 1 {
                AtomicBool::new(true)
            } else {
                AtomicBool::new(false)
            },
            first_row_id: AtomicU64::new(first_row_id),
            last_row_id: AtomicU64::new(last_row_id),
            immutable: if immutable == 1 {
                AtomicBool::new(true)
            } else {
                AtomicBool::new(false)
            },
            mutated: if mutated == 1 {
                AtomicBool::new(true)
            } else {
                AtomicBool::new(false)
            },
        })
    }
}
