use std::{io::Cursor, sync::{atomic::AtomicU64, Arc}};

use super::{error::Result, row::RowWrite};
use byteorder::{LittleEndian, WriteBytesExt};

#[cfg(feature = "enable_long_row")]
use crc::{Crc, CRC_32_ISO_HDLC};

use itertools::Itertools;

use crate::{
    core::{
        db_type::DbType, row_v2::row::{Column, Row, RowFetch}, storage_providers::traits::StorageIO
    }, 
    memory_pool::{MemoryBlock, MEMORY_POOL}, simds::{self, endianess::*}
};

// Number of row pointers to fetch at once in next_row_pointers
const BATCH_SIZE: usize = 5000;

#[cfg(feature = "enable_long_row")]
const TOTAL_LENGTH : usize = 
    16 + // id (u128)
    8 + // length (u64)
    8 + // position (u64)
    1 + // deleted (bool)
    4 + // checksum (u32)
    4 + // cluster (u32)
    16 + // deleted_at (u128)
    16 + // created_at (u128)
    16 + // updated_at (u128)
    2 + // version (u16)
    1   // is_active (bool)
    ;

#[cfg(not(feature = "enable_long_row"))]
const TOTAL_LENGTH : usize = 
    8 + // id (u64)
    4 + // length (u32)
    8 + // position (u64)
    1 // deleted (bool)
    ; 

#[cfg(feature = "enable_long_row")]
#[cfg(not(debug_assertions))]
const CHUNK_SIZE: usize = TOTAL_LENGTH * 3047; // ~320KB chunks

#[cfg(feature = "enable_long_row")]
#[cfg(debug_assertions)]
const CHUNK_SIZE: usize = TOTAL_LENGTH * 2; // 92 * 2 bytes chunks for debugging

#[cfg(not(feature = "enable_long_row"))]
const CHUNK_SIZE: usize = TOTAL_LENGTH * 3047; // ~64KB chunks

// TODO replace with fastcrc32
#[cfg(feature = "enable_long_row")]
const CRC: Crc::<u32>  = Crc::<u32>::new(&CRC_32_ISO_HDLC);

/// Iterates over RowPointers from a StorageIO in 64KB chunks
pub struct RowPointerIterator<'a, S: StorageIO> {
    /// Reference to the storage provider
    io: &'a mut S,
    /// Current position in the storage
    position: u64,
    /// Buffer to store 64KB chunks
    buffer: MemoryBlock,
    /// Current index in the buffer
    buffer_index: usize,
    /// Valid data length in the buffer
    buffer_valid_length: usize,
    /// Total length of the storage
    total_length: usize,
    /// End of data flag
    end_of_data: bool,
}

/// Implementation for RowPointerIterator providing methods to create and iterate over RowPointers
impl<'a, S: StorageIO> RowPointerIterator<'a, S> {
    /// Create a new RowPointerIterator for the given StorageIO
    pub async fn new(io: &'a mut S) -> Result<Self> {
        let total_length = io.get_len().await as usize;

        let buffer_size = if total_length > CHUNK_SIZE {
            CHUNK_SIZE
        } else {
            total_length
        };

        let memory_block = MEMORY_POOL.acquire(buffer_size);

        let mut iterator = RowPointerIterator {
            io,
            position: 0,
            buffer: memory_block,
            buffer_index: 0,
            buffer_valid_length: 0,
            total_length,
            end_of_data: false,
        };
        
        // Load the first chunk of data
        iterator.load_next_chunk().await?;
        
        Ok(iterator)
    }
    
    pub fn reset(&mut self) {
        self.position = 0;
        self.buffer_index = 0;
        self.buffer_valid_length = 0;
        self.end_of_data = false;
    }

    /// Load the next chunk of data into the buffer
    async fn load_next_chunk(&mut self) -> Result<()> {
        // Reset buffer index
        self.buffer_index = 0;

        // Check if we've reached the end of the storage
        if self.position >= self.total_length as u64 {
            self.end_of_data = true;
            return Ok(());
        }
        
        // Calculate how many bytes to read (may be less than CHUNK_SIZE at the end)
        let bytes_remaining = self.total_length as u64 - self.position;
        let bytes_to_read = std::cmp::min(bytes_remaining, CHUNK_SIZE as u64);
        
        // Clear the buffer and ensure capacity

        self.buffer = MEMORY_POOL.acquire(bytes_to_read as usize);
        
        // Read data into the buffer
        let mut read_position = self.position;
        self.io.read_data_into_buffer(&mut read_position, &mut self.buffer.into_slice_mut()).await;
        self.buffer_valid_length = self.buffer.into_slice().len();
        
        // Update position for next read
        self.position += bytes_to_read;
        
        Ok(())
    }
    
    /// Get the next RowPointer from the buffer, loading a new chunk if necessary
    pub async fn next_row_pointer(&mut self) -> Result<Option<RowPointer>> {
        // If we've reached the end of the data, return None
        if self.end_of_data {
            return Ok(None);
        }
        
        // If we've reached the end of the current buffer, load the next chunk
        if self.buffer_index >= self.buffer_valid_length {
            self.load_next_chunk().await?;
            
            // If loading the next chunk reached the end of data, return None
            if self.end_of_data {
                return Ok(None);
            }
        }
        
        let mut slice: [u8; TOTAL_LENGTH] = [0; TOTAL_LENGTH];

        // Parse the RowPointer from the buffer
        slice.copy_from_slice(&self.buffer.into_slice()[self.buffer_index..self.buffer_index + TOTAL_LENGTH]);
        let row_pointer = RowPointer::from_slice(&slice);
        
        // Advance the buffer index
        self.buffer_index += TOTAL_LENGTH;
        
        Ok(Some(row_pointer))
    }
    
    /// Get multiple RowPointers at once, up to BATCH_SIZE
    pub async fn next_row_pointers(&mut self) -> Result<Vec<RowPointer>> {
        let mut pointers = Vec::with_capacity(BATCH_SIZE);
        
        for _ in 0..BATCH_SIZE {
            match self.next_row_pointer().await? {
                Some(pointer) => pointers.push(pointer),
                None => break,
            }
        }
        
        Ok(pointers)
    }

    pub async fn next(&mut self) -> Result<Option<RowPointer>> {
        let result = self.next_row_pointer().await;
        result
    }

    pub async fn read_last(&mut self) -> Option<RowPointer> {
        let mut position = self.io.get_len().await;
        let mut slice: [u8; TOTAL_LENGTH] = [0; TOTAL_LENGTH];

        if position < TOTAL_LENGTH as u64 {
            return None;
        }

        position = position - TOTAL_LENGTH as u64;

        self.io.read_data_into_buffer(&mut position, &mut slice).await;

        let row_pointer = RowPointer::from_slice(&slice);

        Some(row_pointer)
    }
}

#[derive(Debug, Clone)]
pub struct RowPointer{
    #[cfg(feature = "enable_long_row")]
    pub id: u128,
    #[cfg(feature = "enable_long_row")]
    pub length: u64,

    #[cfg(not(feature = "enable_long_row"))]
    pub id: u64,
    #[cfg(not(feature = "enable_long_row"))]
    pub length: u32,

    pub position: u64,
    pub deleted: bool,

    #[cfg(feature = "enable_long_row")]
    pub checksum: u32,
    #[cfg(feature = "enable_long_row")]
    pub cluster: u32,
    #[cfg(feature = "enable_long_row")]
    pub deleted_at: u128,
    #[cfg(feature = "enable_long_row")]
    pub created_at: u128,
    #[cfg(feature = "enable_long_row")]
    pub updated_at: u128,
    #[cfg(feature = "enable_long_row")]
    pub version: u16,
    #[cfg(feature = "enable_long_row")]
    pub is_active: bool,

    pub writing_data: WritingData
}

#[derive(Debug, Clone)]
pub struct WritingData {
    #[cfg(feature = "enable_long_row")]
    pub total_columns_size: u64,

    #[cfg(not(feature = "enable_long_row"))]
    pub total_columns_size: u32,

    pub total_strings_size: u32
}

impl RowPointer {
    /// Gets the current timestamp in milliseconds since UNIX epoch
    pub fn get_timestamp() -> u128 {
        // Get the current timestamp in milliseconds
        let now = std::time::SystemTime::now();
        let duration = now.duration_since(std::time::UNIX_EPOCH).unwrap();
        let timestamp = duration.as_millis();
        timestamp
    }

    /// Gets the current timestamp in milliseconds since UNIX epoch
    pub fn get_timestamp_seconds() -> u64 {
        // Get the current timestamp in milliseconds
        let now = std::time::SystemTime::now();
        let duration = now.duration_since(std::time::UNIX_EPOCH).unwrap();
        let timestamp = duration.as_secs();
        timestamp
    }

    /// Updates the updated_at
    #[cfg(feature = "enable_long_row")]
    pub fn update_timestamp(&mut self) {
        // Update the created_at and updated_at timestamps
        let timestamp = Self::get_timestamp();
        self.updated_at = timestamp as u128;
    }

    /// Creates a new RowPointer with the given parameters
    pub fn new(
        //TODO Replace with AtomicU128 then stabilized
        #[cfg(feature = "enable_long_row")]
        last_id: &AtomicU64,

        #[cfg(not(feature = "enable_long_row"))]
        last_id: &AtomicU64,

        table_length: &AtomicU64,

        #[cfg(feature = "enable_long_row")]
        cluster: u32,

        row_write: &RowWrite
    ) -> Self {
        #[cfg(feature = "enable_long_row")]
        let id = last_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst) as u128;

        #[cfg(not(feature = "enable_long_row"))]
        let id = last_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        #[cfg(feature = "enable_long_row")]
        let (position, total_bytes, total_string_size) = {
            let mut total_bytes = row_write.columns_writing_data.iter().map(|col| col.size as u64).sum::<u64>();

            let total_string_size = row_write.columns_writing_data.iter()
                .filter(|col| col.column_type == DbType::STRING)
                .map(|col| col.data.into_slice().len() as u32)
                .sum::<u32>();

            total_bytes = total_bytes + total_string_size as u64;

            let position = table_length.fetch_add(total_bytes, std::sync::atomic::Ordering::SeqCst);

            (position, total_bytes, total_string_size)
        };

        #[cfg(not(feature = "enable_long_row"))]
        let (position, total_bytes, total_string_size) = {
            let mut total_bytes = row_write.columns_writing_data.iter().map(|col| col.size).sum::<u32>();

            let total_string_size = row_write.columns_writing_data.iter()
                .filter(|col| col.column_type == DbType::STRING)
                .map(|col| col.data.into_slice().len() as u32)
                .sum::<u32>();

            total_bytes = total_bytes + total_string_size;

            let position = table_length.fetch_add(total_bytes as u64, std::sync::atomic::Ordering::SeqCst);

            (position, total_bytes, total_string_size)
        };

        #[cfg(feature = "enable_long_row")]
        let timestamp = Self::get_timestamp();

        #[cfg(feature = "enable_long_row")]
        let checksum: u32 = {
            let all_data_size = row_write.columns_writing_data
                .iter()
                .map(|col| 
                    col.data.into_slice().len() as u64)
                .sum::<u64>();

            let block = MEMORY_POOL.acquire(all_data_size as usize);
            let slice = block.into_slice_mut();

            let mut position: usize = 0;

            for row_write in &row_write.columns_writing_data {
                let column_slice = row_write.data.into_slice();
                let size = column_slice.len();

                slice[position..position + size].copy_from_slice(column_slice);
                position += size;
            }

            CRC.checksum(slice)
        };

        RowPointer {
            id: id + 1,
            length: total_bytes,
            position,
            deleted: false,

            #[cfg(feature = "enable_long_row")]
            checksum,
            #[cfg(feature = "enable_long_row")]
            cluster,
            #[cfg(feature = "enable_long_row")]
            deleted_at: 0,
            #[cfg(feature = "enable_long_row")]
            created_at: timestamp,
            #[cfg(feature = "enable_long_row")]
            updated_at: 0,
            #[cfg(feature = "enable_long_row")]
            version: 0,
            #[cfg(feature = "enable_long_row")]
            is_active: true,

            writing_data: WritingData {
                total_columns_size: total_bytes,
                total_strings_size: total_string_size,
            }
        }
    }

    pub async fn save<S: StorageIO>(
        &self,
        io: &mut S,) -> Result<()> {
        
        let block = self.into_memory_block();
        let slice = block.into_slice();

        #[cfg(feature = "enable_data_verification")]
        let position = io.get_len().await;

        io.append_data(slice, true).await;

        #[cfg(feature = "enable_data_verification")]
        let verify_result = io.verify_data_and_sync(position, slice).await;

        #[cfg(feature = "enable_data_verification")]
        if !verify_result {
            // Rollback the transaction if verification fails
            // TODO
            return Err(super::error::RowError::SavingFailed("Failed to save data, rolling back the transaction.".into()));
        }

        Ok(())
    }

    /// Serializes the RowPointer into a Vec<u8>
    pub fn into_memory_block(&self) -> MemoryBlock {
        let block = MEMORY_POOL.acquire(TOTAL_LENGTH as usize);
        let slice = block.into_slice_mut();
 
        debug_assert!(slice.len() == TOTAL_LENGTH, "Buffer size mismatch");

        let mut cursor = Cursor::new(slice);
        cursor.set_position(0);

        // Write all fields in order
        #[cfg(feature = "enable_long_row")]
        cursor.write_u128::<LittleEndian>(self.id).unwrap();
        #[cfg(feature = "enable_long_row")]
        cursor.write_u64::<LittleEndian>(self.length).unwrap();

        #[cfg(not(feature = "enable_long_row"))]
        cursor.write_u64::<LittleEndian>(self.id).unwrap();

        #[cfg(not(feature = "enable_long_row"))]
        cursor.write_u32::<LittleEndian>(self.length).unwrap();

        cursor.write_u64::<LittleEndian>(self.position).unwrap();
        cursor.write_u8(self.deleted as u8).unwrap();

       #[cfg(feature = "enable_long_row")]
       {
            cursor.write_u32::<LittleEndian>(self.checksum).unwrap();
            cursor.write_u32::<LittleEndian>(self.cluster).unwrap();
            cursor.write_u128::<LittleEndian>(self.deleted_at).unwrap();
            cursor.write_u128::<LittleEndian>(self.created_at).unwrap();
            cursor.write_u128::<LittleEndian>(self.updated_at).unwrap();
            cursor.write_u16::<LittleEndian>(self.version).unwrap();
            cursor.write_u8(self.is_active as u8).unwrap();
       }
  
        block
    }
    
    /// Deserializes a slice of u8 into a RowPointer
    pub fn from_slice(buffer: &[u8; TOTAL_LENGTH]) -> Self {
        // ID field
        #[cfg(feature = "enable_long_row")]
        let id = unsafe { read_u128(buffer.as_ptr()) };

        #[cfg(not(feature = "enable_long_row"))]
        let id = unsafe { read_u64(buffer.as_ptr()) };

        // Length field
        #[cfg(feature = "enable_long_row")]
        let length = unsafe { read_u64(buffer.as_ptr().add(16)) };

        #[cfg(not(feature = "enable_long_row"))]
        let length = unsafe { read_u32(buffer.as_ptr().add(8)) };

        // Position field (u64 in both cases)
        #[cfg(feature = "enable_long_row")]
        let position = unsafe { read_u64(buffer.as_ptr().add(24))};

        #[cfg(not(feature = "enable_long_row"))]
        let position = unsafe { read_u64(buffer.as_ptr().add(12)) };

        // Deleted field (bool - 1 byte)
        #[cfg(feature = "enable_long_row")]
        let deleted = unsafe { read_u8(buffer.as_ptr().add(32)) } != 0;

        #[cfg(not(feature = "enable_long_row"))]
        let deleted = unsafe { read_u8(buffer.as_ptr().add(20)) } != 0;

        // The following fields only exist in the long row format
        #[cfg(feature = "enable_long_row")]
        let checksum = unsafe { read_u32(buffer.as_ptr().add(33)) };

        #[cfg(feature = "enable_long_row")]
        let cluster = unsafe { read_u32(buffer.as_ptr().add(37)) };

        #[cfg(feature = "enable_long_row")]
        let deleted_at = unsafe { read_u128(buffer.as_ptr().add(41)) };

        #[cfg(feature = "enable_long_row")]
        let created_at = unsafe { read_u128(buffer.as_ptr().add(57)) };

        #[cfg(feature = "enable_long_row")]
        let updated_at = unsafe { read_u128(buffer.as_ptr().add(73)) };

        #[cfg(feature = "enable_long_row")]
        let version = unsafe { read_u16(buffer.as_ptr().add(89)) };

        #[cfg(feature = "enable_long_row")]
        let is_active = unsafe { read_u8(buffer.as_ptr().add(91)) } != 0 ;

        // Create the RowPointer struct from the read values
        let row_pointer = RowPointer {
            id,
            length,
            position,
            deleted,
            #[cfg(feature = "enable_long_row")]
            checksum,
            #[cfg(feature = "enable_long_row")]
            cluster,
            #[cfg(feature = "enable_long_row")]
            deleted_at,
            #[cfg(feature = "enable_long_row")]
            created_at,
            #[cfg(feature = "enable_long_row")]
            updated_at,
            #[cfg(feature = "enable_long_row")]
            version,
            #[cfg(feature = "enable_long_row")]
            is_active,
            writing_data: WritingData {
                total_columns_size: 0, // Placeholder, will be set later
                total_strings_size: 0, // Placeholder, will be set later
            }
        };

        row_pointer
    }
    
    /// Reads a RowPointer from the given StorageIO at the specified position
    pub async fn read_from_io<S: StorageIO>(io: &mut S, position: &mut u64) -> Result<Self> {
        let mut slice: [u8; TOTAL_LENGTH] = [0; TOTAL_LENGTH];

        // Read all data in one operation
        io.read_data_into_buffer(position, &mut slice).await;

        let row_pointer = Self::from_slice(&slice);

        Ok(row_pointer)
    }

    /// Fetches specific columns of a row based on the RowFetch specification
    /// 
    /// This method reads only the columns specified in the RowFetch rather than the entire row,
    /// which can be more efficient for queries that only need specific columns.
    /// 
    /// # Arguments
    /// 
    /// * `io` - The storage I/O provider to read from
    /// * `row_fetch` - Specification of which columns to fetch and their positions/sizes
    /// 
    /// # Returns
    /// 
    /// A Result containing the constructed Row with only the requested columns
    pub async fn fetch_row<S: StorageIO>(&self, io: &S, row_fetch: &RowFetch) -> Result<Row> {
        // Skip fetching if the row is marked as deleted
        if self.deleted {
            return Err(super::error::RowError::NotFound("Row is marked as deleted".into()));
        }
        
        // Create a vector to hold all the columns
        let mut columns = Vec::with_capacity(row_fetch.columns_fetching_data.len());
        
        // For each column specified in the fetch data
        for column_data in &row_fetch.columns_fetching_data {
            let mut position = self.position + column_data.column_offset as u64;

            if column_data.column_type == DbType::STRING {
                // For strings, we need to read the size first
                let mut string_position_buffer = [0u8; 8];

                io.read_data_into_buffer(&mut position, &mut string_position_buffer).await;

                // Turn [u8] into u64
                let mut string_row_position = unsafe { simds::endianess::read_u64(string_position_buffer.as_ptr()) };

                let mut string_size_buffer = [0u8; 4];

                io.read_data_into_buffer(&mut position, &mut string_size_buffer).await;
                let string_size = unsafe { simds::endianess::read_u32(string_size_buffer.as_ptr()) };

                // Read the string data
                let string_block = MEMORY_POOL.acquire(string_size as usize);
                let string_slice = string_block.into_slice_mut();

                io.read_data_into_buffer(&mut string_row_position, string_slice).await;

                // Create a column with the string data
                let column = Column {
                    schema_id: 0,
                    data: string_block,
                    column_type: DbType::STRING,
                };

                columns.push(column);
            } else {
                let c_size = column_data.size;

                // Allocate memory for the column data
                let block = MEMORY_POOL.acquire(c_size as usize);
                let slice = block.into_slice_mut();
                
                // Read the column data directly into our buffer
                io.read_data_into_buffer(&mut position, slice).await;

                // Create a Column object with the read data
                let column = Column {
                    schema_id: 0, // Schema ID would be set based on metadata or query context
                    data: block,
                    column_type: column_data.column_type.clone(), // Clone the DbType
                };
                
                columns.push(column);
            }
        }
        
        // Create and return the Row with just the fetched columns
        Ok(Row {
            position: self.position,
            columns,
            length: self.length
        })
    }

    pub async fn fetch_row_async<S: StorageIO>(&self, io: Arc<S>, row_fetch: Arc<RowFetch>) -> Result<Row> {
        // Skip fetching if the row is marked as deleted
        if self.deleted {
            return Err(super::error::RowError::NotFound("Row is marked as deleted".into()));
        }
        
        // Create a vector to hold all the columns
        let mut columns = Vec::with_capacity(row_fetch.columns_fetching_data.len());
        
        // For each column specified in the fetch data
        for column_data in &row_fetch.columns_fetching_data {
            let mut position = self.position + column_data.column_offset as u64;

            if column_data.column_type == DbType::STRING {
                // For strings, we need to read the size first
                let mut string_position_buffer = [0u8; 8];

                io.read_data_into_buffer(&mut position, &mut string_position_buffer).await;

                // Turn [u8] into u64
                let mut string_row_position = unsafe { simds::endianess::read_u64(string_position_buffer.as_ptr()) };

                let mut string_size_buffer = [0u8; 4];

                io.read_data_into_buffer(&mut position, &mut string_size_buffer).await;
                let string_size = unsafe { simds::endianess::read_u32(string_size_buffer.as_ptr()) };

                // Read the string data
                let string_block = MEMORY_POOL.acquire(string_size as usize);
                let string_slice = string_block.into_slice_mut();

                io.read_data_into_buffer(&mut string_row_position, string_slice).await;

                // Create a column with the string data
                let column = Column {
                    schema_id: 0,
                    data: string_block,
                    column_type: DbType::STRING,
                };

                columns.push(column);
            } else {
                let c_size = column_data.size;

                // Allocate memory for the column data
                let block = MEMORY_POOL.acquire(c_size as usize);
                let slice = block.into_slice_mut();
                
                // Read the column data directly into our buffer
                io.read_data_into_buffer(&mut position, slice).await;

                // Create a Column object with the read data
                let column = Column {
                    schema_id: 0, // Schema ID would be set based on metadata or query context
                    data: block,
                    column_type: column_data.column_type.clone(), // Clone the DbType
                };
                
                columns.push(column);
            }
        }
        
        // Create and return the Row with just the fetched columns
        Ok(Row {
            position: self.position,
            columns,
            length: self.length
        })
    }

    pub async fn fetch_row_reuse_async<S: StorageIO>(
        &self, 
        io: &&S, 
        row_fetch: &RowFetch,
        row_reuse: &mut Row) {
        // Skip fetching if the row is marked as deleted
        if self.deleted {
            panic!();
        }
        
        // Create a vector to hold all the columns
        let columns = &mut row_reuse.columns;
        columns.clear();

        let mut schema_id = 0;

        // For each column specified in the fetch data
        for column_data in &row_fetch.columns_fetching_data {
            let mut position = self.position + column_data.column_offset as u64;

            if column_data.column_type == DbType::STRING {
                // For strings, we need to read the size first
                let mut string_position_buffer = [0u8; 8];

                io.read_data_into_buffer(&mut position, &mut string_position_buffer).await;

                // Turn [u8] into u64
                let mut string_row_position = unsafe { simds::endianess::read_u64(string_position_buffer.as_ptr()) };

                let mut string_size_buffer = [0u8; 4];

                io.read_data_into_buffer(&mut position, &mut string_size_buffer).await;
                let string_size = unsafe { simds::endianess::read_u32(string_size_buffer.as_ptr()) };

                // Read the string data
                let string_block = MEMORY_POOL.acquire(string_size as usize);
                let string_slice = string_block.into_slice_mut();

                io.read_data_into_buffer(&mut string_row_position, string_slice).await;

                // Create a column with the string data
                let column = Column {
                    schema_id: schema_id,
                    data: string_block,
                    column_type: DbType::STRING,
                };

                schema_id += 1;

                columns.push(column);
            } else {
                let c_size = column_data.size;

                // Allocate memory for the column data
                let block = MEMORY_POOL.acquire(c_size as usize);
                let slice = block.into_slice_mut();
                
                // Read the column data directly into our buffer
                io.read_data_into_buffer(&mut position, slice).await;

                // Create a Column object with the read data
                let column = Column {
                    schema_id: schema_id, // Schema ID would be set based on metadata or query context
                    data: block,
                    column_type: column_data.column_type.clone(), // Clone the DbType
                };

                schema_id += 1;

                columns.push(column);
            }
        }
        
        row_reuse.position = self.position;
        row_reuse.length = self.length;
    }

    /// Writes a row to storage using the given RowWrite payload
    ///
    /// This function writes row data to the rows_io storage and creates a row pointer in pointers_io.
    ///
    /// # Arguments
    ///
    /// * `pointers_io` - The storage IO for row pointers
    /// * `rows_io` - The storage IO for row data
    /// * `row_write` - The row write payload containing column data to write
    ///
    /// # Returns
    ///
    /// A Result containing the created RowPointer on success
    pub async fn write_row<S: StorageIO>(
        pointers_io: &mut S,
        rows_io: &mut S,

        #[cfg(feature = "enable_long_row")]
        last_id: &AtomicU64,
        
        #[cfg(not(feature = "enable_long_row"))]
        last_id: &AtomicU64,

        table_length: &AtomicU64,

        #[cfg(feature = "enable_long_row")]
        cluster: u32,

        row_write: &RowWrite
    ) -> Result<RowPointer> {
        
        let row_pointer = RowPointer::new(
            last_id,
            table_length,
            #[cfg(feature = "enable_long_row")]
            cluster,
            row_write
        );

        #[cfg(feature = "enable_data_verification")]
        let io_position = row_pointer.position;

        let total_bytes = row_pointer.writing_data.total_columns_size;
        let total_string_size = row_pointer.writing_data.total_strings_size;

        // Count number of string columns
        // let string_columns_count = row_write.columns_writing_data.iter().filter(|col| col.column_type == DbType::STRING).count();
        // total_bytes = total_bytes + string_columns_count as u64 * (4 + 8); // 4 bytes for size and 8 bytes for pointer

        // Allocate memory to store the row data
        let block = MEMORY_POOL.acquire(total_bytes as usize);
        let slice = block.into_slice_mut();

        let mut row_position = 0;

        let string_block = MEMORY_POOL.acquire(total_string_size as usize);
        let string_slice = string_block.into_slice_mut();

        let mut end_of_row: u64 = row_write.columns_writing_data.iter().map(|col| col.size as u64).sum();

        // Write each column data into the buffer
        
        let mut string_data_position: u64 = 0;

        for column in row_write.columns_writing_data.iter().sorted_by(|a, b| a.write_order.cmp(&b.write_order)) {
            let end_of_column = row_position + column.size as u64;

            // If it's string a pointer of u64 must be created which will point to
            // the ending of the row, were the strings are saved.
            // If it's a known size, integers, floats, etc. it's placed into the buffer.
            // Example:
            //              U64 POINTER | U32 SIZE             U64 POINTER | U32 SIZE     BINARY DATA | BINARY DATA
            //    4 bytes | 8 bytes | 4 bytes | 4 bytes | 16 bytes |  8 bytes  |  4 bytes  |   X bytes   |   X bytes  
            // ROW: [I32|STRING_POINTER_1|STRING_SIZE|I32|U128|STRING_POINTER_2|STRING_SIZE|STRING_DATA_1|STRING_DATA_2]
            // 
            if column.column_type == DbType::STRING {
                let string_column_slice = column.data.into_slice();

                let string_size = string_column_slice.len() as u32;

                string_slice[string_data_position as usize..string_data_position as usize + string_size as usize].copy_from_slice(string_column_slice);

                string_data_position = string_data_position + string_size as u64;

                let end_of_row_bytes = end_of_row.to_le_bytes();

                // Write STRING_POINTER
                slice[row_position as usize..row_position as usize + 8].copy_from_slice(&end_of_row_bytes);
             
                // Write STRING_SIZE
                slice[row_position as usize + 8..row_position as usize + 4 + 8].copy_from_slice(&string_size.to_le_bytes());

                end_of_row = end_of_row + string_column_slice.len() as u64;

                row_position = end_of_column;
            } else {
                let column_slice = column.data.into_slice();

                slice[row_position as usize..end_of_column as usize].copy_from_slice(column_slice);

                row_position = end_of_column;
            }
        }

        slice[row_position as usize..].copy_from_slice(&string_slice);

        // Write the row data to the rows_io
        rows_io.append_data(slice, true).await;

        // Write the pointer last
        // Write the row data to the pointers_io
        row_pointer.save(pointers_io).await?;

        #[cfg(feature = "enable_data_verification")]
        let write_result = rows_io.verify_data_and_sync(io_position, slice).await;

        #[cfg(feature = "enable_data_verification")]
        if !write_result {
            // Rollback the transaction if verification fails
            // TODO
            return Err(super::error::RowError::SavingFailed("Failed to save data, rolling back the transaction.".into()));
        }

        Ok(row_pointer)
    }
}

#[cfg(test)]
mod tests {
    use crate::core::{row_v2::row::{ColumnFetchingData, ColumnWritePayload}, storage_providers::mock_file_sync::MockStorageProvider};
    use super::*;

    fn create_string_column(data: &str, write_order: u32) -> ColumnWritePayload {
        let string_bytes = data.as_bytes();

        let string_data = MEMORY_POOL.acquire(string_bytes.len());
        let string_slice = string_data.into_slice_mut();

        string_slice[0..].copy_from_slice(string_bytes);

        ColumnWritePayload {
            data: string_data,
            write_order,
            column_type: DbType::STRING,
            size: 4 + 8
        }
    }

    fn create_u8_column(data: u8, write_order: u32) -> ColumnWritePayload {
        let u8_data = MEMORY_POOL.acquire(1);
        let u8_slice = u8_data.into_slice_mut();
        u8_slice.copy_from_slice(&data.to_le_bytes());

        ColumnWritePayload {
            data: u8_data,
            write_order,
            column_type: DbType::U8,
            size: 1
        }
    }

    fn create_u64_column(data: u64, write_order: u32) -> ColumnWritePayload {
        let u64_data = MEMORY_POOL.acquire(8);
        let u64_slice = u64_data.into_slice_mut();
        u64_slice.copy_from_slice(&data.to_le_bytes());

        ColumnWritePayload {
            data: u64_data,
            write_order,
            column_type: DbType::U64,
            size: 8
        }
    }

    fn create_f32_column(data: f32, write_order: u32) -> ColumnWritePayload {
        let f32_data = MEMORY_POOL.acquire(4);
        let f32_slice = f32_data.into_slice_mut();
        f32_slice.copy_from_slice(&data.to_le_bytes());

        ColumnWritePayload {
            data: f32_data,
            write_order,
            column_type: DbType::F32,
            size: 4
        }
    }

    fn create_f64_column(data: f64, write_order: u32) -> ColumnWritePayload {
        let f64_data = MEMORY_POOL.acquire(8);
        let f64_slice = f64_data.into_slice_mut();
        f64_slice.copy_from_slice(&data.to_le_bytes());

        ColumnWritePayload {
            data: f64_data,
            write_order,
            column_type: DbType::F64,
            size: 8
        }
    }

    fn create_row_write(id: u64) -> RowWrite {
        RowWrite {
            columns_writing_data: vec![
                create_u64_column(id, 0),
                create_string_column("John Doe", 1),
                create_u8_column(30, 2),
                create_string_column("john.doe@example.com", 3),
                create_string_column("123 Main St", 4),
                create_string_column("555-1234", 5),
                create_u8_column(1, 6),
                create_f64_column(1000.50, 7),
                create_u8_column(0, 8),
                create_string_column("1990-01-01", 9),
                create_string_column("2023-10-01", 10),
                create_string_column("Sunsung Phone Andomega 10", 11),
                create_f32_column(100.0, 12),
                create_string_column("2023-10-01", 13),
                create_string_column("New York", 14),
                create_string_column("Credit Card", 15),
                create_string_column("Electronics", 16),
                create_string_column("Smartphones", 17),
                create_string_column("Latest model", 18),
                create_string_column("No notes", 19),
            ],
        }
    }



    // Helper function to create a row pointer and return its serialized form
    fn create_test_row_pointer(id: u64, position: u64, length: u32, deleted: bool) -> RowPointer {
        #[cfg(feature = "enable_long_row")]
        {
            RowPointer {
                id: id as u128,
                length: length as u64,
                position,
                deleted,
                checksum: 0,
                cluster: 0,
                deleted_at: 0,
                created_at: 0,
                updated_at: 0,
                version: 0,
                is_active: true,
                writing_data: WritingData {
                    total_columns_size: 0,
                    total_strings_size: 0,
                },
            }
        }
        
        #[cfg(not(feature = "enable_long_row"))]
        {
            RowPointer {
                id,
                length,
                position,
                deleted,
                writing_data: WritingData {
                    total_columns_size: 0,
                    total_strings_size: 0,
                },
            }
        }
    }
    
    #[tokio::test]
    async fn test_row_pointer_iterator_empty() {
        let mut mock_io = MockStorageProvider::new().await;
        
        let mut iterator = RowPointerIterator::new(&mut mock_io).await.unwrap();
        
        // Should return None for an empty storage
        assert!(iterator.next_row_pointer().await.unwrap().is_none());
    }
    
    #[tokio::test]
    async fn test_row_pointer_iterator_single_pointer() {
        // Create a single row pointer
        let row_pointer = create_test_row_pointer(1, 100, 50, false);

        let mut mock_io = MockStorageProvider::new().await;
        
        row_pointer.save(&mut mock_io).await.unwrap();

        let mut row_pointer_iterator = RowPointerIterator::new(&mut mock_io).await.unwrap();
        
        // Should return our row pointer
        let read_pointer = row_pointer_iterator.next_row_pointer().await.unwrap().unwrap();
        
        assert_eq!(read_pointer.id, 1);
        assert_eq!(read_pointer.position, 100);
        assert_eq!(read_pointer.length, 50);
        assert_eq!(read_pointer.deleted, false);
        

        assert!(row_pointer_iterator.next_row_pointer().await.unwrap().is_none());
    }
    
    #[tokio::test]
    async fn test_row_pointer_iterator_2_pointers() {
        // Create a single row pointer
        let row_pointer = create_test_row_pointer(1, 100, 50, false);

        let mut mock_io = MockStorageProvider::new().await;
        
        row_pointer.save(&mut mock_io).await.unwrap();
        row_pointer.save(&mut mock_io).await.unwrap();

        let mut row_pointer_iterator = RowPointerIterator::new(&mut mock_io).await.unwrap();
        
        // Should return our row pointer
        let read_pointer = row_pointer_iterator.next_row_pointer().await.unwrap().unwrap();
        
        assert_eq!(read_pointer.id, 1);
        assert_eq!(read_pointer.position, 100);
        assert_eq!(read_pointer.length, 50);
        assert_eq!(read_pointer.deleted, false);
        
        // Next call should return None
        assert!(row_pointer_iterator.next_row_pointer().await.unwrap().is_some());
        assert!(row_pointer_iterator.next_row_pointer().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_row_pointer_iterator_3_pointers() {
        // Create a single row pointer
        let row_pointer = create_test_row_pointer(1, 100, 50, false);
        let row_pointer2 = create_test_row_pointer(2, 150, 100, false);

        let mut mock_io = MockStorageProvider::new().await;
        
        row_pointer.save(&mut mock_io).await.unwrap();
        row_pointer2.save(&mut mock_io).await.unwrap();
        row_pointer.save(&mut mock_io).await.unwrap();

        let mut row_pointer_iterator = RowPointerIterator::new(&mut mock_io).await.unwrap();
        
        // Should return our row pointer
        let read_pointer = row_pointer_iterator.next_row_pointer().await.unwrap().unwrap();
        
        assert_eq!(read_pointer.id, 1);
        assert_eq!(read_pointer.position, 100);
        assert_eq!(read_pointer.length, 50);
        assert_eq!(read_pointer.deleted, false);
        
        let read_pointer = row_pointer_iterator.next_row_pointer().await.unwrap().unwrap();
        
        assert_eq!(read_pointer.id, 2);
        assert_eq!(read_pointer.position, 150);
        assert_eq!(read_pointer.length, 100);
        assert_eq!(read_pointer.deleted, false);

        // Next call should return None
        assert!(row_pointer_iterator.next_row_pointer().await.unwrap().is_some());
        assert!(row_pointer_iterator.next_row_pointer().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_save_row_pointers() {
     let mut mock_io_pointers = MockStorageProvider::new().await;
        let mut mock_io_rows = MockStorageProvider::new().await;

        #[cfg(feature = "enable_long_row")]
        let cluster = 0;

        let last_id = AtomicU64::new(0);
        let table_length = AtomicU64::new(0);

        let string_bytes = b"Hello, world!";

        let string_data = MEMORY_POOL.acquire(string_bytes.len());
        let string_slice = string_data.into_slice_mut();

        string_slice[0..].copy_from_slice(string_bytes);

        let i32_bytes = 42_i32.to_le_bytes();
        let i32_data = MEMORY_POOL.acquire(i32_bytes.len());
        let i32_slice = i32_data.into_slice_mut();
        i32_slice.copy_from_slice(&i32_bytes);

        let row_write = RowWrite {
            columns_writing_data: vec![
                ColumnWritePayload {
                    data: string_data,
                    write_order: 0,
                    column_type: DbType::STRING,
                    size: 4 + 8 // String Size + String Pointer
                },
                ColumnWritePayload {
                    data: i32_data,
                    write_order: 1,
                    column_type: DbType::I32,
                    size: i32_bytes.len() as u32
                },
            ],
        };

        let result = RowPointer::write_row(
            &mut mock_io_pointers, 
            &mut mock_io_rows, 
            &last_id, 
            &table_length, 
            #[cfg(feature = "enable_long_row")]
            cluster, 
            &row_write
        ).await;

        assert!(result.is_ok())
    }

    #[tokio::test]
    async fn test_save_and_read_row_pointers() {
        let mut mock_io_pointers = MockStorageProvider::new().await;
        let mut mock_io_rows = MockStorageProvider::new().await;

        #[cfg(feature = "enable_long_row")]
        let cluster = 0;

        let last_id = AtomicU64::new(0);
        let table_length = AtomicU64::new(0);

        let string_bytes = b"Hello, world!";

        let string_data = MEMORY_POOL.acquire(string_bytes.len());
        let string_slice = string_data.into_slice_mut();
        string_slice[0..].copy_from_slice(string_bytes);

        let i32_bytes = 42_i32.to_le_bytes();
        let i32_data = MEMORY_POOL.acquire(i32_bytes.len());
        let i32_slice = i32_data.into_slice_mut();
        i32_slice.copy_from_slice(&i32_bytes);

        let row_write = RowWrite {
            columns_writing_data: vec![
                ColumnWritePayload {
                    data: string_data,
                    write_order: 0,
                    column_type: DbType::STRING,
                    size: 4 + 8 // String Size + String Pointer
                },
                ColumnWritePayload {
                    data: i32_data,
                    write_order: 1,
                    column_type: DbType::I32,
                    size: i32_bytes.len() as u32
                },
            ],
        };

        let _result = RowPointer::write_row(
            &mut mock_io_pointers, 
            &mut mock_io_rows, 
            &last_id, 
            &table_length, 
            #[cfg(feature = "enable_long_row")]
            cluster, 
            &row_write
        ).await;

        let result = RowPointer::write_row(
            &mut mock_io_pointers, 
            &mut mock_io_rows, 
            &last_id, 
            &table_length, 
            #[cfg(feature = "enable_long_row")]
            cluster, 
            &row_write
        ).await;

        assert!(result.is_ok());

        let mut iterator = RowPointerIterator::new(&mut mock_io_pointers).await.unwrap();

        let next_pointer = iterator.next_row_pointer().await.unwrap().unwrap();

        let row_fetch = RowFetch {
            columns_fetching_data: vec![
                ColumnFetchingData {
                    column_offset: 0,
                    column_type: DbType::STRING,
                    size: string_bytes.len() as u32 + 4
                },
                ColumnFetchingData {
                    column_offset: 8 + 4,
                    column_type: DbType::I32,
                    size: i32_bytes.len() as u32
                },
            ],
        };

        let row = next_pointer.fetch_row(
            &mock_io_rows, 
            &row_fetch
        ).await.unwrap();

        row.columns.iter().for_each(|column| {
            match column.column_type {
                DbType::STRING => {
                    let string_slice = column.data.into_slice();
                    assert_eq!(&string_slice[0..], string_bytes);
                },
                DbType::I32 => {
                    let i32_slice = column.data.into_slice();
                    assert_eq!(i32_slice, i32_bytes);
                },
                _ => {}
            }
        });
    }

    #[tokio::test]
    async fn test_save_and_read_row_pointers_async() {
        let mut mock_io_pointers = MockStorageProvider::new().await;
        let mut mock_io_rows = MockStorageProvider::new().await;

        #[cfg(feature = "enable_long_row")]
        let cluster = 0;

        let last_id = AtomicU64::new(0);
        let table_length = AtomicU64::new(0);

        let _result = RowPointer::write_row(
            &mut mock_io_pointers, 
            &mut mock_io_rows, 
            &last_id, 
            &table_length, 
            #[cfg(feature = "enable_long_row")]
            cluster, 
            &create_row_write(0)
        ).await;

        let result = RowPointer::write_row(
            &mut mock_io_pointers, 
            &mut mock_io_rows, 
            &last_id, 
            &table_length, 
            #[cfg(feature = "enable_long_row")]
            cluster, 
            &create_row_write(1)
        ).await;
        assert!(result.is_ok());

        let mut iterator = RowPointerIterator::new(&mut mock_io_pointers).await.unwrap();

        let next_pointer = iterator.next_row_pointer().await.unwrap().unwrap();

        let row_fetch = RowFetch {
            columns_fetching_data: vec![
                ColumnFetchingData {
                    column_offset: 0,
                    column_type: DbType::U64,
                    size: 8
                },
                ColumnFetchingData {
                    column_offset: 8,
                    column_type: DbType::STRING,
                    size: 4 + 8
                },
                ColumnFetchingData {
                    column_offset: 8 + 4 + 8,
                    column_type: DbType::U8,
                    size: 1
                }
            ],
        };

        let row = next_pointer.fetch_row(
            &mock_io_rows, 
            &row_fetch
        ).await.unwrap();

        row.columns.iter().for_each(|column| {
            match column.column_type {
                DbType::U8 => {
                    let u8_slice = column.data.into_slice();
                    assert_eq!(u8_slice, &[30]);
                },
                DbType::STRING => {
                    let string_slice = column.data.into_slice();
                    assert_eq!(&string_slice[0..], b"John Doe");
                },
                DbType::U64 => {
                    let u64_bytes = 0_u64.to_le_bytes();
                    let u64_slice = column.data.into_slice();
                    assert_eq!(u64_slice, u64_bytes);
                },
                _ => {}
            }
        });

    }
}