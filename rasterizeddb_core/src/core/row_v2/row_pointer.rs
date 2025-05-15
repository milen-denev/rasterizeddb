use std::{io::Cursor, sync::atomic::AtomicU64};

use super::{error::Result, row::RowWrite};
use byteorder::{LittleEndian, WriteBytesExt};
use crc::{Crc, CRC_32_ISO_HDLC};
use itertools::Itertools;

use crate::{
    core::{
        db_type::DbType, row_v2::row::{Column, Row, RowFetch}, storage_providers::traits::StorageIO
    }, 
    memory_pool::{MemoryBlock, MEMORY_POOL}, simds
};

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
const CHUNK_SIZE: usize = 5 * 695 * 92; // ~320KB chunks

#[cfg(feature = "enable_long_row")]
#[cfg(debug_assertions)]
const CHUNK_SIZE: usize = 92 * 2; // 92 * 2 bytes chunks for debugging

#[cfg(not(feature = "enable_long_row"))]
const CHUNK_SIZE: usize = 3047 * 21; // ~64KB chunks

// TODO replace with fastcrc32
const CRC: Crc::<u32>  = Crc::<u32>::new(&CRC_32_ISO_HDLC);

/// Iterates over RowPointers from a StorageIO in 64KB chunks
pub struct RowPointerIterator<'a, S: StorageIO> {
    /// Reference to the storage provider
    io: &'a mut S,
    /// Current position in the storage
    position: u64,
    /// Buffer to store 64KB chunks
    buffer: Vec<u8>,
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

        let buffer = vec![0u8; buffer_size];

        let mut iterator = RowPointerIterator {
            io,
            position: 0,
            buffer,
            buffer_index: 0,
            buffer_valid_length: 0,
            total_length,
            end_of_data: false,
        };
        
        // Load the first chunk of data
        iterator.load_next_chunk().await?;
        
        Ok(iterator)
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
        self.buffer.clear();
        self.buffer.resize(bytes_to_read as usize, 0);
        
        // Read data into the buffer
        let mut read_position = self.position;
        self.io.read_data_into_buffer(&mut read_position, &mut self.buffer).await;
        self.buffer_valid_length = self.buffer.len();
        
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
        
        // Check if we have enough data left in the buffer for a complete RowPointer
        if self.buffer_index + TOTAL_LENGTH > self.buffer_valid_length {
            // We need to handle the case where a RowPointer spans two chunks
            let remaining_data = self.buffer[self.buffer_index..].to_vec();
            let remaining_length = remaining_data.len();
            
            // Reset buffer with the remaining data
            self.buffer.clear();
            self.buffer.extend_from_slice(&remaining_data);
            
            // Update buffer index and valid length
            self.buffer_index = 0;
            self.buffer_valid_length = remaining_length;
            
            // Calculate how many additional bytes to read
            let additional_bytes_needed = TOTAL_LENGTH - remaining_length;
            
            // Check if there's enough data left in the storage
            let bytes_remaining_in_storage = self.total_length as u64 - (self.position - remaining_length as u64);
            
            if bytes_remaining_in_storage < additional_bytes_needed as u64 {
                self.end_of_data = true;
                return Ok(None);
            }
            
            // Read the additional bytes
            let mut additional_data = vec![0u8; additional_bytes_needed];
            let mut read_position = self.position - remaining_length as u64;
            self.io.read_data_into_buffer(&mut read_position, &mut additional_data).await;
            
            // Append the additional data to the buffer
            self.buffer.extend_from_slice(&additional_data);
            self.buffer_valid_length = self.buffer.len();
            
            // Update position for next read
            self.position = read_position + additional_bytes_needed as u64;
            
            // If we still don't have enough data for a complete RowPointer, return None
            if self.buffer_valid_length < TOTAL_LENGTH {
                self.end_of_data = true;
                return Ok(None);
            }
        }
        
        // Parse the RowPointer from the buffer
        let slice = &self.buffer[self.buffer_index..self.buffer_index + TOTAL_LENGTH];
        let row_pointer = RowPointer::from_slice(slice)?;
        
        // Advance the buffer index
        self.buffer_index += TOTAL_LENGTH;
        
        Ok(Some(row_pointer))
    }
}

// Implement the Iterator trait for RowPointerIterator for synchronous usage
impl<'a, S: StorageIO> Iterator for RowPointerIterator<'a, S> {
    type Item = Result<RowPointer>;
    
    fn next(&mut self) -> Option<Self::Item> {
        // Since the actual implementation is async, this provides a blocking wrapper
        // This is not ideal for production use but shows how the iterator would work
        // In a real implementation, we'd use a proper async iterator pattern
        
        // Create a future and block on it
        let future = self.next_row_pointer();
        
       
        use tokio::runtime::Handle;
        if let Ok(handle) = Handle::try_current() {
            return handle.block_on(future).transpose();
        } else {
            // If not in an async context, we can't block on the future
            return None;
        }
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
    pub is_active: bool
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
        let total_len = row_write.columns_writing_data.iter().map(|col| col.size as u64).sum::<u64>();

        #[cfg(not(feature = "enable_long_row"))]
        let total_len = row_write.columns_writing_data.iter().map(|col| col.size).sum::<u32>() as u32;

        let position = table_length.fetch_add(total_len as u64, std::sync::atomic::Ordering::SeqCst);

        #[cfg(feature = "enable_long_row")]
        let timestamp = Self::get_timestamp();

        #[cfg(feature = "enable_long_row")]
        let checksum: u32 = {
            let all_data_size = row_write.columns_writing_data
                .iter()
                .map(|col| 
                    unsafe { 
                        col.data.into_wrapper().as_slice().len() 
                    } as u64)
                .sum::<u64>();

            let block = MEMORY_POOL.acquire(all_data_size as usize);
            let mut wrapper = unsafe { block.into_wrapper() };
            let vec = wrapper.as_vec_mut();

            let mut position: usize = 0;

            for row_write in &row_write.columns_writing_data {
                let column_data_wrapper = unsafe { row_write.data.into_wrapper() };
                let column_vec = column_data_wrapper.as_slice();
                let size = column_vec.len();

                vec[position..position + size].copy_from_slice(column_vec);
                position += size;
            }

            CRC.checksum(&vec)
        };

        RowPointer {
            id: id + 1,
            length: total_len,
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
            is_active: true
        }
    }

    pub async fn save<S: StorageIO>(
        &self,
        io: &mut S,) -> Result<()> {
        
        let block = self.into_memory_block();
        let wrapper = unsafe { block.into_wrapper() };

        let position = io.get_len().await;

        io.append_data(wrapper.as_slice()).await;
        let verify_result = io.verify_data_and_sync(position, wrapper.as_slice()).await;

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
        let mut wrapper = unsafe { block.into_wrapper() };
        let buffer = wrapper.as_vec_mut();
 
        debug_assert!(buffer.len() == TOTAL_LENGTH, "Buffer size mismatch");

        let mut cursor = Cursor::new(buffer);
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
    
    /// Writes the RowPointer to the given StorageIO at the specified position
    pub async fn write_to_io<S: StorageIO>(&self, io: &mut S) -> Result<()> {
        let block = self.into_memory_block();
        let wrapper = unsafe { block.into_wrapper() };

        let position = io.get_len().await;

        // Write data to storage
        io.append_data(wrapper.as_slice()).await;
        
        // Verify written data
        io.verify_data_and_sync(position, wrapper.as_slice()).await;
        
        Ok(())
    }
    
    /// Deserializes a slice of u8 into a RowPointer
    pub fn from_slice(buffer: &[u8]) -> Result<Self> {
        use byteorder::{LittleEndian, ReadBytesExt};
        use std::io::Cursor;
        
        let mut cursor = Cursor::new(buffer);

        #[cfg(feature = "enable_long_row")]
        let id = cursor.read_u128::<LittleEndian>()?;
        #[cfg(feature = "enable_long_row")]
        let length = cursor.read_u64::<LittleEndian>()?;

        #[cfg(not(feature = "enable_long_row"))]
        let id = cursor.read_u64::<LittleEndian>()?;
        #[cfg(not(feature = "enable_long_row"))]
        let length = cursor.read_u32::<LittleEndian>().unwrap();

        let position = cursor.read_u64::<LittleEndian>()?;
        let deleted = cursor.read_u8()? != 0;


        #[cfg(feature = "enable_long_row")]
        let checksum = cursor.read_u32::<LittleEndian>()?;

        #[cfg(feature = "enable_long_row")]
        let cluster = cursor.read_u32::<LittleEndian>()?;

        #[cfg(feature = "enable_long_row")]
        let deleted_at = cursor.read_u128::<LittleEndian>()?;

        #[cfg(feature = "enable_long_row")]
        let created_at = cursor.read_u128::<LittleEndian>()?;

        #[cfg(feature = "enable_long_row")]
        let updated_at = cursor.read_u128::<LittleEndian>()?;

        #[cfg(feature = "enable_long_row")]
        let version = cursor.read_u16::<LittleEndian>()?;

        #[cfg(feature = "enable_long_row")]
        let is_active = cursor.read_u8()? != 0;

        Ok(RowPointer {
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
            is_active
        })
    }
    
    /// Reads a RowPointer from the given StorageIO at the specified position
    pub async fn read_from_io<S: StorageIO>(io: &mut S, position: &mut u64) -> Result<Self> {

        let block = MEMORY_POOL.acquire(TOTAL_LENGTH as usize);
        let mut wrapper = unsafe { block.into_wrapper() };
        let mut vec = wrapper.as_vec_mut();
        
        // Read all data in one operation
        io.read_data_into_buffer(position, &mut vec).await;

        let row_pointer = Self::from_slice(&vec)?;
        drop(block);

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
    pub async fn fetch_row<S: StorageIO>(&self, io: &mut S, row_fetch: &RowFetch) -> Result<Row> {
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
                let mut string_wrapper = unsafe { string_block.into_wrapper() };
                let mut string_buffer = string_wrapper.as_vec_mut();

                io.read_data_into_buffer(&mut string_row_position, &mut string_buffer).await;

                println!("Block data: {:?}", string_block);

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
                let mut wrapper = unsafe { block.into_wrapper() };
                let mut buffer = wrapper.as_vec_mut();
                
                // Read the column data directly into our buffer
                io.read_data_into_buffer(&mut position, &mut buffer).await;

                println!("Block data: {:?}", block);

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

        // Write the row data to the pointers_io
        row_pointer.save(pointers_io).await?;

        let io_position = row_pointer.position;

        let mut total_bytes = row_write.columns_writing_data.iter().map(|col| col.size as u64).sum::<u64>();

        let total_string_size = row_write.columns_writing_data.iter()
            .filter(|col| col.column_type == DbType::STRING)
            .map(|col| unsafe { col.data.into_wrapper().as_slice().len() as u64 })
            .sum::<u64>();

        total_bytes = total_bytes + total_string_size;

        // Count number of string columns
        // let string_columns_count = row_write.columns_writing_data.iter().filter(|col| col.column_type == DbType::STRING).count();
        // total_bytes = total_bytes + string_columns_count as u64 * (4 + 8); // 4 bytes for size and 8 bytes for pointer

        // Allocate memory to store the row data
        let block = MEMORY_POOL.acquire(total_bytes as usize);
        let mut wrapper = unsafe { block.into_wrapper() };
        let buffer = wrapper.as_vec_mut();

        let mut row_position = 0;

        let string_block = MEMORY_POOL.acquire(total_string_size as usize);
        let mut string_wrapper = unsafe { string_block.into_wrapper() };
        let string_data = string_wrapper.as_vec_mut();

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
                let string_column_data_wrapper = unsafe { column.data.into_wrapper() };
                let string_column_vec = string_column_data_wrapper.as_slice();

                let string_size = string_column_vec.len() as u32;

                string_data[string_data_position as usize..string_data_position as usize + string_size as usize].copy_from_slice(string_column_vec);

                string_data_position = string_data_position + string_size as u64;

                let end_of_row_bytes = end_of_row.to_le_bytes();

                // Write STRING_POINTER
                buffer[row_position as usize..row_position as usize + 8].copy_from_slice(&end_of_row_bytes);
             
                // Write STRING_SIZE
                buffer[row_position as usize + 8..row_position as usize + 4 + 8].copy_from_slice(&string_size.to_le_bytes());

                end_of_row = end_of_row + string_column_vec.len() as u64;

                row_position = end_of_column;
            } else {
                let column_data_wrapper = unsafe { column.data.into_wrapper() };
                let column_vec = column_data_wrapper.as_slice();

                buffer[row_position as usize..end_of_column as usize].copy_from_slice(column_vec);

                row_position = end_of_column;
            }
        }

        buffer[row_position as usize..].copy_from_slice(&string_data);

        // Write the row data to the rows_io
        rows_io.append_data(buffer.as_slice()).await;
        let write_result = rows_io.verify_data_and_sync(io_position, buffer.as_slice()).await;

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
            }
        }
        
        #[cfg(not(feature = "enable_long_row"))]
        {
            RowPointer {
                id,
                length,
                position,
                deleted,
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
        let cluster = 0;

        let last_id = AtomicU64::new(0);
        let table_length = AtomicU64::new(0);

        let string_bytes = b"Hello, world!";
        let string_data = MEMORY_POOL.acquire(string_bytes.len() + 4);
        let mut string_data_wrapper = unsafe { string_data.into_wrapper() };
        let string_vec = string_data_wrapper.as_vec_mut();
        let string_size = string_bytes.len() as u32;
        let string_size_bytes = string_size.to_le_bytes();
        string_vec[0] = string_size_bytes[0];
        string_vec[1] = string_size_bytes[1];
        string_vec[2] = string_size_bytes[2];
        string_vec[3] = string_size_bytes[3];
        string_vec[4..].copy_from_slice(string_bytes);

        let i32_bytes = 42_i32.to_le_bytes();
        let i32_data = MEMORY_POOL.acquire(i32_bytes.len());
        let mut i32_data_wrapper = unsafe { i32_data.into_wrapper() };
        let i32_vec = i32_data_wrapper.as_vec_mut();
        i32_vec.copy_from_slice(&i32_bytes);

        let row_write = RowWrite {
            columns_writing_data: vec![
                ColumnWritePayload {
                    data: string_data,
                    write_order: 0,
                    column_type: DbType::STRING,
                    size: string_bytes.len() as u32 + 4
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
        let cluster = 0;

        let last_id = AtomicU64::new(0);
        let table_length = AtomicU64::new(0);

        let string_bytes = b"Hello, world!";

        let string_data = MEMORY_POOL.acquire(string_bytes.len());
        let mut string_data_wrapper = unsafe { string_data.into_wrapper() };
        let string_vec = string_data_wrapper.as_vec_mut();

        string_vec[0..].copy_from_slice(string_bytes);

        let i32_bytes = 42_i32.to_le_bytes();
        let i32_data = MEMORY_POOL.acquire(i32_bytes.len());
        let mut i32_data_wrapper = unsafe { i32_data.into_wrapper() };
        let i32_vec = i32_data_wrapper.as_vec_mut();
        i32_vec.copy_from_slice(&i32_bytes);

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
            &mut mock_io_rows, 
            &row_fetch
        ).await.unwrap();

        row.columns.iter().for_each(|column| {
            match column.column_type {
                DbType::STRING => {
                    let wrapper = unsafe { column.data.into_wrapper() };
                    let string_data = wrapper.as_slice();
                    assert_eq!(&string_data[0..], string_bytes);
                },
                DbType::I32 => {
                    let wrapper = unsafe { column.data.into_wrapper() };
                    let i32_data = wrapper.as_slice();
                    assert_eq!(i32_data, i32_bytes);
                },
                _ => {}
            }
        });
    }
}