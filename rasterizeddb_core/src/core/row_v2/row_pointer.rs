use crate::{core::storage_providers::traits::StorageIO, memory_pool::MemoryBlockWrapper};
use crate::memory_pool::MEMORY_POOL;
use std::io::Result;
use byteorder::{LittleEndian, WriteBytesExt};

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
    total_length: u64,
    /// End of data flag
    end_of_data: bool,
}

/// Implementation for RowPointerIterator providing methods to create and iterate over RowPointers
impl<'a, S: StorageIO> RowPointerIterator<'a, S> {
    /// Create a new RowPointerIterator for the given StorageIO
    pub async fn new(io: &'a mut S) -> Result<Self> {
        const CHUNK_SIZE: usize = 64 * 1024; // 64KB chunks
        
        let total_length = io.get_len().await;
        let buffer = Vec::with_capacity(CHUNK_SIZE);
        
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
        const CHUNK_SIZE: usize = 64 * 1024; // 64KB chunks
        
        // Reset buffer index
        self.buffer_index = 0;
        
        // Check if we've reached the end of the storage
        if self.position >= self.total_length {
            self.end_of_data = true;
            return Ok(());
        }
        
        // Calculate how many bytes to read (may be less than CHUNK_SIZE at the end)
        let bytes_remaining = self.total_length - self.position;
        let bytes_to_read = std::cmp::min(bytes_remaining, CHUNK_SIZE as u64);
        
        // Clear the buffer and ensure capacity
        self.buffer.clear();
        self.buffer.reserve(bytes_to_read as usize);
        
        // Read data into the buffer
        self.io.read_data_into_buffer(&mut self.position, &mut self.buffer).await;
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
            // For simplicity, we'll move the remaining data to the beginning of the buffer,
            // load more data, and then parse the RowPointer
            let remaining_data = self.buffer[self.buffer_index..].to_vec();
            self.buffer.clear();
            self.buffer.extend_from_slice(&remaining_data);
            
            // Update buffer index and valid length
            let remaining_length = remaining_data.len();
            self.buffer_index = 0;
            self.buffer_valid_length = remaining_length;
            
            // Calculate how many bytes to read
            let bytes_remaining = self.total_length - (self.position - remaining_length as u64);
            let bytes_to_read = std::cmp::min(bytes_remaining, (64 * 1024 - remaining_length) as u64);
            
            // Read more data into the buffer
            let mut temp_position = self.position - remaining_length as u64 + bytes_to_read;
            self.io.read_data_into_buffer(&mut temp_position, &mut self.buffer).await;
            self.buffer_valid_length = self.buffer.len();
            
            // Update position for next read
            self.position += bytes_to_read;
            
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
        #[cfg(feature = "enable_long_row")]
        id: u128,
        #[cfg(feature = "enable_long_row")]
        length: u64,

        #[cfg(not(feature = "enable_long_row"))]
        id: u64,
        #[cfg(not(feature = "enable_long_row"))]
        length: u32,

        position: u64,

        #[cfg(feature = "enable_long_row")]
        checksum: u32,
        #[cfg(feature = "enable_long_row")]
        cluster: u32
    ) -> Self {
        #[cfg(feature = "enable_long_row")]
        let timestamp = Self::get_timestamp();

        RowPointer {
            id,
            length,
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

    /// Serializes the RowPointer into a Vec<u8>
    pub fn into_wrapper(&self) -> MemoryBlockWrapper {
        let block = MEMORY_POOL.acquire(TOTAL_LENGTH as u32);
        let mut wrapper = unsafe { block.into_wrapper() };
        let buffer = wrapper.as_vec_mut();
        
        // Write all fields in order
        #[cfg(feature = "enable_long_row")]
        buffer.write_u128::<LittleEndian>(self.id).unwrap();
        #[cfg(feature = "enable_long_row")]
        buffer.write_u64::<LittleEndian>(self.length).unwrap();

        #[cfg(not(feature = "enable_long_row"))]
        buffer.write_u64::<LittleEndian>(self.id).unwrap();
        #[cfg(not(feature = "enable_long_row"))]
        buffer.write_u32::<LittleEndian>(self.length).unwrap();

        buffer.write_u64::<LittleEndian>(self.position).unwrap();
        buffer.write_u8(self.deleted as u8).unwrap();

       #[cfg(feature = "enable_long_row")]
       {
            buffer.write_u32::<LittleEndian>(self.checksum).unwrap();
            buffer.write_u32::<LittleEndian>(self.cluster).unwrap();
            buffer.write_u128::<LittleEndian>(self.deleted_at).unwrap();
            buffer.write_u128::<LittleEndian>(self.created_at).unwrap();
            buffer.write_u128::<LittleEndian>(self.updated_at).unwrap();
            buffer.write_u16::<LittleEndian>(self.version).unwrap();
            buffer.write_u8(self.is_active as u8).unwrap();
       }
  
        wrapper
    }
    
    /// Writes the RowPointer to the given StorageIO at the specified position
    pub async fn write_to_io<S: StorageIO>(&self, io: &mut S) -> Result<()> {
        let wrapper = self.into_wrapper();

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

        let block = MEMORY_POOL.acquire(TOTAL_LENGTH as u32);
        let mut wrapper = unsafe { block.into_wrapper() };
        let mut vec = wrapper.as_vec_mut();
        
        // Read all data in one operation
        io.read_data_into_buffer(position, &mut vec).await;

        let row_pointer = Self::from_slice(&vec)?;
        drop(block);

        Ok(row_pointer)
    }
}