use crate::core::storage_providers::traits::StorageIO;
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
    pub fn into_vec(&self) -> Vec<u8> {
        let mut buffer = Vec::with_capacity(TOTAL_LENGTH);
        
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
  
        buffer
    }
    
    /// Writes the RowPointer to the given StorageIO at the specified position
    pub async fn write_to_io<S: StorageIO>(&self, io: &mut S) -> Result<()> {
        let buffer = self.into_vec();

        let position = io.get_len().await;

        // Write data to storage
        io.append_data(buffer.as_slice()).await;
        
        // Verify written data
        io.verify_data_and_sync(position, &buffer).await;
        
        Ok(())
    }
    
    /// Deserializes a Vec<u8> into a RowPointer
    pub fn from_vec(buffer: &[u8]) -> Result<Self> {
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
        let mut buffer = Vec::with_capacity(TOTAL_LENGTH as usize);

        // Read all data in one operation
        io.read_data_into_buffer(position, &mut buffer).await;

        Self::from_vec(&buffer)
    }
}