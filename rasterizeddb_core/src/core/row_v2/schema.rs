use std::io;

use crate::{core::{db_type::DbType, storage_providers::traits::StorageIO}, memory_pool::{MemoryBlock, MEMORY_POOL}};

const SCHEMA_FIELD_SIZE: u64 = 
    129 + // up to 128 ascii chars for name + null terminator
    2 + // db_type u16,
    4 + // size u32,
    8 + // offset u64,
    8 + // write_order u64,
    1; // is_unique u8

/// Size of each chunk to read from storage
const SCHEMA_FIELD_CHUNK_SIZE: usize = SCHEMA_FIELD_SIZE as usize * 100;

/// Number of schema fields to retrieve in a batch
const BATCH_SIZE: usize = 100;

const TABLE_SIZE: usize = 129 * 2; // 128 chars for name  + null terminator and primary key + null terminator

pub struct SchemaField {
    pub name: String,
    pub db_type: DbType,
    pub size: u32,
    pub offset: u64,
    pub write_order: u64,
    pub is_unique: bool
}

impl SchemaField {
    pub fn new(name: String, db_type: DbType, size: u32, offset: u64, write_order: u64) -> Self {
        SchemaField {
            name,
            db_type,
            size,
            offset,
            write_order,
            is_unique: false
        }
    }

    pub fn set_unique(&mut self) {
        self.is_unique = true;
    }

    pub fn into_vec(&self) -> Vec<u8> {
        let mut vec = vec![0; SCHEMA_FIELD_SIZE as usize];
        
        // Filter name to contain only ASCII letters and numbers
        let filtered_name: String = self.name.chars()
            .filter(|c| c.is_ascii_alphanumeric() || *c == '_')
            .collect();
        
        // Copy filtered name bytes into vec (max 128 chars)
        let name_bytes = filtered_name.as_bytes();
        let copy_len = std::cmp::min(name_bytes.len(), 128);
        vec[..copy_len].copy_from_slice(&name_bytes[..copy_len]);
        
        // Name is already null terminated by the initial zeroes
        
        // Add type at position 129
        let type_bytes = (self.db_type.to_byte() as u16).to_le_bytes();
        vec[129..129+2].copy_from_slice(&type_bytes);
        
        // Add size at position 131
        let size_bytes = self.size.to_le_bytes();
        vec[131..131+4].copy_from_slice(&size_bytes);
        
        // Add offset at position 135
        let offset_bytes = self.offset.to_le_bytes();
        vec[135..135+8].copy_from_slice(&offset_bytes);
        
        // Add write_order at position 143
        let write_order_bytes = self.write_order.to_le_bytes();
        vec[143..143+8].copy_from_slice(&write_order_bytes);
        
        // Add is_unique at position 151
        vec[151] = if self.is_unique { 1 } else { 0 };
        
        vec
    }

    pub fn from_vec(vec: &[u8]) -> Result<Self, String> {
        if vec.len() != SCHEMA_FIELD_SIZE as usize {
            return Err(format!("Invalid schema field size: expected {} bytes, got {}", 
                               SCHEMA_FIELD_SIZE, vec.len()));
        }

        // Extract name (read until null terminator or max 128 chars)
        let name_end = vec.iter().take(129).position(|&b| b == 0).unwrap_or(128);

        let name = match std::str::from_utf8(&vec[0..name_end]) {
            Ok(s) => s.to_string(),
            Err(e) => return Err(format!("Invalid UTF-8 in schema field name: {}", e)),
        };

        // Extract type from position 129
        let db_type_value = u16::from_le_bytes([vec[129], vec[130]]);
        let db_type = DbType::from_byte(db_type_value as u8);

        // Extract size from position 131
        let size = u32::from_le_bytes([vec[131], vec[132], vec[133], vec[134]]);

        // Extract offset from position 135
        let offset = u64::from_le_bytes([
            vec[135], vec[136], vec[137], vec[138], 
            vec[139], vec[140], vec[141], vec[142]
        ]);

        // Extract write_order from position 143
        let write_order = u64::from_le_bytes([
            vec[143], vec[144], vec[145], vec[146],
            vec[147], vec[148], vec[149], vec[150]
        ]);

        // Extract is_unique from position 151
        let is_unique = vec[151] != 0;

        Ok(SchemaField {
            name,
            db_type,
            size,
            offset,
            write_order,
            is_unique,
        })
    }

    pub async fn save_field(&self, io: &mut impl StorageIO) -> io::Result<()> {
        let data = self.into_vec();
        io.append_data(&data).await;
        Ok(())
    }
}

/// Iterates over SchemaFields from a StorageIO in 64KB chunks
pub struct SchemaFieldIterator<'a, S: StorageIO> {
    /// Reference to the storage provider
    io: &'a mut S,
    /// Current position in the storage
    position: u64,
    /// Buffer to store chunks
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

impl<'a, S: StorageIO> SchemaFieldIterator<'a, S> {
    /// Create a new SchemaFieldIterator for the given StorageIO
    pub async fn new(io: &'a mut S) -> io::Result<Self> {
        let total_length = io.get_len().await as usize;

        let buffer_size = if total_length > SCHEMA_FIELD_CHUNK_SIZE {
            SCHEMA_FIELD_CHUNK_SIZE
        } else {
            total_length
        };

        let memory_block = MEMORY_POOL.acquire(buffer_size);

        let mut iterator = SchemaFieldIterator {
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
    
    /// Reset the iterator to the beginning of the data
    pub fn reset(&mut self) {
        self.position = 0;
        self.buffer_index = 0;
        self.buffer_valid_length = 0;
        self.end_of_data = false;
    }

    /// Load the next chunk of data into the buffer
    async fn load_next_chunk(&mut self) -> io::Result<()> {
        // Reset buffer index
        self.buffer_index = 0;

        // Check if we've reached the end of the storage
        if self.position >= self.total_length as u64 {
            self.end_of_data = true;
            return Ok(());
        }
        
        // Calculate how many bytes to read (may be less than CHUNK_SIZE at the end)
        let bytes_remaining = self.total_length as u64 - self.position;
        let bytes_to_read = std::cmp::min(bytes_remaining, SCHEMA_FIELD_CHUNK_SIZE as u64);
        
        // Get a buffer of appropriate size from the memory pool
        self.buffer = MEMORY_POOL.acquire(bytes_to_read as usize);
        
        // Read data into the buffer
        let mut read_position = self.position;
        self.io.read_data_into_buffer(&mut read_position, &mut self.buffer.into_slice_mut()).await;
        self.buffer_valid_length = self.buffer.into_slice().len();
        
        // Update position for next read
        self.position += bytes_to_read;
        
        Ok(())
    }
    
    /// Get the next SchemaField from the buffer, loading a new chunk if necessary
    pub async fn next_schema_field(&mut self) -> io::Result<Option<SchemaField>> {
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
        
        // Extract the schema field data from the buffer
        let field_data = &self.buffer.into_slice()[self.buffer_index..self.buffer_index + SCHEMA_FIELD_SIZE as usize];
        
        // Parse the SchemaField from the buffer
        let schema_field = match SchemaField::from_vec(field_data) {
            Ok(field) => field,
            Err(_e) => return Err(io::Error::new(io::ErrorKind::InvalidData, "Failed to parse SchemaField.")),
        };
        
        // Advance the buffer index
        self.buffer_index += SCHEMA_FIELD_SIZE as usize;
        
        Ok(Some(schema_field))
    }
    
    /// Get multiple SchemaFields at once, up to BATCH_SIZE
    pub async fn next_schema_fields(&mut self) -> io::Result<Vec<SchemaField>> {
        let mut fields = Vec::with_capacity(BATCH_SIZE);
        
        for _ in 0..BATCH_SIZE {
            match self.next_schema_field().await? {
                Some(field) => fields.push(field),
                None => break,
            }
        }
        
        Ok(fields)
    }

    /// Alias for next_schema_field to provide a more standard interface
    pub async fn next(&mut self) -> io::Result<Option<SchemaField>> {
        self.next_schema_field().await
    }
}

pub struct TableSchema {
    pub name: String,
    pub fields: Vec<SchemaField>,
    pub primary_key: Option<String>,
    pub indexes: Vec<TableIndex>
}

pub struct TableIndex {
    pub name: String,
    pub fields: Vec<String>,
    pub is_unique: bool,
}

impl TableSchema {
    pub fn new(name: String) -> Self {
        TableSchema {
            name,
            fields: Vec::new(),
            primary_key: None,
            indexes: Vec::new(),
        }
    }

    pub async fn load(schema_io: &mut impl StorageIO) -> io::Result<Self> {
        let buffer = MEMORY_POOL.acquire(TABLE_SIZE);
        schema_io.read_data_into_buffer(&mut 0, &mut buffer.into_slice_mut()).await;
        
        let result = match  TableSchema::from_vec(&buffer.into_slice()) {
            Ok(table_schema) => Ok(table_schema),
            Err(e) => Err(io::Error::new(io::ErrorKind::InvalidData, format!("Failed to parse TableSchema: {}", e))),
        };

        result
    }

    /// Saves the schema to a file
    pub fn save_schema<S: StorageIO>(
        &self, 
        _schema_io: &mut S,
        _indexes_io: &mut S,
        _field_io: &mut S) -> Result<(), String> {
        


        Ok(())
    }

    pub fn into_vec(&self) -> Vec<u8> {
        // Always create a buffer of TABLE_SIZE (129*2) bytes, initialized with zeros
        let mut vec = vec![0; TABLE_SIZE];
        
        // Filter name to contain only ASCII letters and numbers
        let filtered_name: String = self.name.chars()
            .filter(|c| c.is_ascii_alphanumeric() || *c == '_')
            .collect();
        
        // Copy filtered name bytes into vec (max 128 chars)
        let name_bytes = filtered_name.as_bytes();
        let copy_len = std::cmp::min(name_bytes.len(), 128);
        vec[..copy_len].copy_from_slice(&name_bytes[..copy_len]);
        // The null terminator is already there from initializing with zeros
        
        // Primary key bytes (starting at position 129)
        if let Some(primary_key) = &self.primary_key {
            // Filter primary key to ASCII letters and numbers
            let filtered_pk: String = primary_key.chars()
                .filter(|c| c.is_ascii_alphanumeric() || *c == '_')
                .collect();
                
            let pk_bytes = filtered_pk.as_bytes();
            let pk_copy_len = std::cmp::min(pk_bytes.len(), 128);
            vec[129..(129 + pk_copy_len)].copy_from_slice(&pk_bytes[..pk_copy_len]);
            // The null terminator is already there from initializing with zeros
        }
        // If no primary key, this section remains all zeros
        
        vec
    }

    pub fn from_vec(vec: &[u8]) -> Result<Self, String> {
        if vec.len() != TABLE_SIZE {
            return Err(format!("Invalid table schema size: expected {} bytes, got {}", 
                            TABLE_SIZE, vec.len()));
        }
        
        // Extract table name (read until null terminator or max 128 chars)
        let name_end = vec.iter().take(129).position(|&b| b == 0).unwrap_or(128);
        let name = match std::str::from_utf8(&vec[0..name_end]) {
            Ok(s) => s.to_string(),
            Err(e) => return Err(format!("Invalid UTF-8 in table name: {}", e)),
        };
        
        // Extract primary key (read until null terminator, starting at position 129)
        // Check if the first byte of the primary key section is 0 (no primary key)
        let primary_key = if vec[129] == 0 {
            None
        } else {
            // Find the null terminator in the primary key section
            let pk_end = vec[129..].iter().position(|&b| b == 0).unwrap_or(128);
            match std::str::from_utf8(&vec[129..(129 + pk_end)]) {
                Ok(s) if !s.is_empty() => Some(s.to_string()),
                _ => None,
            }
        };
        
        Ok(TableSchema {
            name,
            fields: Vec::new(), // Fields are stored separately
            primary_key,
            indexes: Vec::new(), // Indexes are stored separately
        })
    }
    
    pub async fn save(&self, schema_io: &mut impl StorageIO) -> io::Result<()> {
        let data = self.into_vec();
        schema_io.append_data(&data).await;
        Ok(())
    }

    pub async fn add_field<S: StorageIO>(&mut self, schema_io: &S, field: SchemaField) {
        let mut field_io = schema_io.create_new("FIELDS.db".into()).await;
        _ = field.save_field(&mut field_io).await;
        self.fields.push(field);
    }

    pub fn set_primary_key(&mut self, primary_key: String) {
        self.primary_key = Some(primary_key);
    }

    pub fn add_index(&mut self, index: TableIndex) {
        self.indexes.push(index);
    }
}

/// Iterates over TableSchema objects from a StorageIO in chunks
pub struct TableSchemaIterator<'a, S: StorageIO> {
    /// Reference to the storage provider
    io: &'a mut S,
    /// Current position in the storage
    position: u64,
    /// Buffer to store chunks
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

impl<'a, S: StorageIO> TableSchemaIterator<'a, S> {
    /// Create a new TableSchemaIterator for the given StorageIO
    pub async fn new(io: &'a mut S) -> io::Result<Self> {
        let total_length = io.get_len().await as usize;
        
        // Use a reasonable chunk size (multiple of TABLE_SIZE)
        let chunk_size = 100 * TABLE_SIZE;
        
        let buffer_size = if total_length > chunk_size {
            chunk_size
        } else {
            total_length
        };
        
        let memory_block = MEMORY_POOL.acquire(buffer_size);
        
        let mut iterator = TableSchemaIterator {
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
    
    /// Reset the iterator to the beginning of the data
    pub fn reset(&mut self) {
        self.position = 0;
        self.buffer_index = 0;
        self.buffer_valid_length = 0;
        self.end_of_data = false;
    }
    
    /// Load the next chunk of data into the buffer
    async fn load_next_chunk(&mut self) -> io::Result<()> {
        // Reset buffer index
        self.buffer_index = 0;
        
        // Check if we've reached the end of the storage
        if self.position >= self.total_length as u64 {
            self.end_of_data = true;
            return Ok(());
        }
        
        // Calculate how many bytes to read
        let bytes_remaining = self.total_length as u64 - self.position;
        let chunk_size = 100 * TABLE_SIZE;
        let bytes_to_read = std::cmp::min(bytes_remaining, chunk_size as u64);
        
        // Get a buffer of appropriate size from the memory pool
        self.buffer = MEMORY_POOL.acquire(bytes_to_read as usize);
        
        // Read data into the buffer
        let mut read_position = self.position;
        self.io.read_data_into_buffer(&mut read_position, &mut self.buffer.into_slice_mut()).await;
        self.buffer_valid_length = self.buffer.into_slice().len();
        
        // Update position for next read
        self.position += bytes_to_read;
        
        Ok(())
    }
    
    /// Get the next TableSchema from the buffer, loading a new chunk if necessary
    pub async fn next_table_schema(&mut self) -> io::Result<Option<TableSchema>> {
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
        
        // Extract the table schema data from the buffer
        let schema_data = &self.buffer.into_slice()[self.buffer_index..self.buffer_index + TABLE_SIZE];
        
        // Parse the TableSchema from the buffer
        let table_schema = match TableSchema::from_vec(schema_data) {
            Ok(schema) => schema,
            Err(_e) => return Err(io::Error::new(io::ErrorKind::InvalidData, "Failed to parse TableSchema.")),
        };
        
        // Advance the buffer index
        self.buffer_index += TABLE_SIZE;
        
        Ok(Some(table_schema))
    }
    
    /// Alias for next_table_schema to provide a more standard interface
    pub async fn next(&mut self) -> io::Result<Option<TableSchema>> {
        self.next_table_schema().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{
        db_type::DbType,
        storage_providers::mock_file_sync::MockStorageProvider,
    };
    use std::io;

    // Helper function to create a schema field and return it
    fn create_test_schema_field(name: &str, db_type: DbType, offset: u64, write_order: u64, is_unique: bool) -> SchemaField {
        let db_size = db_type.get_size();
        let mut schema_field = SchemaField::new(
            name.to_string(),
            db_type,
            db_size,
            offset,
            write_order,
        );
        
        if is_unique {
            schema_field.set_unique();
        }
        
        schema_field
    }
    
    // Helper function to save a schema field to storage
    async fn save_schema_field(field: &SchemaField, storage: &mut MockStorageProvider) -> io::Result<()> {
        field.save_field(storage).await
    }
    
    #[tokio::test]
    async fn test_schema_field_iterator_empty() {
        let mut mock_io = MockStorageProvider::new().await;
        
        let mut iterator = SchemaFieldIterator::new(&mut mock_io).await.unwrap();
        
        // Should return None for an empty storage
        assert!(iterator.next_schema_field().await.unwrap().is_none());
    }
    
    #[tokio::test]
    async fn test_schema_field_iterator_single_field() {
        // Create a single schema field
        let schema_field = create_test_schema_field("id", DbType::I32, 0, 1, true);

        let mut mock_io = MockStorageProvider::new().await;
        
        save_schema_field(&schema_field, &mut mock_io).await.unwrap();

        let mut schema_field_iterator = SchemaFieldIterator::new(&mut mock_io).await.unwrap();
        
        // Should return our schema field
        let read_field = schema_field_iterator.next_schema_field().await.unwrap().unwrap();
        
        assert_eq!(read_field.name, "id");
        assert_eq!(read_field.db_type, DbType::I32);
        assert_eq!(read_field.offset, 0);
        assert_eq!(read_field.write_order, 1);
        assert_eq!(read_field.is_unique, true);
        
        // Next call should return None
        assert!(schema_field_iterator.next_schema_field().await.unwrap().is_none());
    }
    
    #[tokio::test]
    async fn test_schema_field_iterator_2_fields() {
        // Create schema fields
        let schema_field1 = create_test_schema_field("id", DbType::I32, 0, 1, true);
        
        let mut mock_io = MockStorageProvider::new().await;
        
        save_schema_field(&schema_field1, &mut mock_io).await.unwrap();
        save_schema_field(&schema_field1, &mut mock_io).await.unwrap();

        let mut schema_field_iterator = SchemaFieldIterator::new(&mut mock_io).await.unwrap();
        
        // Should return our first schema field
        let read_field = schema_field_iterator.next_schema_field().await.unwrap().unwrap();
        
        assert_eq!(read_field.name, "id");
        assert_eq!(read_field.db_type, DbType::I32);
        assert_eq!(read_field.offset, 0);
        assert_eq!(read_field.write_order, 1);
        assert_eq!(read_field.is_unique, true);
        
        // Should return the second copy of the same schema field
        assert!(schema_field_iterator.next_schema_field().await.unwrap().is_some());
        
        // Next call should return None
        assert!(schema_field_iterator.next_schema_field().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_schema_field_iterator_3_fields() {
        // Create schema fields
        let schema_field1 = create_test_schema_field("id", DbType::I32, 0, 1, true);
        let schema_field2 = create_test_schema_field("name", DbType::STRING, 8, 2, false);

        let mut mock_io = MockStorageProvider::new().await;
        
        save_schema_field(&schema_field1, &mut mock_io).await.unwrap();
        save_schema_field(&schema_field2, &mut mock_io).await.unwrap();
        save_schema_field(&schema_field1, &mut mock_io).await.unwrap();

        let mut schema_field_iterator = SchemaFieldIterator::new(&mut mock_io).await.unwrap();
        
        // Should return our first schema field
        let read_field = schema_field_iterator.next_schema_field().await.unwrap().unwrap();
        
        assert_eq!(read_field.name, "id");
        assert_eq!(read_field.db_type, DbType::I32);
        assert_eq!(read_field.offset, 0);
        assert_eq!(read_field.write_order, 1);
        assert_eq!(read_field.is_unique, true);
        
        // Should return the second schema field
        let read_field = schema_field_iterator.next_schema_field().await.unwrap().unwrap();
        
        assert_eq!(read_field.name, "name");
        assert_eq!(read_field.db_type, DbType::STRING);
        assert_eq!(read_field.offset, 8);
        assert_eq!(read_field.write_order, 2);
        assert_eq!(read_field.is_unique, false);

        // Should return the third schema field (duplicate of the first)
        assert!(schema_field_iterator.next_schema_field().await.unwrap().is_some());
        
        // Next call should return None
        assert!(schema_field_iterator.next_schema_field().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_schema_field_iterator_batch_retrieval() {
        let mut mock_io = MockStorageProvider::new().await;
        
        // Create and save multiple schema fields (more than BATCH_SIZE)
        for i in 0..BATCH_SIZE+10 {
            let field_name = format!("field{}", i);
            let schema_field = create_test_schema_field(
                &field_name,
                DbType::I32,
                i as u64 * 8,
                i as u64,
                i % 2 == 0, // Make even-numbered fields unique
            );
            
            save_schema_field(&schema_field, &mut mock_io).await.unwrap();
        }
        
        let mut iterator = SchemaFieldIterator::new(&mut mock_io).await.unwrap();
        
        // Test batch retrieval
        let first_batch = iterator.next_schema_fields().await.unwrap();
        assert_eq!(first_batch.len(), BATCH_SIZE);
        
        // Check first and last items in the batch
        assert_eq!(first_batch[0].name, "field0");
        assert_eq!(first_batch[0].is_unique, true);
        assert_eq!(first_batch[BATCH_SIZE-1].name, format!("field{}", BATCH_SIZE-1));
        
        // Test retrieval of remaining items
        let second_batch = iterator.next_schema_fields().await.unwrap();
        assert_eq!(second_batch.len(), 10);
        
        // Ensure no more items
        let empty_batch = iterator.next_schema_fields().await.unwrap();
        assert_eq!(empty_batch.len(), 0);
    }
    
    #[tokio::test]
    async fn test_schema_field_iterator_reset() {
        let mut mock_io = MockStorageProvider::new().await;
        
        // Create and save schema fields
        let schema_field1 = create_test_schema_field("id", DbType::I32, 0, 1, true);
        let schema_field2 = create_test_schema_field("name", DbType::STRING, 8, 2, false);
        
        save_schema_field(&schema_field1, &mut mock_io).await.unwrap();
        save_schema_field(&schema_field2, &mut mock_io).await.unwrap();
        
        // Create iterator and consume all items
        let mut iterator = SchemaFieldIterator::new(&mut mock_io).await.unwrap();
        
        assert!(iterator.next_schema_field().await.unwrap().is_some());
        assert!(iterator.next_schema_field().await.unwrap().is_some());
        assert!(iterator.next_schema_field().await.unwrap().is_none());
        
        // Reset and verify we can read the items again
        iterator.reset();
        
        // Should be able to read the items again
        let read_field1 = iterator.next_schema_field().await.unwrap().unwrap();
        assert_eq!(read_field1.name, "id");
        
        let read_field2 = iterator.next_schema_field().await.unwrap().unwrap();
        assert_eq!(read_field2.name, "name");
        
        // No more items
        assert!(iterator.next_schema_field().await.unwrap().is_none());
    }

        // Helper function to create a test TableSchema
    fn create_test_table_schema(name: &str, primary_key: Option<&str>) -> TableSchema {
        let mut table = TableSchema::new(name.to_string());
        if let Some(pk) = primary_key {
            table.set_primary_key(pk.to_string());
        }
        table
    }

    // Helper function to save a table schema to storage
    async fn save_table_schema(schema: &TableSchema, storage: &mut MockStorageProvider) -> io::Result<()> {
        schema.save(storage).await
    }

    #[test]
    fn test_table_schema_into_vec() {
        // Table with no primary key
        let schema = create_test_table_schema("users", None);
        let vec = schema.into_vec();
        
        // Verify size
        assert_eq!(vec.len(), TABLE_SIZE);
        
        // Verify name
        let name_bytes = "users".as_bytes();
        for (i, &b) in name_bytes.iter().enumerate() {
            assert_eq!(vec[i], b);
        }
        
        // Verify null terminator after name
        assert_eq!(vec[5], 0);
        
        // Verify primary key section is all zeros
        for i in 129..TABLE_SIZE {
            assert_eq!(vec[i], 0);
        }
        
        // Table with primary key
        let schema = create_test_table_schema("customers", Some("id"));
        let vec = schema.into_vec();
        
        // Verify name
        let name_bytes = "customers".as_bytes();
        for (i, &b) in name_bytes.iter().enumerate() {
            assert_eq!(vec[i], b);
        }
        
        // Verify primary key
        let pk_bytes = "id".as_bytes();
        for (i, &b) in pk_bytes.iter().enumerate() {
            assert_eq!(vec[129 + i], b);
        }
        
        // Verify null terminator after primary key
        assert_eq!(vec[131], 0);
    }

    #[test]
    fn test_table_schema_from_vec() {
        // Create a buffer for a table with name "users" and primary key "id"
        let mut vec = vec![0; TABLE_SIZE];
        
        // Set name
        let name = "users";
        let name_bytes = name.as_bytes();
        vec[..name_bytes.len()].copy_from_slice(name_bytes);
        
        // Set primary key
        let pk = "id";
        let pk_bytes = pk.as_bytes();
        vec[129..129+pk_bytes.len()].copy_from_slice(pk_bytes);
        
        // Parse the buffer
        let schema = TableSchema::from_vec(&vec).unwrap();
        
        // Verify parsed data
        assert_eq!(schema.name, "users");
        assert_eq!(schema.primary_key, Some("id".to_string()));
        
        // Create a buffer for a table with name "products" and no primary key
        let mut vec = vec![0; TABLE_SIZE];
        
        // Set name
        let name = "products";
        let name_bytes = name.as_bytes();
        vec[..name_bytes.len()].copy_from_slice(name_bytes);
        
        // Parse the buffer
        let schema = TableSchema::from_vec(&vec).unwrap();
        
        // Verify parsed data
        assert_eq!(schema.name, "products");
        assert_eq!(schema.primary_key, None);
    }
    
    #[tokio::test]
    async fn test_table_schema_serialize_deserialize() {
        let mut mock_io = MockStorageProvider::new().await;

        // Create schema with fields and indexes
        let mut schema = create_test_table_schema("orders", Some("order_id"));
        
        schema.save(&mut mock_io).await.unwrap();

        // Add fields
        let id_field = SchemaField::new(
            "order_id".to_string(),
            DbType::I64,
            8,
            0,
            1
        );
        schema.add_field(&mut mock_io, id_field).await;
        
        let date_field = SchemaField::new(
            "created_at".to_string(),
            DbType::U64,
            8,
            8,
            2
        );
        schema.add_field(&mut mock_io, date_field).await;
        
        // Serialize
        let vec = schema.into_vec();
        
        // Deserialize
        let deserialized = TableSchema::from_vec(&vec).unwrap();
        
        // Verify core properties (fields are not part of serialization)
        assert_eq!(deserialized.name, "orders");
        assert_eq!(deserialized.primary_key, Some("order_id".to_string()));
        assert!(deserialized.fields.is_empty());
    }
    
    #[test]
    fn test_table_schema_special_chars() {
        // Create schema with special characters in name and primary key
        let mut schema = TableSchema::new("users!@#123".to_string());
        schema.set_primary_key("id!@#123".to_string());
        
        // Serialize
        let vec = schema.into_vec();
        
        // Deserialize
        let deserialized = TableSchema::from_vec(&vec).unwrap();
        
        // Verify filtered content (only alphanumeric characters)
        assert_eq!(deserialized.name, "users123");
        assert_eq!(deserialized.primary_key, Some("id123".to_string()));
    }
    
    #[tokio::test]
    async fn test_table_schema_iterator_empty() {
        let mut mock_io = MockStorageProvider::new().await;
        
        let mut iterator = TableSchemaIterator::new(&mut mock_io).await.unwrap();
        
        // Should return None for an empty storage
        assert!(iterator.next_table_schema().await.unwrap().is_none());
    }
    
    #[tokio::test]
    async fn test_table_schema_iterator_single_schema() {
        // Create a single table schema
        let table_schema = create_test_table_schema("users", Some("id"));

        let mut mock_io = MockStorageProvider::new().await;
        
        save_table_schema(&table_schema, &mut mock_io).await.unwrap();

        let mut iterator = TableSchemaIterator::new(&mut mock_io).await.unwrap();
        
        // Should return our table schema
        let read_schema = iterator.next_table_schema().await.unwrap().unwrap();
        
        assert_eq!(read_schema.name, "users");
        assert_eq!(read_schema.primary_key, Some("id".to_string()));
        
        // Next call should return None
        assert!(iterator.next_table_schema().await.unwrap().is_none());
    }
    
    #[tokio::test]
    async fn test_table_schema_iterator_multiple_schemas() {
        // Create schemas
        let schema1 = create_test_table_schema("users", Some("id"));
        let schema2 = create_test_table_schema("products", Some("product_id"));
        let schema3 = create_test_table_schema("orders", None);

        let mut mock_io = MockStorageProvider::new().await;
        
        // Save schemas to storage
        save_table_schema(&schema1, &mut mock_io).await.unwrap();
        save_table_schema(&schema2, &mut mock_io).await.unwrap();
        save_table_schema(&schema3, &mut mock_io).await.unwrap();

        // Create iterator
        let mut iterator = TableSchemaIterator::new(&mut mock_io).await.unwrap();
        
        // Get first schema
        let read_schema1 = iterator.next_table_schema().await.unwrap().unwrap();
        assert_eq!(read_schema1.name, "users");
        assert_eq!(read_schema1.primary_key, Some("id".to_string()));
        
        // Get second schema
        let read_schema2 = iterator.next_table_schema().await.unwrap().unwrap();
        assert_eq!(read_schema2.name, "products");
        assert_eq!(read_schema2.primary_key, Some("product_id".to_string()));
        
        // Get third schema
        let read_schema3 = iterator.next_table_schema().await.unwrap().unwrap();
        assert_eq!(read_schema3.name, "orders");
        assert_eq!(read_schema3.primary_key, None);
        
        // No more schemas
        assert!(iterator.next_table_schema().await.unwrap().is_none());
    }
    
    #[tokio::test]
    async fn test_table_schema_iterator_reset() {
        // Create schemas
        let schema1 = create_test_table_schema("users", Some("id"));
        let schema2 = create_test_table_schema("products", None);
        
        let mut mock_io = MockStorageProvider::new().await;
        
        // Save schemas to storage
        save_table_schema(&schema1, &mut mock_io).await.unwrap();
        save_table_schema(&schema2, &mut mock_io).await.unwrap();
        
        // Create iterator and read all schemas
        let mut iterator = TableSchemaIterator::new(&mut mock_io).await.unwrap();
        
        assert!(iterator.next_table_schema().await.unwrap().is_some());
        assert!(iterator.next_table_schema().await.unwrap().is_some());
        assert!(iterator.next_table_schema().await.unwrap().is_none());
        
        // Reset iterator
        iterator.reset();
        
        // Should be able to read schemas again
        let read_schema1 = iterator.next_table_schema().await.unwrap().unwrap();
        assert_eq!(read_schema1.name, "users");
        
        let read_schema2 = iterator.next_table_schema().await.unwrap().unwrap();
        assert_eq!(read_schema2.name, "products");
        
        // No more schemas
        assert!(iterator.next_table_schema().await.unwrap().is_none());
    }
    
    #[tokio::test]
    async fn test_table_schema_save_and_iterator() {
        // Create multiple schemas
        let mut mock_io = MockStorageProvider::new().await;
        
        // Create and save schemas
        for i in 0..5 {
            let table_name = format!("table{}", i);
            let primary_key = if i % 2 == 0 {
                Some(format!("id{}", i))
            } else {
                None
            };
            
            let schema = create_test_table_schema(&table_name, primary_key.as_deref());
            save_table_schema(&schema, &mut mock_io).await.unwrap();
        }
        
        // Read back using iterator
        let mut iterator = TableSchemaIterator::new(&mut mock_io).await.unwrap();
        
        for i in 0..5 {
            let schema = iterator.next_table_schema().await.unwrap().unwrap();
            
            let expected_name = format!("table{}", i);
            let expected_pk = if i % 2 == 0 {
                Some(format!("id{}", i))
            } else {
                None
            };
            
            assert_eq!(schema.name, expected_name);
            assert_eq!(schema.primary_key, expected_pk);
        }
        
        // No more schemas
        assert!(iterator.next_table_schema().await.unwrap().is_none());
    }
    
    #[tokio::test]
    async fn test_table_schema_long_names() {
        // Create a schema with long name and primary key
        let long_name = "a".repeat(200);  // More than 128 chars
        let long_pk = "b".repeat(200);    // More than 128 chars
        
        let schema = create_test_table_schema(&long_name, Some(&long_pk));
        
        let mut mock_io = MockStorageProvider::new().await;
        save_table_schema(&schema, &mut mock_io).await.unwrap();
        
        // Read back using iterator
        let mut iterator = TableSchemaIterator::new(&mut mock_io).await.unwrap();
        let read_schema = iterator.next_table_schema().await.unwrap().unwrap();
        
        // Should be truncated to 128 chars
        assert_eq!(read_schema.name.len(), 128);
        assert_eq!(read_schema.primary_key.clone().unwrap().len(), 128);
        
        assert_eq!(read_schema.name, "a".repeat(128));
        assert_eq!(read_schema.primary_key.as_ref().unwrap().to_string(), "b".repeat(128));
    }
}