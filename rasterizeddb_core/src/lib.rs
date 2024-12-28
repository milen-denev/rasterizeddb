use ahash::RandomState;
use moka::sync::Cache;
use once_cell::sync::Lazy;

pub(crate) const HEADER_SIZE: u16 = 30;
pub(crate) const CHUNK_SIZE: u32 = 1_000_000;

pub(crate) const EMPTY_BUFFER: [u8; 8] = [0, 0, 0, 0, 0, 0, 0, 0];

pub(crate) static POSITIONS_CACHE: Lazy<Cache<u64, Vec<(u64, u32)>, RandomState>> = Lazy::new(|| {
    let cache = Cache::builder()
        .max_capacity(u64::MAX)
        .build_with_hasher(ahash::RandomState::new());

    return cache;
});

/// # Rasterized DB (Alpha)
/// ## A new schemaless high-performance database written in Rust from scratch.
/// 
/// #### Features:
/// `enable_index_caching` to enable query caching
/// 
/// #### Create a static TABLE
/// ```rust
/// //Local File Storage Database
/// let io_sync = LocalStorageProvider::new(
///     "Some\\Folder",
///     "database.db"
/// );
/// //In-Memory Database
/// let io_sync = MemoryStorageProvider::new();
/// 
/// let mut table = Table::init(io_sync, false, false).unwrap();
/// ```
/// 
/// #### Create columns, rows and insert them
/// ```rust
/// let mut c1 = Column::new(10 as i32).unwrap();
/// let mut c2 = Column::new(50.0 as f64).unwrap();
/// let mut c3 = Column::new("This is awesome").unwrap();
/// let mut columns_buffer: Vec<u8> = Vec::with_capacity(
///     c1.len() + 
///     c2.len() +
///     c3.len() +
/// );
/// columns_buffer.append(&mut c1.into_vec().unwrap());
/// columns_buffer.append(&mut c2.into_vec().unwrap());
/// columns_buffer.append(&mut c3.into_vec().unwrap());
/// 
/// let insert_row = InsertOrUpdateRow {
///    columns_data: columns_buffer
/// };
/// 
/// table.insert_row(insert_row).await;
/// ```
/// #### Build in-memory file indexes
/// ```rust
/// table.rebuild_in_memory_indexes();
/// ```
/// 
/// #### Retrieve a row
/// ```rust
/// let row_by_id = table.first_or_default_by_id(3)?.unwrap();
/// 
/// // Read columns
/// for column in Column::from_buffer(&row2.columns_data).unwrap().iter() {
///     println!("{}", column.into_value());
/// }
/// 
/// // Column index, value that must be equal
/// let row_by_column_value = table.first_or_default_by_column(2, "This is awesome").unwrap().unwrap();
/// 
/// //Rasterized Query Language (Alpha)
/// let query_evaluation = parse_rql(&format!(r#"
///     BEGIN
///     SELECT FROM NAME_DOESNT_MATTER_FOR_NOW
///     WHERE COL(2) = 'This is awesome'
///     END
/// "#)).unwrap();
/// 
/// // Uses cache: If the same query is repeated, the time to retrieve a row should be in the single-digit range.
/// let row_by_query = table.first_or_default_by_query(query_evaluation).await.unwrap().unwrap();
/// ```
/// #### Update a row
/// ```rust
/// let update_row = InsertOrUpdateRow {
///     columns_data: columns_buffer_update // Same as insert_row
/// };
/// 
/// table.update_row_by_id(3, update_row).await;
/// ```
/// 
/// #### Delete a row
/// ```rust
/// // Invalidates cache automatically
/// table.delete_row_by_id(10).unwrap();
/// ```
/// 
/// #### Table Maintanance
/// ```rust
/// // Vacuum the table
/// table.vacuum_table().await;
/// 
/// // Must rebuild in-memory file indexes after vacuum
/// table.rebuild_in_memory_indexes();
/// ```
pub mod core;

pub mod simds;
pub mod rql;
pub(crate) mod instructions;