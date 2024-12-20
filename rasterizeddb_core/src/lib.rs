use ahash::RandomState;
use moka::sync::Cache;
use once_cell::sync::Lazy;

pub(crate) const HEADER_SIZE: u64 = 30;
pub(crate) const CHUNK_SIZE: u64 = 1_000_000;

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
/// #### Create a static TABLE
/// ```rust
/// static TABLE: LazyLock<Arc<tokio::sync::RwLock<Table>>> = LazyLock::new(|| 
///     Arc::new(RwLock::const_new(Table::init("Some\\Folder", false).unwrap()))
/// );
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
/// let table_clone = TABLE.clone();
/// let mut mutable_table = table_clone.write().await;
/// 
/// // Can be combined with tokio::Spawn as well
/// mutable_table.insert_row(&mut InsertRow {
///     columns_data: columns_buffer
/// }).await.unwrap();
/// ```
/// #### Build in-memory file indexes
/// ```rust
/// mutable_table.rebuild_in_memory_indexes();
/// ```
/// 
/// #### Retrieve a row
/// ```rust
/// let row_by_id = mutable_table.first_or_default_by_id(10).unwrap().unwrap();
/// 
/// // Read columns
/// for column in Column::from_buffer(&row2.columns_data).unwrap().iter() {
///     println!("{}", column.into_value());
/// }
/// 
/// // Column index, value that must be equal
/// let row_by_column_value = mutable_table.first_or_default_by_column(2, "This is awesome").unwrap().unwrap();
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
/// let row_by_query = mutable_table.first_or_default_by_query(query_evaluation).await.unwrap().unwrap();
/// ```
/// 
/// #### Delete a row
/// ```rust
/// // Invalidates cache automatically
/// mutable_table.delete_row_by_id(10).unwrap();
/// ```
pub mod core;

pub mod rql;
pub(crate) mod instructions;