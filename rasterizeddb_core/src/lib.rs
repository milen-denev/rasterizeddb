#[cfg(feature = "enable_index_caching")]
use ahash::RandomState;

#[cfg(feature = "enable_index_caching")]
use moka::sync::Cache;

#[cfg(feature = "enable_index_caching")]
use once_cell::sync::Lazy;

pub(crate) const SERVER_PORT: u16 = 61170;
pub(crate) const HEADER_SIZE: u16 = 39;
pub(crate) const CHUNK_SIZE: u32 = 4 * 1_000_000;

#[cfg(feature = "enable_parallelism")]
pub(crate) const THREADS: usize = 16;

pub(crate) const MAX_PERMITS: usize = 16;

// Number of row pointers to fetch at once in next_row_pointers
pub(crate) const BATCH_SIZE: usize = 1024 * 4;

pub(crate) const IMMEDIATE_WRITE: bool = true;

pub const EMPTY_BUFFER: [u8; 8] = [0, 0, 0, 0, 0, 0, 0, 0];

pub(crate) const WRITE_BATCH_SIZE: usize = 4 * 1024 * 1024; // 4MB
pub(crate) const WRITE_SLEEP_DURATION: tokio::time::Duration = tokio::time::Duration::from_millis(10);

#[cfg(feature = "enable_index_caching")]
pub(crate) static POSITIONS_CACHE: Lazy<Cache<u64, Vec<(u64, u32)>, RandomState>> =
    Lazy::new(|| {
        let cache = Cache::builder()
            .max_capacity(u64::MAX)
            .build_with_hasher(ahash::RandomState::new());

        return cache;
    });

/// # Rasterized DB (Alpha)
/// ## A new schemaless high-performance database written in Rust from scratch.
///
///// #### Features:
/// `enable_index_caching` to enable query caching
/// `enable_parallelism` to enable in parallel table chunk scanning
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
/// let c1 = Column::new(10).unwrap();
/// let c2 = Column::new(-10).unwrap();
/// let str = 'A'.to_string().repeat(100);
/// let c3 = Column::new(str).unwrap();
/// 
/// let mut columns_buffer: Vec<u8> = Vec::with_capacity(
///     c1.len() +
///     c2.len() +
///     c3.len()
/// );
/// 
/// columns_buffer.append(&mut c1.content.to_vec());
/// columns_buffer.append(&mut c2.content.to_vec());
/// columns_buffer.append(&mut c3.content.to_vec());
/// 
/// let insert_row = InsertOrUpdateRow {
///    columns_data: columns_buffer.clone()
/// };
/// 
/// table.insert_row(insert_row).await;
/// //OR unsafe
/// table.insert_row_unsync(insert_row).await;
/// ```
/// #### Build in-memory file indexes
/// ```rust
/// table.rebuild_in_memory_indexes();
/// ```
///
/// #### Retrieve a row
/// ```rust
/// //Rasterized Query Language (Alpha)
/// let query_evaluation = parse_rql(&format!(r#"
///     BEGIN
///     SELECT FROM NAME_DOESNT_MATTER_FOR_NOW
///     WHERE COL(1) = -10
///     LIMIT 2
///     END
/// "#)).unwrap();
///
/// let rows = table.execute_query(query_evaluation.parser_result).await?;
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

pub mod instructions;
pub mod memory_pool;
pub mod rql;
pub mod simds;
pub mod client;
pub mod configuration;
pub mod renderers;
pub mod cache;