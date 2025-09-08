pub(crate) const SERVER_PORT: u16 = 61170;

pub static mut MAX_PERMITS_THREADS: usize = 16;

// Number of row pointers to fetch at once in next_row_pointers
pub static BATCH_SIZE: usize = 1024 * 64;

pub(crate) const IMMEDIATE_WRITE: bool = true;

pub(crate) const WRITE_BATCH_SIZE: usize = 4 * 1024 * 1024; // 4MB
pub(crate) const WRITE_SLEEP_DURATION: tokio::time::Duration =
    tokio::time::Duration::from_millis(10);

pub mod core;

pub mod cache;
pub mod client;
pub mod configuration;
pub mod instructions;
pub mod memory_pool;
pub mod renderers;
pub mod simds;
