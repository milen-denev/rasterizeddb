use std::sync::OnceLock;

pub(crate) const SERVER_PORT: u16 = 61170;

pub static MAX_PERMITS_THREADS: OnceLock<usize> = OnceLock::new(); // 16 by default

// Number of row pointers to fetch at once in next_row_pointers
pub static BATCH_SIZE: OnceLock<usize> = OnceLock::new(); // 64K by default

pub(crate) const IMMEDIATE_WRITE: bool = false;

pub mod core;

pub mod cache;
pub mod client;
pub mod configuration;
pub mod instructions;
pub mod memory_pool;
pub mod renderers;
pub mod simds;
