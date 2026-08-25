//! rdb_studio's guts, as a library.
//!
//! The binary is a thin `main` over this. Splitting them is what lets the
//! dialect layer be tested against real servers: an integration test can drive
//! `dialect` and `conn` directly, which a binary-only crate cannot expose.

pub mod app;
pub mod conn;
pub mod dialect;
pub mod export;
pub mod files;
pub mod model;
pub mod profiles;
pub mod sqlfmt;
pub mod theme;
pub mod types;
pub mod views;
