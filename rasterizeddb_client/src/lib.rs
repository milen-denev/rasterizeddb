//! A client for RasterizedDB's native protocol (rastcp) that does not compile
//! the engine.
//!
//! [`rasterizeddb_core::client::DbClient`] lives inside the server crate and
//! pulls the whole engine in with it. This crate is the same conversation with
//! only the transport underneath: a statement goes out as text, and the answer
//! comes back as a row count, rows, or the server's error message.
//!
//! ```no_run
//! # async fn demo() -> Result<(), rasterizeddb_client::Error> {
//! use rasterizeddb_client::{Client, Options, Param};
//!
//! let client = Client::connect("unix:/run/oxyredox/rdb.sock", Options::default()).await?;
//! client
//!     .execute(
//!         "INSERT INTO items (domain, key, body) VALUES ($1, $2, $3)",
//!         &[Param::Text("example.com"), Param::I64(-7), Param::Bytes(b"\x00\xff")],
//!     )
//!     .await?;
//! let rows = client
//!     .query("SELECT key, body FROM items WHERE domain = $1", &[Param::Text("example.com")])
//!     .await?;
//! for row in rows.iter() {
//!     let key = row.get_i64(0)?;
//!     let body = row.get_opt_bytes(1)?;
//!     # let _ = (key, body);
//! }
//! # Ok(()) }
//! ```
//!
//! # Parameters are rendered, not bound
//!
//! The native protocol carries a statement's text and nothing else: there is
//! no Bind message to put a value beside it. `$1`…`$n` are therefore replaced
//! by literals on this side, by [`render`], under the rules the server's lexer
//! reads them by (PostgreSQL's, with `standard_conforming_strings` on). A
//! value can never become syntax: text is single-quoted with `'` doubled,
//! bytes are a `'\x…'` hex literal, a negative number is parenthesised so it
//! cannot follow a `-` and start a comment, and a placeholder inside a string,
//! a quoted identifier or a comment is left alone. The pgwire listener does
//! the same thing on its side for clients that do bind (`pg_params.rs`).
//!
//! # Results are positional
//!
//! The native protocol sends no column names. Column `i` of a row is item `i`
//! of the statement's select list (or `RETURNING` list), so read rows by the
//! position your SQL put each value in.

mod client;
mod error;
mod login;
mod params;
mod row;

pub use client::{Client, Options};
pub use error::Error;
pub use login::Login;
pub use params::{Param, quote_bytes, quote_text, render};
pub use row::{Row, Rows, tag};

/// `Result` with this crate's [`Error`].
pub type Result<T> = std::result::Result<T, Error>;
