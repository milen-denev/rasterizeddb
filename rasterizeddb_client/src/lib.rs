//! A client SDK and ORM for RasterizedDB's native protocol (rastcp) that does
//! not compile the engine.
//!
//! [`rasterizeddb_core::client::DbClient`] lives inside the server crate and
//! pulls the whole engine in with it. This crate is the same conversation with
//! only the transport underneath: a statement goes out as text, and the answer
//! comes back as a row count, rows, or the server's error message.
//!
//! Three layers, each usable alone:
//!
//! * **SQL**: [`Client::query`]/[`Client::execute`] with `$n` parameters, and
//!   typed reads with [`Row::get`], [`Client::fetch_all`] and the rest.
//! * **Administration**: [`Client::admin`] turns the server's own statements
//!   (stores, placement, indexes, partitions, roles, backups, configuration,
//!   the key/value cache) into typed calls. See [`admin`].
//! * **ORM**: `#[derive(Model)]` maps a struct to a table, with typed column
//!   constants, a query builder, inserts/upserts/updates/deletes that read
//!   back what was stored, DDL and migrations. See [`orm`].
//!
//! ```no_run
//! # async fn demo() -> Result<(), rasterizeddb_client::Error> {
//! use rasterizeddb_client::{Client, Options, Param, params};
//!
//! let client = Client::connect("unix:/run/oxyredox/rdb.sock", Options::default()).await?;
//! client
//!     .execute(
//!         "INSERT INTO items (domain, key, body) VALUES ($1, $2, $3)",
//!         &[Param::Text("example.com"), Param::I64(-7), Param::Bytes(b"\x00\xff")],
//!     )
//!     .await?;
//! let items: Vec<(i64, Option<Vec<u8>>)> = client
//!     .fetch_all("SELECT key, body FROM items WHERE domain = $1", params!["example.com"])
//!     .await?;
//! # let _ = items;
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
//! # One statement at a time, and no transactions
//!
//! Text holding a second statement is refused before it is sent: the native
//! protocol runs only the first statement of an `INSERT …; INSERT …` string
//! and silently drops the rest. Transaction control is refused too: the native
//! protocol has none, and every statement commits on its own. Put related
//! writes in one statement (a multi-row `INSERT`, `INSERT … ON CONFLICT`, an
//! `UPDATE … WHERE`), or use the pgwire listener for a transaction block.
//!
//! # Results are positional
//!
//! The native protocol sends no column names. Column `i` of a row is item `i`
//! of the statement's select list (or `RETURNING` list), so read rows by the
//! position your SQL put each value in. The ORM writes its select lists itself,
//! so its models read correctly by construction.
//!
//! # Types
//!
//! See [`FromSql`] for what each column type arrives as. Two need a cast to be
//! read exactly: `jsonb` and array columns arrive in the engine's binary
//! forms, and a `NUMERIC(p, s)` column without its scale; select them as
//! `col::text`, which reads back as [`Json`], `Vec<T>` or [`Numeric`]. Reading
//! them directly is an error naming that fix, never a wrong value.

// The derive macros name this crate by its path; this lets its own tests use
// them.
extern crate self as rasterizeddb_client;

mod client;
mod error;
mod from_sql;
mod login;
mod params;
mod row;
mod to_sql;
mod types;
mod value;

pub mod admin;
pub mod kv;
pub mod orm;

pub use client::{Client, Options};
pub use error::Error;
pub use from_sql::{FromSql, FromSqlOwned};
pub use login::Login;
pub use params::{
    ArrayRef, Param, check_statement, quote_bytes, quote_ident, quote_text, render, render_dyn,
};
pub use row::{FromRow, Row, Rows, tag};
pub use to_sql::ToSql;
pub use types::{Date, Interval, Json, Numeric, Time, Timestamp, Uuid};
pub use value::Value;

#[cfg(feature = "derive")]
pub use rasterizeddb_client_derive::{FromRow, Model, SqlEnum};

/// Everything an application usually needs: `use rasterizeddb_client::prelude::*;`.
pub mod prelude {
    pub use crate::kv::{KvKey, KvValue};
    pub use crate::orm::{Expr, ModelExt, ModelQuery};
    pub use crate::{
        Client, Date, Error, FromRow, FromSql, Interval, Json, Numeric, Options, Param, Result,
        Row, Rows, Time, Timestamp, ToSql, Uuid, Value, params,
    };
    #[cfg(feature = "derive")]
    pub use crate::{Model, SqlEnum};
}

/// `Result` with this crate's [`Error`].
pub type Result<T> = std::result::Result<T, Error>;

/// What the derive macros' generated code calls. Not a stable API.
#[doc(hidden)]
pub mod __private {
    pub use crate::params::{push_ident, push_param};
    pub use crate::row::check_width;

    #[cfg(feature = "serde_json")]
    pub fn json_param<T: serde::Serialize + ?Sized>(
        value: &T,
        out: &mut String,
    ) -> crate::Result<()> {
        let text = serde_json::to_string(value)
            .map_err(|e| crate::Error::Param(format!("a JSON field did not serialize: {e}")))?;
        push_param(out, &crate::Param::Json(&text))
    }

    pub use crate::kv::push_text_form;

    #[cfg(feature = "serde_json")]
    pub fn json_text<T: serde::Serialize + ?Sized>(
        value: &T,
        out: &mut Vec<u8>,
    ) -> crate::Result<bool> {
        serde_json::to_writer(&mut *out, value)
            .map_err(|e| crate::Error::Param(format!("a JSON field did not serialize: {e}")))?;
        Ok(true)
    }

    #[cfg(feature = "serde_json")]
    pub fn json_from_row<T: serde::de::DeserializeOwned>(
        row: &crate::Row<'_>,
        i: usize,
    ) -> crate::Result<T> {
        let json: Option<crate::Json> = row.get(i)?;
        let text = json.as_ref().map_or("null", |j| j.as_str());
        serde_json::from_str(text).map_err(|e| crate::Error::Column {
            index: i,
            message: format!("the JSON did not deserialize: {e}"),
        })
    }
}
