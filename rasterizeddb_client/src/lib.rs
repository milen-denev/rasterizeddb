#![warn(missing_docs)]
#![cfg_attr(docsrs, feature(doc_cfg))]
//! A client SDK and ORM for [RasterizedDB]'s native protocol (rastcp) that
//! does not compile the engine.
//!
//! [RasterizedDB]: https://github.com/milen-denev/rasterizeddb
//!
//! Four layers, each usable on its own:
//!
//! | layer | what | start at |
//! |---|---|---|
//! | **SQL** | statements with `$n` parameters, typed rows | [`Client`], [`params!`], [`FromRow`] |
//! | **Administration** | stores, placement, indexes, partitions, roles, backups, configuration | [`Client::admin`], [`admin`] |
//! | **ORM** | `#[derive(Model)]`, a query builder, migrations run on connect | [`orm`], [`Model`](derive@Model) |
//! | **Key/value** | the server's cache, typed: maps, closure queries, models as hashes | [`Client::kv`], [`kv`] |
//!
//! ```no_run
//! use rasterizeddb_client::prelude::*;
//!
//! #[derive(Model, Debug)]
//! struct Note {
//!     #[rdb(primary_key, serial)]
//!     id: i64,
//!     body: String,
//!     pinned: bool,
//! }
//!
//! # async fn demo() -> rasterizeddb_client::Result<()> {
//! let client = Client::connect("unix:/run/rdb/rdb.sock", Options::default()).await?;
//! client.create_table::<Note>().await?;
//!
//! let note = client.insert(&Note { id: 0, body: "hello".into(), pinned: false }).await?;
//! let pinned = Note::query().filter(Note::pinned.eq(true)).all(&client).await?;
//!
//! // Plain SQL, typed rows:
//! let rows: Vec<(i64, String)> = client
//!     .fetch_all("SELECT id, body FROM note WHERE id >= $1", params![note.id])
//!     .await?;
//! # let _ = (pinned, rows); Ok(()) }
//! ```
//!
//! # The guide
//!
//! | chapter | |
//! |---|---|
//! | [Getting started](guide::getting_started) | connecting, sharing a client, what the native protocol cannot do |
//! | [Types](guide::types) | the Rust ⇄ SQL mapping, and the three types that need `::text` |
//! | [Queries](guide::queries) | the calls, rows, atomic writes, errors |
//! | [Transactions](guide::transactions) | `begin`/`commit`, the retrying closure, savepoints, lost connections |
//! | [The ORM](guide::orm) | `#[derive(Model)]`, CRUD, the query builder |
//! | [Relations](guide::relations) | belongs-to, has-one, has-many, many-to-many; eager and lazy loading |
//! | [Migrations](guide::migrations) | steps, scenarios, model sync, the `auto_migrate` knob |
//! | [Key/value](guide::kv) | typed values, maps, queries, models as hashes |
//! | [Administration](guide::admin) | stores, indexes, partitions, roles, backups |
//!
//! # Features
//!
//! | feature | default | adds |
//! |---|---|---|
//! | `derive` | yes | `#[derive(Model)]`, `#[derive(FromRow)]`, `#[derive(SqlEnum)]` |
//! | `chrono` | | `chrono`'s date and time types as parameters and results |
//! | `uuid` | | `uuid::Uuid` as a parameter and a result |
//! | `serde_json` | | `serde_json::Value` results, `#[rdb(json)]` fields, [`kv::KvJson`] |
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
//! # One statement at a time; transactions through `begin`
//!
//! Text holding a second statement is refused before it is sent: the server
//! runs one statement per request. A statement sent on a [`Client`] commits on
//! its own. Statements that must commit together run in a [`Transaction`]
//! ([`Client::begin`], [`Client::transaction`]), on a connection of its own;
//! `BEGIN`, `COMMIT` and the rest are never sent as statements, and are
//! refused if written as one.
//!
//! # Results are positional
//!
//! The native protocol sends no column names. Column `i` of a row is item `i`
//! of the statement's select list (or `RETURNING` list), so read rows by the
//! position your SQL put each value in. The ORM writes its select lists itself,
//! so its models read correctly by construction.
//!
//! A program that shows results it did not write the statement for (a console,
//! an export) uses [`Client::simple_query`] instead: the answer pgwire would
//! give, with column names and every cell as PostgreSQL's text, and several
//! statements per call. See [`simple`]. A console's editor, where a `BEGIN`
//! has to outlast the call that sent it, uses a [`SimpleSession`]
//! ([`Client::simple_session`]); a long statement is stopped with
//! [`Client::cancel`] and the [`CancelToken`] it was sent with.

// The derive macros name this crate by its path; this lets its own tests use
// them.
extern crate self as rasterizeddb_client;

mod client;
mod error;
mod executor;
mod from_sql;
mod login;
mod params;
mod row;
mod scram;
mod simple_session;
mod to_sql;
mod transaction;
mod types;
mod value;

pub mod admin;
pub mod kv;
pub mod orm;
pub mod simple;

/// The guide: how to use this crate, chapter by chapter. Documentation only;
/// every example in it is compiled by `cargo test`.
pub mod guide {
    #[doc = include_str!("../docs/getting_started.md")]
    pub mod getting_started {}
    #[doc = include_str!("../docs/types.md")]
    pub mod types {}
    #[doc = include_str!("../docs/queries.md")]
    pub mod queries {}
    #[doc = include_str!("../docs/transactions.md")]
    pub mod transactions {}
    #[doc = include_str!("../docs/orm.md")]
    pub mod orm {}
    #[doc = include_str!("../docs/relations.md")]
    pub mod relations {}
    #[doc = include_str!("../docs/migrations.md")]
    pub mod migrations {}
    #[doc = include_str!("../docs/kv.md")]
    pub mod kv {}
    #[doc = include_str!("../docs/admin.md")]
    pub mod admin {}
}

pub use client::{Client, Options};
pub use error::Error;
pub use executor::Executor;
pub use from_sql::{FromSql, FromSqlOwned};
pub use login::Login;
pub use params::{
    ArrayRef, Param, check_statement, quote_bytes, quote_ident, quote_text, render, render_dyn,
};
pub use row::{FromRow, Row, Rows, tag};
pub use simple::{
    CancelToken, SimpleAnswer, SimpleColumn, SimpleOptions, SimpleResult, TransactionStatus,
};
pub use simple_session::SimpleSession;
/// How a TCP connection decides whether to believe the server
/// ([`Options::trust`]).
pub use rastcp::tls::Trust;

/// A private certificate authority's certificate(s), read from a PEM file,
/// as the [`Trust`] that believes what it signed.
pub fn load_ca(path: &str) -> Result<Trust> {
    Ok(Trust::RootCa(rastcp::tls::load_certificates(path)?))
}
pub use to_sql::ToSql;
pub use transaction::{
    DEFAULT_TRANSACTION_RETRIES, Isolation, SERVER_IDLE_CLOSE, Transaction, TransactionFuture,
    TransactionOptions,
};
pub use types::{Date, Interval, Json, Numeric, Time, Timestamp, Uuid};
pub use value::Value;

#[cfg(feature = "derive")]
pub use rasterizeddb_client_derive::{FromRow, Model, SqlEnum};

/// Everything an application usually needs: `use rasterizeddb_client::prelude::*;`.
pub mod prelude {
    pub use crate::kv::{KvKey, KvValue};
    pub use crate::orm::{Expr, ModelExt, ModelQuery, ModelRelations, count_rows};
    pub use crate::{
        Client, Date, Error, Executor, FromRow, FromSql, Interval, Isolation, Json, Numeric,
        Options, Param, Result, Row, Rows, Time, Timestamp, ToSql, Transaction, TransactionOptions,
        Uuid, Value, params,
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
