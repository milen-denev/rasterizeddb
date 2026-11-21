//! A learned point `SELECT`'s key, handed to the scan of its own table so the
//! index is asked with the statement's values directly rather than through
//! its `WHERE` text read again.
//!
//! A statement learned for its template (`core::rql::prepared`, S16b) renders
//! its `WHERE` from each execution's values, and the scan then tokenized that
//! text and extracted from the tokens the very equalities the template was
//! learned from: on a point query over a unique key, about a tenth of the
//! statement's cost for what the index answers in well under one percent.
//! The learned side binds the values once ([`PointKey::levels`]); a scan of
//! that table under that exact clause builds the predicate the text would
//! have given ([`crate::core::filter::sarg::MaskExpr`]) from them and asks the
//! same `TableIndexes::plan` the text path asks. The index chosen and the key
//! looked up are the text path's own code, not a second copy.
//!
//! What may differ is only whether the predicate built here is the one the
//! text gives, so the key is used only where that is so by construction:
//!
//! * the table it was bound for, by name, and the clause it was bound for,
//!   byte for byte: any other scan in the statement (a subquery's, a
//!   catalogue's) does not match it;
//! * every level a live column of that write order, name and type, so the
//!   column the clause names is the one the key reads (two tables can hold
//!   a column of one name at different write orders);
//! * every such type one whose literal converts to its stored bytes the same
//!   way whatever the value ([`key_type_allowed`]): the integers and plain
//!   text. Not `NUMERIC` (scaled), floats (`-0`, `NaN`), `character(n)`
//!   (trailing spaces), JSON, nor a column compared through a collation
//!   companion;
//! * the indexes built and not loading.
//!
//! Anything else is the text path, in the same call. The rows a key admits
//! are still each checked against the `WHERE`, so a key can only ever cost
//! rows, never add them, and the learned side compares both paths'
//! candidates on the executions that verify the template before it uses one.

use std::future::Future;
use std::sync::Arc;

use smallvec::SmallVec;

use crate::core::db_type::DbType;
use crate::core::row::schema::SchemaField;

/// One execution's key for a learned point `SELECT` over a unique index of
/// a plain table.
#[derive(Debug, Clone)]
pub struct PointKey {
    /// The table it was bound for (`TableSchema::name`).
    pub table: Arc<str>,
    /// The `WHERE` clause exactly as the scan is handed it: the key stands
    /// for this clause and no other.
    pub where_clause: Arc<str>,
    /// Per key column, in the index's key order: its write order, its type,
    /// and the stored bytes it equals in this execution.
    pub levels: SmallVec<[(u32, DbType, SmallVec<[u8; 16]>); 3]>,
    /// Per key column, its name, checked against the live field.
    pub names: SmallVec<[Arc<str>; 3]>,
}

tokio::task_local! {
    static POINT_KEY: Arc<PointKey>;
}

/// Runs `fut` with `key` as the point key of its scans. Not an `async fn`, so
/// `fut` is stored once rather than once as an argument and again where it
/// is awaited.
pub fn scope<F: Future>(key: PointKey, fut: F) -> impl Future<Output = F::Output> {
    POINT_KEY.scope(Arc::new(key), fut)
}

/// The point key in scope, if any.
#[inline]
pub(crate) fn current() -> Option<Arc<PointKey>> {
    POINT_KEY.try_with(Arc::clone).ok()
}

/// Scans whose candidates came from a point key rather than from their
/// clause's text. For tests and A/B runs: it is how one tells the key was
/// used at all.
pub static HITS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Whether a key column of this field may be answered from a bound key: one
/// whose literal becomes its stored bytes the same way whatever the value.
///
/// An allowlist. An integer the column's type holds is stored as its little
/// endian bytes, and a text literal as its bytes, under every value. Every
/// other type converts some values specially (a `NUMERIC` at its scale, a
/// float's `-0`, a `character(n)`'s trailing spaces, a document's
/// normalising), and a column with a collation companion is compared through
/// the companion, not its own bytes.
pub(crate) fn key_type_allowed(field: &SchemaField, fields: &[SchemaField]) -> bool {
    if field.is_deleted {
        return false;
    }
    match field.db_type {
        DbType::I8 | DbType::I16 | DbType::I32 | DbType::I64 | DbType::U8 | DbType::U16 | DbType::U32 | DbType::U64 => true,
        DbType::STRING => {
            !field.json_text
                && field.typmod_packed.is_none()
                && !crate::core::rql::collation::is_companion(&field.name)
                && crate::core::rql::collation::companion(&field.name, fields).is_none()
        }
        _ => false,
    }
}
