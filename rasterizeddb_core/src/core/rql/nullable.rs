//! Nullable columns.
//!
//! # Why a companion column rather than an inline flag byte
//!
//! A `NULL` has to be distinguishable from every value the column could hold,
//! so it needs a bit of storage somewhere. The obvious place is a flag byte
//! immediately before the value — but that changes the row's physical layout,
//! which means the three places that compute row widths, the fetch loop, and
//! the in-place-update path all have to agree about it.
//!
//! Instead, each nullable column `x` gets a companion `BOOL` column named
//! `__null_x`, holding 1 when `x` is `NULL`. It costs the same one byte per
//! nullable column, and it costs the storage engine nothing at all: a companion
//! is an ordinary fixed-width column, so writing, fetching and updating it use
//! the paths that already exist.
//!
//! A column is nullable exactly when its companion is present, so nullability
//! needs no new schema field and the on-disk schema format is unchanged.
//!
//! # Three-valued logic without a three-valued evaluator
//!
//! SQL says any comparison against `NULL` is *unknown*, and a row is returned
//! only when the `WHERE` clause is *true*. The evaluator here is two-valued, so
//! rather than teaching it a third state, the rewrite in
//! [`predicate_rewrite`](crate::core::tokenizer::predicate_rewrite) guards each
//! comparison against a nullable column:
//!
//! ```text
//! country = 'BG'        =>  (__null_country = 0 AND country = 'BG')
//! country IS NULL       =>  __null_country = 1
//! country IS NOT NULL   =>  __null_country = 0
//! ```
//!
//! That reproduces SQL's answers under `AND`, `OR` and negation alike, because
//! "unknown" and "false" differ only under a negation the guard has already
//! applied. It also keeps `WHERE country = ''` from matching a `NULL` row —
//! the collision that makes encoding `NULL` as an empty value wrong.

use smallvec::SmallVec;

use crate::core::db_type::DbType;
use crate::core::row::schema::SchemaField;

/// Prefix marking a column as engine-owned rather than user-declared.
pub const NULL_FLAG_PREFIX: &str = "__null_";

/// Name of the companion column tracking whether `column` is `NULL`.
pub fn flag_column_name(column: &str) -> String {
    format!("{}{}", NULL_FLAG_PREFIX, column)
}

/// True for a companion column, which must stay out of `SELECT *` and out of
/// anything else the client can see.
pub fn is_hidden(name: &str) -> bool {
    name.starts_with(NULL_FLAG_PREFIX)
}

/// The companion column for `column`, if that column is nullable.
pub fn flag_field<'a>(
    column: &str,
    schema: &'a SmallVec<[SchemaField; 20]>,
) -> Option<&'a SchemaField> {
    let name = flag_column_name(column);
    schema
        .iter()
        .find(|f| f.name.eq_ignore_ascii_case(&name) && !f.is_deleted)
}

/// Whether `column` may hold `NULL`.
pub fn is_nullable(column: &str, schema: &SmallVec<[SchemaField; 20]>) -> bool {
    flag_field(column, schema).is_some()
}

/// The user-visible columns of a schema, in write order.
pub fn visible_fields(schema: &SmallVec<[SchemaField; 20]>) -> Vec<SchemaField> {
    let mut out: Vec<SchemaField> = schema
        .iter()
        .filter(|f| !is_hidden(&f.name) && !f.is_deleted)
        .cloned()
        .collect();
    out.sort_by_key(|f| f.write_order);
    out
}

/// The stored form of a flag: one byte, 1 for `NULL`.
pub fn flag_bytes(is_null: bool) -> [u8; 1] {
    [u8::from(is_null)]
}

/// Reads a flag column's stored byte.
pub fn flag_is_set(bytes: &[u8]) -> bool {
    bytes.first().copied().unwrap_or(0) != 0
}

/// The type a companion column is stored as.
pub const FLAG_TYPE: DbType = DbType::BOOL;

#[cfg(test)]
mod tests {
    use super::*;

    fn schema_with(names: &[(&str, DbType)]) -> SmallVec<[SchemaField; 20]> {
        let mut s: SmallVec<[SchemaField; 20]> = SmallVec::new();
        for (i, (name, ty)) in names.iter().enumerate() {
            s.push(SchemaField::new(
                name.to_string(),
                ty.clone(),
                ty.get_size(),
                0,
                i as u64,
                false,
            ));
        }
        s
    }

    #[test]
    fn a_column_is_nullable_exactly_when_its_companion_exists() {
        let s = schema_with(&[
            ("id", DbType::I32),
            ("country", DbType::STRING),
            ("__null_country", DbType::BOOL),
        ]);
        assert!(is_nullable("country", &s));
        assert!(!is_nullable("id", &s));
        // Resolution follows the same case-insensitive rule as column lookup.
        assert!(is_nullable("COUNTRY", &s));
    }

    #[test]
    fn companion_columns_are_hidden_from_the_client() {
        assert!(is_hidden("__null_country"));
        assert!(!is_hidden("country"));
        assert!(
            !is_hidden("null_country"),
            "only the reserved prefix counts"
        );

        let s = schema_with(&[
            ("id", DbType::I32),
            ("country", DbType::STRING),
            ("__null_country", DbType::BOOL),
        ]);
        let visible: Vec<String> = visible_fields(&s).into_iter().map(|f| f.name).collect();
        assert_eq!(visible, vec!["id", "country"]);
    }

    #[test]
    fn visible_fields_come_back_in_write_order() {
        let mut s = schema_with(&[("b", DbType::I32), ("a", DbType::I32)]);
        s[0].write_order = 5;
        s[1].write_order = 1;
        let visible: Vec<String> = visible_fields(&s).into_iter().map(|f| f.name).collect();
        assert_eq!(visible, vec!["a", "b"]);
    }

    #[test]
    fn flag_round_trips() {
        assert!(flag_is_set(&flag_bytes(true)));
        assert!(!flag_is_set(&flag_bytes(false)));
        // A missing or short value reads as "not null", which is the safe
        // default for a row written before the column existed.
        assert!(!flag_is_set(&[]));
    }
}
