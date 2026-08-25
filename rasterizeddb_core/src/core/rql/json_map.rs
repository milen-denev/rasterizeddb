//! JSON columns, mapped onto native tables.
//!
//! A `JSON`/`JSONB` column is not stored as a document. Each one is backed by a
//! real table whose columns are inferred from the data, and the column itself
//! holds a `UUID` referencing the row in that table. A document therefore
//! becomes ordinary typed columns that the query engine, the comparison
//! evaluator and the filter index all understand — rather than an opaque blob
//! that none of them can see into.
//!
//! # Inference
//!
//! A value's type is decided by what it is, in this order:
//!
//! | JSON value                         | column type |
//! |------------------------------------|-------------|
//! | string parsing as a date or time   | `TIMESTAMPTZ` |
//! | string parsing as an IP or network | `INET`      |
//! | any other string                   | `TEXT`      |
//! | integer                            | `BIGINT` (unsigned range) |
//! | number with a fractional part      | `DOUBLE PRECISION` |
//! | boolean                            | `BOOL`      |
//! | array                              | `TEXT[]`    |
//! | object                             | a nested table, referenced by `UUID` |
//! | null                               | leaves the column `NULL` |
//!
//! Order matters: every date and every address is also a string, so the
//! narrower reading is tried first.
//!
//! # Auto-migration
//!
//! A key that has no column yet gets one, so a document with new fields extends
//! the table rather than being rejected. A key whose type no longer fits widens
//! the column — an integer column meeting a fractional number becomes a double,
//! and anything meeting an incompatible type becomes text — because refusing
//! the write would make the column's type depend on whichever document happened
//! to arrive first.

use serde_json::Value;

use crate::core::db_type::DbType;
use crate::core::value_codec;

/// Prefix marking a table as the engine's rather than the user's.
pub const SHADOW_TABLE_PREFIX: &str = "__json_";

/// Longest a generated table name may be.
const MAX_NAME_LEN: usize = 100;

/// Nesting depth beyond which an object is stored as text rather than a further
/// table. Bounds both the table count and the reconstruction recursion.
pub const MAX_NESTING_DEPTH: usize = 8;

/// The table backing `column` of `parent`.
///
/// The readable part makes the table recognisable; the hash makes the name
/// unique, so two columns that would otherwise collide after the name is
/// filtered and truncated still get separate tables.
pub fn shadow_table_name(parent: &str, column: &str) -> String {
    let digest = short_hash(&format!("{}.{}", parent, column));
    let readable: String = format!("{}_{}", parent, column)
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '_')
        .collect();

    let budget = MAX_NAME_LEN
        .saturating_sub(SHADOW_TABLE_PREFIX.len())
        .saturating_sub(digest.len() + 1);
    let readable: String = readable.chars().take(budget).collect();

    format!("{}{}_{}", SHADOW_TABLE_PREFIX, readable, digest)
}

/// True for a table the engine created to back a JSON column.
pub fn is_shadow_table(name: &str) -> bool {
    name.starts_with(SHADOW_TABLE_PREFIX)
}

/// A short, stable hex digest. Not cryptographic — it only has to make names
/// unique, and it must not change between runs.
fn short_hash(text: &str) -> String {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in text.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{:08x}", (hash ^ (hash >> 32)) as u32)
}

/// Column name for a JSON key.
///
/// Keys are arbitrary text while column names are not, so a key that does not
/// survive filtering keeps a hash to stay distinct from its neighbours.
pub fn column_name_for_key(key: &str) -> String {
    let filtered: String = key
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '_' { c } else { '_' })
        .collect();

    let usable = !filtered.is_empty()
        && filtered != key.chars().map(|_| '_').collect::<String>()
        && filtered.eq_ignore_ascii_case(key);

    if usable && filtered.len() <= 64 && !crate::core::rql::nullable::is_hidden(&filtered) {
        filtered
    } else {
        let truncated: String = filtered.chars().take(50).collect();
        format!("{}_{}", truncated, short_hash(key))
    }
}

/// The column for a key whose ordinary column name is already taken by a key
/// that differs from it only in case.
///
/// JSON keys are case-sensitive and column names here are not, so `date` and
/// `Date` would be one column — and a document holding both would write one
/// over the other, while the `INSERT` built for the second spelling named a
/// column the parser could not find. The second arrival takes the same
/// hashed form a key that cannot be a column name at all already takes, so
/// the two stay distinct and every row round-trips. Which spelling is the
/// plain one is whichever arrived first, which is stable for a given table.
pub fn column_name_for_case_collision(column: &str) -> String {
    let truncated: String = column.chars().take(50).collect();
    format!("{}_{}", truncated, short_hash(column))
}

/// The column type a JSON value maps to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Inferred {
    /// A scalar, with the SQL type and the literal to write.
    Scalar { db_type: DbType, literal: String },
    /// An object, which becomes a table of its own.
    Nested,
    /// A JSON `null`: the column stays whatever it is, and the value is absent.
    Null,
}

/// Decides what a JSON value is.
pub fn infer(value: &Value) -> Inferred {
    match value {
        Value::Null => Inferred::Null,
        Value::Bool(b) => Inferred::Scalar {
            db_type: DbType::BOOL,
            literal: if *b { "1" } else { "0" }.to_string(),
        },
        Value::Number(n) => {
            // An integer stays exact; only a fractional value becomes a double.
            if let Some(u) = n.as_u64() {
                Inferred::Scalar { db_type: DbType::U64, literal: u.to_string() }
            } else if let Some(i) = n.as_i64() {
                // Negative integers do not fit the unsigned column, so they
                // take the signed one rather than being silently wrapped.
                Inferred::Scalar { db_type: DbType::I64, literal: i.to_string() }
            } else {
                let f = n.as_f64().unwrap_or(0.0);
                Inferred::Scalar { db_type: DbType::F64, literal: f.to_string() }
            }
        }
        Value::String(s) => {
            // Every date and every address is also a string, so the narrower
            // readings are tried first.
            if looks_like_timestamp(s) {
                return Inferred::Scalar {
                    db_type: DbType::DATETIME,
                    literal: value_codec::quote_sql(s),
                };
            }
            if value_codec::parse_inet(s).is_some() && looks_like_address(s) {
                return Inferred::Scalar {
                    db_type: DbType::INET,
                    literal: value_codec::quote_sql(s),
                };
            }
            Inferred::Scalar { db_type: DbType::STRING, literal: value_codec::quote_sql(s) }
        }
        Value::Array(items) => Inferred::Scalar {
            db_type: DbType::ARRAY,
            literal: value_codec::quote_sql(&array_literal(items)),
        },
        Value::Object(_) => Inferred::Nested,
    }
}

/// Whether a string is a timestamp rather than merely text.
///
/// Requires the shape of a date — digits separated by `-` — so that a bare
/// number, which `parse_timestamp_us` accepts as an epoch, is not mistaken for
/// one.
fn looks_like_timestamp(text: &str) -> bool {
    let trimmed = text.trim();
    if trimmed.len() < 8 {
        return false;
    }
    let date_part = trimmed.split(['T', 't', ' ']).next().unwrap_or("");
    let pieces: Vec<&str> = date_part.split('-').collect();
    if pieces.len() != 3 || !pieces.iter().all(|p| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()))
    {
        return false;
    }
    value_codec::parse_timestamp_us(trimmed).is_some()
}

/// Whether a string is an address rather than text that happens to parse.
///
/// A bare integer is a valid IPv4 address to some parsers; requiring a `.` or
/// `:` keeps `"42"` text.
fn looks_like_address(text: &str) -> bool {
    let trimmed = text.trim();
    trimmed.contains('.') || trimmed.contains(':')
}

/// Renders a JSON array as the `{a,b}` literal an array column takes.
fn array_literal(items: &[Value]) -> String {
    let mut parts: Vec<String> = Vec::with_capacity(items.len());
    for item in items {
        let text = match item {
            Value::Null => {
                parts.push("NULL".to_string());
                continue;
            }
            Value::String(s) => s.clone(),
            // A nested structure inside an array keeps its JSON form, since an
            // array column holds one element type.
            other => other.to_string(),
        };
        let needs_quotes = text.is_empty()
            || text.eq_ignore_ascii_case("NULL")
            || text.contains([',', '{', '}', '"', '\\', ' ']);
        if needs_quotes {
            parts.push(format!("\"{}\"", text.replace('\\', "\\\\").replace('"', "\\\"")));
        } else {
            parts.push(text);
        }
    }
    format!("{{{}}}", parts.join(","))
}

/// The SQL type name for a column of `db_type`, for generated DDL.
pub fn sql_type_name(db_type: &DbType) -> &'static str {
    match db_type {
        DbType::BOOL => "BOOL",
        DbType::U64 => "UBIGINT",
        DbType::I64 => "BIGINT",
        DbType::F64 => "DOUBLE PRECISION",
        DbType::DATETIME => "TIMESTAMPTZ",
        DbType::INET => "INET",
        DbType::ARRAY => "TEXT[]",
        DbType::UUID => "UUID",
        DbType::DECIMAL => "NUMERIC",
        _ => "TEXT",
    }
}

/// The type both `current` and `incoming` fit in.
///
/// Widening rather than refusing is what stops a column's type depending on
/// whichever document happened to arrive first.
pub fn widen(current: &DbType, incoming: &DbType) -> Option<DbType> {
    if current == incoming {
        return None;
    }
    Some(match (current, incoming) {
        // Integers meeting a fractional number, or each other.
        (DbType::U64, DbType::I64) | (DbType::I64, DbType::U64) => DbType::I64,
        (DbType::U64 | DbType::I64, DbType::F64) => DbType::F64,
        (DbType::F64, DbType::U64 | DbType::I64) => return None,
        // Anything else disagreeing becomes text, which every value can be
        // written as.
        _ => DbType::STRING,
    })
}

/// Re-renders a literal for a widened column.
pub fn reliteral(literal: &str, to: &DbType) -> String {
    match to {
        DbType::STRING => {
            if literal.starts_with('\'') {
                literal.to_string()
            } else {
                value_codec::quote_sql(literal)
            }
        }
        _ => literal.to_string(),
    }
}

// --------------------------------------------------- registered columns

/// A column declared `JSON`/`JSONB`, and the table backing it.
///
/// Recorded so a later statement knows the column's `UUID` is a reference to
/// reconstruct from rather than an identifier to return.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsonColumn {
    pub column: String,
    pub shadow_table: String,
    pub is_deleted: bool,
}

/// Bytes one persisted record occupies: the column name, the table name, and a
/// tombstone.
pub const JSON_COLUMN_SIZE: u64 = 129 + 129 + 1;

impl JsonColumn {
    pub fn into_vec(&self) -> Vec<u8> {
        let mut out = vec![0u8; JSON_COLUMN_SIZE as usize];
        let write = |out: &mut [u8], at: usize, text: &str| {
            let bytes = text.as_bytes();
            let len = bytes.len().min(128);
            out[at..at + len].copy_from_slice(&bytes[..len]);
        };
        write(&mut out, 0, &self.column);
        write(&mut out, 129, &self.shadow_table);
        out[258] = u8::from(self.is_deleted);
        out
    }

    pub fn from_vec(raw: &[u8]) -> Result<Self, String> {
        if raw.len() != JSON_COLUMN_SIZE as usize {
            return Err(format!(
                "invalid JSON column record: expected {} bytes, got {}",
                JSON_COLUMN_SIZE,
                raw.len()
            ));
        }
        let read = |at: usize| -> Result<String, String> {
            let end = raw[at..at + 129].iter().position(|&b| b == 0).unwrap_or(128);
            std::str::from_utf8(&raw[at..at + end])
                .map(str::to_string)
                .map_err(|e| format!("invalid UTF-8 in JSON column record: {}", e))
        };
        Ok(JsonColumn {
            column: read(0)?,
            shadow_table: read(129)?,
            is_deleted: raw[258] != 0,
        })
    }
}

fn sidecar_name<S: crate::core::storage_providers::traits::StorageIO>(
    schema_io: &rclite::Arc<S>,
) -> String {
    format!("{}_{}", schema_io.get_name().replace(".db", ""), "JSON.db")
}

/// Reads a table's JSON column registrations.
pub async fn load<S: crate::core::storage_providers::traits::StorageIO>(
    schema_io: rclite::Arc<S>,
) -> smallvec::SmallVec<[JsonColumn; 4]> {
    let io = schema_io.create_new(sidecar_name(&schema_io)).await;
    let len = io.get_len().await;

    let mut out: smallvec::SmallVec<[JsonColumn; 4]> = smallvec::SmallVec::new();
    if len < JSON_COLUMN_SIZE {
        return out;
    }

    let mut buffer = vec![0u8; JSON_COLUMN_SIZE as usize];
    for i in 0..(len / JSON_COLUMN_SIZE) {
        let mut position = i * JSON_COLUMN_SIZE;
        if io.read_data_into_buffer(&mut position, &mut buffer).await.is_err() {
            break;
        }
        match JsonColumn::from_vec(&buffer) {
            Ok(entry) => {
                out.retain(|e| !e.column.eq_ignore_ascii_case(&entry.column));
                if !entry.is_deleted {
                    out.push(entry);
                }
            }
            Err(e) => log::error!("Skipping unreadable JSON column record: {}", e),
        }
    }

    out
}

/// Records a JSON column.
pub async fn append<S: crate::core::storage_providers::traits::StorageIO>(
    schema_io: rclite::Arc<S>,
    entry: &JsonColumn,
) -> std::io::Result<()> {
    let io = schema_io.create_new(sidecar_name(&schema_io)).await;
    io.append_data(&entry.into_vec(), true).await;
    Ok(())
}

/// The table backing `column`, if it is a JSON column.
pub fn shadow_for<'a>(column: &str, registered: &'a [JsonColumn]) -> Option<&'a str> {
    registered
        .iter()
        .find(|c| c.column.eq_ignore_ascii_case(column))
        .map(|c| c.shadow_table.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_registration_round_trips_through_its_bytes() {
        let entry = JsonColumn {
            column: "doc".to_string(),
            shadow_table: shadow_table_name("t", "doc"),
            is_deleted: false,
        };
        assert_eq!(JsonColumn::from_vec(&entry.into_vec()).unwrap(), entry);
    }

    fn scalar(value: Value) -> (DbType, String) {
        match infer(&value) {
            Inferred::Scalar { db_type, literal } => (db_type, literal),
            other => panic!("expected a scalar, got {:?}", other),
        }
    }

    #[test]
    fn integers_and_fractions_are_told_apart() {
        assert_eq!(scalar(json!(42)).0, DbType::U64);
        assert_eq!(scalar(json!(0)).0, DbType::U64);
        // A negative integer does not fit the unsigned column.
        assert_eq!(scalar(json!(-7)).0, DbType::I64);
        assert_eq!(scalar(json!(1.5)).0, DbType::F64);
        assert_eq!(scalar(json!(-0.25)).0, DbType::F64);
    }

    #[test]
    fn dates_are_recognised_before_text() {
        for text in [
            "2026-08-20",
            "2026-08-20 14:30:00",
            "2026-08-20T14:30:00Z",
            "2026-08-20T14:30:00+02:00",
        ] {
            assert_eq!(scalar(json!(text)).0, DbType::DATETIME, "{}", text);
        }
    }

    /// `parse_timestamp_us` accepts a bare integer as an epoch, so a numeric
    /// string would otherwise become a timestamp.
    #[test]
    fn a_numeric_string_is_text_not_a_date() {
        assert_eq!(scalar(json!("12345")).0, DbType::STRING);
        assert_eq!(scalar(json!("2026")).0, DbType::STRING);
        assert_eq!(scalar(json!("not a date")).0, DbType::STRING);
    }

    #[test]
    fn addresses_are_recognised_before_text() {
        assert_eq!(scalar(json!("10.0.0.1")).0, DbType::INET);
        assert_eq!(scalar(json!("192.168.1.0/24")).0, DbType::INET);
        assert_eq!(scalar(json!("2001:db8::1")).0, DbType::INET);
        // Without a dot or colon it is text, not an address.
        assert_eq!(scalar(json!("42")).0, DbType::STRING);
    }

    #[test]
    fn arrays_become_array_columns() {
        let (db_type, literal) = scalar(json!(["a", "b"]));
        assert_eq!(db_type, DbType::ARRAY);
        assert_eq!(literal, "'{a,b}'");
    }

    #[test]
    fn an_array_element_needing_quotes_gets_them() {
        let (_, literal) = scalar(json!(["has,comma", "", null]));
        assert_eq!(literal, "'{\"has,comma\",\"\",NULL}'");
    }

    #[test]
    fn booleans_and_nulls_and_objects_are_classified() {
        assert_eq!(scalar(json!(true)), (DbType::BOOL, "1".to_string()));
        assert_eq!(scalar(json!(false)), (DbType::BOOL, "0".to_string()));
        assert_eq!(infer(&json!(null)), Inferred::Null);
        assert_eq!(infer(&json!({"a": 1})), Inferred::Nested);
    }

    #[test]
    fn a_quote_in_a_string_cannot_escape_the_literal() {
        let (_, literal) = scalar(json!("o'brien"));
        assert_eq!(literal, "'o''brien'");
    }

    #[test]
    fn shadow_names_are_stable_distinct_and_marked() {
        let a = shadow_table_name("users", "profile");
        let b = shadow_table_name("users", "settings");
        assert_ne!(a, b);
        assert_eq!(a, shadow_table_name("users", "profile"), "must be stable across runs");
        assert!(is_shadow_table(&a));
        assert!(!is_shadow_table("users"));
        assert!(a.len() <= MAX_NAME_LEN);
    }

    /// Two columns whose readable parts collide after filtering must still get
    /// separate tables.
    #[test]
    fn names_that_would_collide_after_filtering_stay_distinct() {
        let a = shadow_table_name("t", "a.b");
        let b = shadow_table_name("t", "a_b");
        assert_ne!(a, b);
    }

    #[test]
    fn keys_become_usable_column_names() {
        assert_eq!(column_name_for_key("status"), "status");
        assert_eq!(column_name_for_key("user_id"), "user_id");
        // A key that cannot be used verbatim keeps a hash so it stays distinct.
        let dotted = column_name_for_key("a.b");
        let dashed = column_name_for_key("a-b");
        assert_ne!(dotted, dashed);
        assert!(dotted.starts_with("a_b_"));
        // The reserved prefix cannot be claimed through a key.
        assert_ne!(column_name_for_key("__null_x"), "__null_x");
    }

    #[test]
    fn widening_picks_a_type_both_values_fit() {
        assert_eq!(widen(&DbType::U64, &DbType::U64), None, "no change needed");
        assert_eq!(widen(&DbType::U64, &DbType::F64), Some(DbType::F64));
        assert_eq!(widen(&DbType::U64, &DbType::I64), Some(DbType::I64));
        // A double already holds an integer.
        assert_eq!(widen(&DbType::F64, &DbType::U64), None);
        // Anything genuinely incompatible becomes text.
        assert_eq!(widen(&DbType::U64, &DbType::STRING), Some(DbType::STRING));
        assert_eq!(widen(&DbType::DATETIME, &DbType::INET), Some(DbType::STRING));
    }

    #[test]
    fn a_widened_literal_is_re_rendered_for_its_new_type() {
        assert_eq!(reliteral("42", &DbType::STRING), "'42'");
        assert_eq!(reliteral("'x'", &DbType::STRING), "'x'", "already quoted");
        assert_eq!(reliteral("42", &DbType::F64), "42");
    }
}
