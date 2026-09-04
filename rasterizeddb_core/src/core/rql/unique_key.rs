//! Table-level `UNIQUE` constraints, including composite ones.
//!
//! # Why this exists alongside `SchemaField::is_unique`
//!
//! A single-column `UNIQUE` written inline on a column definition is recorded
//! on the field itself, and [`Table::check_unique`] enforces it. That flag has
//! nowhere to put a constraint spanning *several* columns: `UNIQUE (a, b)`
//! forbids repeating the pair while allowing either column to repeat on its
//! own, so setting the flag on both would reject rows the constraint permits —
//! a stricter database than the one that was asked for.
//!
//! So table-level constraints live in a sidecar of their own, the same shape as
//! the foreign-key sidecar next to it: a fixed-stride array of named records
//! with a tombstone byte, written once and re-read when the table is loaded.
//! That also gives `ALTER TABLE … ADD CONSTRAINT … UNIQUE (…)` somewhere to put
//! a constraint added after the table was created, which a schema field —
//! written at column-creation time — does not.
//!
//! # What a constraint means when a column is NULL
//!
//! PostgreSQL's default is `NULLS DISTINCT`: two rows whose key contains a
//! `NULL` never conflict, however many times the same combination appears. That
//! is what is implemented here, so a nullable column under a unique constraint
//! behaves the way an application expects.

use std::io;

use rclite::Arc;
use smallvec::SmallVec;

use crate::core::storage_providers::traits::StorageIO;

/// Columns one constraint may span.
///
/// A fixed stride is what lets the sidecar be read as an array, so the width
/// has to be decided up front. Eight is past anything a real composite key
/// uses, and the unused slots cost a kilobyte per constraint on disk and
/// nothing at all at runtime.
pub const MAX_COLUMNS: usize = 8;

/// Bytes one persisted constraint occupies: the constraint name, then eight
/// column-name slots, then the column count and the tombstone.
pub const UNIQUE_KEY_SIZE: u64 = 129 + 129 * MAX_COLUMNS as u64 + 2;

/// One table-level `UNIQUE` constraint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UniqueKey {
    pub name: String,
    /// The columns the constraint spans, in the order written. One column is
    /// an ordinary unique constraint; several make a composite one.
    pub columns: Vec<String>,
    pub is_deleted: bool,
}

fn write_name(out: &mut [u8], at: usize, name: &str) {
    let filtered: String = name
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '_')
        .collect();
    let bytes = filtered.as_bytes();
    let len = bytes.len().min(128);
    out[at..at + len].copy_from_slice(&bytes[..len]);
}

fn read_name(raw: &[u8], at: usize) -> Result<String, String> {
    let end = raw[at..at + 129]
        .iter()
        .position(|&b| b == 0)
        .unwrap_or(128);
    std::str::from_utf8(&raw[at..at + end])
        .map(str::to_string)
        .map_err(|e| format!("invalid UTF-8 in unique key record: {}", e))
}

impl UniqueKey {
    /// The name PostgreSQL would give an unnamed constraint: the table, its
    /// columns, and `key`.
    pub fn default_name(table: &str, columns: &[String]) -> String {
        let mut name = table.to_string();
        for column in columns {
            name.push('_');
            name.push_str(column);
        }
        name.push_str("_key");
        name
    }

    pub fn into_vec(&self) -> Vec<u8> {
        let mut out = vec![0u8; UNIQUE_KEY_SIZE as usize];
        write_name(&mut out, 0, &self.name);
        for (i, column) in self.columns.iter().take(MAX_COLUMNS).enumerate() {
            write_name(&mut out, 129 + i * 129, column);
        }
        let count_at = 129 + 129 * MAX_COLUMNS;
        out[count_at] = self.columns.len().min(MAX_COLUMNS) as u8;
        out[count_at + 1] = u8::from(self.is_deleted);
        out
    }

    pub fn from_vec(raw: &[u8]) -> Result<Self, String> {
        if raw.len() != UNIQUE_KEY_SIZE as usize {
            return Err(format!(
                "invalid unique key record: expected {} bytes, got {}",
                UNIQUE_KEY_SIZE,
                raw.len()
            ));
        }
        let count_at = 129 + 129 * MAX_COLUMNS;
        let count = (raw[count_at] as usize).min(MAX_COLUMNS);
        let mut columns = Vec::with_capacity(count);
        for i in 0..count {
            columns.push(read_name(raw, 129 + i * 129)?);
        }
        Ok(UniqueKey {
            name: read_name(raw, 0)?,
            columns,
            is_deleted: raw[count_at + 1] != 0,
        })
    }

    /// How the constraint reads in an error message.
    pub fn describe(&self) -> String {
        format!("{} ({})", self.name, self.columns.join(", "))
    }
}

/// Name of the sidecar holding a table's unique constraints.
fn sidecar_name<S: StorageIO>(schema_io: &Arc<S>) -> String {
    format!("{}_{}", schema_io.get_name().replace(".db", ""), "UK.db")
}

/// Reads a table's unique constraints.
pub async fn load<S: StorageIO>(schema_io: Arc<S>) -> SmallVec<[UniqueKey; 4]> {
    let io = schema_io.create_new(sidecar_name(&schema_io)).await;
    let len = io.get_len().await;

    let mut out: SmallVec<[UniqueKey; 4]> = SmallVec::new();
    if len < UNIQUE_KEY_SIZE {
        return out;
    }

    let count = len / UNIQUE_KEY_SIZE;
    let mut buffer = vec![0u8; UNIQUE_KEY_SIZE as usize];
    for i in 0..count {
        let mut position = i * UNIQUE_KEY_SIZE;
        if io
            .read_data_into_buffer(&mut position, &mut buffer)
            .await
            .is_err()
        {
            break;
        }
        match UniqueKey::from_vec(&buffer) {
            // A tombstoned record is kept on disk so the file keeps its stride.
            Ok(key) if !key.is_deleted => out.push(key),
            Ok(_) => {}
            Err(e) => log::error!("Skipping unreadable unique key record: {}", e),
        }
    }

    out
}

/// Appends a unique constraint.
pub async fn append<S: StorageIO>(schema_io: Arc<S>, key: &UniqueKey) -> io::Result<()> {
    let io = schema_io.create_new(sidecar_name(&schema_io)).await;
    io.append_data(&key.into_vec(), true).await;
    Ok(())
}

/// Marks a unique constraint deleted, by constraint name.
///
/// Returns whether one was found. The record keeps its slot so the file stays a
/// fixed-stride array.
pub async fn remove<S: StorageIO>(schema_io: Arc<S>, name: &str) -> io::Result<bool> {
    let io = schema_io.create_new(sidecar_name(&schema_io)).await;
    let len = io.get_len().await;
    if len < UNIQUE_KEY_SIZE {
        return Ok(false);
    }

    let count = len / UNIQUE_KEY_SIZE;
    let mut buffer = vec![0u8; UNIQUE_KEY_SIZE as usize];
    for i in 0..count {
        let position = i * UNIQUE_KEY_SIZE;
        let mut cursor = position;
        if io
            .read_data_into_buffer(&mut cursor, &mut buffer)
            .await
            .is_err()
        {
            break;
        }
        let Ok(mut key) = UniqueKey::from_vec(&buffer) else {
            continue;
        };
        if key.is_deleted || !key.name.eq_ignore_ascii_case(name) {
            continue;
        }
        key.is_deleted = true;
        io.write_data_seek(io::SeekFrom::Start(position), &key.into_vec())
            .await;
        return Ok(true);
    }

    Ok(false)
}

/// Parses a table-level `UNIQUE (…)` or `PRIMARY KEY (…)` clause, with or
/// without a leading `CONSTRAINT <name>`.
///
/// Returns `Ok(None)` when the clause is not one of those, so a caller can walk
/// a `CREATE TABLE` body and let anything else fall through. An error means the
/// clause *was* a unique constraint and could not be honoured — which a
/// migration needs to hear, because the alternative is an application trusting
/// a constraint that is not there.
pub fn parse_clause(clause: &str, table: &str) -> Result<Option<UniqueKey>, String> {
    let trimmed = clause.trim();
    let upper = trimmed.to_ascii_uppercase();

    // `CONSTRAINT name UNIQUE (…)` names the constraint first.
    let (name, rest) = if upper.starts_with("CONSTRAINT") {
        let mut words = trimmed.splitn(3, char::is_whitespace);
        words.next();
        let given = words
            .next()
            .map(|n| n.trim_matches('"').to_string())
            .filter(|n| !n.is_empty())
            .ok_or_else(|| "CONSTRAINT is missing its name".to_string())?;
        let rest = words.next().unwrap_or("").trim();
        (Some(given), rest)
    } else {
        (None, trimmed)
    };

    let rest_upper = rest.to_ascii_uppercase();
    // `PRIMARY KEY` is `UNIQUE NOT NULL`; the uniqueness half is what is
    // enforced here.
    if !rest_upper.starts_with("UNIQUE") && !rest_upper.starts_with("PRIMARY KEY") {
        return Ok(None);
    }

    let open = rest
        .find('(')
        .ok_or_else(|| "a table-level UNIQUE constraint must list its columns".to_string())?;
    let close = rest[open..]
        .find(')')
        .map(|i| open + i)
        .ok_or_else(|| "UNIQUE column list is not closed".to_string())?;

    let columns: Vec<String> = rest[open + 1..close]
        .split(',')
        .map(|c| c.trim().trim_matches('"').trim().to_string())
        .filter(|c| !c.is_empty())
        .collect();

    if columns.is_empty() {
        return Err("UNIQUE names no columns".to_string());
    }
    if columns.len() > MAX_COLUMNS {
        return Err(format!(
            "a UNIQUE constraint may span at most {} columns, found {}",
            MAX_COLUMNS,
            columns.len()
        ));
    }

    let name = name.unwrap_or_else(|| UniqueKey::default_name(table, &columns));
    Ok(Some(UniqueKey {
        name,
        columns,
        is_deleted: false,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_record_round_trips() {
        let key = UniqueKey {
            name: "users_email_key".to_string(),
            columns: vec!["email".to_string()],
            is_deleted: false,
        };
        assert_eq!(UniqueKey::from_vec(&key.into_vec()).unwrap(), key);
    }

    #[test]
    fn a_composite_record_round_trips() {
        let key = UniqueKey {
            name: "memberships_org_id_user_id_key".to_string(),
            columns: vec![
                "org_id".to_string(),
                "user_id".to_string(),
                "role".to_string(),
            ],
            is_deleted: false,
        };
        assert_eq!(UniqueKey::from_vec(&key.into_vec()).unwrap(), key);
    }

    #[test]
    fn a_tombstone_survives_the_round_trip() {
        let key = UniqueKey {
            name: "gone".to_string(),
            columns: vec!["a".to_string()],
            is_deleted: true,
        };
        assert!(UniqueKey::from_vec(&key.into_vec()).unwrap().is_deleted);
    }

    fn parsed(clause: &str) -> UniqueKey {
        parse_clause(clause, "t").unwrap().unwrap()
    }

    #[test]
    fn a_single_column_constraint_parses() {
        let key = parsed("UNIQUE (email)");
        assert_eq!(key.columns, vec!["email".to_string()]);
        assert_eq!(key.name, "t_email_key");
    }

    #[test]
    fn a_composite_constraint_keeps_every_column_in_order() {
        let key = parsed("UNIQUE (org_id, user_id)");
        assert_eq!(
            key.columns,
            vec!["org_id".to_string(), "user_id".to_string()]
        );
        assert_eq!(key.name, "t_org_id_user_id_key");
    }

    #[test]
    fn a_named_constraint_keeps_its_name() {
        let key = parsed("CONSTRAINT uq_membership UNIQUE (org_id, user_id)");
        assert_eq!(key.name, "uq_membership");
        assert_eq!(
            key.columns,
            vec!["org_id".to_string(), "user_id".to_string()]
        );
    }

    /// `PRIMARY KEY` is `UNIQUE NOT NULL`; the uniqueness half lands here.
    #[test]
    fn a_primary_key_is_a_unique_constraint() {
        assert_eq!(parsed("PRIMARY KEY (id)").columns, vec!["id".to_string()]);
        assert_eq!(
            parsed("CONSTRAINT pk PRIMARY KEY (a, b)").columns,
            vec!["a".to_string(), "b".to_string()]
        );
    }

    #[test]
    fn quoted_identifiers_are_unwrapped() {
        assert_eq!(
            parsed(r#"UNIQUE ("orgId", "userId")"#).columns,
            vec!["orgId".to_string(), "userId".to_string()]
        );
    }

    /// Anything that is not a unique constraint falls through, so a caller can
    /// walk a whole `CREATE TABLE` body with this.
    #[test]
    fn another_kind_of_clause_falls_through() {
        assert_eq!(parse_clause("id INTEGER", "t").unwrap(), None);
        assert_eq!(
            parse_clause("FOREIGN KEY (a) REFERENCES u (b)", "t").unwrap(),
            None
        );
        assert_eq!(parse_clause("CHECK (a > 0)", "t").unwrap(), None);
        assert_eq!(
            parse_clause("CONSTRAINT c CHECK (a > 0)", "t").unwrap(),
            None
        );
    }

    /// A constraint that cannot be honoured is an error, never a silent skip.
    #[test]
    fn an_unusable_constraint_is_reported() {
        assert!(
            parse_clause("UNIQUE", "t")
                .unwrap_err()
                .contains("list its columns")
        );
        assert!(
            parse_clause("UNIQUE (", "t")
                .unwrap_err()
                .contains("not closed")
        );
        assert!(
            parse_clause("UNIQUE ()", "t")
                .unwrap_err()
                .contains("no columns")
        );
        let wide = (0..MAX_COLUMNS + 1)
            .map(|i| format!("c{}", i))
            .collect::<Vec<_>>()
            .join(", ");
        assert!(
            parse_clause(&format!("UNIQUE ({})", wide), "t")
                .unwrap_err()
                .contains("at most")
        );
    }
}
