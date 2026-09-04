//! Foreign keys: definition, persistence, and enforcement.
//!
//! A foreign key is the one constraint an application genuinely relies on the
//! database to hold, because the alternative is every writer remembering to
//! check. Accepting the DDL and enforcing nothing would leave an application
//! believing its references are sound when they are not, so this enforces them.
//!
//! # How enforcement is expressed
//!
//! Rather than reaching into the storage layer, every check is phrased as a
//! query the engine already runs:
//!
//! * inserting or updating a child — `SELECT <parent_key> FROM parent WHERE
//!   <parent_key> = <value> LIMIT 1`, and the row is rejected if that is empty;
//! * deleting a parent — `SELECT`/`DELETE`/`UPDATE` against each referencing
//!   table, according to the declared action.
//!
//! That means constraint checks are filtered and evaluated by the same paths as
//! any other query, and a cascade that triggers another cascade works by
//! recursion rather than by a second mechanism.

use std::io;

use rclite::Arc;
use smallvec::SmallVec;

use crate::core::storage_providers::traits::StorageIO;

/// Bytes one persisted foreign key occupies.
///
/// Four names at 128 characters plus a terminator, then the two actions and the
/// tombstone. Fixed-stride, so the file can be read as an array like the schema
/// fields file.
pub const FOREIGN_KEY_SIZE: u64 = 129 * 4 + 3;

/// What to do to referencing rows when the referenced row goes away.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReferentialAction {
    /// Refuse the change while any row still references it.
    NoAction,
    Restrict,
    Cascade,
    SetNull,
    /// Put the referencing column back to its declared default. A column with
    /// no default reads as `NULL`, as it does in PostgreSQL.
    SetDefault,
}

impl ReferentialAction {
    pub fn parse(text: &str) -> Option<Self> {
        let normalised = text.trim().to_ascii_uppercase();
        Some(match normalised.as_str() {
            "NO ACTION" | "NOACTION" => ReferentialAction::NoAction,
            "RESTRICT" => ReferentialAction::Restrict,
            "CASCADE" => ReferentialAction::Cascade,
            "SET NULL" | "SETNULL" => ReferentialAction::SetNull,
            "SET DEFAULT" | "SETDEFAULT" => ReferentialAction::SetDefault,
            _ => return None,
        })
    }

    fn to_byte(self) -> u8 {
        match self {
            ReferentialAction::NoAction => 0,
            ReferentialAction::Restrict => 1,
            ReferentialAction::Cascade => 2,
            ReferentialAction::SetNull => 3,
            ReferentialAction::SetDefault => 4,
        }
    }

    fn from_byte(byte: u8) -> Self {
        match byte {
            1 => ReferentialAction::Restrict,
            2 => ReferentialAction::Cascade,
            3 => ReferentialAction::SetNull,
            4 => ReferentialAction::SetDefault,
            // Anything unrecognised is the strictest reading, so a record this
            // build cannot interpret refuses the change rather than allowing it.
            _ => ReferentialAction::NoAction,
        }
    }

    /// Whether the action blocks the parent change rather than propagating it.
    pub fn blocks(self) -> bool {
        matches!(
            self,
            ReferentialAction::NoAction | ReferentialAction::Restrict
        )
    }
}

/// One foreign key: a column of this table referencing a column of another.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeignKey {
    pub name: String,
    /// Column on the table that owns this definition.
    pub column: String,
    pub parent_table: String,
    pub parent_column: String,
    pub on_delete: ReferentialAction,
    pub on_update: ReferentialAction,
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
        .map_err(|e| format!("invalid UTF-8 in foreign key record: {}", e))
}

impl ForeignKey {
    pub fn into_vec(&self) -> Vec<u8> {
        let mut out = vec![0u8; FOREIGN_KEY_SIZE as usize];
        write_name(&mut out, 0, &self.name);
        write_name(&mut out, 129, &self.column);
        write_name(&mut out, 258, &self.parent_table);
        write_name(&mut out, 387, &self.parent_column);
        out[516] = self.on_delete.to_byte();
        out[517] = self.on_update.to_byte();
        out[518] = u8::from(self.is_deleted);
        out
    }

    pub fn from_vec(raw: &[u8]) -> Result<Self, String> {
        if raw.len() != FOREIGN_KEY_SIZE as usize {
            return Err(format!(
                "invalid foreign key record: expected {} bytes, got {}",
                FOREIGN_KEY_SIZE,
                raw.len()
            ));
        }
        Ok(ForeignKey {
            name: read_name(raw, 0)?,
            column: read_name(raw, 129)?,
            parent_table: read_name(raw, 258)?,
            parent_column: read_name(raw, 387)?,
            on_delete: ReferentialAction::from_byte(raw[516]),
            on_update: ReferentialAction::from_byte(raw[517]),
            is_deleted: raw[518] != 0,
        })
    }
}

/// Name of the sidecar holding a table's foreign keys.
fn sidecar_name<S: StorageIO>(schema_io: &Arc<S>) -> String {
    format!("{}_{}", schema_io.get_name().replace(".db", ""), "FK.db")
}

/// Reads a table's foreign keys.
pub async fn load<S: StorageIO>(schema_io: Arc<S>) -> SmallVec<[ForeignKey; 4]> {
    let io = schema_io.create_new(sidecar_name(&schema_io)).await;
    let len = io.get_len().await;

    let mut out: SmallVec<[ForeignKey; 4]> = SmallVec::new();
    if len < FOREIGN_KEY_SIZE {
        return out;
    }

    let count = len / FOREIGN_KEY_SIZE;
    let mut buffer = vec![0u8; FOREIGN_KEY_SIZE as usize];
    for i in 0..count {
        let mut position = i * FOREIGN_KEY_SIZE;
        if io
            .read_data_into_buffer(&mut position, &mut buffer)
            .await
            .is_err()
        {
            break;
        }
        match ForeignKey::from_vec(&buffer) {
            // A tombstoned record is kept on disk so the file keeps its stride.
            Ok(fk) if !fk.is_deleted => out.push(fk),
            Ok(_) => {}
            Err(e) => log::error!("Skipping unreadable foreign key record: {}", e),
        }
    }

    out
}

/// Appends a foreign key.
pub async fn append<S: StorageIO>(schema_io: Arc<S>, fk: &ForeignKey) -> io::Result<()> {
    let io = schema_io.create_new(sidecar_name(&schema_io)).await;
    io.append_data(&fk.into_vec(), true).await;
    Ok(())
}

/// Marks a foreign key deleted, by constraint name.
///
/// Returns whether one was found. The record keeps its slot so the file stays a
/// fixed-stride array.
pub async fn remove<S: StorageIO>(schema_io: Arc<S>, name: &str) -> io::Result<bool> {
    let io = schema_io.create_new(sidecar_name(&schema_io)).await;
    let len = io.get_len().await;
    if len < FOREIGN_KEY_SIZE {
        return Ok(false);
    }

    let count = len / FOREIGN_KEY_SIZE;
    let mut buffer = vec![0u8; FOREIGN_KEY_SIZE as usize];
    for i in 0..count {
        let position = i * FOREIGN_KEY_SIZE;
        let mut cursor = position;
        if io
            .read_data_into_buffer(&mut cursor, &mut buffer)
            .await
            .is_err()
        {
            break;
        }
        let Ok(mut fk) = ForeignKey::from_vec(&buffer) else {
            continue;
        };
        if fk.is_deleted || !fk.name.eq_ignore_ascii_case(name) {
            continue;
        }
        fk.is_deleted = true;
        io.write_data_seek(io::SeekFrom::Start(position), &fk.into_vec())
            .await;
        return Ok(true);
    }

    Ok(false)
}

/// Parses a `FOREIGN KEY (…) REFERENCES parent (…) [ON DELETE …] [ON UPDATE …]`
/// clause, with `child_columns` supplying the referencing column when the
/// clause is written inline on a column definition.
///
/// Returns an error naming what could not be handled, so a migration fails
/// loudly rather than proceeding with a constraint that is not there.
pub fn parse_clause(
    clause: &str,
    name: Option<String>,
    inline_column: Option<&str>,
) -> Result<ForeignKey, String> {
    let upper = clause.to_ascii_uppercase();

    let references_at = upper
        .find("REFERENCES")
        .ok_or_else(|| "a foreign key must name what it REFERENCES".to_string())?;

    // Referencing column: either from the `FOREIGN KEY (…)` list, or the column
    // the clause was written on.
    let column = match inline_column {
        Some(c) => c.trim_matches('"').to_string(),
        None => {
            let head = &clause[..references_at];
            let open = head
                .find('(')
                .ok_or_else(|| "FOREIGN KEY must list its columns".to_string())?;
            let close = head[open..]
                .find(')')
                .map(|i| open + i)
                .ok_or_else(|| "FOREIGN KEY column list is not closed".to_string())?;
            let list = &head[open + 1..close];
            if list.contains(',') {
                return Err(
                    "composite foreign keys are not supported yet: a key must be one column"
                        .to_string(),
                );
            }
            list.trim().trim_matches('"').to_string()
        }
    };
    if column.is_empty() {
        return Err("foreign key names no referencing column".to_string());
    }

    // Referenced table and column.
    let tail = clause[references_at + "REFERENCES".len()..].trim();
    let tail_upper = tail.to_ascii_uppercase();
    let actions_at = tail_upper.find(" ON ").unwrap_or(tail.len());
    let target = tail[..actions_at].trim();

    let (parent_table, parent_column) = match target.find('(') {
        None => {
            return Err(format!(
                "REFERENCES {} must name the referenced column",
                target
            ));
        }
        Some(open) => {
            let close = target[open..]
                .find(')')
                .map(|i| open + i)
                .ok_or_else(|| "REFERENCES column list is not closed".to_string())?;
            let list = &target[open + 1..close];
            if list.contains(',') {
                return Err(
                    "composite foreign keys are not supported yet: a key must be one column"
                        .to_string(),
                );
            }
            (
                target[..open]
                    .trim()
                    .split('.')
                    .next_back()
                    .unwrap_or("")
                    .trim_matches('"')
                    .to_string(),
                list.trim().trim_matches('"').to_string(),
            )
        }
    };
    if parent_table.is_empty() || parent_column.is_empty() {
        return Err("REFERENCES names no relation or column".to_string());
    }

    let actions = &tail[actions_at..];
    let on_delete = parse_action(actions, "ON DELETE")?;
    let on_update = parse_action(actions, "ON UPDATE")?;

    Ok(ForeignKey {
        name: name.unwrap_or_else(|| format!("fk_{}_{}", column, parent_table)),
        column,
        parent_table,
        parent_column,
        on_delete,
        on_update,
        is_deleted: false,
    })
}

/// Reads the action after `keyword`, defaulting to `NO ACTION` as SQL does.
fn parse_action(actions: &str, keyword: &str) -> Result<ReferentialAction, String> {
    let upper = actions.to_ascii_uppercase();
    let Some(at) = upper.find(keyword) else {
        return Ok(ReferentialAction::NoAction);
    };

    let rest = actions[at + keyword.len()..].trim();
    let words: Vec<&str> = rest.split_whitespace().collect();
    // The longest spellings are two words, so both are tried before one.
    for take in [2usize, 1] {
        if words.len() >= take {
            if let Some(action) = ReferentialAction::parse(&words[..take].join(" ")) {
                return Ok(action);
            }
        }
    }

    Err(format!(
        "{} {} is not supported yet",
        keyword,
        words.first().copied().unwrap_or("")
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_table_level_clause() {
        let fk = parse_clause(
            "FOREIGN KEY (domain_id) REFERENCES domain (id) ON DELETE CASCADE",
            Some("fk_x".to_string()),
            None,
        )
        .unwrap();
        assert_eq!(fk.name, "fk_x");
        assert_eq!(fk.column, "domain_id");
        assert_eq!(fk.parent_table, "domain");
        assert_eq!(fk.parent_column, "id");
        assert_eq!(fk.on_delete, ReferentialAction::Cascade);
        // Unstated actions default to NO ACTION, as in SQL.
        assert_eq!(fk.on_update, ReferentialAction::NoAction);
    }

    #[test]
    fn parses_an_inline_column_clause() {
        let fk = parse_clause(r#"REFERENCES "domain" ("id")"#, None, Some("domain_id")).unwrap();
        assert_eq!(fk.column, "domain_id");
        assert_eq!(fk.parent_table, "domain");
        assert_eq!(fk.parent_column, "id");
    }

    #[test]
    fn reads_both_actions() {
        let fk = parse_clause(
            "FOREIGN KEY (a) REFERENCES b (c) ON DELETE SET NULL ON UPDATE CASCADE",
            None,
            None,
        )
        .unwrap();
        assert_eq!(fk.on_delete, ReferentialAction::SetNull);
        assert_eq!(fk.on_update, ReferentialAction::Cascade);
    }

    #[test]
    fn strips_quoting_and_schema_qualification() {
        let fk = parse_clause(
            r#"FOREIGN KEY ("domain_id") REFERENCES "public"."domain" ("id")"#,
            None,
            None,
        )
        .unwrap();
        assert_eq!(fk.column, "domain_id");
        assert_eq!(fk.parent_table, "domain");
    }

    /// Accepting a composite key and enforcing one column of it would enforce
    /// something weaker than what was declared.
    #[test]
    fn composite_keys_are_refused_rather_than_partly_enforced() {
        let e = parse_clause("FOREIGN KEY (a, b) REFERENCES t (c, d)", None, None).unwrap_err();
        assert!(e.contains("composite"), "{}", e);
    }

    #[test]
    fn malformed_clauses_name_the_problem() {
        assert!(parse_clause("FOREIGN KEY (a)", None, None).is_err());
        assert!(parse_clause("FOREIGN KEY (a) REFERENCES t", None, None).is_err());
        assert!(
            parse_clause(
                "FOREIGN KEY (a) REFERENCES t (b) ON DELETE SET SOMETHING",
                None,
                None
            )
            .is_err(),
            "an unknown action must be refused rather than read as NO ACTION"
        );
    }

    #[test]
    fn an_unnamed_constraint_gets_a_derived_name() {
        let fk = parse_clause("FOREIGN KEY (user_id) REFERENCES users (id)", None, None).unwrap();
        assert_eq!(fk.name, "fk_user_id_users");
    }

    fn sample() -> ForeignKey {
        ForeignKey {
            name: "fk_users_domain".to_string(),
            column: "domain_id".to_string(),
            parent_table: "domain".to_string(),
            parent_column: "id".to_string(),
            on_delete: ReferentialAction::Cascade,
            on_update: ReferentialAction::Restrict,
            is_deleted: false,
        }
    }

    #[test]
    fn a_record_round_trips_through_its_bytes() {
        let fk = sample();
        let raw = fk.into_vec();
        assert_eq!(raw.len(), FOREIGN_KEY_SIZE as usize);
        assert_eq!(ForeignKey::from_vec(&raw).unwrap(), fk);
    }

    #[test]
    fn a_tombstone_survives_the_round_trip() {
        let mut fk = sample();
        fk.is_deleted = true;
        assert!(ForeignKey::from_vec(&fk.into_vec()).unwrap().is_deleted);
    }

    #[test]
    fn a_short_record_is_rejected_rather_than_read() {
        assert!(ForeignKey::from_vec(&[0u8; 10]).is_err());
    }

    #[test]
    fn actions_parse_from_their_sql_spellings() {
        assert_eq!(
            ReferentialAction::parse("CASCADE"),
            Some(ReferentialAction::Cascade)
        );
        assert_eq!(
            ReferentialAction::parse("set null"),
            Some(ReferentialAction::SetNull)
        );
        assert_eq!(
            ReferentialAction::parse("NO ACTION"),
            Some(ReferentialAction::NoAction)
        );
        assert_eq!(
            ReferentialAction::parse("RESTRICT"),
            Some(ReferentialAction::Restrict)
        );
        assert_eq!(
            ReferentialAction::parse("SET DEFAULT"),
            Some(ReferentialAction::SetDefault)
        );
        assert_eq!(ReferentialAction::parse("nonsense"), None);
    }

    /// An action byte this build does not recognise must read as the strictest
    /// interpretation, so an unreadable record refuses a change rather than
    /// silently allowing one.
    #[test]
    fn an_unknown_action_byte_blocks_rather_than_permits() {
        let mut raw = sample().into_vec();
        raw[516] = 99;
        let fk = ForeignKey::from_vec(&raw).unwrap();
        assert_eq!(fk.on_delete, ReferentialAction::NoAction);
        assert!(fk.on_delete.blocks());
    }

    #[test]
    fn only_blocking_actions_report_as_blocking() {
        assert!(ReferentialAction::NoAction.blocks());
        assert!(ReferentialAction::Restrict.blocks());
        assert!(!ReferentialAction::Cascade.blocks());
        assert!(!ReferentialAction::SetNull.blocks());
    }
}
