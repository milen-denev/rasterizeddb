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

/// Writes `name` into its 128-byte slot as UTF-8, cut at a character
/// boundary (`unique_key::stored_name`'s cut), as the UNIQUE and CHECK
/// sidecars write theirs since phase 3 P1.
///
/// It used to keep only ASCII letters, digits and `_`: `"my fk"` was stored
/// as `myfk` and `外键` as an empty name, so a `DROP CONSTRAINT` by the name
/// the client gave matched nothing. The column and table names went through
/// the same filter. [`read_name`] always decoded UTF-8, so the format is
/// unchanged: a record written filtered reads back as it was written, and an
/// older binary reads the new names. A table or column name that does not
/// fit is refused where the key is defined ([`ForeignKey::storable`]), never
/// cut here: a cut table name would name another table.
fn write_name(out: &mut [u8], at: usize, name: &str) {
    let stored = crate::core::rql::unique_key::stored_name(name);
    let bytes = stored.as_bytes();
    out[at..at + bytes.len()].copy_from_slice(bytes);
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

/// The name PostgreSQL gives an unnamed foreign key on `column` of `table`:
/// `<table>_<column>_fkey`, the stem cut so the label survives
/// (`unique_key::name_with_label`), as every derived name is built. A
/// collision is numbered by the caller (`unique_key::choose_name`), which
/// knows the table's other constraints.
pub fn default_name(table: &str, column: &str) -> String {
    crate::core::rql::unique_key::name_with_label(&key_stem(table, column), FKEY_LABEL)
}

/// `<table>_<column>`, what an unnamed foreign key's `_fkey` follows.
pub fn key_stem(table: &str, column: &str) -> String {
    crate::core::rql::unique_key::key_stem(table, std::slice::from_ref(&column.to_string()))
}

/// The label of an unnamed foreign key's name.
pub const FKEY_LABEL: &str = "_fkey";

impl ForeignKey {
    /// Refuses a key whose referencing column, referenced table or
    /// referenced column does not fit its 128-byte slot: cut, the record
    /// would name another relation or column, and the key would be checked
    /// against the wrong thing or skipped. The constraint's own name is cut
    /// where it is read (`unique_key::stored_name`), as every constraint
    /// name is, so it always fits.
    pub fn storable(&self) -> Result<(), String> {
        for (what, name) in [
            ("column", &self.column),
            ("referenced table", &self.parent_table),
            ("referenced column", &self.parent_column),
        ] {
            if name.len() > crate::core::rql::unique_key::MAX_NAME_BYTES {
                return Err(format!(
                    "foreign key \"{}\": a {what} name over {} bytes is not supported (\"{}\" is {} bytes)",
                    self.name,
                    crate::core::rql::unique_key::MAX_NAME_BYTES,
                    name,
                    name.len()
                ));
            }
        }
        Ok(())
    }

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
pub async fn load<S: StorageIO>(schema_io: Arc<S>) -> Vec<ForeignKey> {
    let io = schema_io.create_new(sidecar_name(&schema_io)).await;
    let len = io.get_len().await;

    let mut out: Vec<ForeignKey> = Vec::new();
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
        if fk.is_deleted || !fk.name.eq_ignore_ascii_case(&crate::core::rql::unique_key::stored_name(name)) {
            continue;
        }
        fk.is_deleted = true;
        // Through the log, not `write_data_seek`: the record went in as a
        // logged append, and the replay at every open put an unlogged
        // overwrite back — the dropped constraint was enforced again.
        io.write_data(position, &fk.into_vec()).await;
        return Ok(true);
    }

    Ok(false)
}

/// Marks deleted every live foreign key whose referenced table is `parent`
/// (compared as the checks compare it): `DROP TABLE parent CASCADE`, which
/// drops the constraints that depend on the table, never the tables that
/// hold them, as PostgreSQL does. Returns how many it marked.
pub async fn remove_referencing<S: StorageIO>(schema_io: Arc<S>, parent: &str) -> io::Result<usize> {
    let io = schema_io.create_new(sidecar_name(&schema_io)).await;
    let len = io.get_len().await;
    let mut marked = 0;
    let count = len / FOREIGN_KEY_SIZE;
    let mut buffer = vec![0u8; FOREIGN_KEY_SIZE as usize];
    for i in 0..count {
        let position = i * FOREIGN_KEY_SIZE;
        let mut cursor = position;
        if io.read_data_into_buffer(&mut cursor, &mut buffer).await.is_err() {
            return Err(io::Error::other(format!(
                "could not read foreign key record {i} of {}",
                sidecar_name(&schema_io)
            )));
        }
        let Ok(mut fk) = ForeignKey::from_vec(&buffer) else {
            continue;
        };
        if fk.is_deleted || !fk.parent_table.eq_ignore_ascii_case(parent) {
            continue;
        }
        fk.is_deleted = true;
        // Logged, as `remove` writes it.
        io.write_data(position, &fk.into_vec()).await;
        marked += 1;
    }
    Ok(marked)
}

/// Parses a `FOREIGN KEY (…) REFERENCES parent (…) [ON DELETE …] [ON UPDATE …]`
/// clause, with `child_columns` supplying the referencing column when the
/// clause is written inline on a column definition.
///
/// Returns an error naming what could not be handled, so a migration fails
/// loudly rather than proceeding with a constraint that is not there.
///
/// `table` is the table the key is declared on: an unnamed key is called
/// [`default_name`]`(table, column)`, PostgreSQL's `<table>_<column>_fkey`.
/// A given `name` is cut to its stored form (`unique_key::stored_name`).
pub fn parse_clause(
    clause: &str,
    table: &str,
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
        name: match name {
            Some(name) => crate::core::rql::unique_key::stored_name(&name),
            None => default_name(table, &column),
        },
        column,
        parent_table,
        parent_column,
        on_delete,
        on_update,
        is_deleted: false,
    })
}

/// The columns a referenced table offers a foreign key, as one table's
/// declarations: its fields, which are `UNIQUE` (inline, or a one-column
/// primary key), its header's primary key, and its unique keys.
pub struct ReferencedSide<'a> {
    pub table: &'a str,
    /// `(name, deleted, unique flag)` of each field.
    pub fields: Vec<(&'a str, bool, bool)>,
    pub primary_key: Option<&'a str>,
    pub unique_keys: &'a [crate::core::rql::unique_key::UniqueKey],
    /// A partitioned table's key columns, outermost first.
    pub partition_key: Option<Vec<String>>,
}

impl<'a> ReferencedSide<'a> {
    pub fn of_schema(
        schema: &'a crate::core::row::schema::TableSchema,
        table: &'a str,
        partition_key: Option<Vec<String>>,
    ) -> Self {
        ReferencedSide {
            table,
            fields: schema
                .fields
                .iter()
                .filter(|f| !crate::core::rql::nullable::is_hidden(&f.name))
                .map(|f| (f.name.as_str(), f.is_deleted, f.is_unique))
                .collect(),
            primary_key: schema.primary_key.as_deref(),
            unique_keys: &schema.unique_keys,
            partition_key,
        }
    }
}

/// Why `fk` cannot reference `parent`, in PostgreSQL's words, or `None`:
///
/// - its referenced column is not a live column of the parent (42703,
///   "column … referenced in foreign key constraint does not exist");
/// - the parent is **partitioned** and no `UNIQUE` or `PRIMARY KEY` of it is
///   exactly that column (42830, "there is no unique constraint matching
///   given keys for referenced table …"; a partial unique index is no such
///   constraint), or the column is not its whole partition key. The second
///   is implied by the first (a unique key holds every partition key column,
///   §1b, and a foreign key is one column), and checked on its own because
///   every check of the key then routes by it: one that could not would
///   have to read every partition (phase 3 P4).
///
/// A **plain** parent's column need not be unique: the plain path has always
/// accepted that, unlike PostgreSQL, and phase 3 P4 leaves it as it was
/// (docs/partitioning.md, "Phase 3 P4 as built", divergences). A delete of
/// one of two parent rows sharing the value then acts on children the other
/// still covers.
pub fn referenced_key_refusal(fk: &ForeignKey, parent: &ReferencedSide<'_>) -> Option<String> {
    let column = fk.parent_column.as_str();
    let Some(&(_, _, flagged)) = parent
        .fields
        .iter()
        .find(|(name, deleted, _)| !*deleted && name.eq_ignore_ascii_case(column))
    else {
        return Some(format!("column \"{column}\" referenced in foreign key constraint does not exist"));
    };
    let unique = flagged
        || parent.primary_key.is_some_and(|p| p.eq_ignore_ascii_case(column))
        || parent.unique_keys.iter().any(|k| {
            !k.is_deleted && k.predicate.is_none() && k.columns.len() == 1 && k.columns[0].eq_ignore_ascii_case(column)
        });
    let not_unique = || {
        format!(
            "there is no unique constraint matching given keys for referenced table \"{}\"",
            parent.table
        )
    };
    if let Some(key) = &parent.partition_key {
        if !unique {
            return Some(not_unique());
        }
        if !(key.len() == 1 && key[0].eq_ignore_ascii_case(column)) {
            return Some(format!(
                "{}: partitioned table \"{}\" is partitioned by ({}), and a foreign key must reference its whole \
                 partition key",
                not_unique(),
                parent.table,
                key.join(", ")
            ));
        }
    }
    None
}

/// Whether a referencing column of type `child` can reference a column of
/// type `parent`, by PostgreSQL's rule as far as this engine's types carry it
/// (a key needs an equality between the two in the referenced key's
/// operator family): integers of any width with each other, the two float
/// widths with each other, text with text (`TEXT`, `VARCHAR`, `CHAR`),
/// `NUMERIC` with `NUMERIC`; any other type only with itself. Across those
/// families PostgreSQL refuses the key (42804), and here a check would
/// compare a literal of one type with a column of another.
pub fn compatible_key_types(child: &crate::core::db_type::DbType, parent: &crate::core::db_type::DbType) -> bool {
    use crate::core::db_type::DbType::*;
    let family = |t: &crate::core::db_type::DbType| match t {
        I8 | I16 | I32 | I64 | I128 | U8 | U16 | U32 | U64 | U128 => 1,
        F32 | F64 => 2,
        STRING | CHAR => 3,
        DECIMAL => 4,
        _ => 0,
    };
    match (family(child), family(parent)) {
        (0, 0) => child == parent,
        (a, b) => a == b,
    }
}

/// PostgreSQL's refusal (42804) of `fk` when its column's type `child` cannot
/// reference the parent column's type `parent` ([`compatible_key_types`]).
pub fn type_refusal(
    fk: &ForeignKey,
    child: &crate::core::db_type::DbType,
    parent: &crate::core::db_type::DbType,
) -> Option<String> {
    (!compatible_key_types(child, parent)).then(|| {
        format!(
            "foreign key constraint \"{}\" cannot be implemented: key columns \"{}\" and \"{}\" are of \
             incompatible types: {:?} and {:?}",
            fk.name, fk.column, fk.parent_column, child, parent
        )
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
            "t",
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
        let fk = parse_clause(r#"REFERENCES "domain" ("id")"#, "t", None, Some("domain_id")).unwrap();
        assert_eq!(fk.column, "domain_id");
        assert_eq!(fk.parent_table, "domain");
        assert_eq!(fk.parent_column, "id");
    }

    #[test]
    fn reads_both_actions() {
        let fk = parse_clause(
            "FOREIGN KEY (a) REFERENCES b (c) ON DELETE SET NULL ON UPDATE CASCADE",
            "t",
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
            "t",
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
        let e = parse_clause("FOREIGN KEY (a, b) REFERENCES t (c, d)", "t", None, None).unwrap_err();
        assert!(e.contains("composite"), "{}", e);
    }

    #[test]
    fn malformed_clauses_name_the_problem() {
        assert!(parse_clause("FOREIGN KEY (a)", "t", None, None).is_err());
        assert!(parse_clause("FOREIGN KEY (a) REFERENCES t", "t", None, None).is_err());
        assert!(
            parse_clause(
                "FOREIGN KEY (a) REFERENCES t (b) ON DELETE SET SOMETHING",
                "t",
                None,
                None
            )
            .is_err(),
            "an unknown action must be refused rather than read as NO ACTION"
        );
    }

    /// PostgreSQL's name, `<table>_<column>_fkey`, built as every derived
    /// name is: on a long table name the stem is cut at a character
    /// boundary and the label kept whole.
    #[test]
    fn an_unnamed_constraint_gets_postgres_name() {
        let fk = parse_clause("FOREIGN KEY (user_id) REFERENCES users (id)", "orders", None, None).unwrap();
        assert_eq!(fk.name, "orders_user_id_fkey");
        let inline = parse_clause("REFERENCES users (id)", "orders", None, Some("buyer")).unwrap();
        assert_eq!(inline.name, "orders_buyer_fkey");
        let long = "é".repeat(70);
        let fk = parse_clause("FOREIGN KEY (a) REFERENCES u (id)", &long, None, None).unwrap();
        assert!(fk.name.len() <= 128 && fk.name.ends_with("_fkey"), "{}", fk.name);
        assert!(fk.name.starts_with("éé"), "{}", fk.name);
    }

    /// A name is kept as written, UTF-8, spaces and all, cut at a character
    /// boundary within its 128-byte slot; the column and table names too.
    #[test]
    fn names_are_stored_as_given() {
        let mut fk = sample();
        fk.name = "my 外键".to_string();
        fk.column = "Ünit col".to_string();
        fk.parent_table = "親".to_string();
        fk.parent_column = "id é".to_string();
        assert_eq!(ForeignKey::from_vec(&fk.into_vec()).unwrap(), fk);
        fk.name = "é".repeat(70);
        let back = ForeignKey::from_vec(&fk.into_vec()).unwrap();
        assert_eq!(back.name, "é".repeat(64), "cut at a char boundary within 128 bytes");
        let named = parse_clause("FOREIGN KEY (a) REFERENCES u (id)", "t", Some("é".repeat(70)), None).unwrap();
        assert_eq!(named.name, back.name, "read in the form it is stored in");
    }

    /// A record an older binary wrote ASCII-filtered reads back as written.
    #[test]
    fn a_filtered_record_still_reads() {
        let mut raw = vec![0u8; FOREIGN_KEY_SIZE as usize];
        raw[..4].copy_from_slice(b"myfk");
        raw[129..130].copy_from_slice(b"a");
        raw[258..259].copy_from_slice(b"p");
        raw[387..389].copy_from_slice(b"id");
        let fk = ForeignKey::from_vec(&raw).unwrap();
        assert_eq!((fk.name.as_str(), fk.column.as_str(), fk.parent_table.as_str()), ("myfk", "a", "p"));
    }

    /// A table or column name that does not fit is refused, never cut: a cut
    /// table name names another table.
    #[test]
    fn an_unstorable_name_is_refused() {
        let mut fk = sample();
        assert!(fk.storable().is_ok());
        fk.parent_table = "p".repeat(129);
        assert!(fk.storable().unwrap_err().contains("not supported"));
        fk.parent_table = "p".repeat(128);
        assert!(fk.storable().is_ok());
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

/// End-to-end cost of foreign-key enforcement on a bulk insert.
///
/// `cargo test --release -p rasterizeddb_core --lib -- --ignored fk_bulk_insert_cost --nocapture`
#[cfg(test)]
mod bench {
    use crate::core::rql::executor;
    use crate::core::rql::lexer_s1::recognize_query_purpose;

    async fn exec(db: &rclite::Arc<crate::core::database::Database>, sql: &str) -> Vec<u8> {
        let purpose = recognize_query_purpose(sql).expect("parse");
        executor::execute(purpose, db.clone()).await
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
    async fn fk_bulk_insert_cost() {
        let _ = crate::MAX_PERMITS_THREADS.get_or_init(|| 8);
        let _ = crate::BATCH_SIZE.get_or_init(|| 1024 * 8);
        let _ = crate::core::processor::concurrent_processor::ENABLE_CACHE.get_or_init(|| false);

        const PARENTS: usize = 1_000;
        const CHILDREN: usize = 10_000;
        const PER_STATEMENT: usize = 1_000;

        for round in 0..3 {
            let catalog = crate::core::database::catalog::DatabaseCatalog::open(None, "fkbench")
                .await
                .unwrap();
            let db = catalog.get("fkbench").unwrap();
            assert_eq!(exec(&db, "CREATE TABLE p (id INT PRIMARY KEY, name TEXT)").await[0], 0);
            let values: Vec<String> = (0..PARENTS).map(|i| format!("({}, 'p{}')", i, i)).collect();
            assert_ne!(
                exec(&db, &format!("INSERT INTO p (id, name) VALUES {}", values.join(", "))).await[0],
                3
            );
            assert_eq!(
                exec(
                    &db,
                    "CREATE TABLE c (id INT PRIMARY KEY, pid INT, \
                     CONSTRAINT fk_c FOREIGN KEY (pid) REFERENCES p (id))"
                )
                .await[0],
                0
            );
            assert_eq!(exec(&db, "CREATE TABLE n (id INT PRIMARY KEY, pid INT)").await[0], 0);

            for (table, label) in [("n", "no foreign key"), ("c", "foreign key")] {
                let t = std::time::Instant::now();
                for chunk in 0..CHILDREN / PER_STATEMENT {
                    let values: Vec<String> = (0..PER_STATEMENT)
                        .map(|i| {
                            let id = chunk * PER_STATEMENT + i;
                            format!("({}, {})", id, (id * 7) % PARENTS)
                        })
                        .collect();
                    let r = exec(
                        &db,
                        &format!("INSERT INTO {} (id, pid) VALUES {}", table, values.join(", ")),
                    )
                    .await;
                    assert_ne!(r[0], 3, "{}", String::from_utf8_lossy(&r[1..]));
                }
                println!(
                    "round {} {:>15}: {} rows in {:?} ({:.1} us/row)",
                    round,
                    label,
                    CHILDREN,
                    t.elapsed(),
                    t.elapsed().as_micros() as f64 / CHILDREN as f64
                );
            }
        }
    }
}
