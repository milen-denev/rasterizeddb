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

/// Bytes one persisted foreign-key record occupies: a one-column key, or one
/// column pair of a composite key ([`ForeignKeyRecord`]).
///
/// Four names at 128 bytes plus a spare byte each, then the two actions and
/// the tombstone. Fixed-stride, so the file can be read as an array like the
/// schema fields file.
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

    /// The action as SQL writes it.
    pub fn sql(self) -> &'static str {
        match self {
            ReferentialAction::NoAction => "NO ACTION",
            ReferentialAction::Restrict => "RESTRICT",
            ReferentialAction::Cascade => "CASCADE",
            ReferentialAction::SetNull => "SET NULL",
            ReferentialAction::SetDefault => "SET DEFAULT",
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

/// One foreign key: columns of this table referencing columns of another
/// table, or of itself.
///
/// One column pair is an ordinary key; several make a composite one, whose
/// columns are compared as one tuple: a row references the parent row whose
/// referenced columns hold all of its values at once, never one parent per
/// column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeignKey {
    pub name: String,
    /// The referencing columns, on the table that owns this definition, in
    /// key order.
    pub columns: Vec<String>,
    pub parent_table: String,
    /// The referenced columns, index for index with `columns`. Empty only
    /// while a `REFERENCES parent` written with no column list waits for the
    /// parent's primary key (`Database::default_referenced_columns`), which
    /// the statement fills in under its lock on the parent; [`Self::storable`]
    /// refuses a key still empty.
    pub parent_columns: Vec<String>,
    pub on_delete: ReferentialAction,
    pub on_update: ReferentialAction,
    /// `MATCH FULL`: the key's columns are all NULL or none is (23503
    /// otherwise). `false` is `MATCH SIMPLE`, the default: a row with any
    /// key column NULL is exempt. One column is the same either way.
    pub match_full: bool,
    /// `ON DELETE SET NULL (…)` / `SET DEFAULT (…)` (PostgreSQL 15): the
    /// referencing columns the action sets. Empty sets every one, which is
    /// also what a list naming all of them means.
    pub on_delete_columns: Vec<String>,
    pub is_deleted: bool,
}

/// The most columns one foreign key may have: the most a unique key may
/// span (`unique_key::MAX_COLUMNS`), since a key must reference one.
pub const MAX_KEY_COLUMNS: usize = crate::core::rql::unique_key::MAX_COLUMNS;

impl ForeignKey {
    /// `(column, parent column)` of a one-column key, `None` for a composite
    /// one (and for one whose referenced column is not resolved yet). The
    /// single-column enforcement paths take a key through this, so a
    /// composite key never reaches them read as its first column.
    pub fn single(&self) -> Option<(&str, &str)> {
        match (self.columns.as_slice(), self.parent_columns.as_slice()) {
            ([column], [parent]) => Some((column.as_str(), parent.as_str())),
            _ => None,
        }
    }

    /// Whether the key spans more than one column.
    pub fn is_composite(&self) -> bool {
        self.columns.len() > 1
    }

    /// The referencing columns as PostgreSQL lists them: `a, b`.
    pub fn column_list(&self) -> String {
        self.columns.join(", ")
    }

    /// The referenced columns as PostgreSQL lists them: `x, y`.
    pub fn parent_column_list(&self) -> String {
        self.parent_columns.join(", ")
    }

    /// Whether `column` is one of the referencing columns.
    pub fn has_column(&self, column: &str) -> bool {
        self.columns.iter().any(|c| c.eq_ignore_ascii_case(column))
    }

    /// Whether `column` is one of the referenced columns.
    pub fn has_parent_column(&self, column: &str) -> bool {
        self.parent_columns.iter().any(|c| c.eq_ignore_ascii_case(column))
    }

    /// The referencing columns `ON DELETE SET NULL` / `SET DEFAULT` writes:
    /// the declared list, or every key column.
    pub fn on_delete_set_columns(&self) -> &[String] {
        if self.on_delete_columns.is_empty() { &self.columns } else { &self.on_delete_columns }
    }

    /// Refuses a key that cannot be stored or cannot hold:
    ///
    /// - one whose referenced column was never resolved (42704, as
    ///   PostgreSQL words a parent without a primary key);
    /// - column lists of different lengths (42830);
    /// - more columns than a unique key may have (54011, PostgreSQL's
    ///   "cannot have more than … keys in a foreign key");
    /// - an `ON DELETE SET … (cols)` naming a column outside the key;
    /// - a referencing column, referenced table or referenced column whose
    ///   name does not fit its 128-byte slot (127 for a composite key's
    ///   referenced column, which is stored after a marker byte): cut, the
    ///   record would name another relation or column, and the key would be
    ///   checked against the wrong thing or skipped. The constraint's own
    ///   name is cut where it is read (`unique_key::stored_name`), as every
    ///   constraint name is, so it always fits.
    pub fn storable(&self) -> Result<(), String> {
        if self.parent_columns.is_empty() {
            return Err(format!("there is no primary key for referenced table \"{}\"", self.parent_table));
        }
        if self.columns.len() != self.parent_columns.len() {
            return Err("number of referencing and referenced columns for foreign key disagree".to_string());
        }
        if self.columns.len() > MAX_KEY_COLUMNS {
            return Err(format!("cannot have more than {MAX_KEY_COLUMNS} keys in a foreign key"));
        }
        if let Some(stray) = self.on_delete_columns.iter().find(|c| !self.has_column(c)) {
            return Err(format!(
                "column \"{stray}\" referenced in ON DELETE SET action must be part of foreign key"
            ));
        }
        let max = crate::core::rql::unique_key::MAX_NAME_BYTES;
        let parent_max = if self.is_composite() { max - 1 } else { max };
        let names = self
            .columns
            .iter()
            .map(|c| ("column", c, max))
            .chain(std::iter::once(("referenced table", &self.parent_table, max)))
            .chain(self.parent_columns.iter().map(|c| ("referenced column", c, parent_max)));
        for (what, name, limit) in names {
            if name.len() > limit {
                return Err(format!(
                    "foreign key \"{}\": a {what} name over {limit} bytes is not supported (\"{}\" is {} bytes)",
                    self.name,
                    name,
                    name.len()
                ));
            }
        }
        Ok(())
    }

    /// The key's records, as they are stored: one for a one-column key,
    /// one per column pair for a composite key ([`ForeignKeyRecord`]).
    pub fn records(&self) -> Vec<ForeignKeyRecord> {
        let count = self.columns.len();
        let composite = count > 1;
        (0..count)
            .map(|i| ForeignKeyRecord {
                name: self.name.clone(),
                column: self.columns[i].clone(),
                parent_table: self.parent_table.clone(),
                parent_column: self.parent_columns.get(i).cloned().unwrap_or_default(),
                on_delete: self.on_delete,
                on_update: self.on_update,
                is_deleted: self.is_deleted,
                member: composite.then_some((i as u8, count as u8)),
                match_full: self.match_full,
                on_delete_sets: composite
                    && self.on_delete_columns.iter().any(|c| c.eq_ignore_ascii_case(&self.columns[i])),
            })
            .collect()
    }

    /// Every record of the key, back to back, as the sidecar holds them.
    pub fn into_vec(&self) -> Vec<u8> {
        self.records().iter().flat_map(ForeignKeyRecord::into_vec).collect()
    }

    /// One key from its records, back to back (what [`Self::into_vec`]
    /// writes).
    pub fn from_vec(raw: &[u8]) -> Result<Self, String> {
        let size = FOREIGN_KEY_SIZE as usize;
        if raw.is_empty() || raw.len() % size != 0 {
            return Err(format!(
                "invalid foreign key record: expected a multiple of {} bytes, got {}",
                FOREIGN_KEY_SIZE,
                raw.len()
            ));
        }
        let mut records: Vec<Option<ForeignKeyRecord>> =
            raw.chunks(size).map(|r| ForeignKeyRecord::from_vec(r).map(Some)).collect::<Result<_, _>>()?;
        // One key, live or dropped: read as live, its first record's
        // tombstone put back on it.
        let deleted = records.first().and_then(|r| r.as_ref()).is_some_and(|r| r.is_deleted);
        for record in records.iter_mut().flatten() {
            record.is_deleted = false;
        }
        let mut keys = assemble(&records, true)?;
        match (keys.pop(), keys.is_empty()) {
            (Some(key), true) => Ok(ForeignKey { is_deleted: deleted, ..key }),
            _ => Err("invalid foreign key record: it does not hold exactly one key".to_string()),
        }
    }
}

/// One stored record of a foreign key: the whole of a one-column key, or one
/// column pair of a composite one.
///
/// # The record
///
/// Four 129-byte name slots (constraint name, referencing column, referenced
/// table, referenced column), then the two actions and the tombstone,
/// [`FOREIGN_KEY_SIZE`] bytes. A name is at most 128 bytes, so the 129th
/// byte of each slot is never part of a name: every build reads a name up
/// to its first NUL or 128 bytes, whichever comes first. Those spare bytes
/// carry what a composite key adds, and a one-column key's record is the one
/// every build has always written (its spare bytes zero, but for `MATCH
/// FULL`, which no build reads differently for one column).
///
/// - byte 128 (the name's spare byte): flags, [`FLAG_COMPOSITE`] and
///   [`FLAG_MATCH_FULL`];
/// - byte 257 (the referencing column's): this pair's position in the key;
/// - byte 386 (the referenced table's): how many pairs the key has;
/// - byte 515 (the referenced column's): [`MEMBER_ON_DELETE_SETS`].
///
/// A composite key's pairs are consecutive records, its first one first,
/// each carrying the constraint's name, actions and tombstone, and each
/// with its **referenced column stored after a [`MEMBER_MARK`] byte**. A
/// build that predates composite keys reads each pair as a one-column key
/// whose referenced column, `"\u{1}x"`, no table has: a write it would
/// check, or a parent DELETE it would act on, fails naming that column,
/// never enforcing `a` alone against `x`. That is the fallback for builds
/// older than the `incompat_features` fence; every build since refuses to
/// open the directory at all ([`crate::core::database::incompat::COMPOSITE_FOREIGN_KEY_V1`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeignKeyRecord {
    pub name: String,
    pub column: String,
    pub parent_table: String,
    pub parent_column: String,
    pub on_delete: ReferentialAction,
    pub on_update: ReferentialAction,
    pub is_deleted: bool,
    /// `(position, pairs)` when the record is one pair of a composite key.
    pub member: Option<(u8, u8)>,
    pub match_full: bool,
    /// A composite key's pair whose column `ON DELETE SET … (cols)` names.
    pub on_delete_sets: bool,
}

/// Byte 128's flag: the record is one pair of a composite key.
pub const FLAG_COMPOSITE: u8 = 0x01;
/// Byte 128's flag: the key is `MATCH FULL`.
pub const FLAG_MATCH_FULL: u8 = 0x02;
/// Byte 515's flag: `ON DELETE SET … (cols)` names this pair's column.
pub const MEMBER_ON_DELETE_SETS: u8 = 0x01;
/// The byte a composite key's referenced column is stored after: no table
/// has a column named with it, so a build that predates composite keys
/// finds none and fails loudly instead of enforcing one column of the key.
pub const MEMBER_MARK: u8 = 0x01;

const FLAGS_AT: usize = 128;
const POSITION_AT: usize = 257;
const PAIRS_AT: usize = 386;
const MEMBER_FLAGS_AT: usize = 515;

impl ForeignKeyRecord {
    pub fn into_vec(&self) -> Vec<u8> {
        let mut out = vec![0u8; FOREIGN_KEY_SIZE as usize];
        write_name(&mut out, 0, &self.name);
        write_name(&mut out, 129, &self.column);
        write_name(&mut out, 258, &self.parent_table);
        match self.member {
            Some((position, pairs)) => {
                out[387] = MEMBER_MARK;
                let bytes = self.parent_column.as_bytes();
                let len = bytes.len().min(127);
                out[388..388 + len].copy_from_slice(&bytes[..len]);
                out[FLAGS_AT] |= FLAG_COMPOSITE;
                out[POSITION_AT] = position;
                out[PAIRS_AT] = pairs;
                if self.on_delete_sets {
                    out[MEMBER_FLAGS_AT] |= MEMBER_ON_DELETE_SETS;
                }
            }
            None => write_name(&mut out, 387, &self.parent_column),
        }
        if self.match_full {
            out[FLAGS_AT] |= FLAG_MATCH_FULL;
        }
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
        let flags = raw[FLAGS_AT];
        // A flag this build does not know is a format it cannot enforce:
        // unreadable, never read as something weaker.
        if flags & !(FLAG_COMPOSITE | FLAG_MATCH_FULL) != 0 {
            return Err(format!("foreign key record carries flags {flags:#04x} this build does not know"));
        }
        let member = (flags & FLAG_COMPOSITE != 0).then(|| (raw[POSITION_AT], raw[PAIRS_AT]));
        let parent_column = match member {
            Some(_) => {
                let stored = read_name(raw, 387)?;
                match stored.strip_prefix(char::from(MEMBER_MARK)) {
                    Some(name) => name.to_string(),
                    None => return Err("composite foreign key record without its column marker".to_string()),
                }
            }
            None => read_name(raw, 387)?,
        };
        Ok(ForeignKeyRecord {
            name: read_name(raw, 0)?,
            column: read_name(raw, 129)?,
            parent_table: read_name(raw, 258)?,
            parent_column,
            on_delete: ReferentialAction::from_byte(raw[516]),
            on_update: ReferentialAction::from_byte(raw[517]),
            is_deleted: raw[518] != 0,
            member,
            match_full: flags & FLAG_MATCH_FULL != 0,
            on_delete_sets: member.is_some() && raw[MEMBER_FLAGS_AT] & MEMBER_ON_DELETE_SETS != 0,
        })
    }

    fn single_key(&self) -> ForeignKey {
        ForeignKey {
            name: self.name.clone(),
            columns: vec![self.column.clone()],
            parent_table: self.parent_table.clone(),
            parent_columns: vec![self.parent_column.clone()],
            on_delete: self.on_delete,
            on_update: self.on_update,
            match_full: self.match_full,
            on_delete_columns: Vec::new(),
            is_deleted: self.is_deleted,
        }
    }
}

/// The records of a composite key starting at `records[at]` (its first
/// pair): the range they span. `None` when `records[at]` is no first pair.
/// Shorter than the key says when the file ends first or a record in it is
/// unreadable or belongs to another key; [`assemble`] refuses that.
pub fn composite_span(records: &[Option<ForeignKeyRecord>], at: usize) -> Option<std::ops::Range<usize>> {
    let head = records.get(at)?.as_ref()?;
    let (0, pairs) = head.member? else {
        return None;
    };
    let mut end = at + 1;
    while end < records.len() && end - at < pairs as usize {
        match &records[end] {
            Some(r) if r.member == Some(((end - at) as u8, pairs)) && r.name == head.name => end += 1,
            _ => break,
        }
    }
    Some(at..end)
}

/// The live keys `records` (a sidecar's, slot by slot; `None` for a slot
/// that is unwritten or unreadable) hold, in file order.
///
/// A composite key is its first pair and the pairs after it; the first
/// pair's tombstone is the key's (a drop writes it first). A live first pair
/// whose pairs are not all there, or a pair of a live key tombstoned alone,
/// is damage: refused when `strict`, else logged and the key left out. A
/// pair with no first pair before it belongs to a dropped key.
pub fn assemble(records: &[Option<ForeignKeyRecord>], strict: bool) -> Result<Vec<ForeignKey>, String> {
    let mut out = Vec::new();
    let mut at = 0;
    while at < records.len() {
        let Some(record) = &records[at] else {
            at += 1;
            continue;
        };
        match record.member {
            None => {
                if !record.is_deleted {
                    out.push(record.single_key());
                }
                at += 1;
            }
            Some((0, pairs)) => {
                let span = composite_span(records, at).expect("a first pair");
                let found = span.len();
                at = span.end;
                if record.is_deleted {
                    continue;
                }
                let members: Vec<&ForeignKeyRecord> = records[span].iter().flatten().collect();
                let damage = if pairs < 2 || pairs as usize > MAX_KEY_COLUMNS {
                    Some(format!("it claims {pairs} columns"))
                } else if found != pairs as usize {
                    Some(format!("{found} of its {pairs} column records are readable"))
                } else if members.iter().any(|m| m.is_deleted) {
                    Some("some of its column records are dropped and others not".to_string())
                } else {
                    None
                };
                if let Some(why) = damage {
                    let message = format!("composite foreign key \"{}\" cannot be read: {why}", record.name);
                    if strict {
                        return Err(message);
                    }
                    log::error!("{message}; it is not enforced");
                    continue;
                }
                let sets: Vec<String> =
                    members.iter().filter(|m| m.on_delete_sets).map(|m| m.column.clone()).collect();
                out.push(ForeignKey {
                    name: record.name.clone(),
                    columns: members.iter().map(|m| m.column.clone()).collect(),
                    parent_table: record.parent_table.clone(),
                    parent_columns: members.iter().map(|m| m.parent_column.clone()).collect(),
                    on_delete: record.on_delete,
                    on_update: record.on_update,
                    match_full: record.match_full,
                    on_delete_columns: if sets.len() == members.len() { Vec::new() } else { sets },
                    is_deleted: false,
                });
            }
            Some(_) => {
                // A later pair with no first pair before it: the rest of a
                // key whose first pair is gone (dropped by a build that wrote
                // one record per drop), or damage.
                if !record.is_deleted && strict {
                    return Err(format!(
                        "composite foreign key \"{}\" cannot be read: a column record has no first record",
                        record.name
                    ));
                }
                at += 1;
            }
        }
    }
    Ok(out)
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

/// The name in the slot at `at`: up to its first NUL, at most 128 bytes.
/// The slot's 129th byte is never part of it (it carries a composite key's
/// layout, [`ForeignKeyRecord`]); every build has read a name this way.
fn read_name(raw: &[u8], at: usize) -> Result<String, String> {
    let end = raw[at..at + 128]
        .iter()
        .position(|&b| b == 0)
        .unwrap_or(128);
    std::str::from_utf8(&raw[at..at + end])
        .map(str::to_string)
        .map_err(|e| format!("invalid UTF-8 in foreign key record: {}", e))
}

/// The name PostgreSQL gives an unnamed foreign key on `columns` of `table`:
/// `<table>_<col1>_<col2>…_fkey`, the stem cut so the label survives
/// (`unique_key::name_with_label`), as every derived name is built. A
/// collision is numbered by the caller (`unique_key::choose_name`), which
/// knows the table's other constraints.
pub fn default_name(table: &str, columns: &[String]) -> String {
    crate::core::rql::unique_key::name_with_label(&key_stem(table, columns), FKEY_LABEL)
}

/// `<table>_<col1>_<col2>…`, what an unnamed foreign key's `_fkey` follows.
pub fn key_stem(table: &str, columns: &[String]) -> String {
    crate::core::rql::unique_key::key_stem(table, columns)
}

/// The label of an unnamed foreign key's name.
pub const FKEY_LABEL: &str = "_fkey";

/// Name of the sidecar holding a table's foreign keys.
fn sidecar_name<S: StorageIO>(schema_io: &Arc<S>) -> String {
    format!("{}_{}", schema_io.get_name().replace(".db", ""), "FK.db")
}

/// Every record of a table's sidecar, slot by slot: `None` for a slot that
/// is unwritten (a record of zeros is an append a power cut kept the length
/// of and dropped, `torn_tail::is_unwritten`) or unreadable.
async fn read_records<S: StorageIO>(schema_io: &Arc<S>) -> Vec<Option<ForeignKeyRecord>> {
    let io = schema_io.create_new(sidecar_name(schema_io)).await;
    let len = io.get_len().await;
    let count = len / FOREIGN_KEY_SIZE;
    let mut out = Vec::with_capacity(count as usize);
    let mut buffer = vec![0u8; FOREIGN_KEY_SIZE as usize];
    for i in 0..count {
        let mut position = i * FOREIGN_KEY_SIZE;
        if io.read_data_into_buffer(&mut position, &mut buffer).await.is_err() {
            break;
        }
        if crate::core::torn_tail::is_unwritten(&buffer) {
            out.push(None);
            continue;
        }
        match ForeignKeyRecord::from_vec(&buffer) {
            Ok(record) => out.push(Some(record)),
            Err(e) => {
                log::error!("Skipping unreadable foreign key record: {}", e);
                out.push(None);
            }
        }
    }
    out
}

/// Reads a table's foreign keys.
pub async fn load<S: StorageIO>(schema_io: Arc<S>) -> Vec<ForeignKey> {
    let records = read_records(&schema_io).await;
    assemble(&records, false).unwrap_or_default()
}

/// Whether `raw`, a table's whole foreign-key sidecar, holds a live pair of
/// a composite key: what a restored directory is fenced for
/// (`incompat::COMPOSITE_FOREIGN_KEY_V1`). Read from the bytes alone.
pub fn sidecar_holds_composite(raw: &[u8]) -> bool {
    raw.chunks_exact(FOREIGN_KEY_SIZE as usize).any(|record| {
        !crate::core::torn_tail::is_unwritten(record) && record[FLAGS_AT] & FLAG_COMPOSITE != 0 && record[518] == 0
    })
}

/// Appends a foreign key.
pub async fn append<S: StorageIO>(schema_io: Arc<S>, fk: &ForeignKey) -> io::Result<()> {
    let mut writes = crate::core::torn_tail::SidecarWrites::default();
    stage(&*schema_io, &mut writes, fk)?;
    writes.commit(&*schema_io).await
}

/// Adds `fk`'s records to `writes`, the statement's sidecar records, which
/// commit as one transaction (`torn_tail::SidecarWrites`): one log sync for
/// all of them, and after a cut all of them or none — a composite key's
/// pairs together.
pub fn stage<S: StorageIO>(
    schema_io: &S,
    writes: &mut crate::core::torn_tail::SidecarWrites,
    fk: &ForeignKey,
) -> io::Result<()> {
    writes.add(crate::core::torn_tail::sidecar_name(schema_io, "FK.db"), &fk.into_vec());
    Ok(())
}

/// Marks a foreign key deleted, by constraint name: every live record with
/// the name, a composite key's first pair first, so a cut between two
/// writes leaves the key dropped.
///
/// Returns whether one was found. The records keep their slots so the file
/// stays a fixed-stride array.
pub async fn remove<S: StorageIO>(schema_io: Arc<S>, name: &str) -> io::Result<bool> {
    let io = schema_io.create_new(sidecar_name(&schema_io)).await;
    let len = io.get_len().await;
    let stored = crate::core::rql::unique_key::stored_name(name);
    let count = len / FOREIGN_KEY_SIZE;
    let mut buffer = vec![0u8; FOREIGN_KEY_SIZE as usize];
    let mut found = false;
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
        let Ok(fk) = ForeignKeyRecord::from_vec(&buffer) else {
            continue;
        };
        if fk.is_deleted || !fk.name.eq_ignore_ascii_case(&stored) {
            continue;
        }
        // The tombstone alone, every other byte as it was. Through the log,
        // not `write_data_seek`: the record went in as a logged append, and
        // the replay at every open put an unlogged overwrite back — the
        // dropped constraint was enforced again.
        buffer[518] = 1;
        io.write_data(position, &buffer).await;
        found = true;
        // A one-column key is one record; a composite one goes on to its
        // other pairs.
        if fk.member.is_none() {
            break;
        }
    }

    Ok(found)
}

/// Marks deleted every live foreign key whose referenced table is `parent`
/// (compared as the checks compare it): `DROP TABLE parent CASCADE`, which
/// drops the constraints that depend on the table, never the tables that
/// hold them, as PostgreSQL does. Every pair of a composite key names the
/// parent, so all of them are marked. Returns how many keys it marked.
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
        let Ok(fk) = ForeignKeyRecord::from_vec(&buffer) else {
            continue;
        };
        if fk.is_deleted || !fk.parent_table.eq_ignore_ascii_case(parent) {
            continue;
        }
        // The tombstone alone, logged, as `remove` writes it.
        buffer[518] = 1;
        io.write_data(position, &buffer).await;
        if fk.member.is_none_or(|(position, _)| position == 0) {
            marked += 1;
        }
    }
    Ok(marked)
}

/// A column list, `a, "b c"`: each name unquoted, in order.
fn column_list(list: &str) -> Result<Vec<String>, String> {
    let columns: Vec<String> = crate::core::rql::unique_key::split_items(list)
        .into_iter()
        .map(|item| item.trim().trim_matches('"').to_string())
        .collect();
    if columns.iter().any(String::is_empty) {
        return Err("a foreign key's column list names an empty column".to_string());
    }
    Ok(columns)
}

/// The parenthesised list `text` starts with (after spaces), and what
/// follows it; `None` when `text` does not start with one.
fn leading_list(text: &str) -> Result<Option<(Vec<String>, &str)>, String> {
    let trimmed = text.trim_start();
    if !trimmed.starts_with('(') {
        return Ok(None);
    }
    let close = crate::core::rql::unique_key::list_close(trimmed)
        .ok_or_else(|| "a foreign key's column list is not closed".to_string())?;
    Ok(Some((column_list(&trimmed[1..close])?, &trimmed[close + 1..])))
}

/// Parses a `FOREIGN KEY (…) REFERENCES parent [(…)] [MATCH …] [ON DELETE …]
/// [ON UPDATE …]` clause, with `inline_column` supplying the referencing
/// column when the clause is written inline on a column definition.
///
/// Returns an error naming what could not be handled, so a migration fails
/// loudly rather than proceeding with a constraint that is not there.
///
/// `table` is the table the key is declared on: an unnamed key is called
/// [`default_name`]`(table, columns)`, PostgreSQL's `<table>_<a>_<b>_fkey`.
/// A given `name` is cut to its stored form (`unique_key::stored_name`).
///
/// What is checked against the tables (the columns exist, a unique key
/// covers the referenced ones, the lists are of one length, the types pair)
/// is the statement's to check, in PostgreSQL's order; this reads the text.
pub fn parse_clause(
    clause: &str,
    table: &str,
    name: Option<String>,
    inline_column: Option<&str>,
) -> Result<ForeignKey, String> {
    // A whole word outside parentheses: `FOREIGN KEY (references_id)` holds
    // the letters in its column list.
    let references_at = crate::core::rql::join::find_keyword(clause, "REFERENCES", 0)
        .ok_or_else(|| "a foreign key must name what it REFERENCES".to_string())?;

    // Referencing columns: from the `FOREIGN KEY (…)` list, or the column
    // the clause was written on.
    let columns = match inline_column {
        Some(c) => vec![c.trim_matches('"').to_string()],
        None => {
            let head = &clause[..references_at];
            let open = head
                .find('(')
                .ok_or_else(|| "FOREIGN KEY must list its columns".to_string())?;
            match leading_list(&head[open..])? {
                Some((columns, _)) => columns,
                None => return Err("FOREIGN KEY column list is not closed".to_string()),
            }
        }
    };
    if columns.is_empty() || columns.iter().any(String::is_empty) {
        return Err("foreign key names no referencing column".to_string());
    }

    // Referenced table, then its column list, if it has one. The relation is
    // the first name (schema-qualified or quoted); what follows (`MATCH
    // SIMPLE`, `DEFERRABLE`, the actions) is not part of it.
    let tail = clause[references_at + "REFERENCES".len()..].trim_start();
    let name_end = {
        let bytes = tail.as_bytes();
        let mut i = 0;
        let mut quoted = false;
        while i < bytes.len() {
            match bytes[i] {
                b'"' => quoted = !quoted,
                b'(' if !quoted => break,
                c if !quoted && c.is_ascii_whitespace() => break,
                _ => {}
            }
            i += 1;
        }
        i
    };
    let relation = &tail[..name_end];
    // The last part of a qualified name: after the last `.` outside quotes.
    let mut part_at = 0;
    let mut quoted = false;
    for (i, b) in relation.bytes().enumerate() {
        match b {
            b'"' => quoted = !quoted,
            b'.' if !quoted => part_at = i + 1,
            _ => {}
        }
    }
    let parent_table = relation[part_at..].trim_matches('"').to_string();
    if parent_table.is_empty() {
        return Err("REFERENCES names no relation".to_string());
    }
    // No column list: the parent's primary key, as in PostgreSQL. Left
    // empty here, where no schema is in reach, and filled in by the
    // statement under its lock on the parent.
    let (parent_columns, rest) = match leading_list(&tail[name_end..])? {
        Some((list, rest)) => {
            if list.is_empty() {
                return Err("REFERENCES lists no columns".to_string());
            }
            (list, rest)
        }
        None => (Vec::new(), &tail[name_end..]),
    };

    // `MATCH FULL | SIMPLE | PARTIAL`, before the actions.
    let actions_at = crate::core::rql::join::find_keyword(rest, "ON", 0).unwrap_or(rest.len());
    let mut match_full = false;
    if let Some(at) = crate::core::rql::join::find_keyword(&rest[..actions_at], "MATCH", 0) {
        let kind = rest[at + "MATCH".len()..].split_whitespace().next().unwrap_or("");
        match kind.to_ascii_uppercase().as_str() {
            "FULL" => match_full = true,
            "SIMPLE" => {}
            "PARTIAL" => return Err("MATCH PARTIAL not yet implemented".to_string()),
            _ => return Err(format!("syntax error at or near \"{kind}\": MATCH takes FULL, SIMPLE or PARTIAL")),
        }
    }

    let actions = &rest[actions_at..];
    let (on_delete, on_delete_columns) = parse_action(actions, "ON DELETE")?;
    let (on_update, on_update_columns) = parse_action(actions, "ON UPDATE")?;
    if on_update_columns.is_some() {
        let what = if on_update == ReferentialAction::SetDefault { "SET DEFAULT" } else { "SET NULL" };
        return Err(format!("a column list with {what} is only supported for ON DELETE actions"));
    }
    let mut on_delete_columns = on_delete_columns.unwrap_or_default();
    // A list naming every key column is no list; a one-column key's only
    // possible list names its column.
    if on_delete_columns.iter().all(|c| columns.iter().any(|k| k.eq_ignore_ascii_case(c)))
        && columns.iter().all(|k| on_delete_columns.iter().any(|c| c.eq_ignore_ascii_case(k)))
    {
        on_delete_columns.clear();
    }

    Ok(ForeignKey {
        name: match name {
            Some(name) => crate::core::rql::unique_key::stored_name(&name),
            None => default_name(table, &columns),
        },
        columns,
        parent_table,
        parent_columns,
        on_delete,
        on_update,
        match_full,
        on_delete_columns,
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

/// Why `fk` cannot reference `parent`, in PostgreSQL's words and order, or
/// `None`:
///
/// - a referenced column is not a live column of the parent (42703,
///   "column … referenced in foreign key constraint does not exist");
/// - a referenced column is listed twice (42830);
/// - no `UNIQUE` or `PRIMARY KEY` of the parent is exactly the referenced
///   columns, in any order (42830, "there is no unique constraint matching
///   given keys for referenced table …"; a partial unique index is no such
///   constraint);
/// - the two column lists are of different lengths (42830);
/// - the parent is **partitioned** and the referenced columns do not hold
///   every column of its partition key, so a check could not route to one
///   partition (42830; phase 3 P4 for one column, which must then be the
///   whole one-column key; any number of columns since composite keys reach
///   partitioned tables). A unique key of a partitioned table holds every
///   partition key column (§1b), so only a record written behind the DDL
///   gets here.
///
/// A plain parent's column must be unique too, as in PostgreSQL. It once
/// need not be (docs/partitioning.md, "Phase 3 P4 as built", divergences),
/// and a delete of one of two parent rows sharing the value then acted on
/// children the other still covered. Keys declared then stay on disk and are
/// enforced as they were; only a new declaration is refused.
pub fn referenced_key_refusal(fk: &ForeignKey, parent: &ReferencedSide<'_>) -> Option<String> {
    let live = |column: &str| {
        parent
            .fields
            .iter()
            .find(|(name, deleted, _)| !*deleted && name.eq_ignore_ascii_case(column))
            .map(|&(_, _, flagged)| flagged)
    };
    let mut flags = Vec::with_capacity(fk.parent_columns.len());
    for column in &fk.parent_columns {
        match live(column) {
            Some(flagged) => flags.push(flagged),
            None => return Some(format!("column \"{column}\" referenced in foreign key constraint does not exist")),
        }
    }
    let not_unique = || {
        format!(
            "there is no unique constraint matching given keys for referenced table \"{}\"",
            parent.table
        )
    };
    let unique = match fk.parent_columns.as_slice() {
        [column] => {
            flags[0]
                || parent.primary_key.is_some_and(|p| p.eq_ignore_ascii_case(column))
                || parent.unique_keys.iter().any(|k| {
                    !k.is_deleted && k.predicate.is_none() && k.columns.len() == 1 && k.columns[0].eq_ignore_ascii_case(column)
                })
        }
        columns => {
            let duplicated =
                columns.iter().enumerate().any(|(i, c)| columns[..i].iter().any(|d| d.eq_ignore_ascii_case(c)));
            if duplicated {
                return Some("foreign key referenced-columns list must not contain duplicates".to_string());
            }
            // The same set of columns, in any order, as PostgreSQL matches a
            // referenced list against a unique index.
            parent.unique_keys.iter().any(|k| {
                !k.is_deleted
                    && k.predicate.is_none()
                    && k.columns.len() == columns.len()
                    && columns.iter().all(|c| k.columns.iter().any(|kc| kc.eq_ignore_ascii_case(c)))
            })
        }
    };
    if !unique {
        return Some(not_unique());
    }
    if fk.columns.len() != fk.parent_columns.len() {
        return Some("number of referencing and referenced columns for foreign key disagree".to_string());
    }
    if let Some(key) = &parent.partition_key {
        // Every partition key column among the referenced ones, so each
        // parent lookup hashes to one partition: PostgreSQL's rule, since a
        // unique key of a partitioned table holds every partition key
        // column (§1b). For one column: the whole one-column key.
        let covered = key.iter().all(|k| fk.parent_columns.iter().any(|c| c.eq_ignore_ascii_case(k)));
        if !covered {
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
/// type `parent`, by PostgreSQL's rule (`ATAddForeignKeyConstraint`) as far
/// as this engine's types carry it: the pair has an equality operator in the
/// referenced key's operator family, or the child's type casts **implicitly**
/// to the parent's.
///
/// - one operator family, any direction: the integer widths; `REAL` and
///   `DOUBLE PRECISION`; `DATE` and `TIMESTAMP`; the text types (`TEXT`,
///   `VARCHAR`, `CHAR`);
/// - an implicit cast, child to parent only: an integer to `NUMERIC`, `REAL`
///   or `DOUBLE`; a `NUMERIC` to `REAL` or `DOUBLE`;
/// - any other type with itself only (`UUID`, `BOOL`, `JSONB`, `BYTEA`, …).
///
/// PostgreSQL refuses the reverse casts (a `NUMERIC` or `DOUBLE` child of an
/// integer key, a `DOUBLE` child of a `NUMERIC` key): no implicit cast runs
/// that way. Everything it accepts is accepted here (P4 round 3: the first
/// rule refused `INT` onto `NUMERIC`, which an import of a PostgreSQL schema
/// may hold).
pub fn compatible_key_types(child: &crate::core::db_type::DbType, parent: &crate::core::db_type::DbType) -> bool {
    use crate::core::db_type::DbType::*;
    #[derive(PartialEq)]
    enum F {
        Int,
        Float,
        Numeric,
        Text,
        Time,
        Other,
    }
    let family = |t: &crate::core::db_type::DbType| match t {
        I8 | I16 | I32 | I64 | I128 | U8 | U16 | U32 | U64 | U128 => F::Int,
        F32 | F64 => F::Float,
        DECIMAL => F::Numeric,
        STRING | CHAR => F::Text,
        DATE | DATETIME | TIMESTAMP => F::Time,
        _ => F::Other,
    };
    match (family(child), family(parent)) {
        (F::Other, F::Other) => child == parent,
        (F::Int, F::Numeric) | (F::Int, F::Float) | (F::Numeric, F::Float) => true,
        (a, b) => a == b && a != F::Other,
    }
}

/// `text`, a key literal as `key_literal` renders one of type `ty`, in a form
/// two types' renderings of one value share (P4 round 3): a number with no
/// sign on zero, no leading or trailing zeros (`7`, `7.00` and `7.0` alike;
/// a float's shortest rendering, `0.1`), a date or timestamp as its
/// microseconds (a date at its midnight), text as written. Two values are
/// equal when their forms are; a coercion that changes the form lost the
/// value (`7.5` to an integer, a non-midnight timestamp to a date).
pub fn canonical_key_text(text: &str, ty: &crate::core::db_type::DbType) -> String {
    use crate::core::db_type::DbType::*;
    let bare = crate::core::rql::column_default::strip_redundant_cast(text.trim());
    let bare = bare.strip_prefix('\'').and_then(|t| t.strip_suffix('\'')).map(|t| t.replace("''", "'")).unwrap_or_else(|| bare.to_string());
    match ty {
        I8 | I16 | I32 | I64 | I128 | U8 | U16 | U32 | U64 | U128 | F32 | F64 | DECIMAL => canonical_number(&bare),
        DATE | DATETIME => match crate::core::value_codec::parse_timestamp_us(&bare) {
            Some(us) => format!("us:{us}"),
            None => bare,
        },
        CHAR => bare.trim_end().to_string(),
        _ => bare,
    }
}

/// A decimal number's canonical text; anything that is not a number (`NaN`,
/// `Infinity`) is returned as written, so it is equal only to itself.
///
/// An exponent is read as the number it writes: a float's text is
/// PostgreSQL's (`9.007199254740992e+15`, `1e-07`), which must meet the
/// integer or decimal it equals (`9007199254740992`).
fn canonical_number(text: &str) -> String {
    let t = text.trim();
    if t.contains(['e', 'E']) {
        return match crate::core::rql::expr::decimal_digits(t) {
            Some((m, e)) if (-400..=400).contains(&e) => {
                let digits = m.unsigned_abs().to_string();
                let plain = if e >= 0 {
                    format!("{digits}{}", "0".repeat(e as usize))
                } else {
                    let places = e.unsigned_abs() as usize;
                    let padded = format!("{}{digits}", "0".repeat(places.saturating_sub(digits.len()) + 1));
                    let (int, frac) = padded.split_at(padded.len() - places);
                    format!("{int}.{frac}")
                };
                canonical_number(&if m < 0 { format!("-{plain}") } else { plain })
            }
            _ => t.to_string(),
        };
    }
    let (negative, digits) = match t.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, t.strip_prefix('+').unwrap_or(t)),
    };
    let (int, frac) = digits.split_once('.').unwrap_or((digits, ""));
    if int.is_empty() && frac.is_empty()
        || !int.bytes().all(|b| b.is_ascii_digit())
        || !frac.bytes().all(|b| b.is_ascii_digit())
    {
        return t.to_string();
    }
    let int = int.trim_start_matches('0');
    let frac = frac.trim_end_matches('0');
    let int = if int.is_empty() { "0" } else { int };
    let body = if frac.is_empty() { int.to_string() } else { format!("{int}.{frac}") };
    if negative && body != "0" { format!("-{body}") } else { body }
}

/// PostgreSQL's refusal (42804) of `fk` when the type `child` of its
/// referencing column `at` cannot reference the type `parent` of the
/// referenced column paired with it ([`compatible_key_types`]). Each pair of
/// a composite key is judged on its own, as PostgreSQL judges it.
pub fn type_refusal(
    fk: &ForeignKey,
    at: usize,
    child: &crate::core::db_type::DbType,
    parent: &crate::core::db_type::DbType,
) -> Option<String> {
    (!compatible_key_types(child, parent)).then(|| {
        format!(
            "foreign key constraint \"{}\" cannot be implemented: key columns \"{}\" and \"{}\" are of \
             incompatible types: {:?} and {:?}",
            fk.name,
            fk.columns.get(at).map(String::as_str).unwrap_or(""),
            fk.parent_columns.get(at).map(String::as_str).unwrap_or(""),
            child,
            parent
        )
    })
}

/// Reads the action after `keyword`, defaulting to `NO ACTION` as SQL does,
/// with the column list a `SET NULL` or `SET DEFAULT` may carry
/// (PostgreSQL 15's `ON DELETE SET NULL (a)`).
fn parse_action(actions: &str, keyword: &str) -> Result<(ReferentialAction, Option<Vec<String>>), String> {
    let upper = actions.to_ascii_uppercase();
    let Some(at) = upper.find(keyword) else {
        return Ok((ReferentialAction::NoAction, None));
    };

    let rest = actions[at + keyword.len()..].trim_start();
    // The words of the action, each with where it ends in `rest`.
    let mut words: Vec<(&str, usize)> = Vec::new();
    let mut from = 0;
    while words.len() < 2 {
        let skipped = rest[from..].len() - rest[from..].trim_start().len();
        let start = from + skipped;
        let len = rest[start..]
            .find(|c: char| c.is_whitespace() || c == '(' || c == ',' || c == ')')
            .unwrap_or(rest.len() - start);
        if len == 0 {
            break;
        }
        words.push((&rest[start..start + len], start + len));
        from = start + len;
    }
    // The longest spellings are two words, so both are tried before one.
    for take in [2usize, 1] {
        if words.len() < take {
            continue;
        }
        let text = words[..take].iter().map(|(w, _)| *w).collect::<Vec<_>>().join(" ");
        let Some(action) = ReferentialAction::parse(&text) else {
            continue;
        };
        let after = &rest[words[take - 1].1..];
        let list = match leading_list(after)? {
            Some((list, _)) => {
                if !matches!(action, ReferentialAction::SetNull | ReferentialAction::SetDefault) {
                    return Err(format!("syntax error at or near \"(\": {keyword} {text} takes no column list"));
                }
                Some(list)
            }
            None => None,
        };
        return Ok((action, list));
    }

    Err(format!(
        "{} {} is not supported yet",
        keyword,
        words.first().map(|(w, _)| *w).unwrap_or("")
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
        assert_eq!(fk.columns, ["domain_id"]);
        assert_eq!(fk.parent_table, "domain");
        assert_eq!(fk.parent_columns, ["id"]);
        assert_eq!(fk.on_delete, ReferentialAction::Cascade);
        // Unstated actions default to NO ACTION, as in SQL.
        assert_eq!(fk.on_update, ReferentialAction::NoAction);
    }

    #[test]
    fn parses_an_inline_column_clause() {
        let fk = parse_clause(r#"REFERENCES "domain" ("id")"#, "t", None, Some("domain_id")).unwrap();
        assert_eq!(fk.columns, ["domain_id"]);
        assert_eq!(fk.parent_table, "domain");
        assert_eq!(fk.parent_columns, ["id"]);
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
        assert_eq!(fk.columns, ["domain_id"]);
        assert_eq!(fk.parent_table, "domain");
    }

    /// A composite key is read whole, its columns in order on both sides,
    /// with `MATCH` and an `ON DELETE SET … (cols)` list; never as its first
    /// column (which would enforce something weaker than what was declared).
    #[test]
    fn composite_keys_are_read_whole() {
        let fk = parse_clause("FOREIGN KEY (a, \"b c\") REFERENCES public.t (\"x\", y)", "c", None, None).unwrap();
        assert_eq!((fk.columns.as_slice(), fk.parent_table.as_str()), (&["a".to_string(), "b c".to_string()][..], "t"));
        assert_eq!(fk.parent_columns, ["x", "y"]);
        assert_eq!(fk.name, "c_a_b c_fkey");
        assert!(fk.single().is_none() && fk.is_composite());
        assert!(!fk.match_full);
        // No column list: the parent's primary key, filled in later.
        let fk = parse_clause("FOREIGN KEY (a,b) REFERENCES p MATCH FULL ON DELETE SET NULL (b)", "c", None, None).unwrap();
        assert!(fk.parent_columns.is_empty() && fk.match_full, "{fk:?}");
        assert_eq!((fk.on_delete, fk.on_delete_columns.as_slice()), (ReferentialAction::SetNull, &["b".to_string()][..]));
        // A list naming every key column is no list.
        let fk = parse_clause("FOREIGN KEY (a, b) REFERENCES p ON DELETE SET DEFAULT (b, a)", "c", None, None).unwrap();
        assert!(fk.on_delete_columns.is_empty());
        let fk = parse_clause("FOREIGN KEY (a, b) REFERENCES p(x,y) MATCH SIMPLE ON UPDATE CASCADE", "c", None, None).unwrap();
        assert_eq!((fk.parent_columns.len(), fk.on_update, fk.match_full), (2, ReferentialAction::Cascade, false));
        // PostgreSQL's refusals of the text alone.
        let partial = parse_clause("FOREIGN KEY (a, b) REFERENCES p MATCH PARTIAL", "c", None, None).unwrap_err();
        assert_eq!(partial, "MATCH PARTIAL not yet implemented");
        let on_update = parse_clause("FOREIGN KEY (a, b) REFERENCES p ON UPDATE SET NULL (b)", "c", None, None).unwrap_err();
        assert!(on_update.contains("only supported for ON DELETE actions"), "{on_update}");
        assert!(parse_clause("FOREIGN KEY (a, b) REFERENCES p ON DELETE CASCADE (b)", "c", None, None).is_err());
    }

    /// The lists must pair up, and a list may not name a column outside the
    /// key: refused before a record could be written.
    #[test]
    fn a_composite_key_must_pair_its_columns_to_be_stored() {
        let mut fk = parse_clause("FOREIGN KEY (a, b) REFERENCES p (x)", "c", None, None).unwrap();
        assert!(fk.storable().unwrap_err().contains("disagree"));
        fk.parent_columns = vec!["x".into(), "y".into()];
        assert!(fk.storable().is_ok());
        fk.on_delete_columns = vec!["z".into()];
        assert!(fk.storable().unwrap_err().contains("must be part of foreign key"));
        fk.on_delete_columns.clear();
        fk.columns = (0..9).map(|i| format!("c{i}")).collect();
        fk.parent_columns = fk.columns.clone();
        assert!(fk.storable().unwrap_err().contains("cannot have more than 8 keys"));
    }

    #[test]
    fn malformed_clauses_name_the_problem() {
        assert!(parse_clause("FOREIGN KEY (a)", "t", None, None).is_err());
        // No referenced column: the parent's primary key, filled in later;
        // until then the key cannot be stored.
        let open = parse_clause("FOREIGN KEY (a) REFERENCES t", "t", None, None).unwrap();
        assert_eq!(open.parent_table.as_str(), "t");
        assert!(open.parent_columns.is_empty());
        assert!(open.storable().unwrap_err().contains("no primary key"));
        for clause in ["FOREIGN KEY (a) REFERENCES t MATCH SIMPLE", "FOREIGN KEY (a) REFERENCES public.t DEFERRABLE", "FOREIGN KEY (a) REFERENCES \"t\""] {
            assert_eq!(parse_clause(clause, "c", None, None).unwrap().parent_table, "t", "{clause}");
        }
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
        fk.columns = vec!["Ünit col".to_string()];
        fk.parent_table = "親".to_string();
        fk.parent_columns = vec!["id é".to_string()];
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
        assert_eq!((fk.name.as_str(), fk.columns[0].as_str(), fk.parent_table.as_str()), ("myfk", "a", "p"));
    }

    /// PostgreSQL's pairs (P4 round 3): what has an operator or an implicit
    /// cast child to parent is accepted, the reverse casts are not.
    #[test]
    fn key_types_pair_as_postgres_pairs_them() {
        use crate::core::db_type::DbType::*;
        for (child, parent) in [
            (I32, I64), (I64, I16), (I32, DECIMAL), (I64, F64), (I16, F32), (DECIMAL, F64), (DECIMAL, DECIMAL),
            (F64, F32), (F32, F64), (DATE, DATETIME), (DATETIME, DATE), (STRING, CHAR), (UUID, UUID), (BOOL, BOOL),
        ] {
            assert!(compatible_key_types(&child, &parent), "{child:?} -> {parent:?}");
        }
        for (child, parent) in [
            (DECIMAL, I32), (F64, I64), (F64, DECIMAL), (STRING, I32), (UUID, STRING), (BOOL, I32), (DATE, STRING),
            (JSONB, STRING), (BYTEA, STRING),
        ] {
            assert!(!compatible_key_types(&child, &parent), "{child:?} -> {parent:?}");
        }
    }

    #[test]
    fn canonical_forms_meet_across_types() {
        use crate::core::db_type::DbType::*;
        assert_eq!(canonical_key_text("7", &I32), canonical_key_text("7.00", &DECIMAL));
        assert_eq!(canonical_key_text("-3", &I64), canonical_key_text("-3.0", &DECIMAL));
        assert_ne!(canonical_key_text("7", &I32), canonical_key_text("7.50", &DECIMAL));
        assert_eq!(canonical_key_text("-0.00", &DECIMAL), "0");
        assert_eq!(canonical_key_text("'it''s'", &STRING), "it's");
        assert_eq!(canonical_key_text("'2024-01-01'", &DATE), canonical_key_text("'2024-01-01 00:00:00'", &DATETIME));
        assert_ne!(canonical_key_text("'2024-01-01'", &DATE), canonical_key_text("'2024-01-01 10:00:00'", &DATETIME));
        assert_ne!(canonical_key_text("9007199254740993", &I64), canonical_key_text("9007199254740992", &F64));
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
            columns: vec!["domain_id".to_string()],
            parent_table: "domain".to_string(),
            parent_columns: vec!["id".to_string()],
            on_delete: ReferentialAction::Cascade,
            on_update: ReferentialAction::Restrict,
            match_full: false,
            on_delete_columns: Vec::new(),
            is_deleted: false,
        }
    }

    fn composite() -> ForeignKey {
        ForeignKey {
            name: "c_a_b_fkey".to_string(),
            columns: vec!["a".to_string(), "b".to_string(), "c".to_string()],
            parent_table: "p".to_string(),
            parent_columns: vec!["x".to_string(), "y".to_string(), "z".repeat(127)],
            on_delete: ReferentialAction::SetNull,
            on_update: ReferentialAction::Cascade,
            match_full: true,
            on_delete_columns: vec!["b".to_string()],
            is_deleted: false,
        }
    }

    /// A composite key is one record per column pair, back to back, and
    /// reads back as it was written: columns in order, MATCH FULL, the
    /// `ON DELETE SET` list.
    #[test]
    fn a_composite_key_round_trips_through_its_records() {
        let fk = composite();
        let raw = fk.into_vec();
        assert_eq!(raw.len(), 3 * FOREIGN_KEY_SIZE as usize);
        assert_eq!(ForeignKey::from_vec(&raw).unwrap(), fk);
        let records: Vec<Option<ForeignKeyRecord>> =
            raw.chunks(FOREIGN_KEY_SIZE as usize).map(|r| ForeignKeyRecord::from_vec(r).ok()).collect();
        assert_eq!(assemble(&records, true).unwrap(), vec![fk.clone()]);
        // A one-column key's record is the one every build writes: its spare
        // bytes zero but for MATCH FULL's flag.
        let single = sample().into_vec();
        assert_eq!(single.len(), FOREIGN_KEY_SIZE as usize);
        assert!([128usize, 257, 386, 515].iter().all(|&at| single[at] == 0));
        assert!(sidecar_holds_composite(&raw) && !sidecar_holds_composite(&single));
    }

    /// What a build that predates composite keys reads from one: every pair
    /// as a one-column key whose referenced column no table has (the marker
    /// byte first), so its checks fail naming that column instead of
    /// enforcing `a` alone against `x`.
    #[test]
    fn an_older_build_reads_a_composite_pair_as_a_key_onto_no_column() {
        let raw = composite().into_vec();
        // The record reader every build before this one had.
        let old_read_name = |raw: &[u8], at: usize| {
            let end = raw[at..at + 129].iter().position(|&b| b == 0).unwrap_or(128);
            String::from_utf8(raw[at..at + end].to_vec()).unwrap()
        };
        for (i, record) in raw.chunks(FOREIGN_KEY_SIZE as usize).enumerate() {
            assert_eq!(old_read_name(record, 0), "c_a_b_fkey");
            assert_eq!(old_read_name(record, 129), ["a", "b", "c"][i]);
            assert_eq!(old_read_name(record, 258), "p");
            let parent = old_read_name(record, 387);
            assert!(parent.starts_with('\u{1}'), "pair {i}'s referenced column reads as {parent:?}");
            assert_ne!(parent.trim_start_matches('\u{1}'), "", "pair {i}");
            assert_eq!(record[518], 0, "live");
        }
    }

    /// Damage is never read as a weaker key: a first pair whose other pairs
    /// are missing or dropped alone is refused when strict, and left out
    /// with an error when loading; a dropped first pair drops the key.
    #[test]
    fn a_damaged_composite_key_is_never_read_as_a_shorter_one() {
        let records: Vec<Option<ForeignKeyRecord>> = composite().records().into_iter().map(Some).collect();
        let mut short = records.clone();
        short.truncate(2);
        assert!(assemble(&short, true).unwrap_err().contains("2 of its 3"));
        assert!(assemble(&short, false).unwrap().is_empty());
        let mut one_dropped = records.clone();
        one_dropped[1].as_mut().unwrap().is_deleted = true;
        assert!(assemble(&one_dropped, true).is_err());
        assert!(assemble(&one_dropped, false).unwrap().is_empty());
        let mut head_dropped = records.clone();
        head_dropped[0].as_mut().unwrap().is_deleted = true;
        assert!(assemble(&head_dropped, false).unwrap().is_empty());
        // The rest of a key whose first pair an older build dropped alone.
        assert!(assemble(&head_dropped, true).unwrap().is_empty());
        // A key around another one-column key reads both.
        let mut mixed: Vec<Option<ForeignKeyRecord>> = sample().records().into_iter().map(Some).collect();
        mixed.extend(records.iter().cloned());
        mixed.push(None);
        mixed.extend(sample().records().into_iter().map(Some));
        assert_eq!(assemble(&mixed, true).unwrap().len(), 3);
        // A flag this build does not know is no key at all.
        let mut raw = sample().into_vec();
        raw[128] = 0x80;
        assert!(ForeignKeyRecord::from_vec(&raw).is_err());
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
