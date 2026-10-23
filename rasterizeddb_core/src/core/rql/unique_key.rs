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
//!
//! # Partial keys
//!
//! `CREATE UNIQUE INDEX … ON t (cols) WHERE pred` is a key with a predicate:
//! the columns must be unique among the rows that satisfy `pred`, and a row
//! outside it constrains nothing. A soft delete (`SET deleted_at = now()`
//! under `WHERE deleted_at IS NULL`) is the common case, and it frees the key.
//!
//! The predicate is evaluated as a `WHERE` is: a row is inside only when the
//! predicate is *true*, so a NULL comparison leaves the row out. It is lowered
//! against the whole schema, companions included, so that `deleted_at IS
//! NULL` reads the column's NULL flag. Lowered against the visible columns
//! alone, the rewriter takes every column to be NOT NULL and folds that test
//! to false, which would have made the index cover nothing.
//!
//! A partial key takes two records in the sidecar: the key itself, flagged
//! partial, and the predicate's text right behind it. Both flags are non-zero
//! in the byte older builds read as the tombstone, so a build that predates
//! partial keys skips them rather than enforcing the key over every row.
//!
//! Only the uniqueness is partial. The index the key brings (see
//! [`crate::core::index::for_schema`]) holds every row, so the planner may use
//! it for any query, as it would any other index: it can never miss a row a
//! query asks for.

use std::io;

use rclite::Arc;

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

/// Longest predicate a partial key can store: the column slots of one record.
pub const MAX_PREDICATE_LEN: usize = 129 * MAX_COLUMNS;

/// The flag byte of a record. Older builds read any non-zero value as a
/// tombstone, which is what keeps them from mistaking a partial key for a
/// full one.
const FLAG_LIVE: u8 = 0;
const FLAG_DELETED: u8 = 1;
const FLAG_PARTIAL: u8 = 2;
const FLAG_PREDICATE: u8 = 3;

/// One table-level `UNIQUE` constraint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UniqueKey {
    pub name: String,
    /// The columns the constraint spans, in the order written. One column is
    /// an ordinary unique constraint; several make a composite one.
    pub columns: Vec<String>,
    pub is_deleted: bool,
    /// The `WHERE` of a partial unique index, as written. `None` for an
    /// ordinary constraint, which covers every row.
    pub predicate: Option<String>,
    /// The predicate compiled against the table's columns, made on first use.
    pub compiled: CompiledPredicate,
}

/// A partial key's predicate, compiled once per loaded schema.
///
/// A clone starts empty rather than sharing the compiled form: the condition
/// names columns by `write_order`, and a clone may be carried into a table
/// whose columns are numbered differently (an `ALTER` rewrite does exactly
/// that). Compiling again there costs one parse; sharing would read the wrong
/// columns.
///
/// The flag records that a predicate which does not compile has been reported.
/// The check runs on every write, and a warning per row buried the one line
/// that mattered.
#[derive(Default)]
pub struct CompiledPredicate(
    std::sync::OnceLock<Result<crate::core::rql::expr::Cond, String>>,
    std::sync::atomic::AtomicBool,
);

impl CompiledPredicate {
    /// True the first time it is asked, and false from then on.
    fn first_report(&self) -> bool {
        !self.1.swap(true, std::sync::atomic::Ordering::Relaxed)
    }
}

impl Clone for CompiledPredicate {
    fn clone(&self) -> Self {
        CompiledPredicate::default()
    }
}

impl PartialEq for CompiledPredicate {
    fn eq(&self, _: &Self) -> bool {
        // A cache: two keys with the same predicate text are the same key.
        true
    }
}

impl Eq for CompiledPredicate {}

impl std::fmt::Debug for CompiledPredicate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CompiledPredicate")
    }
}

/// Compiles a partial key's predicate against a table's columns.
///
/// Lowered by the same rewriter a `WHERE` goes through, against the whole
/// schema, so a nullable column is guarded by its companion and `IS NULL`
/// reads the flag; see the module notes for what goes wrong otherwise. What
/// the condition evaluator cannot read (a function call, `LIKE`) is an error,
/// which `CREATE UNIQUE INDEX` reports rather than storing a key it could not
/// enforce.
pub fn compile_predicate(
    text: &str,
    fields: &smallvec::SmallVec<[crate::core::row::schema::SchemaField; 20]>,
) -> Result<crate::core::rql::expr::Cond, String> {
    let live: Vec<crate::core::row::schema::SchemaField> =
        fields.iter().filter(|f| !f.is_deleted).cloned().collect();
    let live_sv: smallvec::SmallVec<[crate::core::row::schema::SchemaField; 20]> =
        live.iter().cloned().collect();
    let lowered = crate::core::tokenizer::predicate_rewrite::rewrite_where(text, &live_sv)?;
    crate::core::rql::expr::parse_cond(&lowered, &live)
        .map_err(|e| format!("the predicate '{}' is not supported in a unique index: {}", text, e))
}

impl UniqueKey {
    /// An ordinary constraint over `columns`.
    pub fn new(name: String, columns: Vec<String>) -> Self {
        UniqueKey {
            name,
            columns,
            is_deleted: false,
            predicate: None,
            compiled: CompiledPredicate::default(),
        }
    }

    /// The predicate, compiled against `fields`; `Ok(None)` for a key that
    /// covers every row. The fields must be the ones of the schema this key
    /// belongs to: the compiled form is kept for the life of the key.
    pub fn condition(
        &self,
        fields: &smallvec::SmallVec<[crate::core::row::schema::SchemaField; 20]>,
    ) -> Result<Option<&crate::core::rql::expr::Cond>, String> {
        let Some(text) = self.predicate.as_deref() else {
            return Ok(None);
        };
        match self.compiled.0.get_or_init(|| compile_predicate(text, fields)) {
            Ok(cond) => Ok(Some(cond)),
            Err(e) => Err(e.clone()),
        }
    }

    /// Whether `row` is one the key constrains: always for an ordinary key,
    /// and for a partial one when its predicate is true of the row. `row`
    /// must carry every column the predicate reads, companions included,
    /// keyed by `write_order`; a column it lacks reads as NULL.
    pub fn covers(
        &self,
        row: &crate::core::row::row::Row,
        fields: &smallvec::SmallVec<[crate::core::row::schema::SchemaField; 20]>,
    ) -> Result<bool, String> {
        match self.condition(fields)? {
            None => Ok(true),
            Some(cond) => crate::core::rql::expr::eval_cond(cond, row, fields),
        }
    }

    /// The columns, by `write_order`, whose value can move a row into or out
    /// of the predicate: the ones it reads, and each one's other half of a
    /// value and its NULL flag. A statement assigning `deleted_at = NULL` may
    /// carry either half, and the lowered predicate reads only the flag, so
    /// both are listed or an undelete could slip past the check unseen.
    pub fn predicate_columns(
        &self,
        fields: &smallvec::SmallVec<[crate::core::row::schema::SchemaField; 20]>,
    ) -> Result<smallvec::SmallVec<[u64; 8]>, String> {
        let mut out = smallvec::SmallVec::new();
        let Some(cond) = self.condition(fields)? else {
            return Ok(out);
        };
        for field in crate::core::rql::expr::cond_columns(cond) {
            let owner = field
                .name
                .get(crate::core::rql::nullable::NULL_FLAG_PREFIX.len()..)
                .filter(|_| crate::core::rql::nullable::is_hidden(&field.name));
            let column = owner.unwrap_or(&field.name);
            for f in fields.iter().filter(|f| !f.is_deleted) {
                if f.name.eq_ignore_ascii_case(column)
                    || crate::core::rql::nullable::is_flag_name_of(&f.name, column)
                {
                    out.push(f.write_order);
                }
            }
        }
        Ok(out)
    }

    /// The predicate as `pg_indexes` prints it: in parentheses, unless it is a
    /// bare column.
    pub fn predicate_display(&self) -> Option<String> {
        let text = self.predicate.as_deref()?.trim();
        let inner = strip_outer_parens(text);
        let bare = !inner.is_empty()
            && inner.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '"');
        Some(if bare {
            inner.to_string()
        } else {
            format!("({})", inner)
        })
    }
}

/// What a uniqueness check needs to know about `key` for one write.
///
/// - `Ok(None)`: the key cannot be violated by this row, because the row is
///   outside its predicate, or because the predicate no longer compiles (a
///   column it reads was dropped, which in PostgreSQL drops the index).
/// - `Ok(Some((None, _)))`: an ordinary key.
/// - `Ok(Some((Some(condition), columns)))`: a partial key the row falls
///   under, with the condition each existing candidate must also satisfy to
///   conflict, and the columns whose assignment brings a row under it.
/// - `Err`: the predicate could not be evaluated for this row. Refused rather
///   than guessed at either way: guessing "outside" stores a duplicate, and
///   guessing "inside" refuses a row PostgreSQL accepts.
#[allow(clippy::type_complexity)]
pub fn partial_check<'k>(
    key: &'k UniqueKey,
    fields: &smallvec::SmallVec<[crate::core::row::schema::SchemaField; 20]>,
    row_write: &crate::core::row::row::RowWrite,
) -> Result<Option<(Option<&'k crate::core::rql::expr::Cond>, smallvec::SmallVec<[u64; 8]>)>, String> {
    let condition = match key.condition(fields) {
        Ok(None) => return Ok(Some((None, smallvec::SmallVec::new()))),
        Ok(Some(condition)) => condition,
        Err(e) => {
            if key.compiled.first_report() {
                log::warn!(
                    "unique index {} has a predicate that no longer compiles ({}); not enforced",
                    key.describe(),
                    e
                );
            }
            return Ok(None);
        }
    };
    let row = written_row(&row_write.columns_writing_data);
    match crate::core::rql::expr::eval_cond(condition, &row, fields) {
        Ok(true) => Ok(Some((Some(condition), key.predicate_columns(fields)?))),
        Ok(false) => Ok(None),
        Err(e) => Err(format!(
            "could not evaluate the predicate of unique index {} for this row: {}",
            key.describe(),
            e
        )),
    }
}

/// The partial keys that `DROP COLUMN column` takes with it: those whose
/// predicate reads the column. PostgreSQL drops such an index along with the
/// column.
///
/// Left in place, the key went unenforced while `pg_indexes` still listed it,
/// and a column of the same name added later brought it back over values its
/// rows were never checked against. An `ADD COLUMN` rewrite then failed on
/// the first duplicate after the table had been dropped.
///
/// A key reads the column when the compiled predicate names it or its NULL
/// flag, or when the predicate stops compiling without it, which also
/// catches a reference the lowered condition does not keep. A predicate that
/// already fails to compile is left alone: it is not enforced now, and this
/// drop does not change what it reads.
pub fn dropped_with_column(
    keys: &[UniqueKey],
    fields: &smallvec::SmallVec<[crate::core::row::schema::SchemaField; 20]>,
    column: &str,
) -> Vec<String> {
    let dropped: smallvec::SmallVec<[u64; 2]> = fields
        .iter()
        .filter(|f| {
            !f.is_deleted
                && (f.name.eq_ignore_ascii_case(column)
                    || crate::core::rql::nullable::is_flag_name_of(&f.name, column))
        })
        .map(|f| f.write_order)
        .collect();
    let mut without = fields.clone();
    for f in without.iter_mut() {
        if dropped.contains(&f.write_order) {
            f.is_deleted = true;
        }
    }
    keys.iter()
        .filter(|k| !k.is_deleted)
        .filter(|k| {
            let Some(text) = k.predicate.as_deref() else {
                return false;
            };
            let Ok(cond) = compile_predicate(text, fields) else {
                return false;
            };
            crate::core::rql::expr::cond_columns(&cond)
                .iter()
                .any(|f| dropped.contains(&f.write_order))
                || compile_predicate(text, &without).is_err()
        })
        .map(|k| k.name.clone())
        .collect()
}

/// The row a write is about to store, in the shape [`UniqueKey::covers`]
/// reads: one cell per payload, keyed by `write_order`.
pub fn written_row(payloads: &[crate::core::row::row::ColumnWritePayload]) -> crate::core::row::row::Row {
    let mut row = crate::core::row::row::Row::default();
    for payload in payloads {
        row.columns.push(crate::core::row::row::Column {
            schema_id: crate::core::row::row::schema_id_of(payload.write_order as u64),
            data: payload.data.clone(),
            column_type: payload.column_type.clone(),
        });
    }
    row
}

/// `text` without parentheses that enclose all of it.
fn strip_outer_parens(text: &str) -> &str {
    let mut text = text.trim();
    while text.starts_with('(') && text.ends_with(')') {
        // Only when the first parenthesis closes at the very end: `(a) OR
        // (b)` starts and ends with one but is not a group.
        let mut depth = 0i32;
        let mut quote: Option<char> = None;
        let mut closes_at = None;
        for (i, c) in text.char_indices() {
            match quote {
                Some(q) if c == q => quote = None,
                Some(_) => {}
                None => match c {
                    '\'' | '"' => quote = Some(c),
                    '(' => depth += 1,
                    ')' => {
                        depth -= 1;
                        if depth == 0 {
                            closes_at = Some(i);
                            break;
                        }
                    }
                    _ => {}
                },
            }
        }
        if closes_at != Some(text.len() - 1) {
            break;
        }
        text = text[1..text.len() - 1].trim();
    }
    text
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
        out[count_at + 1] = if self.is_deleted {
            FLAG_DELETED
        } else if self.predicate.is_some() {
            FLAG_PARTIAL
        } else {
            FLAG_LIVE
        };
        out
    }

    /// The record holding a partial key's predicate, which follows the key's
    /// own record. `None` for an ordinary key.
    fn predicate_record(&self) -> Option<Vec<u8>> {
        let text = self.predicate.as_deref()?;
        let mut out = vec![0u8; UNIQUE_KEY_SIZE as usize];
        write_name(&mut out, 0, &self.name);
        // Raw text in the column slots: `write_name` would strip every
        // character a predicate needs.
        let bytes = text.as_bytes();
        let len = bytes.len().min(MAX_PREDICATE_LEN);
        out[129..129 + len].copy_from_slice(&bytes[..len]);
        out[129 + 129 * MAX_COLUMNS + 1] = FLAG_PREDICATE;
        Some(out)
    }

    pub fn from_vec(raw: &[u8]) -> Result<Self, String> {
        match decode(raw)? {
            Record::Key(key) => Ok(key),
            Record::Predicate { name, .. } => {
                Err(format!("record for {} holds a predicate, not a key", name))
            }
        }
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

/// One record of the sidecar.
enum Record {
    /// A key. A partial one has `predicate: Some("")` until the record behind
    /// it supplies the text.
    Key(UniqueKey),
    /// The predicate of the partial key in the record before it.
    Predicate { name: String, text: String },
}

fn decode(raw: &[u8]) -> Result<Record, String> {
    if raw.len() != UNIQUE_KEY_SIZE as usize {
        return Err(format!(
            "invalid unique key record: expected {} bytes, got {}",
            UNIQUE_KEY_SIZE,
            raw.len()
        ));
    }
    let count_at = 129 + 129 * MAX_COLUMNS;
    let flag = raw[count_at + 1];
    let name = read_name(raw, 0)?;
    if flag == FLAG_PREDICATE {
        let region = &raw[129..129 + MAX_PREDICATE_LEN];
        let end = region.iter().position(|&b| b == 0).unwrap_or(region.len());
        let text = std::str::from_utf8(&region[..end])
            .map_err(|e| format!("invalid UTF-8 in the predicate of {}: {}", name, e))?
            .to_string();
        return Ok(Record::Predicate { name, text });
    }
    let count = (raw[count_at] as usize).min(MAX_COLUMNS);
    let mut columns = Vec::with_capacity(count);
    for i in 0..count {
        columns.push(read_name(raw, 129 + i * 129)?);
    }
    Ok(Record::Key(UniqueKey {
        name,
        columns,
        // Any other value is a tombstone, as older builds read every
        // non-zero one.
        is_deleted: flag != FLAG_LIVE && flag != FLAG_PARTIAL,
        predicate: (flag == FLAG_PARTIAL).then(String::new),
        compiled: CompiledPredicate::default(),
    }))
}

/// Reads a table's unique constraints.
pub async fn load<S: StorageIO>(schema_io: Arc<S>) -> Vec<UniqueKey> {
    let io = schema_io.create_new(sidecar_name(&schema_io)).await;
    let len = io.get_len().await;

    let mut out: Vec<UniqueKey> = Vec::new();
    if len < UNIQUE_KEY_SIZE {
        return out;
    }

    let count = len / UNIQUE_KEY_SIZE;
    let mut buffer = vec![0u8; UNIQUE_KEY_SIZE as usize];
    // A partial key read just before, waiting for its predicate.
    let mut awaiting: Option<UniqueKey> = None;
    for i in 0..count {
        let mut position = i * UNIQUE_KEY_SIZE;
        if io
            .read_data_into_buffer(&mut position, &mut buffer)
            .await
            .is_err()
        {
            break;
        }
        // A predicate belongs to the record right before it and to no other,
        // so one left behind by a dropped key cannot attach itself to a later
        // key of the same name.
        let pending = awaiting.take();
        match decode(&buffer) {
            Ok(Record::Predicate { name, text }) => match pending {
                Some(mut key) if key.name.eq_ignore_ascii_case(&name) && !text.trim().is_empty() => {
                    key.predicate = Some(text);
                    out.push(key);
                }
                Some(orphan) => unpaired(&orphan),
                None => {}
            },
            Ok(Record::Key(key)) => {
                if let Some(orphan) = pending {
                    unpaired(&orphan);
                }
                // A tombstoned record is kept on disk so the file keeps its
                // stride.
                if key.is_deleted {
                    continue;
                }
                if key.predicate.is_some() {
                    awaiting = Some(key);
                } else {
                    out.push(key);
                }
            }
            Err(e) => {
                if let Some(orphan) = pending {
                    unpaired(&orphan);
                }
                log::error!("Skipping unreadable unique key record: {}", e)
            }
        }
    }
    if let Some(orphan) = awaiting {
        unpaired(&orphan);
    }

    out
}

/// A partial key whose predicate record is missing. Enforcing it over every
/// row would refuse rows the index allows, which is as wrong as a weaker
/// constraint, so it is left out and said loudly.
fn unpaired(key: &UniqueKey) {
    log::error!(
        "unique index {} is partial but its predicate could not be read; it is not being enforced",
        key.describe()
    );
}

/// Appends a unique constraint.
///
/// A partial key and its predicate go in one write, so the key cannot reach
/// the file without the predicate that limits it.
pub async fn append<S: StorageIO>(schema_io: Arc<S>, key: &UniqueKey) -> io::Result<()> {
    if key.predicate.as_ref().is_some_and(|p| p.len() > MAX_PREDICATE_LEN) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "the predicate of a unique index may be at most {} bytes",
                MAX_PREDICATE_LEN
            ),
        ));
    }
    let io = schema_io.create_new(sidecar_name(&schema_io)).await;
    let mut bytes = key.into_vec();
    if let Some(predicate) = key.predicate_record() {
        bytes.extend_from_slice(&predicate);
    }
    io.append_data(&bytes, true).await;
    Ok(())
}

/// Marks a unique constraint deleted, by constraint name.
///
/// Returns whether one was found. The record keeps its slot so the file stays a
/// fixed-stride array. A partial key's predicate record stays too; with the key
/// tombstoned, `load` no longer pairs it with anything.
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
        // Through the log, not `write_data_seek`: the record went in as a
        // logged append, and the replay at every open put an unlogged
        // overwrite back — the dropped constraint was enforced again.
        io.write_data(position, &key.into_vec()).await;
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
    let close = list_close(&rest[open..])
        .map(|i| open + i)
        .ok_or_else(|| "UNIQUE column list is not closed".to_string())?;

    let columns: Vec<String> = split_items(&rest[open + 1..close])
        .into_iter()
        .map(key_column)
        .filter(|c| !matches!(c, Ok(c) if c.is_empty()))
        .collect::<Result<_, _>>()?;

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

    // `UNIQUE (…) WHERE pred` is the form `CREATE UNIQUE INDEX … WHERE pred`
    // is rewritten into: a partial key.
    let tail = rest[close + 1..].trim();
    let predicate = match crate::core::rql::join::find_keyword(tail, "WHERE", 0) {
        Some(0) => {
            let text = tail["WHERE".len()..].trim();
            if text.is_empty() {
                return Err("a partial unique index needs a predicate after WHERE".to_string());
            }
            if text.len() > MAX_PREDICATE_LEN {
                return Err(format!(
                    "the predicate of a unique index may be at most {} bytes",
                    MAX_PREDICATE_LEN
                ));
            }
            Some(text.to_string())
        }
        _ => None,
    };

    let name = name.unwrap_or_else(|| UniqueKey::default_name(table, &columns));
    Ok(Some(UniqueKey {
        predicate,
        ..UniqueKey::new(name, columns)
    }))
}

/// The offset of the `)` closing the `(` that `text` starts with, outside
/// quotes. The first `)` is not it: `pg_dump` writes an expression index as
/// `(lower(email))`.
fn list_close(text: &str) -> Option<usize> {
    let mut depth = 0usize;
    let mut quote: Option<char> = None;
    for (i, c) in text.char_indices() {
        match quote {
            Some(q) if c == q => quote = None,
            Some(_) => {}
            None => match c {
                '\'' | '"' => quote = Some(c),
                '(' => depth += 1,
                ')' => {
                    depth = depth.checked_sub(1)?;
                    if depth == 0 {
                        return Some(i);
                    }
                }
                _ => {}
            },
        }
    }
    None
}

/// `text` split at its top-level commas, outside quotes and parentheses.
fn split_items(text: &str) -> Vec<&str> {
    let mut items = Vec::new();
    let (mut depth, mut start) = (0usize, 0usize);
    let mut quote: Option<char> = None;
    for (i, c) in text.char_indices() {
        match quote {
            Some(q) if c == q => quote = None,
            Some(_) => {}
            None => match c {
                '\'' | '"' => quote = Some(c),
                '(' => depth += 1,
                ')' => depth = depth.saturating_sub(1),
                ',' if depth == 0 => {
                    items.push(&text[start..i]);
                    start = i + 1;
                }
                _ => {}
            },
        }
    }
    items.push(&text[start..]);
    items
}

/// The column one item of a key's column list names.
///
/// `pg_dump` writes index items with what PostgreSQL's B-tree needs for
/// ordering — `email DESC`, `email text_pattern_ops`, `NULLS LAST` — none of
/// which changes which values are equal, so they are dropped. An expression
/// (`lower(email)`) is refused: it used to reach the schema as a column named
/// `lower(email`, and the statement failed as an unknown column, as though
/// the index were mistyped rather than unsupported.
fn key_column(item: &str) -> Result<String, String> {
    let item = item.trim();
    if item.is_empty() {
        return Ok(String::new());
    }
    let (column, rest) = if let Some(quoted) = item.strip_prefix('"') {
        let end = quoted
            .find('"')
            .ok_or_else(|| format!("unterminated identifier in UNIQUE column list: {}", item))?;
        (&quoted[..end], &quoted[end + 1..])
    } else {
        let end = item
            .find(|c: char| !(c.is_alphanumeric() || c == '_' || c == '$'))
            .unwrap_or(item.len());
        (&item[..end], &item[end..])
    };
    let modifiers_only = rest.is_empty()
        || (rest.starts_with(char::is_whitespace)
            && rest.split_whitespace().all(|word| {
                let upper = word.to_ascii_uppercase();
                matches!(upper.as_str(), "ASC" | "DESC" | "NULLS" | "FIRST" | "LAST")
                    || upper.ends_with("_OPS")
            }));
    if column.is_empty() || !modifiers_only {
        return Err(format!(
            "a unique index on an expression is not supported: {}",
            item
        ));
    }
    Ok(column.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_record_round_trips() {
        let key = UniqueKey::new("users_email_key".to_string(), vec!["email".to_string()]);
        assert_eq!(UniqueKey::from_vec(&key.into_vec()).unwrap(), key);
    }

    #[test]
    fn a_composite_record_round_trips() {
        let key = UniqueKey::new(
            "memberships_org_id_user_id_key".to_string(),
            vec![
                "org_id".to_string(),
                "user_id".to_string(),
                "role".to_string(),
            ],
        );
        assert_eq!(UniqueKey::from_vec(&key.into_vec()).unwrap(), key);
    }

    #[test]
    fn a_tombstone_survives_the_round_trip() {
        let key = UniqueKey {
            is_deleted: true,
            ..UniqueKey::new("gone".to_string(), vec!["a".to_string()])
        };
        assert!(UniqueKey::from_vec(&key.into_vec()).unwrap().is_deleted);
    }

    /// A partial key's two records, and what a build that predates them makes
    /// of each: a tombstone, so it never enforces the key over every row.
    #[test]
    fn a_partial_key_round_trips_and_older_builds_skip_it() {
        let key = UniqueKey {
            predicate: Some("deleted_at IS NULL AND status <> 'x'".to_string()),
            ..UniqueKey::new("users_email_live".to_string(), vec!["email".to_string()])
        };
        let main = key.into_vec();
        let predicate = key.predicate_record().expect("a partial key has one");
        match decode(&main).unwrap() {
            Record::Key(k) => {
                assert_eq!(k.columns, key.columns);
                assert!(!k.is_deleted);
                assert_eq!(k.predicate.as_deref(), Some(""));
            }
            Record::Predicate { .. } => panic!("the key record read as a predicate"),
        }
        match decode(&predicate).unwrap() {
            Record::Predicate { name, text } => {
                assert_eq!(name, "users_email_live");
                assert_eq!(text, "deleted_at IS NULL AND status <> 'x'");
            }
            Record::Key(_) => panic!("the predicate record read as a key"),
        }
        // The flag byte an older build tests with `!= 0`.
        let flag_at = 129 + 129 * MAX_COLUMNS + 1;
        assert_ne!(main[flag_at], 0);
        assert_ne!(predicate[flag_at], 0);
        // Dropped, it is an ordinary tombstone.
        let dropped = UniqueKey {
            is_deleted: true,
            ..key.clone()
        };
        assert_eq!(dropped.into_vec()[flag_at], FLAG_DELETED);
        assert!(UniqueKey::new("k".into(), vec!["a".into()]).predicate_record().is_none());
    }

    #[test]
    fn a_partial_clause_keeps_its_predicate() {
        let key = parsed("CONSTRAINT live UNIQUE (email) WHERE (deleted_at IS NULL)");
        assert_eq!(key.columns, vec!["email".to_string()]);
        assert_eq!(key.predicate.as_deref(), Some("(deleted_at IS NULL)"));
        assert_eq!(key.predicate_display().as_deref(), Some("(deleted_at IS NULL)"));
        let key = parsed("UNIQUE (tenant_id, slug) WHERE active");
        assert_eq!(key.predicate_display().as_deref(), Some("active"));
        assert_eq!(parsed("UNIQUE (a)").predicate, None);
        assert!(parse_clause("UNIQUE (a) WHERE", "t").is_err());
        // `(a) OR (b)` is not one group.
        let key = parsed("UNIQUE (a) WHERE (b IS NULL) OR (c > 1)");
        assert_eq!(key.predicate_display().as_deref(), Some("((b IS NULL) OR (c > 1))"));
    }

    /// The predicate is judged as a `WHERE` is: a row is covered only when it
    /// is true, so a NULL leaves the row out, and `IS NULL` reads the
    /// companion rather than being folded to false.
    #[test]
    fn a_predicate_is_judged_like_a_where() {
        use crate::core::db_type::DbType;
        use crate::core::row::row::{Column, Row, schema_id_of};
        use crate::core::row::schema::SchemaField;

        let fields: smallvec::SmallVec<[SchemaField; 20]> = smallvec::smallvec![
            SchemaField::new("email".into(), DbType::STRING, 0, 0, 0, false),
            SchemaField::new("deleted_at".into(), DbType::DATETIME, 8, 0, 1, false),
            SchemaField::new("__null_deleted_at".into(), DbType::U8, 1, 0, 2, false),
            SchemaField::new("archived".into(), DbType::BOOL, 1, 0, 3, false),
            SchemaField::new("__null_archived".into(), DbType::U8, 1, 0, 4, false),
            SchemaField::new("status".into(), DbType::STRING, 0, 0, 5, false),
        ];
        let row = |deleted_null: bool, archived: Option<bool>, status: &str| {
            let cell = |wo: u64, bytes: &[u8], t: DbType| {
                let mut mb = crate::memory_pool::MEMORY_POOL.acquire(bytes.len());
                mb.into_slice_mut().copy_from_slice(bytes);
                Column {
                    schema_id: schema_id_of(wo),
                    data: mb,
                    column_type: t,
                }
            };
            Row {
                position: 0,
                columns: vec![
                    cell(1, &0i64.to_le_bytes(), DbType::DATETIME),
                    cell(2, &[u8::from(deleted_null)], DbType::U8),
                    cell(3, &[u8::from(archived == Some(true))], DbType::BOOL),
                    cell(4, &[u8::from(archived.is_none())], DbType::U8),
                    cell(5, status.as_bytes(), DbType::STRING),
                ]
                .into(),
                length: 0,
            }
        };
        let key = |predicate: &str| UniqueKey {
            predicate: Some(predicate.to_string()),
            ..UniqueKey::new("k".into(), vec!["email".into()])
        };

        let live = key("deleted_at IS NULL");
        assert!(live.covers(&row(true, Some(false), "a"), &fields).unwrap());
        assert!(!live.covers(&row(false, Some(false), "a"), &fields).unwrap());

        // `NOT archived` holds for false only; a NULL is not true.
        let unarchived = key("NOT archived");
        assert!(unarchived.covers(&row(true, Some(false), "a"), &fields).unwrap());
        assert!(!unarchived.covers(&row(true, Some(true), "a"), &fields).unwrap());
        assert!(!unarchived.covers(&row(true, None, "a"), &fields).unwrap());

        let status = key("status IN ('active', 'trialing') AND deleted_at IS NULL");
        assert!(status.covers(&row(true, None, "trialing"), &fields).unwrap());
        assert!(!status.covers(&row(true, None, "canceled"), &fields).unwrap());
        assert!(!status.covers(&row(false, None, "active"), &fields).unwrap());

        // An ordinary key covers every row.
        let full = UniqueKey::new("k".into(), vec!["email".into()]);
        assert!(full.covers(&row(false, None, ""), &fields).unwrap());

        // What the evaluator cannot read is refused, never guessed at.
        assert!(key("lower(status) = 'a'").condition(&fields).is_err());
        assert!(key("status LIKE 'a%'").condition(&fields).is_err());
    }

    fn soft_delete_fields() -> smallvec::SmallVec<[crate::core::row::schema::SchemaField; 20]> {
        use crate::core::db_type::DbType;
        use crate::core::row::schema::SchemaField;
        smallvec::smallvec![
            SchemaField::new("email".into(), DbType::STRING, 0, 0, 0, false),
            SchemaField::new("deleted_at".into(), DbType::DATETIME, 8, 0, 1, false),
            SchemaField::new("__null_deleted_at".into(), DbType::U8, 1, 0, 2, false),
            SchemaField::new("active".into(), DbType::BOOL, 1, 0, 3, false),
            SchemaField::new("__null_active".into(), DbType::U8, 1, 0, 4, false),
            SchemaField::new("note".into(), DbType::STRING, 0, 0, 5, false),
        ]
    }

    /// `DROP COLUMN` takes the partial keys whose predicate reads the column,
    /// through the column or its NULL flag, and leaves every other key.
    #[test]
    fn a_dropped_column_takes_the_predicates_that_read_it() {
        let fields = soft_delete_fields();
        let key = |name: &str, predicate: Option<&str>| UniqueKey {
            predicate: predicate.map(str::to_string),
            ..UniqueKey::new(name.into(), vec!["email".into()])
        };
        let keys = vec![
            key("live", Some("deleted_at IS NULL")),
            key("live_active", Some("active AND deleted_at IS NULL")),
            key("active_only", Some("active")),
            key("full", None),
            // Already unenforced: the drop does not change what it reads.
            key("broken", Some("gone IS NULL")),
        ];
        assert_eq!(dropped_with_column(&keys, &fields, "deleted_at"), vec!["live", "live_active"]);
        assert_eq!(dropped_with_column(&keys, &fields, "ACTIVE"), vec!["live_active", "active_only"]);
        assert!(dropped_with_column(&keys, &fields, "note").is_empty());
        assert!(dropped_with_column(&keys, &fields, "email").is_empty());
    }

    /// A predicate that no longer compiles is reported once, not on every
    /// write the key is checked for.
    #[test]
    fn an_uncompilable_predicate_is_reported_once() {
        let fields = soft_delete_fields();
        let key = UniqueKey {
            predicate: Some("gone IS NULL".to_string()),
            ..UniqueKey::new("k".into(), vec!["email".into()])
        };
        let write = crate::core::row::row::RowWrite {
            columns_writing_data: smallvec::SmallVec::new(),
        };
        for _ in 0..3 {
            assert!(partial_check(&key, &fields, &write).unwrap().is_none());
        }
        assert!(!key.compiled.first_report(), "the failure was already reported");
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

    /// What `pg_dump` writes around a column is dropped; an expression is
    /// refused as unsupported, not misreported as a missing column.
    #[test]
    fn index_item_modifiers_are_dropped_and_expressions_refused() {
        assert_eq!(
            parsed("UNIQUE (email DESC NULLS LAST, \"orgId\" text_pattern_ops)").columns,
            vec!["email".to_string(), "orgId".to_string()]
        );
        for expression in ["UNIQUE (lower(email))", "UNIQUE ((a + b))", "UNIQUE (a, lower(b))", "UNIQUE (a COLLATE \"C\")"] {
            let error = parse_clause(expression, "t").unwrap_err();
            assert!(error.contains("not supported"), "{}: {}", expression, error);
        }
        // The predicate after an expression list is not mistaken for part of it.
        assert!(parse_clause("UNIQUE (lower(email)) WHERE (deleted_at IS NULL)", "t").is_err());
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
