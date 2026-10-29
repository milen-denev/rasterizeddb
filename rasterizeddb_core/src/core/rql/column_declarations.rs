//! What `RENAME COLUMN`, `DROP COLUMN` and `ADD COLUMN` do to the declarations
//! filed under a column's *name*, written as one log transaction.
//!
//! # Why by name is the hazard
//!
//! A column's DEFAULT, generation expression, NUMERIC scale and JSON
//! registration, and every composite UNIQUE key, foreign key (on either side),
//! CHECK and path index that mentions it, name the column by its name. A
//! dropped column keeps its slot and its name is free to take again, so a
//! declaration still naming it passed to the next column given the name: a
//! dropped foreign key became a live one on an unrelated column, and its
//! `ON DELETE CASCADE` deleted rows by whatever values that column held.
//! (`CREATE INDEX` declarations are keyed by write order instead and need none
//! of this: a new column under an old name has a new write order.)
//!
//! So `DROP COLUMN` retires every declaration of the column, `RENAME COLUMN`
//! moves them, and both `RENAME` and `ADD` first retire any record still naming
//! the name they take — left by an older engine's drop, which retired none.
//! Such a record is recognisable without guessing: it names a column no live
//! field has.
//!
//! # Why one transaction
//!
//! These used to be a dozen separately durable writes, and a power cut between
//! two of them left a mix: the field renamed but its `__null_` companion not
//! (every NULL then read as the type's zero), or the constraints naming a
//! column that no longer existed (skipped as "not enforced", for good: nothing
//! reconciles them on open).
//!
//! Every sidecar here is a fixed-stride array, so every change can be made as
//! an *in-place* rewrite of existing records, never an append. In-place writes
//! are held back until their log records are durable and applied at commit, so
//! all of a statement's writes in one directory commit as one transaction and
//! survive a crash all or not at all. An append could not join that: its bytes
//! reach the file before the commit, and writeback can persist them without it.
//!
//! The last-wins sidecars (DEFAULT, generation, scale, JSON, CHECK, path
//! index) are replayed record by record, a tombstone removing the name and a
//! live record setting it, so the *last* record of a name decides. Every edit
//! below therefore targets the last record of a name:
//! - retiring a name flips its last record's tombstone;
//! - replacing a name's value rewrites its last record;
//! - moving `from` to `to` rewrites `to`'s last record with `from`'s value, or,
//!   when `to` has none, renames `from`'s last record — then retires whatever
//!   record of `from` is last after that.
//!
//! A foreign key on another table lives in that table's directory. When it is
//! the same directory it joins the transaction; otherwise it is written in that
//! directory's own transaction just after.

use std::collections::BTreeMap;

use rclite::Arc;

use crate::core::database::Database;
use crate::core::row::schema::{SchemaField, TableSchema};
use crate::core::rql::{
    check, column_default, foreign_key, generated, json_map, nullable, path_index, type_modifier,
    unique_key,
};
use crate::core::storage_providers::backend::DbStorageProvider;
use crate::core::storage_providers::traits::StorageIO;

fn named(a: &str, b: &str) -> bool {
    a.eq_ignore_ascii_case(b)
}

/// One fixed-stride sidecar of a table, read whole, with the records an edit
/// rewrote.
struct Sidecar {
    io: DbStorageProvider,
    stride: u64,
    raw: Vec<Vec<u8>>,
    dirty: std::collections::BTreeSet<usize>,
}

impl Sidecar {
    /// `<schema file stem>_<suffix>`, as every sidecar module names its file.
    async fn read(io_schema: &Arc<DbStorageProvider>, suffix: &str, stride: u64) -> Sidecar {
        let name = format!("{}_{}", io_schema.get_name().replace(".db", ""), suffix);
        let io = io_schema.create_new(name).await;
        let len = io.get_len().await;
        let mut raw = Vec::new();
        for i in 0..len / stride {
            let mut position = i * stride;
            let mut buffer = vec![0u8; stride as usize];
            if io.read_data_into_buffer(&mut position, &mut buffer).await.is_err() {
                break;
            }
            raw.push(buffer);
        }
        Sidecar {
            io,
            stride,
            raw,
            dirty: Default::default(),
        }
    }

    fn set(&mut self, i: usize, bytes: Vec<u8>) {
        debug_assert_eq!(bytes.len() as u64, self.stride);
        if self.raw[i] != bytes {
            self.raw[i] = bytes;
            self.dirty.insert(i);
        }
    }

    /// Logs every rewritten record under `txn`; the last token handed back.
    async fn write(&self, txn: u64) -> u64 {
        let mut token = 0;
        for &i in &self.dirty {
            let t = self
                .io
                .write_data_txn(txn, i as u64 * self.stride, &self.raw[i])
                .await;
            if t != 0 {
                token = t;
            }
        }
        token
    }
}

/// A record of a last-wins sidecar: keyed by a name, set or tombstoned.
trait LastWins: Sized + Clone {
    fn decode(raw: &[u8]) -> Option<Self>;
    fn encode(&self) -> Vec<u8>;
    fn key(&self) -> &str;
    fn set_key(&mut self, key: &str);
    fn deleted(&self) -> bool;
    fn set_deleted(&mut self);
}

macro_rules! last_wins {
    ($ty:ty, $key:ident) => {
        impl LastWins for $ty {
            fn decode(raw: &[u8]) -> Option<Self> {
                <$ty>::from_vec(raw).ok()
            }
            fn encode(&self) -> Vec<u8> {
                self.into_vec()
            }
            fn key(&self) -> &str {
                &self.$key
            }
            fn set_key(&mut self, key: &str) {
                self.$key = key.to_string();
            }
            fn deleted(&self) -> bool {
                self.is_deleted
            }
            fn set_deleted(&mut self) {
                self.is_deleted = true;
            }
        }
    };
}
last_wins!(column_default::ColumnDefault, column);
last_wins!(generated::GeneratedColumn, column);
last_wins!(type_modifier::TypeModifier, column);
last_wins!(json_map::JsonColumn, column);
last_wins!(check::CheckConstraint, name);
last_wins!(path_index::PathIndex, name);

/// The edits of [`LastWins`] records, on one sidecar.
struct Keyed<T: LastWins> {
    file: Sidecar,
    records: Vec<Option<T>>,
}

impl<T: LastWins> Keyed<T> {
    async fn read(io_schema: &Arc<DbStorageProvider>, suffix: &str, stride: u64) -> Self {
        let file = Sidecar::read(io_schema, suffix, stride).await;
        let records = file.raw.iter().map(|r| T::decode(r)).collect();
        Keyed { file, records }
    }

    fn last(&self, key: &str) -> Option<usize> {
        self.records
            .iter()
            .rposition(|r| r.as_ref().is_some_and(|r| named(r.key(), key)))
    }

    /// The value `key` loads as.
    fn live(&self, key: &str) -> Option<T> {
        self.last(key)
            .and_then(|i| self.records[i].clone())
            .filter(|r| !r.deleted())
    }

    /// Every key that loads as live, with its value.
    fn live_all(&self) -> Vec<T> {
        let mut out: Vec<T> = Vec::new();
        for r in self.records.iter().flatten() {
            out.retain(|e| !named(e.key(), r.key()));
            if !r.deleted() {
                out.push(r.clone());
            }
        }
        out
    }

    fn put(&mut self, i: usize, record: T) {
        self.file.set(i, record.encode());
        self.records[i] = Some(record);
    }

    /// `key` loads as absent.
    fn retire(&mut self, key: &str) {
        if let Some(i) = self.last(key)
            && let Some(mut r) = self.records[i].clone()
            && !r.deleted()
        {
            r.set_deleted();
            self.put(i, r);
        }
    }

    /// `key`, which loads live, loads as `value` instead.
    fn replace(&mut self, key: &str, value: T) {
        if let Some(i) = self.last(key) {
            self.put(i, value);
        }
    }

    /// `to` loads as `from` did, and `from` as absent.
    fn move_key(&mut self, from: &str, to: &str) {
        let Some(mut value) = self.live(from) else {
            self.retire(to);
            return;
        };
        value.set_key(to);
        match self.last(to) {
            Some(i) => {
                self.put(i, value);
                self.retire(from);
            }
            None => {
                let at = self.last(from).expect("a live key has a record");
                self.put(at, value);
                self.retire(from);
            }
        }
    }
}

/// The edits of a sidecar whose every live record counts (UNIQUE keys,
/// foreign keys), addressed by position.
struct Listed<T> {
    file: Sidecar,
    records: Vec<Option<T>>,
}

impl Listed<unique_key::UniqueKey> {
    async fn read(io_schema: &Arc<DbStorageProvider>) -> Self {
        let file = Sidecar::read(io_schema, "UK.db", unique_key::UNIQUE_KEY_SIZE).await;
        let mut records: Vec<Option<unique_key::UniqueKey>> =
            file.raw.iter().map(|r| unique_key::UniqueKey::from_vec(r).ok()).collect();
        // A partial key's own record holds only its flag (`predicate:
        // Some("")`); the text is the record after it. Read here, so a rename
        // can rewrite the text and `put` can write it back there.
        for i in 0..records.len() {
            if let Some(k) = records[i].as_mut()
                && k.predicate.as_deref() == Some("")
                && let Some(text) = file
                    .raw
                    .get(i + 1)
                    .and_then(|r| unique_key::paired_predicate(r, &k.name))
            {
                k.predicate = Some(text);
            }
        }
        Listed { file, records }
    }
    fn put(&mut self, i: usize, record: unique_key::UniqueKey) {
        self.file.set(i, record.into_vec());
        // Only a key whose predicate `read` paired carries text, so the
        // record after it is that predicate's. An unpaired one keeps
        // `Some("")` and nothing else is written.
        if record.predicate.as_deref().is_some_and(|p| !p.is_empty())
            && i + 1 < self.file.raw.len()
            && let Some(bytes) = record.predicate_record()
        {
            self.file.set(i + 1, bytes);
        }
        self.records[i] = Some(record);
    }
}

impl Listed<foreign_key::ForeignKey> {
    async fn read(io_schema: &Arc<DbStorageProvider>) -> Self {
        let file = Sidecar::read(io_schema, "FK.db", foreign_key::FOREIGN_KEY_SIZE).await;
        let records = file.raw.iter().map(|r| foreign_key::ForeignKey::from_vec(r).ok()).collect();
        Listed { file, records }
    }
    fn put(&mut self, i: usize, record: foreign_key::ForeignKey) {
        self.file.set(i, record.into_vec());
        self.records[i] = Some(record);
    }
}

/// The table's field records.
struct Fields {
    file: Sidecar,
    records: Vec<Option<SchemaField>>,
}

impl Fields {
    async fn read(io_schema: &Arc<DbStorageProvider>) -> Self {
        let stride = SchemaField::new(String::new(), crate::core::db_type::DbType::I32, 4, 0, 0, false)
            .into_vec()
            .len() as u64;
        let file = Sidecar::read(io_schema, "FIELDS.db", stride).await;
        let records = file.raw.iter().map(|r| SchemaField::from_vec(r).ok()).collect();
        Fields { file, records }
    }

    fn find(&self, name: &str, deleted: bool) -> Vec<usize> {
        (0..self.records.len())
            .filter(|&i| {
                self.records[i]
                    .as_ref()
                    .is_some_and(|f| named(&f.name, name) && f.is_deleted == deleted)
            })
            .collect()
    }

    fn put(&mut self, i: usize, field: SchemaField) {
        self.file.set(i, field.into_vec());
        self.records[i] = Some(field);
    }

    /// Gives a dropped field holding `name` the retired name
    /// `__dropped_<name>_<write order>`, unique within the table, so the name
    /// is free for a live column. See `retire_tombstoned_name`.
    fn retire_dead(&mut self, name: &str) -> Result<(), String> {
        for i in self.find(name, true) {
            let mut field = self.records[i].clone().expect("found");
            let retired = format!("__dropped_{}_{}", name, field.write_order);
            if retired.len() > 128 {
                return Err(format!(
                    "the dropped column {name} cannot give up its name: its retired name would \
                     pass 128 characters"
                ));
            }
            field.name = retired;
            self.put(i, field);
        }
        Ok(())
    }
}

/// Whether a CHECK's text compiles against `fields`, the way the insert path
/// compiles it.
fn check_compiles(expression: &str, fields: &[SchemaField]) -> bool {
    let sv: smallvec::SmallVec<[SchemaField; 20]> = fields.iter().cloned().collect();
    crate::core::tokenizer::predicate_rewrite::rewrite_where(expression, &sv)
        .and_then(|lowered| crate::core::rql::expr::parse_cond(&lowered, fields))
        .is_ok()
}

fn generated_compiles(expression: &str, fields: &[SchemaField]) -> bool {
    generated::parse_expression(expression, fields).is_ok()
}

/// Whether `expression` mentions column `name`, as the rename rewrite reads it.
fn mentions(expression: &str, table: &str, name: &str) -> bool {
    crate::core::rql::executor::rename_column_references(expression, table, name, "__probe__")
        != expression
}

/// The table's visible columns, live and not companions.
fn visible(schema: &TableSchema) -> Vec<SchemaField> {
    nullable::visible_fields(&schema.fields)
}

/// Whether a path index's path reads document column `name`.
fn path_reads(p: &path_index::PathIndex, name: &str) -> bool {
    crate::core::jsonb::parse_path_expression(&p.path).is_some_and(|e| named(&e.column, name))
}

/// The sidecars one statement edits, written as one transaction per
/// directory.
struct Batch {
    /// The table the statement alters, which names its crash points.
    table: String,
    io_schema: Arc<DbStorageProvider>,
    header: Option<Vec<u8>>,
    own: Vec<Sidecar>,
    /// Other tables' foreign-key sidecars: `(table, io, sidecar)`.
    others: Vec<(String, Arc<DbStorageProvider>, Sidecar)>,
}

impl Batch {
    /// Commits everything; returns the other tables whose sidecars changed.
    async fn commit(self) -> Result<Vec<String>, String> {
        let location = self.io_schema.location();
        let mut separate: BTreeMap<String, Vec<(String, Arc<DbStorageProvider>, Sidecar)>> =
            BTreeMap::new();
        let mut joined = Vec::new();
        let mut changed = Vec::new();
        for (table, io, file) in self.others {
            if file.dirty.is_empty() {
                continue;
            }
            if !changed.contains(&table) {
                changed.push(table.clone());
            }
            let at = io.location();
            if at == location {
                joined.push(file);
            } else {
                separate.entry(at).or_default().push((table, io, file));
            }
        }

        let txn = self.io_schema.begin_txn();
        let mut token = 0;
        if let Some(header) = &self.header {
            let t = self.io_schema.write_data_txn(txn, 0, header).await;
            if t != 0 {
                token = t;
            }
        }
        for file in self.own.iter().chain(joined.iter()) {
            let t = file.write(txn).await;
            if t != 0 {
                token = t;
            }
        }
        if token == 0 && txn != 0 {
            // Nothing was logged under it.
            self.io_schema.abort_txn(txn).await;
        }
        crash_points::reached(&self.table, "logged.before_commit");
        self.io_schema
            .commit_durable(token)
            .await
            .map_err(|e| format!("the change could not be made durable: {e}"))?;

        for (_, group) in separate {
            let io = group[0].1.clone();
            let txn = io.begin_txn();
            let mut token = 0;
            for (_, _, file) in &group {
                let t = file.write(txn).await;
                if t != 0 {
                    token = t;
                }
            }
            io.commit_durable(token).await.map_err(|e| {
                format!("the change committed, but a referencing table's key could not follow it: {e}")
            })?;
        }
        Ok(changed)
    }
}

/// The foreign-key sidecar of every *other* table whose keys reference one
/// of `columns` of `table`: the children a column's DDL on `table` reads and
/// may rewrite. Each is held under ACCESS EXCLUSIVE, taken by the statement
/// with `table`'s own (`executor::alter_lock_set`, finding L11), and
/// asserted by `table_held`; a table with no key on those columns has
/// nothing here to change.
async fn other_foreign_keys(
    database: &Arc<Database>,
    table: &str,
    columns: &[&str],
) -> Vec<(String, Arc<DbStorageProvider>, Listed<foreign_key::ForeignKey>)> {
    let ios: Vec<(String, Arc<DbStorageProvider>)> = database
        .fk_children_on(table, Some(columns))
        .into_iter()
        .filter(|name| !named(name, table))
        .filter_map(|name| database.table_held(&name).map(|t| (name, t.io_schema.clone())))
        .collect();
    let mut out = Vec::new();
    for (name, io) in ios {
        let fks = Listed::<foreign_key::ForeignKey>::read(&io).await;
        out.push((name, io, fks));
    }
    out
}

/// The key of `column`'s `character(n)` record in the type-modifier sidecar.
fn bpchar_key(column: &str) -> String {
    format!("{}{}", type_modifier::BPCHAR_MARKER, column)
}

/// The last-wins sidecars of one table.
struct Declarations {
    defaults: Keyed<column_default::ColumnDefault>,
    generated: Keyed<generated::GeneratedColumn>,
    modifiers: Keyed<type_modifier::TypeModifier>,
    json: Keyed<json_map::JsonColumn>,
    checks: Keyed<check::CheckConstraint>,
    paths: Keyed<path_index::PathIndex>,
    unique: Listed<unique_key::UniqueKey>,
    foreign: Listed<foreign_key::ForeignKey>,
}

impl Declarations {
    async fn read(io: &Arc<DbStorageProvider>) -> Self {
        Declarations {
            defaults: Keyed::read(io, "DEF.db", column_default::COLUMN_DEFAULT_SIZE).await,
            generated: Keyed::read(io, "GEN.db", generated::GENERATED_SIZE).await,
            modifiers: Keyed::read(io, "TYP.db", type_modifier::TYPE_MODIFIER_SIZE).await,
            json: Keyed::read(io, "JSON.db", json_map::JSON_COLUMN_SIZE).await,
            checks: Keyed::read(io, "CHK.db", check::CHECK_SIZE).await,
            paths: Keyed::read(io, "PIX.db", path_index::PATH_INDEX_SIZE).await,
            unique: Listed::<unique_key::UniqueKey>::read(io).await,
            foreign: Listed::<foreign_key::ForeignKey>::read(io).await,
        }
    }

    fn into_files(self) -> Vec<Sidecar> {
        vec![
            self.defaults.file,
            self.generated.file,
            self.modifiers.file,
            self.json.file,
            self.checks.file,
            self.paths.file,
            self.unique.file,
            self.foreign.file,
        ]
    }

    /// Retires every record that still names `name`, which no live field has:
    /// the leftovers of a dropped column. `fields` are the table's visible
    /// columns, which do not include `name`.
    fn retire_leftovers(&mut self, table: &str, name: &str, fields: &[SchemaField]) {
        self.defaults.retire(name);
        self.generated.retire(name);
        self.modifiers.retire(name);
        // Its `character(n)` record too (`type_modifier::BPCHAR_MARKER`):
        // left behind, a TEXT column later added or renamed onto the name
        // had its checks declined as if it were `character(n)`.
        self.modifiers.retire(&bpchar_key(name));
        self.json.retire(name);
        self.retire_constraints(table, name, fields);
    }

    /// Retires the UNIQUE keys, this table's foreign keys, CHECKs, path
    /// indexes naming `name`. A CHECK is retired only when it cannot compile
    /// without the column, so a type name spelled like it keeps the check.
    fn retire_constraints(&mut self, table: &str, name: &str, fields: &[SchemaField]) {
        for i in 0..self.unique.records.len() {
            if let Some(mut k) = self.unique.records[i].clone()
                && !k.is_deleted
                && k.columns.iter().any(|c| named(c, name))
            {
                k.is_deleted = true;
                self.unique.put(i, k);
            }
        }
        for i in 0..self.foreign.records.len() {
            if let Some(mut fk) = self.foreign.records[i].clone()
                && !fk.is_deleted
                && (named(&fk.column, name)
                    || (named(&fk.parent_table, table) && named(&fk.parent_column, name)))
            {
                fk.is_deleted = true;
                self.foreign.put(i, fk);
            }
        }
        for c in self.checks.live_all() {
            if mentions(&c.expression, table, name) && !check_compiles(&c.expression, fields) {
                self.checks.retire(&c.name);
            }
        }
        for p in self.paths.live_all() {
            if path_reads(&p, name) {
                self.paths.retire(&p.name);
            }
        }
    }
}

/// Retires another table's foreign keys that name `table`.`name` as parent.
fn retire_parent_side(
    others: &mut [(String, Arc<DbStorageProvider>, Listed<foreign_key::ForeignKey>)],
    table: &str,
    name: &str,
) {
    for (_, _, fks) in others.iter_mut() {
        for i in 0..fks.records.len() {
            if let Some(mut fk) = fks.records[i].clone()
                && !fk.is_deleted
                && named(&fk.parent_table, table)
                && named(&fk.parent_column, name)
            {
                fk.is_deleted = true;
                fks.put(i, fk);
            }
        }
    }
}

fn into_others(
    others: Vec<(String, Arc<DbStorageProvider>, Listed<foreign_key::ForeignKey>)>,
) -> Vec<(String, Arc<DbStorageProvider>, Sidecar)> {
    others.into_iter().map(|(t, io, l)| (t, io, l.file)).collect()
}

/// The schema header with `primary_key` set to `key`, when that changes it.
fn header_with(schema: &TableSchema, key: Option<String>) -> Option<Vec<u8>> {
    if schema.primary_key == key {
        return None;
    }
    let mut header = schema.clone();
    header.primary_key = key;
    Some(header.into_vec())
}

/// `ADD COLUMN name`, before the column is added: retires every declaration
/// still filed under the name, on this table and as a parent of others.
/// Returns whether anything was written, and the other tables that changed.
pub(crate) async fn retire_for_new_column(
    database: &Arc<Database>,
    table: &str,
    schema: &TableSchema,
    io_schema: &Arc<DbStorageProvider>,
    name: &str,
) -> Result<(bool, Vec<String>), String> {
    let fields = visible(schema);
    let mut decl = Declarations::read(io_schema).await;
    decl.retire_leftovers(table, name, &fields);
    let mut others = other_foreign_keys(database, table, &[name]).await;
    retire_parent_side(&mut others, table, name);
    let header = schema
        .primary_key
        .as_deref()
        .is_some_and(|k| named(k, name))
        .then(|| header_with(schema, None))
        .flatten();

    let own = decl.into_files();
    let wrote = header.is_some()
        || own.iter().any(|f| !f.dirty.is_empty())
        || others.iter().any(|(_, _, f)| !f.file.dirty.is_empty());
    if !wrote {
        return Ok((false, Vec::new()));
    }
    let changed = Batch {
        table: table.to_string(),
        io_schema: io_schema.clone(),
        header,
        own,
        others: into_others(others),
    }
    .commit()
    .await?;
    Ok((true, changed))
}

/// `DROP COLUMN name` whole: the field and its companion tombstoned, and every
/// declaration of the column retired with them. Refused, as PostgreSQL refuses
/// it without `CASCADE`, while another table's foreign key (or another column
/// of this one) references the column. Returns the other tables that changed.
pub(crate) async fn drop_column(
    database: &Arc<Database>,
    table: &str,
    schema: &TableSchema,
    io_schema: &Arc<DbStorageProvider>,
    name: &str,
) -> Result<Vec<String>, String> {
    let mut decl = Declarations::read(io_schema).await;
    let mut others = other_foreign_keys(database, table, &[name]).await;

    let depends = |fk: &foreign_key::ForeignKey| {
        !fk.is_deleted && named(&fk.parent_table, table) && named(&fk.parent_column, name)
    };
    for (owner, _, fks) in &others {
        if let Some(fk) = fks.records.iter().flatten().find(|fk| depends(fk)) {
            return Err(format!(
                "cannot drop column {name} of table {table} because constraint {} on table \
                 {owner} depends on it; drop the constraint first",
                fk.name
            ));
        }
    }
    if let Some(fk) = decl
        .foreign
        .records
        .iter()
        .flatten()
        .find(|fk| depends(fk) && !named(&fk.column, name))
    {
        return Err(format!(
            "cannot drop column {name} of table {table} because constraint {} on table {table} \
             depends on it; drop the constraint first",
            fk.name
        ));
    }

    let mut fields = Fields::read(io_schema).await;
    let live = fields.find(name, false);
    if live.is_empty() {
        return Err(format!("column '{}' does not exist", name));
    }
    for i in live
        .into_iter()
        .chain(fields.find(&nullable::flag_column_name(name), false))
    {
        let mut field = fields.records[i].clone().expect("found");
        field.is_deleted = true;
        fields.put(i, field);
    }

    let remaining: Vec<SchemaField> = visible(schema)
        .into_iter()
        .filter(|f| !named(&f.name, name))
        .collect();
    decl.retire_leftovers(table, name, &remaining);
    // Unreachable through the refusal above, but harmless: a self-reference
    // from the column to itself goes with it.
    retire_parent_side(&mut others, table, name);
    let header = schema
        .primary_key
        .as_deref()
        .is_some_and(|k| named(k, name))
        .then(|| header_with(schema, None))
        .flatten();

    let mut own = decl.into_files();
    own.push(fields.file);
    Batch {
        table: table.to_string(),
        io_schema: io_schema.clone(),
        header,
        own,
        others: into_others(others),
    }
    .commit()
    .await
}

/// `RENAME COLUMN from TO to` whole, as one transaction: the field and its
/// companion, the column's own declarations, and every constraint, CHECK,
/// generation expression and path index that mentions it, on this table and
/// as a parent of others. Records still naming `to` — a dropped column's
/// leftovers — are retired first, so the renamed column does not inherit them.
///
/// A CHECK or generation expression that compiled before and would not after
/// its rewrite refuses the statement: storing it would leave it skipped (a
/// check) or failing every insert (an expression).
///
/// Returns the other tables that changed.
pub(crate) async fn rename_column(
    database: &Arc<Database>,
    table: &str,
    schema: &TableSchema,
    io_schema: &Arc<DbStorageProvider>,
    from: &str,
    to: &str,
) -> Result<Vec<String>, String> {
    let filtered: String = to.chars().filter(|c| c.is_ascii_alphanumeric() || *c == '_').collect();
    if filtered != to || to.is_empty() {
        return Err(format!("column name '{}' contains characters that cannot be stored", to));
    }
    // The companion's name has to fit too, when there is one.
    let has_flag = schema
        .fields
        .iter()
        .any(|f| !f.is_deleted && nullable::is_flag_name_of(&f.name, from));
    if to.len() > 128 || (has_flag && nullable::flag_column_name(to).len() > 128) {
        return Err("column names are limited to 128 characters".to_string());
    }
    let rewrite = |e: &str| crate::core::rql::executor::rename_column_references(e, table, from, to);

    let before = visible(schema);
    let after: Vec<SchemaField> = before
        .iter()
        .cloned()
        .map(|mut f| {
            if named(&f.name, from) {
                f.name = to.to_string();
            }
            f
        })
        .collect();

    let mut decl = Declarations::read(io_schema).await;
    let mut others = other_foreign_keys(database, table, &[from, to]).await;

    // Leftovers under `to` first: they name a column no live field has.
    decl.retire_leftovers(table, to, &before);
    retire_parent_side(&mut others, table, to);

    // The column's own declarations follow it.
    decl.defaults.move_key(from, to);
    decl.generated.move_key(from, to);
    decl.modifiers.move_key(from, to);
    // The `character(n)` record follows it as well: left under `from`, a
    // check added after the rename was enforced on a `character(n)` column.
    decl.modifiers.move_key(&bpchar_key(from), &bpchar_key(to));
    decl.json.move_key(from, to);

    // Everything else that mentions it is rewritten. A partial index's
    // predicate names columns too: left naming `from`, it no longer compiled,
    // and a key that cannot read its predicate is not enforced, so renaming
    // `deleted_at` let every live duplicate in. The clone's compiled
    // predicate starts empty, so the rewritten text is what gets compiled.
    for i in 0..decl.unique.records.len() {
        let Some(mut k) = decl.unique.records[i].clone() else { continue };
        if k.is_deleted {
            continue;
        }
        let predicate = k.predicate.as_deref().map(|p| rewrite(p));
        if !k.columns.iter().any(|c| named(c, from)) && predicate.as_deref() == k.predicate.as_deref() {
            continue;
        }
        if predicate.as_ref().is_some_and(|p| p.len() > unique_key::MAX_PREDICATE_LEN) {
            return Err(format!(
                "cannot rename column {from} to {to}: the predicate of unique index {} would \
                 exceed {} characters",
                k.name,
                unique_key::MAX_PREDICATE_LEN
            ));
        }
        for c in k.columns.iter_mut() {
            if named(c, from) {
                *c = to.to_string();
            }
        }
        k.predicate = predicate;
        decl.unique.put(i, k);
    }
    let follow = |fk: &mut foreign_key::ForeignKey, owner_is_table: bool| -> bool {
        let mut moved = false;
        if owner_is_table && named(&fk.column, from) {
            fk.column = to.to_string();
            moved = true;
        }
        if named(&fk.parent_table, table) && named(&fk.parent_column, from) {
            fk.parent_column = to.to_string();
            moved = true;
        }
        moved
    };
    for i in 0..decl.foreign.records.len() {
        if let Some(mut fk) = decl.foreign.records[i].clone()
            && !fk.is_deleted
            && follow(&mut fk, true)
        {
            decl.foreign.put(i, fk);
        }
    }
    for (_, _, fks) in others.iter_mut() {
        for i in 0..fks.records.len() {
            if let Some(mut fk) = fks.records[i].clone()
                && !fk.is_deleted
                && follow(&mut fk, false)
            {
                fks.put(i, fk);
            }
        }
    }
    for c in decl.checks.live_all() {
        let expression = rewrite(&c.expression);
        if expression == c.expression {
            continue;
        }
        if expression.len() > check::MAX_EXPRESSION_LEN {
            return Err(format!(
                "cannot rename column {from} to {to}: check constraint {} would exceed {} characters",
                c.name,
                check::MAX_EXPRESSION_LEN
            ));
        }
        if check_compiles(&c.expression, &before) && !check_compiles(&expression, &after) {
            return Err(format!(
                "cannot rename column {from} to {to}: check constraint {} would no longer \
                 compile as {expression:?}",
                c.name
            ));
        }
        let name = c.name.clone();
        decl.checks.replace(
            &name,
            check::CheckConstraint {
                expression,
                ..c
            },
        );
    }
    // Generation expressions of the *other* live columns. One filed under
    // `to` or under a dead name is a leftover, never rewritten to read the
    // renamed column: it would have become generated from itself.
    for g in decl.generated.live_all() {
        let column_is_live_other = before
            .iter()
            .any(|f| named(&f.name, &g.column) && !named(&f.name, from));
        if !column_is_live_other || named(&g.column, to) {
            continue;
        }
        let expression = rewrite(&g.expression);
        if expression == g.expression {
            continue;
        }
        if expression.len() > generated::MAX_EXPRESSION_LEN {
            return Err(format!(
                "cannot rename column {from} to {to}: the expression of generated column {} would \
                 exceed {} characters",
                g.column,
                generated::MAX_EXPRESSION_LEN
            ));
        }
        if generated_compiles(&g.expression, &before) && !generated_compiles(&expression, &after) {
            return Err(format!(
                "cannot rename column {from} to {to}: the expression of generated column {} would \
                 no longer compile as {expression:?}",
                g.column
            ));
        }
        let column = g.column.clone();
        decl.generated.replace(
            &column,
            generated::GeneratedColumn {
                expression,
                ..g
            },
        );
    }
    for p in decl.paths.live_all() {
        if !path_reads(&p, from) {
            continue;
        }
        let Some(mut path) = crate::core::jsonb::parse_path_expression(&p.path) else {
            continue;
        };
        path.column = to.to_string();
        let name = p.name.clone();
        decl.paths.replace(
            &name,
            path_index::PathIndex {
                path: path.index_key(),
                ..p
            },
        );
    }

    // The field and its companion, and dead fields holding either name.
    let mut fields = Fields::read(io_schema).await;
    let (old_flag, new_flag) = (nullable::flag_column_name(from), nullable::flag_column_name(to));
    fields.retire_dead(to)?;
    fields.retire_dead(&new_flag)?;
    let live = fields.find(from, false);
    let Some(&at) = live.first() else {
        return Err(format!("column '{}' does not exist", from));
    };
    let mut field = fields.records[at].clone().expect("found");
    field.name = to.to_string();
    fields.put(at, field);
    if let Some(&flag_at) = fields.find(&old_flag, false).first() {
        let mut flag = fields.records[flag_at].clone().expect("found");
        flag.name = new_flag;
        fields.put(flag_at, flag);
    }

    let key = match schema.primary_key.as_deref() {
        Some(k) if named(k, from) => Some(to.to_string()),
        Some(k) if named(k, to) => None,
        other => other.map(str::to_string),
    };
    let header = header_with(schema, key);

    let mut own = decl.into_files();
    own.push(fields.file);
    Batch {
        table: table.to_string(),
        io_schema: io_schema.clone(),
        header,
        own,
        others: into_others(others),
    }
    .commit()
    .await
}

/// `RENAME TABLE old TO new`, once the table is renamed: every foreign key
/// naming `old` as its parent — the children's, and the table's own
/// self-references — names `new`. Returns the tables that changed.
pub(crate) async fn retarget_parent(
    database: &Arc<Database>,
    old: &str,
    new: &str,
) -> Result<Vec<String>, String> {
    // The children of `old`, the renamed table's own self-references (under
    // `new` now) included, each under the ACCESS EXCLUSIVE the RENAME took
    // with both names (`executor::alter_lock_set`, finding L11).
    let ios: Vec<(String, Arc<DbStorageProvider>)> = database
        .fk_children(old)
        .into_iter()
        .filter_map(|name| database.table_held(&name).map(|t| (name, t.io_schema.clone())))
        .collect();
    let mut changed = Vec::new();
    // Grouped by directory, one transaction each.
    let mut groups: BTreeMap<String, Vec<(String, Arc<DbStorageProvider>, Sidecar)>> = BTreeMap::new();
    for (name, io) in ios {
        let mut fks = Listed::<foreign_key::ForeignKey>::read(&io).await;
        for i in 0..fks.records.len() {
            if let Some(mut fk) = fks.records[i].clone()
                && !fk.is_deleted
                && named(&fk.parent_table, old)
            {
                fk.parent_table = new.to_string();
                fks.put(i, fk);
            }
        }
        if !fks.file.dirty.is_empty() {
            changed.push(name.clone());
            groups.entry(io.location()).or_default().push((name, io, fks.file));
        }
    }
    for (_, group) in groups {
        let io = group[0].1.clone();
        let txn = io.begin_txn();
        let mut token = 0;
        for (_, _, file) in &group {
            let t = file.write(txn).await;
            if t != 0 {
                token = t;
            }
        }
        io.commit_durable(token)
            .await
            .map_err(|e| format!("a referencing table's key could not follow the rename: {e}"))?;
    }
    Ok(changed)
}

/// Named points inside a DDL statement where a test can take a crash image.
///
/// `reached` is called between a statement's durable writes. A test installs
/// a hook for one table and builds, from inside it, what the disk could hold
/// if power were cut there. Nothing else runs while the hook does, so the
/// image is exactly that point.
#[cfg(test)]
pub(crate) mod crash_points {
    type Hook = Box<dyn FnMut(&str) + Send>;

    static HOOKS: parking_lot::Mutex<Vec<(String, Hook)>> = parking_lot::Mutex::new(Vec::new());

    /// Calls `hook` with each point's label that a statement on `table` reaches
    /// until the guard drops.
    pub(crate) fn install(table: &str, hook: Hook) -> Installed {
        HOOKS.lock().push((table.to_ascii_lowercase(), hook));
        Installed(table.to_ascii_lowercase())
    }

    pub(crate) struct Installed(String);

    impl Drop for Installed {
        fn drop(&mut self) {
            HOOKS.lock().retain(|(t, _)| *t != self.0);
        }
    }

    pub(crate) fn reached(table: &str, label: &str) {
        let table = table.to_ascii_lowercase();
        let mut hooks = HOOKS.lock();
        for (t, hook) in hooks.iter_mut() {
            if *t == table {
                hook(label);
            }
        }
    }
}

/// Outside tests a crash point is nothing.
#[cfg(not(test))]
pub(crate) mod crash_points {
    #[inline(always)]
    pub(crate) fn reached(_table: &str, _label: &str) {}
}
