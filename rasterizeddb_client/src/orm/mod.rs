//! Structs as tables.
//!
//! ```no_run
//! use rasterizeddb_client::prelude::*;
//!
//! #[derive(Model, Debug, Clone)]
//! #[rdb(table = "accounts")]
//! struct Account {
//!     #[rdb(primary_key, serial)]
//!     id: i64,
//!     #[rdb(unique)]
//!     email: String,
//!     #[rdb(index)]
//!     name: Option<String>,
//!     balance: Numeric,
//!     created_at: Timestamp,
//! }
//!
//! # async fn demo(client: &Client) -> rasterizeddb_client::Result<()> {
//! client.create_table::<Account>().await?;
//!
//! let stored = client
//!     .insert(&Account {
//!         id: 0, // serial: the server assigns it, and the stored row comes back
//!         email: "ada@example.com".into(),
//!         name: Some("Ada".into()),
//!         balance: Numeric::new("10.50")?,
//!         created_at: Timestamp::now(),
//!     })
//!     .await?;
//!
//! let rich: Vec<Account> = Account::query()
//!     .filter(Account::balance.gt(Numeric::new("5")?))
//!     .filter(Account::name.is_not_null())
//!     .order_by(Account::created_at.desc())
//!     .limit(10)
//!     .all(client)
//!     .await?;
//!
//! let found = client.find::<Account>(stored.id).await?;
//! client.delete(&stored).await?;
//! # let _ = (rich, found); Ok(()) }
//! ```
//!
//! Projections and aggregates ([`Select::select`], [`Agg`]) read chosen
//! columns, `COUNT`, `SUM`, `AVG`, `MIN` and `MAX` into tuples or
//! [`FromRow`] structs, with `GROUP BY` and `HAVING`.
//!
//! # What the derive writes
//!
//! `#[derive(Model)]` implements [`Model`] and [`FromRow`],
//! and gives the struct one associated constant per column, named as the
//! field (`Account::email`), for the query builder. Field attributes:
//!
//! | attribute | meaning |
//! |---|---|
//! | `primary_key` | part of the primary key (several fields make a composite one) |
//! | `serial` | assigned by the server's sequence: left out of inserts, read back |
//! | `unique` | a `UNIQUE` constraint |
//! | `index`, `index = "hash"` | a secondary index (`btree`, `hash`, `gin`) |
//! | `column = "name"` | the column's name, when it is not the field's |
//! | `sql_type = "VARCHAR(64)"` | the column's type, when the field's default is not wanted |
//! | `default = "now()"` | a `DEFAULT` expression, for the DDL |
//! | `json` | any `Serialize + Deserialize` type, stored as `jsonb` (feature `serde_json`) |
//! | `text` | read as `col::text` and parsed (for a type of your own) |
//! | `skip` | not a column: filled with `Default::default()` on reads |
//! | `version` | an integer for optimistic locking ([`ColumnRole::Version`]) |
//! | `references = "users(id)"` | a foreign key ([`ForeignKey`]) |
//! | `has_many(…)`, `belongs_to(…)`, … | a relation field ([`Relation`], [guide](crate::guide::relations)) |
//!
//! Struct attribute: `#[rdb(table = "name")]`, otherwise the struct's name in
//! snake case.
//!
//! # On any executor
//!
//! Every call takes an [`Executor`]: a
//! [`Client`](crate::Client), a reference to one, or a transaction. The
//! same ORM code runs on its own, each statement committing by itself, or
//! inside a transaction, unchanged. [`ModelExt`] is implemented for every
//! executor (`client.insert(&row)`, `tx.insert(&row)`), and the query
//! builder's terminal calls take one (`.all(&client)`, `.all(&tx)`).
//!
//! Outside a transaction each call is one statement and commits on its own.
//! [`ModelExt::insert_many`] is one multi-row statement per chunk: atomic
//! within a chunk, not across chunks.
//!
//! The [`Migrator`] stays on a [`Client`](crate::Client): its lock row must
//! be visible to other processes while it migrates, which a lock taken
//! inside an uncommitted transaction is not.

mod keyset;
mod migrate;
mod projection;
mod query;
mod relations;

pub use keyset::{Cursor, Page, RowStream};
pub use migrate::{
    AutoMigrate, BoxFuture, MigrationPlan, MigrationReport, Migrator, StepState, StepStatus,
    SyncPolicy,
};
pub use projection::{Agg, Aggregate, Columns, Ordered, Projected, Projection, count_rows};
pub use query::{Column, Expr, Order, Select, Update};
pub use relations::{
    ModelRelations, RELATION_CHUNK, Relation, RelationKind, RelationSlot, Synced, Through, With,
};

use std::future::Future;

use crate::admin::IndexMethod;
use crate::executor::Executor;
use crate::params::{push_ident, push_param};
use crate::row::FromRow;
use crate::to_sql::ToSql;
use crate::{Error, Result};

/// One column of a [`Model`], as the derive describes it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColumnDef {
    /// The column's name.
    pub name: &'static str,
    /// The DDL type: `BIGINT`, `TEXT`, `NUMERIC`, `JSONB`, `BIGINT[]`, …
    pub sql_type: &'static str,
    /// Whether it may hold NULL (the field is an `Option`).
    pub nullable: bool,
    /// Whether it is (part of) the primary key.
    pub primary_key: bool,
    /// Assigned by the server: never written by an insert.
    pub serial: bool,
    /// Whether a unique key covers it.
    pub unique: bool,
    /// The secondary index on it, if any.
    pub index: Option<IndexMethod>,
    /// A `DEFAULT` expression for the DDL.
    pub default: Option<&'static str>,
    /// Read as `col::text`: a type the native protocol sends in a form this
    /// client cannot decode (`jsonb`, arrays, `NUMERIC(p, s)`).
    pub read_as_text: bool,
    /// Names this column had before (`#[rdb(renamed_from = "…")]`): a
    /// migrator that finds one of them, and not this name, renames it.
    pub renamed_from: &'static [&'static str],
    /// What the ORM writes into it itself, if anything.
    pub role: ColumnRole,
    /// The foreign key the DDL declares on it (`#[rdb(references =
    /// "users(id)")]`).
    pub references: Option<ForeignKey>,
}

/// A column's foreign key: `REFERENCES table (column) [ON DELETE …] [ON
/// UPDATE …]`, written by `CREATE TABLE` and by a migrator's `ADD COLUMN`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ForeignKey {
    /// The referenced table.
    pub table: &'static str,
    /// The referenced column: its primary key or a unique column.
    pub column: &'static str,
    /// `CASCADE`, `RESTRICT`, `SET NULL`, `SET DEFAULT` or `NO ACTION`.
    pub on_delete: Option<&'static str>,
    /// As `on_delete`, on an update of the referenced key.
    pub on_update: Option<&'static str>,
}

/// A column the ORM maintains itself, from its field attribute.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum ColumnRole {
    /// An ordinary column: written as the field holds it.
    #[default]
    Plain,
    /// `#[rdb(version)]`, an integer for optimistic locking: an insert
    /// writes 1, every update writes `version + 1` and only matches the row
    /// at the version the struct holds, and a delete matches only that
    /// version too. A write that finds the row at another version is
    /// [`Error::Conflict`].
    Version,
}

/// A struct stored as a table's rows. Derive it: `#[derive(Model)]`.
pub trait Model: FromRow + Sized {
    /// The table's name.
    const TABLE: &'static str;
    /// In select-list order: column `i` of every row this trait reads is
    /// `COLUMNS[i]`.
    const COLUMNS: &'static [ColumnDef];
    /// Names the table had before (`#[rdb(renamed_from = "…")]` on the
    /// struct), for a migrator to rename.
    const RENAMED_FROM: &'static [&'static str] = &[];

    /// Writes this value's column `i` as a literal into `out`.
    fn write_value(&self, column: usize, out: &mut String) -> Result<()>;

    /// Appends column `i`'s text form (see [`crate::kv::text_form`]) to
    /// `out`, for the key/value cache: `false`, writing nothing, for a NULL.
    fn write_text(&self, column: usize, out: &mut Vec<u8>) -> Result<bool>;
}

/// The Rust side of a column type: what DDL names it and how it is read.
/// Implemented for the types the derive maps by itself; give a field of any
/// other type `#[rdb(sql_type = "…")]`.
pub trait SqlType {
    /// The column type a `CREATE TABLE` gives this Rust type.
    const SQL_TYPE: &'static str;
    /// Whether the type holds NULL (`Option<T>`).
    const NULLABLE: bool = false;
    /// See [`ColumnDef::read_as_text`].
    const READ_AS_TEXT: bool = false;
}

impl<T: SqlType> SqlType for Option<T> {
    const SQL_TYPE: &'static str = T::SQL_TYPE;
    const NULLABLE: bool = true;
    const READ_AS_TEXT: bool = T::READ_AS_TEXT;
}

impl<T: SqlType + ?Sized> SqlType for Box<T> {
    const SQL_TYPE: &'static str = T::SQL_TYPE;
    const NULLABLE: bool = T::NULLABLE;
    const READ_AS_TEXT: bool = T::READ_AS_TEXT;
}

macro_rules! sql_type {
    ($($t:ty => $name:literal $(as_text $text:literal)?),* $(,)?) => {$(
        impl SqlType for $t {
            const SQL_TYPE: &'static str = $name;
            $(const READ_AS_TEXT: bool = $text;)?
        }
    )*};
}

sql_type!(
    bool => "BOOLEAN",
    i8 => "SMALLINT",
    i16 => "SMALLINT",
    i32 => "INTEGER",
    i64 => "BIGINT",
    u16 => "INTEGER",
    u32 => "BIGINT",
    u64 => "UBIGINT",
    i128 => "HUGEINT",
    f32 => "REAL",
    f64 => "DOUBLE PRECISION",
    String => "TEXT",
    Vec<u8> => "BYTEA",
    crate::Uuid => "UUID",
    crate::Timestamp => "TIMESTAMPTZ",
    std::time::SystemTime => "TIMESTAMPTZ",
    crate::Date => "DATE",
    crate::Time => "TIME",
    crate::Interval => "INTERVAL",
    crate::Numeric => "NUMERIC" as_text true,
    crate::Json => "JSONB" as_text true,
    Vec<bool> => "BOOLEAN[]" as_text true,
    Vec<i16> => "SMALLINT[]" as_text true,
    Vec<i32> => "INTEGER[]" as_text true,
    Vec<i64> => "BIGINT[]" as_text true,
    Vec<f32> => "REAL[]" as_text true,
    Vec<f64> => "DOUBLE PRECISION[]" as_text true,
    Vec<String> => "TEXT[]" as_text true,
    Vec<crate::Uuid> => "UUID[]" as_text true,
    Vec<crate::Timestamp> => "TIMESTAMPTZ[]" as_text true,
    Vec<crate::Date> => "DATE[]" as_text true,
    Vec<crate::Numeric> => "NUMERIC[]" as_text true,
    Vec<Option<i32>> => "INTEGER[]" as_text true,
    Vec<Option<i64>> => "BIGINT[]" as_text true,
    Vec<Option<String>> => "TEXT[]" as_text true,
);

#[cfg(feature = "uuid")]
sql_type!(uuid::Uuid => "UUID");

#[cfg(feature = "chrono")]
sql_type!(
    chrono::DateTime<chrono::Utc> => "TIMESTAMPTZ",
    chrono::NaiveDateTime => "TIMESTAMP",
    chrono::NaiveDate => "DATE",
    chrono::NaiveTime => "TIME",
);

#[cfg(feature = "serde_json")]
sql_type!(serde_json::Value => "JSONB" as_text true);

// ----------------------------------------------------------------- statements

fn ident(name: &str) -> Result<String> {
    let mut out = String::new();
    push_ident(&mut out, name)?;
    Ok(out)
}

/// The model's select list, in [`Model::COLUMNS`] order, with the casts that
/// make every column readable.
pub fn select_list<M: Model>() -> Result<String> {
    let mut out = String::new();
    for (i, column) in M::COLUMNS.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        push_ident(&mut out, column.name)?;
        if column.read_as_text {
            out.push_str("::text");
        }
    }
    Ok(out)
}

fn primary_key<M: Model>() -> Result<Vec<usize>> {
    let key: Vec<usize> = M::COLUMNS
        .iter()
        .enumerate()
        .filter(|(_, c)| c.primary_key)
        .map(|(i, _)| i)
        .collect();
    if key.is_empty() {
        return Err(Error::Unsupported(format!(
            "{} has no #[rdb(primary_key)] field, so a row of it cannot be found again",
            M::TABLE
        )));
    }
    Ok(key)
}

/// The `#[rdb(version)]` column, if the model has one.
pub(crate) fn version_column<M: Model>() -> Option<usize> {
    M::COLUMNS
        .iter()
        .position(|c| c.role == ColumnRole::Version)
}

/// ` AND version = <the struct's version>`, when the model has one.
fn push_version_check<M: Model>(
    m: &M,
    lead: &str,
    qualifier: Option<&str>,
    out: &mut String,
) -> Result<()> {
    if let Some(v) = version_column::<M>() {
        out.push_str(lead);
        if let Some(table) = qualifier {
            out.push_str(table);
            out.push('.');
        }
        push_ident(out, M::COLUMNS[v].name)?;
        out.push_str(" = ");
        m.write_value(v, out)?;
    }
    Ok(())
}

/// Column `i`'s value as an `INSERT` writes it: the field, or what the ORM
/// maintains itself (`1` for a version).
fn push_insert_value<M: Model>(m: &M, i: usize, out: &mut String) -> Result<()> {
    match M::COLUMNS[i].role {
        ColumnRole::Version => {
            out.push('1');
            Ok(())
        }
        _ => m.write_value(i, out),
    }
}

/// `a = 1 AND b = 'x'` over the primary key, from `m`'s values.
fn key_predicate<M: Model>(m: &M, out: &mut String) -> Result<()> {
    for (n, i) in primary_key::<M>()?.into_iter().enumerate() {
        if n > 0 {
            out.push_str(" AND ");
        }
        push_ident(out, M::COLUMNS[i].name)?;
        out.push_str(" = ");
        m.write_value(i, out)?;
    }
    Ok(())
}

/// `INSERT INTO t (…) VALUES (…), … RETURNING …`, leaving out serial columns.
pub fn insert_sql<M: Model>(rows: &[&M]) -> Result<String> {
    if rows.is_empty() {
        return Err(Error::Param("an insert needs at least one row".to_string()));
    }
    let (head, tail) = insert_frame::<M>()?;
    let mut sql = head;
    for (r, row) in rows.iter().enumerate() {
        if r > 0 {
            sql.push_str(", ");
        }
        push_values_tuple(&mut sql, *row)?;
    }
    sql.push_str(&tail);
    Ok(sql)
}

/// What goes before the first `VALUES` tuple and after the last.
fn insert_frame<M: Model>() -> Result<(String, String)> {
    let mut head = format!("INSERT INTO {} (", ident(M::TABLE)?);
    let mut first = true;
    for column in M::COLUMNS.iter().filter(|c| !c.serial) {
        if !first {
            head.push_str(", ");
        }
        first = false;
        push_ident(&mut head, column.name)?;
    }
    head.push_str(") VALUES ");
    Ok((head, format!(" RETURNING {}", select_list::<M>()?)))
}

/// `(v1, v2, …)` for the row's non-serial columns.
fn push_values_tuple<M: Model>(out: &mut String, row: &M) -> Result<()> {
    out.push('(');
    let mut first = true;
    for (i, column) in M::COLUMNS.iter().enumerate() {
        if column.serial {
            continue;
        }
        if !first {
            out.push_str(", ");
        }
        first = false;
        push_insert_value(row, i, out)?;
    }
    out.push(')');
    Ok(())
}

/// `INSERT … ON CONFLICT (<primary key>) DO UPDATE SET <every other column> =
/// EXCLUDED.<column> RETURNING …`. Every column is written, serial ones too:
/// an upsert names its row. With a `#[rdb(version)]` column the insert
/// writes version 1 and the update `version + 1`, `WHERE` the stored
/// version is the struct's: no row back then means a conflict.
pub fn upsert_sql<M: Model>(m: &M) -> Result<String> {
    let key = primary_key::<M>()?;
    let mut sql = format!("INSERT INTO {} (", ident(M::TABLE)?);
    for (i, column) in M::COLUMNS.iter().enumerate() {
        if i > 0 {
            sql.push_str(", ");
        }
        push_ident(&mut sql, column.name)?;
    }
    sql.push_str(") VALUES (");
    for i in 0..M::COLUMNS.len() {
        if i > 0 {
            sql.push_str(", ");
        }
        push_insert_value(m, i, &mut sql)?;
    }
    sql.push_str(") ON CONFLICT (");
    for (n, &i) in key.iter().enumerate() {
        if n > 0 {
            sql.push_str(", ");
        }
        push_ident(&mut sql, M::COLUMNS[i].name)?;
    }
    let rest: Vec<&ColumnDef> = M::COLUMNS.iter().filter(|c| !c.primary_key).collect();
    if rest.is_empty() {
        sql.push_str(") DO NOTHING");
    } else {
        let table = ident(M::TABLE)?;
        sql.push_str(") DO UPDATE SET ");
        for (n, column) in rest.iter().enumerate() {
            if n > 0 {
                sql.push_str(", ");
            }
            let name = ident(column.name)?;
            match column.role {
                ColumnRole::Version => sql.push_str(&format!("{name} = {table}.{name} + 1")),
                _ => sql.push_str(&format!("{name} = EXCLUDED.{name}")),
            }
        }
        push_version_check(m, " WHERE ", Some(&table), &mut sql)?;
    }
    sql.push_str(" RETURNING ");
    sql.push_str(&select_list::<M>()?);
    Ok(sql)
}

/// `UPDATE t SET <every non-key column> WHERE <key> RETURNING …`; with a
/// `#[rdb(version)]` column, `version = version + 1 … AND version = <the
/// struct's>`.
pub fn update_sql<M: Model>(m: &M) -> Result<String> {
    let mut sql = format!("UPDATE {} SET ", ident(M::TABLE)?);
    let mut any = false;
    for (i, column) in M::COLUMNS.iter().enumerate() {
        if column.primary_key || column.serial {
            continue;
        }
        if any {
            sql.push_str(", ");
        }
        any = true;
        push_ident(&mut sql, column.name)?;
        sql.push_str(" = ");
        match column.role {
            ColumnRole::Version => {
                push_ident(&mut sql, column.name)?;
                sql.push_str(" + 1");
            }
            _ => m.write_value(i, &mut sql)?,
        }
    }
    if !any {
        return Err(Error::Unsupported(format!(
            "{} has no column outside its primary key to update",
            M::TABLE
        )));
    }
    sql.push_str(" WHERE ");
    key_predicate(m, &mut sql)?;
    push_version_check(m, " AND ", None, &mut sql)?;
    sql.push_str(" RETURNING ");
    sql.push_str(&select_list::<M>()?);
    Ok(sql)
}

/// `DELETE FROM t WHERE <primary key>`, and `AND version = <the struct's>`
/// with a `#[rdb(version)]` column.
pub fn delete_sql<M: Model>(m: &M) -> Result<String> {
    let mut sql = format!("DELETE FROM {} WHERE ", ident(M::TABLE)?);
    key_predicate(m, &mut sql)?;
    push_version_check(m, " AND ", None, &mut sql)?;
    Ok(sql)
}

/// What a versioned write that matched no row needs to say why: the row's
/// key as SQL, the version the struct holds, and the statement that reads
/// the version stored. `None` for a model without a version column.
pub(crate) struct VersionProbe {
    table: &'static str,
    key: String,
    expected: String,
    sql: String,
}

impl VersionProbe {
    pub(crate) fn of<M: Model>(m: &M) -> Result<Option<VersionProbe>> {
        let Some(v) = version_column::<M>() else {
            return Ok(None);
        };
        let mut key = String::new();
        key_predicate(m, &mut key)?;
        let mut expected = String::new();
        m.write_value(v, &mut expected)?;
        let sql = format!(
            "SELECT {} FROM {} WHERE {key}",
            ident(M::COLUMNS[v].name)?,
            ident(M::TABLE)?
        );
        Ok(Some(VersionProbe {
            table: M::TABLE,
            key,
            expected,
            sql,
        }))
    }

    /// After a write matched nothing: [`Error::Conflict`] when the row is
    /// there at another version, `None` when there is no such row.
    pub(crate) async fn classify<E: Executor + ?Sized>(self, db: &E) -> Result<Option<Error>> {
        let rows = db.run(&self.sql).await?;
        let Some(row) = rows.get(0) else {
            return Ok(None);
        };
        let stored: Option<i64> = row.get(0)?;
        Ok(Some(Error::Conflict(format!(
            "{} row {} was changed since it was read: the write expected version {}, the \
             table holds version {}; read the row again and retry",
            self.table,
            self.key,
            self.expected,
            stored.map_or("NULL".to_string(), |v| v.to_string())
        ))))
    }
}

fn find_sql<M: Model>(key: &[&dyn ToSql]) -> Result<String> {
    let columns = primary_key::<M>()?;
    if columns.len() != key.len() {
        return Err(Error::Param(format!(
            "{}'s primary key has {} column(s), {} value(s) were given",
            M::TABLE,
            columns.len(),
            key.len()
        )));
    }
    let mut sql = format!(
        "SELECT {} FROM {} WHERE ",
        select_list::<M>()?,
        ident(M::TABLE)?
    );
    for (n, (&i, value)) in columns.iter().zip(key).enumerate() {
        if n > 0 {
            sql.push_str(" AND ");
        }
        push_ident(&mut sql, M::COLUMNS[i].name)?;
        sql.push_str(" = ");
        push_param(&mut sql, &value.to_param())?;
    }
    Ok(sql)
}

/// The statements that create the model's table, its unique keys and its
/// indexes, each safe to run again (`IF NOT EXISTS`).
pub fn create_table_sql<M: Model>() -> Result<Vec<String>> {
    TableSchema::of::<M>().create_sql()
}

/// A model's table as data: what a [`Migrator`] keeps for each model it
/// syncs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TableSchema {
    /// The table's name.
    pub table: &'static str,
    /// Names the table had before.
    pub renamed_from: &'static [&'static str],
    /// The columns, in select-list order.
    pub columns: &'static [ColumnDef],
}

impl TableSchema {
    /// The schema of model `M`.
    pub fn of<M: Model>() -> Self {
        TableSchema {
            table: M::TABLE,
            renamed_from: M::RENAMED_FROM,
            columns: M::COLUMNS,
        }
    }

    /// `name TYPE [NOT NULL] [DEFAULT …]`, as `CREATE TABLE` and
    /// `ADD COLUMN` write a column.
    pub(crate) fn column_ddl(column: &ColumnDef) -> Result<String> {
        let mut line = ident(column.name)?;
        line.push(' ');
        line.push_str(if column.serial {
            match column.sql_type {
                "BIGINT" => "BIGSERIAL",
                "INTEGER" => "SERIAL",
                "SMALLINT" => "SMALLSERIAL",
                other => {
                    return Err(Error::Unsupported(format!(
                        "column {}: a serial column is an integer, not {other}",
                        column.name
                    )));
                }
            }
        } else {
            column.sql_type
        });
        if !column.nullable && !column.serial {
            line.push_str(" NOT NULL");
        }
        if let Some(default) = column.default {
            line.push_str(" DEFAULT ");
            line.push_str(default);
        }
        if let Some(key) = column.references {
            line.push_str(&format!(
                " REFERENCES {} ({})",
                ident(key.table)?,
                ident(key.column)?
            ));
            for (verb, action) in [("DELETE", key.on_delete), ("UPDATE", key.on_update)] {
                if let Some(action) = action {
                    line.push_str(&format!(" ON {verb} {action}"));
                }
            }
        }
        Ok(line)
    }

    /// `CREATE TABLE IF NOT EXISTS`, then the unique keys and indexes.
    pub fn create_sql(&self) -> Result<Vec<String>> {
        let mut lines = Vec::new();
        for column in self.columns {
            lines.push(Self::column_ddl(column)?);
        }
        let key: Vec<String> = self
            .columns
            .iter()
            .filter(|c| c.primary_key)
            .map(|c| ident(c.name))
            .collect::<Result<_>>()?;
        if !key.is_empty() {
            lines.push(format!("PRIMARY KEY ({})", key.join(", ")));
        }
        let mut statements = vec![format!(
            "CREATE TABLE IF NOT EXISTS {} ({})",
            ident(self.table)?,
            lines.join(", ")
        )];
        statements.extend(self.keys_sql()?);
        Ok(statements)
    }

    /// The unique keys and indexes, each a statement that does nothing when
    /// what it makes is already there. Named as PostgreSQL names them
    /// (`<table>_<column>_key`, `<table>_<column>_idx`), so running them again
    /// finds the same names.
    pub fn keys_sql(&self) -> Result<Vec<String>> {
        let mut statements = Vec::new();
        for column in self.columns {
            if column.unique && !column.primary_key {
                let index = crate::admin::Index::on(self.table, &[column.name])
                    .unique()
                    .if_not_exists();
                statements.push(index.to_sql()?);
            }
            if let Some(method) = column.index {
                let index = crate::admin::Index::on(self.table, &[column.name])
                    .using(method)
                    .if_not_exists();
                statements.push(index.to_sql()?);
            }
        }
        Ok(statements)
    }
}

/// Rows per statement in [`ModelExt::insert_many`], at most.
pub const INSERT_CHUNK_ROWS: usize = 500;
/// Bytes of statement text per [`ModelExt::insert_many`] statement, about:
/// well under the 8 MiB answer limit, which the `RETURNING` rows come back
/// under.
pub const INSERT_CHUNK_BYTES: usize = 1 << 20;

/// The ORM's calls on any [`Executor`]: `client.insert(&row)` and the rest,
/// on a [`Client`](crate::Client) or inside a transaction.
///
/// Implemented for every [`Executor`]; bring it into scope (the prelude
/// does) and call it on one. Every future is `Send`, and none borrows the
/// row it was given past the call: the statement is written before the
/// future is returned.
pub trait ModelExt: Executor {
    /// Creates the model's table and indexes, if absent.
    fn create_table<M: Model>(&self) -> impl Future<Output = Result<()>> + Send {
        let statements = create_table_sql::<M>();
        async move {
            for statement in statements? {
                self.run(&statement).await?;
            }
            Ok(())
        }
    }

    /// `DROP TABLE IF EXISTS`.
    fn drop_table<M: Model>(&self) -> impl Future<Output = Result<()>> + Send {
        let sql = ident(M::TABLE).map(|t| format!("DROP TABLE IF EXISTS {t}"));
        async move { self.run(&sql?).await.map(drop) }
    }

    /// Inserts one row and returns it as stored: serial columns and defaults
    /// filled in.
    fn insert<M: Model>(&self, row: &M) -> impl Future<Output = Result<M>> + Send {
        let sql = insert_sql(&[row]);
        async move {
            let rows = self.run(&sql?).await?;
            rows.decode_first()?.ok_or(Error::RowCount {
                expected: "exactly 1",
                got: 0,
            })
        }
    }

    /// Inserts many rows, up to [`INSERT_CHUNK_ROWS`] rows and about
    /// [`INSERT_CHUNK_BYTES`] of text to a statement, and returns them as
    /// stored. A statement is atomic; the whole is not, unless it runs in a
    /// transaction.
    fn insert_many<M: Model>(&self, rows: &[M]) -> impl Future<Output = Result<Vec<M>>> + Send {
        let chunks = insert_chunks(rows);
        let rows_len = rows.len();
        async move {
            // Answers are kept as they came and decoded at the end, so the
            // future holds no `M` across a send (`M` need not be `Send`).
            let mut answers = Vec::new();
            for sql in chunks? {
                answers.push(self.run(&sql).await?);
            }
            let mut out = Vec::with_capacity(rows_len);
            for rows in &answers {
                out.extend(rows.decode_all::<M>()?);
            }
            Ok(out)
        }
    }

    /// Inserts the row, or updates the one with its primary key. The row as
    /// stored. Every column is written as given, a serial key included: an
    /// upsert names its row, so set the key (a new row with `id: 0` is
    /// stored with id 0, not a sequence value; use [`insert`](Self::insert)
    /// for that).
    ///
    /// With a `#[rdb(version)]` column, a new row is stored at version 1 and
    /// an existing one is updated only at the version the struct holds;
    /// at another, [`Error::Conflict`].
    fn upsert<M: Model>(&self, row: &M) -> impl Future<Output = Result<M>> + Send {
        let sql = upsert_sql(row);
        let reread = reread_sql(row);
        let probe = VersionProbe::of(row);
        async move {
            let mut rows = self.run(&sql?).await?;
            if rows.is_empty()
                && let Some(probe) = probe?
            {
                return Err(probe.classify(self).await?.unwrap_or_else(|| {
                    Error::Conflict(
                        "the upsert stored nothing and no row has its key: it was deleted \
                         while the statement ran"
                            .to_string(),
                    )
                }));
            }
            if rows.is_empty() {
                // `DO NOTHING` (a model that is all key) returns no row: the
                // one already there is the answer.
                rows = self.run(&reread?).await?;
            }
            rows.decode_first()?.ok_or(Error::RowCount {
                expected: "exactly 1",
                got: 0,
            })
        }
    }

    /// Updates the row with this primary key to these values, returning it as
    /// stored; [`Error::RowCount`] when there is no such row.
    ///
    /// With a `#[rdb(version)]` column the update writes `version + 1` and
    /// matches only the row at the version the struct holds. When it matches
    /// nothing, one more statement reads the stored version to say why:
    /// [`Error::Conflict`] when the row is there at another version,
    /// [`Error::RowCount`] when it is gone.
    fn update<M: Model>(&self, row: &M) -> impl Future<Output = Result<M>> + Send {
        let sql = update_sql(row);
        let probe = VersionProbe::of(row);
        async move {
            let rows = self.run(&sql?).await?;
            if rows.is_empty()
                && let Some(probe) = probe?
                && let Some(conflict) = probe.classify(self).await?
            {
                return Err(conflict);
            }
            match rows.len() {
                1 => Ok(rows.decode_first()?.expect("one row")),
                got => Err(Error::RowCount {
                    expected: "exactly 1",
                    got,
                }),
            }
        }
    }

    /// Deletes the row with this primary key. Whether there was one.
    ///
    /// With a `#[rdb(version)]` column the delete matches only the row at
    /// the version the struct holds, so a row changed since it was read is
    /// not deleted: [`Error::Conflict`]. A row already gone is `Ok(false)`,
    /// as without a version (one more statement tells the two apart).
    fn delete<M: Model>(&self, row: &M) -> impl Future<Output = Result<bool>> + Send {
        let sql = delete_sql(row);
        let probe = VersionProbe::of(row);
        async move {
            let rows = self.run(&sql?).await?;
            let deleted = rows.rows_affected().unwrap_or(0) > 0;
            if !deleted
                && let Some(probe) = probe?
                && let Some(conflict) = probe.classify(self).await?
            {
                return Err(conflict);
            }
            Ok(deleted)
        }
    }

    /// The row whose (single-column) primary key is `key`.
    fn find<M: Model>(&self, key: impl ToSql) -> impl Future<Output = Result<Option<M>>> + Send {
        let sql = find_sql::<M>(&[&key]);
        async move { fetch_at_most_one(self, sql?).await }
    }

    /// The row whose composite primary key is `key`, in key column order.
    fn find_by_key<M: Model>(
        &self,
        key: &[&dyn ToSql],
    ) -> impl Future<Output = Result<Option<M>>> + Send {
        let sql = find_sql::<M>(key);
        async move { fetch_at_most_one(self, sql?).await }
    }

    /// Every row of the table.
    fn all<M: Model>(&self) -> impl Future<Output = Result<Vec<M>>> + Send {
        let sql = Select::<M>::new().to_sql();
        async move { self.run(&sql?).await?.decode_all() }
    }
}

impl<E: Executor> ModelExt for E {}

async fn fetch_at_most_one<E: Executor + ?Sized, M: Model>(
    db: &E,
    sql: String,
) -> Result<Option<M>> {
    let rows = db.run(&sql).await?;
    match rows.len() {
        0 | 1 => rows.decode_first(),
        got => Err(Error::RowCount {
            expected: "at most 1",
            got,
        }),
    }
}

/// `SELECT … WHERE <primary key>` for `m`'s key.
fn reread_sql<M: Model>(m: &M) -> Result<String> {
    let mut sql = format!(
        "SELECT {} FROM {} WHERE ",
        select_list::<M>()?,
        ident(M::TABLE)?
    );
    key_predicate(m, &mut sql)?;
    Ok(sql)
}

/// The statements [`ModelExt::insert_many`] sends: up to
/// [`INSERT_CHUNK_ROWS`] rows and about [`INSERT_CHUNK_BYTES`] each.
fn insert_chunks<M: Model>(rows: &[M]) -> Result<Vec<String>> {
    let (head, tail) = insert_frame::<M>()?;
    let mut out = Vec::new();
    let mut sql = head.clone();
    let mut in_chunk = 0usize;
    let mut tuple = String::new();
    for row in rows {
        tuple.clear();
        push_values_tuple(&mut tuple, row)?;
        // A full chunk goes before this row joins it; a row past the byte
        // bound on its own still goes, alone.
        if in_chunk > 0
            && (in_chunk == INSERT_CHUNK_ROWS || sql.len() + 2 + tuple.len() > INSERT_CHUNK_BYTES)
        {
            sql.push_str(&tail);
            out.push(std::mem::replace(&mut sql, head.clone()));
            in_chunk = 0;
        }
        if in_chunk > 0 {
            sql.push_str(", ");
        }
        sql.push_str(&tuple);
        in_chunk += 1;
    }
    if in_chunk > 0 {
        sql.push_str(&tail);
        out.push(sql);
    }
    Ok(out)
}

/// What every [`Model`] gets: [`query`](ModelQuery::query) for the builder.
pub trait ModelQuery: Model {
    /// A `SELECT` of this model's rows, to narrow with
    /// [`filter`](Select::filter) and run with [`all`](Select::all).
    fn query() -> Select<Self> {
        Select::new()
    }
}

impl<M: Model> ModelQuery for M {}

#[cfg(all(test, feature = "derive"))]
mod tests;
