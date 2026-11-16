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
//! # What the derive writes
//!
//! `#[derive(Model)]` implements [`Model`] and [`FromRow`](crate::FromRow),
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
//!
//! Struct attribute: `#[rdb(table = "name")]`, otherwise the struct's name in
//! snake case.
//!
//! # No transactions
//!
//! Each call is one statement and commits on its own (the native protocol
//! has no transaction blocks). [`ModelExt::insert_many`] is one multi-row
//! statement per chunk: atomic within a chunk, not across chunks.

mod migrate;
mod query;

pub use migrate::{
    AutoMigrate, BoxFuture, MigrationPlan, MigrationReport, Migrator, StepState, StepStatus,
    SyncPolicy,
};
pub use query::{Column, Expr, Order, Select, Update};

use crate::admin::IndexMethod;
use crate::client::Client;
use crate::params::{push_ident, push_param};
use crate::row::FromRow;
use crate::to_sql::ToSql;
use crate::{Error, Result};

/// One column of a [`Model`], as the derive describes it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColumnDef {
    pub name: &'static str,
    /// The DDL type: `BIGINT`, `TEXT`, `NUMERIC`, `JSONB`, `BIGINT[]`, …
    pub sql_type: &'static str,
    pub nullable: bool,
    pub primary_key: bool,
    /// Assigned by the server: never written by an insert.
    pub serial: bool,
    pub unique: bool,
    pub index: Option<IndexMethod>,
    /// A `DEFAULT` expression for the DDL.
    pub default: Option<&'static str>,
    /// Read as `col::text`: a type the native protocol sends in a form this
    /// client cannot decode (`jsonb`, arrays, `NUMERIC(p, s)`).
    pub read_as_text: bool,
    /// Names this column had before (`#[rdb(renamed_from = "…")]`): a
    /// migrator that finds one of them, and not this name, renames it.
    pub renamed_from: &'static [&'static str],
}

/// A struct stored as a table's rows. Derive it: `#[derive(Model)]`.
pub trait Model: FromRow + Sized {
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
    const SQL_TYPE: &'static str;
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
        row.write_value(i, out)?;
    }
    out.push(')');
    Ok(())
}

/// `INSERT … ON CONFLICT (<primary key>) DO UPDATE SET <every other column> =
/// EXCLUDED.<column> RETURNING …`. Every column is written, serial ones too:
/// an upsert names its row.
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
        m.write_value(i, &mut sql)?;
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
        sql.push_str(") DO UPDATE SET ");
        for (n, column) in rest.iter().enumerate() {
            if n > 0 {
                sql.push_str(", ");
            }
            let name = ident(column.name)?;
            sql.push_str(&format!("{name} = EXCLUDED.{name}"));
        }
    }
    sql.push_str(" RETURNING ");
    sql.push_str(&select_list::<M>()?);
    Ok(sql)
}

/// `UPDATE t SET <every non-key column> WHERE <key> RETURNING …`.
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
        m.write_value(i, &mut sql)?;
    }
    if !any {
        return Err(Error::Unsupported(format!(
            "{} has no column outside its primary key to update",
            M::TABLE
        )));
    }
    sql.push_str(" WHERE ");
    key_predicate(m, &mut sql)?;
    sql.push_str(" RETURNING ");
    sql.push_str(&select_list::<M>()?);
    Ok(sql)
}

pub fn delete_sql<M: Model>(m: &M) -> Result<String> {
    let mut sql = format!("DELETE FROM {} WHERE ", ident(M::TABLE)?);
    key_predicate(m, &mut sql)?;
    Ok(sql)
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
    let mut sql = format!("SELECT {} FROM {} WHERE ", select_list::<M>()?, ident(M::TABLE)?);
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
    pub table: &'static str,
    pub renamed_from: &'static [&'static str],
    pub columns: &'static [ColumnDef],
}

impl TableSchema {
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
        Ok(line)
    }

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

/// The ORM's calls on a [`Client`]: `client.insert(&row)` and the rest.
#[allow(async_fn_in_trait)]
pub trait ModelExt {
    /// Creates the model's table and indexes, if absent.
    async fn create_table<M: Model>(&self) -> Result<()>;
    /// `DROP TABLE IF EXISTS`.
    async fn drop_table<M: Model>(&self) -> Result<()>;
    /// Inserts one row and returns it as stored: serial columns and defaults
    /// filled in.
    async fn insert<M: Model>(&self, row: &M) -> Result<M>;
    /// Inserts many rows, up to [`INSERT_CHUNK_ROWS`] rows and about
    /// [`INSERT_CHUNK_BYTES`] of text to a statement, and returns them as
    /// stored. A statement is atomic; the whole is not.
    async fn insert_many<M: Model>(&self, rows: &[M]) -> Result<Vec<M>>;
    /// Inserts the row, or updates the one with its primary key. The row as
    /// stored. Every column is written as given, a serial key included: an
    /// upsert names its row, so set the key (a new row with `id: 0` is
    /// stored with id 0, not a sequence value; use [`insert`](Self::insert)
    /// for that).
    async fn upsert<M: Model>(&self, row: &M) -> Result<M>;
    /// Updates the row with this primary key to these values, returning it as
    /// stored; [`Error::RowCount`] when there is no such row.
    async fn update<M: Model>(&self, row: &M) -> Result<M>;
    /// Deletes the row with this primary key. Whether there was one.
    async fn delete<M: Model>(&self, row: &M) -> Result<bool>;
    /// The row whose (single-column) primary key is `key`.
    async fn find<M: Model>(&self, key: impl ToSql) -> Result<Option<M>>;
    /// The row whose composite primary key is `key`, in key column order.
    async fn find_by_key<M: Model>(&self, key: &[&dyn ToSql]) -> Result<Option<M>>;
    /// Every row of the table.
    async fn all<M: Model>(&self) -> Result<Vec<M>>;
}

impl ModelExt for Client {
    async fn create_table<M: Model>(&self) -> Result<()> {
        for statement in create_table_sql::<M>()? {
            self.run(&statement).await?;
        }
        Ok(())
    }

    async fn drop_table<M: Model>(&self) -> Result<()> {
        self.run(&format!("DROP TABLE IF EXISTS {}", ident(M::TABLE)?))
            .await
            .map(drop)
    }

    async fn insert<M: Model>(&self, row: &M) -> Result<M> {
        let rows = self.run(&insert_sql(&[row])?).await?;
        rows.decode_first()?
            .ok_or(Error::RowCount { expected: "exactly 1", got: 0 })
    }

    async fn insert_many<M: Model>(&self, rows: &[M]) -> Result<Vec<M>> {
        let (head, tail) = insert_frame::<M>()?;
        let mut out = Vec::with_capacity(rows.len());
        let mut sql = head.clone();
        let mut in_chunk = 0usize;
        let mut tuple = String::new();
        for row in rows {
            tuple.clear();
            push_values_tuple(&mut tuple, row)?;
            // A full chunk goes before this row joins it; a row past the
            // byte bound on its own still goes, alone.
            if in_chunk > 0
                && (in_chunk == INSERT_CHUNK_ROWS || sql.len() + 2 + tuple.len() > INSERT_CHUNK_BYTES)
            {
                sql.push_str(&tail);
                out.extend(self.run(&sql).await?.decode_all::<M>()?);
                sql.clear();
                sql.push_str(&head);
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
            out.extend(self.run(&sql).await?.decode_all::<M>()?);
        }
        Ok(out)
    }

    async fn upsert<M: Model>(&self, row: &M) -> Result<M> {
        let rows = self.run(&upsert_sql(row)?).await?;
        match rows.decode_first()? {
            Some(stored) => Ok(stored),
            // `DO NOTHING` (a model that is all key) returns no row: the one
            // already there is the answer.
            None => {
                let mut sql = format!("SELECT {} FROM {} WHERE ", select_list::<M>()?, ident(M::TABLE)?);
                key_predicate(row, &mut sql)?;
                self.run(&sql)
                    .await?
                    .decode_first()?
                    .ok_or(Error::RowCount { expected: "exactly 1", got: 0 })
            }
        }
    }

    async fn update<M: Model>(&self, row: &M) -> Result<M> {
        let rows = self.run(&update_sql(row)?).await?;
        match rows.len() {
            1 => Ok(rows.decode_first()?.expect("one row")),
            got => Err(Error::RowCount { expected: "exactly 1", got }),
        }
    }

    async fn delete<M: Model>(&self, row: &M) -> Result<bool> {
        let rows = self.run(&delete_sql(row)?).await?;
        Ok(rows.rows_affected().unwrap_or(0) > 0)
    }

    async fn find<M: Model>(&self, key: impl ToSql) -> Result<Option<M>> {
        self.find_by_key::<M>(&[&key]).await
    }

    async fn find_by_key<M: Model>(&self, key: &[&dyn ToSql]) -> Result<Option<M>> {
        let rows = self.run(&find_sql::<M>(key)?).await?;
        match rows.len() {
            0 | 1 => rows.decode_first(),
            got => Err(Error::RowCount { expected: "at most 1", got }),
        }
    }

    async fn all<M: Model>(&self) -> Result<Vec<M>> {
        Select::<M>::new().all(self).await
    }
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
