//! Typed columns, predicates and the `SELECT`/`UPDATE`/`DELETE` builder.
//!
//! Values are written into the statement as literals when a predicate is
//! built, under the same rules as [`render`](crate::render), so a predicate
//! carries no borrowed value and can be stored, cloned and combined.

use std::fmt;
use std::marker::PhantomData;

use super::{Model, ident, select_list};
use crate::client::Client;
use crate::params::{push_ident, push_param};
use crate::to_sql::ToSql;
use crate::{Error, Result};

/// A column of model `M` holding `T`, as the derive declares it
/// (`Account::email`).
pub struct Column<M, T> {
    name: &'static str,
    _model: PhantomData<fn() -> (M, T)>,
}

impl<M, T> Clone for Column<M, T> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<M, T> Copy for Column<M, T> {}

impl<M, T> fmt::Debug for Column<M, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Column({})", self.name)
    }
}

impl<M, T> Column<M, T> {
    pub const fn new(name: &'static str) -> Self {
        Column { name, _model: PhantomData }
    }

    pub const fn name(&self) -> &'static str {
        self.name
    }

    fn compare(&self, op: &str, value: &dyn ToSql) -> Expr {
        Expr::build(|out| {
            push_ident(out, self.name)?;
            out.push_str(op);
            push_param(out, &value.to_param())
        })
    }

    pub fn eq(&self, value: impl ToSql) -> Expr {
        self.compare(" = ", &value)
    }

    pub fn ne(&self, value: impl ToSql) -> Expr {
        self.compare(" <> ", &value)
    }

    pub fn lt(&self, value: impl ToSql) -> Expr {
        self.compare(" < ", &value)
    }

    pub fn le(&self, value: impl ToSql) -> Expr {
        self.compare(" <= ", &value)
    }

    pub fn gt(&self, value: impl ToSql) -> Expr {
        self.compare(" > ", &value)
    }

    pub fn ge(&self, value: impl ToSql) -> Expr {
        self.compare(" >= ", &value)
    }

    /// `LIKE`: `%` any run, `_` one character.
    pub fn like(&self, pattern: &str) -> Expr {
        self.compare(" LIKE ", &pattern)
    }

    /// `ILIKE`: [`like`](Self::like), ignoring case.
    pub fn ilike(&self, pattern: &str) -> Expr {
        self.compare(" ILIKE ", &pattern)
    }

    pub fn between(&self, low: impl ToSql, high: impl ToSql) -> Expr {
        Expr::build(|out| {
            push_ident(out, self.name)?;
            out.push_str(" BETWEEN ");
            push_param(out, &low.to_param())?;
            out.push_str(" AND ");
            push_param(out, &high.to_param())
        })
    }

    pub fn is_null(&self) -> Expr {
        Expr::build(|out| {
            push_ident(out, self.name)?;
            out.push_str(" IS NULL");
            Ok(())
        })
    }

    pub fn is_not_null(&self) -> Expr {
        Expr::build(|out| {
            push_ident(out, self.name)?;
            out.push_str(" IS NOT NULL");
            Ok(())
        })
    }

    /// `col IN (…)`. An empty list matches nothing.
    pub fn is_in<V: ToSql>(&self, values: impl IntoIterator<Item = V>) -> Expr {
        self.in_list(values, false)
    }

    /// `col NOT IN (…)`. An empty list matches every row.
    pub fn not_in<V: ToSql>(&self, values: impl IntoIterator<Item = V>) -> Expr {
        self.in_list(values, true)
    }

    fn in_list<V: ToSql>(&self, values: impl IntoIterator<Item = V>, negated: bool) -> Expr {
        let values: Vec<V> = values.into_iter().collect();
        if values.is_empty() {
            // The engine reads no boolean constant as a predicate; a
            // comparison of constants it does.
            return Expr::raw_unchecked(if negated { "1 = 1" } else { "1 = 0" });
        }
        Expr::build(|out| {
            push_ident(out, self.name)?;
            out.push_str(if negated { " NOT IN (" } else { " IN (" });
            for (i, v) in values.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                push_param(out, &v.to_param())?;
            }
            out.push(')');
            Ok(())
        })
    }

    pub fn asc(&self) -> Order {
        Order::new(self.name, "ASC")
    }

    pub fn desc(&self) -> Order {
        Order::new(self.name, "DESC")
    }
}

/// A predicate: built from [`Column`]s, combined with `&`, `|` and `!` (or
/// [`and`](Expr::and), [`or`](Expr::or), [`not`](Expr::not)).
///
/// A value that cannot be written (text holding a NUL) is kept as the
/// predicate's error and reported when the statement runs.
#[derive(Clone, Debug)]
pub struct Expr {
    sql: std::result::Result<String, String>,
}

impl Expr {
    fn build(f: impl FnOnce(&mut String) -> Result<()>) -> Expr {
        let mut out = String::new();
        Expr {
            sql: f(&mut out).map(|()| out).map_err(|e| e.to_string()),
        }
    }

    fn raw_unchecked(sql: &str) -> Expr {
        Expr { sql: Ok(sql.to_string()) }
    }

    /// SQL of your own, with `$n` placeholders for `params`, for what the
    /// builder does not express (`lower(name) = $1`, a JSON path). The text
    /// is yours: only the parameters are escaped.
    pub fn raw(sql: &str, params: &[&dyn ToSql]) -> Expr {
        Expr {
            sql: crate::params::render_dyn(sql, params).map_err(|e| e.to_string()),
        }
    }

    fn combine(self, op: &str, other: Expr) -> Expr {
        Expr {
            sql: self.sql.and_then(|a| other.sql.map(|b| format!("({a}) {op} ({b})"))),
        }
    }

    pub fn and(self, other: Expr) -> Expr {
        self.combine("AND", other)
    }

    pub fn or(self, other: Expr) -> Expr {
        self.combine("OR", other)
    }

    #[allow(clippy::should_implement_trait)]
    pub fn not(self) -> Expr {
        Expr { sql: self.sql.map(|s| format!("NOT ({s})")) }
    }

    /// Every predicate (`AND`); no predicates match every row.
    pub fn all(exprs: impl IntoIterator<Item = Expr>) -> Expr {
        exprs
            .into_iter()
            .reduce(Expr::and)
            .unwrap_or_else(|| Expr::raw_unchecked("1 = 1"))
    }

    /// Any predicate (`OR`); no predicates match no row.
    pub fn any(exprs: impl IntoIterator<Item = Expr>) -> Expr {
        exprs
            .into_iter()
            .reduce(Expr::or)
            .unwrap_or_else(|| Expr::raw_unchecked("1 = 0"))
    }

    /// The predicate's SQL.
    pub fn to_sql(&self) -> Result<&str> {
        self.sql.as_deref().map_err(|e| Error::Param(e.clone()))
    }
}

impl std::ops::BitAnd for Expr {
    type Output = Expr;
    fn bitand(self, other: Expr) -> Expr {
        self.and(other)
    }
}

impl std::ops::BitOr for Expr {
    type Output = Expr;
    fn bitor(self, other: Expr) -> Expr {
        self.or(other)
    }
}

impl std::ops::Not for Expr {
    type Output = Expr;
    fn not(self) -> Expr {
        Expr::not(self)
    }
}

/// An `ORDER BY` item.
#[derive(Clone, Debug)]
pub struct Order {
    column: &'static str,
    direction: &'static str,
    nulls: Option<&'static str>,
}

impl Order {
    fn new(column: &'static str, direction: &'static str) -> Self {
        Order { column, direction, nulls: None }
    }

    pub fn nulls_first(mut self) -> Self {
        self.nulls = Some("NULLS FIRST");
        self
    }

    pub fn nulls_last(mut self) -> Self {
        self.nulls = Some("NULLS LAST");
        self
    }

    fn push(&self, out: &mut String) -> Result<()> {
        push_ident(out, self.column)?;
        out.push(' ');
        out.push_str(self.direction);
        if let Some(nulls) = self.nulls {
            out.push(' ');
            out.push_str(nulls);
        }
        Ok(())
    }
}

/// A `SELECT` of model `M`'s rows: [`Model::query`](super::ModelQuery::query).
pub struct Select<M> {
    filter: Option<Expr>,
    order: Vec<Order>,
    limit: Option<u64>,
    offset: Option<u64>,
    every_row: bool,
    _model: PhantomData<fn() -> M>,
}

impl<M> Clone for Select<M> {
    fn clone(&self) -> Self {
        Select {
            filter: self.filter.clone(),
            order: self.order.clone(),
            limit: self.limit,
            offset: self.offset,
            every_row: self.every_row,
            _model: PhantomData,
        }
    }
}

impl<M> fmt::Debug for Select<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Select")
            .field("filter", &self.filter)
            .field("order", &self.order)
            .field("limit", &self.limit)
            .field("offset", &self.offset)
            .finish()
    }
}

impl<M: Model> Default for Select<M> {
    fn default() -> Self {
        Self::new()
    }
}

impl<M: Model> Select<M> {
    pub fn new() -> Self {
        Select {
            filter: None,
            order: Vec::new(),
            limit: None,
            offset: None,
            every_row: false,
            _model: PhantomData,
        }
    }

    /// Narrows the rows; several filters must all hold.
    pub fn filter(mut self, expr: Expr) -> Self {
        self.filter = Some(match self.filter.take() {
            None => expr,
            Some(previous) => previous.and(expr),
        });
        self
    }

    pub fn order_by(mut self, order: Order) -> Self {
        self.order.push(order);
        self
    }

    pub fn limit(mut self, n: u64) -> Self {
        self.limit = Some(n);
        self
    }

    pub fn offset(mut self, n: u64) -> Self {
        self.offset = Some(n);
        self
    }

    /// Lets [`delete`](Self::delete) and [`update`](Self::update) run with no
    /// filter, on every row. Without it they refuse, so a forgotten filter
    /// does not empty a table.
    pub fn every_row(mut self) -> Self {
        self.every_row = true;
        self
    }

    fn push_where(&self, out: &mut String) -> Result<()> {
        if let Some(filter) = &self.filter {
            out.push_str(" WHERE ");
            out.push_str(filter.to_sql()?);
        }
        Ok(())
    }

    /// The statement [`all`](Self::all) runs.
    pub fn to_sql(&self) -> Result<String> {
        let mut sql = format!("SELECT {} FROM {}", select_list::<M>()?, ident(M::TABLE)?);
        self.push_where(&mut sql)?;
        if !self.order.is_empty() {
            sql.push_str(" ORDER BY ");
            for (i, order) in self.order.iter().enumerate() {
                if i > 0 {
                    sql.push_str(", ");
                }
                order.push(&mut sql)?;
            }
        }
        if let Some(limit) = self.limit {
            sql.push_str(&format!(" LIMIT {limit}"));
        }
        if let Some(offset) = self.offset {
            sql.push_str(&format!(" OFFSET {offset}"));
        }
        Ok(sql)
    }

    pub async fn all(&self, client: &Client) -> Result<Vec<M>> {
        client.run(&self.to_sql()?).await?.decode_all()
    }

    /// The first row, if any (this adds `LIMIT 1`).
    pub async fn first(&self, client: &Client) -> Result<Option<M>> {
        let one = self.clone().limit(1);
        client.run(&one.to_sql()?).await?.decode_first()
    }

    /// Exactly one row; none or several is [`Error::RowCount`].
    pub async fn one(&self, client: &Client) -> Result<M> {
        let rows = client.run(&self.clone().limit(2).to_sql()?).await?;
        match rows.len() {
            1 => Ok(rows.decode_first()?.expect("one row")),
            got => Err(Error::RowCount { expected: "exactly 1", got }),
        }
    }

    /// `SELECT count(*)` over the filtered rows (limit and offset ignored).
    pub async fn count(&self, client: &Client) -> Result<u64> {
        let mut sql = format!("SELECT COUNT(*) FROM {}", ident(M::TABLE)?);
        self.push_where(&mut sql)?;
        client.fetch_scalar::<i64>(&sql, &[]).await.map(|n| n as u64)
    }

    pub async fn exists(&self, client: &Client) -> Result<bool> {
        let mut sql = format!(
            "SELECT {} FROM {}",
            ident(M::COLUMNS.first().map_or("*", |c| c.name))?,
            ident(M::TABLE)?
        );
        self.push_where(&mut sql)?;
        sql.push_str(" LIMIT 1");
        Ok(!client.run(&sql).await?.is_empty())
    }

    fn check_scope(&self, verb: &str) -> Result<()> {
        if self.filter.is_none() && !self.every_row {
            return Err(Error::Unsupported(format!(
                "{verb} with no filter would touch every row of {}: call every_row() if that is meant",
                M::TABLE
            )));
        }
        if self.limit.is_some() || self.offset.is_some() || !self.order.is_empty() {
            return Err(Error::Unsupported(format!(
                "{verb} takes a filter only: limit, offset and order do not apply"
            )));
        }
        Ok(())
    }

    /// Deletes the filtered rows. How many.
    pub async fn delete(&self, client: &Client) -> Result<u64> {
        self.check_scope("DELETE")?;
        let mut sql = format!("DELETE FROM {}", ident(M::TABLE)?);
        self.push_where(&mut sql)?;
        Ok(client.run(&sql).await?.rows_affected().unwrap_or(0))
    }

    /// An `UPDATE` of the filtered rows: add [`set`](Update::set)s, then
    /// [`run`](Update::run) it.
    pub fn update(&self) -> Update<M> {
        Update { select: self.clone(), sets: Ok(Vec::new()) }
    }
}

/// `UPDATE … SET … WHERE …`, from [`Select::update`].
pub struct Update<M> {
    select: Select<M>,
    sets: std::result::Result<Vec<String>, String>,
}

impl<M: Model> Update<M> {
    /// `column = value`.
    pub fn set<T>(mut self, column: Column<M, T>, value: impl ToSql) -> Self {
        if let Ok(sets) = &mut self.sets {
            let mut out = String::new();
            let written = push_ident(&mut out, column.name)
                .and_then(|()| {
                    out.push_str(" = ");
                    push_param(&mut out, &value.to_param())
                });
            match written {
                Ok(()) => sets.push(out),
                Err(e) => self.sets = Err(e.to_string()),
            }
        }
        self
    }

    /// `column = <sql>`, with `$n` placeholders for `params`:
    /// `.set_raw(Account::visits, "visits + $1", params![1])`.
    pub fn set_raw<T>(mut self, column: Column<M, T>, sql: &str, params: &[&dyn ToSql]) -> Self {
        if let Ok(sets) = &mut self.sets {
            let mut out = String::new();
            let written = push_ident(&mut out, column.name).and_then(|()| {
                out.push_str(" = ");
                out.push_str(&crate::params::render_dyn(sql, params)?);
                Ok(())
            });
            match written {
                Ok(()) => sets.push(out),
                Err(e) => self.sets = Err(e.to_string()),
            }
        }
        self
    }

    pub fn to_sql(&self) -> Result<String> {
        self.select.check_scope("UPDATE")?;
        let sets = self.sets.as_ref().map_err(|e| Error::Param(e.clone()))?;
        if sets.is_empty() {
            return Err(Error::Param("an UPDATE needs at least one set()".to_string()));
        }
        let mut sql = format!("UPDATE {} SET {}", ident(M::TABLE)?, sets.join(", "));
        self.select.push_where(&mut sql)?;
        sql.push_str(" RETURNING ");
        sql.push_str(&select_list::<M>()?);
        Ok(sql)
    }

    /// Runs it, and returns the rows as they now are.
    pub async fn run(&self, client: &Client) -> Result<Vec<M>> {
        client.run(&self.to_sql()?).await?.decode_all()
    }
}
