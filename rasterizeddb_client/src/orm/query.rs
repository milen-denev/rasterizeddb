//! Typed columns, predicates and the `SELECT`/`UPDATE`/`DELETE` builder.
//!
//! Values are written into the statement as literals when a predicate is
//! built, under the same rules as [`render`](crate::render), so a predicate
//! carries no borrowed value and can be stored, cloned and combined.

use std::fmt;
use std::marker::PhantomData;

use std::sync::Arc;

use super::keyset::{Cursor, Direction, keys, order_sql, predicate};
use super::relations::{Count, Eager, Includes, With};
use super::{BoxFuture, Model, ident, select_list};
use crate::executor::Executor;
use crate::params::{push_ident, push_param};
use crate::row::Rows;
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
    /// The column named `name`. The derive declares these; write one by hand for a column a model does not map.
    pub const fn new(name: &'static str) -> Self {
        Column {
            name,
            _model: PhantomData,
        }
    }

    /// The column's name.
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

    /// `column = value`.
    pub fn eq(&self, value: impl ToSql) -> Expr {
        self.compare(" = ", &value)
    }

    /// `column <> value`.
    pub fn ne(&self, value: impl ToSql) -> Expr {
        self.compare(" <> ", &value)
    }

    /// `column < value`.
    pub fn lt(&self, value: impl ToSql) -> Expr {
        self.compare(" < ", &value)
    }

    /// `column <= value`.
    pub fn le(&self, value: impl ToSql) -> Expr {
        self.compare(" <= ", &value)
    }

    /// `column > value`.
    pub fn gt(&self, value: impl ToSql) -> Expr {
        self.compare(" > ", &value)
    }

    /// `column >= value`.
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

    /// `column BETWEEN low AND high` (both ends included).
    pub fn between(&self, low: impl ToSql, high: impl ToSql) -> Expr {
        Expr::build(|out| {
            push_ident(out, self.name)?;
            out.push_str(" BETWEEN ");
            push_param(out, &low.to_param())?;
            out.push_str(" AND ");
            push_param(out, &high.to_param())
        })
    }

    /// `column IS NULL`.
    pub fn is_null(&self) -> Expr {
        Expr::build(|out| {
            push_ident(out, self.name)?;
            out.push_str(" IS NULL");
            Ok(())
        })
    }

    /// `column IS NOT NULL`.
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

    /// Ascending, for [`Select::order_by`].
    pub fn asc(&self) -> Order {
        Order::new(self.name, "ASC")
    }

    /// Descending, for [`Select::order_by`].
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
        Expr {
            sql: Ok(sql.to_string()),
        }
    }

    /// SQL this crate wrote itself, every value in it already a literal.
    pub(crate) fn trusted(sql: String) -> Expr {
        Expr { sql: Ok(sql) }
    }

    /// A predicate that could not be built: its error surfaces when the
    /// statement runs.
    pub(crate) fn failed(error: Error) -> Expr {
        Expr {
            sql: Err(error.to_string()),
        }
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
            sql: self
                .sql
                .and_then(|a| other.sql.map(|b| format!("({a}) {op} ({b})"))),
        }
    }

    /// Both predicates (`&` does the same).
    pub fn and(self, other: Expr) -> Expr {
        self.combine("AND", other)
    }

    /// Either predicate (`|` does the same).
    pub fn or(self, other: Expr) -> Expr {
        self.combine("OR", other)
    }

    /// The predicate's negation (`!` does the same).
    #[allow(clippy::should_implement_trait)]
    pub fn not(self) -> Expr {
        Expr {
            sql: self.sql.map(|s| format!("NOT ({s})")),
        }
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

/// An `ORDER BY` item: a column ([`Column::asc`]) or an aggregate
/// ([`Agg::asc`](super::Agg::asc)).
#[derive(Clone, Debug)]
pub struct Order {
    key: OrderKey,
    direction: &'static str,
    nulls: Option<&'static str>,
}

#[derive(Clone, Debug)]
enum OrderKey {
    Column(&'static str),
    Expression(std::result::Result<String, String>),
}

impl Order {
    fn new(column: &'static str, direction: &'static str) -> Self {
        Order {
            key: OrderKey::Column(column),
            direction,
            nulls: None,
        }
    }

    /// The column it sorts by, when it is one.
    pub(crate) fn column(&self) -> Option<&'static str> {
        match self.key {
            OrderKey::Column(c) => Some(c),
            OrderKey::Expression(_) => None,
        }
    }

    pub(crate) fn descending(&self) -> bool {
        self.direction == "DESC"
    }

    pub(crate) fn expression(
        sql: std::result::Result<String, String>,
        direction: &'static str,
    ) -> Self {
        Order {
            key: OrderKey::Expression(sql),
            direction,
            nulls: None,
        }
    }

    /// NULLs before every value.
    pub fn nulls_first(mut self) -> Self {
        self.nulls = Some("NULLS FIRST");
        self
    }

    /// NULLs after every value.
    pub fn nulls_last(mut self) -> Self {
        self.nulls = Some("NULLS LAST");
        self
    }

    fn push(&self, out: &mut String) -> Result<()> {
        match &self.key {
            OrderKey::Column(c) => push_ident(out, c)?,
            OrderKey::Expression(sql) => {
                out.push_str(sql.as_deref().map_err(|e| Error::Param(e.clone()))?)
            }
        }
        out.push(' ');
        out.push_str(self.direction);
        if let Some(nulls) = self.nulls {
            out.push(' ');
            out.push_str(nulls);
        }
        Ok(())
    }
}

/// Sends a statement on any executor, behind a reference that can be
/// stored: what a relation loaded after the rows arrive is sent on.
pub(crate) trait SendSql: Sync {
    fn send_sql<'a>(&'a self, sql: &'a str) -> BoxFuture<'a, Result<Rows>>;
}

impl<E: Executor> SendSql for E {
    fn send_sql<'a>(&'a self, sql: &'a str) -> BoxFuture<'a, Result<Rows>> {
        Box::pin(self.run(sql))
    }
}

/// A `SELECT` of model `M`'s rows: [`Model::query`](super::ModelQuery::query).
pub struct Select<M> {
    filter: Option<Expr>,
    order: Vec<Order>,
    limit: Option<u64>,
    offset: Option<u64>,
    every_row: bool,
    /// Relations and counts loaded after the rows arrive, in order.
    includes: Includes<M>,
    /// A keyset query's cursor, and which way it reads from it.
    keyset: Option<(Cursor, Direction)>,
    /// Which rows of a soft-deleting model it sees.
    scope: Scope,
    _model: PhantomData<fn() -> M>,
}

/// Which rows of a model with a `#[rdb(soft_delete)]` column a query sees.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Scope {
    Live,
    WithDeleted,
    OnlyDeleted,
}

impl<M> Clone for Select<M> {
    fn clone(&self) -> Self {
        Select {
            filter: self.filter.clone(),
            order: self.order.clone(),
            limit: self.limit,
            offset: self.offset,
            every_row: self.every_row,
            includes: self.includes.clone(),
            keyset: self.keyset.clone(),
            scope: self.scope,
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
            .field("includes", &self.includes.len())
            .finish()
    }
}

impl<M: Model> Default for Select<M> {
    fn default() -> Self {
        Self::new()
    }
}

impl<M: Model> Select<M> {
    /// Every row of the table, in no order.
    pub fn new() -> Self {
        Select {
            filter: None,
            order: Vec::new(),
            limit: None,
            offset: None,
            every_row: false,
            includes: Vec::new(),
            keyset: None,
            scope: Scope::Live,
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

    /// Sorts by `order`; several sort by the first, then the next.
    pub fn order_by(mut self, order: Order) -> Self {
        self.order.push(order);
        self
    }

    /// At most `n` rows.
    pub fn limit(mut self, n: u64) -> Self {
        self.limit = Some(n);
        self
    }

    /// Skips the first `n` rows (with an order, for a stable page).
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

    /// Loads `relation` for every row read, after the rows arrive: one
    /// statement per relation (per [`RELATION_CHUNK`](super::RELATION_CHUNK)
    /// parents), never one per row. Nest with
    /// `.with(User::posts.with(Post::comments))`; narrow with
    /// `.with(User::posts.filter(…).order_by(…).limit(3))`.
    pub fn with<C>(mut self, relation: impl Into<With<M, C>>) -> Self
    where
        M: Send + 'static,
        C: Model + Clone + Send + Sync + 'static,
    {
        self.includes.push(Arc::new(Eager(relation.into())));
        self
    }

    /// Counts `relation` for every row read, into the field marked
    /// `#[rdb(count_of = "<relation>")]`: one `GROUP BY` statement, never
    /// one per row. Filters on the relation narrow what is counted.
    pub fn with_count<C>(mut self, relation: impl Into<With<M, C>>) -> Self
    where
        M: Send + 'static,
        C: Model + Send + Sync + 'static,
    {
        self.includes.push(Arc::new(Count(relation.into())));
        self
    }

    /// Only rows with at least one related row (matching the relation's
    /// filters): `WHERE key IN (SELECT …)`.
    pub fn where_has<C: Model>(self, relation: impl Into<With<M, C>>) -> Self {
        let expr = relation.into().has_expr(false).unwrap_or_else(Expr::failed);
        self.filter(expr)
    }

    /// Only rows with no related row (matching the relation's filters):
    /// `WHERE key IS NULL OR key NOT IN (SELECT …)`.
    pub fn where_doesnt_have<C: Model>(self, relation: impl Into<With<M, C>>) -> Self {
        let expr = relation.into().has_expr(true).unwrap_or_else(Expr::failed);
        self.filter(expr)
    }

    pub(crate) fn has_order(&self) -> bool {
        !self.order.is_empty()
    }

    pub(crate) fn has_includes(&self) -> bool {
        !self.includes.is_empty()
    }

    /// Soft-deleted rows too: for a model with a `#[rdb(soft_delete)]`
    /// column, whose queries leave them out by default. (No effect on
    /// other models.)
    pub fn with_deleted(mut self) -> Self {
        self.scope = Scope::WithDeleted;
        self
    }

    /// Only soft-deleted rows.
    pub fn only_deleted(mut self) -> Self {
        self.scope = Scope::OnlyDeleted;
        self
    }

    /// The `WHERE` condition, without the keyword: the filter, and the
    /// soft-delete scope; `None` for every row.
    pub(crate) fn where_sql(&self) -> Result<Option<String>> {
        let scope = match (super::soft_delete_column::<M>(), self.scope) {
            (None, _) | (Some(_), Scope::WithDeleted) => None,
            (Some(c), Scope::Live) => Some(format!("{} IS NULL", ident(c)?)),
            (Some(c), Scope::OnlyDeleted) => Some(format!("{} IS NOT NULL", ident(c)?)),
        };
        Ok(match (&self.filter, scope) {
            (None, scope) => scope,
            (Some(filter), None) => Some(filter.to_sql()?.to_string()),
            (Some(filter), Some(scope)) => Some(format!("({}) AND {scope}", filter.to_sql()?)),
        })
    }

    fn push_where(&self, out: &mut String) -> Result<()> {
        if let Some(filter) = self.where_sql()? {
            out.push_str(" WHERE ");
            out.push_str(&filter);
        }
        Ok(())
    }

    /// The rows of `rows`, and every relation loaded into them; read
    /// before a cursor, they arrive nearest first and are put back in order.
    async fn finish(&self, db: &dyn SendSql, rows: Rows) -> Result<Vec<M>> {
        let mut out: Vec<M> = super::decode_models(&rows)?;
        if self.keyset_direction() == Some(Direction::Before) {
            out.reverse();
        }
        for include in &self.includes {
            include.load(db, &mut out).await?;
        }
        Ok(out)
    }

    /// [`all`](Self::all) behind a reference that can be stored, for a
    /// relation's rows.
    pub(crate) fn all_dyn<'a>(&'a self, db: &'a dyn SendSql) -> BoxFuture<'a, Result<Vec<M>>>
    where
        M: Send,
    {
        Box::pin(async move {
            let rows = db.send_sql(&self.to_sql()?).await?;
            self.finish(db, rows).await
        })
    }

    /// The rows after `cursor` in the query's order (a keyset page): see
    /// [`page`](Self::page). Replaces an earlier `after` or `before`.
    pub fn after(mut self, cursor: &Cursor) -> Self {
        self.keyset = Some((cursor.clone(), Direction::After));
        self
    }

    /// The rows before `cursor` in the query's order, the nearest last:
    /// the page before a page. Replaces an earlier `after` or `before`.
    pub fn before(mut self, cursor: &Cursor) -> Self {
        self.keyset = Some((cursor.clone(), Direction::Before));
        self
    }

    pub(crate) fn keyset_direction(&self) -> Option<Direction> {
        self.keyset.as_ref().map(|(_, d)| *d)
    }

    pub(crate) fn order_list(&self) -> &[Order] {
        &self.order
    }

    pub(crate) fn limit_value(&self) -> Option<u64> {
        self.limit
    }

    pub(crate) fn page_size(&self) -> Result<u64> {
        match self.limit {
            Some(n) if n > 0 => Ok(n),
            _ => Err(Error::Param(
                "a page needs its size: set limit(n) to a positive number".to_string(),
            )),
        }
    }

    /// A keyset statement: the filter, the cursor's predicate, `ORDER BY`
    /// the keys (reversed to read before a cursor), and `LIMIT`.
    pub(crate) fn keyset_sql(
        &self,
        keys: &[super::keyset::Key],
        limit: Option<u64>,
    ) -> Result<String> {
        if self.offset.is_some() {
            return Err(Error::Unsupported(
                "a keyset page starts at a cursor, not an offset: drop offset()".to_string(),
            ));
        }
        let mut sql = format!("SELECT {} FROM {}", select_list::<M>()?, ident(M::TABLE)?);
        let mut conditions = Vec::new();
        if let Some(filter) = self.where_sql()? {
            conditions.push(format!("({filter})"));
        }
        if let Some((cursor, direction)) = &self.keyset {
            conditions.push(predicate(keys, cursor, *direction)?);
        }
        if !conditions.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&conditions.join(" AND "));
        }
        sql.push_str(" ORDER BY ");
        sql.push_str(&order_sql(
            keys,
            self.keyset_direction() == Some(Direction::Before),
        )?);
        if let Some(limit) = limit {
            sql.push_str(&format!(" LIMIT {limit}"));
        }
        Ok(sql)
    }

    /// Rows `order` of `rows`, in that order, with their relations.
    pub(crate) async fn finish_rows(
        &self,
        db: &dyn SendSql,
        rows: &Rows,
        order: &[usize],
    ) -> Result<Vec<M>> {
        let mut out: Vec<M> = order
            .iter()
            .map(|&i| {
                let mut m: M = rows.get(i).expect("in range").decode()?;
                m.__rdb_after_load();
                Ok(m)
            })
            .collect::<Result<_>>()?;
        for include in &self.includes {
            include.load(db, &mut out).await?;
        }
        Ok(out)
    }

    /// The statement [`all`](Self::all) runs.
    pub fn to_sql(&self) -> Result<String> {
        if self.keyset.is_some() {
            return self.keyset_sql(&keys::<M>(&self.order)?, self.limit);
        }
        let mut sql = format!("SELECT {} FROM {}", select_list::<M>()?, ident(M::TABLE)?);
        self.push_where(&mut sql)?;
        self.push_tail(&mut sql)?;
        Ok(sql)
    }

    /// `ORDER BY …`, `LIMIT`, `OFFSET`.
    pub(crate) fn push_tail(&self, sql: &mut String) -> Result<()> {
        if !self.order.is_empty() {
            sql.push_str(" ORDER BY ");
            for (i, order) in self.order.iter().enumerate() {
                if i > 0 {
                    sql.push_str(", ");
                }
                order.push(sql)?;
            }
        }
        if let Some(limit) = self.limit {
            sql.push_str(&format!(" LIMIT {limit}"));
        }
        if let Some(offset) = self.offset {
            sql.push_str(&format!(" OFFSET {offset}"));
        }
        Ok(())
    }

    /// Every matching row, with the relations [`with`](Self::with) names
    /// loaded into them.
    pub async fn all<E: Executor>(&self, db: &E) -> Result<Vec<M>> {
        let rows = db.run(&self.to_sql()?).await?;
        self.finish(db, rows).await
    }

    /// The first row, if any (this adds `LIMIT 1`).
    pub async fn first<E: Executor>(&self, db: &E) -> Result<Option<M>> {
        let one = self.clone().limit(1);
        let rows = db.run(&one.to_sql()?).await?;
        Ok(self.finish(db, rows).await?.into_iter().next())
    }

    /// Exactly one row; none or several is [`Error::RowCount`].
    pub async fn one<E: Executor>(&self, db: &E) -> Result<M> {
        let rows = db.run(&self.clone().limit(2).to_sql()?).await?;
        match rows.len() {
            1 => Ok(self.finish(db, rows).await?.pop().expect("one row")),
            got => Err(Error::RowCount {
                expected: "exactly 1",
                got,
            }),
        }
    }

    /// `SELECT count(*)` over the filtered rows (limit and offset ignored).
    pub async fn count<E: Executor>(&self, db: &E) -> Result<u64> {
        let mut sql = format!("SELECT COUNT(*) FROM {}", ident(M::TABLE)?);
        self.push_where(&mut sql)?;
        db.fetch_scalar::<i64>(&sql, &[]).await.map(|n| n as u64)
    }

    /// Whether any row matches.
    pub async fn exists<E: Executor>(&self, db: &E) -> Result<bool> {
        let mut sql = format!(
            "SELECT {} FROM {}",
            ident(M::COLUMNS.first().map_or("*", |c| c.name))?,
            ident(M::TABLE)?
        );
        self.push_where(&mut sql)?;
        sql.push_str(" LIMIT 1");
        Ok(!db.run(&sql).await?.is_empty())
    }

    fn check_scope(&self, verb: &str) -> Result<()> {
        if self.filter.is_none() && !self.every_row {
            return Err(Error::Unsupported(format!(
                "{verb} with no filter would touch every row of {}: call every_row() if that is meant",
                M::TABLE
            )));
        }
        if self.limit.is_some()
            || self.offset.is_some()
            || !self.order.is_empty()
            || self.keyset.is_some()
            || !self.includes.is_empty()
        {
            return Err(Error::Unsupported(format!(
                "{verb} takes a filter only: limit, offset, order, a cursor and relations do \
                 not apply"
            )));
        }
        Ok(())
    }

    /// Deletes the filtered rows. How many.
    ///
    /// For a model with a `#[rdb(soft_delete)]` column, it marks them
    /// deleted instead (`UPDATE … SET deleted_at = now()`, with the version
    /// and update time), the ones not deleted yet;
    /// [`force_delete`](Self::force_delete) removes them.
    pub async fn delete<E: Executor>(&self, db: &E) -> Result<u64> {
        let sql = self.delete_sql(false)?;
        Ok(db.run(&sql).await?.rows_affected().unwrap_or(0))
    }

    /// [`delete`](Self::delete), returning the rows deleted as they were
    /// (for a soft delete, as marked): `… RETURNING`, from the statement
    /// itself, so no other write can come between the delete and the read.
    pub async fn delete_returning<E: Executor>(&self, db: &E) -> Result<Vec<M>> {
        let mut sql = self.delete_sql(false)?;
        sql.push_str(" RETURNING ");
        sql.push_str(&select_list::<M>()?);
        super::decode_models(&db.run(&sql).await?)
    }

    /// The statement `delete` (or, `force`d, `force_delete`) sends.
    pub(crate) fn delete_sql(&self, force: bool) -> Result<String> {
        self.check_scope("DELETE")?;
        let column = match super::soft_delete_column::<M>() {
            Some(column) if !force => column,
            _ => {
                let mut sql = format!("DELETE FROM {}", ident(M::TABLE)?);
                self.push_where(&mut sql)?;
                return Ok(sql);
            }
        };
        let column = ident(column)?;
        let mut sets = vec![format!("{column} = now()")];
        sets.extend(super::maintained_sets::<M>(None)?);
        let mut live = self.clone();
        if live.scope == Scope::WithDeleted {
            live.scope = Scope::Live;
        }
        let mut sql = format!("UPDATE {} SET {}", ident(M::TABLE)?, sets.join(", "));
        live.push_where(&mut sql)?;
        Ok(sql)
    }

    /// Removes the filtered rows with a `DELETE`, soft-deleting model or
    /// not; for a soft-deleting model, the rows the query sees (add
    /// [`with_deleted`](Self::with_deleted) to include deleted ones). How
    /// many.
    pub async fn force_delete<E: Executor>(&self, db: &E) -> Result<u64> {
        let sql = self.delete_sql(true)?;
        Ok(db.run(&sql).await?.rows_affected().unwrap_or(0))
    }

    /// An `UPDATE` of the filtered rows: add [`set`](Update::set)s, then
    /// [`run`](Update::run) it.
    pub fn update(&self) -> Update<M> {
        Update {
            select: self.clone(),
            sets: Ok(Vec::new()),
        }
    }
}

/// `UPDATE … SET … WHERE …`, from [`Select::update`].
pub struct Update<M> {
    select: Select<M>,
    /// Each assignment, with the column it assigns.
    sets: std::result::Result<Vec<(&'static str, String)>, String>,
}

impl<M: Model> Update<M> {
    /// `column = value`.
    pub fn set<T>(mut self, column: Column<M, T>, value: impl ToSql) -> Self {
        if let Ok(sets) = &mut self.sets {
            let mut out = String::new();
            let written = push_ident(&mut out, column.name).and_then(|()| {
                out.push_str(" = ");
                push_param(&mut out, &value.to_param())
            });
            match written {
                Ok(()) => sets.push((column.name, out)),
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
                Ok(()) => sets.push((column.name, out)),
                Err(e) => self.sets = Err(e.to_string()),
            }
        }
        self
    }

    /// The statement [`run`](Self::run) sends.
    pub fn to_sql(&self) -> Result<String> {
        self.select.check_scope("UPDATE")?;
        let sets = self.sets.as_ref().map_err(|e| Error::Param(e.clone()))?;
        if sets.is_empty() {
            return Err(Error::Param(
                "an UPDATE needs at least one set()".to_string(),
            ));
        }
        let mut assignments: Vec<String> = sets.iter().map(|(_, a)| a.clone()).collect();
        // A versioned model's rows move to the next version whenever they
        // change, so a holder of the old version cannot overwrite this; an
        // update time moves too. Unless a `set` names the column itself.
        for (role, sql) in [
            (super::ColumnRole::Version, "{} + 1"),
            (super::ColumnRole::UpdatedAt, "now()"),
        ] {
            if let Some(i) = super::role_column::<M>(role) {
                let name = M::COLUMNS[i].name;
                if !sets.iter().any(|(c, _)| *c == name) {
                    let quoted = ident(name)?;
                    assignments.push(format!("{quoted} = {}", sql.replace("{}", &quoted)));
                }
            }
        }
        let mut sql = format!("UPDATE {} SET {}", ident(M::TABLE)?, assignments.join(", "));
        self.select.push_where(&mut sql)?;
        sql.push_str(" RETURNING ");
        sql.push_str(&select_list::<M>()?);
        Ok(sql)
    }

    /// Runs it, and returns the rows as they now are. A model with a
    /// `#[rdb(version)]` column has `version = version + 1` added, and one
    /// with `#[rdb(updated_at)]` `updated_at = now()`, unless a `set` names
    /// that column itself. Soft-deleted rows are left alone unless the
    /// query says [`with_deleted`](Select::with_deleted).
    pub async fn run<E: Executor>(&self, db: &E) -> Result<Vec<M>> {
        super::decode_models(&db.run(&self.to_sql()?).await?)
    }
}
