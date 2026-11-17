//! Projections and aggregates: chosen columns and `COUNT`/`SUM`/`AVG`/`MIN`/
//! `MAX` into tuples or [`FromRow`] structs, with `GROUP BY` and `HAVING`.

use std::fmt;
use std::marker::PhantomData;

use super::query::{Column, Expr, Order, Select};
use super::{Model, ident};
use crate::executor::Executor;
use crate::from_sql::FromSqlOwned;
use crate::params::push_param;
use crate::row::{FromRow, Row};
use crate::to_sql::ToSql;
use crate::types::Numeric;
use crate::{Error, Result};

/// Whether model `M` reads column `name` as `::text`.
fn reads_as_text<M: Model>(name: &str) -> bool {
    M::COLUMNS.iter().any(|c| c.name == name && c.read_as_text)
}

/// A column the projection reads, cast as the model reads it.
fn column_sql<M: Model>(name: &'static str) -> std::result::Result<String, String> {
    let mut out = ident(name).map_err(|e| e.to_string())?;
    if reads_as_text::<M>(name) {
        out.push_str("::text");
    }
    Ok(out)
}

// ------------------------------------------------------------------ Agg

/// An aggregate over model `M`'s rows, read as `T`: from
/// [`Column::count`], [`sum`](Column::sum), [`avg`](Column::avg),
/// [`min`](Column::min), [`max`](Column::max), or [`count_rows`]. Select it
/// with [`Select::select`], compare it in [`Projected::having`], sort by it
/// with [`asc`](Self::asc) / [`desc`](Self::desc).
pub struct Agg<M, T> {
    /// What the select list reads (with a cast to make it decodable).
    read: std::result::Result<String, String>,
    /// The bare aggregate, for `HAVING` and `ORDER BY`.
    bare: std::result::Result<String, String>,
    _types: PhantomData<fn() -> (M, T)>,
}

impl<M, T> Clone for Agg<M, T> {
    fn clone(&self) -> Self {
        Agg {
            read: self.read.clone(),
            bare: self.bare.clone(),
            _types: PhantomData,
        }
    }
}

impl<M, T> fmt::Debug for Agg<M, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Agg({:?})", self.read)
    }
}

impl<M, T> Agg<M, T> {
    fn new(bare: std::result::Result<String, String>, cast: impl FnOnce(&str) -> String) -> Self {
        Agg {
            read: bare.as_deref().map(cast).map_err(|e| e.clone()),
            bare,
            _types: PhantomData,
        }
    }

    fn compare(&self, op: &str, value: &dyn ToSql) -> Expr {
        match &self.bare {
            Ok(bare) => {
                let mut out = format!("{bare}{op}");
                match push_param(&mut out, &value.to_param()) {
                    Ok(()) => Expr::trusted(out),
                    Err(e) => Expr::failed(e),
                }
            }
            Err(e) => Expr::failed(Error::Param(e.clone())),
        }
    }

    /// `aggregate = value`, for [`Projected::having`].
    pub fn eq(&self, value: impl ToSql) -> Expr {
        self.compare(" = ", &value)
    }

    /// `aggregate <> value`.
    pub fn ne(&self, value: impl ToSql) -> Expr {
        self.compare(" <> ", &value)
    }

    /// `aggregate > value`.
    pub fn gt(&self, value: impl ToSql) -> Expr {
        self.compare(" > ", &value)
    }

    /// `aggregate >= value`.
    pub fn ge(&self, value: impl ToSql) -> Expr {
        self.compare(" >= ", &value)
    }

    /// `aggregate < value`.
    pub fn lt(&self, value: impl ToSql) -> Expr {
        self.compare(" < ", &value)
    }

    /// `aggregate <= value`.
    pub fn le(&self, value: impl ToSql) -> Expr {
        self.compare(" <= ", &value)
    }

    /// Ascending by this aggregate, for [`Projected::order_by`].
    pub fn asc(&self) -> Order {
        Order::expression(self.bare.clone(), "ASC")
    }

    /// Descending by this aggregate.
    pub fn desc(&self) -> Order {
        Order::expression(self.bare.clone(), "DESC")
    }
}

/// `COUNT(*)`: the rows in each group (or in all, without `GROUP BY`).
pub fn count_rows<M>() -> Agg<M, i64> {
    Agg::new(Ok("COUNT(*)".to_string()), str::to_string)
}

/// What [`Column::sum`] and [`Column::avg`] read a column type's sum and
/// average as, and the cast that makes each decodable over the native
/// protocol.
pub trait Aggregate {
    /// The type of `SUM(column)`.
    type Sum: FromSqlOwned;
    /// The type of `AVG(column)`.
    type Avg: FromSqlOwned;
    /// `SUM` with its cast: over an integer, `CAST(SUM(x) AS BIGINT)`, the
    /// cast outside the sum, since `SUM(bigint)` is a `NUMERIC` the native
    /// protocol sends without its scale.
    fn sum(column: &str) -> String;
    /// `AVG` with its cast.
    fn avg(column: &str) -> String;
}

macro_rules! integer_aggregate {
    ($($t:ty),*) => {$(
        impl Aggregate for $t {
            type Sum = Option<i64>;
            type Avg = Option<Numeric>;
            fn sum(column: &str) -> String {
                format!("CAST(SUM({column}) AS BIGINT)")
            }
            fn avg(column: &str) -> String {
                format!("AVG({column})::text")
            }
        }
    )*};
}
integer_aggregate!(i16, i32, i64);

macro_rules! float_aggregate {
    ($($t:ty),*) => {$(
        impl Aggregate for $t {
            type Sum = Option<f64>;
            type Avg = Option<f64>;
            fn sum(column: &str) -> String {
                format!("CAST(SUM({column}) AS DOUBLE PRECISION)")
            }
            fn avg(column: &str) -> String {
                format!("CAST(AVG({column}) AS DOUBLE PRECISION)")
            }
        }
    )*};
}
float_aggregate!(f32, f64);

impl Aggregate for Numeric {
    type Sum = Option<Numeric>;
    type Avg = Option<Numeric>;
    fn sum(column: &str) -> String {
        format!("SUM({column})::text")
    }
    fn avg(column: &str) -> String {
        format!("AVG({column})::text")
    }
}

impl<T: Aggregate> Aggregate for Option<T> {
    type Sum = T::Sum;
    type Avg = T::Avg;
    fn sum(column: &str) -> String {
        T::sum(column)
    }
    fn avg(column: &str) -> String {
        T::avg(column)
    }
}

/// A column type `MIN` and `MAX` read back as `Option<Self::Value>` (NULL
/// over no rows).
pub trait Ordered {
    /// The column's type without its `Option`.
    type Value: FromSqlOwned;
}

macro_rules! ordered {
    ($($t:ty),*) => {$(
        impl Ordered for $t {
            type Value = $t;
        }
    )*};
}
ordered!(
    i16,
    i32,
    i64,
    f32,
    f64,
    bool,
    String,
    Numeric,
    crate::Uuid,
    crate::Timestamp,
    crate::Date,
    crate::Time,
    crate::Interval
);

impl<T: Ordered> Ordered for Option<T> {
    type Value = T::Value;
}

impl<M: Model, T> Column<M, T> {
    fn aggregate<R>(&self, f: impl FnOnce(&str) -> String) -> Agg<M, R> {
        let column = ident(self.name()).map_err(|e| e.to_string());
        match column {
            Ok(c) => {
                let bare = f(&c);
                Agg::new(Ok(bare), str::to_string)
            }
            Err(e) => Agg::new(Err(e), str::to_string),
        }
    }

    /// `COUNT(column)`: the non-NULL values.
    pub fn count(&self) -> Agg<M, i64> {
        self.aggregate(|c| format!("COUNT({c})"))
    }

    /// `COUNT(DISTINCT column)`: the distinct non-NULL values.
    pub fn count_distinct(&self) -> Agg<M, i64> {
        self.aggregate(|c| format!("COUNT(DISTINCT {c})"))
    }

    /// `SUM(column)`, NULL (`None`) over no rows. An integer column's sum is
    /// read as `i64` through `CAST(SUM(x) AS BIGINT)`; a sum past `i64` is
    /// the server's error, not a wrapped value.
    pub fn sum(&self) -> Agg<M, T::Sum>
    where
        T: Aggregate,
    {
        let bare = ident(self.name())
            .map(|c| format!("SUM({c})"))
            .map_err(|e| e.to_string());
        let read = ident(self.name())
            .map(|c| T::sum(&c))
            .map_err(|e| e.to_string());
        Agg {
            read,
            bare,
            _types: PhantomData,
        }
    }

    /// `AVG(column)`, NULL (`None`) over no rows: a [`Numeric`] for an
    /// integer or decimal column (read as text, exactly), `f64` for a float.
    pub fn avg(&self) -> Agg<M, T::Avg>
    where
        T: Aggregate,
    {
        let bare = ident(self.name())
            .map(|c| format!("AVG({c})"))
            .map_err(|e| e.to_string());
        let read = ident(self.name())
            .map(|c| T::avg(&c))
            .map_err(|e| e.to_string());
        Agg {
            read,
            bare,
            _types: PhantomData,
        }
    }

    fn extreme(&self, f: &str) -> Agg<M, Option<T::Value>>
    where
        T: Ordered,
    {
        let text = reads_as_text::<M>(self.name());
        let bare = ident(self.name())
            .map(|c| format!("{f}({c})"))
            .map_err(|e| e.to_string());
        Agg::new(bare, |b| {
            if text {
                format!("{b}::text")
            } else {
                b.to_string()
            }
        })
    }

    /// `MIN(column)`, `None` over no rows.
    pub fn min(&self) -> Agg<M, Option<T::Value>>
    where
        T: Ordered,
    {
        self.extreme("MIN")
    }

    /// `MAX(column)`, `None` over no rows.
    pub fn max(&self) -> Agg<M, Option<T::Value>>
    where
        T: Ordered,
    {
        self.extreme("MAX")
    }
}

// ----------------------------------------------------------- Projection

/// What [`Select::select`] reads: a [`Column`], an [`Agg`], or a tuple of
/// them (up to 10), read as the matching value or tuple.
pub trait Projection<M> {
    /// What one row is read as.
    type Output;
    /// Appends the select list.
    fn push(&self, out: &mut Vec<std::result::Result<String, String>>);
    /// Reads one row, from column `at` on; returns the next column.
    fn read(row: &Row<'_>, at: usize) -> Result<(Self::Output, usize)>;
}

impl<M: Model, T: FromSqlOwned> Projection<M> for Column<M, T> {
    type Output = T;
    fn push(&self, out: &mut Vec<std::result::Result<String, String>>) {
        out.push(column_sql::<M>(self.name()));
    }
    fn read(row: &Row<'_>, at: usize) -> Result<(T, usize)> {
        Ok((row.get(at)?, at + 1))
    }
}

impl<M, T: FromSqlOwned> Projection<M> for Agg<M, T> {
    type Output = T;
    fn push(&self, out: &mut Vec<std::result::Result<String, String>>) {
        out.push(self.read.clone());
    }
    fn read(row: &Row<'_>, at: usize) -> Result<(T, usize)> {
        Ok((row.get(at)?, at + 1))
    }
}

macro_rules! tuple_projection {
    ($($p:ident $i:tt),+) => {
        impl<M, $($p: Projection<M>),+> Projection<M> for ($($p,)+) {
            type Output = ($($p::Output,)+);
            fn push(&self, out: &mut Vec<std::result::Result<String, String>>) {
                $(self.$i.push(out);)+
            }
            #[allow(unused_assignments)]
            fn read(row: &Row<'_>, at: usize) -> Result<(Self::Output, usize)> {
                let mut at = at;
                let out = ($({
                    let (value, next) = $p::read(row, at)?;
                    at = next;
                    value
                },)+);
                Ok((out, at))
            }
        }
    };
}
tuple_projection!(A 0);
tuple_projection!(A 0, B 1);
tuple_projection!(A 0, B 1, C 2);
tuple_projection!(A 0, B 1, C 2, D 3);
tuple_projection!(A 0, B 1, C 2, D 3, E 4);
tuple_projection!(A 0, B 1, C 2, D 3, E 4, F 5);
tuple_projection!(A 0, B 1, C 2, D 3, E 4, F 5, G 6);
tuple_projection!(A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7);
tuple_projection!(A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8);
tuple_projection!(A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9);

/// Columns of model `M` named together: one [`Column`] or a tuple of them.
/// For [`Projected::group_by`] and the upsert builder's conflict target.
pub trait Columns<M> {
    /// Their names, in order.
    fn names(&self) -> Vec<&'static str>;
}

impl<M, T> Columns<M> for Column<M, T> {
    fn names(&self) -> Vec<&'static str> {
        vec![self.name()]
    }
}

macro_rules! tuple_columns {
    ($($t:ident $i:tt),+) => {
        impl<M, $($t),+> Columns<M> for ($(Column<M, $t>,)+) {
            fn names(&self) -> Vec<&'static str> {
                vec![$(self.$i.name()),+]
            }
        }
    };
}
tuple_columns!(A 0);
tuple_columns!(A 0, B 1);
tuple_columns!(A 0, B 1, C 2);
tuple_columns!(A 0, B 1, C 2, D 3);
tuple_columns!(A 0, B 1, C 2, D 3, E 4);
tuple_columns!(A 0, B 1, C 2, D 3, E 4, F 5);
tuple_columns!(A 0, B 1, C 2, D 3, E 4, F 5, G 6);
tuple_columns!(A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7);

// ------------------------------------------------------------ Projected

/// A `SELECT` of chosen columns and aggregates: from [`Select::select`].
pub struct Projected<M, P> {
    select: Select<M>,
    items: P,
    group_by: Vec<&'static str>,
    having: Option<Expr>,
    distinct: bool,
}

impl<M, P: Clone> Clone for Projected<M, P> {
    fn clone(&self) -> Self {
        Projected {
            select: self.select.clone(),
            items: self.items.clone(),
            group_by: self.group_by.clone(),
            having: self.having.clone(),
            distinct: self.distinct,
        }
    }
}

impl<M, P> fmt::Debug for Projected<M, P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Projected")
            .field("select", &self.select)
            .field("group_by", &self.group_by)
            .field("having", &self.having)
            .field("distinct", &self.distinct)
            .finish_non_exhaustive()
    }
}

impl<M: Model> Select<M> {
    /// Reads `items` instead of whole models: a column, an aggregate, or a
    /// tuple of them, each row read as the matching value or tuple
    /// (`.select((Post::user_id, Post::views.sum()))` reads
    /// `(i64, Option<i64>)`). The filter, order, limit and offset carry
    /// over; relations do not (they load into models).
    pub fn select<P: Projection<M>>(self, items: P) -> Projected<M, P> {
        Projected {
            select: self,
            items,
            group_by: Vec::new(),
            having: None,
            distinct: false,
        }
    }
}

impl<M: Model, P: Projection<M>> Projected<M, P> {
    /// `GROUP BY` these columns (one, or a tuple).
    pub fn group_by(mut self, columns: impl Columns<M>) -> Self {
        self.group_by.extend(columns.names());
        self
    }

    /// `HAVING`: a condition on the groups, from an [`Agg`]'s comparisons
    /// (`Post::id.count().gt(10)`); several must all hold.
    pub fn having(mut self, expr: Expr) -> Self {
        self.having = Some(match self.having.take() {
            None => expr,
            Some(previous) => previous.and(expr),
        });
        self
    }

    /// `SELECT DISTINCT`: each distinct row once.
    pub fn distinct(mut self) -> Self {
        self.distinct = true;
        self
    }

    /// Sorts by a column or an aggregate (after any order the query had).
    pub fn order_by(mut self, order: Order) -> Self {
        self.select = self.select.order_by(order);
        self
    }

    /// At most `n` rows.
    pub fn limit(mut self, n: u64) -> Self {
        self.select = self.select.limit(n);
        self
    }

    /// The statement [`all`](Self::all) runs.
    pub fn to_sql(&self) -> Result<String> {
        if self.select.has_includes() {
            return Err(Error::Unsupported(
                "a projection reads columns, not models: relations (with, with_count) do not \
                 apply to it"
                    .to_string(),
            ));
        }
        let mut items = Vec::new();
        self.items.push(&mut items);
        let items = items
            .into_iter()
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Error::Param)?;
        let mut sql = String::from("SELECT ");
        if self.distinct {
            sql.push_str("DISTINCT ");
        }
        sql.push_str(&items.join(", "));
        sql.push_str(" FROM ");
        sql.push_str(&ident(M::TABLE)?);
        if let Some(filter) = self.select.where_sql()? {
            sql.push_str(" WHERE ");
            sql.push_str(&filter);
        }
        if !self.group_by.is_empty() {
            sql.push_str(" GROUP BY ");
            let names = self
                .group_by
                .iter()
                .map(|c| ident(c))
                .collect::<Result<Vec<_>>>()?;
            sql.push_str(&names.join(", "));
        }
        if let Some(having) = &self.having {
            sql.push_str(" HAVING ");
            sql.push_str(having.to_sql()?);
        }
        self.select.push_tail(&mut sql)?;
        Ok(sql)
    }

    /// Every row, each read as the projection's output.
    pub async fn all<E: Executor>(&self, db: &E) -> Result<Vec<P::Output>> {
        let rows = db.run(&self.to_sql()?).await?;
        rows.iter().map(|row| read_whole::<M, P>(&row)).collect()
    }

    /// The first row, if any (this adds `LIMIT 1`).
    pub async fn first<E: Executor>(&self, db: &E) -> Result<Option<P::Output>>
    where
        P: Clone,
    {
        let rows = db.run(&self.clone().limit(1).to_sql()?).await?;
        rows.get(0).map(|row| read_whole::<M, P>(&row)).transpose()
    }

    /// Exactly one row: an aggregate without `GROUP BY` returns one.
    pub async fn one<E: Executor>(&self, db: &E) -> Result<P::Output> {
        let rows = db.run(&self.to_sql()?).await?;
        match rows.len() {
            1 => read_whole::<M, P>(&rows.get(0).expect("one row")),
            got => Err(Error::RowCount {
                expected: "exactly 1",
                got,
            }),
        }
    }

    /// Every row, each read as `T` (a [`FromRow`] struct, by position).
    pub async fn all_as<T: FromRow, E: Executor>(&self, db: &E) -> Result<Vec<T>> {
        db.run(&self.to_sql()?).await?.decode_all()
    }
}

fn read_whole<M, P: Projection<M>>(row: &Row<'_>) -> Result<P::Output> {
    let (out, width) = P::read(row, 0)?;
    if width != row.len() {
        return Err(Error::Protocol(format!(
            "the projection reads {width} column(s), the row has {}",
            row.len()
        )));
    }
    Ok(out)
}
