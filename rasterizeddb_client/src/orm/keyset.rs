//! Keyset pagination and streaming: pages that start after (or before) a
//! row's sort key instead of at an offset, and every row of a big table in
//! batches that each fit an answer.

use std::collections::VecDeque;
use std::fmt;
use std::pin::Pin;
use std::task::{Context, Poll};

use super::query::{Order, Select, SendSql};
use super::{BoxFuture, Model, ident};
use crate::executor::Executor;
use crate::params::{push_ident, push_param};
use crate::row::Row;
use crate::to_sql::ToSql;
use crate::types::{Date, Numeric, Time, Timestamp, Uuid};
use crate::value::Value;
use crate::{Error, Result};

/// Where a keyset page starts: the sort key of a row (its `ORDER BY` columns,
/// then its primary key), from [`Page::next_cursor`] / [`Page::prev_cursor`].
///
/// [`encode`](Self::encode) makes it an opaque string for a URL, and
/// [`decode`](Self::decode) reads one back. A decoded cursor holds typed
/// values, which are written into the next statement as literals under the
/// usual escaping, so a cursor string from a client cannot carry SQL.
#[derive(Clone, Debug, PartialEq)]
pub struct Cursor {
    values: Vec<Value>,
}

impl Cursor {
    /// A cursor of these sort-key values, in key order (the `ORDER BY`
    /// columns, then the primary key's).
    pub fn new(values: Vec<Value>) -> Self {
        Cursor { values }
    }

    /// The values, in key order.
    pub fn values(&self) -> &[Value] {
        &self.values
    }

    /// The cursor as a URL-safe string: each value's kind and text form,
    /// hex-encoded.
    pub fn encode(&self) -> Result<String> {
        let mut out = String::new();
        for (i, value) in self.values.iter().enumerate() {
            if i > 0 {
                out.push('.');
            }
            let (kind, text) = match value {
                Value::Null => ("n", String::new()),
                Value::Bool(b) => ("b", b.to_string()),
                Value::I16(v) => ("h", v.to_string()),
                Value::I32(v) => ("i", v.to_string()),
                Value::I64(v) => ("l", v.to_string()),
                Value::U64(v) => ("u", v.to_string()),
                Value::F64(v) => ("d", format!("{v:?}")),
                Value::F32(v) => ("f", format!("{v:?}")),
                Value::Numeric(n) => ("m", n.to_string()),
                Value::Text(s) => ("t", s.clone()),
                Value::Uuid(u) => ("g", u.to_string()),
                Value::Timestamp(t) => ("s", t.micros.to_string()),
                Value::Date(d) => ("a", d.to_string()),
                Value::Time(t) => ("c", t.to_string()),
                other => {
                    return Err(Error::Unsupported(format!(
                        "a cursor cannot carry {other:?}: sort a keyset by integers, text, \
                         decimals, UUIDs, dates or times"
                    )));
                }
            };
            out.push_str(kind);
            for b in text.bytes() {
                out.push_str(&format!("{b:02x}"));
            }
        }
        Ok(out)
    }

    /// Reads a string [`encode`](Self::encode) wrote. Anything else is
    /// [`Error::Param`].
    pub fn decode(text: &str) -> Result<Cursor> {
        let bad = || Error::Param(format!("{text:?} is not a cursor"));
        let mut values = Vec::new();
        if text.is_empty() {
            return Ok(Cursor { values });
        }
        for part in text.split('.') {
            let mut chars = part.chars();
            let kind = chars.next().ok_or_else(bad)?;
            let hex = chars.as_str();
            if hex.len() % 2 != 0 {
                return Err(bad());
            }
            let bytes = (0..hex.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).map_err(|_| bad()))
                .collect::<Result<Vec<u8>>>()?;
            let s = String::from_utf8(bytes).map_err(|_| bad())?;
            values.push(match kind {
                'n' => Value::Null,
                'b' => Value::Bool(s.parse().map_err(|_| bad())?),
                'h' => Value::I16(s.parse().map_err(|_| bad())?),
                'i' => Value::I32(s.parse().map_err(|_| bad())?),
                'l' => Value::I64(s.parse().map_err(|_| bad())?),
                'u' => Value::U64(s.parse().map_err(|_| bad())?),
                'd' => Value::F64(s.parse().map_err(|_| bad())?),
                'f' => Value::F32(s.parse().map_err(|_| bad())?),
                'm' => Value::Numeric(Numeric::new(&s).map_err(|_| bad())?),
                't' => Value::Text(s),
                'g' => Value::Uuid(s.parse::<Uuid>().map_err(|_| bad())?),
                's' => Value::Timestamp(Timestamp {
                    micros: s.parse().map_err(|_| bad())?,
                }),
                'a' => Value::Date(s.parse::<Date>().map_err(|_| bad())?),
                'c' => Value::Time(s.parse::<Time>().map_err(|_| bad())?),
                _ => return Err(bad()),
            });
        }
        Ok(Cursor { values })
    }
}

/// One keyset page: from [`Select::page`].
#[derive(Debug, Clone)]
pub struct Page<M> {
    /// The rows, in the query's order.
    pub items: Vec<M>,
    /// Where the next page starts ([`Select::after`]); `None` on the last.
    pub next_cursor: Option<Cursor>,
    /// Where the page before ends ([`Select::before`]); `None` on the
    /// first.
    pub prev_cursor: Option<Cursor>,
}

/// Which way a keyset query reads from its cursor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Direction {
    After,
    Before,
}

/// One sort key of a keyset: a column, its position in the select list,
/// and whether it sorts descending.
pub(crate) struct Key {
    name: &'static str,
    position: usize,
    descending: bool,
    numeric_text: bool,
}

/// The sort keys of a keyset over `order`: its columns, then the primary
/// key's columns not already among them (in the last column's direction),
/// so that every row has a distinct key.
pub(crate) fn keys<M: Model>(order: &[Order]) -> Result<Vec<Key>> {
    let mut out: Vec<Key> = Vec::new();
    let key_of = |name: &'static str, descending: bool| -> Result<Key> {
        let position = M::COLUMNS
            .iter()
            .position(|c| c.name == name)
            .ok_or_else(|| {
                Error::Unsupported(format!(
                    "a keyset sorts by {}'s columns; {name} is not one",
                    M::TABLE
                ))
            })?;
        let column = &M::COLUMNS[position];
        if column.nullable {
            return Err(Error::Unsupported(format!(
                "a keyset cannot sort by {}.{name}: it is nullable, and a NULL compares as \
                 unknown, so rows would be skipped. Sort by NOT NULL columns",
                M::TABLE
            )));
        }
        let numeric_text =
            column.read_as_text && column.sql_type.to_ascii_uppercase().starts_with("NUMERIC");
        if column.read_as_text && !numeric_text {
            return Err(Error::Unsupported(format!(
                "a keyset cannot sort by {}.{name} ({}): its values arrive as text",
                M::TABLE,
                column.sql_type
            )));
        }
        Ok(Key {
            name,
            position,
            descending,
            numeric_text,
        })
    };
    for order in order {
        let name = order.column().ok_or_else(|| {
            Error::Unsupported("a keyset sorts by columns, not by expressions".to_string())
        })?;
        if out.iter().any(|k| k.name == name) {
            continue;
        }
        out.push(key_of(name, order.descending())?);
    }
    let pk: Vec<&'static str> = M::COLUMNS
        .iter()
        .filter(|c| c.primary_key)
        .map(|c| c.name)
        .collect();
    if pk.is_empty() {
        return Err(Error::Unsupported(format!(
            "a keyset needs a primary key to break ties, and {} has none",
            M::TABLE
        )));
    }
    // The tiebreaker sorts the way the last key does, so a one-direction
    // order stays one direction (and a row-value comparison).
    let tie = out.last().is_some_and(|k| k.descending);
    for name in pk {
        if !out.iter().any(|k| k.name == name) {
            out.push(key_of(name, tie)?);
        }
    }
    Ok(out)
}

/// `ORDER BY` over the keys, reversed for a page read backwards.
pub(crate) fn order_sql(keys: &[Key], reversed: bool) -> Result<String> {
    let mut sql = String::new();
    for (i, key) in keys.iter().enumerate() {
        if i > 0 {
            sql.push_str(", ");
        }
        push_ident(&mut sql, key.name)?;
        sql.push_str(if key.descending != reversed {
            " DESC"
        } else {
            " ASC"
        });
    }
    Ok(sql)
}

/// The predicate for rows past `cursor` in `direction`: a row-value
/// comparison `(a, b) > (x, y)` when every key sorts the same way, an
/// OR-chain `a > x OR (a = x AND b < y) …` when directions differ.
pub(crate) fn predicate(keys: &[Key], cursor: &Cursor, direction: Direction) -> Result<String> {
    if cursor.values.len() != keys.len() {
        return Err(Error::Param(format!(
            "the cursor has {} value(s), the keyset {} ({}): it was made for another order",
            cursor.values.len(),
            keys.len(),
            keys.iter().map(|k| k.name).collect::<Vec<_>>().join(", ")
        )));
    }
    let mut literals = Vec::with_capacity(keys.len());
    for (key, value) in keys.iter().zip(&cursor.values) {
        if value.is_null() {
            return Err(Error::Param(format!("the cursor's {} is NULL", key.name)));
        }
        let mut out = String::new();
        push_param(&mut out, &value.to_param())?;
        literals.push(out);
    }
    // `>` for a key read in its own direction ascending, `<` descending.
    let op = |key: &Key| {
        let forward = (direction == Direction::After) != key.descending;
        if forward { " > " } else { " < " }
    };
    let names = keys
        .iter()
        .map(|k| ident(k.name))
        .collect::<Result<Vec<_>>>()?;
    if keys.len() == 1 {
        return Ok(format!("{}{}{}", names[0], op(&keys[0]), literals[0]));
    }
    if keys.iter().all(|k| k.descending == keys[0].descending) {
        return Ok(format!(
            "({}){}({})",
            names.join(", "),
            op(&keys[0]),
            literals.join(", ")
        ));
    }
    let mut terms = Vec::with_capacity(keys.len());
    for i in 0..keys.len() {
        let mut term = String::from("(");
        for j in 0..i {
            term.push_str(&format!("{} = {} AND ", names[j], literals[j]));
        }
        term.push_str(&format!("{}{}{}", names[i], op(&keys[i]), literals[i]));
        term.push(')');
        terms.push(term);
    }
    Ok(format!("({})", terms.join(" OR ")))
}

/// The cursor of a row of the answer, read from its key columns.
pub(crate) fn cursor_of(keys: &[Key], row: &Row<'_>) -> Result<Cursor> {
    let mut values = Vec::with_capacity(keys.len());
    for key in keys {
        let value: Value = row.get(key.position)?;
        values.push(match value {
            Value::Text(text) if key.numeric_text => Value::Numeric(Numeric::new(&text)?),
            other => other,
        });
    }
    Ok(Cursor { values })
}

impl<M: Model> Select<M> {
    /// The page of up to `limit` rows (set by [`limit`](Select::limit)) in
    /// the query's order, starting after [`after`](Select::after)'s cursor
    /// or ending before [`before`](Select::before)'s; the first page with
    /// neither. The order is the query's `ORDER BY` columns (none: the
    /// primary key), then the primary key to break ties. One statement,
    /// reading one row past the page to know whether another follows.
    pub async fn page<E: Executor>(&self, db: &E) -> Result<Page<M>> {
        self.page_dyn(db).await
    }

    pub(crate) async fn page_dyn(&self, db: &dyn SendSql) -> Result<Page<M>> {
        let size = self.page_size()?;
        let keys = keys::<M>(self.order_list())?;
        let backwards = self.keyset_direction() == Some(Direction::Before);
        let sql = self.keyset_sql(&keys, Some(size + 1))?;
        let rows = db.send_sql(&sql).await?;
        let more = rows.len() as u64 > size;
        let take = rows.len().min(size as usize);
        // Rows read backwards arrive last-first; the page is the `take`
        // nearest the cursor, put back in order.
        let mut order: Vec<usize> = (0..take).collect();
        if backwards {
            order.reverse();
        }
        let cursor_at =
            |i: usize| -> Result<Cursor> { cursor_of(&keys, &rows.get(i).expect("in range")) };
        let (next_cursor, prev_cursor) = if take == 0 {
            (None, None)
        } else {
            let (first, last) = (order[0], order[take - 1]);
            if backwards {
                (
                    Some(cursor_at(last)?),
                    more.then(|| cursor_at(first)).transpose()?,
                )
            } else {
                (
                    more.then(|| cursor_at(last)).transpose()?,
                    self.keyset_direction()
                        .is_some()
                        .then(|| cursor_at(first))
                        .transpose()?,
                )
            }
        };
        let items = self.finish_rows(db, &rows, &order).await?;
        Ok(Page {
            items,
            next_cursor,
            prev_cursor,
        })
    }

    /// Every matching row in batches of `batch_size`, each batch one keyset
    /// statement after the last row of the one before: a table of any size
    /// is read without an answer past the message limit, and without
    /// `OFFSET`'s cost of skipping rows. Rows inserted or changed while it
    /// runs are seen or not depending on where they sort (each batch is its
    /// own statement, unless it runs in a transaction).
    ///
    /// A [`limit`](Select::limit) caps the total. Pull rows with
    /// [`RowStream::next`], or use it as a `futures_core::Stream`.
    pub fn stream<'a, E: Executor>(&self, db: &'a E, batch_size: u64) -> RowStream<'a, M>
    where
        M: Send + 'a,
    {
        RowStream {
            select: self.clone(),
            db,
            batch: batch_size.max(1),
            remaining: self.limit_value(),
            buffer: VecDeque::new(),
            cursor: None,
            done: false,
            pending: None,
        }
    }
}

/// The rows of a [`Select::stream`], fetched a batch at a time.
pub struct RowStream<'a, M> {
    select: Select<M>,
    db: &'a dyn SendSql,
    batch: u64,
    remaining: Option<u64>,
    buffer: VecDeque<M>,
    cursor: Option<Cursor>,
    done: bool,
    pending: Option<BoxFuture<'a, Result<(Vec<M>, Option<Cursor>)>>>,
}

impl<M> fmt::Debug for RowStream<'_, M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RowStream")
            .field("batch", &self.batch)
            .field("remaining", &self.remaining)
            .field("buffered", &self.buffer.len())
            .field("done", &self.done)
            .finish_non_exhaustive()
    }
}

impl<'a, M: Model + Send + 'a> RowStream<'a, M> {
    fn start_batch(&mut self) {
        let size = match self.remaining {
            Some(left) => left.min(self.batch),
            None => self.batch,
        };
        let mut select = self.select.clone().limit(size);
        if let Some(cursor) = &self.cursor {
            select = select.after(cursor);
        }
        let db = self.db;
        self.pending = Some(Box::pin(async move {
            if select.keyset_direction() == Some(Direction::Before) {
                return Err(Error::Unsupported(
                    "a stream reads forward: start it with after(), not before()".to_string(),
                ));
            }
            let page = select.page_dyn(db).await?;
            Ok((page.items, page.next_cursor))
        }));
    }

    /// The next row, or `None` after the last. A failed batch ends the
    /// stream after its error.
    pub async fn next(&mut self) -> Option<Result<M>> {
        std::future::poll_fn(|cx| self.poll_row(cx)).await
    }

    /// The rows of the next batch (at most `batch_size`), or `None` after
    /// the last.
    pub async fn next_batch(&mut self) -> Option<Result<Vec<M>>> {
        if self.buffer.is_empty() {
            match std::future::poll_fn(|cx| self.poll_row(cx)).await? {
                Ok(row) => self.buffer.push_front(row),
                Err(e) => return Some(Err(e)),
            }
        }
        Some(Ok(self.buffer.drain(..).collect()))
    }

    fn poll_row(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<M>>> {
        loop {
            if let Some(row) = self.buffer.pop_front() {
                return Poll::Ready(Some(Ok(row)));
            }
            if self.done || self.remaining == Some(0) {
                return Poll::Ready(None);
            }
            if self.pending.is_none() {
                self.start_batch();
            }
            let pending = self.pending.as_mut().expect("started");
            match pending.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(result) => {
                    self.pending = None;
                    match result {
                        Err(e) => {
                            self.done = true;
                            return Poll::Ready(Some(Err(e)));
                        }
                        Ok((rows, next)) => {
                            if let Some(left) = &mut self.remaining {
                                *left = left.saturating_sub(rows.len() as u64);
                            }
                            match next {
                                Some(cursor) => self.cursor = Some(cursor),
                                None => self.done = true,
                            }
                            self.buffer.extend(rows);
                        }
                    }
                }
            }
        }
    }
}

impl<'a, M: Model + Send + 'a> futures_core::Stream for RowStream<'a, M> {
    type Item = Result<M>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().poll_row(cx)
    }
}

impl<M> Unpin for RowStream<'_, M> {}
