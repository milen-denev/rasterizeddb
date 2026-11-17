//! [`Executor`]: anything statements can be sent on.
//!
//! A [`Client`](crate::Client) is one, and so is a transaction. The ORM, the
//! query builder and the admin calls take any `E: Executor`, so the same
//! code runs on its own (each statement committing by itself) or inside a
//! transaction.

use std::future::Future;

use crate::Result;
use crate::error::Error;
use crate::from_sql::FromSqlOwned;
use crate::params::{Param, check_statement, render, render_dyn};
use crate::row::{FromRow, Rows};
use crate::to_sql::ToSql;

/// Something statements can be sent on: a [`Client`](crate::Client), or a
/// transaction.
///
/// One method is required, [`send_checked`](Self::send_checked); every
/// other is built on it. Every future it returns is `Send`, so a statement
/// can be awaited inside `tokio::spawn`.
pub trait Executor: Sync {
    /// Sends text that is already one statement: checked by
    /// [`check_statement`], or produced by [`render`]. Callers outside this
    /// crate use [`run`](Self::run), which checks first.
    #[doc(hidden)]
    fn send_checked(&self, sql: &str) -> impl Future<Output = Result<Rows>> + Send;

    /// Runs `sql` exactly as given, with no placeholder rendering. Text
    /// holding a second statement is refused with [`Error::Unsupported`]
    /// before anything is sent.
    fn run(&self, sql: &str) -> impl Future<Output = Result<Rows>> + Send {
        async move {
            check_statement(sql)?;
            self.send_checked(sql).await
        }
    }

    /// Runs `sql` with `$1`…`$n` replaced by `params` (see [`render`]).
    fn query(&self, sql: &str, params: &[Param<'_>]) -> impl Future<Output = Result<Rows>> + Send {
        async move {
            let statement = render(sql, params)?;
            self.send_checked(&statement).await
        }
    }

    /// [`query`](Self::query) with values given as [`ToSql`].
    fn query_with(
        &self,
        sql: &str,
        params: &[&dyn ToSql],
    ) -> impl Future<Output = Result<Rows>> + Send {
        async move {
            let statement = render_dyn(sql, params)?;
            self.send_checked(&statement).await
        }
    }

    /// How many rows `sql` affected: the count a write reported, or the
    /// number of rows it returned.
    fn execute(&self, sql: &str, params: &[Param<'_>]) -> impl Future<Output = Result<u64>> + Send {
        async move {
            let rows = self.query(sql, params).await?;
            Ok(rows.rows_affected().unwrap_or(rows.len() as u64))
        }
    }

    /// [`execute`](Self::execute) with values given as [`ToSql`].
    fn exec(&self, sql: &str, params: &[&dyn ToSql]) -> impl Future<Output = Result<u64>> + Send {
        async move {
            let rows = self.query_with(sql, params).await?;
            Ok(rows.rows_affected().unwrap_or(rows.len() as u64))
        }
    }

    /// Every row, each as `T`.
    fn fetch_all<T: FromRow>(
        &self,
        sql: &str,
        params: &[&dyn ToSql],
    ) -> impl Future<Output = Result<Vec<T>>> + Send {
        async move { self.query_with(sql, params).await?.decode_all() }
    }

    /// Exactly one row; none, or more than one, is [`Error::RowCount`].
    fn fetch_one<T: FromRow>(
        &self,
        sql: &str,
        params: &[&dyn ToSql],
    ) -> impl Future<Output = Result<T>> + Send {
        async move {
            let rows = self.query_with(sql, params).await?;
            match rows.len() {
                1 => rows.decode_first().map(|row| row.expect("one row")),
                got => Err(Error::RowCount {
                    expected: "exactly 1",
                    got,
                }),
            }
        }
    }

    /// At most one row; more than one is [`Error::RowCount`].
    fn fetch_optional<T: FromRow>(
        &self,
        sql: &str,
        params: &[&dyn ToSql],
    ) -> impl Future<Output = Result<Option<T>>> + Send {
        async move {
            let rows = self.query_with(sql, params).await?;
            match rows.len() {
                0 | 1 => rows.decode_first(),
                got => Err(Error::RowCount {
                    expected: "at most 1",
                    got,
                }),
            }
        }
    }

    /// The first column of exactly one row.
    fn fetch_scalar<T: FromSqlOwned>(
        &self,
        sql: &str,
        params: &[&dyn ToSql],
    ) -> impl Future<Output = Result<T>> + Send {
        async move {
            let rows = self.query_with(sql, params).await?;
            match rows.len() {
                1 => rows.get(0).expect("one row").get(0),
                got => Err(Error::RowCount {
                    expected: "exactly 1",
                    got,
                }),
            }
        }
    }
}

impl Executor for crate::client::Client {
    fn send_checked(&self, sql: &str) -> impl Future<Output = Result<Rows>> + Send {
        self.send(sql)
    }
}

impl<E: Executor> Executor for &E {
    fn send_checked(&self, sql: &str) -> impl Future<Output = Result<Rows>> + Send {
        (**self).send_checked(sql)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_send<T: Send>(_: T) {}

    /// The futures stay `Send` for a generic executor, so ORM code written
    /// against `E: Executor` can be spawned.
    #[allow(dead_code)]
    fn futures_are_send<E: Executor>(e: &E) {
        assert_send(e.run("SELECT 1"));
        assert_send(e.query_with("SELECT $1", &[&1i64]));
        assert_send(e.fetch_all::<(i64,)>("SELECT 1", &[]));
        assert_send(e.fetch_scalar::<i64>("SELECT 1", &[]));
        assert_send(e.exec("DELETE FROM t", &[]));
    }
}
