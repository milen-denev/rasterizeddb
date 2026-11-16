use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use rastcp::client::TcpClientBuilder;
use rastcp::mux::MuxClient;

use crate::from_sql::FromSqlOwned;
use crate::login::Login;
use crate::params::{Param, check_statement, render, render_dyn};
use crate::row::{FromRow, Rows};
use crate::to_sql::ToSql;
use crate::{Error, Result};

/// How a [`Client`] connects.
#[derive(Debug, Clone)]
pub struct Options {
    /// Connections opened and shared by every caller (at least one). Each one
    /// carries many statements at once, so a few serve many tasks: one
    /// measured as fast as several for a point-query load.
    pub connections: usize,
    /// How long a statement may wait for its answer, and a connection for its
    /// handshake. `None` waits indefinitely.
    pub timeout: Option<Duration>,
    /// Who the connections log in as.
    pub login: Login,
    /// The largest answer accepted, in bytes (rastcp's default, 8 MiB). The
    /// limit is this side's: the server sends an answer of any size. One past
    /// it fails whole with [`Error::Transport`], never arriving cut short, and
    /// costs the connection, which the next statement reopens. Raise it for a
    /// big read, or page the read (`ORDER BY … LIMIT`); the whole answer is
    /// held in memory either way.
    pub max_message_size: usize,
    /// Migrations [`connect`](Client::connect) handles as
    /// [`auto_migrate`](Self::auto_migrate) says. `None` (the default):
    /// nothing is migrated.
    pub migrations: Option<crate::orm::Migrator>,
    /// The knob for [`migrations`](Self::migrations): apply them when the
    /// client connects (the default), only verify that nothing is pending, or
    /// leave them to [`Client::migrate`].
    pub auto_migrate: crate::orm::AutoMigrate,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            connections: 1,
            timeout: Some(Duration::from_secs(30)),
            login: Login::Trust,
            max_message_size: 8 * 1024 * 1024,
            migrations: None,
            auto_migrate: crate::orm::AutoMigrate::Apply,
        }
    }
}

/// A connection to a RasterizedDB server's native protocol, shared by any
/// number of tasks.
///
/// Statements sent together by different tasks run side by side on the
/// server, so two that are in flight at once may take effect in either order;
/// await one before sending the other when the order matters. Nothing is ever
/// sent twice: a statement whose connection fails returns
/// [`Error::Transport`], and whether it ran is unknown.
pub struct Client {
    connections: Vec<MuxClient>,
    next: AtomicUsize,
}

impl Client {
    /// Connects to `addr`: `unix:/path/to/socket` for a unix socket (the
    /// server's `--rdb-socket`), or `host:port` for TCP.
    ///
    /// Every connection is opened and logged in before this returns, so a
    /// wrong path or a refused login is this call's error. So is a migration
    /// that fails, when [`Options::migrations`] is set and
    /// [`Options::auto_migrate`] applies or verifies it.
    ///
    /// Over TCP the transport is TLS, and the server's certificate is not
    /// verified, as with `rasterizeddb_core`'s own client: use a unix socket,
    /// or a network you trust, until certificate options are added here.
    pub async fn connect(addr: &str, options: Options) -> Result<Self> {
        let authenticator = options.login.authenticator()?;
        let mut connections = Vec::with_capacity(options.connections.max(1));
        for _ in 0..options.connections.max(1) {
            let mut builder = TcpClientBuilder::new(addr)
                .verify_certificate(false)
                .max_message_size(options.max_message_size);
            builder = match options.timeout {
                Some(timeout) => builder.timeout(timeout),
                None => builder.no_timeout(),
            };
            if let Some(authenticator) = &authenticator {
                builder = builder.authenticator(authenticator.clone());
            }
            let client = builder.build_multiplexed().await?;
            client.connect().await?;
            connections.push(client);
        }
        let client = Client {
            connections,
            next: AtomicUsize::new(0),
        };
        if let Some(migrator) = &options.migrations {
            use crate::orm::AutoMigrate;
            match options.auto_migrate {
                AutoMigrate::Off => {}
                AutoMigrate::Apply => {
                    migrator.apply(&client).await?;
                }
                AutoMigrate::Verify => {
                    migrator.verify(&client).await?;
                }
            }
        }
        Ok(client)
    }

    /// A client with no connection, for tests that build statements only.
    #[cfg(all(test, feature = "derive"))]
    pub(crate) fn detached() -> Self {
        Client {
            connections: Vec::new(),
            next: AtomicUsize::new(0),
        }
    }

    fn pick(&self) -> &MuxClient {
        let turn = self.next.fetch_add(1, Ordering::Relaxed);
        &self.connections[turn % self.connections.len()]
    }

    /// Runs `sql` exactly as given, with no placeholder rendering.
    ///
    /// Text holding a second statement, or transaction control, is refused
    /// with [`Error::Unsupported`] before anything is sent (see
    /// [`check_statement`](crate::check_statement)).
    pub async fn run(&self, sql: &str) -> Result<Rows> {
        check_statement(sql)?;
        self.send(sql).await
    }

    /// Sends text already checked (by [`check_statement`], or by rendering).
    async fn send(&self, sql: &str) -> Result<Rows> {
        let response = self.pick().send(sql.as_bytes()).await?;
        decode_response(response)
    }

    /// Runs `sql` with `$1`…`$n` replaced by `params` (see [`render`]) and
    /// returns its rows. A statement that returns none answers with an empty
    /// [`Rows`] whose [`Rows::rows_affected`] says what it reported.
    pub async fn query(&self, sql: &str, params: &[Param<'_>]) -> Result<Rows> {
        let statement = render(sql, params)?;
        self.send(&statement).await
    }

    /// [`query`](Self::query) with values given as [`ToSql`], as
    /// [`params!`](crate::params) builds them.
    pub async fn query_with(&self, sql: &str, params: &[&dyn ToSql]) -> Result<Rows> {
        let statement = render_dyn(sql, params)?;
        self.send(&statement).await
    }

    /// [`execute`](Self::execute) with values given as [`ToSql`].
    pub async fn exec(&self, sql: &str, params: &[&dyn ToSql]) -> Result<u64> {
        let rows = self.query_with(sql, params).await?;
        Ok(rows.rows_affected().unwrap_or(rows.len() as u64))
    }

    /// Every row, each as `T`: a tuple, a [`Vec<Value>`](crate::Value), or a
    /// struct deriving [`FromRow`](derive@crate::FromRow).
    ///
    /// ```no_run
    /// # async fn demo(client: &rasterizeddb_client::Client) -> rasterizeddb_client::Result<()> {
    /// use rasterizeddb_client::params;
    /// let rows: Vec<(i64, String)> = client
    ///     .fetch_all("SELECT id, name FROM users WHERE age > $1", params![30])
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub async fn fetch_all<T: FromRow>(&self, sql: &str, params: &[&dyn ToSql]) -> Result<Vec<T>> {
        self.query_with(sql, params).await?.decode_all()
    }

    /// Exactly one row; none, or more than one, is [`Error::RowCount`].
    pub async fn fetch_one<T: FromRow>(&self, sql: &str, params: &[&dyn ToSql]) -> Result<T> {
        let rows = self.query_with(sql, params).await?;
        match rows.len() {
            1 => rows.decode_first().map(|row| row.expect("one row")),
            got => Err(Error::RowCount {
                expected: "exactly 1",
                got,
            }),
        }
    }

    /// At most one row; more than one is [`Error::RowCount`].
    pub async fn fetch_optional<T: FromRow>(
        &self,
        sql: &str,
        params: &[&dyn ToSql],
    ) -> Result<Option<T>> {
        let rows = self.query_with(sql, params).await?;
        match rows.len() {
            0 | 1 => rows.decode_first(),
            got => Err(Error::RowCount {
                expected: "at most 1",
                got,
            }),
        }
    }

    /// The first column of exactly one row: `SELECT count(*) …`,
    /// `… RETURNING id`.
    pub async fn fetch_scalar<T: FromSqlOwned>(
        &self,
        sql: &str,
        params: &[&dyn ToSql],
    ) -> Result<T> {
        let rows = self.query_with(sql, params).await?;
        match rows.len() {
            1 => rows.get(0).expect("one row").get(0),
            got => Err(Error::RowCount {
                expected: "exactly 1",
                got,
            }),
        }
    }

    /// Whether a table of this name exists.
    pub async fn table_exists(&self, name: &str) -> Result<bool> {
        let rows = self
            .query_with(
                "SELECT tablename FROM pg_tables WHERE tablename = $1",
                &[&name],
            )
            .await?;
        Ok(!rows.is_empty())
    }

    /// The server's administrative statements as typed calls: stores, table
    /// placement, indexes, partitions, roles, backups, configuration.
    pub fn admin(&self) -> crate::admin::Admin<'_> {
        crate::admin::Admin::new(self)
    }

    /// Runs `sql` with `params` and returns how many rows it affected: the
    /// count a write reported, or the number of rows a statement returned
    /// (`DELETE … RETURNING`, a `SELECT`).
    pub async fn execute(&self, sql: &str, params: &[Param<'_>]) -> Result<u64> {
        let rows = self.query(sql, params).await?;
        Ok(rows.rows_affected().unwrap_or(rows.len() as u64))
    }

    /// Whether every connection is open.
    pub fn is_connected(&self) -> bool {
        self.connections.iter().all(MuxClient::is_connected)
    }

    /// Closes every connection.
    pub async fn close(&self) {
        for connection in &self.connections {
            connection.close().await;
        }
    }
}

/// The server's answer: a tag byte, then what the tag says follows
/// (`process_incoming_queries` / `run_native_query` in the server).
///
/// 0 is an acknowledgement, optionally followed by a `u64` count; 1 a `u64`
/// count; 2 serialized rows; 3 an error message.
pub(crate) fn decode_response(response: Vec<u8>) -> Result<Rows> {
    let count = |bytes: &[u8]| -> Option<u64> {
        bytes
            .get(1..9)
            .map(|b| u64::from_le_bytes(b.try_into().unwrap()))
    };
    match response.first() {
        None => Err(Error::Protocol(
            "the server sent an empty answer".to_string(),
        )),
        Some(0) => Ok(Rows::affected(count(&response))),
        Some(1) => match count(&response) {
            Some(n) => Ok(Rows::affected(Some(n))),
            None => Err(Error::Protocol(
                "a row count with no count in it".to_string(),
            )),
        },
        Some(2) => {
            let mut data = response;
            data.remove(0);
            Rows::decode(data)
        }
        Some(3) => Err(Error::Server(
            String::from_utf8_lossy(&response[1..]).into_owned(),
        )),
        Some(other) => Err(Error::Protocol(format!("unknown answer tag {other}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn answer_tags() {
        assert_eq!(decode_response(vec![0]).unwrap().rows_affected(), None);
        let mut ack = vec![0];
        ack.extend_from_slice(&4u64.to_le_bytes());
        assert_eq!(decode_response(ack).unwrap().rows_affected(), Some(4));
        let mut count = vec![1];
        count.extend_from_slice(&9u64.to_le_bytes());
        assert_eq!(decode_response(count).unwrap().rows_affected(), Some(9));
        assert!(decode_response(vec![1, 2]).is_err());
        assert!(decode_response(vec![2]).unwrap().is_empty());
        match decode_response(b"\x03no such table".to_vec()) {
            Err(Error::Server(m)) => assert_eq!(m, "no such table"),
            other => panic!("{other:?}"),
        }
        assert!(decode_response(vec![]).is_err());
        assert!(decode_response(vec![7]).is_err());
    }
}
