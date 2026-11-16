use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

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
    /// A connection quiet for this long is reopened before a statement is
    /// sent on it (default 20 s; `None` never does).
    ///
    /// The server closes a connection that has been idle for its request
    /// timeout (30 s). A statement written in the instant it does is lost
    /// with the connection and fails with [`Error::Transport`], and is not
    /// sent again, because whether it ran is unknown. Reopening first, well
    /// inside the server's limit, means no statement is ever sent on a
    /// connection the server may be closing. It costs one handshake after a
    /// quiet spell and no traffic while idle. A connection with a statement in
    /// flight is never reopened under it.
    pub idle_reconnect: Option<Duration>,
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
            idle_reconnect: Some(DEFAULT_IDLE_RECONNECT),
        }
    }
}

/// [`Options::idle_reconnect`]'s default: two thirds of the server's 30 s
/// idle limit, leaving room for clock and scheduling slack.
pub const DEFAULT_IDLE_RECONNECT: Duration = Duration::from_secs(20);

/// One multiplexed connection, and what deciding to reopen it needs.
struct Connection {
    mux: MuxClient,
    /// Senders hold it shared for a statement's whole round trip; a reopen
    /// takes it exclusively, and only when it can at once, so a connection is
    /// never closed under a statement in flight.
    gate: tokio::sync::RwLock<()>,
    /// Statements in flight.
    in_flight: AtomicUsize,
    /// When the connection was last used (sent on, or answered), in µs since
    /// [`Client::epoch`].
    last_used: AtomicU64,
}

/// Counts a statement in flight, and marks the connection used when it ends,
/// however it ends (a dropped future included).
struct InFlight<'a> {
    connection: &'a Connection,
    epoch: Instant,
}

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        self.connection.touch(self.epoch);
        self.connection.in_flight.fetch_sub(1, Ordering::AcqRel);
    }
}

impl Connection {
    fn touch(&self, epoch: Instant) {
        self.last_used
            .store(epoch.elapsed().as_micros() as u64, Ordering::Release);
    }

    fn idle_for(&self, epoch: Instant) -> Duration {
        let now = epoch.elapsed().as_micros() as u64;
        Duration::from_micros(now.saturating_sub(self.last_used.load(Ordering::Acquire)))
    }

    /// Closes the connection if nothing is in flight and it has been quiet for
    /// `limit`; the next send opens a new one. A sender holding the gate, or
    /// one arriving meanwhile, makes this give up: that connection is in use,
    /// so the server is not closing it either.
    async fn reopen_if_idle(&self, limit: Duration, epoch: Instant) {
        if self.in_flight.load(Ordering::Acquire) != 0 || self.idle_for(epoch) < limit {
            return;
        }
        let Ok(_exclusive) = self.gate.try_write() else {
            return;
        };
        if self.in_flight.load(Ordering::Acquire) == 0 && self.idle_for(epoch) >= limit {
            // The connection is marked down; the send that follows dials anew.
            self.mux.close().await;
            self.touch(epoch);
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
    connections: Vec<Connection>,
    next: AtomicUsize,
    idle_reconnect: Option<Duration>,
    /// The clock [`Connection::last_used`] counts from.
    epoch: Instant,
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
            let mux = builder.build_multiplexed().await?;
            mux.connect().await?;
            connections.push(Connection {
                mux,
                gate: tokio::sync::RwLock::new(()),
                in_flight: AtomicUsize::new(0),
                last_used: AtomicU64::new(0),
            });
        }
        let client = Client {
            connections,
            next: AtomicUsize::new(0),
            idle_reconnect: options.idle_reconnect,
            epoch: Instant::now(),
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
            idle_reconnect: None,
            epoch: Instant::now(),
        }
    }

    fn pick(&self) -> &Connection {
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
        let connection = self.pick();
        if let Some(limit) = self.idle_reconnect {
            connection.reopen_if_idle(limit, self.epoch).await;
        }
        let _shared = connection.gate.read().await;
        connection.in_flight.fetch_add(1, Ordering::AcqRel);
        connection.touch(self.epoch);
        let _in_flight = InFlight {
            connection,
            epoch: self.epoch,
        };
        let response = connection.mux.send(sql.as_bytes()).await?;
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

    /// Whether every connection is open right now.
    ///
    /// Not a health check: a connection the server closed after a quiet
    /// spell, or one [`Options::idle_reconnect`] retired, is closed until
    /// the next statement on it reopens it, which happens by itself. A client
    /// that answers `false` here works; one whose server is gone fails its
    /// next statement with [`Error::Transport`].
    pub fn is_connected(&self) -> bool {
        self.connections.iter().all(|c| c.mux.is_connected())
    }

    /// Closes every connection.
    pub async fn close(&self) {
        for connection in &self.connections {
            connection.mux.close().await;
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
