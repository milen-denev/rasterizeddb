use std::sync::atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering};
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
    /// Whether a TCP connection believes the server it reached (a unix
    /// socket has no TLS, and needs none). The default,
    /// [`Trust::AcceptAny`](crate::Trust::AcceptAny), encrypts without
    /// authenticating the server, which suits the self-signed certificate a
    /// server makes for itself; [`load_ca`](crate::load_ca) reads a private
    /// CA's certificate for [`Trust::RootCa`](crate::Trust::RootCa), and
    /// [`Trust::WebPki`](crate::Trust::WebPki) uses the public roots.
    pub trust: crate::Trust,
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
            trust: crate::Trust::AcceptAny,
        }
    }
}

/// `builder` believing the server as `trust` says. Accepting anything is
/// left unsaid: rastcp refuses a unix socket that names any trust at all,
/// since a socket carries no TLS to check, and "accept anything" asks for
/// nothing there.
pub(crate) fn with_trust(builder: TcpClientBuilder, trust: &crate::Trust) -> TcpClientBuilder {
    if trust.authenticates() {
        builder.trust(trust.clone())
    } else {
        builder.verify_certificate(false)
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
    /// Where transactions get their connections: each one its own, never
    /// one of the shared connections above.
    transactions: std::sync::Arc<crate::transaction::Connections>,
    /// Whether the server takes a statement and its values apart
    /// (`rastcp::exec`): [`EXEC_UNKNOWN`] until the first statement with
    /// values asks it, then [`EXEC_YES`] or [`EXEC_NO`].
    exec: AtomicU8,
}

const EXEC_UNKNOWN: u8 = 0;
const EXEC_YES: u8 = 1;
const EXEC_NO: u8 = 2;

impl Client {
    /// Connects to `addr`: `unix:/path/to/socket` for a unix socket (the
    /// server's `--rdb-socket`), or `host:port` for TCP.
    ///
    /// Every connection is opened and logged in before this returns, so a
    /// wrong path or a refused login is this call's error. So is a migration
    /// that fails, when [`Options::migrations`] is set and
    /// [`Options::auto_migrate`] applies or verifies it.
    ///
    /// Over TCP the transport is TLS, and the server's certificate is
    /// checked as [`Options::trust`] says: by default it is not, so use a
    /// unix socket, a network you trust, or a [`Trust`](crate::Trust) that
    /// authenticates. [`Login::Password`](crate::Login::Password) holds
    /// either way: its exchange is bound to the connection, and the server
    /// must prove it holds the password's verifier.
    pub async fn connect(addr: &str, options: Options) -> Result<Self> {
        let authenticator = options.login.authenticator()?;
        let mut connections = Vec::with_capacity(options.connections.max(1));
        for _ in 0..options.connections.max(1) {
            let mut builder =
                with_trust(TcpClientBuilder::new(addr), &options.trust)
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
            transactions: crate::transaction::Connections::new(crate::transaction::Dial {
                addr: addr.to_string(),
                timeout: options.timeout,
                max_message_size: options.max_message_size,
                authenticator,
                trust: options.trust.clone(),
            }),
            exec: AtomicU8::new(EXEC_UNKNOWN),
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
            transactions: crate::transaction::Connections::new(crate::transaction::Dial {
                addr: String::new(),
                timeout: None,
                max_message_size: 0,
                authenticator: None,
                trust: crate::Trust::AcceptAny,
            }),
            exec: AtomicU8::new(EXEC_NO),
        }
    }

    fn pick(&self) -> &Connection {
        let turn = self.next.fetch_add(1, Ordering::Relaxed);
        &self.connections[turn % self.connections.len()]
    }

    /// Opens a transaction, at the server's default isolation (read
    /// committed). See [`Transaction`](crate::Transaction).
    ///
    /// The transaction runs on a connection of its own, never one of the
    /// client's shared ones: a finished transaction's connection is kept a
    /// short while for the next, and otherwise one is opened (and logged
    /// in) for it.
    ///
    /// ```no_run
    /// # async fn demo(client: &rasterizeddb_client::Client) -> rasterizeddb_client::Result<()> {
    /// use rasterizeddb_client::{Executor, params};
    /// let tx = client.begin().await?;
    /// tx.exec("UPDATE account SET balance = balance - $1 WHERE id = $2", params![10, 1]).await?;
    /// tx.exec("UPDATE account SET balance = balance + $1 WHERE id = $2", params![10, 2]).await?;
    /// tx.commit().await?;
    /// # Ok(()) }
    /// ```
    pub async fn begin(&self) -> Result<crate::Transaction> {
        self.begin_with(crate::TransactionOptions::default()).await
    }

    /// [`begin`](Self::begin) with an isolation level or `READ ONLY`.
    /// [`TransactionOptions::retries`](crate::TransactionOptions::retries)
    /// is not used here.
    pub async fn begin_with(
        &self,
        options: crate::TransactionOptions,
    ) -> Result<crate::Transaction> {
        self.transactions.begin(&options).await
    }

    /// Runs `body` in a transaction: commits when it returns `Ok`, rolls
    /// back when it returns `Err`, and after a serialization failure or a
    /// deadlock ([`Error::is_retryable`]) runs it again in a new transaction,
    /// up to [`DEFAULT_TRANSACTION_RETRIES`](crate::DEFAULT_TRANSACTION_RETRIES)
    /// times. **The closure may therefore run more than once**: keep what it
    /// does outside the database to after it returns.
    ///
    /// A COMMIT whose connection failed is never retried: whether it
    /// committed is unknown ([`Error::is_commit_outcome_unknown`]).
    ///
    /// ```no_run
    /// # async fn demo(client: &rasterizeddb_client::Client) -> rasterizeddb_client::Result<()> {
    /// use rasterizeddb_client::{Executor, params};
    /// let moved = client
    ///     .transaction(|tx| {
    ///         Box::pin(async move {
    ///             tx.exec("UPDATE account SET balance = balance - 10 WHERE id = $1", params![1]).await?;
    ///             tx.exec("UPDATE account SET balance = balance + 10 WHERE id = $1", params![2]).await
    ///         })
    ///     })
    ///     .await?;
    /// # let _ = moved; Ok(()) }
    /// ```
    pub async fn transaction<T, F>(&self, body: F) -> Result<T>
    where
        T: Send,
        F: for<'t> FnMut(&'t crate::Transaction) -> crate::TransactionFuture<'t, T> + Send,
    {
        self.transaction_with(crate::TransactionOptions::default(), body)
            .await
    }

    /// [`transaction`](Self::transaction) with an isolation level, `READ
    /// ONLY`, or another retry count.
    pub async fn transaction_with<T, F>(
        &self,
        options: crate::TransactionOptions,
        body: F,
    ) -> Result<T>
    where
        T: Send,
        F: for<'t> FnMut(&'t crate::Transaction) -> crate::TransactionFuture<'t, T> + Send,
    {
        crate::transaction::run(&self.transactions, options, body).await
    }

    /// Runs `sql` exactly as given, with no placeholder rendering.
    ///
    /// Text holding a second statement, or transaction control, is refused
    /// with [`Error::Unsupported`] before anything is sent (see
    /// [`check_statement`](crate::check_statement)); a transaction is opened
    /// with [`begin`](Self::begin).
    pub async fn run(&self, sql: &str) -> Result<Rows> {
        check_statement(sql)?;
        self.send(sql).await
    }

    /// Runs `sql` as a PostgreSQL client's simple query runs, and answers as
    /// pgwire would: each result's column names and type OIDs, every cell as
    /// PostgreSQL's text, each statement's command tag, and the notices. See
    /// [`simple`](crate::simple).
    ///
    /// For code that shows results it did not write the statement for (a
    /// console, an export). `sql` may hold several statements, which run as one
    /// implicit transaction, and `WITH` and derived tables work. Nothing is
    /// rendered: `$1` is sent as written. A `BEGIN` must be ended in the same
    /// call, because the client's connections are shared; one left open is
    /// rolled back and reported as an error.
    ///
    /// A failed statement is [`Error::Server`] with its SQLSTATE; a server
    /// too old for this form refuses it without running anything, as
    /// [`Error::Unsupported`].
    pub async fn simple_query(&self, sql: &str) -> Result<crate::simple::SimpleAnswer> {
        self.simple_query_with(sql, &crate::SimpleOptions::default())
            .await
    }

    /// [`simple_query`](Self::simple_query), with a [`CancelToken`](crate::CancelToken)
    /// that [`cancel`](Self::cancel) stops it by, or a cap on the rows sent
    /// of each result (see [`SimpleOptions`](crate::SimpleOptions)).
    ///
    /// ```no_run
    /// # async fn demo(client: std::sync::Arc<rasterizeddb_client::Client>) -> rasterizeddb_client::Result<()> {
    /// use rasterizeddb_client::{CancelToken, SimpleOptions};
    ///
    /// let token = CancelToken::new();
    /// let options = SimpleOptions { cancel: Some(token.clone()), max_rows: Some(1000) };
    /// let running = {
    ///     let client = client.clone();
    ///     tokio::spawn(async move { client.simple_query_with("SELECT * FROM big", &options).await })
    /// };
    /// // Later, from anywhere holding the client:
    /// client.cancel(&token).await?;
    /// let stopped = running.await.unwrap(); // Err(Server("… (SQLSTATE 57014)"))
    /// # let _ = stopped; Ok(()) }
    /// ```
    pub async fn simple_query_with(
        &self,
        sql: &str,
        options: &crate::SimpleOptions,
    ) -> Result<crate::simple::SimpleAnswer> {
        let request = crate::simple::request(sql, options)?;
        crate::simple::decode(self.send_bytes(&request).await?)
    }

    /// Stops the statement a simple query sent with `token` (by this client,
    /// a [`SimpleSession`](crate::SimpleSession), or any connection of the same
    /// user) is running; it fails with SQLSTATE 57014. Returns whether one
    /// was running: `false` when the query has ended, has not started, or is
    /// between two of its statements. Nothing is reported to the query's
    /// caller beyond its error.
    pub async fn cancel(&self, token: &crate::CancelToken) -> Result<bool> {
        crate::simple::decode_cancel(
            self.send_bytes(&crate::simple::cancel_request(token))
                .await?,
        )
    }

    /// Opens a connection of its own for simple queries, where a transaction
    /// block lasts from one call to the next. See
    /// [`SimpleSession`](crate::SimpleSession).
    pub async fn simple_session(&self) -> Result<crate::SimpleSession> {
        crate::SimpleSession::open(self.transactions.clone(), self.idle_reconnect).await
    }

    /// Sends text already checked (by [`check_statement`], or by rendering).
    pub(crate) async fn send(&self, sql: &str) -> Result<Rows> {
        decode_response(self.send_bytes(sql.as_bytes()).await?)
    }

    async fn send_bytes(&self, request: &[u8]) -> Result<Vec<u8>> {
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
        Ok(connection.mux.send(request).await?)
    }

    /// Runs `sql` with `$1`…`$n` replaced by `params` (see [`render`]) and
    /// returns its rows. A statement that returns none answers with an empty
    /// [`Rows`] whose [`Rows::rows_affected`] says what it reported.
    pub async fn query(&self, sql: &str, params: &[Param<'_>]) -> Result<Rows> {
        if self.sends_exec().await {
            let request = crate::params::exec_request_with(sql, params.len(), |i, out| {
                crate::params::push_param(out, &params[i])
            })?;
            return decode_response(self.send_bytes(&request).await?);
        }
        let statement = render(sql, params)?;
        self.send(&statement).await
    }

    /// Whether statements with values go to the server as their text and
    /// values apart (`rastcp::exec`), which a server that knows the form
    /// answers without finding the values in the text again. Asked of the
    /// server once, with `SELECT 1`: a server without the form refuses the
    /// request without running anything, and its statements are then
    /// rendered as text, as they always were.
    async fn sends_exec(&self) -> bool {
        match self.exec.load(Ordering::Relaxed) {
            EXEC_YES => return true,
            EXEC_NO => return false,
            _ => {}
        }
        let probe = rastcp::exec::encode("SELECT 1", &[], &[]).expect("a short request");
        let answer = match self.send_bytes(&probe).await {
            Ok(answer) => answer,
            // Not an answer about the form: decided by a later statement.
            Err(_) => return false,
        };
        let supported = answer.first() == Some(&2);
        self.exec.store(if supported { EXEC_YES } else { EXEC_NO }, Ordering::Relaxed);
        supported
    }

    /// [`query`](Self::query) with values given as [`ToSql`], as
    /// [`params!`](crate::params) builds them.
    pub async fn query_with(&self, sql: &str, params: &[&dyn ToSql]) -> Result<Rows> {
        if self.sends_exec().await {
            let request = crate::params::exec_request_with(sql, params.len(), |i, out| {
                crate::params::push_param(out, &params[i].to_param())
            })?;
            return decode_response(self.send_bytes(&request).await?);
        }
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

    /// Closes every shared connection, and the connections kept for
    /// transactions. A transaction still open keeps its own until it ends.
    pub async fn close(&self) {
        for connection in &self.connections {
            connection.mux.close().await;
        }
        self.transactions.forget_spares();
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
