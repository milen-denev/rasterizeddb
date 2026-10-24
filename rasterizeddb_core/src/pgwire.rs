use std::{
    collections::HashMap,
    future::Future,
    io,
    pin::Pin,
    sync::Arc as StdArc,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    task::{Context, Poll},
    time::Duration,
};

use log::trace;
use rclite::Arc;

use std::io::BufReader;

use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf},
    net::{TcpListener, TcpStream},
};

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::TlsAcceptor;

use crate::core::{
    database::Database,
    database::catalog::{DatabaseCatalog, DatabaseStatement, parse_database_statement},
    db_type::DbType,
    mvcc::transaction::TxId,
    row::row::Row,
    rql::{
        lexer_query::row_fetch_from_select_query, lexer_s1::QueryPurpose,
        lexer_s1::recognize_query_purpose,
    },
};
use crate::pg_params::{BoundParam, inline_params};

mod param_infer;
#[cfg(test)]
mod prepared_partition_tests;
mod statement_intern;
#[cfg(test)]
mod owner_tests;
#[cfg(test)]
mod temp_relation_tests;
mod wire_out;

/// Start a minimal PostgreSQL wire-protocol server (pgwire).
///
/// This is **not** a full PostgreSQL implementation; it supports the v3 protocol
/// enough for `psql` and most simple drivers to connect using the *simple query*
/// path, and it forwards a subset of SQL to RasterizedDB's query engine.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TlsMode {
    Disable,
    Prefer,
    Require,
}

/// How connections authenticate.
#[derive(Clone, Debug)]
pub struct AuthConfig {
    pub method: crate::auth::AuthMethod,
    /// Role created when the catalogue is empty, so a fresh database can be
    /// connected to at all.
    pub bootstrap_user: String,
    /// Password for that role. `None` leaves it usable only under `trust`,
    /// rather than making it a passwordless superuser.
    pub bootstrap_password: Option<String>,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            // The method every modern driver negotiates, and the only one here
            // that never puts a replayable secret on the wire.
            method: crate::auth::AuthMethod::ScramSha256,
            bootstrap_user: "rasterized".to_string(),
            bootstrap_password: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct TlsConfig {
    pub mode: TlsMode,
    /// PEM certificate chain path (optional; if missing, a self-signed cert is generated)
    pub cert_path: Option<String>,
    /// PEM private key path (optional; if missing, a self-signed cert is generated)
    pub key_path: Option<String>,
}

/// Server-wide bounds on connections, and the defaults a new session's idle
/// timeouts start from.
///
/// Every limit here exists because the absence of it let one misbehaving
/// client take something from all the others: file descriptors held by
/// sockets that never finish their handshake, a connection count with no
/// ceiling, or a transaction left open by a client that went away without
/// closing its socket — which holds back reclamation for the whole database.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConnectionLimits {
    /// Sessions served at once. A connection over this answers its startup
    /// with `53300` ("sorry, too many clients already"), as PostgreSQL's
    /// does. Zero is refused at start-up rather than read as "unlimited": an
    /// unbounded count is the failure this exists to prevent.
    pub max_connections: usize,
    /// How long a connection has, from accept, to finish its startup packet,
    /// any TLS negotiation and authentication. PostgreSQL's
    /// `authentication_timeout`. Zero is refused at start-up.
    pub handshake_timeout: Duration,
    /// The `idle_in_transaction_session_timeout` a session starts with and
    /// `RESET` returns to. Zero disables it, as in PostgreSQL.
    pub idle_in_transaction_session_timeout: Duration,
    /// The `idle_session_timeout` a session starts with and `RESET` returns
    /// to. Zero disables it, as in PostgreSQL.
    pub idle_session_timeout: Duration,
}

impl Default for ConnectionLimits {
    fn default() -> Self {
        Self {
            // PostgreSQL's 100, with headroom: pools here are commonly sized
            // per service, and several services share one server.
            max_connections: 500,
            // PostgreSQL's `authentication_timeout` default.
            handshake_timeout: Duration::from_secs(60),
            idle_in_transaction_session_timeout: Duration::ZERO,
            idle_session_timeout: Duration::ZERO,
        }
    }
}

impl ConnectionLimits {
    /// Refuses the settings that would quietly remove a bound.
    fn validate(&self) -> io::Result<()> {
        if self.max_connections == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "max_connections must be at least 1 (an unlimited connection count is not supported)",
            ));
        }
        if self.handshake_timeout.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "the handshake (authentication) timeout must be greater than zero",
            ));
        }
        for (name, value) in [
            (
                "idle_in_transaction_session_timeout",
                self.idle_in_transaction_session_timeout,
            ),
            ("idle_session_timeout", self.idle_session_timeout),
        ] {
            if value.as_millis() > i32::MAX as u128 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("{} is larger than {} ms", name, i32::MAX),
                ));
            }
        }
        Ok(())
    }

    /// These limits with `max_connections` brought within the process's
    /// descriptor limit, raising the soft limit first if the hard one allows.
    ///
    /// The cap exists so that a flood of clients is told `53300` instead of
    /// running the process out of descriptors. A cap the descriptor limit
    /// cannot hold defeats that twice over: `accept()` starts failing with
    /// `EMFILE` before anyone is refused, so new clients wait in the backlog,
    /// and the engine's own opens — a new table file, a WAL rollover, a
    /// checkpoint — fail inside sessions that were already admitted. A
    /// systemd unit's usual soft limit of 1024 runs out at about 480 sessions,
    /// under the default cap of 500 and before the engine's files are counted.
    ///
    /// Lowering the cap is logged as a warning, and `SHOW max_connections`
    /// reports the value in effect, so an operator can see they did not get
    /// what they asked for.
    fn fit_to_descriptor_limit(self) -> Self {
        #[cfg(unix)]
        {
            let mut limit = libc::rlimit { rlim_cur: 0, rlim_max: 0 };
            // SAFETY: a valid out-pointer to an `rlimit`.
            if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } != 0 {
                log::warn!(
                    "pgwire could not read the descriptor limit ({}); max_connections {} \
                     is not checked against it",
                    io::Error::last_os_error(),
                    self.max_connections
                );
                return self;
            }
            let (soft, hard) = (limit.rlim_cur as u64, limit.rlim_max as u64);
            let plan = plan_descriptors(self.max_connections, soft, hard);
            let mut in_effect = soft;
            if let Some(raise_to) = plan.raise_soft_to {
                let raised = libc::rlimit {
                    rlim_cur: raise_to as libc::rlim_t,
                    rlim_max: limit.rlim_max,
                };
                // SAFETY: a valid pointer to an `rlimit`; a soft limit at or
                // below the hard one is always permitted.
                if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &raised) } == 0 {
                    log::info!(
                        "pgwire raised the open-file soft limit from {} to {} for \
                         max_connections {}",
                        soft,
                        raise_to,
                        self.max_connections
                    );
                    in_effect = raise_to;
                } else {
                    log::warn!(
                        "pgwire could not raise the open-file soft limit from {} to {}: {}",
                        soft,
                        raise_to,
                        io::Error::last_os_error()
                    );
                }
            }
            let max_connections = connections_within(self.max_connections, in_effect);
            if max_connections < self.max_connections {
                log::warn!(
                    "pgwire max_connections lowered from {} to {}: the open-file limit is {} \
                     (hard {}), and each connection can hold {} descriptors with {} kept for \
                     the engine. Raise LimitNOFILE / ulimit -n to serve more.",
                    self.max_connections,
                    max_connections,
                    in_effect,
                    hard,
                    FDS_PER_CONNECTION,
                    RESERVED_SERVER_FDS
                );
            }
            Self { max_connections, ..self }
        }
        #[cfg(not(unix))]
        {
            self
        }
    }
}

/// Descriptors kept out of the connection budget for everything else the
/// process opens: table files and their sidecars, the WAL, checkpoint files,
/// the listener. The same reserve `rastcp`'s server keeps.
const RESERVED_SERVER_FDS: u64 = 256;

/// What one unit of `max_connections` can cost in descriptors. An admitted
/// session holds its socket and the duplicate `PEER_WATCH` peeks on, and the
/// refusals in flight, capped at the same count, each hold the same two.
const FDS_PER_CONNECTION: u64 = 4;

/// How [`ConnectionLimits::fit_to_descriptor_limit`] meets a requested cap.
#[derive(Debug, PartialEq, Eq)]
struct DescriptorPlan {
    /// The soft limit to ask for, when the current one is short and the hard
    /// one leaves room to raise it.
    raise_soft_to: Option<u64>,
    /// The cap if that raise succeeds (or none was needed).
    max_connections: usize,
}

/// The descriptors `requested` connections need, and what to do about a soft
/// limit that is short of them.
///
/// The soft limit is raised only as far as the cap needs, never to the hard
/// limit wholesale: anything else in the process that still uses `select()`
/// breaks on a descriptor above 1024, so a larger table is not free.
fn plan_descriptors(requested: usize, soft: u64, hard: u64) -> DescriptorPlan {
    let needed = (requested as u64)
        .saturating_mul(FDS_PER_CONNECTION)
        .saturating_add(RESERVED_SERVER_FDS);
    let raise_soft_to = (soft < needed && hard > soft).then(|| needed.min(hard));
    DescriptorPlan {
        raise_soft_to,
        max_connections: connections_within(requested, raise_soft_to.unwrap_or(soft)),
    }
}

/// `requested`, lowered to what a soft limit of `soft` descriptors can hold,
/// and never below one: a server that admits nobody is not a working server,
/// and the warning says why it may still run short.
fn connections_within(requested: usize, soft: u64) -> usize {
    let budget = soft.saturating_sub(RESERVED_SERVER_FDS) / FDS_PER_CONNECTION;
    (requested as u64).min(budget).max(1) as usize
}

pub async fn start_pgwire(
    database: Arc<Database>,
    addr: &str,
    port: u16,
    tls: TlsConfig,
) -> io::Result<()> {
    start_pgwire_with_auth(database, addr, port, tls, AuthConfig::default()).await
}

/// Serves one already-open database, reachable under the name it carries.
///
/// For embedding a single database behind the wire protocol. A server with
/// several databases starts from [`start_pgwire_with_catalog`].
pub async fn start_pgwire_with_auth(
    database: Arc<Database>,
    addr: &str,
    port: u16,
    tls: TlsConfig,
    auth: AuthConfig,
) -> io::Result<()> {
    let catalog = DatabaseCatalog::single(database);
    start_pgwire_with_catalog(catalog, addr, port, tls, auth).await
}

/// Serves every database in `catalog`, each connection being routed by the
/// name in its startup packet.
pub async fn start_pgwire_with_catalog(
    catalog: Arc<DatabaseCatalog>,
    addr: &str,
    port: u16,
    tls: TlsConfig,
    auth: AuthConfig,
) -> io::Result<()> {
    start_pgwire_with_limits(catalog, addr, port, tls, auth, ConnectionLimits::default()).await
}

/// The shortest and longest pause after a failed `accept()`.
///
/// The failures worth pausing for are the process or the system running out
/// of descriptors or memory, which a retry in the same instant cannot fix and
/// which a tight retry loop makes worse by spinning a core while the sessions
/// that would free a descriptor need it.
const ACCEPT_BACKOFF_MIN: Duration = Duration::from_millis(50);
const ACCEPT_BACKOFF_MAX: Duration = Duration::from_secs(1);

/// How often the accept loop repeats a warning about the same condition. A
/// connection storm can produce one per accepted socket, and a log line per
/// socket is how a storm fills the root disk with syslog.
const ACCEPT_WARN_INTERVAL: Duration = Duration::from_secs(10);

/// [`start_pgwire_with_catalog`], with explicit connection limits.
///
/// Returns an error only when the listener could not start — the limits are
/// invalid, the port is taken, the certificate is unreadable. A failed
/// `accept()` is retried, never returned.
pub async fn start_pgwire_with_limits(
    catalog: Arc<DatabaseCatalog>,
    addr: &str,
    port: u16,
    tls: TlsConfig,
    auth: AuthConfig,
    limits: ConnectionLimits,
) -> io::Result<()> {
    let limits = prepare_pgwire(&catalog, &auth, limits).await?;
    let listener = TcpListener::bind(format!("{}:{}", addr, port)).await?;
    serve_pgwire(catalog, listener, tls, auth, limits).await
}

/// [`start_pgwire_with_limits`], serving a listener the caller already bound.
///
/// For a caller that must know the port before the server runs — a test
/// harness above all, which binds `127.0.0.1:0`. Handing over a port *number*
/// means letting go of the socket until the server binds it again, and in that
/// window a test running in parallel can be given the same freed port: one
/// test's client then reaches the other test's server and fails on whatever it
/// finds there ("table already exists"), a convincing engine error with nothing
/// wrong in the engine. Holding the socket throughout closes the window.
///
/// The listener is served as it is; connections that arrive before the accept
/// loop starts wait in its backlog.
pub async fn start_pgwire_on_listener(
    catalog: Arc<DatabaseCatalog>,
    listener: TcpListener,
    tls: TlsConfig,
    auth: AuthConfig,
    limits: ConnectionLimits,
) -> io::Result<()> {
    let limits = prepare_pgwire(&catalog, &auth, limits).await?;
    serve_pgwire(catalog, listener, tls, auth, limits).await
}

/// What has to hold before the first connection is accepted: valid limits,
/// fitted to the descriptor budget, and every database's roles.
async fn prepare_pgwire(
    catalog: &Arc<DatabaseCatalog>,
    auth: &AuthConfig,
    limits: ConnectionLimits,
) -> io::Result<ConnectionLimits> {
    limits.validate()?;
    // From here on `limits` is what is enforced and what `SHOW` reports.
    let limits = limits.fit_to_descriptor_limit();

    // Every database needs its roles before the first connection tries to
    // authenticate: they are per database, so one seeded catalogue does not
    // make the others reachable.
    for database in catalog.all() {
        if let Err(e) = seed_roles(&database, auth).await {
            return Err(io::Error::new(io::ErrorKind::Other, e));
        }
    }
    Ok(limits)
}

/// The accept loop, shared by every way of starting the server.
async fn serve_pgwire(
    catalog: Arc<DatabaseCatalog>,
    listener: TcpListener,
    tls: TlsConfig,
    auth: AuthConfig,
    limits: ConnectionLimits,
) -> io::Result<()> {
    let bind_addr = listener.local_addr()?;
    log::info!(
        "pgwire server listening on {} ({} authentication, TLS {})",
        bind_addr,
        auth.method.name(),
        match tls.mode {
            TlsMode::Disable => "disabled",
            TlsMode::Prefer => "optional",
            TlsMode::Require => "required",
        }
    );

    let acceptor = match tls.mode {
        TlsMode::Disable => None,
        TlsMode::Prefer | TlsMode::Require => Some(build_tls_acceptor(&tls)?),
    };

    // Stop accepting on Ctrl+C/SIGTERM and let the sessions already running
    // finish, instead of having the process vanish mid-query.
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
    crate::shutdown::on_termination(move || {
        shutdown_tx.send_replace(true);
    });

    // Sessions admitted, against `max_connections`.
    let active = StdArc::new(AtomicUsize::new(0));
    // Connections over the cap that are still being told so. They need their
    // startup packet read before the refusal can be sent — over TLS, if that
    // is what they asked for — so they hold a socket for up to the handshake
    // timeout. Capped too, at the same number, so a storm costs at most twice
    // `max_connections` descriptors rather than whatever the storm sends.
    let rejecting = StdArc::new(AtomicUsize::new(0));

    let mut backoff = ACCEPT_BACKOFF_MIN;
    let mut failed_accepts: u64 = 0;
    let mut last_capacity_warning: Option<tokio::time::Instant> = None;

    loop {
        let accepted = tokio::select! {
            biased;

            _ = shutdown_rx.changed() => break,

            accepted = listener.accept() => accepted,
        };

        // An accept error is about this process or this one connection, never
        // about the listener, so it must not end the server. It used to: the
        // error was returned with `?`, the caller discarded it, and the process
        // exited — so the `EMFILE` a connection storm produces shut the
        // database down for everyone, including the sessions already running.
        let (stream, peer) = match accepted {
            Ok(accepted) => {
                if failed_accepts > 0 {
                    log::info!(
                        "pgwire accepting again after {} failed accept(s)",
                        failed_accepts
                    );
                }
                failed_accepts = 0;
                backoff = ACCEPT_BACKOFF_MIN;
                accepted
            }
            Err(e) if is_per_connection_accept_error(&e) => {
                // The peer gave up between SYN and accept. Nothing is short;
                // the next connection can be taken at once.
                log::debug!("pgwire accept: {}", e);
                continue;
            }
            Err(e) => {
                failed_accepts += 1;
                if failed_accepts == 1 {
                    log::warn!(
                        "pgwire accept failed: {}; retrying with backoff (up to {:?})",
                        e,
                        ACCEPT_BACKOFF_MAX
                    );
                } else {
                    log::debug!("pgwire accept failed again ({}): {}", failed_accepts, e);
                }
                tokio::select! {
                    biased;

                    _ = shutdown_rx.changed() => break,

                    _ = tokio::time::sleep(backoff) => {}
                }
                backoff = (backoff * 2).min(ACCEPT_BACKOFF_MAX);
                continue;
            }
        };

        // Admission is decided here, when the socket exists, as PostgreSQL's
        // postmaster decides it at fork. The slot is a guard so every way a
        // connection ends — a clean Terminate, an error, a timeout, a panic
        // unwinding the task — gives it back; a counter decremented at the end
        // of the task leaked a slot on every panic.
        let admission = match ConnectionSlot::try_acquire(&active, limits.max_connections) {
            Some(slot) => Admission::Admitted(slot),
            None => {
                let warn_due =
                    last_capacity_warning.is_none_or(|at| at.elapsed() >= ACCEPT_WARN_INTERVAL);
                if warn_due {
                    log::warn!(
                        "pgwire connection limit reached ({} sessions); refusing new \
                         connections with 53300",
                        limits.max_connections
                    );
                    last_capacity_warning = Some(tokio::time::Instant::now());
                }
                match ConnectionSlot::try_acquire(&rejecting, limits.max_connections) {
                    Some(slot) => Admission::TooMany(slot),
                    None => {
                        // Even the refusals are at their bound. Closing without
                        // a word is the only response left that costs nothing;
                        // the client sees the connection reset.
                        log::debug!(
                            "pgwire closing {} unanswered: {} refusals already in flight",
                            peer,
                            limits.max_connections
                        );
                        drop(stream);
                        continue;
                    }
                }
            }
        };

        // Disable Nagle's algorithm on the accepted connection. Linux does NOT
        // inherit TCP_NODELAY from the listener, so without this the server's
        // reply waits on the delayed-ACK timer whenever it leaves in more than
        // one segment — which the protocol's reply shape makes routine, since a
        // query answers with RowDescription, DataRows and CommandComplete
        // written separately.
        //
        // Measured on loopback before this line existed: `SELECT 1` — a
        // statement answered from a match arm, doing no work at all — took
        // **41 ms**, and every other statement paid the same floor on top of
        // whatever it actually cost. `psql` saw it too, so it was never a
        // client's fault. `rastcp` has had this since it hit the same wall;
        // pgwire was simply missed.
        let _ = stream.set_nodelay(true);

        // Without keepalive a peer that vanishes — a client host powered off,
        // a NAT entry expiring — leaves the socket established forever, still
        // holding a connection slot and, if it was inside a block, an open
        // transaction. Best-effort: a socket that refuses it still works.
        if let Err(e) = enable_keepalive(&stream) {
            log::debug!("pgwire could not enable TCP keepalive for {}: {}", peer, e);
        }

        let catalog = catalog.clone();
        let tls_mode = tls.mode;
        let acceptor = acceptor.clone();
        let auth = auth.clone();

        tokio::spawn(async move {
            let context = ConnectionContext {
                too_many: matches!(admission, Admission::TooMany(_)),
                handshake_finished: StdArc::new(AtomicBool::new(false)),
                limits,
            };
            if let Err(e) =
                handle_connection(catalog, stream, acceptor, tls_mode, auth, &context).await
            {
                log::debug!("pgwire connection {} ended: {}", peer, e);
            }
            // Released here at the latest; on a panic, by the unwind.
            drop(admission);
        });
    }

    drain_pgwire_connections(&active).await;

    Ok(())
}

/// Gives a database the bootstrap role it needs to be reachable at all.
///
/// A no-op under `trust`, where no role is consulted, and a no-op on a database
/// whose catalogue already has rows — so this neither resurrects a deleted
/// account nor resets a password on restart.
/// The field a fetched column refers to.
///
/// `ColumnFetchingData::schema_id` is a `write_order`, **not** a position in
/// `fields`. The two agree only while the numbering has no gaps, and
/// `DROP COLUMN` opens one — a table with 22 fields can legitimately hold
/// `write_order` 22, which indexed the slice out of bounds and panicked the
/// worker, dropping the client's connection mid-statement.
fn field_of<'a>(
    fields: &'a [crate::core::row::schema::SchemaField],
    schema_id: u64,
) -> Option<&'a crate::core::row::schema::SchemaField> {
    fields
        .iter()
        .find(|f| f.write_order == schema_id)
        .or_else(|| fields.get(schema_id as usize))
}

async fn seed_roles(database: &Arc<Database>, auth: &AuthConfig) -> Result<(), String> {
    if auth.method == crate::auth::AuthMethod::Trust {
        return Ok(());
    }
    crate::auth::ensure_role_catalogue(
        database,
        &auth.bootstrap_user,
        auth.bootstrap_password.as_deref(),
        auth.method,
    )
    .await
}

/// Waits out the sessions still running once the accept loop has stopped. The
/// grace period bounds it, so one long-running query cannot hold the process
/// open indefinitely.
async fn drain_pgwire_connections(active: &AtomicUsize) {
    const DRAIN_GRACE: Duration = Duration::from_secs(10);
    const DRAIN_POLL: Duration = Duration::from_millis(50);

    let remaining = active.load(Ordering::Relaxed);

    if remaining == 0 {
        log::info!("pgwire server stopped, no active sessions");
        return;
    }

    log::info!("pgwire server stopping, draining {} session(s)", remaining);

    let deadline = tokio::time::Instant::now() + DRAIN_GRACE;

    loop {
        let remaining = active.load(Ordering::Relaxed);

        if remaining == 0 {
            log::info!("All pgwire sessions drained");
            return;
        }

        if tokio::time::Instant::now() >= deadline {
            log::warn!(
                "Shutdown grace period elapsed with {} pgwire session(s) still active; closing anyway",
                remaining
            );
            return;
        }

        tokio::time::sleep(DRAIN_POLL).await;
    }
}

/// Whether an `accept()` error concerns one connection rather than the
/// process, so the listener can carry straight on without pausing.
fn is_per_connection_accept_error(e: &io::Error) -> bool {
    matches!(
        e.kind(),
        io::ErrorKind::ConnectionAborted | io::ErrorKind::ConnectionReset
    )
}

/// One unit of a connection counter, given back when dropped.
struct ConnectionSlot(StdArc<AtomicUsize>);

impl ConnectionSlot {
    /// Takes a unit if the counter is below `cap`. A compare-and-swap rather
    /// than an increment followed by a check, so two connections arriving
    /// together cannot both see room for one.
    fn try_acquire(counter: &StdArc<AtomicUsize>, cap: usize) -> Option<Self> {
        counter
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (n < cap).then_some(n + 1)
            })
            .ok()
            .map(|_| Self(counter.clone()))
    }
}

impl Drop for ConnectionSlot {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Whether a connection was admitted as a session, or is over the cap and is
/// only being told so. Either way it holds a slot in its own counter.
enum Admission {
    Admitted(#[allow(dead_code)] ConnectionSlot),
    TooMany(#[allow(dead_code)] ConnectionSlot),
}

/// What the accept loop decided about a connection, carried into it.
struct ConnectionContext {
    /// Over `max_connections`: answer the startup packet with `53300`.
    too_many: bool,
    /// Set once the session is established, which disarms the handshake
    /// deadline on the stream.
    handshake_finished: StdArc<AtomicBool>,
    limits: ConnectionLimits,
}

/// A stream that fails with `TimedOut` once the handshake deadline passes,
/// until the handshake is marked finished.
///
/// The deadline is enforced on the socket rather than by dropping the
/// connection's future, because the handshake is not only waiting on the
/// client: authentication runs a catalogue query, and a statement future
/// dropped mid-flight can leave its autocommit transaction in progress for
/// good. Here only a wait on the client can time out; the engine's work is
/// never interrupted. Every read and write, the TLS handshake included, sits
/// under it, so a socket that sends nothing, half a startup packet, or a
/// startup packet and then no password is closed alike.
struct HandshakeDeadline<S> {
    inner: S,
    deadline: Pin<Box<tokio::time::Sleep>>,
    finished: StdArc<AtomicBool>,
}

impl<S> HandshakeDeadline<S> {
    fn new(inner: S, timeout: Duration, finished: StdArc<AtomicBool>) -> Self {
        Self {
            inner,
            deadline: Box::pin(tokio::time::sleep(timeout)),
            finished,
        }
    }

    /// Whether the deadline has passed with the handshake still unfinished.
    ///
    /// Polling the timer registers this task to be woken when it fires, which
    /// is what turns a read blocked forever into one that returns an error.
    fn expired(&mut self, cx: &mut Context<'_>) -> bool {
        !self.finished.load(Ordering::Acquire) && self.deadline.as_mut().poll(cx).is_ready()
    }
}

fn handshake_timed_out() -> io::Error {
    io::Error::new(
        io::ErrorKind::TimedOut,
        "connection did not complete its startup and authentication within the handshake timeout",
    )
}

impl<S: AsyncRead + Unpin> AsyncRead for HandshakeDeadline<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_read(cx, buf) {
            Poll::Pending if this.expired(cx) => Poll::Ready(Err(handshake_timed_out())),
            other => other,
        }
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for HandshakeDeadline<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_write(cx, buf) {
            Poll::Pending if this.expired(cx) => Poll::Ready(Err(handshake_timed_out())),
            other => other,
        }
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_write_vectored(cx, bufs) {
            Poll::Pending if this.expired(cx) => Poll::Ready(Err(handshake_timed_out())),
            other => other,
        }
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_flush(cx) {
            Poll::Pending if this.expired(cx) => Poll::Ready(Err(handshake_timed_out())),
            other => other,
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

/// How long an accepted socket may sit silent before the kernel starts
/// probing it, how far apart the probes are, and how many go unanswered
/// before the connection is declared dead: about two minutes from the last
/// traffic to a closed socket. The system defaults wait two hours before the
/// first probe, which is long enough for a dead peer's open transaction to
/// hold back reclamation through a whole working day of churn.
#[cfg(any(target_os = "linux", target_os = "android"))]
const KEEPALIVE_IDLE_SECS: libc::c_int = 60;
#[cfg(any(target_os = "linux", target_os = "android"))]
const KEEPALIVE_INTERVAL_SECS: libc::c_int = 15;
#[cfg(any(target_os = "linux", target_os = "android"))]
const KEEPALIVE_PROBES: libc::c_int = 4;

/// Turns on TCP keepalive for an accepted socket.
///
/// Through `libc` because tokio's `TcpStream` exposes no keepalive setter and
/// `socket2` is not a dependency of this crate.
#[cfg(unix)]
fn enable_keepalive(stream: &TcpStream) -> io::Result<()> {
    use std::os::fd::AsRawFd;

    fn set(
        fd: libc::c_int,
        level: libc::c_int,
        name: libc::c_int,
        value: libc::c_int,
    ) -> io::Result<()> {
        // SAFETY: `fd` is a live socket owned by the caller's `TcpStream` for
        // the duration of the call, and the option value is a `c_int` passed
        // with its exact size, which is what every option set here expects.
        let rc = unsafe {
            libc::setsockopt(
                fd,
                level,
                name,
                &value as *const libc::c_int as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            )
        };
        if rc == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    let fd = stream.as_raw_fd();
    set(fd, libc::SOL_SOCKET, libc::SO_KEEPALIVE, 1)?;
    // Elsewhere the timings stay at the system's defaults: the option names
    // differ per platform, and keepalive with slow timings still bounds a
    // dead peer where none bounded it at all.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        set(fd, libc::IPPROTO_TCP, libc::TCP_KEEPIDLE, KEEPALIVE_IDLE_SECS)?;
        set(fd, libc::IPPROTO_TCP, libc::TCP_KEEPINTVL, KEEPALIVE_INTERVAL_SECS)?;
        set(fd, libc::IPPROTO_TCP, libc::TCP_KEEPCNT, KEEPALIVE_PROBES)?;
    }
    Ok(())
}

#[cfg(not(unix))]
fn enable_keepalive(_stream: &TcpStream) -> io::Result<()> {
    Ok(())
}

/// How long a session that is ending waits to hand the client its last
/// bytes: the `FATAL` that says why, whatever output is still buffered, a TLS
/// `close_notify`.
///
/// None of it changes the outcome, because the session ends either way. A
/// client that has stopped reading — its receive window shut, the kernel's
/// send buffer full — would otherwise park that write for as long as it
/// keeps its socket open, and an idle timeout that ends a session only to
/// wait forever on its goodbye frees nothing: the connection slot, the
/// descriptor and the task all stay held. Keepalive does not catch it either:
/// a peer that is alive and simply not reading answers its probes.
const SESSION_END_WRITE_BOUND: Duration = Duration::from_secs(2);

/// Runs one of a session's final writes, giving up after `bound`.
async fn bounded_final_write<F>(bound: Duration, write: F) -> io::Result<()>
where
    F: Future<Output = io::Result<()>>,
{
    match tokio::time::timeout(bound, write).await {
        Ok(result) => result,
        Err(_) => Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "the client did not take the session's final bytes; closing without them",
        )),
    }
}

const SSL_REQUEST_CODE: i32 = 80877103; // 0x04D2162F (1234, 5679)
const CANCEL_REQUEST_CODE: i32 = 80877102; // 0x04D2162E (1234, 5678)
const GSSENC_REQUEST_CODE: i32 = 80877104; // 0x04D21630 (1234, 5680)

tokio::task_local! {
    /// A second handle on this session's socket, for noticing that the client
    /// has gone while a statement is running.
    ///
    /// A task-local rather than a parameter: the whole session — startup, TLS,
    /// authentication, the message loop and every statement — runs in one task,
    /// and threading it through four signatures would touch the TLS and
    /// non-TLS startup paths for something only the statement path reads.
    static PEER_WATCH: Option<StdArc<PeerWatch>>;

    /// Where this session publishes the statement it is running, for a
    /// `CancelRequest` arriving on another connection to find. A task-local for
    /// the reason `PEER_WATCH` is one.
    static SESSION_CANCEL: StdArc<crate::core::cancel::SessionCancel>;
}

/// The duplicate descriptor `PEER_WATCH` holds, and what the session's read
/// buffer knows that the descriptor cannot see.
///
/// The session reads through [`wire_out::WireStream`], which takes whatever the
/// kernel has in one read. Messages a client pipelined behind the statement
/// running now therefore sit in that buffer rather than in the kernel, where
/// `peek` would have found them — so a client that sent `Query` then
/// `Terminate` and closed would look gone where it used to look present.
/// `input_buffered` restores the old answer: bytes still unread anywhere mean
/// the client is not known to have gone.
pub(crate) struct PeerWatch {
    socket: std::net::TcpStream,
    input_buffered: std::sync::atomic::AtomicBool,
}

impl PeerWatch {
    fn new(socket: std::net::TcpStream) -> Self {
        Self {
            socket,
            input_buffered: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Records whether unread client bytes are held in the session's buffer.
    pub(crate) fn set_input_buffered(&self, buffered: bool) {
        self.input_buffered.store(buffered, Ordering::Relaxed);
    }

    /// `client_has_gone`, answered as if the read buffer were still in the
    /// kernel.
    fn gone(&self) -> bool {
        !self.input_buffered.load(Ordering::Relaxed) && client_has_gone(&self.socket)
    }
}

/// The client's socket, and a duplicate of it to watch.
///
/// `try_clone` dups the descriptor, so the copy sees the same connection: a
/// `peek` on it reports end-of-file when the peer sends FIN, and reports it
/// whether or not the session is inside TLS, because it is watching the TCP
/// stream underneath.
///
/// Watching is best-effort. If the descriptor cannot be duplicated the session
/// runs exactly as it did before, with `statement_timeout` as its only bound.
fn duplicate_for_watch(
    tcp: TcpStream,
) -> io::Result<(TcpStream, Option<StdArc<PeerWatch>>)> {
    let standard = tcp.into_std()?;
    // Kept as a *std* socket, in non-blocking mode, because it is peeked from
    // inside the scan rather than awaited: see `StatementCancel::gone`.
    let watch = standard
        .try_clone()
        .and_then(|copy| {
            copy.set_nonblocking(true)?;
            Ok(copy)
        })
        .map(|copy| StdArc::new(PeerWatch::new(copy)))
        .ok();
    Ok((TcpStream::from_std(standard)?, watch))
}

/// Whether the client on `socket` has gone, without blocking and without
/// consuming anything.
///
/// `MSG_PEEK` leaves the bytes where they are, which it must: the extended
/// protocol pipelines Parse/Bind/Describe/Execute/Sync, and a check that
/// consumed them would steal protocol messages from the session.
fn client_has_gone(socket: &std::net::TcpStream) -> bool {
    let mut byte = [0u8; 1];
    match socket.peek(&mut byte) {
        // Readable and empty is end-of-file: the peer sent FIN.
        Ok(0) => true,
        // Bytes waiting, so the client is still there.
        Ok(_) => false,
        Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => false,
        // A reset — which is what an abandoned client with unread data sends —
        // or any other error: this socket will not come back.
        Err(_) => true,
    }
}

async fn handle_connection(
    catalog: Arc<DatabaseCatalog>,
    tcp: TcpStream,
    acceptor: Option<TlsAcceptor>,
    tls_mode: TlsMode,
    auth: AuthConfig,
    context: &ConnectionContext,
) -> io::Result<()> {
    let (tcp, watch) = duplicate_for_watch(tcp)?;
    // The deadline sits underneath everything, TLS included, and is disarmed
    // once the session is established (`finish_startup`).
    let tcp = HandshakeDeadline::new(
        tcp,
        context.limits.handshake_timeout,
        context.handshake_finished.clone(),
    );
    PEER_WATCH
        .scope(
            watch,
            handle_connection_inner(catalog, tcp, acceptor, tls_mode, auth, context),
        )
        .await
}

async fn handle_connection_inner(
    catalog: Arc<DatabaseCatalog>,
    mut tcp: HandshakeDeadline<TcpStream>,
    acceptor: Option<TlsAcceptor>,
    tls_mode: TlsMode,
    auth: AuthConfig,
    context: &ConnectionContext,
) -> io::Result<()> {
    // PostgreSQL clients may send one or more 8-byte startup *requests* before the
    // real StartupMessage (e.g., GSSENCRequest, SSLRequest). If we don't answer
    // them correctly, many clients will hang during connect and pools will time out.
    loop {
        let (first_len, first_buf) = read_startup_packet_or_refuse(&mut tcp).await?;
        let first_code = i32::from_be_bytes(first_buf[0..4].try_into().unwrap());

        // CancelRequest: length 16, the code, then the process id and secret
        // key a session was given in BackendKeyData. Answered with nothing —
        // PostgreSQL never says whether a cancel matched anything — and the
        // connection, which exists only to carry it, is closed.
        if first_len == 16 && first_code == CANCEL_REQUEST_CODE {
            let pid = i32::from_be_bytes(first_buf[4..8].try_into().unwrap());
            let key = i32::from_be_bytes(first_buf[8..12].try_into().unwrap());
            let stopped = crate::core::cancel::cancel_request(pid, key);
            log::debug!("pgwire CancelRequest for session {}: {}", pid, stopped);
            return Ok(());
        }

        if first_len == 8 {
            match first_code {
                SSL_REQUEST_CODE => {
                    match tls_mode {
                        TlsMode::Disable => {
                            // Deny SSL.
                            tcp.write_all(b"N").await?;
                            tcp.flush().await?;
                            // Client will follow up with a cleartext StartupMessage.
                            continue;
                        }
                        TlsMode::Prefer | TlsMode::Require => {
                            tcp.write_all(b"S").await?;
                            tcp.flush().await?;
                            let acceptor = acceptor.ok_or_else(|| {
                                io::Error::new(io::ErrorKind::Other, "TLS acceptor not configured")
                            })?;
                            let mut tls_stream = acceptor.accept(tcp).await.map_err(|e| {
                                io::Error::new(io::ErrorKind::ConnectionAborted, e.to_string())
                            })?;
                            // After TLS handshake, client sends a fresh StartupMessage.
                            let result =
                                run_session_after_ssl(catalog, &mut tls_stream, &auth, context)
                                    .await;

                            // Close the TLS session before dropping it. Dropping
                            // the stream closes the socket without sending
                            // `close_notify`, which is an unclean shutdown: the
                            // peer cannot tell a finished session from a
                            // truncated one, and sqlx logs a warning every time
                            // it returns a connection to the pool. Best-effort —
                            // the session is over either way, and a client that
                            // has already gone will fail this write. Bounded,
                            // because one that has stopped reading never would.
                            let shutdown = bounded_final_write(
                                SESSION_END_WRITE_BOUND,
                                tls_stream.shutdown(),
                            )
                            .await;
                            if let Err(e) = shutdown {
                                log::debug!("TLS shutdown: {}", e);
                            }
                            return result;
                        }
                    }
                }
                GSSENC_REQUEST_CODE => {
                    // We don't support GSS-encrypted connections; explicitly deny.
                    tcp.write_all(b"N").await?;
                    tcp.flush().await?;
                    continue;
                }
                CANCEL_REQUEST_CODE => {
                    // Best-effort: we don't support cancel, so just close.
                    log::debug!("pgwire received CancelRequest; closing");
                    return Ok(());
                }
                other => {
                    // Unknown 8-byte request (e.g., future extensions). Deny in a
                    // libpq-compatible way: most of these expect a single-byte response.
                    log::debug!(
                        "pgwire received unknown 8-byte startup request code {}; denying",
                        other
                    );
                    tcp.write_all(b"N").await?;
                    tcp.flush().await?;
                    continue;
                }
            }
        }

        // Not an 8-byte request: this should be a cleartext StartupMessage.
        if matches!(tls_mode, TlsMode::Require) {
            // Respond in cleartext (since the client spoke cleartext) and close.
            send_fatal_error_response(
                &mut tcp,
                "08004",
                "TLS is required (send SSLRequest / use sslmode=require)",
            )
            .await?;
            return Ok(());
        }

        // The StartupMessage was already read; its parameters carry the user
        // name, so they are parsed rather than discarded.
        let _ = first_len;
        let parameters = parse_startup_parameters(&first_buf);
        return finish_startup(catalog, &mut tcp, &parameters, &auth, context).await;
    }
}

async fn run_session_after_ssl<S>(
    catalog: Arc<DatabaseCatalog>,
    stream: &mut S,
    auth: &AuthConfig,
    context: &ConnectionContext,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // After SSL negotiation (either accepted or denied), the client sends StartupMessage.
    let (_, payload) = read_startup_packet_or_refuse(stream).await?;
    let parameters = parse_startup_parameters(&payload);
    finish_startup(catalog, stream, &parameters, auth, context).await
}

/// Authenticates, then reports the session parameters and enters the loop.
///
/// A failure is sent as a fatal error rather than a dropped connection, so the
/// client reports why it was refused instead of a broken pipe.
async fn finish_startup<S>(
    catalog: Arc<DatabaseCatalog>,
    stream: &mut S,
    parameters: &HashMap<String, String>,
    auth: &AuthConfig,
    context: &ConnectionContext,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // Over the cap. Refused only now, after the startup packet and any TLS
    // negotiation, because that is where a client is able to read an error:
    // one written in reply to an `SSLRequest` would be taken for the server's
    // answer to it. PostgreSQL refuses at the same point, with the same code.
    if context.too_many {
        log::debug!(
            "connection refused: max_connections ({}) reached",
            context.limits.max_connections
        );
        send_fatal_error_response(stream, "53300", "sorry, too many clients already").await?;
        return Ok(());
    }

    // The database is resolved before the password is checked, because roles
    // are per database: which catalogue answers for this user depends on which
    // database was asked for. A name that does not exist is therefore reported
    // to a client that has not authenticated yet — see
    // [`crate::core::database::catalog`] for why that trade is the right way
    // round.
    //
    // libpq always sends a name; a client that sends none gets the default
    // rather than a refusal.
    let requested = parameters
        .get("database")
        .map(String::as_str)
        .unwrap_or_else(|| catalog.default_name())
        .to_string();

    // Refused with a reason. Before this the exclusion was an accident — a
    // database being imported had no role catalogue yet, so every login failed
    // as "password authentication failed", which says nothing about why.
    if catalog.is_loading(&requested) {
        log::info!("connection refused: {:?} is being imported", requested);
        send_fatal_error_response(
            stream,
            "57P03",
            &format!(
                "database \"{}\" is being imported and is not ready for connections yet; \
                 it will accept them once the import finishes",
                requested
            ),
        )
        .await?;
        return Ok(());
    }

    let Some(session) = catalog.connect(&requested) else {
        log::info!("connection refused: no database named {:?}", requested);
        send_fatal_error_response(
            stream,
            "3D000",
            &format!("database \"{}\" does not exist", requested),
        )
        .await?;
        return Ok(());
    };
    // `session` is held for the life of this connection: `DROP DATABASE` counts
    // what is still attached before it removes anything.
    let database = session.database().clone();

    // From here on the session reads and writes through one buffer each way;
    // see `wire_out` for when output is sent. Nothing reads `stream` directly
    // after this, which is what lets the read buffer run ahead safely.
    let watch = PEER_WATCH.try_with(|w| w.clone()).ok().flatten();
    let mut conn = wire_out::WireStream::new(stream, watch);
    let result =
        authenticate_and_serve(catalog, &database, &mut conn, parameters, auth, context).await;
    // Whatever is still buffered was owed to the client. Best-effort: on an
    // error path the socket may already be gone, and bounded, because a
    // client that stopped reading would otherwise hold the session open here
    // after it has ended.
    let _ = bounded_final_write(SESSION_END_WRITE_BOUND, conn.flush()).await;
    result
}

async fn authenticate_and_serve<S>(
    catalog: Arc<DatabaseCatalog>,
    database: &Arc<Database>,
    stream: &mut S,
    parameters: &HashMap<String, String>,
    auth: &AuthConfig,
    context: &ConnectionContext,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let principal = match authenticate(database, stream, parameters, auth).await {
        Ok(principal) => {
            log::debug!(
                "authenticated {} via {}",
                principal.name,
                auth.method.name()
            );
            principal
        }
        // Out of time is not a wrong password, and saying `28P01` would send
        // whoever reads the log looking for a credential problem. PostgreSQL's
        // words and code for it. Best-effort: the client that let the
        // deadline pass may not be reading, and the deadline still bounds
        // this write.
        Err(e) if e.kind() == io::ErrorKind::TimedOut => {
            log::info!("authentication timed out");
            let _ = send_fatal_error_response(
                stream,
                "57014",
                "canceling authentication due to timeout",
            )
            .await;
            return Ok(());
        }
        Err(e) => {
            log::info!("authentication refused: {}", e);
            send_fatal_error_response(stream, "28P01", &e.to_string()).await?;
            return Ok(());
        }
    };

    // This session's cancel target: a process id unique among live sessions
    // and a secret key from the cryptographic generator, removed from the
    // registry when the session ends (the guard is dropped).
    let mut key = [0u8; 4];
    crate::auth::fill_random(&mut key);
    let registration = crate::core::cancel::register_session(key);
    send_startup_responses(stream, registration.pid, registration.key).await?;
    // The session is established; from here a quiet client is an idle
    // session, bounded (if at all) by the idle timeouts instead.
    context.handshake_finished.store(true, Ordering::Release);
    let startup_timeout = startup_statement_timeout(parameters);
    SESSION_CANCEL
        .scope(
            registration.session.clone(),
            run_message_loop(
                catalog,
                database,
                auth.clone(),
                principal,
                context.limits,
                stream,
                startup_timeout,
            ),
        )
        .await
}

#[cfg(test)]
mod peer_watch_tests {
    use super::client_has_gone;

    /// The assumption the whole disconnect check rests on: a duplicate of the
    /// accepted socket reports the peer's departure.
    #[test]
    fn a_duplicate_descriptor_sees_the_client_go() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let address = listener.local_addr().expect("addr");
        let client = std::net::TcpStream::connect(address).expect("connect");
        let (accepted, _) = listener.accept().expect("accept");

        let watch = accepted.try_clone().expect("dup");
        watch.set_nonblocking(true).expect("nonblocking");

        assert!(!client_has_gone(&watch), "the client is still connected");

        drop(client);
        // The close travels through the loopback; give it a moment.
        for _ in 0..200 {
            if client_has_gone(&watch) {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        panic!("a departed client was never noticed on the duplicate descriptor");
    }
}

/// `statement_timeout` as the StartupMessage asked for it.
///
/// Drivers set this at connection time rather than with a `SET` roundtrip —
/// sea-orm's `ConnectOptions::statement_timeout` becomes libpq's
/// `options=-c statement_timeout=…`, and its documentation says so — so an
/// implementation that only honoured `SET` would leave every sea-orm
/// application unprotected while looking correct from `psql`.
///
/// Both spellings are accepted: the parameter on its own, which PostgreSQL
/// allows for any GUC, and inside `options`.
fn startup_statement_timeout(parameters: &HashMap<String, String>) -> Option<u64> {
    if let Some(direct) = parameters.get("statement_timeout") {
        if let Some(ms) = parse_statement_timeout(direct) {
            return Some(ms);
        }
    }

    // `-c name=value`, `-cname=value` and `--name=value`, space separated. A
    // backslash escapes a space, which is how libpq carries a value containing
    // one; nothing here needs that, but consuming it keeps the split honest.
    let options = parameters.get("options")?;
    let mut fields: Vec<String> = Vec::new();
    let mut current = String::new();
    let mut escaped = false;
    for ch in options.chars() {
        if escaped {
            current.push(ch);
            escaped = false;
        } else if ch == '\\' {
            escaped = true;
        } else if ch.is_whitespace() {
            if !current.is_empty() {
                fields.push(std::mem::take(&mut current));
            }
        } else {
            current.push(ch);
        }
    }
    if !current.is_empty() {
        fields.push(current);
    }

    let mut index = 0usize;
    while index < fields.len() {
        let field = fields[index].as_str();
        // `-c` with the setting in the next field, or joined to it.
        let setting = if field == "-c" || field == "--" {
            index += 1;
            fields.get(index).map(String::as_str)
        } else if let Some(rest) = field.strip_prefix("--") {
            Some(rest)
        } else {
            field.strip_prefix("-c")
        };
        if let Some((name, value)) = setting.and_then(|s| s.split_once('=')) {
            if name.trim().eq_ignore_ascii_case("statement_timeout") {
                if let Some(ms) = parse_statement_timeout(value) {
                    return Some(ms);
                }
            }
        }
        index += 1;
    }
    None
}

/// Who a session is, as established at start-up.
///
/// `superuser` is a snapshot of the role at login, as PostgreSQL's is for the
/// life of a session. Under `trust` nobody proves anything, so the name is
/// whatever the client sent and `authenticated` is false — the native listener
/// draws the same line in [`crate::core::database::NativeContext`]: a gate
/// applied to an anonymous principal would refuse the statement that creates
/// the first account.
#[derive(Clone, Debug)]
struct Principal {
    name: String,
    superuser: bool,
    /// Whether the name was proven with a credential. False under `trust`.
    authenticated: bool,
}

impl Principal {
    /// Whether this session may run a server-administration statement.
    fn may_administer(&self) -> bool {
        !self.authenticated || self.superuser
    }
}

/// The verb of a statement that administers the server rather than the data,
/// or `None` for an ordinary statement.
///
/// These change who can log in, which databases exist, where files are
/// written, or how the process is configured — what PostgreSQL reserves for a
/// superuser (or `CREATEDB`, which this engine does not model separately).
/// Recognised by leading keywords so the check sits in one place ahead of
/// every dispatch path, rather than in each handler where one could be
/// forgotten. The `SHOW` forms are deliberately absent: reading the state is
/// not administering it.
fn privileged_statement(stmt: &str) -> Option<&'static str> {
    let upper = stmt.trim().to_ascii_uppercase();
    let mut words = upper.split_whitespace();
    let first = words.next()?;
    let second = words.next().unwrap_or("");
    match (first, second) {
        ("CREATE", "ROLE") | ("CREATE", "USER") => Some("CREATE ROLE"),
        ("ALTER", "ROLE") | ("ALTER", "USER") => Some("ALTER ROLE"),
        ("DROP", "ROLE") | ("DROP", "USER") => Some("DROP ROLE"),
        ("CREATE", "DATABASE") => Some("CREATE DATABASE"),
        ("DROP", "DATABASE") => Some("DROP DATABASE"),
        ("IMPORT", "DATABASE") => Some("IMPORT DATABASE"),
        ("SET", "CONFIG") => Some("SET CONFIG"),
        ("SET", "FILTER") | ("RESET", "FILTER") => Some("SET FILTER"),
        // Reading what ran is not administration, and `SHOW QUERIES` is
        // deliberately not here. Throwing it away for every other session is.
        ("RESET", "QUERY") => Some("RESET QUERY STATS"),
        ("BACKUP", _) => Some("BACKUP"),
        ("RESTORE", _) => Some("RESTORE"),
        ("STORAGE", _) => Some("STORAGE"),
        ("REPLICATION", _) => Some("REPLICATION"),
        // The key/value store's server-level commands: they change the
        // process's settings, write files, or empty every database at once.
        // Reading the settings (`KV CONFIG GET`) is not administering them.
        ("KV", "FLUSHALL") => Some("KV FLUSHALL"),
        ("KV", "SAVE") | ("KV", "BGSAVE") => Some("KV SAVE"),
        ("KV", "CONFIG") => match words.next().unwrap_or("") {
            "GET" => None,
            _ => Some("KV CONFIG"),
        },
        _ => None,
    }
}

async fn send_startup_responses<S>(stream: &mut S, pid: i32, key: i32) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    // AuthenticationOk was already sent by `authenticate`; sending it twice
    // would leave a client waiting for parameters that never came.
    send_parameter_status(stream, "server_version", "16.3").await?;
    send_parameter_status(stream, "server_version_num", "160003").await?;
    send_parameter_status(stream, "client_encoding", "UTF8").await?;
    send_parameter_status(stream, "DateStyle", "ISO, MDY").await?;
    send_parameter_status(stream, "standard_conforming_strings", "on").await?;
    send_parameter_status(stream, "TimeZone", "UTC").await?;
    send_backend_key_data(stream, pid, key).await?;
    send_ready_for_query(stream, b'I').await?;
    Ok(())
}

async fn run_message_loop<S>(
    catalog: Arc<DatabaseCatalog>,
    database: &Arc<Database>,
    auth: AuthConfig,
    principal: Principal,
    limits: ConnectionLimits,
    stream: &mut S,
    startup_statement_timeout: Option<u64>,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut state = SessionState::new(catalog, database.name.clone(), auth, principal, limits);
    // Applied before the first statement, so a driver that set it at connection
    // time is protected by it from the very first query rather than from
    // whenever it happens to issue a `SET`.
    state.statement_timeout_ms = startup_statement_timeout;
    // A connection that drops mid-transaction must not leave it running: it
    // would hold back reclamation forever and its versions would never resolve.
    // The loop returns from several places, so the cleanup is a guard rather
    // than a line at the end.
    //
    // A statement that panics unwinds out of the loop; caught here so the
    // cleanup below runs for it too, rather than the task dying with the
    // transaction left `InProgress` — which holds `oldest_active`, and with
    // it every snapshot horizon and every vacuum, for the life of the process.
    let result = {
        use futures::FutureExt;
        match std::panic::AssertUnwindSafe(run_message_loop_inner(database, &mut state, stream))
            .catch_unwind()
            .await
        {
            Ok(result) => result,
            Err(payload) => {
                // The panic's own message, so the next occurrence names its
                // cause: a client sees only the socket closing (sqlx reports
                // "UnexpectedEof"), and this line is all that is left.
                let message = payload
                    .downcast_ref::<&str>()
                    .map(|s| s.to_string())
                    .or_else(|| payload.downcast_ref::<String>().cloned())
                    .unwrap_or_else(|| "<non-string panic payload>".to_string());
                log::error!("a statement panicked; closing the connection: {}", message);
                Err(io::Error::other(format!("a statement panicked: {}", message)))
            }
        }
    };
    if let Some(t) = state.transaction.take() {
        log::info!(
            "connection closed with transaction {} open; rolling it back",
            t.id
        );
        // `Database::abort_transaction`, not the manager's `abort`: the latter
        // ends the transaction but leaves its row and key locks in the lock
        // table and its replication buffers in the capture map.
        let wrote = database.transactions.wrote_family(t.id);
        database.abort_transaction(t.id);
        if wrote {
            flush_outcomes(database);
        }
        t.owner.release();
    }
    result
}

/// The transaction a statement opened for itself (a data-modifying CTE
/// outside a block), aborted when dropped unless the statement closed it.
///
/// The statement returns early from several places — an unsupported CTE, a
/// failed derived table, a failed catalog view, a client write error — and
/// none of those used to end it, so it stayed `InProgress` forever and held
/// back `oldest_active` for the life of the process. Dropping also covers a
/// panic unwinding through.
struct OwnedStatementTx<'a> {
    database: &'a Arc<Database>,
    tx: Option<TxId>,
}

impl OwnedStatementTx<'_> {
    /// The statement closes the transaction itself from here on.
    fn disarm(&mut self) {
        self.tx = None;
    }
}

impl Drop for OwnedStatementTx<'_> {
    fn drop(&mut self) {
        if let Some(tx) = self.tx.take() {
            let wrote = self.database.transactions.wrote_family(tx);
            self.database.abort_transaction(tx);
            if wrote {
                flush_outcomes(self.database);
            }
        }
    }
}

async fn run_message_loop_inner<S>(
    database: &Arc<Database>,
    state: &mut SessionState,
    stream: &mut S,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // Whether the last thing sent was a `ReadyForQuery`, i.e. the session is
    // waiting for the client to start something rather than in the middle of
    // an extended-query batch. The idle timeouts run only then, as in
    // PostgreSQL, which arms them where it sends `ReadyForQuery`: a client
    // between `Parse` and `Sync` is mid-request, not idle. Startup ended with
    // one, so the session begins idle.
    let mut ready = true;

    loop {
        let mut tag = [0u8; 1];
        // The timer covers only this wait — the read of the first byte of the
        // next message. A statement that runs for an hour is never subject to
        // it, because nothing is being read while it runs. Output is already
        // flushed here (`ReadyForQuery` flushes), so the timer is not spent
        // waiting on a slow reader either.
        //
        // Cancelling the read is safe: a one-byte `read_exact` that has not
        // completed has consumed nothing, so a message arriving as the timer
        // fires is not torn.
        let idle = if ready { state.idle_limit() } else { None };
        let read = match idle {
            None => stream.read_exact(&mut tag).await,
            Some((limit, kind)) => {
                match tokio::time::timeout(limit, stream.read_exact(&mut tag)).await {
                    Ok(read) => read,
                    Err(_) => return terminate_idle_session(database, state, stream, kind).await,
                }
            }
        };
        match read {
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(e) => return Err(e),
        }
        // Whether handling this message ends with a `ReadyForQuery`: a simple
        // query and a `Sync` always do, the batch messages never do, and an
        // unknown message does unless it falls in a discard window, where it
        // is dropped without a reply.
        ready = match tag[0] {
            b'Q' | b'S' => true,
            b'P' | b'B' | b'D' | b'E' | b'C' | b'H' => false,
            _ => !state.error_in_batch,
        };

        let len = read_i32_be(stream).await?;
        // Checked against the type's limit before anything is allocated, as
        // PostgreSQL does: a length is only a claim, and allocating whatever
        // an authenticated client claims let one message pin up to 2 GB.
        let body = i64::from(len) - 4;
        if body < 0 || body as u64 > frontend_message_limit(tag[0]) {
            let _ = send_fatal_error_response(stream, "08P01", "invalid message length").await;
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid message length",
            ));
        }
        let payload = read_message_body(stream, body as usize).await?;

        // An error opens a discard window that only `Sync` closes.
        //
        // Everything between the two is read off the socket and dropped
        // without a reply, exactly as PostgreSQL does. Three tags still run:
        // `Sync` ends the window, `Terminate` ends the connection, and a
        // simple `Query` is a unit of its own — it carries no `Sync`, answers
        // itself with a `ReadyForQuery`, and so must never be discarded. This
        // test runs *before* the dispatch below, so exempting `Query` here is
        // what lets it reach the arm that clears the window; without the
        // exemption a simple query issued after any failed statement was
        // dropped and the client waited for a reply that would never come.
        if state.error_in_batch && !matches!(tag[0], b'S' | b'X' | b'Q') {
            continue;
        }

        match tag[0] {
            b'Q' => {
                // A simple query is a unit of its own, not part of a batch.
                state.error_in_batch = false;
                let query = read_cstring(&payload);
                handle_simple_query(database, state, stream, query).await?;
                // And it closes its own window: it has already answered with
                // `ReadyForQuery`. A failure inside it (`fail_statement` sets
                // the flag) otherwise left the window open, so the *next*
                // extended batch was discarded up to its Sync and the client
                // read a bare `ReadyForQuery` where it expected ParseComplete.
                state.error_in_batch = false;
            }
            // Extended query protocol (Parse/Bind/Describe/Execute/Sync/...)
            b'P' => {
                handle_parse(state, stream, &payload).await?;
            }
            b'B' => {
                handle_bind(state, stream, &payload).await?;
            }
            b'D' => {
                handle_describe(database, state, stream, &payload).await?;
            }
            b'E' => {
                handle_execute(database, state, stream, &payload).await?;
            }
            b'C' => {
                handle_close(state, stream, &payload).await?;
            }
            b'S' => {
                // Sync: indicates end of an extended-query batch. The status
                // byte reports whether a block is open, which is how a driver
                // knows its BEGIN took effect.
                //
                // This is also what closes the discard window opened by an
                // error earlier in the batch.
                state.error_in_batch = false;
                // Outside a block, Sync ends the implicit transaction and with
                // it every portal's suspended result, as in PostgreSQL. Inside
                // one, a suspended portal lives until the block ends.
                if state.transaction.is_none() {
                    for info in state.portals.values_mut() {
                        info.suspended = None;
                    }
                }
                let status = state.ready_status();
                send_ready_for_query(stream, status).await?;
            }
            b'H' => {
                // Flush: send everything buffered so far (see `wire_out`).
                stream.flush().await?;
            }
            b'X' => return Ok(()),
            _ => {
                send_error_response(
                    stream,
                    "0A000",
                    &format!("unsupported frontend message type '{}'", tag[0] as char),
                )
                .await?;
                let status = state.ready_status();
                send_ready_for_query(stream, status).await?;
            }
        }
    }
}

/// Which idle timeout is running while a session waits for its next message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum IdleKind {
    /// `idle_in_transaction_session_timeout`: a block is open, failed or not.
    InTransaction,
    /// `idle_session_timeout`: no block is open.
    Session,
}

/// Ends a session whose idle timeout expired, the way PostgreSQL does: the
/// open block, if any, is rolled back, then a `FATAL` says why, then the
/// connection closes.
///
/// The rollback comes first, so a client that has read the error can rely on
/// the transaction being gone — not still in progress for however long the
/// connection's teardown takes.
async fn terminate_idle_session<S>(
    database: &Arc<Database>,
    state: &mut SessionState,
    stream: &mut S,
    kind: IdleKind,
) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let (sqlstate, message) = match kind {
        IdleKind::InTransaction => (
            "25P03",
            "terminating connection due to idle-in-transaction timeout",
        ),
        IdleKind::Session => ("57P05", "terminating connection due to idle-session timeout"),
    };
    if let Some(t) = state.transaction.take() {
        log::info!("{}; rolling back transaction {}", message, t.id);
        state.end_block_settings(&t);
        // What `ROLLBACK` does, row locks and replication buffers included:
        // releasing what the idle block holds is the point of the timeout.
        let wrote = database.transactions.wrote_family(t.id);
        database.abort_transaction(t.id);
        if wrote {
            flush_outcomes(database);
        }
        t.owner.release();
    } else {
        log::info!("{}", message);
    }
    // Bounded: the client may be the reason the session went idle, and one
    // that has stopped reading would otherwise keep this session — and its
    // connection slot — for as long as it keeps the socket. An error here is
    // returned only to be logged; the connection closes either way.
    bounded_final_write(
        SESSION_END_WRITE_BOUND,
        send_fatal_error_response(stream, sqlstate, message),
    )
    .await
}

struct SessionState {
    /// Every database this process serves.
    ///
    /// Carried by the session rather than passed down, because `CREATE
    /// DATABASE` is recognised at the bottom of the statement path while the
    /// catalogue is resolved at the top of the connection.
    catalog: Arc<DatabaseCatalog>,
    /// The database this session connected to. `DROP DATABASE` needs it: a
    /// session may not drop the database it is sitting in.
    database_name: String,
    /// How this server authenticates, and what to seed a newly created
    /// database's role catalogue with — without which `CREATE DATABASE` would
    /// produce a database nobody could log into.
    auth: AuthConfig,
    /// Who this session is, and whether it proved it.
    principal: Principal,
    // statement name -> info
    statements: HashMap<String, StatementInfo>,
    // portal name -> info
    portals: HashMap<String, PortalInfo>,
    /// The explicit transaction this session is inside, if any.
    ///
    /// `None` means autocommit: each statement gets a transaction of its own.
    transaction: Option<SessionTransaction>,
    /// What `SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL` set, and
    /// what a `BEGIN` with no level of its own inherits.
    default_isolation: IsolationLevel,
    /// What `SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY` set, and
    /// what a `BEGIN` with no access mode of its own inherits.
    default_read_only: bool,
    /// `SET statement_timeout` for this session, in milliseconds.
    ///
    /// `None` means the session never set one; zero means explicitly disabled,
    /// which is PostgreSQL's default and its way of spelling "no limit". Kept
    /// as milliseconds because that is the unit `SHOW` reports when the value
    /// carries no unit of its own.
    statement_timeout_ms: Option<u64>,
    /// `SET synchronous_commit = off` for this session, if it set one.
    ///
    /// `None` means follow the server, which is what a session that never asks
    /// does — and what `RESET synchronous_commit` puts it back to. Per session
    /// because that is where the choice belongs: a bulk load can give up the
    /// last few milliseconds of durability without every other connection
    /// having to.
    synchronous_commit: Option<bool>,
    /// A transaction owned by the statement currently running, rather than by a
    /// block.
    ///
    /// One statement can need several writes to share a transaction without a
    /// `BEGIN` around them — a data-modifying CTE and the statement it feeds are
    /// one statement, and committing the CTE separately leaves its writes behind
    /// when the outer half fails.
    statement_tx: Option<TxId>,
    /// Whether the statement currently running has reported an error.
    ///
    /// A block records this as `failed`, but a statement outside one has nowhere
    /// to put it — and the CTE wrapper needs the answer to decide whether to
    /// commit the transaction it opened.
    statement_failed: bool,
    /// Whether an error has been reported in the extended-query batch now in
    /// flight, so the rest of it can be discarded.
    ///
    /// The protocol requires it: after an error the server "discards messages
    /// until a Sync message is reached, then issues ReadyForQuery and returns
    /// to normal message processing". Answering them instead is not a
    /// cosmetic deviation. The replies are ones the client never asked for, so
    /// it reads them as answers to its *next* query — decoding whatever
    /// arrives against the wrong column list. Where that lands on a 4-byte
    /// read the client does not error, it panics inside its buffer reader
    /// ("advance out of bounds: the len is 0 but advancing by 4"), which is
    /// what a pipelining client saw whenever a statement in a batch failed.
    ///
    /// Cleared by `Sync`, which ends the batch, and by a simple `Query`, which
    /// is a unit of its own.
    error_in_batch: bool,
    /// The message of the failure `statement_failed` records, for the
    /// boundary log line. The client received it; without it here, a runner
    /// that swallows statement errors (a migration tool reporting only its
    /// own transaction state) leaves the journal with a statement and no
    /// reason.
    statement_error: Option<String>,
    /// The server's connection limits: what `RESET` returns the idle timeouts
    /// to, and what `SHOW max_connections` reports.
    limits: ConnectionLimits,
    /// `idle_in_transaction_session_timeout` for this session. Zero disables
    /// it.
    idle_in_transaction_timeout: Duration,
    /// `idle_session_timeout` for this session. Zero disables it.
    idle_session_timeout: Duration,
}

impl Default for IsolationLevel {
    fn default() -> Self {
        IsolationLevel::ReadCommitted
    }
}

/// How much a transaction is insulated from concurrent ones.
///
/// The engine offers two behaviours, not four. `READ COMMITTED` takes a fresh
/// snapshot per statement; the stricter levels take one for the transaction.
/// `READ UNCOMMITTED` is accepted and behaves as `READ COMMITTED`, which is what
/// PostgreSQL does — a dirty read is permitted by the standard, not required,
/// and this engine cannot produce one.
///
/// `SERIALIZABLE` is accepted and behaves as `REPEATABLE READ`. That is
/// **weaker than the name promises**: write skew between two transactions
/// reading a set and each writing a row the other's predicate would have
/// matched is not detected, because detecting it needs predicate locking. The
/// first-updater-wins rule catches conflicts on the *same row* only.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum IsolationLevel {
    ReadCommitted,
    RepeatableRead,
}

/// The characteristics named on a `BEGIN`, `START TRANSACTION`, `SET
/// TRANSACTION` or `SET SESSION CHARACTERISTICS AS TRANSACTION`.
#[derive(Debug, Default, PartialEq)]
struct TransactionModes {
    level: Option<IsolationLevel>,
    read_only: Option<bool>,
}

impl TransactionModes {
    /// Parses a transaction-mode list: `ISOLATION LEVEL …`, `READ WRITE`,
    /// `READ ONLY`, `[NOT] DEFERRABLE`, separated by commas or spaces.
    ///
    /// Every mode is either applied or refused. The list used to be matched as
    /// a handful of exact strings, so `BEGIN READ ONLY` was a syntax error while
    /// `SET TRANSACTION READ ONLY` was acknowledged and ignored — the second
    /// being the worse of the two. `DEFERRABLE` is accepted and means nothing
    /// here, as in PostgreSQL outside `SERIALIZABLE READ ONLY`.
    fn parse(modes: &str) -> Result<Self, String> {
        let mut out = Self::default();
        let words: Vec<&str> = modes
            .split(|c: char| c.is_whitespace() || c == ',')
            .filter(|w| !w.is_empty())
            .collect();
        let mut i = 0;
        while i < words.len() {
            match words[i] {
                "isolation" => {
                    if words.get(i + 1) != Some(&"level") {
                        return Err("ISOLATION must be followed by LEVEL".to_string());
                    }
                    let (level, width) = match (words.get(i + 2), words.get(i + 3)) {
                        (Some(&"serializable"), _) => (IsolationLevel::RepeatableRead, 1),
                        (Some(&"repeatable"), Some(&"read")) => (IsolationLevel::RepeatableRead, 2),
                        (Some(&"read"), Some(&"committed"))
                        | (Some(&"read"), Some(&"uncommitted")) => {
                            (IsolationLevel::ReadCommitted, 2)
                        }
                        _ => return Err("unrecognised isolation level".to_string()),
                    };
                    out.level = Some(level);
                    i += 2 + width;
                }
                "read" => {
                    out.read_only = Some(match words.get(i + 1) {
                        Some(&"only") => true,
                        Some(&"write") => false,
                        _ => return Err("READ must be followed by ONLY or WRITE".to_string()),
                    });
                    i += 2;
                }
                "deferrable" => i += 1,
                "not" if words.get(i + 1) == Some(&"deferrable") => i += 2,
                other => return Err(format!("unrecognised transaction mode {:?}", other)),
            }
        }
        Ok(out)
    }
}

/// A savepoint established inside a block.
struct Savepoint {
    /// As the client named it. PostgreSQL allows a name to be reused: the newest
    /// one shadows the older, and rolling back to it leaves the older standing.
    name: String,
    /// The sub-transaction opened for it. Writes after this point carry this id.
    id: TxId,
    /// The session's `synchronous_commit` when this savepoint was established.
    ///
    /// `ROLLBACK TO` restores it, as PostgreSQL does: a `SET LOCAL` made after
    /// the savepoint is undone along with the writes it accompanied.
    synchronous_commit: Option<bool>,
}

/// An open `BEGIN` block.
struct SessionTransaction {
    id: TxId,
    /// What to put `SessionState::synchronous_commit` back to when this block
    /// ends, if a `SET LOCAL` inside it changed the value.
    ///
    /// `None` means no `SET LOCAL` happened and nothing is owed. `Some(prev)`
    /// holds the value from *before* the first one, so a second `SET LOCAL` in
    /// the same block does not overwrite the restore target with its
    /// predecessor's value.
    local_synchronous_commit: Option<Option<bool>>,
    /// `idle_in_transaction_session_timeout` from before the first `SET
    /// LOCAL` of it in this block, to be put back when it ends. Same shape and
    /// reason as `local_synchronous_commit`.
    ///
    /// One record per setting rather than one for the pair: a shared record
    /// would make a `SET LOCAL` of one timeout also restore the other, and so
    /// silently undo a plain session `SET` of it made later in the block.
    local_idle_in_transaction: Option<Duration>,
    /// The same for `idle_session_timeout`.
    local_idle_session: Option<Duration>,
    /// The level this block runs at.
    level: IsolationLevel,
    /// `BEGIN READ ONLY` / `SET TRANSACTION READ ONLY`: writes in this block
    /// are refused with `25006`. Enforced rather than accepted and ignored —
    /// a client that asked for a read-only block and got a read-write one
    /// reported as read-only has no way to find out.
    read_only: bool,
    /// Savepoints established in this block, oldest first.
    ///
    /// A stack, because `ROLLBACK TO` names one and discards everything after
    /// it, and `RELEASE` merges one and everything after it into what is below.
    savepoints: Vec<Savepoint>,
    /// Set once a statement in the block has failed.
    ///
    /// PostgreSQL rejects everything after that until the block is closed, and
    /// clients depend on it: the alternative is a transaction that commits
    /// having silently skipped the statement that did not work.
    failed: bool,
    /// The block's lock owner (its root), released when the block ends: by
    /// each ending path after its last durability step and before it replies
    /// (finding L4), and by the drop on any path that forgets to.
    owner: crate::core::mvcc::owner::OwnerRelease,
}

impl SessionState {
    fn new(
        catalog: Arc<DatabaseCatalog>,
        database_name: String,
        auth: AuthConfig,
        principal: Principal,
        limits: ConnectionLimits,
    ) -> Self {
        Self {
            limits,
            idle_in_transaction_timeout: limits.idle_in_transaction_session_timeout,
            idle_session_timeout: limits.idle_session_timeout,
            catalog,
            database_name,
            auth,
            principal,
            statements: HashMap::new(),
            portals: HashMap::new(),
            transaction: None,
            default_isolation: IsolationLevel::default(),
            default_read_only: false,
            statement_timeout_ms: None,
            synchronous_commit: None,
            statement_tx: None,
            statement_failed: false,
            error_in_batch: false,
            statement_error: None,
        }
    }

    /// Whether a commit in this session waits for its `fdatasync`.
    ///
    /// The session's own setting when it has made one, and the server's
    /// otherwise — read fresh each time, so `SET CONFIG wal.synchronous_commit`
    /// reaches connections that were already open.
    fn synchronous_commit(&self) -> bool {
        self.synchronous_commit
            .unwrap_or_else(crate::synchronous_commit)
    }

    /// Puts back what a `SET LOCAL` inside `t` changed.
    ///
    /// Called from every path that closes a block — both COMMIT arms and
    /// ROLLBACK — because a setting that outlived its block would be the
    /// opposite of what `LOCAL` asked for, and for `synchronous_commit` that
    /// means silently running later transactions at a durability nobody chose.
    fn end_block_settings(&mut self, t: &SessionTransaction) {
        if let Some(previous) = t.local_synchronous_commit {
            self.synchronous_commit = previous;
        }
        if let Some(previous) = t.local_idle_in_transaction {
            self.idle_in_transaction_timeout = previous;
        }
        if let Some(previous) = t.local_idle_session {
            self.idle_session_timeout = previous;
        }
    }

    /// The idle timeout to run while waiting for the next message, if one is
    /// set for the state the session is in.
    ///
    /// Inside a block — including one that has failed and is waiting for its
    /// `ROLLBACK`, which still pins its snapshot — that is
    /// `idle_in_transaction_session_timeout`; outside one,
    /// `idle_session_timeout`. PostgreSQL draws the same line.
    fn idle_limit(&self) -> Option<(Duration, IdleKind)> {
        let (limit, kind) = if self.transaction.is_some() {
            (self.idle_in_transaction_timeout, IdleKind::InTransaction)
        } else {
            (self.idle_session_timeout, IdleKind::Session)
        };
        (!limit.is_zero()).then_some((limit, kind))
    }

    /// The transaction status byte for `ReadyForQuery`.
    ///
    /// Clients read this to know whether they are in a block — sqlx and psql
    /// both track it — so reporting `I` throughout would make an open
    /// transaction invisible to the driver.
    fn ready_status(&self) -> u8 {
        match &self.transaction {
            None => b'I',
            Some(t) if t.failed => b'E',
            Some(_) => b'T',
        }
    }

    /// The transaction a statement should run in, or `None` for autocommit.
    ///
    /// Inside a savepoint that is the savepoint's sub-transaction, so its writes
    /// can be discarded on their own.
    fn current_tx(&self) -> Option<TxId> {
        // A statement-owned transaction wins: it exists only while that
        // statement runs, and everything it does belongs to it.
        self.statement_tx.or_else(|| {
            self.transaction
                .as_ref()
                .map(|t| t.savepoints.last().map(|sp| sp.id).unwrap_or(t.id))
        })
    }
}

/// Where a prepared `SELECT` stands on the way to skipping the text path.
/// See `core::rql::prepared` for why it takes two observed executions.
#[derive(Default)]
enum PreparedSelect {
    /// Not yet looked at, or a schema change since.
    #[default]
    Unknown,
    /// Watched once; the next execution with different values checks it.
    Candidate(Box<PreparedLearned>),
    /// Checked: executions with values of the same classes use the parse.
    Ready(Box<PreparedLearned>),
    /// The text path is the only path for this statement.
    Ineligible,
}

struct PreparedLearned {
    /// Schema generation it was learned at. Any schema change starts over.
    generation: u64,
    table_name: String,
    /// The parse, `WHERE` clause blanked; the template supplies it.
    query_row: crate::core::rql::lexer_query::QueryRows,
    shape: String,
    template: crate::core::rql::prepared::WhereTemplate,
    /// Each value's literal as first seen, so the check runs with different
    /// ones, and the class every value must keep.
    literals: Vec<String>,
    classes: Vec<u8>,
    fields: Option<Vec<(String, DbType)>>,
    scales: Option<Vec<u32>>,
    /// Watched executions that could not teach anything (repeated values, a
    /// literal seen twice). Past a few, the statement stops being watched.
    misses: u8,
    /// On a partitioned table, the full-key point shape's route derivation,
    /// learned with the parse and checked against `route_where` on both
    /// verifying executions (S16b). Reset with everything else on a schema
    /// change.
    route: Option<crate::core::rql::prepared::RoutePlan>,
}

/// A cached parse to run instead of the statement text, and the description
/// the text path sent its rows under.
struct PreparedRun {
    rows: Box<crate::core::rql::prepared::PreparedRows>,
    fields: Option<Vec<(String, DbType)>>,
    scales: Option<Vec<u32>>,
}

struct StatementInfo {
    /// Shared rather than owned: every `Execute` and `Describe` needs the text
    /// while also needing `state` mutably, and a deep copy per message was the
    /// price of that before.
    sql: StdArc<str>,
    param_oids: Vec<i32>,
    /// What the client was last told this statement returns, by a `Describe`
    /// of the statement or of one of its portals. See [`ToldShape`].
    ///
    /// A driver caches the description and does not ask again, so an
    /// `ALTER TABLE` between the describe and a later execute leaves it
    /// decoding rows against a shape that no longer exists — a dropped column
    /// keeps appearing, and every column after it is read from the wrong
    /// place. PostgreSQL refuses that with "cached plan must not change result
    /// type" and drivers re-prepare; this is what makes that check possible.
    ///
    /// Only a `Describe` sets it, never an `Execute`. An `Execute` that sends
    /// a fresh `RowDescription` with its rows has not changed what the client
    /// caches: sqlx applies that description to the one execution and keeps
    /// the old one for the next, so treating it as told left every execute
    /// after the first decoding against the old shape.
    described: Option<ToldShape>,
    /// Remembered `Describe` answers, at most one per kind. See
    /// [`DescribeCacheEntry`].
    describe_cache: Vec<DescribeCacheEntry>,
    /// The cached parse of a prepared `SELECT`, once two executions have
    /// shown the text path makes it. See [`PreparedSelect`].
    prepared: PreparedSelect,
    /// `value_free_shape` of `sql`, read once at Parse rather than on every
    /// describe.
    value_free: Option<ValueFreeShape>,
    /// The parameter types the last statement describe reported, where the
    /// client declared none (`param_infer`). Bind decodes values as these: a
    /// client encodes each one as the type it was told.
    inferred_oids: Option<Vec<i32>>,
}

/// The column list a `Describe` gave the client, and how it was computed.
#[derive(Clone, Debug)]
struct ToldShape {
    /// The schema generation the answer was computed at. While it is still
    /// current, the answer is too, and `Execute` checks nothing.
    generation: u64,
    /// `S` or `P`. `Execute` describes the statement again the same way to
    /// compare: a statement whose shape depends on its values would otherwise
    /// look changed when only the way of asking had.
    kind: u8,
    /// Interned (`statement_intern`): every session described with the same
    /// columns holds the same allocation.
    fields: statement_intern::Fields,
}

/// One `Describe` answer, and everything it was computed from.
///
/// A described type is not just a label: the same field list types the
/// payload, so a stale entry corrupts values rather than names. The key is
/// therefore everything that can change the answer:
///
/// * `kind` — a statement (`S`) is described with stand-ins for its
///   placeholders, a portal (`P`) with its bound values;
/// * `text` — exactly what was described: the statement for `S`, the statement
///   with the portal's values inlined for `P`. `SELECT $1 AS v FROM t` is an
///   integer column for one bound value and text for another, so the values
///   themselves are part of the key, not just their types;
/// * the declared parameter types, which decide how a value is inlined — not
///   stored, because the cache lives in the statement they belong to, and a
///   re-Parse keeps it only when they are identical;
/// * `generation` — the database's schema generation, read *before*
///   describing. Every schema change bumps it once it has completed, so an
///   entry computed against an older shape can never match a later lookup.
///
/// Only a description with columns is kept. "No columns" can stop being true
/// when a table is created, which is not a change to any table's shape.
#[derive(Clone, Debug)]
struct DescribeCacheEntry {
    kind: u8,
    generation: u64,
    /// `None`: the statement's own text (a statement describe, or a portal
    /// with nothing bound). Otherwise the text with the values inlined.
    text: Option<Box<str>>,
    fields: statement_intern::Fields,
}

impl DescribeCacheEntry {
    fn answers(&self, kind: u8, generation: u64, inlined: Option<&str>) -> bool {
        self.kind == kind && self.generation == generation && self.text.as_deref() == inlined
    }
}

/// Longest text a describe answer is remembered for. Past this the statement
/// is mostly values — a wide INSERT … RETURNING — and keeping a copy of it per
/// statement would cost more memory than the describe costs time.
const DESCRIBE_CACHE_MAX_TEXT: usize = 16 * 1024;

#[derive(Clone, Debug, Default)]
struct PortalInfo {
    stmt_name: String,
    result_formats: Vec<i16>,
    /// Values bound by the Bind message, positionally. Inlined into the
    /// statement text at Execute time because the engine parses SQL only.
    ///
    /// Shared, so `Execute` and `Describe` can hold them across their use of
    /// `state` without copying every value.
    params: StdArc<[BoundParam]>,
    /// The rest of a result an `Execute` with a row limit stopped part-way
    /// through. See [`SuspendedResult`].
    suspended: Option<Box<SuspendedResult>>,
}

/// What is left of a portal's result after an `Execute` with a nonzero
/// `max_rows` sent `PortalSuspended`: the messages the statement produced, as
/// the wire bytes they will be sent as, and how far into them the client has
/// been sent. The next `Execute` on the portal resumes from `at` instead of
/// running the statement again.
///
/// Held as encoded messages rather than rows so it is independent of how the
/// result was produced: whatever the statement wrote to the wire is what is
/// paged, in order, untouched — a fresh `RowDescription`, the `DataRow`s, the
/// `CommandComplete` at the end.
#[derive(Clone, Debug, Default)]
struct SuspendedResult {
    bytes: Vec<u8>,
    at: usize,
}

impl SuspendedResult {
    /// What is left once a paged result has been sent in full: the portal is
    /// exhausted, and executing it again answers `CommandComplete` with no rows
    /// — as PostgreSQL does — rather than running the statement afresh. A
    /// client paging until it gets an empty page relies on exactly that; it
    /// was otherwise handed the first page again, forever.
    fn exhausted(sent: &[u8]) -> Self {
        // The `CommandComplete` the statement sent, its count replaced by 0.
        let mut tag = None;
        let mut i = 0usize;
        while i + 5 <= sent.len() {
            let len = i32::from_be_bytes([sent[i + 1], sent[i + 2], sent[i + 3], sent[i + 4]]);
            let end = (i + 1 + len.max(4) as usize).min(sent.len());
            if sent[i] == b'C' {
                let body = &sent[i + 5..end];
                let text = String::from_utf8_lossy(body.split(|b| *b == 0).next().unwrap_or(&[]));
                let verb = text.split_whitespace().next().unwrap_or("SELECT").to_string();
                tag = Some(if verb == "INSERT" { "INSERT 0 0".to_string() } else { format!("{} 0", verb) });
            }
            i = end;
        }
        let tag = tag.unwrap_or_else(|| "SELECT 0".to_string());
        let mut bytes = Vec::with_capacity(tag.len() + 6);
        bytes.push(b'C');
        bytes.extend_from_slice(&((tag.len() + 5) as i32).to_be_bytes());
        bytes.extend_from_slice(tag.as_bytes());
        bytes.push(0);
        // `at` is left at 0 and never advanced past this one message: every
        // later Execute replays it.
        SuspendedResult { bytes, at: usize::MAX }
    }
}

/// A stream that keeps what is written to it instead of sending it, and reads
/// from the real stream. Lets `Execute` run a statement exactly as it always
/// does and page the result afterwards.
struct CaptureWrites<'a, S> {
    inner: &'a mut S,
    out: Vec<u8>,
}

impl<S: AsyncRead + Unpin> AsyncRead for CaptureWrites<'_, S> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::pin::Pin::new(&mut *self.inner).poll_read(cx, buf)
    }
}

impl<S: Unpin> AsyncWrite for CaptureWrites<'_, S> {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<io::Result<usize>> {
        self.out.extend_from_slice(buf);
        std::task::Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}

/// Sends the messages of `result` from `result.at` on, stopping before the
/// `DataRow` that would exceed `max_rows` (0: no limit). Returns `true` when it
/// stopped early — `PortalSuspended` has then been sent and `result.at` points
/// at the first row still owed.
async fn send_result_window<S>(
    stream: &mut S,
    result: &mut SuspendedResult,
    max_rows: i32,
) -> io::Result<bool>
where
    S: AsyncWrite + Unpin,
{
    let bytes = &result.bytes;
    let start = result.at;
    let mut i = start;
    let mut rows = 0i32;
    while i + 5 <= bytes.len() {
        let tag = bytes[i];
        let len = i32::from_be_bytes([bytes[i + 1], bytes[i + 2], bytes[i + 3], bytes[i + 4]]);
        if tag == b'D' {
            if max_rows > 0 && rows == max_rows {
                stream.write_all(&bytes[start..i]).await?;
                result.at = i;
                // PortalSuspended.
                send_message(stream, b's', &[]).await?;
                return Ok(true);
            }
            rows += 1;
        }
        i += 1 + (len.max(4) as usize);
    }
    stream.write_all(&bytes[start..]).await?;
    result.at = bytes.len();
    Ok(false)
}

async fn handle_parse<S>(state: &mut SessionState, stream: &mut S, payload: &[u8]) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    // Parse message payload:
    // cstring stmt_name, cstring query, i16 param_count, [i32 param_oid]...
    let (stmt_name, i) = read_cstring_owned(payload, 0)?;
    let (query, i) = read_cstring_owned(payload, i)?;
    let (param_count, mut i) = read_i16_be_from(payload, i)?;

    let mut param_oids = Vec::with_capacity(param_count.max(0) as usize);
    for _ in 0..param_count {
        let (oid, next) = read_i32_be_from(payload, i)?;
        i = next;
        // Postgres uses OID=0 to mean "unspecified"; keep as-is.
        param_oids.push(oid);
    }

    // Stored with escape and dollar-quoted strings already rewritten as the
    // standard strings they mean, so describing, inlining and executing it all
    // read one kind of literal; see `sql_scan`.
    let query = match crate::core::sql_scan::normalize_string_literals(&query) {
        Ok(std::borrow::Cow::Borrowed(_)) => query,
        Ok(std::borrow::Cow::Owned(text)) => text,
        Err(e) => return fail_statement(state, stream, "22025", &e).await,
    };

    // Re-preparing the identical text with the identical parameter types — an
    // unnamed statement re-sent with every command — keeps what was learned
    // describing it. Anything else starts afresh.
    let (describe_cache, prepared) = match state.statements.remove(&stmt_name) {
        Some(old) if *old.sql == *query && old.param_oids == param_oids => (old.describe_cache, old.prepared),
        _ => (Vec::new(), PreparedSelect::Unknown),
    };
    state.statements.insert(
        stmt_name,
        StatementInfo {
            described: None,
            // Shared with every other session that prepared the same text.
            sql: statement_intern::sql(&query),
            param_oids,
            value_free: value_free_shape(&query),
            describe_cache,
            inferred_oids: None,
            prepared,
        },
    );
    // ParseComplete
    send_message(stream, b'1', &[]).await
}

async fn handle_bind<S>(state: &mut SessionState, stream: &mut S, payload: &[u8]) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    // Bind message payload:
    // cstring portal, cstring stmt, i16 fmt_count, [i16 fmt]...
    // i16 param_count, [i32 len + bytes]..., i16 result_fmt_count, [i16 fmt]...
    let (portal, i) = read_cstring_owned(payload, 0)?;
    let (stmt, i) = read_cstring_owned(payload, i)?;
    let (fmt_count, mut i) = read_i16_be_from(payload, i)?;
    let mut param_formats = Vec::with_capacity(fmt_count.max(0) as usize);
    for _ in 0..fmt_count {
        let (fmt, next) = read_i16_be_from(payload, i)?;
        param_formats.push(fmt);
        i = next;
    }

    // Parse declared the parameter types; a portal that binds more values than
    // Parse described leaves the extras unspecified (OID 0).
    //
    // A name Parse never saw is an error, not an empty statement: binding it
    // used to answer BindComplete, and the Execute that followed ran whatever
    // the unnamed statement held.
    // The types the client was told at describe, where it declared none.
    let Some(param_oids) = state
        .statements
        .get(&stmt)
        .map(|s| s.inferred_oids.clone().unwrap_or_else(|| s.param_oids.clone()))
    else {
        return fail_statement(
            state,
            stream,
            "26000",
            &format!("prepared statement \"{}\" does not exist", stmt),
        )
        .await;
    };

    let (param_count, mut i) = read_i16_be_from(payload, i)?;
    let mut params = Vec::with_capacity(param_count.max(0) as usize);
    for idx in 0..param_count as usize {
        let (len, next) = read_i32_be_from(payload, i)?;
        i = next;
        let value = if len < 0 {
            None
        } else {
            let take = len as usize;
            if payload.len() < i + take {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid Bind parameter length",
                ));
            }
            let v = payload[i..i + take].to_vec();
            i += take;
            Some(v)
        };

        // Per protocol: zero formats means all-text, one format applies to
        // every parameter, otherwise formats are positional.
        let format = match param_formats.len() {
            0 => 0,
            1 => param_formats[0],
            _ => param_formats.get(idx).copied().unwrap_or(0),
        };

        params.push(BoundParam {
            value,
            format,
            oid: param_oids.get(idx).copied().unwrap_or(0),
        });
    }

    let (res_fmt_count, mut i) = read_i16_be_from(payload, i)?;
    let mut result_formats = Vec::with_capacity(res_fmt_count as usize);
    for _ in 0..res_fmt_count {
        let (fmt, next) = read_i16_be_from(payload, i)?;
        result_formats.push(fmt);
        i = next;
    }

    state.portals.insert(
        portal,
        PortalInfo {
            stmt_name: stmt,
            result_formats,
            params: params.into(),
            suspended: None,
        },
    );
    // BindComplete
    send_message(stream, b'2', &[]).await
}

async fn handle_describe<S>(
    database: &Arc<Database>,
    state: &mut SessionState,
    stream: &mut S,
    payload: &[u8],
) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    // Describe payload: byte ('S'|'P'), cstring name
    if payload.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid Describe payload",
        ));
    }
    let kind = payload[0];
    let (name, _) = read_cstring_owned(payload, 1)?;

    let (stmt_name, result_formats, bound) = match kind {
        b'S' => (name, vec![], StdArc::<[BoundParam]>::from(Vec::new())),
        b'P' => match state.portals.get(&name).map(|info| {
            (info.stmt_name.clone(), info.result_formats.clone(), info.params.clone())
        }) {
            Some(parts) => parts,
            // Not the unnamed portal: describing a name Bind never saw has to
            // say so, or the client caches the shape of some other statement.
            None => {
                return fail_statement(
                    state,
                    stream,
                    "34000",
                    &format!("portal \"{}\" does not exist", name),
                )
                .await;
            }
        },
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid Describe kind",
            ));
        }
    };

    let Some((sql, param_oids)) = state
        .statements
        .get(&stmt_name)
        .map(|s| (s.sql.clone(), s.param_oids.clone()))
    else {
        return fail_statement(
            state,
            stream,
            "26000",
            &format!("prepared statement \"{}\" does not exist", stmt_name),
        )
        .await;
    };

    // A portal's parameters are already bound, so the statement is described
    // with them inlined. Described raw, `SELECT a FROM t WHERE a = $1` does not
    // parse — the placeholder is not a literal — so `describe_query` gave up
    // and the portal was answered with `NoData`. `Execute` then sent the rows
    // anyway, and the client reported "D message without prior T".
    // Read before describing, never after: a schema change that lands while
    // the describe runs then leaves the entry under a generation no later
    // lookup will see. See `DescribeCacheEntry`.
    let generation = database
        .schema_generation
        .load(std::sync::atomic::Ordering::Acquire);
    let description = description_now(database, state, &stmt_name, kind, &sql, &bound, generation).await?;

    // A *statement* describe answers with ParameterDescription first. A
    // *portal* describe must not: its parameters are already bound, and the
    // protocol says the reply is RowDescription or NoData alone. The extra `t`
    // message left libpq reading the description as malformed, which surfaced
    // as "D message without prior T".
    if kind == b'S' {
        // One OID per parameter, each declared one as declared and the rest
        // inferred from where they stand. Reporting the declared list alone
        // told a client that declared nothing that there was nothing to bind.
        let reported = param_infer::infer(database, &sql, &param_oids);
        send_parameter_description(stream, &reported).await?;
        if let Some(info) = state.statements.get_mut(&stmt_name) {
            info.inferred_oids = (reported != param_oids).then_some(reported);
        }
    }

    // Try to produce RowDescription for SELECT; otherwise NoData.
    if let Some((fields, true)) = description {
        if let Some(info) = state.statements.get_mut(&stmt_name) {
            // Remembered so a later `Execute` can tell whether the shape the
            // client cached is still the shape the table has.
            info.described = Some(ToldShape {
                generation,
                kind,
                fields: fields.clone(),
            });
        }
        send_row_description(stream, &fields, &result_formats).await?;
        return Ok(());
    }

    send_message(stream, b'n', &[]).await
}

/// What a `Describe` of `kind` answers for `stmt_name` at schema generation
/// `generation`: a remembered answer when one fits, otherwise described afresh
/// and remembered. `bound` is the portal's values for `P`, empty for `S`.
///
/// `Execute` asks this too, after a schema change, so the shape it compares
/// with what the client was told is computed exactly the way the telling was.
async fn description_now(
    database: &Arc<Database>,
    state: &mut SessionState,
    stmt_name: &str,
    kind: u8,
    sql: &str,
    bound: &[BoundParam],
    generation: u64,
) -> io::Result<Option<(statement_intern::Fields, bool)>> {
    let lookup = |state: &SessionState, text: Option<&str>| {
        state.statements.get(stmt_name).and_then(|info| {
            info.describe_cache
                .iter()
                .find(|e| e.answers(kind, generation, text))
                .map(|e| e.fields.clone())
        })
    };
    // Decided once, at Parse: whether the values can change the answer.
    let value_free = state.statements.get(stmt_name).and_then(|s| s.value_free);
    let same_shape = value_free == Some(ValueFreeShape::SameShape);
    // A shape the values cannot change is answered before they are inlined:
    // the inlined text is needed only to describe it afresh.
    let early = if same_shape { lookup(state, None) } else { None };
    let inlined = if bound.is_empty() || early.is_some() {
        None
    } else {
        inline_params(sql, bound).ok()
    };
    let query: &str = inlined.as_deref().unwrap_or(sql);
    // What the answer is remembered under: the inlined text, unless the
    // values cannot change it.
    let cache_text = if same_shape { None } else { inlined.as_deref() };
    let cached = early.or_else(|| lookup(state, cache_text));

    let description = match cached {
        Some(fields) => Some((fields, true)),
        // Nothing to describe: the reply is `NoData` whatever the tables hold.
        None if value_free == Some(ValueFreeShape::NoRows) => None,
        None => describe_for_wire(database, query)
            .await?
            .map(|(fields, is_select)| (statement_intern::fields(&fields), is_select)),
    };

    if let Some((fields, true)) = &description {
        if let Some(info) = state.statements.get_mut(stmt_name) {
            if query.len() <= DESCRIBE_CACHE_MAX_TEXT
                && !info
                    .describe_cache
                    .iter()
                    .any(|e| e.answers(kind, generation, cache_text))
            {
                info.describe_cache.retain(|e| e.kind != kind);
                info.describe_cache.push(DescribeCacheEntry {
                    kind,
                    generation,
                    text: cache_text.map(Box::from),
                    fields: fields.clone(),
                });
            }
        }
    }
    Ok(description)
}

/// Describes `query` the way a `Describe` message must: with its unbound
/// placeholders replaced by a stand-in that parses.
///
/// Returns what `describe_query` does — `None` when nothing could be
/// described, `Some((fields, false))` for a statement that returns no rows.
async fn describe_for_wire(
    database: &Arc<Database>,
    query: &str,
) -> io::Result<Option<(Vec<(String, DbType)>, bool)>> {
    // A statement describe has no bound values to inline, so its placeholders
    // are replaced with a stand-in that parses. See `substitute_placeholders`.
    if has_placeholder(query) {
        for stand_in in ["NULL", "1", "'1'"] {
            let candidate = substitute_placeholders(query, stand_in);
            let described = describe_query(database, &candidate).await?;
            // The answer for the stand-in that worked is the answer: it used
            // to be thrown away and the same text described a second time.
            if described.as_ref().is_some_and(|(_, is_select)| *is_select) {
                return Ok(described);
            }
        }
        // A statement that returns no rows is *correctly* described as
        // returning none — an `UPDATE` or `DELETE` without `RETURNING` is not a
        // fault, and warning about those buried the real cases in noise.
        // A `WITH` prefix says nothing about whether rows come back: the CTEs
        // feed a statement that may equally be a `DELETE … USING`, which
        // returns none without a `RETURNING`. Judged on what the CTEs
        // introduce, so a data-modifying statement wrapped in a `WITH` is not
        // reported as an undescribable query once a second by a poller that is
        // working correctly. Noise here is not free — it is what buries the
        // cases that are real faults.
        let effective = match crate::core::rql::cte::split_with(query.trim()) {
            Ok(Some((_, outer))) => outer,
            _ => query.trim().to_string(),
        };
        let upper = effective.trim_start().to_ascii_uppercase();
        let expects_rows = upper.starts_with("SELECT")
            || upper.starts_with("WITH")
            || crate::core::rql::lexer_ir::returning_split(&effective).is_some();
        if expects_rows {
            // Named rather than silently answered with `NoData`: a driver that
            // caches a statement's column list caches *no columns*, and every
            // later read of it comes back with the right values under no
            // resolvable names. That reaches the application as a row whose
            // every field is null, naming nothing.
            log::warn!(
                "Could not describe the columns of a prepared statement; \
                 the client will see it as returning none: {}",
                query.trim()
            );
        }
    }
    describe_query(database, query).await
}

async fn handle_execute<S>(
    database: &Arc<Database>,
    state: &mut SessionState,
    stream: &mut S,
    payload: &[u8],
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // Execute payload: cstring portal, i32 max_rows
    let (portal, i) = read_cstring_owned(payload, 0)?;
    let (max_rows, _) = read_i32_be_from(payload, i)?;

    // A portal an earlier Execute suspended resumes where it stopped; the
    // statement is not run again. One it already ran to the end answers with
    // an empty `CommandComplete`.
    if let Some(mut pending) = state
        .portals
        .get_mut(&portal)
        .and_then(|info| info.suspended.take())
    {
        let next = if pending.at == usize::MAX {
            stream.write_all(&pending.bytes).await?;
            pending
        } else if send_result_window(stream, &mut pending, max_rows).await? {
            pending
        } else {
            Box::new(SuspendedResult::exhausted(&pending.bytes))
        };
        if let Some(info) = state.portals.get_mut(&portal) {
            info.suspended = Some(next);
        }
        return Ok(());
    }

    // An unknown portal is refused, never substituted: the fallback used to
    // be the unnamed statement with no parameters, so an Execute naming a
    // portal that had been closed ran a different statement and reported its
    // result under this one's name.
    let Some(info) = state.portals.get(&portal) else {
        return fail_statement(
            state,
            stream,
            "34000",
            &format!("portal \"{}\" does not exist", portal),
        )
        .await;
    };
    // The values and the text are shared handles, not copies: the shape check
    // below needs `state` mutably, so neither can stay borrowed from it, and
    // copying every bound value per Execute was what that used to cost.
    let (stmt_name, result_formats, params) = (
        info.stmt_name.clone(),
        info.result_formats.clone(),
        info.params.clone(),
    );

    let query: StdArc<str> = state
        .statements
        .get(&stmt_name)
        .map(|s| s.sql.clone())
        .unwrap_or_else(|| StdArc::from(""));
    let query: &str = &query;

    if query.trim().is_empty() {
        state.error_in_batch = true;
        send_error_response(stream, "26000", "prepared statement does not exist").await?;
        return Ok(());
    }

    let mut schema_changed = false;
    // A schema change since this statement was described makes the client's
    // cached column list wrong, and nothing on the wire would tell it.
    if let Some(told) = state
        .statements
        .get(&stmt_name)
        .and_then(|info| info.described.clone())
    {
        let now = database
            .schema_generation
            .load(std::sync::atomic::Ordering::Acquire);
        if now != told.generation {
            // Described the way the client was told, and remembered, so the
            // executes after this one find the answer rather than describe.
            let bound: &[BoundParam] = if told.kind == b'P' { &params } else { &[] };
            let current = description_now(database, state, &stmt_name, told.kind, query, bound, now)
                .await?
                .filter(|(_, is_select)| *is_select)
                .map(|(fields, _)| fields);
            if current.as_deref() != Some(&*told.fields) {
                // The rows go out with a fresh `RowDescription` in front of
                // them, which `Execute` does not normally send.
                //
                // The two alternatives are both worse. Saying nothing leaves
                // the client decoding against the shape it cached, so a dropped
                // column keeps appearing and every column after it is read from
                // the wrong place. Refusing with `0A000 cached plan must not
                // change result type` — what PostgreSQL raises — assumes the
                // client will drop the statement and prepare it again, and sqlx
                // does not: the *next* execute failed the same way, so an
                // `ALTER TABLE` broke every later use of that statement rather
                // than one.
                //
                // On every execute until the client describes again, not only
                // the first. sqlx applies a description that comes with the
                // rows to that execution alone and keeps the cached one, so
                // the second execute was decoded against the old shape: a
                // renamed column read under its old name, a dropped one's
                // neighbour under the dropped one's. `told` is therefore left
                // as it is. A client that never runs DDL still never sees a
                // second description, and one whose statement kept its shape
                // sees none.
                schema_changed = true;
            } else if let Some(info) = state.statements.get_mut(&stmt_name) {
                // What the client was told is still the shape, so it is good
                // at this generation too, and the executes after this one need
                // not ask. Only the generation moves: the columns stay exactly
                // the ones a `Describe` sent.
                if let Some(t) = info.described.as_mut() {
                    t.generation = now;
                }
            }
        }
    }

    // The engine only parses whole statements, so the bound values are folded
    // back into the SQL text as properly quoted literals.
    let query = match inline_params(query, &params) {
        Ok(q) => q,
        Err(e) => {
            state.error_in_batch = true;
            send_error_response(stream, "08P01", &e).await?;
            return Ok(());
        }
    };

    // No row limit — what nearly every client sends — runs straight to the
    // client as it always has. `COPY` is excluded too: it waits for the
    // client to answer what it sent, which a captured reply never reaches.
    let is_copy = query
        .trim_start()
        .get(..4)
        .is_some_and(|w| w.eq_ignore_ascii_case("COPY"));
    if max_rows <= 0 || is_copy {
        // A prepared `SELECT` may run from its cached parse, a fresh
        // description included: a client that never describes again gets one
        // on every execute, and would otherwise never run from it again.
        if !is_copy {
            return execute_watched(
                database,
                state,
                stream,
                &stmt_name,
                &query,
                &params,
                &result_formats,
                !schema_changed,
            )
            .await;
        }
        return execute_recorded(
            database,
            state,
            stream,
            &query,
            &result_formats,
            !schema_changed,
            None,
        )
        .await;
    }

    // A row limit: the statement runs exactly as above, into a buffer, and
    // the result is sent `max_rows` DataRows at a time. The engine builds the
    // whole result before sending any of it either way, so holding the encoded
    // rest costs what the rows already cost.
    let mut capture = CaptureWrites {
        inner: &mut *stream,
        out: Vec::new(),
    };
    execute_recorded(
        database,
        state,
        &mut capture,
        &query,
        &result_formats,
        !schema_changed,
        None,
    )
    .await?;
    let mut result = SuspendedResult {
        bytes: capture.out,
        at: 0,
    };
    let next = if send_result_window(stream, &mut result, max_rows).await? {
        result
    } else {
        SuspendedResult::exhausted(&result.bytes)
    };
    if let Some(info) = state.portals.get_mut(&portal) {
        info.suspended = Some(Box::new(next));
    }
    Ok(())
}

async fn handle_close<S>(state: &mut SessionState, stream: &mut S, payload: &[u8]) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    // Close payload: byte ('S'|'P'), cstring name
    if payload.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid Close payload",
        ));
    }
    let kind = payload[0];
    let (name, _) = read_cstring_owned(payload, 1)?;
    match kind {
        b'S' => {
            state.statements.remove(&name);
        }
        b'P' => {
            state.portals.remove(&name);
        }
        _ => {}
    }

    // CloseComplete
    send_message(stream, b'3', &[]).await
}

/// Replaces `$1`-style placeholders with a stand-in literal, for describing a
/// statement whose parameters are not bound yet.
///
/// A `Describe(statement)` arrives before any `Bind`, so the SQL still holds
/// its placeholders and does not parse — a placeholder is not a literal. The
/// describe then answered `NoData`, and a driver that caches the statement's
/// column list (sqlx does) cached *no columns*: every read came back with the
/// right values under no resolvable names, which sea-orm reports as a model
/// whose fields are all null.
///
/// What the parameters are does not affect which columns come back, so any
/// literal that parses will do. Several are tried because no single one fits
/// every position — `NULL` cannot be a `LIMIT`, a number cannot be compared
/// against a text column, and a string cannot be compared against a number.
///
/// Everything but a placeholder is copied as it stands, in runs. It used to
/// be pushed a byte at a time as `byte as char`, which re-encodes every byte
/// above 0x7F as a character of its own: `"tëst"` became `"tÃ«st"`, so a
/// statement naming a non-ASCII table or column described as nothing.
fn substitute_placeholders(sql: &str, stand_in: &str) -> String {
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut quote: Option<u8> = None;
    let mut copied = 0usize;
    let mut i = 0usize;
    while i < bytes.len() {
        let c = bytes[i];
        match quote {
            Some(q) => {
                if c == q {
                    quote = None;
                }
                i += 1;
            }
            None => {
                if c == b'\'' || c == b'"' {
                    quote = Some(c);
                    i += 1;
                } else if c == b'$' && bytes.get(i + 1).is_some_and(|d| d.is_ascii_digit()) {
                    let mut j = i + 1;
                    while j < bytes.len() && bytes[j].is_ascii_digit() {
                        j += 1;
                    }
                    // `i` and `j` sit on ASCII bytes, so both are boundaries.
                    out.push_str(&sql[copied..i]);
                    out.push_str(stand_in);
                    i = j;
                    copied = j;
                } else {
                    i += 1;
                }
            }
        }
    }
    out.push_str(&sql[copied..]);
    out
}

#[cfg(test)]
mod value_free_shape_tests {
    use super::{ValueFreeShape, value_free_shape};

    #[test]
    fn filter_only_placeholders_share_one_shape() {
        for sql in [
            "SELECT * FROM t WHERE id = $1",
            "select a, b from t join u on u.x = t.x where t.k = $1 and u.v > $2 order by a limit $3",
            "SELECT a FROM t WHERE s = 'no $1 here' AND id = $1",
            "SELECT a FROM t -- $1 in a comment\nWHERE id = $1",
            "SELECT a FROM t WHERE id = $1 FOR UPDATE",
        ] {
            assert_eq!(value_free_shape(sql), Some(ValueFreeShape::SameShape), "{sql}");
        }
    }

    #[test]
    fn a_placeholder_that_can_reach_a_column_is_keyed_by_value() {
        for sql in [
            "SELECT $1 AS v FROM t",
            "SELECT a, $1 FROM t WHERE id = $2",
            "SELECT $1",
            "SELECT x FROM (SELECT $1 AS x) s",
            "SELECT (SELECT max(x) FROM u WHERE y = $1) FROM t",
            "SELECT a FROM t WHERE id = $1 UNION SELECT $2",
            "SELECT a FROM t WHERE id IN ($1, $2)",
            "SELECT * FROM generate_series(1, $1)",
            "WITH c AS (SELECT 1) SELECT * FROM c WHERE x = $1",
            "SELECT a FROM t WHERE b = $tag$x$tag$ AND id = $1",
            "SELECT a FROM t WHERE s = E'\\' , (' AND id = $1",
        ] {
            assert_eq!(value_free_shape(sql), None, "{sql}");
        }
    }

    #[test]
    fn writes_without_returning_have_no_rows() {
        assert_eq!(value_free_shape("INSERT INTO t (k, v) VALUES ($1, $2) ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v"), Some(ValueFreeShape::NoRows));
        assert_eq!(value_free_shape("update t set v = $1 where id = $2"), Some(ValueFreeShape::NoRows));
        assert_eq!(value_free_shape("DELETE FROM t WHERE id = $1"), Some(ValueFreeShape::NoRows));
        assert_eq!(value_free_shape("INSERT INTO t (v) VALUES ($1) RETURNING id"), None);
        assert_eq!(value_free_shape("UPDATE t SET v = $1 RETURNING v, $2"), None);
    }
}

#[cfg(test)]
mod substitute_placeholder_tests {
    #[test]
    fn non_ascii_text_survives_the_stand_in() {
        assert_eq!(
            super::substitute_placeholders(r#"SELECT "naïve" FROM "tëst" WHERE "日本" = $1 AND x = '€$2'"#, "NULL"),
            r#"SELECT "naïve" FROM "tëst" WHERE "日本" = NULL AND x = '€$2'"#
        );
        assert_eq!(super::substitute_placeholders("ü=$12,$1", "'1'"), "ü='1','1'");
    }
}

/// Whether the statement still carries an unbound placeholder.
/// Why a statement's `Describe` answer cannot depend on its bound values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ValueFreeShape {
    /// `INSERT`, `UPDATE` or `DELETE` without `RETURNING`: no columns, ever.
    NoRows,
    /// A plain `SELECT` whose placeholders all stand at the top level after
    /// its `FROM`: in the filter, join conditions, ordering or limit, never in
    /// the select list or a derived table. Its columns are the same for every
    /// binding.
    SameShape,
}

/// Whether one `Describe` answer serves every binding of `sql`, and why.
///
/// The portal describe is keyed by the text with its values inlined, since
/// `SELECT $1 AS v` is an integer column for one value and text for another.
/// A client that describes the portal on every execute, as libpq's
/// `PQsendQueryPrepared` does, then described again for every new value.
/// `None` keeps that keying; anything this cannot read, including any
/// placeholder inside parentheses, is `None`.
fn value_free_shape(sql: &str) -> Option<ValueFreeShape> {
    let bytes = sql.as_bytes();
    // An `E'…'` string escapes its quote with a backslash, which the scan
    // below would read as the string's end.
    if bytes.contains(&b'\\') {
        return None;
    }
    let mut depth = 0i32;
    let mut i = 0usize;
    // Top-level words in order, and whether every placeholder so far stood
    // at the top level after a top-level FROM.
    let mut first: Option<&str> = None;
    let mut seen_from = false;
    let mut placeholders_ok = true;
    let mut set_op = false;
    let mut returning = false;
    while i < bytes.len() {
        let c = bytes[i];
        match c {
            b'\'' | b'"' => {
                i += 1;
                while i < bytes.len() && bytes[i] != c {
                    i += 1;
                }
                i += 1;
            }
            b'-' if bytes.get(i + 1) == Some(&b'-') => {
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                i += 2;
                while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                    i += 1;
                }
                i += 2;
            }
            // A dollar-quoted body is not read; nor is anything after one.
            b'$' if bytes.get(i + 1).is_some_and(|d| !d.is_ascii_digit()) => return None,
            b'$' => {
                if depth != 0 || !seen_from {
                    placeholders_ok = false;
                }
                i += 1;
                while i < bytes.len() && bytes[i].is_ascii_digit() {
                    i += 1;
                }
            }
            b'(' => {
                depth += 1;
                i += 1;
            }
            b')' => {
                depth -= 1;
                i += 1;
            }
            _ if c.is_ascii_alphabetic() || c == b'_' => {
                let start = i;
                while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_' || bytes[i] == b'$') {
                    i += 1;
                }
                let word = &sql[start..i];
                if word.eq_ignore_ascii_case("RETURNING") {
                    returning = true;
                }
                if depth == 0 {
                    if first.is_none() {
                        first = Some(word);
                    } else if word.eq_ignore_ascii_case("FROM") {
                        seen_from = true;
                    } else if ["UNION", "INTERSECT", "EXCEPT"].iter().any(|k| word.eq_ignore_ascii_case(k)) {
                        set_op = true;
                    }
                }
            }
            _ => i += 1,
        }
    }
    let first = first?;
    if ["INSERT", "UPDATE", "DELETE"].iter().any(|k| first.eq_ignore_ascii_case(k)) {
        return (!returning).then_some(ValueFreeShape::NoRows);
    }
    (first.eq_ignore_ascii_case("SELECT") && seen_from && placeholders_ok && !set_op && !returning && depth == 0)
        .then_some(ValueFreeShape::SameShape)
}

fn has_placeholder(sql: &str) -> bool {
    let bytes = sql.as_bytes();
    let mut quote: Option<u8> = None;
    for (i, c) in bytes.iter().enumerate() {
        match quote {
            Some(q) => {
                if *c == q {
                    quote = None;
                }
            }
            None => match c {
                b'\'' | b'"' => quote = Some(*c),
                b'$' if bytes.get(i + 1).is_some_and(|d| d.is_ascii_digit()) => return true,
                _ => {}
            },
        }
    }
    false
}

pub(crate) async fn describe_query(
    database: &Arc<Database>,
    query: &str,
) -> io::Result<Option<(Vec<(String, DbType)>, bool)>> {
    let stmt = query.trim();
    if stmt.is_empty() {
        return Ok(None);
    }
    // A `WITH` prefix is stripped and the statement it introduces is described
    // instead — but the outer query usually reads a CTE *as its source
    // relation*, and that name is not in `database.tables`. Describing the
    // outer statement alone therefore answered `None` for every ordinary
    // `WITH … SELECT … FROM <cte>`: the client got `NoData`, `Execute` sent
    // the rows anyway, and every field came back unreadable — the right row
    // count with no columns, which is the quietest way to be wrong.
    //
    // So each CTE body is described first and its columns become a relation
    // the outer statement can be described against, exactly as the derived
    // table below already does. Describing is not materialising: no CTE is
    // built, and each body is only resolved for its column types. They are
    // accumulated in order so a later CTE may read an earlier one, which is
    // the order `WITH a AS (…), b AS (… FROM a)` is written in.
    if let Ok(Some((defs, outer))) = crate::core::rql::cte::split_with(stmt) {
        let mut relations: Vec<(String, ScratchFields)> = Vec::new();
        for def in &defs {
            match Box::pin(describe_resolved(database, &def.body, &relations)).await? {
                // A body that is itself a `UNION` is materialised with its rows
                // widened, so its relation has the wider column too.
                Some((columns, _)) => {
                    let columns = widen_union_fields(database, &def.body, &relations, columns).await?;
                    relations.push((def.name.clone(), scratch_fields(&columns)));
                }
                // A body of named constants has no relation to resolve; it is
                // described by the same reading that materialises it.
                None => match crate::core::rql::cte::constant_body(&def.body) {
                    Some(constants) => {
                        let columns: Vec<(String, DbType)> =
                            constants.into_iter().map(|c| (c.label, c.db_type)).collect();
                        relations.push((def.name.clone(), scratch_fields(&columns)));
                    }
                    // A body this cannot resolve leaves the rest unresolvable
                    // too. The outer statement is still tried below: it may not
                    // read the CTE at all, which is the shape that always worked.
                    None => break,
                },
            }
        }
        if let Some((fields, true)) = Box::pin(describe_resolved(database, &outer, &relations)).await? {
            return Ok(Some((widen_union_fields(database, &outer, &relations, fields).await?, true)));
        }
    }

    // A derived table is *not* materialised here, deliberately. Doing that
    // describes by executing: the inner query runs with the describe's stand-in
    // literals substituted, so `WHERE domain_id = NULL` and `LIMIT NULL`
    // reached the planner and filled the log with errors for statements that
    // were about to run correctly. Describe must not have side effects.
    //
    // The shape that matters is an aggregate over a derived table — how every
    // paginated count is written — and its type does not depend on the inner
    // relation at all.
    if let Some(fields) = describe_aggregate_over_derived(stmt) {
        return Ok(Some((fields, true)));
    }

    // Anything else over a derived table: resolve the *inner* query's columns
    // and describe the outer statement against them.
    //
    // This is the general form of the special case above, and it is what makes
    // a paginated rollup describable — `SELECT COALESCE(AVG(q.t)::bigint, 0) AS
    // avg_bytes FROM ( … GROUP BY … ) q` computes the right answer and handed
    // the client *no columns*, so every `try_get` failed. Only `COUNT` was
    // answered before, because only its type is fixed regardless of the inner
    // relation.
    //
    // Nothing is materialised: the alias's schema comes from describing the
    // inner query, which is the same question asked one level down, and the
    // outer statement is rewritten to name the alias as an ordinary relation.
    if let Some((start, end, inner, alias)) = crate::core::rql::cte::next_derived_table(stmt) {
        if !alias.is_empty() {
            if let Some((columns, _)) = Box::pin(describe_resolved(database, &inner, &[])).await? {
                let columns = widen_union_fields(database, &inner, &[], columns).await?;
                let fields = scratch_fields(&columns);
                let rewritten = format!("{}{} {}{}", &stmt[..start], alias, alias, &stmt[end..]);
                let relations = [(alias.clone(), fields)];
                if let Some((fields, is_select)) =
                    Box::pin(describe_resolved(database, &rewritten, &relations)).await?
                {
                    return Ok(Some((widen_union_fields(database, stmt, &[], fields).await?, is_select)));
                }
            }
        }
    }

    // A parenthesised first branch of a `UNION` is described on its own, as
    // the simple query path plans it.
    let first_branch = match crate::core::rql::join::split_union(stmt) {
        Some(Ok(parts)) if stmt.starts_with('(') => parts.branches.into_iter().next(),
        _ => None,
    };
    match describe_resolved(database, first_branch.as_deref().unwrap_or(stmt), &[]).await? {
        Some((fields, is_select)) => Ok(Some((widen_union_fields(database, stmt, &[], fields).await?, is_select))),
        None => Ok(None),
    }
}

/// A `UNION` is described by its first branch, but a column that is `int`
/// there and `bigint` in another is a `bigint`, and the executor sends its
/// values as one. Described as the first branch's `int`, the wider values did
/// not fit the description and went out as NULL. Anything but a `UNION` is
/// returned as it is.
///
/// Every path of `describe_query` that can reach the executor's `UNION` comes
/// through here, with the relations it resolved. The executor runs the
/// statement after its CTEs and derived tables are materialised into real
/// relations, and widens by their columns; a describe that did not see them
/// said `int` for a column whose every value, the `int` rows too, was sent as
/// a `bigint`. So the derived tables still inline in `sql` are resolved here
/// as the executor materialises them, each under a name of its own: two
/// branches may each read a derived table called `q`.
///
/// The branches are taken from the text whether or not its trailing clauses
/// read: a prepared `LIMIT $1` is described with a stand-in that the `UNION`
/// splitter refuses, and executed with a number it accepts.
async fn widen_union_fields(
    database: &Arc<Database>,
    sql: &str,
    relations: &[(String, ScratchFields)],
    fields: Vec<(String, DbType)>,
) -> io::Result<Vec<(String, DbType)>> {
    if crate::core::rql::join::union_branches(sql).is_none() {
        return Ok(fields);
    }
    let mut text = sql.to_string();
    let mut relations = relations.to_vec();
    // Bounded as the executor's materialisation is: each pass replaces one
    // derived table with a name.
    for index in 0..16usize {
        let Some((start, end, inner, alias)) = crate::core::rql::cte::next_derived_table(&text) else {
            break;
        };
        // One that cannot be resolved leaves its branch's columns unknown,
        // which widens nothing there.
        let Some((columns, _)) = Box::pin(describe_resolved(database, &inner, &relations)).await? else {
            break;
        };
        let columns = Box::pin(widen_union_fields(database, &inner, &relations, columns)).await?;
        let name = format!("__described_derived_{}", index);
        text = format!("{}{} {}{}", &text[..start], name, alias, &text[end..]);
        relations.push((name, scratch_fields(&columns)));
    }
    let Some(branches) = crate::core::rql::join::union_branches(&text) else {
        return Ok(fields);
    };
    Ok(widen_fields_by(
        crate::core::rql::set_ops::widened_branch_integers(&branches, database, &relations),
        fields,
    ))
}

/// Sets each field that [`crate::core::rql::set_ops::widened_integers`]
/// widens to its wider type.
fn widen_fields_by(
    widened: Vec<Option<DbType>>,
    mut fields: Vec<(String, DbType)>,
) -> Vec<(String, DbType)> {
    for (field, wide) in fields.iter_mut().zip(widened) {
        if let Some(wide) = wide {
            field.1 = wide;
        }
    }
    fields
}

/// The columns of a relation that exists only while one statement is described.
type ScratchFields = crate::core::rql::cte::ScratchFields;

/// Turns a described column list into a schema the planner can resolve names
/// against.
///
/// Offsets are laid out as a real table's would be. Nothing is registered
/// anywhere: this exists for the length of one describe.
fn scratch_fields(columns: &[(String, DbType)]) -> ScratchFields {
    let mut fields: ScratchFields = smallvec::SmallVec::new();
    let mut offset = 0u64;
    for (order, (name, ty)) in columns.iter().enumerate() {
        let size = ty.get_size();
        fields.push(crate::core::row::schema::SchemaField::new(
            name.to_string(),
            ty.clone(),
            size,
            offset,
            order as u64,
            false,
        ));
        offset += size as u64;
    }
    fields
}

/// Describes `SELECT <aggregates> FROM ( … ) alias` without running the inner
/// query.
///
/// Only aggregates whose result type is fixed are answered — `COUNT` is one,
/// and it is the case that matters, since a paginated count is written this way
/// by every ORM. Anything whose type depends on the inner relation returns
/// `None` and is described the ordinary way.
fn describe_aggregate_over_derived(stmt: &str) -> Option<Vec<(String, DbType)>> {
    let upper = stmt.trim_start().to_ascii_uppercase();
    if !upper.starts_with("SELECT") {
        return None;
    }
    let from_at = crate::core::rql::join::find_keyword(stmt, "FROM", 0)?;
    if !stmt[from_at + "FROM".len()..].trim_start().starts_with('(') {
        return None;
    }

    let select_list = stmt["SELECT".len()..from_at].trim();
    let mut fields: Vec<(String, DbType)> = Vec::new();
    for item in crate::core::rql::join::split_top_level_commas(select_list) {
        let item = item.trim();
        let (expr, alias) = crate::core::rql::lexer_query::split_alias(item);
        let open = expr.find('(')?;
        if !expr[..open].trim().eq_ignore_ascii_case("COUNT") {
            return None;
        }
        let label = alias.unwrap_or_else(|| "count".to_string());
        let label: String = label;
        fields.push((label, DbType::I64));
    }
    if fields.is_empty() {
        None
    } else {
        Some(fields)
    }
}

/// Describes a statement whose CTEs and derived tables are already relations.
/// The same statement with everything from `WHERE` onwards removed.
///
/// Only for describing: the output columns of a `SELECT` are decided by its
/// select list and its relation, never by its predicate, so a predicate the
/// describe-time parser cannot read is not a reason to answer "no columns".
/// Returns `None` when there is no `WHERE` to drop, in which case the failure
/// was something else and the caller should not pretend otherwise.
fn describe_without_predicate(sql: &str) -> Option<String> {
    let at = crate::core::rql::join::find_keyword(sql, "WHERE", 0)?;
    Some(sql[..at].trim_end().to_string())
}

async fn describe_resolved(
    database: &Arc<Database>,
    query: &str,
    // Relations that exist only for this statement — a derived table or a CTE
    // whose columns were resolved from its own query. They are not in
    // `database.tables` and must not be put there: describing must not
    // materialise anything.
    derived: &[(String, ScratchFields)],
) -> io::Result<Option<(Vec<(String, DbType)>, bool)>> {
    let stmt = query.trim();
    if stmt.is_empty() {
        return Ok(None);
    }

    // Answered above the engine, so `recognize_query_purpose` never sees it and
    // the arms below could not describe it.
    // Answered above the engine too, and with rows: without an arm here the
    // extended protocol replies `NoData` and then sends `DataRow`s anyway,
    // which every driver reads as a protocol violation.
    #[cfg(feature = "pg_import")]
    if crate::core::migrate::sql::is_import_statement(stmt) {
        return Ok(Some((vec![("import".to_string(), DbType::STRING)], true)));
    }

    // Answered by `handle_builtin_query`, above the engine. Without an arm here
    // `Describe` replied `NoData` and `Execute` sent rows anyway — the same
    // protocol violation the import and `SHOW DATABASES` arms exist to avoid.
    {
        let lower = stmt.to_ascii_lowercase();
        let builtin = !(lower.starts_with("set ")
            || lower.starts_with("show ")
            || lower.starts_with("reset "))
            || recognize_query_purpose(stmt).is_none() && parse_database_statement(stmt).is_none();
        if builtin {
            if lower == "select 1" || lower == "select 1::int" {
                return Ok(Some((vec![("?column?".to_string(), DbType::I32)], true)));
            }
            if lower == "select version()" {
                return Ok(Some((vec![("version".to_string(), DbType::STRING)], true)));
            }
            if let Some(key) = lower.strip_prefix("show ") {
                let label: String = key.trim().to_string();
                return Ok(Some((vec![(label, DbType::STRING)], true)));
            }
        }
    }

    if let Some(Ok(DatabaseStatement::List)) = parse_database_statement(stmt) {
        return Ok(Some((
            [
                ("database_name", DbType::STRING),
                ("location", DbType::STRING),
                ("tables", DbType::I64),
                ("sessions", DbType::I64),
                ("is_default", DbType::BOOL),
                ("is_current", DbType::BOOL),
            ]
            .map(|(name, ty)| (name.to_string(), ty))
            .to_vec(),
            true,
        )));
    }

    let purpose = match recognize_query_purpose(stmt) {
        Some(p) => p,
        None => return Ok(None),
    };

    // A catalogue view does not exist until the statement runs, so the arms
    // below cannot resolve it and answered `NoData` while `Execute` went on to
    // send rows — the protocol violation described above, and the reason a
    // client could read `information_schema.columns` values but not their
    // names.
    //
    // Described from the column list the view is built with rather than by
    // materialising it, because `Describe` must not have side effects. The
    // select list is then resolved by the *same* function the execute path
    // uses, over a schema equivalent to the one it will resolve against, so
    // the description cannot disagree with the rows that follow it.
    if let Some(view) = crate::core::rql::catalog_views::view_of_statement(stmt) {
        if let QueryPurpose::QueryRows(_) = &purpose {
            if let Some(schema) = crate::core::rql::catalog_views::synthetic_schema(view) {
                if let Ok(parsed) = row_fetch_from_select_query(&purpose, &schema) {
                    // An aggregate's columns are its select items, named and
                    // typed by the plan — not the fields it happens to read.
                    // Reading the fetch instead reported `SELECT COUNT(*) AS n
                    // FROM pg_class c` as a column called `oid`: the view's
                    // first field, because that is what COUNT was scanning. The
                    // count was right and its name was another column's.
                    if let Some(plan) = &parsed.aggregate {
                        let fields: Vec<(String, DbType)> = plan
                            .items
                            .iter()
                            .map(|item| {
                                let label: String = item.label().to_string();
                                (label, item.result_type())
                            })
                            .collect();
                        if !fields.is_empty() {
                            return Ok(Some((fields, true)));
                        }
                    }

                    let fields: Vec<(String, DbType)> = parsed
                        .requested_row_fetch
                        .columns_fetching_data
                        .iter()
                        .take(parsed.visible_columns)
                        .filter_map(|c| {
                            field_of(&schema, c.schema_id).map(|f| {
                                let label: String = f.name.clone();
                                (label, f.db_type.clone())
                            })
                        })
                        .collect();
                    if !fields.is_empty() {
                        return Ok(Some((fields, true)));
                    }
                }
            }
        }
    }

    match &purpose {
        // Statements whose rows belong to no relation. Without an arm here the
        // extended protocol answers `Describe` with `NoData` and then sends
        // `DataRow`s anyway, which every driver reads as a protocol violation —
        // so `SHOW ...` worked under `psql`'s simple query path and failed
        // under any ORM.
        QueryPurpose::Introspect(statement) => {
            return Ok(
                crate::core::introspect::result_fields(statement).map(|fields| {
                    (
                        fields
                            .into_iter()
                            .map(|(name, ty)| {
                                let label: String = name;
                                (label, ty)
                            })
                            .collect(),
                        true,
                    )
                }),
            );
        }
        #[cfg(feature = "kv_cache")]
        QueryPurpose::KvCache(sql) => {
            return Ok(Some((crate::core::kv_cache::sql::result_fields(sql), true)));
        }
        QueryPurpose::Replication(sql) => {
            return Ok(Some((
                crate::replication::sql::structured_fields(sql)
                    .unwrap_or_else(crate::replication::sql::result_fields)
                    .into_iter()
                    .map(|(name, ty)| {
                        let label: String = name;
                        (label, ty)
                    })
                    .collect(),
                true,
            )));
        }
        #[cfg(feature = "storage_pools")]
        QueryPurpose::Storage(sql) => {
            return Ok(Some((
                crate::core::storage::sql::structured_fields(sql)
                    .unwrap_or_else(crate::core::storage::sql::result_fields)
                    .into_iter()
                    .map(|(name, ty)| {
                        let label: String = name;
                        (label, ty)
                    })
                    .collect(),
                true,
            )));
        }
        // `SHOW BACKUPS` and `RESTORE` build their columns from the statement,
        // exactly as the three families above do. `BACKUP DATABASE`/`WAL`/
        // `PRUNE` report a count rather than rows, and `result_fields` answers
        // `None` for them — which falls through to `NoData`, which is what they
        // should get.
        #[cfg(feature = "s3_backup")]
        QueryPurpose::Backup(statement) => {
            return Ok(
                crate::core::rql::backup_exec::result_fields(statement).map(|fields| {
                    (
                        fields
                            .into_iter()
                            .map(|(name, ty)| {
                                let label: String = name;
                                (label, ty)
                            })
                            .collect(),
                        true,
                    )
                }),
            );
        }
        QueryPurpose::QueryRows(qr) => {
            if crate::core::rql::join::statement_has_join(&qr.query) {
                // A catalogue view has no entry in `database.tables` — it is
                // materialised into a temp table at *execute* time, which
                // describe must not do. Its column list is fixed and known, so
                // the join is planned against that instead. Without this every
                // join over `pg_class`/`pg_inherits`/`pg_namespace`/`pg_index`
                // described as returning no columns, and the client then read
                // the rows that followed under no resolvable names.
                let schema_of = |name: &str| {
                    if let Some((_, fields)) = derived
                        .iter()
                        .find(|(alias, _)| name.eq_ignore_ascii_case(alias))
                    {
                        return Some(fields.clone());
                    }
                    database
                        .tables
                        .get(name)
                        .map(|t| t.schema.fields.clone())
                        .or_else(|| crate::core::rql::catalog_views::synthetic_schema(name))
                };
                return Ok(
                    match crate::core::rql::join::plan_statement(&qr.query, &schema_of) {
                        Ok(plan) => Some((
                            join_output_columns(&plan, database)
                                .into_iter()
                                .map(|(name, ty)| {
                                    let label: String = name;
                                    (label, ty)
                                })
                                .collect(),
                            true,
                        )),
                        Err(_) => None,
                    },
                );
            }

            // Same reasoning as the join path above: a catalogue view is
            // materialised at execute time and so is absent from
            // `database.tables`, but its column list is fixed and known.
            // Without this a *single-relation* query over one — `SELECT
            // n.nspname FROM pg_namespace n WHERE …` — described as returning
            // nothing, even though the join form had been fixed.
            let view_schema = if database.tables.contains_key(&qr.table_name) {
                None
            } else {
                derived
                    .iter()
                    .find(|(alias, _)| qr.table_name.eq_ignore_ascii_case(alias))
                    .map(|(_, fields)| fields.clone())
                    .or_else(|| crate::core::rql::catalog_views::synthetic_schema(&qr.table_name))
            };
            let real_table = database.tables.get(&qr.table_name);
            let table_fields = match (&view_schema, &real_table) {
                (Some(fields), _) => fields.clone(),
                (None, Some(t)) => t.schema.fields.clone(),
                (None, None) => return Ok(None),
            };
            let parsed = match row_fetch_from_select_query(&purpose, &table_fields) {
                Ok(p) => p,
                // The predicate defeated the parser, but a `RowDescription`
                // does not depend on it: which columns come back is decided by
                // the select list and the relation alone. Describing the
                // statement without its `WHERE` answers the only question being
                // asked, and answering it wrongly is not on the table — the
                // columns are the same either way.
                //
                // `WHERE x IN (SELECT …)` is the case this exists for. It runs
                // correctly, and it used to be answered with `NoData`, so every
                // row came back under no resolvable column names.
                Err(_) => match describe_without_predicate(&qr.query) {
                    Some(reduced) => {
                        let purpose =
                            QueryPurpose::QueryRows(crate::core::rql::lexer_s1::QueryRowsData {
                                table_name: qr.table_name.clone(),
                                query: reduced,
                            });
                        match row_fetch_from_select_query(&purpose, &table_fields) {
                            Ok(p) => p,
                            Err(_) => return Ok(None),
                        }
                    }
                    None => return Ok(None),
                },
            };

            // NOTE: RowDescription wants &str; we keep names static-ish by leaking.
            // This is acceptable for a minimal server; statements are small and few.
            let mut fields = Vec::new();

            // The type a select item is *advertised* as.
            //
            // A shredded JSON column is stored as a `UUID` pointing at its
            // shadow table, and `reported_type` is what says `jsonb` instead —
            // its own doc comment requires every path that describes a column
            // to agree on that. The branches below used `result_type()`, which
            // for a plain column is the raw stored type, so a select mixing
            // such a column with a computed item, a window or an aggregate
            // advertised it as `uuid` and every typed client refused the row.
            // A plain `SELECT` of the same column was fine, which made it look
            // intermittent by query rather than by column.
            let advertised = |item: &crate::core::rql::aggregate::SelectItem| -> DbType {
                match item {
                    crate::core::rql::aggregate::SelectItem::Column { field, .. } => {
                        match &real_table {
                            Some(t) => reported_type(field, &t.schema),
                            None => field.db_type.clone(),
                        }
                    }
                    other => other.result_type(),
                }
            };

            // A select list containing a constant has no fetched column behind
            // that position, so the metadata comes from the list itself.
            if parsed.select_items.iter().any(|i| {
                matches!(
                    i,
                    crate::core::rql::aggregate::SelectItem::Literal { .. }
                        | crate::core::rql::aggregate::SelectItem::Scalar { .. }
                        // A row expression fills an output slot no fetched
                        // column occupies, so the description has to come from
                        // the select list or the row goes out with more fields
                        // than were announced.
                        | crate::core::rql::aggregate::SelectItem::RowExpr { .. }
                        // A window fills an output slot no fetched column
                        // occupies, so its type must come from the select list.
                        | crate::core::rql::aggregate::SelectItem::Window { .. }
                )
            }) {
                for item in &parsed.select_items {
                    let label: String = item.label().to_string();
                    fields.push((label, advertised(item)));
                }
                return Ok(Some((fields, true)));
            }

            // An aggregate result has no schema fields behind it: its columns
            // are the select items, named and typed by the plan.
            if let Some(plan) = &parsed.aggregate {
                for item in &plan.items {
                    let label: String = item.label().to_string();
                    fields.push((label, advertised(item)));
                }
                return Ok(Some((fields, true)));
            }

            // A column is announced under its alias. `SELECT x AS v FROM t` was
            // described as `x`, so a driver reading `v` by name found nothing,
            // and a derived table that renamed a column described as having
            // no columns at all. Matched by the column, each item once, since
            // the fetch need not be in select-list order and one column may be
            // selected twice under two names.
            let mut labels: Vec<Option<&str>> = parsed
                .select_items
                .iter()
                .map(|item| match item {
                    crate::core::rql::aggregate::SelectItem::Column { label, .. } => Some(label.as_str()),
                    _ => None,
                })
                .collect();
            for c in parsed
                .requested_row_fetch
                .columns_fetching_data
                .iter()
                .take(parsed.visible_columns)
            {
                let Some(field) = field_of(&table_fields, c.schema_id) else {
                    continue;
                };
                let aliased = (0..parsed.select_items.len())
                    .find(|&i| {
                        labels[i].is_some()
                            && matches!(&parsed.select_items[i], crate::core::rql::aggregate::SelectItem::Column { field: f, .. }
                                if f.write_order == field.write_order)
                    })
                    .and_then(|i| labels[i].take());
                let label: String = aliased.unwrap_or(&field.name).to_string();
                // A catalogue view has no `TableSchema` and no JSON columns, so
                // the field's own type is what it reports.
                let ty = match &real_table {
                    Some(t) => reported_type(field, &t.schema),
                    None => field.db_type.clone(),
                };
                fields.push((label, ty));
            }
            Ok(Some((fields, true)))
        }
        QueryPurpose::InsertRow(ir) => {
            let Some(table) = database.tables.get(&ir.table_name) else {
                return Ok(Some((Vec::new(), false)));
            };
            match crate::core::rql::lexer_ir::returning_columns(&ir.query, &table.schema.fields) {
                Some(Ok(cols)) => {
                    let fields = cols
                        .iter()
                        .map(|f| {
                            let label: String = f.name.clone();
                            (label, reported_type(f, &table.schema))
                        })
                        .collect();
                    Ok(Some((fields, true)))
                }
                _ => Ok(Some((Vec::new(), false))),
            }
        }

        // `UPDATE … RETURNING` and `DELETE … RETURNING` return rows exactly as
        // an `INSERT … RETURNING` does, and had no arm here at all — they fell
        // through to `NoData`. A driver that caches a prepared statement's
        // column list then cached *no columns*, so the returned rows arrived
        // with no resolvable names: right values, nothing able to read them.
        QueryPurpose::UpdateRow(sql) | QueryPurpose::DeleteRow(sql) => {
            // The relation is the token after `UPDATE`, or after `DELETE FROM`.
            let skip = if matches!(&purpose, QueryPurpose::UpdateRow(_)) {
                1
            } else {
                2
            };
            let name = sql
                .split_whitespace()
                .nth(skip)
                .unwrap_or("")
                .trim_matches('"')
                .to_string();
            let Some(table) = database.tables.get(&name) else {
                return Ok(Some((Vec::new(), false)));
            };
            match crate::core::rql::lexer_ir::returning_columns(sql, &table.schema.fields) {
                Some(Ok(cols)) => {
                    let fields = cols
                        .iter()
                        .map(|f| {
                            let label: String = f.name.clone();
                            (label, reported_type(f, &table.schema))
                        })
                        .collect();
                    Ok(Some((fields, true)))
                }
                _ => Ok(Some((Vec::new(), false))),
            }
        }
        _ => Ok(Some((Vec::new(), false))),
    }
}

/// A cell of a catalogue result set. `SHOW DATABASES` is answered here rather
/// than by the engine, so it builds its own rows.
enum CatalogCell {
    Text(String),
    Number(u64),
    Flag(bool),
}

fn catalog_row(cells: &[CatalogCell]) -> Row {
    let mut columns: crate::core::row::row::RowColumns =
        smallvec::SmallVec::new();
    for (index, cell) in cells.iter().enumerate() {
        let (bytes, column_type): (Vec<u8>, DbType) = match cell {
            CatalogCell::Text(text) => (text.as_bytes().to_vec(), DbType::STRING),
            CatalogCell::Number(n) => (
                crate::core::value_codec::count_as_bigint(*n).to_le_bytes().to_vec(),
                DbType::I64,
            ),
            CatalogCell::Flag(b) => (vec![u8::from(*b)], DbType::BOOL),
        };
        let mut block = crate::memory_pool::MEMORY_POOL.acquire(bytes.len());
        block.into_slice_mut().copy_from_slice(&bytes);
        columns.push(crate::core::row::row::Column {
            schema_id: crate::core::row::row::schema_id_of(index as u64),
            data: block,
            column_type,
        });
    }
    let length = columns.iter().map(|c| c.data.len()).sum::<usize>();
    Row {
        position: 0,
        columns,
        #[cfg(feature = "enable_long_row")]
        length: length as u64,
        #[cfg(not(feature = "enable_long_row"))]
        length: length as u32,
    }
}

/// Reports a statement failure and poisons an open transaction block.
///
/// PostgreSQL refuses everything after a failure until the block is closed. The
/// poisoning has to happen wherever the error is *reported*, not at one chosen
/// point: a statement can fail at parse, at name resolution, or in the engine,
/// and a path that reports without poisoning lets the block commit with a hole
/// in it.
async fn fail_statement<S>(
    state: &mut SessionState,
    stream: &mut S,
    sqlstate: &str,
    message: &str,
) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    state.statement_failed = true;
    state.error_in_batch = true;
    state.statement_error = Some(message.to_string());
    if let Some(t) = state.transaction.as_mut() {
        t.failed = true;
    }
    // The planning paths report every failure as a syntax error; a pattern the
    // `SIMILAR TO` matcher refuses is PostgreSQL's `2201B` instead.
    // Likewise a LIKE pattern ending in its escape (`22025`) and an ESCAPE
    // clause that is not one character (`22019`).
    let sqlstate = if sqlstate != "42601" {
        sqlstate
    } else if message.starts_with("invalid regular expression") {
        "2201B"
    } else if message.starts_with("LIKE pattern must not end with escape character") {
        "22025"
    } else if message.starts_with("invalid escape string") {
        "22019"
    } else if message.ends_with(" is not supported") {
        // A shape the planner recognised and refuses, such as an aggregate
        // over DISTINCT values inside an expression: feature_not_supported,
        // not a syntax error.
        "0A000"
    } else if message.contains(" types ") && message.ends_with(" cannot be matched") {
        // `CASE types integer and text cannot be matched` — PostgreSQL's
        // datatype_mismatch.
        "42804"
    } else {
        sqlstate
    };
    send_error_response(stream, sqlstate, message).await
}

/// Answers `IMPORT DATABASE FROM POSTGRES … INTO …`.
///
/// The whole import runs here, on this connection, which is why it is a
/// statement rather than a background job: a migration is something an operator
/// watches finish, and a connection that returns when it is done is the
/// simplest thing that can report what happened.
#[cfg(feature = "pg_import")]
async fn handle_import_statement<S>(
    state: &mut SessionState,
    stream: &mut S,
    stmt: &str,
    omit_description: bool,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // It creates a database and runs its own transactions against it, neither
    // of which an enclosing block can undo. Refusing is honest; accepting would
    // mean a `ROLLBACK` that leaves a whole database behind.
    if state.transaction.is_some() {
        return fail_statement(
            state,
            stream,
            "25001",
            "IMPORT cannot run inside a transaction block: it creates a database, which a \
             ROLLBACK could not take back",
        )
        .await;
    }

    let statement = match crate::core::migrate::sql::parse(stmt) {
        Ok(statement) => statement,
        Err(e) => return fail_statement(state, stream, "42601", &e).await,
    };

    let report = crate::core::migrate::run::import(
        &state.catalog,
        &statement.source,
        &statement.target,
        &statement.options,
    )
    .await;

    match report {
        Ok(report) => {
            // A real import leaves a database that has to be reachable, and
            // roles are per database — one created without an account could not
            // be connected to and there would be no way in to create one.
            if !report.dry_run {
                if let Some(database) = state.catalog.get(&statement.target) {
                    if let Err(e) = seed_roles(&database, &state.auth).await {
                        log::error!(
                            "The import finished but {:?} has no account to log in with: {}",
                            statement.target,
                            e
                        );
                    }
                }
            }

            let lines = report.lines();
            let fields = crate::core::migrate::run::result_fields();
            let rows: Vec<Row> = lines
                .iter()
                .map(|line| catalog_row(&[CatalogCell::Text(line.clone())]))
                .collect();
            let tag = format!("SELECT {}", rows.len());
            send_query_result(
                stream,
                Some(&fields),
                None,
                &rows,
                &[],
                &tag,
                omit_description,
            )
            .await
        }
        Err(e) => fail_statement(state, stream, "58000", &e).await,
    }
}

/// The SQLSTATE for a failed `CREATE DATABASE`.
///
/// Migration tools branch on the code: `42P04` means "already there, carry
/// on", while a name this server will not accept is a different problem and
/// must not be read as that. A valid name that still failed is the server's
/// problem: its directory could not be made, or holds a log recovery refused.
fn create_database_failure_code(exists: bool, name: &str) -> &'static str {
    if exists {
        "42P04"
    } else if crate::core::database::catalog::validate_name(name).is_err() {
        "42602"
    } else {
        "58000"
    }
}

#[cfg(test)]
#[test]
fn create_database_failure_codes() {
    assert_eq!(create_database_failure_code(true, "taken"), "42P04");
    assert_eq!(create_database_failure_code(false, "../escape"), "42602");
    // A refused recovery is not a bad name: a tool told 42602 rewrites the
    // name instead of looking at the server.
    assert_eq!(create_database_failure_code(false, "adoptable"), "58000");
}

/// Answers `CREATE DATABASE`, `DROP DATABASE` and `SHOW DATABASES`.
///
/// These act on the catalogue rather than on the database the session is
/// attached to, which is why they are answered here: the executor is handed one
/// `Database` and cannot see the others.
///
/// A created database is seeded with the bootstrap role immediately: roles are
/// per database, so one created without an account could not be connected to,
/// and there would be no way in to create one.
async fn handle_database_statement<S>(
    state: &mut SessionState,
    stream: &mut S,
    statement: DatabaseStatement,
    omit_description: bool,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    match statement {
        DatabaseStatement::Create {
            name,
            if_not_exists,
        } => {
            if if_not_exists && state.catalog.get(&name).is_some() {
                // PostgreSQL has no `IF NOT EXISTS` here, but every migration
                // tool writes it; reporting the no-op the way `CREATE TABLE`
                // does beats failing a script that is already idempotent.
                return send_command_complete(stream, "CREATE DATABASE").await;
            }
            match state.catalog.create(&name).await {
                Ok(database) => {
                    if let Err(e) = seed_roles(&database, &state.auth).await {
                        // The database exists but nobody can log into it, which
                        // is worse than not having created it.
                        let _ = state
                            .catalog
                            .drop_database(&name, &state.database_name)
                            .await;
                        fail_statement(state, stream, "58000", &e).await
                    } else {
                        send_command_complete(stream, "CREATE DATABASE").await
                    }
                }
                Err(e) => {
                    let exists = state.catalog.get(&name).is_some();
                    let sqlstate = create_database_failure_code(exists, &name);
                    fail_statement(state, stream, sqlstate, &e).await
                }
            }
        }
        DatabaseStatement::List => {
            let listing = state.catalog.listing();
            let current = state.database_name.clone();
            let fields = vec![
                ("database_name".to_string(), DbType::STRING),
                ("location".to_string(), DbType::STRING),
                ("tables".to_string(), DbType::I64),
                ("sessions".to_string(), DbType::I64),
                ("is_default".to_string(), DbType::BOOL),
                ("is_current".to_string(), DbType::BOOL),
            ];
            let rows: Vec<Row> = listing
                .iter()
                .map(|entry| {
                    catalog_row(&[
                        CatalogCell::Text(entry.name.clone()),
                        CatalogCell::Text(entry.location.clone()),
                        CatalogCell::Number(entry.tables),
                        CatalogCell::Number(entry.sessions),
                        CatalogCell::Flag(entry.is_default),
                        CatalogCell::Flag(entry.name == current),
                    ])
                })
                .collect();
            let tag = format!("SELECT {}", rows.len());
            send_query_result(
                stream,
                Some(&fields),
                None,
                &rows,
                &[],
                &tag,
                omit_description,
            )
            .await
        }
        DatabaseStatement::Drop { name, if_exists } => {
            if if_exists && state.catalog.get(&name).is_none() {
                return send_command_complete(stream, "DROP DATABASE").await;
            }
            let current = state.database_name.clone();
            match state.catalog.drop_database(&name, &current).await {
                Ok(()) => send_command_complete(stream, "DROP DATABASE").await,
                Err(e) => fail_statement(state, stream, "55006", &e).await,
            }
        }
    }
}

/// Runs one statement and records what it cost.
///
/// The wrapper rather than [`execute_single_statement`] itself, because that
/// function returns early on several refusals — a `LOCK TABLE` outside a block,
/// a CTE it cannot materialise — and a statement that was refused is exactly
/// the one someone reading `SHOW QUERIES` is looking for. Both protocols enter
/// here: the simple query loop and the extended protocol's `Execute`.
/// Runs one statement under this session's cancel signal.
///
/// The signal is established here, around the whole of the work — above the
/// statistics machinery and outside every one of its exits. That placement is
/// deliberate: statement recording can be off at build time *or* switched off
/// live, and scoping the signal inside the recorded path would mean turning
/// statement statistics off silently stopped enforcing `statement_timeout`.
/// An operator turning off an observability feature must not disarm a safety
/// one.
async fn execute_recorded<S>(
    database: &Arc<Database>,
    state: &mut SessionState,
    stream: &mut S,
    stmt: &str,
    result_formats: &[i16],
    omit_description: bool,
    prepared: Option<PreparedRun>,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // Zero means no limit, as it does in PostgreSQL, so it produces no
    // deadline rather than one that has already passed.
    let deadline = state
        .statement_timeout_ms
        .filter(|ms| *ms > 0)
        .map(|ms| std::time::Instant::now() + std::time::Duration::from_millis(ms));
    // The scan asks this per batch, in the same place it checks the deadline.
    let cancel = {
        let base = crate::core::cancel::StatementCancel::new(deadline);
        match PEER_WATCH.try_with(|w| w.clone()).ok().flatten() {
            Some(watch) => {
                std::sync::Arc::new(base.watching(Box::new(move || watch.gone())))
            }
            None => std::sync::Arc::new(base),
        }
    };

    // Published for the length of the statement, so a CancelRequest finds
    // *this* statement's signal and nothing else. The guard clears the slot on
    // every way out — including this future being dropped — and only if the
    // slot still holds this statement, so a cancel that arrives late trips a
    // signal no one reads instead of stopping the next statement. Set here,
    // above the query-stats paths, for the reason the scope below is.
    let _published = SESSION_CANCEL
        .try_with(|session| session.publish(cancel.clone()))
        .ok();

    // The statement is **not** dropped when the client goes: it is asked to
    // stop, and unwinds through the ordinary scan-error path. Dropping it would
    // cancel at an await point with none of its cleanup done — a CTE's
    // temporary tables never dropped, an autocommit transaction left
    // `InProgress` holding back reclamation.
    //
    // Notices the engine raises for this statement are collected here and
    // sent ahead of its `CommandComplete` (see `send_command_complete`).
    //
    // One lock owner for the whole statement and everything it runs (CTE and
    // derived bodies, catalog relations and their drops, subqueries, the
    // engine's attempts): the open block's root, or for an autocommit
    // statement its owner O, begun the first time something asks for it and
    // ended (its locks released) as this statement's future ends, on every way
    // out (docs/persistent-indexes.md §7 Owner, Q1). Each arm builds its own
    // future so the statement's is stored once.
    let block_root = state.transaction.as_ref().map(|t| t.id);
    let (transactions, row_locks) = (database.transactions.clone(), database.row_locks.clone());
    crate::core::cancel::STATEMENT_CANCEL
        .scope(cancel, async {
            match block_root {
                Some(root) => {
                    crate::core::mvcc::owner::under(
                        &transactions,
                        root,
                        crate::core::mvcc::owner::OwnerKind::Block,
                        crate::core::notice::collect(execute_recorded_inner(
                            database,
                            state,
                            stream,
                            stmt,
                            result_formats,
                            omit_description,
                            prepared,
                        )),
                    )
                    .await
                }
                None => {
                    crate::core::mvcc::owner::statement(
                        &transactions,
                        &row_locks,
                        crate::core::mvcc::owner::OwnerKind::Statement,
                        crate::core::notice::collect(execute_recorded_inner(
                            database,
                            state,
                            stream,
                            stmt,
                            result_formats,
                            omit_description,
                            prepared,
                        )),
                    )
                    .await
                }
            }
        })
        .await
}

/// Runs a prepared statement's execution from its cached parse when it has
/// one, and otherwise by its text, watching the text path while it learns
/// whether it can have one. See [`PreparedSelect`].
async fn execute_watched<S>(
    database: &Arc<Database>,
    state: &mut SessionState,
    stream: &mut S,
    stmt_name: &str,
    query: &str,
    params: &[BoundParam],
    result_formats: &[i16],
    omit_description: bool,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    use crate::core::rql::prepared as pr;

    enum Step {
        Run(PreparedRun),
        Watch,
        Text,
    }

    let generation = database
        .schema_generation
        .load(std::sync::atomic::Ordering::Acquire);
    // Each value as `inline_params` wrote it into `query`, and its class.
    let literals: Option<Vec<String>> = params
        .iter()
        .map(|p| crate::pg_params::param_literal(p).ok())
        .collect();
    let classes: Option<Vec<u8>> = literals
        .as_ref()
        .and_then(|l| l.iter().map(|x| pr::literal_class(x)).collect());

    let step = match state.statements.get_mut(stmt_name) {
        None => Step::Text,
        Some(info) => {
            if let PreparedSelect::Ready(l) | PreparedSelect::Candidate(l) = &info.prepared {
                if l.generation != generation {
                    info.prepared = PreparedSelect::Unknown;
                }
            }
            match &info.prepared {
                PreparedSelect::Ineligible => Step::Text,
                PreparedSelect::Ready(l) => match (&literals, &classes) {
                    (Some(lits), Some(cls)) if *cls == l.classes => {
                        let mut query_row = l.query_row.clone();
                        query_row.where_clause = l.template.render(lits);
                        Step::Run(PreparedRun {
                            rows: Box::new(pr::PreparedRows {
                                table_name: l.table_name.clone(),
                                query_row,
                                // This execution's own values, through the
                                // cached derivation (I11).
                                key: l.route.as_ref().and_then(|plan| plan.bind(lits)),
                            }),
                            fields: l.fields.clone(),
                            scales: l.scales.clone(),
                        })
                    }
                    _ => Step::Text,
                },
                PreparedSelect::Candidate(_) => {
                    if classes.is_some() { Step::Watch } else { Step::Text }
                }
                PreparedSelect::Unknown => {
                    let sql = info.sql.clone();
                    let shaped = value_free_shape(&sql) == Some(ValueFreeShape::SameShape)
                        && pr::compared_columns(&sql).is_some_and(|c| c.len() == params.len());
                    if !shaped {
                        info.prepared = PreparedSelect::Ineligible;
                        Step::Text
                    } else if classes.is_some() {
                        Step::Watch
                    } else {
                        Step::Text
                    }
                }
            }
        }
    };

    match step {
        Step::Run(run) => {
            PREPARED_RUNS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            execute_recorded(database, state, stream, query, result_formats, omit_description, Some(run)).await
        }
        Step::Text => execute_recorded(database, state, stream, query, result_formats, omit_description, None).await,
        Step::Watch => {
            let (result, capture) = pr::capturing(execute_recorded(
                database,
                state,
                stream,
                query,
                result_formats,
                omit_description,
                None,
            ))
            .await;
            if result.is_ok() && !state.statement_failed {
                if let (Some(literals), Some(classes)) = (literals, classes) {
                    learn_prepared(database, state, stmt_name, generation, literals, classes, capture);
                }
            }
            result
        }
    }
}

/// Executions of a prepared `SELECT` that ran from a cached parse. For tests
/// and diagnostics: it is how a test tells the fast path ran at all.
pub static PREPARED_RUNS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// What a watched execution teaches. The first yields a candidate; the next
/// with different values either confirms it, exactly, or rules the statement
/// out for good.
fn learn_prepared(
    database: &Arc<Database>,
    state: &mut SessionState,
    stmt_name: &str,
    generation: u64,
    literals: Vec<String>,
    classes: Vec<u8>,
    capture: crate::core::rql::prepared::Capture,
) {
    use crate::core::rql::prepared as pr;

    let Some(info) = state.statements.get_mut(stmt_name) else {
        return;
    };
    // A schema change while it ran: what it saw may describe neither shape.
    if database.schema_generation.load(std::sync::atomic::Ordering::Acquire) != generation {
        return;
    }
    let Some((table_name, query_row, fields, scales)) = capture.plain_only() else {
        info.prepared = PreparedSelect::Ineligible;
        return;
    };
    // Every value is compared with a column whose literals no rewrite
    // reinterprets, and has that column's class.
    let Some(columns) = pr::compared_columns(&info.sql) else {
        info.prepared = PreparedSelect::Ineligible;
        return;
    };
    let Some(table) = database.tables.get(&table_name).map(|t| t.value().clone()) else {
        return;
    };
    let mut wanted = Vec::with_capacity(columns.len());
    for c in &columns {
        let class = table
            .schema
            .fields
            .iter()
            .find(|f| !f.is_deleted && f.name.eq_ignore_ascii_case(c))
            .and_then(|f| pr::class_for(&f.db_type));
        match class {
            Some(k) => wanted.push(k),
            None => {
                info.prepared = PreparedSelect::Ineligible;
                return;
            }
        }
    }
    if classes != wanted {
        // These values cannot teach (a number for a text column, say); the
        // next ones may.
        return;
    }
    let shape = pr::shape_of(&query_row);
    info.prepared = match std::mem::take(&mut info.prepared) {
        PreparedSelect::Unknown => match pr::WhereTemplate::derive(&query_row.where_clause, &literals) {
            Some(template) => PreparedSelect::Candidate(Box::new(PreparedLearned {
                generation,
                table_name,
                query_row,
                shape,
                template,
                literals,
                classes,
                fields,
                scales,
                misses: 0,
                route: None,
            })),
            // A value that also occurs elsewhere in the clause; another
            // execution may place it unambiguously.
            None => PreparedSelect::Unknown,
        },
        PreparedSelect::Candidate(mut l) => {
            if literals.iter().zip(&l.literals).any(|(a, b)| a == b) {
                // A value the same as last time checks nothing about where it
                // goes. Give up watching after a few.
                l.misses += 1;
                if l.misses > 8 {
                    PreparedSelect::Ineligible
                } else {
                    PreparedSelect::Candidate(l)
                }
            } else if l.table_name == table_name
                && l.shape == shape
                && l.fields == fields
                && l.scales == scales
                && l.classes == classes
                && l.template.render(&literals) == query_row.where_clause
            {
                l.route = learn_route(database, &l, &columns, &literals);
                let mut query_row = query_row;
                query_row.where_clause = String::new();
                l.query_row = query_row;
                PreparedSelect::Ready(l)
            } else {
                // The text path made something the template would not have:
                // a value rewritten by what it holds. Text path for good.
                PreparedSelect::Ineligible
            }
        }
        other => other,
    };
}

/// The route derivation a statement just verified on a partitioned table
/// keeps (S16b): `Some` only for the full-key point shape, every key column
/// compared with exactly one parameter, and only when the key each of the
/// two verifying executions binds (`l.literals`, then `literals`) routes
/// exactly where `route_where` routes its rendered `WHERE`: one point, the
/// same one. That check is what admits only top-level equalities: an `OR`,
/// a range, a `NOT` or a second constant on the key routes elsewhere, and
/// the statement keeps routing its rendered `WHERE` per execution (S15).
fn learn_route(
    database: &Arc<Database>,
    l: &PreparedLearned,
    columns: &[String],
    literals: &[String],
) -> Option<crate::core::rql::prepared::RoutePlan> {
    use crate::core::partition::Route;
    let Some(crate::core::database::Relation::Partitioned(pt)) = database.relation(&l.table_name) else {
        return None;
    };
    let fields = &pt.shell().schema.fields;
    let key: Vec<(u32, DbType, &str)> = pt
        .spec()
        .ok()?
        .levels()
        .iter()
        .map(|level| {
            let field = fields.iter().find(|f| !f.is_deleted && f.write_order as u32 == level.write_order)?;
            Some((level.write_order, level.db_type.clone(), field.name.as_str()))
        })
        .collect::<Option<_>>()?;
    let plan = crate::core::rql::prepared::RoutePlan::derive(pt.uid(), &key, columns)?;
    for lits in [&l.literals[..], literals] {
        let bound = plan.bind(lits)?;
        let range = pt.keyed_range(bound.uid, &bound.levels)?;
        if pt.route_where(&l.template.render(lits)).ok()? != Route::Points(vec![range]) {
            return None;
        }
    }
    Some(plan)
}

/// One statement: its cached parse when it has one, its text otherwise.
async fn run_statement<S>(
    database: &Arc<Database>,
    state: &mut SessionState,
    stream: &mut S,
    stmt: &str,
    result_formats: &[i16],
    omit_description: bool,
    prepared: Option<PreparedRun>,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    match prepared {
        // A learned parse reads under the statement's one snapshot, as the
        // text path below reads a read-only statement: a partitioned table's
        // fan-out takes its `Visibility` from the scope, so no later read of
        // the same statement can take another (I7 on the fast path).
        //
        // Not every learned parse is read-only. A plain table's `SELECT …
        // FOR UPDATE` is learned too (nothing in `compared_columns` refuses
        // it), and `is_read_only` keeps its text out of the scope, because a
        // conflict retry under the first attempt's snapshot would meet the
        // same superseded versions and conflict again. A learned parse is a
        // plain single-table `SELECT` (a CTE, subquery or union is a rewrite,
        // never learned), so a locking clause, which the parse records in
        // `row_lock`, is its only way to be more than a read.
        Some(run) if run.rows.query_row.row_lock.is_none() => {
            crate::core::rql::statement_snapshot::scope(execute_prepared(
                database,
                state,
                stream,
                stmt,
                run,
                result_formats,
                omit_description,
            ))
            .await
        }
        Some(run) => execute_prepared(database, state, stream, stmt, run, result_formats, omit_description).await,
        // A read-only statement's subqueries, CTEs and relations are read under
        // one snapshot, as PostgreSQL reads a statement; see `statement_snapshot`.
        None if crate::core::rql::statement_snapshot::is_read_only(stmt) => {
            crate::core::rql::statement_snapshot::scope(execute_single_statement(
                database,
                state,
                stream,
                stmt,
                result_formats,
                omit_description,
            ))
            .await
        }
        None => execute_single_statement(database, state, stream, stmt, result_formats, omit_description).await,
    }
}

/// Runs a prepared `SELECT` from its cached parse. What it skips is the
/// statement text's parsing; what it keeps is everything the text path does
/// around a plain `SELECT`: the aborted-block refusal, the transaction it runs
/// in, and the reply.
async fn execute_prepared<S>(
    database: &Arc<Database>,
    state: &mut SessionState,
    stream: &mut S,
    stmt: &str,
    run: PreparedRun,
    result_formats: &[i16],
    omit_description: bool,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    if state.transaction.as_ref().is_some_and(|t| t.failed) {
        send_error_response(
            stream,
            "25P02",
            "current transaction is aborted, commands ignored until end of transaction block",
        )
        .await?;
        return Ok(());
    }
    let resp = match state.current_tx() {
        Some(tx) => {
            let purpose = QueryPurpose::PreparedRows(run.rows);
            crate::core::rql::executor::execute_in_txn(purpose, database.clone(), tx).await
        }
        // Inside an autocommit transaction even though it writes nothing: the
        // in-place overwrite gate sees readers only in `running`, so a read
        // on a bare snapshot could observe half of an in-place UPDATE, or an
        // overwrite whose transaction then aborted.
        None => {
            let purpose = QueryPurpose::PreparedRows(run.rows);
            crate::core::rql::executor::execute_with_sync(purpose, database.clone(), state.synchronous_commit())
                .await
        }
    };
    let result = send_engine_response(
        state,
        stream,
        resp,
        PurposeKind::QueryRows,
        run.fields,
        run.scales,
        result_formats,
        omit_description,
    )
    .await;
    if state.statement_failed {
        match state.statement_error.as_deref() {
            Some(reason) => log::error!("Statement failed ({}): {}", reason, crate::core::redact::statement(stmt)),
            None => log::error!("Statement failed: {}", crate::core::redact::statement(stmt)),
        }
    }
    result
}

async fn execute_recorded_inner<S>(
    database: &Arc<Database>,
    state: &mut SessionState,
    stream: &mut S,
    stmt: &str,
    result_formats: &[i16],
    omit_description: bool,
    prepared: Option<PreparedRun>,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // Whether this statement failed, and why, belongs to this statement. Reset
    // here rather than inside, where the reset sat below the early returns and
    // so left the previous statement's flag standing for them.
    state.statement_failed = false;
    state.statement_error = None;

    #[cfg(not(feature = "query_stats"))]
    {
        return run_statement(
            database,
            state,
            stream,
            stmt,
            result_formats,
            omit_description,
            prepared,
        )
        .await;
    }

    #[cfg(feature = "query_stats")]
    {
        let Some(recording) = crate::core::query_stats::begin() else {
            return run_statement(
                database,
                state,
                stream,
                stmt,
                result_formats,
                omit_description,
                prepared,
            )
            .await;
        };

        // Boxed for the same reason the inner call is: this path is deep, and
        // the future of a statement is the largest one in the process.
        let running = Box::pin(run_statement(
            database,
            state,
            stream,
            stmt,
            result_formats,
            omit_description,
            prepared,
        ));
        let result = crate::core::query_stats::observe(recording.probe(), running).await;

        let failure = state
            .statement_failed
            // A failure always carries its message — `fail_statement` records
            // one — but the record should say a statement failed even if some
            // path one day reports one without.
            .then(|| {
                state
                    .statement_error
                    .clone()
                    .unwrap_or_else(|| "failed".to_string())
            });
        crate::core::query_stats::finish(
            recording,
            stmt,
            &state.database_name,
            &state.principal.name,
            failure.as_deref(),
        );
        result
    }
}

/// Runs one statement, materialising any `WITH` clause first.
///
/// The temporary tables a CTE materialises into are dropped on **every** exit
/// path, which is why this wraps the body rather than living inside it: DDL is
/// not transactional here, so a statement that fails part-way would otherwise
/// leave tables behind to collide with the next run, and the body returns early
/// in a dozen places.
async fn execute_single_statement<S>(
    database: &Arc<Database>,
    state: &mut SessionState,
    stream: &mut S,
    stmt: &str,
    result_formats: &[i16],
    // Under the extended protocol a `RowDescription` belongs to `Describe`,
    // never to `Execute` — sending it from both left the client reading two
    // descriptions for one result and reporting a protocol violation.
    omit_description: bool,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // Comments are whitespace to PostgreSQL, and every parser downstream has
    // to see them that way or it reads prose as SQL. A production statement
    // carried a comment block explaining a rewrite, one line of which quoted
    // `IN (SELECT ...)`; the subquery extractor found that inside the comment
    // and tried to run `SELECT ...` — "cannot parse subquery 'SELECT ...'",
    // the ellipsis being literal prose. Stripped once here, at the one place
    // every statement passes through, rather than in each parser that scans
    // for a keyword. `recognize_query_purpose` already stripped comments to
    // *classify* a statement, but the text it carried onward kept them.
    let uncommented;
    let stmt = if stmt.contains("--") || stmt.contains("/*") {
        uncommented = crate::core::rql::lexer_s1::strip_sql_comments(stmt);
        uncommented.as_str()
    } else {
        stmt
    };

    // `INSERT INTO t WITH … SELECT …` puts its CTEs after the target; they are
    // moved to the front, where everything below expects a `WITH` to be.
    let hoisted;
    let stmt = match crate::core::rql::cte::hoist_insert_with(stmt) {
        Some(rewritten) => {
            hoisted = rewritten;
            hoisted.as_str()
        }
        None => stmt,
    };

    // A data-modifying CTE and the statement it feeds are one statement, so
    // they need one transaction. Outside an explicit block there is none yet —
    // `execute` would open its own *after* the CTE bodies had already run and
    // committed — so one is opened here to span both, and closed on the outcome.
    //
    // That transaction **is** the statement's owner O (§7 Owner, Q1), never a
    // second owner beside it: its CTE bodies and the outer statement hold
    // their locks as O. Should O already have been begun as a plain owner, a
    // transaction of its own acts for it through an alias instead.
    let mut _cte_alias: Option<crate::core::mvcc::row_locks::AliasGuard<'_>> = None;
    let cte_tx = match state.current_tx() {
        Some(tx) => Some((tx, false)),
        None if crate::core::rql::cte::has_with_clause(stmt) => {
            let tx = match crate::core::mvcc::owner::adopt_transaction() {
                Some(o) => o,
                None => {
                    let tx = database.transactions.begin_autocommit();
                    _cte_alias = crate::core::mvcc::owner::ensure()
                        .map(|o| database.row_locks.owner_alias(tx, o.owner));
                    tx
                }
            };
            Some((tx, true))
        }
        None => None,
    };
    let effective_tx = cte_tx
        .map(|(tx, _)| tx)
        .unwrap_or(crate::core::mvcc::transaction::FROZEN_TX);
    let mut owned_tx = OwnedStatementTx {
        database,
        tx: cte_tx.and_then(|(tx, owned)| owned.then_some(tx)),
    };

    // A unique index is a constraint; rewritten into the ALTER TABLE the engine
    // enforces before anything else looks at the statement.
    let rewritten_unique;
    let stmt = match crate::core::rql::lexer_alter::unique_index_as_constraint(stmt) {
        Some(altered) => {
            rewritten_unique = altered;
            rewritten_unique.as_str()
        }
        None => stmt,
    };

    // DDL does not participate in rollback here: an ALTER or DROP applied
    // inside this block persists even if the block later aborts. Every
    // PostgreSQL client assumes the opposite — a migration runner wraps its
    // statements in one transaction precisely to get all-or-nothing — so the
    // divergence is warned when it becomes possible, which is the only moment
    // it can be seen: by abort time the damage is already applied.
    if state.current_tx().is_some() {
        let head = stmt.trim_start().to_ascii_uppercase();
        if head.starts_with("CREATE")
            || head.starts_with("ALTER")
            || head.starts_with("DROP")
            || head.starts_with("TRUNCATE")
        {
            log::warn!(
                "DDL inside an explicit transaction cannot be rolled back by this engine; if this block aborts, its effects will persist: {}",
                stmt.chars().take(120).collect::<String>()
            );
        }
    }

    // A `LOCK TABLE` outside a transaction block would be released the instant
    // the statement ended, so the caller would be told it holds a lock it does
    // not. PostgreSQL refuses it, and the check belongs here because only the
    // session knows whether a block is open.
    if state.transaction.is_none()
        && stmt
            .trim_start()
            .get(..4)
            .is_some_and(|w| w.eq_ignore_ascii_case("LOCK"))
        && crate::core::rql::row_lock::parse_lock_table(stmt).is_some()
    {
        fail_statement(
            state,
            stream,
            "25P01",
            "LOCK TABLE can only be used in transaction blocks",
        )
        .await?;
        return Ok(());
    }

    // `WHERE created_at < NOW()` reaches the predicate tokenizer as the unknown
    // column `NOW`, so the constant parts are evaluated once here, before
    // anything parses the clause.
    let folded_where = crate::core::rql::executor::fold_where_constants(stmt);
    // A fold evaluated something into the text (`CURRENT_DATE`, `now()`,
    // `1 + 1`). Its answer may not hold next time, so a parse made from it is
    // not one to keep.
    if folded_where != stmt {
        crate::core::rql::prepared::note_rewritten();
    }
    let stmt = folded_where.as_str();

    let materialized = match crate::core::rql::cte::materialize(stmt, database, effective_tx).await
    {
        Ok(m) => m,
        Err(e) => {
            // 0A000 is `feature_not_supported`, which is what an unsupported
            // CTE shape is.
            fail_statement(state, stream, "0A000", &e).await?;
            return Ok(());
        }
    };
    let after_cte = materialized
        .as_ref()
        .map(|m| m.rewritten.as_str())
        .unwrap_or(stmt);

    // A derived table — `FROM (SELECT …) alias` — becomes a temporary relation
    // the same way a CTE does, and for the same reason: the scan reads one
    // relation at a time. Run after the CTE rewrite so a derived table inside a
    // CTE body is handled too.
    let derived =
        match crate::core::rql::cte::materialize_derived(after_cte, database, effective_tx).await {
            Ok(d) => d,
            Err(e) => {
                if let Some(m) = &materialized {
                    crate::core::rql::cte::drop_temps(database, &m.temps, effective_tx).await;
                }
                fail_statement(state, stream, "0A000", &e).await?;
                return Ok(());
            }
        };
    let after_derived = derived
        .as_ref()
        .map(|d| d.rewritten.as_str())
        .unwrap_or(after_cte);

    // A statement naming `pg_class` or `pg_stat_user_tables` gets them built
    // now and pointed at temporary tables. Nothing is built for a statement
    // that names none, which is every ordinary one.
    let catalog = match crate::core::rql::catalog_views::materialize(
        after_derived,
        database,
        effective_tx,
        Some(&state.catalog),
    )
    .await
    {
        Ok(c) => c,
        Err(e) => {
            if let Some(d) = &derived {
                crate::core::rql::cte::drop_temps(database, &d.temps, effective_tx).await;
            }
            if let Some(m) = &materialized {
                crate::core::rql::cte::drop_temps(database, &m.temps, effective_tx).await;
            }
            fail_statement(state, stream, "0A000", &e).await?;
            return Ok(());
        }
    };
    let effective = catalog
        .as_ref()
        .map(|c| c.rewritten.as_str())
        .unwrap_or(after_derived);
    // Materialised relations are per execution, so a parse of the rewritten
    // text names relations the next execution will not have.
    if materialized.is_some() || derived.is_some() || catalog.is_some() {
        crate::core::rql::prepared::note_rewritten();
    }

    // The outer statement runs in the same transaction as its CTE bodies.
    let restore_tx = state.statement_tx;
    if let Some((tx, owned)) = cte_tx {
        if owned {
            state.statement_tx = Some(tx);
        }
    }
    let result = Box::pin(execute_single_statement_inner(
        database,
        state,
        stream,
        effective,
        result_formats,
        omit_description,
    ))
    .await;

    state.statement_tx = restore_tx;

    // The statement text is the one thing an error message usually cannot
    // reconstruct, and without it a client-side failure gives no way to tell
    // *which* statement the engine refused. Logged once, at the boundary where
    // the outcome is known.
    if state.statement_failed {
        // Read rather than taken: `execute_recorded` needs the same message to
        // put in the statement log, and clears it before the next statement.
        match state.statement_error.as_deref() {
            // Redacted: a store statement that fails would otherwise write its
            // S3 keys to the server log, and a failing one is the likeliest to
            // be retried, logged again, and pasted into a bug report.
            Some(reason) => log::error!(
                "Statement failed ({}): {}",
                reason,
                crate::core::redact::statement(effective)
            ),
            None => log::error!(
                "Statement failed: {}",
                crate::core::redact::statement(effective)
            ),
        }
    }

    if let Some(c) = &catalog {
        crate::core::rql::cte::drop_temps(database, &c.temps, effective_tx).await;
    }
    if let Some(d) = &derived {
        crate::core::rql::cte::drop_temps(database, &d.temps, effective_tx).await;
    }
    if let Some(m) = &materialized {
        crate::core::rql::cte::drop_temps(database, &m.temps, effective_tx).await;
    }

    // Close the transaction opened above, if this call opened it. A statement
    // inside a `BEGIN` block owns neither the transaction nor the decision.
    //
    // The block poisons on failure, so `state.transaction.failed` is the record
    // of whether the statement worked — the same signal `COMMIT` reads.
    if let Some((tx, owned)) = cte_tx {
        if owned {
            owned_tx.disarm();
            let failed =
                state.statement_failed || state.transaction.as_ref().is_some_and(|t| t.failed);
            // Asked before the commit, which retires the transaction and
            // forgets the answer. A statement whose CTEs only read leaves
            // nothing on disk carrying this id, so it needs no outcome record —
            // and a data-modifying CTE reaches this as a writer, which is why
            // the question is asked of the write paths and not of the SQL.
            let wrote = database.transactions.wrote_family(tx);
            if failed {
                database.abort_transaction(tx);
                if wrote {
                    flush_outcomes(database);
                }
            } else if !wrote {
                let family = database.commit_transaction(tx);
                database.publish_transaction(family).await;
            } else {
                // Commit locally, make the outcome durable, and only then
                // publish — the ordering the replication path depends on, so a
                // peer never receives a transaction this node then disowns.
                //
                // The outcome record is logged without a sync of its own, and
                // the rows' sync covers it as well: they are records in one
                // log, appended rows-first, and replay stops at the first one
                // it cannot read whole.
                //
                // Rows on the other storage locations are synced before the
                // commit, so no session's root flush can carry the outcome to
                // disk ahead of them: `Database::commit_written_transaction`.
                let family = match database.commit_written_transaction(tx) {
                    Ok(family) => family,
                    Err(e) => {
                        log::error!("Could not make the written rows durable: {}", e);
                        database.abort_transaction(tx);
                        flush_outcomes(database);
                        return Err(io::Error::other(format!(
                            "could not commit: the rows written could not be made durable ({})",
                            e
                        )));
                    }
                };
                if let Err(e) = database.transactions.flush_deferred() {
                    log::error!("Could not record the transaction outcome: {}", e);
                    database.abort_transaction(tx);
                    return Err(io::Error::other(format!(
                        "could not commit: the transaction outcome could not be made durable ({})",
                        e
                    )));
                }
                if state.synchronous_commit() {
                    if let Err(e) = database.sync_own_location() {
                        log::error!("Could not make the written rows durable: {}", e);
                        database.abort_transaction(tx);
                        flush_outcomes(database);
                        return Err(io::Error::other(format!(
                            "could not commit: the rows written could not be made durable ({})",
                            e
                        )));
                    }
                }
                database.publish_transaction(family).await;
            }
        }
    }
    result
}

async fn execute_single_statement_inner<S>(
    database: &Arc<Database>,
    state: &mut SessionState,
    stream: &mut S,
    stmt: &str,
    result_formats: &[i16],
    omit_description: bool,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // Every statement, not just the failing ones. A wrong *answer* is reported
    // as success, so when a client rejects what it was sent there is otherwise
    // nothing on this side naming the statement that produced it — which is the
    // position several days of debugging started from. Off at the default
    // level; `--log-level debug` turns it on.
    log::debug!("Statement: {}", crate::core::redact::statement(stmt.trim()));

    if handle_transaction_control(database, state, stream, stmt).await? {
        return Ok(());
    }

    // Once a statement in a block has failed, PostgreSQL refuses the rest of
    // the block. Letting them through would produce a transaction that commits
    // having skipped the statement that did not work.
    if state.transaction.as_ref().is_some_and(|t| t.failed) {
        send_error_response(
            stream,
            "25P02",
            "current transaction is aborted, commands ignored until end of transaction block",
        )
        .await?;
        return Ok(());
    }

    // Ahead of every dispatch below, so a statement that administers the
    // server is refused before any handler — builtin, role, catalogue or
    // engine — sees it.
    if let Some(verb) = privileged_statement(stmt) {
        if !state.principal.may_administer() {
            log::info!(
                "permission denied: {} attempted {} without superuser",
                state.principal.name,
                verb
            );
            fail_statement(
                state,
                stream,
                "42501",
                &format!("permission denied: {} requires superuser", verb),
            )
            .await?;
            return Ok(());
        }
    }

    if handle_builtin_query(state, stream, stmt, omit_description).await? {
        return Ok(());
    }

    // A read-only block refuses what would write, as PostgreSQL does with
    // `25006`. The engine statements are checked once their purpose is known,
    // below; these are answered above the engine and so are checked here.
    let read_only = state
        .transaction
        .as_ref()
        .map(|t| t.read_only)
        .unwrap_or(state.default_read_only);
    if read_only {
        let verb = privileged_statement(stmt).filter(|v| {
            matches!(
                *v,
                "CREATE ROLE"
                    | "ALTER ROLE"
                    | "DROP ROLE"
                    | "CREATE DATABASE"
                    | "DROP DATABASE"
                    | "IMPORT DATABASE"
            )
        });
        if let Some(verb) = verb {
            fail_statement(state, stream, "25006", &read_only_message(verb)).await?;
            return Ok(());
        }
    }

    // Role management is answered by the auth module: the catalogue is an
    // ordinary table, but `CREATE USER` has to become the right verifier rather
    // than storing the password as written.
    if let Some(parsed) = crate::auth::parse_role_statement(stmt) {
        match parsed {
            Err(e) => fail_statement(state, stream, "42601", &e).await?,
            Ok(statement) => {
                let tag = match &statement {
                    crate::auth::RoleStatement::Create { .. } => "CREATE ROLE",
                    crate::auth::RoleStatement::AlterPassword { .. } => "ALTER ROLE",
                    crate::auth::RoleStatement::Drop { .. } => "DROP ROLE",
                };
                match crate::auth::execute_role_statement(database, statement, state.auth.method)
                    .await
                {
                    Ok(()) => send_command_complete(stream, tag).await?,
                    Err(e) => fail_statement(state, stream, "42710", &e).await?,
                }
            }
        }
        return Ok(());
    }

    // `IMPORT DATABASE FROM POSTGRES … INTO …` creates a database, so like the
    // statements below it is answered here rather than by the executor, which
    // is handed one database and cannot see the catalogue.
    #[cfg(feature = "pg_import")]
    if crate::core::migrate::sql::is_import_statement(stmt) {
        return handle_import_statement(state, stream, stmt, omit_description).await;
    }

    // `CREATE`/`DROP DATABASE` act on the catalogue rather than on the database
    // the session is attached to, so they are answered here rather than by the
    // executor, which only ever sees one database.
    if let Some(parsed) = parse_database_statement(stmt) {
        match parsed {
            Err(e) => fail_statement(state, stream, "42601", &e).await?,
            Ok(statement) => {
                handle_database_statement(state, stream, statement, omit_description).await?
            }
        }
        return Ok(());
    }

    // An `IN (SELECT …)` is resolved into a literal list first. It has to
    // happen before the result metadata is derived, because that derivation
    // plans the statement and would reject the subquery it has not yet seen
    // flattened.
    //
    // Not inside a generation expression, which PostgreSQL refuses to hold a
    // subquery at all. Flattened here, `GENERATED ALWAYS AS (a + (SELECT
    // max(a) FROM t))` was created with whatever the maximum was that moment
    // baked in as a constant.
    let flattened;
    let stmt = if (crate::core::rql::subquery::find_in_subquery(stmt).is_some()
        || crate::core::rql::subquery::find_exists(stmt).is_some()
        || crate::core::rql::subquery::find_scalar_subquery(stmt).is_some())
        && !crate::core::rql::generated::declares_generated(stmt)
    {
        match crate::core::rql::executor::substitute_in_subqueries(
            stmt,
            database,
            state
                .current_tx()
                .unwrap_or(crate::core::mvcc::transaction::FROZEN_TX),
        )
        .await
        {
            Ok(s) => {
                // The subquery's answer is now literal text in the statement.
                crate::core::rql::prepared::note_rewritten();
                flattened = s;
                flattened.as_str()
            }
            Err(e) => {
                // A subquery that ran and failed reports its own failure's code,
                // as it would have alone: a refusal inside `IN (…)` is still
                // 0A000. Only a subquery that could not be read is 42601.
                let code = match e.strip_prefix("subquery failed: ") {
                    Some(inner) => sqlstate_for(inner),
                    None => "42601",
                };
                fail_statement(state, stream, code, &e).await?;
                return Ok(());
            }
        }
    } else {
        stmt
    };

    let purpose = match recognize_query_purpose(stmt) {
        Some(p) => p,
        None => {
            fail_statement(state, stream, "42601", "syntax error or unsupported query").await?;
            return Ok(());
        }
    };

    let purpose_kind = match &purpose {
        QueryPurpose::CreateTable(_) => PurposeKind::CreateTable,
        QueryPurpose::DropTable(_) => PurposeKind::DropTable,
        // PostgreSQL tags all three forms `ALTER TABLE`.
        QueryPurpose::AddColumn(_)
        | QueryPurpose::DropColumn(_)
        | QueryPurpose::RenameColumn(_) => PurposeKind::AlterTable,
        QueryPurpose::Vacuum(_) => PurposeKind::Vacuum,
        QueryPurpose::LockTable(_) => PurposeKind::LockTable,
        QueryPurpose::InsertRow(_) => PurposeKind::InsertRow,
        QueryPurpose::UpdateRow(_) => PurposeKind::UpdateRow,
        QueryPurpose::DeleteRow(_) => PurposeKind::DeleteRow,
        QueryPurpose::QueryRows(_) => PurposeKind::QueryRows,
        _ => PurposeKind::Other,
    };

    if read_only {
        let verb = match purpose_kind {
            PurposeKind::CreateTable => Some("CREATE TABLE"),
            PurposeKind::DropTable => Some("DROP TABLE"),
            PurposeKind::AlterTable => Some("ALTER TABLE"),
            PurposeKind::InsertRow => Some("INSERT"),
            PurposeKind::UpdateRow => Some("UPDATE"),
            PurposeKind::DeleteRow => Some("DELETE"),
            _ => None,
        };
        if let Some(verb) = verb {
            fail_statement(state, stream, "25006", &read_only_message(verb)).await?;
            return Ok(());
        }
    }

    // Precompute SELECT column metadata (needed even if result set is empty).
    // The declared decimal scale of each output column, where the statement
    // selects from a table that recorded one.
    let select_scales: Option<Vec<u32>> = match &purpose {
        // Shared with the subquery boundary (`executor::query_output_scales`),
        // which must read the same bytes at the same scales.
        QueryPurpose::QueryRows(_) => crate::core::rql::executor::query_output_scales(&purpose, database),
        // `RETURNING` sends rows too, and a returned column is read straight
        // from storage at its own scale. Without these arms every `RETURNING v`
        // over a `NUMERIC(p, s)` column was rendered at the engine default.
        other => purpose_returning_scales(database, other),
    };

    let select_fields = match &purpose {
        // `SHOW REPLICATION` produces rows without a relation behind them, so
        // its one column is named here rather than derived from a schema.
        QueryPurpose::Replication(sql) => Some(
            crate::replication::sql::structured_fields(sql)
                .unwrap_or_else(crate::replication::sql::result_fields),
        ),
        // Likewise for the storage surface: its rows describe directories, not
        // a relation, so nothing can derive their names from a schema.
        #[cfg(feature = "storage_pools")]
        QueryPurpose::Storage(sql) => Some(
            crate::core::storage::sql::structured_fields(sql)
                .unwrap_or_else(crate::core::storage::sql::result_fields),
        ),
        // Introspection rows belong to no relation either, so their column
        // names come from the statement.
        QueryPurpose::Introspect(statement) => crate::core::introspect::result_fields(statement),
        // A `KV` command's rows belong to no relation; its columns come from
        // the command.
        #[cfg(feature = "kv_cache")]
        QueryPurpose::KvCache(sql) => Some(crate::core::kv_cache::sql::result_fields(sql)),
        QueryPurpose::QueryRows(qr) if crate::core::rql::join::statement_has_join(&qr.query) => {
            let schema_of = |name: &str| database.tables.get(name).map(|t| t.schema.fields.clone());
            match crate::core::rql::join::plan_statement(&qr.query, &schema_of) {
                Ok(plan) => Some(join_output_columns(&plan, database)),
                Err(e) => {
                    fail_statement(state, stream, "42601", &e).await?;
                    return Ok(());
                }
            }
        }
        QueryPurpose::QueryRows(qr) => {
            // Copied out so the map guard — a shard read lock — is not held
            // across the `fail_statement` awaits below, which write to a socket
            // the client may have stopped draining. A pinned read guard blocks
            // any CREATE/DROP/ALTER for a name on that shard for as long as the
            // client stalls. (It does not block later readers: dashmap 6.1's
            // `try_lock_shared_fast` admits a reader unless a writer *holds* the
            // lock, so parked writers do not queue readers behind them.)
            let table = database
                .tables
                .get(&qr.table_name)
                .map(|t| t.value().clone());
            let Some(table) = table else {
                fail_statement(state, stream, "42P01", "relation does not exist").await?;
                return Ok(());
            };
            // The type a select item is *advertised* as. Same rule as the
            // describe path: a shredded JSON column is stored as a `UUID` and
            // `reported_type` is what says `jsonb`. `result_type()` gives the
            // raw stored type for a plain column, so the two branches below
            // announced `uuid` whenever the select list also held a constant,
            // an expression, a window or an aggregate.
            //
            // These metadata feed the value formatter as well as the
            // `RowDescription`, so getting them wrong here did more than
            // mislabel: the formatter rendered the document *as a UUID* and
            // the client read the column as NULL. Storage was correct and the
            // document simply vanished on the wire.
            let advertised = |item: &crate::core::rql::aggregate::SelectItem| -> DbType {
                match item {
                    crate::core::rql::aggregate::SelectItem::Column { field, .. } => {
                        reported_type(field, &table.schema)
                    }
                    other => other.result_type(),
                }
            };
            // A `UNION` is described by its first branch, planned on its own:
            // a parenthesised one, `(SELECT … LIMIT 2) UNION …`, is not a
            // statement the select parser reads whole.
            let first_branch = match crate::core::rql::join::split_union(&qr.query) {
                Some(Ok(parts)) if qr.query.trim_start().starts_with('(') => {
                    parts.branches.first().and_then(|b| recognize_query_purpose(b))
                }
                _ => None,
            };
            match row_fetch_from_select_query(first_branch.as_ref().unwrap_or(&purpose), &table.schema.fields) {
                Ok(parsed)
                    if parsed.select_items.iter().any(|i| {
                        matches!(
                            i,
                            crate::core::rql::aggregate::SelectItem::Literal { .. }
                        | crate::core::rql::aggregate::SelectItem::Scalar { .. }
                        // A row expression fills an output slot no fetched
                        // column occupies, so the description has to come from
                        // the select list or the row goes out with more fields
                        // than were announced.
                        | crate::core::rql::aggregate::SelectItem::RowExpr { .. }
                        // A window fills an output slot no fetched column
                        // occupies, so its type must come from the select list.
                        | crate::core::rql::aggregate::SelectItem::Window { .. }
                        )
                    }) =>
                {
                    Some(
                        parsed
                            .select_items
                            .iter()
                            .map(|item| (item.label().to_string(), advertised(item)))
                            .collect::<Vec<_>>(),
                    )
                }
                Ok(parsed) if parsed.aggregate.is_some() => {
                    let plan = parsed.aggregate.as_ref().unwrap();
                    Some(
                        plan.items
                            .iter()
                            .map(|item| (item.label().to_string(), advertised(item)))
                            .collect::<Vec<_>>(),
                    )
                }
                Ok(parsed) => Some(
                    parsed
                        .requested_row_fetch
                        .columns_fetching_data
                        .iter()
                        .take(parsed.visible_columns)
                        .filter_map(|c| {
                            field_of(&table.schema.fields, c.schema_id).map(|field| {
                                (field.name.clone(), reported_type(field, &table.schema))
                            })
                        })
                        .collect::<Vec<_>>(),
                ),
                Err(e) => {
                    fail_statement(state, stream, "42601", &e).await?;
                    return Ok(());
                }
            }
        }
        QueryPurpose::UpdateRow(sql) | QueryPurpose::DeleteRow(sql) => {
            // The relation is the token after `UPDATE`, or after `DELETE FROM`.
            let skip = if matches!(purpose, QueryPurpose::UpdateRow(_)) {
                1
            } else {
                2
            };
            let name = sql
                .split_whitespace()
                .nth(skip)
                .unwrap_or("")
                .trim_matches('"')
                .to_string();
            // As above: the fields are copied out so no shard read lock is
            // live across the `fail_statement` await.
            let fields = database
                .tables
                .get(&name)
                .map(|t| (t.schema.fields.clone(), t.schema.json_columns.clone()));
            match fields {
                None => None,
                Some((fields, json_columns)) => {
                    match crate::core::rql::lexer_ir::returning_columns(sql, &fields) {
                        Some(Ok(cols)) => Some(
                            cols.iter()
                                .map(|f| (f.name.clone(), reported_type_of(f, &json_columns)))
                                .collect::<Vec<_>>(),
                        ),
                        Some(Err(e)) => {
                            fail_statement(state, stream, "42703", &e).await?;
                            return Ok(());
                        }
                        None => None,
                    }
                }
            }
        }
        QueryPurpose::InsertRow(ir) => match database
            .tables
            .get(&ir.table_name)
            .map(|t| (t.schema.fields.clone(), t.schema.json_columns.clone()))
        {
            None => None,
            Some((fields, json_columns)) => {
                match crate::core::rql::lexer_ir::returning_columns(&ir.query, &fields) {
                    Some(Ok(cols)) => Some(
                        cols.iter()
                            .map(|f| (f.name.clone(), reported_type_of(f, &json_columns)))
                            .collect::<Vec<_>>(),
                    ),
                    Some(Err(e)) => {
                        fail_statement(state, stream, "42703", &e).await?;
                        return Ok(());
                    }
                    None => None,
                }
            }
        },
        // `SHOW BACKUPS` and `RESTORE` return rows that belong to no table, so
        // their column names come from the statement rather than from a schema.
        #[cfg(feature = "s3_backup")]
        QueryPurpose::Backup(statement) => crate::core::rql::backup_exec::result_fields(statement),
        _ => None,
    };
    // A `UNION`'s fields come from its first branch; its integer columns are
    // widened here as `describe_query` widens them, so they match the rows.
    let select_fields = match &purpose {
        QueryPurpose::QueryRows(qr) => {
            select_fields.map(|fields| match crate::core::rql::join::union_branches(&qr.query) {
                // The statement runs as it is here, its CTEs and derived
                // tables already materialised into real relations.
                Some(branches) => widen_fields_by(
                    crate::core::rql::set_ops::widened_branch_integers(&branches, database, &[]),
                    fields,
                ),
                None => fields,
            })
        }
        _ => select_fields,
    };

    crate::core::rql::prepared::note_description(&select_fields, &select_scales);

    // `execute` consumes QueryPurpose, but we still want `purpose_kind`/`select_fields`.
    // Re-recognize the query to get an owned QueryPurpose to pass into the engine.
    //
    // A statement that does not re-recognise fails on its own rather than
    // closing the connection: an `Err` here would end the session, and the
    // client would see only its socket close.
    let Some(purpose_exec) = recognize_query_purpose(stmt) else {
        fail_statement(state, stream, "XX000", "internal error: the statement could not be re-read for execution").await?;
        return Ok(());
    };

    // Inside a block the statement joins the session's transaction, so it sees
    // the block's own earlier writes and its effects stay invisible to everyone
    // else until COMMIT. Outside one, `execute` opens and closes a transaction
    // of its own.
    let resp = match state.current_tx() {
        Some(tx) => {
            crate::core::rql::executor::execute_in_txn(purpose_exec, database.clone(), tx).await
        }
        None => {
            crate::core::rql::executor::execute_with_sync(
                purpose_exec,
                database.clone(),
                state.synchronous_commit(),
            )
            .await
        }
    };
    send_engine_response(
        state,
        stream,
        resp,
        purpose_kind,
        select_fields,
        select_scales,
        result_formats,
        omit_description,
    )
    .await
}

/// Turns the engine's answer to a statement into the wire messages that
/// answer the client.
#[allow(clippy::too_many_arguments)]
async fn send_engine_response<S>(
    state: &mut SessionState,
    stream: &mut S,
    resp: Vec<u8>,
    purpose_kind: PurposeKind,
    select_fields: Option<Vec<(String, DbType)>>,
    select_scales: Option<Vec<u32>>,
    result_formats: &[i16],
    omit_description: bool,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    if resp.is_empty() {
        fail_statement(state, stream, "XX000", "empty engine response").await?;
        return Ok(());
    }

    match resp[0] {
        0 => {
            // OK. Some engine paths also append an 8-byte LE rows-affected count after the tag.
            let rows_affected = if resp.len() >= 9 {
                let mut arr = [0u8; 8];
                arr.copy_from_slice(&resp[1..9]);
                Some(u64::from_le_bytes(arr))
            } else {
                None
            };

            // What a write reported as affected counts as this statement's
            // rows, which is what `pg_stat_statements.rows` means too.
            #[cfg(feature = "query_stats")]
            crate::core::query_stats::note_rows(rows_affected.unwrap_or(0));

            let tag = command_tag(purpose_kind, rows_affected);

            send_command_complete(stream, &tag).await?;
        }
        1 => {
            if resp.len() < 9 {
                fail_statement(state, stream, "XX000", "invalid rows-affected response").await?;
                return Ok(());
            }
            let mut arr = [0u8; 8];
            arr.copy_from_slice(&resp[1..9]);
            let rows = u64::from_le_bytes(arr);
            #[cfg(feature = "query_stats")]
            crate::core::query_stats::note_rows(rows);
            let tag = match purpose_kind {
                PurposeKind::DeleteRow => format!("DELETE {}", rows),
                PurposeKind::InsertRow => format!("INSERT 0 {}", rows),
                PurposeKind::UpdateRow => format!("UPDATE {}", rows),
                _ => format!("UPDATE {}", rows),
            };
            send_command_complete(stream, &tag).await?;
        }
        2 => {
            let rows_bytes = &resp[1..];
            // Checked whole before anything is sent; the rows are then encoded
            // from these bytes in place rather than decoded into `Row`s.
            // A damaged result fails the statement, not the session.
            let row_count = match validate_serialized_rows(rows_bytes) {
                Ok(n) => n,
                Err(e) => {
                    log::error!("the engine returned a damaged result: {}", e);
                    fail_statement(state, stream, "XX000", &format!("internal error: damaged result ({})", e)).await?;
                    return Ok(());
                }
            };
            // `INSERT ... RETURNING` sends rows but must still complete with an
            // INSERT tag; a client that saw "SELECT n" would not count the write.
            let tag = match purpose_kind {
                PurposeKind::InsertRow => format!("INSERT 0 {}", row_count),
                PurposeKind::UpdateRow => format!("UPDATE {}", row_count),
                PurposeKind::DeleteRow => format!("DELETE {}", row_count),
                _ => format!("SELECT {}", row_count),
            };
            send_serialized_query_result(
                stream,
                select_fields.as_deref(),
                select_scales.as_deref(),
                rows_bytes,
                result_formats,
                &tag,
                omit_description,
            )
            .await?;
        }
        3 => {
            let msg = String::from_utf8_lossy(&resp[1..]).to_string();
            // A failure inside a block poisons it: every later statement is
            // refused until COMMIT or ROLLBACK closes it. Carrying on would
            // let the block commit with a hole in it.
            fail_statement(state, stream, sqlstate_for(&msg), &msg).await?;
        }
        _ => {
            fail_statement(state, stream, "XX000", "unknown engine response tag").await?;
        }
    }

    Ok(())
}

fn read_only_message(verb: &str) -> String {
    format!("cannot execute {} in a read-only transaction", verb)
}

/// The SQLSTATE an engine error is reported under.
///
/// The engine returns its failures as text, and every one used to reach the
/// client as `XX000 internal_error`. Drivers do not read the text: sqlx's
/// `is_unique_violation`, sea-orm's conflict handling, Hibernate's
/// `ConstraintViolationException` and every retry-on-`40001` loop branch on
/// the five-character code, so a unique violation under `XX000` is one an
/// application cannot tell from a crash. Classified here, at the one place the
/// engine's answer is turned into a wire message, by the phrases the engine
/// uses — which are PostgreSQL's own where the condition has one.
pub(crate) fn sqlstate_for(message: &str) -> &'static str {
    let m = message.to_ascii_lowercase();
    let has = |needle: &str| m.contains(needle);

    // A write refused at commit because its transaction id could not be
    // reserved (`TransactionManager::check_ids_reserved`). Decided first: the
    // message quotes an operating-system error, whose words could steer the
    // branches below. `disk_full` when the disk is, `io_error` otherwise.
    if has("could not reserve transaction ids") {
        return if has("(the disk is full)") { "53100" } else { "58030" };
    }

    // A literal that is not JSON. Decided first, because the message quotes
    // the literal and the parser's reason, and either can contain the words
    // the branches below look for: `'{"relation x does not exist":'` would
    // otherwise come back as 42P01. Not a prefix test: the plan compiler
    // wraps the message ("could not compile the query plan: …").
    if has("invalid input syntax for type json") {
        return "22P02";
    }
    // `undefined_function`: no operator between the two types. Decided before
    // the "does not exist" branches below, which the names of the types could
    // otherwise steer.
    if has("operator does not exist") {
        return "42883";
    }
    // `invalid_column_reference`: an ON CONFLICT target no unique key
    // matches (a partitioned table's that leaves out a key column, S18).
    // Decided early: the message quotes the column names.
    if has("no unique or exclusion constraint matching the on conflict specification") {
        return "42P10";
    }
    // `undefined_object`: `ON CONFLICT ON CONSTRAINT` naming no constraint.
    if has("constraint \"") && has("\" for table \"") && has("does not exist") {
        return "42704";
    }
    // `57014` is `query_canceled`, which is what a driver watches for to tell
    // "the server gave up on this" apart from "this statement is wrong".
    if has("canceling statement") {
        "57014"
    } else if has("deadlock detected") {
        // `40P01`, which drivers retry like `40001`: one party to the cycle
        // was failed so the others could proceed.
        "40P01"
    } else if has("could not serialize access")
        || has("reclaimed while the query was reading")
        // A uniqueness check that met another transaction's unfinished write
        // of the same key, and outlasted the replays meant to wait it out.
        // Nothing was applied, so re-running is the right response, and only
        // `40001` tells a client so. Two blocks that each insert a key the
        // other already wrote end here, where PostgreSQL would report a
        // deadlock.
        || has("is being written by another transaction")
    {
        // The reclaimed-row guard is a serialisation failure in the sense that
        // matters to a client: the statement applied nothing and re-running it
        // is the correct response. `40001` says exactly that, and every
        // Postgres-shaped retry loop already branches on it — which beats every
        // caller learning one more bespoke message to match on.
        "40001"
    } else if has("duplicate value for unique") || has("duplicate key value") {
        "23505"
    } else if has("violates foreign key constraint")
        || (has("foreign key") && has("does not exist"))
    {
        "23503"
    } else if has("violates not-null constraint") || has("not null") && has("null value") {
        "23502"
    } else if has("cannot affect row a second time") {
        // cardinality_violation.
        "21000"
    } else if has("violates check constraint") || (has("check constraint") && has("is violated by some row")) {
        "23514"
    } else if has("permission denied") || has("is not a superuser") {
        "42501"
    } else if has("relation") && has("does not exist")
        || has("table not found")
        || (has("table ") && has(" not found"))
    {
        "42P01"
    } else if has("column") && has("does not exist") {
        "42703"
    } else if has("table already exists") || (has("relation") && has("already exists")) {
        "42P07"
    } else if has("column") && has("already exists") {
        "42701"
    } else if has("already exists") {
        "42710"
    } else if has("date/time field value out of range") {
        // Before the generic "out of range": a date/time field is 22008.
        "22008"
    } else if has("invalid input syntax for type timestamp")
        || has("invalid input syntax for type date")
        || has("invalid input syntax for type time")
        || has("invalid input syntax for type interval")
    {
        // PostgreSQL's `invalid_datetime_format`, not the generic 22P02.
        "22007"
    } else if has("out of range") || has("numeric field overflow") {
        "22003"
    } else if has("division by zero") {
        "22012"
    } else if has("invalid regular expression") {
        "2201B"
    } else if has("statement too complex") {
        "54001"
    } else if has("invalid hexadecimal") {
        // `byteain`'s hex errors are `invalid_parameter_value`.
        "22023"
    } else if has("invalid input syntax")
        || has("could not parse")
        || has("cannot parse")
        || has("malformed array literal")
    {
        "22P02"
    } else if has("in column generation expression") && has("subquery") {
        // PostgreSQL's own codes for a generation expression it refuses:
        // a subquery is a missing feature, the rest a bad definition.
        "0A000"
    } else if has("both default and generation expression") {
        // PostgreSQL reports the two clauses together as a syntax error.
        "42601"
    } else if has("generation expression") {
        "42P17"
    } else if has("can only be updated to default") || has("cannot insert a non-default value") {
        // `generated_always`: a value written to a generated column.
        "428C9"
    } else if has("not supported") || has("unsupported") || has("not a supported") {
        // The last is the row-lock parser's: "'FOR UPDATE NOWAIT SKIP
        // LOCKED' is not a supported locking clause", which was XX000.
        "0A000"
    } else if has("syntax error") || has("unexpected") || has("expected ") {
        "42601"
    } else {
        "XX000"
    }
}

fn read_cstring_owned(bytes: &[u8], start: usize) -> io::Result<(String, usize)> {
    if start > bytes.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid cstring offset",
        ));
    }
    let rel_end = bytes[start..]
        .iter()
        .position(|&b| b == 0)
        .unwrap_or(bytes.len() - start);
    let end = start + rel_end;
    let s = std::str::from_utf8(&bytes[start..end])
        .unwrap_or("")
        .to_string();
    let next = (end + 1).min(bytes.len());
    Ok((s, next))
}

fn read_i16_be_from(bytes: &[u8], start: usize) -> io::Result<(i16, usize)> {
    let end = start + 2;
    if bytes.len() < end {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "unexpected EOF"));
    }
    let mut buf = [0u8; 2];
    buf.copy_from_slice(&bytes[start..end]);
    Ok((i16::from_be_bytes(buf), end))
}

fn read_i32_be_from(bytes: &[u8], start: usize) -> io::Result<(i32, usize)> {
    let end = start + 4;
    if bytes.len() < end {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "unexpected EOF"));
    }
    let mut buf = [0u8; 4];
    buf.copy_from_slice(&bytes[start..end]);
    Ok((i32::from_be_bytes(buf), end))
}

/// Which statement ran, reduced to what the command tag depends on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PurposeKind {
    CreateTable,
    DropTable,
    AlterTable,
    Vacuum,
    LockTable,
    InsertRow,
    UpdateRow,
    DeleteRow,
    QueryRows,
    Other,
}

/// The `CommandComplete` tag PostgreSQL sends for a statement.
///
/// A client reads this back to confirm which statement ran — migration runners
/// and `psql` both display it — so tagging everything `OK` loses information a
/// real client uses.
fn command_tag(kind: PurposeKind, rows_affected: Option<u64>) -> String {
    match (kind, rows_affected) {
        (PurposeKind::CreateTable, _) => "CREATE TABLE".to_string(),
        (PurposeKind::DropTable, _) => "DROP TABLE".to_string(),
        // PostgreSQL tags add, drop and rename alike.
        (PurposeKind::AlterTable, _) => "ALTER TABLE".to_string(),
        (PurposeKind::Vacuum, _) => "VACUUM".to_string(),
        (PurposeKind::LockTable, _) => "LOCK TABLE".to_string(),
        (PurposeKind::InsertRow, Some(n)) => format!("INSERT 0 {}", n),
        (PurposeKind::UpdateRow, Some(n)) => format!("UPDATE {}", n),
        (PurposeKind::DeleteRow, Some(n)) => format!("DELETE {}", n),
        // Conventional tags when the engine reported no count.
        (PurposeKind::InsertRow, None) => "INSERT 0 0".to_string(),
        (PurposeKind::UpdateRow, None) => "UPDATE 0".to_string(),
        (PurposeKind::DeleteRow, None) => "DELETE 0".to_string(),
        _ => "OK".to_string(),
    }
}

async fn handle_simple_query<S>(
    database: &Arc<Database>,
    state: &mut SessionState,
    stream: &mut S,
    query: &str,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // The simple query protocol may carry several statements separated by ';'.
    // Splitting on the character alone cut inside string literals and comments:
    // a migration whose comment ended a sentence with a semicolon — "identifies
    // the same rows;" — was chopped in half, and the leading fragment parsed as
    // nothing, reported as "syntax error or unsupported query" about a
    // perfectly good statement.
    // Escape and dollar-quoted strings become standard strings before the
    // splitter or anything after it reads the text; see `sql_scan`.
    let normalized = match crate::core::sql_scan::normalize_string_literals(query) {
        Ok(text) => text,
        Err(e) => {
            fail_statement(state, stream, "22025", &e).await?;
            let status = state.ready_status();
            return send_ready_for_query(stream, status).await;
        }
    };
    let statements = split_statements(&normalized);

    for stmt in statements {
        execute_recorded(database, state, stream, &stmt, &[], false, None).await?;
        // PostgreSQL stops a multi-statement string at the first failure.
        // Inside a block the poisoning refuses the rest; outside one nothing
        // did, so a migration sent as one string had its first statement fail
        // and the remainder applied anyway — a partial migration reported,
        // statement by statement, as an error followed by successes.
        //
        // What PostgreSQL also does, and this does not yet: run the whole
        // string as one implicit transaction, so the statements *before* the
        // failure are rolled back too. Outside a block each one here has
        // already committed.
        if state.statement_failed {
            break;
        }
    }

    let status = state.ready_status();
    send_ready_for_query(stream, status).await?;
    Ok(())
}

/// Splits a simple-query payload into statements on top-level semicolons.
///
/// A `;` inside a string literal, a quoted identifier, or a comment is text,
/// not a separator. Fragments that hold nothing but comments or whitespace are
/// dropped rather than sent on to be rejected.
fn split_statements(query: &str) -> Vec<String> {
    let bytes = query.as_bytes();
    let mut out = Vec::new();
    let mut start = 0usize;
    let mut i = 0usize;
    let mut quote: Option<u8> = None;

    while i < bytes.len() {
        let c = bytes[i];
        if let Some(q) = quote {
            if c == q {
                quote = None;
            }
            i += 1;
            continue;
        }
        match c {
            b'\'' | b'"' => {
                quote = Some(c);
                i += 1;
            }
            b'-' if bytes.get(i + 1) == Some(&b'-') => {
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                i += 2;
                while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                    i += 1;
                }
                i = (i + 2).min(bytes.len());
            }
            b';' => {
                out.push(query[start..i].to_string());
                i += 1;
                start = i;
            }
            _ => i += 1,
        }
    }
    if start < query.len() {
        out.push(query[start..].to_string());
    }

    out.into_iter()
        .filter(|s| {
            // Comment-only fragments carry no statement.
            !crate::core::rql::lexer_s1::strip_sql_comments(s)
                .trim()
                .is_empty()
        })
        .map(|s| s.trim().to_string())
        .collect()
}

/// The remainder of `lower` after `keyword`, if it starts with it as whole words.
///
/// Compares against the lowercased statement but returns a slice of it, so the
/// caller still has to take the name from the original text if case matters —
/// which for a savepoint name it does not here, because names are matched
/// case-insensitively throughout.
fn strip_keyword<'a>(lower: &'a str, keyword: &str) -> Option<&'a str> {
    let rest = lower.strip_prefix(keyword)?;
    // A keyword has to end at a word boundary, or `RELEASEFOO` would parse as
    // `RELEASE foo`.
    if rest.is_empty() || rest.starts_with(|c: char| c.is_whitespace()) {
        Some(rest.trim())
    } else {
        None
    }
}

/// The identifier in a savepoint clause, unquoted.
fn savepoint_name(rest: &str) -> Option<String> {
    let name = rest.trim().trim_end_matches(';').trim();
    if name.is_empty() {
        return None;
    }
    // Only the first word: `ROLLBACK TO SAVEPOINT x` may be followed by nothing
    // else, but a trailing token should not silently become part of the name.
    let first = name.split_whitespace().next()?;
    let unquoted = first.trim_matches('"');
    if unquoted.is_empty() {
        None
    } else {
        Some(unquoted.to_string())
    }
}

/// Answers `BEGIN` / `COMMIT` / `ROLLBACK` against the transaction manager.
///
/// These used to be accepted and ignored, which is the most dangerous thing a
/// database can do with them: an application that wrapped work in a transaction
/// and rolled it back had already committed every statement.
///
/// Opens a block on the session with the given characteristics.
fn open_block(
    database: &Arc<Database>,
    state: &mut SessionState,
    level: IsolationLevel,
    read_only: bool,
) {
    let id = database.transactions.begin();
    if level == IsolationLevel::RepeatableRead {
        database.transactions.pin_snapshot(id);
    }
    state.transaction = Some(SessionTransaction {
        id,
        // A chained block starts fresh: `SET LOCAL` belongs to the block that
        // ran it, and `COMMIT AND CHAIN` carries the characteristics over, not
        // the settings.
        local_synchronous_commit: None,
        local_idle_in_transaction: None,
        local_idle_session: None,
        level,
        read_only,
        savepoints: Vec::new(),
        failed: false,
        owner: crate::core::mvcc::owner::OwnerRelease::for_root(&database.row_locks, id),
    });
}

/// Returns `Ok(true)` when the statement was transaction control.
async fn handle_transaction_control<S>(
    database: &Arc<Database>,
    state: &mut SessionState,
    stream: &mut S,
    stmt: &str,
) -> io::Result<bool>
where
    S: AsyncWrite + Unpin,
{
    let lower = stmt
        .trim()
        .trim_end_matches(';')
        .trim()
        .to_ascii_lowercase();
    let lower = lower.split_whitespace().collect::<Vec<_>>().join(" ");

    // `BEGIN [WORK|TRANSACTION] [modes]` / `START TRANSACTION [modes]`.
    let begin_modes = [
        "begin transaction",
        "begin work",
        "begin",
        "start transaction",
    ]
    .iter()
    .find_map(|prefix| {
        let rest = lower.strip_prefix(prefix)?;
        (rest.is_empty() || rest.starts_with(' ')).then(|| rest.trim().to_string())
    });

    // `COMMIT`/`ROLLBACK [WORK|TRANSACTION] [AND [NO] CHAIN]`: the block is
    // closed and, with `AND CHAIN`, a new one opened with the same
    // characteristics before the client is answered.
    let (lower, chain) = match lower.strip_suffix(" and chain") {
        Some(head) => (head.to_string(), true),
        None => match lower.strip_suffix(" and no chain") {
            Some(head) => (head.to_string(), false),
            None => (lower, false),
        },
    };
    let is_commit = lower == "commit"
        || lower == "commit work"
        || lower == "commit transaction"
        || lower == "end"
        || lower == "end work"
        || lower == "end transaction";
    let is_rollback = lower == "rollback"
        || lower == "rollback work"
        || lower == "rollback transaction"
        || lower == "abort";
    if chain && !is_commit && !is_rollback {
        send_error_response(
            stream,
            "42601",
            "AND CHAIN is only valid on COMMIT or ROLLBACK",
        )
        .await?;
        return Ok(true);
    }

    if let Some(modes) = begin_modes {
        let modes = match TransactionModes::parse(&modes) {
            Ok(m) => m,
            Err(e) => {
                send_error_response(stream, "42601", &format!("syntax error in BEGIN: {}", e))
                    .await?;
                return Ok(true);
            }
        };
        if state.transaction.is_some() {
            // PostgreSQL warns and carries on rather than failing: nesting is
            // not supported, but neither is losing the block already open.
            send_notice(
                stream,
                "25001",
                "there is already a transaction in progress",
            )
            .await?;
        } else {
            // Characteristics named on the BEGIN itself, else whatever the
            // session was set to, else the defaults.
            open_block(
                database,
                state,
                modes.level.unwrap_or(state.default_isolation),
                modes.read_only.unwrap_or(state.default_read_only),
            );
        }
        send_command_complete(stream, "BEGIN").await?;
        return Ok(true);
    }

    // The characteristics a chained block inherits.
    let chain_into = chain
        .then(|| state.transaction.as_ref().map(|t| (t.level, t.read_only)))
        .flatten();

    if is_commit {
        match state.transaction.take() {
            None => {
                send_notice(stream, "25P01", "there is no transaction in progress").await?;
                send_command_complete(stream, "COMMIT").await?;
            }
            Some(t) if t.failed => {
                state.end_block_settings(&t);
                // A block that failed cannot commit. PostgreSQL reports the
                // rollback rather than pretending the work landed.
                //
                // The aborted outcome only has to reach disk if the block wrote
                // something for it to disown; a block that only read leaves
                // nothing behind for a restart to misread.
                let wrote = database.transactions.wrote_family(t.id);
                database.abort_transaction(t.id);
                if wrote {
                    flush_outcomes(database);
                }
                t.owner.release();
                send_command_complete(stream, "ROLLBACK").await?;
            }
            Some(t) => {
                state.end_block_settings(&t);
                // Did the block put anything on disk? Asked before the commit,
                // which retires the transaction and forgets the answer.
                //
                // A block that only read needs no durable outcome record:
                // nothing on disk carries its id, so a restart that finds no
                // record of it reads it as aborted, which is the truth. A
                // read-only block therefore commits without touching the disk
                // at all. This is the runtime answer, not the `READ ONLY` the
                // session may have declared — a block that declared nothing and
                // wrote nothing gets the same saving.
                let wrote = database.transactions.wrote_family(t.id);

                if !wrote {
                    let family = database.commit_transaction(t.id);
                    t.owner.release();
                    database.publish_transaction(family).await;
                    if let Some((level, read_only)) = chain_into {
                        open_block(database, state, level, read_only);
                    }
                    send_command_complete(stream, "COMMIT").await?;
                    return Ok(true);
                }

                // The block's rows on the other storage locations are synced
                // before the commit: once committed, any session's root flush
                // can carry the outcome to disk, and it must not get there
                // ahead of them. See `Database::commit_written_transaction`.
                let family = match database.commit_written_transaction(t.id) {
                    Ok(family) => family,
                    Err(e) => {
                        log::error!("Could not make the block's rows durable: {}", e);
                        database.abort_transaction(t.id);
                        flush_outcomes(database);
                        t.owner.release();
                        let message = format!(
                            "could not commit: the rows written could not be made durable ({})",
                            e
                        );
                        // 58030 unless the message names a better code: a
                        // refused id reservation on a full disk is 53100.
                        let code = match sqlstate_for(&message) {
                            "XX000" => "58030",
                            code => code,
                        };
                        send_error_response(stream, code, &message).await?;
                        state.transaction = None;
                        state.statement_failed = false;
                        return Ok(true);
                    }
                };

                // The outcome record goes into the log without a sync of its
                // own, and the sync below covers it along with the block's
                // rows. Everything written inside the block deferred its sync
                // so the block pays one instead of one per row, and the outcome
                // now rides along on that same one rather than adding a second.
                //
                // A commit whose outcome record cannot be written is not a
                // commit: a restart would read the transaction as unfinished and
                // its rows as debris. Reporting success would be reporting a
                // write that silently is not there, so this fails and rolls back
                // — which is what the restart would have concluded anyway.
                if let Err(e) = database.transactions.flush_deferred() {
                    log::error!("Could not record the transaction outcome: {}", e);
                    database.abort_transaction(t.id);
                    t.owner.release();
                    send_error_response(
                        stream,
                        "58030",
                        &format!(
                            "could not commit: the transaction outcome could not be made \
                             durable ({}); it has been rolled back",
                            e
                        ),
                    )
                    .await?;
                    return Ok(true);
                }

                // This is the point durability was promised, so this is where it
                // is taken. A failure is reported as a failed commit rather than
                // silently downgraded.
                //
                // Unless this session set `synchronous_commit = off`, in which
                // case the promise was never made: the records are in the log
                // and the log writer syncs them within `wal_writer_delay_ms`.
                if state.synchronous_commit() {
                    if let Err(e) = database.sync_own_location() {
                        log::error!("Could not make the block's rows durable: {}", e);
                        database.abort_transaction(t.id);
                        flush_outcomes(database);
                        t.owner.release();
                        send_error_response(
                            stream,
                            "58030",
                            &format!(
                                "could not commit: the rows written could not be made durable ({})",
                                e
                            ),
                        )
                        .await?;
                        state.transaction = None;
                        state.statement_failed = false;
                        return Ok(true);
                    }
                }

                // The last durability step is done: the block's table locks go
                // now, before the reply (finding L4).
                t.owner.release();
                // Durable, so the block's writes may now go to the other
                // instances. Before the flush they could have been
                // published and then rolled back here.
                database.publish_transaction(family).await;
                if let Some((level, read_only)) = chain_into {
                    open_block(database, state, level, read_only);
                }
                send_command_complete(stream, "COMMIT").await?;
            }
        }
        return Ok(true);
    }

    // `SET TRANSACTION ISOLATION LEVEL ...` applies to the open block;
    // `SET SESSION CHARACTERISTICS AS TRANSACTION ...` sets what later blocks
    // inherit. Both used to fall through to the blanket `SET` no-op, so asking
    // for a stricter level silently got the default.
    let session_modes = lower
        .strip_prefix("set session characteristics as transaction")
        .map(|rest| (true, rest.trim()));
    let transaction_modes = lower
        .strip_prefix("set transaction")
        .filter(|rest| rest.is_empty() || rest.starts_with(' '))
        .map(|rest| (false, rest.trim()));
    if let Some((for_session, rest)) = session_modes.or(transaction_modes) {
        let modes = match TransactionModes::parse(rest) {
            Ok(m) => m,
            Err(e) => {
                send_error_response(
                    stream,
                    "42601",
                    &format!("syntax error in SET TRANSACTION: {}", e),
                )
                .await?;
                return Ok(true);
            }
        };
        if for_session {
            if let Some(level) = modes.level {
                state.default_isolation = level;
            }
            if let Some(read_only) = modes.read_only {
                state.default_read_only = read_only;
            }
        } else {
            match state.transaction.as_mut() {
                Some(t) => {
                    if let Some(level) = modes.level {
                        // PostgreSQL refuses this once the transaction has done
                        // any work, because its snapshot may already be fixed.
                        // Here the same applies for the same reason.
                        if t.level == IsolationLevel::RepeatableRead
                            && level == IsolationLevel::ReadCommitted
                        {
                            send_error_response(
                                stream,
                                "25001",
                                "cannot change the isolation level of a transaction that has \
                                 already fixed its snapshot",
                            )
                            .await?;
                            return Ok(true);
                        }
                        t.level = level;
                        if level == IsolationLevel::RepeatableRead {
                            database.transactions.pin_snapshot(t.id);
                        }
                    }
                    if let Some(read_only) = modes.read_only {
                        t.read_only = read_only;
                    }
                }
                // Outside a block it sets the next one, which is what
                // PostgreSQL does for `SET TRANSACTION` with no transaction.
                None => {
                    if let Some(level) = modes.level {
                        state.default_isolation = level;
                    }
                    if let Some(read_only) = modes.read_only {
                        state.default_read_only = read_only;
                    }
                }
            }
        }
        send_command_complete(stream, "SET").await?;
        return Ok(true);
    }

    // ---- savepoints ----------------------------------------------------
    //
    // A savepoint is a sub-transaction: writes after it carry an id of their
    // own, so `ROLLBACK TO` can discard them while the block carries on. sea-orm
    // and every ORM that offers nested transactions builds them out of these.
    if let Some(rest) = strip_keyword(&lower, "savepoint") {
        let Some(name) = savepoint_name(rest) else {
            send_error_response(stream, "42601", "SAVEPOINT requires a name").await?;
            return Ok(true);
        };
        // Read before the borrow below, which takes `state` mutably.
        let established_with = state.synchronous_commit;
        match state.transaction.as_mut() {
            None => {
                // PostgreSQL refuses rather than opening a block implicitly: a
                // savepoint with nothing to roll back to is a mistake worth
                // reporting.
                send_error_response(
                    stream,
                    "25P01",
                    "SAVEPOINT can only be used in a transaction block",
                )
                .await?;
            }
            Some(t) => {
                let parent = t.savepoints.last().map(|sp| sp.id).unwrap_or(t.id);
                let id = database.transactions.begin_savepoint(parent);
                // A repeated name shadows the older rather than replacing it, so
                // rolling back to it leaves the older one standing. Same as
                // PostgreSQL.
                t.savepoints.push(Savepoint {
                    name,
                    id,
                    synchronous_commit: established_with,
                });
                send_command_complete(stream, "SAVEPOINT").await?;
            }
        }
        return Ok(true);
    }

    if let Some(rest) = strip_keyword(&lower, "rollback to savepoint")
        .or_else(|| strip_keyword(&lower, "rollback to"))
    {
        let Some(name) = savepoint_name(rest) else {
            send_error_response(stream, "42601", "ROLLBACK TO requires a savepoint name").await?;
            return Ok(true);
        };
        // Set inside the borrow below and applied after it, since restoring it
        // writes to `state` while the block is still borrowed from it.
        let mut restore_sync: Option<Option<bool>> = None;
        match state.transaction.as_mut() {
            None => {
                send_error_response(stream, "25P01", "there is no transaction in progress").await?;
            }
            Some(t) => {
                // Innermost match wins, so a shadowed name resolves to the
                // savepoint that shadowed it.
                match t.savepoints.iter().rposition(|sp| sp.name == name) {
                    None => {
                        send_error_response(
                            stream,
                            "3B001",
                            &format!("savepoint \"{}\" does not exist", name),
                        )
                        .await?;
                    }
                    Some(index) => {
                        // Everything after it goes, and so does its own work —
                        // including a `SET LOCAL` made after it. Captured here
                        // and applied once the borrow of `state` ends.
                        restore_sync = Some(t.savepoints[index].synchronous_commit);
                        let target = t.savepoints[index].id;
                        database.transactions.rollback_savepoint(target);
                        t.savepoints.truncate(index);

                        let parent = t.savepoints.last().map(|sp| sp.id).unwrap_or(t.id);
                        let fresh = database.transactions.begin_savepoint(parent);
                        t.savepoints.push(Savepoint {
                            name,
                            id: fresh,
                            // Re-established at the value just restored.
                            synchronous_commit: restore_sync.flatten(),
                        });

                        // The block is usable again: rolling back to a savepoint
                        // is how an application recovers from a failed statement
                        // without losing the whole transaction.
                        t.failed = false;
                        send_command_complete(stream, "ROLLBACK").await?;
                    }
                }
            }
        }
        if let Some(value) = restore_sync {
            state.synchronous_commit = value;
        }
        return Ok(true);
    }

    if let Some(rest) =
        strip_keyword(&lower, "release savepoint").or_else(|| strip_keyword(&lower, "release"))
    {
        let Some(name) = savepoint_name(rest) else {
            send_error_response(stream, "42601", "RELEASE requires a savepoint name").await?;
            return Ok(true);
        };
        match state.transaction.as_mut() {
            None => {
                send_error_response(stream, "25P01", "there is no transaction in progress").await?;
            }
            Some(t) => match t.savepoints.iter().rposition(|sp| sp.name == name) {
                None => {
                    send_error_response(
                        stream,
                        "3B001",
                        &format!("savepoint \"{}\" does not exist", name),
                    )
                    .await?;
                }
                Some(index) => {
                    // Merged into what is below: the writes stand, they simply
                    // can no longer be rolled back on their own.
                    for sp in t.savepoints.drain(index..) {
                        database.transactions.release_savepoint(sp.id);
                    }
                    send_command_complete(stream, "RELEASE").await?;
                }
            },
        }
        return Ok(true);
    }

    if is_rollback {
        match state.transaction.take() {
            None => {
                send_notice(stream, "25P01", "there is no transaction in progress").await?;
            }
            Some(t) => {
                state.end_block_settings(&t);
                // The versions this transaction wrote stay on disk; marking it
                // aborted is what makes them invisible, and reclamation removes
                // them once no snapshot can reach them.
                //
                // A block that wrote nothing has no versions to make invisible,
                // so its aborted record does not have to reach disk either.
                let wrote = database.transactions.wrote_family(t.id);
                database.abort_transaction(t.id);
                if wrote {
                    flush_outcomes(database);
                }
                t.owner.release();
            }
        }
        if let Some((level, read_only)) = chain_into {
            open_block(database, state, level, read_only);
        }
        send_command_complete(stream, "ROLLBACK").await?;
        return Ok(true);
    }

    Ok(false)
}

/// Makes the outcomes durable, reporting rather than swallowing a failure.
///
/// A commit that is not durable is not a commit: the rows survive through the
/// log, but the record of whether they count would not, and they would come
/// back after a restart as debris.
fn flush_outcomes(database: &Arc<Database>) {
    if let Err(e) = database.transactions.flush() {
        log::error!("Could not make the transaction outcome durable: {}", e);
    }
}

/// Recognises `SET <key> = <value>` and `RESET <key>`, in the spellings clients
/// actually send.
///
/// `None` when the statement is about something else, `Some(None)` for a reset
/// or an explicit `DEFAULT`, and `Some(Some(value))` otherwise.
///
/// `SET LOCAL` is accepted and treated as `SET SESSION`. Honouring the
/// difference would mean unwinding the value at the end of every block, and a
/// session that asked for weaker durability inside one and then kept it is a
/// smaller surprise than one that silently ends up stronger than it asked for.
/// Whether a `SET` names `LOCAL`, so it applies to the open block rather than
/// the session.
///
/// [`strip_setting`] deliberately accepts `SESSION` and `LOCAL` alike, because
/// which one was written does not change how the *value* parses. It does change
/// how long the value lives, which is what this answers.
fn is_set_local(lower: &str) -> bool {
    lower
        .trim()
        .strip_prefix("set ")
        .is_some_and(|rest| rest.trim_start().starts_with("local "))
}

/// `statement_timeout`'s value, in milliseconds.
///
/// PostgreSQL takes a bare number as milliseconds, or a number with a unit.
/// `DEFAULT` and `0` both mean no limit, which is stored as zero rather than
/// as absence so that `SHOW` can tell "set to nothing" from "never set".
fn parse_statement_timeout(value: &str) -> Option<u64> {
    let v = value.trim().trim_matches('\'').trim_matches('"').trim();
    if v.eq_ignore_ascii_case("default") {
        return Some(0);
    }
    // A value that is all digits has no unit; `find` returning `None` is that
    // case, not a failure.
    let digits_end = v.find(|c: char| !c.is_ascii_digit()).unwrap_or(v.len());
    if digits_end == 0 {
        return None;
    }
    let (number, unit) = (&v[..digits_end], v[digits_end..].trim());
    let n: u64 = number.parse().ok()?;
    let multiplier = match unit.to_ascii_lowercase().as_str() {
        "" | "ms" => 1,
        "s" => 1_000,
        "min" => 60_000,
        "h" => 3_600_000,
        "d" => 86_400_000,
        _ => return None,
    };
    n.checked_mul(multiplier)
}

fn strip_setting(lower: &str, key: &str) -> Option<Option<String>> {
    let s = lower.trim().trim_end_matches(';').trim();

    if let Some(rest) = s.strip_prefix("reset ") {
        return (rest.trim() == key).then_some(None);
    }

    let rest = s.strip_prefix("set ")?;
    let rest = rest
        .strip_prefix("session ")
        .or_else(|| rest.strip_prefix("local "))
        .unwrap_or(rest);
    let rest = rest.strip_prefix(key)?;

    // `= value`, `to value`, or nothing we recognise. The last case matters:
    // without it a different key that merely starts with this one's name —
    // `synchronous_commit_timeout`, say — would be swallowed here.
    let mut value = rest.trim();
    if let Some(r) = value.strip_prefix('=') {
        value = r.trim();
    } else if let Some(r) = value.strip_prefix("to ") {
        value = r.trim();
    } else if !value.is_empty() {
        return None;
    }

    if value.is_empty() {
        return None;
    }
    if value == "default" {
        return Some(None);
    }
    Some(Some(value.to_string()))
}

/// Applies `SET`/`RESET` of one of the idle timeouts.
///
/// `value` is what [`strip_setting`] returned: `None` for `RESET` or
/// `DEFAULT`, which go back to the server's default rather than to zero.
async fn set_idle_timeout<S>(
    state: &mut SessionState,
    stream: &mut S,
    lower: &str,
    key: &str,
    kind: IdleKind,
    value: Option<String>,
) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let new = match value {
        None => match kind {
            IdleKind::InTransaction => state.limits.idle_in_transaction_session_timeout,
            IdleKind::Session => state.limits.idle_session_timeout,
        },
        Some(value) => match parse_time_setting(&value, 1) {
            Ok(d) => d,
            Err(e) => {
                return fail_statement(
                    state,
                    stream,
                    "22023",
                    &format!("invalid value for parameter \"{}\": {}", key, e),
                )
                .await;
            }
        },
    };

    // As for `synchronous_commit`: `SET LOCAL` outside a block changes
    // nothing and says so, and inside one records the value to put back —
    // only this setting's, so the other timeout is left to whatever its own
    // statements in the block decide.
    let previous = match kind {
        IdleKind::InTransaction => state.idle_in_transaction_timeout,
        IdleKind::Session => state.idle_session_timeout,
    };
    if is_set_local(lower) {
        match state.transaction.as_mut() {
            None => {
                send_notice(
                    stream,
                    "25P01",
                    "SET LOCAL can only be used in transaction blocks",
                )
                .await?;
                return send_command_complete(stream, "SET").await;
            }
            Some(t) => {
                let record = match kind {
                    IdleKind::InTransaction => &mut t.local_idle_in_transaction,
                    IdleKind::Session => &mut t.local_idle_session,
                };
                record.get_or_insert(previous);
            }
        }
    } else if let Some(t) = state.transaction.as_mut() {
        // A plain `SET` (or `RESET`) after a `SET LOCAL` of the same setting
        // supersedes it: PostgreSQL keeps the session value once the block
        // commits. Leaving the record would put the pre-block value back and
        // silently drop the timeout the client just asked for.
        match kind {
            IdleKind::InTransaction => t.local_idle_in_transaction = None,
            IdleKind::Session => t.local_idle_session = None,
        }
    }

    match kind {
        IdleKind::InTransaction => state.idle_in_transaction_timeout = new,
        IdleKind::Session => state.idle_session_timeout = new,
    }
    let tag = if lower.trim_start().starts_with("reset") {
        "RESET"
    } else {
        "SET"
    };
    send_command_complete(stream, tag).await
}

/// Parses a time-valued setting as PostgreSQL does: a number, optionally
/// followed by one of `us`, `ms`, `s`, `min`, `h` or `d`, optionally quoted.
/// A bare number is in `base_unit_ms` milliseconds — `1` for the idle
/// timeouts, whose unit is milliseconds, and `1000` for
/// `authentication_timeout`, whose unit is seconds.
///
/// The result must fit PostgreSQL's range for these settings, 0 to
/// `i32::MAX` milliseconds. A positive value too small to round to a whole
/// millisecond is refused rather than rounded to zero, because zero means
/// "disabled" and a client that asked for a bound must not silently get none.
pub fn parse_time_setting(raw: &str, base_unit_ms: u64) -> Result<Duration, String> {
    let trimmed = raw.trim();
    let text = trimmed
        .strip_prefix('\'')
        .and_then(|v| v.strip_suffix('\''))
        .unwrap_or(trimmed)
        .trim();
    let split = text
        .find(|c: char| !(c.is_ascii_digit() || matches!(c, '.' | '-' | '+')))
        .unwrap_or(text.len());
    let (number, unit) = text.split_at(split);
    let number: f64 = number
        .parse()
        .map_err(|_| format!("\"{}\" is not a number with an optional time unit", trimmed))?;
    let unit_ms = match unit.trim() {
        "" => base_unit_ms as f64,
        "us" => 0.001,
        "ms" => 1.0,
        "s" => 1_000.0,
        "min" => 60_000.0,
        "h" => 3_600_000.0,
        "d" => 86_400_000.0,
        other => {
            return Err(format!(
                "\"{}\" is not a valid unit; valid units are \"us\", \"ms\", \"s\", \"min\", \"h\" and \"d\"",
                other
            ));
        }
    };
    let ms = (number * unit_ms).round();
    if !ms.is_finite() || ms < 0.0 || ms > i32::MAX as f64 {
        return Err(format!(
            "\"{}\" is outside the valid range (0 .. {} ms)",
            trimmed,
            i32::MAX
        ));
    }
    if ms == 0.0 && number > 0.0 {
        return Err(format!(
            "\"{}\" is less than one millisecond; use 0 to disable",
            trimmed
        ));
    }
    Ok(Duration::from_millis(ms as u64))
}

/// Renders a time setting as PostgreSQL's `SHOW` does: in the largest unit
/// that represents it exactly, and `0` for zero.
pub fn format_time_setting(value: Duration) -> String {
    let ms = value.as_millis();
    if ms == 0 {
        return "0".to_string();
    }
    for (unit, size) in [
        ("d", 86_400_000),
        ("h", 3_600_000),
        ("min", 60_000),
        ("s", 1_000),
    ] {
        if ms % size == 0 {
            return format!("{}{}", ms / size, unit);
        }
    }
    format!("{}ms", ms)
}

async fn handle_builtin_query<S>(
    state: &mut SessionState,
    stream: &mut S,
    stmt: &str,
    omit_description: bool,
) -> io::Result<bool>
where
    S: AsyncWrite + Unpin,
{
    let s = stmt.trim();
    let s_lower = s.to_ascii_lowercase();

    // `synchronous_commit` is a real setting here, not a runtime parameter to be
    // acknowledged and dropped: it decides whether this session's commits wait
    // for their `fdatasync`. Answered before the branch below, which would hand
    // anything the engine recognises to the executor.
    if let Some(rest) = strip_setting(&s_lower, "synchronous_commit") {
        let local = is_set_local(&s_lower);
        // `SET LOCAL` outside a block has nothing to be local to. PostgreSQL
        // warns and changes nothing, and so does this: applying it to the
        // session instead would make a statement asking for one transaction's
        // durability quietly decide every later transaction's.
        if local && state.transaction.is_none() {
            send_notice(
                stream,
                "25P01",
                "SET LOCAL can only be used in transaction blocks",
            )
            .await?;
            send_command_complete(stream, "SET").await?;
            return Ok(true);
        }
        // Recorded before the change, and only for the first `SET LOCAL` in the
        // block, so what is put back at the end is the value the block started
        // with rather than whatever the previous `SET LOCAL` left.
        if local {
            let previous = state.synchronous_commit;
            if let Some(t) = state.transaction.as_mut() {
                t.local_synchronous_commit.get_or_insert(previous);
            }
        }
        match rest {
            // `RESET`, or `SET ... = DEFAULT`: back to following the server.
            None => {
                state.synchronous_commit = None;
                send_command_complete(stream, "SET").await?;
            }
            Some(value) => match crate::parse_synchronous_commit(&value) {
                Some(on) => {
                    state.synchronous_commit = Some(on);
                    send_command_complete(stream, "SET").await?;
                }
                None => {
                    send_error_response(
                        stream,
                        "22023",
                        &format!(
                            "invalid value for parameter \"synchronous_commit\": \"{}\" \
                             (use on, off, local, remote_write or remote_apply)",
                            value
                        ),
                    )
                    .await?;
                }
            },
        }
        return Ok(true);
    }

    // `statement_timeout` is a real setting here too, for the same reason
    // `synchronous_commit` is: something enforces it. Answered before the
    // generic branch below, which would acknowledge it and drop it — which is
    // what it did, while `SHOW` then reported `0`, so a client that set one was
    // told it had not taken and an operator was told statements were unbounded
    // when they had just been bounded.
    if let Some(rest) = strip_setting(&s_lower, "statement_timeout") {
        match rest {
            // `RESET`, or `SET … = DEFAULT`: back to the server's default,
            // which is no limit.
            None => {
                state.statement_timeout_ms = None;
                send_command_complete(stream, "SET").await?;
            }
            Some(value) => match parse_statement_timeout(&value) {
                Some(ms) => {
                    state.statement_timeout_ms = Some(ms);
                    send_command_complete(stream, "SET").await?;
                }
                None => {
                    send_error_response(
                        stream,
                        "22023",
                        &format!(
                            "invalid value for parameter \"statement_timeout\": \"{}\"",
                            value
                        ),
                    )
                    .await?;
                }
            },
        }
        return Ok(true);
    }

    // The idle timeouts are enforced, so they are real settings too: a `SET`
    // acknowledged and dropped would let a pool believe its sessions are
    // bounded when nothing bounds them.
    for (key, kind) in [
        ("idle_in_transaction_session_timeout", IdleKind::InTransaction),
        ("idle_session_timeout", IdleKind::Session),
    ] {
        if let Some(value) = strip_setting(&s_lower, key) {
            set_idle_timeout(state, stream, &s_lower, key, kind, value).await?;
            return Ok(true);
        }
    }

    // Server-wide, fixed at start-up. `SHOW` reports them, so a `SET` that
    // was acknowledged would leave the two disagreeing; PostgreSQL refuses it.
    for key in ["max_connections", "authentication_timeout"] {
        if strip_setting(&s_lower, key).is_some() {
            fail_statement(
                state,
                stream,
                "55P02",
                &format!(
                    "parameter \"{}\" cannot be changed without restarting the server",
                    key
                ),
            )
            .await?;
            return Ok(true);
        }
    }

    // `SET`/`SHOW` are two things at once: PostgreSQL runtime parameters, which
    // psql and every driver send on connect and which mean nothing here, and the
    // engine's own configuration surface — `SET FILTER ...`, `SHOW REPLICATION`,
    // `SHOW BACKUPS`.
    //
    // Answering them all generically swallowed the second kind: `SET FILTER
    // max_f = 20` was accepted and did nothing, and `SHOW FILTER` came back as
    // an empty column named "filter". Only over the Postgres wire — the rastcp
    // path routes to the executor and was always fine, which is why it went
    // unnoticed.
    //
    // So ask the executor first. Anything it recognises is a real statement and
    // belongs to it; only what it does not recognise is a runtime parameter to
    // be acknowledged and dropped. That keeps every engine statement working
    // without each subsystem having to add its own prefix guard here, which is
    // what three of them were starting to do.
    // `SHOW DATABASES` is answered above the engine, by the catalogue, so
    // `recognize_query_purpose` does not see it — and without this it fell
    // through to the runtime-parameter branch below and came back as an empty
    // column named "databases". Same trap the comment above describes, one
    // layer up.
    if (s_lower.starts_with("set ")
        || s_lower.starts_with("show ")
        || s_lower.starts_with("reset "))
        && (recognize_query_purpose(s).is_some() || parse_database_statement(s).is_some())
    {
        return Ok(false);
    }

    // SET statements (psql uses these on connect).
    if s_lower.starts_with("set ") {
        send_command_complete(stream, "SET").await?;
        return Ok(true);
    }

    // DDL the engine accepts without acting on. Each carries a reason: an index
    // is unnecessary because the engine maintains its own, and the rest name
    // machinery RasterizedDB does not have. Logged rather than silent, so a
    // migration that depended on one is traceable.
    // Never in place of the engine. The no-op list is consulted before the
    // executor, so an entry naming a statement the engine implements would
    // answer `CommandComplete` and run nothing — which is how `VACUUM` came to
    // work over the native protocol and do nothing over this one. The list no
    // longer names such a statement; this makes a future one that does fail
    // safe rather than silently.
    if crate::core::rql::lexer_alter::no_op_reason(s).is_some()
        && recognize_query_purpose(s).is_some()
    {
        return Ok(false);
    }

    if let Some(reason) = crate::core::rql::lexer_alter::no_op_reason(s) {
        log::info!(
            "accepted without acting: {} ({})",
            s.lines().next().unwrap_or(s),
            reason
        );
        let tag = s
            .split_whitespace()
            .take(2)
            .map(|w| w.to_ascii_uppercase())
            .collect::<Vec<_>>()
            .join(" ");
        send_command_complete(stream, &tag).await?;
        return Ok(true);
    }

    // Common introspection queries.
    if s_lower == "select 1" || s_lower == "select 1::int" {
        if !omit_description {
            send_row_description(stream, &[("?column?", DbType::I32)], &[]).await?;
        }
        send_data_row(stream, &[Some(b"1")]).await?;
        send_command_complete(stream, "SELECT 1").await?;
        return Ok(true);
    }

    // The function PostgreSQL's own extension offers for this. Answered here
    // rather than by the row evaluator, which has no way to reach the statement
    // log — the same reason `pg_current_wal_lsn()` is rewritten before it gets
    // there. `RESET QUERY STATS` is the engine's own spelling of it.
    if s_lower.starts_with("select pg_stat_statements_reset(") && s_lower.ends_with(')') {
        if !state.principal.may_administer() {
            fail_statement(
                state,
                stream,
                "42501",
                "permission denied: pg_stat_statements_reset requires superuser",
            )
            .await?;
            return Ok(true);
        }
        #[cfg(feature = "query_stats")]
        crate::core::query_stats::reset();
        if !omit_description {
            send_row_description(stream, &[("pg_stat_statements_reset", DbType::STRING)], &[])
                .await?;
        }
        send_data_row(stream, &[Some(b"")]).await?;
        send_command_complete(stream, "SELECT 1").await?;
        return Ok(true);
    }

    if s_lower == "select version()" {
        if !omit_description {
            send_row_description(stream, &[("version", DbType::STRING)], &[]).await?;
        }
        send_data_row(stream, &[Some(b"rasterizeddb (pgwire) 0.2.0")]).await?;
        send_command_complete(stream, "SELECT 1").await?;
        return Ok(true);
    }

    if s_lower.starts_with("show ") {
        let key = s_lower[5..].trim();
        // A parameter this server does not have is an error, as it is in
        // PostgreSQL. It used to be answered with an empty string under a
        // success tag, so `SHOW transaction_isolation` — which JDBC and psql
        // both issue — told the client the level was "".
        let Some(value) = show_parameter(state, key) else {
            fail_statement(
                state,
                stream,
                "42704",
                &format!("unrecognized configuration parameter \"{}\"", key),
            )
            .await?;
            return Ok(true);
        };

        if !omit_description {
            send_row_description(stream, &[(key, DbType::STRING)], &[]).await?;
        }
        send_data_row(stream, &[Some(value.as_bytes())]).await?;
        send_command_complete(stream, "SHOW").await?;
        return Ok(true);
    }

    Ok(false)
}

/// The value of a run-time parameter as `SHOW` reports it, or `None` for one
/// this server does not have.
///
/// The session-dependent ones are answered from the session: the isolation
/// level and access mode a client set are what it reads back, and
/// `is_superuser` is what the client was authenticated as.
fn show_parameter(state: &SessionState, key: &str) -> Option<String> {
    let level_name = |level: IsolationLevel| match level {
        IsolationLevel::ReadCommitted => "read committed",
        IsolationLevel::RepeatableRead => "repeatable read",
    };
    let on_off = |b: bool| if b { "on" } else { "off" };
    let value = match key {
        "server_version" => "16.3".to_string(),
        "server_version_num" => "160003".to_string(),
        "server_encoding" | "client_encoding" => "UTF8".to_string(),
        "datestyle" => "ISO, MDY".to_string(),
        "intervalstyle" => "postgres".to_string(),
        "standard_conforming_strings" | "integer_datetimes" => "on".to_string(),
        "timezone" | "log_timezone" => "UTC".to_string(),
        "search_path" => "\"$user\", public".to_string(),
        "application_name" => String::new(),
        "max_identifier_length" => "63".to_string(),
        "lc_collate" | "lc_ctype" => "C".to_string(),
        "is_superuser" => on_off(state.principal.may_administer()).to_string(),
        "session_authorization" | "role" => state.principal.name.clone(),
        "transaction_isolation" => level_name(
            state
                .transaction
                .as_ref()
                .map(|t| t.level)
                .unwrap_or(state.default_isolation),
        )
        .to_string(),
        "default_transaction_isolation" => level_name(state.default_isolation).to_string(),
        "transaction_read_only" => on_off(
            state
                .transaction
                .as_ref()
                .map(|t| t.read_only)
                .unwrap_or(state.default_read_only),
        )
        .to_string(),
        "default_transaction_read_only" => on_off(state.default_read_only).to_string(),
        // Reported as this session sees it: its own setting if it made one, the
        // server's otherwise.
        "synchronous_commit" => on_off(state.synchronous_commit()).to_string(),
        "wal_writer_delay" => format!("{}ms", crate::wal_writer_delay_ms()),
        // Reported as this session set it. It used to answer a hardcoded "0"
        // whatever had been set, which reads as "no timeout configured" — so a
        // client that set one, and an operator checking whether statements were
        // bounded, were both told the opposite of the truth.
        "statement_timeout" => match state.statement_timeout_ms {
            Some(ms) => format!("{}ms", ms),
            None => "0".to_string(),
        },
        // Still unenforced, and still answered as disabled, which is what it
        // is. Not grouped with `statement_timeout` any more precisely because
        // that one now means something.
        "lock_timeout" => "0".to_string(),
        // Enforced per session (see `idle_limit`), and reported as this
        // session has them.
        "idle_in_transaction_session_timeout" => {
            format_time_setting(state.idle_in_transaction_timeout)
        }
        "idle_session_timeout" => format_time_setting(state.idle_session_timeout),
        "max_connections" => state.limits.max_connections.to_string(),
        "authentication_timeout" => format_time_setting(state.limits.handshake_timeout),
        _ => return None,
    };
    Some(value)
}

/// The type a column is reported as.
///
/// A JSON column stores a `UUID` referencing the row its document was mapped
/// to, but the client declared it JSON and is handed the document, so it is
/// advertised as text rather than as an identifier.
fn reported_type(
    field: &crate::core::row::schema::SchemaField,
    schema: &crate::core::row::schema::TableSchema,
) -> DbType {
    reported_type_of(field, &schema.json_columns)
}

/// The type a column is advertised as, given the table's JSON columns.
///
/// A JSON column stores a `UUID` pointing at the document's rows, but what goes
/// over the wire is the document as text. Advertising the storage type made a
/// typed client reject the column outright — sqlx refused
/// `Option<serde_json::Value>` against "SQL type UUID" — even though the bytes
/// it received were the JSON. Every path that describes a column has to agree
/// on this, `RETURNING` included.
fn reported_type_of(
    field: &crate::core::row::schema::SchemaField,
    json_columns: &[crate::core::rql::json_map::JsonColumn],
) -> DbType {
    if crate::core::rql::json_map::shadow_for(&field.name, json_columns).is_some() {
        // `jsonb`, not the `uuid` it is stored as and not `text` either. A
        // driver checks the advertised OID against the Rust type it was asked
        // for, so both of those were refused outright — most recently as "Rust
        // type Option<serde_json::Value> (as SQL type JSONB) is not compatible
        // with SQL type TEXT". The value on the wire is the document in both
        // cases; only the name was wrong.
        DbType::JSON
    } else {
        field.db_type.clone()
    }
}

/// A join's output columns, with shredded JSON columns advertised as `jsonb`.
///
/// `JoinPlan::output_columns` reports each item's raw stored type, and a
/// shredded JSON column is stored as the `UUID` pointing at its shadow table —
/// so a joined select advertised `uuid` and every typed client refused the
/// row, while the identical single-relation select was right. The join knows
/// which relation each item came from (`JoinSelectItem::rel` indexes
/// `JoinPlan::relations`), which is what makes the table's JSON columns
/// reachable from here.
fn join_output_columns(
    plan: &crate::core::rql::join::JoinPlan,
    database: &Arc<Database>,
) -> Vec<(String, DbType)> {
    let base = plan.output_columns();

    // Maps one projected position to the type its source column is advertised
    // as, or leaves the type alone when the position holds no column.
    let advertised_at = |position: usize, fallback: DbType| -> DbType {
        let Some(item) = plan.items.get(position) else {
            return fallback;
        };
        // An expression's type is the expression's own, worked out when it was
        // parsed; there is no source column to advertise it as.
        if item.scalar.is_some() || item.literal.is_some() || item.expr.is_some() {
            return fallback;
        }
        let Some(relation) = plan.relations.get(item.rel) else {
            return fallback;
        };
        match database.tables.get(&relation.table) {
            Some(table) => reported_type_of(&item.field, &table.schema.json_columns),
            None => fallback,
        }
    };

    match &plan.aggregate {
        None => base
            .into_iter()
            .enumerate()
            .map(|(position, (name, ty))| (name, advertised_at(position, ty)))
            .collect(),
        // An aggregate over a join folds the *projected* rows, so a grouping
        // key's `write_order` is its position in that projection — which is
        // the index into `items` the mapping above needs.
        Some(plan_agg) => base
            .into_iter()
            .zip(plan_agg.items.iter())
            .map(|((name, ty), item)| match item {
                crate::core::rql::aggregate::SelectItem::Column { field, .. } => {
                    (name, advertised_at(field.write_order as usize, ty))
                }
                _ => (name, ty),
            })
            .collect(),
    }
}

async fn send_query_result<S>(
    stream: &mut S,
    fields: Option<&[(String, DbType)]>,
    // Declared decimal scale per column, where one was declared.
    //
    // Carried beside the types rather than inside them: a `NUMERIC(38,16)`
    // column stores sixteen fractional digits and a `NUMERIC(10,2)` stores two,
    // and rendering either at the engine's default scale would print a number
    // that is wrong by powers of ten. `DbType` has no room to say which.
    scales: Option<&[u32]>,
    rows: &[Row],
    result_formats: &[i16],
    command_tag: &str,
    omit_description: bool,
) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let derived: Vec<(String, DbType)>;
    let col_meta: &[(String, DbType)] = if let Some(f) = fields {
        f
    } else if let Some(first) = rows.first() {
        derived = first
            .columns
            .iter()
            .enumerate()
            .map(|(i, c)| (format!("column{}", i + 1), c.column_type.clone()))
            .collect();
        &derived
    } else {
        &[]
    };

    if !omit_description {
        send_row_description(stream, col_meta, result_formats).await?;
    }

    // Each row is encoded straight into one reused frame — cells written in
    // place, lengths patched in after — and handed over as a single write.
    let mut frame: Vec<u8> = Vec::with_capacity(256);
    for row in rows {
        // Counted here as `send_data_row` counts the rows it sends.
        #[cfg(feature = "query_stats")]
        crate::core::query_stats::note_rows(1);

        frame.clear();
        let start = wire_out::begin_message(&mut frame, b'D');
        let count: i16 = row.columns.len().try_into().unwrap_or(i16::MAX);
        frame.extend_from_slice(&count.to_be_bytes());
        for (i, col) in row.columns.iter().enumerate() {
            encode_result_cell(
                &mut frame,
                i,
                &col.column_type,
                col.data.into_slice(),
                col_meta,
                scales,
                result_formats,
            );
        }
        wire_out::finish_message(&mut frame, start)?;
        stream.write_all(&frame).await?;
    }

    send_command_complete(stream, command_tag).await?;
    Ok(())
}

/// One cell of a DataRow: its length, then its value in the requested format.
///
/// Shared by both result senders, so a decoded row and a serialized one
/// cannot be encoded differently.
#[inline]
fn encode_result_cell(
    frame: &mut Vec<u8>,
    i: usize,
    stored_type: &DbType,
    data: &[u8],
    col_meta: &[(String, DbType)],
    scales: Option<&[u32]>,
    result_formats: &[i16],
) {
    let format_code = if result_formats.is_empty() {
        0
    } else if result_formats.len() == 1 {
        result_formats[0]
    } else {
        *result_formats.get(i).unwrap_or(&0)
    };

    // The row carries the *storage* type — `BYTEA` and `ARRAY` are
    // both stored as `STRING` — so formatting follows the declared
    // type instead. `DbType::NULL` is the exception: the executor sets
    // it on the row to mark a NULL, and that must win.
    let db_type = if *stored_type == DbType::NULL {
        &DbType::NULL
    } else {
        col_meta.get(i).map(|(_, t)| t).unwrap_or(stored_type)
    };

    let scale = scales.and_then(|s| s.get(i)).copied();
    let len_at = frame.len();
    frame.extend_from_slice(&[0u8; 4]);
    // A `numeric` that carries its own display scale (an `AVG` result; see
    // `value_codec::decimal_cell_with_scale`) is written at that scale, which
    // overrides the column's, and keeps it in the binary form's `dscale`.
    let own_scale = (*db_type == DbType::DECIMAL)
        .then(|| crate::core::value_codec::split_decimal_cell(data))
        .flatten()
        .and_then(|(v, s)| s.map(|s| (v, s)));
    let present = if let Some((v, s)) = own_scale {
        if format_code == 1 {
            frame.extend_from_slice(&crate::core::value_codec::encode_pg_numeric_with_dscale(v, s));
        } else {
            crate::core::value_codec::format_decimal_scaled_into(v, s, frame);
        }
        true
    } else if format_code == 1 {
        wire_out::encode_cell_binary(db_type, data, scale, frame)
    } else {
        wire_out::encode_cell_text(db_type, data, scale, frame)
    };
    let len: i32 = if present {
        (frame.len() - len_at - 4).try_into().unwrap_or(i32::MAX)
    } else {
        -1
    };
    frame[len_at..len_at + 4].copy_from_slice(&len.to_be_bytes());
}

/// How many rows a serialized engine result holds, after checking every row
/// and column length in it.
///
/// Run before anything is sent, so a damaged result fails the statement
/// cleanly instead of after a RowDescription and part of the rows have gone
/// out — the guarantee decoding the whole result up front used to give.
fn validate_serialized_rows(rows_bytes: &[u8]) -> io::Result<usize> {
    let mut count = 0usize;
    for span in crate::core::row::row::serialized_rows(rows_bytes) {
        for cell in crate::core::row::row::serialized_cells(span?)? {
            cell?;
        }
        count += 1;
    }
    Ok(count)
}

/// [`send_query_result`] over the engine's serialized rows, encoding each
/// DataRow from the bytes in place.
///
/// The rows used to be decoded into a `Vec<Row>` first — a `Row` is ~1.5 KB
/// whatever it holds, so a million-row result cost ~1.5 GB of decoded copies
/// on top of the bytes they came from, only to be read once and dropped.
/// `rows_bytes` must have passed [`validate_serialized_rows`].
async fn send_serialized_query_result<S>(
    stream: &mut S,
    fields: Option<&[(String, DbType)]>,
    scales: Option<&[u32]>,
    rows_bytes: &[u8],
    result_formats: &[i16],
    command_tag: &str,
    omit_description: bool,
) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let invalid = |e: io::Error| io::Error::new(io::ErrorKind::InvalidData, e.to_string());

    let derived: Vec<(String, DbType)>;
    let col_meta: &[(String, DbType)] = if let Some(f) = fields {
        f
    } else if let Some(first) = crate::core::row::row::serialized_rows(rows_bytes).next() {
        let mut meta = Vec::new();
        for (i, cell) in crate::core::row::row::serialized_cells(first.map_err(invalid)?)
            .map_err(invalid)?
            .enumerate()
        {
            meta.push((format!("column{}", i + 1), cell.map_err(invalid)?.column_type));
        }
        derived = meta;
        &derived
    } else {
        &[]
    };

    if !omit_description {
        send_row_description(stream, col_meta, result_formats).await?;
    }

    let mut frame: Vec<u8> = Vec::with_capacity(256);
    for (sent, span) in crate::core::row::row::serialized_rows(rows_bytes).enumerate() {
        #[cfg(feature = "query_stats")]
        crate::core::query_stats::note_rows(1);

        // A buffered write completes without suspending, so a large result
        // could be encoded start to finish without giving the worker thread
        // back. One yield per 4096 rows lets the tasks queued behind it run.
        if sent % 4096 == 4095 && crate::core::processor::concurrent_processor::latency_guards_on() {
            tokio::task::yield_now().await;
        }

        frame.clear();
        let start = wire_out::begin_message(&mut frame, b'D');
        // The column count is not in the row's header, so it is patched in
        // once the cells are written.
        let count_at = frame.len();
        frame.extend_from_slice(&[0u8; 2]);
        let mut columns = 0usize;
        for (i, cell) in crate::core::row::row::serialized_cells(span.map_err(invalid)?)
            .map_err(invalid)?
            .enumerate()
        {
            let cell = cell.map_err(invalid)?;
            encode_result_cell(
                &mut frame,
                i,
                &cell.column_type,
                cell.data,
                col_meta,
                scales,
                result_formats,
            );
            columns += 1;
        }
        let count: i16 = columns.try_into().unwrap_or(i16::MAX);
        frame[count_at..count_at + 2].copy_from_slice(&count.to_be_bytes());
        wire_out::finish_message(&mut frame, start)?;
        stream.write_all(&frame).await?;
    }

    send_command_complete(stream, command_tag).await?;
    Ok(())
}

#[cfg(test)]
mod serialized_result_tests {
    //! The serialized-row sender against the decoded-row one it replaced on
    //! the query path: same rows, same bytes on the wire, for every shape the
    //! encoder branches on — NULL markers, declared types that differ from the
    //! stored ones, NUMERIC scales by slot, text/binary/mixed formats, derived
    //! column names, and rows of different widths.
    use super::*;
    use crate::core::row::row::{Column, rows_into_vec};
    use crate::memory_pool::MEMORY_POOL;

    fn cell(schema_id: u64, ty: DbType, bytes: &[u8]) -> Column {
        let mut data = MEMORY_POOL.acquire(bytes.len());
        if !bytes.is_empty() {
            data.into_slice_mut().copy_from_slice(bytes);
        }
        Column {
            schema_id: crate::core::row::row::schema_id_of(schema_id),
            data,
            column_type: ty,
        }
    }

    fn rows() -> Vec<Row> {
        let mut out = Vec::new();
        for i in 0..40i64 {
            let mut columns = crate::core::row::row::RowColumns::new();
            columns.push(cell(0, DbType::I64, &(i * 1_000_003 - 7).to_le_bytes()));
            columns.push(if i % 5 == 0 {
                cell(1, DbType::NULL, &[])
            } else {
                cell(1, DbType::STRING, format!("text {i} 'q' ü").as_bytes())
            });
            columns.push(cell(2, DbType::DECIMAL, &((i as i128 - 20) * 123_456_789).to_le_bytes()));
            columns.push(cell(3, DbType::F64, &[f64::NAN, -0.0, 1.5, f64::INFINITY][i as usize % 4].to_le_bytes()));
            // Stored as text, declared BYTEA.
            columns.push(cell(4, DbType::STRING, &[0u8, 1, 0xff, i as u8]));
            columns.push(cell(5, DbType::DATETIME, &(i * 86_400_000_000).to_le_bytes()));
            columns.push(cell(6, DbType::BOOL, &[(i % 2) as u8]));
            if i % 7 == 3 {
                // A row narrower than the description.
                columns.truncate(4);
            }
            out.push(Row {
                position: 0,
                columns,
                length: 0,
            });
        }
        out
    }

    async fn both(
        rows: &[Row],
        fields: Option<&[(String, DbType)]>,
        scales: Option<&[u32]>,
        formats: &[i16],
        omit: bool,
    ) -> (Vec<u8>, Vec<u8>) {
        let bytes = rows_into_vec(rows.to_vec());
        let decoded = crate::core::row::row::vec_into_rows(&bytes).unwrap();
        let mut old = Vec::new();
        send_query_result(&mut old, fields, scales, &decoded, formats, "SELECT x", omit)
            .await
            .unwrap();
        assert_eq!(validate_serialized_rows(&bytes).unwrap(), rows.len());
        let mut new = Vec::new();
        send_serialized_query_result(&mut new, fields, scales, &bytes, formats, "SELECT x", omit)
            .await
            .unwrap();
        (old, new)
    }

    #[tokio::test]
    async fn serialized_rows_go_out_byte_identical() {
        let fields: Vec<(String, DbType)> = [
            ("id", DbType::I64),
            ("t", DbType::STRING),
            ("n", DbType::DECIMAL),
            ("f", DbType::F64),
            ("b", DbType::BYTEA),
            ("ts", DbType::DATETIME),
            ("ok", DbType::BOOL),
        ]
        .into_iter()
        .map(|(n, t)| (n.to_string(), t))
        .collect();
        let scale_sets: [Option<Vec<u32>>; 3] = [None, Some(vec![0, 0, 2, 0, 0, 0, 0]), Some(vec![0, 0, 16])];
        let format_sets: [&[i16]; 4] = [&[], &[0], &[1], &[0, 1, 1, 0, 1, 0, 1]];
        let all = rows();
        let mut compared = 0;
        for subset in [&all[..0], &all[..1], &all[..]] {
            for fields in [Some(fields.as_slice()), None] {
                for scales in &scale_sets {
                    for formats in format_sets {
                        for omit in [false, true] {
                            let (old, new) =
                                both(subset, fields, scales.as_deref(), formats, omit).await;
                            assert_eq!(
                                old,
                                new,
                                "rows {} fields {} scales {:?} formats {:?} omit {}",
                                subset.len(),
                                fields.is_some(),
                                scales,
                                formats,
                                omit
                            );
                            compared += 1;
                        }
                    }
                }
            }
        }
        assert_eq!(compared, 3 * 2 * 3 * 4 * 2);
    }

    #[test]
    fn a_damaged_result_is_refused_before_anything_is_sent() {
        let bytes = rows_into_vec(rows());
        assert!(validate_serialized_rows(&bytes[..bytes.len() - 2]).is_err());
        // A column length that runs past its row.
        let mut bad = bytes.clone();
        let header = if cfg!(feature = "enable_long_row") { 8 } else { 4 };
        bad[header..header + 8].copy_from_slice(&u64::MAX.to_le_bytes());
        assert!(validate_serialized_rows(&bad).is_err());
    }
}

fn format_column_binary(db_type: DbType, bytes: &[u8], scale: Option<u32>) -> Option<Vec<u8>> {
    if matches!(db_type, DbType::NULL | DbType::START | DbType::END) {
        return None;
    }

    match db_type {
        // `jsonb`'s binary form is a one-byte format version followed by the
        // document text. Sent as bare text it reached the client as
        // "unsupported JSONB format version 0" — the first character of the
        // document was being read as the version.
        DbType::JSON => {
            let mut out = Vec::with_capacity(bytes.len() + 1);
            out.push(1u8);
            out.extend_from_slice(bytes);
            Some(out)
        }
        // The same wire form, from the stored bytes rather than from a
        // document that was rebuilt out of a shadow table.
        DbType::JSONB => {
            if bytes.is_empty() {
                return None;
            }
            let mut out = Vec::with_capacity(bytes.len() + 1);
            out.push(1u8);
            crate::core::jsonb::render_text_into(bytes, &mut out).ok()?;
            Some(out)
        }
        // `int2` on the wire. An `I8` is signed: `bytes[0] as i16` zero-extended
        // it, so -1 reached a binary client as 255 while the text path said -1.
        DbType::I8 => {
            if bytes.len() == 1 {
                Some(i16::from(bytes[0] as i8).to_be_bytes().to_vec())
            } else {
                None
            }
        }
        // The unsigned widths are advertised as `int8`, so they go out as
        // eight bytes; at their own width a binary client refused them.
        DbType::U8 | DbType::U16 | DbType::U32 => {
            if matches!(bytes.len(), 1 | 2 | 4) {
                let mut le = [0u8; 8];
                le[..bytes.len()].copy_from_slice(bytes);
                Some(u64::from_le_bytes(le).to_be_bytes().to_vec())
            } else {
                None
            }
        }
        DbType::I16 => {
            if bytes.len() == 2 {
                let mut arr = [0u8; 2];
                arr.copy_from_slice(bytes);
                let val = u16::from_le_bytes(arr);
                Some(val.to_be_bytes().to_vec())
            } else {
                None
            }
        }
        DbType::I32 => {
            if bytes.len() == 4 {
                let mut arr = [0u8; 4];
                arr.copy_from_slice(bytes);
                let val = u32::from_le_bytes(arr);
                Some(val.to_be_bytes().to_vec())
            } else {
                None
            }
        }
        DbType::I64 => {
            if bytes.len() == 8 {
                let mut arr = [0u8; 8];
                arr.copy_from_slice(bytes);
                let val = u64::from_le_bytes(arr);
                Some(val.to_be_bytes().to_vec())
            } else {
                None
            }
        }
        // Advertised as `numeric` (`DbType::pg_oid`): integers wider than
        // `bigint`, at scale 0. Sent as an 8-byte `int8`, a `UBIGINT` above
        // `i64::MAX` decoded as negative; a `HUGEINT` went out as text.
        DbType::U64 | DbType::I128 | DbType::U128 => wide_integer_numeric(db_type, bytes),
        DbType::F32 => {
            if bytes.len() == 4 {
                let mut arr = [0u8; 4];
                arr.copy_from_slice(bytes);
                let val = u32::from_le_bytes(arr);
                Some(val.to_be_bytes().to_vec())
            } else {
                None
            }
        }
        DbType::F64 => {
            if bytes.len() == 8 {
                let mut arr = [0u8; 8];
                arr.copy_from_slice(bytes);
                let val = u64::from_le_bytes(arr);
                Some(val.to_be_bytes().to_vec())
            } else {
                None
            }
        }
        DbType::BOOL => {
            if !bytes.is_empty() {
                Some(vec![if bytes[0] != 0 { 1 } else { 0 }])
            } else {
                Some(vec![0])
            }
        }
        DbType::STRING | DbType::UNKNOWN => Some(bytes.to_vec()),
        // Both are stored in the representation Postgres uses on the wire, so
        // the binary form is the stored bytes unchanged.
        // Both are stored in the representation Postgres uses on the wire, so
        // the binary form is the stored bytes unchanged.
        DbType::BYTEA | DbType::ARRAY => Some(bytes.to_vec()),
        // Postgres sends a UUID as its 16 raw octets, which is exactly how it
        // is stored.
        DbType::UUID => {
            if bytes.len() == crate::core::value_codec::UUID_LEN {
                Some(bytes.to_vec())
            } else {
                None
            }
        }
        // Binary `numeric` is a base-10000 digit string, not a machine integer.
        DbType::DECIMAL => {
            // A value carrying its own display scale (an `AVG` result).
            if let Some((v, Some(own))) = crate::core::value_codec::split_decimal_cell(bytes) {
                return Some(crate::core::value_codec::encode_pg_numeric_with_dscale(v, own));
            }
            if bytes.len() == 16 {
                let mut arr = [0u8; 16];
                arr.copy_from_slice(bytes);
                // The column's declared scale, not the engine default: a
                // `NUMERIC(p, s)` column stores its values scaled by its own
                // `s`, and dividing by the default here sent the value off by
                // a power of ten. The text renderer beside this one was
                // already given the scale, which is why the same query
                // answered differently over the two protocols.
                Some(crate::core::value_codec::encode_pg_numeric_scaled(
                    i128::from_le_bytes(arr),
                    scale.unwrap_or(crate::core::value_codec::DECIMAL_SCALE),
                ))
            } else {
                None
            }
        }
        // Binary `timestamptz` counts microseconds from 2000-01-01, big-endian.
        DbType::DATETIME => {
            if bytes.len() == 8 {
                let mut arr = [0u8; 8];
                arr.copy_from_slice(bytes);
                let us = i64::from_le_bytes(arr);
                // Wrapping, as release builds always did; a debug build
                // panicked on a value within 2^63 µs of the bottom.
                Some(
                    us.wrapping_sub(crate::core::value_codec::PG_EPOCH_OFFSET_US)
                        .to_be_bytes()
                        .to_vec(),
                )
            } else {
                None
            }
        }
        DbType::CHAR => {
            if bytes.len() == 4 {
                let mut arr = [0u8; 4];
                arr.copy_from_slice(bytes);
                let code = u32::from_le_bytes(arr);
                char::from_u32(code)
                    .map(|c| c.to_string().into_bytes())
                    .or_else(|| Some(bytes.to_vec()))
            } else {
                Some(bytes.to_vec())
            }
        }
        // `inet`'s binary form: family (2 = IPv4, 3 = IPv6), prefix bits, the
        // is-cidr flag, the address length, the address. It fell through to
        // the catch-all below, which sent the *text* rendering to a client
        // that asked for binary, and tokio-postgres failed to deserialise it.
        // Stored as family (4/6), sixteen address bytes, prefix.
        DbType::INET => {
            if bytes.len() != crate::core::value_codec::INET_LEN {
                return None;
            }
            let (family, len) = match bytes[0] {
                4 => (2u8, 4usize),
                6 => (3u8, 16usize),
                _ => return None,
            };
            let mut out = Vec::with_capacity(4 + len);
            out.extend_from_slice(&[family, bytes[17], 0, len as u8]);
            out.extend_from_slice(&bytes[1..1 + len]);
            Some(out)
        }
        // Stored in PostgreSQL's binary layout already, so the binary result is
        // the stored bytes. Falling through to the catch-all below would have
        // sent the *text* rendering to a client that asked for binary — the
        // shape of bug that makes a value arrive wrong rather than make the
        // query fail.
        DbType::INTERVAL
        | DbType::MACADDR
        | DbType::MACADDR8
        | DbType::BIT
        | DbType::VARBIT
        | DbType::DATE
        | DbType::TIME
        | DbType::TIMETZ
        | DbType::POINT
        | DbType::LINE
        | DbType::LSEG
        | DbType::BOX
        | DbType::PATH
        | DbType::POLYGON
        | DbType::CIRCLE
        | DbType::INT4RANGE
        | DbType::INT8RANGE
        | DbType::NUMRANGE
        | DbType::TSRANGE
        | DbType::TSTZRANGE
        | DbType::DATERANGE => Some(bytes.to_vec()),
        _ => format_column_text(db_type, bytes).map(|s| s.into_bytes()),
    }
}

fn format_column_text(db_type: DbType, bytes: &[u8]) -> Option<String> {
    format_column_text_scaled(db_type, bytes, None)
}

/// A `UBIGINT`, `HUGEINT` or `UHUGEINT` value as binary `numeric` at scale 0.
///
/// Read through the text renderer, so the two formats cannot disagree about
/// which number the bytes hold (a value stored as decimal text included).
fn wide_integer_numeric(db_type: DbType, bytes: &[u8]) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(8 + 2 * 10);
    match (&db_type, bytes.len()) {
        (DbType::U64, 8) => crate::core::value_codec::encode_pg_numeric_u128_into(
            u64::from_le_bytes(bytes.try_into().ok()?) as u128,
            &mut out,
        ),
        (DbType::U128, 16) => crate::core::value_codec::encode_pg_numeric_u128_into(
            u128::from_le_bytes(bytes.try_into().ok()?),
            &mut out,
        ),
        (DbType::I128, 16) => crate::core::value_codec::encode_pg_numeric_i128_into(
            i128::from_le_bytes(bytes.try_into().ok()?),
            &mut out,
        ),
        _ => {
            let text = format_column_text(db_type, bytes)?;
            if text.starts_with('-') {
                crate::core::value_codec::encode_pg_numeric_i128_into(text.parse().ok()?, &mut out);
            } else {
                crate::core::value_codec::encode_pg_numeric_u128_into(text.parse().ok()?, &mut out);
            }
        }
    }
    Some(out)
}

/// `String::from_utf8_lossy(bytes).to_string()`, with one copy instead of
/// three when the bytes are valid UTF-8.
fn text_from_bytes(bytes: &[u8]) -> String {
    match std::str::from_utf8(bytes) {
        Ok(text) => text.to_owned(),
        Err(_) => String::from_utf8_lossy(bytes).into_owned(),
    }
}

/// The declared decimal scale of each `RETURNING` item of a write, for every
/// reader of its rows: the client's, and a data-modifying CTE's relation,
/// which read 7.25 in a `NUMERIC(12,2)` column as 0.000000725 without it.
pub(crate) fn purpose_returning_scales(
    database: &Arc<Database>,
    purpose: &QueryPurpose,
) -> Option<Vec<u32>> {
    match purpose {
        QueryPurpose::UpdateRow(sql) | QueryPurpose::DeleteRow(sql) => {
            let skip = if matches!(purpose, QueryPurpose::UpdateRow(_)) { 1 } else { 2 };
            let name = sql.split_whitespace().nth(skip).unwrap_or("").trim_matches('"').to_string();
            returning_scales(database, &name, sql)
        }
        QueryPurpose::InsertRow(ir) => returning_scales(database, &ir.table_name, &ir.query),
        _ => None,
    }
}

/// The declared decimal scale of each `RETURNING` item, when the table has any
/// `NUMERIC(p, s)` column. A computed item is evaluated at the engine scale.
fn returning_scales(database: &Arc<Database>, table: &str, sql: &str) -> Option<Vec<u32>> {
    let fields = database.tables.get(table).and_then(|t| {
        (!t.schema.type_modifiers.is_empty()).then(|| t.schema.fields.clone())
    })?;
    let items = crate::core::rql::lexer_ir::returning_items(sql, &fields)?.ok()?;
    let engine = crate::core::value_codec::DECIMAL_SCALE;
    Some(
        items
            .iter()
            .map(|item| match item {
                crate::core::rql::lexer_ir::ReturningItem::Column(field) => field.scale.unwrap_or(engine),
                crate::core::rql::lexer_ir::ReturningItem::Computed { .. } => engine,
            })
            .collect(),
    )
}

/// Renders a value, honouring a column's declared decimal scale.
fn format_column_text_scaled(db_type: DbType, bytes: &[u8], scale: Option<u32>) -> Option<String> {
    // A value carrying its own display scale (an `AVG` result) wins over the
    // column's.
    if db_type == DbType::DECIMAL {
        if let Some((v, Some(own))) = crate::core::value_codec::split_decimal_cell(bytes) {
            return Some(crate::core::value_codec::format_decimal_scaled(v, own));
        }
    }
    if let (DbType::DECIMAL, Some(scale)) = (&db_type, scale) {
        if bytes.len() == 16 {
            let mut arr = [0u8; 16];
            arr.copy_from_slice(bytes);
            return Some(crate::core::value_codec::format_decimal_scaled(
                i128::from_le_bytes(arr),
                scale,
            ));
        }
    }
    format_column_text_inner(db_type, bytes)
}

fn format_column_text_inner(db_type: DbType, bytes: &[u8]) -> Option<String> {
    // NULL-like logical types: no value to send.
    if matches!(db_type, DbType::NULL | DbType::START | DbType::END) {
        return None;
    }

    trace!(
        "Formatting column of type {:?} with bytes: {:?}",
        db_type, bytes
    );

    // Some older/alternate paths may store fixed-width numeric types as ASCII decimal
    // bytes (optionally NUL-padded). When pgwire formats these as binary little-endian
    // integers, the result looks like a huge, incorrect number (e.g. "200000" -> 0x323030303030).
    // Detect this shape and fall back to returning the decimal string.
    /// Reads a value stored as decimal text rather than encoded.
    ///
    /// **Only consulted when the byte width does not match the column's type.**
    /// Consulting it first meant any integer whose little-endian bytes happened
    /// to be ASCII digits was rendered as those digits: `48` came back as `0`,
    /// `57` as `9`, and `13616` as `05` — a wrong value reported as success, on
    /// the most-travelled path in the engine.
    fn ascii_decimal(bytes: &[u8]) -> Option<&str> {
        if bytes.is_empty() {
            return None;
        }

        // Trim trailing NULs (common with fixed-width fields).
        let mut end = bytes.len();
        while end > 0 && bytes[end - 1] == 0 {
            end -= 1;
        }
        if end == 0 {
            return None;
        }
        let trimmed = &bytes[..end];
        if trimmed.iter().all(|b| b.is_ascii_digit()) {
            std::str::from_utf8(trimmed).ok()
        } else {
            None
        }
    }

    // Helper specifically per width because of Rust type system.
    fn le_i8(bytes: &[u8]) -> Option<i8> {
        if bytes.len() != 1 {
            return None;
        }
        Some(i8::from_le_bytes([bytes[0]]))
    }
    fn le_u8(bytes: &[u8]) -> Option<u8> {
        if bytes.len() != 1 {
            return None;
        }
        Some(u8::from_le_bytes([bytes[0]]))
    }
    fn le_i16(bytes: &[u8]) -> Option<i16> {
        if bytes.len() != 2 {
            return None;
        }
        let mut arr = [0u8; 2];
        arr.copy_from_slice(&bytes[..2]);
        Some(i16::from_le_bytes(arr))
    }
    fn le_u16(bytes: &[u8]) -> Option<u16> {
        if bytes.len() != 2 {
            return None;
        }
        let mut arr = [0u8; 2];
        arr.copy_from_slice(&bytes[..2]);
        Some(u16::from_le_bytes(arr))
    }
    fn le_i32(bytes: &[u8]) -> Option<i32> {
        if bytes.len() != 4 {
            return None;
        }
        let mut arr = [0u8; 4];
        arr.copy_from_slice(&bytes[..4]);
        Some(i32::from_le_bytes(arr))
    }
    fn le_u32(bytes: &[u8]) -> Option<u32> {
        if bytes.len() != 4 {
            return None;
        }
        let mut arr = [0u8; 4];
        arr.copy_from_slice(&bytes[..4]);
        Some(u32::from_le_bytes(arr))
    }
    fn le_i64(bytes: &[u8]) -> Option<i64> {
        if bytes.len() != 8 {
            return None;
        }
        let mut arr = [0u8; 8];
        arr.copy_from_slice(&bytes[..8]);
        Some(i64::from_le_bytes(arr))
    }
    fn le_u64(bytes: &[u8]) -> Option<u64> {
        if bytes.len() != 8 {
            return None;
        }
        let mut arr = [0u8; 8];
        arr.copy_from_slice(&bytes[..8]);
        Some(u64::from_le_bytes(arr))
    }
    fn le_i128(bytes: &[u8]) -> Option<i128> {
        if bytes.len() != 16 {
            return None;
        }
        let mut arr = [0u8; 16];
        arr.copy_from_slice(&bytes[..16]);
        Some(i128::from_le_bytes(arr))
    }
    fn le_u128(bytes: &[u8]) -> Option<u128> {
        if bytes.len() != 16 {
            return None;
        }
        let mut arr = [0u8; 16];
        arr.copy_from_slice(&bytes[..16]);
        Some(u128::from_le_bytes(arr))
    }

    let s = match db_type {
        // `JSON` renders as text: by the time a value reaches here the document
        // has already been rebuilt from its shadow table, so the bytes are the
        // JSON itself. The type exists to change what the column is *called* on
        // the wire, not how it is written.
        DbType::STRING | DbType::UNKNOWN | DbType::JSON => {
            // Treat as UTF-8 text. Valid text — nearly always — is one
            // validating pass and one copy; only invalid bytes take the lossy
            // path and its replacement characters.
            text_from_bytes(bytes)
        }

        // A stored document, rendered back to the JSON text a client expects.
        // An empty slot is a NULL that reached here as bytes rather than
        // through the companion column, and prints as nothing.
        DbType::JSONB => {
            if bytes.is_empty() {
                String::new()
            } else {
                let mut out = Vec::with_capacity(bytes.len());
                match crate::core::jsonb::render_text_into(bytes, &mut out) {
                    Ok(()) => String::from_utf8(out).ok()?,
                    // Refused rather than shown as mojibake: a column holding
                    // something that is not a document is a bug worth seeing.
                    Err(e) => {
                        log::error!("a JSONB value could not be decoded: {}", e);
                        return None;
                    }
                }
            }
        }

        DbType::DATETIME => {
            // 8-byte LE microseconds since the Unix epoch. The server reports
            // TimeZone=UTC at startup, so the offset is always +00.
            let Some(us) = le_i64(bytes) else { return None };
            let mut out = Vec::with_capacity(29);
            crate::core::value_codec::format_timestamp_us_into(us, &mut out);
            out.extend_from_slice(b"+00");
            String::from_utf8(out).ok()?
        }

        DbType::UUID => {
            if bytes.len() != crate::core::value_codec::UUID_LEN {
                return None;
            }
            crate::core::value_codec::format_uuid(bytes)
        }

        DbType::INET => match crate::core::value_codec::format_inet(bytes) {
            Some(s) => s,
            None => return None,
        },

        DbType::BYTEA => crate::core::value_codec::format_bytea(bytes),

        DbType::ARRAY => match crate::core::value_codec::format_array(bytes) {
            Some(s) => s,
            None => return None,
        },

        DbType::DECIMAL => match crate::core::value_codec::split_decimal_cell(bytes) {
            Some((v, Some(own))) => crate::core::value_codec::format_decimal_scaled(v, own),
            _ => {
                let Some(v) = le_i128(bytes) else { return None };
                crate::core::value_codec::format_decimal(v)
            }
        },

        DbType::BOOL => {
            // Postgres canonical text is 't' or 'f'.
            let is_true = bytes.first().copied().unwrap_or(0) != 0;
            if is_true {
                "t".to_string()
            } else {
                "f".to_string()
            }
        }

        DbType::CHAR => {
            // Either a 4-byte LE codepoint or plain UTF-8.
            if bytes.len() == 4 {
                let mut arr = [0u8; 4];
                arr.copy_from_slice(bytes);
                let code = u32::from_le_bytes(arr);
                char::from_u32(code)
                    .map(|c| c.to_string())
                    .unwrap_or_else(|| code.to_string())
            } else {
                text_from_bytes(bytes)
            }
        }

        DbType::I8 => {
            // The declared width decides, not what the bytes look like.
            // Trying `ascii_decimal` first rendered any integer whose
            // little-endian bytes happened to be ASCII digits as text:
            // 48 came back as "0", 57 as "9", 13616 as "05". The
            // ASCII form is only consulted when the width does not
            // match the type, which is how a value stored as text
            // rather than encoded is recognised.
            if bytes.len() == 1 {
                match le_i8(bytes) {
                    Some(v) => v.to_string(),
                    None => return None,
                }
            } else if let Some(s) = ascii_decimal(bytes) {
                s.to_string()
            } else if let Some(v) = le_i8(bytes) {
                v.to_string()
            } else {
                return None;
            }
        }
        DbType::I16 => {
            // The declared width decides, not what the bytes look like.
            // Trying `ascii_decimal` first rendered any integer whose
            // little-endian bytes happened to be ASCII digits as text:
            // 48 came back as "0", 57 as "9", 13616 as "05". The
            // ASCII form is only consulted when the width does not
            // match the type, which is how a value stored as text
            // rather than encoded is recognised.
            if bytes.len() == 2 {
                match le_i16(bytes) {
                    Some(v) => v.to_string(),
                    None => return None,
                }
            } else if let Some(s) = ascii_decimal(bytes) {
                s.to_string()
            } else if let Some(v) = le_i16(bytes) {
                v.to_string()
            } else {
                return None;
            }
        }
        DbType::I32 => {
            // The declared width decides, not what the bytes look like.
            // Trying `ascii_decimal` first rendered any integer whose
            // little-endian bytes happened to be ASCII digits as text:
            // 48 came back as "0", 57 as "9", 13616 as "05". The
            // ASCII form is only consulted when the width does not
            // match the type, which is how a value stored as text
            // rather than encoded is recognised.
            if bytes.len() == 4 {
                match le_i32(bytes) {
                    Some(v) => v.to_string(),
                    None => return None,
                }
            } else if let Some(s) = ascii_decimal(bytes) {
                s.to_string()
            } else if let Some(v) = le_i32(bytes) {
                v.to_string()
            } else {
                return None;
            }
        }
        DbType::I64 => {
            // The declared width decides, not what the bytes look like.
            // Trying `ascii_decimal` first rendered any integer whose
            // little-endian bytes happened to be ASCII digits as text:
            // 48 came back as "0", 57 as "9", 13616 as "05". The
            // ASCII form is only consulted when the width does not
            // match the type, which is how a value stored as text
            // rather than encoded is recognised.
            if bytes.len() == 8 {
                match le_i64(bytes) {
                    Some(v) => v.to_string(),
                    None => return None,
                }
            } else if let Some(s) = ascii_decimal(bytes) {
                s.to_string()
            } else if let Some(v) = le_i64(bytes) {
                v.to_string()
            } else {
                return None;
            }
        }
        DbType::I128 => {
            // Width first, as for `U64`: sixteen bytes that happen to be
            // ASCII digits are still a binary `i128`.
            if let Some(v) = le_i128(bytes) {
                v.to_string()
            } else if let Some(s) = ascii_decimal(bytes) {
                s.to_string()
            } else {
                return None;
            }
        }

        DbType::U8 => {
            // The declared width decides, not what the bytes look like.
            // Trying `ascii_decimal` first rendered any integer whose
            // little-endian bytes happened to be ASCII digits as text:
            // 48 came back as "0", 57 as "9", 13616 as "05". The
            // ASCII form is only consulted when the width does not
            // match the type, which is how a value stored as text
            // rather than encoded is recognised.
            if bytes.len() == 1 {
                match le_u8(bytes) {
                    Some(v) => v.to_string(),
                    None => return None,
                }
            } else if let Some(s) = ascii_decimal(bytes) {
                s.to_string()
            } else if let Some(v) = le_u8(bytes) {
                v.to_string()
            } else {
                return None;
            }
        }
        DbType::U16 => {
            // The declared width decides, not what the bytes look like.
            // Trying `ascii_decimal` first rendered any integer whose
            // little-endian bytes happened to be ASCII digits as text:
            // 48 came back as "0", 57 as "9", 13616 as "05". The
            // ASCII form is only consulted when the width does not
            // match the type, which is how a value stored as text
            // rather than encoded is recognised.
            if bytes.len() == 2 {
                match le_u16(bytes) {
                    Some(v) => v.to_string(),
                    None => return None,
                }
            } else if let Some(s) = ascii_decimal(bytes) {
                s.to_string()
            } else if let Some(v) = le_u16(bytes) {
                v.to_string()
            } else {
                return None;
            }
        }
        DbType::U32 => {
            // The declared width decides, not what the bytes look like.
            // Trying `ascii_decimal` first rendered any integer whose
            // little-endian bytes happened to be ASCII digits as text:
            // 48 came back as "0", 57 as "9", 13616 as "05". The
            // ASCII form is only consulted when the width does not
            // match the type, which is how a value stored as text
            // rather than encoded is recognised.
            if bytes.len() == 4 {
                match le_u32(bytes) {
                    Some(v) => v.to_string(),
                    None => return None,
                }
            } else if let Some(s) = ascii_decimal(bytes) {
                s.to_string()
            } else if let Some(v) = le_u32(bytes) {
                v.to_string()
            } else {
                return None;
            }
        }
        DbType::U64 => {
            // The declared width decides, not what the bytes look like.
            // Trying `ascii_decimal` first rendered any integer whose
            // little-endian bytes happened to be ASCII digits as text:
            // 48 came back as "0", 57 as "9", 13616 as "05". The
            // ASCII form is only consulted when the width does not
            // match the type, which is how a value stored as text
            // rather than encoded is recognised.
            if bytes.len() == 8 {
                match le_u64(bytes) {
                    Some(v) => v.to_string(),
                    None => return None,
                }
            } else if let Some(s) = ascii_decimal(bytes) {
                s.to_string()
            } else if let Some(v) = le_u64(bytes) {
                v.to_string()
            } else {
                return None;
            }
        }
        DbType::U128 => {
            if let Some(v) = le_u128(bytes) {
                v.to_string()
            } else if let Some(s) = ascii_decimal(bytes) {
                s.to_string()
            } else {
                return None;
            }
        }

        DbType::F32 => {
            // Floats are usually binary; if that fails, try ASCII.
            if bytes.len() == 4 {
                let mut arr = [0u8; 4];
                arr.copy_from_slice(&bytes[..4]);
                // Formatted as an `f32`, not widened first: `1.1f32` as an
                // `f64` prints as 1.100000023841858, which is the same value
                // and a different number as far as anyone reading it is
                // concerned.
                format_float_32(f32::from_le_bytes(arr))
            } else if let Ok(text) = std::str::from_utf8(bytes) {
                text.to_string()
            } else {
                return None;
            }
        }
        DbType::F64 => {
            if bytes.len() == 8 {
                let mut arr = [0u8; 8];
                arr.copy_from_slice(&bytes[..8]);
                format_float(f64::from_le_bytes(arr))
            } else if let Ok(text) = std::str::from_utf8(bytes) {
                text.to_string()
            } else {
                return None;
            }
        }

        // Stored in PostgreSQL's own wire layout, so the text form is derived
        // from the same bytes the binary path sends unchanged. Deriving both
        // from one source is the point: a `NUMERIC` once reached a client
        // multiplied by a power of ten because the two paths each worked the
        // value out separately.
        DbType::INTERVAL => match crate::core::pg_types::format_interval(bytes) {
            Some(s) => s,
            None => return None,
        },
        DbType::MACADDR | DbType::MACADDR8 => crate::core::pg_types::format_macaddr(bytes),
        DbType::BIT | DbType::VARBIT => match crate::core::pg_types::format_bit_string(bytes) {
            Some(s) => s,
            None => return None,
        },
        DbType::DATE => match crate::core::pg_types::format_date(bytes) {
            Some(s) => s,
            None => return None,
        },
        DbType::TIME => match crate::core::pg_types::format_time(bytes) {
            Some(s) => s,
            None => return None,
        },
        DbType::TIMETZ => match crate::core::pg_types::format_timetz(bytes) {
            Some(s) => s,
            None => return None,
        },
        DbType::POINT => match crate::core::pg_types::format_point(bytes) {
            Some(s) => s,
            None => return None,
        },
        DbType::LINE => match crate::core::pg_types::format_line(bytes) {
            Some(s) => s,
            None => return None,
        },
        DbType::LSEG => match crate::core::pg_types::format_lseg(bytes) {
            Some(s) => s,
            None => return None,
        },
        DbType::BOX => match crate::core::pg_types::format_box(bytes) {
            Some(s) => s,
            None => return None,
        },
        DbType::PATH => match crate::core::pg_types::format_path(bytes) {
            Some(s) => s,
            None => return None,
        },
        DbType::POLYGON => match crate::core::pg_types::format_polygon(bytes) {
            Some(s) => s,
            None => return None,
        },
        DbType::CIRCLE => match crate::core::pg_types::format_circle(bytes) {
            Some(s) => s,
            None => return None,
        },
        DbType::INT4RANGE
        | DbType::INT8RANGE
        | DbType::NUMRANGE
        | DbType::TSRANGE
        | DbType::TSTZRANGE
        | DbType::DATERANGE => match crate::core::pg_types::format_range(&db_type, bytes) {
            Some(s) => s,
            None => return None,
        },

        DbType::START | DbType::END | DbType::NULL => {
            // Already handled at top, but keep arm to satisfy match exhaustiveness.
            return None;
        }
    };

    trace!("Formatted column text: {}", s);

    Some(s)
}

/// Runs the authentication exchange for one connection.
///
/// Returns the authenticated principal, or an error whose message is what the
/// client is told. Every failure reports the same thing — a wrong password and
/// an unknown user are indistinguishable, so the protocol cannot be used to
/// enumerate accounts.
async fn authenticate<S>(
    database: &Arc<Database>,
    stream: &mut S,
    parameters: &HashMap<String, String>,
    config: &AuthConfig,
) -> io::Result<Principal>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    use crate::auth::{AuthMethod, ScramServer, Verifier};

    let user = parameters.get("user").cloned().unwrap_or_default();

    if config.method == AuthMethod::Trust {
        send_auth_ok(stream).await?;
        return Ok(Principal {
            name: user,
            superuser: false,
            authenticated: false,
        });
    }

    if user.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "no user name was supplied",
        ));
    }

    // A missing role still runs the exchange to completion, so the timing and
    // the messages match those of a wrong password.
    let role = crate::auth::lookup_role(database, &user).await;
    let verifier = role
        .as_ref()
        .and_then(|r| r.verifier.clone())
        .unwrap_or(Verifier::Plain(String::new()));
    let may_login = role.as_ref().map(|r| r.can_login).unwrap_or(false);
    let superuser = role.as_ref().map(|r| r.is_superuser).unwrap_or(false);

    let denied = || {
        io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!("password authentication failed for user \"{}\"", user),
        )
    };

    match config.method {
        AuthMethod::Trust => unreachable!("handled above"),

        AuthMethod::Password => {
            // AuthenticationCleartextPassword
            send_message(stream, b'R', &3i32.to_be_bytes()).await?;
            let password = read_password_message(stream).await?;
            if !may_login || !crate::auth::verify_password(&verifier, &user, &password) {
                return Err(denied());
            }
        }

        AuthMethod::Md5 => {
            let mut salt = [0u8; 4];
            crate::auth::fill_random(&mut salt);
            let mut payload = Vec::with_capacity(8);
            payload.extend_from_slice(&5i32.to_be_bytes());
            payload.extend_from_slice(&salt);
            send_message(stream, b'R', &payload).await?;

            let response = read_password_message(stream).await?;
            if !may_login || !crate::auth::verify_md5(&verifier, &user, &salt, &response) {
                return Err(denied());
            }
        }

        AuthMethod::ScramSha256 => {
            // AuthenticationSASL, offering the one mechanism implemented.
            let mut payload = Vec::new();
            payload.extend_from_slice(&10i32.to_be_bytes());
            payload.extend_from_slice(b"SCRAM-SHA-256\0");
            payload.push(0);
            send_message(stream, b'R', &payload).await?;

            let (mechanism, client_first) = read_sasl_initial_response(stream).await?;
            if mechanism != "SCRAM-SHA-256" {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unsupported SASL mechanism: {}", mechanism),
                ));
            }

            let (server, server_first) = ScramServer::start(verifier, &client_first)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

            // AuthenticationSASLContinue
            let mut payload = Vec::new();
            payload.extend_from_slice(&11i32.to_be_bytes());
            payload.extend_from_slice(server_first.as_bytes());
            send_message(stream, b'R', &payload).await?;

            let client_final = read_password_message(stream).await?;
            let server_final = server.finish(&client_final).map_err(|_| denied())?;
            if !may_login {
                return Err(denied());
            }

            // AuthenticationSASLFinal
            let mut payload = Vec::new();
            payload.extend_from_slice(&12i32.to_be_bytes());
            payload.extend_from_slice(server_final.as_bytes());
            send_message(stream, b'R', &payload).await?;
        }
    }

    send_auth_ok(stream).await?;
    Ok(Principal {
        name: user,
        superuser,
        authenticated: true,
    })
}

/// Reads a PasswordMessage ('p') and returns its payload as text.
async fn read_password_message<S>(stream: &mut S) -> io::Result<String>
where
    S: AsyncRead + Unpin,
{
    let (tag, payload) = read_message(stream).await?;
    if tag != b'p' {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "expected a password message",
        ));
    }
    // The trailing NUL is present for a password but not for SASL data.
    let end = payload
        .iter()
        .position(|&b| b == 0)
        .unwrap_or(payload.len());
    Ok(String::from_utf8_lossy(&payload[..end]).to_string())
}

/// Reads a SASLInitialResponse: mechanism name, length, then the client's
/// first message.
async fn read_sasl_initial_response<S>(stream: &mut S) -> io::Result<(String, String)>
where
    S: AsyncRead + Unpin,
{
    let (tag, payload) = read_message(stream).await?;
    if tag != b'p' {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "expected a SASL initial response",
        ));
    }

    let (mechanism, i) = read_cstring_owned(&payload, 0)?;
    let (len, i) = read_i32_be_from(&payload, i)?;
    if len < 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "SASL initial response has no data",
        ));
    }
    let end = (i + len as usize).min(payload.len());
    Ok((
        mechanism,
        String::from_utf8_lossy(&payload[i..end]).to_string(),
    ))
}

/// Reads one tagged protocol message.
async fn read_message<S>(stream: &mut S) -> io::Result<(u8, Vec<u8>)>
where
    S: AsyncRead + Unpin,
{
    let mut tag = [0u8; 1];
    stream.read_exact(&mut tag).await?;
    let len = read_i32_be(stream).await?;
    // Only authentication exchanges read through here, before the client has
    // proved anything, so the length is capped as PostgreSQL caps an auth
    // message (PG_MAX_AUTH_TOKEN_LENGTH) rather than allocated as asked.
    if len > MAX_AUTH_MESSAGE_LENGTH {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "authentication message too long",
        ));
    }
    if len < 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid message length",
        ));
    }
    let mut payload = vec![0u8; (len - 4) as usize];
    stream.read_exact(&mut payload).await?;
    Ok((tag[0], payload))
}

async fn send_auth_ok<S>(stream: &mut S) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let mut payload = Vec::with_capacity(4);
    payload.extend_from_slice(&0i32.to_be_bytes());
    send_message(stream, b'R', &payload).await
}

async fn send_parameter_status<S>(stream: &mut S, key: &str, value: &str) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let mut payload = Vec::with_capacity(key.len() + value.len() + 2);
    payload.extend_from_slice(key.as_bytes());
    payload.push(0);
    payload.extend_from_slice(value.as_bytes());
    payload.push(0);
    send_message(stream, b'S', &payload).await
}

async fn send_backend_key_data<S>(stream: &mut S, pid: i32, key: i32) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    // What a client sends back in a CancelRequest to stop this session's
    // running statement; see `cancel::register_session`.
    let mut payload = Vec::with_capacity(8);
    payload.extend_from_slice(&pid.to_be_bytes());
    payload.extend_from_slice(&key.to_be_bytes());
    send_message(stream, b'K', &payload).await
}

async fn send_ready_for_query<S>(stream: &mut S, status: u8) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    send_message(stream, b'Z', &[status]).await?;
    // The end of a statement or batch: the client is waiting for all of it.
    stream.flush().await
}

async fn send_command_complete<S>(stream: &mut S, tag: &str) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    // Whatever the engine raised for this statement goes first, as
    // PostgreSQL orders it: notices, then the completion they belong to.
    for notice in crate::core::notice::drain() {
        send_engine_notice(stream, &notice).await?;
    }
    let mut payload = Vec::with_capacity(tag.len() + 1);
    payload.extend_from_slice(tag.as_bytes());
    payload.push(0);
    send_message(stream, b'C', &payload).await
}

/// A `NoticeResponse` for a notice the engine raised: severity (localised
/// `S` and non-localised `V`), SQLSTATE and message.
async fn send_engine_notice<S>(stream: &mut S, notice: &crate::core::notice::Notice) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let mut payload = Vec::with_capacity(notice.message.len() + 32);
    for (field, value) in [
        (b'S', notice.severity),
        (b'V', notice.severity),
        (b'C', notice.sqlstate),
        (b'M', notice.message.as_str()),
    ] {
        payload.push(field);
        payload.extend_from_slice(value.as_bytes());
        payload.push(0);
    }
    payload.push(0);
    send_message(stream, b'N', &payload).await
}

async fn send_error_response<S>(stream: &mut S, sqlstate: &str, message: &str) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    // ErrorResponse fields: (type_byte, cstring)..., terminated by 0.
    // Minimal set: Severity (S), SQLSTATE (C), Message (M)
    let mut payload = Vec::new();

    payload.push(b'S');
    payload.extend_from_slice(b"ERROR");
    payload.push(0);

    payload.push(b'C');
    payload.extend_from_slice(sqlstate.as_bytes());
    payload.push(0);

    payload.push(b'M');
    payload.extend_from_slice(message.as_bytes());
    payload.push(0);

    payload.push(0);

    send_message(stream, b'E', &payload).await?;
    // Sent now rather than at the next `ReadyForQuery`: an error must reach
    // the client even on a path that ends the session instead.
    stream.flush().await
}
/// A `NoticeResponse`: something the client should hear about that is not an
/// error. `BEGIN` inside a block and `COMMIT` outside one are both notices in
/// PostgreSQL, not failures, and a driver that saw them as errors would abort
/// work that is meant to carry on.
async fn send_notice<S>(stream: &mut S, sqlstate: &str, message: &str) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let mut payload = Vec::new();

    payload.push(b'S');
    payload.extend_from_slice(b"WARNING");
    payload.push(0);

    payload.push(b'C');
    payload.extend_from_slice(sqlstate.as_bytes());
    payload.push(0);

    payload.push(b'M');
    payload.extend_from_slice(message.as_bytes());
    payload.push(0);

    payload.push(0);

    send_message(stream, b'N', &payload).await
}

async fn send_row_description<S, N>(
    stream: &mut S,
    fields: &[(N, DbType)],
    result_formats: &[i16],
) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
    N: AsRef<str>,
{
    // RowDescription message:
    // 'T'
    // int32 length
    // int16 field_count
    // For each field:
    //   string field_name
    //   int32 table_oid (0 = no table)
    //   int16 column_attr (0 = no column)
    //   int32 type_oid
    //   int16 type_size
    //   int32 type_modifier
    //   int16 format_code (0 = text, 1 = binary)

    let mut payload = Vec::new();

    // number of fields
    payload.extend_from_slice(&(fields.len() as i16).to_be_bytes());

    for (i, (name, db_type)) in fields.iter().enumerate() {
        // field name
        payload.extend_from_slice(name.as_ref().as_bytes());
        payload.push(0);

        // table_oid
        payload.extend_from_slice(&0i32.to_be_bytes());

        // column_attr
        payload.extend_from_slice(&0i16.to_be_bytes());

        // type_oid
        let oid = db_type.pg_oid();
        payload.extend_from_slice(&(oid as i32).to_be_bytes());

        // type_size
        let size = db_type.pg_type_size();
        payload.extend_from_slice(&(size as i16).to_be_bytes());

        // type_modifier
        payload.extend_from_slice(&(-1i32).to_be_bytes());

        // format_code
        let format_code = if result_formats.is_empty() {
            0
        } else if result_formats.len() == 1 {
            result_formats[0]
        } else {
            *result_formats.get(i).unwrap_or(&0)
        };
        payload.extend_from_slice(&format_code.to_be_bytes());
    }

    send_message(stream, b'T', &payload).await
}

async fn send_parameter_description<S>(stream: &mut S, param_oids: &[i32]) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let mut payload = Vec::with_capacity(2 + (4 * param_oids.len()));
    let count: i16 = param_oids.len().try_into().unwrap_or(i16::MAX);
    payload.extend_from_slice(&count.to_be_bytes());
    for oid in param_oids {
        payload.extend_from_slice(&oid.to_be_bytes());
    }
    // ParameterDescription
    send_message(stream, b't', &payload).await
}

async fn send_data_row<S>(stream: &mut S, values: &[Option<&[u8]>]) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    // One place counts every row any statement returns — a `SELECT`, a
    // `RETURNING`, `SHOW TABLES`, a catalogue view — because they all leave
    // through here. Counting at each producer instead would have missed
    // whichever one was added next.
    #[cfg(feature = "query_stats")]
    crate::core::query_stats::note_rows(1);

    // The whole frame in one buffer, length patched in at the end, so the
    // row is one write.
    let mut frame = Vec::with_capacity(
        7 + values.iter().map(|v| 4 + v.map_or(0, <[u8]>::len)).sum::<usize>(),
    );
    let start = wire_out::begin_message(&mut frame, b'D');
    let count: i16 = values.len().try_into().unwrap_or(i16::MAX);
    frame.extend_from_slice(&count.to_be_bytes());

    for v in values {
        match v {
            None => frame.extend_from_slice(&(-1i32).to_be_bytes()),
            Some(bytes) => {
                let len: i32 = bytes.len().try_into().unwrap_or(i32::MAX);
                frame.extend_from_slice(&len.to_be_bytes());
                frame.extend_from_slice(bytes);
            }
        }
    }

    wire_out::finish_message(&mut frame, start)?;
    stream.write_all(&frame).await
}

// fn db_type_to_pg(t: DbType) -> (i32, i16) {
//     // OIDs: https://www.postgresql.org/docs/current/datatype-oid.html
//     match t {
//         DbType::I8 | DbType::U8 => (21, 2), // int2
//         DbType::I16 | DbType::U16 => (21, 2),
//         DbType::I32 | DbType::U32 => (23, 4), // int4
//         DbType::I64 | DbType::U64 => (20, 8), // int8
//         DbType::I128 | DbType::U128 => (1700, -1), // numeric
//         DbType::F32 => (700, 4),
//         DbType::F64 => (701, 8),
//         DbType::DATETIME => (1114, 8),
//         DbType::BOOL => (16, 1),
//         DbType::CHAR | DbType::STRING | DbType::UNKNOWN | DbType::NULL | DbType::START | DbType::END => (25, -1),
//     }
// }

async fn send_message<S>(stream: &mut S, tag: u8, payload: &[u8]) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let len: i32 = (4 + payload.len())
        .try_into()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "message too large"))?;

    let mut header = [0u8; 5];
    header[0] = tag;
    header[1..].copy_from_slice(&len.to_be_bytes());
    stream.write_all(&header).await?;
    stream.write_all(payload).await?;
    // Not flushed here. The session stream (`wire_out::WireStream`) buffers,
    // and is flushed at `ReadyForQuery`, at a `Flush` message, on an error,
    // and before any read that has to wait on the client — the last of which
    // covers every exchange, such as authentication, that waits for a reply
    // without a `ReadyForQuery`. The few writes made before the session is
    // wrapped flush themselves.
    Ok(())
}

/// PostgreSQL's `PQ_LARGE_MESSAGE_LIMIT` and `PQ_SMALL_MESSAGE_LIMIT`.
const LARGE_MESSAGE_LIMIT: u64 = 0x3fff_ffff;
const SMALL_MESSAGE_LIMIT: u64 = 10_000;

/// The longest body a frontend message of type `tag` may have, as
/// PostgreSQL's `SocketBackend` assigns it: messages that carry statement text
/// or data may be large, the fixed-shape control messages may not. A type
/// PostgreSQL does not accept gets the small limit; it is refused once read.
fn frontend_message_limit(tag: u8) -> u64 {
    match tag {
        // Query, FunctionCall, CopyData, Bind, Parse.
        b'Q' | b'F' | b'd' | b'B' | b'P' => LARGE_MESSAGE_LIMIT,
        // Terminate, Close, Describe, Execute, Flush, Sync, CopyDone, CopyFail.
        _ => SMALL_MESSAGE_LIMIT,
    }
}

/// Reads a message body of `len` bytes, growing the buffer as bytes arrive.
///
/// The length has passed the type's limit, but it is still only a claim: a
/// client that claims 900 MB and sends nothing would otherwise pin 900 MB for
/// as long as it keeps the socket open. The buffer starts small and grows by
/// what has actually been read.
async fn read_message_body<S>(stream: &mut S, len: usize) -> io::Result<Vec<u8>>
where
    S: AsyncRead + Unpin,
{
    const STEP: usize = 1 << 20;
    let mut payload: Vec<u8> = Vec::with_capacity(len.min(STEP));
    while payload.len() < len {
        let want = (len - payload.len()).min(STEP);
        let start = payload.len();
        payload.resize(start + want, 0);
        stream.read_exact(&mut payload[start..]).await?;
    }
    Ok(payload)
}

async fn read_i32_be<S>(stream: &mut S) -> io::Result<i32>
where
    S: AsyncRead + Unpin,
{
    let mut buf = [0u8; 4];
    stream.read_exact(&mut buf).await?;
    Ok(i32::from_be_bytes(buf))
}

/// The `key=value` pairs of a StartupMessage, after its four-byte version.
///
/// The user name is here, so authentication cannot begin until it is read —
/// which is why the parameters can no longer be discarded.
fn parse_startup_parameters(payload: &[u8]) -> HashMap<String, String> {
    let mut out = HashMap::new();
    // The first four bytes are the protocol version.
    let mut i = 4usize;
    while i < payload.len() {
        let (key, next) = match read_cstring_owned(payload, i) {
            Ok(v) => v,
            Err(_) => break,
        };
        if key.is_empty() {
            break;
        }
        let (value, next) = match read_cstring_owned(payload, next) {
            Ok(v) => v,
            Err(_) => break,
        };
        out.insert(key, value);
        i = next;
    }
    out
}

/// PostgreSQL's `MAX_STARTUP_PACKET_LENGTH`.
const MAX_STARTUP_PACKET_LENGTH: i32 = 10_000;
/// PostgreSQL's `PG_MAX_AUTH_TOKEN_LENGTH`, plus the four length bytes.
const MAX_AUTH_MESSAGE_LENGTH: i32 = 65_535 + 4;

async fn read_startup_packet<S>(stream: &mut S) -> io::Result<(i32, Vec<u8>)>
where
    S: AsyncRead + Unpin,
{
    let len = read_i32_be(stream).await?;
    // Bounded before allocating: the length is chosen by a client that has not
    // authenticated, and up to 2 GiB of `vec![0; len]` was an allocation the
    // process could not survive. PostgreSQL caps the startup packet at 10,000
    // bytes (MAX_STARTUP_PACKET_LENGTH) for the same reason.
    if !(8..=MAX_STARTUP_PACKET_LENGTH).contains(&len) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid startup packet",
        ));
    }
    let mut buf = vec![0u8; (len - 4) as usize];
    stream.read_exact(&mut buf).await?;
    Ok((len, buf))
}

/// [`read_startup_packet`], telling the client why before the connection is
/// closed over a length out of range.
///
/// The refusal used to be silent — the socket simply closed — so a client
/// with a broken framing layer saw a reset and nothing to say what it had
/// sent wrong. `08P01` with PostgreSQL's wording for it. Best-effort, and
/// bounded by the handshake deadline underneath: the client that sent a
/// nonsense length may not be reading at all.
async fn read_startup_packet_or_refuse<S>(stream: &mut S) -> io::Result<(i32, Vec<u8>)>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    match read_startup_packet(stream).await {
        Err(e) if e.kind() == io::ErrorKind::InvalidData => {
            let _ =
                send_fatal_error_response(stream, "08P01", "invalid length of startup packet")
                    .await;
            Err(e)
        }
        other => other,
    }
}

async fn send_fatal_error_response<S>(
    stream: &mut S,
    sqlstate: &str,
    message: &str,
) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let mut payload = Vec::new();

    payload.push(b'S');
    payload.extend_from_slice(b"FATAL");
    payload.push(0);

    payload.push(b'C');
    payload.extend_from_slice(sqlstate.as_bytes());
    payload.push(0);

    payload.push(b'M');
    payload.extend_from_slice(message.as_bytes());
    payload.push(0);

    payload.push(0);

    send_message(stream, b'E', &payload).await?;
    // The session ends after this; nothing else will flush it.
    stream.flush().await
}

/// Installs the process-wide crypto provider rustls needs.
///
/// rustls 0.23 refuses to build a configuration until one is chosen, and
/// panics rather than erroring if it is missing. Installing it once here means
/// a TLS listener works with no set-up from the caller.
fn ensure_crypto_provider() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        // Already installed by another component is not a failure.
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    });
}

fn build_tls_acceptor(tls: &TlsConfig) -> io::Result<TlsAcceptor> {
    ensure_crypto_provider();
    let (certs, key) = match (&tls.cert_path, &tls.key_path) {
        (Some(cert_path), Some(key_path)) => load_tls_from_pem(cert_path, key_path)?,
        (None, None) => {
            // Reuse rastcp's self-signed generator.
            rastcp::cert::generate_self_signed_cert()
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?
        }
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "pgwire TLS requires both --pg-cert and --pg-key, or neither",
            ));
        }
    };

    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;

    Ok(TlsAcceptor::from(StdArc::new(config)))
}

fn load_tls_from_pem(
    cert_path: &str,
    key_path: &str,
) -> io::Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
    let cert_bytes = std::fs::read(cert_path)?;
    let key_bytes = std::fs::read(key_path)?;

    let mut cert_reader = BufReader::new(cert_bytes.as_slice());
    let certs = rustls_pemfile::certs(&mut cert_reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;

    let mut key_reader = BufReader::new(key_bytes.as_slice());
    let key = rustls_pemfile::private_key(&mut key_reader)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "no private key found"))?
        .clone_key();

    Ok((certs, key))
}

fn read_cstring(bytes: &[u8]) -> &str {
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    std::str::from_utf8(&bytes[..end]).unwrap_or("")
}

#[cfg(test)]
mod descriptor_budget_tests {
    use super::*;

    /// A soft limit short of the cap is raised as far as the cap needs, and
    /// the cap then stands.
    #[test]
    fn a_short_soft_limit_is_raised_to_what_the_cap_needs() {
        assert_eq!(
            plan_descriptors(500, 1024, 524_288),
            DescriptorPlan {
                raise_soft_to: Some(500 * FDS_PER_CONNECTION + RESERVED_SERVER_FDS),
                max_connections: 500,
            }
        );
    }

    /// A hard limit the cap cannot fit in lowers the cap, rather than leaving
    /// a cap that runs the process out of descriptors before it refuses
    /// anyone.
    #[test]
    fn a_hard_limit_too_low_for_the_cap_lowers_it() {
        assert_eq!(
            plan_descriptors(500, 1024, 1024),
            DescriptorPlan {
                raise_soft_to: None,
                max_connections: (1024 - RESERVED_SERVER_FDS as usize) / 4,
            }
        );
        assert_eq!(
            plan_descriptors(500, 1024, 2048),
            DescriptorPlan {
                raise_soft_to: Some(2048),
                max_connections: (2048 - RESERVED_SERVER_FDS as usize) / 4,
            }
        );
    }

    /// Enough descriptors already: nothing is raised and nothing lowered,
    /// including under an unlimited soft limit.
    #[test]
    fn a_sufficient_limit_changes_nothing() {
        assert_eq!(
            plan_descriptors(500, 65_536, 65_536),
            DescriptorPlan { raise_soft_to: None, max_connections: 500 }
        );
        assert_eq!(
            plan_descriptors(usize::MAX, u64::MAX, u64::MAX).raise_soft_to,
            None
        );
    }

    /// A limit below the reserve still admits one session rather than none.
    #[test]
    fn the_cap_never_falls_below_one() {
        assert_eq!(connections_within(500, 64), 1);
    }
}

#[cfg(test)]
mod engine_notice_tests {
    use super::*;

    /// A notice the engine raises during a statement reaches the client as a
    /// `NoticeResponse` with S, V, C and M fields, before that statement's
    /// `CommandComplete` — the order PostgreSQL uses and drivers expect.
    #[tokio::test]
    async fn engine_notices_precede_command_complete() {
        let (mut ours, mut theirs) = tokio::io::duplex(4096);
        crate::core::notice::collect(async {
            crate::core::notice::push("WARNING", "01000", "space not returned");
            send_command_complete(&mut ours, "VACUUM").await.unwrap();
            // Sent once: the next statement starts clean.
            send_command_complete(&mut ours, "SELECT 0").await.unwrap();
        })
        .await;
        drop(ours);
        let mut bytes = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut theirs, &mut bytes)
            .await
            .unwrap();

        assert_eq!(bytes[0], b'N', "the notice must come first");
        let len = i32::from_be_bytes(bytes[1..5].try_into().unwrap()) as usize;
        let body = &bytes[5..1 + len];
        let fields: Vec<(u8, String)> = body
            .split(|b| *b == 0)
            .filter(|f| !f.is_empty())
            .map(|f| (f[0], String::from_utf8_lossy(&f[1..]).to_string()))
            .collect();
        assert_eq!(
            fields,
            vec![
                (b'S', "WARNING".to_string()),
                (b'V', "WARNING".to_string()),
                (b'C', "01000".to_string()),
                (b'M', "space not returned".to_string()),
            ]
        );
        let rest = &bytes[1 + len..];
        assert_eq!(rest[0], b'C');
        let tags: Vec<u8> = {
            let mut tags = Vec::new();
            let mut i = 0;
            while i < rest.len() {
                tags.push(rest[i]);
                let l = i32::from_be_bytes(rest[i + 1..i + 5].try_into().unwrap()) as usize;
                i += 1 + l;
            }
            tags
        };
        assert_eq!(tags, vec![b'C', b'C'], "the notice was sent twice");
    }
}

#[cfg(test)]
mod pre_auth_length_tests {
    use super::*;

    /// A client that has not authenticated chooses these lengths. A 2 GiB
    /// claim must be refused as malformed before anything is allocated — the
    /// old code allocated it and only then failed to read (or, on a smaller
    /// machine, took the process down).
    fn run<F: std::future::Future>(f: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(f)
    }

    #[test]
    fn an_oversized_startup_packet_is_refused_before_allocating() {
        run(async {
        for len in [i32::MAX, MAX_STARTUP_PACKET_LENGTH + 1] {
            let bytes = len.to_be_bytes();
            let mut stream: &[u8] = &bytes;
            let err = read_startup_packet(&mut stream).await.unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::InvalidData, "length {len}");
        }
        // The largest legal one still reads.
        let mut packet = MAX_STARTUP_PACKET_LENGTH.to_be_bytes().to_vec();
        packet.resize(MAX_STARTUP_PACKET_LENGTH as usize, 0);
        let mut stream: &[u8] = &packet;
        assert!(read_startup_packet(&mut stream).await.is_ok());
        });
    }

    #[test]
    fn an_oversized_auth_message_is_refused_before_allocating() {
        run(async {
        let mut bytes = vec![b'p'];
        bytes.extend_from_slice(&i32::MAX.to_be_bytes());
        let mut stream: &[u8] = &bytes;
        let err = read_message(&mut stream).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        });
    }
}

#[cfg(test)]
mod session_end_tests {
    use super::*;
    use crate::core::database::DEFAULT_DATABASE_NAME;

    /// A session ended by its idle timeout must close even when the client
    /// has stopped reading, rather than wait forever on its goodbye.
    ///
    /// A 16-byte pipe stands in for a client whose receive window has shut:
    /// it reads the startup replies, then nothing more, so the `FATAL` —
    /// longer than the pipe — can never be written in full. Both the error and
    /// the final flush behind it must give up, or the session keeps its
    /// connection slot for as long as the client keeps the socket. Over TCP
    /// the same state needs the kernel's send buffer filled to within a few
    /// bytes of its auto-tuned size, which no test can arrange reliably.
    #[tokio::test]
    async fn an_idle_session_ends_even_when_the_client_is_not_reading() {
        let _ = crate::MAX_PERMITS_THREADS.set(4);
        let _ = crate::BATCH_SIZE.set(10_000);
        let catalog = DatabaseCatalog::open(None, DEFAULT_DATABASE_NAME)
            .await
            .expect("in-memory catalogue");
        let auth = AuthConfig {
            method: crate::auth::AuthMethod::Trust,
            ..AuthConfig::default()
        };
        let context = ConnectionContext {
            too_many: false,
            handshake_finished: StdArc::new(AtomicBool::new(false)),
            limits: ConnectionLimits {
                idle_session_timeout: Duration::from_millis(100),
                ..ConnectionLimits::default()
            },
        };
        let parameters = HashMap::from([("user".to_string(), "rasterized".to_string())]);

        let (mut server_end, mut client_end) = tokio::io::duplex(16);
        // Boxed: the session future holds the whole statement path.
        let session = Box::pin(finish_startup(
            catalog,
            &mut server_end,
            &parameters,
            &auth,
            &context,
        ));
        let client = async {
            // The startup replies, through ReadyForQuery; then silence, with
            // the pipe still open.
            loop {
                let mut header = [0u8; 5];
                client_end.read_exact(&mut header).await.expect("startup reply");
                let len = i32::from_be_bytes(header[1..5].try_into().unwrap()) as usize;
                let mut body = vec![0u8; len - 4];
                client_end.read_exact(&mut body).await.expect("startup reply body");
                if header[0] == b'Z' {
                    break;
                }
            }
        };
        let (ended, ()) =
            tokio::join!(tokio::time::timeout(Duration::from_secs(15), session), client);
        let result = ended.expect("the session must end by itself although the client stopped reading");
        // Ended by the bound on its last write, not by anything else.
        assert_eq!(result.expect_err("the FATAL could not be delivered").kind(), io::ErrorKind::TimedOut);
        drop(client_end);
    }
}

#[cfg(test)]
mod time_setting_tests {
    use super::{format_time_setting, parse_time_setting};
    use std::time::Duration;

    #[test]
    fn units_and_bare_numbers_parse_as_postgresql_reads_them() {
        let ms = |v: u64| Ok(Duration::from_millis(v));
        assert_eq!(parse_time_setting("250", 1), ms(250));
        assert_eq!(parse_time_setting("'250'", 1), ms(250));
        assert_eq!(parse_time_setting("5s", 1), ms(5_000));
        assert_eq!(parse_time_setting("'5 s'", 1), ms(5_000));
        assert_eq!(parse_time_setting("1.5s", 1), ms(1_500));
        assert_eq!(parse_time_setting("2min", 1), ms(120_000));
        assert_eq!(parse_time_setting("1h", 1), ms(3_600_000));
        assert_eq!(parse_time_setting("1d", 1), ms(86_400_000));
        assert_eq!(parse_time_setting("1500us", 1), ms(2));
        assert_eq!(parse_time_setting("0", 1), ms(0));
        // A bare number for `authentication_timeout` is seconds.
        assert_eq!(parse_time_setting("60", 1_000), ms(60_000));
    }

    /// Each of these used to be — or could easily become — a zero, which
    /// disables the timeout instead of setting it.
    #[test]
    fn anything_else_is_refused_rather_than_read_as_zero() {
        for bad in ["", "soon", "-1", "5 parsecs", "1us", "nan", "inf", "99999999999", "1e3"] {
            assert!(parse_time_setting(bad, 1).is_err(), "{:?} must be refused", bad);
        }
    }

    #[test]
    fn show_uses_the_largest_exact_unit() {
        let show = |v: u64| format_time_setting(Duration::from_millis(v));
        assert_eq!(show(0), "0");
        assert_eq!(show(200), "200ms");
        assert_eq!(show(1_500), "1500ms");
        assert_eq!(show(5_000), "5s");
        assert_eq!(show(60_000), "1min");
        assert_eq!(show(90_000), "90s");
        assert_eq!(show(7_200_000), "2h");
        assert_eq!(show(86_400_000), "1d");
    }

    /// What `SHOW` prints, `SET` reads back to the same value.
    #[test]
    fn show_output_round_trips() {
        for v in [1u64, 200, 1_500, 5_000, 60_000, 90_000, 7_200_000, 86_400_000] {
            let d = Duration::from_millis(v);
            assert_eq!(parse_time_setting(&format_time_setting(d), 1), Ok(d));
        }
    }
}

#[cfg(test)]
mod sqlstate_tests {
    use super::sqlstate_for;

    #[test]
    fn engine_failures_map_to_the_codes_drivers_branch_on() {
        for (message, code) in [
            (
                "duplicate value for UNIQUE column \"email\" (row 7 already holds it)",
                "23505",
            ),
            (
                "insert or update on table \"orders\" violates foreign key constraint \"fk\": key (1) is not present in \"users\"",
                "23503",
            ),
            (
                "update or delete on table \"users\" violates foreign key constraint \"fk\" on table \"orders\"",
                "23503",
            ),
            (
                "null value in column \"name\" of relation \"users\" violates not-null constraint",
                "23502",
            ),
            (
                "could not serialize access due to concurrent update of row 3 by transaction 9",
                "40001",
            ),
            // The reclaimed-row guard. PMS keys its read retry on this code, so
            // the phrase and the code are a contract, not an implementation
            // detail — the exact text `EvalError::RowMovedUnderScan` produces.
            (
                "a row was reclaimed while the query was reading it; retry the statement",
                "40001",
            ),
            // A uniqueness conflict with an unfinished write that the replays
            // could not wait out. It reached clients as XX000, which no retry
            // loop treats as retryable.
            (
                "UNIQUE column \"k\" is being written by another transaction",
                "40001",
            ),
            (
                "UNIQUE constraint users_email_live (email) is being written by another transaction",
                "40001",
            ),
            ("relation 'nope' does not exist", "42P01"),
            ("Table not found", "42P01"),
            ("column 'nope' does not exist", "42703"),
            ("operator does not exist: date = text", "42883"),
            (
                "could not compile the query plan: operator does not exist: jsonb = integer",
                "42883",
            ),
            ("Table already exists", "42P07"),
            ("column 'x' already exists", "42701"),
            ("constraint 'u' already exists on 'users'", "42710"),
            ("value 300 is out of range for type U8", "22003"),
            ("permission denied: CREATE ROLE requires superuser", "42501"),
            ("NOT LIKE is not supported yet", "0A000"),
            ("'FOR UPDATE NOWAIT SKIP LOCKED' is not a supported locking clause", "0A000"),
            (
                "unexpected Ident(\"x\") after the end of the expression",
                "42601",
            ),
            ("something nobody anticipated", "XX000"),
            (
                "could not commit: the rows written could not be made durable (could not \
                 reserve transaction ids from 9 in /d/db_transactions.next (the disk is full): \
                 No space left on device (os error 28). Transaction 9 was rolled back: \
                 committed under an id that is not reserved, its rows would be taken as \
                 another transaction's after a restart)",
                "53100",
            ),
            (
                "could not reserve transaction ids from 9 in /d/db_transactions.next: \
                 Input/output error (os error 5). Transaction 9 was rolled back",
                "58030",
            ),
        ] {
            assert_eq!(sqlstate_for(message), code, "{message:?}");
        }
    }
}

#[cfg(test)]
mod authorization_tests {
    use super::{Principal, privileged_statement};

    #[test]
    fn administration_is_recognised_regardless_of_case_and_spacing() {
        for (sql, verb) in [
            ("CREATE USER mallory PASSWORD 'x' SUPERUSER", "CREATE ROLE"),
            ("  create\n role r", "CREATE ROLE"),
            ("ALTER USER u PASSWORD 'y'", "ALTER ROLE"),
            ("drop role r;", "DROP ROLE"),
            ("CREATE DATABASE staging", "CREATE DATABASE"),
            ("DROP DATABASE IF EXISTS staging", "DROP DATABASE"),
            (
                "IMPORT DATABASE FROM POSTGRES 'postgresql://x' INTO y",
                "IMPORT DATABASE",
            ),
            ("SET CONFIG filter.max_f = 14", "SET CONFIG"),
            ("SET FILTER max_f = 14", "SET FILTER"),
            ("RESET FILTER", "SET FILTER"),
            ("BACKUP DATABASE", "BACKUP"),
            ("RESTORE DATABASE FROM S3 TO '/etc'", "RESTORE"),
            ("STORAGE ADD LOCATION '/mnt/x'", "STORAGE"),
            ("REPLICATION ENABLE", "REPLICATION"),
            ("KV FLUSHALL", "KV FLUSHALL"),
            ("kv save", "KV SAVE"),
            ("KV BGSAVE", "KV SAVE"),
            ("KV CONFIG SET maxmemory 1gb", "KV CONFIG"),
            ("KV CONFIG RESETSTAT", "KV CONFIG"),
        ] {
            assert_eq!(privileged_statement(sql), Some(verb), "{sql:?}");
        }
    }

    #[test]
    fn reading_state_and_ordinary_statements_are_not_administration() {
        for sql in [
            "SHOW BACKUPS",
            "SHOW STORAGE LOCATIONS",
            "SHOW REPLICATION STATS",
            "SHOW CONFIG",
            "SHOW FILTER",
            "KV CONFIG GET maxmemory",
            "KV GET session:1",
            "KV FLUSHDB",
            "SET search_path TO public",
            "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE",
            "SELECT * FROM roles",
            "CREATE TABLE users (id INT)",
            "DROP TABLE users",
            "INSERT INTO t VALUES (1)",
            "VACUUM",
            "",
        ] {
            assert_eq!(privileged_statement(sql), None, "{sql:?}");
        }
    }

    #[test]
    fn only_a_proven_non_superuser_is_refused() {
        let trusted = Principal {
            name: "anyone".into(),
            superuser: false,
            authenticated: false,
        };
        let admin = Principal {
            name: "root".into(),
            superuser: true,
            authenticated: true,
        };
        let user = Principal {
            name: "app".into(),
            superuser: false,
            authenticated: true,
        };
        assert!(trusted.may_administer(), "trust has no one to refuse");
        assert!(admin.may_administer());
        assert!(!user.may_administer());
    }
}

#[cfg(test)]
mod command_tag_tests {
    #[test]
    fn an_integer_is_never_rendered_as_the_ascii_its_bytes_spell() {
        // Reported from the console: a table's `id` column showed `05`, `15`,
        // `25` … in the middle of a page of five-digit ids. 13616 is 0x3530,
        // whose little-endian bytes are 0x30 0x35 — ASCII "05". The renderer
        // consulted that spelling before decoding the integer, so any value
        // whose bytes happen to be ASCII digits came back as text. It affects
        // the plainest values there are: 48 rendered as "0" and 57 as "9".
        for value in [
            0i32, 1, 47, 48, 49, 53, 57, 58, 12336, 12337, 13616, 13625, 14649, 65536,
        ] {
            assert_eq!(
                format_column_text(DbType::I32, &value.to_le_bytes()),
                Some(value.to_string()),
                "i32 {} rendered wrongly",
                value
            );
        }
        for value in [48i64, 57, 3472328296227680304] {
            assert_eq!(
                format_column_text(DbType::I64, &value.to_le_bytes()),
                Some(value.to_string()),
                "i64 {} rendered wrongly",
                value
            );
        }
        // A single byte holding ASCII '0' is the number 48, not the digit 0.
        assert_eq!(
            format_column_text(DbType::I8, &[b'0']),
            Some("48".to_string())
        );
    }

    #[test]
    fn a_value_stored_as_text_is_still_read_when_its_width_does_not_match() {
        // The ASCII path exists for values written as decimal text rather than
        // encoded; it stays reachable, just no longer ahead of the type.
        assert_eq!(
            format_column_text(DbType::I32, b"123"),
            Some("123".to_string())
        );
        assert_eq!(
            format_column_text(DbType::I64, b"4567"),
            Some("4567".to_string())
        );
    }

    use super::*;

    /// A migration runner reads the tag back to confirm which statement ran, so
    /// DDL must not all report `OK`.
    #[test]
    fn ddl_reports_the_statement_that_ran() {
        assert_eq!(command_tag(PurposeKind::CreateTable, None), "CREATE TABLE");
        assert_eq!(command_tag(PurposeKind::DropTable, None), "DROP TABLE");
        assert_eq!(command_tag(PurposeKind::AlterTable, None), "ALTER TABLE");
        assert_eq!(command_tag(PurposeKind::Vacuum, None), "VACUUM");
    }

    #[test]
    fn dml_carries_the_row_count() {
        assert_eq!(command_tag(PurposeKind::InsertRow, Some(1)), "INSERT 0 1");
        assert_eq!(command_tag(PurposeKind::UpdateRow, Some(7)), "UPDATE 7");
        assert_eq!(command_tag(PurposeKind::DeleteRow, Some(0)), "DELETE 0");
    }

    /// The engine does not always report a count. A tag of the right shape with
    /// a zero count is still parseable by a client; `OK` is not.
    #[test]
    fn a_missing_count_still_produces_a_well_formed_tag() {
        assert_eq!(command_tag(PurposeKind::InsertRow, None), "INSERT 0 0");
        assert_eq!(command_tag(PurposeKind::UpdateRow, None), "UPDATE 0");
        assert_eq!(command_tag(PurposeKind::DeleteRow, None), "DELETE 0");
    }

    /// A row count on a DDL tag would be malformed — PostgreSQL sends none.
    #[test]
    fn ddl_never_carries_a_count() {
        assert_eq!(
            command_tag(PurposeKind::CreateTable, Some(3)),
            "CREATE TABLE"
        );
        assert_eq!(command_tag(PurposeKind::AlterTable, Some(3)), "ALTER TABLE");
    }
}

/// Renders a float the way PostgreSQL does.
///
/// Rust writes the non-finite values as `inf`, `-inf` and `NaN`; PostgreSQL
/// writes `Infinity`, `-Infinity` and `NaN`, and that is what a client parses.
/// A driver handed `inf` for a `float8` rejects the row — the value was right
/// and unreadable, which is the worst combination.
fn format_float(value: f64) -> String {
    match non_finite(
        value.is_nan(),
        value.is_infinite(),
        value.is_sign_negative(),
    ) {
        Some(text) => text.to_string(),
        None => value.to_string(),
    }
}

/// The same, at single precision.
fn format_float_32(value: f32) -> String {
    match non_finite(
        value.is_nan(),
        value.is_infinite(),
        value.is_sign_negative(),
    ) {
        Some(text) => text.to_string(),
        None => value.to_string(),
    }
}

fn non_finite(is_nan: bool, is_infinite: bool, is_negative: bool) -> Option<&'static str> {
    if is_nan {
        Some("NaN")
    } else if is_infinite {
        Some(if is_negative { "-Infinity" } else { "Infinity" })
    } else {
        None
    }
}

#[cfg(test)]
mod describe_placeholder_tests {
    use super::{has_placeholder, substitute_placeholders};

    #[test]
    fn a_statement_describe_substitutes_its_placeholders() {
        // `Describe(statement)` arrives before any `Bind`, so the SQL still
        // holds `$1` and does not parse. The describe then answered `NoData`,
        // and a driver caching the statement's column list cached *no columns*:
        // every read came back with the right values under no resolvable
        // names, which sea-orm reports as a model whose fields are all null.
        let sql = "SELECT id, key FROM t WHERE key = $1 LIMIT 1";
        assert!(has_placeholder(sql));
        assert_eq!(
            substitute_placeholders(sql, "NULL"),
            "SELECT id, key FROM t WHERE key = NULL LIMIT 1"
        );
        assert_eq!(
            substitute_placeholders(sql, "'1'"),
            "SELECT id, key FROM t WHERE key = '1' LIMIT 1"
        );
    }

    #[test]
    fn a_placeholder_past_the_ninth_is_replaced_whole() {
        // `$10` is one placeholder, not `$1` followed by a `0`.
        assert_eq!(
            substitute_placeholders("SELECT a FROM t WHERE b = $10", "1"),
            "SELECT a FROM t WHERE b = 1"
        );
    }

    #[test]
    fn a_dollar_inside_a_literal_is_data() {
        // Otherwise a row holding '$1' had its own text rewritten.
        let sql = "SELECT a FROM t WHERE b = '$1' AND c = $2";
        assert!(has_placeholder(sql));
        assert_eq!(
            substitute_placeholders(sql, "9"),
            "SELECT a FROM t WHERE b = '$1' AND c = 9"
        );
    }

    #[test]
    fn a_statement_without_placeholders_is_left_alone() {
        let sql = "SELECT a FROM t WHERE b = 'x'";
        assert!(!has_placeholder(sql));
        assert_eq!(substitute_placeholders(sql, "NULL"), sql);
    }

    #[test]
    fn a_bare_dollar_is_not_a_placeholder() {
        assert!(!has_placeholder("SELECT price$ FROM t"));
    }
}

#[cfg(test)]
mod setting_tests {
    use super::*;

    /// The spellings clients actually send. `SET x = v`, `SET x TO v`, with and
    /// without `SESSION`/`LOCAL`, quoted or bare, and with a trailing `;`.
    #[test]
    fn the_spellings_a_client_sends_are_all_recognised() {
        for stmt in [
            "set synchronous_commit = off",
            "set synchronous_commit=off",
            "set synchronous_commit to off",
            "set session synchronous_commit = off",
            "set local synchronous_commit = off",
            "set synchronous_commit = 'off';",
        ] {
            let parsed = strip_setting(stmt, "synchronous_commit")
                .unwrap_or_else(|| panic!("not recognised: {}", stmt))
                .unwrap_or_else(|| panic!("read as a reset: {}", stmt));
            assert_eq!(
                crate::parse_synchronous_commit(&parsed),
                Some(false),
                "{} did not read as off",
                stmt
            );
        }
    }

    /// `RESET` and `= DEFAULT` both mean "follow the server again", which is a
    /// different answer from "off" and must not be confused with it.
    #[test]
    fn reset_and_default_ask_to_follow_the_server() {
        assert_eq!(
            strip_setting("reset synchronous_commit", "synchronous_commit"),
            Some(None)
        );
        assert_eq!(
            strip_setting("set synchronous_commit = default", "synchronous_commit"),
            Some(None)
        );
    }

    /// A key that merely starts with this one's name is a different key. Without
    /// the guard for it, `SET synchronous_commit_timeout = 5` would be swallowed
    /// here and silently change durability instead.
    #[test]
    fn a_longer_key_that_starts_the_same_is_not_this_one() {
        assert_eq!(
            strip_setting("set synchronous_commit_timeout = 5", "synchronous_commit"),
            None
        );
        assert_eq!(
            strip_setting("reset synchronous_commit_timeout", "synchronous_commit"),
            None
        );
    }

    #[test]
    fn other_statements_are_left_alone() {
        for stmt in [
            "select 1",
            "set search_path = public",
            "show synchronous_commit",
            "set transaction isolation level serializable",
        ] {
            assert_eq!(
                strip_setting(stmt, "synchronous_commit"),
                None,
                "claimed: {}",
                stmt
            );
        }
    }
}

/// Statement families whose rows belong to no relation must be describable.
///
/// The failure this guards is silent and one-sided: `Execute` sends the rows
/// whatever happens, so the row *count* is right and only the field list is
/// missing. `psql` never sees it — the simple query path serialises the columns
/// from the same `Rows` — while every extended-protocol client (sqlx, sea-orm,
/// and so any Rust client) reads `column_names()` as `[]` and fails every
/// `try_get`. It reached a user as "rows=2, first_cols=Some([])".
#[cfg(test)]
mod describe_rowless_family_tests {
    use super::describe_query;
    use crate::core::database::Database;
    use rclite::Arc;

    async fn open() -> (tempfile::TempDir, Arc<Database>) {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = Arc::new(
            Database::new_named(&dir.path().to_string_lossy(), false, "describetests").await,
        );
        (dir, db)
    }

    #[cfg(feature = "s3_backup")]
    #[tokio::test(flavor = "multi_thread")]
    async fn show_backups_describes_its_columns() {
        let (_dir, db) = open().await;

        let (fields, is_select) = describe_query(&db, "SHOW BACKUPS")
            .await
            .expect("describe failed")
            .expect("SHOW BACKUPS described as returning no columns");

        assert!(
            is_select,
            "SHOW BACKUPS returns rows, so it must describe as a select"
        );
        let names: Vec<&str> = fields.iter().map(|(name, _)| name.as_str()).collect();
        assert_eq!(
            names,
            vec![
                "kind",
                "id",
                "created_at",
                "start_lsn",
                "end_lsn",
                "uncompressed_bytes",
                "compressed_bytes",
                "detail",
            ],
            "the described columns must be the ones the executor emits"
        );
    }

    #[cfg(feature = "s3_backup")]
    #[tokio::test(flavor = "multi_thread")]
    async fn restore_describes_its_columns() {
        let (_dir, db) = open().await;

        let (fields, is_select) =
            describe_query(&db, "RESTORE DATABASE FROM S3 TO '/tmp/describe-only'")
                .await
                .expect("describe failed")
                .expect("RESTORE described as returning no columns");

        assert!(is_select);
        assert_eq!(fields.first().map(|(name, _)| name.as_str()), Some("snapshot_id"));
    }

    /// A counted statement is *correctly* undescribable — it reports a count,
    /// not rows, and `NoData` is the honest answer. Pinned so that a future fix
    /// to the two above does not "fix" these into claiming columns they never
    /// send.
    #[cfg(feature = "s3_backup")]
    #[tokio::test(flavor = "multi_thread")]
    async fn the_counted_backup_statements_describe_as_no_rows() {
        let (_dir, db) = open().await;

        for sql in ["BACKUP DATABASE", "BACKUP WAL", "BACKUP PRUNE"] {
            let described = describe_query(&db, sql).await.expect("describe failed");
            assert!(
                described.is_none() || !described.unwrap().1,
                "{sql} reports a count, so it must not describe as returning rows"
            );
        }
    }

    /// The sibling families, which were fixed when this bug class was first
    /// found. Here so that the next one to grow a statement surface is caught
    /// by a test that already names the trap.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_other_rowless_families_still_describe() {
        let (_dir, db) = open().await;

        let mut cases: Vec<&str> = vec!["SHOW REPLICATION"];
        if cfg!(feature = "storage_pools") {
            cases.push("SHOW STORAGE");
        }

        for sql in cases {
            let (fields, is_select) = describe_query(&db, sql)
                .await
                .expect("describe failed")
                .unwrap_or_else(|| panic!("{sql} described as returning no columns"));
            assert!(is_select, "{sql}");
            assert!(
                !fields.is_empty(),
                "{sql} described with an empty column list"
            );
        }
    }
}

#[cfg(test)]
mod union_widening_tests {
    use super::*;

    async fn open() -> (tempfile::TempDir, Arc<Database>) {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = Arc::new(Database::new_named(&dir.path().to_string_lossy(), false, "uniontests").await);
        for sql in [
            "CREATE TABLE t32 (id INT, name TEXT, n INT)",
            "CREATE TABLE t64 (a BIGINT, b BIGINT, c BIGINT, d BIGINT)",
        ] {
            let purpose = recognize_query_purpose(sql).expect(sql);
            crate::core::rql::executor::execute(purpose, db.clone()).await;
        }
        (dir, db)
    }

    /// A prepared `LIMIT $1` is first described with the stand-in `LIMIT
    /// NULL`, which `split_union` refuses. The widening was read only from a
    /// split that succeeded, so this text kept the first branch's `int`, while
    /// the statement executed with a bound number is widened to `bigint`. Only
    /// the stand-in being refused further up kept that from the wire.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_unreadable_union_tail_still_widens() {
        let (_dir, db) = open().await;
        for sql in [
            "SELECT n FROM t32 UNION SELECT d FROM t64 ORDER BY 1 LIMIT NULL",
            "SELECT n FROM t32 UNION SELECT d FROM t64 ORDER BY 1 LIMIT 10 OFFSET NULL",
        ] {
            let fields = vec![("n".to_string(), DbType::I32)];
            let widened = widen_union_fields(&db, sql, &[], fields).await.expect(sql);
            assert_eq!(widened, vec![("n".to_string(), DbType::I64)], "{sql}");
        }
    }

    /// A `*` is several columns, so the items after it are not at the
    /// positions their places in the list say. `SELECT *, n FROM t32` was
    /// typed `[?, int]`, and paired with `t64`'s second column that widened
    /// output column 1, `name`, a text column, to `bigint`. The engine refuses
    /// `*` beside other items today, which is all that kept this from a
    /// client; PostgreSQL 17 refuses the `UNION` itself, as text and bigint.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_star_does_not_shift_the_widening_onto_another_column() {
        let (_dir, db) = open().await;
        assert_eq!(crate::core::rql::cte::declared_item_types("SELECT *, n FROM t32", &db), vec![]);
        assert_eq!(
            crate::core::rql::cte::declared_item_types("SELECT n, *, id FROM t32", &db),
            vec![Some(DbType::I32)],
            "the items before the `*` are where they say",
        );
        let branches: Vec<String> =
            crate::core::rql::join::union_branches("SELECT *, n FROM t32 UNION ALL SELECT a, b, c, d FROM t64")
                .expect("a UNION");
        let widened = crate::core::rql::set_ops::widened_branch_integers(&branches, &db, &[]);
        assert!(widened.iter().all(Option::is_none), "{widened:?}");
    }
}

#[cfg(all(test, feature = "storage_pools"))]
mod cross_dir_commit_order_tests {
    use super::*;
    use crate::core::storage::LocationRole;
    use crate::core::sync_audit;

    async fn run<S: AsyncRead + AsyncWrite + Unpin>(
        db: &Arc<Database>,
        state: &mut SessionState,
        stream: &mut S,
        sql: &str,
    ) {
        Box::pin(execute_single_statement(db, state, stream, sql, &[], false))
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }

    /// Reruns allowed when the log writer's round takes a commit's sync.
    const MAX_ATTEMPTS: usize = 5;

    /// Log syncs of `watch`'s directory since the last call: those issued
    /// from this thread, and those from any other.
    fn log_syncs(watch: &sync_audit::Watch) -> (usize, usize) {
        let me = std::thread::current().id();
        let logs: Vec<_> = watch
            .take_events()
            .into_iter()
            .filter(|e| e.tag == "wal.log")
            .collect();
        let here = logs.iter().filter(|e| e.thread == me).count();
        (here, logs.len() - here)
    }

    /// Whether a commit's sync of `watch`'s log was seen on this thread.
    /// `None` when it was not but another thread synced that log in the
    /// window — the log writer, whose round leaves the commit's sync nothing
    /// to do and so records nothing: inconclusive, to be rerun. Panics with
    /// `what` when nothing synced it at all.
    fn commit_synced_here(
        watch: &sync_audit::Watch,
        earlier_foreign: usize,
        what: &str,
    ) -> Option<()> {
        let (here, elsewhere) = log_syncs(watch);
        if here > 0 {
            return Some(());
        }
        assert!(earlier_foreign + elsewhere > 0, "{what}");
        None
    }

    /// `COMMIT` and an autocommit statement whose CTE writes both sync the
    /// other storage locations before the commit, also under
    /// `synchronous_commit = off`. Nothing else on the session's thread syncs
    /// those logs in that mode (the root's sync is skipped, which is checked at
    /// `COMMIT`), so a sync seen here can only be
    /// `commit_written_transaction`'s. Once
    /// committed, any session's root flush can make the outcome durable, and it
    /// must not get there ahead of the rows: the crash-image tests in
    /// `core::database::cross_dir_commit_tests` show what this order buys.
    ///
    /// The process-wide log writer syncs every log on a timer. A round landing
    /// between a write and its commit makes the commit's sync a no-op, which
    /// records nothing, so such an attempt is rerun instead of being read as a
    /// missing sync; if every attempt is disturbed the test proves nothing and
    /// says so rather than failing.
    #[tokio::test(flavor = "multi_thread")]
    async fn commits_sync_other_locations_first_without_waiting() {
        let _ = crate::MAX_PERMITS_THREADS.set(4);
        let _ = crate::BATCH_SIZE.set(10_000);
        let base = tempfile::tempdir().unwrap();
        let root = base.path().join("root");
        let disk = base.path().join("disk");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::create_dir_all(&disk).unwrap();
        let root_s = root.to_string_lossy().into_owned();
        let disk_s = disk.to_string_lossy().into_owned();

        let db = Arc::new(Database::new_named(&root_s, false, "pgcrossdir").await);
        db.storage
            .add_location(&disk_s, LocationRole::Primary, 1, true)
            .unwrap();
        db.storage.pin("pg_disk", &disk_s).unwrap();

        let catalog = DatabaseCatalog::single(db.clone());
        let auth = AuthConfig {
            method: crate::auth::AuthMethod::Trust,
            ..AuthConfig::default()
        };
        let principal = Principal {
            name: "rasterized".to_string(),
            superuser: false,
            authenticated: false,
        };
        let mut state = SessionState::new(
            catalog,
            db.name.clone(),
            auth,
            principal,
            ConnectionLimits::default(),
        );
        // Large enough for every reply, so nothing has to drain it meanwhile.
        let (mut server, mut client) = tokio::io::duplex(1 << 20);

        run(&db, &mut state, &mut server, "CREATE TABLE pg_disk (id INT, label TEXT)").await;
        assert!(
            disk.join("pg_disk.db").exists(),
            "the table is not on the second location, so there is nothing to test"
        );
        // Reserves a chunk of row ids now, a write that syncs the second
        // location's log by itself and would otherwise land inside a statement
        // below and pass for the commit's sync.
        run(&db, &mut state, &mut server, "INSERT INTO pg_disk (id, label) VALUES (0, 'warm_disk')").await;
        state.synchronous_commit = Some(false);
        let root_watch = sync_audit::watch(&root);
        let disk_watch = sync_audit::watch(&disk);

        // An explicit block. A foreign sync during the INSERT counts too: it
        // may land after the row and before COMMIT.
        let mut confirmed = false;
        for attempt in 1..=MAX_ATTEMPTS {
            let _ = (log_syncs(&root_watch), log_syncs(&disk_watch));
            run(&db, &mut state, &mut server, "BEGIN").await;
            let insert =
                format!("INSERT INTO pg_disk (id, label) VALUES ({attempt}, 'block_disk')");
            run(&db, &mut state, &mut server, &insert).await;
            let _ = log_syncs(&root_watch);
            let (_, during_insert) = log_syncs(&disk_watch);
            run(&db, &mut state, &mut server, "COMMIT").await;
            assert_eq!(
                log_syncs(&root_watch).0,
                0,
                "synchronous_commit = off still synced the root log at COMMIT, so this \
                 test cannot tell the pre-commit sync from the post-commit one"
            );
            if commit_synced_here(
                &disk_watch,
                during_insert,
                "COMMIT committed a write to the second location without syncing that \
                 location's log first",
            )
            .is_some()
            {
                confirmed = true;
                break;
            }
            eprintln!("COMMIT: attempt {attempt} disturbed by a background sync");
        }
        if !confirmed {
            eprintln!("COMMIT: every attempt was disturbed by a background sync");
        }

        // An autocommit statement that writes through a CTE, which pgwire
        // commits itself rather than through the executor. The root is not
        // checked here: dropping the CTE's temporary relation syncs the root's
        // catalogue whatever the commit does.
        let mut confirmed = false;
        for attempt in 1..=MAX_ATTEMPTS {
            let _ = log_syncs(&disk_watch);
            let cte = format!(
                "WITH d AS (INSERT INTO pg_disk (id, label) VALUES ({}, 'cte_disk') \
                 RETURNING id) SELECT id FROM d",
                100 + attempt
            );
            run(&db, &mut state, &mut server, &cte).await;
            if commit_synced_here(
                &disk_watch,
                0,
                "a writing CTE committed to the second location without syncing that \
                 location's log first",
            )
            .is_some()
            {
                confirmed = true;
                break;
            }
            eprintln!("CTE: attempt {attempt} disturbed by a background sync");
        }
        if !confirmed {
            eprintln!("CTE: every attempt was disturbed by a background sync");
        }

        // Both statements really committed and none failed.
        run(&db, &mut state, &mut server, "SELECT label FROM pg_disk").await;
        drop(state);
        drop(server);
        let mut bytes = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut client, &mut bytes)
            .await
            .unwrap();
        let mut rest = &bytes[..];
        while rest.len() >= 5 {
            let len = i32::from_be_bytes(rest[1..5].try_into().unwrap()) as usize;
            assert_ne!(
                rest[0],
                b'E',
                "a statement failed: {}",
                String::from_utf8_lossy(&rest[5..1 + len])
            );
            rest = &rest[1 + len..];
        }
        let text = String::from_utf8_lossy(&bytes);
        assert!(
            text.contains("block_disk") && text.contains("cte_disk"),
            "a commit did not take effect"
        );

        drop(root_watch);
        drop(disk_watch);
        drop(db);
        crate::core::wal::remove_for(&root_s);
        crate::core::wal::remove_for(&disk_s);
    }
}
