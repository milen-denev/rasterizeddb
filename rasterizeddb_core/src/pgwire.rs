use std::{
    collections::HashMap,
    io,
    sync::Arc as StdArc,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

use log::trace;
use rclite::Arc;

use std::io::BufReader;

use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::TlsAcceptor;

use crate::core::{
    database::Database,
    database::catalog::{DatabaseCatalog, DatabaseStatement, parse_database_statement},
    db_type::DbType,
    mvcc::transaction::TxId,
    row::row::{Row, vec_into_rows},
    rql::{
        lexer_query::row_fetch_from_select_query, lexer_s1::QueryPurpose,
        lexer_s1::recognize_query_purpose,
    },
};
use crate::pg_params::{BoundParam, inline_params};

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
    // Every database needs its roles before the first connection tries to
    // authenticate: they are per database, so one seeded catalogue does not
    // make the others reachable.
    for database in catalog.all() {
        if let Err(e) = seed_roles(&database, &auth).await {
            return Err(io::Error::new(io::ErrorKind::Other, e));
        }
    }

    let bind_addr = format!("{}:{}", addr, port);
    let listener = TcpListener::bind(&bind_addr).await?;
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

    let active = StdArc::new(AtomicUsize::new(0));

    loop {
        let (stream, peer) = tokio::select! {
            biased;

            _ = shutdown_rx.changed() => break,

            accepted = listener.accept() => accepted?,
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

        let catalog = catalog.clone();
        let tls_mode = tls.mode;
        let acceptor = acceptor.clone();
        let active = active.clone();
        let auth = auth.clone();

        active.fetch_add(1, Ordering::Relaxed);

        tokio::spawn(async move {
            if let Err(e) = handle_connection(catalog, stream, acceptor, tls_mode, auth).await {
                log::debug!("pgwire connection {} ended: {}", peer, e);
            }

            active.fetch_sub(1, Ordering::Relaxed);
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

const SSL_REQUEST_CODE: i32 = 80877103; // 0x04D2162F (1234, 5679)
const CANCEL_REQUEST_CODE: i32 = 80877102; // 0x04D2162E (1234, 5678)
const GSSENC_REQUEST_CODE: i32 = 80877104; // 0x04D21630 (1234, 5680)

async fn handle_connection(
    catalog: Arc<DatabaseCatalog>,
    mut tcp: TcpStream,
    acceptor: Option<TlsAcceptor>,
    tls_mode: TlsMode,
    auth: AuthConfig,
) -> io::Result<()> {
    // PostgreSQL clients may send one or more 8-byte startup *requests* before the
    // real StartupMessage (e.g., GSSENCRequest, SSLRequest). If we don't answer
    // them correctly, many clients will hang during connect and pools will time out.
    loop {
        let (first_len, first_buf) = read_startup_packet(&mut tcp).await?;
        let first_code = i32::from_be_bytes(first_buf[0..4].try_into().unwrap());

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
                                run_session_after_ssl(catalog, &mut tls_stream, &auth).await;

                            // Close the TLS session before dropping it. Dropping
                            // the stream closes the socket without sending
                            // `close_notify`, which is an unclean shutdown: the
                            // peer cannot tell a finished session from a
                            // truncated one, and sqlx logs a warning every time
                            // it returns a connection to the pool. Best-effort —
                            // the session is over either way, and a client that
                            // has already gone will fail this write.
                            if let Err(e) = tls_stream.shutdown().await {
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
        return run_session_after_startup(catalog, &mut tcp, &parameters, &auth).await;
    }
}

async fn run_session_after_ssl<S>(
    catalog: Arc<DatabaseCatalog>,
    stream: &mut S,
    auth: &AuthConfig,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // After SSL negotiation (either accepted or denied), the client sends StartupMessage.
    let (_, payload) = read_startup_packet(stream).await?;
    let parameters = parse_startup_parameters(&payload);
    finish_startup(catalog, stream, &parameters, auth).await
}

async fn run_session_after_startup(
    catalog: Arc<DatabaseCatalog>,
    stream: &mut TcpStream,
    parameters: &HashMap<String, String>,
    auth: &AuthConfig,
) -> io::Result<()> {
    finish_startup(catalog, stream, parameters, auth).await
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
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
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

    let principal = match authenticate(&database, stream, parameters, auth).await {
        Ok(principal) => {
            log::debug!(
                "authenticated {} via {}",
                principal.name,
                auth.method.name()
            );
            principal
        }
        Err(e) => {
            log::info!("authentication refused: {}", e);
            send_fatal_error_response(stream, "28P01", &e.to_string()).await?;
            return Ok(());
        }
    };

    send_startup_responses(stream).await?;
    run_message_loop(catalog, &database, auth.clone(), principal, stream).await
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
        ("BACKUP", _) => Some("BACKUP"),
        ("RESTORE", _) => Some("RESTORE"),
        ("STORAGE", _) => Some("STORAGE"),
        ("REPLICATION", _) => Some("REPLICATION"),
        _ => None,
    }
}

async fn send_startup_responses<S>(stream: &mut S) -> io::Result<()>
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
    send_backend_key_data(stream).await?;
    send_ready_for_query(stream, b'I').await?;
    Ok(())
}

async fn run_message_loop<S>(
    catalog: Arc<DatabaseCatalog>,
    database: &Arc<Database>,
    auth: AuthConfig,
    principal: Principal,
    stream: &mut S,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut state = SessionState::new(catalog, database.name.clone(), auth, principal);
    // A connection that drops mid-transaction must not leave it running: it
    // would hold back reclamation forever and its versions would never resolve.
    // The loop returns from several places, so the cleanup is a guard rather
    // than a line at the end.
    let result = run_message_loop_inner(database, &mut state, stream).await;
    if let Some(t) = state.transaction.take() {
        log::info!(
            "connection closed with transaction {} open; rolling it back",
            t.id
        );
        database.transactions.abort(t.id);
        flush_outcomes(database);
    }
    result
}

async fn run_message_loop_inner<S>(
    database: &Arc<Database>,
    state: &mut SessionState,
    stream: &mut S,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    loop {
        let mut tag = [0u8; 1];
        match stream.read_exact(&mut tag).await {
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(e) => return Err(e),
        }

        let len = read_i32_be(stream).await?;
        if len < 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid message length",
            ));
        }
        let mut payload = vec![0u8; (len - 4) as usize];
        stream.read_exact(&mut payload).await?;

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
                let status = state.ready_status();
                send_ready_for_query(stream, status).await?;
            }
            b'H' => {
                // Flush: no server-side buffering beyond what AsyncWrite already does.
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
}

impl SessionState {
    fn new(
        catalog: Arc<DatabaseCatalog>,
        database_name: String,
        auth: AuthConfig,
        principal: Principal,
    ) -> Self {
        Self {
            catalog,
            database_name,
            auth,
            principal,
            statements: HashMap::new(),
            portals: HashMap::new(),
            transaction: None,
            default_isolation: IsolationLevel::default(),
            default_read_only: false,
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

struct StatementInfo {
    sql: String,
    param_oids: Vec<i32>,
    /// The column list this statement was last described with, and the schema
    /// generation it was described at.
    ///
    /// A driver caches the description and does not ask again, so an
    /// `ALTER TABLE` between the describe and a later execute leaves it
    /// decoding rows against a shape that no longer exists — a dropped column
    /// keeps appearing, and every column after it is read from the wrong
    /// place. PostgreSQL refuses that with "cached plan must not change result
    /// type" and drivers re-prepare; this is what makes that check possible.
    described: Option<(u64, Vec<(String, DbType)>)>,
}

#[derive(Clone, Debug, Default)]
struct PortalInfo {
    stmt_name: String,
    result_formats: Vec<i16>,
    /// Values bound by the Bind message, positionally. Inlined into the
    /// statement text at Execute time because the engine parses SQL only.
    params: Vec<BoundParam>,
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

    state.statements.insert(
        stmt_name,
        StatementInfo {
            described: None,
            sql: query,
            param_oids,
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
    let Some(param_oids) = state.statements.get(&stmt).map(|s| s.param_oids.clone()) else {
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
            params,
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
        b'S' => (name, vec![], Vec::new()),
        b'P' => match state.portals.get(&name).cloned() {
            Some(info) => (info.stmt_name, info.result_formats, info.params),
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

    let Some(query) = state.statements.get(&stmt_name).map(|s| s.sql.as_str()) else {
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
    let inlined;
    let query = if bound.is_empty() {
        query
    } else {
        match inline_params(query, &bound) {
            Ok(sql) => {
                inlined = sql;
                inlined.as_str()
            }
            Err(_) => query,
        }
    };

    // A statement describe has no bound values to inline, so its placeholders
    // are replaced with a stand-in that parses. See `substitute_placeholders`.
    let mut substituted: Option<String> = None;
    if has_placeholder(query) {
        for stand_in in ["NULL", "1", "'1'"] {
            let candidate = substitute_placeholders(query, stand_in);
            let describable = describe_query(database, &candidate)
                .await?
                .is_some_and(|(_, is_select)| is_select);
            if describable {
                substituted = Some(candidate);
                break;
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
        if substituted.is_none() && expects_rows {
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
    let query = match &substituted {
        Some(sql) => sql.as_str(),
        None => query,
    };

    // A *statement* describe answers with ParameterDescription first. A
    // *portal* describe must not: its parameters are already bound, and the
    // protocol says the reply is RowDescription or NoData alone. The extra `t`
    // message left libpq reading the description as malformed, which surfaced
    // as "D message without prior T".
    if kind == b'S' {
        let param_oids: Vec<i32> = state
            .statements
            .get(&stmt_name)
            .map(|s| s.param_oids.clone())
            .unwrap_or_default();
        send_parameter_description(stream, &param_oids).await?;
    }

    // Try to produce RowDescription for SELECT; otherwise NoData.
    if let Some((fields, is_select)) = describe_query(database, query).await? {
        if is_select {
            // Remembered so a later `Execute` can tell whether the shape the
            // client cached is still the shape the table has.
            if let Some(info) = state.statements.get_mut(&stmt_name) {
                info.described = Some((
                    database
                        .schema_generation
                        .load(std::sync::atomic::Ordering::Acquire),
                    fields
                        .iter()
                        .map(|(n, t)| (n.to_string(), t.clone()))
                        .collect(),
                ));
            }
            send_row_description(stream, &fields, &result_formats).await?;
        } else {
            // NoData
            send_message(stream, b'n', &[]).await?;
        }
        return Ok(());
    }

    send_message(stream, b'n', &[]).await
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
    let _ = read_i32_be_from(payload, i)?;

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
    let (stmt_name, result_formats, params) = (
        info.stmt_name.clone(),
        info.result_formats.clone(),
        info.params.clone(),
    );

    // Owned: the shape check below needs `state` mutably, and holding a
    // borrow of the statement across it would pin the map.
    let query = state
        .statements
        .get(&stmt_name)
        .map(|s| s.sql.clone())
        .unwrap_or_default();
    let query = query.as_str();

    if query.trim().is_empty() {
        state.error_in_batch = true;
        send_error_response(stream, "26000", "prepared statement does not exist").await?;
        return Ok(());
    }

    let mut schema_changed = false;
    // A schema change since this statement was described makes the client's
    // cached column list wrong, and nothing on the wire would tell it. Refused
    // the way PostgreSQL refuses it, which every driver answers by re-preparing
    // — where silently sending rows of a different shape would have it decode
    // them against the old one.
    if let Some((generation, described)) = state
        .statements
        .get(&stmt_name)
        .and_then(|info| info.described.clone())
    {
        let now = database
            .schema_generation
            .load(std::sync::atomic::Ordering::Acquire);
        if now != generation {
            let current = describe_query(database, query)
                .await?
                .filter(|(_, is_select)| *is_select)
                .map(|(fields, _)| {
                    fields
                        .iter()
                        .map(|(n, t)| (n.to_string(), t.clone()))
                        .collect::<Vec<_>>()
                });
            let changed = current.as_deref() != Some(described.as_slice());
            if let Some(info) = state.statements.get_mut(&stmt_name) {
                info.described = Some((now, current.clone().unwrap_or(described)));
            }
            if changed {
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
                // Only on the execute that first notices the change, so a
                // client that never runs DDL never sees a second description.
                schema_changed = true;
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

    execute_single_statement(
        database,
        state,
        stream,
        &query,
        &result_formats,
        !schema_changed,
    )
    .await
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
fn substitute_placeholders(sql: &str, stand_in: &str) -> String {
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut quote: Option<u8> = None;
    let mut i = 0usize;
    while i < bytes.len() {
        let c = bytes[i];
        match quote {
            Some(q) => {
                out.push(c as char);
                if c == q {
                    quote = None;
                }
                i += 1;
            }
            None => {
                if c == b'\'' || c == b'"' {
                    quote = Some(c);
                    out.push(c as char);
                    i += 1;
                } else if c == b'$' && bytes.get(i + 1).is_some_and(|d| d.is_ascii_digit()) {
                    let mut j = i + 1;
                    while j < bytes.len() && bytes[j].is_ascii_digit() {
                        j += 1;
                    }
                    out.push_str(stand_in);
                    i = j;
                } else {
                    out.push(c as char);
                    i += 1;
                }
            }
        }
    }
    out
}

/// Whether the statement still carries an unbound placeholder.
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

async fn describe_query(
    database: &Arc<Database>,
    query: &str,
) -> io::Result<Option<(Vec<(&'static str, DbType)>, bool)>> {
    let stmt = query.trim();
    if stmt.is_empty() {
        return Ok(None);
    }

    // A `WITH` prefix is stripped and the statement it introduces is described
    // instead. The CTE bodies decide what the *rows* are, never what the
    // columns are, so describing the outer query answers the question — and it
    // does so without building the CTE, which is the part that must not happen
    // here. If the outer query reads the CTE as a relation the description
    // falls through to `None` below, since that name genuinely does not exist
    // yet.
    if let Ok(Some((_, outer))) = crate::core::rql::cte::split_with(stmt) {
        if let Some(fields) = Box::pin(describe_resolved(database, &outer, None)).await? {
            if fields.1 {
                return Ok(Some(fields));
            }
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
            if let Some((columns, _)) = Box::pin(describe_resolved(database, &inner, None)).await? {
                let mut fields: smallvec::SmallVec<[crate::core::row::schema::SchemaField; 20]> =
                    smallvec::SmallVec::new();
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
                let rewritten = format!("{}{} {}{}", &stmt[..start], alias, alias, &stmt[end..]);
                if let Some(described) = Box::pin(describe_resolved(
                    database,
                    &rewritten,
                    Some((&alias, &fields)),
                ))
                .await?
                {
                    return Ok(Some(described));
                }
            }
        }
    }

    return describe_resolved(database, stmt, None).await;
}

/// Describes `SELECT <aggregates> FROM ( … ) alias` without running the inner
/// query.
///
/// Only aggregates whose result type is fixed are answered — `COUNT` is one,
/// and it is the case that matters, since a paginated count is written this way
/// by every ORM. Anything whose type depends on the inner relation returns
/// `None` and is described the ordinary way.
fn describe_aggregate_over_derived(stmt: &str) -> Option<Vec<(&'static str, DbType)>> {
    let upper = stmt.trim_start().to_ascii_uppercase();
    if !upper.starts_with("SELECT") {
        return None;
    }
    let from_at = crate::core::rql::join::find_keyword(stmt, "FROM", 0)?;
    if !stmt[from_at + "FROM".len()..].trim_start().starts_with('(') {
        return None;
    }

    let select_list = stmt["SELECT".len()..from_at].trim();
    let mut fields: Vec<(&'static str, DbType)> = Vec::new();
    for item in crate::core::rql::join::split_top_level_commas(select_list) {
        let item = item.trim();
        let (expr, alias) = crate::core::rql::lexer_query::split_alias(item);
        let open = expr.find('(')?;
        if !expr[..open].trim().eq_ignore_ascii_case("COUNT") {
            return None;
        }
        let label = alias.unwrap_or_else(|| "count".to_string());
        let leaked: &'static str = Box::leak(label.into_boxed_str());
        fields.push((leaked, DbType::I64));
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
    // A relation that exists only for this statement — a derived table whose
    // columns were resolved from its inner query. It is not in
    // `database.tables` and must not be put there: describing must not
    // materialise anything.
    derived: Option<(
        &str,
        &smallvec::SmallVec<[crate::core::row::schema::SchemaField; 20]>,
    )>,
) -> io::Result<Option<(Vec<(&'static str, DbType)>, bool)>> {
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
        return Ok(Some((vec![("import", DbType::STRING)], true)));
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
                return Ok(Some((vec![("?column?", DbType::I32)], true)));
            }
            if lower == "select version()" {
                return Ok(Some((vec![("version", DbType::STRING)], true)));
            }
            if let Some(key) = lower.strip_prefix("show ") {
                let leaked: &'static str = Box::leak(key.trim().to_string().into_boxed_str());
                return Ok(Some((vec![(leaked, DbType::STRING)], true)));
            }
        }
    }

    if let Some(Ok(DatabaseStatement::List)) = parse_database_statement(stmt) {
        return Ok(Some((
            vec![
                ("database_name", DbType::STRING),
                ("location", DbType::STRING),
                ("tables", DbType::U64),
                ("sessions", DbType::U64),
                ("is_default", DbType::BOOL),
                ("is_current", DbType::BOOL),
            ],
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
                        let fields: Vec<(&'static str, DbType)> = plan
                            .items
                            .iter()
                            .map(|item| {
                                let leaked: &'static str =
                                    Box::leak(item.label().to_string().into_boxed_str());
                                (leaked, item.result_type())
                            })
                            .collect();
                        if !fields.is_empty() {
                            return Ok(Some((fields, true)));
                        }
                    }

                    let fields: Vec<(&'static str, DbType)> = parsed
                        .requested_row_fetch
                        .columns_fetching_data
                        .iter()
                        .take(parsed.visible_columns)
                        .filter_map(|c| {
                            field_of(&schema, c.schema_id).map(|f| {
                                let leaked: &'static str =
                                    Box::leak(f.name.clone().into_boxed_str());
                                (leaked, f.db_type.clone())
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
                                let leaked: &'static str = Box::leak(name.into_boxed_str());
                                (leaked, ty)
                            })
                            .collect(),
                        true,
                    )
                }),
            );
        }
        QueryPurpose::Replication(sql) => {
            return Ok(Some((
                crate::replication::sql::structured_fields(sql)
                    .unwrap_or_else(crate::replication::sql::result_fields)
                    .into_iter()
                    .map(|(name, ty)| {
                        let leaked: &'static str = Box::leak(name.into_boxed_str());
                        (leaked, ty)
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
                        let leaked: &'static str = Box::leak(name.into_boxed_str());
                        (leaked, ty)
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
                                let leaked: &'static str = Box::leak(name.into_boxed_str());
                                (leaked, ty)
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
                    if let Some((alias, fields)) = derived {
                        if name.eq_ignore_ascii_case(alias) {
                            return Some((*fields).clone());
                        }
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
                                    let leaked: &'static str = Box::leak(name.into_boxed_str());
                                    (leaked, ty)
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
                    .filter(|(alias, _)| qr.table_name.eq_ignore_ascii_case(alias))
                    .map(|(_, fields)| (*fields).clone())
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
                    let leaked: &'static str = Box::leak(item.label().to_string().into_boxed_str());
                    fields.push((leaked, advertised(item)));
                }
                return Ok(Some((fields, true)));
            }

            // An aggregate result has no schema fields behind it: its columns
            // are the select items, named and typed by the plan.
            if let Some(plan) = &parsed.aggregate {
                for item in &plan.items {
                    let leaked: &'static str = Box::leak(item.label().to_string().into_boxed_str());
                    fields.push((leaked, advertised(item)));
                }
                return Ok(Some((fields, true)));
            }

            for c in parsed
                .requested_row_fetch
                .columns_fetching_data
                .iter()
                .take(parsed.visible_columns)
            {
                let Some(field) = field_of(&table_fields, c.schema_id) else {
                    continue;
                };
                let leaked: &'static str = Box::leak(field.name.clone().into_boxed_str());
                // A catalogue view has no `TableSchema` and no JSON columns, so
                // the field's own type is what it reports.
                let ty = match &real_table {
                    Some(t) => reported_type(field, &t.schema),
                    None => field.db_type.clone(),
                };
                fields.push((leaked, ty));
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
                            let leaked: &'static str = Box::leak(f.name.clone().into_boxed_str());
                            (leaked, reported_type(f, &table.schema))
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
                            let leaked: &'static str = Box::leak(f.name.clone().into_boxed_str());
                            (leaked, reported_type(f, &table.schema))
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
    let mut columns: smallvec::SmallVec<[crate::core::row::row::Column; 32]> =
        smallvec::SmallVec::new();
    for (index, cell) in cells.iter().enumerate() {
        let (bytes, column_type): (Vec<u8>, DbType) = match cell {
            CatalogCell::Text(text) => (text.as_bytes().to_vec(), DbType::STRING),
            CatalogCell::Number(n) => (n.to_le_bytes().to_vec(), DbType::U64),
            CatalogCell::Flag(b) => (vec![u8::from(*b)], DbType::BOOL),
        };
        let mut block = crate::memory_pool::MEMORY_POOL.acquire(bytes.len());
        block.into_slice_mut().copy_from_slice(&bytes);
        columns.push(crate::core::row::row::Column {
            schema_id: index as u64,
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
                // Migration tools branch on the code: `42P04` means "already
                // there, carry on", while a name this server will not accept is
                // a different problem and must not be read as that.
                Err(e) => {
                    let sqlstate = if state.catalog.get(&name).is_some() {
                        "42P04"
                    } else {
                        "42602"
                    };
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
                ("tables".to_string(), DbType::U64),
                ("sessions".to_string(), DbType::U64),
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

    // A data-modifying CTE and the statement it feeds are one statement, so
    // they need one transaction. Outside an explicit block there is none yet —
    // `execute` would open its own *after* the CTE bodies had already run and
    // committed — so one is opened here to span both, and closed on the outcome.
    let cte_tx = match state.current_tx() {
        Some(tx) => Some((tx, false)),
        None if crate::core::rql::cte::has_with_clause(stmt) => {
            Some((database.transactions.begin_autocommit(), true))
        }
        None => None,
    };
    let effective_tx = cte_tx
        .map(|(tx, _)| tx)
        .unwrap_or(crate::core::mvcc::transaction::FROZEN_TX);

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

    // The outer statement runs in the same transaction as its CTE bodies.
    let restore_tx = state.statement_tx;
    if let Some((tx, owned)) = cte_tx {
        if owned {
            state.statement_tx = Some(tx);
        }
    }
    state.statement_failed = false;

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
        match state.statement_error.take() {
            Some(reason) => log::error!("Statement failed ({}): {}", reason, effective),
            None => log::error!("Statement failed: {}", effective),
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
                let family = database.commit_transaction(tx);
                if let Err(e) = database.transactions.flush_deferred() {
                    log::error!("Could not record the transaction outcome: {}", e);
                    database.abort_transaction(tx);
                    return Err(io::Error::other(format!(
                        "could not commit: the transaction outcome could not be made durable ({})",
                        e
                    )));
                }
                if state.synchronous_commit() {
                    if let Err(e) = database.sync_durable() {
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
    let flattened;
    let stmt = if crate::core::rql::subquery::find_in_subquery(stmt).is_some()
        || crate::core::rql::subquery::find_exists(stmt).is_some()
        || crate::core::rql::subquery::find_scalar_subquery(stmt).is_some()
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
                flattened = s;
                flattened.as_str()
            }
            Err(e) => {
                fail_statement(state, stream, "42601", &e).await?;
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
        QueryPurpose::QueryRows(qr) => database.tables.get(&qr.table_name).and_then(|t| {
            let schema = &t.schema;
            if schema.type_modifiers.is_empty() {
                return None;
            }
            crate::core::rql::lexer_query::row_fetch_from_select_query(&purpose, &schema.fields)
                .ok()
                .map(|parsed| {
                    parsed
                        .requested_row_fetch
                        .columns_fetching_data
                        .iter()
                        .take(parsed.visible_columns)
                        .map(|c| {
                            field_of(&schema.fields, c.schema_id).map_or(0, |field| {
                                crate::core::rql::type_modifier::scale_of(
                                    &field.name,
                                    &schema.type_modifiers,
                                ) as u32
                            })
                        })
                        .collect::<Vec<_>>()
                })
        }),
        _ => None,
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
            match row_fetch_from_select_query(&purpose, &table.schema.fields) {
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

    // `execute` consumes QueryPurpose, but we still want `purpose_kind`/`select_fields`.
    // Re-recognize the query to get an owned QueryPurpose to pass into the engine.
    let purpose_exec = recognize_query_purpose(stmt)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "query purpose disappeared"))?;

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
            let rows = vec_into_rows(rows_bytes)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
            // `INSERT ... RETURNING` sends rows but must still complete with an
            // INSERT tag; a client that saw "SELECT n" would not count the write.
            let tag = match purpose_kind {
                PurposeKind::InsertRow => format!("INSERT 0 {}", rows.len()),
                PurposeKind::UpdateRow => format!("UPDATE {}", rows.len()),
                PurposeKind::DeleteRow => format!("DELETE {}", rows.len()),
                _ => format!("SELECT {}", rows.len()),
            };
            send_query_result(
                stream,
                select_fields.as_deref(),
                select_scales.as_deref(),
                &rows,
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
fn sqlstate_for(message: &str) -> &'static str {
    let m = message.to_ascii_lowercase();
    let has = |needle: &str| m.contains(needle);

    if has("could not serialize access") {
        "40001"
    } else if has("duplicate value for unique") || has("duplicate key value") {
        "23505"
    } else if has("violates foreign key constraint")
        || (has("foreign key") && has("does not exist"))
    {
        "23503"
    } else if has("violates not-null constraint") || has("not null") && has("null value") {
        "23502"
    } else if has("violates check constraint") {
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
    } else if has("out of range") {
        "22003"
    } else if has("division by zero") {
        "22012"
    } else if has("invalid input syntax") || has("could not parse") || has("cannot parse") {
        "22P02"
    } else if has("not supported") || has("unsupported") {
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
    let statements = split_statements(query);

    for stmt in statements {
        execute_single_statement(database, state, stream, &stmt, &[], false).await?;
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
        level,
        read_only,
        savepoints: Vec::new(),
        failed: false,
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
                    database.publish_transaction(family).await;
                    if let Some((level, read_only)) = chain_into {
                        open_block(database, state, level, read_only);
                    }
                    send_command_complete(stream, "COMMIT").await?;
                    return Ok(true);
                }

                let family = database.commit_transaction(t.id);

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
                    if let Err(e) = database.sync_durable() {
                        log::error!("Could not make the block's rows durable: {}", e);
                        database.abort_transaction(t.id);
                        flush_outcomes(database);
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
        "statement_timeout" | "lock_timeout" | "idle_in_transaction_session_timeout" => {
            "0".to_string()
        }
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
    let col_meta: Vec<(String, DbType)> = if let Some(f) = fields {
        f.to_vec()
    } else if let Some(first) = rows.first() {
        first
            .columns
            .iter()
            .enumerate()
            .map(|(i, c)| (format!("column{}", i + 1), c.column_type.clone()))
            .collect()
    } else {
        Vec::new()
    };

    let fields_ref: Vec<(&str, DbType)> = col_meta
        .iter()
        .map(|(n, t)| (n.as_str(), t.clone()))
        .collect();

    if !omit_description {
        send_row_description(stream, &fields_ref, result_formats).await?;
    }

    for row in rows {
        let mut values = Vec::with_capacity(row.columns.len());
        for (i, col) in row.columns.iter().enumerate() {
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
            let db_type = if col.column_type == DbType::NULL {
                DbType::NULL
            } else {
                col_meta
                    .get(i)
                    .map(|(_, t)| t.clone())
                    .unwrap_or_else(|| col.column_type.clone())
            };

            let scale = scales.and_then(|s| s.get(i)).copied();
            let val = if format_code == 1 {
                format_column_binary(db_type, col.data.into_slice(), scale)
            } else {
                format_column_text_scaled(db_type, col.data.into_slice(), scale)
                    .map(|s| s.into_bytes())
            };
            values.push(val);
        }
        let refs: Vec<Option<&[u8]>> = values.iter().map(|v| v.as_deref()).collect();
        send_data_row(stream, &refs).await?;
    }

    send_command_complete(stream, command_tag).await?;
    Ok(())
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
            let document = crate::core::jsonb::decode(bytes).ok()?;
            let text = document.to_string();
            let mut out = Vec::with_capacity(text.len() + 1);
            out.push(1u8);
            out.extend_from_slice(text.as_bytes());
            Some(out)
        }
        DbType::I8 | DbType::U8 => {
            if bytes.len() == 1 {
                let val = bytes[0] as i16;
                Some(val.to_be_bytes().to_vec())
            } else {
                None
            }
        }
        DbType::I16 | DbType::U16 => {
            if bytes.len() == 2 {
                let mut arr = [0u8; 2];
                arr.copy_from_slice(bytes);
                let val = u16::from_le_bytes(arr);
                Some(val.to_be_bytes().to_vec())
            } else {
                None
            }
        }
        DbType::I32 | DbType::U32 => {
            if bytes.len() == 4 {
                let mut arr = [0u8; 4];
                arr.copy_from_slice(bytes);
                let val = u32::from_le_bytes(arr);
                Some(val.to_be_bytes().to_vec())
            } else {
                None
            }
        }
        DbType::I64 | DbType::U64 => {
            if bytes.len() == 8 {
                let mut arr = [0u8; 8];
                arr.copy_from_slice(bytes);
                let val = u64::from_le_bytes(arr);
                Some(val.to_be_bytes().to_vec())
            } else {
                None
            }
        }
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
                Some(
                    (us - crate::core::value_codec::PG_EPOCH_OFFSET_US)
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

/// Renders a value, honouring a column's declared decimal scale.
fn format_column_text_scaled(db_type: DbType, bytes: &[u8], scale: Option<u32>) -> Option<String> {
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
            // Treat as UTF-8 text.
            String::from_utf8_lossy(bytes).to_string()
        }

        // A stored document, rendered back to the JSON text a client expects.
        // An empty slot is a NULL that reached here as bytes rather than
        // through the companion column, and prints as nothing.
        DbType::JSONB => {
            if bytes.is_empty() {
                String::new()
            } else {
                match crate::core::jsonb::decode(bytes) {
                    Ok(document) => document.to_string(),
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
            format!("{}+00", crate::core::value_codec::format_timestamp_us(us))
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

        DbType::DECIMAL => {
            let Some(v) = le_i128(bytes) else { return None };
            crate::core::value_codec::format_decimal(v)
        }

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
                String::from_utf8_lossy(bytes).to_string()
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
            if let Some(s) = ascii_decimal(bytes) {
                s.to_string()
            } else if let Some(v) = le_i128(bytes) {
                v.to_string()
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
            if let Some(s) = ascii_decimal(bytes) {
                s.to_string()
            } else if let Some(v) = le_u128(bytes) {
                v.to_string()
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
            for byte in salt.iter_mut() {
                *byte = fastrand::u8(..);
            }
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

async fn send_backend_key_data<S>(stream: &mut S) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    // We don't implement CancelRequest, so this is just filler.
    let pid = std::process::id() as i32;
    let key = fastrand::i32(..);
    let mut payload = Vec::with_capacity(8);
    payload.extend_from_slice(&pid.to_be_bytes());
    payload.extend_from_slice(&key.to_be_bytes());
    send_message(stream, b'K', &payload).await
}

async fn send_ready_for_query<S>(stream: &mut S, status: u8) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    send_message(stream, b'Z', &[status]).await
}

async fn send_command_complete<S>(stream: &mut S, tag: &str) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let mut payload = Vec::with_capacity(tag.len() + 1);
    payload.extend_from_slice(tag.as_bytes());
    payload.push(0);
    send_message(stream, b'C', &payload).await
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

    send_message(stream, b'E', &payload).await
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

async fn send_row_description<S>(
    stream: &mut S,
    fields: &[(&str, DbType)],
    result_formats: &[i16],
) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
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
        payload.extend_from_slice(name.as_bytes());
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
    let mut payload = Vec::new();
    let count: i16 = values.len().try_into().unwrap_or(i16::MAX);
    payload.extend_from_slice(&count.to_be_bytes());

    for v in values {
        match v {
            None => payload.extend_from_slice(&(-1i32).to_be_bytes()),
            Some(bytes) => {
                let len: i32 = bytes.len().try_into().unwrap_or(i32::MAX);
                payload.extend_from_slice(&len.to_be_bytes());
                payload.extend_from_slice(bytes);
            }
        }
    }

    send_message(stream, b'D', &payload).await
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

    stream.write_all(&[tag]).await?;
    stream.write_all(&len.to_be_bytes()).await?;
    stream.write_all(payload).await?;
    // For TLS streams (tokio-rustls), writes may be buffered until flush.
    stream.flush().await?;
    Ok(())
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

async fn read_startup_packet<S>(stream: &mut S) -> io::Result<(i32, Vec<u8>)>
where
    S: AsyncRead + Unpin,
{
    let len = read_i32_be(stream).await?;
    if len < 8 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid startup packet",
        ));
    }
    let mut buf = vec![0u8; (len - 4) as usize];
    stream.read_exact(&mut buf).await?;
    Ok((len, buf))
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

    send_message(stream, b'E', &payload).await
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
            ("relation 'nope' does not exist", "42P01"),
            ("Table not found", "42P01"),
            ("column 'nope' does not exist", "42703"),
            ("Table already exists", "42P07"),
            ("column 'x' already exists", "42701"),
            ("constraint 'u' already exists on 'users'", "42710"),
            ("value 300 is out of range for type U8", "22003"),
            ("permission denied: CREATE ROLE requires superuser", "42501"),
            ("NOT LIKE is not supported yet", "0A000"),
            (
                "unexpected Ident(\"x\") after the end of the expression",
                "42601",
            ),
            ("something nobody anticipated", "XX000"),
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
        let names: Vec<&str> = fields.iter().map(|(name, _)| *name).collect();
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
        assert_eq!(fields.first().map(|(name, _)| *name), Some("snapshot_id"));
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
