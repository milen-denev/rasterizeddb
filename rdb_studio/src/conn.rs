//! Talking to the server.
//!
//! # Two transports, one answer
//!
//! A RasterizedDB profile speaks the server's native protocol by default,
//! through `rasterizeddb_client`; a PostgreSQL profile, or a RasterizedDB one
//! set to it, speaks the PostgreSQL wire protocol through `tokio-postgres`.
//! Everything above this module sees the same [`Grid`]s either way, because
//! the native session asks for the simple-query form
//! (`Client::simple_query`): the server runs the text through its PostgreSQL
//! path and answers with the messages pgwire would have sent, column names,
//! text cells and SQLSTATEs included. The engine's own native answer carries
//! no column names, and its type tags call a `jsonb` document text, so a
//! console cannot be built on it.
//!
//! The query editor has a native connection of its own
//! ([`Session::run_editor`]), so a `BEGIN` stays open from one run to the
//! next as it does over the wire protocol; the panels keep to the shared one
//! and so never see into the editor's open block. A run carries a cancel
//! token and the editor's row limit; see [`Canceller`].
//!
//! What the native protocol does not do, and the console therefore does not
//! offer over it: a second database (a native listener serves one, so
//! switching database is refused rather than reconnecting to the same one
//! under another name) and prepared statements in the benchmark. A statement
//! is also cut off by the server's `--rdb-request-timeout`, 30 s by default.
//!
//! # Why the simple query protocol
//!
//! Every value comes back as text. That is exactly what a console wants: it
//! renders `2026-08-21 10:14:00+00` and `{"a":1}` and `\x0fae` without knowing
//! anything about timestamps, JSON or bytea, and a type the driver has never
//! heard of displays as whatever the server said rather than as a decode error.
//!
//! The extended protocol would hand back binary and require a `FromSql` for
//! every type either engine can produce — including RasterizedDB's own, which
//! `postgres-types` has no impl for. The cost of the simple protocol is that
//! there are no bound parameters, so every literal this console sends is
//! escaped by [`crate::dialect`] instead. That is a real trade and the escaping
//! is tested.
//!
//! # Why a server certificate is not verified by default
//!
//! RasterizedDB generates a self-signed certificate when none is configured, so
//! a verifying client refuses the server the user just started on their own
//! machine. `TlsMode::Require` encrypts and accepts that certificate;
//! `TlsMode::Prefer` does the same and falls back to cleartext. Neither
//! authenticates the server, and the connection screen says so — an encrypted
//! link to an unauthenticated peer is worth having against a passive observer
//! and worth nothing against an active one.
//!
//! A profile that ticks "verify the server's certificate" believes only a
//! certificate signed by its CA file (or a public authority, with none) that
//! names the host, on either transport, and never falls back to cleartext.
//! That needs a server given a real certificate: `--pg-cert`/`--pg-key` for
//! the wire protocol, `--rdb-cert`/`--rdb-key` for the native one.

use std::sync::Arc;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, SignatureScheme};
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

use crate::model::{Connection, Flavor, Grid, NativeLogin, Outcome, TlsMode, Transport};

/// The client underneath a session.
#[derive(Clone)]
enum Backend {
    Wire(Arc<WireLink>),
    Native(Arc<rasterizeddb_client::Client>),
}

/// A wire session's connection, reopened when the server has closed it.
///
/// A `tokio_postgres::Client` whose connection ended answers every call with
/// "connection closed", so one restart of the server (or an idle connection
/// cut by a firewall) left every panel failing until the profile was opened
/// again. A call that finds the connection closed opens a new one first. The
/// statement had not been sent on the old one, so nothing runs twice; one
/// that was in flight when the connection died fails as it did, and the next
/// call reopens.
struct WireLink {
    client: std::sync::RwLock<Arc<Client>>,
    /// Held while a new connection is opened, so callers that all found the
    /// connection closed open one between them, not one each.
    reopening: tokio::sync::Mutex<()>,
    profile: Connection,
    /// The session's notices, which a reopened connection's go to as well.
    notices: Arc<std::sync::Mutex<Vec<String>>>,
}

/// A live connection, and what the console learned about it while opening one.
#[derive(Clone)]
pub struct Session {
    backend: Backend,
    /// Which protocol it speaks.
    pub transport: Transport,
    pub flavor: Flavor,
    pub database: String,
    pub server: String,
    /// Whether the link is encrypted, which the status bar reports.
    pub encrypted: bool,
    /// Whether the server's certificate was verified, not just accepted.
    pub verified: bool,
    /// What the wire connection's server said as notices, as the driver's
    /// connection task heard them; drained by each run of the editor. Unused
    /// over native, whose notices come back with their answer.
    notices: Arc<std::sync::Mutex<Vec<String>>>,
    /// The editor's own native connection ([`Session::run_editor`]), opened by
    /// its first run.
    editor: Arc<tokio::sync::OnceCell<rasterizeddb_client::SimpleSession>>,
}

/// The notices kept for the editor, at most: a server raising one per row of
/// a loop must not grow the console without bound.
const NOTICES_KEPT: usize = 500;

impl std::fmt::Debug for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Session")
            .field("transport", &self.transport)
            .field("flavor", &self.flavor)
            .field("database", &self.database)
            .field("encrypted", &self.encrypted)
            .finish_non_exhaustive()
    }
}

impl PartialEq for Session {
    /// Two handles onto the same connection are the same session. iced compares
    /// state to decide what to redraw, and a client has no equality of its own.
    fn eq(&self, other: &Self) -> bool {
        match (&self.backend, &other.backend) {
            (Backend::Wire(a), Backend::Wire(b)) => Arc::ptr_eq(a, b),
            (Backend::Native(a), Backend::Native(b)) => Arc::ptr_eq(a, b),
            _ => false,
        }
    }
}

/// Whether `host` names a directory holding a unix socket rather than a
/// machine: a path, as libpq and tokio-postgres read it. The server's socket
/// is `<host>/.s.PGSQL.<port>` (`--pg-socket-dir`).
pub fn is_socket_dir(host: &str) -> bool {
    host.starts_with('/')
}

/// What a profile connects to, as the status bar and the error messages
/// name it: `host:port`, or the socket file.
pub fn endpoint(host: &str, port: u16) -> String {
    if is_socket_dir(host) {
        format!("{}/.s.PGSQL.{}", host.trim_end_matches('/'), port)
    } else {
        format!("{}:{}", host, port)
    }
}

/// Opens a connection, honouring the TLS mode.
///
/// `Prefer` genuinely retries: it tries TLS and, if the handshake fails,
/// connects again in cleartext. Reporting the TLS error would be wrong — a
/// server started with `--pg-tls disable` refuses the handshake, and that is
/// the configuration `prefer` exists for.
///
/// A host that is a directory reaches the server's unix socket, which carries
/// no TLS whatever the profile says: the server answers a TLS request there
/// with "no", as PostgreSQL does, so `require` would refuse every connection.
/// Only root can watch a local socket, so nothing is given up.
pub async fn connect(profile: Connection) -> Result<Session, String> {
    match profile.effective_transport() {
        Transport::Native => connect_native(profile).await,
        Transport::Wire => connect_wire(profile).await,
    }
}

/// Where a native profile connects: a host that is a path is the socket file
/// itself (`--rdb-socket PATH`), not a directory as for the wire protocol.
pub fn native_address(host: &str, port: u16) -> String {
    if is_socket_dir(host) {
        format!("unix:{}", host)
    } else {
        format!("{}:{}", host, port)
    }
}

/// How long opening a native connection may take, the login included.
const NATIVE_CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15);

/// The largest answer a native session accepts. The native client holds a
/// whole answer in memory and refuses one past its limit rather than cutting
/// it short; 8 MiB, its default, is a few thousand rows of text, which a
/// console's `SELECT *` passes easily.
const NATIVE_MAX_ANSWER: usize = 512 * 1024 * 1024;

/// Opens a native-protocol session.
///
/// The connection is checked by asking which database it serves, which does
/// two things a successful login does not: it proves the server answers the
/// simple-query form every panel depends on (an older one refuses it, and
/// that is said here rather than as an error in every panel), and it names
/// the database from the server's side. A native listener serves one
/// database, whatever the profile says, so the profile's name is not trusted
/// for it.
async fn connect_native(profile: Connection) -> Result<Session, String> {
    use rasterizeddb_client::{Login, Options};

    let address = native_address(&profile.host, profile.port);
    let socket = is_socket_dir(&profile.host);
    let login = match profile.login {
        NativeLogin::Password => {
            if profile.password.is_empty() {
                return Err(
                    "a password login needs a password; choose the trust or peer login for a listener \
                     that asks for none"
                        .to_string(),
                );
            }
            Login::password(profile.user.clone(), profile.password.clone())
        }
        NativeLogin::Peer if !socket => {
            return Err(
                "a peer login works only on a unix socket: set Host to the socket file (--rdb-socket)"
                    .to_string(),
            );
        }
        NativeLogin::Peer if profile.user.trim().is_empty() => Login::Peer,
        NativeLogin::Peer => Login::PeerAs(profile.user.clone()),
        NativeLogin::Trust => Login::Trust,
    };
    // A unix socket carries no TLS, so there is nothing to verify there.
    let verified = profile.verify_server && !socket;
    let trust = if !verified {
        rasterizeddb_client::Trust::AcceptAny
    } else if profile.ca_file.trim().is_empty() {
        rasterizeddb_client::Trust::WebPki
    } else {
        rasterizeddb_client::load_ca(profile.ca_file.trim())
            .map_err(|e| format!("the CA file: {}", native_error(&e)))?
    };
    let options = Options {
        connections: 1,
        // Statements are bounded by the server's request timeout; a
        // client-side one would only abandon an answer the server is still
        // working on.
        timeout: None,
        login,
        max_message_size: NATIVE_MAX_ANSWER,
        trust,
        ..Options::default()
    };

    let client = match tokio::time::timeout(
        NATIVE_CONNECT_TIMEOUT,
        rasterizeddb_client::Client::connect(&address, options),
    )
    .await
    {
        Ok(Ok(client)) => client,
        Ok(Err(e)) => return Err(describe_native(&e.to_string(), &profile, &address)),
        Err(_) => {
            return Err(format!(
                "{} did not finish the handshake in {} s{}",
                address,
                NATIVE_CONNECT_TIMEOUT.as_secs(),
                if profile.login == NativeLogin::Password {
                    " — a listener under trust authentication never answers a password login; \
                     choose the trust login for it"
                } else {
                    ""
                }
            ));
        }
    };

    // `SHOW DATABASES`, the sidebar's own statement: its current row is the
    // database this listener serves. (`current_database()` answers too since
    // the server read session values over native, but a server from before
    // that does not, and every server answers this.)
    let database = match client
        .simple_query(crate::dialect::databases_sql(Flavor::Rasterized))
        .await
    {
        Ok(answer) => {
            let listed = outcomes(answer)
                .into_iter()
                .find_map(|outcome| match outcome {
                    Outcome::Rows(grid) => Some(crate::dialect::parse_databases(&grid)),
                    Outcome::Affected { .. } => None,
                })
                .unwrap_or_default();
            match listed.iter().find(|d| d.is_current) {
                Some(current) => current.name.clone(),
                None if listed.len() == 1 => listed[0].name.clone(),
                None => {
                    return Err(format!(
                        "{} did not say which database it serves (SHOW DATABASES marked none current)",
                        address
                    ));
                }
            }
        }
        Err(rasterizeddb_client::Error::Unsupported(_)) => {
            return Err(format!(
                "the server at {} is too old for the console over the native protocol: it does not \
                 answer the simple-query form. Set Protocol to PostgreSQL wire (and the port to its \
                 --pg-port), or upgrade the server",
                address
            ));
        }
        Err(e) => return Err(describe_native(&native_error(&e), &profile, &address)),
    };

    Ok(Session {
        backend: Backend::Native(Arc::new(client)),
        transport: Transport::Native,
        flavor: Flavor::Rasterized,
        database,
        server: if socket {
            format!("unix:{}", profile.host)
        } else {
            address
        },
        // Native TCP is always TLS (the certificate verified only when the
        // profile asks); a unix socket carries none.
        encrypted: !socket,
        verified,
        notices: Arc::default(),
        editor: Arc::default(),
    })
}

/// A native connection error, named for what fixes it.
fn describe_native(error: &str, profile: &Connection, address: &str) -> String {
    if error.contains("Connection refused") {
        return format!(
            "nothing is listening on {} — is the server running, with its native protocol on \
             --rdb-port {}?",
            address, profile.port
        );
    }
    if is_socket_dir(&profile.host) && error.contains("No such file or directory") {
        return format!(
            "there is no socket at {} — is the server running with --rdb-socket {}?",
            profile.host, profile.host
        );
    }
    if profile.verify_server
        && (error.contains("UnknownIssuer")
            || error.contains("invalid peer certificate")
            || error.contains("NotValidForName"))
    {
        return describe_tls(error);
    }
    error.to_string()
}

/// A native client error as the panels show one: the server's message alone,
/// as the wire transport's [`sql_error`] gives it, so a refusal reads the same
/// whichever protocol carried it.
pub(crate) fn native_error(error: &rasterizeddb_client::Error) -> String {
    match error {
        rasterizeddb_client::Error::Server(message) => match message.rfind(" (SQLSTATE ") {
            Some(at) if message.ends_with(')') => message[..at].to_string(),
            _ => message.clone(),
        },
        // The server gave up on it: its request timeout, which the native
        // protocol has and the wire protocol does not. Whether a write had
        // taken effect is unknown.
        e if e.is_server_timeout() => {
            "the server abandoned the statement at its native request timeout (30 s unless \
             --rdb-request-timeout says otherwise); whether a write took effect is unknown. Raise \
             the timeout, or run long statements over the PostgreSQL wire protocol"
                .to_string()
        }
        other => other.to_string(),
    }
}

async fn connect_wire(profile: Connection) -> Result<Session, String> {
    let notices = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (client, encrypted, verified) = open_wire(&profile, notices.clone()).await?;
    Ok(Session {
        transport: Transport::Wire,
        flavor: profile.flavor,
        database: profile.database.clone(),
        server: endpoint(&profile.host, profile.port),
        encrypted,
        verified,
        backend: Backend::Wire(Arc::new(WireLink {
            client: std::sync::RwLock::new(Arc::new(client)),
            reopening: tokio::sync::Mutex::new(()),
            profile,
            notices: notices.clone(),
        })),
        notices,
        editor: Arc::default(),
    })
}

/// One wire connection for `profile`: the client, whether the link is
/// encrypted, and whether the server's certificate was verified.
async fn open_wire(
    profile: &Connection,
    notices: Arc<std::sync::Mutex<Vec<String>>>,
) -> Result<(Client, bool, bool), String> {
    let mut config = tokio_postgres::Config::new();
    config
        .host(&profile.host)
        .port(profile.port)
        .dbname(&profile.database)
        .user(&profile.user)
        .application_name("rdb_studio");
    if !profile.password.is_empty() {
        config.password(&profile.password);
    }

    let socket = is_socket_dir(&profile.host);
    let verify = profile.verify_server && !socket;
    if verify && profile.tls == TlsMode::Disable {
        return Err(
            "verifying the server needs TLS: set TLS to `require`, or untick the verification"
                .to_string(),
        );
    }
    let attempt_tls = !socket && matches!(profile.tls, TlsMode::Prefer | TlsMode::Require);
    if attempt_tls {
        let connector = if verify {
            verifying_connector(profile.ca_file.trim())?
        } else {
            tls_connector()
        };
        match connect_tls(&config, connector, notices.clone()).await {
            Ok(client) => return Ok((client, true, verify)),
            // A verifying profile never falls back to cleartext: the point
            // of it is that nobody in between can read or answer for the
            // server.
            Err(e) if profile.tls == TlsMode::Require || verify => {
                return Err(describe_tls(&e));
            }
            Err(_) => { /* prefer: fall through to cleartext */ }
        }
    }

    let (client, connection) = config
        .connect(NoTls)
        .await
        .map_err(|e| describe(&e.to_string(), profile))?;
    drive(connection, notices);
    Ok((client, false, false))
}

/// What the editor is told when its connection had to be opened again: the
/// server ends a closed connection's transaction, so a block the editor had
/// open is gone.
pub const WIRE_REOPENED_NOTICE: &str = "the connection to the server was lost and has been opened \
     again; a transaction that was open on it was rolled back by the server";

async fn connect_tls(
    config: &tokio_postgres::Config,
    connector: tokio_postgres_rustls::MakeRustlsConnect,
    notices: Arc<std::sync::Mutex<Vec<String>>>,
) -> Result<Client, String> {
    let (client, connection) = config.connect(connector).await.map_err(|e| with_causes(&e))?;
    drive(connection, notices);
    Ok(client)
}

/// An error with what caused it: tokio-postgres says only "error performing
/// TLS handshake", and why the certificate was refused is in its source.
fn with_causes(error: &dyn std::error::Error) -> String {
    let mut text = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        let cause_text = cause.to_string();
        if !text.contains(&cause_text) {
            text.push_str(": ");
            text.push_str(&cause_text);
        }
        source = cause.source();
    }
    text
}

/// TLS that believes only a certificate signed by the CA in `ca_file` (or,
/// with none, by a public authority) and naming the host connected to.
fn verifying_connector(ca_file: &str) -> Result<tokio_postgres_rustls::MakeRustlsConnect, String> {
    let mut roots = rustls::RootCertStore::empty();
    if ca_file.is_empty() {
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    } else {
        use rustls::pki_types::pem::PemObject;
        let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(ca_file)
            .map_err(|e| format!("the CA file {}: {}", ca_file, e))?
            .collect::<Result<_, _>>()
            .map_err(|e| format!("the CA file {}: {}", ca_file, e))?;
        if certs.is_empty() {
            return Err(format!("the CA file {} holds no certificate", ca_file));
        }
        for cert in certs {
            roots
                .add(cert)
                .map_err(|e| format!("the CA file {}: {}", ca_file, e))?;
        }
    }
    let config = tls_builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(tokio_postgres_rustls::MakeRustlsConnect::new(config))
}

/// rustls with `ring` named, rather than the process default: the build
/// carries more than one provider, so there is no default to find unless
/// `main` installed one first, and the live tests use this module without it.
fn tls_builder() -> rustls::ConfigBuilder<rustls::ClientConfig, rustls::WantsVerifier> {
    rustls::ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_safe_default_protocol_versions()
        .expect("ring supports the default protocol versions")
}

/// A TLS failure, named for what fixes it.
fn describe_tls(error: &str) -> String {
    if error.contains("UnknownIssuer") || error.contains("invalid peer certificate") {
        return format!(
            "the server's certificate was not accepted ({}): it is not signed by the CA the profile \
             trusts, or does not name this host. Point the profile at the CA that signed it, or \
             untick the verification",
            error
        );
    }
    format!("TLS connection refused: {}", error)
}

/// The TLS a wire connection (and its cancel requests) are made with.
fn tls_connector() -> tokio_postgres_rustls::MakeRustlsConnect {
    let tls_config = tls_builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(AcceptAnyServer))
        .with_no_client_auth();
    tokio_postgres_rustls::MakeRustlsConnect::new(tls_config)
}

/// Runs the connection's own future, which is what actually moves bytes,
/// keeping the notices it hears for the editor.
///
/// Dropping it would leave a `Client` whose every call hangs, so it is spawned
/// and deliberately detached: its end is the connection closing, and the next
/// query is what reports that.
fn drive<S, T>(
    mut connection: tokio_postgres::Connection<S, T>,
    notices: Arc<std::sync::Mutex<Vec<String>>>,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        loop {
            match std::future::poll_fn(|cx| connection.poll_message(cx)).await {
                Some(Ok(tokio_postgres::AsyncMessage::Notice(notice))) => {
                    let mut kept = notices.lock().unwrap_or_else(|e| e.into_inner());
                    if kept.len() < NOTICES_KEPT {
                        kept.push(notice.message().to_string());
                    }
                }
                Some(Ok(_)) => {}
                Some(Err(e)) => {
                    log_line(&format!("connection closed: {}", e));
                    break;
                }
                None => break,
            }
        }
    });
}

/// Turns a driver error into something a person can act on.
///
/// The two failures a first connection actually hits are a server that refuses
/// cleartext and a database name that does not exist, and both report as
/// something opaque unless they are named.
fn describe(error: &str, profile: &Connection) -> String {
    if error.contains("server does not support SSL") || error.contains("no TLS") {
        return format!("{} — the server has TLS disabled; set TLS to `disable`", error);
    }
    let at = endpoint(&profile.host, profile.port);
    if error.contains("3D000") || error.contains("does not exist") {
        return format!("{} — no database named `{}` on {}", error, profile.database, at);
    }
    if error.contains("Connection refused") {
        return format!("nothing is listening on {} — is the server running?", at);
    }
    if is_socket_dir(&profile.host) && error.contains("No such file or directory") {
        return format!(
            "there is no socket at {} — is the server running with --pg-socket-dir {} \
             and --pg-port {}?",
            at, profile.host, profile.port
        );
    }
    error.to_string()
}

impl WireLink {
    /// The connection as it stands, open or not.
    fn current(&self) -> Arc<Client> {
        self.client.read().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// An open connection, and whether it had to be opened again for this
    /// call. The new one's notices go where the old one's did.
    async fn open(&self) -> Result<(Arc<Client>, bool), String> {
        let current = self.current();
        if !current.is_closed() {
            return Ok((current, false));
        }
        let _reopening = self.reopening.lock().await;
        // Another caller may have opened it while this one waited.
        let current = self.current();
        if !current.is_closed() {
            return Ok((current, false));
        }
        let (client, _, _) = open_wire(&self.profile, self.notices.clone()).await.map_err(|e| {
            format!("the connection to the server was lost, and opening it again failed: {e}")
        })?;
        let client = Arc::new(client);
        *self.client.write().unwrap_or_else(|e| e.into_inner()) = client.clone();
        log_line("connection reopened");
        Ok((client, true))
    }
}

impl Session {
    /// The driver underneath, for the benchmark, which has to send a statement
    /// without building a grid of its answer each time — that would be
    /// measuring this console's allocator rather than the server.
    ///
    /// `None` for a native session; see [`native`](Self::native).
    pub fn client(&self) -> Option<Arc<Client>> {
        match &self.backend {
            Backend::Wire(link) => Some(link.current()),
            Backend::Native(_) => None,
        }
    }

    /// What protects the link, as the status bar names it: `TLS`, `SOCKET`
    /// (a local unix socket, which carries none and needs none: only root can
    /// watch it), or `PLAIN`.
    pub fn link(&self) -> &'static str {
        if self.encrypted && self.verified {
            "TLS VERIFIED"
        } else if self.encrypted {
            "TLS"
        } else if self.server.starts_with("unix:") || self.server.starts_with('/') {
            "SOCKET"
        } else {
            "PLAIN"
        }
    }

    /// The native client underneath, for the same reason; `None` for a wire
    /// session.
    pub fn native(&self) -> Option<&rasterizeddb_client::Client> {
        match &self.backend {
            Backend::Native(client) => Some(client),
            Backend::Wire(_) => None,
        }
    }

    /// Runs one statement and returns its rows.
    ///
    /// Everything the console itself sends goes through here; a statement that
    /// returns no rows comes back as an empty grid carrying the server's tag.
    pub async fn query(&self, sql: &str) -> Result<Grid, String> {
        let outcomes = self.run(sql).await?;
        Ok(outcomes
            .into_iter()
            .find_map(|outcome| match outcome {
                Outcome::Rows(grid) => Some(grid),
                Outcome::Affected { .. } => None,
            })
            .unwrap_or_default())
    }

    /// Runs whatever the user typed, which may be several statements.
    ///
    /// Each is reported separately, in order, because a script whose third
    /// statement failed has to say which one — and because a `SELECT` after two
    /// `INSERT`s should still show its rows.
    pub async fn run(&self, sql: &str) -> Result<Vec<Outcome>, String> {
        let client = match &self.backend {
            Backend::Wire(link) => link.open().await?.0,
            Backend::Native(client) => return run_native(client, sql).await,
        };
        let messages = client.simple_query(sql).await.map_err(|e| sql_error(&e))?;
        let mut collector = Collector::new(None);
        for message in messages {
            collector.take(message);
        }
        Ok(collector.finish())
    }

    /// Runs a statement for its effect, returning what the server said.
    pub async fn execute(&self, sql: &str) -> Result<String, String> {
        let outcomes = self.run(sql).await?;
        Ok(outcomes
            .last()
            .map(|outcome| match outcome {
                Outcome::Rows(grid) => {
                    // A `SET CONFIG` reports what it changed in its one row,
                    // which is more useful than "1 row".
                    grid.rows
                        .first()
                        .and_then(|row| row.first().cloned().flatten())
                        .unwrap_or_else(|| grid.tag.clone())
                }
                Outcome::Affected { tag, .. } => tag.clone(),
            })
            .unwrap_or_else(|| "ok".to_string()))
    }
}

/// Gathers a wire answer's messages into outcomes, keeping at most `cap` rows
/// of each result and counting the rest.
struct Collector {
    outcomes: Vec<Outcome>,
    current: Option<Grid>,
    /// The current result's rows, kept or not.
    seen: u64,
    cap: Option<u64>,
}

impl Collector {
    fn new(cap: Option<u64>) -> Self {
        Collector { outcomes: Vec::new(), current: None, seen: 0, cap }
    }

    fn grid(columns: Vec<String>) -> Grid {
        Grid { columns, rows: Vec::new(), tag: String::new(), total: None }
    }

    /// Ends the current result.
    fn close(&mut self) {
        if let Some(mut grid) = self.current.take() {
            grid.tag = rows_tag(grid.rows.len());
            if self.seen > grid.rows.len() as u64 {
                grid.total = Some(self.seen);
            }
            self.outcomes.push(Outcome::Rows(grid));
        }
        self.seen = 0;
    }

    fn take(&mut self, message: SimpleQueryMessage) {
        match message {
            SimpleQueryMessage::RowDescription(columns) => {
                // A new description ends the previous statement's rows even
                // when no CommandComplete separated them.
                self.close();
                self.current =
                    Some(Self::grid(columns.iter().map(|c| c.name().to_string()).collect()));
            }
            SimpleQueryMessage::Row(row) => {
                // A server may send rows without having sent a description
                // this session — name the columns from the row itself
                // rather than dropping it.
                let grid = self.current.get_or_insert_with(|| {
                    Self::grid(row.columns().iter().map(|c| c.name().to_string()).collect())
                });
                self.seen += 1;
                if self.cap.is_none_or(|cap| (grid.rows.len() as u64) < cap) {
                    grid.rows.push(
                        (0..grid.columns.len())
                            .map(|index| row.get(index).map(str::to_string))
                            .collect(),
                    );
                }
            }
            SimpleQueryMessage::CommandComplete(affected) => {
                if self.current.is_some() {
                    self.close();
                } else {
                    self.outcomes.push(affected_outcome(affected));
                }
            }
            _ => {}
        }
    }

    fn finish(mut self) -> Vec<Outcome> {
        self.close();
        self.outcomes
    }
}

fn rows_tag(rows: usize) -> String {
    format!("{} row{}", rows, if rows == 1 { "" } else { "s" })
}

fn affected_outcome(affected: u64) -> Outcome {
    Outcome::Affected {
        tag: format!("{} row{} affected", affected, if affected == 1 { "" } else { "s" }),
        rows: affected,
    }
}

/// Stops a run of the editor, from another task while the run waits.
#[derive(Clone)]
pub enum Canceller {
    /// PostgreSQL's CancelRequest, on a connection of its own.
    Wire { token: tokio_postgres::CancelToken, tls: bool },
    /// A native CANCEL, on the shared connection, of the token the run was
    /// sent with.
    Native {
        client: Arc<rasterizeddb_client::Client>,
        token: rasterizeddb_client::CancelToken,
    },
}

impl std::fmt::Debug for Canceller {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Canceller::Wire { .. } => "Canceller::Wire",
            Canceller::Native { .. } => "Canceller::Native",
        })
    }
}

impl Canceller {
    /// Asks the server to stop the run; what to tell the user, either way.
    ///
    /// A stopped statement fails with SQLSTATE 57014, which is how the run
    /// itself reports it. Over the wire protocol the server does not say
    /// whether anything was running, as in PostgreSQL.
    pub async fn cancel(self) -> Result<String, String> {
        match self {
            Canceller::Wire { token, tls } => {
                let sent = if tls {
                    token.cancel_query(tls_connector()).await
                } else {
                    token.cancel_query(NoTls).await
                };
                sent.map(|()| "cancel requested".to_string())
                    .map_err(|e| format!("the cancel request was not delivered: {}", e))
            }
            Canceller::Native { client, token } => match client.cancel(&token).await {
                Ok(true) => Ok("cancel requested".to_string()),
                Ok(false) => Ok("nothing was running to cancel: the statement had finished, or \
                                 the run was between two statements"
                    .to_string()),
                Err(e) => Err(native_error(&e)),
            },
        }
    }
}

/// What a run of the editor answered.
#[derive(Debug, Clone, PartialEq)]
pub struct EditorAnswer {
    pub result: Result<Vec<Outcome>, String>,
    /// The notices the run raised, in order.
    pub notices: Vec<String>,
    /// Where the editor's transaction stands after the run; `None` over the
    /// wire protocol, whose driver does not say.
    pub transaction: Option<rasterizeddb_client::TransactionStatus>,
}

impl Session {
    /// What stops the next run of the editor. Made before the run starts, so
    /// the run can be stopped while it waits.
    pub fn canceller(&self) -> Canceller {
        match &self.backend {
            Backend::Wire(link) => Canceller::Wire {
                token: link.current().cancel_token(),
                tls: self.encrypted,
            },
            Backend::Native(client) => Canceller::Native {
                client: client.clone(),
                token: rasterizeddb_client::CancelToken::new(),
            },
        }
    }

    /// Runs what the user typed in the editor: [`run`](Self::run), with at
    /// most `max_rows` rows kept of each result (the rest counted in
    /// [`Grid::total`]), stoppable by `canceller`, and with the notices.
    ///
    /// Over native the editor has a connection of its own, opened by its
    /// first run, so a `BEGIN` stays open from one run to the next as it does
    /// over the wire protocol. The panels keep to the shared connection, and
    /// so do not see what the editor's open block has not committed.
    pub async fn run_editor(
        &self,
        sql: &str,
        max_rows: Option<u64>,
        canceller: &Canceller,
    ) -> EditorAnswer {
        match (&self.backend, canceller) {
            (Backend::Native(client), Canceller::Native { token, .. }) => {
                let editor = match self
                    .editor
                    .get_or_try_init(|| client.simple_session())
                    .await
                {
                    Ok(editor) => editor,
                    Err(e) => {
                        return EditorAnswer {
                            result: Err(native_error(&e)),
                            notices: Vec::new(),
                            transaction: None,
                        };
                    }
                };
                let options = rasterizeddb_client::SimpleOptions {
                    cancel: Some(token.clone()),
                    max_rows,
                };
                let ran = editor.query_with(sql, &options).await;
                let transaction = Some(editor.transaction());
                match ran {
                    Ok(answer) => EditorAnswer {
                        notices: answer.notices.clone(),
                        result: Ok(outcomes(answer)),
                        transaction,
                    },
                    Err(e) => EditorAnswer {
                        result: Err(native_error(&e)),
                        notices: Vec::new(),
                        transaction,
                    },
                }
            }
            (Backend::Wire(link), _) => {
                self.notices.lock().unwrap_or_else(|e| e.into_inner()).clear();
                let (client, reopened) = match link.open().await {
                    Ok(opened) => opened,
                    Err(e) => {
                        return EditorAnswer { result: Err(e), notices: Vec::new(), transaction: None };
                    }
                };
                let result = run_wire_capped(&client, sql, max_rows).await;
                let mut notices =
                    std::mem::take(&mut *self.notices.lock().unwrap_or_else(|e| e.into_inner()));
                if reopened {
                    notices.insert(0, WIRE_REOPENED_NOTICE.to_string());
                }
                EditorAnswer { result, notices, transaction: None }
            }
            (Backend::Native(_), Canceller::Wire { .. }) => EditorAnswer {
                result: Err("internal: a wire canceller for a native session".to_string()),
                notices: Vec::new(),
                transaction: None,
            },
        }
    }

    /// Where the editor's own connection's transaction stands; `None` before
    /// its first run, and over the wire protocol.
    pub fn editor_transaction(&self) -> Option<rasterizeddb_client::TransactionStatus> {
        self.editor.get().map(|editor| editor.transaction())
    }

    /// Keeps the editor's open block from being closed as idle by the server
    /// (its request timeout, 30 s by default). Nothing to do outside a block.
    pub async fn keep_editor_alive(&self) -> Result<(), String> {
        match self.editor.get() {
            Some(editor) if editor.transaction() != rasterizeddb_client::TransactionStatus::Idle => {
                editor.ping().await.map_err(|e| native_error(&e))
            }
            _ => Ok(()),
        }
    }
}

/// The wire transport's run of the editor: the answer streamed, so a result
/// past the row limit is counted rather than held.
async fn run_wire_capped(
    client: &Client,
    sql: &str,
    max_rows: Option<u64>,
) -> Result<Vec<Outcome>, String> {
    use futures_core::Stream;

    let stream = client.simple_query_raw(sql).await.map_err(|e| sql_error(&e))?;
    let mut stream = Box::pin(stream);
    let mut collector = Collector::new(max_rows);
    while let Some(message) = std::future::poll_fn(|cx| stream.as_mut().poll_next(cx)).await {
        collector.take(message.map_err(|e| sql_error(&e))?);
    }
    Ok(collector.finish())
}

/// Strips the driver's wrapper off a server error.
///
/// `tokio_postgres::Error` displays as "db error: ERROR: <message>", and the
/// prefix is noise in a panel that is already labelled as an error.
pub(crate) fn sql_error(error: &tokio_postgres::Error) -> String {
    let text = error
        .as_db_error()
        .map(|db| db.message().to_string())
        .unwrap_or_else(|| error.to_string());
    text.trim_start_matches("db error: ")
        .trim_start_matches("ERROR: ")
        .to_string()
}

/// [`Session::run`] over the native protocol: the simple-query form's
/// answer, made into the outcomes the wire transport makes, with the same
/// tags.
async fn run_native(
    client: &rasterizeddb_client::Client,
    sql: &str,
) -> Result<Vec<Outcome>, String> {
    let answer = client
        .simple_query(sql)
        .await
        .map_err(|e| native_error(&e))?;
    Ok(outcomes(answer))
}

/// A simple-query answer as the outcomes the wire transport makes.
fn outcomes(answer: rasterizeddb_client::SimpleAnswer) -> Vec<Outcome> {
    use rasterizeddb_client::SimpleResult;

    answer
        .results
        .into_iter()
        .map(|result| {
            // Every row, as the tag counts them: more than were sent when the
            // row limit kept fewer.
            let counted = result.row_count();
            match result {
                SimpleResult::Rows { columns, rows, .. } => {
                    let count = rows.len();
                    Outcome::Rows(Grid {
                        total: counted.filter(|n| *n > count as u64),
                        columns: columns.into_iter().map(|c| c.name).collect(),
                        rows,
                        tag: rows_tag(count),
                    })
                }
                // As tokio-postgres reports a tag without a count: zero.
                SimpleResult::Command { rows, .. } => affected_outcome(rows.unwrap_or(0)),
            }
        })
        .collect()
}

/// Detects which dialect a server speaks.
///
/// `SELECT version()` rather than probing for a `pg_catalog` relation. Both
/// servers answer it — RasterizedDB from a match arm, PostgreSQL for real — so
/// nothing fails and nothing is logged.
///
/// The previous probe was `SELECT 1 FROM pg_database LIMIT 1`, which detected
/// the dialect by *failing* against RasterizedDB. It worked, and it wrote an
/// `ERROR` into the server's log on every single connection. A console that
/// puts a red line in someone's journal every time it opens is teaching them to
/// ignore the log it appears in.
pub async fn detect_flavor(session: &Session) -> Flavor {
    // Only RasterizedDB speaks the native protocol.
    if session.transport == Transport::Native {
        return Flavor::Rasterized;
    }
    let version = session
        .query("SELECT version()")
        .await
        .ok()
        .and_then(|grid| grid.rows.first().and_then(|row| row.first().cloned()).flatten())
        .unwrap_or_default()
        .to_ascii_lowercase();

    // Positive identification of PostgreSQL, rather than of everything else:
    // an unknown server that speaks this protocol is far more likely to be
    // RasterizedDB — which is what this console is for — and the connection
    // screen lets the choice be overridden either way.
    if version.contains("postgresql") {
        Flavor::Postgres
    } else {
        Flavor::Rasterized
    }
}

fn log_line(message: &str) {
    eprintln!("rdb_studio: {}", message);
}

/// Encrypts without authenticating. See the module docs for why, and for what
/// it is and is not worth.
#[derive(Debug)]
struct AcceptAnyServer;

impl ServerCertVerifier for AcceptAnyServer {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

#[cfg(test)]
mod socket_tests {
    use super::*;

    #[test]
    fn a_path_is_a_socket_directory_and_a_name_is_a_host() {
        assert!(is_socket_dir("/run/rasterizeddb"));
        assert!(is_socket_dir("/tmp"));
        for host in ["127.0.0.1", "localhost", "db.example.com", "::1", ""] {
            assert!(!is_socket_dir(host), "{host:?} was taken for a socket directory");
        }
    }

    #[test]
    fn the_endpoint_names_the_socket_file_a_server_creates() {
        assert_eq!(endpoint("/run/rdb", 5432), "/run/rdb/.s.PGSQL.5432");
        assert_eq!(endpoint("/run/rdb/", 6543), "/run/rdb/.s.PGSQL.6543");
        assert_eq!(endpoint("127.0.0.1", 5432), "127.0.0.1:5432");
    }

    #[test]
    fn a_native_host_that_is_a_path_is_the_socket_file() {
        assert_eq!(
            native_address("/run/rdb/rdb.sock", 61170),
            "unix:/run/rdb/rdb.sock"
        );
        assert_eq!(native_address("127.0.0.1", 61170), "127.0.0.1:61170");
    }

    #[test]
    fn a_native_refusal_reads_as_the_wire_transport_s_does() {
        let e = rasterizeddb_client::Error::Server(
            "relation \"x\" does not exist (SQLSTATE 42P01)".into(),
        );
        assert_eq!(native_error(&e), "relation \"x\" does not exist");
        // A parenthesis that is not the code's is the message's own.
        let e = rasterizeddb_client::Error::Server("bad value (got 3)".into());
        assert_eq!(native_error(&e), "bad value (got 3)");
    }

    #[test]
    fn a_profile_saved_before_the_choice_still_speaks_the_wire_protocol() {
        let saved = r#"{"name":"old","flavor":"Rasterized","host":"127.0.0.1","port":5432,
            "database":"postgres","user":"u","tls":"Prefer"}"#;
        let profile: Connection = serde_json::from_str(saved).unwrap();
        assert_eq!(profile.transport, Transport::Wire);
        assert_eq!(profile.effective_transport(), Transport::Wire);
        // A new one is native, on the native port.
        let fresh = Connection::default();
        assert_eq!(fresh.effective_transport(), Transport::Native);
        assert_eq!(fresh.port, crate::model::NATIVE_PORT);
        // And PostgreSQL never is, whatever was picked.
        let pg = Connection {
            flavor: Flavor::Postgres,
            ..Connection::default()
        };
        assert_eq!(pg.effective_transport(), Transport::Wire);
    }

    /// A live check against a real server, for a run that has one:
    /// `RDB_STUDIO_SOCKET_DIR=/dir RDB_STUDIO_SOCKET_PORT=5432 cargo test -p
    /// rdb_studio -- --ignored`. The profile asks for TLS, as the default one
    /// does, and must connect anyway.
    #[tokio::test]
    #[ignore]
    async fn connects_to_a_live_server_on_its_socket_with_tls_asked_for() {
        let Ok(dir) = std::env::var("RDB_STUDIO_SOCKET_DIR") else {
            return;
        };
        let port = std::env::var("RDB_STUDIO_SOCKET_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(5432);
        let profile = Connection {
            host: dir.clone(),
            port,
            user: std::env::var("RDB_STUDIO_SOCKET_USER").unwrap_or_else(|_| "rasterized".into()),
            password: std::env::var("RDB_STUDIO_SOCKET_PASSWORD").unwrap_or_default(),
            tls: TlsMode::Require,
            transport: Transport::Wire,
            ..Connection::default()
        };
        let session = connect(profile).await.expect("connected over the socket");
        assert!(!session.encrypted);
        assert_eq!(session.server, endpoint(&dir, port));
        let rows = session
            .client()
            .expect("a wire session")
            .simple_query("SELECT 1")
            .await
            .expect("a query");
        assert!(!rows.is_empty());
    }
}
