//! Talking to the server.
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

use std::sync::Arc;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, SignatureScheme};
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

use crate::model::{Connection, Flavor, Grid, Outcome, TlsMode};

/// A live connection, and what the console learned about it while opening one.
#[derive(Clone)]
pub struct Session {
    client: Arc<Client>,
    pub flavor: Flavor,
    pub database: String,
    pub server: String,
    /// Whether the link is encrypted, which the status bar reports.
    pub encrypted: bool,
}

impl std::fmt::Debug for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Session")
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
        Arc::ptr_eq(&self.client, &other.client)
    }
}

/// Opens a connection, honouring the TLS mode.
///
/// `Prefer` genuinely retries: it tries TLS and, if the handshake fails,
/// connects again in cleartext. Reporting the TLS error would be wrong — a
/// server started with `--pg-tls disable` refuses the handshake, and that is
/// the configuration `prefer` exists for.
pub async fn connect(profile: Connection) -> Result<Session, String> {
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

    let server = format!("{}:{}", profile.host, profile.port);

    let attempt_tls = matches!(profile.tls, TlsMode::Prefer | TlsMode::Require);
    if attempt_tls {
        match connect_tls(&config).await {
            Ok(client) => {
                return Ok(Session {
                    client: Arc::new(client),
                    flavor: profile.flavor,
                    database: profile.database,
                    server,
                    encrypted: true,
                });
            }
            Err(e) if profile.tls == TlsMode::Require => {
                return Err(format!("TLS connection refused: {}", e));
            }
            Err(_) => { /* prefer: fall through to cleartext */ }
        }
    }

    let (client, connection) = config
        .connect(NoTls)
        .await
        .map_err(|e| describe(&e.to_string(), &profile))?;
    drive(connection);

    Ok(Session {
        client: Arc::new(client),
        flavor: profile.flavor,
        database: profile.database,
        server,
        encrypted: false,
    })
}

async fn connect_tls(config: &tokio_postgres::Config) -> Result<Client, String> {
    let tls_config = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(AcceptAnyServer))
        .with_no_client_auth();
    let connector = tokio_postgres_rustls::MakeRustlsConnect::new(tls_config);
    let (client, connection) = config.connect(connector).await.map_err(|e| e.to_string())?;
    drive(connection);
    Ok(client)
}

/// Runs the connection's own future, which is what actually moves bytes.
///
/// Dropping it would leave a `Client` whose every call hangs, so it is spawned
/// and deliberately detached: its end is the connection closing, and the next
/// query is what reports that.
fn drive<S, T>(connection: tokio_postgres::Connection<S, T>)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            log_line(&format!("connection closed: {}", e));
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
    if error.contains("3D000") || error.contains("does not exist") {
        return format!(
            "{} — no database named `{}` on {}:{}",
            error, profile.database, profile.host, profile.port
        );
    }
    if error.contains("Connection refused") {
        return format!(
            "nothing is listening on {}:{} — is the server running?",
            profile.host, profile.port
        );
    }
    error.to_string()
}

impl Session {
    /// The driver underneath, for the benchmark, which has to send a statement
    /// without building a grid of its answer each time — that would be
    /// measuring this console's allocator rather than the server.
    pub fn client(&self) -> &Client {
        &self.client
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
        let messages = self
            .client
            .simple_query(sql)
            .await
            .map_err(|e| sql_error(&e))?;

        let mut outcomes = Vec::new();
        let mut current: Option<Grid> = None;

        for message in messages {
            match message {
                SimpleQueryMessage::RowDescription(columns) => {
                    // A new description ends the previous statement's rows even
                    // when no CommandComplete separated them.
                    if let Some(grid) = current.take() {
                        outcomes.push(Outcome::Rows(grid));
                    }
                    current = Some(Grid {
                        columns: columns.iter().map(|c| c.name().to_string()).collect(),
                        rows: Vec::new(),
                        tag: String::new(),
                    });
                }
                SimpleQueryMessage::Row(row) => {
                    // A server may send rows without having sent a description
                    // this session — name the columns from the row itself
                    // rather than dropping it.
                    let grid = current.get_or_insert_with(|| Grid {
                        columns: row.columns().iter().map(|c| c.name().to_string()).collect(),
                        rows: Vec::new(),
                        tag: String::new(),
                    });
                    grid.rows.push(
                        (0..grid.columns.len())
                            .map(|index| row.get(index).map(str::to_string))
                            .collect(),
                    );
                }
                SimpleQueryMessage::CommandComplete(affected) => match current.take() {
                    Some(mut grid) => {
                        grid.tag = format!("{} row{}", grid.rows.len(),
                                           if grid.rows.len() == 1 { "" } else { "s" });
                        outcomes.push(Outcome::Rows(grid));
                    }
                    None => outcomes.push(Outcome::Affected {
                        tag: format!("{} row{} affected", affected,
                                     if affected == 1 { "" } else { "s" }),
                        rows: affected,
                    }),
                },
                _ => {}
            }
        }

        if let Some(mut grid) = current.take() {
            grid.tag = format!("{} rows", grid.rows.len());
            outcomes.push(Outcome::Rows(grid));
        }

        Ok(outcomes)
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
