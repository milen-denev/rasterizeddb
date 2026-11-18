//! The MySQL (and MariaDB) client protocol, beside the other listeners.
//!
//! A MySQL connection is translated, not reimplemented: each one drives an
//! in-process PostgreSQL session ([`crate::pgwire::proxied::PgBridge`]), so
//! transactions, savepoints, isolation, prepared statements, cancellation
//! and the privilege gates are the ones every other client of the engine
//! gets. This module owns MySQL's wire format (handshake, authentication,
//! packets, text and binary rows), its dialect ([`translate`]), and the
//! session statements connectors send ([`admin`]).
//!
//! **Authentication** is `caching_sha2_password`, MySQL 8's default, checked
//! against the same role catalogue the PostgreSQL listener uses: the first
//! login of a role sends its password once over TLS (or the unix socket) and
//! is checked against the stored SCRAM verifier; later ones prove it with a
//! scramble against a hash kept in memory, as MySQL does. There is no RSA
//! exchange, so a client on plain TCP must use TLS for its first login.
//!
//! **What MySQL does that the engine does differently** is translated where
//! the answer can be made the same, and refused where it cannot — never
//! answered differently in silence. See [`translate`] for the details.

pub(crate) mod admin;
pub(crate) mod conn;
pub(crate) mod lex;
pub(crate) mod load;
pub(crate) mod packet;
pub(crate) mod results;
pub(crate) mod translate;

use std::collections::HashMap;
use std::io;
use std::sync::Arc as StdArc;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use rclite::Arc;

use crate::core::database::catalog::DatabaseCatalog;
use crate::pgwire::ConnectionLimits;

/// The version string clients are told. MySQL 8 connectors read the leading
/// number to decide what the server can do.
pub(crate) const SERVER_VERSION: &str = "8.0.36-rasterizeddb";

/// How a MySQL client proves who it is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MyAuth {
    /// Whatever user name is given is accepted.
    Trust,
    /// `caching_sha2_password` against the role catalogue.
    Password,
}

impl MyAuth {
    pub fn parse(text: &str) -> Option<Self> {
        match text.trim().to_ascii_lowercase().replace('_', "-").as_str() {
            "trust" => Some(MyAuth::Trust),
            "password" | "caching-sha2-password" | "scram-sha-256" => Some(MyAuth::Password),
            _ => None,
        }
    }
}

/// Whether TLS is offered, and whether it is required.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MyTls {
    Disable,
    Prefer,
    Require,
}

impl MyTls {
    pub fn parse(text: &str) -> Option<Self> {
        match text.trim().to_ascii_lowercase().as_str() {
            "disable" | "off" => Some(MyTls::Disable),
            "prefer" => Some(MyTls::Prefer),
            "require" | "on" => Some(MyTls::Require),
            _ => None,
        }
    }
}

/// A unix socket to serve on.
#[derive(Clone, Debug)]
pub struct MySocket {
    pub path: std::path::PathBuf,
    pub mode: u32,
    pub group: Option<u32>,
    /// How callers on the socket authenticate; `None` uses the TCP method.
    pub auth: Option<MyAuth>,
}

/// The MySQL listener's configuration.
#[derive(Clone, Debug)]
pub struct MyConfig {
    /// TCP address and port; `None` serves the socket only.
    pub tcp: Option<(String, u16)>,
    pub socket: Option<MySocket>,
    pub auth: MyAuth,
    pub tls: MyTls,
    pub cert_path: Option<String>,
    pub key_path: Option<String>,
    /// Role created in an empty role catalogue, so a fresh server can be
    /// reached at all (the same one the PostgreSQL listener creates).
    pub bootstrap_user: String,
    pub bootstrap_password: Option<String>,
    pub limits: ConnectionLimits,
}

/// Everything a connection needs from its listener.
pub(crate) struct Shared {
    pub(crate) config: MyConfig,
    pub(crate) catalog: Arc<DatabaseCatalog>,
    pub(crate) acceptor: Option<tokio_rustls::TlsAcceptor>,
    pub(crate) active: AtomicUsize,
    /// Set when the server is stopping: connections close at their next
    /// idle moment.
    pub(crate) stopping: tokio::sync::watch::Sender<bool>,
    pub(crate) started: Instant,
    pub(crate) questions: std::sync::atomic::AtomicU64,
}

/// A live connection, for `KILL` and `SHOW PROCESSLIST`.
pub(crate) struct Registered {
    pub(crate) user: String,
    pub(crate) host: String,
    pub(crate) database: parking_lot::Mutex<String>,
    /// The session's cancel target, once it has one.
    pub(crate) cancel: parking_lot::Mutex<Option<(i32, i32)>>,
    pub(crate) kill: tokio::sync::Notify,
    pub(crate) since: Instant,
    pub(crate) query: parking_lot::Mutex<Option<String>>,
}

pub(crate) fn registry() -> &'static parking_lot::Mutex<HashMap<u32, StdArc<Registered>>> {
    static REGISTRY: std::sync::OnceLock<parking_lot::Mutex<HashMap<u32, StdArc<Registered>>>> =
        std::sync::OnceLock::new();
    REGISTRY.get_or_init(Default::default)
}

pub(crate) fn next_connection_id() -> u32 {
    static NEXT: AtomicU32 = AtomicU32::new(1);
    loop {
        let id = NEXT.fetch_add(1, Ordering::Relaxed);
        if id != 0 && !registry().lock().contains_key(&id) {
            return id;
        }
    }
}

/// A bound listener, not yet serving.
pub struct MyListener {
    tcp: Option<tokio::net::TcpListener>,
    #[cfg(unix)]
    unix: Option<(tokio::net::UnixListener, rastcp::net::SocketFile)>,
    config: MyConfig,
}

impl MyListener {
    /// The TCP address actually bound (port 0 resolved).
    pub fn local_addr(&self) -> Option<std::net::SocketAddr> {
        self.tcp.as_ref().and_then(|l| l.local_addr().ok())
    }
}

/// Binds what `config` asks for. Refused here, at start-up, rather than in a
/// task: a port that cannot be had should stop the server with the reason.
pub async fn bind(config: MyConfig) -> io::Result<MyListener> {
    if config.tcp.is_none() && config.socket.is_none() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "the MySQL protocol has neither a TCP port nor a unix socket to serve",
        ));
    }
    #[cfg(unix)]
    let unix = match &config.socket {
        Some(socket) => {
            let auth = socket.auth.unwrap_or(config.auth);
            if auth == MyAuth::Trust && socket.mode & 0o007 != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "refusing to serve the MySQL protocol on {} with mode {:o} and trust \
                         authentication: every user on the machine would have unauthenticated access",
                        socket.path.display(),
                        socket.mode
                    ),
                ));
            }
            let (listener, file) =
                rastcp::net::SocketFile::bind(&socket.path, socket.mode, socket.group)
                    .map_err(|e| io::Error::other(e.to_string()))?;
            Some((tokio::net::UnixListener::from_std(listener)?, file))
        }
        None => None,
    };
    #[cfg(not(unix))]
    if config.socket.is_some() {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "unix sockets are not supported on this platform",
        ));
    }
    let tcp = match &config.tcp {
        Some((addr, port)) => Some(tokio::net::TcpListener::bind(format!("{addr}:{port}")).await?),
        None => None,
    };
    Ok(MyListener {
        tcp,
        #[cfg(unix)]
        unix,
        config,
    })
}

/// A running server, to be drained at shutdown.
pub struct MyServer {
    shared: StdArc<Shared>,
    task: tokio::task::JoinHandle<()>,
}

/// Starts serving `listener` on `catalog`: roles prepared, TLS loaded, then
/// the accept loop in a task.
pub async fn serve(listener: MyListener, catalog: Arc<DatabaseCatalog>) -> io::Result<MyServer> {
    let config = listener.config.clone();
    if config.auth == MyAuth::Password
        || config.socket.as_ref().and_then(|s| s.auth) == Some(MyAuth::Password)
    {
        for database in catalog.all() {
            crate::auth::ensure_role_catalogue(
                &database,
                &config.bootstrap_user,
                config.bootstrap_password.as_deref(),
                crate::auth::AuthMethod::ScramSha256,
            )
            .await
            .map_err(io::Error::other)?;
        }
    }
    let acceptor = match config.tls {
        MyTls::Disable => None,
        _ => Some(crate::pgwire::proxied::tls_acceptor(
            config.cert_path.clone(),
            config.key_path.clone(),
        )?),
    };
    if let Some((addr, port)) = &config.tcp {
        let public = addr
            .parse::<std::net::IpAddr>()
            .map(|ip| !ip.is_loopback())
            .unwrap_or(true);
        if public && config.tls != MyTls::Require {
            log::warn!(
                "the MySQL protocol is bound to {addr}:{port} with TLS {:?}: clients may connect unencrypted \
                 over a network interface",
                config.tls
            );
        }
    }
    let (stopping, _) = tokio::sync::watch::channel(false);
    let shared = StdArc::new(Shared {
        config,
        catalog,
        acceptor,
        active: AtomicUsize::new(0),
        stopping,
        started: Instant::now(),
        questions: std::sync::atomic::AtomicU64::new(0),
    });
    let serving = shared.clone();
    let task = tokio::spawn(async move { accept_loop(listener, serving).await });
    if let Some(addr) = shared.config.tcp.as_ref() {
        log::info!("MySQL protocol listening on {}:{}", addr.0, addr.1);
    }
    if let Some(socket) = shared.config.socket.as_ref() {
        log::info!(
            "MySQL protocol listening on unix socket {}",
            socket.path.display()
        );
    }
    Ok(MyServer { shared, task })
}

impl MyServer {
    /// Stops accepting, asks every connection to close at its next idle
    /// moment, and waits up to `grace` for them to. Called before the
    /// databases are sealed at shutdown, so no statement of this listener is
    /// still running when they are.
    pub async fn drain(self, grace: Duration) {
        let _ = self.shared.stopping.send(true);
        self.task.abort();
        let deadline = Instant::now() + grace;
        while self.shared.active.load(Ordering::Acquire) > 0 && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let left = self.shared.active.load(Ordering::Acquire);
        if left > 0 {
            log::warn!("{left} MySQL connection(s) still running at shutdown");
        }
    }
}

async fn accept_loop(listener: MyListener, shared: StdArc<Shared>) {
    let MyListener {
        tcp,
        #[cfg(unix)]
        unix,
        ..
    } = listener;
    let mut backoff = Duration::from_millis(50);
    loop {
        let accepted = tokio::select! {
            r = async {
                match &tcp {
                    Some(l) => l.accept().await.map(|(s, peer)| {
                        let _ = s.set_nodelay(true);
                        (packet::MyStream::Tcp(s), peer.to_string(), None)
                    }),
                    None => std::future::pending().await,
                }
            } => r,
            r = async {
                #[cfg(unix)]
                {
                    match &unix {
                        Some((l, _)) => match l.accept().await {
                            Ok((s, _)) => {
                                let uid = s.peer_cred().ok().map(|c| c.uid());
                                Ok((packet::MyStream::Unix(s), "localhost".to_string(), uid))
                            }
                            Err(e) => Err(e),
                        },
                        None => std::future::pending().await,
                    }
                }
                #[cfg(not(unix))]
                {
                    std::future::pending::<io::Result<(packet::MyStream, String, Option<u32>)>>().await
                }
            } => r,
        };
        match accepted {
            Ok((stream, peer, uid)) => {
                backoff = Duration::from_millis(50);
                let shared = shared.clone();
                let over = shared.active.fetch_add(1, Ordering::AcqRel)
                    >= shared.config.limits.max_connections;
                tokio::spawn(async move {
                    let result =
                        conn::serve_connection(stream, peer.clone(), uid, shared.clone(), over)
                            .await;
                    shared.active.fetch_sub(1, Ordering::AcqRel);
                    if let Err(e) = result {
                        log::debug!("MySQL connection from {peer} ended: {e}");
                    }
                });
            }
            Err(e) => {
                log::warn!("MySQL listener: accept failed: {e}");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(1));
            }
        }
    }
}
