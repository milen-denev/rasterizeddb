//! The SQL Server client protocol (TDS 7.4, and TDS 8.0's TLS-first form),
//! beside the other listeners.
//!
//! Built as the MySQL listener is ([`crate::mysql`]): each connection drives
//! an in-process PostgreSQL session ([`crate::pgwire::proxied::PgBridge`]),
//! so transactions, savepoints, isolation, cancellation and the privilege
//! gates are the engine's own. This module owns TDS ([`tds`]), T-SQL
//! ([`translate`]), the batch interpreter (variables, IF/WHILE, TRY/CATCH)
//! and the RPC procedures drivers call (`sp_executesql`, `sp_prepare`,
//! `sp_prepexec`, `sp_execute`, `sp_unprepare`).
//!
//! **Authentication** is SQL Server authentication: a login name and
//! password, checked against the role catalogue the other listeners use.
//! The password travels inside TLS (TDS encrypts the whole connection here;
//! login-only encryption is not offered). Windows authentication is not.

pub(crate) mod conn;
pub(crate) mod lex;
pub(crate) mod tds;
pub(crate) mod routines;
pub(crate) mod translate;

use std::io;
use std::sync::Arc as StdArc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use rclite::Arc;

use crate::core::database::catalog::DatabaseCatalog;
use crate::pgwire::ConnectionLimits;

/// How a SQL Server client proves who it is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MsAuth {
    Trust,
    /// SQL Server authentication against the role catalogue.
    Password,
}

impl MsAuth {
    pub fn parse(text: &str) -> Option<Self> {
        match text.trim().to_ascii_lowercase().as_str() {
            "trust" => Some(MsAuth::Trust),
            "password" | "sql" | "scram-sha-256" => Some(MsAuth::Password),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MsTls {
    Disable,
    Prefer,
    Require,
}

impl MsTls {
    pub fn parse(text: &str) -> Option<Self> {
        match text.trim().to_ascii_lowercase().as_str() {
            "disable" | "off" => Some(MsTls::Disable),
            "prefer" => Some(MsTls::Prefer),
            "require" | "on" => Some(MsTls::Require),
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct MsSocket {
    pub path: std::path::PathBuf,
    pub mode: u32,
    pub group: Option<u32>,
    pub auth: Option<MsAuth>,
}

#[derive(Clone, Debug)]
pub struct MsConfig {
    pub tcp: Option<(String, u16)>,
    pub socket: Option<MsSocket>,
    pub auth: MsAuth,
    pub tls: MsTls,
    pub cert_path: Option<String>,
    pub key_path: Option<String>,
    pub bootstrap_user: String,
    pub bootstrap_password: Option<String>,
    pub limits: ConnectionLimits,
}

pub(crate) struct Shared {
    pub(crate) config: MsConfig,
    pub(crate) catalog: Arc<DatabaseCatalog>,
    pub(crate) acceptor: Option<tokio_rustls::TlsAcceptor>,
    pub(crate) active: AtomicUsize,
    pub(crate) stopping: tokio::sync::watch::Sender<bool>,
}

pub struct MsListener {
    tcp: Option<tokio::net::TcpListener>,
    #[cfg(unix)]
    unix: Option<(tokio::net::UnixListener, rastcp::net::SocketFile)>,
    config: MsConfig,
}

impl MsListener {
    pub fn local_addr(&self) -> Option<std::net::SocketAddr> {
        self.tcp.as_ref().and_then(|l| l.local_addr().ok())
    }
}

pub async fn bind(config: MsConfig) -> io::Result<MsListener> {
    if config.tcp.is_none() && config.socket.is_none() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "the SQL Server protocol has neither a TCP port nor a unix socket to serve",
        ));
    }
    #[cfg(unix)]
    let unix = match &config.socket {
        Some(socket) => {
            let auth = socket.auth.unwrap_or(config.auth);
            if auth == MsAuth::Trust && socket.mode & 0o007 != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "refusing to serve the SQL Server protocol on {} with mode {:o} and trust \
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
    Ok(MsListener {
        tcp,
        #[cfg(unix)]
        unix,
        config,
    })
}

pub struct MsServer {
    shared: StdArc<Shared>,
    task: tokio::task::JoinHandle<()>,
}

pub async fn serve(listener: MsListener, catalog: Arc<DatabaseCatalog>) -> io::Result<MsServer> {
    let config = listener.config.clone();
    if config.auth == MsAuth::Password
        || config.socket.as_ref().and_then(|s| s.auth) == Some(MsAuth::Password)
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
        MsTls::Disable => None,
        _ => Some(crate::pgwire::proxied::tls_acceptor_with_alpn(
            config.cert_path.clone(),
            config.key_path.clone(),
            vec![b"tds/8.0".to_vec()],
        )?),
    };
    if let Some((addr, port)) = &config.tcp {
        let public = addr
            .parse::<std::net::IpAddr>()
            .map(|ip| !ip.is_loopback())
            .unwrap_or(true);
        if public && config.tls != MsTls::Require {
            log::warn!(
                "the SQL Server protocol is bound to {addr}:{port} with TLS {:?}: a client that declines \
                 encryption sends its password in the clear",
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
    });
    let serving = shared.clone();
    let task = tokio::spawn(async move { accept_loop(listener, serving).await });
    if let Some((addr, port)) = shared.config.tcp.as_ref() {
        log::info!("SQL Server protocol listening on {addr}:{port}");
    }
    if let Some(socket) = shared.config.socket.as_ref() {
        log::info!(
            "SQL Server protocol listening on unix socket {}",
            socket.path.display()
        );
    }
    Ok(MsServer { shared, task })
}

impl MsServer {
    /// Stops accepting and waits up to `grace` for connections to close at
    /// their next idle moment.
    pub async fn drain(self, grace: Duration) {
        let _ = self.shared.stopping.send(true);
        self.task.abort();
        let deadline = Instant::now() + grace;
        while self.shared.active.load(Ordering::Acquire) > 0 && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let left = self.shared.active.load(Ordering::Acquire);
        if left > 0 {
            log::warn!("{left} SQL Server connection(s) still running at shutdown");
        }
    }
}

async fn accept_loop(listener: MsListener, shared: StdArc<Shared>) {
    let MsListener {
        tcp,
        #[cfg(unix)]
        unix,
        ..
    } = listener;
    let mut backoff = Duration::from_millis(50);
    loop {
        let accepted: io::Result<(tds::Base, String)> = tokio::select! {
            r = async {
                match &tcp {
                    Some(l) => l.accept().await.map(|(s, peer)| {
                        let _ = s.set_nodelay(true);
                        (tds::Base::Tcp(s), peer.to_string())
                    }),
                    None => std::future::pending().await,
                }
            } => r,
            r = async {
                #[cfg(unix)]
                {
                    match &unix {
                        Some((l, _)) => l.accept().await.map(|(s, _)| (tds::Base::Unix(s), "localhost".to_string())),
                        None => std::future::pending().await,
                    }
                }
                #[cfg(not(unix))]
                {
                    std::future::pending::<io::Result<(tds::Base, String)>>().await
                }
            } => r,
        };
        match accepted {
            Ok((base, peer)) => {
                backoff = Duration::from_millis(50);
                let shared = shared.clone();
                let over = shared.active.fetch_add(1, Ordering::AcqRel)
                    >= shared.config.limits.max_connections;
                tokio::spawn(async move {
                    let result =
                        conn::serve_connection(base, peer.clone(), shared.clone(), over).await;
                    shared.active.fetch_sub(1, Ordering::AcqRel);
                    if let Err(e) = result {
                        log::debug!("SQL Server connection from {peer} ended: {e}");
                    }
                });
            }
            Err(e) => {
                log::warn!("SQL Server listener: accept failed: {e}");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(1));
            }
        }
    }
}
