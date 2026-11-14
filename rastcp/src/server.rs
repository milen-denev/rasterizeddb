use cacheguard::CacheGuard;
use log::{debug, error, info, warn};
use rustls::ServerConfig;
use rustls::compress::CompressionCache;
use rustls::crypto::aws_lc_rs::Ticketer;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::ServerSessionMemoryCache;
use socket2::{Domain, Protocol, SockAddr, Socket, TcpKeepalive, Type};
use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;
use tokio_rustls::TlsAcceptor;

use crate::cert::generate_self_signed_cert;
use crate::interrupt::{Guard, Interrupted, StopListener, StopSignal};
use crate::session::{AuthChannel, Principal, ServerAuthenticator};
use crate::tls::{ClientAuth, Fingerprint};
use crate::frame::{FrameReader, FrameWriter, KIND_DATA, KIND_FAILED, KIND_TIMED_OUT, Tag};
use crate::error::RastcpError;
use crate::{ALPN_MULTIPLEXED, ALPN_PROTOCOL};

const DEFAULT_MAX_CONNECTIONS: usize = 512;
const HARD_MAX_CONNECTIONS: usize = 4_096;
const RESERVED_SERVER_FDS: usize = 256;
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
/// How long a caller has to finish proving who it is. Short: nothing has been
/// authenticated yet, so the connection slot it holds is one an anonymous peer
/// is holding.
const DEFAULT_AUTH_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_SHUTDOWN_GRACE: Duration = Duration::from_secs(10);
/// How many requests one multiplexed connection may have being handled at
/// once. Past it the connection is not read until one finishes, so the peer
/// is slowed by TCP rather than the server by an unbounded number of tasks.
const DEFAULT_MAX_IN_FLIGHT: usize = 256;
const ACCEPT_POLL_INTERVAL: Duration = Duration::from_secs(1);
const DRAIN_POLL_INTERVAL: Duration = Duration::from_millis(50);

fn runtime_connection_budget() -> usize {
    #[cfg(unix)]
    {
        let mut limit = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };

        if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } == 0 {
            let soft_limit = if limit.rlim_cur == libc::RLIM_INFINITY {
                usize::MAX
            } else {
                limit.rlim_cur as usize
            };

            return soft_limit
                .saturating_sub(RESERVED_SERVER_FDS)
                .saturating_div(2)
                .clamp(1, HARD_MAX_CONNECTIONS);
        }
    }

    DEFAULT_MAX_CONNECTIONS
}

fn normalize_max_connections_with_budget(
    requested_max_connections: Option<usize>,
    connection_budget: usize,
) -> usize {
    let budget = connection_budget.clamp(1, HARD_MAX_CONNECTIONS);

    requested_max_connections
        .unwrap_or(DEFAULT_MAX_CONNECTIONS)
        .max(1)
        .min(budget)
}

fn normalize_max_connections(requested_max_connections: Option<usize>) -> usize {
    normalize_max_connections_with_budget(requested_max_connections, runtime_connection_budget())
}

/// Cloneable trigger for a server's graceful shutdown.
///
/// Signalling it stops the accept loop, lets in-flight requests finish, and
/// closes idle keep-alive connections with a proper TLS `close_notify`. Without
/// it the process just dies with connections open, which leaves peers stuck in
/// `CLOSE_WAIT` and the listening port in `TIME_WAIT`.
#[derive(Clone)]
pub struct ShutdownHandle {
    signal: Arc<StopSignal>,
}

impl ShutdownHandle {
    /// Asks the server to stop. Idempotent, and safe to call from a signal
    /// handler task while the server is still accepting. It sticks: an accept
    /// loop that starts afterwards stops at once.
    pub fn shutdown(&self) {
        self.signal.stop();
    }

    pub fn is_shutting_down(&self) -> bool {
        self.signal.is_stopping()
    }
}

/// What one turn of an accept loop produced.
enum AcceptStep {
    Connection(TcpStream, SocketAddr),
    Retry,
    Shutdown,
}

pub struct TcpServerBuilder {
    addr: String,
    port: u16,
    custom_certs: Option<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)>,
    max_connections: Option<usize>,
    connection_backoff_ms: Option<u64>,
    session_cache_size: usize,
    handshake_timeout: Duration,
    request_timeout: Duration,
    max_message_size: usize,
    max_in_flight: usize,
    inline_handlers: bool,
    shutdown_grace: Duration,
    client_auth: ClientAuth,
    authenticator: Option<Arc<dyn ServerAuthenticator>>,
    auth_timeout: Duration,
    allow_tls12: bool,
    early_data: bool,
    /// A socket the caller already bound, served instead of binding
    /// `addr:port`. See [`TcpServerBuilder::with_listener`].
    listener: Option<std::net::TcpListener>,
    reuse_port: bool,
}

pub struct TcpServer {
    listener: TcpListener,
    acceptor: TlsAcceptor,
    max_connections: usize,
    /// Padded inside the `Arc`, not outside it. An `Arc<AtomicUsize>` lays the
    /// counter out directly after the strong and weak reference counts, and this
    /// handle is cloned once per accepted connection — so the clone's refcount
    /// increment and the connection count's own increment were writes to the
    /// same cache line, from the accept loop and from every connection task
    /// that finishes. The guard moves the payload off the refcounts' line.
    current_connections: Arc<CacheGuard<std::sync::atomic::AtomicUsize>>,
    connection_backoff_ms: u64,
    handshake_timeout: Duration,
    request_timeout: Duration,
    max_message_size: usize,
    max_in_flight: usize,
    inline_handlers: bool,
    shutdown_grace: Duration,
    auth_timeout: Duration,
    authenticator: Option<Arc<dyn ServerAuthenticator>>,
    stop: Arc<StopSignal>,
    /// SHA-256 of the certificate this server presents, so an operator can pin
    /// it on the other nodes without going and finding the file.
    certificate_fingerprint: Fingerprint,
}

impl TcpServerBuilder {
    pub fn new(addr: &str, port: u16) -> Self {
        Self {
            addr: addr.to_string(),
            port,
            custom_certs: None,
            max_connections: None,
            connection_backoff_ms: Some(100), // Default backoff of 100ms
            session_cache_size: 1024,         // Increased from 128
            handshake_timeout: Duration::from_secs(10),
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            max_message_size: crate::common::DEFAULT_MAX_MESSAGE_SIZE,
            max_in_flight: DEFAULT_MAX_IN_FLIGHT,
            inline_handlers: false,
            shutdown_grace: DEFAULT_SHUTDOWN_GRACE,
            client_auth: ClientAuth::None,
            authenticator: None,
            auth_timeout: DEFAULT_AUTH_TIMEOUT,
            // TLS 1.2 is off. Every peer that speaks this protocol is built
            // from this crate, so there is no legacy client to accommodate,
            // and 1.3 removes the whole family of downgrade, renegotiation and
            // weak-cipher problems 1.2 configurations have to be audited for.
            allow_tls12: false,
            // 0-RTT is off: early data is replayable by anyone who captured it,
            // and a replayed request on a database protocol is a repeated
            // write, not a repeated page view.
            early_data: false,
            listener: None,
            reuse_port: false,
        }
    }

    /// A builder that serves `listener` rather than binding an address itself.
    ///
    /// For callers that must know the port before the server exists — tests
    /// above all, which bind `127.0.0.1:0` and hand the socket over. Taking a
    /// port *number* instead means letting go of the socket first, and in the
    /// window between that and the server's own bind another test can be
    /// handed the same freed port: one of them then fails to bind, or, worse,
    /// a client reaches the other test's server. Holding the socket throughout
    /// closes the window.
    ///
    /// The listener's own address is what is served; `addr` and `port` are
    /// taken from it for logging. Keepalive is applied to it as it is to a
    /// socket this builder binds.
    pub fn from_listener(listener: std::net::TcpListener) -> std::io::Result<Self> {
        let local = listener.local_addr()?;
        Ok(Self::new(&local.ip().to_string(), local.port()).with_listener(listener))
    }

    /// Serves `listener` instead of binding `addr:port`. See
    /// [`TcpServerBuilder::from_listener`].
    pub fn with_listener(mut self, listener: std::net::TcpListener) -> Self {
        self.listener = Some(listener);
        self
    }

    /// The credential exchange every caller must complete before its first
    /// request is served.
    ///
    /// Orthogonal to [`TcpServerBuilder::client_auth`], which asks a different
    /// question — is this machine part of the cluster — and is answered inside
    /// the TLS handshake. A deployment can sensibly use both, one, or (on a
    /// loopback listener) neither.
    pub fn authenticator(mut self, authenticator: Arc<dyn ServerAuthenticator>) -> Self {
        self.authenticator = Some(authenticator);
        self
    }

    /// How long a caller has to finish authenticating. Default 10s.
    pub fn auth_timeout(mut self, timeout: Duration) -> Self {
        self.auth_timeout = timeout;
        self
    }

    /// Also accept TLS 1.2, for a peer that cannot speak 1.3.
    ///
    /// Off by default, and worth leaving off: 1.2 brings back negotiable key
    /// exchange and cipher suites, and with them the downgrade dances 1.3
    /// removed. Turning it on also silently disables channel binding — the
    /// exporter this crate binds an authentication exchange to is a 1.3
    /// construct — so an authenticated server refuses to build with it on.
    pub fn allow_tls12(mut self, allow: bool) -> Self {
        self.allow_tls12 = allow;
        self
    }

    /// Accept TLS 1.3 early data (0-RTT).
    ///
    /// Off by default. Early data is replayable: an attacker who captured a
    /// 0-RTT request can send it again, and the server has no way to tell the
    /// copy from the original. That is tolerable for an idempotent GET and not
    /// for `INSERT`. Turn it on only for a workload where every request that
    /// could arrive first is idempotent.
    pub fn early_data(mut self, enabled: bool) -> Self {
        self.early_data = enabled;
        self
    }

    /// Whether callers must present a certificate, and which are acceptable.
    ///
    /// Anything other than [`ClientAuth::None`] makes the handshake itself the
    /// authorisation check: a caller without an acceptable certificate never
    /// reaches the request handler.
    pub fn client_auth(mut self, client_auth: ClientAuth) -> Self {
        self.client_auth = client_auth;
        self
    }

    pub fn with_certificates(
        mut self,
        certs: Vec<CertificateDer<'static>>,
        key: PrivateKeyDer<'static>,
    ) -> Self {
        self.custom_certs = Some((certs, key));
        self
    }

    pub fn max_connections(mut self, max: usize) -> Self {
        self.max_connections = Some(max);
        self
    }

    pub fn connection_backoff_ms(mut self, ms: u64) -> Self {
        self.connection_backoff_ms = Some(ms);
        self
    }

    pub fn session_cache_size(mut self, size: usize) -> Self {
        self.session_cache_size = size;
        self
    }

    pub fn handshake_timeout(mut self, timeout: Duration) -> Self {
        self.handshake_timeout = timeout;
        self
    }

    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// The largest request this server reads, in bytes. A longer one ends the
    /// connection before any of it is buffered. The default is
    /// [`crate::common::DEFAULT_MAX_MESSAGE_SIZE`].
    pub fn max_message_size(mut self, bytes: usize) -> Self {
        self.max_message_size = bytes;
        self
    }

    /// Lets several servers listen on this address at once (`SO_REUSEPORT`).
    ///
    /// The kernel then deals incoming connections between them. That is how a
    /// process runs one server per core, each on a thread and a
    /// single-threaded runtime of its own, so that a connection never leaves
    /// the core that accepted it; measured on an echo handler that saves 4–11%
    /// of the server's cycles per request against one server on a
    /// work-stealing runtime. Every server sharing the address has to set
    /// this, and it has no effect on a listener passed in already bound.
    ///
    /// Off by default: with it on, a second process started by mistake binds
    /// the same port without complaint and takes a share of the connections.
    pub fn reuse_port(mut self, reuse: bool) -> Self {
        self.reuse_port = reuse;
        self
    }

    /// How many requests a multiplexed connection may have being handled at
    /// once. Default 256.
    ///
    /// A connection at the limit is not read until a request finishes, so a
    /// peer that sends faster than its requests are served is held back by
    /// TCP. A plain (`rastcp/1`) connection handles one request at a time
    /// whatever this is.
    pub fn max_in_flight(mut self, requests: usize) -> Self {
        self.max_in_flight = requests.max(1);
        self
    }

    /// Whether a multiplexed connection starts each handler on its own task
    /// before handing it to the runtime. Off by default.
    ///
    /// Off, every request of a multiplexed connection is given to the runtime
    /// as it arrives, so its requests run on every core at once. That is what
    /// a handler that does real work needs: the connection only moves bytes.
    ///
    /// On, the connection polls each handler once itself, and only one that
    /// has to wait is given to the runtime. A handler that answers without
    /// waiting then costs no task and no allocation — worth having when
    /// handlers are that cheap, as a cache lookup is: an echo is served at a
    /// third of the cost. It is the wrong choice for anything that computes
    /// before it first waits, because that computing is then done by the
    /// connection, one request after another, while the rest of the machine
    /// idles. Measured with the database's point query, 16 callers on one
    /// connection ran at a third of the rate with this on.
    ///
    /// A plain (`rastcp/1`) connection runs its handlers itself whatever
    /// this is: its requests are answered in order, one at a time.
    pub fn inline_handlers(mut self, inline: bool) -> Self {
        self.inline_handlers = inline;
        self
    }

    /// How long a shutdown waits for in-flight connections before closing them
    /// anyway. Past this point a stuck handler would keep the process alive.
    pub fn shutdown_grace(mut self, grace: Duration) -> Self {
        self.shutdown_grace = grace;
        self
    }

    pub async fn build(self) -> Result<TcpServer, RastcpError> {
        self.build_sync()
    }

    /// Builds and binds without being a future.
    ///
    /// Nothing in the build actually awaits — it loads certificates, makes a
    /// socket and binds it — and a caller that is not itself a future needs to
    /// learn whether the bind succeeded. Returning a future only lets that
    /// failure surface later, inside a spawned task, where the operator who
    /// asked for the listener cannot be told about it.
    ///
    /// Must be called from within a Tokio runtime: registering the listener
    /// with the reactor needs one.
    pub fn build_sync(self) -> Result<TcpServer, RastcpError> {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        
        let max_connections = normalize_max_connections(self.max_connections);

        if let Some(requested_max_connections) = self.max_connections {
            if max_connections < requested_max_connections {
                warn!(
                    "Clamping requested max_connections from {} to {} to stay within the process fd budget",
                    requested_max_connections,
                    max_connections
                );
            }
        }

        // Load TLS certificates
        let (certs, private_key) = match self.custom_certs {
            Some(certs_key) => certs_key,
            None => generate_self_signed_cert()
                .map_err(|e| RastcpError::CertificateLoading(e.to_string()))?,
        };

        let certificate_fingerprint = certs
            .first()
            .map(crate::tls::fingerprint)
            .unwrap_or([0u8; 32]);

        if self.allow_tls12 && self.authenticator.is_some() {
            return Err(RastcpError::AuthenticationError(
                "an authenticated server cannot allow TLS 1.2: the channel binding that keeps \
                 the credential exchange from being relayed is a TLS 1.3 exporter"
                    .to_string(),
            ));
        }

        // Create TLS config. TLS 1.3 only unless an operator asked otherwise:
        // see `TcpServerBuilder::allow_tls12`.
        let versions: &[&'static rustls::SupportedProtocolVersion] = if self.allow_tls12 {
            &[&rustls::version::TLS13, &rustls::version::TLS12]
        } else {
            &[&rustls::version::TLS13]
        };
        let builder = ServerConfig::builder_with_protocol_versions(versions);

        let wants_cert = match crate::tls::client_verifier(&self.client_auth)? {
            Some(verifier) => builder.with_client_cert_verifier(verifier),
            None => builder.with_no_client_auth(),
        };
        let mut server_config = wants_cert
            .with_single_cert(certs, private_key)
            .map_err(|e| RastcpError::CertificateLoading(e.to_string()))?;

        // 0 disables 0-RTT entirely; see `TcpServerBuilder::early_data`.
        server_config.max_early_data_size = if self.early_data { 1024 } else { 0 };
        server_config.ticketer = Ticketer::new().unwrap();
        server_config.session_storage = ServerSessionMemoryCache::new(self.session_cache_size);
        // A protocol identifier of our own, rather than claiming to be HTTP.
        // Naming the protocol is what stops a client of *this* server being
        // steered into handing its bytes to some other TLS service that happens
        // to share the certificate, and vice versa.
        // Both versions are offered; a client asks for the one it speaks.
        server_config.alpn_protocols = vec![ALPN_MULTIPLEXED.to_vec(), ALPN_PROTOCOL.to_vec()];
        server_config.cert_compression_cache = Arc::new(CompressionCache::new(64));
        // Half-RTT data goes out before the client's Finished — and so before a
        // client certificate has been verified. Harmless when nobody is being
        // asked to prove anything; a disclosure to an unverified peer when they
        // are.
        server_config.send_half_rtt_data =
            !self.client_auth.authenticates() && self.authenticator.is_none();
        server_config.send_tls13_tickets = 2;

        let listener = match self.listener {
            Some(listener) => Self::adopt(listener)?,
            None => Self::bind_socket(&self.addr, self.port, self.reuse_port)?,
        };
        let tokio_listener = TcpListener::from_std(listener)
            .map_err(|e| RastcpError::SocketBindingError(e.to_string()))?;

        let acceptor = TlsAcceptor::from(Arc::new(server_config));

        info!("Server built to listen on {}:{}", self.addr, self.port);

        Ok(TcpServer {
            listener: tokio_listener,
            acceptor,
            max_connections,
            current_connections: Arc::new(CacheGuard::new(std::sync::atomic::AtomicUsize::new(0))),
            connection_backoff_ms: self.connection_backoff_ms.unwrap_or(100),
            handshake_timeout: self.handshake_timeout,
            request_timeout: self.request_timeout,
            max_message_size: self.max_message_size,
            max_in_flight: self.max_in_flight,
            inline_handlers: self.inline_handlers,
            shutdown_grace: self.shutdown_grace,
            auth_timeout: self.auth_timeout,
            authenticator: self.authenticator,
            stop: StopSignal::new(),
            certificate_fingerprint,
        })
    }

    /// Readies a socket the caller bound for the accept loop: non-blocking, as
    /// the reactor needs, and with the keepalive a socket bound here gets.
    fn adopt(listener: std::net::TcpListener) -> Result<std::net::TcpListener, RastcpError> {
        listener
            .set_nonblocking(true)
            .map_err(|e| RastcpError::SocketBindingError(e.to_string()))?;
        let socket = socket2::SockRef::from(&listener);
        _ = socket.set_tcp_nodelay(true);
        socket
            .set_tcp_keepalive(&TcpKeepalive::new().with_time(Duration::from_secs(7200)))
            .map_err(|e| RastcpError::SocketKeepAliveError(e.to_string()))?;
        Ok(listener)
    }

    /// Binds and listens on `addr:port`.
    fn bind_socket(
        addr: &str,
        port: u16,
        reuse_port: bool,
    ) -> Result<std::net::TcpListener, RastcpError> {
        let ip_addr = IpAddr::from_str(addr).map_err(|_| {
            RastcpError::SocketBindingError(format!("{:?} is not an IP address to bind", addr))
        })?;

        let addr = SocketAddr::from((ip_addr, port));

        let socket = Socket::new_raw(Domain::IPV4, Type::STREAM, Some(Protocol::TCP))
            .map_err(|e| RastcpError::SocketBindingError(e.to_string()))?;

        _ = socket.set_tcp_nodelay(true);
        _ = socket.set_nonblocking(true);
        // SO_REUSEADDR lets the listener rebind while sockets from a previous
        // instance are still draining in TIME_WAIT. Without it, restarting right
        // after a Ctrl+C fails with EADDRINUSE until the kernel expires those
        // sockets (up to 60s). It does not allow a second live listener on the
        // same address - that would need SO_REUSEPORT.
        socket
            .set_reuse_address(true)
            .map_err(|e| RastcpError::SocketBindingError(e.to_string()))?;
        if reuse_port {
            #[cfg(unix)]
            socket
                .set_reuse_port(true)
                .map_err(|e| RastcpError::SocketBindingError(e.to_string()))?;

            #[cfg(not(unix))]
            return Err(RastcpError::SocketBindingError(
                "sharing a port between listeners is not supported on this platform".to_string(),
            ));
        }
        // Not `_ =`: a bind that failed has to reach the caller, or the server
        // reports itself started while holding no port.
        socket
            .bind(&SockAddr::from(addr))
            .map_err(|e| RastcpError::SocketBindingError(format!("{}: {}", addr, e)))?;
        socket
            .listen(i32::MAX)
            .map_err(|e| RastcpError::SocketListeningError(e.to_string()))?;
        let tcp_keep_alive = &TcpKeepalive::new().with_time(Duration::from_secs(7200));
        _ = socket
            .set_tcp_keepalive(tcp_keep_alive)
            .map_err(|e| RastcpError::SocketKeepAliveError(e.to_string()))?;

        Ok(socket.into())
    }
}

impl TcpServer {
    pub async fn new(addr: &str, port: u16) -> Result<Self, RastcpError> {
        TcpServerBuilder::new(addr, port).build().await
    }

    /// SHA-256 of the certificate this server presents.
    ///
    /// What the other nodes pin. Printed at start-up so setting a cluster up
    /// does not need a detour through `openssl`.
    pub fn certificate_fingerprint(&self) -> Fingerprint {
        self.certificate_fingerprint
    }

    /// Hands out a trigger for a graceful shutdown of this server.
    pub fn shutdown_handle(&self) -> ShutdownHandle {
        ShutdownHandle {
            signal: self.stop.clone(),
        }
    }

    /// Asks this server to stop accepting and drain. See [`ShutdownHandle`].
    pub fn shutdown(&self) {
        self.shutdown_handle().shutdown();
    }

    pub fn is_shutting_down(&self) -> bool {
        self.stop.is_stopping()
    }

    pub fn active_connections(&self) -> usize {
        self.current_connections
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// One turn of an accept loop: a connection, a retry, or a shutdown.
    ///
    /// The accept still carries its own timeout so a loop that is not watching
    /// anything else cannot park forever, and the shutdown branch is `biased`
    /// so a pending stop wins over a connection that arrives in the same tick.
    async fn next_connection(&self, shutdown: &mut StopListener) -> AcceptStep {
        tokio::select! {
            biased;

            _ = shutdown.stopped() => AcceptStep::Shutdown,

            accepted = tokio::time::timeout(ACCEPT_POLL_INTERVAL, self.listener.accept()) => {
                match accepted {
                    Ok(Ok((stream, addr))) => AcceptStep::Connection(stream, addr),
                    Ok(Err(e)) => {
                        error!("Failed to accept connection: {}", e);
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        AcceptStep::Retry
                    }
                    // Timeout on accept - just continue
                    Err(_) => AcceptStep::Retry,
                }
            }
        }
    }

    /// Sleeps out a capacity backoff, cutting it short if a shutdown lands.
    /// Returns false when the caller should stop accepting.
    async fn sleep_or_shutdown(
        &self,
        duration: Duration,
        shutdown: &mut StopListener,
    ) -> bool {
        tokio::select! {
            biased;

            _ = shutdown.stopped() => false,
            _ = tokio::time::sleep(duration) => true,
        }
    }

    /// Waits for the connections still being served to finish once the accept
    /// loop has stopped. Handlers close their own sockets when they see the
    /// shutdown, so this is only bounded waiting, and the grace period keeps a
    /// single wedged handler from holding the process open forever.
    async fn drain_connections(&self) {
        let active = self.active_connections();

        if active == 0 {
            info!("Accept loop stopped, no active connections; shutdown complete");
            return;
        }

        info!(
            "Accept loop stopped, draining {} active connection(s) (grace {:?})",
            active, self.shutdown_grace
        );

        let deadline = tokio::time::Instant::now() + self.shutdown_grace;

        loop {
            let active = self.active_connections();

            if active == 0 {
                info!("All connections drained; shutdown complete");
                return;
            }

            if tokio::time::Instant::now() >= deadline {
                warn!(
                    "Shutdown grace period elapsed with {} connection(s) still active; closing anyway",
                    active
                );
                return;
            }

            debug!("Waiting for {} active connection(s) to finish", active);
            tokio::time::sleep(DRAIN_POLL_INTERVAL).await;
        }
    }

    /// The accept loop every `run*` method shares.
    ///
    /// One loop rather than one per handler shape: the capacity backoff, the
    /// shutdown handling and — since authentication was added — the check that
    /// a caller proved who it is all live here, so a new `run*` variant cannot
    /// quietly be written without them.
    async fn accept_loop<S, Fut>(&self, serve: S) -> Result<(), RastcpError>
    where
        S: Fn(TcpStream, StopListener) -> Fut + Send + Sync,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let mut shutdown = self.stop.listen();
        let mut consecutive_max_connections = 0;

        loop {
            if self.is_shutting_down() {
                break;
            }

            let current = self
                .current_connections
                .load(std::sync::atomic::Ordering::Relaxed);
            if current >= self.max_connections {
                consecutive_max_connections += 1;

                // Exponential backoff if consistently at capacity
                let backoff = std::cmp::min(
                    self.connection_backoff_ms * consecutive_max_connections,
                    1000, // Max 1 second backoff
                );

                warn!(
                    "At connection capacity ({}/{}), backing off for {}ms",
                    current, self.max_connections, backoff
                );

                if !self
                    .sleep_or_shutdown(Duration::from_millis(backoff), &mut shutdown)
                    .await
                {
                    break;
                }

                continue;
            } else {
                consecutive_max_connections = 0;
            }

            let (stream, addr) = match self.next_connection(&mut shutdown).await {
                AcceptStep::Connection(stream, addr) => (stream, addr),
                AcceptStep::Retry => continue,
                AcceptStep::Shutdown => break,
            };

            info!("New connection from {}", addr);

            let conn_counter = self.current_connections.clone();
            // Each connection watches the same signal, so a shutdown also ends
            // idle keep-alive sessions instead of waiting out the grace period.
            let connection_shutdown = self.stop.listen();

            // Increment connection counter
            let current = conn_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            debug!("Active connections: {}", current + 1);

            let connection = serve(stream, connection_shutdown);

            // Given back when the task ends, however it ends. A handler that
            // panics unwinds this task, and a count decremented only on the
            // way out of an orderly return would then include that connection
            // for ever: enough panics and the server is at capacity with
            // nobody connected.
            let open = OpenConnection(conn_counter);

            tokio::spawn(async move {
                let _open = open;
                connection.await;
            });
        }

        self.drain_connections().await;

        Ok(())
    }

    /// Serves with a handler that is given the authenticated caller.
    ///
    /// Every other `run*` method is this one with the principal dropped, so
    /// there is exactly one path from an accepted socket to a served request.
    async fn serve_all<H, Fut>(&self, handler: H) -> Result<(), RastcpError>
    where
        H: Fn(Arc<Principal>, Vec<u8>) -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output = Vec<u8>> + Send + 'static,
    {
        let acceptor = self.acceptor.clone();
        let handshake_timeout = self.handshake_timeout;
        let auth_timeout = self.auth_timeout;
        let request_timeout = self.request_timeout;
        let max_message_size = self.max_message_size;
        let max_in_flight = self.max_in_flight;
        let inline_handlers = self.inline_handlers;
        let authenticator = self.authenticator.clone();

        self.accept_loop(move |stream, connection_shutdown| {
            let acceptor = acceptor.clone();
            let handler = handler.clone();
            let authenticator = authenticator.clone();

            async move {
                if let Err(e) = Self::serve_connection(
                    stream,
                    acceptor,
                    handshake_timeout,
                    auth_timeout,
                    request_timeout,
                    max_message_size,
                    max_in_flight,
                    inline_handlers,
                    authenticator,
                    connection_shutdown,
                    handler,
                )
                .await
                {
                    error!("Connection error: {}", e);
                }
            }
        })
        .await
    }

    /// Accepts until the server is asked to shut down, then drains.
    pub async fn run<F, Fut>(&self, handler: F) -> Result<(), RastcpError>
    where
        F: Fn(Vec<u8>) -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output = Vec<u8>> + Send + 'static,
    {
        self.serve_all(move |_principal, data| handler(data)).await
    }

    /// As [`TcpServer::run`], with the caller's identity passed to the handler.
    ///
    /// Without an authenticator configured every request arrives as
    /// [`Principal::anonymous`], which is not a superuser — so a handler that
    /// gates privileged work on the principal refuses it rather than allowing
    /// it when authentication is switched off.
    pub async fn run_with_principal<F, Fut>(&self, handler: F) -> Result<(), RastcpError>
    where
        F: Fn(Arc<Principal>, Vec<u8>) -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output = Vec<u8>> + Send + 'static,
    {
        self.serve_all(handler).await
    }

    // New method that accepts a context object which is passed to each handler call
    pub async fn run_with_context<A, F, Fut>(
        &self,
        context: A,
        handler: F,
    ) -> Result<(), RastcpError>
    where
        A: Clone + Send + Sync + 'static,
        F: Fn(A, Vec<u8>) -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output = Vec<u8>> + Send + 'static,
    {
        self.serve_all(move |_principal, data| handler(context.clone(), data))
            .await
    }

    /// [`TcpServer::run_with_context`] plus the caller's identity.
    pub async fn run_with_context_and_principal<A, F, Fut>(
        &self,
        context: A,
        handler: F,
    ) -> Result<(), RastcpError>
    where
        A: Clone + Send + Sync + 'static,
        F: Fn(A, Arc<Principal>, Vec<u8>) -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output = Vec<u8>> + Send + 'static,
    {
        self.serve_all(move |principal, data| handler(context.clone(), principal, data))
            .await
    }

    // New method that accepts a mutable reference to a context through Arc<xutex::AsyncMutex<>>
    pub async fn run_with_shared_context<A, F, Fut>(
        &self,
        context: Arc<xutex::AsyncMutex<A>>,
        handler: F,
    ) -> Result<(), RastcpError>
    where
        A: Send + Sync + 'static,
        F: Fn(Arc<xutex::AsyncMutex<A>>, Vec<u8>) -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output = Vec<u8>> + Send + 'static,
    {
        self.serve_all(move |_principal, data| handler(context.clone(), data))
            .await
    }

    /// Runs the credential exchange, if one was configured.
    ///
    /// Returns [`Principal::anonymous`] when it was not, so a server without an
    /// authenticator behaves exactly as it did before — but a server *with* one
    /// cannot reach the request loop without a principal, because the only way
    /// past this function is a principal or an error.
    async fn authenticate_connection(
        tls_stream: &mut tokio_rustls::server::TlsStream<TcpStream>,
        authenticator: Option<&Arc<dyn ServerAuthenticator>>,
        auth_timeout: Duration,
        peer_addr: SocketAddr,
    ) -> Result<Principal, RastcpError> {
        let Some(authenticator) = authenticator else {
            return Ok(Principal::anonymous());
        };

        // Derived before the stream is borrowed mutably, and derived at all
        // because an exchange that cannot bind itself to this connection must
        // fail rather than run unbound.
        let binding = crate::session::channel_binding_from(tls_stream.get_ref().1)?;

        let mut channel = AuthChannel::new(tls_stream, auth_timeout, binding, peer_addr);

        // The whole exchange is bounded, not just each message: a peer that
        // answers every message just inside the per-message timeout could
        // otherwise hold a connection slot indefinitely.
        match tokio::time::timeout(auth_timeout, authenticator.authenticate(&mut channel)).await {
            Ok(result) => result,
            Err(_) => Err(RastcpError::AuthenticationError(format!(
                "{} did not finish authenticating within {:?}",
                peer_addr, auth_timeout
            ))),
        }
    }

    /// Writes out the replies that are waiting, under the connection's deadline.
    async fn send_replies(
        guard: &mut Guard,
        writer: &mut FrameWriter,
        tls_stream: &mut ServerTls,
    ) -> Result<(), RastcpError> {
        if writer.is_empty() {
            return Ok(());
        }

        guard
            .run(writer.flush(tls_stream), false)
            .await
            .unwrap_or(Err(RastcpError::ConnectionTimeout))
    }

    /// Records why a connection stopped yielding requests, at the level each
    /// reason deserves: a peer hanging up is ordinary, a malformed stream is
    /// not.
    fn log_read_end(peer_addr: SocketAddr, e: RastcpError) {
        match e {
            RastcpError::ConnectionClosed => {
                info!("Connection closed by peer: {}", peer_addr);
            }
            RastcpError::Io(e) if e.kind() == std::io::ErrorKind::ConnectionReset => {
                info!("Connection reset by peer: {}", peer_addr);
            }
            RastcpError::Tls(e) => {
                warn!("TLS error from {}: {}", peer_addr, e);
            }
            e => {
                error!("Error reading from {}: {}", peer_addr, e);
            }
        }
    }

    /// The request loop of a plain (`rastcp/1`) connection: requests are
    /// handled one at a time, and answered in the order they arrived.
    async fn serve_in_order<H, Fut>(
        tls_stream: &mut ServerTls,
        guard: &mut Guard,
        peer_addr: SocketAddr,
        principal: Arc<Principal>,
        handler: &H,
        max_message_size: usize,
    ) where
        H: Fn(Arc<Principal>, Vec<u8>) -> Fut + Send + Sync,
        Fut: Future<Output = Vec<u8>> + Send,
    {
        let mut reader = FrameReader::new();
        let mut writer = FrameWriter::new();

        'connection: loop {
            // Nothing is owed to the peer at this point, so this is where the
            // connection may sit idle, and where a shutdown ends it: the peer
            // gets a close_notify and a clean EOF, instead of a socket that
            // dies under it and leaves it stuck in CLOSE_WAIT.
            let mut next = match guard
                .run(reader.frame(tls_stream, max_message_size), true)
                .await
            {
                Ok(next) => next,
                Err(Interrupted::Stopping) => {
                    debug!("Shutting down, closing connection to {}", peer_addr);
                    break;
                }
                Err(Interrupted::TimedOut) => Err(RastcpError::ConnectionTimeout),
            };

            // Then every request that has already arrived is served before
            // anything is written back, so a peer that sent several gets them
            // answered with one write. They are still handled one at a time
            // and in order: the replies carry nothing to match them to their
            // requests but their position.
            let mut served = 0usize;

            loop {
                let data = match next {
                    Ok(data) => data,
                    Err(e) => {
                        // The requests before this one were served, and their
                        // replies are still owed.
                        if let Err(e) = Self::send_replies(guard, &mut writer, tls_stream).await {
                            debug!("Failed to write response to {}: {}", peer_addr, e);
                        }
                        Self::log_read_end(peer_addr, e);
                        break 'connection;
                    }
                };

                debug!("Received {} bytes from {}", data.len(), peer_addr);

                // Not stoppable from here on: a request that was read is
                // answered, which is what lets a shutdown drain.
                let mut call = std::pin::pin!(handler(principal.clone(), data));

                let response = match poll_once(call.as_mut()).await {
                    Some(response) => response,
                    None => {
                        // This one has to wait for something. The replies
                        // already made do not wait with it.
                        if let Err(e) = Self::send_replies(guard, &mut writer, tls_stream).await {
                            error!("Failed to write response to {}: {}", peer_addr, e);
                            break 'connection;
                        }

                        match guard.run(call, false).await {
                            Ok(response) => response,
                            Err(_) => {
                                error!("Handler timed out for {}", peer_addr);
                                break 'connection;
                            }
                        }
                    }
                };

                debug!("Replying with {} bytes to {}", response.len(), peer_addr);

                let queued = match writer.queue(&response) {
                    Ok(true) if !writer.is_full() => Ok(()),
                    Ok(true) => Self::send_replies(guard, &mut writer, tls_stream).await,
                    Ok(false) => guard
                        .run(writer.send_large(tls_stream, &response), false)
                        .await
                        .unwrap_or(Err(RastcpError::ConnectionTimeout)),
                    Err(e) => Err(e),
                };

                if let Err(e) = queued {
                    error!("Failed to write response to {}: {}", peer_addr, e);
                    break 'connection;
                }

                // A stopping server finishes what it has read and reads no
                // more; the wait at the top of the outer loop closes.
                if guard.is_stopping() {
                    break;
                }

                served += 1;

                // Asking the connection for more costs a trip through the TLS
                // stream and the reactor even when the answer is no, and for a
                // peer that sends one request and waits it always is no. So
                // the question is put only to a peer that has shown it sends
                // ahead: more bytes are already here, or this batch is
                // already more than one request.
                if !reader.has_buffered() && served < 2 {
                    break;
                }

                match reader.frame_now(tls_stream, max_message_size).await {
                    Some(frame) => next = frame,
                    None => break,
                }
            }

            if let Err(e) = Self::send_replies(guard, &mut writer, tls_stream).await {
                error!("Failed to write response to {}: {}", peer_addr, e);
                break;
            }
        }
    }

    /// Queues a reply on a multiplexed connection, or writes it at once if it
    /// is too large to be worth copying.
    async fn reply(
        guard: &mut Guard,
        writer: &mut FrameWriter,
        tls_stream: &mut ServerTls,
        tag: Tag,
        payload: &[u8],
    ) -> Result<(), RastcpError> {
        if writer.queue_tagged(tag, payload)? {
            return Ok(());
        }

        guard
            .run(writer.send_large_tagged(tls_stream, tag, payload), false)
            .await
            .unwrap_or(Err(RastcpError::ConnectionTimeout))
    }

    /// The next thing a multiplexed connection has to deal with: a handler
    /// that finished, or — when `reading` — a request that arrived.
    ///
    /// Finished handlers first: a reply that is ready is a caller waiting.
    fn poll_event(
        reader: &mut FrameReader,
        tls_stream: &mut ServerTls,
        in_flight: &mut InFlight,
        cx: &mut std::task::Context<'_>,
        reading: bool,
        max_message_size: usize,
    ) -> std::task::Poll<Event> {
        if !in_flight.is_empty() {
            if let std::task::Poll::Ready(Some(done)) = in_flight.poll_join_next_with_id(cx) {
                return std::task::Poll::Ready(Event::Done(done));
            }
        }

        if reading {
            if let std::task::Poll::Ready(frame) = reader.poll_tagged(tls_stream, cx, max_message_size) {
                return std::task::Poll::Ready(Event::Frame(frame));
            }
        }

        std::task::Poll::Pending
    }

    /// The request loop of a multiplexed (`rastcp/2`) connection.
    ///
    /// Every frame carries an id the client chose and the reply carries it
    /// back, so nothing here has to happen in order. Each request is handed to
    /// the runtime as a task of its own and the connection goes on reading;
    /// a reply is written when it is ready, whatever else was answered in
    /// between. That is what lets one connection carry many callers, and lets
    /// their requests run on every core instead of queueing behind each other.
    ///
    /// With `inline_handlers` the connection polls each handler once first
    /// and answers on the spot if that finished it; see
    /// [`TcpServerBuilder::inline_handlers`] for when that is and is not a
    /// good idea.
    #[allow(clippy::too_many_arguments)]
    async fn serve_multiplexed<H, Fut>(
        tls_stream: &mut ServerTls,
        guard: &mut Guard,
        peer_addr: SocketAddr,
        principal: Arc<Principal>,
        handler: &H,
        max_message_size: usize,
        request_timeout: Duration,
        max_in_flight: usize,
        inline_handlers: bool,
    ) where
        H: Fn(Arc<Principal>, Vec<u8>) -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output = Vec<u8>> + Send + 'static,
    {
        let mut reader = FrameReader::tagged();
        let mut writer = FrameWriter::new();
        let mut in_flight = InFlight(JoinSet::new());
        // Which request each task is handling, for the one case where the
        // task cannot say so itself: it panicked.
        let mut ids: std::collections::HashMap<tokio::task::Id, u32> =
            std::collections::HashMap::new();
        // Where, with `inline_handlers`, a handler is polled for the first
        // time. One that finishes there leaves the cell to be used again, so
        // it costs no allocation; one that does not takes the cell with it to
        // its task.
        let mut cell: std::pin::Pin<Box<Option<Fut>>> = Box::pin(None);
        // Set once the server is stopping: no more requests are read, the
        // ones being handled are answered, and then the connection closes.
        let mut draining = false;

        'connection: loop {
            if draining && in_flight.is_empty() {
                debug!("Shutting down, closing connection to {}", peer_addr);
                break;
            }

            let reading = !draining && in_flight.len() < max_in_flight;
            let idle = in_flight.is_empty();
            let wait = std::future::poll_fn(|cx| {
                Self::poll_event(&mut reader, tls_stream, &mut in_flight, cx, reading, max_message_size)
            });

            // An idle connection is closed after the timeout, as a plain one
            // is. A connection with requests in flight is not idle however
            // long they take; each of them has the timeout to itself.
            let mut event = if idle {
                match guard.run(wait, true).await {
                    Ok(event) => event,
                    Err(Interrupted::Stopping) => {
                        draining = true;
                        continue;
                    }
                    Err(Interrupted::TimedOut) => Event::Frame(Err(RastcpError::ConnectionTimeout)),
                }
            } else {
                match guard.run_unbounded(wait, !draining).await {
                    Some(event) => event,
                    None => {
                        draining = true;
                        continue;
                    }
                }
            };

            // Then everything else that is ready, before anything is written.
            let mut handled = 0usize;

            loop {
                let replied = match event {
                    Event::Frame(Err(e)) => {
                        // Replies already made are still owed.
                        if let Err(e) = Self::send_replies(guard, &mut writer, tls_stream).await {
                            debug!("Failed to write response to {}: {}", peer_addr, e);
                        }
                        Self::log_read_end(peer_addr, e);
                        break 'connection;
                    }
                    Event::Frame(Ok((tag, _))) if tag.kind != KIND_DATA => {
                        if let Err(e) = Self::send_replies(guard, &mut writer, tls_stream).await {
                            debug!("Failed to write response to {}: {}", peer_addr, e);
                        }
                        warn!("{} sent a frame of kind {} as a request", peer_addr, tag.kind);
                        break 'connection;
                    }
                    Event::Frame(Ok((tag, data))) if !inline_handlers => {
                        debug!("Received {} bytes from {}", data.len(), peer_addr);

                        let handler = handler.clone();
                        let principal = principal.clone();
                        let id = tag.id;
                        let task = in_flight.spawn(async move {
                            // Made here, in the task, and boxed: a handler's
                            // future can run to tens of kilobytes, and built
                            // on the connection it would be copied on its way
                            // into the task.
                            let call = Box::pin(handler(principal, data));
                            (id, tokio::time::timeout(request_timeout, call).await.ok())
                        });
                        ids.insert(task.id(), id);
                        Ok(())
                    }
                    Event::Frame(Ok((tag, data))) => {
                        debug!("Received {} bytes from {}", data.len(), peer_addr);

                        cell.set(Some(handler(principal.clone(), data)));
                        let first = match cell.as_mut().as_pin_mut() {
                            Some(call) => poll_once(call).await,
                            None => None,
                        };

                        match first {
                            Some(response) => {
                                cell.set(None);
                                Self::reply(guard, &mut writer, tls_stream, tag, &response).await
                            }
                            None => {
                                let mut call = std::mem::replace(&mut cell, Box::pin(None));
                                let id = tag.id;
                                let task = in_flight.spawn(async move {
                                    let run = std::future::poll_fn(|cx| match call.as_mut().as_pin_mut() {
                                        Some(call) => call.poll(cx),
                                        None => std::task::Poll::Pending,
                                    });
                                    (id, tokio::time::timeout(request_timeout, run).await.ok())
                                });
                                ids.insert(task.id(), id);
                                Ok(())
                            }
                        }
                    }
                    Event::Done(Ok((task, (id, Some(response))))) => {
                        ids.remove(&task);
                        debug!("Replying with {} bytes to {}", response.len(), peer_addr);
                        Self::reply(guard, &mut writer, tls_stream, Tag::data(id), &response).await
                    }
                    Event::Done(Ok((task, (id, None)))) => {
                        ids.remove(&task);
                        error!("Handler timed out for {}", peer_addr);
                        Self::reply(guard, &mut writer, tls_stream, Tag { id, kind: KIND_TIMED_OUT }, &[])
                            .await
                    }
                    Event::Done(Err(failure)) => match ids.remove(&failure.id()) {
                        // The request is answered, so its caller is not left
                        // waiting, and the connection's other requests are
                        // not made to pay for this one.
                        Some(id) => {
                            error!("Handler failed for {}: {}", peer_addr, failure);
                            let tag = Tag { id, kind: KIND_FAILED };
                            Self::reply(guard, &mut writer, tls_stream, tag, b"the handler panicked").await
                        }
                        None => Ok(()),
                    },
                };

                if let Err(e) = replied {
                    error!("Failed to write response to {}: {}", peer_addr, e);
                    break 'connection;
                }

                handled += 1;

                if writer.is_full() {
                    if let Err(e) = Self::send_replies(guard, &mut writer, tls_stream).await {
                        error!("Failed to write response to {}: {}", peer_addr, e);
                        break 'connection;
                    }
                }

                // Handlers that have finished cost nothing to ask about.
                if let Some(done) = in_flight.try_join_next_with_id() {
                    event = Event::Done(done);
                    continue;
                }

                if guard.is_stopping() {
                    draining = true;
                }

                // Asking the connection for more is not free; see the same
                // test in `serve_in_order`.
                let reading = !draining && in_flight.len() < max_in_flight;
                if !reading || (handled < 2 && !reader.has_buffered()) {
                    break;
                }

                let more = std::future::poll_fn(|cx| {
                    std::task::Poll::Ready(match reader.poll_tagged(tls_stream, cx, max_message_size) {
                        std::task::Poll::Ready(frame) => Some(frame),
                        std::task::Poll::Pending => None,
                    })
                })
                .await;

                match more {
                    Some(frame) => event = Event::Frame(frame),
                    None => break,
                }
            }

            if let Err(e) = Self::send_replies(guard, &mut writer, tls_stream).await {
                error!("Failed to write response to {}: {}", peer_addr, e);
                break;
            }
        }

    }

    /// One accepted socket, from handshake to close.
    async fn serve_connection<H, Fut>(
        stream: TcpStream,
        acceptor: TlsAcceptor,
        handshake_timeout: Duration,
        auth_timeout: Duration,
        request_timeout: Duration,
        max_message_size: usize,
        max_in_flight: usize,
        inline_handlers: bool,
        authenticator: Option<Arc<dyn ServerAuthenticator>>,
        shutdown: StopListener,
        handler: H,
    ) -> Result<(), RastcpError>
    where
        H: Fn(Arc<Principal>, Vec<u8>) -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output = Vec<u8>> + Send + 'static,
    {
        // Disable Nagle's algorithm on the accepted connection. Linux does
        // NOT inherit TCP_NODELAY from the listener, so without this an
        // idle reused connection waits ~40 ms for the delayed-ACK timer
        // before the server's response is flushed.
        let _ = stream.set_nodelay(true);

        let peer_addr = match stream.peer_addr() {
            Ok(addr) => addr,
            Err(e) => {
                error!("Failed to get peer address: {}", e);
                return Err(RastcpError::Io(e));
            }
        };
        debug!("Accepting TLS connection from {}", peer_addr);

        // Apply timeout to TLS handshake to prevent hanging connections
        let mut tls_stream = match tokio::time::timeout(handshake_timeout, acceptor.accept(stream))
            .await
        {
            Ok(Ok(tls_stream)) => tls_stream,
            Ok(Err(e)) => {
                error!("TLS handshake failed with {}: {}", peer_addr, e);
                return Ok(());
            }
            Err(_) => {
                error!("TLS handshake timed out with {}", peer_addr);
                return Ok(());
            }
        };
        debug!("TLS handshake completed with {}", peer_addr);

        let principal = match Self::authenticate_connection(
            &mut tls_stream,
            authenticator.as_ref(),
            auth_timeout,
            peer_addr,
        )
        .await
        {
            Ok(principal) => Arc::new(principal),
            Err(e) => {
                // At info, not error: a refused credential is an ordinary
                // event on a public listener, and logging it loudly is how a
                // log becomes noise nobody reads.
                info!("Refused {}: {}", peer_addr, e);
                // The peer is told by the exchange itself, which has already
                // written whatever it wants to say; all that is left is to
                // close cleanly instead of dropping the socket.
                let _ = tls_stream.shutdown().await;
                return Ok(());
            }
        };

        if !principal.is_anonymous() {
            debug!("{} authenticated as {}", peer_addr, principal);
        }

        // One stop listener and one timer for the life of the connection, in
        // place of a timer per read, per handler call and per write.
        let mut guard = Guard::new(shutdown, request_timeout);

        // The handshake settled which protocol this connection speaks.
        if tls_stream.get_ref().1.alpn_protocol() == Some(ALPN_MULTIPLEXED) {
            Self::serve_multiplexed(
                &mut tls_stream,
                &mut guard,
                peer_addr,
                principal,
                &handler,
                max_message_size,
                request_timeout,
                max_in_flight,
                inline_handlers,
            )
            .await;
        } else {
            Self::serve_in_order(
                &mut tls_stream,
                &mut guard,
                peer_addr,
                principal,
                &handler,
                max_message_size,
            )
            .await;
        }

        // Explicit shutdown to ensure clean TLS termination
        if let Err(e) = tls_stream.shutdown().await {
            debug!("TLS shutdown error with {}: {}", peer_addr, e);
        }

        debug!("Connection handler completed for {}", peer_addr);
        Ok(())
    }
}

/// One connection's share of the server's connection count.
struct OpenConnection(Arc<CacheGuard<std::sync::atomic::AtomicUsize>>);

impl Drop for OpenConnection {
    fn drop(&mut self) {
        let remaining = self.0.fetch_sub(1, std::sync::atomic::Ordering::Relaxed) - 1;
        debug!("Connection closed. Active connections: {}", remaining);
    }
}

/// A server-side TLS connection.
type ServerTls = tokio_rustls::server::TlsStream<TcpStream>;

/// The handlers of one multiplexed connection that are running as tasks: each
/// yields the id of its request and its reply, or `None` if it ran out of
/// time.
///
/// Dropping a `JoinSet` cancels its tasks, each wherever it happened to be
/// waiting. This lets them go instead, on every way out of the connection —
/// a lost peer, an error, a panic — so a request that was begun is finished,
/// as the handler of a plain connection is finished before a lost peer is
/// even noticed.
struct InFlight(JoinSet<(u32, Option<Vec<u8>>)>);

impl Drop for InFlight {
    fn drop(&mut self) {
        self.0.detach_all();
    }
}

impl std::ops::Deref for InFlight {
    type Target = JoinSet<(u32, Option<Vec<u8>>)>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for InFlight {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// What a multiplexed connection wakes up for.
enum Event {
    /// A request, or the reason there will be no more.
    Frame(Result<(Tag, Vec<u8>), RastcpError>),
    /// A handler finished, or panicked.
    Done(Result<(tokio::task::Id, (u32, Option<Vec<u8>>)), tokio::task::JoinError>),
}

/// Polls `future` once: its output if it had one ready, `None` if it has to
/// wait. The task is left registered with whatever the future is waiting for,
/// so polling it again later loses nothing.
async fn poll_once<F: Future>(mut future: std::pin::Pin<&mut F>) -> Option<F::Output> {
    std::future::poll_fn(|cx| {
        std::task::Poll::Ready(match future.as_mut().poll(cx) {
            std::task::Poll::Ready(output) => Some(output),
            std::task::Poll::Pending => None,
        })
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_max_connections_caps_requested_limit_to_budget() {
        let max_connections = normalize_max_connections_with_budget(Some(5_000), 64);

        assert_eq!(max_connections, 64);
    }

    #[test]
    fn normalize_max_connections_uses_default_when_unset() {
        let max_connections = normalize_max_connections_with_budget(None, 2_048);

        assert_eq!(max_connections, DEFAULT_MAX_CONNECTIONS);
    }
}
