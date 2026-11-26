//! Listener plumbing shared by the protocols served beside SQL: the Redis
//! protocol (`kv_cache`) and the broker's native, Kafka, AMQP and MQTT ports.
//!
//! - [`bind`] binds a TCP port, a unix socket, or both. The socket goes
//!   first, because it is the one most likely to be refused.
//! - [`Sock`] is a connection over TCP, TLS over TCP, or a unix socket, as
//!   one `AsyncRead + AsyncWrite`.
//! - [`serve_per_core`] runs connections on one I/O thread per core. Each
//!   thread has its own current-thread runtime, and the accept loop deals
//!   connections out round-robin (see `kv_cache::resp::serve` for why that
//!   beats a work-stealing runtime here).

use std::io;

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

/// How long a TLS client has to finish its handshake, from accept.
pub const TLS_HANDSHAKE: std::time::Duration = std::time::Duration::from_secs(10);

/// Where and how a protocol is served on a unix socket.
#[derive(Clone, Debug)]
pub struct SocketSpec {
    pub path: std::path::PathBuf,
    /// The file's permissions; connecting needs write permission.
    pub mode: u32,
    /// The group that owns the file, so that its members may connect.
    pub group: Option<u32>,
}

impl SocketSpec {
    /// Whether users outside the file's owner and group can connect.
    pub fn admits_others(&self) -> bool {
        self.mode & 0o007 != 0
    }
}

/// What [`bind`] bound: a TCP listener, a unix socket, or both.
pub struct Listeners {
    pub(crate) tcp: Option<TcpListener>,
    #[cfg(unix)]
    pub(crate) unix: Option<(tokio::net::UnixListener, rastcp::net::SocketFile)>,
}

impl Listeners {
    /// The TCP port actually bound, if any — the kernel's choice when `0`
    /// was asked for.
    pub fn tcp_port(&self) -> Option<u16> {
        self.tcp.as_ref().and_then(|l| l.local_addr().ok()).map(|a| a.port())
    }

    pub fn has_tcp(&self) -> bool {
        self.tcp.is_some()
    }

    /// The unix socket's path, if one is bound.
    pub fn socket_path(&self) -> Option<std::path::PathBuf> {
        #[cfg(unix)]
        {
            self.unix.as_ref().map(|(_, f)| f.path().to_path_buf())
        }
        #[cfg(not(unix))]
        {
            None
        }
    }

    /// The next connection from either listener.
    pub async fn accept(&self) -> io::Result<(StdSock, String)> {
        #[cfg(unix)]
        {
            tokio::select! {
                accepted = accept_tcp(self.tcp.as_ref()) => accepted,
                accepted = accept_unix(self.unix.as_ref()) => accepted,
            }
        }
        #[cfg(not(unix))]
        {
            accept_tcp(self.tcp.as_ref()).await
        }
    }
}

/// Whether `addr` is a loopback address (or `localhost`): where an
/// unauthenticated listener may bind.
pub fn is_loopback(addr: &str) -> bool {
    addr.parse::<std::net::IpAddr>().map(|ip| ip.is_loopback()).unwrap_or(addr == "localhost")
}

/// Binds `port` on `addr` and/or the unix `socket`.
pub async fn bind(addr: &str, port: Option<u16>, socket: Option<&SocketSpec>) -> io::Result<Listeners> {
    // The socket first: it is the one most likely to be refused (a live
    // server on the path, a file that is not a socket), and refusing it should
    // not leave a TCP port bound behind.
    #[cfg(unix)]
    let unix = match socket {
        Some(socket) => {
            let (listener, file) =
                rastcp::net::SocketFile::bind(&socket.path, socket.mode, socket.group)
                    .map_err(|e| io::Error::other(e.to_string()))?;
            Some((tokio::net::UnixListener::from_std(listener)?, file))
        }
        None => None,
    };
    #[cfg(not(unix))]
    if socket.is_some() {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "unix sockets are not supported on this platform",
        ));
    }

    let tcp = match port {
        Some(port) => Some(TcpListener::bind((addr, port)).await?),
        None => None,
    };

    Ok(Listeners {
        tcp,
        #[cfg(unix)]
        unix,
    })
}

/// A connection on its way to the I/O thread that will own it, out of the
/// accepting runtime's reactor.
pub enum StdSock {
    Tcp(std::net::TcpStream),
    #[cfg(unix)]
    Unix(std::os::unix::net::UnixStream),
}

/// A client connection: TCP, TLS over TCP, or a unix socket.
pub enum Sock {
    Tcp(TcpStream),
    /// Boxed: the session state is a few kilobytes, and the plain variants
    /// are a descriptor.
    Tls(Box<tokio_rustls::server::TlsStream<TcpStream>>),
    #[cfg(unix)]
    Unix(tokio::net::UnixStream),
}

impl Sock {
    pub fn from_std(socket: StdSock) -> io::Result<Sock> {
        Ok(match socket {
            StdSock::Tcp(socket) => Sock::Tcp(TcpStream::from_std(socket)?),
            #[cfg(unix)]
            StdSock::Unix(socket) => Sock::Unix(tokio::net::UnixStream::from_std(socket)?),
        })
    }

    pub async fn readable(&self) -> io::Result<()> {
        match self {
            Sock::Tcp(sock) => sock.readable().await,
            // The TCP socket under the session: bytes there are a record
            // (the client's next command, or its close) or the end.
            Sock::Tls(tls) => tls.get_ref().0.readable().await,
            #[cfg(unix)]
            Sock::Unix(sock) => sock.readable().await,
        }
    }

    /// Looks at the next byte without taking it and without waiting for
    /// one: `Some(0)` when the client has closed its end, `Some(1)` when a
    /// byte is there, `None` when nothing is (a readiness that promised data
    /// it did not have, which Windows' emulated readiness gives). An awaited
    /// `peek` used to sit there instead: a client parked in BLPOP stopped
    /// answering, past its wake-up and its own timeout, until its next byte.
    /// Seeing nothing clears the readiness, so the next `readable` waits.
    ///
    /// Under TLS this looks at the TCP socket: a client that closes sends a
    /// `close_notify` record before its FIN, which reads as data, so a closed
    /// TLS client parked in a blocking pop is noticed at its timeout or at
    /// its wake-up rather than at once.
    pub fn try_peek_byte(&self) -> Option<io::Result<usize>> {
        let mut byte = [0u8; 1];
        let tcp = match self {
            Sock::Tcp(sock) => Some(sock),
            Sock::Tls(tls) => Some(tls.get_ref().0),
            #[cfg(unix)]
            Sock::Unix(_) => None,
        };
        match self {
            Sock::Tcp(_) | Sock::Tls(_) => {
                let sock = tcp.expect("a TCP socket");
                let waker = std::task::Waker::noop();
                let mut cx = std::task::Context::from_waker(waker);
                let mut buf = tokio::io::ReadBuf::new(&mut byte);
                match sock.poll_peek(&mut cx, &mut buf) {
                    std::task::Poll::Ready(r) => Some(r),
                    std::task::Poll::Pending => None,
                }
            }
            // tokio's unix stream has no `peek`; the socket has, through
            // `recv` with MSG_PEEK on its descriptor.
            #[cfg(unix)]
            Sock::Unix(sock) => {
                use std::os::fd::AsRawFd;
                let peeked = sock.try_io(tokio::io::Interest::READABLE, || {
                    // SAFETY: a one-byte buffer owned here, and a
                    // descriptor the stream keeps open for the call.
                    let n = unsafe {
                        libc::recv(
                            sock.as_raw_fd(),
                            byte.as_mut_ptr().cast(),
                            1,
                            libc::MSG_PEEK | libc::MSG_DONTWAIT,
                        )
                    };
                    if n < 0 {
                        Err(io::Error::last_os_error())
                    } else {
                        Ok(n as usize)
                    }
                });
                match peeked {
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => None,
                    r => Some(r),
                }
            }
        }
    }
}

impl tokio::io::AsyncRead for Sock {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        match self.get_mut() {
            Sock::Tcp(sock) => std::pin::Pin::new(sock).poll_read(cx, buf),
            Sock::Tls(tls) => std::pin::Pin::new(tls.as_mut()).poll_read(cx, buf),
            #[cfg(unix)]
            Sock::Unix(sock) => std::pin::Pin::new(sock).poll_read(cx, buf),
        }
    }
}

impl tokio::io::AsyncWrite for Sock {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<io::Result<usize>> {
        match self.get_mut() {
            Sock::Tcp(sock) => std::pin::Pin::new(sock).poll_write(cx, buf),
            Sock::Tls(tls) => std::pin::Pin::new(tls.as_mut()).poll_write(cx, buf),
            #[cfg(unix)]
            Sock::Unix(sock) => std::pin::Pin::new(sock).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        match self.get_mut() {
            Sock::Tcp(sock) => std::pin::Pin::new(sock).poll_flush(cx),
            Sock::Tls(tls) => std::pin::Pin::new(tls.as_mut()).poll_flush(cx),
            #[cfg(unix)]
            Sock::Unix(sock) => std::pin::Pin::new(sock).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        match self.get_mut() {
            Sock::Tcp(sock) => std::pin::Pin::new(sock).poll_shutdown(cx),
            Sock::Tls(tls) => std::pin::Pin::new(tls.as_mut()).poll_shutdown(cx),
            #[cfg(unix)]
            Sock::Unix(sock) => std::pin::Pin::new(sock).poll_shutdown(cx),
        }
    }
}

/// The next connection on `listener`, or never if there is none.
pub async fn accept_tcp(listener: Option<&TcpListener>) -> io::Result<(StdSock, String)> {
    let Some(listener) = listener else {
        return std::future::pending().await;
    };
    let (socket, peer) = listener.accept().await?;
    let _ = socket.set_nodelay(true);
    // Leaves this runtime's epoll; the worker registers it with its own.
    Ok((StdSock::Tcp(socket.into_std()?), peer.to_string()))
}

#[cfg(unix)]
pub async fn accept_unix(
    listener: Option<&(tokio::net::UnixListener, rastcp::net::SocketFile)>,
) -> io::Result<(StdSock, String)> {
    let Some((listener, file)) = listener else {
        return std::future::pending().await;
    };
    let (socket, _) = listener.accept().await?;
    // Named as Redis names a unix client in CLIENT LIST: the path, port 0.
    Ok((StdSock::Unix(socket.into_std()?), format!("{}:0", file.path().display())))
}


/// A TLS handshake on a fresh TCP connection, bounded by [`TLS_HANDSHAKE`].
/// `None` (logged under `what`) when the client failed or took too long; any
/// other connection is returned as it came.
pub async fn tls_accept(
    socket: Sock,
    tls: Option<&tokio_rustls::TlsAcceptor>,
    peer: &str,
    what: &str,
) -> Option<Sock> {
    match (socket, tls) {
        (Sock::Tcp(tcp), Some(tls)) => match tokio::time::timeout(TLS_HANDSHAKE, tls.accept(tcp)).await {
            Ok(Ok(session)) => Some(Sock::Tls(Box::new(session))),
            Ok(Err(e)) => {
                log::warn!("{what}: TLS handshake with {peer} failed: {e}");
                None
            }
            Err(_) => {
                log::warn!("{what}: {peer} did not finish its TLS handshake within {TLS_HANDSHAKE:?}");
                None
            }
        },
        (socket, _) => Some(socket),
    }
}

/// Accepts connections until a listener fails, running each on one of
/// `threads` I/O threads named `<name>-io-<n>`. `handler` is called on the
/// connection's own thread, inside its runtime.
pub async fn serve_per_core<F, Fut>(listeners: Listeners, name: &str, threads: usize, handler: F) -> io::Result<()>
where
    F: Fn(Sock, String) -> Fut + Send + Sync + Clone + 'static,
    Fut: std::future::Future<Output = ()> + 'static,
{
    let threads = threads.max(1);
    let mut workers = Vec::with_capacity(threads);
    for i in 0..threads {
        let (tx, mut rx) = mpsc::unbounded_channel::<(StdSock, String)>();
        let handler = handler.clone();
        let label = name.to_string();
        std::thread::Builder::new().name(format!("{name}-io-{i}")).spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
                Ok(rt) => rt,
                Err(e) => {
                    log::error!("{label}: I/O thread {i} could not start: {e}");
                    return;
                }
            };
            let local = tokio::task::LocalSet::new();
            local.block_on(&runtime, async move {
                while let Some((socket, peer)) = rx.recv().await {
                    let Ok(socket) = Sock::from_std(socket) else { continue };
                    tokio::task::spawn_local(handler(socket, peer));
                }
            });
        })?;
        workers.push(tx);
    }
    let mut next = 0usize;
    loop {
        let (socket, peer) = listeners.accept().await?;
        if let Err(refused) = workers[next % threads].send((socket, peer)) {
            log::error!("{name}: I/O thread {} is gone; refusing a connection from {}", next % threads, refused.0.1);
        }
        next = next.wrapping_add(1);
    }
}

/// One I/O thread per CPU this process may run on, unless `threads` says.
pub fn default_threads(threads: Option<usize>) -> usize {
    threads.unwrap_or_else(|| std::thread::available_parallelism().map_or(4, |n| n.get())).max(1)
}

/// Why a login failed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LoginError {
    /// No such role, it may not log in, or the password is wrong. One answer
    /// for all three, so a client cannot tell which.
    Denied,
    /// The role is fine, but the tenancy policy keeps it off this protocol.
    NotAdmitted(String),
}

/// Checks `user` and `password` against the database's roles, the ones the
/// PostgreSQL port authenticates, then asks the tenancy policy to admit the
/// session on `protocol`. The role lookup runs SQL through the engine, so it
/// is sent to `main` (the server's runtime) rather than run on the caller's
/// I/O thread. Returns whether the role is a superuser.
pub async fn login(
    database: &rclite::Arc<crate::core::database::Database>,
    main: &tokio::runtime::Handle,
    user: &str,
    password: &str,
    protocol: &str,
) -> Result<bool, LoginError> {
    let (db, name) = (database.clone(), user.to_string());
    let role = main
        .spawn(async move { crate::auth::lookup_role(&db, &name).await })
        .await
        .ok()
        .flatten();
    let superuser = match role {
        Some(role)
            if role.can_login
                && role.verifier.as_ref().is_some_and(|v| crate::auth::verify_password(v, user, password)) =>
        {
            role.is_superuser
        }
        _ => return Err(LoginError::Denied),
    };
    let ctx = crate::core::policy_hooks::SessionContext { role: user, database: &database.name, protocol, superuser };
    crate::core::policy_hooks::tenancy().admit(&ctx).map_err(LoginError::NotAdmitted)?;
    Ok(superuser)
}
