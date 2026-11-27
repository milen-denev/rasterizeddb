//! What a connection runs over: TCP with TLS, or a unix socket.
//!
//! A unix socket carries no TLS. On one machine there is nothing for TLS to
//! protect against: only root can see what passes through a local socket, and
//! root can read the memory of the processes at either end anyway. Who may
//! connect is decided by the socket file's permissions, and the kernel tells
//! the server which process — which user — is at the other end, so a server
//! can know its caller without either side presenting a certificate.
//!
//! What TLS also did, besides encrypting, was name the protocol (ALPN) and
//! give the credential exchange a value to bind itself to (the exporter). On a
//! unix socket the client names the protocol in an eight-byte preamble — the
//! same identifier it would have offered in the handshake — and the exchange
//! binds itself to [`crate::session::UNIX_CHANNEL_BINDING`] instead.

use std::fmt;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;

use crate::error::RastcpError;
use crate::session::ChannelBinding;

/// Who is at the other end of a connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Peer {
    /// A TCP peer, by its address.
    Tcp(SocketAddr),
    /// A process on this machine, as the kernel reports it.
    #[cfg(unix)]
    Unix(UnixPeer),
}

/// The credentials of the process at the other end of a unix socket, as the
/// kernel recorded them when it connected. Not something the peer says about
/// itself, so they can be relied on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UnixPeer {
    pub uid: u32,
    pub gid: u32,
    /// Not reported on every platform.
    pub pid: Option<i32>,
}

impl Peer {
    /// The address of a TCP peer; `None` for a unix one.
    pub fn socket_addr(&self) -> Option<SocketAddr> {
        match self {
            Peer::Tcp(addr) => Some(*addr),
            #[cfg(unix)]
            Peer::Unix(_) => None,
        }
    }

    /// The credentials of a unix peer; `None` for a TCP one.
    #[cfg(unix)]
    pub fn unix(&self) -> Option<&UnixPeer> {
        match self {
            Peer::Unix(peer) => Some(peer),
            Peer::Tcp(_) => None,
        }
    }
}

impl fmt::Display for Peer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Peer::Tcp(addr) => write!(f, "{}", addr),
            #[cfg(unix)]
            Peer::Unix(peer) => match peer.pid {
                Some(pid) => write!(f, "unix socket (uid {}, pid {})", peer.uid, pid),
                None => write!(f, "unix socket (uid {})", peer.uid),
            },
        }
    }
}

/// Where a client connects.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Endpoint {
    /// `host:port`.
    Tcp(String),
    /// The path of a unix socket, given as `unix:<path>`.
    #[cfg(unix)]
    Unix(std::path::PathBuf),
}

/// The prefix that makes an address a unix socket's path.
pub const UNIX_PREFIX: &str = "unix:";

impl Endpoint {
    pub(crate) fn parse(addr: &str) -> Result<Endpoint, RastcpError> {
        match addr.strip_prefix(UNIX_PREFIX) {
            None => Ok(Endpoint::Tcp(addr.to_string())),
            #[cfg(unix)]
            Some("") => Err(RastcpError::ProtocolError(
                "a unix socket address needs a path after `unix:`".to_string(),
            )),
            #[cfg(unix)]
            Some(path) => Ok(Endpoint::Unix(std::path::PathBuf::from(path))),
            #[cfg(not(unix))]
            Some(_) => Err(RastcpError::ProtocolError(
                "unix sockets are not supported on this platform".to_string(),
            )),
        }
    }
}

/// Which protocol a connection speaks once it is open.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Version {
    /// `rastcp/1`: one request at a time, answered in order.
    Plain,
    /// `rastcp/2`: tagged frames, many requests in flight.
    Multiplexed,
}

impl Version {
    /// The identifier a client offers: in the TLS handshake, or as the
    /// preamble on a unix socket. Both are eight bytes.
    pub(crate) fn identifier(self) -> &'static [u8] {
        match self {
            Version::Plain => crate::ALPN_PROTOCOL,
            Version::Multiplexed => crate::ALPN_MULTIPLEXED,
        }
    }

    pub(crate) fn from_identifier(identifier: &[u8]) -> Option<Version> {
        if identifier == crate::ALPN_PROTOCOL {
            Some(Version::Plain)
        } else if identifier == crate::ALPN_MULTIPLEXED {
            Some(Version::Multiplexed)
        } else {
            None
        }
    }
}

/// The length of the preamble a client sends first on a unix socket.
pub(crate) const PREAMBLE_LEN: usize = 8;

/// Forwards the I/O traits of an enum of streams to whichever it holds.
///
/// Vectored writes are forwarded too: the TLS layer hands its records to the
/// socket as a vectored write, and a wrapper that answered "not vectored"
/// would have it make one write per record.
macro_rules! forward_io {
    ($name:ident { $($variant:ident $(#[$attr:meta])?),+ }) => {
        impl AsyncRead for $name {
            fn poll_read(
                self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                buf: &mut ReadBuf<'_>,
            ) -> Poll<io::Result<()>> {
                match self.get_mut() {
                    $( $(#[$attr])? $name::$variant(stream) => Pin::new(stream).poll_read(cx, buf), )+
                }
            }
        }

        impl AsyncWrite for $name {
            fn poll_write(
                self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                buf: &[u8],
            ) -> Poll<io::Result<usize>> {
                match self.get_mut() {
                    $( $(#[$attr])? $name::$variant(stream) => Pin::new(stream).poll_write(cx, buf), )+
                }
            }

            fn poll_write_vectored(
                self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                bufs: &[io::IoSlice<'_>],
            ) -> Poll<io::Result<usize>> {
                match self.get_mut() {
                    $( $(#[$attr])? $name::$variant(stream) => Pin::new(stream).poll_write_vectored(cx, bufs), )+
                }
            }

            fn is_write_vectored(&self) -> bool {
                match self {
                    $( $(#[$attr])? $name::$variant(stream) => stream.is_write_vectored(), )+
                }
            }

            fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
                match self.get_mut() {
                    $( $(#[$attr])? $name::$variant(stream) => Pin::new(stream).poll_flush(cx), )+
                }
            }

            fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
                match self.get_mut() {
                    $( $(#[$attr])? $name::$variant(stream) => Pin::new(stream).poll_shutdown(cx), )+
                }
            }
        }
    };
}

/// A connection a server accepted, before anything has been said on it.
pub(crate) enum Accepted {
    Tcp(TcpStream),
    #[cfg(unix)]
    Unix(tokio::net::UnixStream),
}

/// A server's side of an open connection.
pub(crate) enum ServerStream {
    Tls(tokio_rustls::server::TlsStream<TcpStream>),
    #[cfg(unix)]
    Unix(tokio::net::UnixStream),
}

forward_io!(ServerStream { Tls, Unix #[cfg(unix)] });

impl ServerStream {
    /// The certificate the client presented in the handshake (DER), if any.
    pub(crate) fn peer_certificate(&self) -> Option<Vec<u8>> {
        match self {
            ServerStream::Tls(stream) => stream.get_ref().1.peer_certificates().and_then(|c| c.first()).map(|c| c.as_ref().to_vec()),
            #[cfg(unix)]
            ServerStream::Unix(_) => None,
        }
    }

    /// The value a credential exchange on this connection binds itself to.
    pub(crate) fn channel_binding(&self) -> Result<ChannelBinding, RastcpError> {
        match self {
            ServerStream::Tls(stream) => crate::session::channel_binding_from(stream.get_ref().1),
            #[cfg(unix)]
            ServerStream::Unix(_) => Ok(crate::session::UNIX_CHANNEL_BINDING),
        }
    }
}

/// A client's side of an open connection.
pub(crate) enum ClientStream {
    Tls(tokio_rustls::client::TlsStream<TcpStream>),
    #[cfg(unix)]
    Unix(tokio::net::UnixStream),
}

forward_io!(ClientStream { Tls, Unix #[cfg(unix)] });

impl ClientStream {
    /// The value a credential exchange on this connection binds itself to.
    pub(crate) fn channel_binding(&self) -> Result<ChannelBinding, RastcpError> {
        match self {
            ClientStream::Tls(stream) => crate::session::channel_binding_from(stream.get_ref().1),
            #[cfg(unix)]
            ClientStream::Unix(_) => Ok(crate::session::UNIX_CHANNEL_BINDING),
        }
    }

    /// Who the client reached: the server's address, or the credentials of the
    /// process serving the socket.
    pub(crate) fn peer(&self) -> Option<Peer> {
        match self {
            ClientStream::Tls(stream) => stream.get_ref().0.peer_addr().ok().map(Peer::Tcp),
            #[cfg(unix)]
            ClientStream::Unix(stream) => unix_peer(stream).ok().map(Peer::Unix),
        }
    }
}

/// The credentials of the process at the other end of `stream`.
#[cfg(unix)]
pub(crate) fn unix_peer(stream: &tokio::net::UnixStream) -> io::Result<UnixPeer> {
    let credentials = stream.peer_cred()?;
    Ok(UnixPeer {
        uid: credentials.uid(),
        gid: credentials.gid(),
        pid: credentials.pid(),
    })
}

/// The file a unix listener is bound to, removed when the listener goes.
///
/// Removed only if it is still the file this listener bound. A server that is
/// replaced while it shuts down — a restart that binds the path again before
/// the old process is gone — must not take the new server's socket with it.
#[cfg(unix)]
pub struct SocketFile {
    path: std::path::PathBuf,
    device: u64,
    inode: u64,
}

#[cfg(unix)]
impl SocketFile {
    /// Binds a listener on `path`, readable and writable as `mode` says, and
    /// owned by `group` when one is given.
    ///
    /// A socket file left behind by a server that did not shut down cleanly is
    /// replaced. A socket somebody is listening on is not, and neither is a
    /// path that holds anything other than a socket: a mistyped path must not
    /// cost an operator a file.
    ///
    /// Every listener in this workspace that serves a unix socket binds it
    /// here, so they all treat a path the same way.
    pub fn bind(
        path: &std::path::Path,
        mode: u32,
        group: Option<u32>,
    ) -> Result<(std::os::unix::net::UnixListener, SocketFile), RastcpError> {
        use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};

        let failed = |what: &str, e: io::Error| {
            RastcpError::SocketBindingError(format!("{} {}: {}", what, path.display(), e))
        };

        match std::fs::symlink_metadata(path) {
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(failed("could not look at", e)),
            Ok(existing) if !existing.file_type().is_socket() => {
                return Err(RastcpError::SocketBindingError(format!(
                    "{} exists and is not a socket; it is left alone",
                    path.display()
                )));
            }
            Ok(_) => match std::os::unix::net::UnixStream::connect(path) {
                Ok(_) => {
                    return Err(RastcpError::SocketBindingError(format!(
                        "another server is listening on {}",
                        path.display()
                    )));
                }
                // Nobody is listening: what is there was left behind.
                Err(e) if e.kind() == io::ErrorKind::ConnectionRefused => {
                    std::fs::remove_file(path)
                        .map_err(|e| failed("could not remove the stale socket", e))?;
                }
                Err(e) => return Err(failed("could not tell whether anyone listens on", e)),
            },
        }

        let listener = std::os::unix::net::UnixListener::bind(path)
            .map_err(|e| failed("could not bind", e))?;
        listener
            .set_nonblocking(true)
            .map_err(|e| failed("could not make non-blocking the listener on", e))?;

        // The group first, then the mode, so that a group-writable mode never
        // applies to a group other than the one asked for. The file is
        // created with the process's umask, which by default lets only the
        // owner write to it — and connecting needs write permission — so the
        // moment before these is not a wider opening than the default one.
        if let Some(group) = group {
            std::os::unix::fs::chown(path, None, Some(group))
                .map_err(|e| failed(&format!("could not give group {} the socket", group), e))?;
        }
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
            .map_err(|e| failed("could not set the permissions of", e))?;

        let bound = std::fs::metadata(path).map_err(|e| failed("could not look at", e))?;

        Ok((
            listener,
            SocketFile {
                path: path.to_path_buf(),
                device: bound.dev(),
                inode: bound.ino(),
            },
        ))
    }

    pub fn path(&self) -> &std::path::Path {
        &self.path
    }
}

/// The id of the group called `name`, or of the group whose id `name` is.
#[cfg(unix)]
pub fn group_id(name: &str) -> Result<u32, RastcpError> {
    if let Ok(id) = name.parse::<u32>() {
        return Ok(id);
    }

    let wanted = std::ffi::CString::new(name)
        .map_err(|_| RastcpError::SocketBindingError(format!("{:?} is not a group name", name)))?;
    let mut entry: libc::group = unsafe { std::mem::zeroed() };
    let mut found: *mut libc::group = std::ptr::null_mut();
    let mut buffer = vec![0u8; 4096];

    loop {
        // SAFETY: every pointer is to memory owned here and live for the call;
        // the entry's strings land in `buffer`, which is not read afterwards.
        let status = unsafe {
            libc::getgrnam_r(
                wanted.as_ptr(),
                &mut entry,
                buffer.as_mut_ptr().cast(),
                buffer.len(),
                &mut found,
            )
        };

        if status == libc::ERANGE && buffer.len() < 1 << 20 {
            buffer.resize(buffer.len() * 2, 0);
            continue;
        }
        if status != 0 || found.is_null() {
            return Err(RastcpError::SocketBindingError(format!("there is no group {:?}", name)));
        }
        return Ok(entry.gr_gid);
    }
}

#[cfg(unix)]
impl Drop for SocketFile {
    fn drop(&mut self) {
        use std::os::unix::fs::MetadataExt;

        if let Ok(now) = std::fs::symlink_metadata(&self.path) {
            if now.dev() == self.device && now.ino() == self.inode {
                let _ = std::fs::remove_file(&self.path);
            }
        }
    }
}

/// What a server listens on.
pub(crate) enum Listener {
    Tcp(tokio::net::TcpListener),
    #[cfg(unix)]
    Unix {
        listener: tokio::net::UnixListener,
        /// Removes the socket file when the server is dropped.
        file: SocketFile,
    },
}

impl Listener {
    pub(crate) async fn accept(&self) -> io::Result<(Accepted, Peer)> {
        match self {
            Listener::Tcp(listener) => {
                let (stream, addr) = listener.accept().await?;
                Ok((Accepted::Tcp(stream), Peer::Tcp(addr)))
            }
            #[cfg(unix)]
            Listener::Unix { listener, .. } => {
                let (stream, _) = listener.accept().await?;
                // Asked of the kernel at once: it is what the connection is
                // authorised by, so a connection whose credentials cannot be
                // had is not served.
                let peer = unix_peer(&stream)?;
                Ok((Accepted::Unix(stream), Peer::Unix(peer)))
            }
        }
    }

    /// What the server is listening on, for logs.
    pub(crate) fn describe(&self) -> String {
        match self {
            Listener::Tcp(listener) => listener
                .local_addr()
                .map(|addr| addr.to_string())
                .unwrap_or_else(|_| "an unknown address".to_string()),
            #[cfg(unix)]
            Listener::Unix { file, .. } => format!("unix:{}", file.path().display()),
        }
    }
}
