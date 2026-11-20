//! How a connection logs in.

use std::fmt;
use std::sync::Arc;

use rastcp::error::RastcpError;
use rastcp::session::{AuthChannel, AuthFuture, ClientAuthenticator};

/// Who a connection logs in as.
///
/// `Debug` never prints a password.
#[derive(Clone, Default)]
pub enum Login {
    /// No exchange at all: for a server that asks for none, such as a unix
    /// socket under `--rdb-socket-auth trust`. Against a server that does ask,
    /// the connection is refused.
    #[default]
    Trust,
    /// As the operating-system user this process runs as, with no password,
    /// on a unix socket under `--rdb-socket-auth peer`. A role of that name
    /// must exist and be allowed to log in.
    Peer,
    /// As `user`, by peer authentication. The server accepts it only when
    /// `user` is the name of the operating-system user connecting.
    PeerAs(String),
    /// As `user` with `password`, by SCRAM-SHA-256 bound to the connection
    /// (`tls-exporter` over TCP, rastcp's own binding on a unix socket). The
    /// password never crosses the wire, and the server must prove it holds
    /// the user's verifier: a server that lets the connection in without the
    /// exchange (one run under `trust`) is refused at once, because it never
    /// proved it is the server meant; the error says to use [`Login::Trust`]
    /// for such a server.
    Password {
        /// The role to log in as.
        user: String,
        /// Its password.
        password: String,
    },
}

impl Login {
    /// [`Login::Password`] for `user`.
    pub fn password(user: impl Into<String>, password: impl Into<String>) -> Self {
        Login::Password {
            user: user.into(),
            password: password.into(),
        }
    }
}

impl fmt::Debug for Login {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Login::Trust => f.write_str("Trust"),
            Login::Peer => f.write_str("Peer"),
            Login::PeerAs(user) => f.debug_tuple("PeerAs").field(user).finish(),
            Login::Password { user, .. } => f
                .debug_struct("Password")
                .field("user", user)
                .finish_non_exhaustive(),
        }
    }
}

impl Login {
    pub(crate) fn authenticator(
        &self,
    ) -> Result<Option<Arc<dyn ClientAuthenticator>>, RastcpError> {
        let user = match self {
            Login::Trust => return Ok(None),
            Login::Password { user, password } => {
                return Ok(Some(Arc::new(PasswordLogin {
                    user: user.clone(),
                    password: password.clone(),
                }) as Arc<dyn ClientAuthenticator>));
            }
            Login::Peer => current_os_user().ok_or_else(|| {
                RastcpError::AuthenticationError(NO_OS_USER.to_string())
            })?,
            Login::PeerAs(user) => user.clone(),
        };
        Ok(Some(
            Arc::new(PeerLogin { user }) as Arc<dyn ClientAuthenticator>
        ))
    }
}

// The exchange's opening message and the server's reply tags
// (`rasterizeddb_core::native_auth`).
const PROTOCOL_VERSION: u8 = 1;
const C_HELLO: u8 = 0x01;
const C_SASL_INITIAL: u8 = 0x02;
const C_SASL_FINAL: u8 = 0x03;
const S_OK: u8 = 0x00;
const S_SASL: u8 = 0x01;
const S_CONTINUE: u8 = 0x02;
const S_ERROR: u8 = 0xFF;

/// A server that runs no login exchange (a unix socket under
/// `--rdb-socket-auth trust`) takes the hello for a request: a multiplexed
/// connection is closed at once, a plain one answers it as a request. Either
/// way the login failed for a reason the caller can act on, said as such
/// rather than as a closed connection or the bytes of a query's answer.
fn no_exchange_here(what_happened: &str) -> RastcpError {
    RastcpError::AuthenticationError(format!(
        "the server did not take part in the login ({what_happened}): it most likely asks for \
         none, as a unix socket under --rdb-socket-auth trust does; connect with Login::Trust \
         if that server is meant"
    ))
}

/// The server's answer to the hello.
async fn hello_reply(channel: &mut AuthChannel<'_>) -> Result<(u8, String), RastcpError> {
    match server_reply(channel).await {
        Err(RastcpError::ConnectionClosed) => Err(no_exchange_here("it closed the connection")),
        Ok((tag, _)) if !matches!(tag, S_OK | S_SASL | S_ERROR) => {
            Err(no_exchange_here(&format!("it answered with a message of kind {tag}")))
        }
        other => other,
    }
}

/// The opening message: who the caller wants to be.
fn hello(user: &str) -> Result<Vec<u8>, RastcpError> {
    let len = u16::try_from(user.len())
        .map_err(|_| RastcpError::AuthenticationError("the user name is too long".to_string()))?;
    let mut hello = Vec::with_capacity(4 + user.len());
    hello.push(C_HELLO);
    hello.push(PROTOCOL_VERSION);
    hello.extend_from_slice(&len.to_le_bytes());
    hello.extend_from_slice(user.as_bytes());
    Ok(hello)
}

/// One tagged message from the server: its tag, and the rest as text.
async fn server_reply(channel: &mut AuthChannel<'_>) -> Result<(u8, String), RastcpError> {
    let message = channel.recv().await?;
    match message.split_first() {
        Some((tag, rest)) => Ok((*tag, String::from_utf8_lossy(rest).into_owned())),
        None => Err(RastcpError::AuthenticationError(
            "the server sent an empty authentication message".to_string(),
        )),
    }
}

struct PasswordLogin {
    user: String,
    password: String,
}

impl fmt::Debug for PasswordLogin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PasswordLogin")
            .field("user", &self.user)
            .finish_non_exhaustive()
    }
}

impl PasswordLogin {
    async fn run(&self, channel: &mut AuthChannel<'_>) -> Result<(), RastcpError> {
        channel.send(&hello(&self.user)?).await?;
        match hello_reply(channel).await? {
            (S_SASL, _) => {}
            (S_OK, _) => {
                return Err(RastcpError::AuthenticationError(
                    "the server let the connection in without a password exchange, so it never \
                     proved who it is; log in with Login::Trust if that server is meant"
                        .to_string(),
                ));
            }
            (_, message) => return Err(RastcpError::AuthenticationError(message)),
        }

        let (mut scram, client_first) =
            crate::scram::ScramClient::start(&self.user, &self.password, channel.channel_binding())
                .map_err(|e| {
                    RastcpError::AuthenticationError(format!("no random bytes for a nonce: {e}"))
                })?;
        let mut message = vec![C_SASL_INITIAL];
        message.extend_from_slice(client_first.as_bytes());
        channel.send(&message).await?;

        let server_first = match server_reply(channel).await? {
            (S_CONTINUE, payload) => payload,
            (_, message) => return Err(RastcpError::AuthenticationError(message)),
        };
        let client_final = scram
            .respond(&server_first)
            .map_err(RastcpError::AuthenticationError)?;
        let mut message = vec![C_SASL_FINAL];
        message.extend_from_slice(client_final.as_bytes());
        channel.send(&message).await?;

        let server_final = match server_reply(channel).await? {
            (S_OK, payload) => payload,
            (_, message) => return Err(RastcpError::AuthenticationError(message)),
        };
        // Not optional: without it this side has proved itself to whoever
        // answered.
        scram
            .verify(&server_final)
            .map_err(RastcpError::AuthenticationError)
    }
}

impl ClientAuthenticator for PasswordLogin {
    fn authenticate<'a>(&'a self, channel: &'a mut AuthChannel<'_>) -> AuthFuture<'a, ()> {
        Box::pin(self.run(channel))
    }
}

struct PeerLogin {
    user: String,
}

impl fmt::Debug for PeerLogin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PeerLogin")
            .field("user", &self.user)
            .finish()
    }
}

impl PeerLogin {
    async fn run(&self, channel: &mut AuthChannel<'_>) -> Result<(), RastcpError> {
        let len = u16::try_from(self.user.len()).map_err(|_| {
            RastcpError::AuthenticationError("the user name is too long".to_string())
        })?;
        let mut hello = Vec::with_capacity(4 + self.user.len());
        hello.push(C_HELLO);
        hello.push(PROTOCOL_VERSION);
        hello.extend_from_slice(&len.to_le_bytes());
        hello.extend_from_slice(self.user.as_bytes());
        channel.send(&hello).await?;
        match hello_reply(channel).await? {
            (S_OK, _) => Ok(()),
            (S_SASL, _) => Err(RastcpError::AuthenticationError(
                "the server asks for a password: peer login needs a unix socket under \
                 --rdb-socket-auth peer"
                    .to_string(),
            )),
            (_, message) => Err(RastcpError::AuthenticationError(message)),
        }
    }
}

impl ClientAuthenticator for PeerLogin {
    fn authenticate<'a>(&'a self, channel: &'a mut AuthChannel<'_>) -> AuthFuture<'a, ()> {
        Box::pin(self.run(channel))
    }
}

#[cfg(unix)]
const NO_OS_USER: &str = "this process's user has no name in the password database";
#[cfg(not(unix))]
const NO_OS_USER: &str =
    "peer login needs a unix socket, which this platform does not have; log in with a password";

/// The name of the operating-system user this process runs as. Only a unix
/// socket carries the peer's credentials, so elsewhere there is none to give.
#[cfg(not(unix))]
pub(crate) fn current_os_user() -> Option<String> {
    None
}

/// The name of the operating-system user this process runs as.
#[cfg(unix)]
pub(crate) fn current_os_user() -> Option<String> {
    // SAFETY: getuid cannot fail and has no preconditions.
    let uid = unsafe { libc::getuid() };
    let mut entry: libc::passwd = unsafe { std::mem::zeroed() };
    let mut found: *mut libc::passwd = std::ptr::null_mut();
    let mut buffer = vec![0u8; 4096];
    loop {
        // SAFETY: every pointer is to memory owned here and live for the call;
        // `getpwuid_r` writes the entry's strings into `buffer`, which outlives
        // the read of `pw_name` below.
        let status = unsafe {
            libc::getpwuid_r(
                uid,
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
        if status != 0 || found.is_null() || entry.pw_name.is_null() {
            return None;
        }
        // SAFETY: `pw_name` points into `buffer`, NUL-terminated by getpwuid_r.
        let name = unsafe { std::ffi::CStr::from_ptr(entry.pw_name) };
        return name.to_str().ok().map(str::to_string);
    }
}
