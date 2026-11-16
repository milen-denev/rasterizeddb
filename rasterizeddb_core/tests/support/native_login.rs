//! The client half of the native login, for the tests that drive it against
//! the server half (`src/native_auth.rs`). Test-only: applications log in
//! through `rasterizeddb_client::Login`, whose exchange is a copy of this one.
//!
//! The protocol bytes are repeated from `src/native_auth.rs`, so a change to
//! the exchange there fails these tests until this side follows.

#![allow(dead_code)]

use std::fmt;

use rastcp::error::RastcpError;
use rastcp::session::{AuthChannel, AuthFuture, ClientAuthenticator};
use rasterizeddb_core::auth::{ChannelBinding, ScramClient};

const PROTOCOL_VERSION: u8 = 1;
const C_HELLO: u8 = 0x01;
const C_SASL_INITIAL: u8 = 0x02;
const C_SASL_FINAL: u8 = 0x03;
const S_OK: u8 = 0x00;
const S_SASL: u8 = 0x01;
const S_CONTINUE: u8 = 0x02;

/// The login name of the user this process runs as.
pub fn current_os_user() -> Option<String> {
    // SAFETY: getuid cannot fail and has no preconditions.
    rasterizeddb_core::native_auth::os_user_name(unsafe { libc::getuid() })
}

/// Presents a user name and password.
pub struct PasswordAuthenticator {
    user: String,
    password: String,
    /// Whether the server is required to actually run the SCRAM exchange.
    ///
    /// True by default, and the reason is worth stating: the exchange is what
    /// proves the *server* is genuine. A client that accepted "you are in" from
    /// whoever answered would hand its queries to anyone able to intercept the
    /// connection — which, against the default trust setting, is anyone in the
    /// network path. A client carrying a password therefore insists on using
    /// it.
    require_scram: bool,
}

impl fmt::Debug for PasswordAuthenticator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The password is deliberately absent: this type ends up inside a
        // client that something else will one day print.
        f.debug_struct("PasswordAuthenticator")
            .field("user", &self.user)
            .field("require_scram", &self.require_scram)
            .finish_non_exhaustive()
    }
}

impl PasswordAuthenticator {
    pub fn new(user: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            user: user.into(),
            password: password.into(),
            require_scram: true,
        }
    }

    /// Accept a server that waves the connection through without a challenge.
    ///
    /// Only for a server deliberately run under `trust` — on a loopback
    /// listener, say. It gives up the proof that the server is the one meant,
    /// so it is opt-in and named for what it costs.
    pub fn allow_trust(mut self) -> Self {
        self.require_scram = false;
        self
    }

    async fn run(&self, channel: &mut AuthChannel<'_>) -> Result<(), RastcpError> {
        channel.send(&hello_message(&self.user)?).await?;

        match server_reply(channel).await? {
            (S_OK, _) if !self.require_scram => return Ok(()),
            (S_OK, _) => {
                return Err(RastcpError::AuthenticationError(
                    "the server accepted the connection without a challenge, so it never proved \
                     who it is; pass allow_trust if that is really intended"
                        .to_string(),
                ));
            }
            (S_SASL, _) => {}
            (_, message) => return Err(RastcpError::AuthenticationError(message)),
        }

        let binding = ChannelBinding::TlsExporter(channel.channel_binding().to_vec());
        let (mut scram, client_first) = ScramClient::start(&self.user, &self.password, binding);

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

        // Not optional: without this the client has proved itself to whoever
        // answered, which is exactly what an impersonator wants.
        scram
            .verify(&server_final)
            .map_err(RastcpError::AuthenticationError)
    }
}

impl ClientAuthenticator for PasswordAuthenticator {
    fn authenticate<'a>(&'a self, channel: &'a mut AuthChannel<'_>) -> AuthFuture<'a, ()> {
        Box::pin(self.run(channel))
    }
}

/// Logs in as the operating-system user this process runs as, on a unix
/// socket whose server is set to [`AuthMethod::Peer`]. No password: the
/// server asks the kernel who connected.
#[derive(Clone)]
pub struct PeerAuthenticator {
    user: String,
}

impl fmt::Debug for PeerAuthenticator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PeerAuthenticator")
            .field("user", &self.user)
            .finish()
    }
}

impl PeerAuthenticator {
    /// Logs in as `user`, which the server accepts only if it is the name of
    /// the operating-system user this process runs as.
    pub fn new(user: impl Into<String>) -> Self {
        Self { user: user.into() }
    }

    /// Logs in as the operating-system user this process runs as.
    #[cfg(unix)]
    pub fn current_user() -> Result<Self, RastcpError> {
        current_os_user().map(Self::new).ok_or_else(|| {
            RastcpError::AuthenticationError(
                "this process's user has no name in the password database".to_string(),
            )
        })
    }

    async fn run(&self, channel: &mut AuthChannel<'_>) -> Result<(), RastcpError> {
        channel.send(&hello_message(&self.user)?).await?;

        match server_reply(channel).await? {
            (S_OK, _) => Ok(()),
            (S_SASL, _) => Err(RastcpError::AuthenticationError(
                "the server asks for a password: peer authentication needs a unix socket whose \
                 server is set to --rdb-socket-auth peer"
                    .to_string(),
            )),
            (_, message) => Err(RastcpError::AuthenticationError(message)),
        }
    }
}

impl ClientAuthenticator for PeerAuthenticator {
    fn authenticate<'a>(&'a self, channel: &'a mut AuthChannel<'_>) -> AuthFuture<'a, ()> {
        Box::pin(self.run(channel))
    }
}

/// The opening message: who the caller wants to be.
fn hello_message(user: &str) -> Result<Vec<u8>, RastcpError> {
    let mut hello = Vec::with_capacity(4 + user.len());
    hello.push(C_HELLO);
    hello.push(PROTOCOL_VERSION);
    let len = u16::try_from(user.len())
        .map_err(|_| RastcpError::AuthenticationError("the user name is too long".to_string()))?;
    hello.extend_from_slice(&len.to_le_bytes());
    hello.extend_from_slice(user.as_bytes());
    Ok(hello)
}

/// Reads one tagged message from the server.
async fn server_reply(channel: &mut AuthChannel<'_>) -> Result<(u8, String), RastcpError> {
    let message = channel.recv().await?;
    match message.split_first() {
        Some((tag, rest)) => Ok((*tag, String::from_utf8_lossy(rest).to_string())),
        None => Err(RastcpError::AuthenticationError(
            "the server sent an empty authentication message".to_string(),
        )),
    }
}
