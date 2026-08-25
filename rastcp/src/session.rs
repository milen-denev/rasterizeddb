//! Proving *who* is calling, once TLS has established *that nobody can listen*.
//!
//! [`crate::tls`] answers a different question: it decides whether the machine
//! on the other end holds an acceptable certificate. That is the right check
//! between the nodes of one cluster, where every node is issued an identity by
//! whoever runs it. It is the wrong check for a client: handing a certificate
//! and its private key to every application that wants to run a query means
//! managing a certificate authority for the whole fleet, and it says nothing
//! about *which user* the application is acting as.
//!
//! So this module adds the second kind of authentication — a credential
//! exchange that runs immediately after the handshake, before a single request
//! is served — and leaves what that exchange *is* to the crate above. rastcp
//! transports messages; it does not know what a role is.
//!
//! # What the transport guarantees the exchange
//!
//! - It runs on a connection that is already encrypted, so a password-bearing
//!   exchange never crosses the wire in the clear.
//! - It is framed and capped ([`MAX_AUTH_MESSAGE`]), so an unauthenticated
//!   caller cannot make the server allocate an 8 MiB buffer per connection.
//! - It is bounded by a timeout, so an unauthenticated caller cannot hold a
//!   connection slot open by saying nothing.
//! - It can bind itself to *this* TLS connection through
//!   [`AuthChannel::channel_binding`] — the RFC 9266 `tls-exporter` value.
//!   An exchange that mixes that value into what it signs cannot be relayed by
//!   a man in the middle onto a different connection, which is the one attack
//!   a password exchange over unauthenticated TLS is otherwise open to.

use std::fmt;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite};

use crate::common::{read_message_limited_timeout, write_message_timeout};
use crate::error::RastcpError;

/// Cap on a single authentication message.
///
/// Generous for a credential exchange and small enough that a connection that
/// has proved nothing yet cannot reserve much memory.
pub const MAX_AUTH_MESSAGE: usize = 64 * 1024;

/// Exporter label for `tls-exporter` channel binding (RFC 9266 §3).
pub const CHANNEL_BINDING_LABEL: &[u8] = b"EXPORTER-Channel-Binding";

/// Length of the exported channel-binding value, as RFC 9266 fixes it.
pub const CHANNEL_BINDING_LEN: usize = 32;

/// The value that identifies one TLS connection to the exchange running inside
/// it. See [`AuthChannel::channel_binding`].
pub type ChannelBinding = [u8; CHANNEL_BINDING_LEN];

/// Who a connection turned out to belong to.
///
/// Produced by a [`ServerAuthenticator`] and handed to every request on that
/// connection, so a handler can refuse work the caller is not entitled to
/// without repeating the authentication.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Principal {
    /// The name the caller authenticated as.
    pub user: String,
    /// Whether that name is allowed to do everything.
    pub superuser: bool,
}

impl Principal {
    pub fn new(user: impl Into<String>, superuser: bool) -> Self {
        Self {
            user: user.into(),
            superuser,
        }
    }

    /// The principal of a server that was never given an authenticator.
    ///
    /// Deliberately not a superuser: code that gates on `superuser` must not
    /// silently pass everything when authentication is switched off.
    pub fn anonymous() -> Self {
        Self {
            user: String::new(),
            superuser: false,
        }
    }

    /// Whether anyone actually proved anything.
    pub fn is_anonymous(&self) -> bool {
        self.user.is_empty()
    }
}

impl fmt::Display for Principal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_anonymous() {
            f.write_str("<anonymous>")
        } else {
            f.write_str(&self.user)
        }
    }
}

/// Any TLS stream an exchange can run over.
///
/// Its only purpose is to let [`AuthChannel`] hold the stream behind a `dyn`
/// reference, so authenticators can themselves be trait objects stored in a
/// builder.
pub trait AuthIo: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> AuthIo for T {}

/// The framed, capped, time-bounded view of the connection that an
/// authenticator is allowed to use.
///
/// Messages are the same length-prefixed frames the request loop uses, so an
/// exchange needs no framing of its own — but with a much smaller cap, because
/// nothing has been proved yet.
pub struct AuthChannel<'a> {
    io: &'a mut dyn AuthIo,
    timeout: Duration,
    binding: ChannelBinding,
    peer: SocketAddr,
}

impl<'a> AuthChannel<'a> {
    pub fn new(
        io: &'a mut dyn AuthIo,
        timeout: Duration,
        binding: ChannelBinding,
        peer: SocketAddr,
    ) -> Self {
        Self {
            io,
            timeout,
            binding,
            peer,
        }
    }

    /// Reads the next message, failing if the peer takes too long or sends
    /// something oversized.
    pub async fn recv(&mut self) -> Result<Vec<u8>, RastcpError> {
        read_message_limited_timeout(&mut self.io, MAX_AUTH_MESSAGE, self.timeout).await
    }

    /// Sends one message.
    pub async fn send(&mut self, message: &[u8]) -> Result<(), RastcpError> {
        write_message_timeout(&mut self.io, message, self.timeout).await
    }

    /// The `tls-exporter` channel binding for this connection (RFC 9266).
    ///
    /// Both ends derive the same 32 bytes from the TLS key schedule, and no
    /// third party can derive them for a connection it does not terminate. An
    /// exchange that signs over this value therefore proves not only that the
    /// caller knows a secret but that it is speaking on *this* connection — so
    /// a man in the middle who relays the messages onto a second connection
    /// ends up with a proof that does not verify there.
    pub fn channel_binding(&self) -> &ChannelBinding {
        &self.binding
    }

    /// Where the caller is dialling from, for rate limiting and logs.
    pub fn peer_addr(&self) -> SocketAddr {
        self.peer
    }
}

impl fmt::Debug for AuthChannel<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AuthChannel")
            .field("peer", &self.peer)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

/// A future an authenticator returns, boxed so authenticators can be `dyn`.
pub type AuthFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, RastcpError>> + Send + 'a>>;

/// The server half of a credential exchange.
///
/// Runs once per connection, between the TLS handshake and the first request.
/// Returning an error closes the connection without serving anything.
///
/// # Implementing one safely
///
/// - Report every failure the same way. A message that distinguishes "no such
///   user" from "wrong password" turns the protocol into an account
///   enumeration oracle.
/// - Compare secrets in constant time.
/// - Mix [`AuthChannel::channel_binding`] into whatever the caller signs, when
///   the exchange has anything to sign.
pub trait ServerAuthenticator: Send + Sync + fmt::Debug {
    fn authenticate<'a>(&'a self, channel: &'a mut AuthChannel<'_>) -> AuthFuture<'a, Principal>;
}

/// The client half of the same exchange.
pub trait ClientAuthenticator: Send + Sync + fmt::Debug {
    fn authenticate<'a>(&'a self, channel: &'a mut AuthChannel<'_>) -> AuthFuture<'a, ()>;
}

/// Derives the `tls-exporter` channel binding from a finished handshake.
///
/// A failure here is not survivable: it means the connection has no exporter,
/// so a bound exchange cannot be run and must not silently fall back to an
/// unbound one.
pub(crate) fn channel_binding_from<Data>(
    connection: &rustls::ConnectionCommon<Data>,
) -> Result<ChannelBinding, RastcpError> {
    let mut binding = [0u8; CHANNEL_BINDING_LEN];
    connection
        .export_keying_material(&mut binding[..], CHANNEL_BINDING_LABEL, Some(&[]))
        .map_err(|e| {
            RastcpError::AuthenticationError(format!(
                "this connection exports no channel binding: {}",
                e
            ))
        })?;
    Ok(binding)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_anonymous_principal_is_not_a_superuser() {
        let principal = Principal::anonymous();
        assert!(principal.is_anonymous());
        assert!(
            !principal.superuser,
            "code gating on `superuser` must not pass everything when authentication is off"
        );
        assert_eq!(principal.to_string(), "<anonymous>");
    }

    #[test]
    fn a_named_principal_shows_its_name() {
        let principal = Principal::new("alice", true);
        assert!(!principal.is_anonymous());
        assert_eq!(principal.to_string(), "alice");
    }
}
