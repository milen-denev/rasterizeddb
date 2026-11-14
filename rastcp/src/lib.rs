pub mod cert;
pub mod client;
pub mod common;
pub mod config;
pub mod error;
mod frame;
mod interrupt;
pub mod mux;
pub mod server;
pub mod session;
pub mod tls;

/// The ALPN protocol identifier both ends offer.
///
/// Naming the protocol inside the handshake is what makes a cross-protocol
/// attack impossible: a client of this server cannot be steered into talking to
/// some other TLS service that happens to present an acceptable certificate,
/// because that service will not agree to speak `rastcp/1`.
pub const ALPN_PROTOCOL: &[u8] = b"rastcp/1";

/// The ALPN identifier of the multiplexed protocol.
///
/// The same transport with one difference: every frame carries an id the
/// client chose, so one connection can have many requests in flight and the
/// server may answer them in any order. A server offers both; a client asks
/// for the one it speaks. See [`mux::MuxClient`].
pub const ALPN_MULTIPLEXED: &[u8] = b"rastcp/2";
