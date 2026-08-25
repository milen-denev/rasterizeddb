pub mod cert;
pub mod client;
pub mod common;
pub mod config;
pub mod error;
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
