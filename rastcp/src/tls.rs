//! Deciding who is on the other end of a connection.
//!
//! TLS gives two things, and they are separable: a channel nobody can read, and
//! proof of who you are talking to. The first is free. The second is the one
//! that takes configuration, and skipping it leaves a connection that is
//! encrypted against a passive eavesdropper and wide open to an active one — an
//! attacker who can intercept the TCP connection presents any certificate at
//! all, is believed, and from then on sees and rewrites everything, including
//! any shared secret sent *inside* the tunnel.
//!
//! So this module exists to make the second thing configurable, in two shapes:
//!
//! - [`Trust::Pinned`] / [`ClientAuth::RequirePinned`] — "I will speak to these
//!   exact certificates", by SHA-256 of the certificate itself. No certificate
//!   authority to run: generate a self-signed certificate per node and give
//!   every node the others' fingerprints. Right for a handful of machines.
//! - [`Trust::RootCa`] / [`ClientAuth::RequireCa`] — "I will speak to anything
//!   this authority has signed". One certificate authority for the cluster,
//!   which signs a certificate per node. Right when nodes are added often
//!   enough that re-distributing fingerprints is a chore.
//!
//! Both are available in both directions, and a cluster should use both
//! directions: verifying the server proves you reached the node you meant, and
//! requiring a client certificate proves the caller is entitled to be there.

use std::sync::Arc;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{CryptoProvider, WebPkiSupportedAlgorithms};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::{DigitallySignedStruct, DistinguishedName, Error, RootCertStore, SignatureScheme};
use sha2::{Digest, Sha256};

use crate::error::RastcpError;

/// A certificate's identity for pinning: SHA-256 over its DER encoding.
///
/// The same value OpenSSL prints as `-fingerprint -sha256`, so an operator can
/// check one without this tool.
pub type Fingerprint = [u8; 32];

/// How a client decides whether to believe the server it reached.
#[derive(Clone, Debug)]
pub enum Trust {
    /// Believe anything. Encrypted, unauthenticated, and trivially
    /// impersonated by anyone in the network path. Only for a loopback or a
    /// link that is already trusted by other means.
    AcceptAny,
    /// The public web PKI, as packaged by `webpki-roots`.
    WebPki,
    /// A private certificate authority.
    RootCa(Vec<CertificateDer<'static>>),
    /// Exactly these end-entity certificates.
    Pinned(Vec<Fingerprint>),
}

impl Trust {
    /// Whether this actually establishes who the peer is.
    pub fn authenticates(&self) -> bool {
        !matches!(self, Trust::AcceptAny)
    }
}

/// A certificate and key a client presents when a server asks for one.
#[derive(Debug)]
pub struct ClientIdentity {
    pub certs: Vec<CertificateDer<'static>>,
    pub key: PrivateKeyDer<'static>,
}

impl ClientIdentity {
    /// Loads one from PEM files.
    pub fn load(cert_path: &str, key_path: &str) -> Result<ClientIdentity, RastcpError> {
        Ok(ClientIdentity {
            certs: load_certificates(cert_path)?,
            key: load_private_key(key_path)?,
        })
    }
}

/// Whether a server demands a certificate from its callers, and which.
#[derive(Clone, Debug)]
pub enum ClientAuth {
    /// Anyone may connect. Authorisation, if any, happens above TLS.
    None,
    /// A certificate signed by this authority is required.
    RequireCa(Vec<CertificateDer<'static>>),
    /// One of these exact certificates is required.
    RequirePinned(Vec<Fingerprint>),
}

impl ClientAuth {
    pub fn authenticates(&self) -> bool {
        !matches!(self, ClientAuth::None)
    }
}

/// SHA-256 over a certificate's DER encoding.
pub fn fingerprint(cert: &CertificateDer<'_>) -> Fingerprint {
    let mut hasher = Sha256::new();
    hasher.update(cert.as_ref());
    hasher.finalize().into()
}

/// Renders a fingerprint as lowercase hex, which is what the flags take.
pub fn format_fingerprint(value: &Fingerprint) -> String {
    let mut out = String::with_capacity(64);
    for byte in value {
        out.push_str(&format!("{:02x}", byte));
    }
    out
}

/// Reads a fingerprint written as hex, tolerating the `:` and whitespace that
/// `openssl x509 -fingerprint` puts in and the case it uses.
pub fn parse_fingerprint(text: &str) -> Result<Fingerprint, RastcpError> {
    let cleaned: String = text
        .chars()
        .filter(|c| !c.is_whitespace() && *c != ':')
        .collect();
    // Accept the `SHA256:<hex>` form some tools print.
    let cleaned = cleaned
        .strip_prefix("SHA256")
        .unwrap_or(&cleaned)
        .trim_start_matches('=')
        .to_string();

    if cleaned.len() != 64 {
        return Err(RastcpError::CertificateLoading(format!(
            "a SHA-256 fingerprint is 64 hex characters; got {}",
            cleaned.len()
        )));
    }

    let mut out = [0u8; 32];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&cleaned[i * 2..i * 2 + 2], 16).map_err(|_| {
            RastcpError::CertificateLoading("a fingerprint must be hexadecimal".to_string())
        })?;
    }
    Ok(out)
}

/// Reads a PEM certificate chain.
pub fn load_certificates(path: &str) -> Result<Vec<CertificateDer<'static>>, RastcpError> {
    let bytes = std::fs::read(path)
        .map_err(|e| RastcpError::CertificateLoading(format!("cannot read {}: {}", path, e)))?;
    let mut reader = std::io::BufReader::new(bytes.as_slice());

    let certs = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| RastcpError::CertificateLoading(format!("cannot parse {}: {}", path, e)))?;

    if certs.is_empty() {
        return Err(RastcpError::CertificateLoading(format!(
            "{} holds no certificates",
            path
        )));
    }
    Ok(certs)
}

/// Reads a PEM private key, in any of the encodings `rustls-pemfile` knows.
pub fn load_private_key(path: &str) -> Result<PrivateKeyDer<'static>, RastcpError> {
    let bytes = std::fs::read(path)
        .map_err(|e| RastcpError::CertificateLoading(format!("cannot read {}: {}", path, e)))?;
    let mut reader = std::io::BufReader::new(bytes.as_slice());

    rustls_pemfile::private_key(&mut reader)
        .map_err(|e| RastcpError::CertificateLoading(format!("cannot parse {}: {}", path, e)))?
        .ok_or_else(|| RastcpError::CertificateLoading(format!("{} holds no private key", path)))
}

fn root_store(certs: &[CertificateDer<'static>]) -> Result<RootCertStore, RastcpError> {
    let mut roots = RootCertStore::empty();
    for cert in certs {
        roots.add(cert.clone()).map_err(|e| {
            RastcpError::CertificateLoading(format!("not a usable authority certificate: {}", e))
        })?;
    }
    Ok(roots)
}

fn provider() -> Arc<CryptoProvider> {
    // The process installs one at start-up; fall back to building one rather
    // than panicking if some embedding never did.
    CryptoProvider::get_default()
        .cloned()
        .unwrap_or_else(|| Arc::new(rustls::crypto::aws_lc_rs::default_provider()))
}

/// Verifies that the certificate presented is one of a known set.
///
/// Chain and name validation are deliberately not performed: with a pin there
/// is no chain to walk and no name to trust — the certificate *is* the identity,
/// and a self-signed one is exactly as good as a signed one. What still has to
/// hold, and does, is that the peer proves possession of the matching private
/// key, which the handshake signature below establishes.
#[derive(Debug)]
struct PinnedServerVerifier {
    pins: Vec<Fingerprint>,
    algorithms: WebPkiSupportedAlgorithms,
}

impl ServerCertVerifier for PinnedServerVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, Error> {
        let presented = fingerprint(end_entity);
        // Constant-time over the whole set: a timing difference here would let
        // a caller learn a pin a byte at a time.
        let mut matched = false;
        for pin in &self.pins {
            use subtle::ConstantTimeEq;
            matched |= bool::from(presented.ct_eq(pin));
        }

        if matched {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(Error::General(format!(
                "the server presented certificate {}, which is not among the pinned ones",
                format_fingerprint(&presented)
            )))
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.algorithms.supported_schemes()
    }
}

/// The mirror of [`PinnedServerVerifier`], for a server checking its callers.
#[derive(Debug)]
struct PinnedClientVerifier {
    pins: Vec<Fingerprint>,
    algorithms: WebPkiSupportedAlgorithms,
    /// Empty: a pinning server offers no authority hints, because there is no
    /// authority. Clients configured for this cluster send their certificate
    /// regardless of hints.
    hints: Vec<DistinguishedName>,
}

impl ClientCertVerifier for PinnedClientVerifier {
    fn offer_client_auth(&self) -> bool {
        true
    }

    fn client_auth_mandatory(&self) -> bool {
        true
    }

    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        &self.hints
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _now: UnixTime,
    ) -> Result<ClientCertVerified, Error> {
        let presented = fingerprint(end_entity);
        let mut matched = false;
        for pin in &self.pins {
            use subtle::ConstantTimeEq;
            matched |= bool::from(presented.ct_eq(pin));
        }

        if matched {
            Ok(ClientCertVerified::assertion())
        } else {
            Err(Error::General(format!(
                "the caller presented certificate {}, which is not among the pinned ones",
                format_fingerprint(&presented)
            )))
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.algorithms.supported_schemes()
    }
}

/// Builds the server-certificate verifier a [`Trust`] describes.
///
/// `None` means "use rustls' own default for the configured roots", which is
/// what [`Trust::WebPki`] and [`Trust::RootCa`] want — they are ordinary
/// verification against a root store and need no custom code.
pub(crate) fn server_verifier(
    trust: &Trust,
) -> Result<Option<Arc<dyn ServerCertVerifier>>, RastcpError> {
    match trust {
        Trust::AcceptAny | Trust::WebPki | Trust::RootCa(_) => Ok(None),
        Trust::Pinned(pins) => {
            if pins.is_empty() {
                return Err(RastcpError::CertificateLoading(
                    "pinned trust needs at least one fingerprint".to_string(),
                ));
            }
            Ok(Some(Arc::new(PinnedServerVerifier {
                pins: pins.clone(),
                algorithms: provider().signature_verification_algorithms,
            })))
        }
    }
}

/// Builds the client-certificate verifier a [`ClientAuth`] describes.
pub(crate) fn client_verifier(
    auth: &ClientAuth,
) -> Result<Option<Arc<dyn ClientCertVerifier>>, RastcpError> {
    match auth {
        ClientAuth::None => Ok(None),
        ClientAuth::RequireCa(certs) => {
            let roots = root_store(certs)?;
            let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
                .build()
                .map_err(|e| {
                    RastcpError::CertificateLoading(format!(
                        "cannot build a client verifier from the given authority: {}",
                        e
                    ))
                })?;
            Ok(Some(verifier))
        }
        ClientAuth::RequirePinned(pins) => {
            if pins.is_empty() {
                return Err(RastcpError::CertificateLoading(
                    "pinned client auth needs at least one fingerprint".to_string(),
                ));
            }
            Ok(Some(Arc::new(PinnedClientVerifier {
                pins: pins.clone(),
                algorithms: provider().signature_verification_algorithms,
                hints: Vec::new(),
            })))
        }
    }
}

/// The root store a [`Trust`] wants, for the cases that use ordinary
/// verification.
pub(crate) fn trust_roots(trust: &Trust) -> Result<Option<RootCertStore>, RastcpError> {
    match trust {
        Trust::RootCa(certs) => Ok(Some(root_store(certs)?)),
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_fingerprint_round_trips_through_its_text() {
        let value: Fingerprint = [
            0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab,
            0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67,
            0x89, 0xab, 0xcd, 0xef,
        ];
        let text = format_fingerprint(&value);
        assert_eq!(text.len(), 64);
        assert_eq!(parse_fingerprint(&text).unwrap(), value);
    }

    #[test]
    fn the_shapes_other_tools_print_are_accepted() {
        let plain = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let expected = parse_fingerprint(plain).unwrap();

        let colons = plain
            .as_bytes()
            .chunks(2)
            .map(|c| std::str::from_utf8(c).unwrap())
            .collect::<Vec<_>>()
            .join(":");
        assert_eq!(parse_fingerprint(&colons).unwrap(), expected);
        assert_eq!(parse_fingerprint(&plain.to_ascii_uppercase()).unwrap(), expected);
        assert_eq!(parse_fingerprint(&format!("SHA256:{}", plain)).unwrap(), expected);
        assert_eq!(parse_fingerprint(&format!("  {}  ", plain)).unwrap(), expected);
    }

    #[test]
    fn a_fingerprint_of_the_wrong_length_is_refused() {
        assert!(parse_fingerprint("abcd").is_err());
        assert!(parse_fingerprint("").is_err());
    }

    #[test]
    fn a_non_hex_fingerprint_is_refused() {
        let bad = "z".repeat(64);
        assert!(parse_fingerprint(&bad).is_err());
    }

    #[test]
    fn a_generated_certificate_has_a_stable_fingerprint() {
        let (certs, _) = crate::cert::generate_self_signed_cert().unwrap();
        let first = fingerprint(&certs[0]);
        assert_eq!(first, fingerprint(&certs[0]), "hashing is not supposed to vary");

        let (other, _) = crate::cert::generate_self_signed_cert().unwrap();
        assert_ne!(
            first,
            fingerprint(&other[0]),
            "two independently generated certificates must not collide"
        );
    }

    #[test]
    fn accept_any_is_the_only_trust_that_does_not_authenticate() {
        assert!(!Trust::AcceptAny.authenticates());
        assert!(Trust::WebPki.authenticates());
        assert!(Trust::Pinned(vec![[0u8; 32]]).authenticates());
        assert!(!ClientAuth::None.authenticates());
        assert!(ClientAuth::RequirePinned(vec![[0u8; 32]]).authenticates());
    }

    #[test]
    fn pinned_trust_without_a_pin_is_refused_rather_than_trusting_everything() {
        assert!(server_verifier(&Trust::Pinned(Vec::new())).is_err());
        assert!(client_verifier(&ClientAuth::RequirePinned(Vec::new())).is_err());
    }
}
