use log::{debug, info};
use rustls::client::Resumption;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, Error, SignatureScheme};
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio_rustls::{TlsConnector, client::TlsStream};

use crate::ALPN_PROTOCOL;
use crate::common::{read_message, read_message_timeout, write_message, write_message_timeout};
use crate::config::ConfigBuilderExt;
use crate::error::RastcpError;
use crate::session::{AuthChannel, ClientAuthenticator};
use crate::tls::{ClientIdentity, Trust};

/// How long the credential exchange may take, when there is one.
const DEFAULT_AUTH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

// A certificate verifier that accepts any certificate
#[derive(Debug)]
#[allow(dead_code)]
struct AcceptAnyCertificate;

impl ServerCertVerifier for AcceptAnyCertificate {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, Error> {
        // This is insecure but convenient for development
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        // Accept any signature
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        // Accept any signature
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        // Support all common signature schemes
        vec![
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::ED25519,
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::RSA_PKCS1_SHA384,
            SignatureScheme::RSA_PKCS1_SHA512,
        ]
    }
}

pub struct TcpClientBuilder {
    addr: String,
    timeout: Option<std::time::Duration>,
    verify_certificate: bool,
    trust: Option<Trust>,
    identity: Option<ClientIdentity>,
    authenticator: Option<Arc<dyn ClientAuthenticator>>,
    auth_timeout: std::time::Duration,
    allow_tls12: bool,
}

pub struct TcpClient {
    addr: String,
    connector: TlsConnector,
    server_name: ServerName<'static>,
    connection: Option<TlsStream<TcpStream>>,
    timeout: Option<std::time::Duration>,
    authenticator: Option<Arc<dyn ClientAuthenticator>>,
    auth_timeout: std::time::Duration,
}

impl TcpClientBuilder {
    pub fn new(addr: &str) -> Self {
        Self {
            addr: addr.to_string(),
            timeout: Some(std::time::Duration::from_secs(30)), // Default timeout
            verify_certificate: false,
            trust: None,
            identity: None,
            authenticator: None,
            auth_timeout: DEFAULT_AUTH_TIMEOUT,
            // See `crate::server::TcpServerBuilder::allow_tls12`.
            allow_tls12: false,
        }
    }

    /// The credentials this client presents once the tunnel is up.
    ///
    /// Runs after the handshake and before the first request, on every
    /// connection this client opens — including the ones it re-opens after a
    /// transport error, so a reconnect cannot end up as an unauthenticated
    /// session.
    pub fn authenticator(mut self, authenticator: Arc<dyn ClientAuthenticator>) -> Self {
        self.authenticator = Some(authenticator);
        self
    }

    /// How long the credential exchange may take. Default 10s.
    pub fn auth_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.auth_timeout = timeout;
        self
    }

    /// Also offer TLS 1.2. Off by default; see
    /// [`crate::server::TcpServerBuilder::allow_tls12`].
    pub fn allow_tls12(mut self, allow: bool) -> Self {
        self.allow_tls12 = allow;
        self
    }

    /// How this client decides whether to believe the server it reached.
    ///
    /// Takes precedence over [`TcpClientBuilder::verify_certificate`], which
    /// only chooses between the public web PKI and believing anything.
    pub fn trust(mut self, trust: Trust) -> Self {
        self.trust = Some(trust);
        self
    }

    /// The certificate this client presents when the server asks for one.
    ///
    /// Required against a server configured with
    /// [`crate::tls::ClientAuth`]: without it the handshake fails, because the
    /// server has been told not to accept anonymous callers.
    pub fn identity(mut self, identity: ClientIdentity) -> Self {
        self.identity = Some(identity);
        self
    }

    pub fn timeout(mut self, timeout: std::time::Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn no_timeout(mut self) -> Self {
        self.timeout = None;
        self
    }

    pub fn verify_certificate(mut self, verify: bool) -> Self {
        self.verify_certificate = verify;
        self
    }

    pub async fn build(self) -> Result<TcpClient, RastcpError> {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        // `trust` when given; otherwise the older boolean, which chose between
        // the public roots and believing anything.
        let trust = self.trust.unwrap_or({
            if self.verify_certificate { Trust::WebPki } else { Trust::AcceptAny }
        });

        if self.allow_tls12 && self.authenticator.is_some() {
            return Err(RastcpError::AuthenticationError(
                "an authenticated client cannot allow TLS 1.2: the channel binding that keeps \
                 the credential exchange from being relayed is a TLS 1.3 exporter"
                    .to_string(),
            ));
        }

        // TLS 1.3 only unless an operator asked otherwise.
        let versions: &[&'static rustls::SupportedProtocolVersion] = if self.allow_tls12 {
            &[&rustls::version::TLS13, &rustls::version::TLS12]
        } else {
            &[&rustls::version::TLS13]
        };
        let builder = ClientConfig::builder_with_protocol_versions(versions);
        let wants_client_cert = match crate::tls::server_verifier(&trust)? {
            // A verifier of our own: pinning.
            Some(verifier) => builder.dangerous().with_custom_certificate_verifier(verifier),
            // Ordinary verification against a root store, or none at all.
            None => match &trust {
                Trust::WebPki => builder.with_webpki_roots(),
                Trust::RootCa(_) => {
                    let roots = crate::tls::trust_roots(&trust)?
                        .expect("RootCa always yields a root store");
                    builder.with_root_certificates(roots)
                }
                Trust::AcceptAny => builder
                    .dangerous()
                    .with_custom_certificate_verifier(Arc::new(AcceptAnyCertificate)),
                Trust::Pinned(_) => unreachable!("pinning always builds a verifier"),
            },
        };

        let mut client_config = match self.identity {
            Some(identity) => wants_client_cert
                .with_client_auth_cert(identity.certs, identity.key)
                .map_err(|e| {
                    RastcpError::CertificateLoading(format!(
                        "the client certificate and key do not go together: {}",
                        e
                    ))
                })?,
            None => wants_client_cert.with_no_client_auth(),
        };

        // 0-RTT stays off: early data is replayable, and a replayed request on
        // a database protocol is a repeated write. Resumption itself is kept —
        // it is the part that saves a round trip without the replay window.
        client_config.enable_early_data = false;
        client_config.resumption = Resumption::in_memory_sessions(128);
        client_config.enable_sni = true;
        client_config.alpn_protocols = vec![ALPN_PROTOCOL.to_vec()];
        // Writing session keys to disk is a debugging aid that hands anyone who
        // can read that file the ability to decrypt every recorded session, so
        // it is behind a feature rather than an environment variable alone.
        #[cfg(feature = "tls-keylog")]
        {
            client_config.key_log = Arc::new(rustls::KeyLogFile::new());
        }
        let config = client_config;

        // Get server name from address
        let server_name = ServerName::try_from(
            self.addr
                .split(':')
                .next()
                .ok_or_else(|| RastcpError::CertificateLoading("Invalid address format".into()))?,
        )
        .map_err(|_| RastcpError::CertificateLoading("Invalid server name".into()))?
        .to_owned();

        Ok(TcpClient {
            addr: self.addr,
            connector: TlsConnector::from(Arc::new(config)),
            server_name,
            connection: None,
            timeout: self.timeout,
            authenticator: self.authenticator,
            auth_timeout: self.auth_timeout,
        })
    }
}

impl TcpClient {
    pub async fn new(addr: &str) -> Result<Self, RastcpError> {
        TcpClientBuilder::new(addr).build().await
    }

    pub async fn connect(&mut self) -> Result<(), RastcpError> {
        if self.connection.is_none() {
            let stream = match self.timeout {
                Some(timeout) => tokio::time::timeout(timeout, TcpStream::connect(&self.addr))
                    .await
                    .map_err(|_| RastcpError::ConnectionTimeout)??,
                None => TcpStream::connect(&self.addr).await?,
            };

            // Disable Nagle's algorithm: small request/response RPCs over a
            // long-lived connection otherwise hit Linux's ~40 ms
            // delayed-ACK timer between turns.
            let _ = stream.set_nodelay(true);

            info!("Connected to {}", self.addr);

            let mut tls_stream = match self.timeout {
                Some(timeout) => {
                    tokio::time::timeout(timeout, self.connector.connect(self.server_name.clone(), stream))
                        .await
                        .map_err(|_| RastcpError::TlsHandshakeTimeout)??
                }
                None => self
                    .connector
                    .connect(self.server_name.clone(), stream)
                    .await?,
            };
            debug!("TLS connection established");

            self.authenticate(&mut tls_stream).await?;

            self.connection = Some(tls_stream);
        }
        Ok(())
    }

    /// Presents credentials, if this client was given any.
    ///
    /// Deliberately part of `connect` rather than something a caller does
    /// afterwards: a client that reconnects after a transport error would
    /// otherwise resume sending requests on a fresh, unauthenticated session.
    async fn authenticate(
        &self,
        tls_stream: &mut TlsStream<TcpStream>,
    ) -> Result<(), RastcpError> {
        let Some(authenticator) = self.authenticator.as_ref() else {
            return Ok(());
        };

        let binding = crate::session::channel_binding_from(tls_stream.get_ref().1)?;
        let peer = tls_stream
            .get_ref()
            .0
            .peer_addr()
            .unwrap_or_else(|_| std::net::SocketAddr::from(([0, 0, 0, 0], 0)));

        let mut channel = AuthChannel::new(tls_stream, self.auth_timeout, binding, peer);

        match tokio::time::timeout(self.auth_timeout, authenticator.authenticate(&mut channel))
            .await
        {
            Ok(result) => result,
            Err(_) => Err(RastcpError::ConnectionTimeout),
        }
    }

    // Modified send method to use the existing connection
    pub async fn send(&mut self, data: Vec<u8>) -> Result<Vec<u8>, RastcpError> {
        self.send_borrowed(&data).await
    }

    /// Sends a request the caller still owns, and returns the response.
    ///
    /// The same exchange as [`TcpClient::send`], without taking the request
    /// buffer. A caller that sends repeatedly — a replication link shipping a
    /// batch every few hundred microseconds — can then build each request in
    /// one buffer it keeps, instead of allocating and growing a fresh one per
    /// request up to whatever its largest batch needs.
    pub async fn send_borrowed(&mut self, data: &[u8]) -> Result<Vec<u8>, RastcpError> {
        // Connect if not connected
        if !self.is_connected() {
            self.connect().await?;
        }

        let io_result = {
            let stream = self.connection.as_mut().ok_or(RastcpError::NotConnected)?;

            if let Some(timeout) = self.timeout {
                write_message_timeout(stream, data, timeout).await?;
                debug!("Sent {} bytes", data.len());
                read_message_timeout(stream, timeout).await
            } else {
                write_message(stream, data).await?;
                debug!("Sent {} bytes", data.len());
                read_message(stream).await
            }
        };

        match io_result {
            Ok(response) => {
                debug!("Received {} bytes response", response.len());
                Ok(response)
            }
            Err(err) => {
                self.connection = None;
                Err(err)
            }
        }
    }

    // Add a close method to explicitly close the connection
    pub async fn close(&mut self) -> Result<(), RastcpError> {
        self.connection = None;
        Ok(())
    }

    // Add a is_connected method to check connection status
    pub fn is_connected(&self) -> bool {
        self.connection.is_some()
    }

    // Add a reconnect method to force a new connection
    pub async fn reconnect(&mut self) -> Result<(), RastcpError> {
        self.close().await?;
        self.connect().await
    }
}
