use log::{debug, info};
use rustls::client::Resumption;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, Error, SignatureScheme};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, ready};
use tokio::io::AsyncWrite;
use tokio::net::TcpStream;
use tokio_rustls::{TlsConnector, client::TlsStream};

use crate::mux::MuxClient;
use crate::{ALPN_MULTIPLEXED, ALPN_PROTOCOL};
use crate::config::ConfigBuilderExt;
use crate::error::RastcpError;
use crate::frame::{FrameReader, FrameWriter};
use crate::interrupt::Deadline;
use crate::session::{AuthChannel, ClientAuthenticator};
use crate::tls::{ClientIdentity, Trust};

/// How long the credential exchange may take, when there is one.
const DEFAULT_AUTH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// How many requests a multiplexed client keeps in flight unless told
/// otherwise. Half the server's default, so two such clients sharing a server
/// connection limit still fit.
const DEFAULT_MAX_IN_FLIGHT: usize = 128;

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
    max_message_size: usize,
    max_in_flight: usize,
    reconnect_when_idle_closed: bool,
}

pub struct TcpClient {
    dialer: Dialer,
    connection: Option<Connection>,
}

/// Everything needed to open an authenticated connection to one server, and
/// the limits that apply to it once open. What a builder turns into, and what
/// a client keeps so it can open the connection again.
pub(crate) struct Dialer {
    addr: String,
    connector: TlsConnector,
    server_name: ServerName<'static>,
    pub(crate) timeout: Option<std::time::Duration>,
    authenticator: Option<Arc<dyn ClientAuthenticator>>,
    auth_timeout: std::time::Duration,
    pub(crate) max_message_size: usize,
    /// Only a multiplexed client has more than one request in flight.
    pub(crate) max_in_flight: usize,
    /// See [`TcpClientBuilder::reconnect_when_idle_closed`].
    reconnect_when_idle_closed: bool,
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
            max_message_size: crate::common::DEFAULT_MAX_MESSAGE_SIZE,
            max_in_flight: DEFAULT_MAX_IN_FLIGHT,
            reconnect_when_idle_closed: false,
        }
    }

    /// Whether a client that finds its idle connection closed opens another
    /// and sends on that, instead of sending into the closed one and failing.
    /// Off by default.
    ///
    /// A server closes a connection that has been quiet for its request
    /// timeout. With this on, a connection unused for 100 ms is looked at
    /// before a request is put on it, and replaced if the server has let go
    /// of it; the request is sent once, on the new connection, and the caller
    /// never sees the old one fail.
    ///
    /// Turn it on only for a protocol in which every request stands alone. The
    /// new connection is authenticated like any other this client opens, but
    /// nothing else is done to it: a caller that sets its connection up with
    /// requests of its own — a handshake, a session — would find itself
    /// sending on one that had none of that, without having been told. Such a
    /// caller needs the failure, which is what it gets with this off. Has no
    /// effect on a multiplexed client, which always knows when its connection
    /// closes because it is always reading.
    pub fn reconnect_when_idle_closed(mut self, reconnect: bool) -> Self {
        self.reconnect_when_idle_closed = reconnect;
        self
    }

    /// How many requests a multiplexed client keeps in flight at once.
    /// Default 128; further callers wait their turn.
    ///
    /// Keep it at or below the server's own limit
    /// ([`crate::server::TcpServerBuilder::max_in_flight`], default 256):
    /// above that the server stops reading until it catches up, and the
    /// surplus waits in socket buffers instead of here. Has no effect on a
    /// client built with [`TcpClientBuilder::build`].
    pub fn max_in_flight(mut self, requests: usize) -> Self {
        self.max_in_flight = requests.max(1);
        self
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

    /// The largest response this client reads, in bytes. The default is
    /// [`crate::common::DEFAULT_MAX_MESSAGE_SIZE`].
    pub fn max_message_size(mut self, bytes: usize) -> Self {
        self.max_message_size = bytes;
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
        Ok(TcpClient {
            dialer: self.dialer(ALPN_PROTOCOL)?,
            connection: None,
        })
    }

    /// Builds a client many tasks can share, speaking the multiplexed
    /// protocol. See [`MuxClient`].
    ///
    /// Must be called, and the client used, inside a Tokio runtime: the
    /// connection is driven by a task of its own.
    pub async fn build_multiplexed(self) -> Result<MuxClient, RastcpError> {
        Ok(MuxClient::new(self.dialer(ALPN_MULTIPLEXED)?))
    }

    /// The TLS configuration and limits this builder describes, offering
    /// `alpn` as the protocol to speak.
    fn dialer(self, alpn: &[u8]) -> Result<Dialer, RastcpError> {
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
        client_config.alpn_protocols = vec![alpn.to_vec()];
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

        Ok(Dialer {
            addr: self.addr,
            connector: TlsConnector::from(Arc::new(config)),
            server_name,
            timeout: self.timeout,
            authenticator: self.authenticator,
            auth_timeout: self.auth_timeout,
            max_message_size: self.max_message_size,
            max_in_flight: self.max_in_flight,
            reconnect_when_idle_closed: self.reconnect_when_idle_closed,
        })
    }
}

/// An open, authenticated connection and everything that belongs to it.
///
/// One struct so that all of it is dropped together. Bytes read ahead, a
/// request half written and a timer armed for the old socket are all wrong for
/// the connection that replaces it.
struct Connection {
    stream: TlsStream<TcpStream>,
    reader: FrameReader,
    writer: FrameWriter,
    /// The one timer behind every timeout on this connection. Made with it,
    /// because both belong to the runtime that opened it; `None` when the
    /// client has no timeout.
    deadline: Option<Deadline>,
    /// When a request last finished on this connection.
    last_used: std::time::Instant,
}

/// How long a connection sits unused before it is checked, ahead of the next
/// request, for having been closed. A busy connection is never checked: the
/// check costs a trip through the TLS stream, and a server does not close a
/// connection it heard from a moment ago.
const CHECK_WHEN_IDLE_FOR: std::time::Duration = std::time::Duration::from_millis(100);

/// Runs `wait` under `deadline`, if there is one.
async fn within<T, F>(deadline: &mut Option<Deadline>, wait: F) -> Result<T, RastcpError>
where
    F: Future<Output = Result<T, RastcpError>>,
{
    match deadline {
        Some(deadline) => deadline.run(wait).await.unwrap_or(Err(RastcpError::ConnectionTimeout)),
        None => wait.await,
    }
}

impl Connection {
    /// Whether the server is known to have let go of this connection.
    ///
    /// A server closes a connection that has been quiet for its request
    /// timeout. The close arrives while nobody is reading, so without this the
    /// client would learn of it only by sending its next request into the
    /// closed connection and failing — a failure that says nothing about
    /// whether the request ran. Looking first costs nothing that matters and
    /// turns that into a reconnect before anything is sent.
    ///
    /// Anything that can be read here is a reason to stop using the
    /// connection: the end of the stream, an error, or a frame nobody asked
    /// for, after which the two ends no longer agree on whose turn it is.
    async fn has_gone(&mut self, max_message_size: usize) -> bool {
        self.last_used.elapsed() >= CHECK_WHEN_IDLE_FOR
            && self
                .reader
                .frame_now(&mut self.stream, max_message_size)
                .await
                .is_some()
    }

    /// Sends one request and reads its response. Each half gets the whole
    /// timeout, as it did when each had a timer of its own.
    async fn exchange(
        &mut self,
        request: &[u8],
        max_message_size: usize,
    ) -> Result<Vec<u8>, RastcpError> {
        let Self {
            stream,
            reader,
            writer,
            deadline,
            last_used,
        } = self;

        within(deadline, async {
            if writer.queue(request)? {
                writer.flush(stream).await
            } else {
                writer.send_large(stream, request).await
            }
        })
        .await?;
        debug!("Sent {} bytes", request.len());

        let response = within(deadline, reader.frame(stream, max_message_size)).await?;
        *last_used = std::time::Instant::now();
        Ok(response)
    }

    /// Sends every request and reads every response, both at once.
    ///
    /// Writing the whole batch before reading anything would deadlock on a
    /// large one: the server stops reading when its replies have nowhere to
    /// go, this side stops writing when the server stops reading, and neither
    /// moves again. So responses are taken off the connection while requests
    /// are still going onto it.
    async fn exchange_batch<B: AsRef<[u8]>>(
        &mut self,
        requests: &[B],
        max_message_size: usize,
    ) -> Result<Vec<Vec<u8>>, RastcpError> {
        let Self {
            stream,
            reader,
            writer,
            deadline,
            last_used,
        } = self;

        let mut pump = Pump {
            requests,
            next: 0,
            writer,
            written: 0,
            large: None,
            flushed: false,
            reader,
            responses: Vec::with_capacity(requests.len()),
            max_message_size,
        };

        // One wait per response rather than one for the batch: the timeout
        // bounds how long the server may go quiet, as it does for requests
        // sent one at a time, not how long a large batch may take in all.
        while pump.responses.len() < requests.len() {
            let before = pump.responses.len();
            within(
                deadline,
                std::future::poll_fn(|cx| pump.poll_progress(stream, cx, before)),
            )
            .await?;
        }

        *last_used = std::time::Instant::now();
        Ok(pump.responses)
    }
}

/// The state of one batch in flight: what is still to be written and what has
/// come back.
struct Pump<'a, B> {
    requests: &'a [B],
    /// The first request not yet handed to the writer.
    next: usize,
    writer: &'a mut FrameWriter,
    /// How much of what the writer holds has reached the stream.
    written: usize,
    /// A request too large to copy, and how much of it has been written.
    large: Option<(&'a [u8], usize)>,
    flushed: bool,
    reader: &'a mut FrameReader,
    responses: Vec<Vec<u8>>,
    max_message_size: usize,
}

impl<B: AsRef<[u8]>> Pump<'_, B> {
    /// Writes as much as the stream takes without waiting. Pending only when
    /// the stream is; ready once every request has been written and flushed.
    fn poll_send(
        &mut self,
        stream: &mut TlsStream<TcpStream>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), RastcpError>> {
        loop {
            let queued = self.writer.queued();
            if self.written < queued.len() {
                let n = ready!(Pin::new(&mut *stream).poll_write(cx, &queued[self.written..]))?;
                if n == 0 {
                    return Poll::Ready(Err(std::io::Error::from(std::io::ErrorKind::WriteZero).into()));
                }
                self.written += n;
                continue;
            }
            self.writer.clear();
            self.written = 0;

            if let Some((payload, at)) = self.large.as_mut() {
                if *at < payload.len() {
                    let n = ready!(Pin::new(&mut *stream).poll_write(cx, &payload[*at..]))?;
                    if n == 0 {
                        return Poll::Ready(Err(std::io::Error::from(std::io::ErrorKind::WriteZero).into()));
                    }
                    *at += n;
                    continue;
                }
                self.large = None;
            }

            if self.next < self.requests.len() {
                // As many small requests as make one good-sized write, or the
                // prefix of one large one.
                while self.next < self.requests.len() && !self.writer.is_full() {
                    let request = self.requests[self.next].as_ref();
                    self.next += 1;

                    if !self.writer.queue(request)? {
                        self.writer.queue_prefix(request.len())?;
                        self.large = Some((request, 0));
                        break;
                    }
                }
                continue;
            }

            if !self.flushed {
                ready!(Pin::new(&mut *stream).poll_flush(cx))?;
                self.flushed = true;
            }

            return Poll::Ready(Ok(()));
        }
    }

    /// Moves the batch along. Ready once more responses are in than `before`.
    fn poll_progress(
        &mut self,
        stream: &mut TlsStream<TcpStream>,
        cx: &mut Context<'_>,
        before: usize,
    ) -> Poll<Result<(), RastcpError>> {
        // Pending here is not the end of this poll: the stream not taking more
        // is exactly when the responses have to be read.
        let sent = match self.poll_send(stream, cx) {
            Poll::Ready(Ok(())) => true,
            Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
            Poll::Pending => false,
        };

        while self.responses.len() < self.requests.len() {
            match self.reader.poll_frame(stream, cx, self.max_message_size) {
                Poll::Ready(Ok(response)) => self.responses.push(response),
                Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                Poll::Pending => break,
            }
        }

        if self.responses.len() == self.requests.len() && !sent {
            // An answer to a request that has not been sent. Whatever the
            // peer is, it is not following this protocol.
            return Poll::Ready(Err(RastcpError::ProtocolError(
                "the server answered more requests than it was sent".to_string(),
            )));
        }

        if self.responses.len() > before {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
}

impl Dialer {
    /// Opens a connection: TCP, the TLS handshake, then the credential
    /// exchange if this client was given credentials.
    pub(crate) async fn open(&self) -> Result<TlsStream<TcpStream>, RastcpError> {
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
            Some(timeout) => tokio::time::timeout(
                timeout,
                self.connector.connect(self.server_name.clone(), stream),
            )
            .await
            .map_err(|_| RastcpError::TlsHandshakeTimeout)??,
            None => {
                self.connector
                    .connect(self.server_name.clone(), stream)
                    .await?
            }
        };
        debug!("TLS connection established");

        self.authenticate(&mut tls_stream).await?;

        Ok(tls_stream)
    }

    /// Presents credentials, if this client was given any.
    ///
    /// Deliberately part of opening a connection rather than something a
    /// caller does afterwards: a client that reconnects after a transport
    /// error would otherwise resume sending requests on a fresh,
    /// unauthenticated session.
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
}

impl TcpClient {
    pub async fn new(addr: &str) -> Result<Self, RastcpError> {
        TcpClientBuilder::new(addr).build().await
    }

    pub async fn connect(&mut self) -> Result<(), RastcpError> {
        if self.connection.is_none() {
            let stream = self.dialer.open().await?;

            self.connection = Some(Connection {
                stream,
                reader: FrameReader::new(),
                writer: FrameWriter::new(),
                deadline: self.dialer.timeout.map(Deadline::new),
                last_used: std::time::Instant::now(),
            });
        }
        Ok(())
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
        let max_message_size = self.dialer.max_message_size;
        self.ready(max_message_size).await?;

        let connection = self.connection.as_mut().ok_or(RastcpError::NotConnected)?;

        match connection.exchange(data, max_message_size).await {
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

    /// Sends several requests at once and returns their responses, in order.
    ///
    /// The requests leave together and the server answers them together, so a
    /// batch of small requests costs a handful of system calls on each side
    /// instead of two per request. The server handles them one after another
    /// in the order given, exactly as if each had been sent with
    /// [`TcpClient::send`] after the response to the one before.
    ///
    /// All or nothing from the caller's side: on an error the connection is
    /// dropped and no responses are returned, though the server may already
    /// have carried out any number of the requests. Batch only what is safe to
    /// send again, or what the caller can otherwise reconcile.
    ///
    /// The timeout bounds the wait for each next response, not the batch.
    pub async fn send_batch<B: AsRef<[u8]>>(
        &mut self,
        requests: &[B],
    ) -> Result<Vec<Vec<u8>>, RastcpError> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }

        let max_message_size = self.dialer.max_message_size;
        self.ready(max_message_size).await?;

        let connection = self.connection.as_mut().ok_or(RastcpError::NotConnected)?;

        match connection.exchange_batch(requests, max_message_size).await {
            Ok(responses) => {
                debug!("Received {} responses", responses.len());
                Ok(responses)
            }
            Err(err) => {
                self.connection = None;
                Err(err)
            }
        }
    }

    /// Makes sure there is a connection to send on: opens one if there is
    /// none, and — for a client that asked — replaces one the server closed
    /// while it sat idle.
    async fn ready(&mut self, max_message_size: usize) -> Result<(), RastcpError> {
        if !self.dialer.reconnect_when_idle_closed {
            // The connection is used as it is. If the server has closed it
            // the request fails, and a caller that set the connection up
            // itself learns that it has to do so again.
        } else if let Some(connection) = self.connection.as_mut() {
            if connection.has_gone(max_message_size).await {
                debug!("The idle connection to the server was closed; opening another");
                self.connection = None;
            }
        }

        if !self.is_connected() {
            self.connect().await?;
        }

        Ok(())
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
