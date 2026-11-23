//! What the transport insists on before anyone gets to authenticate.
//!
//! These are the defaults, not the options: a deployment that never configures
//! anything still gets TLS 1.3, no 0-RTT, and a named protocol. Each test
//! therefore builds a server the ordinary way and then tries to reach it the
//! wrong way.

use std::sync::Arc;
use std::time::Duration;

use rastcp::client::TcpClientBuilder;
use rastcp::server::TcpServerBuilder;
use rastcp::session::{AuthChannel, AuthFuture, Principal, ServerAuthenticator};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, SignatureScheme};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

/// A loopback listener on a port of the kernel's choosing, and that port.
///
/// The server is handed the listener itself, never the number to bind again:
/// a port that is let go can be given to a test running in parallel before the
/// server binds it, which fails one of them or connects a client to the wrong
/// server.
fn loopback() -> (std::net::TcpListener, u16) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("a loopback port");
    let port = listener.local_addr().expect("a bound address").port();
    (listener, port)
}

/// An authenticator that lets everyone in, for tests about the transport
/// rather than about credentials.
#[derive(Debug)]
struct AcceptEveryone;

impl ServerAuthenticator for AcceptEveryone {
    fn authenticate<'a>(&'a self, channel: &'a mut AuthChannel<'_>) -> AuthFuture<'a, Principal> {
        Box::pin(async move {
            let _ = channel.recv().await?;
            channel.send(b"ok").await?;
            Ok(Principal::new("anyone", false))
        })
    }
}

#[derive(Debug)]
struct AcceptAnyCertificate;

impl ServerCertVerifier for AcceptAnyCertificate {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::ED25519,
            SignatureScheme::RSA_PKCS1_SHA256,
        ]
    }
}

/// Starts an echo server with the crate's defaults.
async fn serve_default(listener: std::net::TcpListener) {
    let server = TcpServerBuilder::from_listener(listener)
        .expect("a bound listener")
        .build()
        .await
        .expect("the server should build");

    tokio::spawn(async move {
        let _ = server.run(|data| async move { data }).await;
    });

    tokio::time::sleep(Duration::from_millis(150)).await;
}

/// A client that offers nothing but TLS 1.2 — what a peer stuck on an older
/// stack, or one being steered into a downgrade, would send.
async fn connect_with_tls12_only(port: u16, alpn: &[&[u8]]) -> Result<(), String> {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let mut config =
        ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS12])
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(AcceptAnyCertificate))
            .with_no_client_auth();
    config.alpn_protocols = alpn.iter().map(|p| p.to_vec()).collect();

    let stream = TcpStream::connect(("127.0.0.1", port))
        .await
        .map_err(|e| e.to_string())?;
    let name = ServerName::try_from("127.0.0.1").expect("a valid name");

    TlsConnector::from(Arc::new(config))
        .connect(name, stream)
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

#[tokio::test]
async fn a_tls12_only_client_cannot_connect_to_a_default_server() {
    let (listener, port) = loopback();
    serve_default(listener).await;

    let err = connect_with_tls12_only(port, &[rastcp::ALPN_PROTOCOL])
        .await
        .expect_err("TLS 1.2 must not be negotiated by default");
    assert!(
        err.to_lowercase().contains("protocol version")
            || err.to_lowercase().contains("handshake")
            || err.to_lowercase().contains("alert"),
        "unexpected failure: {}",
        err
    );
}

#[tokio::test]
async fn a_default_client_and_server_still_talk_to_each_other() {
    let (listener, port) = loopback();
    serve_default(listener).await;

    let mut client = TcpClientBuilder::new(&format!("127.0.0.1:{}", port))
        .timeout(Duration::from_secs(5))
        .build()
        .await
        .expect("the client should build");

    let response = client.send(b"ping".to_vec()).await.expect("an echo");
    assert_eq!(response, b"ping");
}

/// The protocol identifier is part of the handshake, so a peer that will not
/// speak it is turned away rather than served bytes it might feed elsewhere.
#[tokio::test]
async fn a_client_offering_another_protocol_is_refused() {
    let (listener, port) = loopback();
    serve_default(listener).await;

    assert!(
        connect_with_tls12_only(port, &[b"h2"]).await.is_err(),
        "a client that will not speak rastcp/1 must not be served"
    );
}

/// Allowing TLS 1.2 would silently remove the exporter the credential exchange
/// binds itself to, so the combination is refused at build time rather than
/// running unbound.
#[tokio::test]
async fn an_authenticated_server_refuses_to_allow_tls12() {
    let err = match TcpServerBuilder::new("127.0.0.1", 0)
        .authenticator(Arc::new(AcceptEveryone))
        .allow_tls12(true)
        .build()
        .await
    {
        Ok(_) => panic!("the combination must not build"),
        Err(e) => e,
    };

    assert!(err.to_string().contains("TLS 1.2"), "{}", err);
}

#[tokio::test]
async fn an_authenticated_client_refuses_to_allow_tls12() {
    #[derive(Debug)]
    struct SaysNothing;
    impl rastcp::session::ClientAuthenticator for SaysNothing {
        fn authenticate<'a>(
            &'a self,
            _channel: &'a mut AuthChannel<'_>,
        ) -> AuthFuture<'a, ()> {
            Box::pin(async { Ok(()) })
        }
    }

    let err = match TcpClientBuilder::new("127.0.0.1:1")
        .authenticator(Arc::new(SaysNothing))
        .allow_tls12(true)
        .build()
        .await
    {
        Ok(_) => panic!("the combination must not build"),
        Err(e) => e,
    };

    assert!(err.to_string().contains("TLS 1.2"), "{}", err);
}

/// Both ends of one connection have to derive the same channel binding, or a
/// bound exchange could never complete; and two connections must not share one,
/// or binding would prove nothing.
#[tokio::test]
async fn each_connection_has_its_own_channel_binding() {
    use std::sync::Mutex;

    static SEEN: Mutex<Vec<[u8; 32]>> = Mutex::new(Vec::new());

    #[derive(Debug)]
    struct RecordsBinding;

    impl ServerAuthenticator for RecordsBinding {
        fn authenticate<'a>(
            &'a self,
            channel: &'a mut AuthChannel<'_>,
        ) -> AuthFuture<'a, Principal> {
            Box::pin(async move {
                let client_view = channel.recv().await?;
                let ours = *channel.channel_binding();
                assert_eq!(
                    client_view.as_slice(),
                    ours.as_slice(),
                    "both ends of one connection must derive the same binding"
                );
                SEEN.lock().expect("a poisoned lock").push(ours);
                channel.send(b"ok").await?;
                Ok(Principal::new("anyone", false))
            })
        }
    }

    #[derive(Debug)]
    struct SendsBinding;

    impl rastcp::session::ClientAuthenticator for SendsBinding {
        fn authenticate<'a>(&'a self, channel: &'a mut AuthChannel<'_>) -> AuthFuture<'a, ()> {
            Box::pin(async move {
                let binding = *channel.channel_binding();
                channel.send(&binding).await?;
                channel.recv().await.map(|_| ())
            })
        }
    }

    let (listener, port) = loopback();
    let server = TcpServerBuilder::from_listener(listener)
        .expect("a bound listener")
        .authenticator(Arc::new(RecordsBinding))
        .build()
        .await
        .expect("the server should build");

    tokio::spawn(async move {
        let _ = server.run(|data| async move { data }).await;
    });
    tokio::time::sleep(Duration::from_millis(150)).await;

    for _ in 0..2 {
        let mut client = TcpClientBuilder::new(&format!("127.0.0.1:{}", port))
            .timeout(Duration::from_secs(5))
            .authenticator(Arc::new(SendsBinding))
            .build()
            .await
            .expect("the client should build");
        client.send(b"ping".to_vec()).await.expect("an echo");
    }

    let seen = SEEN.lock().expect("a poisoned lock");
    assert_eq!(seen.len(), 2, "both connections should have been recorded");
    assert_ne!(
        seen[0], seen[1],
        "two connections sharing a binding would make binding prove nothing"
    );
}
