//! Does the authentication actually authenticate?
//!
//! Every test here answers that by trying to get in without the right
//! credential and requiring that it fail. A test that only checks the happy
//! path would pass just as well against a verifier that returns `Ok` for
//! everything, which is precisely the bug this machinery exists to remove.

use std::time::Duration;

use rastcp::cert::generate_self_signed_cert;
use rastcp::client::TcpClientBuilder;
use rastcp::server::TcpServerBuilder;
use rastcp::tls::{ClientAuth, ClientIdentity, Fingerprint, Trust, fingerprint};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

/// A port nothing is listening on.
fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("a loopback port");
    let port = listener.local_addr().expect("a bound address").port();
    drop(listener);
    port
}

struct Identity {
    certs: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
}

impl Identity {
    fn generate() -> Identity {
        let (certs, key) = generate_self_signed_cert().expect("a self-signed certificate");
        Identity { certs, key }
    }

    fn fingerprint(&self) -> Fingerprint {
        fingerprint(&self.certs[0])
    }

    fn as_client(&self) -> ClientIdentity {
        ClientIdentity {
            certs: self.certs.clone(),
            key: self.key.clone_key(),
        }
    }
}

/// Starts an echo server on `port` with the given identity and client policy.
async fn serve(port: u16, identity: &Identity, client_auth: ClientAuth) {
    let server = TcpServerBuilder::new("127.0.0.1", port)
        .with_certificates(identity.certs.clone(), identity.key.clone_key())
        .client_auth(client_auth)
        .build()
        .await
        .expect("the server should build");

    tokio::spawn(async move {
        let _ = server.run(|data| async move { data }).await;
    });

    // Let the accept loop reach the listener before anyone dials it.
    tokio::time::sleep(Duration::from_millis(150)).await;
}

/// Tries one exchange, returning whether it got through.
async fn exchange(port: u16, trust: Trust, identity: Option<ClientIdentity>) -> bool {
    let mut builder = TcpClientBuilder::new(&format!("127.0.0.1:{}", port))
        .timeout(Duration::from_secs(5))
        .trust(trust);
    if let Some(identity) = identity {
        builder = builder.identity(identity);
    }

    let Ok(mut client) = builder.build().await else {
        return false;
    };
    client.send(b"ping".to_vec()).await.map(|r| r == b"ping").unwrap_or(false)
}

#[tokio::test]
async fn a_pinned_client_reaches_the_server_it_pinned() {
    let port = free_port();
    let server = Identity::generate();
    serve(port, &server, ClientAuth::None).await;

    assert!(
        exchange(port, Trust::Pinned(vec![server.fingerprint()]), None).await,
        "the pinned fingerprint is the server's own, so this must connect"
    );
}

/// The test that matters: a pin has to *reject* something.
#[tokio::test]
async fn a_pinned_client_refuses_a_server_it_did_not_pin() {
    let port = free_port();
    let server = Identity::generate();
    serve(port, &server, ClientAuth::None).await;

    // A certificate generated the same way, but not this server's — which is
    // exactly what an impersonating middlebox would present.
    let impostor = Identity::generate();

    assert!(
        !exchange(port, Trust::Pinned(vec![impostor.fingerprint()]), None).await,
        "a certificate that is not the pinned one must not be accepted"
    );
}

#[tokio::test]
async fn a_pinned_client_accepts_any_of_several_pins() {
    let port = free_port();
    let server = Identity::generate();
    serve(port, &server, ClientAuth::None).await;

    let other = Identity::generate();
    let pins = vec![other.fingerprint(), server.fingerprint()];

    assert!(
        exchange(port, Trust::Pinned(pins), None).await,
        "a cluster lists every member's fingerprint, so any of them must do"
    );
}

#[tokio::test]
async fn a_server_requiring_a_certificate_refuses_an_anonymous_caller() {
    let port = free_port();
    let server = Identity::generate();
    let client = Identity::generate();
    serve(port, &server, ClientAuth::RequirePinned(vec![client.fingerprint()])).await;

    assert!(
        !exchange(port, Trust::Pinned(vec![server.fingerprint()]), None).await,
        "the server was told to require a certificate, so an anonymous caller must be turned away"
    );
}

#[tokio::test]
async fn a_server_requiring_a_certificate_accepts_the_one_it_pinned() {
    let port = free_port();
    let server = Identity::generate();
    let client = Identity::generate();
    serve(port, &server, ClientAuth::RequirePinned(vec![client.fingerprint()])).await;

    assert!(
        exchange(
            port,
            Trust::Pinned(vec![server.fingerprint()]),
            Some(client.as_client())
        )
        .await,
        "both ends hold the other's pin, so this is the configuration that should work"
    );
}

#[tokio::test]
async fn a_server_requiring_a_certificate_refuses_an_unpinned_one() {
    let port = free_port();
    let server = Identity::generate();
    let client = Identity::generate();
    serve(port, &server, ClientAuth::RequirePinned(vec![client.fingerprint()])).await;

    // A caller with a perfectly valid certificate that this cluster has never
    // heard of.
    let outsider = Identity::generate();

    assert!(
        !exchange(
            port,
            Trust::Pinned(vec![server.fingerprint()]),
            Some(outsider.as_client())
        )
        .await,
        "holding *a* certificate is not the same as holding an accepted one"
    );
}

/// Both directions at once, which is how a cluster is meant to be configured.
#[tokio::test]
async fn mutual_pinning_authenticates_both_ends() {
    let port = free_port();
    let server = Identity::generate();
    let client = Identity::generate();
    serve(port, &server, ClientAuth::RequirePinned(vec![client.fingerprint()])).await;

    assert!(
        exchange(
            port,
            Trust::Pinned(vec![server.fingerprint()]),
            Some(client.as_client())
        )
        .await
    );

    // Swapping either half breaks it.
    let stranger = Identity::generate();
    assert!(
        !exchange(
            port,
            Trust::Pinned(vec![stranger.fingerprint()]),
            Some(client.as_client())
        )
        .await,
        "wrong server pin"
    );
    assert!(
        !exchange(
            port,
            Trust::Pinned(vec![server.fingerprint()]),
            Some(stranger.as_client())
        )
        .await,
        "wrong client certificate"
    );
}

/// The escape hatch still works, and is still the thing that authenticates
/// nothing.
#[tokio::test]
async fn accept_any_connects_to_anything() {
    let port = free_port();
    let server = Identity::generate();
    serve(port, &server, ClientAuth::None).await;

    assert!(exchange(port, Trust::AcceptAny, None).await);
    assert!(!Trust::AcceptAny.authenticates());
}
