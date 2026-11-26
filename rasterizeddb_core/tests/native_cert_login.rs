//! Certificate login over the native protocol (`--rdb-auth cert`): the real
//! listener, the real client, certificates signed at test time. A process of
//! its own because the client CA is set once per process.

// The spawned server future nests past the default limit, as in
// native_auth_tests.
#![recursion_limit = "256"]

use std::time::Duration;

use rasterizeddb_client::{Client, Login, Options};
use rasterizeddb_core::auth::{AuthMethod, ensure_role_catalogue, execute_role_statement, parse_role_statement};
use rasterizeddb_core::core::database::{Database, NativeListen};
use rasterizeddb_core::native_auth::NativeAuthConfig;
use rcgen::{BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair};
use rclite::Arc;

struct Ca {
    params: CertificateParams,
    key: KeyPair,
    pem: String,
}

fn ca(name: &str) -> Ca {
    let key = KeyPair::generate().unwrap();
    let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.distinguished_name.push(DnType::CommonName, name);
    let pem = params.self_signed(&key).unwrap().pem();
    Ca { params, key, pem }
}

/// A client certificate for common name `cn` signed by `ca`: (cert, key) PEM.
fn client(ca: &Ca, cn: &str) -> (String, String) {
    let key = KeyPair::generate().unwrap();
    let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
    params.distinguished_name.push(DnType::CommonName, cn);
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    let issuer = Issuer::from_params(&ca.params, &ca.key);
    (params.signed_by(&key, &issuer).unwrap().pem(), key.serialize_pem())
}

fn write(dir: &std::path::Path, name: &str, text: &str) -> String {
    let p = dir.join(name);
    std::fs::write(&p, text).unwrap();
    p.to_str().unwrap().to_string()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_certificate_logs_in_as_the_role_it_names_and_nothing_else_does() {
    let _ = rasterizeddb_core::MAX_PERMITS_THREADS.get_or_init(|| 4);
    let _ = rasterizeddb_core::BATCH_SIZE.get_or_init(|| 1024);
    let dir = tempfile::tempdir().unwrap();

    let good = ca("rdb test ca");
    let other = ca("someone else's ca");
    rasterizeddb_core::pgwire::client_cert::set_ca(write(dir.path(), "ca.pem", &good.pem));
    let (carol_cert, carol_key) = client(&good, "carol");
    let (mallory_cert, mallory_key) = client(&other, "carol");
    let carol = (write(dir.path(), "carol.pem", &carol_cert), write(dir.path(), "carol.key", &carol_key));
    let mallory = (write(dir.path(), "mallory.pem", &mallory_cert), write(dir.path(), "mallory.key", &mallory_key));

    let location = dir.path().join("db");
    let database = Arc::new(Database::new_with_backend(location.to_str().unwrap(), true).await);
    ensure_role_catalogue(&database, "alice", Some("hunter2"), AuthMethod::ScramSha256).await.unwrap();
    let statement = parse_role_statement("CREATE ROLE carol LOGIN").unwrap().unwrap();
    execute_role_statement(&database, statement, AuthMethod::ScramSha256).await.unwrap();

    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let listen = NativeListen {
        addr: "127.0.0.1".to_string(),
        port,
        tcp: true,
        socket: None,
        request_timeout: None,
        certificate: None,
    };
    let auth = NativeAuthConfig { method: AuthMethod::Cert, ..NativeAuthConfig::default() };
    let server = tokio::spawn(Database::start_db_with_auth_on(database, auth, listen));
    for _ in 0..200 {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() || server.is_finished() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(!server.is_finished(), "the listener did not start: {:?}", server.await);
    let addr = format!("127.0.0.1:{port}");

    // The certificate names carol, and the CA signed it: in, as carol.
    let c = Client::connect_with_certificate(&addr, Options::default(), "carol", &carol.0, &carol.1)
        .await
        .expect("carol's certificate logs in as carol");
    let rows = c.run("SELECT current_user").await.unwrap();
    assert_eq!(rows.get(0).unwrap().get::<String>(0).unwrap(), "carol");

    // The same certificate does not log in as anyone else.
    assert!(
        Client::connect_with_certificate(&addr, Options::default(), "alice", &carol.0, &carol.1).await.is_err(),
        "carol's certificate logged in as alice"
    );
    // A certificate for carol from another CA is refused in the handshake.
    assert!(
        Client::connect_with_certificate(&addr, Options::default(), "carol", &mallory.0, &mallory.1).await.is_err(),
        "a certificate from another CA got in"
    );
    // No certificate at all, even with alice's right password, is refused.
    let password = Options { login: Login::password("alice", "hunter2"), ..Options::default() };
    assert!(Client::connect(&addr, password).await.is_err(), "a caller with no certificate got in");
    server.abort();
}
