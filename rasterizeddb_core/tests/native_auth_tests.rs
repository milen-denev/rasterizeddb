//! Does the native protocol's authentication actually keep anyone out?
//!
//! Every test drives the real client half against the real server half over a
//! real TLS connection, because the two halves agreeing with each other is the
//! only property that matters and unit tests of either alone cannot show it.
//! Most of them try to get in *without* the right credential and require that
//! it fail: a suite of happy paths would pass against an authenticator that
//! returns `Ok` for everyone.

// As the library and the server binary: proving this file's server future
// `Send` runs past the default 128 since the database gained the broker.
#![recursion_limit = "256"]
use std::sync::Arc as StdArc;
use std::time::Duration;

use rastcp::client::TcpClientBuilder;
use rastcp::server::TcpServerBuilder;
use rastcp::session::{ClientAuthenticator, ServerAuthenticator};
use rasterizeddb_core::auth::{
    AuthMethod, ensure_role_catalogue, execute_role_statement, parse_role_statement,
};
use rasterizeddb_core::core::database::Database;
use rasterizeddb_core::native_auth::RoleAuthenticator;
use rclite::Arc;

#[path = "support/native_login.rs"]
mod native_login;
use native_login::PasswordAuthenticator;

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

/// An in-memory database holding the roles a test needs.
async fn database_with_roles(roles: &[&str]) -> Arc<Database> {
    // Unique per process and per call. A released port number named it once,
    // and two tests can be handed the same one.
    static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let location = std::env::temp_dir().join(format!(
        "rdb_auth_test_{}_{}",
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    let database = Arc::new(
        Database::new_with_backend(location.to_str().expect("a UTF-8 temp path"), true).await,
    );

    // `alice` is the bootstrap superuser every test logs in as.
    ensure_role_catalogue(&database, "alice", Some("hunter2"), AuthMethod::ScramSha256)
        .await
        .expect("the role catalogue should be created");

    for sql in roles {
        let statement = parse_role_statement(sql)
            .unwrap_or_else(|| panic!("{} is a role statement", sql))
            .unwrap_or_else(|e| panic!("{}: {}", sql, e));
        execute_role_statement(&database, statement, AuthMethod::ScramSha256)
            .await
            .unwrap_or_else(|e| panic!("{}: {}", sql, e));
    }

    database
}

/// Serves on `listener`, answering every request with the caller's name and
/// whether it is a superuser — so a test can check not only that the caller got
/// in but that it got in as the right person.
async fn serve(listener: std::net::TcpListener, database: Arc<Database>, method: AuthMethod) {
    let authenticator =
        StdArc::new(RoleAuthenticator::new(database, method)) as StdArc<dyn ServerAuthenticator>;

    let server = TcpServerBuilder::from_listener(listener)
        .expect("a bound listener")
        .authenticator(authenticator)
        .build()
        .await
        .expect("the server should build");

    tokio::spawn(async move {
        let _ = server
            .run_with_principal(|principal, _request| async move {
                format!("{}:{}", principal, principal.superuser).into_bytes()
            })
            .await;
    });

    // Let the accept loop reach the listener before anyone dials it.
    tokio::time::sleep(Duration::from_millis(150)).await;
}

/// One exchange as `user`, returning what the server said it saw.
async fn login(port: u16, credentials: Option<(&str, &str)>) -> Result<String, String> {
    let mut builder =
        TcpClientBuilder::new(&format!("127.0.0.1:{}", port)).timeout(Duration::from_secs(5));

    if let Some((user, password)) = credentials {
        builder = builder.authenticator(StdArc::new(PasswordAuthenticator::new(user, password))
            as StdArc<dyn ClientAuthenticator>);
    }

    let mut client = builder.build().await.map_err(|e| e.to_string())?;
    let response = client
        .send(b"SELECT 1".to_vec())
        .await
        .map_err(|e| e.to_string())?;

    Ok(String::from_utf8_lossy(&response).to_string())
}

#[tokio::test]
async fn the_right_password_gets_in_as_the_right_user() {
    let (listener, port) = loopback();
    serve(
        listener,
        database_with_roles(&[]).await,
        AuthMethod::ScramSha256,
    )
    .await;

    assert_eq!(
        login(port, Some(("alice", "hunter2"))).await.unwrap(),
        "alice:true",
        "the bootstrap role is a superuser and the server should see it as one"
    );
}

/// The test that matters most: a wrong password has to be refused.
#[tokio::test]
async fn a_wrong_password_is_refused() {
    let (listener, port) = loopback();
    serve(
        listener,
        database_with_roles(&[]).await,
        AuthMethod::ScramSha256,
    )
    .await;

    let err = login(port, Some(("alice", "not hunter2")))
        .await
        .expect_err("a wrong password must not get in");
    assert!(err.contains("authentication failed"), "{}", err);
}

#[tokio::test]
async fn an_unknown_user_is_refused_and_looks_the_same_as_a_wrong_password() {
    let (listener, port) = loopback();
    serve(
        listener,
        database_with_roles(&[]).await,
        AuthMethod::ScramSha256,
    )
    .await;

    let unknown = login(port, Some(("nobody", "hunter2")))
        .await
        .expect_err("an account that does not exist must not get in");
    let wrong = login(port, Some(("alice", "wrong")))
        .await
        .expect_err("a wrong password must not get in");

    assert_eq!(
        unknown.replace("nobody", "USER"),
        wrong.replace("alice", "USER"),
        "the two failures must be indistinguishable, or the listener enumerates accounts"
    );
}

#[tokio::test]
async fn a_caller_that_presents_nothing_is_refused() {
    let (listener, port) = loopback();
    serve(
        listener,
        database_with_roles(&[]).await,
        AuthMethod::ScramSha256,
    )
    .await;

    assert!(
        login(port, None).await.is_err(),
        "a client that skips the exchange must not be served"
    );
}

#[tokio::test]
async fn a_role_that_may_not_log_in_is_refused_even_with_the_right_password() {
    let (listener, port) = loopback();
    let database = database_with_roles(&["CREATE ROLE reader NOLOGIN PASSWORD 'letmein'"]).await;
    serve(listener, database, AuthMethod::ScramSha256).await;

    assert!(
        login(port, Some(("reader", "letmein"))).await.is_err(),
        "knowing the password is not the same as being allowed to connect"
    );
}

#[tokio::test]
async fn an_ordinary_user_is_not_a_superuser() {
    let (listener, port) = loopback();
    let database = database_with_roles(&["CREATE USER bob PASSWORD 'bobpass'"]).await;
    serve(listener, database, AuthMethod::ScramSha256).await;

    assert_eq!(
        login(port, Some(("bob", "bobpass"))).await.unwrap(),
        "bob:false"
    );
}

/// A role created without a password exists but can authenticate to nothing.
#[tokio::test]
async fn a_role_with_no_password_cannot_authenticate() {
    let (listener, port) = loopback();
    let database = database_with_roles(&["CREATE USER carol"]).await;
    serve(listener, database, AuthMethod::ScramSha256).await;

    assert!(login(port, Some(("carol", ""))).await.is_err());
    assert!(login(port, Some(("carol", "anything"))).await.is_err());
}

/// `trust` still means what it says, and the caller's name still arrives — to
/// a client that has said it will accept being waved through.
#[tokio::test]
async fn trust_accepts_any_password_but_still_reports_the_user() {
    let (listener, port) = loopback();
    serve(listener, database_with_roles(&[]).await, AuthMethod::Trust).await;

    let mut client = TcpClientBuilder::new(&format!("127.0.0.1:{}", port))
        .timeout(Duration::from_secs(5))
        .authenticator(
            StdArc::new(PasswordAuthenticator::new("alice", "whatever").allow_trust())
                as StdArc<dyn ClientAuthenticator>,
        )
        .build()
        .await
        .expect("the client should build");

    assert_eq!(
        String::from_utf8_lossy(&client.send(b"SELECT 1".to_vec()).await.unwrap()),
        "alice:true"
    );
}

/// A client carrying a password must not be talked out of using it: a server
/// that says "you are in" without a challenge has proved nothing about itself.
#[tokio::test]
async fn a_client_with_a_password_refuses_a_server_that_never_challenges_it() {
    let (listener, port) = loopback();
    serve(listener, database_with_roles(&[]).await, AuthMethod::Trust).await;

    let err = login(port, Some(("alice", "hunter2")))
        .await
        .expect_err("an unchallenged accept must not satisfy a password-bearing client");
    assert!(err.contains("never proved who it is"), "{}", err);
}

/// A reconnect must authenticate again rather than resuming as nobody.
#[tokio::test]
async fn a_reconnected_client_authenticates_again() {
    let (listener, port) = loopback();
    serve(
        listener,
        database_with_roles(&[]).await,
        AuthMethod::ScramSha256,
    )
    .await;

    let mut client = TcpClientBuilder::new(&format!("127.0.0.1:{}", port))
        .timeout(Duration::from_secs(5))
        .authenticator(StdArc::new(PasswordAuthenticator::new("alice", "hunter2"))
            as StdArc<dyn ClientAuthenticator>)
        .build()
        .await
        .expect("the client should build");

    assert_eq!(
        String::from_utf8_lossy(&client.send(b"SELECT 1".to_vec()).await.unwrap()),
        "alice:true"
    );

    client
        .reconnect()
        .await
        .expect("the reconnect should authenticate");

    assert_eq!(
        String::from_utf8_lossy(&client.send(b"SELECT 1".to_vec()).await.unwrap()),
        "alice:true",
        "the second connection must be authenticated too"
    );
}

/// The multiplexed transport opens its connection on a path of its own, so
/// the same two facts are checked for it: the caller is asked to prove who it
/// is, and every request on the shared connection is served as that caller —
/// including on the connection it opens after the first is closed.
#[tokio::test]
async fn a_shared_connection_authenticates_and_serves_every_request_as_that_user() {
    let (listener, port) = loopback();
    serve(
        listener,
        database_with_roles(&[]).await,
        AuthMethod::ScramSha256,
    )
    .await;

    let shared = |password: &str| {
        TcpClientBuilder::new(&format!("127.0.0.1:{}", port))
            .timeout(Duration::from_secs(5))
            .authenticator(StdArc::new(PasswordAuthenticator::new("alice", password))
                as StdArc<dyn ClientAuthenticator>)
            .build_multiplexed()
    };

    let wrong = shared("not-the-password").await.expect("the client should build");
    assert!(
        wrong.send(b"SELECT 1").await.is_err(),
        "a wrong password reached the request handler over a shared connection"
    );
    assert!(!wrong.is_connected());

    let client = shared("hunter2").await.expect("the client should build");

    let mut requests = Vec::new();
    for _ in 0..16 {
        let client = client.clone();
        requests.push(tokio::spawn(async move { client.send(b"SELECT 1").await }));
    }
    for request in requests {
        let response = request.await.unwrap().expect("an authenticated request");
        assert_eq!(String::from_utf8_lossy(&response), "alice:true");
    }

    client.close().await;

    assert_eq!(
        String::from_utf8_lossy(&client.send(b"SELECT 1").await.unwrap()),
        "alice:true",
        "the connection opened after a close must be authenticated too"
    );
}

// ------------------------------------------------------------ unix socket

#[cfg(unix)]
mod unix_socket {
    use super::*;
    use crate::native_login::{PeerAuthenticator, current_os_user};

    fn socket_path(name: &str) -> std::path::PathBuf {
        static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        std::path::PathBuf::from(format!(
            "/tmp/rdb-auth-{}-{}-{}.sock",
            std::process::id(),
            name,
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ))
    }

    /// Serves `database`'s roles on a unix socket under `method`, answering
    /// with the caller's name as `serve` does. The server is returned so the
    /// socket lasts as long as the test holds it.
    async fn serve_socket(
        path: &std::path::Path,
        database: Arc<Database>,
        method: AuthMethod,
    ) -> tokio::task::JoinHandle<()> {
        let authenticator = StdArc::new(RoleAuthenticator::new(database, method))
            as StdArc<dyn ServerAuthenticator>;
        let server = TcpServerBuilder::unix(path)
            .authenticator(authenticator)
            .build()
            .await
            .expect("the server should build");

        tokio::spawn(async move {
            let _ = server
                .run_with_principal(|principal, _request| async move {
                    format!("{}:{}", principal, principal.superuser).into_bytes()
                })
                .await;
        })
    }

    async fn login_with(
        path: &std::path::Path,
        authenticator: StdArc<dyn ClientAuthenticator>,
    ) -> Result<String, String> {
        let mut client = TcpClientBuilder::new(&format!("unix:{}", path.display()))
            .timeout(Duration::from_secs(5))
            .authenticator(authenticator)
            .build()
            .await
            .map_err(|e| e.to_string())?;
        let response = client
            .send(b"SELECT 1".to_vec())
            .await
            .map_err(|e| e.to_string())?;
        Ok(String::from_utf8_lossy(&response).to_string())
    }

    #[tokio::test]
    async fn a_password_works_on_a_unix_socket_and_a_wrong_one_does_not() {
        let path = socket_path("scram");
        let task = serve_socket(&path, database_with_roles(&[]).await, AuthMethod::ScramSha256).await;

        let right = login_with(&path, StdArc::new(PasswordAuthenticator::new("alice", "hunter2"))).await;
        assert_eq!(right.as_deref(), Ok("alice:true"));

        let wrong = login_with(&path, StdArc::new(PasswordAuthenticator::new("alice", "hunter3"))).await;
        assert!(wrong.is_err(), "a wrong password was accepted on a unix socket: {wrong:?}");

        task.abort();
    }

    #[tokio::test]
    async fn peer_lets_in_the_role_named_after_the_connecting_user_and_nobody_else() {
        let me = current_os_user().expect("the test runs as a named user");
        let path = socket_path("peer");
        let database =
            database_with_roles(&[&format!("CREATE ROLE \"{me}\" LOGIN")]).await;
        let task = serve_socket(&path, database, AuthMethod::Peer).await;

        // As myself: in, with no password anywhere.
        let mine = login_with(&path, StdArc::new(PeerAuthenticator::new(me.clone()))).await;
        assert_eq!(mine.as_deref(), Ok(format!("{me}:false").as_str()));

        // As anyone else, even a role that exists and is a superuser: the
        // kernel says who connected, and it is not them.
        let other = login_with(&path, StdArc::new(PeerAuthenticator::new("alice"))).await;
        assert!(other.is_err(), "peer let a caller in as a user it is not: {other:?}");

        task.abort();
    }

    #[tokio::test]
    async fn peer_needs_a_role_that_may_log_in() {
        let me = current_os_user().expect("the test runs as a named user");
        let path = socket_path("nologin");

        // No such role at all.
        let task = serve_socket(&path, database_with_roles(&[]).await, AuthMethod::Peer).await;
        let missing = login_with(&path, StdArc::new(PeerAuthenticator::new(me.clone()))).await;
        assert!(missing.is_err(), "peer let in a user with no role: {missing:?}");
        task.abort();
        tokio::time::sleep(Duration::from_millis(50)).await;

        // A role that may not log in.
        let path = socket_path("nologin2");
        let database = database_with_roles(&[&format!("CREATE ROLE \"{me}\" NOLOGIN")]).await;
        let task = serve_socket(&path, database, AuthMethod::Peer).await;
        let barred = login_with(&path, StdArc::new(PeerAuthenticator::new(me))).await;
        assert!(barred.is_err(), "peer let in a role that may not log in: {barred:?}");
        task.abort();
    }

    #[tokio::test]
    async fn peer_on_a_tcp_listener_lets_nobody_in() {
        // Never configured that way — start-up refuses it — but if it were,
        // a caller over TCP has no kernel-reported user, and must not be
        // taken at its word.
        let me = current_os_user().expect("the test runs as a named user");
        let (listener, port) = loopback();
        let database = database_with_roles(&[&format!("CREATE ROLE \"{me}\" LOGIN")]).await;

        let authenticator = StdArc::new(RoleAuthenticator::new(database, AuthMethod::Peer))
            as StdArc<dyn ServerAuthenticator>;
        let server = TcpServerBuilder::from_listener(listener)
            .unwrap()
            .authenticator(authenticator)
            .build()
            .await
            .unwrap();
        let task = tokio::spawn(async move {
            let _ = server.run(|data| async move { data }).await;
        });

        let mut client = TcpClientBuilder::new(&format!("127.0.0.1:{port}"))
            .timeout(Duration::from_secs(5))
            .authenticator(StdArc::new(PeerAuthenticator::new(me)) as StdArc<dyn ClientAuthenticator>)
            .build()
            .await
            .unwrap();
        assert!(client.send(b"SELECT 1".to_vec()).await.is_err());

        task.abort();
    }

    /// The real listener, as the server binary starts it.
    mod listener {
        use super::*;
        use rasterizeddb_client::{Client, Login, Options};
        use rasterizeddb_core::core::database::{NativeListen, NativeSocket};
        use rasterizeddb_core::native_auth::NativeAuthConfig;

        /// A client on `path` (a unix socket) logging in as `login`.
        async fn connect(path: &std::path::Path, login: Login) -> rasterizeddb_client::Result<Client> {
            Client::connect(
                &format!("unix:{}", path.display()),
                Options {
                    login,
                    ..Options::default()
                },
            )
            .await
        }

        /// Port 0: the kernel picks a free one at the server's own bind, so
        /// no number is let go and handed to a parallel test in between.
        fn free_port() -> u16 {
            0
        }

        #[tokio::test]
        async fn a_socket_every_user_can_reach_is_refused_without_authentication() {
            let path = socket_path("shared");
            let mut socket = NativeSocket::new(&path);
            socket.mode = 0o666;
            socket.method = Some(AuthMethod::Trust);

            let listen = NativeListen {
                addr: "127.0.0.1".to_string(),
                port: free_port(),
                tcp: true,
                socket: Some(socket),
                request_timeout: None,
                certificate: None,
            };
            let auth = NativeAuthConfig {
                method: AuthMethod::Trust,
                ..NativeAuthConfig::default()
            };

            let outcome = tokio::time::timeout(
                Duration::from_secs(30),
                Database::start_db_with_auth_on(database_with_roles(&[]).await, auth, listen),
            )
            .await
            .expect("a refusal is immediate, not a server that runs");

            let error = outcome.expect_err("a world-writable socket was served with trust");
            assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
            assert!(!path.exists(), "the refused socket was created anyway");
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn a_socket_shared_with_a_group_may_run_without_authentication() {
            // The group is an operator's choice of who may connect, as the
            // owner is: trust is allowed on a socket only they can reach.
            let _ = rasterizeddb_core::MAX_PERMITS_THREADS.get_or_init(|| 4);
            let _ = rasterizeddb_core::BATCH_SIZE.get_or_init(|| 1024);

            let path = socket_path("group");
            let mut socket = NativeSocket::new(&path);
            socket.mode = 0o660;
            socket.method = Some(AuthMethod::Trust);
            let listen = NativeListen {
                addr: "127.0.0.1".to_string(),
                port: free_port(),
                tcp: true,
                socket: Some(socket),
                request_timeout: None,
                certificate: None,
            };
            let auth = NativeAuthConfig {
                method: AuthMethod::Trust,
                ..NativeAuthConfig::default()
            };

            let server = tokio::spawn(Database::start_db_with_auth_on(
                database_with_roles(&[]).await,
                auth,
                listen,
            ));
            for _ in 0..200 {
                if path.exists() || server.is_finished() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            assert!(!server.is_finished(), "a group-only socket with trust was refused");

            // The listener binds the file (it appears with the umask's mode)
            // and only then sets the mode asked for; read before that and the
            // test sees the umask's. Wait for the mode, bounded.
            use std::os::unix::fs::PermissionsExt;
            let mode = || std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            for _ in 0..200 {
                if mode() == 0o660 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            assert_eq!(mode(), 0o660);

            let client = connect(&path, Login::Trust)
                .await
                .expect("connected with no credentials");
            let reply = client.run("SELECT 1").await;
            assert!(reply.is_ok(), "{:?}", reply.err());

            server.abort();
            let _ = server.await;
        }

        #[tokio::test]
        async fn tcp_off_with_no_socket_is_refused() {
            let listen = NativeListen {
                addr: "127.0.0.1".to_string(),
                port: free_port(),
                tcp: false,
                socket: None,
                request_timeout: None,
                certificate: None,
            };
            let outcome = tokio::time::timeout(
                Duration::from_secs(30),
                Database::start_db_with_auth_on(
                    database_with_roles(&[]).await,
                    NativeAuthConfig::default(),
                    listen,
                ),
            )
            .await
            .expect("a refusal is immediate, not a server that runs");
            assert_eq!(outcome.unwrap_err().kind(), std::io::ErrorKind::InvalidInput);
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn tcp_off_serves_the_socket_and_opens_no_port() {
            let _ = rasterizeddb_core::MAX_PERMITS_THREADS.get_or_init(|| 4);
            let _ = rasterizeddb_core::BATCH_SIZE.get_or_init(|| 1024);

            // A port of our own, held until the server has started: if the
            // server bound TCP anyway it would fail to start on it, and if it
            // did not, the port is still ours afterwards.
            let (held, port) = loopback();

            let path = socket_path("notcp");
            let mut socket = NativeSocket::new(&path);
            socket.method = Some(AuthMethod::Trust);
            let listen = NativeListen {
                addr: "127.0.0.1".to_string(),
                port,
                tcp: false,
                socket: Some(socket),
                request_timeout: None,
                certificate: None,
            };
            let auth = NativeAuthConfig {
                method: AuthMethod::Trust,
                ..NativeAuthConfig::default()
            };

            let server = tokio::spawn(Database::start_db_with_auth_on(
                database_with_roles(&[]).await,
                auth,
                listen,
            ));
            for _ in 0..200 {
                if path.exists() || server.is_finished() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            assert!(!server.is_finished(), "a socket-only listener did not start");
            tokio::time::sleep(Duration::from_millis(200)).await;
            assert!(!server.is_finished(), "a socket-only listener stopped, as one that tried the held port would");

            // Let go of the port only now, and ask at once. Released before
            // the server started, as it used to be, it was any process's to
            // take for the quarter of a second and more until this line, and
            // on a box running other suites one did: "something answers".
            drop(held);
            assert!(
                std::net::TcpStream::connect(("127.0.0.1", port)).is_err(),
                "something answers on the TCP port with --rdb-tcp off"
            );

            let client = connect(&path, Login::Trust)
                .await
                .expect("connected over the socket");
            assert!(client.run("SELECT 1").await.is_ok());

            server.abort();
            let _ = server.await;
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn a_query_on_the_socket_runs_as_the_connecting_user() {
            let _ = rasterizeddb_core::MAX_PERMITS_THREADS.get_or_init(|| 4);
            let _ = rasterizeddb_core::BATCH_SIZE.get_or_init(|| 1024);

            let me = current_os_user().expect("the test runs as a named user");
            let path = socket_path("listener");
            let database = database_with_roles(&[&format!("CREATE ROLE \"{me}\" LOGIN")]).await;

            let mut socket = NativeSocket::new(&path);
            socket.method = Some(AuthMethod::Peer);
            let listen = NativeListen {
                addr: "127.0.0.1".to_string(),
                port: free_port(),
                tcp: true,
                socket: Some(socket),
                request_timeout: None,
                certificate: None,
            };
            // The TCP side asks for a password, which exists: the socket's
            // method is its own.
            let auth = NativeAuthConfig::default();

            let server = tokio::spawn(Database::start_db_with_auth_on(database, auth, listen));

            for _ in 0..200 {
                if path.exists() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }

            let client = connect(&path, Login::Peer)
                .await
                .expect("logged in as this process's user");

            client
                .run("CREATE TABLE peer_t (id INTEGER, name VARCHAR)")
                .await
                .expect("CREATE TABLE");
            client
                .run("INSERT INTO peer_t (id, name) VALUES (1, 'over a unix socket')")
                .await
                .expect("INSERT");

            let rows = client.run("SELECT id, name FROM peer_t").await.expect("SELECT");
            assert_eq!(rows.len(), 1);
            let row = rows.get(0).unwrap();
            assert_eq!(row.get_i32(0).unwrap(), 1);
            assert_eq!(row.get_text(1).unwrap(), "over a unix socket");

            server.abort();
            let _ = server.await;
        }
    }
}
