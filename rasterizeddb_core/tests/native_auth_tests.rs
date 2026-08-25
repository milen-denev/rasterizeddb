//! Does the native protocol's authentication actually keep anyone out?
//!
//! Every test drives the real client half against the real server half over a
//! real TLS connection, because the two halves agreeing with each other is the
//! only property that matters and unit tests of either alone cannot show it.
//! Most of them try to get in *without* the right credential and require that
//! it fail: a suite of happy paths would pass against an authenticator that
//! returns `Ok` for everyone.

use std::sync::Arc as StdArc;
use std::time::Duration;

use rasterizeddb_core::auth::{AuthMethod, ensure_role_catalogue, execute_role_statement, parse_role_statement};
use rasterizeddb_core::core::database::Database;
use rasterizeddb_core::native_auth::{PasswordAuthenticator, RoleAuthenticator};
use rastcp::client::TcpClientBuilder;
use rastcp::server::TcpServerBuilder;
use rastcp::session::{ClientAuthenticator, ServerAuthenticator};
use rclite::Arc;

/// A port nothing is listening on.
fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("a loopback port");
    let port = listener.local_addr().expect("a bound address").port();
    drop(listener);
    port
}

/// An in-memory database holding the roles a test needs.
async fn database_with_roles(roles: &[&str]) -> Arc<Database> {
    let location = std::env::temp_dir().join(format!("rdb_auth_test_{}", free_port()));
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

/// Serves on `port`, answering every request with the caller's name and
/// whether it is a superuser — so a test can check not only that the caller got
/// in but that it got in as the right person.
async fn serve(port: u16, database: Arc<Database>, method: AuthMethod) {
    let authenticator =
        StdArc::new(RoleAuthenticator::new(database, method)) as StdArc<dyn ServerAuthenticator>;

    let server = TcpServerBuilder::new("127.0.0.1", port)
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
        builder = builder.authenticator(
            StdArc::new(PasswordAuthenticator::new(user, password)) as StdArc<dyn ClientAuthenticator>
        );
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
    let port = free_port();
    serve(port, database_with_roles(&[]).await, AuthMethod::ScramSha256).await;

    assert_eq!(
        login(port, Some(("alice", "hunter2"))).await.unwrap(),
        "alice:true",
        "the bootstrap role is a superuser and the server should see it as one"
    );
}

/// The test that matters most: a wrong password has to be refused.
#[tokio::test]
async fn a_wrong_password_is_refused() {
    let port = free_port();
    serve(port, database_with_roles(&[]).await, AuthMethod::ScramSha256).await;

    let err = login(port, Some(("alice", "not hunter2")))
        .await
        .expect_err("a wrong password must not get in");
    assert!(err.contains("authentication failed"), "{}", err);
}

#[tokio::test]
async fn an_unknown_user_is_refused_and_looks_the_same_as_a_wrong_password() {
    let port = free_port();
    serve(port, database_with_roles(&[]).await, AuthMethod::ScramSha256).await;

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
    let port = free_port();
    serve(port, database_with_roles(&[]).await, AuthMethod::ScramSha256).await;

    assert!(
        login(port, None).await.is_err(),
        "a client that skips the exchange must not be served"
    );
}

#[tokio::test]
async fn a_role_that_may_not_log_in_is_refused_even_with_the_right_password() {
    let port = free_port();
    let database = database_with_roles(&["CREATE ROLE reader NOLOGIN PASSWORD 'letmein'"]).await;
    serve(port, database, AuthMethod::ScramSha256).await;

    assert!(
        login(port, Some(("reader", "letmein"))).await.is_err(),
        "knowing the password is not the same as being allowed to connect"
    );
}

#[tokio::test]
async fn an_ordinary_user_is_not_a_superuser() {
    let port = free_port();
    let database = database_with_roles(&["CREATE USER bob PASSWORD 'bobpass'"]).await;
    serve(port, database, AuthMethod::ScramSha256).await;

    assert_eq!(
        login(port, Some(("bob", "bobpass"))).await.unwrap(),
        "bob:false"
    );
}

/// A role created without a password exists but can authenticate to nothing.
#[tokio::test]
async fn a_role_with_no_password_cannot_authenticate() {
    let port = free_port();
    let database = database_with_roles(&["CREATE USER carol"]).await;
    serve(port, database, AuthMethod::ScramSha256).await;

    assert!(login(port, Some(("carol", ""))).await.is_err());
    assert!(login(port, Some(("carol", "anything"))).await.is_err());
}

/// `trust` still means what it says, and the caller's name still arrives — to
/// a client that has said it will accept being waved through.
#[tokio::test]
async fn trust_accepts_any_password_but_still_reports_the_user() {
    let port = free_port();
    serve(port, database_with_roles(&[]).await, AuthMethod::Trust).await;

    let mut client = TcpClientBuilder::new(&format!("127.0.0.1:{}", port))
        .timeout(Duration::from_secs(5))
        .authenticator(StdArc::new(
            PasswordAuthenticator::new("alice", "whatever").allow_trust(),
        ) as StdArc<dyn ClientAuthenticator>)
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
    let port = free_port();
    serve(port, database_with_roles(&[]).await, AuthMethod::Trust).await;

    let err = login(port, Some(("alice", "hunter2")))
        .await
        .expect_err("an unchallenged accept must not satisfy a password-bearing client");
    assert!(err.contains("never proved who it is"), "{}", err);
}

/// A reconnect must authenticate again rather than resuming as nobody.
#[tokio::test]
async fn a_reconnected_client_authenticates_again() {
    let port = free_port();
    serve(port, database_with_roles(&[]).await, AuthMethod::ScramSha256).await;

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

    client.reconnect().await.expect("the reconnect should authenticate");

    assert_eq!(
        String::from_utf8_lossy(&client.send(b"SELECT 1".to_vec()).await.unwrap()),
        "alice:true",
        "the second connection must be authenticated too"
    );
}
