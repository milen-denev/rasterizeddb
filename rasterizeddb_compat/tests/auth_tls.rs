//! Authentication and encrypted transport, driven by a real driver.
//!
//! These go through sqlx rather than a hand-written client, because the point
//! is that an off-the-shelf PostgreSQL driver negotiates with this server the
//! way it would with PostgreSQL — including choosing SCRAM-SHA-256 itself.

use rasterizeddb_compat::TestServer;
use rasterizeddb_core::auth::AuthMethod;
use rasterizeddb_core::pgwire::TlsMode;

/// Connects and runs one trivial statement, so a failure is a connection
/// failure rather than a query failure.
async fn can_connect(url: &str) -> Result<(), String> {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(url)
        .await
        .map_err(|e| e.to_string())?;
    sqlx::query("SELECT 1")
        .fetch_all(&pool)
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

#[tokio::test(flavor = "multi_thread")]
async fn scram_accepts_the_right_password() {
    let server = TestServer::start_with(TlsMode::Disable, AuthMethod::ScramSha256, Some("s3cret"))
        .await
        .expect("server should start");

    can_connect(&server.url_as("rasterized", Some("s3cret")))
        .await
        .expect("the right password should be accepted");
}

#[tokio::test(flavor = "multi_thread")]
async fn scram_rejects_the_wrong_password() {
    let server = TestServer::start_with(TlsMode::Disable, AuthMethod::ScramSha256, Some("s3cret"))
        .await
        .expect("server should start");

    let err = can_connect(&server.url_as("rasterized", Some("wrong")))
        .await
        .expect_err("the wrong password must be refused");
    assert!(
        err.contains("password authentication failed"),
        "the refusal should say why: {}",
        err
    );
}

/// An unknown user must be refused the same way a wrong password is, so the
/// protocol cannot be used to find out which accounts exist.
#[tokio::test(flavor = "multi_thread")]
async fn an_unknown_user_is_refused_indistinguishably() {
    let server = TestServer::start_with(TlsMode::Disable, AuthMethod::ScramSha256, Some("s3cret"))
        .await
        .expect("server should start");

    let unknown = can_connect(&server.url_as("nosuchuser", Some("s3cret")))
        .await
        .expect_err("unknown user");
    let wrong = can_connect(&server.url_as("rasterized", Some("wrong")))
        .await
        .expect_err("wrong password");

    assert!(unknown.contains("password authentication failed"), "{}", unknown);
    assert!(wrong.contains("password authentication failed"), "{}", wrong);
}

#[tokio::test(flavor = "multi_thread")]
async fn md5_accepts_the_right_password_and_refuses_the_wrong_one() {
    let server = TestServer::start_with(TlsMode::Disable, AuthMethod::Md5, Some("s3cret"))
        .await
        .expect("server should start");

    can_connect(&server.url_as("rasterized", Some("s3cret")))
        .await
        .expect("the right password should be accepted");
    assert!(
        can_connect(&server.url_as("rasterized", Some("wrong"))).await.is_err(),
        "the wrong password must be refused"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn cleartext_accepts_the_right_password_and_refuses_the_wrong_one() {
    let server = TestServer::start_with(TlsMode::Disable, AuthMethod::Password, Some("s3cret"))
        .await
        .expect("server should start");

    can_connect(&server.url_as("rasterized", Some("s3cret")))
        .await
        .expect("the right password should be accepted");
    assert!(
        can_connect(&server.url_as("rasterized", Some("wrong"))).await.is_err(),
        "the wrong password must be refused"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn trust_accepts_any_password() {
    let server = TestServer::start_with(TlsMode::Disable, AuthMethod::Trust, None)
        .await
        .expect("server should start");

    can_connect(&server.url_as("anyone", Some("anything")))
        .await
        .expect("trust verifies nothing");
}

/// No certificate is configured, so one is generated at start-up — an encrypted
/// connection with no prior setup.
#[tokio::test(flavor = "multi_thread")]
async fn tls_works_with_a_generated_self_signed_certificate() {
    let server = TestServer::start_with(TlsMode::Require, AuthMethod::Trust, None)
        .await
        .expect("server should start");

    can_connect(&server.url())
        .await
        .expect("an encrypted connection should be established");
}

/// The combination a deployment actually uses: the password never crosses the
/// wire, and what does cross it is encrypted.
#[tokio::test(flavor = "multi_thread")]
async fn scram_over_tls_is_the_deployment_shape() {
    let server = TestServer::start_with(TlsMode::Require, AuthMethod::ScramSha256, Some("s3cret"))
        .await
        .expect("server should start");

    can_connect(&server.url_as("rasterized", Some("s3cret")))
        .await
        .expect("SCRAM over TLS should connect");
    assert!(
        can_connect(&server.url_as("rasterized", Some("wrong"))).await.is_err(),
        "TLS must not make the password check optional"
    );
}

/// `require` means required: a cleartext connection has to be refused rather
/// than quietly downgraded.
#[tokio::test(flavor = "multi_thread")]
async fn requiring_tls_refuses_an_unencrypted_connection() {
    let server = TestServer::start_with(TlsMode::Require, AuthMethod::Trust, None)
        .await
        .expect("server should start");

    let plain = server.url().replace("sslmode=require", "sslmode=disable");
    let err = can_connect(&plain).await.expect_err("cleartext must be refused");
    assert!(
        err.to_ascii_lowercase().contains("tls") || err.to_ascii_lowercase().contains("ssl"),
        "the refusal should say TLS is required: {}",
        err
    );
}

/// Roles created over SQL must be usable to connect, or the catalogue is only
/// half a feature.
#[tokio::test(flavor = "multi_thread")]
async fn a_role_created_over_sql_can_connect() {
    let server = TestServer::start_with(TlsMode::Disable, AuthMethod::ScramSha256, Some("boot"))
        .await
        .expect("server should start");

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&server.url_as("rasterized", Some("boot")))
        .await
        .expect("bootstrap role connects");

    sqlx::query("CREATE USER alice PASSWORD 'wonderland'")
        .execute(&pool)
        .await
        .expect("create user");

    can_connect(&server.url_as("alice", Some("wonderland")))
        .await
        .expect("the new role should connect");
    assert!(
        can_connect(&server.url_as("alice", Some("nope"))).await.is_err(),
        "the wrong password must still be refused"
    );

    // Changing the password invalidates the old one.
    sqlx::query("ALTER USER alice PASSWORD 'looking-glass'")
        .execute(&pool)
        .await
        .expect("alter user");
    assert!(can_connect(&server.url_as("alice", Some("wonderland"))).await.is_err());
    can_connect(&server.url_as("alice", Some("looking-glass")))
        .await
        .expect("the new password works");

    sqlx::query("DROP USER alice").execute(&pool).await.expect("drop user");
    assert!(
        can_connect(&server.url_as("alice", Some("looking-glass"))).await.is_err(),
        "a dropped role must not connect"
    );
}

/// `CREATE ROLE` without LOGIN is a group, not an account.
#[tokio::test(flavor = "multi_thread")]
async fn a_role_without_login_cannot_connect() {
    let server = TestServer::start_with(TlsMode::Disable, AuthMethod::ScramSha256, Some("boot"))
        .await
        .expect("server should start");

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&server.url_as("rasterized", Some("boot")))
        .await
        .expect("bootstrap role connects");

    sqlx::query("CREATE ROLE readers PASSWORD 'x'")
        .execute(&pool)
        .await
        .expect("create role");

    assert!(
        can_connect(&server.url_as("readers", Some("x"))).await.is_err(),
        "a role without LOGIN must not be able to connect"
    );
}

/// The stored secret must not be the password: a stolen catalogue should not
/// yield anything replayable.
#[tokio::test(flavor = "multi_thread")]
async fn the_catalogue_stores_a_verifier_not_the_password() {
    let server = TestServer::start_with(TlsMode::Disable, AuthMethod::ScramSha256, Some("boot"))
        .await
        .expect("server should start");

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&server.url_as("rasterized", Some("boot")))
        .await
        .expect("connect");

    sqlx::query("CREATE USER carol PASSWORD 'plaintext-secret'")
        .execute(&pool)
        .await
        .expect("create user");

    use sqlx::Row;
    let row = sqlx::query("SELECT password FROM rdb_roles WHERE name = 'carol'")
        .fetch_one(&pool)
        .await
        .expect("read back");
    let stored: String = row.try_get("password").expect("password column");

    assert!(
        stored.starts_with("SCRAM-SHA-256$"),
        "expected a SCRAM verifier, got {:?}",
        stored
    );
    assert!(
        !stored.contains("plaintext-secret"),
        "the password itself must not be stored"
    );
}

/// Authentication without authorisation is half a feature: a role that can
/// log in must not be able to make itself a superuser, change other accounts,
/// or administer the server. PostgreSQL reports `42501 insufficient_privilege`
/// for this, and drivers branch on that code.
#[tokio::test(flavor = "multi_thread")]
async fn a_non_superuser_cannot_administer_the_server() {
    let server = TestServer::start_with(TlsMode::Disable, AuthMethod::ScramSha256, Some("boot"))
        .await
        .expect("server should start");

    let admin = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&server.url_as("rasterized", Some("boot")))
        .await
        .expect("bootstrap role connects");

    sqlx::query("CREATE USER app PASSWORD 'app'")
        .execute(&admin)
        .await
        .expect("the bootstrap role is a superuser and may create roles");

    let app = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&server.url_as("app", Some("app")))
        .await
        .expect("the new role connects");

    for sql in [
        "CREATE USER mallory PASSWORD 'x' SUPERUSER",
        "ALTER USER rasterized PASSWORD 'hijacked'",
        "DROP USER rasterized",
        "CREATE DATABASE sneaky",
        "DROP DATABASE postgres",
        "SET CONFIG filter.max_f = 14",
    ] {
        let err = sqlx::query(sql)
            .execute(&app)
            .await
            .expect_err(&format!("{sql:?} must be refused for a non-superuser"));
        let db_err = err.as_database_error().expect("a server-side refusal");
        assert_eq!(
            db_err.code().as_deref(),
            Some("42501"),
            "{sql:?}: expected insufficient_privilege, got {err}"
        );
    }

    // The refusal must not have leaked through: the superuser still logs in
    // with its own password, and nothing was created.
    can_connect(&server.url_as("rasterized", Some("boot")))
        .await
        .expect("the superuser's password is unchanged");
    assert!(
        can_connect(&server.url_as("mallory", Some("x"))).await.is_err(),
        "the refused CREATE USER must not have created anything"
    );

    // Ordinary work is unaffected: the role still reads and writes data.
    sqlx::query("CREATE TABLE app_owned (id INT)")
        .execute(&app)
        .await
        .expect("DDL on tables is not administration");
    sqlx::query("INSERT INTO app_owned VALUES (1)")
        .execute(&app)
        .await
        .expect("writes are not administration");

    // And the superuser is not gated.
    sqlx::query("CREATE USER dave PASSWORD 'd'")
        .execute(&admin)
        .await
        .expect("a superuser may still create roles");
}

/// Under `trust` nobody proves who they are, so there is no one to refuse —
/// the gate would otherwise stop the statement that creates the first account.
#[tokio::test(flavor = "multi_thread")]
async fn trust_does_not_gate_administration() {
    let server = TestServer::start_with(TlsMode::Disable, AuthMethod::Trust, None)
        .await
        .expect("server should start");

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&server.url_as("anyone", None))
        .await
        .expect("trust connects");

    sqlx::query("CREATE USER first PASSWORD 'f'")
        .execute(&pool)
        .await
        .expect("the first account is created without a credential to gate on");
}
