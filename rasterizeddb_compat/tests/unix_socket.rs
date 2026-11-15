//! The PostgreSQL protocol on a unix socket, driven by stock drivers.
//!
//! What a deployment with no TCP port needs: tokio-postgres and sqlx find the
//! socket where PostgreSQL would put it (`<dir>/.s.PGSQL.<port>`), connect
//! without TLS although the server defaults to requiring it on TCP, and log in
//! under the socket's own authentication — trust on a socket only its owner
//! and group can reach, or peer, which takes the kernel's word for who
//! connected.

#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::time::Duration;

use rasterizeddb_core::auth::{AuthMethod, execute_role_statement, parse_role_statement};
use rasterizeddb_core::core::database::DEFAULT_DATABASE_NAME;
use rasterizeddb_core::core::database::catalog::DatabaseCatalog;
use rasterizeddb_core::native_auth::current_os_user;
use rasterizeddb_core::pgwire::{
    AuthConfig, ConnectionLimits, PgListen, PgSocket, TlsConfig, TlsMode, start_pgwire_on,
};

/// The port in the socket's name. No TCP port is opened, so any number does;
/// a distinct one per server keeps their file names apart.
fn socket_port() -> u16 {
    static NEXT: std::sync::atomic::AtomicU16 = std::sync::atomic::AtomicU16::new(0);
    40_000 + (std::process::id() % 10_000) as u16
        + NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

fn socket_dir() -> PathBuf {
    static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let dir = PathBuf::from(format!(
        "/tmp/rdb-pgsock-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).expect("a socket directory");
    dir
}

/// A server on a unix socket only, on a thread and runtime of its own as the
/// compat harness runs one. Dropping it stops the server; the socket file
/// goes with it.
struct SocketServer {
    dir: PathBuf,
    port: u16,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Drop for SocketServer {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Starts one, with `roles` created first. `Err` is the server's refusal to
/// start.
fn start(
    socket_method: AuthMethod,
    mode: u32,
    roles: Vec<String>,
) -> Result<SocketServer, String> {
    let _ = rasterizeddb_core::MAX_PERMITS_THREADS.set(4);
    let _ = rasterizeddb_core::BATCH_SIZE.set(1_000);

    let dir = socket_dir();
    let port = socket_port();
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let (ready_tx, ready_rx) = std::sync::mpsc::channel::<Result<(), String>>();

    let socket = PgSocket {
        dir: dir.clone(),
        port,
        mode,
        group: None,
        method: Some(socket_method),
    };
    let path = socket.path();

    let thread = std::thread::Builder::new()
        .stack_size(32 * 1024 * 1024)
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .thread_stack_size(32 * 1024 * 1024)
                .enable_all()
                .build()
                .expect("a runtime");
            runtime.block_on(async move {
                let catalog = DatabaseCatalog::open(None, DEFAULT_DATABASE_NAME).await.expect("a catalog");
                let database = catalog.get(DEFAULT_DATABASE_NAME).expect("the default database");

                rasterizeddb_core::auth::ensure_role_catalogue(
                    &database,
                    "rasterized",
                    Some("s3cret"),
                    AuthMethod::ScramSha256,
                )
                .await
                .expect("a role catalogue");
                for sql in &roles {
                    let statement = parse_role_statement(sql).expect("a role statement").expect(sql);
                    execute_role_statement(&database, statement, AuthMethod::ScramSha256)
                        .await
                        .expect(sql);
                }

                let listen = PgListen { tcp: None, socket: Some(socket) };
                // TLS required, as the binary's default is: a unix socket must
                // be served without it all the same.
                let tls = TlsConfig { mode: TlsMode::Require, cert_path: None, key_path: None };
                let auth = AuthConfig {
                    method: AuthMethod::ScramSha256,
                    bootstrap_user: "rasterized".to_string(),
                    bootstrap_password: Some("s3cret".to_string()),
                };

                let serving = start_pgwire_on(catalog, listen, tls, auth, ConnectionLimits::default());
                tokio::pin!(serving);

                // Ready once the socket exists, or refused if the server
                // returned first.
                let started = async {
                    for _ in 0..400 {
                        if path.exists() {
                            return;
                        }
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                };
                tokio::select! {
                    outcome = &mut serving => {
                        let _ = ready_tx.send(Err(match outcome {
                            Ok(()) => "the server returned at once".to_string(),
                            Err(e) => e.to_string(),
                        }));
                        return;
                    }
                    _ = started => {
                        let _ = ready_tx.send(Ok(()));
                    }
                }
                tokio::select! {
                    _ = serving => {}
                    _ = stop_rx => {}
                }
            });
            runtime.shutdown_timeout(Duration::from_secs(2));
        })
        .expect("a server thread");

    let server = SocketServer {
        dir,
        port,
        stop: Some(stop_tx),
        thread: Some(thread),
    };
    match ready_rx.recv_timeout(Duration::from_secs(30)) {
        Ok(Ok(())) => Ok(server),
        Ok(Err(e)) => Err(e),
        Err(_) => Err("the server neither started nor refused within 30 s".to_string()),
    }
}

async fn tokio_postgres_connect(
    dir: &Path,
    port: u16,
    user: &str,
    password: Option<&str>,
) -> Result<tokio_postgres::Client, tokio_postgres::Error> {
    let mut config = tokio_postgres::Config::new();
    config.host_path(dir).port(port).user(user).dbname("postgres");
    if let Some(password) = password {
        config.password(password);
    }
    let (client, connection) = config.connect(tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(client)
}

#[tokio::test(flavor = "multi_thread")]
async fn tokio_postgres_finds_the_socket_and_queries_without_tls() {
    let server = start(AuthMethod::Trust, 0o660, vec![]).expect("trust on a 660 socket is allowed");

    use std::os::unix::fs::PermissionsExt;
    let path = server.dir.join(format!(".s.PGSQL.{}", server.port));
    assert_eq!(std::fs::metadata(&path).unwrap().permissions().mode() & 0o777, 0o660);

    let client = tokio_postgres_connect(&server.dir, server.port, "rasterized", None)
        .await
        .expect("connected over the socket, with no TLS, although TCP would require it");

    client
        .batch_execute("CREATE TABLE sock_t (id INTEGER, name VARCHAR)")
        .await
        .expect("CREATE TABLE");
    // The extended protocol: a prepared statement with parameters.
    let insert = client
        .prepare("INSERT INTO sock_t (id, name) VALUES ($1, $2)")
        .await
        .expect("prepare");
    for id in 0..20i32 {
        client.execute(&insert, &[&id, &format!("row {id}")]).await.expect("insert");
    }
    let rows = client
        .query("SELECT id, name FROM sock_t WHERE id >= $1 ORDER BY id", &[&15i32])
        .await
        .expect("select");
    assert_eq!(rows.len(), 5);
    assert_eq!(rows[0].get::<_, i32>(0), 15);
    assert_eq!(rows[0].get::<_, String>(1), "row 15");
}

#[tokio::test(flavor = "multi_thread")]
async fn sqlx_connects_over_the_socket_too() {
    let server = start(AuthMethod::ScramSha256, 0o600, vec![]).expect("a server");

    let options = sqlx::postgres::PgConnectOptions::new()
        .socket(&server.dir)
        .port(server.port)
        .username("rasterized")
        .password("s3cret")
        .database("postgres");
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect_with(options)
        .await
        .expect("SCRAM over the socket");
    let one: (i32,) = sqlx::query_as("SELECT 1").fetch_one(&pool).await.expect("a row");
    assert_eq!(one.0, 1);

    // And a wrong password is still a wrong password there.
    let wrong = sqlx::postgres::PgConnectOptions::new()
        .socket(&server.dir)
        .port(server.port)
        .username("rasterized")
        .password("wrong")
        .database("postgres");
    assert!(sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect_with(wrong)
        .await
        .is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn peer_lets_in_the_connecting_user_and_nobody_else() {
    let me = current_os_user().expect("the test runs as a named user");
    let server = start(AuthMethod::Peer, 0o600, vec![format!("CREATE ROLE \"{me}\" LOGIN")])
        .expect("a peer server");

    let mine = tokio_postgres_connect(&server.dir, server.port, &me, None).await;
    let client = mine.expect("logged in as this process's user, with no password");
    assert_eq!(client.query_one("SELECT 1", &[]).await.unwrap().get::<_, i32>(0), 1);

    // Another role, superuser and all, even with its right password: the
    // kernel says who connected, and it is not them.
    let other = tokio_postgres_connect(&server.dir, server.port, "rasterized", Some("s3cret")).await;
    let error = other.err().expect("peer let in a user it is not");
    let said = error.as_db_error().map(|e| e.message().to_string()).unwrap_or_else(|| error.to_string());
    assert!(said.contains("Peer authentication failed"), "the refusal should say why: {said}");
}

#[tokio::test(flavor = "multi_thread")]
async fn trust_on_a_socket_every_user_can_reach_is_refused() {
    let refused = start(AuthMethod::Trust, 0o666, vec![]);
    assert!(refused.is_err(), "trust was served on a world-writable socket");
}
