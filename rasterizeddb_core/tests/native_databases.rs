//! Choosing a database over the native protocol, end to end.
//!
//! One server, as the binary runs it: a catalogue of several databases, the
//! native listener on loopback TCP under SCRAM and on a unix socket under
//! trust. Clients are `rasterizeddb_client` (what applications use) and raw
//! rastcp connections where the protocol itself is the subject.
//!
//! What must hold: a connection runs in the database it asked for, on every
//! connection a client opens (shared, reopened, a transaction's); a login is
//! checked against the roles of the database it names, never the default
//! database's; a database that does not exist is refused, never replaced by
//! the default one; and a connection's database cannot be changed under it.
//!
//! Every wait is bounded, and every directory is a `TempDir`.

#![cfg(unix)]

use std::future::Future;
use std::sync::Arc as StdArc;
use std::time::Duration;

use rasterizeddb_client::{Client, Executor, Login, Options};
use rasterizeddb_core::auth::AuthMethod;
use rasterizeddb_core::core::database::catalog::DatabaseCatalog;
use rasterizeddb_core::core::database::{Database, NativeListen, NativeSocket};
use rasterizeddb_core::native_auth::NativeAuthConfig;

#[path = "support/native_login.rs"]
mod native_login;
use native_login::PasswordAuthenticator;

const TEST_LIMIT: Duration = Duration::from_secs(180);
const PASSWORD: &str = "hunter2";

async fn bounded<F: Future>(test: F) -> F::Output {
    tokio::time::timeout(TEST_LIMIT, test)
        .await
        .expect("the test wedged")
}

/// The server: its own thread and runtime, ended when this is dropped.
struct Harness {
    /// `unix:/…`, trust.
    socket: String,
    /// `127.0.0.1:port`, SCRAM.
    tcp: String,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
    // Last: removed once the server thread has ended.
    _dir: tempfile::TempDir,
}

impl Harness {
    /// The default database `postgres`, and `kopa` and `other` created after
    /// the listener seeded the default one's roles, so they carry its
    /// `rasterized` account as a `CREATE DATABASE` does.
    async fn start() -> Harness {
        let dir = tempfile::Builder::new()
            .prefix("rdb_ndb")
            .tempdir()
            .expect("a temp dir");
        let data = dir.path().join("data");
        std::fs::create_dir_all(&data).unwrap();
        let socket_path = dir.path().join("n.sock");
        let (shutdown, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let (bound_tx, bound_rx) = tokio::sync::oneshot::channel::<(String, String)>();
        let thread = std::thread::Builder::new()
            .name("native-db-server".to_string())
            .stack_size(32 * 1024 * 1024)
            .spawn(move || {
                let _ = rasterizeddb_core::MAX_PERMITS_THREADS.get_or_init(|| 4);
                let _ = rasterizeddb_core::BATCH_SIZE.get_or_init(|| 1024);
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(4)
                    .thread_stack_size(32 * 1024 * 1024)
                    .enable_all()
                    .build()
                    .expect("server runtime");
                runtime.block_on(async move {
                    let catalog = DatabaseCatalog::open(Some(data.to_str().unwrap()), "postgres")
                        .await
                        .expect("the catalogue opens");
                    let mut socket = NativeSocket::new(&socket_path);
                    socket.mode = 0o600;
                    socket.method = Some(AuthMethod::Trust);
                    let listen = NativeListen {
                        socket: Some(socket),
                        request_timeout: Some(Duration::from_secs(30)),
                        ..NativeListen::new("127.0.0.1", 0)
                    };
                    let auth = NativeAuthConfig {
                        method: AuthMethod::ScramSha256,
                        bootstrap_user: "rasterized".to_string(),
                        bootstrap_password: Some(PASSWORD.to_string()),
                    };
                    let server = Database::bind_native_in(catalog.clone(), auth, listen)
                        .await
                        .expect("the native listeners bind");
                    catalog.create("kopa").await.expect("kopa is created");
                    catalog.create("other").await.expect("other is created");
                    let _ = bound_tx.send((
                        server.socket_endpoint().expect("a socket"),
                        server.tcp_endpoint().expect("a TCP address"),
                    ));
                    let handles = server.shutdown_handles();
                    let serving = server.run();
                    tokio::pin!(serving);
                    tokio::select! {
                        _ = &mut serving => {}
                        _ = shutdown_rx => {
                            for handle in handles {
                                handle.shutdown();
                            }
                            let _ = tokio::time::timeout(Duration::from_secs(10), serving).await;
                        }
                    }
                });
                runtime.shutdown_timeout(Duration::from_secs(2));
            })
            .expect("the server thread");
        let (socket, tcp) = tokio::time::timeout(Duration::from_secs(60), bound_rx)
            .await
            .expect("the server started in time")
            .expect("the server thread bound its listeners");
        Harness {
            socket,
            tcp: tcp.trim_start_matches("tcp:").to_string(),
            shutdown: Some(shutdown),
            thread: Some(thread),
            _dir: dir,
        }
    }

    async fn trust(&self, database: Option<&str>) -> rasterizeddb_client::Result<Client> {
        Client::connect(&self.socket, options(Login::Trust, database)).await
    }

    /// A trust connection running as `role` (`Client::connect_as_role`).
    async fn trust_as(&self, role: &str, database: Option<&str>) -> rasterizeddb_client::Result<Client> {
        Client::connect_as_role(&self.socket, options(Login::Trust, database), role).await
    }

    async fn password(
        &self,
        user: &str,
        password: &str,
        database: Option<&str>,
    ) -> rasterizeddb_client::Result<Client> {
        Client::connect(&self.tcp, options(Login::password(user, password), database)).await
    }

    /// A raw `rastcp/1` connection to the trust socket.
    async fn raw_socket(&self) -> rastcp::client::TcpClient {
        let mut client = rastcp::client::TcpClientBuilder::new(&self.socket)
            .timeout(Duration::from_secs(30))
            .build()
            .await
            .expect("a native connection");
        client.connect().await.expect("connects");
        client
    }

    /// A raw `rastcp/1` connection to the SCRAM listener, logged in with the
    /// hello that names no database.
    async fn raw_password(&self) -> rastcp::client::TcpClient {
        let mut client = rastcp::client::TcpClientBuilder::new(&self.tcp)
            .timeout(Duration::from_secs(30))
            .authenticator(StdArc::new(PasswordAuthenticator::new("rasterized", PASSWORD)))
            .build()
            .await
            .expect("a native connection");
        client.connect().await.expect("connects and logs in");
        client
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn options(login: Login, database: Option<&str>) -> Options {
    Options {
        login,
        database: database.map(str::to_string),
        timeout: Some(Duration::from_secs(30)),
        ..Options::default()
    }
}

async fn count(client: &Client, table: &str) -> rasterizeddb_client::Result<i64> {
    client
        .fetch_scalar(&format!("SELECT COUNT(*) FROM {table}"), &[])
        .await
}

async fn current_database(client: &Client) -> String {
    let answer = client
        .simple_query("SELECT current_database()")
        .await
        .expect("current_database() answers");
    match &answer.results[0] {
        rasterizeddb_client::SimpleResult::Rows { rows, .. } => {
            rows[0][0].clone().expect("a name")
        }
        other => panic!("current_database() answered {other:?}"),
    }
}

/// The text of a simple-form answer (`\x04` then PostgreSQL messages), for
/// matching a SQLSTATE in it.
fn simple_text(answer: &[u8]) -> String {
    assert_eq!(answer.first(), Some(&4), "a simple-form answer: {answer:?}");
    String::from_utf8_lossy(answer).into_owned()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_trust_connection_runs_in_the_database_it_names() {
    bounded(async {
        let server = Harness::start().await;

        let kopa = server.trust(Some("kopa")).await.expect("kopa connects");
        assert_eq!(current_database(&kopa).await, "kopa");
        kopa.execute("CREATE TABLE dish (id INT, name TEXT)", &[]).await.unwrap();
        kopa.execute("INSERT INTO dish (id, name) VALUES (1, 'moussaka')", &[]).await.unwrap();

        // The default database has no such table: the statement did not land
        // there.
        let default = server.trust(None).await.expect("the default connects");
        assert_eq!(current_database(&default).await, "postgres");
        let missing = count(&default, "dish").await;
        assert!(missing.is_err(), "the default database sees kopa's table: {missing:?}");

        // Nor in another named database.
        let other = server.trust(Some("other")).await.expect("other connects");
        assert!(count(&other, "dish").await.is_err());

        // A second client of kopa sees the row.
        let again = server.trust(Some("kopa")).await.unwrap();
        assert_eq!(count(&again, "dish").await.unwrap(), 1);

        // The same table name in another database is another table.
        other.execute("CREATE TABLE dish (id INT, name TEXT)", &[]).await.unwrap();
        assert_eq!(count(&other, "dish").await.unwrap(), 0);
        assert_eq!(count(&kopa, "dish").await.unwrap(), 1);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn every_connection_a_client_opens_uses_its_database() {
    bounded(async {
        let server = Harness::start().await;
        let setup = server.trust(Some("kopa")).await.unwrap();
        setup.execute("CREATE TABLE ticket (n INT)", &[]).await.unwrap();

        // A transaction runs on a connection of its own (rastcp/1, dialled for
        // it): it must be in kopa too, or its INSERT fails.
        let client = Client::connect(
            &server.socket,
            Options {
                idle_reconnect: Some(Duration::from_millis(50)),
                ..options(Login::Trust, Some("kopa"))
            },
        )
        .await
        .unwrap();
        let tx = client.begin().await.expect("a transaction opens");
        tx.exec("INSERT INTO ticket (n) VALUES (1)", &[]).await.expect("in kopa");
        tx.commit().await.expect("commits");
        assert_eq!(count(&setup, "ticket").await.unwrap(), 1);

        // The shared connection is reopened after a quiet spell; the new one
        // must be selected again before it is used.
        tokio::time::sleep(Duration::from_millis(200)).await;
        client.execute("INSERT INTO ticket (n) VALUES (2)", &[]).await.expect("still kopa");
        assert_eq!(count(&setup, "ticket").await.unwrap(), 2);

        // So is the simple-query session a console uses.
        let session = client.simple_session().await.expect("a simple session");
        let answer = session.query("SELECT current_database()").await.unwrap();
        match &answer.results[0] {
            rasterizeddb_client::SimpleResult::Rows { rows, .. } => {
                assert_eq!(rows[0][0].as_deref(), Some("kopa"))
            }
            other => panic!("{other:?}"),
        }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_database_that_does_not_exist_is_refused_not_replaced() {
    bounded(async {
        let server = Harness::start().await;

        let trust = server.trust(Some("nope")).await.err().expect("refused on trust");
        let text = trust.to_string();
        assert!(text.contains("database \"nope\" does not exist"), "{text}");
        assert!(text.contains("3D000"), "{text}");

        let scram = server
            .password("rasterized", PASSWORD, Some("nope"))
            .await
            .err()
            .expect("refused under SCRAM");
        assert!(
            scram.to_string().contains("database \"nope\" does not exist"),
            "{scram}"
        );
        assert!(!scram.may_predate_database_selection(), "{scram}");
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_password_login_is_checked_against_the_named_databases_roles() {
    bounded(async {
        let server = Harness::start().await;

        // kopa's `rasterized` gets a password of its own, and kopa gets a role
        // the default database does not have. The trust socket asks no
        // password; a trust connection runs as the role it names, so it names
        // kopa's superuser to change kopa's roles (an unnamed one is no
        // administrator: roles plan R0).
        let admin = server.trust_as("rasterized", Some("kopa")).await.unwrap();
        admin.execute("ALTER USER rasterized PASSWORD 'kopa-pw'", &[]).await.unwrap();
        admin.execute("CREATE USER waiter PASSWORD 'tray'", &[]).await.unwrap();
        admin.execute("CREATE TABLE menu (id INT)", &[]).await.unwrap();

        // kopa's password opens kopa; the default database's does not.
        let kopa = server
            .password("rasterized", "kopa-pw", Some("kopa"))
            .await
            .expect("kopa's own password opens kopa");
        assert_eq!(count(&kopa, "menu").await.unwrap(), 0);
        assert!(
            server.password("rasterized", PASSWORD, Some("kopa")).await.is_err(),
            "the default database's password opened kopa"
        );

        // And the other way round.
        let default = server
            .password("rasterized", PASSWORD, None)
            .await
            .expect("the default database keeps its password");
        assert!(count(&default, "menu").await.is_err(), "default sees kopa's table");
        assert!(server.password("rasterized", "kopa-pw", None).await.is_err());

        // A role of kopa's alone logs in to kopa and nowhere else.
        let waiter = server.password("waiter", "tray", Some("kopa")).await.expect("waiter in kopa");
        assert_eq!(current_database(&waiter).await, "kopa");
        assert!(server.password("waiter", "tray", None).await.is_err());
        assert!(server.password("waiter", "tray", Some("other")).await.is_err());
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_connections_database_cannot_change_under_it() {
    bounded(async {
        let server = Harness::start().await;

        // Trust: selected before anything else, or not at all.
        let mut raw = server.raw_socket().await;
        let first = raw.send(b"SELECT 1".to_vec()).await.unwrap();
        assert_ne!(first.first(), Some(&3), "SELECT 1 ran");
        let late = simple_text(&raw.send(b"\x01DATABASE;kopa".to_vec()).await.unwrap());
        assert!(late.contains("08P01"), "a late selection was taken: {late}");
        // Naming the database it already runs in changes nothing, and is fine.
        let same = simple_text(&raw.send(b"\x01DATABASE;postgres".to_vec()).await.unwrap());
        assert!(same.contains("SET"), "{same}");
        assert!(!same.contains("08P01"), "{same}");

        // A selection first is taken, once.
        let mut raw = server.raw_socket().await;
        let chosen = simple_text(&raw.send(b"\x01DATABASE;kopa".to_vec()).await.unwrap());
        assert!(chosen.contains("SET"), "{chosen}");
        let again = simple_text(&raw.send(b"\x01DATABASE;other".to_vec()).await.unwrap());
        assert!(again.contains("08P01"), "{again}");
        let missing = simple_text(
            &server.raw_socket().await.send(b"\x01DATABASE;nope".to_vec()).await.unwrap(),
        );
        assert!(missing.contains("3D000"), "{missing}");

        // A login checked against the default database's roles cannot move to
        // another database's: its role there may be someone else.
        let mut raw = server.raw_password().await;
        let moved = simple_text(&raw.send(b"\x01DATABASE;kopa".to_vec()).await.unwrap());
        assert!(moved.contains("28000"), "a login moved database: {moved}");
        let stay = simple_text(&raw.send(b"\x01DATABASE;postgres".to_vec()).await.unwrap());
        assert!(stay.contains("SET"), "{stay}");
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_native_connection_counts_among_its_databases_sessions() {
    bounded(async {
        let server = Harness::start().await;
        let kopa = server.trust(Some("kopa")).await.unwrap();
        // Its first request makes its session, in kopa.
        assert_eq!(current_database(&kopa).await, "kopa");

        let answer = kopa.simple_query("SHOW DATABASES").await.unwrap();
        let rasterizeddb_client::SimpleResult::Rows { columns, rows, .. } = &answer.results[0]
        else {
            panic!("SHOW DATABASES answered {:?}", answer.results[0]);
        };
        let at = |name: &str| columns.iter().position(|c| c.name == name).unwrap();
        let (name, current, sessions) = (at("database_name"), at("is_current"), at("sessions"));
        // A native session resolves against a view of its own database, so
        // that is what it lists; whatever else is listed, the current one is
        // kopa, never the default.
        let kopa_row = rows
            .iter()
            .find(|r| r[name].as_deref() == Some("kopa"))
            .unwrap_or_else(|| panic!("no kopa in {rows:?}"));
        assert_eq!(kopa_row[current].as_deref(), Some("t"), "kopa is not current: {rows:?}");
        for other in rows.iter().filter(|r| r[name].as_deref() != Some("kopa")) {
            assert_ne!(other[current].as_deref(), Some("t"), "{rows:?}");
        }
        let n: i64 = kopa_row[sessions].as_deref().unwrap().parse().unwrap();
        assert!(n >= 1, "kopa's native connection is not counted: {rows:?}");
    })
    .await;
}

/// What a trust connection may do, by the role it names (roles plan R0:
/// `trust` authenticates as the named role, never as an administrator by
/// itself). Unnamed: no role statement, no table another role owns, only
/// what it made itself or PUBLIC is granted. Named: that role, superuser
/// or not. A role that does not exist is refused at connect.
#[tokio::test(flavor = "multi_thread")]
async fn a_trust_connection_is_the_role_it_names() {
    bounded(async {
        let server = Harness::start().await;
        let admin = server.trust_as("rasterized", None).await.expect("the superuser names itself");
        admin.execute("CREATE TABLE menu (id INT)", &[]).await.unwrap();
        admin.execute("INSERT INTO menu (id) VALUES (1)", &[]).await.unwrap();
        admin.execute("CREATE USER waiter PASSWORD 'tray'", &[]).await.unwrap();

        // Unnamed: no administrator, and not the owner of menu.
        let anonymous = server.trust(None).await.expect("an unnamed trust connection still connects");
        let refused = anonymous.execute("ALTER USER rasterized PASSWORD 'x'", &[]).await;
        assert!(refused.is_err(), "an unnamed trust connection changed a role");
        let read = count(&anonymous, "menu").await;
        assert!(read.is_err(), "an unnamed trust connection read a table it does not own: {read:?}");
        // What PUBLIC is granted it has; what it makes it owns.
        admin.execute("GRANT SELECT ON menu TO PUBLIC", &[]).await.unwrap();
        assert_eq!(count(&anonymous, "menu").await.unwrap(), 1);
        anonymous.execute("CREATE TABLE scratch (n INT)", &[]).await.expect("it makes its own tables");
        anonymous.execute("INSERT INTO scratch (n) VALUES (1)", &[]).await.expect("and writes them");

        // Named, not a superuser: that role's privileges, no more.
        let waiter = server.trust_as("waiter", None).await.expect("waiter names itself");
        assert!(waiter.execute("ALTER USER rasterized PASSWORD 'x'", &[]).await.is_err());
        assert_eq!(count(&waiter, "menu").await.unwrap(), 1, "PUBLIC's grant");
        // The superuser, named, administers.
        admin.execute("ALTER USER waiter PASSWORD 'tray2'", &[]).await.expect("the named superuser administers");

        // A role that does not exist, or may not log in, is refused at once.
        let nobody = server.trust_as("nobody", None).await.err().expect("an unknown role is refused");
        assert!(nobody.to_string().contains("does not exist"), "{nobody}");
        // With a database: the role is checked against that database's roles.
        admin.execute("CREATE DATABASE kitchen", &[]).await.unwrap();
        let elsewhere = server.trust_as("waiter", Some("kitchen")).await.err().expect("waiter is not kitchen's");
        assert!(elsewhere.to_string().contains("does not exist"), "{elsewhere}");
        let kitchen = server.trust_as("rasterized", Some("kitchen")).await.expect("kitchen's superuser");
        assert_eq!(current_database(&kitchen).await, "kitchen");
    })
    .await;
}
