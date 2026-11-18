//! The SQL Server protocol, driven by tiberius: TLS inside PRELOGIN, SQL
//! Server authentication, batches, sp_executesql with typed parameters,
//! IDENTITY and SCOPE_IDENTITY, error numbers, nested transactions, the unix
//! socket, and the same rows seen over PostgreSQL.

use rasterizeddb_core::{
    core::database::catalog::DatabaseCatalog,
    mssql::{MsAuth, MsConfig, MsSocket, MsTls},
    pgwire::{AuthConfig, ConnectionLimits, TlsConfig, TlsMode, start_pgwire_on_listener},
};
use rclite::Arc;
use tiberius::{AuthMethod, Client, Config, EncryptionLevel};
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

const PASSWORD: &str = "Tds-Pass#1";

struct Servers {
    port: u16,
    pg_port: u16,
    socket: std::path::PathBuf,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl Drop for Servers {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
        let _ = std::fs::remove_file(&self.socket);
    }
}

fn start() -> Servers {
    let _ = env_logger::builder().is_test(true).try_init();
    static NEXT: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    // A socket path must fit sun_path: kept short, under /tmp.
    let socket = std::path::PathBuf::from(format!(
        "/tmp/rdb_tds_{}_{}.sock",
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    let socket_path = socket.clone();
    let (ports_tx, ports_rx) = std::sync::mpsc::channel();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let handle = std::thread::Builder::new()
        .stack_size(32 * 1024 * 1024)
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .thread_stack_size(32 * 1024 * 1024)
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async move {
                let catalog: Arc<DatabaseCatalog> = DatabaseCatalog::open(None, "postgres").await.unwrap();
                let config = MsConfig {
                    tcp: Some(("127.0.0.1".into(), 0)),
                    socket: Some(MsSocket { path: socket_path, mode: 0o600, group: None, auth: None }),
                    auth: MsAuth::Password,
                    tls: MsTls::Prefer,
                    cert_path: None,
                    key_path: None,
                    bootstrap_user: "rasterized".into(),
                    bootstrap_password: Some(PASSWORD.into()),
                    limits: ConnectionLimits::default(),
                };
                let listener = rasterizeddb_core::mssql::bind(config).await.unwrap();
                let port = listener.local_addr().unwrap().port();
                let server = rasterizeddb_core::mssql::serve(listener, catalog.clone()).await.unwrap();
                let pg = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let pg_port = pg.local_addr().unwrap().port();
                ports_tx.send((port, pg_port)).unwrap();
                let auth = AuthConfig {
                    method: rasterizeddb_core::auth::AuthMethod::ScramSha256,
                    bootstrap_user: "rasterized".into(),
                    bootstrap_password: Some(PASSWORD.into()),
                };
                let tls = TlsConfig { mode: TlsMode::Disable, cert_path: None, key_path: None };
                tokio::select! {
                    _ = start_pgwire_on_listener(catalog, pg, tls, auth, ConnectionLimits::default()) => {}
                    _ = shutdown_rx => {}
                }
                server.drain(std::time::Duration::from_secs(2)).await;
            });
            rt.shutdown_timeout(std::time::Duration::from_secs(2));
        })
        .unwrap();
    let (port, pg_port) = ports_rx.recv().unwrap();
    Servers {
        port,
        pg_port,
        socket,
        shutdown: Some(shutdown_tx),
        handle: Some(handle),
    }
}

fn config(s: &Servers, password: &str, encryption: EncryptionLevel) -> Config {
    let mut c = Config::new();
    c.host("127.0.0.1");
    c.port(s.port);
    c.authentication(AuthMethod::sql_server("rasterized", password));
    c.encryption(encryption);
    c.trust_cert();
    c.database("postgres");
    c
}

async fn connect(s: &Servers) -> Client<Compat<tokio::net::TcpStream>> {
    let c = config(s, PASSWORD, EncryptionLevel::Required);
    let tcp = tokio::net::TcpStream::connect(("127.0.0.1", s.port)).await.unwrap();
    tcp.set_nodelay(true).unwrap();
    Client::connect(c, tcp.compat_write()).await.expect("tds connect")
}

fn number(e: &tiberius::error::Error) -> Option<u32> {
    match e {
        tiberius::error::Error::Server(t) => Some(t.code()),
        _ => None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tiberius_end_to_end() {
    let s = start();
    let mut c = connect(&s).await;

    let row = c.simple_query("SELECT @@VERSION").await.unwrap().into_row().await.unwrap().unwrap();
    assert!(row.get::<&str, _>(0).unwrap().contains("RasterizedDB"));

    c.simple_query(
        "IF OBJECT_ID(N'dbo.Items') IS NULL
         CREATE TABLE [dbo].[Items] (
            [Id] int IDENTITY(1,1) NOT NULL,
            [Name] nvarchar(50) NOT NULL,
            [Price] decimal(10,2) NULL,
            [Seen] datetime2 NULL,
            [Ok] bit NOT NULL DEFAULT ((1)),
            [Data] varbinary(max) NULL,
            CONSTRAINT [PK_Items] PRIMARY KEY CLUSTERED ([Id] ASC),
            CONSTRAINT [UQ_Items_Name] UNIQUE ([Name])
         )",
    )
    .await
    .unwrap()
    .into_results()
    .await
    .unwrap();

    // sp_executesql with typed parameters, and SCOPE_IDENTITY.
    let seen = chrono::NaiveDate::from_ymd_opt(2024, 5, 6).unwrap().and_hms_micro_opt(7, 8, 9, 123456).unwrap();
    let price = rust_decimal::Decimal::new(1999, 2);
    let id: i32 = c
        .query(
            "INSERT INTO Items (Name, Price, Seen, Data) VALUES (@P1, @P2, @P3, @P4); SELECT CAST(SCOPE_IDENTITY() AS int)",
            &[&"widget", &price, &seen, &vec![0u8, 1, 255]],
        )
        .await
        .unwrap()
        .into_results()
        .await
        .unwrap()
        .last()
        .unwrap()[0]
        .get::<i32, _>(0)
        .unwrap();
    assert_eq!(id, 1);

    let row = c
        .query("SELECT Id, Name, Price, Seen, Ok, Data FROM Items WHERE Id = @P1", &[&id])
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.get::<i32, _>("Id"), Some(1));
    assert_eq!(row.get::<&str, _>("Name"), Some("widget"));
    assert_eq!(row.get::<rust_decimal::Decimal, _>("Price"), Some(price));
    assert_eq!(row.get::<chrono::NaiveDateTime, _>("Seen"), Some(seen));
    assert_eq!(row.get::<bool, _>("Ok"), Some(true));
    assert_eq!(row.get::<&[u8], _>("Data"), Some(&[0u8, 1, 255][..]));

    // Rows affected, and errors by SQL Server's numbers.
    let r = c.execute("INSERT INTO Items (Name) VALUES (@P1), (@P2)", &[&"a", &"b"]).await.unwrap();
    assert_eq!(r.total(), 2);
    let e = c.execute("INSERT INTO Items (Name) VALUES (@P1)", &[&"a"]).await.unwrap_err();
    assert_eq!(number(&e), Some(2627), "{e}");
    let e = c.simple_query("SELECT Nope FROM Items").await.unwrap_err();
    assert_eq!(number(&e), Some(207), "{e}");
    let e = c.simple_query("SELECT Nope AS [N] FROM Items").await.unwrap_err();
    assert_eq!(number(&e), Some(207), "{e}");
    let e = c.simple_query("SELECT * FROM Missing").await.unwrap_err();
    assert_eq!(number(&e), Some(208), "{e}");

    // An optional filter, with the parameter NULL and not.
    for (p, want) in [(None, 3), (Some("a"), 1)] {
        let n = c
            .query("SELECT COUNT(*) FROM Items WHERE @P1 IS NULL OR Name = @P1", &[&p])
            .await
            .unwrap()
            .into_row()
            .await
            .unwrap()
            .unwrap()
            .get::<i32, _>(0);
        assert_eq!(n, Some(want), "{p:?}");
    }
    let n = c
        .simple_query("DECLARE @n nvarchar(10) = NULL; SELECT COUNT(*) FROM Items WHERE @n IS NOT NULL AND Name = @n")
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap()
        .get::<i32, _>(0);
    assert_eq!(n, Some(0));

    // Nested transactions: only the outer COMMIT commits; a failed
    // statement inside fails alone.
    c.simple_query("BEGIN TRAN").await.unwrap().into_results().await.unwrap();
    c.simple_query("BEGIN TRAN").await.unwrap().into_results().await.unwrap();
    let n = c.simple_query("SELECT @@TRANCOUNT").await.unwrap().into_row().await.unwrap().unwrap().get::<i32, _>(0);
    assert_eq!(n, Some(2));
    c.execute("INSERT INTO Items (Name) VALUES (@P1)", &[&"t1"]).await.unwrap();
    assert!(c.execute("INSERT INTO Items (Name) VALUES (@P1)", &[&"a"]).await.is_err());
    c.simple_query("COMMIT").await.unwrap().into_results().await.unwrap();
    c.simple_query("COMMIT").await.unwrap().into_results().await.unwrap();
    let n = c
        .simple_query("SELECT COUNT(*) FROM Items WHERE Name = N't1'")
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap()
        .get::<i32, _>(0);
    assert_eq!(n, Some(1));
    c.simple_query("BEGIN TRAN; DELETE FROM Items; ROLLBACK").await.unwrap().into_results().await.unwrap();
    let n = c.simple_query("SELECT COUNT(*) FROM Items").await.unwrap().into_row().await.unwrap().unwrap().get::<i32, _>(0);
    assert_eq!(n, Some(4));

    // TOP, OFFSET/FETCH, + as concatenation, OUTPUT.
    let rows = c
        .simple_query("SELECT TOP (2) N'<' + Name + N'>' AS x FROM Items ORDER BY Id")
        .await
        .unwrap()
        .into_first_result()
        .await
        .unwrap();
    let names: Vec<&str> = rows.iter().map(|r| r.get::<&str, _>(0).unwrap()).collect();
    assert_eq!(names, vec!["<widget>", "<a>"]);
    let rows = c
        .simple_query("SELECT Name FROM Items ORDER BY Id OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY")
        .await
        .unwrap()
        .into_first_result()
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    let out = c
        .query("INSERT INTO Items (Name) OUTPUT INSERTED.Id VALUES (@P1)", &[&"out"])
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap()
        .get::<i32, _>(0)
        .unwrap();
    assert!(out > 4);

    // The same rows over PostgreSQL.
    let (pg, conn) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=rasterized password={PASSWORD} dbname=postgres", s.pg_port),
        tokio_postgres::NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(conn);
    let n: i64 = pg.query_one("SELECT COUNT(*) FROM items", &[]).await.unwrap().get(0);
    assert_eq!(n, 5);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tiberius_socket_and_refusals() {
    let s = start();
    // The unix socket: no TLS there, and the same login.
    let unix = tokio::net::UnixStream::connect(&s.socket).await.unwrap();
    let mut c = Client::connect(config(&s, PASSWORD, EncryptionLevel::NotSupported), unix.compat_write())
        .await
        .expect("socket connect");
    let v = c.simple_query("SELECT 40 + 2").await.unwrap().into_row().await.unwrap().unwrap().get::<i32, _>(0);
    assert_eq!(v, Some(42));

    // A wrong password is 18456.
    let tcp = tokio::net::TcpStream::connect(("127.0.0.1", s.port)).await.unwrap();
    let e = Client::connect(config(&s, "wrong", EncryptionLevel::Required), tcp.compat_write())
        .await
        .err()
        .expect("a wrong password is refused");
    assert_eq!(number(&e), Some(18456), "{e}");
}
