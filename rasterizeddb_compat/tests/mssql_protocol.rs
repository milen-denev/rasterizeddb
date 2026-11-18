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
    let tcp = tokio::net::TcpStream::connect(("127.0.0.1", s.port))
        .await
        .unwrap();
    tcp.set_nodelay(true).unwrap();
    Client::connect(c, tcp.compat_write())
        .await
        .expect("tds connect")
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

    let row = c
        .simple_query("SELECT @@VERSION")
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap();
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
    let seen = chrono::NaiveDate::from_ymd_opt(2024, 5, 6)
        .unwrap()
        .and_hms_micro_opt(7, 8, 9, 123456)
        .unwrap();
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
        .query(
            "SELECT Id, Name, Price, Seen, Ok, Data FROM Items WHERE Id = @P1",
            &[&id],
        )
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
    let r = c
        .execute(
            "INSERT INTO Items (Name) VALUES (@P1), (@P2)",
            &[&"a", &"b"],
        )
        .await
        .unwrap();
    assert_eq!(r.total(), 2);
    let e = c
        .execute("INSERT INTO Items (Name) VALUES (@P1)", &[&"a"])
        .await
        .unwrap_err();
    assert_eq!(number(&e), Some(2627), "{e}");
    let e = c.simple_query("SELECT Nope FROM Items").await.unwrap_err();
    assert_eq!(number(&e), Some(207), "{e}");
    let e = c
        .simple_query("SELECT Nope AS [N] FROM Items")
        .await
        .unwrap_err();
    assert_eq!(number(&e), Some(207), "{e}");
    let e = c.simple_query("SELECT * FROM Missing").await.unwrap_err();
    assert_eq!(number(&e), Some(208), "{e}");

    // An optional filter, with the parameter NULL and not.
    for (p, want) in [(None, 3), (Some("a"), 1)] {
        let n = c
            .query(
                "SELECT COUNT(*) FROM Items WHERE @P1 IS NULL OR Name = @P1",
                &[&p],
            )
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
    c.simple_query("BEGIN TRAN")
        .await
        .unwrap()
        .into_results()
        .await
        .unwrap();
    c.simple_query("BEGIN TRAN")
        .await
        .unwrap()
        .into_results()
        .await
        .unwrap();
    let n = c
        .simple_query("SELECT @@TRANCOUNT")
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap()
        .get::<i32, _>(0);
    assert_eq!(n, Some(2));
    c.execute("INSERT INTO Items (Name) VALUES (@P1)", &[&"t1"])
        .await
        .unwrap();
    assert!(
        c.execute("INSERT INTO Items (Name) VALUES (@P1)", &[&"a"])
            .await
            .is_err()
    );
    c.simple_query("COMMIT")
        .await
        .unwrap()
        .into_results()
        .await
        .unwrap();
    c.simple_query("COMMIT")
        .await
        .unwrap()
        .into_results()
        .await
        .unwrap();
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
    c.simple_query("BEGIN TRAN; DELETE FROM Items; ROLLBACK")
        .await
        .unwrap()
        .into_results()
        .await
        .unwrap();
    let n = c
        .simple_query("SELECT COUNT(*) FROM Items")
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap()
        .get::<i32, _>(0);
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
        .query(
            "INSERT INTO Items (Name) OUTPUT INSERTED.Id VALUES (@P1)",
            &[&"out"],
        )
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
        &format!(
            "host=127.0.0.1 port={} user=rasterized password={PASSWORD} dbname=postgres",
            s.pg_port
        ),
        tokio_postgres::NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(conn);
    let n: i64 = pg
        .query_one("SELECT COUNT(*) FROM items", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(n, 5);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tiberius_socket_and_refusals() {
    let s = start();
    // The unix socket: no TLS there, and the same login.
    let unix = tokio::net::UnixStream::connect(&s.socket).await.unwrap();
    let mut c = Client::connect(
        config(&s, PASSWORD, EncryptionLevel::NotSupported),
        unix.compat_write(),
    )
    .await
    .expect("socket connect");
    let v = c
        .simple_query("SELECT 40 + 2")
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap()
        .get::<i32, _>(0);
    assert_eq!(v, Some(42));

    // A wrong password is 18456.
    let tcp = tokio::net::TcpStream::connect(("127.0.0.1", s.port))
        .await
        .unwrap();
    let e = Client::connect(
        config(&s, "wrong", EncryptionLevel::Required),
        tcp.compat_write(),
    )
    .await
    .err()
    .expect("a wrong password is refused");
    assert_eq!(number(&e), Some(18456), "{e}");
}

async fn batch(
    c: &mut Client<Compat<tokio::net::TcpStream>>,
    sql: &str,
) -> Result<Vec<Vec<tiberius::Row>>, tiberius::error::Error> {
    c.simple_query(sql).await?.into_results().await
}

/// Stored procedures (parameters, defaults, OUTPUT, RETURN, nesting, their
/// own #tables), table variables and cursors: kept by the front end, since
/// the engine has none of them.
#[tokio::test(flavor = "multi_thread")]
async fn tiberius_procedures_cursors_table_variables() {
    let s = start();
    let mut c = connect(&s).await;
    batch(&mut c, "CREATE TABLE Nums (N int PRIMARY KEY, Label nvarchar(20))").await.unwrap();
    batch(&mut c, "INSERT INTO Nums VALUES (1, N'one'), (2, N'two'), (3, N'three')").await.unwrap();

    batch(
        &mut c,
        "CREATE PROCEDURE dbo.CountAbove @min int = 0, @total int OUTPUT AS
         BEGIN
           SET NOCOUNT ON;
           SELECT @total = COUNT(*) FROM Nums WHERE N > @min;
           SELECT Label FROM Nums WHERE N > @min ORDER BY N;
           RETURN 7
         END",
    )
    .await
    .unwrap();
    let r = batch(
        &mut c,
        "DECLARE @t int, @rc int; EXEC @rc = CountAbove @min = 1, @total = @t OUTPUT; SELECT @t AS total, @rc AS rc",
    )
    .await
    .unwrap();
    let labels: Vec<&str> = r[0].iter().map(|row| row.get::<&str, _>(0).unwrap()).collect();
    assert_eq!(labels, ["two", "three"]);
    assert_eq!(r[1][0].get::<i32, _>("total"), Some(2));
    assert_eq!(r[1][0].get::<i32, _>("rc"), Some(7));
    // The default, positionally.
    let r = batch(&mut c, "DECLARE @t int; EXEC CountAbove DEFAULT, @t OUTPUT; SELECT @t").await.unwrap();
    assert_eq!(r.last().unwrap()[0].get::<i32, _>(0), Some(3));
    // Errors by SQL Server's numbers.
    let e = batch(&mut c, "EXEC CountAbove").await.unwrap_err();
    assert_eq!(number(&e), Some(201), "{e}");
    let e = batch(&mut c, "EXEC NoSuchProc").await.unwrap_err();
    assert_eq!(number(&e), Some(2812), "{e}");
    let e = batch(&mut c, "CREATE PROCEDURE CountAbove AS SELECT 1").await.unwrap_err();
    assert_eq!(number(&e), Some(2714), "{e}");
    let e = batch(&mut c, "DECLARE @t int; EXEC CountAbove 1, @t OUTPUT, 5").await.unwrap_err();
    assert_eq!(number(&e), Some(8144), "{e}");
    // The catalogue sees it.
    let r = batch(
        &mut c,
        "SELECT CASE WHEN OBJECT_ID(N'dbo.CountAbove', N'P') IS NOT NULL THEN 1 ELSE 0 END AS p,
                CASE WHEN OBJECT_ID(N'dbo.CountAbove', N'U') IS NULL THEN 1 ELSE 0 END AS u;
         SELECT COUNT(*) AS n FROM sys.procedures WHERE name = N'countabove'",
    )
    .await
    .unwrap();
    assert_eq!(r[0][0].get::<i32, _>("p"), Some(1));
    assert_eq!(r[0][0].get::<i32, _>("u"), Some(1));
    assert_eq!(r[1][0].get::<i32, _>("n"), Some(1));
    // CREATE OR ALTER replaces it; DROP removes it.
    batch(&mut c, "CREATE OR ALTER PROCEDURE CountAbove @min int = 0, @total int OUTPUT AS SET @total = 99").await.unwrap();
    let r = batch(&mut c, "DECLARE @t int; EXEC CountAbove @total = @t OUTPUT; SELECT @t").await.unwrap();
    assert_eq!(r.last().unwrap()[0].get::<i32, _>(0), Some(99));
    batch(&mut c, "DROP PROCEDURE CountAbove").await.unwrap();
    let e = batch(&mut c, "EXEC CountAbove").await.unwrap_err();
    assert_eq!(number(&e), Some(2812), "{e}");
    batch(&mut c, "DROP PROCEDURE IF EXISTS CountAbove").await.unwrap();
    let e = batch(&mut c, "DROP PROCEDURE CountAbove").await.unwrap_err();
    assert_eq!(number(&e), Some(3701), "{e}");

    // A #table made in a procedure goes when it returns: a second call
    // makes it again.
    batch(
        &mut c,
        "CREATE PROCEDURE Fill AS BEGIN
           CREATE TABLE #work (n int);
           INSERT INTO #work SELECT N FROM Nums;
           SELECT COUNT(*) AS c FROM #work
         END",
    )
    .await
    .unwrap();
    for _ in 0..2 {
        let r = batch(&mut c, "EXEC Fill").await.unwrap();
        assert_eq!(r.last().unwrap()[0].get::<i32, _>("c"), Some(3));
    }
    // Nesting stops at 32.
    batch(
        &mut c,
        "CREATE PROCEDURE Deep @n int AS BEGIN DECLARE @m int = @n - 1; IF @n > 0 EXEC Deep @m END",
    )
    .await
    .unwrap();
    batch(&mut c, "EXEC Deep 10").await.unwrap();
    let e = batch(&mut c, "EXEC Deep 40").await.unwrap_err();
    assert_eq!(number(&e), Some(217), "{e}");

    // Table variables: declared once per scope, gone after the batch.
    let r = batch(
        &mut c,
        "DECLARE @t TABLE (id int, name nvarchar(10));
         DECLARE @i int = 0;
         WHILE @i < 2 BEGIN
           DECLARE @again TABLE (x int);
           INSERT INTO @again VALUES (@i);
           SET @i = @i + 1
         END
         INSERT INTO @t VALUES (1, N'a'), (2, N'b');
         UPDATE @t SET name = N'B' WHERE id = 2;
         SELECT t.name FROM @t AS t ORDER BY t.id;
         SELECT COUNT(*) AS c FROM @again",
    )
    .await
    .unwrap();
    let names: Vec<&str> = r[0].iter().map(|row| row.get::<&str, _>(0).unwrap()).collect();
    assert_eq!(names, ["a", "B"]);
    assert_eq!(r[1][0].get::<i32, _>("c"), Some(2));
    assert!(batch(&mut c, "SELECT * FROM @t").await.is_err());

    // A cursor loop, and a scrollable cursor read without INTO.
    let r = batch(
        &mut c,
        "DECLARE @n int, @sum int = 0;
         DECLARE c CURSOR LOCAL FAST_FORWARD FOR SELECT N FROM Nums ORDER BY N;
         OPEN c;
         FETCH NEXT FROM c INTO @n;
         WHILE @@FETCH_STATUS = 0
         BEGIN
           SET @sum = @sum + @n;
           FETCH NEXT FROM c INTO @n;
         END
         CLOSE c;
         DEALLOCATE c;
         SELECT @sum AS s",
    )
    .await
    .unwrap();
    assert_eq!(r.last().unwrap()[0].get::<i32, _>("s"), Some(6));
    let r = batch(
        &mut c,
        "DECLARE s SCROLL CURSOR FOR SELECT N, Label FROM Nums ORDER BY N;
         OPEN s;
         FETCH LAST FROM s;
         FETCH ABSOLUTE 1 FROM s;
         CLOSE s;
         DEALLOCATE s",
    )
    .await
    .unwrap();
    assert_eq!(r[0][0].get::<i32, _>(0), Some(3));
    assert_eq!(r[1][0].get::<&str, _>(1), Some("one"));
    let e = batch(&mut c, "DECLARE f CURSOR FOR SELECT N FROM Nums; OPEN f; FETCH LAST FROM f").await.unwrap_err();
    assert_eq!(number(&e), Some(16911), "{e}");
    let e = batch(&mut c, "OPEN nothere").await.unwrap_err();
    assert_eq!(number(&e), Some(16916), "{e}");

    // A batch's #table lasts for the connection, and only it sees it.
    batch(&mut c, "CREATE TABLE #keep (a int); INSERT INTO #keep VALUES (1), (2)").await.unwrap();
    batch(&mut c, "SELECT N INTO #copy FROM Nums WHERE N > 1").await.unwrap();
    let r = batch(&mut c, "SELECT COUNT(*) AS k FROM #keep; SELECT COUNT(*) AS c FROM #copy").await.unwrap();
    assert_eq!(r[0][0].get::<i32, _>("k"), Some(2));
    assert_eq!(r[1][0].get::<i32, _>("c"), Some(2));
    let mut other = connect(&s).await;
    let e = batch(&mut other, "SELECT * FROM #keep").await.unwrap_err();
    assert_eq!(number(&e), Some(208), "{e}");
    batch(&mut other, "CREATE TABLE #keep (b nvarchar(5)); INSERT INTO #keep VALUES (N'x')").await.unwrap();
    let r = batch(&mut other, "SELECT b FROM #keep").await.unwrap();
    assert_eq!(r[0][0].get::<&str, _>(0), Some("x"));
    let r = batch(
        &mut c,
        "IF OBJECT_ID('tempdb..#keep') IS NOT NULL DROP TABLE #keep;
         SELECT CASE WHEN OBJECT_ID('tempdb..#keep') IS NULL THEN 1 ELSE 0 END AS gone",
    )
    .await
    .unwrap();
    assert_eq!(r.last().unwrap()[0].get::<i32, _>("gone"), Some(1));
}
