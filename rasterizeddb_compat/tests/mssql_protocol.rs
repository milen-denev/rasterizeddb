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
    start_with(false)
}

/// A server whose text columns are case-insensitive by default
/// (`MsConfig::case_insensitive`, `--mssql-collation ci`).
fn start_with(case_insensitive: bool) -> Servers {
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
                    // No unix socket where the platform has none: `bind` refuses one
                    // there, and only `tiberius_socket_and_refusals` (unix only) uses it.
                    socket: cfg!(unix).then(|| MsSocket { path: socket_path, mode: 0o600, group: None, auth: None }),
                    auth: MsAuth::Password,
                    tls: MsTls::Prefer,
                    cert_path: None,
                    key_path: None,
                    bootstrap_user: "rasterized".into(),
                    bootstrap_password: Some(PASSWORD.into()),
                    limits: ConnectionLimits::default(),
                    // Most tests pin the byte-compared answers they were
                    // written against; `start_with(true)` is SQL Server's
                    // case-insensitive default.
                    case_insensitive,
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
    connect_to(s, "postgres").await
}

async fn connect_to(s: &Servers, database: &str) -> Client<Compat<tokio::net::TcpStream>> {
    let mut c = config(s, PASSWORD, EncryptionLevel::Required);
    c.database(database);
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

#[cfg(unix)]
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
    batch(
        &mut c,
        "CREATE TABLE Nums (N int PRIMARY KEY, Label nvarchar(20))",
    )
    .await
    .unwrap();
    batch(
        &mut c,
        "INSERT INTO Nums VALUES (1, N'one'), (2, N'two'), (3, N'three')",
    )
    .await
    .unwrap();

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
    let labels: Vec<&str> = r[0]
        .iter()
        .map(|row| row.get::<&str, _>(0).unwrap())
        .collect();
    assert_eq!(labels, ["two", "three"]);
    assert_eq!(r[1][0].get::<i32, _>("total"), Some(2));
    assert_eq!(r[1][0].get::<i32, _>("rc"), Some(7));
    // The default, positionally.
    let r = batch(
        &mut c,
        "DECLARE @t int; EXEC CountAbove DEFAULT, @t OUTPUT; SELECT @t",
    )
    .await
    .unwrap();
    assert_eq!(r.last().unwrap()[0].get::<i32, _>(0), Some(3));
    // Errors by SQL Server's numbers.
    let e = batch(&mut c, "EXEC CountAbove").await.unwrap_err();
    assert_eq!(number(&e), Some(201), "{e}");
    let e = batch(&mut c, "EXEC NoSuchProc").await.unwrap_err();
    assert_eq!(number(&e), Some(2812), "{e}");
    let e = batch(&mut c, "CREATE PROCEDURE CountAbove AS SELECT 1")
        .await
        .unwrap_err();
    assert_eq!(number(&e), Some(2714), "{e}");
    let e = batch(&mut c, "DECLARE @t int; EXEC CountAbove 1, @t OUTPUT, 5")
        .await
        .unwrap_err();
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
    batch(
        &mut c,
        "CREATE OR ALTER PROCEDURE CountAbove @min int = 0, @total int OUTPUT AS SET @total = 99",
    )
    .await
    .unwrap();
    let r = batch(
        &mut c,
        "DECLARE @t int; EXEC CountAbove @total = @t OUTPUT; SELECT @t",
    )
    .await
    .unwrap();
    assert_eq!(r.last().unwrap()[0].get::<i32, _>(0), Some(99));
    batch(&mut c, "DROP PROCEDURE CountAbove").await.unwrap();
    let e = batch(&mut c, "EXEC CountAbove").await.unwrap_err();
    assert_eq!(number(&e), Some(2812), "{e}");
    batch(&mut c, "DROP PROCEDURE IF EXISTS CountAbove")
        .await
        .unwrap();
    let e = batch(&mut c, "DROP PROCEDURE CountAbove")
        .await
        .unwrap_err();
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
    let names: Vec<&str> = r[0]
        .iter()
        .map(|row| row.get::<&str, _>(0).unwrap())
        .collect();
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
    let e = batch(
        &mut c,
        "DECLARE f CURSOR FOR SELECT N FROM Nums; OPEN f; FETCH LAST FROM f",
    )
    .await
    .unwrap_err();
    assert_eq!(number(&e), Some(16911), "{e}");
    let e = batch(&mut c, "OPEN nothere").await.unwrap_err();
    assert_eq!(number(&e), Some(16916), "{e}");

    // A batch's #table lasts for the connection, and only it sees it.
    batch(
        &mut c,
        "CREATE TABLE #keep (a int); INSERT INTO #keep VALUES (1), (2)",
    )
    .await
    .unwrap();
    batch(&mut c, "SELECT N INTO #copy FROM Nums WHERE N > 1")
        .await
        .unwrap();
    let r = batch(
        &mut c,
        "SELECT COUNT(*) AS k FROM #keep; SELECT COUNT(*) AS c FROM #copy",
    )
    .await
    .unwrap();
    assert_eq!(r[0][0].get::<i32, _>("k"), Some(2));
    assert_eq!(r[1][0].get::<i32, _>("c"), Some(2));
    let mut other = connect(&s).await;
    let e = batch(&mut other, "SELECT * FROM #keep").await.unwrap_err();
    assert_eq!(number(&e), Some(208), "{e}");
    batch(
        &mut other,
        "CREATE TABLE #keep (b nvarchar(5)); INSERT INTO #keep VALUES (N'x')",
    )
    .await
    .unwrap();
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

/// A general MERGE: run as its parts, atomically, as SQL Server would.
#[tokio::test(flavor = "multi_thread")]
async fn tiberius_merge() {
    let s = start();
    let mut c = connect(&s).await;
    batch(
        &mut c,
        "CREATE TABLE Stock (Id int PRIMARY KEY, Qty int, Note nvarchar(10))",
    )
    .await
    .unwrap();
    batch(
        &mut c,
        "INSERT INTO Stock VALUES (1, 10, N'a'), (2, 20, N'b'), (3, 30, N'c'), (4, 40, N'd')",
    )
    .await
    .unwrap();
    // 1: updated; 2: deleted (Qty 0); 3: untouched by source → deleted by
    // BY SOURCE only when Qty > 25; 4: not in source, Qty 40 → deleted;
    // 5: inserted.
    let r = batch(
        &mut c,
        "MERGE INTO dbo.Stock WITH (HOLDLOCK) AS t
         USING (VALUES (1, 5), (2, 0), (5, 7)) AS s (Id, Qty) ON t.Id = s.Id
         WHEN MATCHED AND s.Qty = 0 THEN DELETE
         WHEN MATCHED THEN UPDATE SET Qty = t.Qty + s.Qty, Note = CASE WHEN s.Qty > 3 THEN N'big' ELSE N'small' END
         WHEN NOT MATCHED BY TARGET THEN INSERT (Id, Qty, Note) VALUES (s.Id, s.Qty, N'new')
         WHEN NOT MATCHED BY SOURCE AND t.Qty > 35 THEN DELETE
         WHEN NOT MATCHED BY SOURCE THEN UPDATE SET Note = N'kept';
         SELECT @@ROWCOUNT AS n;
         SELECT Id, Qty, Note FROM Stock ORDER BY Id",
    )
    .await
    .unwrap();
    assert_eq!(r[0][0].get::<i32, _>("n"), Some(5));
    let rows: Vec<(i32, i32, String)> = r[1]
        .iter()
        .map(|row| {
            (
                row.get(0).unwrap(),
                row.get(1).unwrap(),
                row.get::<&str, _>(2).unwrap().to_string(),
            )
        })
        .collect();
    assert_eq!(
        rows,
        [
            (1, 15, "big".to_string()),
            (3, 30, "kept".to_string()),
            (5, 7, "new".to_string())
        ]
    );
    // From a table, with parameters.
    batch(&mut c, "CREATE TABLE Incoming (Id int, Qty int)")
        .await
        .unwrap();
    batch(&mut c, "INSERT INTO Incoming VALUES (3, 1), (6, 2)")
        .await
        .unwrap();
    let n = c
        .execute(
            "MERGE Stock AS t USING Incoming AS s ON t.Id = s.Id
             WHEN MATCHED THEN UPDATE SET Qty = s.Qty * @P1
             WHEN NOT MATCHED THEN INSERT (Id, Qty, Note) VALUES (s.Id, s.Qty * @P1, @P2);",
            &[&100i32, &"param"],
        )
        .await
        .unwrap();
    assert_eq!(n.total(), 2);
    let r = batch(
        &mut c,
        "SELECT Qty, Note FROM Stock WHERE Id IN (3, 6) ORDER BY Id",
    )
    .await
    .unwrap();
    assert_eq!(r[0][0].get::<i32, _>(0), Some(100));
    assert_eq!(r[0][1].get::<i32, _>(0), Some(200));
    assert_eq!(r[0][1].get::<&str, _>(1), Some("param"));
    // A target row matched twice is an error, and nothing changes.
    let e = batch(
        &mut c,
        "MERGE Stock AS t USING (VALUES (1, 1), (1, 2)) AS s (Id, Qty) ON t.Id = s.Id
         WHEN MATCHED THEN UPDATE SET Qty = s.Qty
         WHEN NOT MATCHED THEN INSERT (Id, Qty) VALUES (s.Id + 100, s.Qty);",
    )
    .await
    .unwrap_err();
    assert_eq!(number(&e), Some(8672), "{e}");
    let r = batch(
        &mut c,
        "SELECT Qty FROM Stock WHERE Id = 1; SELECT COUNT(*) FROM Stock WHERE Id > 100",
    )
    .await
    .unwrap();
    assert_eq!(r[0][0].get::<i32, _>(0), Some(15));
    assert_eq!(r[1][0].get::<i32, _>(0), Some(0));
    // Inside a transaction that is rolled back.
    batch(
        &mut c,
        "BEGIN TRAN;
         MERGE Stock AS t USING (VALUES (1, 99)) AS s (Id, Qty) ON t.Id = s.Id WHEN MATCHED THEN UPDATE SET Qty = s.Qty;
         ROLLBACK",
    )
    .await
    .unwrap();
    let r = batch(&mut c, "SELECT Qty FROM Stock WHERE Id = 1")
        .await
        .unwrap();
    assert_eq!(r[0][0].get::<i32, _>(0), Some(15));
}

/// FOR JSON PATH/AUTO, FOR XML PATH (and the STUFF(… FOR XML PATH('')) way
/// of joining strings), and T-SQL inside subqueries (TOP).
#[tokio::test(flavor = "multi_thread")]
async fn tiberius_for_json_and_xml() {
    let s = start();
    let mut c = connect(&s).await;
    batch(
        &mut c,
        "CREATE TABLE Authors (Id int PRIMARY KEY, Name nvarchar(40), Born int)",
    )
    .await
    .unwrap();
    batch(
        &mut c,
        "CREATE TABLE Books (Id int PRIMARY KEY, AuthorId int, Title nvarchar(60))",
    )
    .await
    .unwrap();
    batch(
        &mut c,
        "INSERT INTO Authors VALUES (1, N'Ann \"A\" <x>', 1970), (2, N'Bo', NULL)",
    )
    .await
    .unwrap();
    batch(
        &mut c,
        "INSERT INTO Books VALUES (10, 1, N'First'), (11, 1, N'Second'), (12, 2, N'Third')",
    )
    .await
    .unwrap();

    let json = |r: &Vec<Vec<tiberius::Row>>| r[0][0].get::<&str, _>(0).unwrap().to_string();
    // Compact, in column order, NULLs left out, nested by dotted names.
    let r = batch(&mut c, "SELECT Id, Name AS [person.name], Born AS [person.born] FROM Authors ORDER BY Id FOR JSON PATH").await.unwrap();
    assert_eq!(
        json(&r),
        r#"[{"Id":1,"person":{"name":"Ann \"A\" <x>","born":1970}},{"Id":2,"person":{"name":"Bo"}}]"#
    );
    let r = batch(&mut c, "SELECT Id, Born FROM Authors WHERE Id = 2 FOR JSON PATH, INCLUDE_NULL_VALUES, WITHOUT_ARRAY_WRAPPER").await.unwrap();
    assert_eq!(json(&r), r#"{"Id":2,"Born":null}"#);
    let r = batch(
        &mut c,
        "SELECT Id FROM Authors ORDER BY Id DESC FOR JSON AUTO, ROOT('authors')",
    )
    .await
    .unwrap();
    assert_eq!(json(&r), r#"{"authors":[{"Id":2},{"Id":1}]}"#);
    // A nested FOR JSON is JSON, not a string; aggregates go through a
    // derived table.
    let r = batch(
        &mut c,
        "SELECT a.Name AS name,
                (SELECT b.Title AS title FROM Books b WHERE b.AuthorId = a.Id ORDER BY b.Id FOR JSON PATH) AS books
         FROM Authors a ORDER BY a.Id FOR JSON PATH",
    )
    .await
    .unwrap();
    assert_eq!(
        json(&r),
        r#"[{"name":"Ann \"A\" <x>","books":[{"title":"First"},{"title":"Second"}]},{"name":"Bo","books":[{"title":"Third"}]}]"#
    );
    let r = batch(&mut c, "SELECT AuthorId AS a, COUNT(*) AS n FROM Books GROUP BY AuthorId ORDER BY AuthorId FOR JSON PATH").await.unwrap();
    assert_eq!(json(&r), r#"[{"a":1,"n":2},{"a":2,"n":1}]"#);
    // No rows: no rows at the top, NULL in a subquery.
    let r = batch(&mut c, "SELECT Id FROM Authors WHERE Id > 5 FOR JSON PATH")
        .await
        .unwrap();
    assert!(r.is_empty() || r[0].is_empty(), "{r:?}");
    let r = batch(
        &mut c,
        "SELECT ISNULL((SELECT Id FROM Authors WHERE Id > 5 FOR JSON PATH), '[]') AS j",
    )
    .await
    .unwrap();
    assert_eq!(json(&r), "[]");
    let e = batch(&mut c, "SELECT Id + 1 FROM Authors FOR JSON PATH")
        .await
        .unwrap_err();
    assert_eq!(number(&e), Some(13605), "{e}");

    // FOR XML PATH: elements, escaping, and the string-joining idioms.
    let r = batch(
        &mut c,
        "SELECT Id, Name FROM Authors ORDER BY Id FOR XML PATH('author'), ROOT('authors')",
    )
    .await
    .unwrap();
    assert_eq!(
        json(&r),
        "<authors><author><Id>1</Id><Name>Ann \"A\" &lt;x&gt;</Name></author><author><Id>2</Id><Name>Bo</Name></author></authors>"
    );
    let r = batch(
        &mut c,
        "SELECT a.Id, STUFF((SELECT ', ' + b.Title FROM Books b WHERE b.AuthorId = a.Id ORDER BY b.Id FOR XML PATH('')), 1, 2, '') AS titles
         FROM Authors a ORDER BY a.Id",
    )
    .await
    .unwrap();
    assert_eq!(r[0][0].get::<&str, _>("titles"), Some("First, Second"));
    assert_eq!(r[0][1].get::<&str, _>("titles"), Some("Third"));
    let r = batch(
        &mut c,
        "SELECT (SELECT ';' + Name FROM Authors ORDER BY Id FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)') AS raw,
                (SELECT ';' + Name FROM Authors ORDER BY Id FOR XML PATH('')) AS escaped",
    )
    .await
    .unwrap();
    assert_eq!(r[0][0].get::<&str, _>("raw"), Some(";Ann \"A\" <x>;Bo"));
    assert_eq!(
        r[0][0].get::<&str, _>("escaped"),
        Some(";Ann \"A\" &lt;x&gt;;Bo")
    );

    // TOP inside a subquery.
    let r = batch(
        &mut c,
        "SELECT (SELECT TOP 1 Title FROM Books ORDER BY Id DESC) AS last",
    )
    .await
    .unwrap();
    assert_eq!(r[0][0].get::<&str, _>("last"), Some("Third"));
}

/// Full-text search: CREATE FULLTEXT CATALOG / INDEX, CONTAINS, FREETEXT,
/// CONTAINSTABLE and FREETEXTTABLE, answered from the engine's GIN indexes.
#[tokio::test(flavor = "multi_thread")]
async fn tiberius_full_text_search() {
    let s = start();
    let mut c = connect(&s).await;
    batch(
        &mut c,
        "CREATE TABLE Docs (Id int NOT NULL PRIMARY KEY, Title nvarchar(100), Body nvarchar(max));
         INSERT INTO Docs VALUES
           (1, N'Running shoes', N'Light shoes for a daily run'),
           (2, N'Trail guide', N'Where the trail runs past the old mill'),
           (3, N'Apple pie', N'A recipe for apple pie with a crumbly crust'),
           (4, N'Pie charts', N'Charts for applications'),
           (5, NULL, N'The quick brown fox jumps over the lazy dog');
         CREATE FULLTEXT CATALOG ftCatalog AS DEFAULT;
         CREATE FULLTEXT INDEX ON dbo.Docs (Title LANGUAGE 1033, Body) KEY INDEX PK_Docs ON ftCatalog WITH CHANGE_TRACKING AUTO;
         ALTER FULLTEXT INDEX ON Docs START FULL POPULATION;",
    )
    .await
    .unwrap();
    let ids = |r: &Vec<Vec<tiberius::Row>>| r[0].iter().map(|row| row.get::<i32, _>(0).unwrap()).collect::<Vec<_>>();

    for (condition, want) in [
        ("CONTAINS(Body, 'pie')", vec![3]),
        ("CONTAINS((Title, Body), '\"apple pie\"')", vec![3]),
        ("CONTAINS((Title, Body), 'pie AND NOT crust')", vec![4]),
        ("CONTAINS((Title, Body), '\"appl*\"')", vec![3, 4]),
        ("CONTAINS(Body, 'fox OR mill')", vec![2, 5]),
        ("CONTAINS(Body, 'NEAR((quick, fox), 2)')", vec![5]),
        ("CONTAINS(Body, 'NEAR((fox, quick), 2, TRUE)')", vec![]),
        ("CONTAINS((Title, Body), 'FORMSOF(INFLECTIONAL, run)')", vec![1, 2]),
        ("FREETEXT((Title, Body), 'running')", vec![1, 2]),
        ("CONTAINS(Body, 'run')", vec![1]),
    ] {
        let r = batch(&mut c, &format!("SELECT Id FROM Docs WHERE {condition} ORDER BY Id")).await.unwrap();
        assert_eq!(ids(&r), want, "{condition}");
    }

    // A parameter, as sp_executesql binds it.
    let r = c
        .query("SELECT Id FROM Docs WHERE CONTAINS(Body, @P1) ORDER BY Id", &[&"crust OR charts"])
        .await
        .unwrap()
        .into_results()
        .await
        .unwrap();
    assert_eq!(ids(&r), vec![3, 4]);

    // The table-valued forms join on KEY and order by RANK.
    let r = batch(
        &mut c,
        "SELECT d.Id, ct.RANK FROM Docs AS d INNER JOIN CONTAINSTABLE(Docs, *, 'pie') AS ct ON d.Id = ct.[KEY] ORDER BY ct.RANK DESC, d.Id",
    )
    .await
    .unwrap();
    let ranked: Vec<(i32, i32)> = r[0].iter().map(|row| (row.get(0).unwrap(), row.get(1).unwrap())).collect();
    assert_eq!(ranked.iter().map(|(id, _)| *id).collect::<Vec<_>>(), vec![3, 4], "{ranked:?}");
    assert!(ranked.iter().all(|(_, rank)| (1..=1000).contains(rank)), "{ranked:?}");
    let r = batch(&mut c, "SELECT [KEY] FROM FREETEXTTABLE(Docs, (Title, Body), 'runs', 1)").await.unwrap();
    assert_eq!(r[0].len(), 1);

    // Refused rather than read another way.
    let e = batch(&mut c, "SELECT Id FROM Docs WHERE CONTAINS(Body, 'NOT fox')").await.expect_err("NOT first");
    assert_eq!(number(&e), Some(7630), "{e}");

    batch(&mut c, "DROP FULLTEXT INDEX ON Docs; DROP FULLTEXT CATALOG ftCatalog").await.unwrap();
    // Without the index the predicates still answer, from a scan.
    let r = batch(&mut c, "SELECT Id FROM Docs WHERE CONTAINS(Body, 'pie') ORDER BY Id").await.unwrap();
    assert_eq!(ids(&r), vec![3]);
}

/// A column's case-insensitive collation is the engine's.
#[tokio::test(flavor = "multi_thread")]
async fn tiberius_column_collations() {
    let s = start();
    let mut c = connect(&s).await;
    batch(
        &mut c,
        "CREATE TABLE Accounts (Id int PRIMARY KEY, Login nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL UNIQUE, \
         City nvarchar(50) COLLATE Latin1_General_CI_AI, Code varchar(10) COLLATE Latin1_General_BIN2);
         INSERT INTO Accounts VALUES (1, N'Admin', N'Zürich', 'AB'), (2, N'guest', N'Bern', 'cd');",
    )
    .await
    .unwrap();
    let ids = |r: &Vec<Vec<tiberius::Row>>| r[0].iter().map(|row| row.get::<i32, _>(0).unwrap()).collect::<Vec<_>>();
    for (condition, want) in [
        ("Login = N'ADMIN'", vec![1]),
        ("City = N'zurich'", vec![1]),
        ("Login LIKE N'G%'", vec![2]),
        ("Code = 'ab'", vec![]),
    ] {
        let r = batch(&mut c, &format!("SELECT Id FROM Accounts WHERE {condition} ORDER BY Id")).await.unwrap();
        assert_eq!(ids(&r), want, "{condition}");
    }
    let r = c
        .query("SELECT Id FROM Accounts WHERE Login = @P1", &[&"GUEST"])
        .await
        .unwrap()
        .into_results()
        .await
        .unwrap();
    assert_eq!(ids(&r), vec![2]);
    let e = batch(&mut c, "INSERT INTO Accounts VALUES (3, N'admin', NULL, NULL)").await.expect_err("duplicate");
    assert_eq!(number(&e), Some(2627), "{e}");
}

fn int(rows: &[tiberius::Row]) -> Vec<i32> {
    rows.iter().map(|r| r.get::<i32, _>(0).unwrap()).collect()
}

fn texts(rows: &[tiberius::Row]) -> Vec<Option<String>> {
    rows.iter().map(|r| r.get::<&str, _>(0).map(str::to_string)).collect()
}

/// What EF Core 10 contexts named like DenevCloud's send: a mixed-case
/// database, `[dbo].[DenevCloud.ERP.Colors]` tables, filtered unique keys,
/// sp_rename of an index, the DropColumn script's catalogue join, the
/// migration lock, APPLY, and text that compares without case as under
/// SQL Server's default collation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tiberius_ef_core_shapes() {
    let s = start_with(true);
    let mut c = connect(&s).await;
    batch(&mut c, "CREATE DATABASE [DenevColors]").await.unwrap();
    drop(c);
    // The database is found without case, however the login spells it.
    let _ = connect_to(&s, "DENEVCOLORS").await;
    let mut c = connect_to(&s, "DenevColors").await;
    batch(
        &mut c,
        "CREATE TABLE [dbo].[DenevCloud.ERP.Colors] ([Id] int NOT NULL, [Name] nvarchar(50) NOT NULL, \
         [Code] nvarchar(20) NULL, [Deleted] bit NOT NULL DEFAULT 0, CONSTRAINT [PK_DenevCloud.ERP.Colors] PRIMARY KEY ([Id]))",
    )
    .await
    .unwrap();
    batch(
        &mut c,
        "INSERT INTO [dbo].[DenevCloud.ERP.Colors] ([Id], [Name], [Code]) VALUES (1, N'Red', N'R1'), (2, N'blue', NULL), (3, N'RED', NULL)",
    )
    .await
    .unwrap();
    // Every statement reads the dotted name as one table: UPDATE and
    // DELETE once answered 0 rows, a SELECT a relation named 'colors'.
    let r = batch(&mut c, "UPDATE [d] SET [d].[Code] = N'B1' FROM [dbo].[DenevCloud.ERP.Colors] AS [d] WHERE [d].[Id] = 2; SELECT @@ROWCOUNT")
        .await
        .unwrap();
    assert_eq!(int(r.last().unwrap()), vec![1]);
    let r = batch(&mut c, "SELECT [d].[Id] FROM [dbo].[DenevCloud.ERP.Colors] AS [d] WHERE [d].[Code] = N'b1'").await.unwrap();
    assert_eq!(int(&r[0]), vec![2], "text compares without case");
    let r = batch(&mut c, "SELECT COUNT(*) FROM [dbo].[DenevCloud.ERP.Colors] WHERE [Name] = N'red'").await.unwrap();
    assert_eq!(int(&r[0]), vec![2]);
    let r = batch(&mut c, "SELECT [Name] FROM [dbo].[DenevCloud.ERP.Colors] GROUP BY [Name] ORDER BY [Name]").await.unwrap();
    assert_eq!(texts(&r[0]), vec![Some("blue".into()), Some("RED".into())]);
    let r = batch(
        &mut c,
        "SELECT [Name] FROM [dbo].[DenevCloud.ERP.Colors] EXCEPT SELECT N'RED' ORDER BY [Name]",
    )
    .await
    .unwrap();
    assert_eq!(texts(&r[0]), vec![Some("blue".into())]);
    let r = batch(
        &mut c,
        "SELECT CASE WHEN OBJECT_ID(N'[dbo].[DenevCloud.ERP.Colors]') IS NULL THEN 0 ELSE 1 END; SELECT [name] FROM sys.tables",
    )
    .await
    .unwrap();
    assert_eq!(int(&r[0]), vec![1]);
    // Shown as declared (SqlBulkCopy matches names exactly), found without case.
    assert!(texts(&r[1]).contains(&Some("DenevCloud.ERP.Colors".into())), "{:?}", texts(&r[1]));
    // OUTER APPLY, as EF writes FirstOrDefault in a projection.
    let r = batch(
        &mut c,
        "SELECT [d].[Id], [x].[Name] FROM [dbo].[DenevCloud.ERP.Colors] AS [d] OUTER APPLY (SELECT TOP(1) [e].[Name] \
         FROM [dbo].[DenevCloud.ERP.Colors] AS [e] WHERE [e].[Id] > [d].[Id] ORDER BY [e].[Id]) AS [x] ORDER BY [d].[Id]",
    )
    .await
    .unwrap();
    assert_eq!(r[0].len(), 3);
    let names: Vec<Option<String>> = r[0].iter().map(|row| row.get::<&str, _>(1).map(str::to_string)).collect();
    assert_eq!(names, vec![Some("blue".into()), Some("RED".into()), None]);
    // A unique index filtered by `IS NOT NULL` is the key, under the
    // collation; NULLs repeat. Any other filter on a unique index is
    // refused (it was once built whole).
    batch(&mut c, "CREATE UNIQUE INDEX [IX_DenevCloud.ERP.Colors_Code] ON [dbo].[DenevCloud.ERP.Colors] ([Code]) WHERE [Code] IS NOT NULL")
        .await
        .unwrap();
    let e = batch(&mut c, "INSERT INTO [dbo].[DenevCloud.ERP.Colors] ([Id], [Name], [Code]) VALUES (4, N'x', N'r1')").await.unwrap_err();
    assert!(matches!(number(&e), Some(2601 | 2627)), "{e:?}");
    batch(&mut c, "INSERT INTO [dbo].[DenevCloud.ERP.Colors] ([Id], [Name], [Code]) VALUES (5, N'y', NULL)").await.unwrap();
    assert!(batch(&mut c, "CREATE UNIQUE INDEX [IX_D] ON [dbo].[DenevCloud.ERP.Colors] ([Name]) WHERE [Deleted] = 0").await.is_err());
    // sp_rename of an index, then DROP INDEX by the new name.
    batch(&mut c, "EXEC sp_rename N'[DenevCloud.ERP.Colors].[IX_DenevCloud.ERP.Colors_Code]', N'IX_Code', 'INDEX'").await.unwrap();
    batch(&mut c, "DROP INDEX [IX_Code] ON [dbo].[DenevCloud.ERP.Colors]").await.unwrap();
    batch(&mut c, "INSERT INTO [dbo].[DenevCloud.ERP.Colors] ([Id], [Name], [Code]) VALUES (6, N'z', N'r1')").await.unwrap();
    // EF Core's DropColumn script: the join has the columns it reads.
    batch(
        &mut c,
        "DECLARE @var sysname; SELECT @var = [d].[name] FROM [sys].[default_constraints] [d] \
         INNER JOIN [sys].[columns] [c] ON [d].[parent_column_id] = [c].[column_id] AND [d].[parent_object_id] = [c].[object_id] \
         WHERE ([d].[parent_object_id] = OBJECT_ID(N'[DenevCloud.ERP.Colors]') AND [c].[name] = N'Deleted'); \
         IF @var IS NOT NULL EXEC(N'ALTER TABLE [DenevCloud.ERP.Colors] DROP CONSTRAINT [' + @var + '];'); \
         ALTER TABLE [DenevCloud.ERP.Colors] DROP COLUMN [Deleted];",
    )
    .await
    .unwrap();
    // The migration lock: held by one connection, refused to another
    // within its timeout, free again once released.
    let lock = "DECLARE @r int; EXEC @r = sp_getapplock @Resource = '__EFMigrationsLock', @LockOwner = 'Session', @LockMode = 'Exclusive', @LockTimeout = 0; SELECT @r";
    let r = batch(&mut c, lock).await.unwrap();
    assert_eq!(int(r.last().unwrap()), vec![0]);
    let mut other = connect_to(&s, "denevcolors").await;
    let r = batch(&mut other, lock).await.unwrap();
    assert_eq!(int(r.last().unwrap()), vec![-1]);
    batch(&mut c, "DECLARE @r int; EXEC @r = sp_releaseapplock @Resource = '__EFMigrationsLock', @LockOwner = 'Session'; SELECT @r").await.unwrap();
    let r = batch(&mut other, lock).await.unwrap();
    assert_eq!(int(r.last().unwrap()), vec![0]);
    // tinyint is a TDS tinyint (EF reads a byte?), 0..=255.
    batch(&mut c, "CREATE TABLE [dbo].[DenevCloud.ERP.Companies] ([Id] int PRIMARY KEY, [N] tinyint NULL)").await.unwrap();
    batch(&mut c, "INSERT INTO [dbo].[DenevCloud.ERP.Companies] VALUES (1, 255), (2, 0), (3, NULL)").await.unwrap();
    let r = batch(&mut c, "SELECT [N] FROM [dbo].[DenevCloud.ERP.Companies] ORDER BY [Id]").await.unwrap();
    let bytes: Vec<Option<u8>> = r[0].iter().map(|row| row.get::<u8, _>(0)).collect();
    assert_eq!(bytes, vec![Some(255), Some(0), None]);
    assert!(batch(&mut c, "INSERT INTO [dbo].[DenevCloud.ERP.Companies] VALUES (4, 256)").await.is_err());
    // A bound SELECT the engine describes as rowless (a derived table
    // beside a ROW_NUMBER one) still sends its column metadata: the rows
    // reached the client without it and the connection broke.
    let q = "SELECT [d6].[Id], [d6].[c], [d8].[Name] FROM (SELECT TOP(@P1) [d].[Id], (SELECT TOP(1) [x].[Code] \
             FROM [dbo].[DenevCloud.ERP.Colors] AS [x] WHERE [x].[Id] = [d].[Id]) AS [c] FROM [dbo].[DenevCloud.ERP.Colors] AS [d]) AS [d6] \
             LEFT JOIN (SELECT [d7].[Name], [d7].[Id] FROM (SELECT [e].[Name], [e].[Id], ROW_NUMBER() OVER(PARTITION BY [e].[Id] ORDER BY [e].[Name]) AS [row] \
             FROM [dbo].[DenevCloud.ERP.Colors] AS [e]) AS [d7] WHERE [d7].[row] <= 1) AS [d8] ON [d6].[Id] = [d8].[Id] ORDER BY [d6].[Id]";
    let rows = c.query(q, &[&10i32]).await.unwrap().into_first_result().await.unwrap();
    assert_eq!(rows.len(), 5, "every color");
    assert_eq!(rows[0].get::<i32, _>(0), Some(1));
    assert_eq!(rows[0].get::<&str, _>(2), Some("Red"));
    // Search terms a user types, bound: what is stored is found, and a value
    // that looks like SQL is only a value.
    batch(&mut c, "INSERT INTO [dbo].[DenevCloud.ERP.Colors] ([Id], [Name], [Code]) VALUES (20, N'x''); DROP TABLE t; --', N'Е''\\')").await.unwrap();
    for (term, id) in [("x'); DROP TABLE t; --", 20), ("Е'\\", 20)] {
        let rows = c
            .query("SELECT [d].[Id] FROM [dbo].[DenevCloud.ERP.Colors] AS [d] WHERE [d].[Name] = @P1 OR [d].[Code] = @P1", &[&term])
            .await
            .unwrap()
            .into_first_result()
            .await
            .unwrap();
        assert_eq!(rows.iter().map(|r| r.get::<i32, _>(0).unwrap()).collect::<Vec<_>>(), vec![id], "{term}");
    }
    let r = batch(&mut c, "SELECT COUNT(*) FROM [dbo].[DenevCloud.ERP.Colors]").await.unwrap();
    assert_eq!(int(&r[0]), vec![6], "the table is all there");
    // Rows written with their own identity values (IDENTITY_INSERT, EF
    // seeding, a copy's KeepIdentity) move the counter past them: the next
    // generated id was 1, a duplicate key once it reached a copied row.
    batch(&mut c, "CREATE TABLE [dbo].[DenevCloud.ERP.Seeded] ([Id] int IDENTITY(1,1) NOT NULL PRIMARY KEY, [Name] nvarchar(20))").await.unwrap();
    batch(
        &mut c,
        "SET IDENTITY_INSERT [dbo].[DenevCloud.ERP.Seeded] ON; INSERT INTO [dbo].[DenevCloud.ERP.Seeded] ([Id], [Name]) VALUES (5, N'a'), (100, N'b'), (7, N'c'); SET IDENTITY_INSERT [dbo].[DenevCloud.ERP.Seeded] OFF",
    )
    .await
    .unwrap();
    let r = batch(&mut c, "INSERT INTO [dbo].[DenevCloud.ERP.Seeded] ([Name]) VALUES (N'd'); SELECT CAST(SCOPE_IDENTITY() AS int)").await.unwrap();
    assert!(int(r.last().unwrap())[0] > 100, "{:?}", int(r.last().unwrap()));
    // SqlBulkCopy reads the identity column from FMTONLY's metadata
    // (fIdentity) to leave it out unless KeepIdentity.
    let r = batch(&mut c, "SET FMTONLY ON; SELECT * FROM [dbo].[DenevCloud.ERP.Seeded]; SET FMTONLY OFF").await.unwrap();
    assert_eq!(r.iter().map(|x| x.len()).sum::<usize>(), 0, "FMTONLY answers no rows");
    // EnsureDeleted's statement before DROP DATABASE.
    drop(other);
    drop(c);
    let mut c = connect(&s).await;
    batch(
        &mut c,
        "IF SERVERPROPERTY('EngineEdition') <> 5 BEGIN ALTER DATABASE [DenevColors] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; END; DROP DATABASE [DenevColors];",
    )
    .await
    .unwrap();
}

/// Names keep the case they were declared in, as SQL Server shows them:
/// SqlBulkCopy maps columns by case-sensitive name and copied nothing
/// when `SELECT *` and sp_tablecollations answered `id`, `companyid`. An
/// unaliased expression has no name (SqlBulkCopy and Dapper read `''`, not
/// `count_big`), an alias stays as typed beside a column spelt otherwise,
/// and the catalogue finds a name however a lookup spells it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tiberius_declared_names() {
    async fn check(c: &mut Client<Compat<tokio::net::TcpStream>>, sql: &str, want: &[&str]) {
        let mut q = c.simple_query(sql).await.unwrap();
        let got: Vec<String> = q.columns().await.unwrap().unwrap_or_default().iter().map(|c| c.name().to_string()).collect();
        q.into_results().await.unwrap();
        assert_eq!(got, want, "{sql}");
    }
    let s = start_with(true);
    let mut c = connect(&s).await;
    batch(
        &mut c,
        "CREATE TABLE [dbo].[DenevCloud.ERP.Categories] ([Id] int IDENTITY(1,1) NOT NULL PRIMARY KEY, \
         [CompanyId] uniqueidentifier NOT NULL, [Code] nvarchar(20) NULL, [N] int NULL); \
         ALTER TABLE [dbo].[DenevCloud.ERP.Categories] ADD [SortOrder] int NULL; \
         INSERT INTO [dbo].[DenevCloud.ERP.Categories] ([CompanyId], [Code], [N]) VALUES (NEWID(), N'AB-1', 1)",
    )
    .await
    .unwrap();
    check(&mut c, "SELECT * FROM [dbo].[DenevCloud.ERP.Categories]", &["Id", "CompanyId", "Code", "N", "SortOrder"]).await;
    check(&mut c, "SELECT [c].[code], [c].[Code] AS [code2] FROM [dbo].[DenevCloud.ERP.Categories] AS [c]", &["Code", "code2"]).await;
    check(&mut c, "SELECT COUNT_BIG(*), [n] AS [n] FROM [dbo].[DenevCloud.ERP.Categories] GROUP BY [n]", &["", "n"]).await;

    let r = batch(&mut c, "EXEC sp_tablecollations_100 N'[dbo].[DenevCloud.ERP.Categories]'").await.unwrap();
    let got: Vec<(String, Option<String>)> = r[0]
        .iter()
        .map(|r| (r.get::<&str, _>(1).unwrap().to_string(), r.get::<&str, _>(3).map(str::to_string)))
        .collect();
    assert_eq!(got.iter().map(|g| g.0.as_str()).collect::<Vec<_>>(), ["Id", "CompanyId", "Code", "N", "SortOrder"]);
    assert_eq!(got[2].1.as_deref(), Some("SQL_Latin1_General_CP1_CI_AS"));

    // The catalogue shows declared names and finds them without case.
    let r = batch(&mut c, "SELECT name FROM sys.tables WHERE name = N'denevcloud.erp.categories'").await.unwrap();
    assert_eq!(texts(&r[0]), [Some("DenevCloud.ERP.Categories".to_string())]);
    let r = batch(&mut c, "SELECT t.name FROM sys.tables AS t WHERE t.name LIKE N'DENEVCLOUD.ERP.CAT%'").await.unwrap();
    assert_eq!(texts(&r[0]), [Some("DenevCloud.ERP.Categories".to_string())]);
    let r = c
        .query(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = @P1 ORDER BY ORDINAL_POSITION",
            &[&"denevcloud.erp.CATEGORIES"],
        )
        .await
        .unwrap()
        .into_first_result()
        .await
        .unwrap();
    assert_eq!(texts(&r).into_iter().flatten().collect::<Vec<_>>(), ["Id", "CompanyId", "Code", "N", "SortOrder"]);
    let r = batch(&mut c, "SELECT name FROM sys.columns WHERE object_id = OBJECT_ID(N'[dbo].[DenevCloud.ERP.Categories]') AND name <> N'id'")
        .await
        .unwrap();
    assert_eq!(texts(&r[0]).into_iter().flatten().collect::<Vec<_>>(), ["CompanyId", "Code", "N", "SortOrder"]);

    // Recorded, not remembered by the session: a new connection shows them.
    drop(c);
    let mut c = connect(&s).await;
    check(&mut c, "SELECT * FROM [dbo].[DenevCloud.ERP.Categories]", &["Id", "CompanyId", "Code", "N", "SortOrder"]).await;
    // A table dropped and created again shows its new spelling only.
    batch(&mut c, "DROP TABLE [dbo].[DenevCloud.ERP.Categories]; CREATE TABLE [DENEVCLOUD.ERP.CATEGORIES] ([ID] int PRIMARY KEY)").await.unwrap();
    let r = batch(&mut c, "SELECT name FROM sys.tables WHERE name LIKE N'denevcloud%'; SELECT name FROM sys.columns WHERE object_id = OBJECT_ID(N'[denevcloud.erp.categories]')")
        .await
        .unwrap();
    assert_eq!(texts(&r[0]), [Some("DENEVCLOUD.ERP.CATEGORIES".to_string())]);
    assert_eq!(texts(&r[1]), [Some("ID".to_string())]);
}
