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
    start_config(case_insensitive, 2601)
}

/// [`start_with`], a duplicate in a unique index reported as
/// `unique_index_error` (`--mssql-unique-index-error`).
fn start_config(case_insensitive: bool, unique_index_error: i32) -> Servers {
    start_at(case_insensitive, unique_index_error, None)
}

/// [`start_config`], the databases kept in `root` (on disk) when given: a
/// partitioned table is not made in memory.
fn start_at(case_insensitive: bool, unique_index_error: i32, root: Option<std::path::PathBuf>) -> Servers {
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
                let catalog: Arc<DatabaseCatalog> =
                    DatabaseCatalog::open(root.as_deref().and_then(|p| p.to_str()), "postgres").await.unwrap();
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
                    unique_index_error,
                    guid_order: case_insensitive,
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

    // A table named `__…` is the client's when it made it (EF Core's
    // history table): listed, where every `__` name was taken for the
    // engine's own and hidden. The engine's own stay hidden.
    batch(
        &mut c,
        "IF OBJECT_ID(N'[__EFMigrationsHistory]') IS NULL BEGIN CREATE TABLE [__EFMigrationsHistory] \
         ([MigrationId] nvarchar(150) NOT NULL, [ProductVersion] nvarchar(32) NOT NULL, \
         CONSTRAINT [PK___EFMigrationsHistory] PRIMARY KEY ([MigrationId])); END;",
    )
    .await
    .unwrap();
    let r = batch(
        &mut c,
        "SELECT name FROM sys.tables WHERE name = N'__EFMigrationsHistory'; \
         SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'__EFMigrationsHistory' ORDER BY ORDINAL_POSITION; \
         SELECT COUNT(*) FROM sys.objects WHERE name LIKE N'[_][_]rdb%'",
    )
    .await
    .unwrap();
    assert_eq!(texts(&r[0]), [Some("__EFMigrationsHistory".to_string())]);
    assert_eq!(texts(&r[1]).into_iter().flatten().collect::<Vec<_>>(), ["MigrationId", "ProductVersion"]);
    assert_eq!(int(&r[2]), [0], "the engine's own relations are listed");

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

/// Constraint violations come back as SQL Server's: error 547 for a
/// foreign key, 2627 for a primary or unique key, 2601 for a unique index,
/// worded as its messages and naming everything as declared (the engine's
/// read `"denevcloud․erp․kids"`). And sp_rename keeps the declared names.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tiberius_constraint_errors() {
    let s = start_with(true);
    let mut c = connect(&s).await;
    batch(
        &mut c,
        "CREATE TABLE [dbo].[DenevCloud.ERP.Companies] ([Id] int NOT NULL, [Code] nvarchar(20) NOT NULL, [Tax] nvarchar(20) NULL, \
         CONSTRAINT [PK_DenevCloud.ERP.Companies] PRIMARY KEY ([Id]), CONSTRAINT [AK_Companies_Tax] UNIQUE ([Tax])); \
         CREATE UNIQUE INDEX [IX_DenevCloud.ERP.Companies_Code] ON [dbo].[DenevCloud.ERP.Companies] ([Code]); \
         CREATE TABLE [dbo].[DenevCloud.ERP.ClientSpecifics] ([Id] int NOT NULL PRIMARY KEY, [CompanyId] int NOT NULL, \
         CONSTRAINT [FK_DenevCloud.ERP.ClientSpecifics_DenevCloud.ERP.Companies_CompanyId] FOREIGN KEY ([CompanyId]) \
         REFERENCES [dbo].[DenevCloud.ERP.Companies] ([Id])); \
         INSERT INTO [dbo].[DenevCloud.ERP.Companies] ([Id], [Code], [Tax]) VALUES (1, N'C1', N'T1'); \
         INSERT INTO [dbo].[DenevCloud.ERP.ClientSpecifics] ([Id], [CompanyId]) VALUES (10, 1)",
    )
    .await
    .unwrap();
    let fails = |e: Result<Vec<Vec<tiberius::Row>>, tiberius::error::Error>| -> (u32, String) {
        let e = e.err().expect("refused");
        let tiberius::error::Error::Server(t) = &e else { panic!("{e}") };
        (t.code(), t.message().to_string())
    };
    let (n, m) = fails(batch(&mut c, "INSERT INTO [dbo].[DenevCloud.ERP.ClientSpecifics] ([Id], [CompanyId]) VALUES (11, 99)").await);
    assert_eq!(n, 547, "{m}");
    assert!(
        m.starts_with(
            "The INSERT statement conflicted with the FOREIGN KEY constraint \"FK_DenevCloud.ERP.ClientSpecifics_DenevCloud.ERP.Companies_CompanyId\". \
             The conflict occurred in database \"postgres\", table \"dbo.DenevCloud.ERP.Companies\", column 'Id'."
        ),
        "{m}"
    );
    assert!(m.contains("\"FK_DenevCloud.ERP.ClientSpecifics_DenevCloud.ERP.Companies_CompanyId\""), "{m}");
    assert!(!m.contains('\u{2024}'), "{m}");
    let (n, m) = fails(batch(&mut c, "DELETE FROM [dbo].[DenevCloud.ERP.Companies] WHERE [Id] = 1").await);
    assert_eq!(n, 547, "{m}");
    assert!(
        m.starts_with("The DELETE statement conflicted with the REFERENCE constraint \"FK_DenevCloud.ERP.ClientSpecifics_DenevCloud.ERP.Companies_CompanyId\".")
            && m.contains("table \"dbo.DenevCloud.ERP.ClientSpecifics\", column 'CompanyId'."),
        "{m}"
    );
    let (n, m) = fails(batch(&mut c, "INSERT INTO [dbo].[DenevCloud.ERP.Companies] ([Id], [Code]) VALUES (1, N'C9')").await);
    assert_eq!(n, 2627, "{m}");
    assert!(
        m.starts_with("Violation of PRIMARY KEY constraint 'PK_DenevCloud.ERP.Companies'. Cannot insert duplicate key in object 'dbo.DenevCloud.ERP.Companies'."),
        "{m}"
    );
    let (n, m) = fails(batch(&mut c, "INSERT INTO [dbo].[DenevCloud.ERP.Companies] ([Id], [Code], [Tax]) VALUES (2, N'C2', N'T1')").await);
    assert_eq!(n, 2627, "{m}");
    assert!(m.starts_with("Violation of UNIQUE KEY constraint 'AK_Companies_Tax'."), "{m}");
    let (n, m) = fails(batch(&mut c, "INSERT INTO [dbo].[DenevCloud.ERP.Companies] ([Id], [Code]) VALUES (3, N'c1')").await);
    assert_eq!(n, 2601, "{m}");
    assert!(
        m.starts_with("Cannot insert duplicate key row in object 'dbo.DenevCloud.ERP.Companies' with unique index 'IX_DenevCloud.ERP.Companies_Code'."),
        "{m}"
    );

    // sp_rename: the new spellings show, a column's and an index's.
    batch(&mut c, "EXEC sp_rename N'[dbo].[DenevCloud.ERP.Companies].[Tax]', N'TaxNumber', 'COLUMN'").await.unwrap();
    batch(&mut c, "EXEC sp_rename N'[DenevCloud.ERP.Companies].[IX_DenevCloud.ERP.Companies_Code]', N'IX_Code', 'INDEX'").await.unwrap();
    let mut q = c.simple_query("SELECT * FROM [dbo].[DenevCloud.ERP.Companies]").await.unwrap();
    let got: Vec<String> = q.columns().await.unwrap().unwrap_or_default().iter().map(|c| c.name().to_string()).collect();
    q.into_results().await.unwrap();
    assert_eq!(got, ["Id", "Code", "TaxNumber"]);
    let (n, m) = fails(batch(&mut c, "INSERT INTO [dbo].[DenevCloud.ERP.Companies] ([Id], [Code]) VALUES (3, N'C1')").await);
    assert_eq!(n, 2601, "{m}");
    assert!(m.contains("with unique index 'IX_Code'"), "{m}");
    // A table renamed keeps its columns' spellings.
    batch(&mut c, "EXEC sp_rename N'[dbo].[DenevCloud.ERP.ClientSpecifics]', N'DenevCloud.ERP.Clients'").await.unwrap();
    let r = batch(&mut c, "SELECT name FROM sys.tables WHERE name LIKE N'%clients'; SELECT * FROM [DenevCloud.ERP.Clients]").await.unwrap();
    assert_eq!(texts(&r[0]), [Some("DenevCloud.ERP.Clients".to_string())]);
    let cols: Vec<&str> = r[1][0].columns().iter().map(|c| c.name()).collect();
    assert_eq!(cols, ["Id", "CompanyId"]);
}

/// T-SQL's TRY_CAST / TRY_CONVERT are NULL where the conversion fails, as
/// SQL Server's: they were plain casts, and the statement failed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tiberius_try_cast() {
    let s = start_with(true);
    let mut c = connect(&s).await;
    batch(&mut c, "CREATE TABLE [Raw] ([Id] int PRIMARY KEY, [V] nvarchar(40) NULL); \
                   INSERT INTO [Raw] VALUES (1, N'12'), (2, N'x1'), (3, NULL), (4, N'2024-02-29'), (5, N'99999999999')")
        .await
        .unwrap();
    let r = batch(&mut c, "SELECT TRY_CAST([V] AS int) FROM [Raw] ORDER BY [Id]").await.unwrap();
    let got: Vec<Option<i32>> = r[0].iter().map(|r| r.get::<i32, _>(0)).collect();
    assert_eq!(got, [Some(12), None, None, None, None]);
    let r = batch(&mut c, "SELECT [Id] FROM [Raw] WHERE TRY_CONVERT(int, [V]) IS NOT NULL").await.unwrap();
    assert_eq!(int(&r[0]), [1]);
    let r = batch(&mut c, "SELECT [Id] FROM [Raw] WHERE TRY_CAST([V] AS date) IS NOT NULL").await.unwrap();
    assert_eq!(int(&r[0]), [4]);
    let r = batch(&mut c, "SELECT TRY_CAST(N'abc' AS bigint), TRY_CAST(N'7' AS bigint), TRY_CONVERT(uniqueidentifier, N'nope')").await.unwrap();
    assert_eq!(r[0][0].get::<i64, _>(0), None);
    assert_eq!(r[0][0].get::<i64, _>(1), Some(7));
    assert_eq!(r[0][0].get::<uuid::Uuid, _>(2), None);
}

/// A database shows the name CREATE DATABASE gave it (`DB_NAME()`,
/// sys.databases, an error's "in database"), where the engine folds it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tiberius_database_name_case() {
    let s = start_with(true);
    let mut c = connect(&s).await;
    // `IF DB_ID(N'x') IS NULL CREATE DATABASE [x]`: DB_ID was 5 for any
    // name, and the CREATE was skipped.
    batch(&mut c, "IF DB_ID(N'DenevCloudERP') IS NULL CREATE DATABASE [DenevCloudERP]").await.unwrap();
    let r = batch(&mut c, "SELECT name FROM sys.databases WHERE name = N'denevclouderp'").await.unwrap();
    assert_eq!(texts(&r[0]), [Some("DenevCloudERP".to_string())]);
    batch(&mut c, "IF DB_ID(N'DenevCloudERP') IS NULL CREATE DATABASE [DenevCloudERP]").await.unwrap();
    batch(&mut c, "CREATE DATABASE [Other]").await.unwrap();
    let ids = |r: &[tiberius::Row]| -> Vec<Option<i32>> { r.iter().map(|r| r.get::<i32, _>(0)).collect() };
    let r = batch(
        &mut c,
        "SELECT DB_ID(N'DoesNotExist42'); SELECT DB_ID(N'master'); SELECT DB_ID(); SELECT DB_ID(N'denevCLOUDerp'); \
         SELECT DB_ID(N'Other'); SELECT database_id FROM sys.databases WHERE name = N'DenevCloudERP'",
    )
    .await
    .unwrap();
    assert_eq!(ids(&r[0]), [None]);
    assert_eq!(ids(&r[1]), [Some(1)]);
    assert_eq!(ids(&r[2]), [Some(1)], "connected to the default database, which is master");
    let erp = ids(&r[3])[0].expect("an id");
    assert!(erp > 4);
    assert_ne!(ids(&r[4])[0], Some(erp));
    assert_eq!(ids(&r[5]), [Some(erp)]);
    // A parameter, and DB_NAME of the id.
    let r = c.query("SELECT DB_ID(@P1), DB_NAME(@P2), DB_NAME(1), DB_NAME(424242)", &[&"DenevCloudERP", &erp]).await.unwrap().into_first_result().await.unwrap();
    assert_eq!(r[0].get::<i32, _>(0), Some(erp));
    assert_eq!(r[0].get::<&str, _>(1), Some("DenevCloudERP"));
    assert_eq!(r[0].get::<&str, _>(2), Some("master"));
    assert_eq!(r[0].get::<&str, _>(3), None);
    // The same id from inside the database, after dropping another.
    batch(&mut c, "DROP DATABASE [Other]").await.unwrap();
    // From inside it, after USE and on a login naming it.
    let r = batch(&mut c, "USE [DENEVCLOUDERP]; SELECT DB_NAME()").await.unwrap();
    assert_eq!(texts(r.last().unwrap()), [Some("DenevCloudERP".to_string())]);
    let r = batch(&mut c, "SELECT DB_ID()").await.unwrap();
    assert_eq!(ids(&r[0]), [Some(erp)]);
    drop(c);
    let mut c = connect_to(&s, "denevclouderp").await;
    let r = batch(&mut c, "SELECT DB_NAME()").await.unwrap();
    assert_eq!(texts(&r[0]), [Some("DenevCloudERP".to_string())]);
    batch(&mut c, "CREATE TABLE [P] ([Id] int PRIMARY KEY); CREATE TABLE [K] ([Id] int PRIMARY KEY, [PId] int NOT NULL, \
                   CONSTRAINT [FK_K_P] FOREIGN KEY ([PId]) REFERENCES [P] ([Id]))")
        .await
        .unwrap();
    let e = batch(&mut c, "INSERT INTO [K] VALUES (1, 9)").await.err().expect("refused");
    assert!(e.to_string().contains("in database \"DenevCloudERP\""), "{e}");
    drop(c);
    // Dropped, it is gone from the list.
    let mut c = connect(&s).await;
    batch(&mut c, "DROP DATABASE [DenevCloudERP]").await.unwrap();
    let r = batch(&mut c, "SELECT name FROM sys.databases WHERE name = N'denevclouderp'").await.unwrap();
    assert!(r[0].is_empty());
}

/// `--mssql-unique-index-error 2627`: a unique index's duplicate reported
/// as a constraint's, for applications that test that number only.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tiberius_unique_index_error_setting() {
    let s = start_config(true, 2627);
    let mut c = connect(&s).await;
    batch(&mut c, "CREATE TABLE [U] ([Id] int PRIMARY KEY, [Code] nvarchar(9) NOT NULL); CREATE UNIQUE INDEX [IX_U_Code] ON [U] ([Code]); \
                   INSERT INTO [U] VALUES (1, N'a')")
        .await
        .unwrap();
    let e = batch(&mut c, "INSERT INTO [U] VALUES (2, N'A')").await.err().expect("refused");
    let tiberius::error::Error::Server(t) = &e else { panic!("{e}") };
    assert_eq!(t.code(), 2627, "{e}");
    assert!(t.message().contains("with unique index 'IX_U_Code'"), "{e}");
}

/// A database made before names were recorded: sp_rdb_record_names reads
/// the script that made it (EF Core's idempotent one: IF … BEGIN … END,
/// EXEC(N'…'), sp_rename, GO) and records the names without running it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tiberius_record_names_from_a_script() {
    let s = start_with(true);
    let mut c = connect(&s).await;
    let script = "IF NOT EXISTS (SELECT * FROM [__EFMigrationsHistory] WHERE [MigrationId] = N'1')
BEGIN
    CREATE TABLE [DenevCloud.ERP.Units] ([Id] int NOT NULL IDENTITY, [ShortName] nvarchar(10) NOT NULL, [Old] int NULL,
        CONSTRAINT [PK_DenevCloud.ERP.Units] PRIMARY KEY ([Id]));
END;
GO
IF NOT EXISTS (SELECT * FROM [__EFMigrationsHistory] WHERE [MigrationId] = N'2')
BEGIN
    EXEC sp_rename N'[DenevCloud.ERP.Units].[Old]', N'Factor', 'COLUMN';
    ALTER TABLE [DenevCloud.ERP.Units] ADD [Code] nvarchar(5) NULL;
    EXEC(N'CREATE UNIQUE INDEX [IX_DenevCloud.ERP.Units_Code] ON [DenevCloud.ERP.Units] ([Code]) WHERE [Code] IS NOT NULL');
END;
GO";
    // The database as an older build left it: made, its names not kept.
    // Made over PostgreSQL, which records none (the names catalogue is the
    // front end's own: a client cannot empty it).
    let (pg, conn) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=rasterized password={PASSWORD} dbname=postgres", s.pg_port),
        tokio_postgres::NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(conn);
    pg.batch_execute(
        "CREATE TABLE \"denevcloud\u{2024}erp\u{2024}units\" (id serial NOT NULL, shortname varchar(10) COLLATE rdb_ci NOT NULL, factor integer NULL, \
         code varchar(5) COLLATE rdb_ci NULL, CONSTRAINT \"pk_denevcloud\u{2024}erp\u{2024}units\" PRIMARY KEY (id)); \
         CREATE UNIQUE INDEX \"denevcloud\u{2024}erp\u{2024}units__ix_denevcloud\u{2024}erp\u{2024}units_code\" ON \"denevcloud\u{2024}erp\u{2024}units\" (code); \
         INSERT INTO \"denevcloud\u{2024}erp\u{2024}units\" (shortname, code) VALUES ('kg', 'K')",
    )
    .await
    .unwrap();
    // A client reads and writes no catalogue of the front end's.
    assert!(batch(&mut c, "DELETE FROM __rdb_tds_names").await.is_err());
    async fn names(c: &mut Client<Compat<tokio::net::TcpStream>>) -> Vec<String> {
        let mut q = c.simple_query("SELECT * FROM [DenevCloud.ERP.Units]").await.unwrap();
        let got = q.columns().await.unwrap().unwrap_or_default().iter().map(|c| c.name().to_string()).collect();
        q.into_results().await.unwrap();
        got
    }
    assert_eq!(names(&mut c).await, ["id", "shortname", "factor", "code"]);
    // Nothing in the script runs: no table, no history row.
    let q = format!("EXEC sp_rdb_record_names @script = N'{}'", script.replace('\'', "''"));
    batch(&mut c, &q).await.unwrap();
    assert_eq!(names(&mut c).await, ["Id", "ShortName", "Factor", "Code"]);
    let r = batch(&mut c, "SELECT name FROM sys.tables WHERE name LIKE N'denevcloud%'; SELECT COUNT(*) FROM [DenevCloud.ERP.Units]").await.unwrap();
    assert_eq!(texts(&r[0]), [Some("DenevCloud.ERP.Units".to_string())]);
    assert_eq!(int(&r[1]), [1]);
    let e = batch(&mut c, "INSERT INTO [DenevCloud.ERP.Units] ([ShortName], [Code]) VALUES (N'g', N'k')").await.err().expect("refused");
    assert!(e.to_string().contains("with unique index 'IX_DenevCloud.ERP.Units_Code'"), "{e}");
}

/// INFORMATION_SCHEMA.COLUMNS has SQL Server's columns: tools build a
/// column's type as `DATA_TYPE + ISNULL('(' + CAST(DATETIME_PRECISION AS
/// varchar) + ')', '')`, which failed for want of DATETIME_PRECISION. And
/// a CAST to a character type without a length keeps 30 characters.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tiberius_information_schema_columns() {
    let s = start_with(true);
    let mut c = connect(&s).await;
    batch(
        &mut c,
        "CREATE TABLE [DenevCloud.ERP.Orders] ([Id] int NOT NULL PRIMARY KEY, [Total] decimal(18,2) NOT NULL, \
         [Created] datetime2 NOT NULL, [Note] nvarchar(40) NULL, [Body] nvarchar(max) NULL, [Code] varchar(10) COLLATE Latin1_General_BIN2 NULL)",
    )
    .await
    .unwrap();
    let r = batch(
        &mut c,
        "SELECT DATA_TYPE + ISNULL('(' + CAST(DATETIME_PRECISION AS varchar) + ')','') FROM INFORMATION_SCHEMA.COLUMNS \
         WHERE TABLE_NAME='DenevCloud.ERP.Orders' AND COLUMN_NAME='Created'",
    )
    .await
    .unwrap();
    assert_eq!(texts(&r[0]), [Some("datetime2(6)".to_string())]);
    let r = batch(
        &mut c,
        "SELECT COLUMN_NAME, NUMERIC_PRECISION, NUMERIC_SCALE, CHARACTER_MAXIMUM_LENGTH, CHARACTER_OCTET_LENGTH, COLLATION_NAME \
         FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'DenevCloud.ERP.Orders' ORDER BY ORDINAL_POSITION",
    )
    .await
    .unwrap();
    let got: Vec<(String, Option<i32>, Option<i32>, Option<i32>, Option<i32>, Option<String>)> = r[0]
        .iter()
        .map(|r| {
            (
                r.get::<&str, _>(0).unwrap().to_string(),
                r.get::<i32, _>(1),
                r.get::<i32, _>(2),
                r.get::<i32, _>(3),
                r.get::<i32, _>(4),
                r.get::<&str, _>(5).map(str::to_string),
            )
        })
        .collect();
    let ci = Some("SQL_Latin1_General_CP1_CI_AS".to_string());
    assert_eq!(
        got,
        [
            ("Id".into(), Some(10), Some(0), None, None, None),
            ("Total".into(), Some(18), Some(2), None, None, None),
            ("Created".into(), None, None, None, None, None),
            ("Note".into(), None, None, Some(40), Some(80), ci.clone()),
            ("Body".into(), None, None, Some(-1), Some(-1), ci),
            ("Code".into(), None, None, Some(10), Some(20), Some("Latin1_General_BIN2".to_string())),
        ]
    );
    let long = "x".repeat(40);
    let r = batch(&mut c, &format!("SELECT CAST(N'{long}' AS nvarchar), CONVERT(varchar, N'{long}'), CAST(N'{long}' AS nvarchar(max))")).await.unwrap();
    assert_eq!(r[0][0].get::<&str, _>(0).map(str::len), Some(30));
    assert_eq!(r[0][0].get::<&str, _>(1).map(str::len), Some(30));
    assert_eq!(r[0][0].get::<&str, _>(2).map(str::len), Some(40));
}

/// A `uniqueidentifier` orders as SQL Server's: its last six bytes first,
/// then bytes 8-9, 7-6, 5-4, 3-0 (`--mssql-guid-order sqlserver`). It
/// ordered by its bytes, and ORDER BY, keyset paging, min/max came out in
/// another order than SQL Server's.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tiberius_guid_order() {
    let s = start_with(true);
    let mut c = connect(&s).await;
    // In SQL Server's order: b < c < a < d (byte 10 outranks byte 15, which
    // outranks byte 9, which outranks byte 0).
    let (a, b, cc, d) = (
        "00000000-0000-0000-0000-000000000001",
        "01000000-0000-0000-0000-000000000000",
        "00000000-0000-0000-0001-000000000000",
        "00000000-0000-0000-0000-010000000000",
    );
    batch(
        &mut c,
        &format!(
            "CREATE TABLE [G] ([Id] uniqueidentifier NOT NULL PRIMARY KEY, [ParentId] uniqueidentifier NULL, [N] int NOT NULL); \
             CREATE INDEX [IX_G_ParentId] ON [G] ([ParentId]); \
             INSERT INTO [G] VALUES ('{a}', NULL, 1), ('{b}', '{a}', 2), ('{cc}', '{a}', 3), ('{d}', NULL, 4)"
        ),
    )
    .await
    .unwrap();
    let order = |r: &[tiberius::Row]| int(r);
    let r = batch(&mut c, "SELECT [N] FROM [G] ORDER BY [Id]").await.unwrap();
    assert_eq!(order(&r[0]), [2, 3, 1, 4]);
    let r = batch(&mut c, "SELECT [N] FROM [G] ORDER BY [Id] DESC").await.unwrap();
    assert_eq!(order(&r[0]), [4, 1, 3, 2]);
    // Keyset paging: the page after b.
    let r = c
        .query("SELECT TOP 2 [N] FROM [G] WHERE [Id] > @P1 ORDER BY [Id]", &[&uuid::Uuid::parse_str(b).unwrap()])
        .await
        .unwrap()
        .into_first_result()
        .await
        .unwrap();
    assert_eq!(order(&r), [3, 1]);
    let r = batch(&mut c, &format!("SELECT [N] FROM [G] WHERE [Id] BETWEEN '{cc}' AND '{d}' ORDER BY [Id]")).await.unwrap();
    assert_eq!(order(&r[0]), [3, 1, 4]);
    let r = batch(&mut c, "SELECT CAST(MIN([Id]) AS nvarchar(36)), CAST(MAX([Id]) AS nvarchar(36)) FROM [G]").await.unwrap();
    assert_eq!(r[0][0].get::<&str, _>(0), Some(b));
    assert_eq!(r[0][0].get::<&str, _>(1), Some(d));
    // Equality, joins, foreign-key shapes and SELECT * are the value's.
    let r = batch(&mut c, &format!("SELECT [N] FROM [G] WHERE [Id] = '{cc}'; SELECT [N] FROM [G] WHERE [ParentId] = '{a}' ORDER BY [Id]")).await.unwrap();
    assert_eq!(order(&r[0]), [3]);
    assert_eq!(order(&r[1]), [2, 3]);
    let r = batch(&mut c, "SELECT k.[N] FROM [G] k JOIN [G] p ON k.[ParentId] = p.[Id] ORDER BY k.[Id] DESC").await.unwrap();
    assert_eq!(order(&r[0]), [3, 2]);
    let mut q = c.simple_query("SELECT * FROM [G]").await.unwrap();
    let cols: Vec<String> = q.columns().await.unwrap().unwrap_or_default().iter().map(|c| c.name().to_string()).collect();
    q.into_results().await.unwrap();
    assert_eq!(cols, ["Id", "ParentId", "N"]);
    let e = batch(&mut c, &format!("INSERT INTO [G] VALUES ('{a}', NULL, 9)")).await.err().expect("a duplicate");
    let tiberius::error::Error::Server(t) = &e else { panic!("{e}") };
    assert_eq!(t.code(), 2627, "{e}");
}

/// A declared fractional-second precision rounds as SQL Server's does
/// (half away from zero): `datetime2(3)`, `datetimeoffset(2)`, `time(0)`
/// kept six digits whatever was declared.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tiberius_time_precision() {
    let s = start_with(true);
    let mut c = connect(&s).await;
    batch(
        &mut c,
        "CREATE TABLE [T] ([Id] int PRIMARY KEY, [A] datetime2(3) NOT NULL, [B] datetimeoffset(2) NOT NULL, [C] time(0) NOT NULL, [D] datetime2 NOT NULL); \
         INSERT INTO [T] VALUES (1, '2026-03-23 15:03:07.0429169', '2026-03-23 15:03:07.0451 +00:00', '15:03:07.5', '2026-03-23 15:03:07.0429169')",
    )
    .await
    .unwrap();
    let r = batch(
        &mut c,
        "SELECT CONVERT(varchar(30), [A], 121), CAST([C] AS varchar(20)) FROM [T]; \
         SELECT COUNT(*) FROM [T] WHERE [A] = '2026-03-23 15:03:07.043' AND [B] = '2026-03-23 15:03:07.05 +00:00' AND [C] = '15:03:08'; \
         SELECT COLUMN_NAME, DATETIME_PRECISION FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'T' AND DATETIME_PRECISION IS NOT NULL ORDER BY ORDINAL_POSITION",
    )
    .await
    .unwrap();
    assert_eq!(r[0][0].get::<&str, _>(0), Some("2026-03-23 15:03:07.043"));
    assert!(r[0][0].get::<&str, _>(1).is_some_and(|t| t.starts_with("15:03:08")), "{:?}", r[0][0].get::<&str, _>(1));
    assert_eq!(int(&r[1]), [1]);
    let got: Vec<(String, Option<i32>)> = r[2].iter().map(|r| (r.get::<&str, _>(0).unwrap().to_string(), r.get::<i32, _>(1))).collect();
    assert_eq!(got, [("A".into(), Some(3)), ("B".into(), Some(2)), ("C".into(), Some(0)), ("D".into(), Some(6))]);
}

/// `datetime2(7)` (EF Core's DateTime), `datetimeoffset(7)`, `time(7)` keep
/// their seventh fractional digit (100 ns): the engine keeps microseconds,
/// and a value copied or written by an application came back a digit short,
/// so `WHERE Created = @value` with the value it had written matched
/// nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tiberius_hundred_nanoseconds() {
    use chrono::{NaiveDate, NaiveDateTime};
    let s = start_with(true);
    let mut c = connect(&s).await;
    batch(
        &mut c,
        "CREATE TABLE [O] ([Id] int PRIMARY KEY, [Created] datetime2 NOT NULL, [T] time(7) NULL, [Six] datetime2(6) NULL)",
    )
    .await
    .unwrap();
    let at = |nanos: u32| -> NaiveDateTime { NaiveDate::from_ymd_opt(2026, 3, 23).unwrap().and_hms_nano_opt(15, 3, 7, nanos).unwrap() };
    // A literal, a parameter, and EF Core's batched MERGE.
    batch(&mut c, "INSERT INTO [O] ([Id], [Created], [T], [Six]) VALUES (1, '2026-03-23 15:03:07.0429169', '15:03:07.9999999', '2026-03-23 15:03:07.0429169')")
        .await
        .unwrap();
    c.execute("INSERT INTO [O] ([Id], [Created]) VALUES (@P1, @P2)", &[&2i32, &at(42_916_500)]).await.unwrap();
    batch(
        &mut c,
        "MERGE [O] USING (VALUES (3, '2026-03-23 15:03:07.0429161', 0), (4, '2026-03-23 15:03:07.9999995', 1)) AS i ([Id], [Created], _Position) ON 1=0 \
         WHEN NOT MATCHED THEN INSERT ([Id], [Created]) VALUES (i.[Id], i.[Created]) OUTPUT INSERTED.[Id], i._Position;",
    )
    .await
    .unwrap();
    let created = |r: &[tiberius::Row]| -> Vec<NaiveDateTime> { r.iter().map(|r| r.get::<NaiveDateTime, _>(0).unwrap()).collect() };
    let r = batch(&mut c, "SELECT [Created] FROM [O] ORDER BY [Id]").await.unwrap();
    assert_eq!(created(&r[0]), [at(42_916_900), at(42_916_500), at(42_916_100), at(999_999_500)]);
    // SELECT *, an alias, a derived table, a join.
    let r = batch(
        &mut c,
        "SELECT * FROM [O] WHERE [Id] = 1; \
         SELECT [o].[Created] AS [When] FROM [O] AS [o] WHERE [o].[Id] = 1; \
         SELECT [x].[Created] FROM (SELECT TOP 1 [o].[Created] FROM [O] AS [o] ORDER BY [o].[Id]) AS [x]; \
         SELECT [b].[Created] FROM [O] AS [a] JOIN [O] AS [b] ON [a].[Id] = [b].[Id] WHERE [a].[Id] = 1",
    )
    .await
    .unwrap();
    assert_eq!(r[0][0].get::<NaiveDateTime, _>(1), Some(at(42_916_900)));
    assert_eq!(r[0][0].get::<chrono::NaiveTime, _>(2), Some(chrono::NaiveTime::from_hms_nano_opt(15, 3, 7, 999_999_900).unwrap()));
    // datetime2(6) keeps six, as declared.
    assert_eq!(r[0][0].get::<NaiveDateTime, _>(3), Some(at(42_917_000)));
    assert_eq!(r[1][0].columns()[0].name(), "When");
    assert_eq!(created(&r[1]), [at(42_916_900)]);
    assert_eq!(created(&r[2]), [at(42_916_900)]);
    assert_eq!(created(&r[3]), [at(42_916_900)]);
    // Equality with the value written finds it, and only it: ids 1-3 share
    // their microsecond.
    let ids = |r: &[tiberius::Row]| int(r);
    for (nanos, want) in [(42_916_900u32, vec![1]), (42_916_500, vec![2]), (42_916_100, vec![3]), (42_916_200, vec![])] {
        let r = c
            .query("SELECT [Id] FROM [O] WHERE [Created] = @P1 ORDER BY [Id]", &[&at(nanos)])
            .await
            .unwrap()
            .into_first_result()
            .await
            .unwrap();
        assert_eq!(ids(&r), want, "{nanos}");
    }
    let r = c.query("SELECT [Id] FROM [O] WHERE [Created] > @P1 ORDER BY [Created]", &[&at(42_916_500)]).await.unwrap().into_first_result().await.unwrap();
    assert_eq!(ids(&r), [1, 4]);
    let r = c.query("SELECT [Id] FROM [O] WHERE [Created] <= @P1 ORDER BY [Created] DESC", &[&at(42_916_500)]).await.unwrap().into_first_result().await.unwrap();
    assert_eq!(ids(&r), [2, 3]);
    let r = batch(&mut c, "SELECT [Id] FROM [O] ORDER BY [Created], [Id]").await.unwrap();
    assert_eq!(ids(&r[0]), [3, 2, 1, 4]);
    // An UPDATE writes the digit too.
    c.execute("UPDATE [O] SET [Created] = @P1 WHERE [Id] = 4", &[&at(42_916_300)]).await.unwrap();
    let r = batch(&mut c, "SELECT [Created] FROM [O] WHERE [Id] = 4").await.unwrap();
    assert_eq!(created(&r[0]), [at(42_916_300)]);
    // The digit stays out of sight.
    let r = batch(&mut c, "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'O'").await.unwrap();
    assert_eq!(int(&r[0]), [4]);
    // datetimeoffset(7) too, its value in UTC.
    batch(
        &mut c,
        "CREATE TABLE [Z] ([Id] int PRIMARY KEY, [At] datetimeoffset NOT NULL); \
         INSERT INTO [Z] ([Id], [At]) VALUES (1, '2026-03-23 15:03:07.1234567 +00:00')",
    )
    .await
    .unwrap();
    let r = batch(&mut c, "SELECT [At] FROM [Z]").await.unwrap();
    let got = r[0][0].get::<chrono::DateTime<chrono::FixedOffset>, _>(0).unwrap();
    assert_eq!(got.naive_utc(), at(123_456_700));
}

/// Every way a statement reads a `datetime2(7)` column gives SQL Server's
/// answer, or is refused: `=` reading the seventh digit while IN, BETWEEN
/// or an UPDATE's WHERE compared to the microsecond would be two answers to
/// one question. Values 1-3 share their microsecond; 4 is a year's last
/// 100 ns, which the engine's microsecond rounds into the next year.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tiberius_hundred_nanoseconds_everywhere() {
    use chrono::{NaiveDate, NaiveDateTime};
    let s = start_with(true);
    let mut c = connect(&s).await;
    batch(
        &mut c,
        "CREATE TABLE [E] ([Id] int PRIMARY KEY, [At] datetime2 NOT NULL, [Copy] datetime2 NULL); \
         INSERT INTO [E] ([Id], [At]) VALUES (1, '2026-03-23 15:03:07.0429169'), (2, '2026-03-23 15:03:07.0429165'), \
         (3, '2026-03-23 15:03:07.0429161'), (4, '2026-12-31 23:59:59.9999999')",
    )
    .await
    .unwrap();
    let at = |nanos: u32| -> NaiveDateTime { NaiveDate::from_ymd_opt(2026, 3, 23).unwrap().and_hms_nano_opt(15, 3, 7, nanos).unwrap() };
    let ids = |r: &[tiberius::Row]| int(r);
    async fn q(c: &mut Client<Compat<tokio::net::TcpStream>>, sql: &str, p: Vec<NaiveDateTime>) -> Vec<tiberius::Row> {
        let params: Vec<&dyn tiberius::ToSql> = p.iter().map(|v| v as &dyn tiberius::ToSql).collect();
        c.query(sql, &params).await.unwrap().into_first_result().await.unwrap()
    }
    let (v1, v2, v3) = (at(42_916_900), at(42_916_500), at(42_916_100));
    assert_eq!(ids(&q(&mut c, "SELECT [Id] FROM [E] WHERE [At] IN (@P1, @P2) ORDER BY [Id]", vec![v1, v3]).await), [1, 3]);
    assert_eq!(ids(&q(&mut c, "SELECT [Id] FROM [E] WHERE [At] NOT IN (@P1) AND [Id] < 4 ORDER BY [Id]", vec![v2]).await), [1, 3]);
    assert_eq!(ids(&q(&mut c, "SELECT [Id] FROM [E] WHERE [At] BETWEEN @P1 AND @P2 ORDER BY [Id]", vec![v3, v2]).await), [2, 3]);
    assert_eq!(ids(&q(&mut c, "SELECT [Id] FROM [E] WHERE @P1 = [At]", vec![v2]).await), [2]);
    assert_eq!(ids(&q(&mut c, "SELECT [Id] FROM [E] WHERE @P1 < [At] AND [Id] < 4", vec![v2]).await), [1]);
    let r = q(&mut c, "SELECT CASE WHEN [At] = @P1 THEN 1 ELSE 0 END FROM [E] ORDER BY [Id]", vec![v2]).await;
    assert_eq!(ids(&r), [0, 1, 0, 0]);
    // EF's tracked UPDATE/DELETE and ExecuteUpdate/ExecuteDelete.
    c.execute("UPDATE [E] SET [Copy] = [At] WHERE [At] = @P1", &[&v2]).await.unwrap();
    c.execute("DELETE FROM [E] WHERE [At] = @P1", &[&v3]).await.unwrap();
    let r = batch(&mut c, "SELECT [Id] FROM [E] WHERE [Copy] IS NOT NULL; SELECT COUNT(*) FROM [E]").await.unwrap();
    assert_eq!(ids(&r[0]), [2]);
    assert_eq!(int(&r[1]), [3]);
    let r = batch(&mut c, "SELECT [Copy] FROM [E] WHERE [Id] = 2").await.unwrap();
    assert_eq!(r[0][0].get::<NaiveDateTime, _>(0), Some(v2));
    // What reads a coarser unit reads the value's own: the year's last
    // 100 ns is still that year, that day.
    let r = batch(
        &mut c,
        "SELECT YEAR([At]), DATEPART(second, [At]), CONVERT(varchar(10), CAST([At] AS date), 23) FROM [E] WHERE [Id] = 4; \
         SELECT CAST([At] AS varchar(30)), CONVERT(varchar(30), [At], 126) FROM [E] WHERE [Id] = 1; \
         SELECT MAX([At]), MIN([At]) FROM [E] WHERE [Id] < 4; \
         SELECT DATEADD(day, 1, [At]) FROM [E] WHERE [Id] = 4; \
         SELECT COUNT(*) FROM (SELECT DISTINCT [At] FROM [E]) AS [t]; \
         SELECT [At], COUNT(*) FROM [E] GROUP BY [At] ORDER BY [At]",
    )
    .await
    .unwrap();
    assert_eq!(r[0][0].get::<i32, _>(0), Some(2026));
    assert_eq!(r[0][0].get::<i32, _>(1), Some(59));
    assert_eq!(r[0][0].get::<&str, _>(2), Some("2026-12-31"));
    assert_eq!(r[1][0].get::<&str, _>(0), Some("2026-03-23 15:03:07.0429169"));
    assert_eq!(r[1][0].get::<&str, _>(1), Some("2026-03-23T15:03:07.0429169"));
    assert_eq!(r[2][0].get::<NaiveDateTime, _>(0), Some(v1));
    assert_eq!(r[2][0].get::<NaiveDateTime, _>(1), Some(v2));
    let next = NaiveDate::from_ymd_opt(2027, 1, 1).unwrap().and_hms_nano_opt(23, 59, 59, 999_999_900).unwrap();
    assert_eq!(r[3][0].get::<NaiveDateTime, _>(0), Some(next));
    assert_eq!(int(&r[4]), [3]);
    assert_eq!(r[5].len(), 3);
    assert_eq!(r[5][0].get::<NaiveDateTime, _>(0), Some(v2));
    // What cannot read the digit yet is refused, not answered to the
    // microsecond.
    for sql in [
        "SELECT ISNULL([At], [Copy]) FROM [E]",
        "SELECT [Id] FROM [E] WHERE DATEADD(day, 1, [At]) > SYSDATETIME()",
        "INSERT INTO [E] ([Id], [At]) SELECT 9, [At] FROM [E] WHERE [Id] = 1",
        "SELECT [Id] FROM [E] WHERE [At] IN (SELECT [Copy] FROM [E])",
        "SELECT DATEDIFF(nanosecond, [At], [Copy]) FROM [E]",
        "SELECT COUNT(DISTINCT [At]) FROM [E]",
    ] {
        let e = batch(&mut c, sql).await.err().unwrap_or_else(|| panic!("{sql} was answered"));
        let tiberius::error::Error::Server(t) = &e else { panic!("{e}") };
        assert!(t.message().contains("seventh digit"), "{sql}: {e}");
    }
}

/// A grouped or DISTINCT query sorted by a uniqueidentifier sorts in SQL
/// Server's order too.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tiberius_guid_order_grouped() {
    let s = start_with(true);
    let mut c = connect(&s).await;
    // In SQL Server's order: b < a.
    let (a, b) = ("00000000-0000-0000-0000-000000000001", "01000000-0000-0000-0000-000000000000");
    batch(
        &mut c,
        &format!(
            "CREATE TABLE [L] ([Id] int PRIMARY KEY, [G] uniqueidentifier NOT NULL); \
             INSERT INTO [L] VALUES (1, '{a}'), (2, '{b}'), (3, '{a}')"
        ),
    )
    .await
    .unwrap();
    let r = batch(
        &mut c,
        "SELECT [G], COUNT(*) FROM [L] GROUP BY [G] ORDER BY [G]; \
         SELECT DISTINCT [G] FROM [L] ORDER BY [G]",
    )
    .await
    .unwrap();
    let grouped: Vec<(String, i32)> = r[0].iter().map(|r| (r.get::<uuid::Uuid, _>(0).unwrap().to_string(), r.get::<i32, _>(1).unwrap())).collect();
    assert_eq!(grouped, [(b.to_string(), 1), (a.to_string(), 2)]);
    let g: Vec<String> = r[1].iter().map(|r| r.get::<uuid::Uuid, _>(0).unwrap().to_string()).collect();
    assert_eq!(g, [b, a]);
    // A nullable one: NULL first, then SQL Server's order, through the
    // engine's DISTINCT-then-sort and a derived table's alias.
    batch(&mut c, &format!("CREATE TABLE [N] ([Id] int PRIMARY KEY, [G] uniqueidentifier NULL); INSERT INTO [N] VALUES (1, '{a}'), (2, '{b}'), (3, NULL)"))
        .await
        .unwrap();
    let r = batch(
        &mut c,
        "SELECT DISTINCT [G] FROM [N] ORDER BY [G]; SELECT [x].[K] FROM (SELECT [G] AS [K] FROM [N]) AS [x] ORDER BY [x].[K] DESC",
    )
    .await
    .unwrap();
    let g = |r: &[tiberius::Row]| -> Vec<Option<String>> { r.iter().map(|r| r.get::<uuid::Uuid, _>(0).map(|u| u.to_string())).collect() };
    assert_eq!(g(&r[0]), [None, Some(b.to_string()), Some(a.to_string())]);
    assert_eq!(g(&r[1]), [Some(a.to_string()), Some(b.to_string()), None]);
    // Over a join, grouped and DISTINCT: refused before, as their output
    // sorted by the bytes.
    batch(&mut c, "CREATE TABLE [Line] ([Id] int PRIMARY KEY, [LId] int NOT NULL, [Qty] int NOT NULL); INSERT INTO [Line] VALUES (1, 1, 5), (2, 2, 7), (3, 3, 1), (4, 1, 2)")
        .await
        .unwrap();
    let r = batch(
        &mut c,
        "SELECT [l].[G], SUM([x].[Qty]) FROM [L] AS [l] INNER JOIN [Line] AS [x] ON [l].[Id] = [x].[LId] GROUP BY [l].[G] ORDER BY [l].[G]; \
         SELECT DISTINCT [l].[G] FROM [L] AS [l] INNER JOIN [Line] AS [x] ON [l].[Id] = [x].[LId] ORDER BY [l].[G] DESC",
    )
    .await
    .unwrap();
    let grouped: Vec<(String, i32)> = r[0].iter().map(|r| (r.get::<uuid::Uuid, _>(0).unwrap().to_string(), r.get::<i32, _>(1).unwrap())).collect();
    assert_eq!(grouped, [(b.to_string(), 7), (a.to_string(), 8)]);
    assert_eq!(g(&r[1]), [Some(a.to_string()), Some(b.to_string())]);
}

/// EF Core's split query: the parent (a derived table with select-list
/// subqueries, LEFT JOINed to each product's first image) and its child (the
/// same products INNER JOINed to their images) come in the same
/// uniqueidentifier order, SQL Server's. EF Core pairs the two by position.
/// The parent's derived table lost the column's order in its copy (its
/// subqueries make its body a join) and came sorted by the bytes: the
/// children went to the wrong parents, with no error.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tiberius_split_query_parent_and_child_agree_on_guid_order() {
    let s = start_with(true);
    let mut c = connect(&s).await;
    // SQL Server's order is ascending `k`; the bytes' is descending.
    let id = |k: u32| format!("{:08x}-0000-0000-0000-{k:012}", 0xff00 - k);
    batch(
        &mut c,
        "CREATE TABLE [P] ([Id] uniqueidentifier NOT NULL PRIMARY KEY, [Name] nvarchar(50) NOT NULL, [N] int NOT NULL); \
         CREATE TABLE [Price] ([Id] int PRIMARY KEY, [ProductId] uniqueidentifier NOT NULL, [Price] int NOT NULL); \
         CREATE TABLE [Image] ([Id] int PRIMARY KEY, [ProductId] uniqueidentifier NOT NULL, [ListingOrder] int NOT NULL, [Url] nvarchar(50) NOT NULL)",
    )
    .await
    .unwrap();
    const N: u32 = 8;
    let products: Vec<String> = (1..=N).map(|k| format!("('{}', N'Name{k}', {k})", id(k))).collect();
    let prices: Vec<String> = (1..=N).map(|k| format!("({k}, '{}', {})", id(k), k * 10)).collect();
    let images: Vec<String> = (1..=N * 2).map(|i| format!("({i}, '{}', {}, N'u{i}')", id((i + 1) / 2), i % 2)).collect();
    batch(
        &mut c,
        &format!(
            "INSERT INTO [P] VALUES {}; INSERT INTO [Price] VALUES {}; INSERT INTO [Image] VALUES {}",
            products.join(", "),
            prices.join(", "),
            images.join(", ")
        ),
    )
    .await
    .unwrap();
    let r = batch(
        &mut c,
        "SELECT [d6].[Id], [d6].[N], [d6].[c], [d8].[Url] FROM (\
             SELECT TOP(10) [d].[Id], [d].[N], (SELECT TOP(1) [d3].[Price] FROM [Price] AS [d3] WHERE [d].[Id] = [d3].[ProductId]) AS [c], \
             (SELECT COUNT(*) FROM [Image] AS [d4] WHERE [d].[Id] = [d4].[ProductId]) AS [c0] FROM [P] AS [d] WHERE [d].[N] > 2) AS [d6] \
         LEFT JOIN (SELECT [d7].[Url], [d7].[ProductId] FROM (SELECT [d2].[Url], [d2].[ProductId], \
             ROW_NUMBER() OVER(PARTITION BY [d2].[ProductId] ORDER BY [d2].[ListingOrder]) AS [row] FROM [Image] AS [d2]) AS [d7] \
             WHERE [d7].[row] <= 1) AS [d8] ON [d6].[Id] = [d8].[ProductId] ORDER BY [d6].[Id]; \
         SELECT [d14].[Url], [d11].[Id], [d14].[Id] FROM (SELECT TOP(10) [d].[Id] FROM [P] AS [d] WHERE [d].[N] > 2) AS [d11] \
         INNER JOIN (SELECT [d10].[Url], [d10].[Id], [d10].[ListingOrder], [d10].[ProductId] FROM [Image] AS [d10]) AS [d14] \
             ON [d11].[Id] = [d14].[ProductId] ORDER BY [d11].[Id], [d14].[ListingOrder]; \
         SELECT [q].[N] FROM (SELECT [d].[N], [d].[Name], (SELECT TOP(1) [d3].[Price] FROM [Price] AS [d3] WHERE [d].[Id] = [d3].[ProductId]) AS [c] \
             FROM [P] AS [d]) AS [q] WHERE [q].[Name] = N'NAME5'",
    )
    .await
    .unwrap();
    let want: Vec<String> = (3..=N).map(id).collect();
    let parents: Vec<String> = r[0].iter().map(|r| r.get::<uuid::Uuid, _>(0).unwrap().to_string()).collect();
    assert_eq!(parents, want);
    for (row, k) in r[0].iter().zip(3..=N) {
        assert_eq!(row.get::<i32, _>(1), Some(k as i32));
        assert_eq!(row.get::<i32, _>(2), Some(k as i32 * 10));
        assert_eq!(row.get::<&str, _>(3), Some(format!("u{}", k * 2).as_str()));
    }
    // The child's parents, in the order they come: the parent's.
    let mut of_children: Vec<String> = r[1].iter().map(|r| r.get::<uuid::Uuid, _>(1).unwrap().to_string()).collect();
    assert_eq!(of_children.len(), 2 * want.len());
    of_children.dedup();
    assert_eq!(of_children, parents);
    // And a text column of such a derived table compares as its table's
    // does (case-insensitively here).
    assert_eq!(r[2].iter().map(|r| r.get::<i32, _>(0).unwrap()).collect::<Vec<_>>(), [5]);
}

/// A set operation's branch in parentheses, and a row assignment mixing an
/// aggregate with a plain value: the engine rewrites both through a derived
/// table it names itself. Named `__rdb_…`, the role guard took the alias
/// for a catalogue and refused the client's statement ("permission denied:
/// "__rdb_branch0" is a system catalog").
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tiberius_engine_named_aliases_pass_the_catalogue_guard() {
    let s = start();
    let mut c = connect(&s).await;
    let r = batch(&mut c, "(SELECT 1 UNION SELECT 2) INTERSECT SELECT 2; SELECT 2 INTERSECT (SELECT 1 UNION SELECT 2)").await.unwrap();
    for rows in &r {
        assert_eq!(rows.iter().map(|r| r.get::<i32, _>(0).unwrap()).collect::<Vec<_>>(), [2]);
    }
    batch(&mut c, "CREATE TABLE [G] ([Id] int PRIMARY KEY, [N] int NULL, [V] nvarchar(20) NULL); INSERT INTO [G] VALUES (1, 5, N'a'), (2, 9, N'b')")
        .await
        .unwrap();
    let r = batch(&mut c, "UPDATE [G] SET [N] = (SELECT MAX([x].[N]) FROM [G] AS [x]) WHERE [Id] = 1; SELECT [N] FROM [G] ORDER BY [Id]").await.unwrap();
    assert_eq!(r.last().unwrap().iter().map(|r| r.get::<i32, _>(0).unwrap()).collect::<Vec<_>>(), [9, 9]);
}

/// A `datetime2(7)` selected beside a correlated subquery over several
/// rows keeps its seventh digit. The subquery is joined to a copy of the
/// outer rows, and the copy was made of the visible columns alone: the
/// statement failed with `column d.__t7_at does not exist`. With TOP, where
/// the rows the limit keeps are copied first, and without; and through a
/// join whose operand is computed, which copies its relation the same way.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tiberius_datetime2_beside_a_correlated_subquery() {
    let s = start();
    let mut c = connect(&s).await;
    batch(
        &mut c,
        "CREATE TABLE [Ev] ([Id] int PRIMARY KEY, [At] datetime2(7) NOT NULL, [Until] datetime2(7) NULL, [Name] nvarchar(20) NOT NULL); \
         CREATE TABLE [Hit] ([Id] int PRIMARY KEY, [EvId] int NOT NULL, [N] int NOT NULL); \
         INSERT INTO [Ev] VALUES (1, '2026-01-02 03:04:05.1234567', '2026-02-02 00:00:00.0000001', N'a'), \
             (2, '2026-01-02 03:04:05.7654321', NULL, N'b'), (3, '2026-01-02 03:04:05.0000009', '2026-03-03 00:00:00.9999999', N'c'); \
         INSERT INTO [Hit] VALUES (1, 1, 10), (2, 1, 5), (3, 3, 7)",
    )
    .await
    .unwrap();
    let text = |r: &tiberius::Row, at: usize| r.get::<chrono::NaiveDateTime, _>(at).map(|t| {
        use chrono::Timelike;
        format!("{}.{:07}", t.format("%Y-%m-%d %H:%M:%S"), t.nanosecond() / 100)
    });
    let want = [
        (1, Some("2026-01-02 03:04:05.1234567"), Some("2026-02-02 00:00:00.0000001"), Some(15), 2),
        (2, Some("2026-01-02 03:04:05.7654321"), None, None, 0),
        (3, Some("2026-01-02 03:04:05.0000009"), Some("2026-03-03 00:00:00.9999999"), Some(7), 1),
    ];
    for sql in [
        "SELECT [d].[Id], [d].[At], [d].[Until], (SELECT SUM([h].[N]) FROM [Hit] AS [h] WHERE [h].[EvId] = [d].[Id]) AS [Total], \
             (SELECT COUNT(*) FROM [Hit] AS [h] WHERE [h].[EvId] = [d].[Id]) AS [Hits] FROM [Ev] AS [d] ORDER BY [d].[Id]",
        "SELECT TOP(3) [d].[Id], [d].[At], [d].[Until], (SELECT SUM([h].[N]) FROM [Hit] AS [h] WHERE [h].[EvId] = [d].[Id]) AS [Total], \
             (SELECT COUNT(*) FROM [Hit] AS [h] WHERE [h].[EvId] = [d].[Id]) AS [Hits] FROM [Ev] AS [d] WHERE [d].[Id] > 0 ORDER BY [d].[Id]",
    ] {
        let r = batch(&mut c, sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
        let got: Vec<_> = r[0]
            .iter()
            .map(|r| (r.get::<i32, _>(0).unwrap(), text(r, 1), text(r, 2), r.get::<i32, _>(3), r.get::<i32, _>(4).unwrap()))
            .collect();
        let want: Vec<_> = want.iter().map(|(id, at, until, total, hits)| (*id, at.map(str::to_string), until.map(str::to_string), *total, *hits)).collect();
        assert_eq!(got, want, "{sql}");
    }
    // TOP keeps the first two by the time, seventh digit and all.
    let r = batch(
        &mut c,
        "SELECT TOP(2) [d].[Id], [d].[At], (SELECT COUNT(*) FROM [Hit] AS [h] WHERE [h].[EvId] = [d].[Id]) AS [Hits] FROM [Ev] AS [d] ORDER BY [d].[At]",
    )
    .await
    .unwrap();
    let got: Vec<_> = r[0].iter().map(|r| (r.get::<i32, _>(0).unwrap(), text(r, 1), r.get::<i32, _>(2).unwrap())).collect();
    assert_eq!(got, [(3, Some("2026-01-02 03:04:05.0000009".to_string()), 1), (1, Some("2026-01-02 03:04:05.1234567".to_string()), 2)]);
    // A join on a computed operand copies its relation too.
    let r = batch(
        &mut c,
        "SELECT [d].[Id], [d].[At], [h].[N] FROM [Ev] AS [d] INNER JOIN [Hit] AS [h] ON [d].[Id] + 0 = [h].[EvId] ORDER BY [h].[Id]",
    )
    .await
    .unwrap();
    let got: Vec<_> = r[0].iter().map(|r| (r.get::<i32, _>(0).unwrap(), text(r, 1), r.get::<i32, _>(2).unwrap())).collect();
    assert_eq!(
        got,
        [
            (1, Some("2026-01-02 03:04:05.1234567".to_string()), 10),
            (1, Some("2026-01-02 03:04:05.1234567".to_string()), 5),
            (3, Some("2026-01-02 03:04:05.0000009".to_string()), 7)
        ]
    );
}

/// What the ERP application's pages and writes sent that a query pass had
/// not: a string `+` in a WHERE (every such WHERE failed with "unexpected
/// character '|'", the tables all having case-insensitive and
/// uniqueidentifier columns), EF Core's date-part equality written as
/// `~CAST(a ^ b AS bit)` (SQL Server's XOR and the NOT of a bit), a
/// ROW_NUMBER over joined tables inside a derived table, and an unknown
/// column, which is error 207.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tiberius_application_findings() {
    let s = start_with(true);
    let mut c = connect(&s).await;
    batch(
        &mut c,
        "CREATE TABLE [Co] ([Id] uniqueidentifier NOT NULL PRIMARY KEY, [Name] nvarchar(50) NOT NULL, [Users] nvarchar(max) NULL, [Placed] datetime2(7) NULL); \
         INSERT INTO [Co] VALUES ('00000000-0000-0000-0000-000000000001', N'Acme', NULL, '2026-03-05 10:00:00.1234567'), \
             ('00000000-0000-0000-0000-000000000002', N'Bolt', N'u1', '2026-04-05 10:00:00'), \
             ('00000000-0000-0000-0000-000000000003', N'Core', N'U9;u2', NULL)",
    )
    .await
    .unwrap();
    // The application's own UPDATE: append a user once, with parameters.
    let append = "UPDATE [Co] SET [Users] = COALESCE([Users] + N';', N'') + @P1 WHERE [Id] = @P2 AND (COALESCE([Users], N'') NOT LIKE N'%' + @P1 + N'%')";
    for (id, times) in [("00000000-0000-0000-0000-000000000001", 2), ("00000000-0000-0000-0000-000000000003", 1)] {
        for _ in 0..times {
            c.execute(append, &[&"u9", &uuid::Uuid::parse_str(id).unwrap()]).await.unwrap();
        }
    }
    let r = batch(&mut c, "SELECT [Users] FROM [Co] ORDER BY [Name]").await.unwrap();
    let users: Vec<Option<String>> = r[0].iter().map(|r| r.get::<&str, _>(0).map(str::to_string)).collect();
    // Core already holds 'U9' (case-insensitively): not appended again.
    assert_eq!(users, [Some("u9".to_string()), Some("u1".to_string()), Some("U9;u2".to_string())]);
    let r = batch(
        &mut c,
        "SELECT [Name] FROM [Co] WHERE [Name] = N'AC' + N'ME'; \
         SELECT [Name] FROM [Co] WHERE [Name] + N'!' = N'bolt!'; \
         SELECT [Name] FROM [Co] WHERE [Users] LIKE N'%' + N'U' + N'%' ORDER BY [Name]",
    )
    .await
    .unwrap();
    let names = |rows: &[tiberius::Row]| -> Vec<String> { rows.iter().map(|r| r.get::<&str, _>(0).unwrap().to_string()).collect() };
    assert_eq!(names(&r[0]), ["Acme"]);
    assert_eq!(names(&r[1]), ["Bolt"]);
    assert_eq!(names(&r[2]), ["Acme", "Bolt", "Core"]);
    // EF Core's `x.Placed.Value.Month == 3`, and `^`, `&`, `|`, `~` over
    // integers.
    let r = batch(
        &mut c,
        "SELECT COUNT(*) FROM [Co] AS [d] WHERE CASE WHEN [d].[Placed] IS NOT NULL THEN ~CAST(DATEPART(month, CONVERT(date, [d].[Placed])) ^ 3 AS bit) ELSE CAST(0 AS bit) END = CAST(1 AS bit) \
             AND CASE WHEN [d].[Placed] IS NOT NULL THEN ~CAST(DATEPART(year, CONVERT(date, [d].[Placed])) ^ DATEPART(year, CONVERT(date, [d].[Placed])) AS bit) ELSE CAST(0 AS bit) END = CAST(1 AS bit); \
         SELECT 6 ^ 3, 6 & 3, 6 | 3, ~6, CAST(6 ^ 6 AS bit), CAST(6 ^ 5 AS bit); \
         SELECT [d].[Name] FROM [Co] AS [d] WHERE DATEPART(month, [d].[Placed]) ^ 4 = 0",
    )
    .await
    .unwrap();
    assert_eq!(r[0][0].get::<i32, _>(0), Some(1));
    assert_eq!((0..4).map(|i| r[1][0].get::<i32, _>(i).unwrap()).collect::<Vec<_>>(), [5, 2, 7, -7]);
    assert_eq!((r[1][0].get::<bool, _>(4), r[1][0].get::<bool, _>(5)), (Some(false), Some(true)));
    assert_eq!(names(&r[2]), ["Bolt"]);
    // ROW_NUMBER over joined tables inside a derived table, ordered by the
    // uniqueidentifier keys of both.
    batch(
        &mut c,
        "CREATE TABLE [Li] ([Id] uniqueidentifier NOT NULL PRIMARY KEY, [CoId] uniqueidentifier NOT NULL, [N] int NOT NULL); \
         INSERT INTO [Li] VALUES ('00000000-0000-0000-0000-00000000000b', '00000000-0000-0000-0000-000000000001', 20), \
             ('00000000-0000-0000-0000-00000000000a', '00000000-0000-0000-0000-000000000001', 10), \
             ('00000000-0000-0000-0000-00000000000c', '00000000-0000-0000-0000-000000000002', 30)",
    )
    .await
    .unwrap();
    let r = batch(
        &mut c,
        "SELECT [s].[Name], [s].[N] FROM (SELECT [d7].[Name], [d8].[N], ROW_NUMBER() OVER(PARTITION BY [d7].[Id] ORDER BY [d7].[Id], [d8].[Id]) AS [row] \
             FROM [Co] AS [d7] INNER JOIN [Li] AS [d8] ON [d7].[Id] = [d8].[CoId]) AS [s] WHERE [s].[row] <= 1 ORDER BY [s].[Name]",
    )
    .await
    .unwrap();
    let got: Vec<(String, i32)> = r[0].iter().map(|r| (r.get::<&str, _>(0).unwrap().to_string(), r.get::<i32, _>(1).unwrap())).collect();
    assert_eq!(got, [("Acme".to_string(), 10), ("Bolt".to_string(), 30)]);
    // An unknown column is SQL Server's 207, alone and in a join.
    for sql in [
        "SELECT [c].[Nope] FROM [Co] AS [c]",
        "SELECT [c].[Id], [c].[Nope] FROM [Co] AS [c] INNER JOIN [Li] AS [l] ON [c].[Id] = [l].[CoId]",
    ] {
        let e = batch(&mut c, sql).await.err().unwrap_or_else(|| panic!("{sql} was answered"));
        assert_eq!(number(&e), Some(207), "{sql}: {e}");
    }
}

/// The engine's `PARTITION BY HASH` through this front end: on CREATE TABLE
/// (it was dropped with the storage clauses, and a plain table reported as
/// made), and as `ALTER TABLE … PARTITION BY HASH` for a table that has
/// rows. A uniqueidentifier key partitions (its SQL Server order is only an
/// order). What there is no such thing as here is refused, not ignored: a
/// partition scheme, RANGE.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tiberius_partition_by_hash() {
    // On disk, and bounded: removed when the test is done.
    let dir = std::env::temp_dir().join(format!("rdb_tds_partition_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    struct Remove(std::path::PathBuf);
    impl Drop for Remove {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let s = start_at(true, 2601, Some(dir.clone()));
    let _remove = Remove(dir);
    let mut c = connect(&s).await;
    let (pg, conn) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=rasterized password={PASSWORD} dbname=postgres", s.pg_port),
        tokio_postgres::NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(conn);
    let partitions = |rows: Vec<tokio_postgres::SimpleQueryMessage>| rows.iter().filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_))).count();
    batch(
        &mut c,
        "CREATE TABLE [PtNew] ([Id] uniqueidentifier NOT NULL, [CompanyId] uniqueidentifier NOT NULL, [Name] nvarchar(50) NULL, [At] datetime2(7) NULL, \
             CONSTRAINT [PK_PtNew] PRIMARY KEY ([Id], [CompanyId])) PARTITION BY HASH ([CompanyId]) PARTITIONS 4; \
         CREATE TABLE [PtOld] ([Id] uniqueidentifier NOT NULL, [CompanyId] uniqueidentifier NOT NULL, [Name] nvarchar(50) NULL, [N] int NOT NULL, \
             CONSTRAINT [PK_PtOld] PRIMARY KEY ([Id], [CompanyId])) ON [PRIMARY]",
    )
    .await
    .unwrap();
    assert_eq!(partitions(pg.simple_query("SHOW PARTITIONS ptnew").await.unwrap()), 4);
    assert!(pg.simple_query("SHOW PARTITIONS ptold").await.is_err(), "a plain table");
    let (a, b) = ("11111111-1111-1111-1111-111111111111", "22222222-2222-2222-2222-222222222222");
    for table in ["PtNew", "PtOld"] {
        let extra = if table == "PtNew" { "'2026-01-02 03:04:05.1234567'" } else { "7" };
        batch(
            &mut c,
            &format!(
                "INSERT INTO [{table}] VALUES ('00000000-0000-0000-0000-000000000001', '{a}', N'Alpha', {extra}), \
                 ('00000000-0000-0000-0000-000000000002', '{a}', N'beta', {extra}), ('01000000-0000-0000-0000-000000000000', '{b}', N'Gamma', {extra})"
            ),
        )
        .await
        .unwrap();
    }
    // A table with rows, converted in place.
    batch(&mut c, "ALTER TABLE [PtOld] PARTITION BY HASH ([CompanyId]) PARTITIONS 8").await.unwrap();
    assert_eq!(partitions(pg.simple_query("SHOW PARTITIONS ptold").await.unwrap()), 8);
    for table in ["PtNew", "PtOld"] {
        let r = batch(
            &mut c,
            &format!(
                "SELECT [Name] FROM [{table}] WHERE [CompanyId] = '{a}' AND [Name] = N'ALPHA'; \
                 SELECT [Name] FROM [{table}] ORDER BY [Id]; \
                 SELECT COUNT(*) FROM [{table}]"
            ),
        )
        .await
        .unwrap();
        let names = |rows: &[tiberius::Row]| -> Vec<String> { rows.iter().map(|r| r.get::<&str, _>(0).unwrap().to_string()).collect() };
        assert_eq!(names(&r[0]), ["Alpha"], "{table}");
        // SQL Server's uniqueidentifier order, across partitions.
        assert_eq!(names(&r[1]), ["Gamma", "Alpha", "beta"], "{table}");
        assert_eq!(r[2][0].get::<i32, _>(0), Some(3), "{table}");
    }
    let at = batch(&mut c, "SELECT [At] FROM [PtNew] WHERE [CompanyId] = '11111111-1111-1111-1111-111111111111' AND [Name] = N'beta'").await.unwrap();
    assert_eq!(
        at[0][0].get::<chrono::NaiveDateTime, _>(0),
        Some(chrono::NaiveDate::from_ymd_opt(2026, 1, 2).unwrap().and_hms_nano_opt(3, 4, 5, 123_456_700).unwrap())
    );
    for (sql, what) in [
        ("CREATE TABLE [PtR] ([Id] int NOT NULL PRIMARY KEY, [At] datetime2 NOT NULL) PARTITION BY RANGE ([At])", "only by HASH"),
        ("CREATE TABLE [PtS] ([Id] int NOT NULL PRIMARY KEY, [At] datetime2 NOT NULL) ON psMonth([At])", "partition scheme"),
        ("ALTER TABLE [PtOld] PARTITION BY LIST ([CompanyId])", "only by HASH"),
    ] {
        let e = batch(&mut c, sql).await.err().unwrap_or_else(|| panic!("{sql} was accepted"));
        let tiberius::error::Error::Server(t) = &e else { panic!("{e}") };
        assert!(t.message().contains(what), "{sql}: {e}");
    }
}

/// A grouped query sorted by an aggregate that is not in its select list
/// (EF Core's `GroupBy(…).OrderByDescending(g => g.Count()).Select(g =>
/// g.Key)`): refused as "ORDER BY term 'count(*)' is not in the SELECT
/// list". And what SQL Server's index catalogue says of a table: its
/// indexes once each under the names they were declared with, and their
/// columns (`sys.index_columns` was empty, `filter_definition` unknown, the
/// primary key listed twice).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tiberius_grouped_order_and_index_catalogue() {
    let s = start_with(true);
    let mut c = connect(&s).await;
    batch(
        &mut c,
        "CREATE TABLE [Note] ([Id] int NOT NULL, [UserId] nvarchar(40) NOT NULL, [Kind] int NOT NULL, [Email] nvarchar(60) NULL, CONSTRAINT [PK_Note] PRIMARY KEY ([Id])); \
         CREATE INDEX [IX_Note_UserId_Kind] ON [Note] ([UserId], [Kind]); \
         CREATE UNIQUE INDEX [EmailIndex] ON [Note] ([Email]) WHERE [Email] IS NOT NULL; \
         INSERT INTO [Note] VALUES (1, N'ann', 1, N'a@x'), (2, N'Bob', 1, NULL), (3, N'bob', 2, N'b@x'), (4, N'BOB', 2, NULL), (5, N'cy', 1, NULL), (6, N'ann', 3, NULL)",
    )
    .await
    .unwrap();
    let r = batch(
        &mut c,
        "SELECT TOP(1) [UserId] FROM [Note] GROUP BY [UserId] ORDER BY COUNT(*) DESC; \
         SELECT [UserId] FROM [Note] GROUP BY [UserId] ORDER BY COUNT(*) DESC, [UserId]; \
         SELECT [n].[Kind], COUNT(*) AS [c] FROM [Note] AS [n] GROUP BY [n].[Kind] ORDER BY MAX([n].[Id]) DESC; \
         SELECT COUNT(*) FROM [Note] GROUP BY [Kind] ORDER BY [Kind] DESC",
    )
    .await
    .unwrap();
    let text = |rows: &[tiberius::Row]| -> Vec<String> { rows.iter().map(|r| r.get::<&str, _>(0).unwrap().to_lowercase()).collect() };
    assert_eq!(text(&r[0]), ["bob"]);
    assert_eq!(text(&r[1]), ["bob", "ann", "cy"]);
    let kinds: Vec<(i32, i32)> = r[2].iter().map(|r| (r.get::<i32, _>(0).unwrap(), r.get::<i32, _>(1).unwrap())).collect();
    assert_eq!(kinds, [(3, 1), (1, 3), (2, 2)]);
    assert_eq!(r[3].iter().map(|r| r.get::<i32, _>(0).unwrap()).collect::<Vec<_>>(), [1, 2, 3]);

    let r = batch(
        &mut c,
        "SELECT [i].[name], [i].[type_desc], [i].[is_unique], [i].[is_primary_key], [i].[filter_definition] FROM [sys].[indexes] AS [i] \
             WHERE [i].[object_id] = OBJECT_ID(N'Note') ORDER BY [i].[index_id]; \
         SELECT [i].[name], [c].[name], [ic].[key_ordinal], [ic].[is_included_column] FROM [sys].[indexes] AS [i] \
             INNER JOIN [sys].[index_columns] AS [ic] ON [i].[object_id] = [ic].[object_id] AND [i].[index_id] = [ic].[index_id] \
             INNER JOIN [sys].[columns] AS [c] ON [ic].[object_id] = [c].[object_id] AND [ic].[column_id] = [c].[column_id] \
             WHERE [i].[object_id] = OBJECT_ID(N'Note') ORDER BY [i].[index_id], [ic].[key_ordinal]",
    )
    .await
    .unwrap();
    let indexes: Vec<(String, String, bool, bool)> = r[0]
        .iter()
        .map(|r| (r.get::<&str, _>(0).unwrap().to_string(), r.get::<&str, _>(1).unwrap().to_string(), r.get::<bool, _>(2).unwrap(), r.get::<bool, _>(3).unwrap()))
        .collect();
    assert_eq!(
        indexes,
        [
            ("PK_Note".to_string(), "CLUSTERED".to_string(), true, true),
            ("EmailIndex".to_string(), "NONCLUSTERED".to_string(), true, false),
            ("IX_Note_UserId_Kind".to_string(), "NONCLUSTERED".to_string(), false, false),
        ]
    );
    let columns: Vec<(String, String, i32)> =
        r[1].iter().map(|r| (r.get::<&str, _>(0).unwrap().to_string(), r.get::<&str, _>(1).unwrap().to_string(), r.get::<i32, _>(2).unwrap())).collect();
    assert_eq!(
        columns,
        [
            ("PK_Note".to_string(), "Id".to_string(), 1),
            ("EmailIndex".to_string(), "Email".to_string(), 1),
            ("IX_Note_UserId_Kind".to_string(), "UserId".to_string(), 1),
            ("IX_Note_UserId_Kind".to_string(), "Kind".to_string(), 2),
        ]
    );
}

/// A result of many rows with large `nvarchar(max)` and `varbinary(max)`
/// values, several a row, read whole with TLS and without: every value as
/// it was written. The values cross packets, and each `max` value goes out
/// in chunks that end with their packet (`tds::put_plp`): sent as one chunk
/// across packets, Microsoft.Data.SqlClient failed on a `SELECT *` over
/// such a table after a few hundred rows or waited for ever (it is its
/// reader that the chunking is for; this client reads either).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tiberius_large_max_values_stream() {
    let s = start();
    let mut c = connect(&s).await;
    batch(
        &mut c,
        "CREATE TABLE [Big] ([Id] int NOT NULL PRIMARY KEY, [A] nvarchar(max) NULL, [N] int NOT NULL, [B] nvarchar(max) NULL, [Raw] varbinary(max) NULL, [C] nvarchar(max) NOT NULL)",
    )
    .await
    .unwrap();
    const ROWS: i32 = 240;
    // Lengths around a packet's payload and far past it, an empty string, a
    // NULL, and text outside ASCII (two and four bytes of UTF-16 a letter).
    let text = |k: i32, salt: u32| -> String {
        let len = match k % 8 {
            0 => 0,
            1 => 1,
            2 => 3990 + k as usize,
            3 => 4096,
            4 => 9000 + 7 * k as usize,
            5 => 20_001,
            6 => 131,
            _ => 4000 - k as usize,
        };
        (0..len).map(|i| ['a', 'ж', 'é', '😀', 'z'][(i + salt as usize + k as usize) % 5]).collect()
    };
    let raw = |k: i32| -> Option<Vec<u8>> { (k % 5 != 0).then(|| (0..(k as usize * 97) % 11_000).map(|i| (i * 31 + k as usize) as u8).collect()) };
    for chunk in (1..=ROWS).collect::<Vec<_>>().chunks(8) {
        for &k in chunk {
            let a = (k % 7 != 0).then(|| text(k, 1));
            let b = (k % 3 != 0).then(|| text(k + 3, 2));
            c.execute(
                "INSERT INTO [Big] ([Id], [A], [N], [B], [Raw], [C]) VALUES (@P1, @P2, @P3, @P4, @P5, @P6)",
                &[&k, &a, &(k * 3), &b, &raw(k), &text(k + 5, 3)],
            )
            .await
            .unwrap();
        }
    }
    let check = |rows: &[tiberius::Row]| {
        assert_eq!(rows.len(), ROWS as usize);
        for (row, k) in rows.iter().zip(1..=ROWS) {
            assert_eq!(row.get::<i32, _>(0), Some(k));
            assert_eq!(row.get::<&str, _>(1).map(str::to_string), (k % 7 != 0).then(|| text(k, 1)), "A of {k}");
            assert_eq!(row.get::<i32, _>(2), Some(k * 3));
            assert_eq!(row.get::<&str, _>(3).map(str::to_string), (k % 3 != 0).then(|| text(k + 3, 2)), "B of {k}");
            assert_eq!(row.get::<&[u8], _>(4).map(<[u8]>::to_vec), raw(k), "Raw of {k}");
            assert_eq!(row.get::<&str, _>(5), Some(text(k + 5, 3).as_str()), "C of {k}");
        }
    };
    // Under TLS (each packet its own record).
    let r = batch(&mut c, "SELECT * FROM [Big] ORDER BY [Id]").await.unwrap();
    check(&r[0]);
    // And without: the packets back to back on the socket.
    let mut plain = config(&s, PASSWORD, EncryptionLevel::NotSupported);
    plain.database("postgres");
    let tcp = tokio::net::TcpStream::connect(("127.0.0.1", s.port)).await.unwrap();
    tcp.set_nodelay(true).unwrap();
    let mut p = Client::connect(plain, tcp.compat_write()).await.expect("a connection without TLS");
    let r = batch(&mut p, "SELECT * FROM [Big] ORDER BY [Id]").await.unwrap();
    check(&r[0]);
    let r = batch(&mut p, "SELECT [C], [Id] FROM [Big] WHERE [Id] % 8 = 5 ORDER BY [Id] DESC; SELECT COUNT(*) FROM [Big]").await.unwrap();
    assert_eq!(r[0].len(), 30);
    assert!(r[0].iter().all(|row| row.get::<&str, _>(0).is_some_and(|c| c.chars().count() == text(row.get::<i32, _>(1).unwrap() + 5, 3).chars().count())));
    assert_eq!(r[1][0].get::<i32, _>(0), Some(ROWS));
}

/// NULLs sort first ascending and last descending, as SQL Server's (NULL is
/// its least value); the engine put them last ascending. And a sort by a
/// nullable `datetime2(7)` then a uniqueidentifier with TOP took the server
/// down: the GUID's key term shifted the sort keys' positions past the end.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tiberius_null_order_and_mixed_keys() {
    let s = start_with(true);
    let mut c = connect(&s).await;
    batch(
        &mut c,
        "CREATE TABLE [Ord] ([Id] uniqueidentifier NOT NULL PRIMARY KEY, [Created] datetime2 NULL, [N] int NULL); \
         INSERT INTO [Ord] ([Id], [Created], [N]) VALUES \
         ('00000000-0000-0000-0000-000000000003', NULL, 3), ('00000000-0000-0000-0000-000000000002', '2026-01-01 10:00:00.1234567', 2), \
         ('00000000-0000-0000-0000-000000000001', NULL, NULL), ('01000000-0000-0000-0000-000000000000', '2026-01-01 10:00:00.1234561', 1)",
    )
    .await
    .unwrap();
    let ids = |r: &[tiberius::Row]| -> Vec<String> { r.iter().map(|r| r.get::<uuid::Uuid, _>(0).unwrap().to_string()[..8].to_string() + &r.get::<uuid::Uuid, _>(0).unwrap().to_string()[34..]).collect() };
    let r = batch(&mut c, "SELECT TOP(10) [Id], [Created] FROM [Ord] ORDER BY [Created], [Id]").await.unwrap();
    // NULL first (then by Id in SQL Server's order), then the two by their
    // seventh digit.
    assert_eq!(ids(&r[0]), ["0000000001", "0000000003", "0100000000", "0000000002"]);
    let r = batch(&mut c, "SELECT [N] FROM [Ord] ORDER BY [N]; SELECT [N] FROM [Ord] ORDER BY [N] DESC").await.unwrap();
    let ns = |r: &[tiberius::Row]| -> Vec<Option<i32>> { r.iter().map(|r| r.get::<i32, _>(0)).collect() };
    assert_eq!(ns(&r[0]), [None, Some(1), Some(2), Some(3)]);
    assert_eq!(ns(&r[1]), [Some(3), Some(2), Some(1), None]);
    let r = batch(&mut c, "SELECT [N], ROW_NUMBER() OVER (ORDER BY [N]) FROM [Ord] ORDER BY [N]").await.unwrap();
    assert_eq!(r[0][0].get::<i64, _>(1), Some(1));
    assert_eq!(r[0][0].get::<i32, _>(0), None);
}

/// A catalogue lookup for one table (`WHERE TABLE_NAME = @t`, what EF Core
/// and copy tools send per table) builds only that table's rows: every
/// table's were built and filtered, seconds a lookup with a hundred
/// tables. What reads more than one table still sees them all.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tiberius_catalogue_lookup_for_one_table() {
    let s = start_with(true);
    let mut c = connect(&s).await;
    let mut ddl = String::new();
    for k in 0..60 {
        ddl.push_str(&format!("CREATE TABLE [DenevCloud.ERP.T{k}] ([Id] int PRIMARY KEY, [Name] nvarchar(20) NULL, [At] datetime2 NULL); "));
    }
    batch(&mut c, &ddl).await.unwrap();
    let started = std::time::Instant::now();
    for k in 0..60 {
        let r = c
            .query(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = @P1 AND TABLE_NAME = @P2 ORDER BY ORDINAL_POSITION",
                &[&"dbo", &format!("denevcloud.erp.t{k}")],
            )
            .await
            .unwrap()
            .into_first_result()
            .await
            .unwrap();
        assert_eq!(texts(&r).into_iter().flatten().collect::<Vec<_>>(), ["Id", "Name", "At"]);
    }
    let per_lookup = started.elapsed() / 60;
    assert!(per_lookup < std::time::Duration::from_millis(500), "{per_lookup:?} a lookup");
    // An OR, a lookup by another column, and none: every table's rows.
    let r = batch(
        &mut c,
        "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'DenevCloud.ERP.T1' OR TABLE_NAME = N'DenevCloud.ERP.T2'; \
         SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = N'Name'; \
         SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES",
    )
    .await
    .unwrap();
    assert_eq!(int(&r[0]), [6]);
    assert_eq!(int(&r[1]), [60]);
    assert_eq!(int(&r[2]), [60]);
}

/// SUM and AVG of an int column are ints, as SQL Server's (AVG cut toward
/// zero): the engine answered a bigint, which EF Core's GetInt32 refuses,
/// and an exact average (3.5 where SQL Server says 3).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tiberius_int_sum_and_avg() {
    let s = start_with(true);
    let mut c = connect(&s).await;
    batch(
        &mut c,
        "CREATE TABLE [Q] ([Id] int PRIMARY KEY, [G] int NOT NULL, [N] int NOT NULL, [S] smallint NOT NULL, [B] bigint NOT NULL, [D] decimal(10,2) NOT NULL); \
         INSERT INTO [Q] VALUES (1, 1, 5, 5, 5, 1.25), (2, 1, 2, 2, 2, 2.50), (3, 2, -3, -3, -3, 0), (4, 2, -2, -2, -2, 0)",
    )
    .await
    .unwrap();
    let r = batch(
        &mut c,
        "SELECT [G], SUM([N]), AVG([N]), SUM([S]), AVG([S]) FROM [Q] GROUP BY [G] ORDER BY [G]; \
         SELECT SUM([B]), SUM([D]) FROM [Q]",
    )
    .await
    .unwrap();
    let row = |k: usize| -> (i32, i32, i32, i32) {
        let r = &r[0][k];
        (r.get::<i32, _>(1).unwrap(), r.get::<i32, _>(2).unwrap(), r.get::<i32, _>(3).unwrap(), r.get::<i32, _>(4).unwrap())
    };
    assert_eq!(row(0), (7, 3, 7, 3));
    // Toward zero: -2.5 is -2.
    assert_eq!(row(1), (-5, -2, -5, -2));
    // A bigint's SUM stays a bigint, a decimal's a decimal.
    assert_eq!(r[1][0].get::<i64, _>(0), Some(2));
    assert_eq!(r[1][0].get::<rust_decimal::Decimal, _>(1).map(|d| d.to_string()), Some("3.75".to_string()));
}

/// A decimal expression has SQL Server's type, not one its values suggest:
/// EF Core's `COALESCE([WholePrice], 0.0)` over a decimal(19,6) column,
/// sent with typed parameters, came back with scale 1 (0.0 for 0.000000).
/// And `COUNT(DISTINCT x)` is an int: it was refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tiberius_decimal_types_and_count_distinct() {
    let s = start_with(true);
    let mut c = connect(&s).await;
    batch(
        &mut c,
        "CREATE TABLE [Pr] ([Id] int PRIMARY KEY, [ProductId] uniqueidentifier NOT NULL, [PriceType] int NOT NULL, \
         [WholePrice] decimal(19,6) NULL, [Qty] int NOT NULL); \
         INSERT INTO [Pr] VALUES (1, '3e84a183-b6c9-41da-951f-c2d3bb610290', 100, 0, -3), \
         (2, '3e84a183-b6c9-41da-951f-c2d3bb610290', 200, 12.5, 4), (3, '00000000-0000-0000-0000-000000000001', 100, NULL, 2)",
    )
    .await
    .unwrap();
    let product = uuid::Uuid::parse_str("3e84a183-b6c9-41da-951f-c2d3bb610290").unwrap();
    let r = c
        .query(
            "SELECT TOP(1) COALESCE([d].[WholePrice], 0.0), [d].[WholePrice] + 1, [d].[WholePrice] * 2, SUM([d].[WholePrice]) OVER () \
             FROM [Pr] AS [d] WHERE [d].[ProductId] = @P1 AND [d].[PriceType] = @P2",
            &[&product, &100i32],
        )
        .await
        .unwrap()
        .into_first_result()
        .await
        .unwrap();
    let dec = |k: usize| r[0].get::<rust_decimal::Decimal, _>(k).map(|d| d.to_string());
    assert_eq!(dec(0), Some("0.000000".to_string()));
    assert_eq!(dec(1), Some("1.000000".to_string()));
    // SQL Server's product type: p1 + p2 + 1 digits, s1 + s2 places (19,6 * 1,0).
    assert_eq!(dec(2), Some("0.000000".to_string()));
    let r = batch(
        &mut c,
        "SELECT COALESCE([WholePrice], 0.0) FROM [Pr] WHERE [Id] = 3; \
         SELECT COUNT(DISTINCT [ProductId]), COUNT(DISTINCT [PriceType]) AS [Types] FROM [Pr]; \
         SELECT SUM([Qty]), MIN([Qty]) FROM [Pr]",
    )
    .await
    .unwrap();
    assert_eq!(r[0][0].get::<rust_decimal::Decimal, _>(0).map(|d| d.to_string()), Some("0.000000".to_string()));
    assert_eq!(r[1][0].get::<i32, _>(0), Some(2));
    assert_eq!(r[1][0].get::<i32, _>(1), Some(2));
    assert_eq!(r[1][0].columns()[0].name(), "");
    assert_eq!(r[1][0].columns()[1].name(), "Types");
    // A negative literal in VALUES (written `- 3`, refused).
    assert_eq!(r[2][0].get::<i32, _>(0), Some(3));
    assert_eq!(r[2][0].get::<i32, _>(1), Some(-3));
}

/// Rows tied on a first key are ordered by a uniqueidentifier second key in
/// SQL Server's order, also through a LEFT JOIN and with the first key also
/// selected through a CAST (the product page's variant list differed from
/// Azure's in 11 places).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tiberius_guid_tiebreak_through_a_left_join() {
    let s = start_with(true);
    let mut c = connect(&s).await;
    // In SQL Server's order: b < cc < a < d.
    let (a, b, cc, d) = (
        "00000000-0000-0000-0000-000000000001",
        "01000000-0000-0000-0000-000000000000",
        "00000000-0000-0000-0001-000000000000",
        "00000000-0000-0000-0000-010000000000",
    );
    batch(
        &mut c,
        &format!(
            "CREATE TABLE [VT] ([Id] uniqueidentifier NOT NULL PRIMARY KEY, [PublicName] nvarchar(20) NULL); \
             CREATE TABLE [V] ([Id] uniqueidentifier NOT NULL, [VariantTypeId] uniqueidentifier NULL, [ListingOrder] tinyint NOT NULL, \
             [Hidden] bit NOT NULL, [N] int NOT NULL, CONSTRAINT [PK_V] PRIMARY KEY ([Id])); \
             INSERT INTO [VT] VALUES ('{a}', N'Colour'); \
             INSERT INTO [V] VALUES ('{a}', '{a}', 1, 0, 1), ('{b}', NULL, 1, 0, 2), ('{cc}', '{a}', 1, 0, 3), ('{d}', NULL, 0, 0, 4)"
        ),
    )
    .await
    .unwrap();
    let r = batch(
        &mut c,
        "SELECT [d0].[PublicName] AS [TypeName], CAST([d].[ListingOrder] AS int) AS [VariantListingOrder], [d].[N] \
         FROM [V] AS [d] LEFT JOIN [VT] AS [d0] ON [d].[VariantTypeId] = [d0].[Id] \
         WHERE [d].[Hidden] = CAST(0 AS bit) ORDER BY [d].[ListingOrder], [d].[Id]",
    )
    .await
    .unwrap();
    let ns: Vec<i32> = r[0].iter().map(|r| r.get::<i32, _>(2).unwrap()).collect();
    // ListingOrder 0 first (d), then the three tied at 1 by Id: b, cc, a.
    assert_eq!(ns, [4, 2, 3, 1]);
    // With the sort key also selected (`[d].[Id]` in the list): the ORDER BY
    // went out as the output's name, which the join's sort could not trace
    // to the column's order.
    let r = batch(
        &mut c,
        "SELECT CAST([d].[ListingOrder] AS int) AS [VariantListingOrder], [d].[Id], [d].[N] \
         FROM [V] AS [d] LEFT JOIN [VT] AS [d0] ON [d].[VariantTypeId] = [d0].[Id] ORDER BY [d].[ListingOrder], [d].[Id]",
    )
    .await
    .unwrap();
    assert_eq!(r[0].iter().map(|r| r.get::<i32, _>(2).unwrap()).collect::<Vec<_>>(), [4, 2, 3, 1]);
    // The same filtered to the tied rows, as the product page reads them.
    let r = batch(
        &mut c,
        "SELECT [d].[N] FROM [V] AS [d] LEFT JOIN [VT] AS [d0] ON [d].[VariantTypeId] = [d0].[Id] \
         WHERE [d].[ListingOrder] = 1 ORDER BY [d].[ListingOrder], [d].[Id]; \
         SELECT IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'V' AND COLUMN_NAME = N'ListingOrder'",
    )
    .await
    .unwrap();
    assert_eq!(r[0].iter().map(|r| r.get::<i32, _>(0).unwrap()).collect::<Vec<_>>(), [2, 3, 1]);
    // NOT NULL, reported as such (every column but the key read YES).
    assert_eq!(texts(&r[1]), [Some("NO".to_string())]);
    assert!(batch(&mut c, "INSERT INTO [V] ([Id], [ListingOrder], [Hidden], [N]) VALUES (NEWID(), NULL, 0, 9)").await.is_err());
}

/// EF Core's category page: products joined to their category, the first
/// image a subquery in the select list, `TOP(@p)` over an order of both
/// tables. The subquery ran for every product the category has before the
/// TOP kept nine (432 ms against SQL Server's 40); the rows the TOP keeps
/// are the join's to decide, and are taken first. The answer is the one
/// written: the case-insensitive filter on the joined table, the order by a
/// uniqueidentifier, a product with no image, the joined table's
/// `datetime2(7)` to its seventh digit, and the columns' names.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tiberius_top_before_a_subquery_through_a_join() {
    let s = start_with(true);
    let mut c = connect(&s).await;
    let guid = |kind: u8, n: u32| format!("{kind:02x}000000-0000-0000-0000-{n:012x}");
    batch(
        &mut c,
        "CREATE TABLE [Cat] ([Id] uniqueidentifier PRIMARY KEY, [Slang] nvarchar(50) NOT NULL, [Made] datetime2(7) NOT NULL); \
         CREATE TABLE [Prod] ([Id] uniqueidentifier PRIMARY KEY, [Name] nvarchar(50) NOT NULL, [CategoryId] uniqueidentifier NULL, \
             [Status] int NOT NULL, [ListingOrder] int NOT NULL, [Price] decimal(18,2) NULL); \
         CREATE TABLE [Img] ([Id] int PRIMARY KEY, [ProductId] uniqueidentifier NOT NULL, [Url] nvarchar(100) NOT NULL, [ListingOrder] int NOT NULL); \
         CREATE INDEX [IX_Img_Product] ON [Img] ([ProductId])",
    )
    .await
    .unwrap();
    batch(
        &mut c,
        &format!(
            "INSERT INTO [Cat] VALUES ('{}', N'phone-cases', '2026-01-02 03:04:05.1234567'), ('{}', N'other', '2026-01-02 03:04:05.7654321')",
            guid(0xc0, 1),
            guid(0xc0, 2)
        ),
    )
    .await
    .unwrap();
    // Forty products: one in four in the other category, one in ten in
    // none, one in seven not on sale. Two images for two products of three,
    // the second listed first.
    let category = |i: u32| if i % 10 == 0 { None } else { Some(if i % 4 == 0 { 2 } else { 1 }) };
    let on_sale = |i: u32| i % 7 != 0;
    let listing = |i: u32| i % 5;
    let products: Vec<String> = (1..=40u32)
        .map(|i| {
            let cat = category(i).map_or("NULL".to_string(), |c| format!("'{}'", guid(0xc0, c)));
            let price = if i % 9 == 0 { "NULL".to_string() } else { format!("{i}.50") };
            format!("('{}', N'p{i}', {cat}, {}, {}, {price})", guid(0xa0, i), u8::from(on_sale(i)), listing(i))
        })
        .collect();
    batch(&mut c, &format!("INSERT INTO [Prod] VALUES {}", products.join(", "))).await.unwrap();
    let images: Vec<String> = (1..=40u32)
        .filter(|i| i % 3 != 0)
        .flat_map(|i| [format!("({}, '{}', N'i{i}a', 2)", i * 2, guid(0xa0, i)), format!("({}, '{}', N'i{i}b', 1)", i * 2 + 1, guid(0xa0, i))])
        .collect();
    batch(&mut c, &format!("INSERT INTO [Img] VALUES {}", images.join(", "))).await.unwrap();

    let mut kept: Vec<u32> = (1..=40).filter(|i| category(*i) == Some(1) && on_sale(*i)).collect();
    kept.sort_by_key(|i| (std::cmp::Reverse(listing(*i)), *i));
    assert!(kept.len() > 9);
    let sql = "SELECT TOP(@P3) [d].[Name], ROUND(COALESCE([d].[Price], 0.0), 2), \
               (SELECT TOP(1) [d1].[Url] FROM [Img] AS [d1] WHERE [d].[Id] = [d1].[ProductId] ORDER BY [d1].[ListingOrder]), \
               [d].[Id], [d0].[Id], [d0].[Made], [d0].[Slang] AS [Category] \
               FROM [Prod] AS [d] LEFT JOIN [Cat] AS [d0] ON [d].[CategoryId] = [d0].[Id] \
               WHERE [d].[Status] = @P1 AND [d0].[Slang] = @P2 \
               ORDER BY [d].[ListingOrder] DESC, [d].[Id], [d0].[Id]";
    for top in [9i32, 3, 100] {
        let r = c.query(sql, &[&1i32, &"PHONE-cases", &top]).await.unwrap().into_first_result().await.unwrap();
        let names: Vec<String> = r[0].columns().iter().map(|c| c.name().to_string()).collect();
        assert_eq!(names[0], "Name");
        assert_eq!(names[3..], ["Id", "Id", "Made", "Category"]);
        let got: Vec<_> = r
            .iter()
            .map(|r| {
                use chrono::Timelike;
                let made = r.get::<chrono::NaiveDateTime, _>(5).unwrap();
                (
                    r.get::<&str, _>(0).unwrap().to_string(),
                    r.get::<&str, _>(2).map(str::to_string),
                    r.get::<uuid::Uuid, _>(3).unwrap().to_string(),
                    r.get::<uuid::Uuid, _>(4).unwrap().to_string(),
                    format!("{}.{:07}", made.format("%Y-%m-%d %H:%M:%S"), made.nanosecond() / 100),
                    r.get::<&str, _>(6).unwrap().to_string(),
                )
            })
            .collect();
        let want: Vec<_> = kept
            .iter()
            .take(top as usize)
            .map(|i| {
                (
                    format!("p{i}"),
                    (i % 3 != 0).then(|| format!("i{i}b")),
                    guid(0xa0, *i),
                    guid(0xc0, 1),
                    "2026-01-02 03:04:05.1234567".to_string(),
                    "phone-cases".to_string(),
                )
            })
            .collect();
        assert_eq!(got, want, "TOP {top}");
    }
}

/// SQL Server's `ROUND` rounds the value and keeps its type: over a
/// `decimal(19,6)` the answer is still one (7.99 comes back 7.990000),
/// where PostgreSQL's `round(x, 2)` is a numeric of two places, and the
/// application read a price of another scale than SQL Server sent. A float
/// stays a float (the statement was refused: PostgreSQL has no
/// `round(double precision, integer)`), an integer an integer. Half away
/// from zero; a third argument that is not 0 truncates.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tiberius_round_keeps_the_type() {
    let s = start();
    let mut c = connect(&s).await;
    batch(
        &mut c,
        "CREATE TABLE [Rnd] ([Id] int PRIMARY KEY, [Price] decimal(19,6) NULL, [P2] decimal(18,2) NULL, [F] float NULL, [N] int NULL, [B] bigint NULL); \
         INSERT INTO [Rnd] VALUES (1, 7.99, 7.99, 7.987, 12345, 9876543210), (2, NULL, NULL, NULL, NULL, NULL), \
             (3, 1.234567, 1.23, -2.5, -12355, -15), (4, 2.5, 0.05, 0.125, 7, 5)",
    )
    .await
    .unwrap();
    let dec = |r: &tiberius::Row, i: usize| r.get::<tiberius::numeric::Numeric, _>(i).map(|n| (n.value(), n.scale()));
    // Decimals: the column's scale whatever the places, NULL kept, and a
    // COALESCE under it (EF Core's `ROUND(COALESCE([d].[Price], 0.0), 2)`).
    let r = batch(
        &mut c,
        "SELECT ROUND([Price], 2), ROUND([Price], 0), ROUND([Price], -1), ROUND([P2], 1), ROUND([P2], 4), ROUND([Price], 2, 1), \
             ROUND(COALESCE([Price], 0.0), 2) FROM [Rnd] ORDER BY [Id]",
    )
    .await
    .unwrap();
    let got: Vec<Vec<Option<(i128, u8)>>> = r[0].iter().map(|r| (0..7).map(|i| dec(r, i)).collect()).collect();
    let six = |units: i128| Some((units, 6u8));
    let two = |units: i128| Some((units, 2u8));
    assert_eq!(
        got,
        [
            vec![six(7_990_000), six(8_000_000), six(10_000_000), two(800), two(799), six(7_990_000), six(7_990_000)],
            vec![None, None, None, None, None, None, six(0)],
            vec![six(1_230_000), six(1_000_000), six(0), two(120), two(123), six(1_230_000), six(1_230_000)],
            vec![six(2_500_000), six(3_000_000), six(0), two(10), two(5), six(2_500_000), six(2_500_000)],
        ]
    );
    // Floats and integers, each in its own type.
    let r = batch(
        &mut c,
        "SELECT ROUND([F], 2), ROUND([F], 0), ROUND([F], 1, 1), ROUND([N], -2), ROUND([N], 1), ROUND([B], -1) FROM [Rnd] ORDER BY [Id]",
    )
    .await
    .unwrap();
    let got: Vec<_> = r[0]
        .iter()
        .map(|r| (r.get::<f64, _>(0), r.get::<f64, _>(1), r.get::<f64, _>(2), r.get::<i32, _>(3), r.get::<i32, _>(4), r.get::<i64, _>(5)))
        .collect();
    assert_eq!(
        got,
        [
            (Some(7.99), Some(8.0), Some(7.9), Some(12300), Some(12345), Some(9_876_543_210)),
            (None, None, None, None, None, None),
            (Some(-2.5), Some(-3.0), Some(-2.5), Some(-12400), Some(-12355), Some(-20)),
            (Some(0.13), Some(0.0), Some(0.1), Some(0), Some(7), Some(10)),
        ]
    );
    // Through a join, in its WHERE and its ORDER BY, over a computed value.
    let r = batch(
        &mut c,
        "SELECT [a].[Id], ROUND([a].[Price] * 2, 2), ROUND([b].[P2], 0) FROM [Rnd] AS [a] INNER JOIN [Rnd] AS [b] ON [a].[Id] = [b].[Id] \
         WHERE ROUND([a].[P2], 1) <> 1.2 ORDER BY ROUND([b].[Price], 0) DESC, [a].[Id]",
    )
    .await
    .unwrap();
    let got: Vec<_> = r[0].iter().map(|r| (r.get::<i32, _>(0).unwrap(), dec(r, 1), dec(r, 2))).collect();
    assert_eq!(got, [(1, six(15_980_000), two(800)), (4, six(5_000_000), two(0))]);
}

/// What EF Core's SQL Server provider writes for a byte constant and for a
/// list: `CAST(1 AS tinyint)` ("cast to 'utinyint' is not supported yet")
/// and `OPENJSON(@list)` ("'OPENJSON' is not supported").
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tiberius_tinyint_cast_and_openjson() {
    let s = start_with(true);
    let mut c = connect(&s).await;
    batch(
        &mut c,
        "CREATE TABLE [State] ([Id] tinyint PRIMARY KEY, [Watermark] bigint NOT NULL); \
         INSERT INTO [State] VALUES (0, 10), (1, 11), (200, 12), (255, 13); \
         CREATE TABLE [Product] ([ProductId] uniqueidentifier PRIMARY KEY, [N] int NOT NULL, [Name] nvarchar(20) NULL); \
         INSERT INTO [Product] VALUES ('00000000-0000-0000-0000-000000000001', 1, N'one'), \
           ('00000000-0000-0000-0000-000000000002', 2, N'two'), ('00000000-0000-0000-0000-000000000003', 3, NULL)",
    )
    .await
    .unwrap();

    // A byte compared with a constant, as EF Core writes it.
    let r = batch(&mut c, "SELECT TOP(2) [d].[Id], [d].[Watermark] FROM [dbo].[State] AS [d] WHERE [d].[Id] = CAST(1 AS tinyint)").await.unwrap();
    assert_eq!(r[0].len(), 1);
    assert_eq!(r[0][0].get::<u8, _>(0), Some(1));
    assert_eq!(r[0][0].get::<i64, _>(1), Some(11));
    let r = batch(&mut c, "SELECT [d].[Watermark] FROM [State] AS [d] WHERE [d].[Id] >= CAST(200 AS tinyint) ORDER BY [d].[Id]").await.unwrap();
    assert_eq!(r[0].iter().map(|r| r.get::<i64, _>(0).unwrap()).collect::<Vec<_>>(), [12, 13]);
    // The cast's own value and type, at both ends of the range.
    let r = batch(&mut c, "SELECT CAST(0 AS tinyint), CAST(255 AS tinyint), CAST(N'7' AS tinyint), CAST(1.6 AS tinyint), CAST(NULL AS tinyint)").await.unwrap();
    let got: Vec<Option<u8>> = (0..5).map(|k| r[0][0].get::<u8, _>(k)).collect();
    assert_eq!(got, [Some(0), Some(255), Some(7), Some(2), None]);
    // Past it: an overflow, never the low byte.
    for sql in ["SELECT CAST(256 AS tinyint)", "SELECT CAST(-1 AS tinyint)", "SELECT CAST(N'300' AS tinyint)", "SELECT [Id] FROM [State] WHERE [Watermark] = CAST(999 AS tinyint)"] {
        let e = batch(&mut c, sql).await.expect_err(sql);
        assert_eq!(number(&e), Some(8115), "{sql}: {e}");
    }

    // `ids.Contains(p.ProductId)`: the list as a JSON array parameter.
    let contains = "SELECT [d].[N] FROM [Product] AS [d] WHERE [d].[ProductId] IN (SELECT [p].[value] FROM OPENJSON(@P1) WITH ([value] uniqueidentifier '$') AS [p]) ORDER BY [d].[N]";
    let ns = |rows: Vec<tiberius::Row>| rows.iter().map(|r| r.get::<i32, _>(0).unwrap()).collect::<Vec<_>>();
    let list = r#"["00000000-0000-0000-0000-000000000003","00000000-0000-0000-0000-000000000001","00000000-0000-0000-0000-00000000000f"]"#;
    let r = c.query(contains, &[&list]).await.unwrap().into_first_result().await.unwrap();
    assert_eq!(ns(r), [1, 3]);
    // Another list through the same text: nothing of the first is kept.
    let r = c.query(contains, &[&r#"["00000000-0000-0000-0000-000000000002"]"#]).await.unwrap().into_first_result().await.unwrap();
    assert_eq!(ns(r), [2]);
    // An empty list holds nothing, and everything is outside it; so is a NULL one.
    let r = c.query(contains, &[&"[]"]).await.unwrap().into_first_result().await.unwrap();
    assert_eq!(ns(r), [] as [i32; 0]);
    let outside = "SELECT [d].[N] FROM [Product] AS [d] WHERE [d].[N] NOT IN (SELECT [p].[value] FROM OPENJSON(@P1) WITH ([value] int '$') AS [p]) ORDER BY [d].[N]";
    let r = c.query(outside, &[&"[]"]).await.unwrap().into_first_result().await.unwrap();
    assert_eq!(ns(r), [1, 2, 3]);
    let r = c.query(outside, &[&Option::<&str>::None]).await.unwrap().into_first_result().await.unwrap();
    assert_eq!(ns(r), [1, 2, 3]);
    let r = c.query(outside, &[&"[2, 9]"]).await.unwrap().into_first_result().await.unwrap();
    assert_eq!(ns(r), [1, 3]);
    // A NULL in the list: nothing is known to be outside it.
    let r = c.query(outside, &[&"[2, null]"]).await.unwrap().into_first_result().await.unwrap();
    assert_eq!(ns(r), [] as [i32; 0]);
    // Strings, with a quote, and an integer list beside another parameter.
    let r = c
        .query(
            "SELECT [d].[N] FROM [Product] AS [d] WHERE [d].[Name] IN (SELECT [n].[value] FROM OPENJSON(@P1) WITH ([value] nvarchar(20) '$') AS [n]) AND [d].[N] > @P2 ORDER BY [d].[N]",
            &[&r#"["one","two","it's"]"#, &1i32],
        )
        .await
        .unwrap()
        .into_first_result()
        .await
        .unwrap();
    assert_eq!(ns(r), [2]);

    // The relation itself: joined, filtered and counted.
    let r = c
        .query(
            "SELECT [d].[N], [p].[value] FROM [Product] AS [d] INNER JOIN OPENJSON(@P1) WITH ([value] int '$') AS [p] ON [d].[N] = [p].[value] ORDER BY [d].[N]",
            &[&"[3, 1, 7]"],
        )
        .await
        .unwrap()
        .into_first_result()
        .await
        .unwrap();
    assert_eq!(r.iter().map(|r| (r.get::<i32, _>(0).unwrap(), r.get::<i32, _>(1).unwrap())).collect::<Vec<_>>(), [(1, 1), (3, 3)]);
    let r = c.query("SELECT COUNT(*) FROM OPENJSON(@P1) WITH ([value] int '$') AS [p]", &[&"[]"]).await.unwrap().into_first_result().await.unwrap();
    assert_eq!(r[0].get::<i32, _>(0), Some(0));
    let r = c
        .query("SELECT [d].[N] FROM [Product] AS [d] WHERE EXISTS (SELECT 1 FROM OPENJSON(@P1) WITH ([value] int '$') AS [p] WHERE [p].[value] = [d].[N]) ORDER BY [d].[N]", &[&"[2, 3]"])
        .await
        .unwrap()
        .into_first_result()
        .await
        .unwrap();
    assert_eq!(ns(r), [2, 3]);

    // The bare form: key, value and type of each element, or of each member.
    let r = c
        .query("SELECT [p].[key], [p].[value], [p].[type] FROM OPENJSON(@P1) AS [p] ORDER BY CAST([p].[key] AS int)", &[&r#"["a", 2.5, null, true, [1], {"k": 1}]"#])
        .await
        .unwrap()
        .into_first_result()
        .await
        .unwrap();
    let got: Vec<(String, Option<String>, i32)> = r
        .iter()
        .map(|r| (r.get::<&str, _>(0).unwrap().to_string(), r.get::<&str, _>(1).map(str::to_string), r.get::<i32, _>(2).unwrap()))
        .collect();
    let row = |k: &str, v: Option<&str>, t: i32| (k.to_string(), v.map(str::to_string), t);
    assert_eq!(
        got,
        [row("0", Some("a"), 1), row("1", Some("2.5"), 2), row("2", None, 0), row("3", Some("true"), 3), row("4", Some("[1]"), 4), row("5", Some(r#"{"k":1}"#), 5)]
    );
    let r = batch(&mut c, r#"SELECT [key], [value] FROM OPENJSON(N'{"b": 1, "a": "x"}') ORDER BY [key]"#).await.unwrap();
    assert_eq!(texts(&r[0]), [Some("a".to_string()), Some("b".to_string())]);
    // The bare form as EF Core joins it, in the list's order.
    let r = c
        .query(
            "SELECT [d].[N] FROM OPENJSON(@P1) AS [i] INNER JOIN [Product] AS [d] ON CAST([i].[value] AS int) = [d].[N] ORDER BY CAST([i].[key] AS int)",
            &[&"[3, 1]"],
        )
        .await
        .unwrap()
        .into_first_result()
        .await
        .unwrap();
    assert_eq!(ns(r), [3, 1]);

    // Objects by column: a default path, a given one, a missing member.
    let r = batch(
        &mut c,
        r#"DECLARE @j nvarchar(max) = N'[{"Id": 2, "Tag": {"Name": "x"}}, {"Id": 5}]';
           SELECT o.[Id], o.[Name] FROM OPENJSON(@j) WITH ([Id] int, [Name] nvarchar(10) '$.Tag.Name') AS o ORDER BY o.[Id]"#,
    )
    .await
    .unwrap();
    let got: Vec<(i32, Option<String>)> = r[0].iter().map(|r| (r.get::<i32, _>(0).unwrap(), r.get::<&str, _>(1).map(str::to_string))).collect();
    assert_eq!(got, [(2, Some("x".to_string())), (5, None)]);

    // Not JSON, and JSON read from a row: said, not answered.
    let e = c.query("SELECT [value] FROM OPENJSON(@P1)", &[&"[1,"]).await.err().expect("malformed JSON");
    assert_eq!(number(&e), Some(13609), "{e}");
    let e = batch(&mut c, "SELECT o.[value] FROM [Product] AS [d] CROSS APPLY OPENJSON([d].[Name]) AS o").await.unwrap_err();
    assert_eq!(number(&e), Some(40000), "{e}");
}
