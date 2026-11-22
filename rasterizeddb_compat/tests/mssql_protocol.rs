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
