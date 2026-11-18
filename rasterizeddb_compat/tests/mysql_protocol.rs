//! The MySQL protocol, driven by sqlx's MySQL driver: TLS and
//! caching_sha2_password (full, then fast), the text protocol, prepared
//! statements with typed binds and the binary row format, transactions with
//! MySQL's statement atomicity, autocommit off, upserts, insert ids, errors
//! by MySQL number, and the same rows seen over PostgreSQL at the same time.

use rasterizeddb_core::{
    core::database::catalog::DatabaseCatalog,
    mysql::{MyAuth, MyConfig, MyTls},
    pgwire::{AuthConfig, ConnectionLimits, TlsConfig, TlsMode, start_pgwire_on_listener},
};
use rclite::Arc;
use sqlx::{Connection, Executor, Row};

struct Servers {
    mysql_port: u16,
    pg_port: u16,
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
    }
}

const PASSWORD: &str = "s3cret-pw";

/// One in-memory catalogue served over MySQL and PostgreSQL at once.
fn start(auth: MyAuth) -> Servers {
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
                let config = MyConfig {
                    tcp: Some(("127.0.0.1".into(), 0)),
                    socket: None,
                    auth,
                    tls: MyTls::Prefer,
                    cert_path: None,
                    key_path: None,
                    bootstrap_user: "rasterized".into(),
                    bootstrap_password: Some(PASSWORD.into()),
                    limits: ConnectionLimits::default(),
                };
                let listener = rasterizeddb_core::mysql::bind(config).await.unwrap();
                let mysql_port = listener.local_addr().unwrap().port();
                let server = rasterizeddb_core::mysql::serve(listener, catalog.clone()).await.unwrap();
                let pg = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let pg_port = pg.local_addr().unwrap().port();
                ports_tx.send((mysql_port, pg_port)).unwrap();
                let pg_auth = AuthConfig {
                    method: rasterizeddb_core::auth::AuthMethod::ScramSha256,
                    bootstrap_user: "rasterized".into(),
                    bootstrap_password: Some(PASSWORD.into()),
                };
                let tls = TlsConfig { mode: TlsMode::Disable, cert_path: None, key_path: None };
                tokio::select! {
                    _ = start_pgwire_on_listener(catalog, pg, tls, pg_auth, ConnectionLimits::default()) => {}
                    _ = shutdown_rx => {}
                }
                server.drain(std::time::Duration::from_secs(2)).await;
            });
            rt.shutdown_timeout(std::time::Duration::from_secs(2));
        })
        .unwrap();
    let (mysql_port, pg_port) = ports_rx.recv().unwrap();
    Servers {
        mysql_port,
        pg_port,
        shutdown: Some(shutdown_tx),
        handle: Some(handle),
    }
}

fn mysql_url(s: &Servers, db: &str, ssl: &str) -> String {
    format!(
        "mysql://rasterized:{PASSWORD}@127.0.0.1:{}/{db}?ssl-mode={ssl}",
        s.mysql_port
    )
}

async fn mysql(s: &Servers) -> sqlx::MySqlConnection {
    sqlx::MySqlConnection::connect(&mysql_url(s, "postgres", "required"))
        .await
        .expect("mysql connect")
}

fn code(e: &sqlx::Error) -> (Option<u16>, Option<String>) {
    match e {
        sqlx::Error::Database(d) => {
            let my = d.try_downcast_ref::<sqlx::mysql::MySqlDatabaseError>();
            (my.map(|m| m.number()), d.code().map(|c| c.into_owned()))
        }
        _ => (None, None),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_driver_end_to_end() {
    let s = start(MyAuth::Password);

    // First login: full authentication over TLS. Second: the fast path.
    let mut c = mysql(&s).await;
    let mut c2 = mysql(&s).await;

    let v: String = sqlx::query_scalar("SELECT @@version_comment")
        .fetch_one(&mut c)
        .await
        .unwrap();
    assert!(v.contains("RasterizedDB"), "{v}");
    let db: String = sqlx::query_scalar("SELECT DATABASE()")
        .fetch_one(&mut c2)
        .await
        .unwrap();
    assert_eq!(db, "postgres");

    c.execute(
        "CREATE TABLE `items` (
            `id` BIGINT NOT NULL AUTO_INCREMENT,
            `Name` VARCHAR(64) NOT NULL,
            `qty` INT NOT NULL DEFAULT 0,
            `price` DECIMAL(10,2),
            `ok` BOOLEAN NOT NULL DEFAULT TRUE,
            `seen` DATETIME(6),
            `blob` BLOB,
            `ratio` DOUBLE,
            PRIMARY KEY (`id`),
            UNIQUE KEY `uq_name` (`Name`)
        ) ENGINE=InnoDB",
    )
    .await
    .unwrap();

    // A prepared insert with typed binds; the auto-increment id comes back
    // in the OK packet.
    let seen = chrono::NaiveDate::from_ymd_opt(2024, 5, 6)
        .unwrap()
        .and_hms_micro_opt(7, 8, 9, 123456)
        .unwrap();
    let r = sqlx::query("INSERT INTO items (Name, qty, price, ok, seen, `blob`, ratio) VALUES (?, ?, ?, ?, ?, ?, ?)")
        .bind("widget")
        .bind(3i32)
        .bind(rust_decimal::Decimal::new(1999, 2))
        .bind(false)
        .bind(seen)
        .bind(vec![0u8, 1, 255])
        .bind(0.25f64)
        .execute(&mut c)
        .await
        .unwrap();
    assert_eq!(r.rows_affected(), 1);
    assert_eq!(r.last_insert_id(), 1);
    let r = sqlx::query("INSERT INTO items (Name) VALUES (?), (?)")
        .bind("gadget")
        .bind("gizmo")
        .execute(&mut c)
        .await
        .unwrap();
    assert_eq!(r.rows_affected(), 2);
    assert_eq!(r.last_insert_id(), 2, "the first id of a multi-row insert");

    // Binary rows decode to the right Rust types.
    let row =
        sqlx::query("SELECT id, Name, qty, price, ok, seen, `blob`, ratio FROM items WHERE id = ?")
            .bind(1i64)
            .fetch_one(&mut c)
            .await
            .unwrap();
    assert_eq!(row.get::<i64, _>("id"), 1);
    assert_eq!(row.get::<String, _>("Name"), "widget");
    assert_eq!(row.get::<i32, _>("qty"), 3);
    assert_eq!(
        row.get::<rust_decimal::Decimal, _>("price"),
        rust_decimal::Decimal::new(1999, 2)
    );
    assert!(!row.get::<bool, _>("ok"));
    assert_eq!(row.get::<chrono::NaiveDateTime, _>("seen"), seen);
    assert_eq!(row.get::<Vec<u8>, _>("blob"), vec![0u8, 1, 255]);
    assert_eq!(row.get::<f64, _>("ratio"), 0.25);

    // NULLs and the text protocol.
    let rows = c
        .fetch_all("SELECT id, price, `blob` FROM items ORDER BY id")
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);
    assert!(
        rows[1]
            .try_get::<Option<rust_decimal::Decimal>, _>("price")
            .unwrap()
            .is_none()
    );

    // Upsert on the unique key, and INSERT IGNORE.
    sqlx::query("INSERT INTO items (Name, qty) VALUES (?, ?) ON DUPLICATE KEY UPDATE qty = qty + VALUES(qty)")
        .bind("widget")
        .bind(10)
        .execute(&mut c)
        .await
        .unwrap();
    let qty: i32 = sqlx::query_scalar("SELECT qty FROM items WHERE Name = 'widget'")
        .fetch_one(&mut c)
        .await
        .unwrap();
    assert_eq!(qty, 13);
    let r = c
        .execute("INSERT IGNORE INTO items (Name) VALUES ('widget')")
        .await
        .unwrap();
    assert_eq!(r.rows_affected(), 0);

    // A duplicate is ER_DUP_ENTRY, SQLSTATE 23000.
    let e = c
        .execute("INSERT INTO items (Name) VALUES ('gadget')")
        .await
        .unwrap_err();
    assert_eq!(code(&e), (Some(1062), Some("23000".into())), "{e}");
    let e = c.execute("SELECT nope FROM items").await.unwrap_err();
    assert_eq!(code(&e).0, Some(1054), "{e}");
    let e = c
        .execute("SELECT nope AS Nope FROM items")
        .await
        .unwrap_err();
    assert_eq!(code(&e).0, Some(1054), "{e}");
    let e = c.execute("SELECT * FROM missing_table").await.unwrap_err();
    assert_eq!(code(&e).0, Some(1146), "{e}");

    // In a transaction, a failed statement fails alone (MySQL), and the
    // rest commits.
    {
        let mut tx = c.begin().await.unwrap();
        tx.execute("INSERT INTO items (Name) VALUES ('t1')")
            .await
            .unwrap();
        assert!(
            tx.execute("INSERT INTO items (Name) VALUES ('gadget')")
                .await
                .is_err()
        );
        tx.execute("INSERT INTO items (Name) VALUES ('t2')")
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }
    let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM items WHERE Name IN ('t1', 't2')")
        .fetch_one(&mut c)
        .await
        .unwrap();
    assert_eq!(n, 2);
    {
        let mut tx = c.begin().await.unwrap();
        tx.execute("DELETE FROM items WHERE Name = 't1'")
            .await
            .unwrap();
        tx.rollback().await.unwrap();
    }
    let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM items WHERE Name = 't1'")
        .fetch_one(&mut c)
        .await
        .unwrap();
    assert_eq!(n, 1, "the rolled-back delete is undone");

    // autocommit = 0: work is held until COMMIT, and ROLLBACK drops it.
    c2.execute("SET autocommit = 0").await.unwrap();
    c2.execute("INSERT INTO items (Name) VALUES ('held')")
        .await
        .unwrap();
    let seen_elsewhere: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM items WHERE Name = 'held'")
        .fetch_one(&mut c)
        .await
        .unwrap();
    assert_eq!(
        seen_elsewhere, 0,
        "uncommitted work is not visible to another connection"
    );
    c2.execute("ROLLBACK").await.unwrap();
    c2.execute("SET autocommit = 1").await.unwrap();
    let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM items WHERE Name = 'held'")
        .fetch_one(&mut c2)
        .await
        .unwrap();
    assert_eq!(n, 0);

    // MySQL's division, LIMIT offset, and functions.
    let row = c
        .fetch_one("SELECT 7 / 2 AS q, 7 DIV 2 AS d, IFNULL(NULL, 'x') AS f, CONCAT('a', 1) AS c")
        .await
        .unwrap();
    assert_eq!(
        row.get::<rust_decimal::Decimal, _>("q"),
        rust_decimal::Decimal::new(35, 1)
    );
    assert_eq!(row.get::<i64, _>("d"), 3);
    assert_eq!(row.get::<String, _>("f"), "x");
    let names: Vec<String> = sqlx::query_scalar("SELECT Name FROM items ORDER BY id LIMIT 1, 2")
        .fetch_all(&mut c)
        .await
        .unwrap();
    assert_eq!(names, vec!["gadget".to_string(), "gizmo".to_string()]);

    // SHOW and DESCRIBE.
    let tables: Vec<String> = c
        .fetch_all("SHOW TABLES")
        .await
        .unwrap()
        .iter()
        .map(|r| r.get::<String, _>(0))
        .collect();
    assert_eq!(tables, vec!["items".to_string()]);
    let fields = c.fetch_all("DESCRIBE items").await.unwrap();
    assert_eq!(fields[0].get::<String, _>("Field"), "id");
    assert_eq!(fields[0].get::<String, _>("Key"), "PRI");
    assert_eq!(fields[0].get::<String, _>("Extra"), "auto_increment");

    // The same rows over PostgreSQL, at the same time.
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
    pg.execute("INSERT INTO items (name) VALUES ('from-pg')", &[])
        .await
        .unwrap();
    let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM items WHERE Name = 'from-pg'")
        .fetch_one(&mut c)
        .await
        .unwrap();
    assert_eq!(n, 1);

    // Databases: CREATE DATABASE, then a connection to it, then USE.
    c.execute("CREATE DATABASE IF NOT EXISTS shop")
        .await
        .unwrap();
    let mut c3 = sqlx::MySqlConnection::connect(&mysql_url(&s, "shop", "required"))
        .await
        .unwrap();
    c3.execute("CREATE TABLE t (a INT)").await.unwrap();
    c.execute("USE shop").await.unwrap();
    let db: String = sqlx::query_scalar("SELECT DATABASE()")
        .fetch_one(&mut c)
        .await
        .unwrap();
    assert_eq!(db, "shop");

    c.close().await.unwrap();
    c2.close().await.unwrap();
    c3.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_refuses_a_wrong_password_and_plain_first_login() {
    let s = start(MyAuth::Password);
    let wrong = format!(
        "mysql://rasterized:nope@127.0.0.1:{}/postgres?ssl-mode=required",
        s.mysql_port
    );
    let e = sqlx::MySqlConnection::connect(&wrong).await.unwrap_err();
    assert_eq!(code(&e).0, Some(1045), "{e}");
    // Without TLS, a first login has no way to send its password safely.
    let e = sqlx::MySqlConnection::connect(&mysql_url(&s, "postgres", "disabled"))
        .await
        .unwrap_err();
    assert!(
        e.to_string().to_lowercase().contains("secure")
            || e.to_string().contains("RSA")
            || code(&e).0.is_some(),
        "{e}"
    );
    // An unknown database.
    let e = sqlx::MySqlConnection::connect(&mysql_url(&s, "nosuch", "required"))
        .await
        .unwrap_err();
    assert_eq!(code(&e).0, Some(1049), "{e}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_prepared_session_statements_and_catalogue() {
    let s = start(MyAuth::Trust);
    let mut c = sqlx::MySqlConnection::connect(&format!(
        "mysql://anyone@127.0.0.1:{}/postgres?ssl-mode=disabled",
        s.mysql_port
    ))
    .await
    .unwrap();
    c.execute("CREATE TABLE people (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(20), born DATE, t TIME)").await.unwrap();
    sqlx::query("INSERT INTO people (name, born, t) VALUES (?, ?, ?)")
        .bind("ada")
        .bind(chrono::NaiveDate::from_ymd_opt(1815, 12, 10).unwrap())
        .bind(chrono::NaiveTime::from_hms_opt(13, 14, 15).unwrap())
        .execute(&mut c)
        .await
        .unwrap();
    // Statements the engine does not prepare (SHOW, SET) still prepare, and
    // answer in the binary row format.
    let rows = sqlx::query("SHOW COLUMNS FROM people")
        .fetch_all(&mut c)
        .await
        .unwrap();
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[1].get::<String, _>("Field"), "name");
    sqlx::query("SET @who = ?")
        .bind("ada")
        .execute(&mut c)
        .await
        .unwrap();
    let name: String = sqlx::query_scalar("SELECT name FROM people WHERE name = @who")
        .fetch_one(&mut c)
        .await
        .unwrap();
    assert_eq!(name, "ada");
    // Dates and times round-trip through binds and binary rows.
    let row = sqlx::query("SELECT born, t FROM people WHERE id = ?")
        .bind(1)
        .fetch_one(&mut c)
        .await
        .unwrap();
    assert_eq!(
        row.get::<chrono::NaiveDate, _>("born"),
        chrono::NaiveDate::from_ymd_opt(1815, 12, 10).unwrap()
    );
    assert_eq!(
        row.get::<chrono::NaiveTime, _>("t"),
        chrono::NaiveTime::from_hms_opt(13, 14, 15).unwrap()
    );
    // MySQL's information_schema calls the database TABLE_SCHEMA.
    let found: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
    )
    .bind("postgres")
    .bind("people")
    .fetch_one(&mut c)
    .await
    .unwrap();
    assert_eq!(found, 1);
    // A query cancelled with KILL QUERY from another connection.
    let id: i64 = sqlx::query_scalar("SELECT CONNECTION_ID()")
        .fetch_one(&mut c)
        .await
        .unwrap();
    let mut killer = sqlx::MySqlConnection::connect(&format!(
        "mysql://anyone@127.0.0.1:{}/postgres?ssl-mode=disabled",
        s.mysql_port
    ))
    .await
    .unwrap();
    let kill = format!("KILL QUERY {id}");
    killer.execute(sqlx::AssertSqlSafe(kill)).await.unwrap();
    c.close().await.unwrap();
}

/// Point-select latency over MySQL against the same over PostgreSQL, one
/// connection each: what the translation costs. `--ignored --nocapture`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn mysql_point_select_overhead() {
    let s = start(MyAuth::Trust);
    let mut my = sqlx::MySqlConnection::connect(&format!(
        "mysql://rasterized@127.0.0.1:{}/postgres?ssl-mode=disabled",
        s.mysql_port
    ))
    .await
    .unwrap();
    let mut pg = sqlx::PgConnection::connect(&format!(
        "postgres://rasterized:{PASSWORD}@127.0.0.1:{}/postgres?sslmode=disable",
        s.pg_port
    ))
    .await
    .unwrap();
    my.execute("CREATE TABLE bench (id INT PRIMARY KEY, v VARCHAR(40))")
        .await
        .unwrap();
    for start in (1..=10_000).step_by(1000) {
        let values: Vec<String> = (start..start + 1000)
            .map(|i| format!("({i}, 'value {i}')"))
            .collect();
        my.execute(sqlx::AssertSqlSafe(format!(
            "INSERT INTO bench VALUES {}",
            values.join(",")
        )))
        .await
        .unwrap();
    }
    const N: usize = 5000;
    for round in 0..2 {
        let t = std::time::Instant::now();
        for i in 0..N {
            let _: String = sqlx::query_scalar("SELECT v FROM bench WHERE id = $1")
                .bind((i % 10_000 + 1) as i32)
                .fetch_one(&mut pg)
                .await
                .unwrap();
        }
        let pg_us = t.elapsed().as_micros() as f64 / N as f64;
        let t = std::time::Instant::now();
        for i in 0..N {
            let _: String = sqlx::query_scalar("SELECT v FROM bench WHERE id = ?")
                .bind((i % 10_000 + 1) as i32)
                .fetch_one(&mut my)
                .await
                .unwrap();
        }
        let my_prep_us = t.elapsed().as_micros() as f64 / N as f64;
        let t = std::time::Instant::now();
        for i in 0..N {
            let q = format!("SELECT v FROM bench WHERE id = {}", i % 10_000 + 1);
            let _ = my.fetch_one(sqlx::AssertSqlSafe(q)).await.unwrap();
        }
        let my_text_us = t.elapsed().as_micros() as f64 / N as f64;
        let t = std::time::Instant::now();
        for i in 0..N {
            let q = format!("SELECT v FROM bench WHERE id = {}", i % 10_000 + 1);
            let _ = pg
                .fetch_one(sqlx::raw_sql(sqlx::AssertSqlSafe(q)))
                .await
                .unwrap();
        }
        let pg_text_us = t.elapsed().as_micros() as f64 / N as f64;
        println!(
            "round {round}: pgwire prepared {pg_us:.1} us, simple {pg_text_us:.1} us; mysql prepared {my_prep_us:.1} us, text {my_text_us:.1} us"
        );
    }
}

async fn changed(c: &mut sqlx::MySqlConnection, id: i32) -> String {
    sqlx::query_scalar::<_, String>("SELECT CAST(changed AS CHAR) FROM posts WHERE id = ?")
        .bind(id)
        .fetch_one(c)
        .await
        .unwrap()
}

/// TEMPORARY tables and ON UPDATE CURRENT_TIMESTAMP, which the engine does
/// not have: kept by the MySQL front end.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_temporary_tables_and_on_update() {
    let s = start(MyAuth::Password);
    let mut c = mysql(&s).await;
    let mut other = mysql(&s).await;

    // A temporary table hides a table of its name, for this connection
    // only, and is not committed or rolled back with a transaction.
    c.execute("CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(10))").await.unwrap();
    c.execute("INSERT INTO t VALUES (1, 'real')").await.unwrap();
    c.execute("START TRANSACTION").await.unwrap();
    c.execute("INSERT INTO t VALUES (2, 'real2')").await.unwrap();
    c.execute("CREATE TEMPORARY TABLE t (id INT, v VARCHAR(10))").await.unwrap();
    c.execute("INSERT INTO t VALUES (7, 'temp')").await.unwrap();
    let v: String = sqlx::query_scalar("SELECT v FROM t WHERE id = 7").fetch_one(&mut c).await.unwrap();
    assert_eq!(v, "temp");
    c.execute("COMMIT").await.unwrap();
    let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM t").fetch_one(&mut other).await.unwrap();
    assert_eq!(n, 2, "the other connection sees the real table, with the committed row");
    let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM t AS x JOIN t2 ON 1 = 0")
        .fetch_one(&mut c)
        .await
        .map(|_: i64| 0)
        .unwrap_or(-1);
    assert_eq!(n, -1);
    let names: Vec<String> = sqlx::query_scalar::<_, String>("SHOW TABLES").fetch_all(&mut c).await.unwrap();
    assert_eq!(names, ["t"], "{names:?}");
    c.execute("DROP TEMPORARY TABLE t").await.unwrap();
    let v: String = sqlx::query_scalar("SELECT v FROM t WHERE id = 1").fetch_one(&mut c).await.unwrap();
    assert_eq!(v, "real");
    let e = c.execute("DROP TEMPORARY TABLE t").await.unwrap_err();
    assert_eq!(code(&e).0, Some(1051), "{e}");
    c.execute("DROP TEMPORARY TABLE IF EXISTS t").await.unwrap();
    // Gone when the connection closes.
    c.execute("CREATE TEMPORARY TABLE scratch (a INT)").await.unwrap();
    drop(c);
    let mut c = mysql(&s).await;
    assert!(c.execute("SELECT * FROM scratch").await.is_err());

    // ON UPDATE CURRENT_TIMESTAMP: set when an UPDATE changes the row, not
    // when it leaves it as it was or sets the column itself.
    c.execute(
        "CREATE TABLE posts (id INT PRIMARY KEY, title VARCHAR(20),
            changed DATETIME(6) DEFAULT '2000-01-01 00:00:00' ON UPDATE CURRENT_TIMESTAMP(6))",
    )
    .await
    .unwrap();
    c.execute("INSERT INTO posts (id, title) VALUES (1, 'a'), (2, 'b')").await.unwrap();

    c.execute("UPDATE posts SET title = 'a' WHERE id = 1").await.unwrap();
    assert!(changed(&mut c, 1).await.starts_with("2000-01-01"), "an unchanged row keeps it");
    c.execute("UPDATE posts SET title = 'A' WHERE id = 1").await.unwrap();
    assert!(!changed(&mut c, 1).await.starts_with("2000"), "a changed row gets the time");
    assert!(changed(&mut c, 2).await.starts_with("2000-01-01"), "other rows keep it");
    c.execute("UPDATE posts SET title = 'B', changed = '2010-05-05 00:00:00' WHERE id = 2").await.unwrap();
    assert!(changed(&mut c, 2).await.starts_with("2010-05-05"), "set explicitly wins");
    c.execute("INSERT INTO posts (id, title) VALUES (2, 'B2') ON DUPLICATE KEY UPDATE title = VALUES(title)").await.unwrap();
    assert!(!changed(&mut c, 2).await.starts_with("2010"), "an upsert that changes the row sets it");
    // MODIFY without the attribute removes it.
    c.execute("ALTER TABLE posts MODIFY changed DATETIME(6) DEFAULT '2000-01-01 00:00:00'").await.unwrap();
    c.execute("UPDATE posts SET changed = '2001-01-01 00:00:00'").await.unwrap();
    c.execute("UPDATE posts SET title = 'Z' WHERE id = 1").await.unwrap();
    assert!(changed(&mut c, 1).await.starts_with("2001-01-01"), "no longer set");
    // A table made through the attribute by RENAME keeps it.
    c.execute("ALTER TABLE posts MODIFY changed DATETIME(6) NULL ON UPDATE CURRENT_TIMESTAMP(6)").await.unwrap();
    c.execute("RENAME TABLE posts TO articles").await.unwrap();
    c.execute("UPDATE articles SET title = 'Y' WHERE id = 1").await.unwrap();
    let v: Option<String> = sqlx::query_scalar("SELECT CAST(changed AS CHAR) FROM articles WHERE id = 1").fetch_one(&mut c).await.unwrap();
    assert!(v.is_some_and(|v| !v.starts_with("2001")), "renamed table keeps the attribute");
    let names: Vec<String> = sqlx::query_scalar::<_, String>("SHOW TABLES").fetch_all(&mut c).await.unwrap();
    assert!(!names.iter().any(|n| n.starts_with("__")), "{names:?}");
}
