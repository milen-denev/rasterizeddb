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
    // A trust login runs as the role it names, which must exist (roles plan
    // R0, the same rule as pgwire's and the native protocol's): the
    // bootstrap superuser here.
    let mut c = sqlx::MySqlConnection::connect(&format!(
        "mysql://rasterized@127.0.0.1:{}/postgres?ssl-mode=disabled",
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
    // KILL QUERY of another connection's statement: as that role or a
    // superuser.
    let mut killer = sqlx::MySqlConnection::connect(&format!(
        "mysql://rasterized@127.0.0.1:{}/postgres?ssl-mode=disabled",
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
    c.execute("CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(10))")
        .await
        .unwrap();
    c.execute("INSERT INTO t VALUES (1, 'real')").await.unwrap();
    c.execute("START TRANSACTION").await.unwrap();
    c.execute("INSERT INTO t VALUES (2, 'real2')")
        .await
        .unwrap();
    c.execute("CREATE TEMPORARY TABLE t (id INT, v VARCHAR(10))")
        .await
        .unwrap();
    c.execute("INSERT INTO t VALUES (7, 'temp')").await.unwrap();
    let v: String = sqlx::query_scalar("SELECT v FROM t WHERE id = 7")
        .fetch_one(&mut c)
        .await
        .unwrap();
    assert_eq!(v, "temp");
    c.execute("COMMIT").await.unwrap();
    let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM t")
        .fetch_one(&mut other)
        .await
        .unwrap();
    assert_eq!(
        n, 2,
        "the other connection sees the real table, with the committed row"
    );
    let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM t AS x JOIN t2 ON 1 = 0")
        .fetch_one(&mut c)
        .await
        .map(|_: i64| 0)
        .unwrap_or(-1);
    assert_eq!(n, -1);
    let names: Vec<String> = sqlx::query_scalar::<_, String>("SHOW TABLES")
        .fetch_all(&mut c)
        .await
        .unwrap();
    assert_eq!(names, ["t"], "{names:?}");
    c.execute("DROP TEMPORARY TABLE t").await.unwrap();
    let v: String = sqlx::query_scalar("SELECT v FROM t WHERE id = 1")
        .fetch_one(&mut c)
        .await
        .unwrap();
    assert_eq!(v, "real");
    let e = c.execute("DROP TEMPORARY TABLE t").await.unwrap_err();
    assert_eq!(code(&e).0, Some(1051), "{e}");
    c.execute("DROP TEMPORARY TABLE IF EXISTS t").await.unwrap();
    // Gone when the connection closes.
    c.execute("CREATE TEMPORARY TABLE scratch (a INT)")
        .await
        .unwrap();
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
    c.execute("INSERT INTO posts (id, title) VALUES (1, 'a'), (2, 'b')")
        .await
        .unwrap();

    c.execute("UPDATE posts SET title = 'a' WHERE id = 1")
        .await
        .unwrap();
    assert!(
        changed(&mut c, 1).await.starts_with("2000-01-01"),
        "an unchanged row keeps it"
    );
    c.execute("UPDATE posts SET title = 'A' WHERE id = 1")
        .await
        .unwrap();
    assert!(
        !changed(&mut c, 1).await.starts_with("2000"),
        "a changed row gets the time"
    );
    assert!(
        changed(&mut c, 2).await.starts_with("2000-01-01"),
        "other rows keep it"
    );
    c.execute("UPDATE posts SET title = 'B', changed = '2010-05-05 00:00:00' WHERE id = 2")
        .await
        .unwrap();
    assert!(
        changed(&mut c, 2).await.starts_with("2010-05-05"),
        "set explicitly wins"
    );
    c.execute("INSERT INTO posts (id, title) VALUES (2, 'B2') ON DUPLICATE KEY UPDATE title = VALUES(title)").await.unwrap();
    assert!(
        !changed(&mut c, 2).await.starts_with("2010"),
        "an upsert that changes the row sets it"
    );
    // MODIFY without the attribute removes it.
    c.execute("ALTER TABLE posts MODIFY changed DATETIME(6) DEFAULT '2000-01-01 00:00:00'")
        .await
        .unwrap();
    c.execute("UPDATE posts SET changed = '2001-01-01 00:00:00'")
        .await
        .unwrap();
    c.execute("UPDATE posts SET title = 'Z' WHERE id = 1")
        .await
        .unwrap();
    assert!(
        changed(&mut c, 1).await.starts_with("2001-01-01"),
        "no longer set"
    );
    // A table made through the attribute by RENAME keeps it.
    c.execute("ALTER TABLE posts MODIFY changed DATETIME(6) NULL ON UPDATE CURRENT_TIMESTAMP(6)")
        .await
        .unwrap();
    c.execute("RENAME TABLE posts TO articles").await.unwrap();
    c.execute("UPDATE articles SET title = 'Y' WHERE id = 1")
        .await
        .unwrap();
    let v: Option<String> =
        sqlx::query_scalar("SELECT CAST(changed AS CHAR) FROM articles WHERE id = 1")
            .fetch_one(&mut c)
            .await
            .unwrap();
    assert!(
        v.is_some_and(|v| !v.starts_with("2001")),
        "renamed table keeps the attribute"
    );
    let names: Vec<String> = sqlx::query_scalar::<_, String>("SHOW TABLES")
        .fetch_all(&mut c)
        .await
        .unwrap();
    assert!(!names.iter().any(|n| n.starts_with("__")), "{names:?}");
}

/// Stored procedures in MySQL's language, run by the front end: parameters
/// (IN, OUT into user variables, INOUT), variables, IF/CASE/loops with
/// labels, a cursor loop with a NOT FOUND handler, an EXIT handler, SIGNAL,
/// result sets from the body, and the catalogue statements.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_stored_procedures() {
    let s = start(MyAuth::Password);
    let mut c = mysql(&s).await;
    c.execute("CREATE TABLE acct (id INT PRIMARY KEY, owner VARCHAR(20), balance INT)")
        .await
        .unwrap();
    c.execute("INSERT INTO acct VALUES (1, 'ann', 100), (2, 'bo', 50), (3, 'cy', 0)")
        .await
        .unwrap();

    // One query, as a client sends it after DELIMITER: the body's `;`s stay in it.
    c.execute(
        "CREATE DEFINER=`root`@`%` PROCEDURE transfer(IN p_from INT, IN p_to INT, IN p_amount INT, OUT p_status VARCHAR(20))
         MODIFIES SQL DATA
         BEGIN
           DECLARE have INT DEFAULT 0;
           DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; SET p_status = 'failed'; END;
           SELECT balance INTO have FROM acct WHERE id = p_from;
           IF have < p_amount THEN
             SET p_status = 'insufficient';
           ELSE
             START TRANSACTION;
             UPDATE acct SET balance = balance - p_amount WHERE id = p_from;
             UPDATE acct SET balance = balance + p_amount WHERE id = p_to;
             COMMIT;
             SET p_status = 'ok';
           END IF;
         END",
    )
    .await
    .unwrap();
    c.execute("CALL transfer(1, 2, 30, @st)").await.unwrap();
    let st: String = sqlx::query_scalar("SELECT @st")
        .fetch_one(&mut c)
        .await
        .unwrap();
    assert_eq!(st, "ok");
    c.execute("CALL transfer(3, 1, 10, @st)").await.unwrap();
    let st: String = sqlx::query_scalar("SELECT @st")
        .fetch_one(&mut c)
        .await
        .unwrap();
    assert_eq!(st, "insufficient");
    let balances: Vec<i32> = sqlx::query_scalar("SELECT balance FROM acct ORDER BY id")
        .fetch_all(&mut c)
        .await
        .unwrap();
    assert_eq!(balances, [70, 80, 0]);

    // A cursor loop with a CONTINUE handler, INOUT, a label, CASE, and a
    // result set from the body.
    c.execute(
        "CREATE PROCEDURE summarize(INOUT p_total INT)
         BEGIN
           DECLARE done INT DEFAULT FALSE;
           DECLARE v INT;
           DECLARE n INT DEFAULT 0;
           DECLARE cur CURSOR FOR SELECT balance FROM acct ORDER BY id;
           DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
           OPEN cur;
           read_loop: LOOP
             FETCH cur INTO v;
             IF done THEN LEAVE read_loop; END IF;
             SET n = n + 1;
             CASE WHEN v > 75 THEN SET p_total = p_total + v * 2; ELSE SET p_total = p_total + v; END CASE;
           END LOOP;
           CLOSE cur;
           SELECT n AS accounts, p_total AS total;
         END",
    )
    .await
    .unwrap();
    c.execute("SET @t = 1000").await.unwrap();
    let row = sqlx::query("CALL summarize(@t)")
        .fetch_one(&mut c)
        .await
        .unwrap();
    assert_eq!(row.get::<i64, _>("accounts"), 3);
    assert_eq!(row.get::<i64, _>("total"), 1000 + 70 + 160 + 0);
    let t: i64 = sqlx::query_scalar("SELECT @t")
        .fetch_one(&mut c)
        .await
        .unwrap();
    assert_eq!(t, 1230);

    // SIGNAL, an unknown procedure, a wrong argument count.
    c.execute("CREATE PROCEDURE guard(IN x INT) BEGIN IF x < 0 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'negative'; END IF; SELECT x; END").await.unwrap();
    let e = c.execute("CALL guard(-1)").await.unwrap_err();
    assert_eq!(code(&e), (Some(1644), Some("45000".into())), "{e}");
    assert!(e.to_string().contains("negative"), "{e}");
    let e = c.execute("CALL nothere()").await.unwrap_err();
    assert_eq!(code(&e).0, Some(1305), "{e}");
    let e = c.execute("CALL guard(1, 2)").await.unwrap_err();
    assert_eq!(code(&e).0, Some(1318), "{e}");
    // The connection is in step after the errors.
    let one: i32 = sqlx::query_scalar("SELECT 1")
        .fetch_one(&mut c)
        .await
        .unwrap();
    assert_eq!(one, 1);

    // Catalogue: SHOW PROCEDURE STATUS, SHOW CREATE PROCEDURE, DROP.
    let names: Vec<String> = sqlx::query("SHOW PROCEDURE STATUS")
        .fetch_all(&mut c)
        .await
        .unwrap()
        .iter()
        .map(|r| r.get::<String, _>("Name"))
        .collect();
    assert_eq!(names, ["guard", "summarize", "transfer"]);
    let row = sqlx::query("SHOW CREATE PROCEDURE guard")
        .fetch_one(&mut c)
        .await
        .unwrap();
    assert!(
        row.get::<String, _>("Create Procedure")
            .contains("SIGNAL SQLSTATE '45000'")
    );
    let e = c
        .execute("CREATE PROCEDURE guard() SELECT 1")
        .await
        .unwrap_err();
    assert_eq!(code(&e).0, Some(1304), "{e}");
    c.execute("DROP PROCEDURE guard").await.unwrap();
    c.execute("DROP PROCEDURE IF EXISTS guard").await.unwrap();
    let e = c.execute("DROP PROCEDURE guard").await.unwrap_err();
    assert_eq!(code(&e).0, Some(1305), "{e}");
    let e = c
        .execute("CREATE FUNCTION f() RETURNS INT RETURN 1")
        .await
        .unwrap_err();
    assert_eq!(code(&e).0, Some(1235), "{e}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_full_text_search() {
    let s = start(MyAuth::Password);
    let mut c = mysql(&s).await;
    c.execute(
        "CREATE TABLE articles (id INT PRIMARY KEY, title VARCHAR(200), body TEXT, FULLTEXT KEY ft (title, body))",
    )
    .await
    .unwrap();
    c.execute(
        "INSERT INTO articles VALUES
         (1, 'MySQL Tutorial', 'DBMS stands for DataBase ...'),
         (2, 'How To Use MySQL Well', 'After you went through a ...'),
         (3, 'Optimizing MySQL', 'In this tutorial, we show ...'),
         (4, '1001 MySQL Tricks', '1. Never run mysqld as root. 2. ...'),
         (5, 'MySQL vs. YourSQL', 'In the following database comparison ...'),
         (6, 'MySQL Security', 'When configured properly, MySQL ...'),
         (7, NULL, 'a tutorial without a title')",
    )
    .await
    .unwrap();
    let ids = |rows: Vec<sqlx::mysql::MySqlRow>| rows.iter().map(|r| r.get::<i32, _>(0)).collect::<Vec<_>>();

    // Natural language: any word, case-insensitive.
    let rows = sqlx::query("SELECT id FROM articles WHERE MATCH (title, body) AGAINST ('DATABASE') ORDER BY id")
        .fetch_all(&mut c)
        .await
        .unwrap();
    assert_eq!(ids(rows), vec![1, 5]);
    let rows = sqlx::query("SELECT id FROM articles WHERE MATCH (title, body) AGAINST ('tutorial security' IN NATURAL LANGUAGE MODE) ORDER BY id")
        .fetch_all(&mut c)
        .await
        .unwrap();
    assert_eq!(ids(rows), vec![1, 3, 6, 7]);
    // Words InnoDB never indexes find nothing: a stop word, a short word.
    let rows = sqlx::query("SELECT id FROM articles WHERE MATCH (title, body) AGAINST ('the') OR MATCH (title, body) AGAINST ('as')")
        .fetch_all(&mut c)
        .await
        .unwrap();
    assert!(rows.is_empty());

    // Boolean mode.
    let rows = sqlx::query("SELECT id FROM articles WHERE MATCH (title, body) AGAINST ('+MySQL -YourSQL' IN BOOLEAN MODE) ORDER BY id")
        .fetch_all(&mut c)
        .await
        .unwrap();
    assert_eq!(ids(rows), vec![1, 2, 3, 4, 6]);
    let rows = sqlx::query("SELECT id FROM articles WHERE MATCH (title, body) AGAINST ('tutor*' IN BOOLEAN MODE) ORDER BY id")
        .fetch_all(&mut c)
        .await
        .unwrap();
    assert_eq!(ids(rows), vec![1, 3, 7]);
    let rows = sqlx::query("SELECT id FROM articles WHERE MATCH (title, body) AGAINST ('\"database comparison\"' IN BOOLEAN MODE)")
        .fetch_all(&mut c)
        .await
        .unwrap();
    assert_eq!(ids(rows), vec![5]);

    // Relevance: a number, 0 where the row does not match, ordered by.
    let rows = sqlx::query(
        "SELECT id, MATCH (title, body) AGAINST ('security tutorial') AS score FROM articles ORDER BY score DESC, id",
    )
    .fetch_all(&mut c)
    .await
    .unwrap();
    let scores: Vec<(i32, f64)> = rows.iter().map(|r| (r.get(0), r.get(1))).collect();
    assert!(scores[..4].iter().all(|(_, s)| *s > 0.0), "{scores:?}");
    assert!(scores[4..].iter().all(|(_, s)| *s == 0.0), "{scores:?}");
    let rows = sqlx::query("SELECT id FROM articles WHERE MATCH (title, body) AGAINST ('tutorial') > 0 ORDER BY id")
        .fetch_all(&mut c)
        .await
        .unwrap();
    assert_eq!(ids(rows), vec![1, 3, 7]);

    // A search string bound at execute.
    let rows = sqlx::query("SELECT id FROM articles WHERE MATCH (title, body) AGAINST (? IN BOOLEAN MODE) ORDER BY id")
        .bind("+root +mysqld")
        .fetch_all(&mut c)
        .await
        .unwrap();
    assert_eq!(ids(rows), vec![4]);

    // The other two ways to declare one, and dropping one.
    c.execute("CREATE TABLE notes (id INT PRIMARY KEY, txt TEXT)").await.unwrap();
    c.execute("INSERT INTO notes VALUES (1, 'alpha beta'), (2, 'gamma delta')").await.unwrap();
    c.execute("ALTER TABLE notes ADD FULLTEXT INDEX ft_txt (txt)").await.unwrap();
    c.execute("CREATE FULLTEXT INDEX ft_txt2 ON notes (txt)").await.unwrap();
    let rows = sqlx::query("SELECT id FROM notes WHERE MATCH (txt) AGAINST ('delta')").fetch_all(&mut c).await.unwrap();
    assert_eq!(ids(rows), vec![2]);
    c.execute("DROP INDEX ft_txt2 ON notes").await.unwrap();

    // Refused rather than answered differently.
    for sql in [
        "SELECT id FROM articles WHERE MATCH (title, body) AGAINST ('database' WITH QUERY EXPANSION)",
        "CREATE TABLE ng (id INT PRIMARY KEY, t TEXT, FULLTEXT KEY f (t) WITH PARSER ngram)",
    ] {
        let e = c.execute(sql).await.expect_err(sql);
        assert_eq!(code(&e).0, Some(1235), "{sql}: {e}");
    }
}

/// A column's case-insensitive collation is the engine's; a table's default
/// is not applied (schemas loaded before keep their answers).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_column_collations() {
    let s = start(MyAuth::Password);
    let mut c = mysql(&s).await;
    c.execute(
        "CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(100) COLLATE utf8mb4_0900_ai_ci UNIQUE, \
         login VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin, nick VARCHAR(20)) \
         ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
    )
    .await
    .unwrap();
    c.execute("INSERT INTO users VALUES (1, 'Ann@Example.com', 'Ann', 'Annie'), (2, 'bo@x.org', 'bo', 'Bo')")
        .await
        .unwrap();
    let id: i32 = sqlx::query_scalar("SELECT id FROM users WHERE email = ?")
        .bind("ANN@example.COM")
        .fetch_one(&mut c)
        .await
        .unwrap();
    assert_eq!(id, 1);
    // Accents too, under ai_ci.
    let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM users WHERE email = 'bó@x.org'").fetch_one(&mut c).await.unwrap();
    assert_eq!(n, 1);
    // `_bin` and the table default: the bytes.
    let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM users WHERE login = 'ann' OR nick = 'annie'")
        .fetch_one(&mut c)
        .await
        .unwrap();
    assert_eq!(n, 0);
    let e = c.execute("INSERT INTO users VALUES (3, 'BO@X.ORG', 'x', 'y')").await.expect_err("duplicate");
    assert_eq!(code(&e).0, Some(1062), "{e}");
    let rows = sqlx::query("SHOW FULL COLUMNS FROM users").fetch_all(&mut c).await.unwrap();
    let collations: Vec<Option<String>> = rows.iter().map(|r| r.get::<Option<String>, _>(2)).collect();
    assert_eq!(
        collations,
        [None, Some("utf8mb4_0900_ai_ci".into()), Some("utf8mb4_bin".into()), Some("utf8mb4_bin".into())]
    );
    let rows = sqlx::query("SHOW CREATE TABLE users").fetch_all(&mut c).await.unwrap();
    let ddl: String = rows[0].get(1);
    assert!(ddl.contains("`email` varchar(100) COLLATE utf8mb4_0900_ai_ci"), "{ddl}");
    assert!(ddl.contains("UNIQUE KEY"), "{ddl}");
    assert!(!ddl.contains("__ci"), "{ddl}");
}

/// MySQL sums, averages and orders booleans — its BOOLEAN is TINYINT(1),
/// and `SUM(active)` is how it counts — though PostgreSQL has none of these
/// aggregates and refuses them on its own protocol (42883). A statement the
/// MySQL front end sends keeps them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_aggregates_over_a_boolean_still_answer() {
    let s = start(MyAuth::Trust);
    let mut c = sqlx::MySqlConnection::connect(&format!(
        "mysql://anyone@127.0.0.1:{}/postgres?ssl-mode=disabled",
        s.mysql_port
    ))
    .await
    .unwrap();
    c.execute("CREATE TABLE flags (id INT PRIMARY KEY, active TINYINT(1), done BOOLEAN)").await.unwrap();
    c.execute("INSERT INTO flags VALUES (1, 1, TRUE), (2, 0, TRUE), (3, 1, FALSE)").await.unwrap();
    let row = sqlx::query(
        "SELECT CAST(SUM(active) AS SIGNED) AS a, CAST(SUM(done) AS SIGNED) AS d, \
         CAST(MAX(active) AS SIGNED) AS m, CAST(MIN(done) AS SIGNED) AS n FROM flags",
    )
    .fetch_one(&mut c)
    .await
    .unwrap();
    assert_eq!(
        (row.get::<i64, _>("a"), row.get::<i64, _>("d"), row.get::<i64, _>("m"), row.get::<i64, _>("n")),
        (2, 2, 1, 0)
    );
}
