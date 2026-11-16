//! The SDK, the admin calls, the ORM and the migrator against a real server.
//!
//! Needs `RDB_SERVER_BIN`, as `live.rs` does; without it each test says so and
//! passes. The ORM parts need the `derive` feature (on by default).
#![cfg(feature = "derive")]

use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use rasterizeddb_client::admin::{Index, IndexMethod, Placement, Role, Store};
use rasterizeddb_client::orm::{AutoMigrate, Migrator, StepState, SyncPolicy};
use rasterizeddb_client::prelude::*;
use rasterizeddb_client::tag;
use tokio::process::{Child, Command};

struct Server {
    child: Child,
    dir: PathBuf,
    socket: String,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

async fn server(name: &str) -> Option<Server> {
    let Ok(bin) = std::env::var("RDB_SERVER_BIN") else {
        eprintln!("RDB_SERVER_BIN is not set: skipping {name}");
        return None;
    };
    let dir = std::env::temp_dir().join(format!("rdb_sdk_{name}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("db")).unwrap();
    let socket = dir.join("s.sock");
    let child = Command::new(bin)
        .arg("--location")
        .arg(dir.join("db"))
        .arg("--rdb-socket")
        .arg(&socket)
        .args([
            "--rdb-tcp",
            "off",
            "--rdb-socket-auth",
            "trust",
            "--socket-mode",
            "600",
        ])
        .args(["--log-level", "warn"])
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .expect("start the server");
    let server = Server {
        child,
        dir,
        socket: format!("unix:{}", socket.display()),
    };
    for _ in 0..600 {
        if std::os::unix::net::UnixStream::connect(&socket).is_ok() {
            return Some(server);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("the server did not open {} within 60 s", socket.display());
}

fn options() -> Options {
    Options {
        timeout: Some(Duration::from_secs(30)),
        ..Options::default()
    }
}

async fn connect(server: &Server) -> Client {
    Client::connect(&server.socket, options())
        .await
        .expect("connect")
}

fn bounded<F: std::future::Future<Output = ()>>(f: F) -> impl std::future::Future<Output = ()> {
    async move {
        tokio::time::timeout(Duration::from_secs(300), f)
            .await
            .expect("the test did not finish within 300 s")
    }
}

// ------------------------------------------------------------------- types

#[tokio::test(flavor = "multi_thread")]
async fn every_type_round_trips() {
    bounded(async {
        let Some(server) = server("types").await else {
            return;
        };
        let c = connect(&server).await;
        c.run(
            "CREATE TABLE ty (id BIGINT NOT NULL, f4 REAL, f8 DOUBLE PRECISION, n NUMERIC(10,2), \
             nu NUMERIC, u UUID, ts TIMESTAMP, tz TIMESTAMPTZ, d DATE, t TIME, iv INTERVAL, \
             j JSONB, a BIGINT[], ta TEXT[], ub UBIGINT, b BOOLEAN, by BYTEA, PRIMARY KEY (id))",
        )
        .await
        .unwrap();

        let uuid: Uuid = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11".parse().unwrap();
        let ts: Timestamp = "2026-11-16 12:34:56.789012".parse().unwrap();
        let date: Date = "2026-02-28".parse().unwrap();
        let time: Time = "23:59:59.5".parse().unwrap();
        let iv = Interval::new(14, -3, 4_005_000_006);
        let n = Numeric::new("-12.34").unwrap();
        let nu = Numeric::new("0.000001").unwrap();
        let json = Json::new(r#"{"k": [1, "it's"]}"#);
        let ints = vec![Some(1i64), None, Some(-3)];
        let texts = vec![
            "a b".to_string(),
            "q\"z,{}".to_string(),
            "NULL".to_string(),
            "it's\\".to_string(),
        ];
        let bytes: Vec<u8> = (0..=255).collect();
        c.exec(
            "INSERT INTO ty (id, f4, f8, n, nu, u, ts, tz, d, t, iv, j, a, ta, ub, b, by) VALUES \
             ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)",
            params![
                1i64,
                0.1f32,
                -1e-300f64,
                n,
                nu,
                uuid,
                ts,
                ts,
                date,
                time,
                iv,
                json,
                ints,
                texts,
                u64::MAX,
                true,
                bytes
            ],
        )
        .await
        .unwrap();
        c.exec(
            "INSERT INTO ty (id, f8) VALUES ($1, $2)",
            params![2i64, f64::NAN],
        )
        .await
        .unwrap();
        c.exec(
            "INSERT INTO ty (id, f8) VALUES ($1, $2)",
            params![3i64, f64::NEG_INFINITY],
        )
        .await
        .unwrap();

        // Read directly: the types with their own tags.
        let row: (
            i64,
            f32,
            f64,
            Uuid,
            Timestamp,
            Timestamp,
            Date,
            Time,
            Interval,
            u64,
            bool,
            Vec<u8>,
        ) = c
            .fetch_one(
                "SELECT id, f4, f8, u, ts, tz, d, t, iv, ub, b, by FROM ty WHERE id = $1",
                params![1i64],
            )
            .await
            .unwrap();
        assert_eq!(
            row,
            (
                1,
                0.1,
                -1e-300,
                uuid,
                ts,
                ts,
                date,
                time,
                iv,
                u64::MAX,
                true,
                bytes.clone()
            )
        );

        // NUMERIC(10,2) and JSONB and arrays need ::text; read directly they
        // are errors that say so, never wrong values.
        let rows = c.run("SELECT n, nu, j FROM ty WHERE id = 1").await.unwrap();
        let r = rows.get(0).unwrap();
        let e = r.get::<Numeric>(0).unwrap_err().to_string();
        assert!(e.contains("::text"), "{e}");
        assert_eq!(
            r.get::<Numeric>(1).unwrap(),
            nu,
            "unconstrained NUMERIC carries its scale"
        );
        assert!(
            r.get::<Json>(2).is_err(),
            "a stored JSONB document is not JSON text"
        );

        let row: (Numeric, Numeric, Json, Vec<Option<i64>>, Vec<String>) = c
            .fetch_one(
                "SELECT n::text, nu::text, j::text, a::text, ta::text FROM ty WHERE id = 1",
                params![],
            )
            .await
            .unwrap();
        assert_eq!(row.0, n);
        assert_eq!(row.1, nu);
        assert!(row.2.as_str().contains("it's"), "{:?}", row.2);
        assert_eq!(row.3, ints);
        assert_eq!(row.4, texts);
        // The array read directly arrives in its binary form, which decodes too.
        let direct: Vec<Option<i64>> = c
            .fetch_scalar("SELECT a FROM ty WHERE id = 1", params![])
            .await
            .unwrap();
        assert_eq!(direct, ints);

        assert!(
            c.fetch_scalar::<f64>("SELECT f8 FROM ty WHERE id = 2", params![])
                .await
                .unwrap()
                .is_nan()
        );
        assert_eq!(
            c.fetch_scalar::<f64>("SELECT f8 FROM ty WHERE id = 3", params![])
                .await
                .unwrap(),
            f64::NEG_INFINITY
        );

        // Typed values compare against their columns.
        for (sql, value) in [
            ("SELECT id FROM ty WHERE u = $1", &uuid as &dyn ToSql),
            ("SELECT id FROM ty WHERE tz = $1", &ts),
            ("SELECT id FROM ty WHERE d = $1", &date),
            ("SELECT id FROM ty WHERE t = $1", &time),
            ("SELECT id FROM ty WHERE iv = $1", &iv),
            ("SELECT id FROM ty WHERE n = $1", &n),
            ("SELECT id FROM ty WHERE f4 = $1", &0.1f32),
        ] {
            let ids: Vec<(i64,)> = c.fetch_all(sql, &[value]).await.unwrap();
            assert_eq!(ids, vec![(1,)], "{sql}");
        }
        let ids: Vec<(i64,)> = c
            .fetch_all(
                "SELECT id FROM ty WHERE id = ANY($1) ORDER BY id",
                params![vec![1i64, 3]],
            )
            .await
            .unwrap();
        assert_eq!(ids, vec![(1,), (3,)]);

        // Aggregates: AVG carries its scale; SUM over BIGINT does not.
        let avg: Numeric = c
            .fetch_scalar("SELECT AVG(id) FROM ty", params![])
            .await
            .unwrap();
        assert!(avg.as_str().starts_with("2"), "{avg:?}");
        assert!(
            c.fetch_scalar::<Numeric>("SELECT SUM(id) FROM ty", params![])
                .await
                .is_err()
        );
        assert_eq!(
            c.fetch_scalar::<i64>("SELECT SUM(id)::bigint FROM ty", params![])
                .await
                .unwrap(),
            6
        );
        let values = c
            .run("SELECT id, f8, u, d FROM ty WHERE id = 1")
            .await
            .unwrap()
            .get(0)
            .unwrap()
            .values()
            .unwrap();
        assert_eq!(values[0], Value::I64(1));
        assert_eq!(values[2], Value::Uuid(uuid));
        assert_eq!(
            c.run("SELECT id FROM ty WHERE id = 1")
                .await
                .unwrap()
                .get(0)
                .unwrap()
                .tag(0),
            Some(tag::I64)
        );
    })
    .await
}

#[tokio::test(flavor = "multi_thread")]
async fn what_native_cannot_run_is_refused_before_it_is_sent() {
    bounded(async {
        let Some(server) = server("guards").await else {
            return;
        };
        let c = connect(&server).await;
        c.run("CREATE TABLE g (id BIGINT NOT NULL, PRIMARY KEY (id))")
            .await
            .unwrap();
        let e = c
            .run("INSERT INTO g (id) VALUES (1); INSERT INTO g (id) VALUES (2)")
            .await
            .unwrap_err();
        assert!(matches!(e, Error::Unsupported(_)), "{e}");
        assert_eq!(
            c.fetch_scalar::<i64>("SELECT COUNT(*) FROM g", params![])
                .await
                .unwrap(),
            0,
            "nothing ran"
        );
        assert!(matches!(c.run("BEGIN").await, Err(Error::Unsupported(_))));
        // A ';' in a value is data.
        c.exec("INSERT INTO g (id) VALUES ($1)", params![1i64])
            .await
            .unwrap();
        c.run("INSERT INTO g (id) VALUES (2);").await.unwrap();
        assert_eq!(
            c.fetch_scalar::<i64>("SELECT COUNT(*) FROM g", params![])
                .await
                .unwrap(),
            2
        );
        let dup = c
            .exec("INSERT INTO g (id) VALUES ($1)", params![1i64])
            .await
            .unwrap_err();
        assert!(dup.is_unique_violation(), "{dup}");
        assert!(
            c.fetch_one::<(i64,)>("SELECT id FROM g WHERE id = $1", params![99i64])
                .await
                .unwrap_err()
                .is_no_rows()
        );
        assert!(matches!(
            c.fetch_optional::<(i64,)>("SELECT id FROM g", params![])
                .await,
            Err(Error::RowCount { got: 2, .. })
        ));
        assert!(c.table_exists("g").await.unwrap());
        assert!(!c.table_exists("nope").await.unwrap());
    })
    .await
}

// ------------------------------------------------------------------- admin

#[tokio::test(flavor = "multi_thread")]
async fn admin_calls() {
    bounded(async {
        let Some(server) = server("admin").await else {
            return;
        };
        let c = connect(&server).await;
        let admin = c.admin();

        let kv_dir = server.dir.join("kv1");
        let store = Store::directory("local1", kv_dir.to_str().unwrap())
            .prefix("p")
            .if_not_exists();
        let lines = admin.add_store(&store).await.unwrap();
        assert!(lines[0].contains("local1"), "{lines:?}");
        admin
            .add_store(&store)
            .await
            .expect("IF NOT EXISTS on the same definition");
        let info = admin.store("local1").await.unwrap().expect("listed");
        assert_eq!(
            (info.kind.as_str(), info.prefix.as_str(), info.tables),
            ("dir", "p", 0)
        );
        admin
            .place_table("on_kv", &Placement::kv("local1"))
            .await
            .unwrap();
        c.run("CREATE TABLE on_kv (id BIGINT NOT NULL, PRIMARY KEY (id))")
            .await
            .unwrap();
        c.exec("INSERT INTO on_kv (id) VALUES ($1)", params![5i64])
            .await
            .unwrap();
        assert_eq!(admin.store("local1").await.unwrap().unwrap().tables, 1);
        assert!(
            admin.drop_store("local1").await.is_err(),
            "a store with tables is not dropped"
        );

        c.run("CREATE TABLE users (id BIGINT NOT NULL, email TEXT, name TEXT, PRIMARY KEY (id))")
            .await
            .unwrap();
        admin
            .create_index(&Index::on("users", &["email"]).unique().if_not_exists())
            .await
            .unwrap();
        admin
            .create_index(&Index::on("users", &["email"]).unique().if_not_exists())
            .await
            .unwrap();
        admin
            .create_index(
                &Index::on("users", &["name"])
                    .using(IndexMethod::Hash)
                    .if_not_exists(),
            )
            .await
            .unwrap();
        c.exec(
            "INSERT INTO users (id, email) VALUES (1, $1)",
            params!["a@b"],
        )
        .await
        .unwrap();
        assert!(
            c.exec(
                "INSERT INTO users (id, email) VALUES (2, $1)",
                params!["a@b"]
            )
            .await
            .unwrap_err()
            .is_unique_violation()
        );
        admin.drop_index("users_name_idx", true).await.unwrap();

        let tables = admin.tables().await.unwrap();
        assert!(
            tables
                .iter()
                .any(|t| t.name == "users" && t.primary_key == "id"),
            "{tables:?}"
        );
        let columns = admin.columns("users").await.unwrap();
        assert_eq!(
            columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(),
            ["id", "email", "name"]
        );
        assert!(columns[0].primary_key && columns[1].unique);
        let config = admin.config().await.unwrap();
        assert!(
            config
                .iter()
                .any(|e| e.section == "vacuum" && e.key == "enabled")
        );
        admin.vacuum(Some("users"), false).await.unwrap();
        assert!(!admin.index_checkpoint("users").await.unwrap().is_empty());

        // A password holding flag words sets no flags (fixed in the server).
        admin
            .create_role(&Role::user("app").password("x NOLOGIN SUPERUSER ж"))
            .await
            .unwrap();
        let (login, superuser): (bool, bool) = c
            .fetch_one(
                "SELECT can_login, is_superuser FROM rdb_roles WHERE name = $1",
                params!["app"],
            )
            .await
            .unwrap();
        assert!(login && !superuser);
        admin
            .set_role_password("app", Some("o'brien"))
            .await
            .unwrap();
        admin.drop_role("app", false).await.unwrap();
        admin.drop_role("app", true).await.unwrap();
        assert!(admin.create_role(&Role::user("a b")).await.is_err());

        // Binary-safe KV.
        let kv = admin.kv();
        let key = [0u8, 0xff, b'\''];
        let value: Vec<u8> = (0..=255).rev().collect();
        kv.set(&key, &value, None).await.unwrap();
        assert_eq!(kv.get(&key).await.unwrap(), Some(value));
        assert!(kv.exists(&key).await.unwrap());
        assert_eq!(kv.incr_by(b"n", 5).await.unwrap(), 5);
        assert_eq!(kv.del(&[&key]).await.unwrap(), 1);
        assert_eq!(kv.get(&key).await.unwrap(), None);
    })
    .await
}

// --------------------------------------------------------------------- orm

#[derive(SqlEnum, Debug, Clone, Copy, PartialEq)]
enum Plan {
    Free,
    Pro,
}

#[derive(Model, Debug, Clone, PartialEq)]
#[rdb(table = "accounts")]
struct Account {
    #[rdb(primary_key, serial)]
    id: i64,
    #[rdb(unique)]
    email: String,
    #[rdb(index)]
    name: Option<String>,
    balance: Numeric,
    tags: Vec<String>,
    plan: Plan,
    seen: Timestamp,
    #[rdb(column = "order", default = "0")]
    position: i32,
}

fn account(email: &str, balance: &str) -> Account {
    Account {
        id: 0,
        email: email.into(),
        name: None,
        balance: Numeric::new(balance).unwrap(),
        tags: vec!["x".into(), "y z".into()],
        plan: Plan::Free,
        seen: Timestamp::from_unix_seconds(1_700_000_000),
        position: 0,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_orm() {
    bounded(async {
        let Some(server) = server("orm").await else {
            return;
        };
        let c = connect(&server).await;
        c.create_table::<Account>().await.unwrap();
        c.create_table::<Account>()
            .await
            .expect("again: IF NOT EXISTS");

        let a = c.insert(&account("ada@x", "10.50")).await.unwrap();
        assert!(a.id > 0, "the serial id came back");
        assert_eq!(a.balance.as_str(), "10.50");
        let many = c
            .insert_many(&[
                account("bob@x", "-1"),
                account("cy@x", "99.99"),
                account("o'neil@x", "0"),
            ])
            .await
            .unwrap();
        assert_eq!(many.len(), 3);
        assert!(
            c.insert(&account("ada@x", "1"))
                .await
                .unwrap_err()
                .is_unique_violation()
        );

        let found: Account = c.find(a.id).await.unwrap().expect("found");
        assert_eq!(found, a);
        assert_eq!(c.find::<Account>(-1i64).await.unwrap(), None);

        let rich = Account::query()
            .filter(Account::balance.gt(Numeric::new("5").unwrap()))
            .order_by(Account::balance.desc())
            .all(&c)
            .await
            .unwrap();
        assert_eq!(
            rich.iter().map(|a| a.email.as_str()).collect::<Vec<_>>(),
            ["cy@x", "ada@x"]
        );
        assert_eq!(
            Account::query()
                .filter(Account::email.like("%@x"))
                .count(&c)
                .await
                .unwrap(),
            4
        );
        assert!(
            Account::query()
                .filter(Account::email.eq("o'neil@x"))
                .exists(&c)
                .await
                .unwrap()
        );
        assert_eq!(
            Account::query()
                .filter(Account::id.is_in(Vec::<i64>::new()))
                .count(&c)
                .await
                .unwrap(),
            0
        );
        let first = Account::query()
            .order_by(Account::email.asc())
            .first(&c)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.email, "ada@x");

        let mut changed = a.clone();
        changed.name = Some("Ada".into());
        changed.plan = Plan::Pro;
        changed.position = 7;
        let stored = c.update(&changed).await.unwrap();
        assert_eq!(stored, changed);
        let mut ghost = a.clone();
        ghost.id = -5;
        assert!(c.update(&ghost).await.unwrap_err().is_no_rows());

        let upserted = c
            .upsert(&Account {
                email: "ada@new".into(),
                ..changed.clone()
            })
            .await
            .unwrap();
        assert_eq!(upserted.email, "ada@new");
        assert_eq!(
            Account::query().count(&c).await.unwrap(),
            4,
            "updated, not inserted"
        );

        let updated = Account::query()
            .filter(Account::plan.eq(Plan::Free))
            .update()
            .set(Account::name, "free")
            .set_raw(Account::position, "\"order\" + $1", params![1])
            .run(&c)
            .await
            .unwrap();
        assert_eq!(updated.len(), 3);
        assert!(
            updated
                .iter()
                .all(|a| a.name.as_deref() == Some("free") && a.position == 1)
        );
        assert!(
            Account::query().delete(&c).await.is_err(),
            "no filter, no every_row()"
        );
        assert_eq!(
            Account::query()
                .filter(Account::balance.lt(0))
                .delete(&c)
                .await
                .unwrap(),
            1
        );
        assert!(c.delete(&upserted).await.unwrap());
        assert!(!c.delete(&upserted).await.unwrap());
        assert_eq!(c.all::<Account>().await.unwrap().len(), 2);

        // Past one statement's worth of rows: three statements, every row
        // back, in order, with its own id.
        let bulk: Vec<Account> = (0..1100)
            .map(|i| account(&format!("bulk{i}@x"), "1"))
            .collect();
        let stored = c.insert_many(&bulk).await.unwrap();
        assert_eq!(stored.len(), 1100);
        assert!(stored.iter().zip(&bulk).all(|(s, b)| s.email == b.email));
        let mut ids: Vec<i64> = stored.iter().map(|a| a.id).collect();
        ids.dedup();
        assert_eq!(ids.len(), 1100);
        // Rows big enough that the byte bound splits them, not the row count.
        let big: Vec<Account> = (0..6)
            .map(|i| Account {
                tags: vec!["x".repeat(300_000)],
                ..account(&format!("big{i}@x"), "1")
            })
            .collect();
        assert_eq!(c.insert_many(&big).await.unwrap().len(), 6);
    })
    .await
}

// -------------------------------------------------------------- migrations

#[derive(Model, Debug, Clone, PartialEq)]
#[rdb(table = "people")]
struct PersonV1 {
    #[rdb(primary_key, serial)]
    id: i64,
    full_name: String,
    age: i32,
}

/// The same table a release later: a column renamed, one added with a
/// default, one added nullable, and a unique key.
#[derive(Model, Debug, Clone, PartialEq)]
#[rdb(table = "people")]
struct PersonV2 {
    #[rdb(primary_key, serial)]
    id: i64,
    #[rdb(renamed_from = "full_name")]
    name: String,
    age: i32,
    #[rdb(default = "'active'")]
    status: String,
    #[rdb(unique)]
    email: Option<String>,
}

/// And then renamed, and `age` dropped from the struct.
#[derive(Model, Debug, Clone, PartialEq)]
#[rdb(table = "members", renamed_from = "people")]
struct PersonV3 {
    #[rdb(primary_key, serial)]
    id: i64,
    name: String,
    status: String,
    #[rdb(unique)]
    email: Option<String>,
}

#[tokio::test(flavor = "multi_thread")]
async fn migrations_on_connect_and_by_hand() {
    bounded(async {
        let Some(server) = server("migrate").await else { return };
        let steps = || {
            Migrator::new()
                .step("0001_settings", &["CREATE TABLE IF NOT EXISTS settings (k TEXT NOT NULL, v TEXT, PRIMARY KEY (k))"])
                .down(&["DROP TABLE IF EXISTS settings"])
                .code_step("0002_seed", "v1", |c| {
                    Box::pin(async move {
                        c.exec("INSERT INTO settings (k, v) VALUES ($1, $2)", params!["mode", "fast"]).await?;
                        Ok(())
                    })
                })
                .down(&["DELETE FROM settings WHERE k = 'mode'"])
        };

        // Applied when the client connects, then nothing left to do.
        let opts = Options { migrations: Some(steps().model::<PersonV1>()), ..options() };
        let c = Client::connect(&server.socket, opts.clone()).await.unwrap();
        let v: String = c.fetch_scalar("SELECT v FROM settings WHERE k = 'mode'", params![]).await.unwrap();
        assert_eq!(v, "fast");
        let p = c.insert(&PersonV1 { id: 0, full_name: "Ann".into(), age: 30 }).await.unwrap();
        Client::connect(&server.socket, opts.clone()).await.expect("a second start finds nothing to do");
        let plan = steps().model::<PersonV1>().verify(&c).await.expect("up to date");
        assert!(plan.is_up_to_date());

        // Verify refuses a database that is behind, and changes nothing.
        let behind = steps().step("0003_more", &["CREATE TABLE IF NOT EXISTS more (id BIGINT NOT NULL, PRIMARY KEY (id))"]);
        let verify = Options { migrations: Some(behind.clone()), auto_migrate: AutoMigrate::Verify, ..options() };
        let e = Client::connect(&server.socket, verify).await.err().expect("pending");
        assert!(e.to_string().contains("0003_more"), "{e}");
        assert!(!c.table_exists("more").await.unwrap());
        let off = Options { migrations: Some(behind.clone()), auto_migrate: AutoMigrate::Off, ..options() };
        Client::connect(&server.socket, off).await.expect("Off does nothing");
        assert!(!c.table_exists("more").await.unwrap());
        assert_eq!(c.migrate(&behind).await.unwrap().applied, ["0003_more"]);

        // An edited step is refused unless accepted.
        let edited = Migrator::new()
            .step("0001_settings", &["CREATE TABLE IF NOT EXISTS settings (k TEXT NOT NULL, PRIMARY KEY (k))"])
            .code_step("0002_seed", "v1", |_| Box::pin(async { Ok(()) }))
            .step("0003_more", &["CREATE TABLE IF NOT EXISTS more (id BIGINT NOT NULL, PRIMARY KEY (id))"]);
        assert!(c.migrate(&edited).await.unwrap_err().to_string().contains("0001_settings"));
        assert_eq!(c.migrate(&edited.clone().accept_changed(true)).await.unwrap().accepted, ["0001_settings"]);

        // An older build (fewer steps) is refused unless told to ignore.
        let older = Migrator::new().step("0001_settings", &["CREATE TABLE IF NOT EXISTS settings (k TEXT NOT NULL, PRIMARY KEY (k))"]);
        assert!(c.migrate(&older).await.unwrap_err().to_string().contains("0003_more"));
        assert!(!c.migrate(&older.ignore_unknown(true)).await.unwrap().warnings.is_empty());
        let status = edited.status(&c).await.unwrap();
        assert!(status.iter().all(|s| s.state == StepState::Applied), "{status:?}");

        // Model sync: rename a column, add two, add a unique key.
        // Model-only migrators share the record table with the steps above:
        // to them those steps are unknown, which they are told to accept.
        let v2 = Migrator::new().model::<PersonV2>().ignore_unknown(true);
        let plan = v2.plan(&c).await.unwrap();
        assert!(plan.schema_changes.iter().any(|s| s.contains("RENAME COLUMN full_name TO name")), "{plan:?}");
        let report = c.migrate(&v2).await.unwrap();
        assert_eq!(report.schema_changes.len(), 4, "{report:?}");
        assert!(report.warnings.iter().all(|w| !w.contains("people.")), "no drift: {report:?}");
        let ann: PersonV2 = c.find(p.id).await.unwrap().unwrap();
        assert_eq!((ann.name.as_str(), ann.age, ann.status.as_str(), ann.email.as_deref()), ("Ann", 30, "active", None));
        assert!(v2.plan(&c).await.unwrap().drift.is_empty());

        // Rename the table; `age` is no longer in the struct: drift, kept.
        let v3 = Migrator::new().model::<PersonV3>().ignore_unknown(true);
        let report = c.migrate(&v3).await.unwrap();
        assert!(report.schema_changes.iter().any(|s| s.contains("RENAME TO members")), "{report:?}");
        assert!(report.warnings.iter().any(|w| w.contains("members.age is not in the model")), "{report:?}");
        assert!(c.table_exists("members").await.unwrap() && !c.table_exists("people").await.unwrap());
        let ann: PersonV3 = c.find(p.id).await.unwrap().unwrap();
        assert_eq!(ann.name, "Ann");
        // fail_on_drift turns it into an error; exact() drops the column.
        let strict = Migrator::new()
            .model::<PersonV3>()
            .ignore_unknown(true)
            .sync_policy(SyncPolicy { fail_on_drift: true, ..SyncPolicy::default() });
        assert!(c.migrate(&strict).await.is_err());
        let exact = Migrator::new().model::<PersonV3>().ignore_unknown(true).sync_policy(SyncPolicy::exact());
        c.migrate(&exact).await.unwrap();
        let after = Migrator::new().model::<PersonV3>().plan(&c).await.unwrap();
        assert!(after.schema_changes.iter().all(|s| s.contains("IF NOT EXISTS")) && after.drift.is_empty(), "{after:?}");

        // Baseline: a database built before its migrations.
        let fresh = Migrator::new()
            .table("other_migrations")
            .step("a", &["CREATE TABLE IF NOT EXISTS settings (k TEXT NOT NULL, PRIMARY KEY (k))"])
            .step("b", &["CREATE TABLE IF NOT EXISTS b_table (id BIGINT NOT NULL, PRIMARY KEY (id))"])
            .baseline("a");
        let report = c.migrate(&fresh).await.unwrap();
        assert_eq!((report.baselined, report.applied), (vec!["a".to_string()], vec!["b".to_string()]));

        // Rollback runs downs newest first.
        let undone = steps().rollback_to(&c, Some("0001_settings")).await;
        assert!(undone.unwrap_err().to_string().contains("0003_more"), "a later step this list lacks");
        assert_eq!(c.fetch_scalar::<i64>("SELECT COUNT(*) FROM settings", params![]).await.unwrap(), 1, "nothing undone");
        let undone = edited
            .clone()
            .accept_changed(true)
            .rollback_to(&c, Some("0001_settings"))
            .await;
        assert!(undone.is_err(), "0002 and 0003 have no down() in this list");
        let with_downs = steps()
            .step("0003_more", &["CREATE TABLE IF NOT EXISTS more (id BIGINT NOT NULL, PRIMARY KEY (id))"])
            .down(&["DROP TABLE IF EXISTS more"])
            .accept_changed(true);
        c.migrate(&with_downs).await.unwrap();
        let undone = with_downs.rollback_to(&c, Some("0001_settings")).await.unwrap();
        assert_eq!(undone, ["0003_more", "0002_seed"]);
        assert!(!c.table_exists("more").await.unwrap());
        assert_eq!(c.fetch_scalar::<i64>("SELECT COUNT(*) FROM settings", params![]).await.unwrap(), 0);
    })
    .await
}

#[tokio::test(flavor = "multi_thread")]
async fn instances_starting_together_migrate_once() {
    bounded(async {
        let Some(server) = server("lock").await else {
            return;
        };
        let counter = || {
            Migrator::new()
                .step(
                    "0001",
                    &["CREATE TABLE IF NOT EXISTS hits (id BIGSERIAL, PRIMARY KEY (id))"],
                )
                .code_step("0002_once", "v1", |c| {
                    Box::pin(async move {
                        c.run("INSERT INTO hits (id) VALUES (DEFAULT)").await?;
                        tokio::time::sleep(Duration::from_millis(300)).await;
                        Ok(())
                    })
                })
        };
        let mut starts = Vec::new();
        for _ in 0..6 {
            let socket = server.socket.clone();
            let opts = Options {
                migrations: Some(counter()),
                ..options()
            };
            starts.push(tokio::spawn(async move {
                Client::connect(&socket, opts).await.map(drop)
            }));
        }
        for start in starts {
            start.await.unwrap().expect("every instance starts");
        }
        let c = connect(&server).await;
        assert_eq!(
            c.fetch_scalar::<i64>("SELECT COUNT(*) FROM hits", params![])
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            c.fetch_scalar::<i64>("SELECT COUNT(*) FROM rdb_client_migrations_lock", params![])
                .await
                .unwrap(),
            0,
            "the lock is released"
        );
    })
    .await
}

// ---------------------------------------------------------------------- kv

#[derive(Model, Debug, Clone, PartialEq)]
#[rdb(table = "sessions")]
struct Session {
    #[rdb(primary_key)]
    token: Uuid,
    user_id: i64,
    plan: Plan,
    note: Option<String>,
    roles: Vec<String>,
    seen: Timestamp,
}

#[tokio::test(flavor = "multi_thread")]
async fn the_typed_cache() {
    bounded(async {
        let Some(server) = server("kv").await else {
            return;
        };
        let c = connect(&server).await;
        let kv = c.kv();

        // One typed value.
        kv.set_as("Value 1", &41i32, None).await.unwrap();
        assert_eq!(kv.get_as::<i32>("Value 1").await.unwrap(), Some(41));
        assert_eq!(kv.get_as::<i32>("missing").await.unwrap(), None);

        // A typed map, and queries over it.
        let hits = kv.map::<String, i64>("hits");
        for (path, n) in [
            ("/api/a", 150),
            ("/api/b", 20),
            ("/web/x*", 500),
            ("/api/c", 101),
        ] {
            hits.set(&path.to_string(), &n).await.unwrap();
        }
        assert_eq!(hits.incr_by(&"/api/b".to_string(), 1).await.unwrap(), 21);
        let mut busy = hits
            .query()
            .prefix("/api")
            .filter(|_, v| *v > 100)
            .all()
            .await
            .unwrap();
        busy.sort();
        assert_eq!(
            busy,
            [("/api/a".to_string(), 150), ("/api/c".to_string(), 101)]
        );
        assert_eq!(
            hits.query().pattern("/web/*").keys().await.unwrap(),
            ["/web/x*"]
        );
        assert_eq!(
            hits.query().prefix("/web/x*").count().await.unwrap(),
            1,
            "a glob character in a prefix is literal"
        );
        assert_eq!(hits.query().count().await.unwrap(), 4);
        assert_eq!(hits.query().limit(2).all().await.unwrap().len(), 2);

        // The whole keyspace, filtered by key: other entries hold other
        // types, so lenient() skips what does not read as i32.
        let found = kv
            .query::<String, i32>()
            .filter_keys(|key| key == "Value 1")
            .all()
            .await
            .unwrap();
        assert_eq!(found, [("Value 1".to_string(), 41)]);
        let ints = kv.query::<String, i32>().lenient().all().await.unwrap();
        assert!(ints.len() >= 5, "{ints:?}");

        let leases = kv.map::<Uuid, String>("lease");
        let id = Uuid::new_v4().unwrap();
        assert!(
            leases
                .set_if_absent(&id, &"me".to_string(), Some(Duration::from_secs(30)))
                .await
                .unwrap()
        );
        assert!(
            !leases
                .set_if_absent(&id, &"you".to_string(), None)
                .await
                .unwrap()
        );
        assert!(
            leases
                .ttl(&id)
                .await
                .unwrap()
                .is_some_and(|t| t <= Duration::from_secs(30))
        );
        assert_eq!(
            leases
                .get_many(&[&id, &Uuid::new_v4().unwrap()])
                .await
                .unwrap(),
            [Some("me".to_string()), None]
        );
        assert_eq!(hits.query().prefix("/api").delete().await.unwrap(), 3);
        assert_eq!(hits.clear().await.unwrap(), 1);
        assert!(
            kv.query::<String, i32>().delete().await.is_err(),
            "no scope, no delete"
        );

        // A model as a hash.
        let sessions = kv.repo::<Session>();
        let s = Session {
            token: Uuid::new_v4().unwrap(),
            user_id: 7,
            plan: Plan::Pro,
            note: Some("a'b".into()),
            roles: vec!["admin".into(), "a,b".into()],
            seen: Timestamp::from_micros(1_700_000_000_123_456),
        };
        sessions
            .save_with_ttl(&s, Duration::from_secs(60))
            .await
            .unwrap();
        assert_eq!(sessions.get(&[&s.token]).await.unwrap(), Some(s.clone()));
        let mut cleared = s.clone();
        cleared.note = None;
        sessions.save(&cleared).await.unwrap();
        assert_eq!(
            sessions.get(&[&s.token]).await.unwrap(),
            Some(cleared.clone()),
            "a NULL removes its field"
        );
        let other = Session {
            token: Uuid::new_v4().unwrap(),
            user_id: 8,
            ..s.clone()
        };
        sessions.save(&other).await.unwrap();
        assert_eq!(
            sessions.filter(|x| x.user_id == 8).await.unwrap(),
            [other.clone()]
        );
        assert_eq!(
            sessions
                .find(|x| x.plan == Plan::Pro && x.user_id == 7)
                .await
                .unwrap(),
            Some(cleared)
        );
        assert_eq!(sessions.all().await.unwrap().len(), 2);
        assert!(sessions.delete(&other).await.unwrap());
        assert!(!sessions.exists(&[&other.token]).await.unwrap());

        // Lists, sets, sorted sets, hashes.
        kv.rpush(b"q", &[b"1", b"2"]).await.unwrap();
        assert_eq!(kv.lpop(b"q").await.unwrap().as_deref(), Some(&b"1"[..]));
        kv.sadd(b"s", &[b"x", b"\xff"]).await.unwrap();
        assert!(kv.sismember(b"s", b"\xff").await.unwrap());
        kv.zadd(b"z", &[(2.5, b"b"), (1.0, b"a")]).await.unwrap();
        assert_eq!(
            kv.zrange(b"z", 0, -1, true).await.unwrap(),
            [(b"b".to_vec(), 2.5), (b"a".to_vec(), 1.0)]
        );
        assert_eq!(kv.hincr_by(b"h", b"n", 3).await.unwrap(), 3);
        assert_eq!(
            kv.hget(b"h", b"n").await.unwrap().as_deref(),
            Some(&b"3"[..])
        );
    })
    .await
}
