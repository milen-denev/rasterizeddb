//! The release build's training run for profile-guided optimisation (PGO)
//! and BOLT.
//!
//! It starts a server binary on a fresh directory with both protocols, drives
//! it through a mixed workload, then stops it the way an operator would, so
//! that an instrumented binary writes its profile on the way out. The workload
//! covers bulk loads, point reads and writes, filtered scans, sorts,
//! aggregates, joins, subqueries, JSONB, upserts and transactions, over the
//! native protocol and over pgwire (simple and prepared), single-connection
//! and concurrent.
//!
//! ```text
//! pgo_train <server binary> [--scale N]
//! ```
//!
//! Every statement must succeed, and the row counts are checked: a workload
//! that quietly stops early trains the optimiser on start-up code alone, and
//! the release would still build.
//!
//! The stop is SIGTERM on unix and Ctrl+Break on Windows. The server is
//! started in its own process group there, so the event reaches it alone.

use std::io;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{ExitCode, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use rasterizeddb_client::{Client, Executor, Login, Options, Param};
use tokio::process::{Child, Command};
use tokio_postgres::NoTls;

const USER: &str = "rasterized";
const PASSWORD: &str = "pgo-train";
const CITIES: [&str; 12] = [
    "Sofia", "Berlin", "Lisbon", "Oslo", "Madrid", "Vienna", "Prague", "Athens", "Dublin",
    "Riga", "Tallinn", "Warsaw",
];
const STATUSES: [&str; 4] = ["new", "paid", "shipped", "cancelled"];
const BATCH: usize = 500;

type Error = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, Error>;

fn main() -> ExitCode {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("a tokio runtime");
    match runtime.block_on(run()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            // tokio-postgres's Display is only "db error"; the source has the
            // server's message and SQLSTATE.
            eprintln!("pgo_train: {e}");
            let mut source = e.source();
            while let Some(cause) = source {
                eprintln!("  caused by: {cause}");
                source = cause.source();
            }
            ExitCode::FAILURE
        }
    }
}

struct Args {
    server: PathBuf,
    scale: usize,
}

fn args() -> Result<Args> {
    let mut server = None;
    let mut scale = 1;
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--scale" => {
                scale = it.next().ok_or("--scale needs a number")?.parse()?;
                if scale == 0 {
                    return Err("--scale must be at least 1".into());
                }
            }
            _ if server.is_none() => server = Some(PathBuf::from(arg)),
            _ => return Err(format!("unexpected argument {arg:?}").into()),
        }
    }
    let server = server.ok_or("usage: pgo_train <server binary> [--scale N]")?;
    Ok(Args { server, scale })
}

async fn run() -> Result<()> {
    let args = args()?;
    #[cfg(windows)]
    console::ensure();

    let started = Instant::now();
    let mut server = Server::start(&args.server).await?;
    if let Err(e) = workload(&server, args.scale).await {
        server.print_log();
        return Err(e);
    }
    if let Err(e) = server.stop().await {
        server.print_log();
        return Err(e);
    }
    println!("training run finished in {:.1?}", started.elapsed());
    Ok(())
}

// ---------------------------------------------------------------------------
// The server process
// ---------------------------------------------------------------------------

struct Server {
    child: Option<Child>,
    dir: PathBuf,
    log: PathBuf,
    pg_port: u16,
    rdb_port: u16,
}

impl Server {
    async fn start(bin: &Path) -> Result<Server> {
        let dir = std::env::temp_dir().join(format!("rdb-pgo-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("db"))?;
        let (pg_port, rdb_port) = two_free_ports()?;
        let log = dir.join("server.log");
        let out = std::fs::File::create(&log)?;
        let err = out.try_clone()?;

        let mut command = Command::new(bin);
        command
            .arg("--location")
            .arg(dir.join("db"))
            .args(["--protocol", "both", "--log-level", "warn"])
            // Loopback only. The native side still runs TLS, and the
            // cipher code is rustls's, not the server's.
            .args(["--pg-tls", "disable"])
            .args(["--pg-port", &pg_port.to_string()])
            .args(["--rdb-port", &rdb_port.to_string()])
            .env("RASTERIZEDDB_PG_PASSWORD", PASSWORD)
            .stdin(Stdio::null())
            .stdout(out)
            .stderr(err)
            .kill_on_drop(true);
        // CREATE_NEW_PROCESS_GROUP: the group's id is the server's pid, so
        // the Ctrl+Break that stops it reaches nothing else.
        #[cfg(windows)]
        command.creation_flags(0x0000_0200);
        let child = command
            .spawn()
            .map_err(|e| format!("could not start {}: {e}", bin.display()))?;

        let mut server = Server {
            child: Some(child),
            dir,
            log,
            pg_port,
            rdb_port,
        };
        server.wait_ready().await?;
        Ok(server)
    }

    async fn wait_ready(&mut self) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            if let Some(status) = self.child.as_mut().and_then(|c| c.try_wait().ok().flatten()) {
                self.print_log();
                return Err(format!("the server exited during start-up ({status})").into());
            }
            if listening(self.pg_port) && listening(self.rdb_port) {
                return Ok(());
            }
            if Instant::now() > deadline {
                self.print_log();
                return Err("the server did not open both ports within 120 s".into());
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// Stops the server as an operator would and waits for a clean exit,
    /// which is when an instrumented binary writes its profile.
    async fn stop(&mut self) -> Result<()> {
        let mut child = self.child.take().ok_or("the server was already stopped")?;
        let pid = child.id().ok_or("the server had already exited")?;
        let asked = Instant::now();
        signal_stop(pid)?;
        let status = tokio::time::timeout(Duration::from_secs(300), child.wait())
            .await
            .map_err(|_| "the server did not stop within 300 s of the stop signal")??;
        if !status.success() {
            return Err(format!("the server exited with {status} on the stop signal").into());
        }
        println!("{:<34} {:>8.2?}", "graceful stop", asked.elapsed());
        Ok(())
    }

    fn print_log(&self) {
        match std::fs::read_to_string(&self.log) {
            Ok(text) => {
                let lines: Vec<&str> = text.lines().collect();
                let from = lines.len().saturating_sub(80);
                eprintln!("--- server log (last {} lines) ---", lines.len() - from);
                for line in &lines[from..] {
                    eprintln!("{line}");
                }
                eprintln!("--- end of server log ---");
            }
            Err(e) => eprintln!("(could not read the server log: {e})"),
        }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        if let Some(child) = self.child.as_mut() {
            let _ = child.start_kill();
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn two_free_ports() -> Result<(u16, u16)> {
    // Both listeners are held until both ports are known, so the two differ.
    let a = TcpListener::bind("127.0.0.1:0")?;
    let b = TcpListener::bind("127.0.0.1:0")?;
    Ok((a.local_addr()?.port(), b.local_addr()?.port()))
}

fn listening(port: u16) -> bool {
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    TcpStream::connect_timeout(&addr, Duration::from_millis(200)).is_ok()
}

#[cfg(unix)]
fn signal_stop(pid: u32) -> Result<()> {
    let pid = i32::try_from(pid)?;
    // SAFETY: kill has no memory preconditions; pid is our own child.
    if unsafe { libc::kill(pid, libc::SIGTERM) } != 0 {
        return Err(io::Error::last_os_error().into());
    }
    Ok(())
}

#[cfg(windows)]
fn signal_stop(pid: u32) -> Result<()> {
    console::ctrl_break(pid)
}

#[cfg(windows)]
mod console {
    use std::ffi::c_void;
    use std::io;

    const CTRL_BREAK_EVENT: u32 = 1;
    const STD_HANDLES: [u32; 3] = [-10i32 as u32, -11i32 as u32, -12i32 as u32];

    unsafe extern "system" {
        fn AllocConsole() -> i32;
        fn GetStdHandle(which: u32) -> *mut c_void;
        fn SetStdHandle(which: u32, handle: *mut c_void) -> i32;
        fn GenerateConsoleCtrlEvent(event: u32, group: u32) -> i32;
    }

    /// Console control events travel only between processes that share a
    /// console, and a CI runner may start this one without any. Give it one
    /// (the server inherits it) and keep the redirected standard handles,
    /// which `AllocConsole` would otherwise point at the new console.
    pub fn ensure() {
        // SAFETY: plain Win32 calls on this process's own standard handles.
        unsafe {
            let saved = STD_HANDLES.map(|which| GetStdHandle(which));
            if AllocConsole() != 0 {
                for (which, handle) in STD_HANDLES.into_iter().zip(saved) {
                    if !handle.is_null() && handle as isize != -1 {
                        SetStdHandle(which, handle);
                    }
                }
            }
        }
    }

    pub fn ctrl_break(pid: u32) -> super::Result<()> {
        // SAFETY: no pointers; `pid` names the process group the server was
        // started as.
        if unsafe { GenerateConsoleCtrlEvent(CTRL_BREAK_EVENT, pid) } == 0 {
            return Err(io::Error::last_os_error().into());
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// The workload
// ---------------------------------------------------------------------------

async fn phase<T>(name: &str, work: impl Future<Output = Result<T>>) -> Result<T> {
    let started = Instant::now();
    let value = work.await.map_err(|e| format!("{name}: {e}"))?;
    println!("{name:<34} {:>8.2?}", started.elapsed());
    Ok(value)
}

async fn connect_native(server: &Server, connections: usize) -> Result<Client> {
    let options = Options {
        login: Login::password(USER, PASSWORD),
        timeout: Some(Duration::from_secs(300)),
        connections,
        ..Options::default()
    };
    Ok(Client::connect(&format!("127.0.0.1:{}", server.rdb_port), options).await?)
}

async fn connect_pg(server: &Server) -> Result<tokio_postgres::Client> {
    let config = format!(
        "host=127.0.0.1 port={} user={USER} password={PASSWORD} dbname=postgres",
        server.pg_port
    );
    let (client, connection) = tokio_postgres::connect(&config, NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("pgwire connection ended: {e}");
        }
    });
    Ok(client)
}

struct Sizes {
    customers: i64,
    orders: i64,
    events: i64,
    point_ops: i64,
}

async fn workload(server: &Server, scale: usize) -> Result<()> {
    let scale = scale as i64;
    let sizes = Sizes {
        customers: 20_000 * scale,
        orders: 100_000 * scale,
        events: 5_000 * scale,
        point_ops: 3_000 * scale,
    };
    let native = Arc::new(connect_native(server, 4).await?);
    let pg = connect_pg(server).await?;

    phase("schema", schema(&native)).await?;
    phase("load customers (native)", load_customers(&native, &sizes)).await?;
    phase("load orders (pgwire)", load_orders(&pg, &sizes)).await?;
    phase("load events, JSONB (native)", load_events(&native, &sizes)).await?;
    phase("indexes", indexes(&native)).await?;
    phase("check counts", check_counts(&native, &pg, &sizes)).await?;
    phase("point reads (native)", point_reads_native(&native, &sizes)).await?;
    phase("point reads (pgwire prepared)", point_reads_pg(&pg, &sizes)).await?;
    phase("index lookups", index_lookups(&native, &pg, &sizes)).await?;
    phase("scans and filters", scans(&native, &pg)).await?;
    phase("sorts and top-N", sorts(&native, &pg)).await?;
    phase("aggregates and grouping", aggregates(&native, &pg)).await?;
    phase("joins and subqueries", joins(&native, &pg)).await?;
    phase("JSONB queries", json_queries(&native, &pg)).await?;
    phase("point writes", point_writes(&native, &pg, &sizes)).await?;
    phase("transactions", transactions(&native, &pg, &sizes)).await?;
    phase("concurrent mixed", concurrent(server, &native, &sizes)).await?;
    phase("bulk updates and deletes", bulk_writes(&native, &pg, &sizes)).await?;
    phase("check counts after writes", check_counts_after(&native, &pg, &sizes)).await?;
    Ok(())
}

async fn schema(native: &Client) -> Result<()> {
    for sql in [
        "CREATE TABLE customers (id BIGINT NOT NULL, name TEXT NOT NULL, city TEXT NOT NULL, \
         tier INTEGER NOT NULL, balance DOUBLE PRECISION NOT NULL, active BOOLEAN NOT NULL, \
         joined TIMESTAMP NOT NULL, PRIMARY KEY (id))",
        "CREATE TABLE orders (id BIGINT NOT NULL, customer_id BIGINT NOT NULL, \
         amount NUMERIC(12,2) NOT NULL, status TEXT NOT NULL, note TEXT, \
         created TIMESTAMP NOT NULL, PRIMARY KEY (id))",
        "CREATE TABLE events (id BIGINT NOT NULL, payload JSONB NOT NULL, PRIMARY KEY (id))",
    ] {
        native.run(sql).await?;
    }
    Ok(())
}

fn timestamp(i: i64) -> String {
    format!(
        "'2025-{:02}-{:02} {:02}:{:02}:{:02}'",
        1 + i % 12,
        1 + i % 28,
        i % 24,
        (i * 7) % 60,
        (i * 13) % 60
    )
}

async fn load_customers(native: &Client, sizes: &Sizes) -> Result<()> {
    let mut id = 0;
    while id < sizes.customers {
        let end = (id + BATCH as i64).min(sizes.customers);
        let rows: Vec<String> = (id..end)
            .map(|i| {
                format!(
                    "({i}, 'customer {i}', '{}', {}, {}, {}, {})",
                    CITIES[(i % 12) as usize],
                    1 + i % 5,
                    ((i * 37) % 10_000) as f64 / 7.0,
                    i % 3 != 0,
                    timestamp(i)
                )
            })
            .collect();
        native
            .run(&format!("INSERT INTO customers VALUES {}", rows.join(", ")))
            .await?;
        id = end;
    }
    Ok(())
}

async fn load_orders(pg: &tokio_postgres::Client, sizes: &Sizes) -> Result<()> {
    let mut id = 0;
    while id < sizes.orders {
        let end = (id + BATCH as i64).min(sizes.orders);
        let rows: Vec<String> = (id..end)
            .map(|i| {
                // Skewed: some customers have many orders, most have a few.
                let customer = (i * i + 7 * i) % sizes.customers;
                let note = if i % 5 == 0 {
                    "NULL".to_string()
                } else {
                    format!("'order note {i}'")
                };
                format!(
                    "({i}, {customer}, {}.{:02}, '{}', {note}, {})",
                    (i * 31) % 1_000,
                    i % 100,
                    STATUSES[(i % 4) as usize],
                    timestamp(i)
                )
            })
            .collect();
        pg.batch_execute(&format!("INSERT INTO orders VALUES {}", rows.join(", ")))
            .await?;
        id = end;
    }
    Ok(())
}

async fn load_events(native: &Client, sizes: &Sizes) -> Result<()> {
    const KINDS: [&str; 5] = ["click", "view", "purchase", "signup", "logout"];
    let mut id = 0;
    while id < sizes.events {
        let end = (id + BATCH as i64).min(sizes.events);
        let rows: Vec<String> = (id..end)
            .map(|i| {
                format!(
                    r#"({i}, '{{"kind": "{}", "user": {}, "score": {}, "tags": ["t{}", "t{}"], "meta": {{"source": "s{}"}}}}')"#,
                    KINDS[(i % 5) as usize],
                    i % sizes.customers,
                    (i * 17) % 100,
                    i % 7,
                    i % 3,
                    i % 4
                )
            })
            .collect();
        native
            .run(&format!("INSERT INTO events VALUES {}", rows.join(", ")))
            .await?;
        id = end;
    }
    Ok(())
}

async fn indexes(native: &Client) -> Result<()> {
    native
        .run("CREATE INDEX customers_city ON customers (city)")
        .await?;
    native
        .run("CREATE INDEX orders_customer ON orders (customer_id)")
        .await?;
    Ok(())
}

async fn native_count(native: &Client, sql: &str) -> Result<i64> {
    Ok(native.fetch_scalar::<i64>(sql, &[]).await?)
}

async fn pg_count(pg: &tokio_postgres::Client, sql: &str) -> Result<i64> {
    Ok(pg.query_one(sql, &[]).await?.try_get::<_, i64>(0)?)
}

fn expect(what: &str, got: i64, want: i64) -> Result<()> {
    if got != want {
        return Err(format!("{what}: expected {want}, found {got}").into());
    }
    Ok(())
}

async fn check_counts(native: &Client, pg: &tokio_postgres::Client, sizes: &Sizes) -> Result<()> {
    expect(
        "customers",
        native_count(native, "SELECT COUNT(*) FROM customers").await?,
        sizes.customers,
    )?;
    expect(
        "orders",
        pg_count(pg, "SELECT COUNT(*) FROM orders").await?,
        sizes.orders,
    )?;
    expect(
        "events",
        native_count(native, "SELECT COUNT(*) FROM events").await?,
        sizes.events,
    )?;
    Ok(())
}

/// A key spread over the whole table, different on each step.
fn key(step: i64, modulo: i64) -> i64 {
    (step * 7_919 + 13) % modulo
}

async fn point_reads_native(native: &Client, sizes: &Sizes) -> Result<()> {
    for step in 0..sizes.point_ops {
        let id = key(step, sizes.customers);
        let rows = native
            .query(
                "SELECT id, name, city, tier, balance, active, joined FROM customers WHERE id = $1",
                &[Param::I64(id)],
            )
            .await?;
        expect("a point read", rows.len() as i64, 1)?;
    }
    Ok(())
}

async fn point_reads_pg(pg: &tokio_postgres::Client, sizes: &Sizes) -> Result<()> {
    let by_id = pg
        .prepare("SELECT id, name, city, tier, balance, active, joined FROM customers WHERE id = $1")
        .await?;
    let order = pg
        .prepare("SELECT id, customer_id, amount, status, note, created FROM orders WHERE id = $1")
        .await?;
    for step in 0..sizes.point_ops {
        let rows = pg.query(&by_id, &[&key(step, sizes.customers)]).await?;
        expect("a prepared point read", rows.len() as i64, 1)?;
        let rows = pg.query(&order, &[&key(step, sizes.orders)]).await?;
        expect("a prepared order read", rows.len() as i64, 1)?;
    }
    Ok(())
}

async fn index_lookups(native: &Client, pg: &tokio_postgres::Client, sizes: &Sizes) -> Result<()> {
    let by_customer = pg
        .prepare("SELECT id, amount, status FROM orders WHERE customer_id = $1")
        .await?;
    for step in 0..sizes.point_ops / 3 {
        let customer = key(step, sizes.customers);
        pg.query(&by_customer, &[&customer]).await?;
        native
            .query(
                "SELECT id, amount FROM orders WHERE customer_id = $1 AND status = 'paid'",
                &[Param::I64(customer)],
            )
            .await?;
    }
    for city in CITIES {
        native
            .query(
                "SELECT id, name, balance FROM customers WHERE city = $1 AND tier = 3",
                &[Param::Text(city)],
            )
            .await?;
    }
    Ok(())
}

/// Runs each statement over both protocols, pgwire as a simple query.
async fn both(native: &Client, pg: &tokio_postgres::Client, statements: &[&str]) -> Result<()> {
    for sql in statements {
        let started = Instant::now();
        native
            .run(sql)
            .await
            .map_err(|e| format!("{sql} (native): {e}"))?;
        let native_took = started.elapsed();
        let started = Instant::now();
        pg.simple_query(sql)
            .await
            .map_err(|e| format!("{sql} (pgwire): {e}"))?;
        let pg_took = started.elapsed();
        // A statement this slow dominates the profile; worth knowing.
        if native_took + pg_took > Duration::from_secs(2) {
            println!("  slow: native {native_took:.2?}, pgwire {pg_took:.2?}: {sql}");
        }
    }
    Ok(())
}

async fn scans(native: &Client, pg: &tokio_postgres::Client) -> Result<()> {
    for _ in 0..3 {
        both(
            native,
            pg,
            &[
                "SELECT COUNT(*) FROM customers WHERE tier = 3 AND balance > 500",
                "SELECT id, name FROM customers WHERE name LIKE 'customer 12%'",
                "SELECT id, name, city FROM customers WHERE city IN ('Sofia', 'Berlin', 'Lisbon') AND active",
                "SELECT id, amount FROM orders WHERE amount BETWEEN 100 AND 200 AND status <> 'cancelled'",
                "SELECT COUNT(*) FROM orders WHERE note IS NULL",
                "SELECT id, upper(name), length(city) FROM customers WHERE id < 500",
                "SELECT id FROM orders WHERE created >= '2025-06-01 00:00:00' AND created < '2025-07-01 00:00:00'",
                "SELECT id, CASE WHEN balance > 1000 THEN 'high' WHEN balance > 100 THEN 'mid' ELSE 'low' END FROM customers WHERE tier = 1",
                "SELECT COUNT(*) FROM orders WHERE customer_id IN (SELECT id FROM customers WHERE tier = 5)",
            ],
        )
        .await?;
    }
    Ok(())
}

async fn sorts(native: &Client, pg: &tokio_postgres::Client) -> Result<()> {
    for _ in 0..3 {
        both(
            native,
            pg,
            &[
                "SELECT id, name, balance FROM customers ORDER BY balance DESC LIMIT 20",
                "SELECT id, amount FROM orders WHERE status = 'paid' ORDER BY amount DESC, id LIMIT 50",
                "SELECT id, name FROM customers WHERE city = 'Oslo' ORDER BY name LIMIT 100 OFFSET 20",
                "SELECT id, created FROM orders ORDER BY created DESC LIMIT 10",
                "SELECT DISTINCT city FROM customers ORDER BY city",
            ],
        )
        .await?;
    }
    Ok(())
}

async fn aggregates(native: &Client, pg: &tokio_postgres::Client) -> Result<()> {
    for _ in 0..3 {
        both(
            native,
            pg,
            &[
                "SELECT city, COUNT(*), SUM(balance), AVG(balance), MIN(balance), MAX(balance) FROM customers GROUP BY city ORDER BY city",
                "SELECT status, COUNT(*), SUM(amount), AVG(amount) FROM orders GROUP BY status",
                "SELECT tier, active, COUNT(*) FROM customers GROUP BY tier, active ORDER BY tier",
                "SELECT customer_id, COUNT(*) AS n FROM orders GROUP BY customer_id HAVING COUNT(*) > 20 ORDER BY n DESC LIMIT 10",
                "SELECT COUNT(DISTINCT customer_id) FROM orders",
                "SELECT SUM(amount) FROM orders WHERE status = 'shipped'",
            ],
        )
        .await?;
    }
    Ok(())
}

async fn joins(native: &Client, pg: &tokio_postgres::Client) -> Result<()> {
    for _ in 0..2 {
        both(
            native,
            pg,
            &[
                "SELECT c.city, COUNT(*), SUM(o.amount) FROM orders o JOIN customers c ON o.customer_id = c.id WHERE c.tier = 2 GROUP BY c.city ORDER BY c.city",
                "SELECT c.id, c.name, o.id, o.amount FROM customers c JOIN orders o ON o.customer_id = c.id WHERE c.id < 200 ORDER BY o.amount DESC LIMIT 25",
                "SELECT c.id, COUNT(o.id) FROM customers c LEFT JOIN orders o ON o.customer_id = c.id WHERE c.id < 300 GROUP BY c.id",
                // Few outer rows: each one scans orders in full (the index on
                // customer_id goes unused inside a correlated subquery), and
                // fifty of them made up half of the whole training run.
                "SELECT c.name, (SELECT COUNT(*) FROM orders o WHERE o.customer_id = c.id) FROM customers c WHERE c.id < 5",
                "WITH totals AS (SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id) SELECT COUNT(*) FROM totals WHERE total > 1000",
                "SELECT id, name FROM customers c WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id AND o.status = 'cancelled') AND c.id < 1000",
            ],
        )
        .await?;
    }
    Ok(())
}

async fn json_queries(native: &Client, pg: &tokio_postgres::Client) -> Result<()> {
    for _ in 0..3 {
        both(
            native,
            pg,
            &[
                "SELECT COUNT(*) FROM events WHERE payload->>'kind' = 'purchase'",
                "SELECT payload->>'kind', COUNT(*) FROM events GROUP BY payload->>'kind'",
                "SELECT id, payload->'meta'->>'source' FROM events WHERE id < 200",
                "SELECT id FROM events WHERE (payload->>'score')::int > 90",
            ],
        )
        .await?;
    }
    Ok(())
}

async fn point_writes(native: &Client, pg: &tokio_postgres::Client, sizes: &Sizes) -> Result<()> {
    let bump = pg
        .prepare("UPDATE orders SET note = 'touched' WHERE id = $1")
        .await?;
    for step in 0..sizes.point_ops / 2 {
        let changed = native
            .execute(
                "UPDATE customers SET balance = balance + 1.5 WHERE id = $1",
                &[Param::I64(key(step, sizes.customers))],
            )
            .await?;
        expect("a point update", changed as i64, 1)?;
        let changed = pg.execute(&bump, &[&key(step, sizes.orders)]).await?;
        expect("a prepared point update", changed as i64, 1)?;
    }
    // Upserts that hit and miss, and inserts that hand back their key.
    for step in 0..sizes.point_ops / 10 {
        let id = if step % 2 == 0 {
            key(step, sizes.customers)
        } else {
            sizes.customers + step
        };
        native
            .query(
                "INSERT INTO customers VALUES ($1, 'upserted', 'Sofia', 1, 0, true, '2026-01-01 00:00:00') \
                 ON CONFLICT (id) DO UPDATE SET balance = customers.balance + 1",
                &[Param::I64(id)],
            )
            .await?;
    }
    for step in 0..sizes.point_ops / 10 {
        let id = sizes.orders + step;
        let returned = pg
            .query(
                "INSERT INTO orders VALUES ($1, 1, 9.99, 'new', NULL, '2026-01-01 00:00:00') RETURNING id",
                &[&id],
            )
            .await?;
        expect("an INSERT … RETURNING", returned.len() as i64, 1)?;
    }
    Ok(())
}

async fn transactions(native: &Client, pg: &tokio_postgres::Client, sizes: &Sizes) -> Result<()> {
    let base = sizes.orders + sizes.point_ops;
    for step in 0..sizes.point_ops / 10 {
        let tx = native.begin().await?;
        tx.query(
            "INSERT INTO orders VALUES ($1, $2, 25.00, 'new', 'in a transaction', '2026-02-01 00:00:00')",
            &[Param::I64(base + step), Param::I64(key(step, sizes.customers))],
        )
        .await?;
        tx.query(
            "UPDATE customers SET balance = balance - 25 WHERE id = $1",
            &[Param::I64(key(step, sizes.customers))],
        )
        .await?;
        if step % 5 == 4 {
            tx.rollback().await?;
        } else {
            tx.commit().await?;
        }
    }
    for step in 0..sizes.point_ops / 20 {
        let rollback = step % 4 == 3;
        let end = if rollback { "ROLLBACK" } else { "COMMIT" };
        pg.batch_execute(&format!(
            "BEGIN; UPDATE customers SET tier = tier WHERE id = {}; \
             UPDATE orders SET status = 'paid' WHERE id = {}; {end}",
            key(step, sizes.customers),
            key(step, sizes.orders)
        ))
        .await?;
    }
    Ok(())
}

/// Several clients at once on both protocols: point reads, point writes and
/// small scans, as an application server would send them.
async fn concurrent(server: &Server, native: &Arc<Client>, sizes: &Sizes) -> Result<()> {
    let customers = sizes.customers;
    let per_task = sizes.point_ops / 4;
    let mut tasks = tokio::task::JoinSet::new();
    for task in 0..6i64 {
        let pg = connect_pg(server).await?;
        tasks.spawn(async move {
            let read = pg
                .prepare("SELECT id, name, balance FROM customers WHERE id = $1")
                .await?;
            let write = pg
                .prepare("UPDATE customers SET active = NOT active WHERE id = $1")
                .await?;
            for step in 0..per_task {
                let id = key(step * 6 + task, customers);
                pg.query(&read, &[&id]).await?;
                if step % 4 == 0 {
                    pg.execute(&write, &[&id]).await?;
                }
                if step % 50 == 0 {
                    pg.simple_query("SELECT city, COUNT(*) FROM customers WHERE tier = 4 GROUP BY city")
                        .await?;
                }
            }
            Ok::<(), Error>(())
        });
    }
    for task in 0..4i64 {
        let native = Arc::clone(native);
        tasks.spawn(async move {
            for step in 0..per_task {
                let id = key(step * 4 + task + 1, customers);
                native
                    .query("SELECT id, city, tier FROM customers WHERE id = $1", &[Param::I64(id)])
                    .await?;
                if step % 4 == 1 {
                    native
                        .query(
                            "UPDATE customers SET balance = balance * 1.0 WHERE id = $1",
                            &[Param::I64(id)],
                        )
                        .await?;
                }
            }
            Ok::<(), Error>(())
        });
    }
    while let Some(joined) = tasks.join_next().await {
        joined??;
    }
    Ok(())
}

async fn bulk_writes(native: &Client, pg: &tokio_postgres::Client, sizes: &Sizes) -> Result<()> {
    let third = sizes.orders / 3;
    native
        .query(
            "UPDATE orders SET status = 'archived' WHERE status = 'shipped' AND id < $1",
            &[Param::I64(third)],
        )
        .await?;
    pg.execute(
        "UPDATE customers SET tier = 5 WHERE city = 'Riga' AND balance > 1200",
        &[],
    )
    .await?;
    let deleted = pg
        .execute(
            "DELETE FROM orders WHERE status = 'cancelled' AND id < $1",
            &[&(sizes.orders / 2)],
        )
        .await?;
    if deleted == 0 {
        return Err("the bulk DELETE removed nothing".into());
    }
    native
        .run("DELETE FROM events WHERE payload->>'kind' = 'logout'")
        .await?;
    Ok(())
}

async fn check_counts_after(
    native: &Client,
    pg: &tokio_postgres::Client,
    sizes: &Sizes,
) -> Result<()> {
    // Every cancelled order below half the original ids is gone.
    expect(
        "cancelled orders left below the cut",
        pg_count(
            pg,
            &format!(
                "SELECT COUNT(*) FROM orders WHERE status = 'cancelled' AND id < {}",
                sizes.orders / 2
            ),
        )
        .await?,
        0,
    )?;
    expect(
        "logout events left",
        native_count(
            native,
            "SELECT COUNT(*) FROM events WHERE payload->>'kind' = 'logout'",
        )
        .await?,
        0,
    )?;
    Ok(())
}
