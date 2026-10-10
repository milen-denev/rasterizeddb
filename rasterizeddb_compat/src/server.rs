//! Boots a RasterizedDB pgwire server in-process for tests.

use std::sync::OnceLock;

use rasterizeddb_core::{
    BATCH_SIZE, MAX_PERMITS_THREADS,
    auth::AuthMethod,
    core::{
        database::{DEFAULT_DATABASE_NAME, Database, catalog::DatabaseCatalog},
        processor::concurrent_processor::ENABLE_CACHE,
    },
    pgwire::{AuthConfig, TlsConfig, TlsMode, start_pgwire_with_catalog},
};
use rclite::Arc;

/// A running in-memory server, addressable over the Postgres wire protocol.
pub struct TestServer {
    port: u16,
    tls: bool,
    /// Fires when the harness is dropped, ending the server's `block_on`.
    ///
    /// Without it the server runs forever: `start_pgwire_with_catalog` never
    /// returns, so the thread, its runtime's four workers and the listening
    /// socket all outlive the test that made them.
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    /// The server's own thread. Held so it lives as long as the harness, and
    /// joined on drop so its threads are gone before the next test starts.
    handle: Option<std::thread::JoinHandle<()>>,
    database: Arc<Database>,
    catalog: Arc<DatabaseCatalog>,
}

impl TestServer {
    /// The engine behind this server.
    ///
    /// For assertions the wire protocol cannot express — counters, internal
    /// tables, version stamps. A test that can make its point through SQL should.
    pub fn database(&self) -> &Arc<Database> {
        &self.database
    }

    /// Every database this server serves, for the tests that create more than
    /// the default one.
    pub fn catalog(&self) -> &Arc<DatabaseCatalog> {
        &self.catalog
    }
}

/// Stops the server when the test that started it is done.
///
/// Each `TestServer` runs its own thread plus a four-worker runtime, every one
/// of them with a 32 MiB stack to match the product binary. Left running, a
/// forty-test suite ends up with two hundred live threads and several gigabytes
/// of reserved stack, and stalls partway through — while every test in it still
/// passes when run alone, which is what makes the cause so hard to see. Each
/// test must therefore give its threads back.
impl Drop for TestServer {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            // An error means the server thread is already gone, which is the
            // outcome this wants anyway.
            let _ = tx.send(());
        }
        if let Some(handle) = self.handle.take() {
            // A panic inside the server thread surfaces here as an error. It is
            // not this drop's business to report it — a test whose server died
            // has already failed on its own terms.
            let _ = handle.join();
        }
    }
}

/// The engine keeps its tuning knobs in process-wide `OnceLock`s that the real
/// binary sets from CLI flags. Tests set them once, on first use.
fn init_globals() {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        let _ = MAX_PERMITS_THREADS.set(4);
        let _ = BATCH_SIZE.set(10_000);
        let _ = ENABLE_CACHE.set(false);
        let _ = env_logger::builder().is_test(true).try_init();
    });
}

impl TestServer {
    /// Starts a fresh in-memory database on a free port, with TLS and
    /// authentication both off — the shape most tests want.
    pub async fn start() -> anyhow::Result<Self> {
        Self::start_with(TlsMode::Disable, AuthMethod::Trust, None).await
    }

    /// Starts a server with a specific transport and authentication setup.
    ///
    /// With TLS enabled and no certificate configured, a self-signed one is
    /// generated at start-up, which is what makes an encrypted connection
    /// possible without any prior setup.
    pub async fn start_with(
        tls_mode: TlsMode,
        method: AuthMethod,
        password: Option<&str>,
    ) -> anyhow::Result<Self> {
        Self::start_rooted(tls_mode, method, password, None).await
    }

    /// Starts a server whose catalogue lives in `root` on disk, for the tests
    /// that must exercise the file-backed storage path (the WAL, fsync, the
    /// mmap read path) rather than the in-memory one. The caller owns `root`
    /// and removes it.
    pub async fn start_file_backed(root: &str) -> anyhow::Result<Self> {
        Self::start_rooted(TlsMode::Disable, AuthMethod::Trust, None, Some(root)).await
    }

    async fn start_rooted(
        tls_mode: TlsMode,
        method: AuthMethod,
        password: Option<&str>,
        root: Option<&str>,
    ) -> anyhow::Result<Self> {
        init_globals();

        // Ask the OS for a free port, then hand the number to the server. The
        // listener is dropped first so the server can bind it; a collision
        // would only happen if something else grabbed the port in between.
        let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let port = probe.local_addr()?.port();
        drop(probe);

        // In-memory unless a root was given, so the catalogue holds only what
        // a test creates through it.
        let catalog = DatabaseCatalog::open(root, DEFAULT_DATABASE_NAME)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;
        // Kept so a test can look at engine-side counters that the wire does not
        // expose — the in-place update tally, for one.
        let database_handle = catalog
            .get(DEFAULT_DATABASE_NAME)
            .expect("the catalogue always holds its default database");
        let catalog_handle = catalog.clone();
        let auth = AuthConfig {
            method,
            bootstrap_user: "rasterized".to_string(),
            bootstrap_password: password.map(str::to_string),
        };
        // The server gets a runtime of its own, on threads with the same stack
        // the real binary gives them.
        //
        // `main.rs` sets 32 MiB deliberately: `execute_with_context` is one
        // large match whose future is the sum of every statement arm's locals,
        // and a statement that nests — a multi-row INSERT re-dispatching each
        // tuple, a foreign-key cascade — needs two such frames at once. On
        // tokio's 2 MiB default that overflows, so a harness running the server
        // on the ambient test runtime was exercising a configuration the product
        // does not ship, and failing on statements that work in production.
        //
        // Matching the binary is the point: this harness exists to test what is
        // deployed. It is not cover for unbounded growth — that would overflow
        // at 32 MiB too, and the day it does is the day to shrink the
        // dispatcher rather than raise this number.
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let handle = std::thread::Builder::new()
            .name("rasterizeddb-compat-server".to_string())
            .stack_size(32 * 1024 * 1024)
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(4)
                    .thread_stack_size(32 * 1024 * 1024)
                    .enable_all()
                    .build()
                    .expect("server runtime");
                rt.block_on(async move {
                    // No certificate path: a self-signed one is generated.
                    let tls = TlsConfig { mode: tls_mode, cert_path: None, key_path: None };
                    // The server never returns on its own, so the harness being
                    // dropped is what ends it. Dropping the accept loop's future
                    // is what closes the listener.
                    tokio::select! {
                        _ = start_pgwire_with_catalog(catalog, "127.0.0.1", port, tls, auth) => {}
                        _ = shutdown_rx => {}
                    }
                });
                // Connection tasks can still be parked in the runtime. Bounded,
                // because a client that never disconnects must not wedge the
                // suite — the point of this is to reclaim threads, and a hang
                // here would cost more than the leak did.
                rt.shutdown_timeout(std::time::Duration::from_secs(2));
            })
            .expect("spawn server thread");

        let server = Self {
            port,
            tls: !matches!(tls_mode, TlsMode::Disable),
            shutdown: Some(shutdown_tx),
            handle: Some(handle),
            database: database_handle,
            catalog: catalog_handle,
        };
        server.wait_until_accepting().await?;
        Ok(server)
    }

    /// libpq-style connection string for this server.
    pub fn url(&self) -> String {
        self.url_as("rasterized", None)
    }

    /// Connection string for a specific role and password.
    ///
    /// The certificate is self-signed, so verification is not requested: the
    /// point of `sslmode=require` here is an encrypted channel, not a proof of
    /// identity that a self-signed certificate cannot give.
    pub fn url_as(&self, user: &str, password: Option<&str>) -> String {
        self.url_for(DEFAULT_DATABASE_NAME, user, password)
    }

    /// Connection string naming a specific database, for the tests that serve
    /// more than one.
    pub fn url_for(&self, database: &str, user: &str, password: Option<&str>) -> String {
        let credentials = match password {
            Some(p) => format!("{}:{}", user, p),
            None => user.to_string(),
        };
        format!(
            "postgres://{}@127.0.0.1:{}/{}?sslmode={}",
            credentials,
            self.port,
            // The server routes on this name now, so it has to be one the
            // catalogue holds rather than a label.
            database,
            if self.tls { "require" } else { "disable" }
        )
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    /// Connects a raw sqlx pool.
    pub async fn connect_sqlx(&self) -> anyhow::Result<sqlx::PgPool> {
        Ok(sqlx::postgres::PgPoolOptions::new()
            .max_connections(4)
            .connect(&self.url())
            .await?)
    }

    /// Connects a pool to a database other than the default.
    pub async fn connect_sqlx_to(&self, database: &str) -> anyhow::Result<sqlx::PgPool> {
        Ok(sqlx::postgres::PgPoolOptions::new()
            .max_connections(4)
            .connect(&self.url_for(database, "rasterized", None))
            .await?)
    }

    /// Connects sea-orm.
    pub async fn connect_sea_orm(&self) -> anyhow::Result<sea_orm::DatabaseConnection> {
        Ok(sea_orm::Database::connect(self.url()).await?)
    }

    async fn wait_until_accepting(&self) -> anyhow::Result<()> {
        for _ in 0..200 {
            if tokio::net::TcpStream::connect(("127.0.0.1", self.port)).await.is_ok() {
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        anyhow::bail!("pgwire server did not start listening on port {}", self.port)
    }
}
