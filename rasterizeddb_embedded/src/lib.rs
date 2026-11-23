//! RasterizedDB inside your process.
//!
//! [`Builder::start`] opens a database (a directory, or memory) and runs the
//! full server engine on a thread of its own, the way the `rasterizeddb_core`
//! binary runs it. It then serves the native protocol on a private endpoint,
//! and [`Embedded::connect`] hands back a [`rasterizeddb_client::Client`]
//! for it. Everything the client SDK offers works here unchanged: typed
//! queries, transactions, the ORM, migrations and the key/value API.
//!
//! ```no_run
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! use rasterizeddb_embedded::Builder;
//!
//! let db = Builder::open("./data").start().await?;
//! let client = db.connect().await?;
//! client.execute("CREATE TABLE IF NOT EXISTS notes (id BIGINT, body TEXT)", &[]).await?;
//! client.execute("INSERT INTO notes VALUES (1, 'hello')", &[]).await?;
//! let rows = client.query("SELECT body FROM notes WHERE id = 1", &[]).await?;
//! # let _ = rows;
//! db.shutdown().await?;
//! # Ok(()) }
//! ```
//!
//! # Why a thread and an endpoint of its own
//!
//! - **Stacks.** The engine runs every statement on 32 MiB stacks, as the
//!   server binary does; on tokio's 2 MiB default a nested statement can
//!   overflow. Its own runtime gives it those stacks whatever runtime (or
//!   none) the application uses.
//! - **One code path.** Statements reach the engine through the same native
//!   session layer a network client's do (transactions, views, sequences,
//!   functions, privileges), so an embedded database answers exactly as a
//!   server would.
//! - **Signals stay yours.** Nothing here installs a Ctrl+C or SIGTERM
//!   handler. Stop the engine with [`Embedded::shutdown`]: it drains the
//!   requests in flight, seals the index snapshots and checkpoints the
//!   write-ahead log, so the next open has nothing to replay. Dropping an
//!   [`Embedded`] does the same, blocking the dropping thread until it is
//!   done.
//!
//! # The endpoint
//!
//! On unix it is a socket in a fresh `0700` directory under the temporary
//! directory, with the socket file `0600`. Only processes of the same
//! operating-system user can open it, so no password is needed. On other
//! platforms (Windows) there are no unix sockets, and the endpoint is a TLS
//! listener on `127.0.0.1` with a port the system picks. Any local process can
//! reach that port, so [`Builder::password`] is required there unless
//! [`Builder::allow_unauthenticated_loopback`] says otherwise.
//!
//! # CPU
//!
//! On x86-64 the engine needs AVX2 (an x86-64-v3 CPU: Intel Haswell, AMD
//! Excavator or later). [`Builder::start`] refuses on a CPU without it rather
//! than fail later with an illegal instruction. Build with
//! `-C target-cpu=x86-64-v3` (as the server binary is) so the vector paths
//! inline. aarch64 needs nothing extra.

use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use rasterizeddb_core::auth::AuthMethod;
use rasterizeddb_core::core::database::catalog::DatabaseCatalog;
use rasterizeddb_core::core::database::{DEFAULT_DATABASE_NAME, Database, NativeListen};
use rasterizeddb_core::native_auth::NativeAuthConfig;
use tokio::sync::oneshot;

pub use rasterizeddb_client as client;
pub use rasterizeddb_client::{Client, Login, Options};
/// The engine itself, for what the client cannot express (counters, internal
/// state). Its API is the server's internals and changes between releases.
pub use rasterizeddb_core as engine;

/// The stack every engine thread gets, as in the server binary.
const ENGINE_STACK: usize = 32 * 1024 * 1024;

/// The role a fresh database is given when [`Builder::password`] names none.
const DEFAULT_USER: &str = "rasterized";

enum Location {
    Memory,
    Directory(PathBuf),
}

/// What to open and how to serve it. Start with [`Builder::open`] or
/// [`Builder::in_memory`].
pub struct Builder {
    location: Location,
    worker_threads: Option<usize>,
    credentials: Option<(String, String)>,
    allow_unauthenticated_loopback: bool,
    request_timeout: Option<Duration>,
}

impl Builder {
    /// A database kept in `directory`, created if it does not exist. A
    /// directory written by the server binary opens here, and the reverse.
    /// One process at a time may open a directory.
    pub fn open(directory: impl Into<PathBuf>) -> Self {
        Self::new(Location::Directory(directory.into()))
    }

    /// A database held in memory only, gone when the engine stops.
    pub fn in_memory() -> Self {
        Self::new(Location::Memory)
    }

    fn new(location: Location) -> Self {
        Builder {
            location,
            worker_threads: None,
            credentials: None,
            allow_unauthenticated_loopback: false,
            request_timeout: None,
        }
    }

    /// Threads the engine's runtime runs on (default: one per CPU, as
    /// tokio chooses).
    pub fn worker_threads(mut self, threads: usize) -> Self {
        self.worker_threads = Some(threads.max(1));
        self
    }

    /// Require a SCRAM-SHA-256 login as `user`. A fresh database gets that
    /// role with this password; an existing one must already have it.
    /// Required off unix (see the crate documentation).
    pub fn password(mut self, user: impl Into<String>, password: impl Into<String>) -> Self {
        self.credentials = Some((user.into(), password.into()));
        self
    }

    /// Off unix, serve the loopback endpoint without authentication, so that
    /// any local process can use the database. Ignored on unix, whose socket
    /// admits this user only.
    pub fn allow_unauthenticated_loopback(mut self, allow: bool) -> Self {
        self.allow_unauthenticated_loopback = allow;
        self
    }

    /// How long one statement may run before it is abandoned (default
    /// 30 s, the server's). A connection idle this long is closed too; the
    /// client reopens it before its next statement.
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = Some(timeout);
        self
    }

    /// Opens the database and starts serving it. Callable from any async
    /// runtime: the engine runs on threads of its own.
    pub async fn start(self) -> io::Result<Embedded> {
        let (ready_rx, embedded) = self.launch()?;
        match ready_rx.await {
            Ok(Ok(ready)) => Ok(embedded.ready(ready)),
            Ok(Err(e)) => Err(embedded.failed(e)),
            Err(_) => Err(embedded.failed(io::Error::other("the engine thread ended before it started"))),
        }
    }

    /// [`Builder::start`] for a caller outside any async runtime. Must not be
    /// called from inside one (it blocks until the engine is up).
    pub fn start_blocking(self) -> io::Result<Embedded> {
        let (ready_rx, embedded) = self.launch()?;
        match ready_rx.blocking_recv() {
            Ok(Ok(ready)) => Ok(embedded.ready(ready)),
            Ok(Err(e)) => Err(embedded.failed(e)),
            Err(_) => Err(embedded.failed(io::Error::other("the engine thread ended before it started"))),
        }
    }

    fn launch(self) -> io::Result<(oneshot::Receiver<io::Result<Ready>>, Embedded)> {
        let missing = rasterizeddb_core::simds::cpu::features().baseline_missing();
        if !missing.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!(
                    "RasterizedDB needs an x86-64-v3 CPU (AVX2, BMI1/2, FMA, LZCNT, MOVBE); this one lacks: {}",
                    missing.join(", ")
                ),
            ));
        }
        if rasterizeddb_core::TEST_ONLY_FEATURES.first().is_some() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!(
                    "rasterizeddb_core was built with the test-only `{}` feature, which a database must not run with",
                    rasterizeddb_core::TEST_ONLY_FEATURES[0]
                ),
            ));
        }

        let root = match &self.location {
            Location::Memory => None,
            Location::Directory(dir) => {
                std::fs::create_dir_all(dir)?;
                Some(
                    dir.to_str()
                        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "the database path is not UTF-8"))?
                        .to_string(),
                )
            }
        };

        let (auth, login) = match &self.credentials {
            Some((user, password)) => (
                NativeAuthConfig {
                    method: AuthMethod::ScramSha256,
                    bootstrap_user: user.clone(),
                    bootstrap_password: Some(password.clone()),
                },
                Login::password(user.clone(), password.clone()),
            ),
            None => {
                if cfg!(not(unix)) && !self.allow_unauthenticated_loopback {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "off unix the embedded endpoint is a loopback TCP port any local process can reach: \
                         set Builder::password, or Builder::allow_unauthenticated_loopback(true)",
                    ));
                }
                (
                    NativeAuthConfig {
                        method: AuthMethod::Trust,
                        bootstrap_user: DEFAULT_USER.to_string(),
                        bootstrap_password: None,
                    },
                    Login::Trust,
                )
            }
        };

        let private_dir = private_socket_dir()?;
        let listen = NativeListen {
            addr: "127.0.0.1".to_string(),
            port: 0,
            tcp: private_dir.is_none(),
            socket: private_dir.as_ref().map(|dir| {
                let mut socket = rasterizeddb_core::core::database::NativeSocket::new(dir.join("native.sock"));
                socket.mode = 0o600;
                socket
            }),
            request_timeout: self.request_timeout,
            certificate: None,
        };

        let (ready_tx, ready_rx) = oneshot::channel();
        let (stop_tx, stop_rx) = oneshot::channel();
        let (done_tx, done_rx) = oneshot::channel();
        let workers = self.worker_threads;
        let spawned = std::thread::Builder::new()
            .name("rasterizeddb-engine".to_string())
            .stack_size(ENGINE_STACK)
            .spawn(move || {
                let result = engine_thread(root, auth, listen, workers, ready_tx, stop_rx);
                let _ = done_tx.send(result);
            });
        let thread = match spawned {
            Ok(thread) => thread,
            Err(e) => {
                remove_private_dir(&private_dir);
                return Err(e);
            }
        };
        Ok((
            ready_rx,
            Embedded {
                endpoint: String::new(),
                login,
                catalog: None,
                stop: Some(stop_tx),
                done: Some(done_rx),
                thread: Some(thread),
                private_dir,
            },
        ))
    }
}

struct Ready {
    endpoint: String,
    catalog: rclite::Arc<DatabaseCatalog>,
}

fn engine_thread(
    root: Option<String>,
    auth: NativeAuthConfig,
    listen: NativeListen,
    workers: Option<usize>,
    ready_tx: oneshot::Sender<io::Result<Ready>>,
    stop_rx: oneshot::Receiver<()>,
) -> io::Result<()> {
    let mut runtime = tokio::runtime::Builder::new_multi_thread();
    runtime
        .thread_name("rasterizeddb-worker")
        .thread_stack_size(ENGINE_STACK)
        .enable_all();
    if let Some(workers) = workers {
        runtime.worker_threads(workers);
    }
    let runtime = match runtime.build() {
        Ok(runtime) => runtime,
        Err(e) => {
            let _ = ready_tx.send(Err(io::Error::new(e.kind(), e.to_string())));
            return Err(e);
        }
    };
    let result = runtime.block_on(async move {
        let started = async {
            let catalog = DatabaseCatalog::open(root.as_deref(), DEFAULT_DATABASE_NAME)
                .await
                .map_err(|e| io::Error::other(format!("could not open the database: {e}")))?;
            let server = Database::bind_native_in(catalog.clone(), auth, listen).await?;
            let endpoint = server
                .socket_endpoint()
                .or_else(|| server.tcp_endpoint())
                .ok_or_else(|| io::Error::other("the engine bound no endpoint"))?;
            io::Result::Ok((catalog, server, endpoint))
        };
        let (catalog, server, endpoint) = match started.await {
            Ok(started) => started,
            Err(e) => {
                let _ = ready_tx.send(Err(io::Error::new(e.kind(), e.to_string())));
                return Err(e);
            }
        };
        let handles = server.shutdown_handles();
        let serving = tokio::spawn(server.run());
        let _ = ready_tx.send(Ok(Ready {
            endpoint,
            catalog: catalog.clone(),
        }));

        // A stop request, or the Embedded gone without one: either way, stop.
        let _ = stop_rx.await;
        for handle in &handles {
            handle.shutdown();
        }
        let served = match serving.await {
            Ok(result) => result,
            Err(e) => Err(io::Error::other(format!("the native listener panicked: {e}"))),
        };
        // The listeners have drained: the clean point the server binary
        // seals and checkpoints at.
        rasterizeddb_core::shutdown::seal_all(&catalog).await;
        rasterizeddb_core::shutdown::finalize();
        served
    });
    // Connection tasks may still be parked; bounded so a client that never
    // disconnects cannot hold the host's shutdown.
    runtime.shutdown_timeout(Duration::from_secs(5));
    result
}

/// A running embedded database. Stop it with [`Embedded::shutdown`] (or by
/// dropping it).
pub struct Embedded {
    endpoint: String,
    login: Login,
    catalog: Option<rclite::Arc<DatabaseCatalog>>,
    stop: Option<oneshot::Sender<()>>,
    done: Option<oneshot::Receiver<io::Result<()>>>,
    thread: Option<std::thread::JoinHandle<()>>,
    private_dir: Option<PathBuf>,
}

impl Embedded {
    fn ready(mut self, ready: Ready) -> Self {
        self.endpoint = ready.endpoint;
        self.catalog = Some(ready.catalog);
        self
    }

    /// The thread is ending (or never started): reap it and its directory.
    fn failed(mut self, e: io::Error) -> io::Error {
        self.stop.take();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
        remove_private_dir(&self.private_dir.take());
        e
    }

    /// The address a [`Client`] connects to: `unix:/…/native.sock`, or
    /// `127.0.0.1:<port>` off unix.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// The [`Options`] [`Embedded::connect`] uses: the client's defaults with
    /// this database's login. Adjust and pass to [`Embedded::connect_with`].
    pub fn options(&self) -> Options {
        Options {
            login: self.login.clone(),
            ..Options::default()
        }
    }

    /// A client for this database, with [`Embedded::options`]. Clients are
    /// cheap to clone and share; one usually serves a whole application.
    pub async fn connect(&self) -> Result<Client, rasterizeddb_client::Error> {
        Client::connect(&self.endpoint, self.options()).await
    }

    /// A client with `options` (connections, timeouts, migrations, …).
    /// Their `login` is used as given.
    pub async fn connect_with(&self, options: Options) -> Result<Client, rasterizeddb_client::Error> {
        Client::connect(&self.endpoint, options).await
    }

    /// The engine's catalogue of databases, for what SQL cannot reach.
    pub fn catalog(&self) -> &rclite::Arc<DatabaseCatalog> {
        self.catalog.as_ref().expect("a started Embedded has its catalogue")
    }

    /// Stops the engine: no new connections, requests in flight finish,
    /// index snapshots are sealed and the write-ahead log is checkpointed.
    /// Returns what the listener ended with.
    pub async fn shutdown(mut self) -> io::Result<()> {
        self.stop.take().map(|stop| stop.send(()));
        let result = match self.done.take() {
            Some(done) => done
                .await
                .unwrap_or_else(|_| Err(io::Error::other("the engine thread panicked"))),
            None => Ok(()),
        };
        // It has sent its result, so it is about to end; joining is brief.
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
        remove_private_dir(&self.private_dir.take());
        result
    }

    /// [`Embedded::shutdown`] for a caller outside any async runtime.
    pub fn shutdown_blocking(mut self) -> io::Result<()> {
        self.stop.take().map(|stop| stop.send(()));
        let result = match self.done.take() {
            Some(done) => done
                .blocking_recv()
                .unwrap_or_else(|_| Err(io::Error::other("the engine thread panicked"))),
            None => Ok(()),
        };
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
        remove_private_dir(&self.private_dir.take());
        result
    }
}

impl Drop for Embedded {
    fn drop(&mut self) {
        // Dropping the sender is the stop request; the join waits for the
        // seal and checkpoint so the directory is left clean.
        self.stop.take();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
        remove_private_dir(&self.private_dir.take());
    }
}

/// A fresh `0700` directory for the socket, short enough for a socket path
/// (at most 107 bytes). `None` off unix.
#[cfg(unix)]
fn private_socket_dir() -> io::Result<Option<PathBuf>> {
    use std::os::unix::fs::DirBuilderExt;
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let base = std::env::temp_dir();
    let base = if base.as_os_str().len() > 80 { PathBuf::from("/tmp") } else { base };
    for _ in 0..16 {
        let dir = base.join(format!(
            "rdb-embedded-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        match std::fs::DirBuilder::new().mode(0o700).create(&dir) {
            Ok(()) => return Ok(Some(dir)),
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(e),
        }
    }
    Err(io::Error::other("could not create a private directory for the embedded socket"))
}

#[cfg(not(unix))]
fn private_socket_dir() -> io::Result<Option<PathBuf>> {
    let _ = (&AtomicU64::new(0), Ordering::Relaxed);
    Ok(None)
}

fn remove_private_dir(dir: &Option<PathBuf>) {
    if let Some(dir) = dir {
        let _ = std::fs::remove_dir_all(dir);
    }
}
