use byteorder::{ByteOrder, LittleEndian};
use rastcp::client::{TcpClient, TcpClientBuilder};
use rastcp::error::RastcpError;
use rastcp::session::ClientAuthenticator;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use cacheguard::CacheGuard;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

use crate::SERVER_PORT;
use crate::core::row::row::{self, ReturnResult};
use crate::core::support_types::QueryExecutionResult;
use crate::native_auth::PasswordAuthenticator;

const MIN_POOL_SIZE: usize = 1;
const DEFAULT_MAX_POOL_SIZE: usize = 10;
const RESERVED_CLIENT_FDS: usize = 128;
const HARD_MAX_POOL_SIZE: usize = 512;
const DEFAULT_TRANSPORT_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_CHECKOUT_TIMEOUT: Duration = Duration::from_secs(5);

fn rastcp_error_to_io_error(error: RastcpError) -> io::Error {
    match error {
        RastcpError::Io(err) => io::Error::new(err.kind(), err),
        RastcpError::ConnectionClosed => {
            io::Error::new(io::ErrorKind::UnexpectedEof, "Connection closed")
        }
        RastcpError::ConnectionTimeout
        | RastcpError::TlsHandshakeTimeout
        | RastcpError::HandlerTimeout => io::Error::new(io::ErrorKind::TimedOut, error.to_string()),
        RastcpError::ConnectionLimitReached | RastcpError::ConnectionBackpressure => {
            io::Error::new(io::ErrorKind::WouldBlock, error.to_string())
        }
        RastcpError::NotConnected => io::Error::new(io::ErrorKind::NotConnected, error.to_string()),
        RastcpError::InvalidMessageFormat
        | RastcpError::MessageTooLarge { .. }
        | RastcpError::ProtocolError(_) => {
            io::Error::new(io::ErrorKind::InvalidData, error.to_string())
        }
        RastcpError::CertificateLoading(_)
        | RastcpError::AuthenticationError(_)
        | RastcpError::SocketBindingError(_)
        | RastcpError::SocketListeningError(_)
        | RastcpError::SocketKeepAliveError(_) => {
            io::Error::new(io::ErrorKind::Other, error.to_string())
        }
        RastcpError::Tls(_) => io::Error::new(io::ErrorKind::ConnectionAborted, error.to_string()),
    }
}

fn is_retryable_transport_error(kind: io::ErrorKind) -> bool {
    matches!(
        kind,
        io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::BrokenPipe
            | io::ErrorKind::TimedOut
            | io::ErrorKind::UnexpectedEof
            | io::ErrorKind::NotConnected
    )
}

fn runtime_connection_budget() -> usize {
    #[cfg(unix)]
    {
        let mut limit = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };

        if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } == 0 {
            let soft_limit = if limit.rlim_cur == libc::RLIM_INFINITY {
                usize::MAX
            } else {
                limit.rlim_cur as usize
            };

            return soft_limit
                .saturating_sub(RESERVED_CLIENT_FDS)
                .saturating_div(2)
                .clamp(MIN_POOL_SIZE, HARD_MAX_POOL_SIZE);
        }
    }

    DEFAULT_MAX_POOL_SIZE
}

fn normalize_pool_limits_with_budget(
    requested_min_pool_size: usize,
    requested_max_pool_size: usize,
    connection_budget: usize,
) -> (usize, usize) {
    let normalized_budget = connection_budget.clamp(MIN_POOL_SIZE, HARD_MAX_POOL_SIZE);
    let max_pool_size = requested_max_pool_size
        .max(MIN_POOL_SIZE)
        .min(normalized_budget);
    let min_pool_size = requested_min_pool_size
        .max(MIN_POOL_SIZE)
        .min(max_pool_size);

    (min_pool_size, max_pool_size)
}

fn normalize_pool_limits(
    requested_min_pool_size: usize,
    requested_max_pool_size: usize,
) -> (usize, usize) {
    normalize_pool_limits_with_budget(
        requested_min_pool_size,
        requested_max_pool_size,
        runtime_connection_budget(),
    )
}

/// A pooled client connection
struct PooledClient {
    client: TcpClient,
    last_used: Instant,
}

/// The credentials a pool presents on every connection it opens, if any.
type Credentials = Option<Arc<dyn ClientAuthenticator>>;

/// Builds one connection, authenticating it if the pool has credentials.
///
/// Every path that opens a connection goes through here — first fill,
/// replacement after a transport error, and background replenishment — so a
/// connection cannot enter the pool having skipped authentication.
async fn open_connection(
    server_address: &str,
    transport_timeout: Duration,
    credentials: &Credentials,
) -> io::Result<TcpClient> {
    let mut builder = TcpClientBuilder::new(server_address)
        .verify_certificate(false)
        .timeout(transport_timeout);

    if let Some(credentials) = credentials {
        builder = builder.authenticator(credentials.clone());
    }

    let mut client = builder.build().await.map_err(rastcp_error_to_io_error)?;

    client.connect().await.map_err(rastcp_error_to_io_error)?;

    Ok(client)
}

impl PooledClient {
    async fn new(
        server_address: &str,
        transport_timeout: Duration,
        credentials: &Credentials,
    ) -> io::Result<Self> {
        Ok(PooledClient {
            client: open_connection(server_address, transport_timeout, credentials).await?,
            last_used: Instant::now(),
        })
    }

    async fn ensure_connected(
        &mut self,
        server_address: &str,
        transport_timeout: Duration,
        credentials: &Credentials,
    ) -> io::Result<()> {
        // Check connection and reconnect if needed
        if !self.client.is_connected() {
            self.client = open_connection(server_address, transport_timeout, credentials).await?;
        }
        self.last_used = Instant::now();
        Ok(())
    }
}

/// A client borrowed from the pool that returns to the pool when dropped
struct PooledClientGuard {
    client: Option<PooledClient>,
    pool: Arc<Mutex<Vec<PooledClient>>>,
    available_notify: Arc<Notify>,
    _permit: OwnedSemaphorePermit,
}

impl PooledClientGuard {
    fn new(
        client: PooledClient,
        pool: Arc<Mutex<Vec<PooledClient>>>,
        available_notify: Arc<Notify>,
        permit: OwnedSemaphorePermit,
    ) -> Self {
        PooledClientGuard {
            client: Some(client),
            pool,
            available_notify,
            _permit: permit,
        }
    }

    async fn send(&mut self, data: Vec<u8>) -> io::Result<Vec<u8>> {
        if let Some(client) = &mut self.client {
            client.last_used = Instant::now();
            client
                .client
                .send(data)
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::ConnectionAborted, e.to_string()))
        } else {
            Err(io::Error::new(
                io::ErrorKind::Other,
                "Client already returned to pool",
            ))
        }
    }
}

impl Drop for PooledClientGuard {
    fn drop(&mut self) {
        if let Some(client) = self.client.take() {
            let mut guard = self.pool.lock().expect("pool mutex poisoned");
            guard.push(client);
            drop(guard);
            self.available_notify.notify_one();
        }
    }
}

/// Connection pool for database clients
struct DbClientPool {
    available_clients: Arc<Mutex<Vec<PooledClient>>>,
    available_notify: Arc<Notify>,
    server_address: String,
    min_pool_size: usize,
    max_pool_size: usize,
    /// Padded: this is the only field of the pool that is written, and every
    /// other field on the line — the address, the size bounds, the three
    /// timeouts — is immutable and read on every checkout. One thread taking a
    /// connection was evicting the pool's whole configuration from every other
    /// thread's L1.
    current_size: CacheGuard<AtomicUsize>,
    checkout_permits: Arc<Semaphore>,
    transport_timeout: Duration,
    checkout_timeout: Duration,
    idle_timeout: Duration,
    credentials: Credentials,
}

impl DbClientPool {
    async fn new_with_server_address_and_timeouts(
        server_address: String,
        min_pool_size: usize,
        max_pool_size: usize,
        transport_timeout: Duration,
        checkout_timeout: Duration,
        credentials: Credentials,
    ) -> io::Result<Self> {
        let (min_pool_size, max_pool_size) = normalize_pool_limits(min_pool_size, max_pool_size);

        // Create initial connection pool
        let mut clients = Vec::with_capacity(min_pool_size);
        for _ in 0..min_pool_size {
            match PooledClient::new(&server_address, transport_timeout, &credentials).await {
                Ok(client) => clients.push(client),
                Err(e) => {
                    // If we can't create enough connections, return error
                    if clients.is_empty() {
                        return Err(e);
                    }
                    // Otherwise continue with fewer connections
                    break;
                }
            }
        }

        let initial_client_count = clients.len();

        Ok(DbClientPool {
            available_clients: Arc::new(Mutex::new(clients)),
            available_notify: Arc::new(Notify::new()),
            server_address,
            min_pool_size,
            max_pool_size,
            current_size: AtomicUsize::new(initial_client_count).into(),
            checkout_permits: Arc::new(Semaphore::new(max_pool_size)),
            transport_timeout,
            checkout_timeout,
            idle_timeout: Duration::from_secs(300), // 5 minutes
            credentials,
        })
    }

    /// Get a client from the pool or create a new one
    async fn get_client(&self) -> io::Result<PooledClientGuard> {
        let permit = self.checkout_permits.clone().acquire_owned();

        let permit = tokio::time::timeout(self.checkout_timeout, permit)
            .await
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::TimedOut,
                    "Timed out waiting for a pooled client",
                )
            })?
            .map_err(|_| io::Error::other("Client pool is shutting down"))?;

        let wait_deadline = Instant::now() + self.checkout_timeout;

        loop {
            let pooled_client = {
                let mut clients = self.available_clients.lock().expect("pool mutex poisoned");
                clients.pop()
            };

            if let Some(mut pooled_client) = pooled_client {
                if pooled_client
                    .ensure_connected(
                        &self.server_address,
                        self.transport_timeout,
                        &self.credentials,
                    )
                    .await
                    .is_ok()
                {
                    return Ok(PooledClientGuard::new(
                        pooled_client,
                        self.available_clients.clone(),
                        self.available_notify.clone(),
                        permit,
                    ));
                }

                self.current_size.fetch_sub(1, Ordering::AcqRel);
                continue;
            }

            if self.try_reserve_connection_slot() {
                match PooledClient::new(
                    &self.server_address,
                    self.transport_timeout,
                    &self.credentials,
                )
                .await
                {
                    Ok(pooled_client) => {
                        return Ok(PooledClientGuard::new(
                            pooled_client,
                            self.available_clients.clone(),
                            self.available_notify.clone(),
                            permit,
                        ));
                    }
                    Err(err) => {
                        self.current_size.fetch_sub(1, Ordering::AcqRel);
                        return Err(err);
                    }
                }
            }

            let remaining_wait = wait_deadline.saturating_duration_since(Instant::now());
            if remaining_wait.is_zero() {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "Timed out waiting for an available pooled client",
                ));
            }

            if tokio::time::timeout(remaining_wait, self.available_notify.notified())
                .await
                .is_err()
            {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "Timed out waiting for an available pooled client",
                ));
            }
        }
    }

    fn try_reserve_connection_slot(&self) -> bool {
        self.current_size
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current_size| {
                (current_size < self.max_pool_size).then_some(current_size + 1)
            })
            .is_ok()
    }

    /// Perform connection pool maintenance
    async fn maintain_pool(&self) -> io::Result<()> {
        let now = Instant::now();
        let checked_out_clients = self
            .max_pool_size
            .saturating_sub(self.checkout_permits.available_permits());
        let min_available_to_keep = self.min_pool_size.saturating_sub(checked_out_clients);

        let removed_clients = {
            let mut clients = self.available_clients.lock().expect("pool mutex poisoned");
            let current_len = clients.len();

            if current_len <= min_available_to_keep {
                0
            } else {
                let mut to_remove = Vec::new();
                for (index, client) in clients.iter().enumerate() {
                    if current_len - to_remove.len() <= min_available_to_keep {
                        break;
                    }

                    if now.duration_since(client.last_used) >= self.idle_timeout {
                        to_remove.push(index);
                    }
                }

                for index in to_remove.iter().rev() {
                    clients.remove(*index);
                }

                to_remove.len()
            }
        };

        if removed_clients > 0 {
            self.current_size
                .fetch_sub(removed_clients, Ordering::AcqRel);
        }

        let mut replenished_clients = Vec::new();
        let deficit = self
            .min_pool_size
            .saturating_sub(self.current_size.load(Ordering::Acquire));

        for _ in 0..deficit {
            match PooledClient::new(
                &self.server_address,
                self.transport_timeout,
                &self.credentials,
            )
            .await
            {
                Ok(client) => replenished_clients.push(client),
                Err(_) => break,
            }
        }

        if !replenished_clients.is_empty() {
            let replenished_count = replenished_clients.len();
            let mut clients = self.available_clients.lock().expect("pool mutex poisoned");
            clients.extend(replenished_clients);
            self.current_size
                .fetch_add(replenished_count, Ordering::AcqRel);
            self.available_notify.notify_waiters();
        }

        Ok(())
    }
}

/// Client for connecting to RasterizedDB
pub struct DbClient {
    pool: Arc<DbClientPool>,
    connected: AtomicBool,
}

/// Who a client authenticates as.
///
/// The password is used to answer a SCRAM challenge and is never sent: see
/// [`crate::native_auth`]. It is held in memory for the life of the client
/// because every reconnection has to authenticate again.
#[derive(Clone)]
pub struct DbCredentials {
    pub user: String,
    pub password: String,
}

impl DbCredentials {
    pub fn new(user: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            user: user.into(),
            password: password.into(),
        }
    }
}

impl std::fmt::Debug for DbCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DbCredentials")
            .field("user", &self.user)
            .finish_non_exhaustive()
    }
}

/// Completes an address with the default port, unless it already names one.
///
/// The server's port is configurable (`--rdb-port`), so an address has to be
/// able to carry one. Everything that came before passed a bare host and
/// expects [`SERVER_PORT`] to be appended, so that stays the default.
///
/// A bare IPv6 literal is bracketed on the way through: `::1` has colons of
/// its own, and appending `:61170` to it would produce something that parses
/// as neither an address nor a port.
fn resolve_server_address(address: &str) -> String {
    let address = address.trim();

    // Bracketed already: `[::1]:61170` carries a port, `[::1]` does not.
    if address.starts_with('[') {
        return if address.contains("]:") {
            address.to_string()
        } else {
            format!("{}:{}", address, SERVER_PORT)
        };
    }

    match address.rsplit_once(':') {
        // `host:port`, but only when what follows really is a port. Anything
        // else with a single colon is not a shape this accepts.
        Some((host, port))
            if !host.contains(':')
                && !port.is_empty()
                && port.bytes().all(|b| b.is_ascii_digit()) =>
        {
            address.to_string()
        }
        // More than one colon and no brackets: a bare IPv6 literal.
        Some(_) if address.matches(':').count() > 1 => {
            format!("[{}]:{}", address, SERVER_PORT)
        }
        _ => format!("{}:{}", address, SERVER_PORT),
    }
}

impl DbClient {
    /// Create a new database client connecting to the specified address.
    ///
    /// Accepts a bare host, which reaches the default port, or `host:port` for
    /// a server started with `--rdb-port`.
    ///
    /// Presents no credentials, which only reaches a server running without
    /// authentication. See [`DbClient::new_authenticated`].
    pub async fn new(addr: Option<&str>) -> io::Result<Self> {
        Self::with_options(
            addr,
            2,
            DEFAULT_MAX_POOL_SIZE,
            DEFAULT_TRANSPORT_TIMEOUT,
            DEFAULT_CHECKOUT_TIMEOUT,
            None,
        )
        .await
    }

    /// As [`DbClient::new`], logging in as `user`.
    pub async fn new_authenticated(
        addr: Option<&str>,
        user: &str,
        password: &str,
    ) -> io::Result<Self> {
        Self::with_options(
            addr,
            2,
            DEFAULT_MAX_POOL_SIZE,
            DEFAULT_TRANSPORT_TIMEOUT,
            DEFAULT_CHECKOUT_TIMEOUT,
            Some(DbCredentials::new(user, password)),
        )
        .await
    }

    /// Create a new database client with custom pool settings
    pub async fn with_pool_settings(
        addr: Option<&str>,
        min_pool_size: usize,
        max_pool_size: usize,
    ) -> io::Result<Self> {
        Self::with_pool_settings_and_timeouts(
            addr,
            min_pool_size,
            max_pool_size,
            DEFAULT_TRANSPORT_TIMEOUT,
            DEFAULT_CHECKOUT_TIMEOUT,
        )
        .await
    }

    pub async fn with_pool_settings_and_timeouts(
        addr: Option<&str>,
        min_pool_size: usize,
        max_pool_size: usize,
        transport_timeout: Duration,
        checkout_timeout: Duration,
    ) -> io::Result<Self> {
        Self::with_options(
            addr,
            min_pool_size,
            max_pool_size,
            transport_timeout,
            checkout_timeout,
            None,
        )
        .await
    }

    /// Every knob at once, including the credentials.
    ///
    /// The one place a pool is built, so the maintenance task and the
    /// credentials cannot be attached on some paths and forgotten on others.
    pub async fn with_options(
        addr: Option<&str>,
        min_pool_size: usize,
        max_pool_size: usize,
        transport_timeout: Duration,
        checkout_timeout: Duration,
        credentials: Option<DbCredentials>,
    ) -> io::Result<Self> {
        let address = addr.unwrap_or("127.0.0.1");
        let server_address = resolve_server_address(address);

        let credentials: Credentials = credentials.map(|c| {
            Arc::new(PasswordAuthenticator::new(c.user, c.password)) as Arc<dyn ClientAuthenticator>
        });

        let pool = DbClientPool::new_with_server_address_and_timeouts(
            server_address,
            min_pool_size,
            max_pool_size,
            transport_timeout,
            checkout_timeout,
            credentials,
        )
        .await?;

        // Start a background task to maintain the pool
        let pool_arc = Arc::new(pool);
        let pool_ref = Arc::clone(&pool_arc);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                interval.tick().await;
                let _ = pool_ref.maintain_pool().await;
            }
        });

        Ok(DbClient {
            pool: pool_arc,
            connected: AtomicBool::new(true),
        })
    }

    /// Connect to the database server
    pub async fn connect(&self) -> io::Result<()> {
        if !self.connected.load(Ordering::Relaxed) {
            // Get a client to verify connectivity
            let _client = self.pool.get_client().await?;
            self.connected.store(true, Ordering::Relaxed);
        }
        Ok(())
    }

    /// Close the connection to the database server
    pub async fn close(&self) -> io::Result<()> {
        // We don't actually close all connections, just mark as disconnected
        // Individual connections will be cleaned up by the maintenance task
        self.connected.store(false, Ordering::Relaxed);
        Ok(())
    }

    /// Execute an RQL query and return the result
    pub async fn execute_query(&self, query: &str) -> io::Result<QueryExecutionResult> {
        // Make sure we're connected
        if !self.connected.load(Ordering::Relaxed) {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "Client is disconnected",
            ));
        }

        let query_bytes = query.as_bytes().to_vec();
        let mut last_error = None;

        for attempt in 0..=1 {
            let mut client_guard = self.pool.get_client().await?;

            match client_guard.send(query_bytes.clone()).await {
                Ok(response) => return Self::parse_query_response(response),
                Err(err) if attempt == 0 && is_retryable_transport_error(err.kind()) => {
                    last_error = Some(err);
                    continue;
                }
                Err(err) => return Err(err),
            }
        }

        Err(last_error.unwrap_or_else(|| io::Error::other("Query execution failed")))
    }

    fn parse_query_response(response: Vec<u8>) -> io::Result<QueryExecutionResult> {
        // Parse the response
        if response.is_empty() {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Empty response"));
        }

        // Get the result type from the first byte
        match response[0] {
            0 => Ok(QueryExecutionResult::Ok),
            1 => {
                if response.len() < 9 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Invalid rows affected response",
                    ));
                }
                let rows_affected = LittleEndian::read_u64(&response[1..9]);
                Ok(QueryExecutionResult::RowsAffected(rows_affected))
            }
            2 => {
                // Extract the serialized rows data (everything after the marker byte)
                let rows_data = response[1..].to_vec();
                Ok(QueryExecutionResult::RowsResult(Box::new(rows_data)))
            }
            3 => {
                // Error message is a UTF-8 string after the marker byte
                let error_message = String::from_utf8(response[1..].to_vec()).map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "Invalid error message")
                })?;
                Ok(QueryExecutionResult::Error(error_message))
            }
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid result type marker",
            )),
        }
    }

    /// Helper method to extract rows from a QueryExecutionResult
    pub fn extract_rows(result: QueryExecutionResult) -> io::Result<Option<ReturnResult>> {
        match result {
            QueryExecutionResult::RowsResult(data) => row::vec_into_rows(&data)
                .map(|row| Some(ReturnResult::Rows(row)))
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string())),
            QueryExecutionResult::Error(msg) => Err(io::Error::new(io::ErrorKind::Other, msg)),
            _ => Ok(None),
        }
    }

    /// Check if the client is connected
    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Relaxed)
    }

    /// Force reconnection to the server
    pub async fn reconnect(&self) -> io::Result<()> {
        self.close().await?;
        self.connect().await
    }

    /// Get current pool stats
    pub fn get_pool_stats(&self) -> (usize, usize) {
        let current_size = self.pool.current_size.load(Ordering::Relaxed);
        let max_size = self.pool.max_pool_size;
        (current_size, max_size)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rastcp::server::TcpServerBuilder;
    use std::net::TcpListener;

    #[test]
    fn normalize_pool_limits_caps_requested_max_to_budget() {
        let (min_pool_size, max_pool_size) = normalize_pool_limits_with_budget(10, 5_000, 32);

        assert_eq!(min_pool_size, 10);
        assert_eq!(max_pool_size, 32);
    }

    #[test]
    fn normalize_pool_limits_keeps_min_within_max() {
        let (min_pool_size, max_pool_size) = normalize_pool_limits_with_budget(64, 4, 128);

        assert_eq!(min_pool_size, 4);
        assert_eq!(max_pool_size, 4);
    }

    #[tokio::test]
    async fn execute_query_retries_on_timed_out_transport() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_clone = attempts.clone();

        let server = TcpServerBuilder::new("127.0.0.1", port)
            .request_timeout(Duration::from_secs(1))
            .build()
            .await
            .unwrap();

        let server_handle = tokio::spawn(async move {
            server
                .run(move |_| {
                    let attempts = attempts_clone.clone();
                    async move {
                        let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                        if attempt == 0 {
                            // Comfortably past the client's budget below. What
                            // the test needs is only that this stall outlasts
                            // the timeout; the absolute values are chosen so the
                            // *handshake*, which shares that budget, still fits
                            // on a loaded machine.
                            tokio::time::sleep(Duration::from_millis(1_500)).await;
                        }
                        vec![0]
                    }
                })
                .await
                .unwrap();
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        let client = DbClient {
            pool: Arc::new(
                DbClientPool::new_with_server_address_and_timeouts(
                    format!("127.0.0.1:{port}"),
                    1,
                    4,
                    // 500 ms rather than 50. The stall above is 3x this, which
                    // is all the retry needs — but the TLS handshake is billed
                    // against the same budget, and 50 ms is not enough for one
                    // on a saturated box. It failed twice in twelve full-suite
                    // runs under 24x CPU load, always as "TLS handshake
                    // timeout" and never as a wrong retry count: a ceiling that
                    // can only be missed in the false-failure direction.
                    Duration::from_millis(500),
                    Duration::from_secs(5),
                    None,
                )
                .await
                .unwrap(),
            ),
            connected: AtomicBool::new(true),
        };

        let result = client.execute_query("SELECT 1").await.unwrap();

        assert!(matches!(result, QueryExecutionResult::Ok));
        assert_eq!(attempts.load(Ordering::SeqCst), 2);

        server_handle.abort();
    }

    #[tokio::test]
    async fn get_client_waits_for_released_connection_when_pool_is_saturated() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        let server = TcpServerBuilder::new("127.0.0.1", port)
            .max_connections(4)
            .build()
            .await
            .unwrap();

        let server_handle = tokio::spawn(async move {
            server
                .run(|_| async move {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    vec![0]
                })
                .await
                .unwrap();
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        let pool = Arc::new(
            DbClientPool::new_with_server_address_and_timeouts(
                format!("127.0.0.1:{port}"),
                1,
                1,
                DEFAULT_TRANSPORT_TIMEOUT,
                DEFAULT_CHECKOUT_TIMEOUT,
                None,
            )
            .await
            .unwrap(),
        );

        let first_guard = pool.get_client().await.unwrap();
        let waiting_pool = pool.clone();
        let waiting_task = tokio::spawn(async move { waiting_pool.get_client().await });

        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(!waiting_task.is_finished());

        drop(first_guard);

        let second_guard = tokio::time::timeout(Duration::from_secs(2), waiting_task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        drop(second_guard);

        server_handle.abort();
    }
}

#[cfg(test)]
mod address_tests {
    use super::{SERVER_PORT, resolve_server_address};

    #[test]
    fn a_bare_host_gets_the_default_port() {
        assert_eq!(
            resolve_server_address("127.0.0.1"),
            format!("127.0.0.1:{SERVER_PORT}")
        );
        assert_eq!(
            resolve_server_address("db.internal"),
            format!("db.internal:{SERVER_PORT}")
        );
    }

    #[test]
    fn an_explicit_port_is_kept() {
        assert_eq!(resolve_server_address("127.0.0.1:7000"), "127.0.0.1:7000");
        assert_eq!(
            resolve_server_address("db.internal:7000"),
            "db.internal:7000"
        );
    }

    #[test]
    fn a_bare_ipv6_literal_is_bracketed_before_the_port_is_appended() {
        // Appending to the bare literal would give `::1:61170`, which parses
        // as neither an address nor a port.
        assert_eq!(
            resolve_server_address("::1"),
            format!("[::1]:{SERVER_PORT}")
        );
        assert_eq!(
            resolve_server_address("fe80::1"),
            format!("[fe80::1]:{SERVER_PORT}")
        );
    }

    #[test]
    fn a_bracketed_ipv6_keeps_or_gains_a_port() {
        assert_eq!(resolve_server_address("[::1]:7000"), "[::1]:7000");
        assert_eq!(
            resolve_server_address("[::1]"),
            format!("[::1]:{SERVER_PORT}")
        );
    }

    #[test]
    fn surrounding_whitespace_does_not_become_part_of_the_host() {
        assert_eq!(
            resolve_server_address("  127.0.0.1  "),
            format!("127.0.0.1:{SERVER_PORT}")
        );
    }
}
