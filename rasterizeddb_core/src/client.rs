use byteorder::{ByteOrder, LittleEndian};
use rastcp::client::{TcpClient, TcpClientBuilder};
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

use crate::SERVER_PORT;
use crate::core::database::QueryExecutionResult;
use crate::core::row_v2::row::{self, ReturnResult};

/// A pooled client connection
struct PooledClient {
    client: TcpClient,
    last_used: Instant,
}

impl PooledClient {
    async fn new(server_address: &str) -> io::Result<Self> {
        let mut client = TcpClientBuilder::new(server_address)
            .verify_certificate(false)
            .timeout(Duration::from_secs(30))
            .build()
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::ConnectionRefused, e.to_string()))?;

        client
            .connect()
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::ConnectionRefused, e.to_string()))?;

        Ok(PooledClient {
            client,
            last_used: Instant::now(),
        })
    }

    async fn ensure_connected(&mut self, server_address: &str) -> io::Result<()> {
        // Check connection and reconnect if needed
        if !self.client.is_connected() {
            self.client = TcpClientBuilder::new(server_address)
                .verify_certificate(false)
                .timeout(Duration::from_secs(30))
                .build()
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::ConnectionRefused, e.to_string()))?;

            self.client
                .connect()
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::ConnectionRefused, e.to_string()))?;
        }
        self.last_used = Instant::now();
        Ok(())
    }
}

/// A client borrowed from the pool that returns to the pool when dropped
struct PooledClientGuard {
    client: Option<PooledClient>,
    pool: Arc<Mutex<Vec<PooledClient>>>,
}

impl PooledClientGuard {
    fn new(client: PooledClient, pool: Arc<Mutex<Vec<PooledClient>>>) -> Self {
        PooledClientGuard {
            client: Some(client),
            pool,
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
        // Take ownership of the client (if any) to return it to the pool
        if let Some(client) = self.client.take() {
            // Use a blocking operation to return the client to the pool
            // This is acceptable in a Drop implementation
            let pool = self.pool.clone();
            let _ = tokio::task::block_in_place(|| {
                futures::executor::block_on(async {
                    let mut guard = pool.lock().await;
                    guard.push(client);
                })
            });
        }
    }
}

/// Connection pool for database clients
struct DbClientPool {
    available_clients: Arc<Mutex<Vec<PooledClient>>>,
    server_address: String,
    min_pool_size: usize,
    max_pool_size: usize,
    current_size: AtomicUsize,
    _connection_timeout: Duration,
    idle_timeout: Duration,
}

impl DbClientPool {
    /// Create a new client pool
    async fn new(
        addr: Option<&str>,
        min_pool_size: usize,
        max_pool_size: usize,
    ) -> io::Result<Self> {
        let address = addr.unwrap_or("127.0.0.1");
        let server_address = format!("{}:{}", address, SERVER_PORT);

        // Create initial connection pool
        let mut clients = Vec::with_capacity(min_pool_size);
        for _ in 0..min_pool_size {
            match PooledClient::new(&server_address).await {
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

        Ok(DbClientPool {
            available_clients: Arc::new(Mutex::new(clients)),
            server_address,
            min_pool_size,
            max_pool_size,
            current_size: AtomicUsize::new(min_pool_size),
            _connection_timeout: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(300), // 5 minutes
        })
    }

    /// Get a client from the pool or create a new one
    async fn get_client(&self) -> io::Result<PooledClientGuard> {
        let mut clients_opt = None;

        // Try to get a client from the pool
        {
            let mut clients = self.available_clients.lock().await;
            if !clients.is_empty() {
                // Take a client from the pool if available
                let client = clients.remove(0);
                clients_opt = Some(client);
            } else {
                // Check if we can expand the pool
                let current_size = self.current_size.load(Ordering::Acquire);
                if current_size < self.max_pool_size {
                    // Create a new client and add it to the pool
                    let new_client = PooledClient::new(&self.server_address).await?;
                    clients_opt = Some(new_client);
                    self.current_size.fetch_add(1, Ordering::Release);
                }
            }
        }

        // If we couldn't get a client immediately and the pool is at max capacity,
        // wait for one to become available
        if clients_opt.is_none() {
            let mut retry_count = 0;
            while retry_count < 10 {
                tokio::time::sleep(Duration::from_millis(100)).await;

                let mut clients = self.available_clients.lock().await;
                if !clients.is_empty() {
                    clients_opt = Some(clients.remove(0));
                    break;
                }

                retry_count += 1;
            }

            if clients_opt.is_none() {
                return Err(io::Error::new(
                    io::ErrorKind::ResourceBusy,
                    "No available connections in the pool",
                ));
            }
        }

        // At this point, we should definitely have a client
        let mut client_to_use = clients_opt.unwrap();

        // Ensure the connection is active
        if let Err(_) = client_to_use.ensure_connected(&self.server_address).await {
            // If connection fails, create a new one
            client_to_use = PooledClient::new(&self.server_address).await?;
        }

        // Create a guard that will return the client to the pool when dropped
        Ok(PooledClientGuard::new(
            client_to_use,
            self.available_clients.clone(),
        ))
    }

    /// Perform connection pool maintenance
    async fn maintain_pool(&self) -> io::Result<()> {
        let mut clients = self.available_clients.lock().await;
        let now = Instant::now();

        // Store current length before modification
        let current_len = clients.len();

        // Calculate minimum connections to keep
        let min_to_keep = self.min_pool_size;

        // Remove idle connections exceeding min_pool_size
        if current_len > min_to_keep {
            // Pre-determine which clients to keep based on last_used time
            let mut to_remove = Vec::new();
            for i in 0..clients.len() {
                if clients.len() - to_remove.len() > min_to_keep
                    && now.duration_since(clients[i].last_used) >= self.idle_timeout
                {
                    to_remove.push(i);
                }
            }

            // Remove clients in reverse order to maintain correct indices
            for &i in to_remove.iter().rev() {
                clients.remove(i);
            }

            // Update the current size counter
            if clients.len() != current_len {
                self.current_size.store(clients.len(), Ordering::Release);
            }
        }

        // Ensure we have at least min_pool_size connections
        let current_len = clients.len();
        if current_len < min_to_keep {
            for _ in current_len..min_to_keep {
                match PooledClient::new(&self.server_address).await {
                    Ok(client) => clients.push(client),
                    Err(_) => break,
                }
            }
            self.current_size.store(clients.len(), Ordering::Release);
        }

        Ok(())
    }
}

/// Client for connecting to RasterizedDB
pub struct DbClient {
    pool: Arc<DbClientPool>,
    connected: AtomicBool,
}

impl DbClient {
    /// Create a new database client connecting to the specified address
    pub async fn new(addr: Option<&str>) -> io::Result<Self> {
        // Default pool sizes
        let min_pool_size = 2;
        let max_pool_size = 10;

        let pool = DbClientPool::new(addr, min_pool_size, max_pool_size).await?;

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

    /// Create a new database client with custom pool settings
    pub async fn with_pool_settings(
        addr: Option<&str>,
        min_pool_size: usize,
        max_pool_size: usize,
    ) -> io::Result<Self> {
        let pool = DbClientPool::new(addr, min_pool_size, max_pool_size).await?;

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

        // Get a client from the pool
        let mut client_guard = self.pool.get_client().await?;

        // Send the query
        let response = client_guard.send(query.as_bytes().to_vec()).await?;

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
