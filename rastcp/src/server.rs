use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use log::{debug, error, info, warn};
use rustls::compress::CompressionCache;
use rustls::crypto::aws_lc_rs::Ticketer;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::ServerSessionMemoryCache;
use rustls::ServerConfig;
use socket2::{Domain, Protocol, SockAddr, Socket, TcpKeepalive, Type};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsAcceptor;

use crate::cert::generate_self_signed_cert;
use crate::common::{read_message, write_message};
use crate::error::RastcpError;

pub struct TcpServerBuilder {
    addr: String,
    port: u16,
    custom_certs: Option<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)>,
    max_connections: Option<usize>,
    connection_backoff_ms: Option<u64>,
    session_cache_size: usize,
    handshake_timeout: Duration,
}

pub struct TcpServer {
    listener: TcpListener,
    acceptor: TlsAcceptor,
    max_connections: Option<usize>,
    current_connections: Arc<std::sync::atomic::AtomicUsize>,
    connection_backoff_ms: u64,
    _handshake_timeout: Duration,
}

impl TcpServerBuilder {
    pub fn new(addr: &str, port: u16) -> Self {
        Self {
            addr: addr.to_string(),
            port,
            custom_certs: None,
            max_connections: None,
            connection_backoff_ms: Some(100), // Default backoff of 100ms
            session_cache_size: 1024, // Increased from 128
            handshake_timeout: Duration::from_secs(10),
        }
    }

    pub fn with_certificates(mut self, certs: Vec<CertificateDer<'static>>, key: PrivateKeyDer<'static>) -> Self {
        self.custom_certs = Some((certs, key));
        self
    }

    pub fn max_connections(mut self, max: usize) -> Self {
        self.max_connections = Some(max);
        self
    }
    
    pub fn connection_backoff_ms(mut self, ms: u64) -> Self {
        self.connection_backoff_ms = Some(ms);
        self
    }
    
    pub fn session_cache_size(mut self, size: usize) -> Self {
        self.session_cache_size = size;
        self
    }
    
    pub fn handshake_timeout(mut self, timeout: Duration) -> Self {
        self.handshake_timeout = timeout;
        self
    }

    pub async fn build(self) -> Result<TcpServer, RastcpError> {
        // Load TLS certificates
        let (certs, private_key) = match self.custom_certs {
            Some(certs_key) => certs_key,
            None => generate_self_signed_cert().map_err(|e| RastcpError::CertificateLoading(e.to_string()))?,
        };
        
        // Create TLS config
        let mut server_config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, private_key)
            .map_err(|e| RastcpError::CertificateLoading(e.to_string()))?;
        
        server_config.max_early_data_size = 1024;
        server_config.ticketer = Ticketer::new().unwrap();
        server_config.session_storage = ServerSessionMemoryCache::new(self.session_cache_size);
        server_config.alpn_protocols = vec!["h2".into(), "http/1.1".into()];
        server_config.cert_compression_cache = Arc::new(CompressionCache::new(64));
        server_config.send_half_rtt_data = true;
        server_config.send_tls13_tickets = 2;

        // Create server
        let ip_addr = IpAddr::from_str(&self.addr).unwrap();

        let addr = if cfg!(debug_assertions) {
            SocketAddr::from((ip_addr, self.port))
        } else {
            SocketAddr::from((ip_addr, self.port))
        };

        let socket = Socket::new_raw(Domain::IPV4, Type::STREAM, Some(Protocol::TCP)).unwrap();

        _ = socket.set_tcp_nodelay(true);
        _ = socket.set_nonblocking(true);
        _ = socket.set_reuse_address(false);
        _ = socket.set_recv_buffer_size(1024 * 16); // Increased buffer sizes
        _ = socket.set_send_buffer_size(1024 * 32);
        _ = socket.bind(&SockAddr::from(addr)).map_err(|e| RastcpError::SocketBindingError(e.to_string()))?;
        _ = socket.listen(i32::MAX).map_err(|e| RastcpError::SocketListeningError(e.to_string()))?;
        let tcp_keep_alive = &TcpKeepalive::new().with_time(Duration::from_secs(7200));
        _ = socket.set_tcp_keepalive(tcp_keep_alive).map_err(|e| RastcpError::SocketKeepAliveError(e.to_string()))?;

        let listener: std::net::TcpListener = socket.into();
        let tokio_listener = TcpListener::from_std(listener).unwrap();

        let acceptor = TlsAcceptor::from(Arc::new(server_config));
        
        info!("Server built to listen on {}:{}", self.addr, self.port);
        
        Ok(TcpServer {
            listener: tokio_listener,
            acceptor,
            max_connections: self.max_connections,
            current_connections: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            connection_backoff_ms: self.connection_backoff_ms.unwrap_or(100),
            _handshake_timeout: self.handshake_timeout,
        })
    }
}

impl TcpServer {
    pub async fn new(addr: &str, port: u16) -> Result<Self, RastcpError> {
        TcpServerBuilder::new(addr, port).build().await
    }

    // Original run method remains unchanged for backward compatibility
    pub async fn run<F, Fut>(&self, handler: F) -> Result<(), RastcpError>
    where
        F: Fn(Vec<u8>) -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output = Vec<u8>> + Send + 'static,
    {
        let acceptor = self.acceptor.clone();
        let mut consecutive_max_connections = 0;
        
        loop {
            // Check if we're at max connections
            if let Some(max) = self.max_connections {
                let current = self.current_connections.load(std::sync::atomic::Ordering::Relaxed);
                if current >= max {
                    consecutive_max_connections += 1;
                    
                    // Exponential backoff if consistently at capacity
                    let backoff = std::cmp::min(
                        self.connection_backoff_ms * consecutive_max_connections,
                        1000 // Max 1 second backoff
                    );
                    
                    warn!("At connection capacity ({}/{}), backing off for {}ms", 
                          current, max, backoff);
                    
                    tokio::time::sleep(std::time::Duration::from_millis(backoff)).await;
                    continue;
                } else {
                    consecutive_max_connections = 0;
                }
            }
            
            // Use timeout for accepting connections to avoid blocking indefinitely
            match tokio::time::timeout(
                Duration::from_secs(1),
                self.listener.accept()
            ).await {
                Ok(Ok((stream, addr))) => {
                    info!("New connection from {}", addr);
                    
                    let acceptor = acceptor.clone();
                    let handler = handler.clone();
                    let conn_counter = self.current_connections.clone();
                    
                    // Increment connection counter
                    let current = conn_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    debug!("Active connections: {}", current + 1);
                    
                    tokio::spawn(async move {
                        if let Err(e) = Self::handle_connection(stream, acceptor, handler).await {
                            error!("Connection error: {}", e);
                        }
                        // Decrement connection counter when done
                        let remaining = conn_counter.fetch_sub(1, std::sync::atomic::Ordering::Relaxed) - 1;
                        debug!("Connection closed. Active connections: {}", remaining);
                    });
                }
                Ok(Err(e)) => {
                    error!("Failed to accept connection: {}", e);
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(_) => {
                    // Timeout on accept - just continue
                    continue;
                }
            }
        }
    }
    
    // New method that accepts a context object which is passed to each handler call
    pub async fn run_with_context<A, F, Fut>(&self, context: A, handler: F) -> Result<(), RastcpError>
    where
        A: Clone + Send + Sync + 'static,
        F: Fn(A, Vec<u8>) -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output = Vec<u8>> + Send + 'static,
    {
        let acceptor = self.acceptor.clone();
        let mut consecutive_max_connections = 0;
        
        loop {
            // Check if we're at max connections
            if let Some(max) = self.max_connections {
                let current = self.current_connections.load(std::sync::atomic::Ordering::Relaxed);
                if current >= max {
                    consecutive_max_connections += 1;
                    
                    // Exponential backoff if consistently at capacity
                    let backoff = std::cmp::min(
                        self.connection_backoff_ms * consecutive_max_connections,
                        1000 // Max 1 second backoff
                    );
                    
                    warn!("At connection capacity ({}/{}), backing off for {}ms", 
                          current, max, backoff);
                    
                    tokio::time::sleep(std::time::Duration::from_millis(backoff)).await;
                    continue;
                } else {
                    consecutive_max_connections = 0;
                }
            }
            
            // Use timeout for accepting connections to avoid blocking indefinitely
            match tokio::time::timeout(
                Duration::from_secs(1),
                self.listener.accept()
            ).await {
                Ok(Ok((stream, addr))) => {
                    info!("New connection from {}", addr);
                    
                    let acceptor = acceptor.clone();
                    let handler = handler.clone();
                    let context = context.clone();
                    let conn_counter = self.current_connections.clone();
                    
                    // Increment connection counter
                    let current = conn_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    debug!("Active connections: {}", current + 1);
                    
                    tokio::spawn(async move {
                        if let Err(e) = Self::handle_connection_with_context(stream, acceptor, context, handler).await {
                            error!("Connection error: {}", e);
                        }
                        // Decrement connection counter when done
                        let remaining = conn_counter.fetch_sub(1, std::sync::atomic::Ordering::Relaxed) - 1;
                        debug!("Connection closed. Active connections: {}", remaining);
                    });
                }
                Ok(Err(e)) => {
                    error!("Failed to accept connection: {}", e);
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(_) => {
                    // Timeout on accept - just continue
                    continue;
                }
            }
        }
    }
    
    // New method that accepts a mutable reference to a context through Arc<Mutex<>>
    pub async fn run_with_shared_context<A, F, Fut>(&self, context: Arc<tokio::sync::Mutex<A>>, handler: F) -> Result<(), RastcpError>
    where
        A: Send + Sync + 'static,
        F: Fn(Arc<tokio::sync::Mutex<A>>, Vec<u8>) -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output = Vec<u8>> + Send + 'static,
    {
        let acceptor = self.acceptor.clone();
        let mut consecutive_max_connections = 0;
        
        loop {
            // Check if we're at max connections
            if let Some(max) = self.max_connections {
                let current = self.current_connections.load(std::sync::atomic::Ordering::Relaxed);
                if current >= max {
                    consecutive_max_connections += 1;
                    
                    // Exponential backoff if consistently at capacity
                    let backoff = std::cmp::min(
                        self.connection_backoff_ms * consecutive_max_connections,
                        1000 // Max 1 second backoff
                    );
                    
                    warn!("At connection capacity ({}/{}), backing off for {}ms", 
                          current, max, backoff);
                    
                    tokio::time::sleep(std::time::Duration::from_millis(backoff)).await;
                    continue;
                } else {
                    consecutive_max_connections = 0;
                }
            }
            
            // Use timeout for accepting connections to avoid blocking indefinitely
            match tokio::time::timeout(
                Duration::from_secs(1),
                self.listener.accept()
            ).await {
                Ok(Ok((stream, addr))) => {
                    info!("New connection from {}", addr);
                    
                    let acceptor = acceptor.clone();
                    let handler = handler.clone();
                    let context = context.clone();
                    let conn_counter = self.current_connections.clone();
                    
                    // Increment connection counter
                    let current = conn_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    debug!("Active connections: {}", current + 1);
                    
                    tokio::spawn(async move {
                        if let Err(e) = Self::handle_connection_with_shared_context(stream, acceptor, context, handler).await {
                            error!("Connection error: {}", e);
                        }
                        // Decrement connection counter when done
                        let remaining = conn_counter.fetch_sub(1, std::sync::atomic::Ordering::Relaxed) - 1;
                        debug!("Connection closed. Active connections: {}", remaining);
                    });
                }
                Ok(Err(e)) => {
                    error!("Failed to accept connection: {}", e);
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(_) => {
                    // Timeout on accept - just continue
                    continue;
                }
            }
        }
    }
    
    async fn handle_connection<F, Fut>(
        stream: TcpStream,
        acceptor: TlsAcceptor,
        handler: F,
    ) -> Result<(), RastcpError>
    where
        F: Fn(Vec<u8>) -> Fut + Send + Sync,
        Fut: Future<Output = Vec<u8>> + Send,
    {
        let peer_addr = stream.peer_addr()?;
        debug!("Accepting TLS connection from {}", peer_addr);

        // Apply timeout to TLS handshake to prevent hanging connections
        match tokio::time::timeout(
            Duration::from_secs(30), 
            acceptor.accept(stream)
        ).await {
            Ok(Ok(mut tls_stream)) => {
                debug!("TLS handshake completed with {}", peer_addr);
                
                loop {
                    match read_message(&mut tls_stream).await {
                        Ok(data) => {
                            debug!("Received {} bytes from {}", data.len(), peer_addr);
                            
                            // Process the data with the handler
                            let response = match tokio::time::timeout(
                                Duration::from_secs(30), // Add timeout for handler
                                handler(data)
                            ).await {
                                Ok(response) => response,
                                Err(_) => {
                                    error!("Handler timed out for {}", peer_addr);
                                    break;
                                }
                            };
                            
                            debug!("Replying with {} bytes to {}", response.len(), peer_addr);

                            // Send response back
                            if let Err(e) = write_message(&mut tls_stream, &response).await {
                                error!("Failed to write response to {}: {}", peer_addr, e);
                                break;
                            }
                        }
                        Err(RastcpError::ConnectionClosed) => {
                            info!("Connection closed by peer: {}", peer_addr);
                            break;
                        }
                        Err(RastcpError::Io(e)) if e.kind() == std::io::ErrorKind::ConnectionReset => {
                            info!("Connection reset by peer: {}", peer_addr);
                            break;
                        }
                        Err(RastcpError::Tls(e)) => {
                            // Special handling for TLS errors
                            warn!("TLS error from {}: {}", peer_addr, e);
                            break;
                        }
                        Err(e) => {
                            error!("Error reading from {}: {}", peer_addr, e);
                            break;
                        }
                    }
                }
                
                // Explicit shutdown to ensure clean TLS termination
                if let Err(e) = tls_stream.shutdown().await {
                    debug!("TLS shutdown error with {}: {}", peer_addr, e);
                }
            },
            Ok(Err(e)) => {
                error!("TLS handshake failed with {}: {}", peer_addr, e);
            },
            Err(_) => {
                error!("TLS handshake timed out with {}", peer_addr);
            }
        }

        debug!("Connection handler completed for {}", peer_addr);
        Ok(())
    }
    
    async fn handle_connection_with_context<A, F, Fut>(
        stream: TcpStream,
        acceptor: TlsAcceptor,
        context: A,
        handler: F,
    ) -> Result<(), RastcpError>
    where
        A: Clone + Send + Sync,
        F: Fn(A, Vec<u8>) -> Fut + Send + Sync,
        Fut: Future<Output = Vec<u8>> + Send,
    {
        let peer_addr = stream.peer_addr()?;
        debug!("Accepting TLS connection from {}", peer_addr);

        // Apply timeout to TLS handshake to prevent hanging connections
        match tokio::time::timeout(
            Duration::from_secs(30), 
            acceptor.accept(stream)
        ).await {
            Ok(Ok(mut tls_stream)) => {
                debug!("TLS handshake completed with {}", peer_addr);
                
                loop {
                    match read_message(&mut tls_stream).await {
                        Ok(data) => {
                            debug!("Received {} bytes from {}", data.len(), peer_addr);
                            
                            // Process the data with the handler, passing the context
                            let context_clone = context.clone();
                            let response = match tokio::time::timeout(
                                Duration::from_secs(30), // Add timeout for handler
                                handler(context_clone, data)
                            ).await {
                                Ok(response) => response,
                                Err(_) => {
                                    error!("Handler timed out for {}", peer_addr);
                                    break;
                                }
                            };
                            
                            debug!("Replying with {} bytes to {}", response.len(), peer_addr);

                            // Send response back
                            if let Err(e) = write_message(&mut tls_stream, &response).await {
                                error!("Failed to write response to {}: {}", peer_addr, e);
                                break;
                            }
                        }
                        Err(RastcpError::ConnectionClosed) => {
                            info!("Connection closed by peer: {}", peer_addr);
                            break;
                        }
                        Err(RastcpError::Io(e)) if e.kind() == std::io::ErrorKind::ConnectionReset => {
                            info!("Connection reset by peer: {}", peer_addr);
                            break;
                        }
                        Err(RastcpError::Tls(e)) => {
                            // Special handling for TLS errors
                            warn!("TLS error from {}: {}", peer_addr, e);
                            break;
                        }
                        Err(e) => {
                            error!("Error reading from {}: {}", peer_addr, e);
                            break;
                        }
                    }
                }
                
                // Explicit shutdown to ensure clean TLS termination
                if let Err(e) = tls_stream.shutdown().await {
                    debug!("TLS shutdown error with {}: {}", peer_addr, e);
                }
            },
            Ok(Err(e)) => {
                error!("TLS handshake failed with {}: {}", peer_addr, e);
            },
            Err(_) => {
                error!("TLS handshake timed out with {}", peer_addr);
            }
        }

        debug!("Connection handler completed for {}", peer_addr);
        Ok(())
    }
    
    async fn handle_connection_with_shared_context<A, F, Fut>(
        stream: TcpStream,
        acceptor: TlsAcceptor,
        context: Arc<tokio::sync::Mutex<A>>,
        handler: F,
    ) -> Result<(), RastcpError>
    where
        A: Send + Sync,
        F: Fn(Arc<tokio::sync::Mutex<A>>, Vec<u8>) -> Fut + Send + Sync,
        Fut: Future<Output = Vec<u8>> + Send,
    {
        let peer_addr = stream.peer_addr()?;
        debug!("Accepting TLS connection from {}", peer_addr);
       
        // Apply timeout to TLS handshake to prevent hanging connections
        match tokio::time::timeout(
            Duration::from_secs(30), 
            acceptor.accept(stream)
        ).await {
            Ok(Ok(mut tls_stream)) => {
                debug!("TLS handshake completed with {}", peer_addr);
                
                loop {
                    match read_message(&mut tls_stream).await {
                        Ok(data) => {
                            debug!("Received {} bytes from {}", data.len(), peer_addr);
                            
                            // Process the data with the handler, passing the shared context
                            let response = match tokio::time::timeout(
                                Duration::from_secs(30), // Add timeout for handler
                                handler(context.clone(), data)
                            ).await {
                                Ok(response) => response,
                                Err(_) => {
                                    error!("Handler timed out for {}", peer_addr);
                                    break;
                                }
                            };
                            
                            debug!("Replying with {} bytes to {}", response.len(), peer_addr);

                            // Send response back
                            if let Err(e) = write_message(&mut tls_stream, &response).await {
                                error!("Failed to write response to {}: {}", peer_addr, e);
                                break;
                            }
                        }
                        Err(RastcpError::ConnectionClosed) => {
                            info!("Connection closed by peer: {}", peer_addr);
                            break;
                        }
                        Err(RastcpError::Io(e)) if e.kind() == std::io::ErrorKind::ConnectionReset => {
                            info!("Connection reset by peer: {}", peer_addr);
                            break;
                        }
                        Err(RastcpError::Tls(e)) => {
                            // Special handling for TLS errors
                            warn!("TLS error from {}: {}", peer_addr, e);
                            break;
                        }
                        Err(e) => {
                            error!("Error reading from {}: {}", peer_addr, e);
                            break;
                        }
                    }
                }
                
                // Explicit shutdown to ensure clean TLS termination
                if let Err(e) = tls_stream.shutdown().await {
                    debug!("TLS shutdown error with {}: {}", peer_addr, e);
                }
            },
            Ok(Err(e)) => {
                error!("TLS handshake failed with {}: {}", peer_addr, e);
            },
            Err(_) => {
                error!("TLS handshake timed out with {}", peer_addr);
            }
        }

        debug!("Connection handler completed for {}", peer_addr);
        Ok(())
    }
}
