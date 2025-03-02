use std::future::Future;
use std::sync::Arc;
use log::{debug, error, info};
use rustls::ServerConfig;
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsAcceptor;

use crate::cert::generate_self_signed_cert;
use crate::common::{read_message, write_message};
use crate::error::RastcpError;

pub struct TcpServer {
    listener: TcpListener,
    acceptor: TlsAcceptor,
}

impl TcpServer {
    pub async fn new(
        addr: &str
    ) -> Result<Self, RastcpError> {
        // Load TLS certificates and keys
        let (certs, private_key) = generate_self_signed_cert().unwrap();
        
        // Create TLS config
        let config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, private_key)
            .map_err(|e| RastcpError::CertificateLoading(e.to_string()))?;
        
        // Create server
        let listener = TcpListener::bind(addr).await?;
        let acceptor = TlsAcceptor::from(Arc::new(config));
        
        info!("Server listening on {}", addr);
        
        Ok(Self {
            listener,
            acceptor,
        })
    }

    pub async fn run<F, Fut>(&self, handler: F) -> Result<(), RastcpError>
    where
        F: Fn(Vec<u8>) -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output = Vec<u8>> + Send + 'static,
    {
        let acceptor = self.acceptor.clone();
        
        loop {
            match self.listener.accept().await {
                Ok((stream, addr)) => {
                    info!("New connection from {}", addr);
                    
                    let acceptor = acceptor.clone();
                    let handler = handler.clone();
                    
                    tokio::spawn(async move {
                        if let Err(e) = Self::handle_connection(stream, acceptor, handler).await {
                            error!("Connection error: {}", e);
                        }
                    });
                }
                Err(e) => {
                    error!("Failed to accept connection: {}", e);
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
        
        let mut tls_stream = acceptor.accept(stream).await?;
        
        loop {
            match read_message(&mut tls_stream).await {
                Ok(data) => {
                    debug!("Received {} bytes from {}", data.len(), peer_addr);
                    
                    // Process the data with the handler
                    let response = handler(data).await;
                    
                    // Send response back
                    write_message(&mut tls_stream, &response).await?;
                }
                Err(RastcpError::ConnectionClosed) => {
                    info!("Connection closed by peer: {}", peer_addr);
                    break;
                }
                Err(e) => {
                    error!("Error reading from {}: {}", peer_addr, e);
                    break;
                }
            }
        }
        
        Ok(())
    }
}
