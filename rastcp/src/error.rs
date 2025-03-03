use thiserror::Error;

#[derive(Error, Debug)]
pub enum RastcpError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("TLS error: {0}")]
    Tls(#[from] rustls::Error),
    
    #[error("Connection closed")]
    ConnectionClosed,
    
    #[error("Connection timeout")]
    ConnectionTimeout,
    
    #[error("Certificate loading error: {0}")]
    CertificateLoading(String),
    
    #[error("Invalid message format")]
    InvalidMessageFormat,
    
    #[error("Connection limit reached")]
    ConnectionLimitReached,
    
    #[error("Protocol error: {0}")]
    ProtocolError(String),
    
    #[error("Authentication error: {0}")]
    AuthenticationError(String),
}
