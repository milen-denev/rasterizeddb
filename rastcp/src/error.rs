use thiserror::Error;

#[derive(Error, Debug)]
pub enum RastcpError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("TLS error: {0}")]
    Tls(#[from] rustls::Error),
    
    #[error("Connection closed")]
    ConnectionClosed,
    
    #[error("Certificate loading error: {0}")]
    CertificateLoading(String),
    
    #[error("Invalid message format")]
    InvalidMessageFormat,
}
