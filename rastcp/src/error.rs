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

    #[error("Socket binding error: {0}")]
    SocketBindingError(String),

    #[error("Socket listening error: {0}")]
    SocketListeningError(String),

    #[error("Socket keep alive error: {0}")]
    SocketKeepAliveError(String),
    
    #[error("TLS handshake timeout")]
    TlsHandshakeTimeout,
    
    #[error("Handler timeout")]
    HandlerTimeout,
    
    #[error("Connection backpressure")]
    ConnectionBackpressure,
}
