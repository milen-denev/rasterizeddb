use std::sync::Arc;
use log::{debug, info};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{ServerName, CertificateDer, UnixTime};
use rustls::{ClientConfig, Error, SignatureScheme};
use tokio::net::TcpStream;
use tokio_rustls::{TlsConnector, client::TlsStream};

use crate::common::{read_message, write_message};
use crate::error::RastcpError;

// A certificate verifier that accepts any certificate
#[derive(Debug)]
struct AcceptAnyCertificate;

impl ServerCertVerifier for AcceptAnyCertificate {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, Error> {
        // This is insecure but convenient for development
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        // Accept any signature
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        // Accept any signature
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        // Support all common signature schemes
        vec![
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::ED25519,
        ]
    }
}

pub struct TcpClientBuilder {
    addr: String,
    timeout: Option<std::time::Duration>,
    verify_certificate: bool,
}

pub struct TcpClient {
    addr: String,
    connector: TlsConnector,
    server_name: ServerName<'static>,
    connection: Option<TlsStream<TcpStream>>,
    timeout: Option<std::time::Duration>,
}

impl TcpClientBuilder {
    pub fn new(addr: &str) -> Self {
        Self {
            addr: addr.to_string(),
            timeout: Some(std::time::Duration::from_secs(30)), // Default timeout
            verify_certificate: false,
        }
    }

    pub fn timeout(mut self, timeout: std::time::Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn no_timeout(mut self) -> Self {
        self.timeout = None;
        self
    }

    pub fn verify_certificate(mut self, verify: bool) -> Self {
        self.verify_certificate = verify;
        self
    }

    pub async fn build(self) -> Result<TcpClient, RastcpError> {
        let config = if self.verify_certificate {
            // Use default certificate verification
            ClientConfig::builder()
                .with_root_certificates(rustls::RootCertStore::empty())
                .with_no_client_auth()
        } else {
            // Accept any certificate
            ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(AcceptAnyCertificate))
                .with_no_client_auth()
        };

        // Get server name from address
        let server_name = ServerName::try_from(self.addr.split(':')
            .next()
            .ok_or_else(|| RastcpError::CertificateLoading("Invalid address format".into()))?)
            .map_err(|_| RastcpError::CertificateLoading("Invalid server name".into()))?
            .to_owned();
        
        Ok(TcpClient {
            addr: self.addr,
            connector: TlsConnector::from(Arc::new(config)),
            server_name,
            connection: None,
            timeout: self.timeout,
        })
    }
}

impl TcpClient {
    pub async fn new(addr: &str) -> Result<Self, RastcpError> {
        TcpClientBuilder::new(addr).build().await
    }
    
    pub async fn connect(&mut self) -> Result<(), RastcpError> {
        if self.connection.is_none() {
            let stream = match self.timeout {
                Some(timeout) => {
                    tokio::time::timeout(
                        timeout, 
                        TcpStream::connect(&self.addr)
                    ).await.map_err(|_| RastcpError::ConnectionTimeout)??
                },
                None => TcpStream::connect(&self.addr).await?,
            };
            
            info!("Connected to {}", self.addr);
            
            let tls_stream = self.connector.connect(self.server_name.clone(), stream).await?;
            debug!("TLS connection established");
            
            self.connection = Some(tls_stream);
        }
        Ok(())
    }
    
    // Modified send method to use the existing connection
    pub async fn send(&mut self, data: Vec<u8>) -> Result<Vec<u8>, RastcpError> {
        // Connect if not connected
        self.connect().await?;
        
        let stream = self.connection.as_mut().unwrap();
        
        // Send data using the connection
        write_message(stream, &data).await?;
        debug!("Sent {} bytes", data.len());
        
        // Receive response
        let response = read_message(stream).await?;
        debug!("Received {} bytes response", response.len());
        
        Ok(response)
    }
    
    // Add a close method to explicitly close the connection
    pub async fn close(&mut self) -> Result<(), RastcpError> {
        self.connection = None;
        Ok(())
    }

    // Add a is_connected method to check connection status
    pub fn is_connected(&self) -> bool {
        self.connection.is_some()
    }

    // Add a reconnect method to force a new connection
    pub async fn reconnect(&mut self) -> Result<(), RastcpError> {
        self.close().await?;
        self.connect().await
    }
}
