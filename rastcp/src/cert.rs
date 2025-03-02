use rcgen::generate_simple_self_signed;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use std::io::BufReader;

pub fn generate_self_signed_cert() -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), Box<dyn std::error::Error>> {
    let subject_alt_names = vec!["localhost".to_string()];
    
    // Generate the certificate
    let cert_key = generate_simple_self_signed(subject_alt_names)?;
    
    // Get PEM representations
    let cert_pem = cert_key.cert.pem();
    let key_pem = cert_key.key_pair.serialize_pem();
    
    // Parse certificate PEM
    let mut cert_reader = BufReader::new(cert_pem.as_bytes());
   
    let certs = rustls_pemfile::certs(&mut cert_reader)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    // Parse private key PEM
    let mut key_reader = BufReader::new(key_pem.as_bytes());

    let private_key = rustls_pemfile::private_key(&mut key_reader).unwrap().unwrap().clone_key();

    Ok((certs, private_key))
}