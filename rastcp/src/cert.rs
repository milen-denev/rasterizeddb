use rcgen::generate_simple_self_signed;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use std::io::BufReader;

pub fn generate_self_signed_cert()
-> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), Box<dyn std::error::Error>> {
    let subject_alt_names = vec!["localhost".to_string()];

    // Generate the certificate
    let cert_key = generate_simple_self_signed(subject_alt_names)?;

    // Get PEM representations
    let cert_pem = cert_key.cert.pem();
    let key_pem = cert_key.signing_key.serialize_pem();

    // Parse certificate PEM
    let mut cert_reader = BufReader::new(cert_pem.as_bytes());

    let certs = rustls_pemfile::certs(&mut cert_reader)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    // Parse private key PEM
    let mut key_reader = BufReader::new(key_pem.as_bytes());

    let private_key = rustls_pemfile::private_key(&mut key_reader)
        .unwrap()
        .unwrap()
        .clone_key();

    Ok((certs, private_key))
}

/// Generates a self-signed certificate valid for `names`, and writes it as a
/// PEM pair.
///
/// Enough for pinned authentication, which cares about the certificate itself
/// rather than who signed it or what name it claims. The names are still filled
/// in so the same pair also works with ordinary verification if a deployment
/// later moves to a certificate authority.
///
/// Returns the paths written and the certificate's SHA-256 fingerprint — the
/// value the other nodes pin.
pub fn write_self_signed_identity(
    dir: &std::path::Path,
    names: &[String],
) -> Result<(std::path::PathBuf, std::path::PathBuf, crate::tls::Fingerprint), Box<dyn std::error::Error>>
{
    let names: Vec<String> = if names.is_empty() {
        vec!["localhost".to_string()]
    } else {
        names.to_vec()
    };

    let cert_key = generate_simple_self_signed(names)?;
    let cert_pem = cert_key.cert.pem();
    let key_pem = cert_key.signing_key.serialize_pem();

    std::fs::create_dir_all(dir)?;
    let cert_path = dir.join("replication-cert.pem");
    let key_path = dir.join("replication-key.pem");

    std::fs::write(&cert_path, cert_pem.as_bytes())?;
    std::fs::write(&key_path, key_pem.as_bytes())?;

    // The key is the node's identity in the cluster; anything that can read it
    // can join. Readable by its owner only, on platforms that have the notion.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600))?;
    }

    let mut reader = BufReader::new(cert_pem.as_bytes());
    let certs = rustls_pemfile::certs(&mut reader).collect::<Result<Vec<_>, _>>()?;
    let fingerprint = crate::tls::fingerprint(
        certs.first().ok_or("the generated certificate did not parse")?,
    );

    Ok((cert_path, key_path, fingerprint))
}
