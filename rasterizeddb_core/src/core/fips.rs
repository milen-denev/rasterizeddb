//! FIPS 140-3 mode (Cargo feature `fips`).
//!
//! FIPS 140-3 asks that every cryptographic function a product relies on for
//! security runs inside a validated module, using approved algorithms. With
//! `fips`, that module is AWS-LC's FIPS module, through `aws-lc-rs`'s `fips`
//! feature, which is already this server's TLS provider:
//!
//! - **TLS** (pgwire, MySQL, TDS, the native protocol through rustls): the
//!   rustls FIPS provider, approved suites and groups only.
//! - **SCRAM-SHA-256** logins, **SigV4** signing for S3, client-certificate
//!   fingerprints: SHA-256, HMAC-SHA-256 and PBKDF2 from the module
//!   ([`sha256`], [`hmac_sha256`], [`pbkdf2_sha256`]).
//! - **Refused:** `md5` authentication (MD5 is not an approved hash for
//!   authentication).
//!
//! At start-up [`self_check`] asks the module whether it is in FIPS mode,
//! which runs its power-on self-tests. The server refuses to start when it
//! is not, rather than run "FIPS" on code that is not.
//!
//! Building with `fips` needs what AWS-LC's FIPS build needs: CMake, Go, a C
//! compiler and Perl. For a certified deployment, the AWS-LC version must be
//! one named on a current CMVP certificate; that is a release decision, not
//! something this code can check.
//!
//! Without the feature these functions are the RustCrypto implementations the
//! server always used, byte for byte the same results.

/// SHA-256.
pub fn sha256(data: &[u8]) -> [u8; 32] {
    #[cfg(feature = "fips")]
    {
        let d = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, data);
        d.as_ref().try_into().expect("SHA-256 is 32 bytes")
    }
    #[cfg(not(feature = "fips"))]
    {
        use sha2::Digest;
        sha2::Sha256::digest(data).into()
    }
}

/// HMAC-SHA-256.
pub fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    #[cfg(feature = "fips")]
    {
        let k = aws_lc_rs::hmac::Key::new(aws_lc_rs::hmac::HMAC_SHA256, key);
        aws_lc_rs::hmac::sign(&k, data).as_ref().try_into().expect("HMAC-SHA-256 is 32 bytes")
    }
    #[cfg(not(feature = "fips"))]
    {
        use hmac::{KeyInit, Mac};
        let mut mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(key).expect("HMAC accepts any key length");
        mac.update(data);
        mac.finalize().into_bytes().into()
    }
}

/// PBKDF2-HMAC-SHA-256 into `out`.
pub fn pbkdf2_sha256(password: &[u8], salt: &[u8], iterations: u32, out: &mut [u8]) {
    #[cfg(feature = "fips")]
    {
        let n = std::num::NonZeroU32::new(iterations.max(1)).expect("non-zero");
        aws_lc_rs::pbkdf2::derive(aws_lc_rs::pbkdf2::PBKDF2_HMAC_SHA256, n, salt, password, out);
    }
    #[cfg(not(feature = "fips"))]
    pbkdf2::pbkdf2_hmac::<sha2::Sha256>(password, salt, iterations, out);
}

/// Whether this build is a FIPS build.
pub const ENABLED: bool = cfg!(feature = "fips");

/// Confirms the module is in FIPS mode (running its self-tests); a FIPS build
/// that is not refuses to start. Nothing to do in a build without `fips`.
pub fn self_check() -> Result<(), String> {
    #[cfg(feature = "fips")]
    {
        aws_lc_rs::try_fips_mode().map_err(|e| format!("the AWS-LC FIPS module is not in FIPS mode: {e}"))
    }
    #[cfg(not(feature = "fips"))]
    Ok(())
}

/// Makes rustls's FIPS provider the process's, before anything else installs
/// one. Nothing in a build without `fips`.
pub fn install_tls_provider() {
    #[cfg(feature = "fips")]
    let _ = rustls::crypto::default_fips_provider().install_default();
}

/// Refuses an authentication method a FIPS build does not offer.
pub fn check_auth_method(method: crate::auth::AuthMethod) -> Result<(), String> {
    if ENABLED && method == crate::auth::AuthMethod::Md5 {
        return Err("md5 authentication is not offered in FIPS mode: use scram-sha-256 or cert".to_string());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_answers() {
        let hex = |b: &[u8]| b.iter().map(|x| format!("{x:02x}")).collect::<String>();
        assert_eq!(hex(&sha256(b"abc")), "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
        // RFC 4231 test case 2.
        assert_eq!(
            hex(&hmac_sha256(b"Jefe", b"what do ya want for nothing?")),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
        let mut out = [0u8; 32];
        pbkdf2_sha256(b"password", b"salt", 1, &mut out);
        assert_eq!(hex(&out), "120fb6cffcf8b32c43e7225256c4f837a86548c92ccc35480805987cb70be17b");
        assert!(self_check().is_ok());
    }
}
