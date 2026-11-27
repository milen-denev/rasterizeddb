//! Envelope encryption (docs/ledger-and-vault-plan.md §4.1, §4.4).
//!
//! Data is encrypted with **data keys** (DEKs): AES-256-GCM, one per secret
//! version, random. A DEK is never stored in the clear. It is **wrapped**
//! by a key-encryption key (KEK) that a [`KeyProvider`] holds:
//!
//! - `file:<path>`: 32 bytes (or 64 hex characters) in a file that only its
//!   owner may read. For tests and development: the key is on the same host
//!   as the data. A vault refuses it unless `RDB_VAULT_ALLOW_FILE_KEY=1`.
//! - `pkcs11:module=<lib>;token=<label>;key=<label>[;pin-source=<file>|pin-env=<VAR>][;mechanism=kwp|cbc]`:
//!   a key inside an HSM, a cloud HSM or a TPM 2.0 (through tpm2-pkcs11).
//!   The KEK never leaves the device. Wrapping and unwrapping run inside it.
//!
//! - `mlkem:<path>` (feature `pqc`): a hybrid ML-KEM-768 + X25519 key pair
//!   ([`pqc`]), for wrapping that stays sound against a quantum computer.
//!
//! AES itself runs on AES-NI (x86-64) or the ARMv8 Crypto Extensions,
//! which the `aes` crate picks at run time.

pub mod secure;

#[cfg(feature = "pkcs11")]
mod pkcs11;

#[cfg(feature = "pqc")]
pub mod pqc;
#[cfg(feature = "tpm")]
pub mod tpm;
#[cfg(feature = "pqc")]
pub mod pqc_sign;

#[cfg(not(feature = "fips"))]
use aes_gcm::aead::{Aead, KeyInit, Payload};
#[cfg(not(feature = "fips"))]
use aes_gcm::{Aes256Gcm, Nonce};
use zeroize::Zeroizing;

pub type Dek = Zeroizing<[u8; 32]>;

/// Holds a key-encryption key and wraps data keys with it.
pub trait KeyProvider: Send + Sync {
    /// What the provider is, for messages and `SHOW` (never a secret).
    fn describe(&self) -> String;
    fn wrap(&self, dek: &[u8; 32]) -> Result<Vec<u8>, String>;
    fn unwrap(&self, wrapped: &[u8]) -> Result<Dek, String>;
    /// Keys that live in the provider's hardware and are used there, when it
    /// has any (a PKCS#11 token).
    fn hsm(&self) -> Option<&dyn HsmKeys> {
        None
    }
}

/// Keys generated inside a hardware token, used there and never exported
/// (`VAULT … CREATE KEY … TYPE 'hsm-aes256-gcm' | 'hsm-ecdsa-p256'`). Each is
/// found by its label.
pub trait HsmKeys {
    fn generate_aes(&self, label: &str) -> Result<(), String>;
    /// `iv ‖ ciphertext ‖ tag`, AES-256-GCM on the token.
    fn aes_encrypt(&self, label: &str, plain: &[u8], aad: &[u8]) -> Result<Vec<u8>, String>;
    fn aes_decrypt(&self, label: &str, sealed: &[u8], aad: &[u8]) -> Result<Zeroizing<Vec<u8>>, String>;
    fn generate_ec(&self, label: &str) -> Result<(), String>;
    /// An ECDSA P-256 signature (`r ‖ s`) of SHA-256(`message`), on the token.
    fn ec_sign(&self, label: &str, message: &[u8]) -> Result<Vec<u8>, String>;
    fn ec_verify(&self, label: &str, message: &[u8], signature: &[u8]) -> Result<bool, String>;
    /// The public key, as the token's `CKA_EC_POINT`.
    fn ec_public(&self, label: &str) -> Result<Vec<u8>, String>;
}

/// A fresh random data key.
pub fn new_dek() -> Result<Dek, String> {
    let mut k = Zeroizing::new([0u8; 32]);
    fill_random(&mut k[..])?;
    Ok(k)
}

/// Fills `buf` from the operating system's random source.
#[allow(unreachable_code)]
pub fn fill_random(buf: &mut [u8]) -> Result<(), String> {
    // FIPS: the module's DRBG.
    #[cfg(feature = "fips")]
    return aws_lc_rs::rand::fill(buf).map_err(|_| "no random bytes from the FIPS module".to_string());
    // Off Linux: the platform's source (BCryptGenRandom on Windows).
    #[cfg(all(not(feature = "fips"), not(target_os = "linux")))]
    return getrandom::fill(buf).map_err(|e| format!("no random bytes: {e}"));
    #[cfg(all(not(feature = "fips"), target_os = "linux"))]
    let mut done = 0;
    #[cfg(all(not(feature = "fips"), target_os = "linux"))]
    while done < buf.len() {
        // getrandom(2): blocks only before the pool is first seeded.
        let n = unsafe { libc::getrandom(buf[done..].as_mut_ptr().cast(), buf.len() - done, 0) };
        if n < 0 {
            let e = std::io::Error::last_os_error();
            if e.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            return Err(format!("no random bytes: {e}"));
        }
        done += n as usize;
    }
    Ok(())
}

/// `nonce ‖ ciphertext ‖ tag` of `plain` under `key`, bound to `aad`: the
/// FIPS module's AES-256-GCM in a `fips` build.
#[cfg(feature = "fips")]
pub fn seal(key: &[u8; 32], plain: &[u8], aad: &[u8]) -> Result<Vec<u8>, String> {
    use aws_lc_rs::aead::{AES_256_GCM, Aad, LessSafeKey, Nonce, UnboundKey};
    let k = LessSafeKey::new(UnboundKey::new(&AES_256_GCM, key).map_err(|_| "bad key".to_string())?);
    let mut nonce = [0u8; 12];
    fill_random(&mut nonce)?;
    let mut buf = plain.to_vec();
    k.seal_in_place_append_tag(Nonce::assume_unique_for_key(nonce), Aad::from(aad), &mut buf)
        .map_err(|_| "encryption failed".to_string())?;
    let mut out = nonce.to_vec();
    out.extend_from_slice(&buf);
    Ok(out)
}

/// [`seal`]'s inverse in a `fips` build.
#[cfg(feature = "fips")]
pub fn open(key: &[u8; 32], sealed: &[u8], aad: &[u8]) -> Result<Zeroizing<Vec<u8>>, String> {
    use aws_lc_rs::aead::{AES_256_GCM, Aad, LessSafeKey, Nonce, UnboundKey};
    if sealed.len() < 12 + 16 {
        return Err("XX001: a sealed value is too short".to_string());
    }
    let k = LessSafeKey::new(UnboundKey::new(&AES_256_GCM, key).map_err(|_| "bad key".to_string())?);
    let (nonce, ct) = sealed.split_at(12);
    let mut buf = Zeroizing::new(ct.to_vec());
    let n = k
        .open_in_place(Nonce::try_assume_unique_for_key(nonce).map_err(|_| "bad nonce".to_string())?, Aad::from(aad), &mut buf)
        .map_err(|_| "XX001: a sealed value does not authenticate: wrong key, or it was altered".to_string())?
        .len();
    buf.truncate(n);
    Ok(buf)
}

/// `nonce ‖ ciphertext ‖ tag` of `plain` under `key`, bound to `aad`.
#[cfg(not(feature = "fips"))]
pub fn seal(key: &[u8; 32], plain: &[u8], aad: &[u8]) -> Result<Vec<u8>, String> {
    let cipher = Aes256Gcm::new_from_slice(key).map_err(|e| e.to_string())?;
    let mut nonce = [0u8; 12];
    fill_random(&mut nonce)?;
    let ct = cipher
        .encrypt(&Nonce::from(nonce), Payload { msg: plain, aad })
        .map_err(|_| "encryption failed".to_string())?;
    let mut out = Vec::with_capacity(12 + ct.len());
    out.extend_from_slice(&nonce);
    out.extend_from_slice(&ct);
    Ok(out)
}

/// The plaintext of [`seal`]'s output, or an error when the key, the AAD or
/// a single byte is wrong.
#[cfg(not(feature = "fips"))]
pub fn open(key: &[u8; 32], sealed: &[u8], aad: &[u8]) -> Result<Zeroizing<Vec<u8>>, String> {
    if sealed.len() < 12 + 16 {
        return Err("XX001: a sealed value is too short".to_string());
    }
    let cipher = Aes256Gcm::new_from_slice(key).map_err(|e| e.to_string())?;
    let (nonce, ct) = sealed.split_at(12);
    cipher
        .decrypt(Nonce::from_slice(nonce), Payload { msg: ct, aad })
        .map(Zeroizing::new)
        .map_err(|_| "XX001: a sealed value does not authenticate: wrong key, or it was altered".to_string())
}

/// The provider a URI names.
pub fn provider(uri: &str) -> Result<Box<dyn KeyProvider>, String> {
    if let Some(path) = uri.strip_prefix("file:") {
        return Ok(Box::new(FileKey::open(path)?));
    }
    if let Some(rest) = uri.strip_prefix("pkcs11:") {
        #[cfg(feature = "pkcs11")]
        return Ok(Box::new(pkcs11::Pkcs11Key::open(rest)?));
        #[cfg(not(feature = "pkcs11"))]
        {
            let _ = rest;
            return Err("this build has no PKCS#11 key provider (feature `pkcs11`)".to_string());
        }
    }
    // FIPS mode: only what the validated module (or a validated HSM behind
    // PKCS#11) does. The ML-KEM implementation here is not inside it.
    if cfg!(feature = "fips") && uri.starts_with("mlkem:") {
        return Err("mlkem: keys are not offered in FIPS mode: the ML-KEM here is not the validated module's; \
                    use pkcs11: with a FIPS 140-3 validated HSM"
            .to_string());
    }
    if let Some(rest) = uri.strip_prefix("tpm:") {
        #[cfg(feature = "tpm")]
        return Ok(Box::new(tpm::TpmKey::open(rest)?));
        #[cfg(not(feature = "tpm"))]
        {
            let _ = rest;
            return Err("this build has no TPM key provider (feature `tpm`)".to_string());
        }
    }
    if let Some(rest) = uri.strip_prefix("mlkem:") {
        #[cfg(feature = "pqc")]
        return Ok(Box::new(pqc::MlKemKey::open(rest)?));
        #[cfg(not(feature = "pqc"))]
        {
            let _ = rest;
            return Err("this build has no post-quantum key provider (feature `pqc`)".to_string());
        }
    }
    Err(format!("{uri}: a key provider is file:<path>, pkcs11:<attributes> or mlkem:<path>"))
}

/// A key-encryption key read from a file.
pub struct FileKey {
    path: String,
    kek: Dek,
}

impl FileKey {
    pub fn open(path: &str) -> Result<FileKey, String> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(path).map_err(|e| format!("{path}: {e}"))?.permissions().mode();
            if mode & 0o077 != 0 {
                return Err(format!("{path}: a key file must be readable by its owner only (chmod 600), it is {:o}", mode & 0o777));
            }
        }
        let raw = Zeroizing::new(std::fs::read(path).map_err(|e| format!("{path}: {e}"))?);
        let mut kek = Zeroizing::new([0u8; 32]);
        let text = std::str::from_utf8(&raw).map(str::trim).unwrap_or("");
        if text.len() == 64 && text.bytes().all(|b| b.is_ascii_hexdigit()) {
            for i in 0..32 {
                kek[i] = u8::from_str_radix(&text[2 * i..2 * i + 2], 16).map_err(|e| e.to_string())?;
            }
        } else if raw.len() == 32 {
            kek.copy_from_slice(&raw);
        } else {
            return Err(format!("{path}: a key file holds 32 bytes or 64 hex characters"));
        }
        Ok(FileKey { path: path.to_string(), kek })
    }
}

const WRAP_AAD: &[u8] = b"rdb-dek-wrap-v1";

impl KeyProvider for FileKey {
    fn describe(&self) -> String {
        format!("file:{}", self.path)
    }

    fn wrap(&self, dek: &[u8; 32]) -> Result<Vec<u8>, String> {
        seal(&self.kek, dek, WRAP_AAD)
    }

    fn unwrap(&self, wrapped: &[u8]) -> Result<Dek, String> {
        let plain = open(&self.kek, wrapped, WRAP_AAD)?;
        let mut k = Zeroizing::new([0u8; 32]);
        if plain.len() != 32 {
            return Err("XX001: an unwrapped data key is not 32 bytes".to_string());
        }
        k.copy_from_slice(&plain);
        Ok(k)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sealing_round_trips_and_any_change_fails() {
        let k = new_dek().unwrap();
        let s = seal(&k, b"hunter2", b"aad").unwrap();
        assert_eq!(&open(&k, &s, b"aad").unwrap()[..], b"hunter2");
        assert!(open(&k, &s, b"other").is_err());
        let mut t = s.clone();
        t[20] ^= 1;
        assert!(open(&k, &t, b"aad").is_err());
        let other = new_dek().unwrap();
        assert!(open(&other, &s, b"aad").is_err());
    }

    #[test]
    fn a_file_key_must_be_private_and_wraps() {
        let path = std::env::temp_dir().join(format!("rdb-kek-{}", std::process::id()));
        std::fs::write(&path, "00".repeat(32)).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
            assert!(FileKey::open(path.to_str().unwrap()).is_err());
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        let p = FileKey::open(path.to_str().unwrap()).unwrap();
        let dek = new_dek().unwrap();
        let w = p.wrap(&dek).unwrap();
        assert_eq!(*p.unwrap(&w).unwrap(), *dek);
        let _ = std::fs::remove_file(&path);
    }
}
