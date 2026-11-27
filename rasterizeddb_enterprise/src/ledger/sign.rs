//! Anchor signatures (docs/ledger-and-vault-plan.md §4.5, feature `pqc`):
//! ML-DSA-65 (FIPS 204), so an anchor proves which database wrote it, and
//! keeps proving it after quantum computers can forge today's signatures.
//!
//! - `RDB_LEDGER_SIGNING_KEY=<file>`: 64 hex characters, a seed the signing
//!   key is derived from. The file must be readable by its owner only, and
//!   with it set every anchor carries a `sig-mldsa65` line.
//! - `RDB_LEDGER_VERIFY_KEY=<hex>`: the public key an auditor checks the
//!   signatures with, printed by `SHOW LEDGER` when signing is on. Without
//!   it, a signed anchor is checked against the signing key, when this
//!   process has one.

use std::sync::OnceLock;

use ml_dsa::{KeyExport, KeyInit, Keypair, MlDsa65, Signature, SignatureEncoding, Signer, SigningKey, Verifier, VerifyingKey};

static KEY: OnceLock<Option<SigningKey<MlDsa65>>> = OnceLock::new();

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn unhex(s: &str) -> Option<Vec<u8>> {
    if s.len() % 2 != 0 {
        return None;
    }
    (0..s.len() / 2).map(|i| u8::from_str_radix(&s[2 * i..2 * i + 2], 16).ok()).collect()
}

fn key() -> Option<&'static SigningKey<MlDsa65>> {
    KEY.get_or_init(|| {
        let path = std::env::var("RDB_LEDGER_SIGNING_KEY").ok()?;
        if cfg!(feature = "fips") {
            log::error!("RDB_LEDGER_SIGNING_KEY is ignored in FIPS mode: this ML-DSA is not the validated module's");
            return None;
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if std::fs::metadata(&path).ok()?.permissions().mode() & 0o077 != 0 {
                log::error!("{path}: the ledger signing key must be readable by its owner only; anchors go unsigned");
                return None;
            }
        }
        let text = zeroize::Zeroizing::new(std::fs::read_to_string(&path).ok()?);
        let seed = zeroize::Zeroizing::new(unhex(text.trim())?);
        let seed: [u8; 32] = seed.as_slice().try_into().ok()?;
        Some(SigningKey::<MlDsa65>::from_seed(&seed.into()))
    })
    .as_ref()
}

/// The verifying key, hex, when signing is on.
pub fn public_hex() -> Option<String> {
    key().map(|k| hex(k.verifying_key().to_bytes().as_slice()))
}

/// The signature line for `text`, when signing is on.
pub fn sign(text: &str) -> Option<String> {
    let sig: Signature<MlDsa65> = key()?.sign(text.as_bytes());
    Some(format!("sig-mldsa65 {}\n", hex(sig.to_bytes().as_slice())))
}

/// Whether `sig_hex` signs `text`: `None` when there is no key to check with.
pub fn verify(text: &str, sig_hex: &str) -> Option<bool> {
    let vk: VerifyingKey<MlDsa65> = match std::env::var("RDB_LEDGER_VERIFY_KEY") {
        Ok(h) => VerifyingKey::new_from_slice(&unhex(&h)?).ok()?,
        Err(_) => key()?.verifying_key(),
    };
    let Some(bytes) = unhex(sig_hex) else { return Some(false) };
    let Ok(sig) = Signature::<MlDsa65>::try_from(bytes.as_slice()) else { return Some(false) };
    Some(vk.verify(text.as_bytes(), &sig).is_ok())
}
