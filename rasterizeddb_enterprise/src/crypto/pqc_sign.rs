//! ML-DSA-65 with a vault key's 32-byte material as the seed (`VAULT … SIGN
//! KEY`, a key of type `mldsa65`). The signing key is derived on each use and
//! dropped after; it is never stored or returned.

use ml_dsa::{KeyExport, Keypair, MlDsa65, Signature, SignatureEncoding, Signer, SigningKey, Verifier};

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn key(seed: &[u8; 32]) -> SigningKey<MlDsa65> {
    SigningKey::<MlDsa65>::from_seed(&(*seed).into())
}

/// The signature, hex.
pub fn sign(seed: &[u8; 32], msg: &[u8]) -> String {
    let sig: Signature<MlDsa65> = key(seed).sign(msg);
    hex(sig.to_bytes().as_slice())
}

/// Whether `sig_hex` signs `msg`.
pub fn verify(seed: &[u8; 32], msg: &[u8], sig_hex: &str) -> bool {
    let Some(bytes) = (0..sig_hex.len() / 2).map(|i| u8::from_str_radix(sig_hex.get(2 * i..2 * i + 2)?, 16).ok()).collect::<Option<Vec<u8>>>() else {
        return false;
    };
    let Ok(sig) = Signature::<MlDsa65>::try_from(bytes.as_slice()) else { return false };
    key(seed).verifying_key().verify(msg, &sig).is_ok()
}

/// The verifying key, hex: what anyone checks signatures with.
pub fn public(seed: &[u8; 32]) -> String {
    hex(key(seed).verifying_key().to_bytes().as_slice())
}

#[cfg(test)]
mod tests {
    #[test]
    fn signs_and_verifies() {
        let seed = [7u8; 32];
        let s = super::sign(&seed, b"msg");
        assert!(super::verify(&seed, b"msg", &s));
        assert!(!super::verify(&seed, b"other", &s));
        assert!(!super::public(&seed).is_empty());
    }
}
