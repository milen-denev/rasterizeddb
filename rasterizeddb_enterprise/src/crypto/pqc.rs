//! Post-quantum key wrapping (docs/ledger-and-vault-plan.md §4.5, feature
//! `pqc`).
//!
//! What a quantum computer threatens here is public-key cryptography. AES-256
//! (data) and SHA-256 (the chain) keep a margin of about 128 bits even
//! against Grover's search. So the post-quantum option replaces the one
//! place a key could travel under public-key cryptography: wrapping data keys
//! for a holder of a private key that is not on this host.
//!
//! The `mlkem:<key file>` provider wraps each data key with a **hybrid**
//! KEM: ML-KEM-768 (FIPS 203) *and* X25519. The wrapping key is derived from
//! both shared secrets, so breaking it needs breaking both. A flaw found in
//! the young post-quantum scheme still leaves X25519, and a quantum computer
//! still faces ML-KEM:
//!
//! ```text
//! kek     = SHA-256("rdb-hybrid-kem-v1" ‖ ss_mlkem ‖ ss_x25519 ‖ ct_mlkem ‖ x25519_ephemeral_public)
//! wrapped = ct_mlkem (1088) ‖ x25519_ephemeral_public (32) ‖ AES-256-GCM(kek, dek)
//! ```
//!
//! The key file holds the three 32-byte seeds the private keys are derived
//! from (ML-KEM's `d` and `z`, X25519's secret) as 192 hex characters, and
//! must be readable by its owner only. `mlkem:<path>;create` makes one when
//! none exists.

use ml_kem::kem::Decapsulate;
use ml_kem::{B32, EncapsulateDeterministic, EncodedSizeUser, KemCore, MlKem768};
use sha2::{Digest, Sha256};
use x25519_dalek::{PublicKey, StaticSecret};
use zeroize::Zeroizing;

use super::{Dek, KeyProvider, fill_random, open, seal};

type Dk = <MlKem768 as KemCore>::DecapsulationKey;
type Ek = <MlKem768 as KemCore>::EncapsulationKey;

const CT_LEN: usize = 1088;
const AAD: &[u8] = b"rdb-hybrid-wrap-v1";

pub struct MlKemKey {
    path: String,
    dk: Dk,
    ek: Ek,
    x_secret: StaticSecret,
    x_public: PublicKey,
}

fn hex32(s: &str) -> Result<Zeroizing<[u8; 32]>, String> {
    let mut out = Zeroizing::new([0u8; 32]);
    for i in 0..32 {
        out[i] = u8::from_str_radix(s.get(2 * i..2 * i + 2).ok_or("short key")?, 16).map_err(|e| e.to_string())?;
    }
    Ok(out)
}

impl MlKemKey {
    pub fn open(spec: &str) -> Result<MlKemKey, String> {
        let (path, create) = match spec.strip_suffix(";create") {
            Some(p) => (p, true),
            None => (spec, false),
        };
        if create && !std::path::Path::new(path).exists() {
            let mut seeds = Zeroizing::new([0u8; 96]);
            fill_random(&mut seeds[..])?;
            let text: Zeroizing<String> = Zeroizing::new(seeds.iter().map(|b| format!("{b:02x}")).collect());
            #[cfg(unix)]
            {
                use std::io::Write;
                use std::os::unix::fs::OpenOptionsExt;
                let mut f = std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .mode(0o600)
                    .open(path)
                    .map_err(|e| format!("{path}: {e}"))?;
                f.write_all(text.as_bytes()).and_then(|()| f.sync_all()).map_err(|e| format!("{path}: {e}"))?;
            }
            #[cfg(not(unix))]
            std::fs::write(path, text.as_bytes()).map_err(|e| format!("{path}: {e}"))?;
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(path).map_err(|e| format!("{path}: {e}"))?.permissions().mode();
            if mode & 0o077 != 0 {
                return Err(format!("{path}: a key file must be readable by its owner only (chmod 600)"));
            }
        }
        let text = Zeroizing::new(std::fs::read_to_string(path).map_err(|e| format!("{path}: {e}"))?);
        let t = text.trim();
        if t.len() != 192 || !t.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(format!("{path}: an ML-KEM key file holds 192 hex characters"));
        }
        let d = hex32(&t[0..64])?;
        let z = hex32(&t[64..128])?;
        let x = hex32(&t[128..192])?;
        let (dk, ek) = MlKem768::generate_deterministic(&B32::from(*d), &B32::from(*z));
        let x_secret = StaticSecret::from(*x);
        let x_public = PublicKey::from(&x_secret);
        Ok(MlKemKey { path: path.to_string(), dk, ek, x_secret, x_public })
    }

    fn kek(ss_kem: &[u8], ss_x: &[u8], ct: &[u8], eph: &[u8; 32]) -> Zeroizing<[u8; 32]> {
        let mut h = Sha256::new();
        h.update(b"rdb-hybrid-kem-v1");
        h.update(ss_kem);
        h.update(ss_x);
        h.update(ct);
        h.update(eph);
        Zeroizing::new(h.finalize().into())
    }

    /// The public half, for a wrapper on another host: ML-KEM's encapsulation
    /// key then X25519's public key, hex.
    pub fn public_hex(&self) -> String {
        let mut out: String = self.ek.as_bytes().iter().map(|b| format!("{b:02x}")).collect();
        out.extend(self.x_public.as_bytes().iter().map(|b| format!("{b:02x}")));
        out
    }
}

impl KeyProvider for MlKemKey {
    fn describe(&self) -> String {
        format!("mlkem:{} (ML-KEM-768 + X25519)", self.path)
    }

    fn wrap(&self, dek: &[u8; 32]) -> Result<Vec<u8>, String> {
        let mut m = Zeroizing::new([0u8; 32]);
        fill_random(&mut m[..])?;
        let (ct, ss_kem) =
            self.ek.encapsulate_deterministic(&B32::from(*m)).map_err(|_| "ML-KEM encapsulation failed".to_string())?;
        let mut eph = Zeroizing::new([0u8; 32]);
        fill_random(&mut eph[..])?;
        let eph_secret = StaticSecret::from(*eph);
        let eph_public = PublicKey::from(&eph_secret);
        let ss_x = eph_secret.diffie_hellman(&self.x_public);
        let kek = Self::kek(ss_kem.as_slice(), ss_x.as_bytes(), ct.as_slice(), eph_public.as_bytes());
        let sealed = seal(&kek, dek, AAD)?;
        let mut out = Vec::with_capacity(CT_LEN + 32 + sealed.len());
        out.extend_from_slice(ct.as_slice());
        out.extend_from_slice(eph_public.as_bytes());
        out.extend_from_slice(&sealed);
        Ok(out)
    }

    fn unwrap(&self, wrapped: &[u8]) -> Result<Dek, String> {
        if wrapped.len() < CT_LEN + 32 + 28 {
            return Err("XX001: a hybrid-wrapped data key is too short".to_string());
        }
        let (ct, rest) = wrapped.split_at(CT_LEN);
        let (eph, sealed) = rest.split_at(32);
        let ct_arr = ml_kem::array::Array::try_from(ct).map_err(|_| "XX001: bad ML-KEM ciphertext".to_string())?;
        let ss_kem = self.dk.decapsulate(&ct_arr).map_err(|_| "XX001: ML-KEM decapsulation failed".to_string())?;
        let eph: [u8; 32] = eph.try_into().unwrap();
        let ss_x = self.x_secret.diffie_hellman(&PublicKey::from(eph));
        let kek = Self::kek(ss_kem.as_slice(), ss_x.as_bytes(), ct, &eph);
        let plain = open(&kek, sealed, AAD)?;
        if plain.len() != 32 {
            return Err("XX001: an unwrapped data key is not 32 bytes".to_string());
        }
        let mut k = Zeroizing::new([0u8; 32]);
        k.copy_from_slice(&plain);
        Ok(k)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_hybrid_wrap_round_trips_and_resists_tampering() {
        let path = std::env::temp_dir().join(format!("rdb-mlkem-{}", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let k = MlKemKey::open(&format!("{};create", path.display())).unwrap();
        let dek = super::super::new_dek().unwrap();
        let w = k.wrap(&dek).unwrap();
        assert_eq!(*k.unwrap(&w).unwrap(), *dek);
        let mut bad = w.clone();
        bad[5] ^= 1; // the ML-KEM ciphertext: implicit rejection gives another key
        assert!(k.unwrap(&bad).is_err());
        let mut bad = w.clone();
        bad[CT_LEN + 3] ^= 1; // the X25519 ephemeral key
        assert!(k.unwrap(&bad).is_err());
        let again = MlKemKey::open(path.to_str().unwrap()).unwrap();
        assert_eq!(*again.unwrap(&w).unwrap(), *dek, "the key file gives back the same keys");
        let _ = std::fs::remove_file(&path);
    }
}
