//! The PKCS#11 key provider: the key-encryption key lives in an HSM, a
//! cloud HSM (AWS CloudHSM, Azure Managed HSM, Thales/Entrust Luna) or a
//! TPM 2.0 through tpm2-pkcs11, and never leaves it. A data key is wrapped
//! by asking the device to encrypt it, and unwrapped by asking it to
//! decrypt.
//!
//! ```text
//! pkcs11:module=/usr/lib/x86_64-linux-gnu/softhsm/libsofthsm2.so;token=rdb;key=vault-kek;pin-source=/etc/rasterizeddb/hsm.pin
//! ```
//!
//! - `module`: the vendor's PKCS#11 library, loaded at run time.
//! - `token`: the token label.
//! - `key`: the AES key object's label (`CKA_SENSITIVE`, not
//!   `CKA_EXTRACTABLE`).
//! - `pin-source=<file>` or `pin-env=<VAR>`: the user PIN, never in SQL.
//! - `mechanism=gcm` (AES-256-GCM on the token, the default: the widest
//!   supported, SoftHSM and the cloud HSMs included), `mechanism=kwp` (AES
//!   key wrap with padding, RFC 5649, where the token encrypts with it) or
//!   `mechanism=cbc` (AES-CBC-PAD with a random IV), for tokens with neither,
//!   as some TPM stacks are. A vault keeps the mechanism it was made with.

use std::collections::HashMap;

use cryptoki::context::{CInitializeArgs, Pkcs11};
use cryptoki::mechanism::Mechanism;
use cryptoki::object::{Attribute, AttributeType, KeyType, ObjectClass, ObjectHandle};
use cryptoki::session::{Session, UserType};
use cryptoki::types::AuthPin;
use parking_lot::Mutex;
use zeroize::Zeroizing;

use super::{Dek, HsmKeys, KeyProvider, fill_random};

pub struct Pkcs11Key {
    describe: String,
    mech: Mech,
    // One logged-in session, used one call at a time.
    session: Mutex<(Session, Option<ObjectHandle>)>,
    _context: Pkcs11,
}

fn attrs(rest: &str) -> HashMap<String, String> {
    rest.split(';')
        .filter_map(|kv| kv.split_once('='))
        .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_string()))
        .collect()
}

impl Pkcs11Key {
    pub fn open(rest: &str) -> Result<Pkcs11Key, String> {
        let a = attrs(rest);
        let need = |k: &str| a.get(k).cloned().ok_or_else(|| format!("pkcs11: `{k}=` is required"));
        let module = need("module")?;
        let token = need("token")?;
        let key = need("key")?;
        let pin: Zeroizing<String> = Zeroizing::new(match (a.get("pin-source"), a.get("pin-env")) {
            (Some(file), _) => std::fs::read_to_string(file).map_err(|e| format!("pkcs11 pin-source {file}: {e}"))?.trim().to_string(),
            (None, Some(var)) => std::env::var(var).map_err(|_| format!("pkcs11: {var} is not set"))?,
            (None, None) => return Err("pkcs11: give the PIN by pin-source=<file> or pin-env=<VAR>".to_string()),
        });
        let mech = match a.get("mechanism").map(|m| m.to_ascii_lowercase()).as_deref() {
            None | Some("gcm") => Mech::Gcm,
            Some("kwp") => Mech::Kwp,
            Some("cbc") => Mech::Cbc,
            Some(other) => return Err(format!("pkcs11: mechanism={other}: gcm, kwp or cbc")),
        };
        let context = Pkcs11::new(&module).map_err(|e| format!("pkcs11 module {module}: {e}"))?;
        // A module another provider in this process already initialized
        // (two vaults on one HSM) answers CKR_CRYPTOKI_ALREADY_INITIALIZED:
        // usable as it is.
        match context.initialize(CInitializeArgs::OsThreads) {
            Ok(()) => {}
            Err(cryptoki::error::Error::Pkcs11(cryptoki::error::RvError::CryptokiAlreadyInitialized, _)) => {}
            Err(e) => return Err(format!("pkcs11 initialize: {e}")),
        }
        let slot = context
            .get_slots_with_token()
            .map_err(|e| format!("pkcs11 slots: {e}"))?
            .into_iter()
            .find(|s| context.get_token_info(*s).is_ok_and(|i| i.label().trim() == token))
            .ok_or_else(|| format!("pkcs11: no token labelled {token}"))?;
        // Read-write: keys used in place are generated on the token.
        let session = context.open_rw_session(slot).map_err(|e| format!("pkcs11 session: {e}"))?;
        // Login is per application in PKCS#11: a second provider on the same
        // token (two vaults on one HSM) finds the user logged in already.
        match session.login(UserType::User, Some(&AuthPin::from(pin.to_string()))) {
            Ok(()) => {}
            Err(cryptoki::error::Error::Pkcs11(cryptoki::error::RvError::UserAlreadyLoggedIn, _)) => {}
            Err(e) => return Err(format!("pkcs11 login: {e}")),
        }
        let handle = session
            .find_objects(&[Attribute::Class(ObjectClass::SECRET_KEY), Attribute::Label(key.as_bytes().to_vec())])
            .map_err(|e| format!("pkcs11 find key: {e}"))?
            .into_iter()
            .next()
            .ok_or_else(|| format!("pkcs11: no secret key labelled {key} on token {token}"))?;
        Ok(Pkcs11Key {
            describe: format!("pkcs11:module={module};token={token};key={key}"),
            mech,
            session: Mutex::new((session, Some(handle))),
            _context: context,
        })
    }
}

impl KeyProvider for Pkcs11Key {
    fn describe(&self) -> String {
        self.describe.clone()
    }

    fn hsm(&self) -> Option<&dyn HsmKeys> {
        Some(self)
    }

    fn wrap(&self, dek: &[u8; 32]) -> Result<Vec<u8>, String> {
        let s = self.session.lock();
        let err = |e: cryptoki::error::Error| format!("pkcs11 wrap: {e}");
        match self.mech {
            Mech::Gcm => {
                let mut iv = [0u8; 12];
                fill_random(&mut iv)?;
                let mut iv_param = iv;
                let params = cryptoki::mechanism::aead::GcmParams::new(&mut iv_param, WRAP_AAD, 128.into()).map_err(err)?;
                let ct = s.0.encrypt(&Mechanism::AesGcm(params), kek(&s)?, dek).map_err(err)?;
                let mut out = iv.to_vec();
                out.extend_from_slice(&ct);
                Ok(out)
            }
            Mech::Cbc => {
                let mut iv = [0u8; 16];
                fill_random(&mut iv)?;
                let ct = s.0.encrypt(&Mechanism::AesCbcPad(iv), kek(&s)?, dek).map_err(err)?;
                let mut out = iv.to_vec();
                out.extend_from_slice(&ct);
                Ok(out)
            }
            Mech::Kwp => s.0.encrypt(&Mechanism::AesKeyWrapPad, kek(&s)?, dek).map_err(err),
        }
    }

    fn unwrap(&self, wrapped: &[u8]) -> Result<Dek, String> {
        let s = self.session.lock();
        let short = || "XX001: a wrapped data key is too short".to_string();
        let err = |e: cryptoki::error::Error| format!("pkcs11 unwrap: {e}");
        let plain = Zeroizing::new(match self.mech {
            Mech::Gcm => {
                if wrapped.len() < 12 + 16 {
                    return Err(short());
                }
                let (iv, ct) = wrapped.split_at(12);
                let mut iv: [u8; 12] = iv.try_into().unwrap();
                let params = cryptoki::mechanism::aead::GcmParams::new(&mut iv, WRAP_AAD, 128.into()).map_err(err)?;
                s.0.decrypt(&Mechanism::AesGcm(params), kek(&s)?, ct).map_err(err)?
            }
            Mech::Cbc => {
                if wrapped.len() < 16 {
                    return Err(short());
                }
                let (iv, ct) = wrapped.split_at(16);
                s.0.decrypt(&Mechanism::AesCbcPad(iv.try_into().unwrap()), kek(&s)?, ct).map_err(err)?
            }
            Mech::Kwp => s.0.decrypt(&Mechanism::AesKeyWrapPad, kek(&s)?, wrapped).map_err(err)?,
        });
        if plain.len() != 32 {
            return Err("XX001: an unwrapped data key is not 32 bytes".to_string());
        }
        let mut k = Zeroizing::new([0u8; 32]);
        k.copy_from_slice(&plain);
        Ok(k)
    }
}

/// How data keys are wrapped on the token.
#[derive(Clone, Copy, Debug)]
enum Mech {
    Gcm,
    Kwp,
    Cbc,
}

/// Bound into every GCM wrap, so a wrapped key is not taken for anything else.
const WRAP_AAD: &[u8] = b"rdb-dek-wrap-pkcs11-v1";

/// The key-encryption key's handle.
fn kek(s: &(Session, Option<ObjectHandle>)) -> Result<ObjectHandle, String> {
    s.1.ok_or_else(|| "pkcs11: this session has no key-encryption key".to_string())
}

/// P-256's OID as `CKA_EC_PARAMS` wants it (DER).
const P256: &[u8] = &[0x06, 0x08, 0x2A, 0x86, 0x48, 0xCE, 0x3D, 0x03, 0x01, 0x07];

impl Pkcs11Key {
    fn find(&self, session: &Session, class: ObjectClass, label: &str) -> Result<ObjectHandle, String> {
        session
            .find_objects(&[Attribute::Class(class), Attribute::Label(label.as_bytes().to_vec())])
            .map_err(|e| format!("pkcs11 find {label}: {e}"))?
            .into_iter()
            .next()
            .ok_or_else(|| format!("42704: the token holds no key labelled {label}"))
    }

    fn exists(&self, session: &Session, label: &str) -> bool {
        session
            .find_objects(&[Attribute::Label(label.as_bytes().to_vec())])
            .is_ok_and(|v| !v.is_empty())
    }
}

impl HsmKeys for Pkcs11Key {
    fn generate_aes(&self, label: &str) -> Result<(), String> {
        let s = self.session.lock();
        if self.exists(&s.0, label) {
            return Err(format!("42710: the token already holds a key labelled {label}"));
        }
        s.0.generate_key(
            &Mechanism::AesKeyGen,
            &[
                Attribute::Class(ObjectClass::SECRET_KEY),
                Attribute::KeyType(KeyType::AES),
                Attribute::ValueLen(32.into()),
                Attribute::Token(true),
                Attribute::Private(true),
                Attribute::Sensitive(true),
                Attribute::Extractable(false),
                Attribute::Encrypt(true),
                Attribute::Decrypt(true),
                Attribute::Label(label.as_bytes().to_vec()),
            ],
        )
        .map(drop)
        .map_err(|e| format!("pkcs11 generate AES key: {e}"))
    }

    fn aes_encrypt(&self, label: &str, plain: &[u8], aad: &[u8]) -> Result<Vec<u8>, String> {
        let s = self.session.lock();
        let key = self.find(&s.0, ObjectClass::SECRET_KEY, label)?;
        let mut iv = [0u8; 12];
        fill_random(&mut iv)?;
        let mut iv_param = iv;
        let params = cryptoki::mechanism::aead::GcmParams::new(&mut iv_param, aad, 128.into()).map_err(|e| e.to_string())?;
        let ct = s.0.encrypt(&Mechanism::AesGcm(params), key, plain).map_err(|e| format!("pkcs11 AES-GCM: {e}"))?;
        let mut out = iv.to_vec();
        out.extend_from_slice(&ct);
        Ok(out)
    }

    fn aes_decrypt(&self, label: &str, sealed: &[u8], aad: &[u8]) -> Result<Zeroizing<Vec<u8>>, String> {
        if sealed.len() < 12 + 16 {
            return Err("XX001: a sealed value is too short".to_string());
        }
        let s = self.session.lock();
        let key = self.find(&s.0, ObjectClass::SECRET_KEY, label)?;
        let (iv, ct) = sealed.split_at(12);
        let mut iv: [u8; 12] = iv.try_into().unwrap();
        let params = cryptoki::mechanism::aead::GcmParams::new(&mut iv, aad, 128.into()).map_err(|e| e.to_string())?;
        s.0.decrypt(&Mechanism::AesGcm(params), key, ct)
            .map(Zeroizing::new)
            .map_err(|_| "XX001: a sealed value does not authenticate: wrong key, or it was altered".to_string())
    }

    fn generate_ec(&self, label: &str) -> Result<(), String> {
        let s = self.session.lock();
        if self.exists(&s.0, label) {
            return Err(format!("42710: the token already holds a key labelled {label}"));
        }
        s.0.generate_key_pair(
            &Mechanism::EccKeyPairGen,
            &[
                Attribute::Token(true),
                Attribute::Verify(true),
                Attribute::EcParams(P256.to_vec()),
                Attribute::Label(label.as_bytes().to_vec()),
            ],
            &[
                Attribute::Token(true),
                Attribute::Private(true),
                Attribute::Sensitive(true),
                Attribute::Extractable(false),
                Attribute::Sign(true),
                Attribute::Label(label.as_bytes().to_vec()),
            ],
        )
        .map(drop)
        .map_err(|e| format!("pkcs11 generate EC key pair: {e}"))
    }

    fn ec_sign(&self, label: &str, message: &[u8]) -> Result<Vec<u8>, String> {
        let s = self.session.lock();
        let key = self.find(&s.0, ObjectClass::PRIVATE_KEY, label)?;
        let digest = rasterizeddb_core::core::fips::sha256(message);
        s.0.sign(&Mechanism::Ecdsa, key, &digest).map_err(|e| format!("pkcs11 ECDSA sign: {e}"))
    }

    fn ec_verify(&self, label: &str, message: &[u8], signature: &[u8]) -> Result<bool, String> {
        let s = self.session.lock();
        let key = self.find(&s.0, ObjectClass::PUBLIC_KEY, label)?;
        let digest = rasterizeddb_core::core::fips::sha256(message);
        Ok(s.0.verify(&Mechanism::Ecdsa, key, &digest, signature).is_ok())
    }

    fn ec_public(&self, label: &str) -> Result<Vec<u8>, String> {
        let s = self.session.lock();
        let key = self.find(&s.0, ObjectClass::PUBLIC_KEY, label)?;
        let attrs = s.0.get_attributes(key, &[AttributeType::EcPoint]).map_err(|e| format!("pkcs11 read public key: {e}"))?;
        attrs
            .into_iter()
            .find_map(|a| match a {
                Attribute::EcPoint(p) => Some(p),
                _ => None,
            })
            .ok_or_else(|| "the token gave no EC point".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Against SoftHSM when `RDB_TEST_SOFTHSM_MODULE` names its library and
    /// `SOFTHSM2_CONF` a configuration whose token `rdb-test` (user PIN
    /// `1234`) exists; skipped otherwise.
    #[test]
    fn keys_wrap_and_are_used_inside_the_token() {
        let Ok(module) = std::env::var("RDB_TEST_SOFTHSM_MODULE") else { return };
        let uri = format!("module={module};token=rdb-test;key=kek;pin-env=RDB_TEST_SOFTHSM_PIN");
        // SAFETY: the test process's own environment, set before use.
        unsafe { std::env::set_var("RDB_TEST_SOFTHSM_PIN", "1234") };
        // The KEK itself is made inside the token first: opening needs it.
        {
            let k = Pkcs11Key::open(&uri.replace("key=kek", "key=bootstrap-missing"));
            assert!(k.is_err(), "no such key yet");
        }
        let boot = bootstrap(&module);
        if let Err(e) = boot.generate_aes("kek") {
            assert!(e.contains("already holds"), "making the KEK: {e}");
        }
        let k = Pkcs11Key::open(&uri).unwrap_or_else(|e| panic!("the KEK is on the token: {e}"));
        let dek = crate::crypto::new_dek().unwrap();
        let wrapped = k.wrap(&dek).unwrap();
        assert_eq!(*k.unwrap(&wrapped).unwrap(), *dek);

        let label = format!("rdb:test:{}", std::process::id());
        k.generate_aes(&label).unwrap();
        let sealed = k.aes_encrypt(&label, b"payload", b"aad").unwrap();
        assert_eq!(&k.aes_decrypt(&label, &sealed, b"aad").unwrap()[..], b"payload");
        assert!(k.aes_decrypt(&label, &sealed, b"other").is_err());

        let ec = format!("rdb:test-ec:{}", std::process::id());
        k.generate_ec(&ec).unwrap();
        let sig = k.ec_sign(&ec, b"message").unwrap();
        assert!(k.ec_verify(&ec, b"message", &sig).unwrap());
        assert!(!k.ec_verify(&ec, b"tampered", &sig).unwrap());
        assert!(!k.ec_public(&ec).unwrap().is_empty());
    }

    /// A session on the test token whose AES key `kek` may not exist yet.
    fn bootstrap(module: &str) -> Pkcs11Key {
        let context = Pkcs11::new(module).unwrap();
        context.initialize(CInitializeArgs::OsThreads).ok();
        let slot = context
            .get_slots_with_token()
            .unwrap()
            .into_iter()
            .find(|s| context.get_token_info(*s).is_ok_and(|i| i.label().trim() == "rdb-test"))
            .expect("token rdb-test");
        let session = context.open_rw_session(slot).unwrap();
        session.login(UserType::User, Some(&AuthPin::from("1234".to_string()))).ok();
        Pkcs11Key {
            describe: "bootstrap".into(),
            mech: Mech::Gcm,
            session: Mutex::new((session, None)),
            _context: context,
        }
    }
}
