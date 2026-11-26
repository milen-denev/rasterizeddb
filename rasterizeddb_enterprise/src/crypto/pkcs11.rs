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
//! - `mechanism=kwp` (AES key wrap with padding, RFC 5649, the default) or
//!   `mechanism=cbc` (AES-CBC-PAD with a random IV), for tokens without key
//!   wrap, as some TPM stacks are.

use std::collections::HashMap;

use cryptoki::context::{CInitializeArgs, Pkcs11};
use cryptoki::mechanism::Mechanism;
use cryptoki::object::{Attribute, ObjectClass, ObjectHandle};
use cryptoki::session::{Session, UserType};
use cryptoki::types::AuthPin;
use parking_lot::Mutex;
use zeroize::Zeroizing;

use super::{Dek, KeyProvider, fill_random};

pub struct Pkcs11Key {
    describe: String,
    cbc: bool,
    // One logged-in session, used one call at a time.
    session: Mutex<(Session, ObjectHandle)>,
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
        let cbc = a.get("mechanism").is_some_and(|m| m.eq_ignore_ascii_case("cbc"));
        let context = Pkcs11::new(&module).map_err(|e| format!("pkcs11 module {module}: {e}"))?;
        context.initialize(CInitializeArgs::OsThreads).map_err(|e| format!("pkcs11 initialize: {e}"))?;
        let slot = context
            .get_slots_with_token()
            .map_err(|e| format!("pkcs11 slots: {e}"))?
            .into_iter()
            .find(|s| context.get_token_info(*s).is_ok_and(|i| i.label().trim() == token))
            .ok_or_else(|| format!("pkcs11: no token labelled {token}"))?;
        let session = context.open_ro_session(slot).map_err(|e| format!("pkcs11 session: {e}"))?;
        session
            .login(UserType::User, Some(&AuthPin::from(pin.to_string())))
            .map_err(|e| format!("pkcs11 login: {e}"))?;
        let handle = session
            .find_objects(&[Attribute::Class(ObjectClass::SECRET_KEY), Attribute::Label(key.as_bytes().to_vec())])
            .map_err(|e| format!("pkcs11 find key: {e}"))?
            .into_iter()
            .next()
            .ok_or_else(|| format!("pkcs11: no secret key labelled {key} on token {token}"))?;
        Ok(Pkcs11Key {
            describe: format!("pkcs11:module={module};token={token};key={key}"),
            cbc,
            session: Mutex::new((session, handle)),
            _context: context,
        })
    }
}

impl KeyProvider for Pkcs11Key {
    fn describe(&self) -> String {
        self.describe.clone()
    }

    fn wrap(&self, dek: &[u8; 32]) -> Result<Vec<u8>, String> {
        let s = self.session.lock();
        if self.cbc {
            let mut iv = [0u8; 16];
            fill_random(&mut iv)?;
            let ct = s.0.encrypt(&Mechanism::AesCbcPad(iv), s.1, dek).map_err(|e| format!("pkcs11 wrap: {e}"))?;
            let mut out = iv.to_vec();
            out.extend_from_slice(&ct);
            Ok(out)
        } else {
            s.0.encrypt(&Mechanism::AesKeyWrapPad, s.1, dek).map_err(|e| format!("pkcs11 wrap: {e}"))
        }
    }

    fn unwrap(&self, wrapped: &[u8]) -> Result<Dek, String> {
        let s = self.session.lock();
        let plain = Zeroizing::new(if self.cbc {
            if wrapped.len() < 16 {
                return Err("XX001: a wrapped data key is too short".to_string());
            }
            let (iv, ct) = wrapped.split_at(16);
            s.0.decrypt(&Mechanism::AesCbcPad(iv.try_into().unwrap()), s.1, ct)
        } else {
            s.0.decrypt(&Mechanism::AesKeyWrapPad, s.1, wrapped)
        }
        .map_err(|e| format!("pkcs11 unwrap: {e}"))?);
        if plain.len() != 32 {
            return Err("XX001: an unwrapped data key is not 32 bytes".to_string());
        }
        let mut k = Zeroizing::new([0u8; 32]);
        k.copy_from_slice(&plain);
        Ok(k)
    }
}
