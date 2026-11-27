//! The TPM 2.0 key provider (`tpm:`, feature `tpm`): the vault's
//! key-encryption key is **sealed** by this machine's TPM. The blob on disk
//! is useless anywhere else: only the TPM that sealed it, with its storage
//! hierarchy unchanged, can unseal it. A stolen disk or a copied data
//! directory does not carry the key.
//!
//! ```text
//! tpm:/etc/rasterizeddb/vault.tpmkey[;tcti=device:/dev/tpmrm0][;create]
//! ```
//!
//! - The path holds the sealed object (its public and private parts),
//!   created on first use with `;create`, readable by its owner only.
//! - `tcti` selects the TPM: `device:/dev/tpmrm0` (the default),
//!   `swtpm:port=2321` for a software TPM in tests, or `tabrmd`.
//! - The KEK is unsealed once, at open, over an encrypted HMAC session (the
//!   value does not cross the bus in the clear). It then lives in locked
//!   memory and wraps data keys with AES-256-GCM, as the file provider does.
//!
//! The server needs access to the TPM device. On Linux that means
//! membership of the `tss` group for `/dev/tpmrm0`.

use std::str::FromStr;

use tss_esapi::attributes::{ObjectAttributesBuilder, SessionAttributesBuilder};
use tss_esapi::constants::SessionType;
use tss_esapi::interface_types::algorithm::{HashingAlgorithm, PublicAlgorithm};
use tss_esapi::interface_types::key_bits::RsaKeyBits;
use tss_esapi::interface_types::resource_handles::Hierarchy;
use tss_esapi::structures::{
    KeyedHashScheme, Private, Public, PublicBuilder, PublicKeyedHashParameters, RsaExponent, SensitiveData,
    SymmetricDefinition, SymmetricDefinitionObject,
};
use tss_esapi::tcti_ldr::TctiNameConf;
use tss_esapi::traits::{Marshall, UnMarshall};
use tss_esapi::Context;
use zeroize::Zeroizing;

use super::{Dek, KeyProvider, fill_random, open, seal};

const MAGIC: &[u8] = b"RDBTPM1\n";
const WRAP_AAD: &[u8] = b"rdb-dek-wrap-tpm-v1";

pub struct TpmKey {
    path: String,
    kek: Dek,
}

fn tpm_err(e: tss_esapi::Error) -> String {
    format!("tpm: {e}")
}

/// A context with an encrypted HMAC session, so a parameter (the sealed
/// secret) crosses to and from the TPM encrypted.
fn context(tcti: &str) -> Result<Context, String> {
    let conf = TctiNameConf::from_str(tcti).map_err(tpm_err)?;
    let mut ctx = Context::new(conf).map_err(tpm_err)?;
    let session = ctx
        .start_auth_session(None, None, None, SessionType::Hmac, SymmetricDefinition::AES_256_CFB, HashingAlgorithm::Sha256)
        .map_err(tpm_err)?;
    let (attrs, mask) = SessionAttributesBuilder::new().with_decrypt(true).with_encrypt(true).build();
    let session = session.ok_or("tpm: no session")?;
    ctx.tr_sess_set_attributes(session, attrs, mask).map_err(tpm_err)?;
    ctx.set_sessions((Some(session), None, None));
    Ok(ctx)
}

/// The storage root key: the same template under the owner hierarchy gives
/// the same key every time, so nothing about it is stored.
fn srk(ctx: &mut Context) -> Result<tss_esapi::handles::KeyHandle, String> {
    let public = tss_esapi::utils::create_restricted_decryption_rsa_public(
        SymmetricDefinitionObject::AES_256_CFB,
        RsaKeyBits::Rsa2048,
        RsaExponent::default(),
    )
    .map_err(tpm_err)?;
    Ok(ctx.create_primary(Hierarchy::Owner, public, None, None, None, None).map_err(tpm_err)?.key_handle)
}

fn sealed_template() -> Result<Public, String> {
    let attributes = ObjectAttributesBuilder::new()
        .with_fixed_tpm(true)
        .with_fixed_parent(true)
        .with_no_da(true)
        .with_admin_with_policy(true)
        .with_user_with_auth(true)
        .build()
        .map_err(tpm_err)?;
    PublicBuilder::new()
        .with_public_algorithm(PublicAlgorithm::KeyedHash)
        .with_name_hashing_algorithm(HashingAlgorithm::Sha256)
        .with_object_attributes(attributes)
        .with_auth_policy(Default::default())
        .with_keyed_hash_parameters(PublicKeyedHashParameters::new(KeyedHashScheme::Null))
        .with_keyed_hash_unique_identifier(Default::default())
        .build()
        .map_err(tpm_err)
}

fn encode(public: &Public, private: &Private) -> Result<Vec<u8>, String> {
    let p = public.marshall().map_err(tpm_err)?;
    let s = private.value();
    let mut out = MAGIC.to_vec();
    out.extend_from_slice(&(p.len() as u32).to_le_bytes());
    out.extend_from_slice(&p);
    out.extend_from_slice(&(s.len() as u32).to_le_bytes());
    out.extend_from_slice(s);
    Ok(out)
}

fn decode(bytes: &[u8]) -> Result<(Public, Private), String> {
    let bad = || "tpm: the sealed key file is damaged".to_string();
    let rest = bytes.strip_prefix(MAGIC).ok_or_else(bad)?;
    let take = |b: &[u8]| -> Result<(Vec<u8>, usize), String> {
        let n = u32::from_le_bytes(b.get(..4).ok_or_else(bad)?.try_into().unwrap()) as usize;
        Ok((b.get(4..4 + n).ok_or_else(bad)?.to_vec(), 4 + n))
    };
    let (p, used) = take(rest)?;
    let (s, _) = take(&rest[used..])?;
    Ok((Public::unmarshall(&p).map_err(tpm_err)?, Private::try_from(s).map_err(tpm_err)?))
}

impl TpmKey {
    pub fn open(spec: &str) -> Result<TpmKey, String> {
        let mut parts = spec.split(';');
        let path = parts.next().unwrap_or("").to_string();
        let mut tcti = "device:/dev/tpmrm0".to_string();
        let mut create = false;
        for p in parts {
            match p.split_once('=') {
                Some(("tcti", v)) => tcti = v.to_string(),
                None if p == "create" => create = true,
                _ => return Err(format!("tpm: unknown attribute {p:?}")),
            }
        }
        if path.is_empty() {
            return Err("tpm: name the sealed key file: tpm:<path>".to_string());
        }
        let mut ctx = context(&tcti)?;
        let parent = srk(&mut ctx)?;
        if create && !std::path::Path::new(&path).exists() {
            let mut kek = Zeroizing::new(vec![0u8; 32]);
            fill_random(&mut kek)?;
            let sensitive = SensitiveData::try_from(kek.to_vec()).map_err(tpm_err)?;
            let created = ctx.create(parent, sealed_template()?, None, Some(sensitive), None, None).map_err(tpm_err)?;
            let blob = encode(&created.out_public, &created.out_private)?;
            write_private(&path, &blob)?;
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).map_err(|e| format!("{path}: {e}"))?.permissions().mode();
            if mode & 0o077 != 0 {
                return Err(format!("{path}: a sealed key file must be readable by its owner only (chmod 600)"));
            }
        }
        let (public, private) = decode(&std::fs::read(&path).map_err(|e| format!("{path}: {e}"))?)?;
        let loaded = ctx.load(parent, private, public).map_err(|e| {
            format!("{path}: this TPM cannot load the sealed key (sealed by another TPM, or its storage hierarchy changed): {e}")
        })?;
        let unsealed = ctx.unseal(loaded.into()).map_err(tpm_err)?;
        let value = Zeroizing::new(unsealed.value().to_vec());
        let _ = ctx.flush_context(loaded.into());
        let _ = ctx.flush_context(parent.into());
        if value.len() != 32 {
            return Err("tpm: the sealed key is not 32 bytes".to_string());
        }
        let mut kek = Zeroizing::new([0u8; 32]);
        kek.copy_from_slice(&value);
        Ok(TpmKey { path, kek })
    }
}

fn write_private(path: &str, bytes: &[u8]) -> Result<(), String> {
    use std::io::Write;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut f = options.open(path).map_err(|e| format!("{path}: {e}"))?;
    f.write_all(bytes).and_then(|()| f.sync_all()).map_err(|e| format!("{path}: {e}"))
}

impl KeyProvider for TpmKey {
    fn describe(&self) -> String {
        format!("tpm:{} (sealed by this machine's TPM)", self.path)
    }

    fn wrap(&self, dek: &[u8; 32]) -> Result<Vec<u8>, String> {
        seal(&self.kek, dek, WRAP_AAD)
    }

    fn unwrap(&self, wrapped: &[u8]) -> Result<Dek, String> {
        let plain = open(&self.kek, wrapped, WRAP_AAD)?;
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

    /// Against a TPM when `RDB_TEST_TPM_TCTI` names one (a software TPM in
    /// CI: `swtpm:port=2321`); skipped otherwise.
    #[test]
    fn a_sealed_key_unseals_on_its_tpm_and_wraps() {
        let Ok(tcti) = std::env::var("RDB_TEST_TPM_TCTI") else { return };
        let path = std::env::temp_dir().join(format!("rdb-tpm-{}.key", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let k = TpmKey::open(&format!("{};tcti={tcti};create", path.display())).unwrap();
        let dek = crate::crypto::new_dek().unwrap();
        let w = k.wrap(&dek).unwrap();
        let again = TpmKey::open(&format!("{};tcti={tcti}", path.display())).unwrap();
        assert_eq!(*again.unwrap(&w).unwrap(), *dek, "the same sealed key unseals to the same KEK");
        let mut blob = std::fs::read(&path).unwrap();
        let n = blob.len();
        blob[n - 5] ^= 1;
        std::fs::remove_file(&path).unwrap();
        write_private(path.to_str().unwrap(), &blob).unwrap();
        assert!(TpmKey::open(&format!("{};tcti={tcti}", path.display())).is_err(), "an altered blob does not load");
        let _ = std::fs::remove_file(&path);
    }
}
