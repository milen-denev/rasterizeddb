//! Signed cluster maps (sharding-plan.md B2).
//!
//! The authority signs a map's canonical bytes (`ShardMap::to_binary`, which
//! carries the cluster, the epoch, every shard and range, and the digest)
//! with Ed25519. A shard started with `--authority-key` refuses a map whose
//! signature is missing or does not verify, at start and at `SHARD MAP
//! INSTALL`, so nobody who can reach a shard's admin port alone can hand it
//! a map. A map file's signature is the hex in `<map>.sig` beside it.

use std::sync::OnceLock;

use aws_lc_rs::signature::{ED25519, UnparsedPublicKey};
use rasterizeddb_router::ShardMap;

static AUTHORITY: OnceLock<[u8; 32]> = OnceLock::new();

/// Sets the authority's public key; once, at start.
pub fn set_authority(key: [u8; 32]) {
    let _ = AUTHORITY.set(key);
}

/// Whether maps must be signed.
pub fn required() -> bool {
    AUTHORITY.get().is_some()
}

/// Parses a public key written as 64 hex digits.
pub fn parse_key(hex: &str) -> Result<[u8; 32], String> {
    let bytes = parse_hex(hex.trim())?;
    <[u8; 32]>::try_from(bytes.as_slice()).map_err(|_| "an Ed25519 public key is 32 bytes (64 hex digits)".to_string())
}

pub fn parse_hex(hex: &str) -> Result<Vec<u8>, String> {
    if hex.len() % 2 != 0 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err("not hex".to_string());
    }
    Ok((0..hex.len()).step_by(2).map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap()).collect())
}

/// `Ok` when no authority is configured, or `signature` is the authority's
/// over `map`.
pub fn check(map: &ShardMap, signature: Option<&[u8]>) -> Result<(), String> {
    let Some(key) = AUTHORITY.get() else {
        return Ok(());
    };
    let Some(sig) = signature else {
        return Err(format!(
            "the cluster map at epoch {} is not signed, and this shard accepts only maps the authority signed",
            map.epoch()
        ));
    };
    UnparsedPublicKey::new(&ED25519, key)
        .verify(&map.to_binary(), sig)
        .map_err(|_| format!("the cluster map at epoch {}'s signature does not verify against the authority's key", map.epoch()))
}

/// The signature beside a map file (`<path>.sig`, hex), if there is one.
pub fn read_signature(path: &std::path::Path) -> Result<Option<Vec<u8>>, String> {
    let mut sig = path.as_os_str().to_owned();
    sig.push(".sig");
    match std::fs::read_to_string(&sig) {
        Ok(t) => parse_hex(t.trim()).map(Some).map_err(|e| format!("{}: {e}", std::path::Path::new(&sig).display())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(format!("{}: {e}", std::path::Path::new(&sig).display())),
    }
}
