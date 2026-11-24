//! SCRAM-SHA-256 and SCRAM-SHA-256-PLUS, server side (RFC 5802, RFC 7677;
//! §4.9 G9).
//!
//! The gateway verifies a client's proof itself, against the role's
//! StoredKey and ServerKey fetched from the home shard (`rshard::AuthFetch`)
//! and cached. It never sees or holds a password. Channel binding is
//! `tls-server-end-point`, the SHA-256 of the certificate the gateway
//! presents, which works because the gateway terminates the client's TLS.

use sha2::{Digest, Sha256};

pub const MECHANISM: &str = "SCRAM-SHA-256";
pub const MECHANISM_PLUS: &str = "SCRAM-SHA-256-PLUS";

pub fn sha256(b: &[u8]) -> [u8; 32] {
    Sha256::digest(b).into()
}

pub fn hmac(key: &[u8], msg: &[u8]) -> [u8; 32] {
    let mut k = [0u8; 64];
    if key.len() > 64 {
        k[..32].copy_from_slice(&sha256(key));
    } else {
        k[..key.len()].copy_from_slice(key);
    }
    let mut inner = Sha256::new();
    let mut outer = Sha256::new();
    let ipad: Vec<u8> = k.iter().map(|b| b ^ 0x36).collect();
    let opad: Vec<u8> = k.iter().map(|b| b ^ 0x5c).collect();
    inner.update(&ipad);
    inner.update(msg);
    outer.update(&opad);
    outer.update(inner.finalize());
    outer.finalize().into()
}

/// Constant-time equality.
pub fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

pub fn b64_encode(b: &[u8]) -> String {
    let mut out = String::with_capacity(b.len().div_ceil(3) * 4);
    for chunk in b.chunks(3) {
        let n = (chunk[0] as u32) << 16 | (*chunk.get(1).unwrap_or(&0) as u32) << 8 | *chunk.get(2).unwrap_or(&0) as u32;
        out.push(B64[(n >> 18) as usize & 63] as char);
        out.push(B64[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 { B64[(n >> 6) as usize & 63] as char } else { '=' });
        out.push(if chunk.len() > 2 { B64[n as usize & 63] as char } else { '=' });
    }
    out
}

pub fn b64_decode(s: &str) -> Option<Vec<u8>> {
    let s = s.trim_end_matches('=');
    let mut out = Vec::with_capacity(s.len() * 3 / 4);
    let mut acc = 0u32;
    let mut bits = 0u32;
    for c in s.bytes() {
        let v = B64.iter().position(|&x| x == c)? as u32;
        acc = acc << 6 | v;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
        }
    }
    Some(out)
}

/// What the server knows about the role being authenticated.
pub struct Verifier {
    pub iterations: u32,
    pub salt: Vec<u8>,
    pub stored_key: [u8; 32],
    pub server_key: [u8; 32],
    /// False for an unknown role or one without a SCRAM verifier: the
    /// exchange runs to its end anyway and fails there, so a client cannot
    /// tell an unknown role from a wrong password.
    pub real: bool,
}

impl Verifier {
    /// A stand-in for an unknown role: a salt derived from the name and a
    /// gateway secret, so repeated attempts see the same salt.
    pub fn fake(role: &str, secret: &[u8; 32]) -> Verifier {
        let salt = hmac(secret, role.as_bytes())[..16].to_vec();
        Verifier { iterations: 4096, salt, stored_key: [0; 32], server_key: [0; 32], real: false }
    }
}

/// One exchange's state between its messages.
pub struct Exchange {
    client_first_bare: String,
    server_first: String,
    nonce: String,
    gs2_header: String,
    /// The channel binding the client asked for.
    plus: bool,
}

/// The first client message: `(mechanism, client-first-message)` as sent in
/// SASLInitialResponse, checked and answered. `cbind` is the end-point hash
/// when the connection is TLS.
pub fn server_first(
    mechanism: &str,
    client_first: &str,
    verifier: &Verifier,
    cbind: Option<&[u8; 32]>,
    server_nonce: &str,
) -> Result<(Exchange, String), String> {
    let plus = match mechanism {
        MECHANISM => false,
        MECHANISM_PLUS => true,
        m => return Err(format!("SASL mechanism {m:?} is not offered")),
    };
    // gs2-header: cbind-flag "," [authzid] ","
    let mut parts = client_first.splitn(3, ',');
    let flag = parts.next().ok_or("malformed SCRAM client-first-message")?;
    let authz = parts.next().ok_or("malformed SCRAM client-first-message")?;
    let bare = parts.next().ok_or("malformed SCRAM client-first-message")?;
    match (flag, plus) {
        ("n", false) => {}
        // The client could bind but thinks the server cannot: a downgrade
        // when the server offered -PLUS (RFC 5802 §6).
        ("y", false) if cbind.is_some() => return Err("SCRAM channel binding downgrade refused".into()),
        ("y", false) => {}
        (f, true) if f == "p=tls-server-end-point" && cbind.is_some() => {}
        (f, true) if f.starts_with("p=") => return Err(format!("SCRAM channel binding {:?} is not supported", &f[2..])),
        _ => return Err("SCRAM channel binding flag does not match the mechanism".into()),
    }
    if !authz.is_empty() {
        return Err("SCRAM authorization identity is not supported".into());
    }
    let cnonce = bare
        .split(',')
        .find_map(|a| a.strip_prefix("r="))
        .ok_or("SCRAM client-first-message has no nonce")?;
    if cnonce.is_empty() || !cnonce.bytes().all(|c| (0x21..=0x7e).contains(&c) && c != b',') {
        return Err("SCRAM client nonce is malformed".into());
    }
    let nonce = format!("{cnonce}{server_nonce}");
    let server_first = format!("r={nonce},s={},i={}", b64_encode(&verifier.salt), verifier.iterations);
    let ex = Exchange {
        client_first_bare: bare.to_string(),
        server_first: server_first.clone(),
        nonce,
        gs2_header: format!("{flag},{authz},"),
        plus,
    };
    Ok((ex, server_first))
}

/// The client's final message, verified: `Ok(server-final-message)` when the
/// proof is right.
pub fn server_final(ex: &Exchange, client_final: &str, verifier: &Verifier, cbind: Option<&[u8; 32]>) -> Result<String, String> {
    let (without_proof, proof) = client_final.rsplit_once(",p=").ok_or("SCRAM client-final-message has no proof")?;
    let mut channel = None;
    let mut nonce = None;
    for a in without_proof.split(',') {
        if let Some(v) = a.strip_prefix("c=") {
            channel = Some(v);
        } else if let Some(v) = a.strip_prefix("r=") {
            nonce = Some(v);
        }
    }
    if nonce != Some(ex.nonce.as_str()) {
        return Err("SCRAM nonce does not match".into());
    }
    let mut expected = ex.gs2_header.as_bytes().to_vec();
    if ex.plus {
        expected.extend_from_slice(cbind.ok_or("SCRAM channel binding without TLS")?);
    }
    let channel = b64_decode(channel.ok_or("SCRAM client-final-message has no channel binding")?).ok_or("bad base64")?;
    if !ct_eq(&channel, &expected) {
        return Err("SCRAM channel binding does not match".into());
    }
    let proof = b64_decode(proof).ok_or("bad base64 in SCRAM proof")?;
    if proof.len() != 32 {
        return Err("SCRAM proof has the wrong length".into());
    }
    let auth_message = format!("{},{},{}", ex.client_first_bare, ex.server_first, without_proof);
    let client_signature = hmac(&verifier.stored_key, auth_message.as_bytes());
    let mut client_key = [0u8; 32];
    for i in 0..32 {
        client_key[i] = proof[i] ^ client_signature[i];
    }
    let ok = ct_eq(&sha256(&client_key), &verifier.stored_key);
    if !ok || !verifier.real {
        return Err("password authentication failed".into());
    }
    let server_signature = hmac(&verifier.server_key, auth_message.as_bytes());
    Ok(format!("v={}", b64_encode(&server_signature)))
}

/// A fresh server nonce: 18 random bytes, base64.
pub fn nonce() -> String {
    b64_encode(&random_bytes::<18>())
}

/// Random bytes from the OS (getrandom through std's hasher seeds is not a
/// CSPRNG; read /dev/urandom where it exists, else mix the clock and a
/// counter through SHA-256, which is enough for nonces and cancel keys but
/// is logged once as weaker).
pub fn random_bytes<const N: usize>() -> [u8; N] {
    let mut out = [0u8; N];
    #[cfg(unix)]
    {
        use std::io::Read;
        if let Ok(mut f) = std::fs::File::open("/dev/urandom")
            && f.read_exact(&mut out).is_ok()
        {
            return out;
        }
    }
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let mut seed = Vec::new();
    let t = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
    seed.extend_from_slice(&t.as_nanos().to_le_bytes());
    seed.extend_from_slice(&COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed).to_le_bytes());
    seed.extend_from_slice(&std::process::id().to_le_bytes());
    let mut i = 0;
    while i < N {
        seed.push(i as u8);
        let h = sha256(&seed);
        let n = (N - i).min(32);
        out[i..i + n].copy_from_slice(&h[..n]);
        i += n;
    }
    out
}
