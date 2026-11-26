//! RasterizedDB enterprise client features (licensed: see LICENSE), over the
//! GPL [`rasterizeddb_client`] (docs/ledger-and-vault-plan.md §5).
//!
//! - [`Vault`]: a secret vault, as an application replaces Azure Key Vault
//!   or AWS Secrets Manager with it. Values come back as [`SecretString`],
//!   zeroed when dropped, and can be cached for a while
//!   ([`Vault::cached`]).
//! - [`LedgerAdmin`]: `VERIFY LEDGER`, `SHOW LEDGER`, and raising a ledger
//!   table's retention or placing a legal hold.
//! - [`LedgerModelExt`]: what a ledger model (`#[rdb(ledger = "7 years")]`)
//!   does beyond inserting: deleting rows past their retention.
//!
//! The server's guard is what holds. This crate only says it in Rust.
//!
//! ```no_run
//! # async fn demo(client: &rasterizeddb_client::Client) -> rasterizeddb_client::Result<()> {
//! use rasterizeddb_client_enterprise::Vault;
//! let vault = Vault::new(client, "app_secrets");
//! vault.set("db-password", "s3cr3t").await?;
//! let pw = vault.get("db-password").await?;
//! assert_eq!(pw.expose(), "s3cr3t");
//! # Ok(()) }
//! ```

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use rasterizeddb_client::{Client, Error, Result};
use zeroize::Zeroizing;

/// A secret's text, zeroed when dropped. Never printed by `Debug`.
#[derive(Clone)]
pub struct SecretString(Zeroizing<String>);

impl SecretString {
    pub fn new(s: String) -> SecretString {
        SecretString(Zeroizing::new(s))
    }

    /// The text. Kept out of `Display` and `Debug` on purpose: reading it is
    /// a choice.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for SecretString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SecretString(…)")
    }
}

fn quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

fn ident(s: &str) -> Result<&str> {
    if !s.is_empty() && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        Ok(s)
    } else {
        Err(Error::Param(format!("{s:?} is not a plain table name")))
    }
}

async fn lines(client: &Client, sql: &str) -> Result<Vec<String>> {
    let rows = client.run(sql).await?;
    rows.iter().map(|r| r.get::<String>(0)).collect()
}

/// A secret's metadata, as `VAULT … LIST` gives it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecretInfo {
    pub name: String,
    pub version: i64,
    /// `active`, `disabled` or `deleted`.
    pub state: String,
}

/// Options for [`Vault::set_with`].
#[derive(Clone, Debug, Default)]
pub struct SetOptions {
    pub content_type: Option<String>,
    /// UTC, `YYYY-MM-DD` or `YYYY-MM-DDTHH:MM:SSZ`.
    pub expires: Option<String>,
    pub not_before: Option<String>,
    pub tags: Option<String>,
}

/// A secret vault on the server (`CREATE VAULT`).
#[cfg(feature = "vault")]
pub struct Vault<'c> {
    client: &'c Client,
    name: String,
}

#[cfg(feature = "vault")]
impl<'c> Vault<'c> {
    pub fn new(client: &'c Client, vault: &str) -> Vault<'c> {
        Vault { client, name: vault.to_string() }
    }

    fn head(&self, verb: &str) -> Result<String> {
        Ok(format!("VAULT {} {verb}", ident(&self.name)?))
    }

    /// Stores a new version of `secret`; returns its version number.
    pub async fn set(&self, secret: &str, value: &str) -> Result<i64> {
        self.set_with(secret, value, &SetOptions::default()).await
    }

    pub async fn set_with(&self, secret: &str, value: &str, options: &SetOptions) -> Result<i64> {
        // Built in zeroizing memory: the statement carries the value.
        let mut sql = Zeroizing::new(format!("{} SECRET {} = {}", self.head("SET")?, quote(secret), quote(value)));
        if let Some(t) = &options.content_type {
            sql.push_str(&format!(" CONTENT TYPE {}", quote(t)));
        }
        if let Some(t) = &options.expires {
            sql.push_str(&format!(" EXPIRES {}", quote(t)));
        }
        if let Some(t) = &options.not_before {
            sql.push_str(&format!(" NOT BEFORE {}", quote(t)));
        }
        if let Some(t) = &options.tags {
            sql.push_str(&format!(" TAGS {}", quote(t)));
        }
        let out = lines(self.client, &sql).await?;
        out.first()
            .and_then(|l| l.rsplit(' ').next())
            .and_then(|v| v.parse().ok())
            .ok_or_else(|| Error::Protocol(format!("unexpected VAULT SET answer {out:?}")))
    }

    /// The newest version of `secret`. Every read is audited on the server.
    pub async fn get(&self, secret: &str) -> Result<SecretString> {
        let sql = format!("{} SECRET {}", self.head("GET")?, quote(secret));
        self.read(&sql).await
    }

    /// One version of `secret`.
    pub async fn get_version(&self, secret: &str, version: i64) -> Result<SecretString> {
        let sql = format!("{} SECRET {} VERSION {version}", self.head("GET")?, quote(secret));
        self.read(&sql).await
    }

    async fn read(&self, sql: &str) -> Result<SecretString> {
        let rows = self.client.run(sql).await?;
        let row = rows.get(0).ok_or_else(|| Error::Protocol("VAULT GET returned no row".to_string()))?;
        Ok(SecretString::new(row.get::<String>(0)?))
    }

    pub async fn list(&self) -> Result<Vec<SecretInfo>> {
        let out = lines(self.client, &self.head("LIST")?).await?;
        Ok(out
            .iter()
            .filter_map(|l| {
                let mut w = l.split_whitespace();
                let name = w.next()?.to_string();
                let version = w.next()?.trim_start_matches('v').parse().ok()?;
                let state = w.next()?.to_string();
                Some(SecretInfo { name, version, state })
            })
            .collect())
    }

    pub async fn versions(&self, secret: &str) -> Result<Vec<String>> {
        lines(self.client, &format!("{} SECRET {}", self.head("VERSIONS")?, quote(secret))).await
    }

    async fn change(&self, verb: &str, secret: &str) -> Result<()> {
        lines(self.client, &format!("{} SECRET {}", self.head(verb)?, quote(secret))).await.map(drop)
    }

    pub async fn disable(&self, secret: &str) -> Result<()> {
        self.change("DISABLE", secret).await
    }

    pub async fn enable(&self, secret: &str) -> Result<()> {
        self.change("ENABLE", secret).await
    }

    /// Soft delete: [`Vault::recover`] undoes it until it is purged.
    pub async fn delete(&self, secret: &str) -> Result<()> {
        self.change("DELETE", secret).await
    }

    pub async fn recover(&self, secret: &str) -> Result<()> {
        self.change("RECOVER", secret).await
    }

    /// Destroys a deleted secret's keys once its purge protection has passed.
    pub async fn purge(&self, secret: &str) -> Result<()> {
        self.change("PURGE", secret).await
    }

    /// Rewraps every data key, under the same provider or `to` another.
    pub async fn rotate_key(&self, to: Option<&str>) -> Result<Vec<String>> {
        let mut sql = format!("{} KEY", self.head("ROTATE")?);
        if let Some(t) = to {
            sql.push_str(&format!(" TO {}", quote(t)));
        }
        lines(self.client, &sql).await
    }

    /// The audit trail, each line marked `chained` or `UNCHAINED`.
    pub async fn audit(&self, secret: Option<&str>) -> Result<Vec<String>> {
        let mut sql = self.head("AUDIT")?;
        if let Some(s) = secret {
            sql.push_str(&format!(" SECRET {}", quote(s)));
        }
        lines(self.client, &sql).await
    }

    /// Creates a key used in place: `aes256-gcm` (encrypt, decrypt) or
    /// `mldsa65` (post-quantum sign, verify). Its material never leaves the
    /// server.
    pub async fn create_key(&self, key: &str, kind: &str) -> Result<()> {
        lines(self.client, &format!("{} KEY {} TYPE {}", self.head("CREATE")?, quote(key), quote(kind))).await.map(drop)
    }

    async fn key_op(&self, op: &str, key: &str, value: &str, extra: &str) -> Result<String> {
        let sql = Zeroizing::new(format!("{} KEY {} VALUE {}{extra}", self.head(op)?, quote(key), quote(value)));
        lines(self.client, &sql).await?.into_iter().next().ok_or_else(|| Error::Protocol(format!("VAULT {op} returned no row")))
    }

    /// Ciphertext (base64) of `plaintext` under an `aes256-gcm` key.
    pub async fn encrypt(&self, key: &str, plaintext: &str) -> Result<String> {
        self.key_op("ENCRYPT", key, plaintext, "").await
    }

    pub async fn decrypt(&self, key: &str, ciphertext: &str) -> Result<SecretString> {
        self.key_op("DECRYPT", key, ciphertext, "").await.map(SecretString::new)
    }

    /// An ML-DSA-65 signature (hex) of `message` under an `mldsa65` key.
    pub async fn sign(&self, key: &str, message: &str) -> Result<String> {
        self.key_op("SIGN", key, message, "").await
    }

    pub async fn verify(&self, key: &str, message: &str, signature: &str) -> Result<bool> {
        let out = self.key_op("VERIFY", key, message, &format!(" SIGNATURE {}", quote(signature))).await?;
        Ok(out == "true")
    }

    /// An `mldsa65` key's public half (hex), to verify its signatures anywhere.
    pub async fn public_key(&self, key: &str) -> Result<String> {
        lines(self.client, &format!("{} KEY {}", self.head("PUBLIC")?, quote(key)))
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| Error::Protocol("VAULT PUBLIC KEY returned no row".to_string()))
    }

    /// A cache in front of [`Vault::get`]: a value is read from the server
    /// at most once per `ttl` (and audited then). Values are zeroed when they
    /// leave the cache.
    pub fn cached(self, ttl: Duration) -> CachedVault<'c> {
        CachedVault { vault: self, ttl, entries: Mutex::new(HashMap::new()) }
    }
}

/// [`Vault::cached`].
#[cfg(feature = "vault")]
pub struct CachedVault<'c> {
    vault: Vault<'c>,
    ttl: Duration,
    entries: Mutex<HashMap<String, (Instant, SecretString)>>,
}

#[cfg(feature = "vault")]
impl CachedVault<'_> {
    pub async fn get(&self, secret: &str) -> Result<SecretString> {
        if let Some((at, v)) = self.entries.lock().unwrap().get(secret) {
            if at.elapsed() < self.ttl {
                return Ok(v.clone());
            }
        }
        let v = self.vault.get(secret).await?;
        self.entries.lock().unwrap().insert(secret.to_string(), (Instant::now(), v.clone()));
        Ok(v)
    }

    /// Forgets every cached value.
    pub fn clear(&self) {
        self.entries.lock().unwrap().clear();
    }
}

/// What `VERIFY LEDGER` found.
#[cfg(feature = "ledger")]
#[derive(Clone, Debug)]
pub struct LedgerReport {
    pub lines: Vec<String>,
}

#[cfg(feature = "ledger")]
impl LedgerReport {
    /// Whether nothing was found tampered with, rolled back or unreadable.
    pub fn is_intact(&self) -> bool {
        !self.lines.iter().any(|l| l.starts_with("TAMPERED") || l.starts_with("UNKNOWN"))
    }

    /// Whether every check could also be anchored outside the host.
    pub fn is_anchored(&self) -> bool {
        self.lines.iter().any(|l| l.starts_with("OK        anchor")) && !self.lines.iter().any(|l| l.starts_with("WARNING   no anchors"))
    }
}

/// What [`verify_receipt`] proved.
#[cfg(feature = "ledger")]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReceiptProof {
    /// The row's leaf, as the receipt states it and the entry contains it.
    pub leaf: String,
    /// The head the receipt chains to.
    pub head_seq: u64,
    pub head: String,
}

#[cfg(feature = "ledger")]
fn unhex(s: &str) -> Option<Vec<u8>> {
    if s.len() % 2 != 0 {
        return None;
    }
    (0..s.len() / 2).map(|i| u8::from_str_radix(&s[2 * i..2 * i + 2], 16).ok()).collect()
}

/// Checks a `SHOW LEDGER RECEIPT` answer without trusting the server: the
/// row's leaf is in the stated entry, and hashing forward from that entry
/// through every later payload digest reaches the stated head. Pass
/// `expected_head` (hex), read from an anchor yourself, to tie the proof to
/// something the server could not have rewritten; without it the proof
/// only shows internal consistency.
#[cfg(feature = "ledger")]
pub fn verify_receipt(lines: &[String], expected_head: Option<&str>) -> std::result::Result<ReceiptProof, String> {
    use sha2::{Digest, Sha256};
    let bad = |why: &str| Err(format!("receipt does not verify: {why}"));
    if lines.first().map(String::as_str) != Some("receipt rdb-v1") {
        return bad("not a receipt rdb-v1");
    }
    let row: Vec<&str> = lines.get(1).map(|l| l.split_whitespace().collect()).unwrap_or_default();
    let entry: Vec<&str> = lines.get(2).map(|l| l.split_whitespace().collect()).unwrap_or_default();
    let (["row", _ledger, _ts, leaf], ["entry", _seq, prev, payload]) = (row.as_slice(), entry.as_slice()) else {
        return bad("malformed");
    };
    let (Some(leaf_b), Some(prev_b), Some(payload_b)) = (unhex(leaf), unhex(prev), unhex(payload)) else {
        return bad("not hex");
    };
    // The leaf is one of the entry's items (each 61 bytes after a 28-byte head,
    // the leaf its last 32).
    let n = payload_b.get(24..28).map(|b| u32::from_le_bytes(b.try_into().unwrap()) as usize).unwrap_or(0);
    if payload_b.len() != 28 + n * 61 || !(0..n).any(|k| payload_b[28 + k * 61 + 29..28 + (k + 1) * 61] == leaf_b[..]) {
        return bad("the entry does not contain the leaf");
    }
    let step = |prev: &[u8], digest: &[u8]| -> Vec<u8> {
        let mut h = Sha256::new();
        h.update(b"rdb-chain-v1");
        h.update(prev);
        h.update(digest);
        h.finalize().to_vec()
    };
    let mut hash = step(&prev_b, &Sha256::digest(&payload_b));
    let mut last = 3;
    for line in &lines[3..] {
        let Some(d) = line.strip_prefix("after ") else { break };
        let Some(d) = unhex(d.trim()) else { return bad("not hex") };
        hash = step(&hash, &d);
        last += 1;
    }
    let head: Vec<&str> = lines.get(last).map(|l| l.split_whitespace().collect()).unwrap_or_default();
    let ["head", seq, head_hex, _] = head.as_slice() else { return bad("no head line") };
    let hex: String = hash.iter().map(|b| format!("{b:02x}")).collect();
    if &hex != head_hex {
        return bad("hashing forward does not reach the stated head");
    }
    if let Some(expected) = expected_head {
        if !expected.eq_ignore_ascii_case(&hex) {
            return bad("the head is not the one you expected");
        }
    }
    Ok(ReceiptProof { leaf: leaf.to_string(), head_seq: seq.parse().unwrap_or(0), head: hex })
}

/// The ledger statements.
#[cfg(feature = "ledger")]
pub struct LedgerAdmin<'c> {
    client: &'c Client,
}

#[cfg(feature = "ledger")]
impl<'c> LedgerAdmin<'c> {
    pub fn new(client: &'c Client) -> LedgerAdmin<'c> {
        LedgerAdmin { client }
    }

    /// `VERIFY LEDGER [table]`.
    pub async fn verify(&self, table: Option<&str>) -> Result<LedgerReport> {
        let sql = match table {
            Some(t) => format!("VERIFY LEDGER {}", ident(t)?),
            None => "VERIFY LEDGER".to_string(),
        };
        Ok(LedgerReport { lines: lines(self.client, &sql).await? })
    }

    /// `SHOW LEDGER RECEIPT`: the proof a row (by its `__ledger_ts`) is in
    /// the chain. Check it with [`verify_receipt`].
    pub async fn receipt(&self, table: &str, row_stamp: i64) -> Result<Vec<String>> {
        lines(self.client, &format!("SHOW LEDGER RECEIPT {} {row_stamp}", ident(table)?)).await
    }

    /// `SHOW LEDGER [table]`.
    pub async fn show(&self, table: Option<&str>) -> Result<Vec<String>> {
        let sql = match table {
            Some(t) => format!("SHOW LEDGER {}", ident(t)?),
            None => "SHOW LEDGER".to_string(),
        };
        lines(self.client, &sql).await
    }

    /// Raises `table`'s retention, e.g. `"10 years"`. Never lowers it.
    pub async fn raise_retention(&self, table: &str, retention: &str) -> Result<()> {
        lines(self.client, &format!("ALTER LEDGER TABLE {} SET RETENTION {retention}", ident(table)?)).await.map(drop)
    }

    pub async fn place_hold(&self, table: &str, hold: &str) -> Result<()> {
        lines(self.client, &format!("ALTER LEDGER TABLE {} SET LEGAL HOLD {}", ident(table)?, quote(hold))).await.map(drop)
    }

    pub async fn release_hold(&self, table: &str, hold: &str) -> Result<()> {
        lines(self.client, &format!("ALTER LEDGER TABLE {} RELEASE LEGAL HOLD {}", ident(table)?, quote(hold))).await.map(drop)
    }

    /// Records, in the chain, that a restore rolled the ledger back (superuser).
    pub async fn acknowledge_rollback(&self, reason: &str) -> Result<Vec<String>> {
        lines(self.client, &format!("ACKNOWLEDGE LEDGER ROLLBACK {}", quote(reason))).await
    }
}

/// What a ledger model does beyond inserting.
#[cfg(feature = "ledger")]
pub trait LedgerModelExt {
    /// Deletes the rows `where_clause` selects (SQL, no `WHERE`). The
    /// server refuses the whole statement (55000) when any of them is still
    /// within its retention or under a legal hold, so pick rows by their
    /// insert time (`__ledger_ts`, microseconds) to stay clear of that.
    fn delete_expired<M: rasterizeddb_client::orm::Model>(
        &self,
        where_clause: &str,
    ) -> impl std::future::Future<Output = Result<u64>> + Send;
}

#[cfg(feature = "ledger")]
impl LedgerModelExt for Client {
    fn delete_expired<M: rasterizeddb_client::orm::Model>(
        &self,
        where_clause: &str,
    ) -> impl std::future::Future<Output = Result<u64>> + Send {
        let sql = if M::LEDGER.is_none() {
            Err(Error::Unsupported(format!("{} is not a ledger model", M::TABLE)))
        } else {
            ident(M::TABLE).map(|t| format!("DELETE FROM {t} WHERE {where_clause}"))
        };
        async move { self.exec(&sql?, &[]).await }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secrets_do_not_print() {
        let s = SecretString::new("hunter2".to_string());
        assert_eq!(format!("{s:?}"), "SecretString(…)");
        assert_eq!(s.expose(), "hunter2");
    }

    #[test]
    fn names_are_plain_and_values_quoted() {
        assert!(ident("app_secrets").is_ok());
        assert!(ident("x; DROP").is_err());
        assert_eq!(quote("it's"), "'it''s'");
    }
}
