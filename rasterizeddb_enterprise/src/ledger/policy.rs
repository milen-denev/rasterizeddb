//! A ledger table's (or vault's) policy, as its sidecar records it
//! (`rasterizeddb_core::core::ledger_hooks::policy_sidecar`).
//!
//! The sidecar is a sequence of records, appended and never rewritten:
//!
//! ```text
//! "RDBL" | u32 LE payload length | payload | u32 LE crc32c(payload)
//! ```
//!
//! A payload is text, one setting per line:
//!
//! ```text
//! v1
//! kind ledger            # or vault; first record only
//! retention_us 220898160000000
//! drop_idle_us 7776000000000
//! hash sha256
//! anchor dir:/mnt/disk2/ledger
//! hold case-4411 set 1764100000000000
//! hold case-4411 release 1764200000000000
//! ```
//!
//! Folding the records gives the policy in force. The first record creates
//! it; later ones may only make it stricter or manage holds. A later record
//! that loosens the policy (a shorter retention, another kind) is not
//! skipped: it means the file was written by something other than this
//! code, so the table refuses to open.

use std::collections::BTreeMap;

use rasterizeddb_core::core::hash::crc32c::crc32c;
use rasterizeddb_core::core::row::schema::SpecialTable;

const MAGIC: &[u8; 4] = b"RDBL";

/// The hash a ledger table chains with.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HashAlg {
    Sha256,
}

impl HashAlg {
    fn parse(s: &str) -> Result<HashAlg, String> {
        match s {
            "sha256" => Ok(HashAlg::Sha256),
            other => Err(format!("unknown ledger hash {other:?}")),
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            HashAlg::Sha256 => "sha256",
        }
    }
}

/// The folded policy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LedgerPolicy {
    /// The table's ledger identity: random, fixed at creation, kept across
    /// rewrites (which give the table a new incarnation). Every leaf and
    /// chain item names it.
    pub id: u128,
    pub kind: SpecialTable,
    /// A row may be deleted only this long after its trusted insert time.
    pub retention_us: i64,
    /// The table may be dropped only after no row was inserted for this long.
    pub drop_idle_us: i64,
    pub hash: HashAlg,
    /// Where block digests are anchored (`dir:…`, `s3://…`).
    pub anchors: Vec<String>,
    /// Holds in force, by name, with the time each was placed.
    pub holds: BTreeMap<String, i64>,
    /// `VERSIONED`: rows may be updated, every version is kept and chained.
    /// Fixed at creation.
    pub versioned: bool,
    /// A vault's key provider (`file:…`, `pkcs11:…`). Fixed at creation;
    /// rotating the key wraps the same data keys under a new one.
    pub key_provider: Option<String>,
}

impl LedgerPolicy {
    /// A new table's first record.
    pub fn new(kind: SpecialTable, retention_us: i64, drop_idle_us: i64) -> LedgerPolicy {
        LedgerPolicy {
            id: fastrand::u128(1..),
            kind,
            retention_us,
            drop_idle_us,
            hash: HashAlg::Sha256,
            anchors: Vec::new(),
            holds: BTreeMap::new(),
            key_provider: None,
            versioned: false,
        }
    }

    /// The payload of the record that creates this policy.
    pub fn creation_payload(&self) -> String {
        let kind = match self.kind {
            SpecialTable::Vault => "vault",
            _ => "ledger",
        };
        let mut p = format!(
            "v1\nkind {kind}\nid {:032x}\nretention_us {}\ndrop_idle_us {}\nhash {}\n",
            self.id,
            self.retention_us,
            self.drop_idle_us,
            self.hash.name()
        );
        for a in &self.anchors {
            p.push_str("anchor ");
            p.push_str(a);
            p.push('\n');
        }
        if self.versioned {
            p.push_str("mode versioned\n");
        }
        if let Some(k) = &self.key_provider {
            p.push_str("key_provider ");
            p.push_str(k);
            p.push('\n');
        }
        p
    }
}

/// One record around `payload`, ready to append.
pub fn record(payload: &str) -> Vec<u8> {
    let body = payload.as_bytes();
    let mut out = Vec::with_capacity(body.len() + 12);
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&(body.len() as u32).to_le_bytes());
    out.extend_from_slice(body);
    out.extend_from_slice(&crc32c(body).to_le_bytes());
    out
}

/// Splits `bytes` into record payloads. A torn last record (a crash during
/// an append) is dropped. Anything else that does not frame is damage.
pub(crate) fn payloads(bytes: &[u8]) -> Result<Vec<&str>, String> {
    let mut out = Vec::new();
    let mut at = 0;
    while at < bytes.len() {
        let rest = &bytes[at..];
        if rest.len() < 8 || &rest[..4] != MAGIC {
            if rest.iter().all(|b| *b == 0) || rest.len() < 8 {
                break; // an append that did not finish
            }
            return Err(format!("the ledger policy sidecar is damaged at byte {at}"));
        }
        let len = u32::from_le_bytes(rest[4..8].try_into().unwrap()) as usize;
        let Some(frame) = rest.get(8..8 + len + 4) else {
            // A torn last append, unless another record follows: then the
            // length was altered, and dropping the records after it (a hold,
            // a raised retention) would loosen the policy unnoticed.
            if rest[4..].windows(MAGIC.len()).any(|w| w == MAGIC) {
                return Err(format!("a ledger policy record at byte {at} runs past the end, with records after it"));
            }
            break;
        };
        let (body, crc) = frame.split_at(len);
        if crc32c(body) != u32::from_le_bytes(crc.try_into().unwrap()) {
            if at + 8 + len + 4 == bytes.len() {
                break; // the last record, torn inside
            }
            return Err(format!("a ledger policy record at byte {at} fails its checksum"));
        }
        out.push(std::str::from_utf8(body).map_err(|_| format!("a ledger policy record at byte {at} is not text"))?);
        at += 8 + len + 4;
    }
    Ok(out)
}

/// Folds `bytes` into the policy in force for a table of `kind`.
pub fn fold(bytes: &[u8], kind: SpecialTable) -> Result<LedgerPolicy, String> {
    let records = payloads(bytes)?;
    let Some(first) = records.first() else {
        return Err("it has no ledger policy record; its policy sidecar is missing or empty".to_string());
    };
    let mut policy: Option<LedgerPolicy> = None;
    for (i, payload) in records.iter().enumerate() {
        let mut lines = payload.lines().map(str::trim).filter(|l| !l.is_empty() && !l.starts_with('#'));
        if lines.next() != Some("v1") {
            return Err(format!("ledger policy record {i} is in a format this build does not know"));
        }
        let bad = |line: &str| format!("ledger policy record {i}: cannot read {line:?}");
        for line in lines {
            let w: Vec<&str> = line.split_whitespace().collect();
            match (w.as_slice(), policy.as_mut()) {
                (["kind", k], None) if i == 0 => {
                    let k = match *k {
                        "ledger" => SpecialTable::Ledger,
                        "vault" => SpecialTable::Vault,
                        _ => return Err(bad(line)),
                    };
                    if k != kind {
                        return Err(format!("its policy says {k:?} but its header says {kind:?}"));
                    }
                    policy = Some(LedgerPolicy::new(k, 0, 0));
                }
                (["id", h], Some(p)) if i == 0 => {
                    p.id = u128::from_str_radix(h, 16).map_err(|_| bad(line))?;
                }
                (["retention_us", v], Some(p)) => {
                    let v: i64 = v.parse().map_err(|_| bad(line))?;
                    if v < p.retention_us {
                        return Err(format!("ledger policy record {i} shortens the retention; refused as tampering"));
                    }
                    p.retention_us = v;
                }
                (["drop_idle_us", v], Some(p)) => {
                    let v: i64 = v.parse().map_err(|_| bad(line))?;
                    if v < p.drop_idle_us {
                        return Err(format!("ledger policy record {i} shortens the drop-idle time; refused as tampering"));
                    }
                    p.drop_idle_us = v;
                }
                (["hash", h], Some(p)) if i == 0 => p.hash = HashAlg::parse(h)?,
                (["anchor", a], Some(p)) => {
                    if !p.anchors.iter().any(|x| x == a) {
                        p.anchors.push(a.to_string());
                    }
                }
                // Set at creation; a later record moves the vault to another
                // provider (`VAULT … ROTATE KEY TO`), never to none.
                (["key_provider", k], Some(p)) => p.key_provider = Some(k.to_string()),
                (["mode", "versioned"], Some(p)) if i == 0 => p.versioned = true,
                (["hold", name, "set", at], Some(p)) => {
                    let at: i64 = at.parse().map_err(|_| bad(line))?;
                    p.holds.entry(name.to_string()).or_insert(at);
                }
                (["hold", name, "release", _at], Some(p)) => {
                    p.holds.remove(*name);
                }
                _ => return Err(bad(line)),
            }
        }
    }
    let _ = first;
    policy.ok_or_else(|| "its first ledger policy record names no kind".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_policy_round_trips_and_only_tightens() {
        let p = LedgerPolicy::new(SpecialTable::Ledger, 100, 50);
        let mut bytes = record(&p.creation_payload());
        assert_eq!(fold(&bytes, SpecialTable::Ledger).unwrap(), p);
        bytes.extend(record("v1\nretention_us 200\nhold h set 7\n"));
        let q = fold(&bytes, SpecialTable::Ledger).unwrap();
        assert_eq!(q.retention_us, 200);
        assert_eq!(q.holds.get("h"), Some(&7));
        let mut loosened = bytes.clone();
        loosened.extend(record("v1\nretention_us 10\n"));
        assert!(fold(&loosened, SpecialTable::Ledger).unwrap_err().contains("tampering"));
        bytes.extend(record("v1\nhold h release 9\n"));
        assert!(fold(&bytes, SpecialTable::Ledger).unwrap().holds.is_empty());
    }

    #[test]
    fn a_missing_sidecar_or_a_kind_mismatch_refuses() {
        assert!(fold(&[], SpecialTable::Ledger).is_err());
        let bytes = record(&LedgerPolicy::new(SpecialTable::Ledger, 1, 1).creation_payload());
        assert!(fold(&bytes, SpecialTable::Vault).is_err());
    }

    #[test]
    fn a_torn_last_record_is_dropped_and_a_damaged_middle_one_refused() {
        let mut bytes = record(&LedgerPolicy::new(SpecialTable::Ledger, 1, 1).creation_payload());
        let second = record("v1\nretention_us 5\n");
        bytes.extend_from_slice(&second[..second.len() - 3]);
        assert_eq!(fold(&bytes, SpecialTable::Ledger).unwrap().retention_us, 1);
        let mut damaged = record(&LedgerPolicy::new(SpecialTable::Ledger, 1, 1).creation_payload());
        let n = damaged.len();
        damaged[n - 6] ^= 1;
        damaged.extend(record("v1\nretention_us 5\n"));
        assert!(fold(&damaged, SpecialTable::Ledger).is_err());
    }
}
