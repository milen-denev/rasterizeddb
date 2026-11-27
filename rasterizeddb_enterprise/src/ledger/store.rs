//! The digest store: one hash-chained log per database
//! (docs/ledger-and-vault-plan.md §3.4).
//!
//! Every committed transaction that wrote a ledger table appends one entry.
//! The entry lists, for each row inserted, the table's ledger id, the
//! row's stamp, how many columns it had, and the row's **leaf** (a SHA-256
//! of its cells). For each row deleted, it lists the id and the stamp.
//! Policy changes and acknowledged rollbacks are entries too. Each entry's
//! hash covers the one before it:
//!
//! ```text
//! frame = "RDBC" | u32 LE payload length | payload | entry hash (32)
//! entry hash = SHA-256("rdb-chain-v1" ‖ previous entry hash ‖ SHA-256(payload))
//! payload = u64 seq | u64 txid | i64 time | u32 items | items…
//! item    = u8 op | u128 ledger id | i64 row stamp | u32 width | leaf (32)
//! ```
//!
//! So editing, removing or reordering any entry breaks every hash after
//! it. Only an outside copy of the newest hash (an anchor, L3) catches the
//! whole chain being recomputed.
//!
//! The file lives under `ledger_dir/<store>/chain.log`, where `ledger_dir`
//! comes from `RDB_LEDGER_DIR` or the enterprise configuration. It should be
//! another disk. Without one it lives in the database's directory, and the
//! start-up log says so. Appends are synced before the commit they belong
//! to, so a commit never outlives its entry.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use parking_lot::Mutex;

/// SHA-256 over `parts` in order: through the core's FIPS backend, so a
/// `fips` build hashes the chain inside the validated module.
fn sha256(parts: &[&[u8]]) -> Hash {
    let mut buf = Vec::with_capacity(parts.iter().map(|p| p.len()).sum());
    for p in parts {
        buf.extend_from_slice(p);
    }
    rasterizeddb_core::core::fips::sha256(&buf)
}

const MAGIC: &[u8; 4] = b"RDBC";
const DOMAIN: &[u8] = b"rdb-chain-v1";
const LEAF_DOMAIN: &[u8] = b"rdb-leaf-v1";

pub const OP_INSERT: u8 = 1;
pub const OP_DELETE: u8 = 2;
pub const OP_EVENT: u8 = 3;
/// A new version of a row of a versioned ledger table: its stamp, and the
/// new version's width and leaf.
pub const OP_UPDATE: u8 = 4;
/// A record added to a table's policy (`ALTER LEDGER TABLE`): its leaf is
/// the record's payload hash ([`policy_leaf`]), chained once the record is
/// written, so VERIFY finds a record cut from the policy sidecar.
pub const OP_POLICY: u8 = 5;

pub type Hash = [u8; 32];

/// One item of an entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Item {
    pub op: u8,
    pub ledger: u128,
    pub row_ts: i64,
    pub width: u32,
    pub leaf: Hash,
}

const ITEM_LEN: usize = 1 + 16 + 8 + 4 + 32;

/// One entry, read back.
#[derive(Clone, Debug)]
pub struct Entry {
    pub seq: u64,
    pub txid: u64,
    pub time: i64,
    pub items: Vec<Item>,
    pub hash: Hash,
    /// The hash before it, and its payload: what a receipt carries.
    pub prev: Hash,
    pub payload: Vec<u8>,
}

/// A row's leaf: its table's ledger id, its stamp, and its cells (by write
/// order, up to `width`, ascending).
pub fn leaf(ledger: u128, row_ts: i64, cells: &[(u32, &[u8])]) -> (u32, Hash) {
    let mut sorted: Vec<&(u32, &[u8])> = cells.iter().collect();
    sorted.sort_by_key(|(o, _)| *o);
    let width = sorted.last().map_or(0, |(o, _)| *o + 1);
    (width, leaf_upto(ledger, row_ts, width, &sorted.iter().map(|(o, b)| (*o, *b)).collect::<Vec<_>>()))
}

/// The leaf over the cells with write order below `width`, as a row read
/// back after columns were added is checked against what it had.
pub fn leaf_upto(ledger: u128, row_ts: i64, width: u32, cells: &[(u32, &[u8])]) -> Hash {
    let mut sorted: Vec<&(u32, &[u8])> = cells.iter().filter(|(o, _)| *o < width).collect();
    sorted.sort_by_key(|(o, _)| *o);
    let mut buf = Vec::new();
    buf.extend_from_slice(LEAF_DOMAIN);
    buf.extend_from_slice(&ledger.to_le_bytes());
    buf.extend_from_slice(&row_ts.to_le_bytes());
    buf.extend_from_slice(&width.to_le_bytes());
    for (order, bytes) in sorted {
        buf.extend_from_slice(&order.to_le_bytes());
        buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(bytes);
    }
    sha256(&[&buf])
}

/// A policy record's leaf: a hash of its payload.
pub fn policy_leaf(payload: &str) -> Hash {
    sha256(&[b"rdb-policy-v1", payload.as_bytes()])
}

/// An event's leaf: a hash of its text.
pub fn event_leaf(text: &str) -> Hash {
    sha256(&[b"rdb-event-v1", text.as_bytes()])
}

/// An entry's hash, from the one before it and its payload's digest. Over
/// the digest rather than the payload, so a receipt proves one entry's place
/// with 32 bytes per later entry instead of their payloads.
pub fn entry_hash(prev: &Hash, payload: &[u8]) -> Hash {
    entry_hash_of_digest(prev, &payload_digest(payload))
}

pub fn payload_digest(payload: &[u8]) -> Hash {
    sha256(&[payload])
}

pub fn entry_hash_of_digest(prev: &Hash, digest: &Hash) -> Hash {
    sha256(&[DOMAIN, prev, digest])
}

fn encode(seq: u64, txid: u64, time: i64, items: &[Item]) -> Vec<u8> {
    let mut p = Vec::with_capacity(28 + items.len() * ITEM_LEN);
    p.extend_from_slice(&seq.to_le_bytes());
    p.extend_from_slice(&txid.to_le_bytes());
    p.extend_from_slice(&time.to_le_bytes());
    p.extend_from_slice(&(items.len() as u32).to_le_bytes());
    for i in items {
        p.push(i.op);
        p.extend_from_slice(&i.ledger.to_le_bytes());
        p.extend_from_slice(&i.row_ts.to_le_bytes());
        p.extend_from_slice(&i.width.to_le_bytes());
        p.extend_from_slice(&i.leaf);
    }
    p
}

fn decode(p: &[u8]) -> Option<(u64, u64, i64, Vec<Item>)> {
    let seq = u64::from_le_bytes(p.get(0..8)?.try_into().ok()?);
    let txid = u64::from_le_bytes(p.get(8..16)?.try_into().ok()?);
    let time = i64::from_le_bytes(p.get(16..24)?.try_into().ok()?);
    let n = u32::from_le_bytes(p.get(24..28)?.try_into().ok()?) as usize;
    if p.len() != 28 + n * ITEM_LEN {
        return None;
    }
    let mut items = Vec::with_capacity(n);
    for k in 0..n {
        let b = &p[28 + k * ITEM_LEN..28 + (k + 1) * ITEM_LEN];
        items.push(Item {
            op: b[0],
            ledger: u128::from_le_bytes(b[1..17].try_into().ok()?),
            row_ts: i64::from_le_bytes(b[17..25].try_into().ok()?),
            width: u32::from_le_bytes(b[25..29].try_into().ok()?),
            leaf: b[29..61].try_into().ok()?,
        });
    }
    Some((seq, txid, time, items))
}

/// What reading a chain found.
#[derive(Debug, Default)]
pub struct ChainRead {
    pub entries: Vec<Entry>,
    /// Where the chain stops holding: the first entry whose frame, sequence
    /// or hash is wrong, and why. Entries after it are not read.
    pub broken: Option<(u64, String)>,
    /// A last frame cut short by a crash during its append, dropped.
    pub torn_tail: bool,
    /// The byte length of the good prefix.
    pub good_len: u64,
}

/// Reads and checks a chain's bytes.
pub fn read(bytes: &[u8]) -> ChainRead {
    let mut out = ChainRead::default();
    let mut prev: Hash = [0; 32];
    let mut at = 0usize;
    let mut expect_seq = 1u64;
    while at < bytes.len() {
        let rest = &bytes[at..];
        if rest.len() < 8 || &rest[..4] != MAGIC {
            if rest.len() < 8 || rest.iter().all(|b| *b == 0) {
                out.torn_tail = true;
            } else {
                out.broken = Some((expect_seq, format!("no entry frame at byte {at}")));
            }
            break;
        }
        let len = u32::from_le_bytes(rest[4..8].try_into().unwrap()) as usize;
        let Some(frame) = rest.get(8..8 + len + 32) else {
            // Past the end: the last append cut short by a crash, if nothing
            // after it looks like another entry. With another entry's frame
            // after it, the length was altered: truncating there would
            // throw away every later entry and hide the edit.
            if rest[4..].windows(MAGIC.len()).any(|w| w == MAGIC) {
                out.broken = Some((expect_seq, format!("entry {expect_seq}'s length runs past the end, with entries after it")));
            } else {
                out.torn_tail = true;
            }
            break;
        };
        let (payload, stored) = frame.split_at(len);
        let hash = entry_hash(&prev, payload);
        if hash.as_slice() != stored {
            if at + 8 + len + 32 == bytes.len() && !out.entries.is_empty() {
                // A last append half on disk reads as a bad hash too; a
                // broken last entry is reported, not dropped, unless it is
                // also short of its declared length (handled above).
            }
            out.broken = Some((expect_seq, format!("entry {expect_seq}'s hash does not follow from the one before it")));
            break;
        }
        let Some((seq, txid, time, items)) = decode(payload) else {
            out.broken = Some((expect_seq, format!("entry {expect_seq} does not decode")));
            break;
        };
        if seq != expect_seq {
            out.broken = Some((expect_seq, format!("entry {expect_seq} is numbered {seq}")));
            break;
        }
        out.entries.push(Entry { seq, txid, time, items, hash, prev, payload: payload.to_vec() });
        prev = hash;
        expect_seq += 1;
        at += 8 + len + 32;
        out.good_len = at as u64;
    }
    out
}

/// An open chain: its file (none for a database in memory) and its head.
pub struct Chain {
    file: Option<File>,
    pub path: Option<PathBuf>,
    pub head: Hash,
    pub seq: u64,
    len: u64,
    /// For a database in memory: the bytes a file would hold.
    memory: Vec<u8>,
    /// Ledger id → newest row stamp seen (drop-idle checks).
    pub last_insert: HashMap<u128, i64>,
}

impl Chain {
    /// Opens (creating) the chain for `store`.
    fn open(store: &str) -> Result<Chain, String> {
        if store.starts_with("memory:") {
            return Ok(Chain { file: None, path: None, head: [0; 32], seq: 0, len: 0, memory: Vec::new(), last_insert: HashMap::new() });
        }
        let dir = chain_dir(store);
        std::fs::create_dir_all(&dir).map_err(|e| format!("{}: {e}", dir.display()))?;
        let path = dir.join("chain.log");
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&path)
            .map_err(|e| format!("{}: {e}", path.display()))?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).map_err(|e| format!("{}: {e}", path.display()))?;
        let r = read(&bytes);
        if let Some((seq, why)) = &r.broken {
            return Err(format!(
                "XX001: the ledger chain {} is broken at entry {seq}: {why}; ledger writes stop until it is examined",
                path.display()
            ));
        }
        if r.torn_tail {
            // A crash during an append: its commit never happened (the append
            // is synced before the commit), so the partial frame goes.
            file.set_len(r.good_len).map_err(|e| format!("{}: {e}", path.display()))?;
            file.sync_all().map_err(|e| format!("{}: {e}", path.display()))?;
        }
        let mut c = Chain {
            file: Some(file),
            path: Some(path),
            head: r.entries.last().map_or([0; 32], |e| e.hash),
            seq: r.entries.last().map_or(0, |e| e.seq),
            len: r.good_len,
            memory: Vec::new(),
            last_insert: HashMap::new(),
        };
        for e in &r.entries {
            super::clock::raise_high_water(e.time);
            for i in &e.items {
                super::clock::raise_high_water(i.row_ts);
                if i.op == OP_INSERT {
                    let t = c.last_insert.entry(i.ledger).or_insert(i.row_ts);
                    *t = (*t).max(i.row_ts);
                }
            }
        }
        Ok(c)
    }

    /// Appends one entry and syncs it.
    pub fn append(&mut self, txid: u64, items: &[Item]) -> Result<Hash, String> {
        let seq = self.seq + 1;
        let time = super::clock::now_micros();
        let payload = encode(seq, txid, time, items);
        let hash = entry_hash(&self.head, &payload);
        let mut frame = Vec::with_capacity(8 + payload.len() + 32);
        frame.extend_from_slice(MAGIC);
        frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        frame.extend_from_slice(&payload);
        frame.extend_from_slice(&hash);
        match self.file.as_mut() {
            None => self.memory.extend_from_slice(&frame),
            Some(file) => {
                let written = file.write_all(&frame).and_then(|()| file.sync_data());
                if let Err(e) = written {
                    // Taken back: the commit it was for will not happen.
                    let _ = file.set_len(self.len);
                    return Err(format!("the ledger chain could not be written: {e}"));
                }
            }
        }
        self.len += frame.len() as u64;
        self.seq = seq;
        self.head = hash;
        for i in items {
            if i.op == OP_INSERT {
                let t = self.last_insert.entry(i.ledger).or_insert(i.row_ts);
                *t = (*t).max(i.row_ts);
            }
        }
        Ok(hash)
    }

    /// The whole chain, read again from its file and checked.
    pub fn reread(&self) -> Result<ChainRead, String> {
        match &self.path {
            None => Ok(read(&self.memory)),
            Some(p) => std::fs::read(p).map(|b| read(&b)).map_err(|e| format!("{}: {e}", p.display())),
        }
    }
}

static LEDGER_DIR: OnceLock<Option<PathBuf>> = OnceLock::new();

/// Sets the directory chains live under (from the enterprise configuration);
/// once, before the first chain opens.
pub fn set_ledger_dir(dir: PathBuf) {
    let _ = LEDGER_DIR.set(Some(dir));
}

fn ledger_dir() -> Option<&'static Path> {
    LEDGER_DIR
        .get_or_init(|| std::env::var_os("RDB_LEDGER_DIR").map(PathBuf::from))
        .as_deref()
}

/// The directory `store`'s chain lives in.
pub fn chain_dir(store: &str) -> PathBuf {
    match ledger_dir() {
        Some(root) => {
            // One directory per database, named from its path.
            let key: String = store
                .chars()
                .map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '_' })
                .collect();
            root.join(key.trim_matches('_'))
        }
        None => Path::new(store).join("rdb_ledger"),
    }
}

/// Whether `store`'s chain shares a device with its data: an attacker or a
/// failure that takes one takes both.
pub fn same_device(store: &str) -> Option<bool> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let data = std::fs::metadata(store).ok()?.dev();
        let chain = std::fs::metadata(chain_dir(store)).ok()?.dev();
        Some(data == chain)
    }
    #[cfg(not(unix))]
    {
        let _ = store;
        None
    }
}

static CHAINS: OnceLock<Mutex<HashMap<String, Arc<Mutex<Chain>>>>> = OnceLock::new();

/// The open chain of `store`, opened on first use.
pub fn chain(store: &str) -> Result<Arc<Mutex<Chain>>, String> {
    let chains = CHAINS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut map = chains.lock();
    if let Some(c) = map.get(store) {
        return Ok(c.clone());
    }
    let c = Chain::open(store)?;
    super::anchor::load_acks(store);
    if same_device(store) == Some(true) {
        log::warn!(
            "the ledger chain of {store} is on the same device as its data ({}); set RDB_LEDGER_DIR or \
             `ledger_dir` to another disk so one failure or one attacker cannot take both",
            c.path.as_ref().map_or_else(String::new, |p| p.display().to_string())
        );
    }
    let c = Arc::new(Mutex::new(c));
    map.insert(store.to_string(), c.clone());
    Ok(c)
}

/// Reads `store`'s chain from disk again, replacing any copy this process
/// held: a database opened (again) must check the file as it is now, not
/// trust a head from before it closed.
pub fn reopen(store: &str) -> Result<Arc<Mutex<Chain>>, String> {
    if let Some(chains) = CHAINS.get() {
        chains.lock().remove(store);
    }
    chain(store)
}

/// The newest stamp inserted under `ledger` in any open chain.
pub fn last_insert(ledger: u128) -> Option<i64> {
    let chains = CHAINS.get()?;
    chains.lock().values().filter_map(|c| c.lock().last_insert.get(&ledger).copied()).max()
}

pub fn hex(h: &[u8]) -> String {
    h.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(ts: i64) -> Item {
        let (width, leaf) = leaf(7, ts, &[(0, b"a"), (1, &ts.to_le_bytes())]);
        Item { op: OP_INSERT, ledger: 7, row_ts: ts, width, leaf }
    }

    #[test]
    fn a_chain_reads_back_and_any_edit_breaks_it() {
        let mut c = Chain::open("memory:t").unwrap();
        c.append(5, &[item(1), item(2)]).unwrap();
        c.append(6, &[item(3)]).unwrap();
        let r = c.reread().unwrap();
        assert!(r.broken.is_none());
        assert_eq!(r.entries.len(), 2);
        let mut bytes = c.memory.clone();
        bytes[40] ^= 1;
        assert!(read(&bytes).broken.is_some());
        let first_len = 8 + 28 + 2 * ITEM_LEN + 32;
        let without_first = c.memory[first_len..].to_vec();
        assert!(read(&without_first).broken.is_some(), "a dropped entry breaks the chain");
    }

    #[test]
    fn an_altered_length_is_not_taken_for_a_torn_tail() {
        let mut c = Chain::open("memory:len").unwrap();
        c.append(1, &[item(1)]).unwrap();
        c.append(2, &[item(2)]).unwrap();
        let mut bytes = c.memory.clone();
        bytes[5] = 0x7f; // the first entry's length, now far past the end
        let r = read(&bytes);
        assert!(r.broken.is_some() && !r.torn_tail, "{r:?}");
        // A genuinely torn last append is still dropped.
        let cut = c.memory[..c.memory.len() - 10].to_vec();
        let r = read(&cut);
        assert!(r.broken.is_none() && r.torn_tail && r.entries.len() == 1);
    }

    #[test]
    fn a_leaf_ignores_columns_added_later_and_sees_any_change() {
        let (w, l) = leaf(1, 10, &[(0, b"x"), (1, b"y")]);
        assert_eq!(leaf_upto(1, 10, w, &[(0, b"x"), (1, b"y"), (2, b"new")]), l);
        assert_ne!(leaf_upto(1, 10, w, &[(0, b"x"), (1, b"z")]), l);
        assert_ne!(leaf_upto(1, 11, w, &[(0, b"x"), (1, b"y")]), l);
    }
}
