//! `KV SAVE` and the load at open: the keyspace as one file.
//!
//! ```text
//! magic     "RDBKVC01"
//! count     u64
//! entry*    key_len u32, key, expires_at u64, tag u8, payload
//!           tag 0 string   len u32, bytes
//!           tag 1 integer  i64
//!           tag 2 hash     n u32, (field_len u32, field, value_len u32, value)*
//!           tag 3 list     n u32, (len u32, bytes)*
//!           tag 4 set      n u32, (len u32, bytes)*
//! crc32     over everything above
//! ```
//!
//! Little-endian throughout, like the rest of the engine's files. Written to
//! a sibling and renamed into place, so a crash mid-save leaves the previous
//! snapshot whole, and the CRC is what turns a torn or truncated file into a
//! refusal rather than a partial keyspace loaded without comment.
//!
//! The snapshot is a walk of the table, not a frozen moment: a key written
//! while the walk is past its slot is not in it, one written before the walk
//! reaches its slot is. For a cache that is the right trade — freezing the
//! whole table for the duration of a save would stall every writer for as
//! long as the file takes — and it is the same promise Redis's `BGSAVE` makes.

use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use crc::{CRC_32_ISCSI, Crc};

use super::{Entry, KvCache, KvError, Value, now_ms};

const MAGIC: &[u8; 8] = b"RDBKVC01";
const CRC32: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);

const TAG_STRING: u8 = 0;
const TAG_INTEGER: u8 = 1;
const TAG_HASH: u8 = 2;
const TAG_LIST: u8 = 3;
const TAG_SET: u8 = 4;

/// Writes every live key to the store's snapshot path. Returns how many.
pub fn save(store: &KvCache) -> Result<usize, KvError> {
    let path = store.snapshot_path()?;
    let generation = store.generation();
    let mut body = Vec::with_capacity(64 * 1024);
    body.extend_from_slice(MAGIC);
    body.extend_from_slice(&0u64.to_le_bytes());
    let mut count = 0u64;
    store.for_each_live(|entry| {
        encode_entry(&mut body, entry);
        count += 1;
    });
    body[8..16].copy_from_slice(&count.to_le_bytes());
    let crc = CRC32.checksum(&body);
    body.extend_from_slice(&crc.to_le_bytes());

    let tmp = path.with_extension("snap.tmp");
    let io = |e: std::io::Error| KvError::Other(format!("{}: {e}", path.display()));
    {
        let mut file = File::create(&tmp).map_err(io)?;
        file.write_all(&body).map_err(io)?;
        file.sync_all().map_err(io)?;
    }
    fs::rename(&tmp, &path).map_err(io)?;
    if let Some(dir) = path.parent() {
        // The rename is what makes the new file the snapshot; a directory
        // that has not been synced can forget it happened.
        if let Ok(dir) = File::open(dir) {
            let _ = dir.sync_all();
        }
    }
    store.mark_saved(generation);
    Ok(count as usize)
}

/// Loads `path` into `store`. Returns how many entries were restored and how
/// many were left out — expired since the save, or not fitting the store.
pub fn load(store: &KvCache, path: &Path) -> Result<(usize, usize), KvError> {
    let io = |e: std::io::Error| KvError::Other(format!("{}: {e}", path.display()));
    let mut bytes = Vec::new();
    File::open(path)
        .map_err(io)?
        .read_to_end(&mut bytes)
        .map_err(io)?;
    let corrupt = |what: &str| KvError::Other(format!("{} is not a snapshot: {what}", path.display()));

    if bytes.len() < MAGIC.len() + 8 + 4 {
        return Err(corrupt("too short"));
    }
    if &bytes[..MAGIC.len()] != MAGIC {
        return Err(corrupt("wrong magic"));
    }
    let (body, trailer) = bytes.split_at(bytes.len() - 4);
    let expected = u32::from_le_bytes(trailer.try_into().unwrap_or([0; 4]));
    if CRC32.checksum(body) != expected {
        return Err(corrupt("checksum mismatch, so it is torn or truncated"));
    }

    let mut reader = Reader {
        bytes: body,
        at: MAGIC.len(),
    };
    let count = reader.u64().ok_or_else(|| corrupt("no count"))?;
    let now = now_ms();
    let mut restored = 0usize;
    let mut skipped = 0usize;
    let mut full = false;
    for index in 0..count {
        let entry = decode_entry(&mut reader)
            .ok_or_else(|| corrupt(&format!("entry {index} of {count} is malformed")))?;
        let expires_at = entry.expires_at();
        if expires_at != 0 && now >= expires_at {
            skipped += 1;
            continue;
        }
        if full {
            skipped += 1;
            continue;
        }
        match store.restore(entry) {
            Ok(()) => restored += 1,
            Err(KvError::Full { .. }) => {
                full = true;
                skipped += 1;
            }
            Err(_) => skipped += 1,
        }
    }
    if full {
        log::warn!(
            "kv cache: {} held more keys than this store's max_keys ({}); {} were not loaded",
            path.display(),
            store.max_keys(),
            skipped
        );
    }
    // A load is a set of writes the file already holds.
    store.mark_saved(store.generation());
    Ok((restored, skipped))
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    out.extend_from_slice(bytes);
}

fn encode_entry(out: &mut Vec<u8>, entry: &Arc<Entry>) {
    put_bytes(out, &entry.key);
    out.extend_from_slice(&entry.expires_at().to_le_bytes());
    match &entry.value {
        Value::Str(s) => {
            out.push(TAG_STRING);
            put_bytes(out, s);
        }
        Value::Int(n) => {
            out.push(TAG_INTEGER);
            out.extend_from_slice(&n.to_le_bytes());
        }
        Value::Hash(map) => {
            out.push(TAG_HASH);
            let map = map.read();
            out.extend_from_slice(&(map.len() as u32).to_le_bytes());
            for (field, value) in map.iter() {
                put_bytes(out, field);
                put_bytes(out, value);
            }
        }
        Value::List(list) => {
            out.push(TAG_LIST);
            let list = list.read();
            out.extend_from_slice(&(list.len() as u32).to_le_bytes());
            for value in list.iter() {
                put_bytes(out, value);
            }
        }
        Value::Set(set) => {
            out.push(TAG_SET);
            let set = set.read();
            out.extend_from_slice(&(set.len() as u32).to_le_bytes());
            for member in set.iter() {
                put_bytes(out, member);
            }
        }
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl Reader<'_> {
    fn take(&mut self, n: usize) -> Option<&[u8]> {
        let end = self.at.checked_add(n)?;
        let slice = self.bytes.get(self.at..end)?;
        self.at = end;
        Some(slice)
    }
    fn u8(&mut self) -> Option<u8> {
        self.take(1).map(|b| b[0])
    }
    fn u32(&mut self) -> Option<u32> {
        self.take(4).map(|b| u32::from_le_bytes(b.try_into().unwrap()))
    }
    fn u64(&mut self) -> Option<u64> {
        self.take(8).map(|b| u64::from_le_bytes(b.try_into().unwrap()))
    }
    fn i64(&mut self) -> Option<i64> {
        self.take(8).map(|b| i64::from_le_bytes(b.try_into().unwrap()))
    }
    fn bytes(&mut self) -> Option<Box<[u8]>> {
        let len = self.u32()? as usize;
        self.take(len).map(Box::from)
    }
}

fn decode_entry(reader: &mut Reader<'_>) -> Option<Arc<Entry>> {
    use ahash::RandomState;
    use parking_lot::RwLock;

    let key = reader.bytes()?;
    let expires_at = reader.u64()?;
    let value = match reader.u8()? {
        TAG_STRING => Value::Str(reader.bytes()?),
        TAG_INTEGER => Value::Int(reader.i64()?),
        TAG_HASH => {
            let n = reader.u32()? as usize;
            let mut map = std::collections::HashMap::with_capacity_and_hasher(n, RandomState::new());
            for _ in 0..n {
                let field = reader.bytes()?;
                let value = reader.bytes()?;
                map.insert(field, value);
            }
            Value::Hash(RwLock::new(map))
        }
        TAG_LIST => {
            let n = reader.u32()? as usize;
            let mut list = std::collections::VecDeque::with_capacity(n);
            for _ in 0..n {
                list.push_back(reader.bytes()?);
            }
            Value::List(RwLock::new(list))
        }
        TAG_SET => {
            let n = reader.u32()? as usize;
            let mut set = std::collections::HashSet::with_capacity_and_hasher(n, RandomState::new());
            for _ in 0..n {
                set.insert(reader.bytes()?);
            }
            Value::Set(RwLock::new(set))
        }
        _ => return None,
    };
    Some(Arc::new(Entry {
        key,
        expires_at: AtomicU64::new(expires_at),
        value,
    }))
}
