//! `SAVE` and the load at open: every database as one file.
//!
//! ```text
//! magic      "RDBKVC01"
//! section*   db u32, then entry* each led by the byte 1, then the byte 0
//! end        db = u32::MAX
//! crc32      over everything above (IEEE, crc32fast)
//!
//! entry      key_len u32, key, expires_at u64, tag u8, payload
//!            tag 0 string   len u32, bytes
//!            tag 1 integer  i64
//!            tag 2 hash     n u64, (len u32, field, len u32, value)*
//!            tag 3 list     n u64, (len u32, bytes)*
//!            tag 4 set      n u64, (len u32, bytes)*
//!            tag 5 zset     n u64, (len u32, member, score f64)*
//! ```
//!
//! Little-endian throughout, like the engine's other files.
//!
//! **Writing streams.** One shard is encoded under its read lock into a
//! buffer, the buffer goes to the file, and the next shard follows. The save
//! never holds more than one shard's bytes. Writers wait only on the one
//! shard being encoded, never on the whole store. The file is a sibling
//! renamed into place after an fsync, and the directory is fsynced after the
//! rename, so a crash leaves the previous snapshot whole.
//!
//! **Loading verifies first.** The CRC is checked in a first pass over the
//! file before a single key is stored. A torn or truncated snapshot is
//! refused, never loaded in part.
//!
//! Like Redis's `BGSAVE`, this is a walk of a live store, not a frozen moment.
//! Each key is consistent, and keys in different shards may come from
//! slightly different instants.

use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::sync::atomic::Ordering;

use super::store::{Slot, Value, now_ms};
use super::values::{HashVal, ListVal, SetVal, ZSet};
use super::{DATABASES, KvCacheHandle, KvError};

const MAGIC: &[u8; 8] = b"RDBKVC01";
const END: u32 = u32::MAX;

const TAG_STRING: u8 = 0;
const TAG_INTEGER: u8 = 1;
const TAG_HASH: u8 = 2;
const TAG_LIST: u8 = 3;
const TAG_SET: u8 = 4;
const TAG_ZSET: u8 = 5;

/// A writer that keeps the running CRC of what passed through it.
struct Crc<W: Write> {
    inner: W,
    hasher: crc32fast::Hasher,
}

impl<W: Write> Write for Crc<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.hasher.update(&buf[..n]);
        Ok(n)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    out.extend_from_slice(bytes);
}

fn encode(out: &mut Vec<u8>, slot: &Slot) {
    out.push(1);
    put_bytes(out, slot.key());
    out.extend_from_slice(&slot.expires_at.to_le_bytes());
    match &slot.value {
        Value::Str => {
            out.push(TAG_STRING);
            put_bytes(out, slot.str_bytes());
        }
        Value::Int(n) => {
            out.push(TAG_INTEGER);
            out.extend_from_slice(&n.to_le_bytes());
        }
        Value::Hash(map) => {
            out.push(TAG_HASH);
            out.extend_from_slice(&(map.len() as u64).to_le_bytes());
            for (f, v) in map.iter() {
                put_bytes(out, f);
                put_bytes(out, v);
            }
        }
        Value::List(list) => {
            out.push(TAG_LIST);
            out.extend_from_slice(&(list.len() as u64).to_le_bytes());
            for v in list.iter() {
                put_bytes(out, v);
            }
        }
        Value::Set(set) => {
            out.push(TAG_SET);
            out.extend_from_slice(&(set.len() as u64).to_le_bytes());
            for m in set.iter() {
                put_bytes(out, m);
            }
        }
        Value::ZSet(z) => {
            out.push(TAG_ZSET);
            out.extend_from_slice(&(z.len() as u64).to_le_bytes());
            for (score, m) in z.iter() {
                put_bytes(out, m);
                out.extend_from_slice(&score.to_le_bytes());
            }
        }
    }
}

/// Writes every database's live keys. Returns how many keys.
pub fn save(handle: &KvCacheHandle) -> Result<usize, KvError> {
    let path = handle.snapshot_path()?;
    let tmp = path.with_extension("snap.tmp");
    let io = |e: std::io::Error| KvError::Other(format!("{}: {e}", path.display()));
    let now = now_ms();
    let mut keys = 0usize;
    // Each database's version as the walk began, recorded as saved only once
    // the file is in place.
    let mut versions = Vec::new();
    {
        let file = File::create(&tmp).map_err(io)?;
        let mut w = Crc {
            inner: BufWriter::with_capacity(1 << 20, file),
            hasher: crc32fast::Hasher::new(),
        };
        w.write_all(MAGIC).map_err(io)?;
        let mut buf = Vec::with_capacity(64 * 1024);
        for db in 0..DATABASES {
            let Some(store) = handle.existing(db) else {
                continue;
            };
            let version = store.total_version();
            w.write_all(&(db as u32).to_le_bytes()).map_err(io)?;
            for cell in store.cells() {
                buf.clear();
                {
                    let shard = cell.lock.read();
                    for slot in shard.iter() {
                        if !slot.is_expired(now) {
                            encode(&mut buf, slot);
                            keys += 1;
                        }
                    }
                }
                w.write_all(&buf).map_err(io)?;
            }
            w.write_all(&[0]).map_err(io)?;
            versions.push((store.clone(), version));
        }
        w.write_all(&END.to_le_bytes()).map_err(io)?;
        let crc = w.hasher.clone().finalize();
        let mut inner = w.inner;
        inner.write_all(&crc.to_le_bytes()).map_err(io)?;
        let file = inner.into_inner().map_err(|e| io(e.into_error()))?;
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
    for (store, version) in versions {
        store.saved_version.store(version, Ordering::Relaxed);
    }
    Ok(keys)
}

/// Checks a snapshot's magic and CRC without loading it.
fn verify(path: &Path) -> Result<(), KvError> {
    let corrupt = |what: &str| KvError::Other(format!("{} is not a snapshot: {what}", path.display()));
    let io = |e: std::io::Error| KvError::Other(format!("{}: {e}", path.display()));
    let len = fs::metadata(path).map_err(io)?.len();
    if len < (MAGIC.len() + 4 + 4) as u64 {
        return Err(corrupt("too short"));
    }
    let mut r = BufReader::with_capacity(1 << 20, File::open(path).map_err(io)?);
    let mut hasher = crc32fast::Hasher::new();
    let mut remaining = len - 4;
    let mut buf = vec![0u8; 1 << 20];
    let mut first = true;
    while remaining > 0 {
        let n = (remaining as usize).min(buf.len());
        r.read_exact(&mut buf[..n]).map_err(io)?;
        if first {
            if n < MAGIC.len() || &buf[..MAGIC.len()] != MAGIC {
                return Err(corrupt("wrong magic"));
            }
            first = false;
        }
        hasher.update(&buf[..n]);
        remaining -= n as u64;
    }
    let mut trailer = [0u8; 4];
    r.read_exact(&mut trailer).map_err(io)?;
    if hasher.finalize() != u32::from_le_bytes(trailer) {
        return Err(corrupt("checksum mismatch, so it is torn or truncated"));
    }
    Ok(())
}

struct Reader<R: Read> {
    r: R,
}

impl<R: Read> Reader<R> {
    fn exact<const N: usize>(&mut self) -> Option<[u8; N]> {
        let mut b = [0u8; N];
        self.r.read_exact(&mut b).ok()?;
        Some(b)
    }
    fn u8(&mut self) -> Option<u8> {
        self.exact::<1>().map(|b| b[0])
    }
    fn u32(&mut self) -> Option<u32> {
        self.exact::<4>().map(u32::from_le_bytes)
    }
    fn u64(&mut self) -> Option<u64> {
        self.exact::<8>().map(u64::from_le_bytes)
    }
    fn bytes(&mut self) -> Option<Vec<u8>> {
        let len = self.u32()? as usize;
        let mut v = Vec::new();
        // `take` bounds the read by what the file actually holds, so a
        // corrupt length cannot make this allocate gigabytes up front.
        (&mut self.r).take(len as u64).read_to_end(&mut v).ok()?;
        (v.len() == len).then_some(v)
    }
}

fn decode<R: Read>(r: &mut Reader<R>) -> Option<Slot> {
    let key = r.bytes()?;
    let expires_at = r.u64()?;
    let tag = r.u8()?;
    let cap = |n: u64| (n as usize).min(1 << 16);
    Some(match tag {
        TAG_STRING => Slot::string(&key, &r.bytes()?, expires_at),
        TAG_INTEGER => Slot::with_value(&key, Value::Int(r.u64()? as i64), expires_at),
        TAG_HASH => {
            let n = r.u64()?;
            let mut map = HashVal::with_capacity(cap(n));
            for _ in 0..n {
                let f = r.bytes()?;
                let v = r.bytes()?;
                map.insert(&f, &v);
            }
            Slot::with_value(&key, Value::Hash(Box::new(map)), expires_at)
        }
        TAG_LIST => {
            let n = r.u64()?;
            let mut list = ListVal::new();
            for _ in 0..n {
                list.push_back(&r.bytes()?);
            }
            Slot::with_value(&key, Value::List(Box::new(list)), expires_at)
        }
        TAG_SET => {
            let n = r.u64()?;
            let mut set = SetVal::with_capacity(cap(n));
            for _ in 0..n {
                set.insert(&r.bytes()?);
            }
            Slot::with_value(&key, Value::Set(Box::new(set)), expires_at)
        }
        TAG_ZSET => {
            let n = r.u64()?;
            let mut z = ZSet::new();
            for _ in 0..n {
                let m = r.bytes()?;
                let score = f64::from_le_bytes(r.exact::<8>()?);
                if score.is_nan() {
                    return None;
                }
                z.insert(&m, score);
            }
            Slot::with_value(&key, Value::ZSet(Box::new(z)), expires_at)
        }
        _ => return None,
    })
}

/// Loads `path` into the handle's databases. Returns how many keys were
/// restored and how many had expired since the save.
pub fn load(handle: &KvCacheHandle, path: &Path) -> Result<(usize, usize), KvError> {
    verify(path)?;
    let corrupt = |what: String| KvError::Other(format!("{} is not a snapshot: {what}", path.display()));
    let io = |e: std::io::Error| KvError::Other(format!("{}: {e}", path.display()));
    let mut r = Reader {
        r: BufReader::with_capacity(1 << 20, File::open(path).map_err(io)?),
    };
    r.exact::<8>().ok_or_else(|| corrupt("no magic".into()))?;
    let now = now_ms();
    let (mut restored, mut skipped) = (0usize, 0usize);
    loop {
        let db = r.u32().ok_or_else(|| corrupt("missing database header".into()))?;
        if db == END {
            break;
        }
        let db = db as usize;
        if db >= DATABASES {
            return Err(corrupt(format!("database {db} is out of range")));
        }
        let store = handle.db(db);
        loop {
            match r.u8() {
                Some(0) => break,
                Some(1) => {}
                _ => return Err(corrupt(format!("database {db}: malformed entry marker"))),
            }
            let slot = decode(&mut r).ok_or_else(|| corrupt(format!("database {db}: malformed entry")))?;
            if slot.is_expired(now) {
                skipped += 1;
                continue;
            }
            let h = store.hash(slot.key());
            store.cell(h).lock.write().put(h, slot);
            restored += 1;
        }
        store
            .saved_version
            .store(store.total_version(), Ordering::Relaxed);
    }
    Ok((restored, skipped))
}
