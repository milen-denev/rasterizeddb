//! Paged indexes: an index whose entries live in sorted files on disk,
//! searched through a bounded block cache, with only its recent changes in
//! memory (docs/disk-indexes.md).
//!
//! A [`PagedStore`] is the storage behind a [`LiveIndex`](super::LiveIndex)
//! under `hybrid` or `disk` residency. It answers exactly the questions the
//! in-memory maps do, with one difference the whole design rests on: it may
//! list a slot under a key it no longer holds. Nothing is ever removed from a
//! file in place; a stale entry stays until a merge drops it, and the scan's
//! re-check filters it out meanwhile. It never lists too little.
//!
//! ```text
//!  lookups ──▶ memory part   (BTreeMap, inserts and removes land here)
//!          ──▶ frozen parts  (immutable, being written as runs)
//!          ──▶ runs          (immutable files, newest first)
//! ```
//!
//! # Presence and merges
//!
//! A merge drops every slot whose presence bit (the index's own bitmap,
//! shared with this store) is clear: a slot `forget` retired. That is sound
//! because a paged index sets a slot's bit **before** it lists the slot's
//! entry (the in-memory index does the reverse, for its snapshot capture,
//! which a paged index does not take). So a bit read clear by a merge means
//! no insert of that slot had begun, and an insert that begins later lists its
//! entry in the memory part, never in the runs the merge is rewriting.
//!
//! # Lifetime
//!
//! Nothing here is trusted across a restart (phase 1): a store starts empty,
//! and its index is rebuilt from rows. Run files live under the table's
//! `<stem>.idxr/` directory, in a subdirectory of their own per store
//! instance, and a run's file is deleted when the last reference to it goes.
//! A store's subdirectory goes with the store; ones left by an earlier process
//! are removed when a store of the same table opens.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::ops::Bound;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use parking_lot::{Mutex, RwLock};

use super::{KeyBuf, Slots};

// ---------------------------------------------------------------------------
// Settings
// ---------------------------------------------------------------------------

/// Where an index's entries live (`--index-residency`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Residency {
    /// Every entry in memory, as always.
    Memory,
    /// Runs on disk behind a generous cache and memory part.
    Hybrid,
    /// Runs on disk behind a small cache and memory part.
    Disk,
}

impl Residency {
    pub fn parse(s: &str) -> Option<Residency> {
        match s.trim().trim_matches('\'').to_ascii_lowercase().as_str() {
            "memory" => Some(Residency::Memory),
            "hybrid" => Some(Residency::Hybrid),
            "disk" => Some(Residency::Disk),
            _ => None,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Residency::Memory => "memory",
            Residency::Hybrid => "hybrid",
            Residency::Disk => "disk",
        }
    }
}

/// 255: not set; [`residency`] then reads `RASTERIZEDDB_INDEX_RESIDENCY`.
static RESIDENCY: AtomicU8 = AtomicU8::new(255);
/// Explicit budgets in MiB; 0 means the residency's default.
static CACHE_MB: AtomicU64 = AtomicU64::new(0);
static DELTA_MB: AtomicU64 = AtomicU64::new(0);

/// The residency set by `--index-residency`, else the environment's
/// `RASTERIZEDDB_INDEX_RESIDENCY` (how a whole test suite is run paged), else
/// `memory`.
pub fn residency() -> Residency {
    match RESIDENCY.load(Ordering::Relaxed) {
        0 => Residency::Memory,
        1 => Residency::Hybrid,
        2 => Residency::Disk,
        _ => {
            static FROM_ENV: OnceLock<Residency> = OnceLock::new();
            *FROM_ENV.get_or_init(|| {
                std::env::var("RASTERIZEDDB_INDEX_RESIDENCY")
                    .ok()
                    .and_then(|v| Residency::parse(&v))
                    .unwrap_or(Residency::Memory)
            })
        }
    }
}

/// Takes effect for tables opened afterwards.
pub fn set_residency(r: Residency) {
    let v = match r {
        Residency::Memory => 0,
        Residency::Hybrid => 1,
        Residency::Disk => 2,
    };
    RESIDENCY.store(v, Ordering::Relaxed);
    cache().resize(cache_bytes());
}

/// `--index-cache-mb`; 0 restores the residency's default.
pub fn set_cache_mb(mb: u64) {
    CACHE_MB.store(mb, Ordering::Relaxed);
    cache().resize(cache_bytes());
}

/// `--index-delta-mb`; 0 restores the residency's default.
pub fn set_delta_mb(mb: u64) {
    DELTA_MB.store(mb, Ordering::Relaxed);
}

/// The block cache's budget in bytes.
pub fn cache_bytes() -> u64 {
    let mb = match CACHE_MB.load(Ordering::Relaxed) {
        0 => match residency() {
            Residency::Memory => 0,
            Residency::Hybrid => 1024,
            Residency::Disk => 64,
        },
        mb => mb,
    };
    mb * 1024 * 1024
}

/// The memory part's budget in bytes, past which it is written as a run.
/// `RASTERIZEDDB_INDEX_DELTA_KB`, when no flag sets it, makes it tiny for a
/// test suite: every index then spills and merges constantly.
pub fn delta_bytes() -> u64 {
    if DELTA_MB.load(Ordering::Relaxed) == 0 {
        static FROM_ENV: OnceLock<Option<u64>> = OnceLock::new();
        let kb = *FROM_ENV.get_or_init(|| {
            std::env::var("RASTERIZEDDB_INDEX_DELTA_KB").ok().and_then(|v| v.trim().parse().ok())
        });
        if let Some(kb) = kb.filter(|kb| *kb > 0) {
            return kb * 1024;
        }
    }
    let mb = match DELTA_MB.load(Ordering::Relaxed) {
        0 => match residency() {
            Residency::Memory | Residency::Hybrid => 256,
            Residency::Disk => 16,
        },
        mb => mb,
    };
    mb * 1024 * 1024
}

/// What one store is built with: the settings at its open, or a test's own.
#[derive(Debug, Clone, Copy)]
pub struct Budgets {
    /// Bytes of keys and slots the memory part holds before it is frozen.
    pub delta_bytes: u64,
    /// Bytes a block is filled to before the next entry starts a new one.
    pub block_bytes: usize,
    /// Runs past which they are merged into one.
    pub runs_max: usize,
}

impl Budgets {
    pub fn current() -> Budgets {
        Budgets {
            delta_bytes: delta_bytes(),
            block_bytes: if residency() == Residency::Disk { 64 * 1024 } else { 16 * 1024 },
            runs_max: RUNS_MAX,
        }
    }
}

/// Runs a store keeps before merging them into one.
const RUNS_MAX: usize = 8;

/// Slots one entry of a run carries; a longer posting list continues in the
/// next entry under the same key.
const CHUNK_SLOTS: usize = 4096;

/// Rough bytes of bookkeeping one memory-part key costs beyond its bytes.
const KEY_OVERHEAD: u64 = 48;

// ---------------------------------------------------------------------------
// Block cache
// ---------------------------------------------------------------------------

const CACHE_SHARDS: usize = 64;

struct CacheEntry {
    key: (u64, u32),
    data: Arc<[u8]>,
    referenced: bool,
}

#[derive(Default)]
struct CacheShard {
    index: HashMap<(u64, u32), usize>,
    entries: Vec<CacheEntry>,
    hand: usize,
    bytes: u64,
}

/// A process-wide CLOCK cache of verified run blocks, keyed by
/// `(run id, block)`. Runs never change, so an entry is never stale; a dropped
/// run's blocks age out.
pub struct BlockCache {
    shards: Box<[Mutex<CacheShard>]>,
    per_shard: AtomicU64,
    pub hits: AtomicU64,
    pub misses: AtomicU64,
}

impl BlockCache {
    fn new(total: u64) -> BlockCache {
        BlockCache {
            shards: (0..CACHE_SHARDS).map(|_| Mutex::new(CacheShard::default())).collect(),
            per_shard: AtomicU64::new(total / CACHE_SHARDS as u64),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    fn resize(&self, total: u64) {
        self.per_shard.store(total / CACHE_SHARDS as u64, Ordering::Relaxed);
        for shard in self.shards.iter() {
            let mut s = shard.lock();
            Self::evict_to(&mut s, total / CACHE_SHARDS as u64);
        }
    }

    fn shard(&self, key: (u64, u32)) -> &Mutex<CacheShard> {
        let h = key.0.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ (key.1 as u64).wrapping_mul(0xC2B2_AE3D_27D4_EB4F);
        &self.shards[(h >> 32) as usize % CACHE_SHARDS]
    }

    fn get(&self, key: (u64, u32)) -> Option<Arc<[u8]>> {
        let mut s = self.shard(key).lock();
        let at = *s.index.get(&key)?;
        s.entries[at].referenced = true;
        self.hits.fetch_add(1, Ordering::Relaxed);
        Some(s.entries[at].data.clone())
    }

    fn insert(&self, key: (u64, u32), data: Arc<[u8]>) {
        let budget = self.per_shard.load(Ordering::Relaxed);
        if data.len() as u64 > budget {
            return;
        }
        let mut s = self.shard(key).lock();
        if s.index.contains_key(&key) {
            return;
        }
        Self::evict_to(&mut s, budget - data.len() as u64);
        let at = s.entries.len();
        s.bytes += data.len() as u64;
        s.entries.push(CacheEntry { key, data, referenced: false });
        s.index.insert(key, at);
    }

    /// Evicts by CLOCK until the shard holds at most `budget` bytes.
    fn evict_to(s: &mut CacheShard, budget: u64) {
        while s.bytes > budget && !s.entries.is_empty() {
            if s.hand >= s.entries.len() {
                s.hand = 0;
            }
            if s.entries[s.hand].referenced {
                s.entries[s.hand].referenced = false;
                s.hand += 1;
                continue;
            }
            let gone = s.entries.swap_remove(s.hand);
            s.bytes -= gone.data.len() as u64;
            s.index.remove(&gone.key);
            if s.hand < s.entries.len() {
                let moved = s.entries[s.hand].key;
                s.index.insert(moved, s.hand);
            }
        }
    }

    /// Bytes held, across shards.
    pub fn bytes(&self) -> u64 {
        self.shards.iter().map(|s| s.lock().bytes).sum()
    }
}

/// The process's block cache.
pub fn cache() -> &'static BlockCache {
    static CACHE: OnceLock<BlockCache> = OnceLock::new();
    CACHE.get_or_init(|| BlockCache::new(cache_bytes()))
}

// ---------------------------------------------------------------------------
// Encoding
// ---------------------------------------------------------------------------

const RUN_MAGIC: &[u8; 8] = b"RDBIDXR1";
const RUN_VERSION: u32 = 1;
const FOOTER_MAGIC: &[u8; 4] = b"RIDX";
const HEADER_LEN: u64 = 16;
const FOOTER_LEN: u64 = 40;

fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn get_varint(b: &[u8], at: &mut usize) -> io::Result<u64> {
    let mut v = 0u64;
    let mut shift = 0;
    loop {
        let byte = *b.get(*at).ok_or_else(|| corrupt("varint past the end"))?;
        *at += 1;
        if shift > 63 {
            return Err(corrupt("varint too long"));
        }
        v |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            return Ok(v);
        }
        shift += 7;
    }
}

fn corrupt(why: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, format!("index run: {why}"))
}

fn crc(bytes: &[u8]) -> u32 {
    crate::core::hash::crc32c::crc32c(bytes)
}

/// One entry of a block, borrowed from it.
struct EntryRef<'a> {
    key: &'a [u8],
    n_slots: usize,
    slot_bytes: &'a [u8],
}

impl EntryRef<'_> {
    fn slots_into(&self, out: &mut Vec<u64>) -> io::Result<()> {
        let mut at = 0usize;
        let mut prev = 0u64;
        for i in 0..self.n_slots {
            let d = get_varint(self.slot_bytes, &mut at)?;
            prev = if i == 0 { d } else { prev.checked_add(d).ok_or_else(|| corrupt("slot overflow"))? };
            out.push(prev);
        }
        Ok(())
    }
}

/// The entries of a verified block, in order.
fn block_entries(block: &[u8], mut f: impl FnMut(EntryRef<'_>) -> io::Result<bool>) -> io::Result<()> {
    let body = &block[..block.len() - 4];
    let mut at = 0usize;
    let n = get_varint(body, &mut at)?;
    for _ in 0..n {
        let klen = get_varint(body, &mut at)? as usize;
        let key = body.get(at..at + klen).ok_or_else(|| corrupt("key past the end"))?;
        at += klen;
        let n_slots = get_varint(body, &mut at)? as usize;
        let sbytes = get_varint(body, &mut at)? as usize;
        let slot_bytes = body.get(at..at + sbytes).ok_or_else(|| corrupt("slots past the end"))?;
        at += sbytes;
        if !f(EntryRef { key, n_slots, slot_bytes })? {
            return Ok(());
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Runs
// ---------------------------------------------------------------------------

struct Fence {
    first: KeyBuf,
    offset: u64,
    len: u32,
}

/// An immutable sorted file of `(key, slots)` entries. Its file is deleted
/// when the last reference to it goes.
pub struct Run {
    id: u64,
    path: PathBuf,
    /// A sealed run's file outlives it: the snapshot names it (phase 2).
    keep: bool,
    file: File,
    fences: Vec<Fence>,
    n_entries: u64,
    n_keys: u64,
    bytes: u64,
}

fn next_run_id() -> u64 {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

impl Drop for Run {
    fn drop(&mut self) {
        if !self.keep {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

impl Run {
    fn open(path: PathBuf) -> io::Result<Run> {
        let file = File::open(&path)?;
        let len = file.metadata()?.len();
        if len < HEADER_LEN + FOOTER_LEN {
            return Err(corrupt("shorter than its header and footer"));
        }
        let mut header = [0u8; HEADER_LEN as usize];
        file.read_exact_at(&mut header, 0)?;
        if &header[..8] != RUN_MAGIC || u32::from_le_bytes(header[8..12].try_into().unwrap()) != RUN_VERSION {
            return Err(corrupt("bad header"));
        }
        let mut footer = [0u8; FOOTER_LEN as usize];
        file.read_exact_at(&mut footer, len - FOOTER_LEN)?;
        let u64_at = |i: usize| u64::from_le_bytes(footer[i..i + 8].try_into().unwrap());
        let (f_off, f_len, n_entries, n_keys) = (u64_at(0), u64_at(8), u64_at(16), u64_at(24));
        let f_crc = u32::from_le_bytes(footer[32..36].try_into().unwrap());
        if &footer[36..40] != FOOTER_MAGIC || f_off.checked_add(f_len) != Some(len - FOOTER_LEN) {
            return Err(corrupt("bad footer"));
        }
        let mut raw = vec![0u8; f_len as usize];
        file.read_exact_at(&mut raw, f_off)?;
        if crc(&raw) != f_crc {
            return Err(corrupt("fence checksum mismatch"));
        }
        let mut at = 0usize;
        let count = get_varint(&raw, &mut at)?;
        let mut fences = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let klen = get_varint(&raw, &mut at)? as usize;
            let first = KeyBuf::from_slice(raw.get(at..at + klen).ok_or_else(|| corrupt("fence key"))?);
            at += klen;
            let offset = get_varint(&raw, &mut at)?;
            let blen = get_varint(&raw, &mut at)? as u32;
            fences.push(Fence { first, offset, len: blen });
        }
        Ok(Run { id: next_run_id(), path, keep: false, file, fences, n_entries, n_keys, bytes: len })
    }

    /// Block `b`, verified, through the cache. `cached` false reads around
    /// it, for a merge's sequential pass that would only evict hot blocks.
    fn block(&self, b: usize, cached: bool) -> io::Result<Arc<[u8]>> {
        let key = (self.id, b as u32);
        if cached {
            if let Some(hit) = cache().get(key) {
                return Ok(hit);
            }
            cache().misses.fetch_add(1, Ordering::Relaxed);
        }
        let f = &self.fences[b];
        let mut buf = vec![0u8; f.len as usize];
        self.file.read_exact_at(&mut buf, f.offset)?;
        if buf.len() < 4 {
            return Err(corrupt("block too short"));
        }
        let (body, sum) = buf.split_at(buf.len() - 4);
        if crc(body) != u32::from_le_bytes(sum.try_into().unwrap()) {
            return Err(corrupt("block checksum mismatch"));
        }
        let data: Arc<[u8]> = buf.into();
        if cached {
            cache().insert(key, data.clone());
        }
        Ok(data)
    }

    /// The first block that may hold a key at or above `lo`.
    fn first_block(&self, lo: Bound<&[u8]>) -> usize {
        match lo {
            Bound::Unbounded => 0,
            Bound::Included(k) | Bound::Excluded(k) => {
                // A key equal to a block's first key may also end the block
                // before it, when a posting list continues across them.
                self.fences.partition_point(|f| f.first.as_slice() < k).saturating_sub(1)
            }
        }
    }

    /// Every entry with a key in `[lo, hi]` (as bounded), in order, until `f`
    /// answers `false`.
    fn scan(
        &self,
        lo: Bound<&[u8]>,
        hi: Bound<&[u8]>,
        mut f: impl FnMut(&EntryRef<'_>) -> io::Result<bool>,
    ) -> io::Result<()> {
        let below_lo = |k: &[u8]| match lo {
            Bound::Unbounded => false,
            Bound::Included(l) => k < l,
            Bound::Excluded(l) => k <= l,
        };
        let above_hi = |k: &[u8]| match hi {
            Bound::Unbounded => false,
            Bound::Included(h) => k > h,
            Bound::Excluded(h) => k >= h,
        };
        for b in self.first_block(lo)..self.fences.len() {
            if above_hi(&self.fences[b].first) {
                break;
            }
            let block = self.block(b, true)?;
            let mut done = false;
            block_entries(&block, |e| {
                if below_lo(e.key) {
                    return Ok(true);
                }
                if above_hi(e.key) {
                    done = true;
                    return Ok(false);
                }
                if !f(&e)? {
                    done = true;
                    return Ok(false);
                }
                Ok(true)
            })?;
            if done {
                break;
            }
        }
        Ok(())
    }

    /// Every entry in order, read around the cache, merged per key.
    fn for_each_key(&self, mut f: impl FnMut(&[u8], Vec<u64>) -> io::Result<()>) -> io::Result<()> {
        let mut pending: Option<(KeyBuf, Vec<u64>)> = None;
        for b in 0..self.fences.len() {
            let block = self.block(b, false)?;
            block_entries(&block, |e| {
                match &mut pending {
                    Some((k, slots)) if k.as_slice() == e.key => e.slots_into(slots)?,
                    _ => {
                        if let Some((k, slots)) = pending.take() {
                            f(&k, slots)?;
                        }
                        let mut slots = Vec::with_capacity(e.n_slots);
                        e.slots_into(&mut slots)?;
                        pending = Some((KeyBuf::from_slice(e.key), slots));
                    }
                }
                Ok(true)
            })?;
        }
        if let Some((k, slots)) = pending {
            f(&k, slots)?;
        }
        Ok(())
    }
}

/// Writes one run, entry by entry in key order, to a temporary name; the file
/// takes its final name at [`RunWriter::finish`].
struct RunWriter {
    out: BufWriter<File>,
    /// CRC-32C of every byte written, for a sealed run's reference.
    digest: crate::core::hash::crc32c::Digest,
    tmp: PathBuf,
    path: PathBuf,
    offset: u64,
    block: Vec<u8>,
    block_n: u64,
    block_first: Option<KeyBuf>,
    fences: Vec<(KeyBuf, u64, u32)>,
    n_entries: u64,
    n_keys: u64,
    last_key: Option<KeyBuf>,
    target: usize,
    entry: Vec<u8>,
}

impl RunWriter {
    fn create(path: PathBuf, target: usize) -> io::Result<RunWriter> {
        let tmp = path.with_extension("tmp");
        let mut out = BufWriter::with_capacity(256 * 1024, File::create(&tmp)?);
        let mut header = Vec::with_capacity(HEADER_LEN as usize);
        header.extend_from_slice(RUN_MAGIC);
        header.extend_from_slice(&RUN_VERSION.to_le_bytes());
        header.extend_from_slice(&0u32.to_le_bytes());
        out.write_all(&header)?;
        let mut digest = crate::core::hash::crc32c::Digest::new();
        digest.update(&header);
        Ok(RunWriter {
            out,
            digest,
            tmp,
            path,
            offset: HEADER_LEN,
            block: Vec::with_capacity(target + 1024),
            block_n: 0,
            block_first: None,
            fences: Vec::new(),
            n_entries: 0,
            n_keys: 0,
            last_key: None,
            target,
            entry: Vec::new(),
        })
    }

    /// Adds `key`'s slots, sorted and distinct, after every key added so far.
    fn add(&mut self, key: &[u8], slots: &[u64]) -> io::Result<()> {
        if slots.is_empty() {
            return Ok(());
        }
        if self.last_key.as_deref() != Some(key) {
            debug_assert!(self.last_key.as_deref().is_none_or(|l| l < key), "keys out of order");
            self.n_keys += 1;
            self.last_key = Some(KeyBuf::from_slice(key));
        }
        for chunk in slots.chunks(CHUNK_SLOTS) {
            self.entry.clear();
            put_varint(&mut self.entry, key.len() as u64);
            self.entry.extend_from_slice(key);
            put_varint(&mut self.entry, chunk.len() as u64);
            let mut slot_bytes = Vec::with_capacity(chunk.len() * 2);
            let mut prev = 0u64;
            for (i, &s) in chunk.iter().enumerate() {
                put_varint(&mut slot_bytes, if i == 0 { s } else { s - prev });
                prev = s;
            }
            put_varint(&mut self.entry, slot_bytes.len() as u64);
            self.entry.extend_from_slice(&slot_bytes);
            if self.block_n > 0 && self.block.len() + self.entry.len() > self.target {
                self.finish_block()?;
            }
            if self.block_n == 0 {
                self.block_first = Some(KeyBuf::from_slice(key));
            }
            self.block.extend_from_slice(&self.entry);
            self.block_n += 1;
            self.n_entries += 1;
        }
        Ok(())
    }

    fn finish_block(&mut self) -> io::Result<()> {
        if self.block_n == 0 {
            return Ok(());
        }
        let mut framed = Vec::with_capacity(self.block.len() + 16);
        put_varint(&mut framed, self.block_n);
        framed.extend_from_slice(&self.block);
        let sum = crc(&framed);
        framed.extend_from_slice(&sum.to_le_bytes());
        self.out.write_all(&framed)?;
        self.digest.update(&framed);
        let first = self.block_first.take().expect("a block with entries has a first key");
        self.fences.push((first, self.offset, framed.len() as u32));
        self.offset += framed.len() as u64;
        self.block.clear();
        self.block_n = 0;
        Ok(())
    }

    fn finish(self) -> io::Result<Run> {
        self.finish_with(false).map(|(run, _)| run)
    }

    /// Finishes the file and, when `durable`, syncs it and its directory
    /// before and after the rename: a sealed run must be on disk before the
    /// snapshot naming it is. Returns the run and the whole file's CRC-32C.
    fn finish_with(mut self, durable: bool) -> io::Result<(Run, u32)> {
        self.finish_block()?;
        let mut raw = Vec::with_capacity(self.fences.len() * 32 + 8);
        put_varint(&mut raw, self.fences.len() as u64);
        for (first, off, len) in &self.fences {
            put_varint(&mut raw, first.len() as u64);
            raw.extend_from_slice(first);
            put_varint(&mut raw, *off);
            put_varint(&mut raw, *len as u64);
        }
        self.out.write_all(&raw)?;
        self.digest.update(&raw);
        let mut footer = Vec::with_capacity(FOOTER_LEN as usize);
        footer.extend_from_slice(&self.offset.to_le_bytes());
        footer.extend_from_slice(&(raw.len() as u64).to_le_bytes());
        footer.extend_from_slice(&self.n_entries.to_le_bytes());
        footer.extend_from_slice(&self.n_keys.to_le_bytes());
        footer.extend_from_slice(&crc(&raw).to_le_bytes());
        footer.extend_from_slice(FOOTER_MAGIC);
        self.out.write_all(&footer)?;
        self.digest.update(&footer);
        self.out.flush()?;
        // A working run is not synced: it is trusted only for the life of the
        // process that wrote it, which outlives none of its writes. Renamed
        // with the handle still open, which Unix allows; the drop that
        // follows then finds no temporary to remove.
        if durable {
            self.out.get_ref().sync_all()?;
        }
        let path = self.path.clone();
        std::fs::rename(&self.tmp, &path)?;
        if durable {
            if let Some(dir) = path.parent() {
                File::open(dir)?.sync_all()?;
            }
        }
        let digest = std::mem::take(&mut self.digest).finalize();
        Ok((Run::open(path)?, digest))
    }
}

impl Drop for RunWriter {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.tmp);
    }
}

// ---------------------------------------------------------------------------
// Store
// ---------------------------------------------------------------------------

/// Where a table's paged indexes keep their runs.
#[derive(Debug, Clone)]
pub struct Placement {
    /// `<location>/<stem>.idxr`.
    pub dir: PathBuf,
}

impl Placement {
    pub fn for_table(location: &Path, stem: &str) -> Placement {
        Placement { dir: location.join(format!("{stem}.idxr")) }
    }
}

/// A token unique to this process, so a store can tell its own
/// subdirectories from an earlier process's.
fn process_token() -> &'static str {
    static TOKEN: OnceLock<String> = OnceLock::new();
    TOKEN.get_or_init(|| {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos() as u64);
        format!("p{:x}{:x}", std::process::id(), nanos)
    })
}

#[derive(Default)]
struct State {
    memory: BTreeMap<KeyBuf, Slots>,
    memory_bytes: u64,
    frozen: Vec<Arc<BTreeMap<KeyBuf, Slots>>>,
    /// Oldest first.
    runs: Vec<Arc<Run>>,
}

struct Inner {
    dir: PathBuf,
    name: String,
    budgets: Budgets,
    state: RwLock<State>,
    /// Bumped by `clear`: a background job that started before one publishes
    /// nothing.
    epoch: AtomicU64,
    generation: AtomicU64,
    busy: AtomicBool,
    present: Arc<RwLock<Vec<u64>>>,
    /// The last background failure, for the log and the tests.
    failure: Mutex<Option<String>>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        // The runs' own drops delete their files first.
        self.state.get_mut().runs.clear();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// The storage of one paged index. See the module docs.
pub struct PagedStore {
    inner: Arc<Inner>,
}

/// A consistent view of every source at one instant: the memory part's
/// answer is taken under the lock, the rest are shared handles.
struct Sources {
    frozen: Vec<Arc<BTreeMap<KeyBuf, Slots>>>,
    runs: Vec<Arc<Run>>,
}

fn present_in(bits: &[u64], slot: u64) -> bool {
    bits.get((slot / 64) as usize).is_some_and(|w| w & (1 << (slot % 64)) != 0)
}

fn add_sorted(slots: &mut Slots, slot: u64) {
    match slots.last() {
        None => slots.push(slot),
        Some(&last) if last < slot => slots.push(slot),
        Some(_) => {
            if let Err(at) = slots.binary_search(&slot) {
                slots.insert(at, slot);
            }
        }
    }
}

impl PagedStore {
    /// A new, empty store for index `name` under `placement`. Removes what an
    /// earlier process left there (phase 1 trusts nothing across a restart).
    pub fn open(
        placement: &Placement,
        name: &str,
        budgets: Budgets,
        present: Arc<RwLock<Vec<u64>>>,
    ) -> io::Result<PagedStore> {
        static INSTANCE: AtomicU64 = AtomicU64::new(1);
        std::fs::create_dir_all(&placement.dir)?;
        let token = process_token();
        if let Ok(entries) = std::fs::read_dir(&placement.dir) {
            for e in entries.flatten() {
                let n = e.file_name();
                let n = n.to_string_lossy();
                // `sealed/` holds what a snapshot names; an open adopts or
                // ignores it, and a seal replaces it.
                if !n.starts_with(token) && n != SEALED_DIR {
                    let _ = std::fs::remove_dir_all(e.path());
                }
            }
        }
        let safe: String = name
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() || c == '_' { c } else { '_' })
            .collect();
        let dir = placement
            .dir
            .join(format!("{token}-{}-{safe}", INSTANCE.fetch_add(1, Ordering::Relaxed)));
        std::fs::create_dir_all(&dir)?;
        Ok(PagedStore {
            inner: Arc::new(Inner {
                dir,
                name: name.to_string(),
                budgets,
                state: RwLock::new(State::default()),
                epoch: AtomicU64::new(0),
                generation: AtomicU64::new(0),
                busy: AtomicBool::new(false),
                present,
                failure: Mutex::new(None),
            }),
        })
    }

    /// Lists `slot` under `key`. The caller has already set the slot's
    /// presence bit (see the module docs).
    pub fn insert(&self, slot: u64, key: KeyBuf) {
        let freeze = {
            let mut s = self.inner.state.write();
            let len = key.len() as u64;
            match s.memory.get_mut(&key) {
                Some(slots) => add_sorted(slots, slot),
                None => {
                    s.memory_bytes += len + KEY_OVERHEAD;
                    s.memory.entry(key).or_default().push(slot);
                }
            }
            s.memory_bytes += 8;
            if s.memory_bytes >= self.inner.budgets.delta_bytes {
                let full = std::mem::take(&mut s.memory);
                s.memory_bytes = 0;
                s.frozen.push(Arc::new(full));
                true
            } else {
                false
            }
        };
        if freeze {
            Inner::kick(&self.inner);
        }
    }

    /// Removes `slot` from under `key` in the memory part. An entry already in
    /// a frozen part or a run stays, stale, until a merge or rebuild.
    pub fn remove(&self, slot: u64, key: &[u8]) {
        let mut s = self.inner.state.write();
        let empty = match s.memory.get_mut(key) {
            Some(slots) => {
                if let Ok(at) = slots.binary_search(&slot) {
                    slots.remove(at);
                }
                slots.is_empty()
            }
            None => false,
        };
        if empty {
            s.memory.remove(key);
        }
    }

    pub fn clear(&self) {
        self.inner.epoch.fetch_add(1, Ordering::AcqRel);
        let mut s = self.inner.state.write();
        *s = State::default();
    }

    /// Distinct keys and entries, summed over every source: an upper bound,
    /// since a key or slot may sit in more than one.
    pub fn sizes(&self) -> (usize, usize) {
        let s = self.inner.state.read();
        let mut keys = s.memory.len();
        let mut entries: usize = s.memory.values().map(|v| v.len()).sum();
        for f in &s.frozen {
            keys += f.len();
            entries += f.values().map(|v| v.len()).sum::<usize>();
        }
        for r in &s.runs {
            keys += r.n_keys as usize;
            entries += r.n_entries as usize;
        }
        (keys, entries)
    }

    /// Bytes on disk, and runs.
    pub fn disk(&self) -> (u64, usize) {
        let s = self.inner.state.read();
        (s.runs.iter().map(|r| r.bytes).sum(), s.runs.len())
    }

    /// The last background failure, if any.
    pub fn failure(&self) -> Option<String> {
        self.inner.failure.lock().clone()
    }

    /// Takes the memory part's answer for `[lo, hi]` under the lock, and the
    /// other sources as handles.
    fn view(&self, lo: Bound<&[u8]>, hi: Bound<&[u8]>, mut f: impl FnMut(&[u8], &Slots) -> bool) -> (Sources, bool) {
        let s = self.inner.state.read();
        let mut more = true;
        if range_ok(lo, hi) {
            for (k, slots) in s.memory.range::<[u8], _>((lo, hi)) {
                if !f(k, slots) {
                    more = false;
                    break;
                }
            }
        }
        (Sources { frozen: s.frozen.clone(), runs: s.runs.clone() }, more)
    }

    /// Appends every slot listed under a key in `[lo, hi]`, from every source.
    /// `Ok(false)` once more than `cap` were found (the caller then gives the
    /// index up for this question, as for the in-memory maps).
    pub fn collect(&self, lo: Bound<&[u8]>, hi: Bound<&[u8]>, out: &mut Vec<u64>, cap: usize) -> io::Result<bool> {
        if !range_ok(lo, hi) {
            return Ok(true);
        }
        let (src, more) = self.view(lo, hi, |_, slots| {
            out.extend_from_slice(slots);
            out.len() <= cap
        });
        if !more {
            return Ok(false);
        }
        for f in src.frozen.iter().rev() {
            for (_, slots) in f.range::<[u8], _>((lo, hi)) {
                out.extend_from_slice(slots);
                if out.len() > cap {
                    return Ok(false);
                }
            }
        }
        for r in src.runs.iter().rev() {
            let mut over = false;
            r.scan(lo, hi, |e| {
                if out.len() + e.n_slots > cap {
                    over = true;
                    return Ok(false);
                }
                e.slots_into(out)?;
                Ok(true)
            })?;
            if over {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Slots listed under exactly `key`, from every source: an upper bound.
    pub fn listed_under(&self, key: &[u8]) -> io::Result<usize> {
        let b = Bound::Included(key);
        let mut n = 0usize;
        let (src, _) = self.view(b, b, |_, slots| {
            n += slots.len();
            true
        });
        for f in &src.frozen {
            n += f.get(key).map_or(0, |s| s.len());
        }
        for r in &src.runs {
            r.scan(b, b, |e| {
                n += e.n_slots;
                Ok(true)
            })?;
        }
        Ok(n)
    }

    /// Every slot under exactly `key`, sorted and distinct.
    pub fn posting(&self, key: &[u8]) -> io::Result<Vec<u64>> {
        let mut out = Vec::new();
        let b = Bound::Included(key);
        self.collect(b, b, &mut out, usize::MAX)?;
        out.sort_unstable();
        out.dedup();
        Ok(out)
    }

    /// Every key and its slots (union over sources, sorted and distinct), in
    /// key order. Slots whose presence bit is clear in `present` are left out
    /// when `drop_absent`.
    pub fn for_each_key(
        &self,
        drop_absent: bool,
        mut f: impl FnMut(&[u8], &[u64]) -> io::Result<()>,
    ) -> io::Result<()> {
        let (memory, src) = {
            let s = self.inner.state.read();
            (s.memory.clone(), Sources { frozen: s.frozen.clone(), runs: s.runs.clone() })
        };
        let present = drop_absent.then(|| self.inner.present.read().clone());
        let mut maps: Vec<&BTreeMap<KeyBuf, Slots>> = vec![&memory];
        maps.extend(src.frozen.iter().map(|f| f.as_ref()));
        merge_sources(&maps, &src.runs, present.as_deref(), |k, s| f(k, s))
    }

    /// Replaces nothing: adds a run built from `entries`, sorted by key, whose
    /// slots' presence bits the caller set first.
    pub fn load(&self, entries: Vec<(KeyBuf, Slots)>) -> io::Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let epoch = self.inner.epoch.load(Ordering::Acquire);
        let mut w = RunWriter::create(self.inner.next_path(), self.inner.budgets.block_bytes)?;
        for (k, slots) in &entries {
            w.add(k, slots)?;
        }
        let run = Arc::new(w.finish()?);
        let mut s = self.inner.state.write();
        if self.inner.epoch.load(Ordering::Acquire) == epoch {
            s.runs.push(run);
        }
        drop(s);
        Inner::kick(&self.inner);
        Ok(())
    }

    /// Writes every entry this store lists, merged into one run, durably, in
    /// `<stem>.idxr/sealed/`, for a snapshot to name (phase 2): called by a
    /// seal's capture, under the gate, so nothing writes meanwhile. Keeps the
    /// one earlier sealed run of this index that a published snapshot may
    /// still name (the newest two), and removes any older. `Ok(None)` when `cancelled` turned
    /// true midway; the partial file is removed.
    pub fn seal(&self, cancelled: &dyn Fn() -> bool) -> io::Result<Option<SealedRun>> {
        let sealed = self.sealed_dir();
        std::fs::create_dir_all(&sealed)?;
        let prefix = format!("{}.", safe_name(&self.inner.name));
        let mut existing: Vec<(u64, PathBuf)> = std::fs::read_dir(&sealed)?
            .flatten()
            .filter_map(|e| {
                let n = e.file_name().to_string_lossy().to_string();
                let seq = n.strip_prefix(&prefix)?.strip_suffix(".run")?.parse::<u64>().ok()?;
                Some((seq, e.path()))
            })
            .collect();
        existing.sort();
        let next = existing.last().map_or(1, |(seq, _)| seq + 1);
        // The published snapshot names the newest sealed run whose seal was
        // published, and a seal may still be skipped after writing its run,
        // so the newest two are kept; anything older no snapshot can name.
        // Removing one a snapshot does name would cost a rebuild, not an
        // answer: an adoption checks the file and refuses a missing one.
        if existing.len() > 2 {
            for (_, path) in &existing[..existing.len() - 2] {
                let _ = std::fs::remove_file(path);
            }
        }
        if cancelled() {
            return Ok(None);
        }
        let name = format!("{prefix}{next}.run");
        let mut w = RunWriter::create(sealed.join(&name), self.inner.budgets.block_bytes)?;
        let mut keys = 0usize;
        let mut stopped = false;
        // Exactly the index's sorted stream (`for_each_sorted`), dropping
        // nothing: a snapshot is that stream, a superset, as the in-memory
        // indexes write theirs. Background merges are what drop forgotten
        // slots.
        let written = self.for_each_key(false, |k, slots| {
            keys += 1;
            if keys % 1024 == 0 && cancelled() {
                stopped = true;
                return Err(io::Error::other("cancelled"));
            }
            w.add(k, slots)
        });
        // Checked again at the end: a small index never reaches the check
        // inside the loop. The writer's drop removes the partial file.
        if stopped || cancelled() {
            return Ok(None);
        }
        written?;
        let (run, crc) = w.finish_with(true)?;
        let mut run = run;
        run.keep = true;
        Ok(Some(SealedRun { name, len: run.bytes, crc }))
    }

    /// Takes a sealed run, checked whole against its length and CRC-32C, as
    /// this store's first run (phase 2). `Err` when it is missing, short or
    /// does not match: the caller rebuilds from rows instead.
    pub fn adopt(&self, sealed: &SealedRun) -> io::Result<()> {
        if sealed.name.contains('/') || sealed.name.contains("..") {
            return Err(corrupt("a sealed run's name names another directory"));
        }
        let path = self.sealed_dir().join(&sealed.name);
        let file = File::open(&path)?;
        let len = file.metadata()?.len();
        if len != sealed.len {
            return Err(corrupt("a sealed run's length does not match its snapshot"));
        }
        let mut digest = crate::core::hash::crc32c::Digest::new();
        let mut buf = vec![0u8; 1 << 20];
        let mut at = 0u64;
        while at < len {
            let n = ((len - at) as usize).min(buf.len());
            file.read_exact_at(&mut buf[..n], at)?;
            digest.update(&buf[..n]);
            at += n as u64;
        }
        if digest.finalize() != sealed.crc {
            return Err(corrupt("a sealed run's checksum does not match its snapshot"));
        }
        let mut run = Run::open(path)?;
        run.keep = true;
        let mut s = self.inner.state.write();
        s.runs.insert(0, Arc::new(run));
        Ok(())
    }

    fn sealed_dir(&self) -> PathBuf {
        self.inner.dir.parent().map_or_else(|| self.inner.dir.join(SEALED_DIR), |p| p.join(SEALED_DIR))
    }

    /// Waits for the background writer to finish what it has (tests). Kicks
    /// it only when it is not running: `kick` raises the busy flag itself, so
    /// kicking on every look would never see it down.
    pub fn settle(&self) {
        loop {
            let idle = {
                let s = self.inner.state.read();
                s.frozen.is_empty() && s.runs.len() <= self.inner.budgets.runs_max
            };
            if !self.inner.busy.load(Ordering::Acquire) {
                if idle || self.inner.failure.lock().is_some() {
                    return;
                }
                Inner::kick(&self.inner);
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }
}

/// Whether paging is turned off for tables under `dir`: a test's override,
/// for the tests of the in-memory snapshot format, which edit or drop that
/// format's entries and would otherwise run paged under
/// `RASTERIZEDDB_INDEX_RESIDENCY`. Always `false` outside tests.
pub fn memory_forced(dir: &Path) -> bool {
    #[cfg(test)]
    {
        return FORCED.lock().iter().any(|d| dir.starts_with(d));
    }
    #[cfg(not(test))]
    {
        let _ = dir;
        false
    }
}

#[cfg(test)]
static FORCED: Mutex<Vec<PathBuf>> = Mutex::new(Vec::new());

/// Keeps every index of a table under `dir` in memory until dropped.
#[cfg(test)]
pub(crate) struct ForcedMemory(PathBuf);

#[cfg(test)]
impl Drop for ForcedMemory {
    fn drop(&mut self) {
        FORCED.lock().retain(|d| *d != self.0);
    }
}

#[cfg(test)]
pub(crate) fn force_memory_under(dir: &Path) -> ForcedMemory {
    FORCED.lock().push(dir.to_path_buf());
    ForcedMemory(dir.to_path_buf())
}

/// The subdirectory of `<stem>.idxr/` holding sealed runs.
const SEALED_DIR: &str = "sealed";

fn safe_name(name: &str) -> String {
    name.chars().map(|c| if c.is_ascii_alphanumeric() || c == '_' { c } else { '_' }).collect()
}

/// A sealed run, as a snapshot's INDEX section names it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealedRun {
    /// The file's name inside `<stem>.idxr/sealed/`.
    pub name: String,
    pub len: u64,
    /// CRC-32C of the whole file.
    pub crc: u32,
}

/// The payload a paged index puts in its INDEX section instead of entries:
/// its sealed run, unkeyed slots and presence words. The magic is not an
/// INDEX-section version, so an in-memory index refuses it (and rebuilds),
/// while a paged index refuses nothing else that is not one of these.
pub const RUN_REF_MAGIC: &[u8; 8] = b"RDBRUNRF";

pub fn encode_run_ref(run: &SealedRun, unkeyed: &[u64], present: &[u64]) -> Vec<u8> {
    let mut out = Vec::with_capacity(64 + run.name.len() + 8 * (unkeyed.len() + present.len()));
    out.extend_from_slice(RUN_REF_MAGIC);
    put_varint(&mut out, run.name.len() as u64);
    out.extend_from_slice(run.name.as_bytes());
    out.extend_from_slice(&run.len.to_le_bytes());
    out.extend_from_slice(&run.crc.to_le_bytes());
    put_varint(&mut out, unkeyed.len() as u64);
    for s in unkeyed {
        put_varint(&mut out, *s);
    }
    put_varint(&mut out, present.len() as u64);
    for w in present {
        out.extend_from_slice(&w.to_le_bytes());
    }
    out
}

/// A run reference's parts; `None` when `bytes` are not one.
pub fn decode_run_ref(bytes: &[u8]) -> io::Result<Option<(SealedRun, Vec<u64>, Vec<u64>)>> {
    let Some(rest) = bytes.strip_prefix(RUN_REF_MAGIC.as_slice()) else {
        return Ok(None);
    };
    let mut at = 0usize;
    let nlen = get_varint(rest, &mut at)? as usize;
    let name = rest.get(at..at + nlen).ok_or_else(|| corrupt("run reference name"))?;
    let name = std::str::from_utf8(name).map_err(|_| corrupt("run reference name"))?.to_string();
    at += nlen;
    let fixed = rest.get(at..at + 12).ok_or_else(|| corrupt("run reference length"))?;
    let len = u64::from_le_bytes(fixed[..8].try_into().unwrap());
    let crc = u32::from_le_bytes(fixed[8..12].try_into().unwrap());
    at += 12;
    let n_unkeyed = get_varint(rest, &mut at)? as usize;
    if n_unkeyed > rest.len() {
        return Err(corrupt("run reference unkeyed count"));
    }
    let mut unkeyed = Vec::with_capacity(n_unkeyed);
    for _ in 0..n_unkeyed {
        unkeyed.push(get_varint(rest, &mut at)?);
    }
    let n_words = get_varint(rest, &mut at)? as usize;
    let words = rest.get(at..at + n_words * 8).ok_or_else(|| corrupt("run reference presence"))?;
    let present = words.chunks_exact(8).map(|c| u64::from_le_bytes(c.try_into().unwrap())).collect();
    if at + n_words * 8 != rest.len() {
        return Err(corrupt("run reference trailing bytes"));
    }
    Ok(Some((SealedRun { name, len, crc }, unkeyed, present)))
}

fn range_ok(lo: Bound<&[u8]>, hi: Bound<&[u8]>) -> bool {
    match (lo, hi) {
        (Bound::Included(l) | Bound::Excluded(l), Bound::Included(h) | Bound::Excluded(h)) => {
            !(l > h || (l == h && (matches!(lo, Bound::Excluded(_)) || matches!(hi, Bound::Excluded(_)))))
        }
        _ => true,
    }
}

/// K-way merge of in-memory maps and runs by key, each key's slots unioned,
/// sorted and distinct; slots absent from `present` dropped when given.
fn merge_sources(
    maps: &[&BTreeMap<KeyBuf, Slots>],
    runs: &[Arc<Run>],
    present: Option<&[u64]>,
    mut f: impl FnMut(&[u8], &[u64]) -> io::Result<()>,
) -> io::Result<()> {
    // Runs are streamed one key at a time on threads of their own, through
    // bounded channels, so a merge holds a few keys per run, not whole runs.
    let mut cursors: Vec<Box<dyn Iterator<Item = io::Result<(KeyBuf, Vec<u64>)>> + Send>> = Vec::new();
    for m in maps {
        let items: Vec<(KeyBuf, Vec<u64>)> = m.iter().map(|(k, s)| (k.clone(), s.to_vec())).collect();
        cursors.push(Box::new(items.into_iter().map(Ok)));
    }
    for r in runs {
        let (tx, rx) = std::sync::mpsc::sync_channel::<io::Result<(KeyBuf, Vec<u64>)>>(64);
        let run = r.clone();
        std::thread::spawn(move || {
            let res = run.for_each_key(|k, slots| {
                tx.send(Ok((KeyBuf::from_slice(k), slots))).map_err(|_| io::Error::other("merge reader gone"))
            });
            if let Err(e) = res {
                let _ = tx.send(Err(e));
            }
        });
        cursors.push(Box::new(rx.into_iter()));
    }
    let mut heads: Vec<Option<(KeyBuf, Vec<u64>)>> = Vec::with_capacity(cursors.len());
    for c in cursors.iter_mut() {
        heads.push(c.next().transpose()?);
    }
    let mut merged: Vec<u64> = Vec::new();
    loop {
        let Some(min) = heads.iter().filter_map(|h| h.as_ref().map(|(k, _)| k.clone())).min() else {
            return Ok(());
        };
        merged.clear();
        for i in 0..heads.len() {
            if heads[i].as_ref().is_some_and(|(k, _)| *k == min) {
                let (_, slots) = heads[i].take().unwrap();
                merged.extend(slots);
                heads[i] = cursors[i].next().transpose()?;
            }
        }
        merged.sort_unstable();
        merged.dedup();
        if let Some(bits) = present {
            merged.retain(|s| present_in(bits, *s));
        }
        if !merged.is_empty() {
            f(&min, &merged)?;
        }
    }
}

impl Inner {
    fn next_path(&self) -> PathBuf {
        self.dir.join(format!("{}.run", self.generation.fetch_add(1, Ordering::Relaxed)))
    }

    /// Starts the background writer unless it is running.
    fn kick(this: &Arc<Inner>) {
        if this.busy.swap(true, Ordering::AcqRel) {
            return;
        }
        let me = this.clone();
        let spawned = std::thread::Builder::new()
            .name("rdb-index-runs".into())
            .spawn(move || {
                loop {
                    me.work();
                    me.busy.store(false, Ordering::Release);
                    // Work that arrived after the last look, and found the
                    // flag still up, is picked up here.
                    let pending = {
                        let s = me.state.read();
                        !s.frozen.is_empty() || s.runs.len() > me.budgets.runs_max
                    };
                    if !pending || me.failure.lock().is_some() || me.busy.swap(true, Ordering::AcqRel) {
                        return;
                    }
                }
            });
        if spawned.is_err() {
            this.busy.store(false, Ordering::Release);
        }
    }

    /// Writes every frozen part as a run, merging whenever the runs pass the
    /// limit: between parts too, so a steady stream of inserts, which never
    /// leaves the frozen list empty, cannot pile up runs every lookup reads.
    fn work(&self) {
        loop {
            if !self.merge_if_needed() {
                return;
            }
            let (part, epoch) = {
                let s = self.state.read();
                (s.frozen.first().cloned(), self.epoch.load(Ordering::Acquire))
            };
            let Some(part) = part else { break };
            match self.write_part(&part) {
                Ok(run) => {
                    let mut s = self.state.write();
                    if self.epoch.load(Ordering::Acquire) == epoch {
                        if let Some(at) = s.frozen.iter().position(|f| Arc::ptr_eq(f, &part)) {
                            s.frozen.remove(at);
                            s.runs.push(Arc::new(run));
                        }
                    }
                }
                Err(e) => {
                    // The part stays in memory and answers lookups; nothing is
                    // lost, only not yet on disk.
                    self.fail(format!("writing a run of {}: {e}", self.name));
                    return;
                }
            }
        }
        if self.merge_if_needed() {
            // A whole pass with nothing failing clears an earlier failure.
            *self.failure.lock() = None;
        }
    }

    /// Merges the runs into one when there are more than the limit. `false`
    /// when that failed (recorded; the runs stay as they were).
    fn merge_if_needed(&self) -> bool {
        let (runs, epoch) = {
            let s = self.state.read();
            (s.runs.clone(), self.epoch.load(Ordering::Acquire))
        };
        if runs.len() <= self.budgets.runs_max {
            return true;
        }
        match self.merge(&runs) {
            Ok(merged) => {
                let mut s = self.state.write();
                // Only the runs merged are replaced; any written meanwhile
                // stay, newer than the merged one.
                if self.epoch.load(Ordering::Acquire) == epoch
                    && s.runs.len() >= runs.len()
                    && s.runs.iter().zip(&runs).all(|(a, b)| Arc::ptr_eq(a, b))
                {
                    let newer: Vec<Arc<Run>> = s.runs.drain(runs.len()..).collect();
                    s.runs.clear();
                    if let Some(m) = merged {
                        s.runs.push(Arc::new(m));
                    }
                    s.runs.extend(newer);
                }
                true
            }
            Err(e) => {
                self.fail(format!("merging the runs of {}: {e}", self.name));
                false
            }
        }
    }

    fn write_part(&self, part: &BTreeMap<KeyBuf, Slots>) -> io::Result<Run> {
        let mut w = RunWriter::create(self.next_path(), self.budgets.block_bytes)?;
        for (k, slots) in part {
            w.add(k, slots)?;
        }
        w.finish()
    }

    /// One run from `runs`, dropping slots `forget` retired; `None` when
    /// nothing is left.
    fn merge(&self, runs: &[Arc<Run>]) -> io::Result<Option<Run>> {
        let present = self.present.read().clone();
        let mut w = RunWriter::create(self.next_path(), self.budgets.block_bytes)?;
        let mut any = false;
        merge_sources(&[], runs, Some(&present), |k, slots| {
            any = true;
            w.add(k, slots)
        })?;
        if !any {
            return Ok(None);
        }
        w.finish().map(Some)
    }

    fn fail(&self, why: String) {
        log::error!("index runs: {why}; the entries stay in memory");
        *self.failure.lock() = Some(why);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store(delta: u64, block: usize, runs_max: usize) -> (tempfile::TempDir, PagedStore, Arc<RwLock<Vec<u64>>>) {
        let dir = tempfile::tempdir().unwrap();
        let present = Arc::new(RwLock::new(Vec::new()));
        let s = PagedStore::open(
            &Placement::for_table(dir.path(), "t"),
            "ix",
            Budgets { delta_bytes: delta, block_bytes: block, runs_max },
            present.clone(),
        )
        .unwrap();
        (dir, s, present)
    }

    fn set(present: &RwLock<Vec<u64>>, slot: u64, on: bool) {
        let mut b = present.write();
        let w = (slot / 64) as usize;
        if b.len() <= w {
            b.resize(w + 1, 0);
        }
        if on {
            b[w] |= 1 << (slot % 64);
        } else {
            b[w] &= !(1 << (slot % 64));
        }
    }

    fn k(n: u64) -> KeyBuf {
        KeyBuf::from_slice(&n.to_be_bytes())
    }

    #[test]
    fn a_run_round_trips_and_a_key_spans_blocks() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = RunWriter::create(dir.path().join("a.run"), 256).unwrap();
        let long: Vec<u64> = (0..10_000).map(|i| i * 3).collect();
        w.add(&k(1), &[5]).unwrap();
        w.add(&k(2), &long).unwrap();
        w.add(&k(3), &[1, 2]).unwrap();
        let run = w.finish().unwrap();
        // Three chunks of the long key, each a block of its own past the
        // target, between the other two keys' blocks.
        assert!(run.fences.len() >= 5, "the long key spans blocks: {}", run.fences.len());
        assert_eq!(run.n_keys, 3);
        let mut got = Vec::new();
        run.scan(Bound::Included(&k(2)), Bound::Included(&k(2)), |e| {
            e.slots_into(&mut got)?;
            Ok(true)
        })
        .unwrap();
        assert_eq!(got, long);
        let mut all = Vec::new();
        run.for_each_key(|key, slots| {
            all.push((KeyBuf::from_slice(key), slots));
            Ok(())
        })
        .unwrap();
        assert_eq!(all, vec![(k(1), vec![5]), (k(2), long), (k(3), vec![1, 2])]);
        let path = run.path.clone();
        drop(run);
        assert!(!path.exists(), "a dropped run deletes its file");
    }

    #[test]
    fn a_flipped_byte_is_an_error_not_a_short_answer() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = RunWriter::create(dir.path().join("b.run"), 64).unwrap();
        for i in 0..100 {
            w.add(&k(i), &[i]).unwrap();
        }
        let run = w.finish().unwrap();
        let mut bytes = std::fs::read(&run.path).unwrap();
        bytes[(HEADER_LEN + 5) as usize] ^= 0xff;
        std::fs::write(&run.path, &bytes).unwrap();
        let reopened = Run::open(run.path.clone()).unwrap();
        let r = reopened.scan(Bound::Unbounded, Bound::Unbounded, |_| Ok(true));
        assert!(r.is_err(), "a corrupt block must fail the read");
        std::mem::forget(reopened);
    }

    /// The store against a model: every lookup lists at least what the model
    /// holds, and nothing that was never listed under that key.
    #[test]
    fn the_store_lists_a_superset_through_spills_and_merges() {
        let (_dir, s, present) = store(4096, 128, 3);
        let mut model: BTreeMap<KeyBuf, Vec<u64>> = BTreeMap::new();
        let mut ever: BTreeMap<KeyBuf, Vec<u64>> = BTreeMap::new();
        let mut rng = 0x2545_F491_4F6C_DD1Du64;
        let mut next = || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        let mut live: Vec<(u64, KeyBuf)> = Vec::new();
        for slot in 0..4000u64 {
            let key = k(next() % 300);
            set(&present, slot, true);
            s.insert(slot, key.clone());
            model.entry(key.clone()).or_default().push(slot);
            ever.entry(key.clone()).or_default().push(slot);
            live.push((slot, key));
            // Now and then a removal (a key change) or a forget (vacuum).
            if next() % 5 == 0 && !live.is_empty() {
                let at = (next() as usize) % live.len();
                let (gone, key) = live.swap_remove(at);
                s.remove(gone, &key);
                model.get_mut(&key).unwrap().retain(|x| *x != gone);
                if next() % 2 == 0 {
                    set(&present, gone, false);
                }
            }
            if slot % 500 == 0 {
                s.settle();
            }
            // Merged as they come, not only once writes stop: a lookup
            // reads every run, so runs must not pile up under a write load.
            assert!(s.disk().1 <= 3 + 2, "runs piled up: {:?}", s.disk());
        }
        s.settle();
        assert!(s.failure().is_none(), "{:?}", s.failure());
        assert!(s.disk().1 <= 3, "merged down to the limit: {:?}", s.disk());
        for key in 0..300u64 {
            let key = k(key);
            let got = s.posting(&key).unwrap();
            let want = model.get(&key).cloned().unwrap_or_default();
            for w in &want {
                assert!(got.contains(w), "slot {w} missing under {key:?}");
            }
            let once = ever.get(&key).cloned().unwrap_or_default();
            for g in &got {
                assert!(once.contains(g), "slot {g} was never under {key:?}");
            }
        }
        // A range over everything is the union.
        let mut all = Vec::new();
        assert!(s.collect(Bound::Unbounded, Bound::Unbounded, &mut all, usize::MAX).unwrap());
        all.sort_unstable();
        all.dedup();
        for slots in model.values() {
            for x in slots {
                assert!(all.binary_search(x).is_ok());
            }
        }
        // A cap stops early.
        let mut capped = Vec::new();
        assert!(!s.collect(Bound::Unbounded, Bound::Unbounded, &mut capped, 10).unwrap());
    }

    /// Phase 2: a sealed run outlives its store, and a store opened later
    /// (a process restart: a new instance under the same placement) adopts
    /// it and answers as the first did. Anything not exactly what was sealed
    /// is refused.
    #[test]
    fn a_sealed_run_is_adopted_by_the_next_store_and_nothing_else_is() {
        let dir = tempfile::tempdir().unwrap();
        let placement = Placement::for_table(dir.path(), "t");
        let budgets = Budgets { delta_bytes: 2048, block_bytes: 128, runs_max: 3 };
        let present = Arc::new(RwLock::new(Vec::new()));
        let first = PagedStore::open(&placement, "ix", budgets, present.clone()).unwrap();
        for slot in 0..3000u64 {
            set(&present, slot, true);
            first.insert(slot, k(slot % 97));
        }
        first.settle();
        let sealed = first.seal(&|| false).unwrap().expect("not cancelled");
        let want: Vec<Vec<u64>> = (0..97).map(|key| first.posting(&k(key)).unwrap()).collect();
        drop(first);
        assert!(placement.dir.join(SEALED_DIR).join(&sealed.name).exists(), "the sealed run outlives its store");

        let second = PagedStore::open(&placement, "ix", budgets, Arc::new(RwLock::new(Vec::new()))).unwrap();
        assert!(placement.dir.join(SEALED_DIR).join(&sealed.name).exists(), "an open leaves sealed/ alone");
        second.adopt(&sealed).unwrap();
        for key in 0..97u64 {
            assert_eq!(second.posting(&k(key)).unwrap(), want[key as usize], "key {key}");
        }

        // A reference that does not match the file is refused.
        let refuse = |r: &SealedRun| {
            let s = PagedStore::open(&placement, "ix", budgets, Arc::new(RwLock::new(Vec::new()))).unwrap();
            assert!(s.adopt(r).is_err(), "{r:?} was adopted");
        };
        refuse(&SealedRun { crc: sealed.crc ^ 1, ..sealed.clone() });
        refuse(&SealedRun { len: sealed.len + 1, ..sealed.clone() });
        refuse(&SealedRun { name: "../x.run".into(), ..sealed.clone() });
        refuse(&SealedRun { name: "ix.999.run".into(), ..sealed.clone() });
        // A flipped byte in the file itself: a copy, under its own name, so
        // the run the second store adopted stays sound.
        let path = placement.dir.join(SEALED_DIR).join(&sealed.name);
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[100] ^= 0xff;
        std::fs::write(placement.dir.join(SEALED_DIR).join("ix.0.run"), &bytes).unwrap();
        refuse(&SealedRun { name: "ix.0.run".into(), ..sealed.clone() });
        std::fs::remove_file(placement.dir.join(SEALED_DIR).join("ix.0.run")).unwrap();

        // Sealing again keeps the newest earlier run and no older; a
        // cancelled seal leaves nothing behind.
        for _ in 0..3 {
            second.seal(&|| false).unwrap().unwrap();
        }
        let runs = || std::fs::read_dir(placement.dir.join(SEALED_DIR)).unwrap().count();
        assert_eq!(runs(), 3, "the run just sealed and the two before it");
        assert!(second.seal(&|| true).unwrap().is_none(), "cancelled");
        assert_eq!(runs(), 2, "a cancelled seal leaves no file of its own");
    }

    #[test]
    fn a_run_reference_round_trips_and_refuses_garbage() {
        let run = SealedRun { name: "ix.3.run".into(), len: 12345, crc: 0xDEAD_BEEF };
        let bytes = encode_run_ref(&run, &[2, 9], &[0b101, 0, 1]);
        assert_eq!(decode_run_ref(&bytes).unwrap(), Some((run, vec![2, 9], vec![0b101, 0, 1])));
        assert_eq!(decode_run_ref(b"\x01\x00 an index section").unwrap(), None);
        assert!(decode_run_ref(&bytes[..bytes.len() - 3]).is_err());
        let mut longer = bytes.clone();
        longer.push(0);
        assert!(decode_run_ref(&longer).is_err());
    }

    #[test]
    fn clear_forgets_everything_and_its_files() {
        let (_dir, s, present) = store(100, 64, 2);
        for slot in 0..500 {
            set(&present, slot, true);
            s.insert(slot, k(slot % 7));
        }
        s.settle();
        assert!(s.disk().0 > 0);
        s.clear();
        assert_eq!(s.sizes(), (0, 0));
        assert!(s.posting(&k(1)).unwrap().is_empty());
    }
}
