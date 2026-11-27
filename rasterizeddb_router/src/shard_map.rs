//! The shard map: which shard owns each level-0 block (sharding-plan.md A1,
//! §4.3 B1a.3).
//!
//! A map is a sorted list of `(lo_block, shard_id)` entries covering every
//! level-0 block `[0, 2⁴⁸)`: entry `j` owns `[lo_j, lo_{j+1})`. Unlike a
//! partition's [`RangeMap`](crate::map::RangeMap), one shard may own many
//! ranges. Boundaries are whole blocks, so a shard key value (one block) is
//! never split between shards.
//!
//! Two encodings carry the same content:
//! - a **text** form, one line per item, for files an operator edits;
//! - a **binary** form, length- and checksum-framed, for the wire.
//!
//! Both carry a `digest` over the canonical content, so two parties can
//! compare maps by 8 bytes, and the epoch, which only ever goes up.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use crate::key::{LEVEL0_BITS, ROUTE_V1, block_of};
use crate::xxh64::xxh64;

/// The number of level-0 blocks: `2⁴⁸`.
pub const BLOCKS: u64 = 1 << LEVEL0_BITS;

/// The text form's first line.
pub const TEXT_MAGIC: &str = "rasterizeddb shard map";

/// The binary form's first eight bytes.
pub const BINARY_MAGIC: [u8; 8] = *b"RDBSMAP\0";

/// The encoding's own version (not the routing function's).
pub const FORMAT_V1: u16 = 1;

/// The most shards a map may name.
pub const MAX_SHARDS: usize = 65_536;

/// The most entries a map may hold.
pub const MAX_ENTRIES: usize = 1 << 20;

/// The longest address a shard line may carry.
const MAX_ADDR: usize = 1024;

/// Digest seeds ("RDBSMD" / "RDBSMF").
const DIGEST_SEED: u64 = 0x5244_4253_4D44_0001;
const FRAME_SEED: u64 = 0x5244_4253_4D46_0001;

/// One shard's identity and where to reach it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShardInfo {
    pub id: u32,
    /// Its primary's address, `host:port` (or a unix socket path); empty when
    /// the map only assigns ranges and the addresses come from elsewhere.
    pub addr: String,
    /// Standby addresses, tried in order when the primary is down (Track A's
    /// warm standbys, D7).
    pub standbys: Vec<String>,
}

/// A validated shard map. Construct with [`ShardMap::new`],
/// [`ShardMap::initial`] or a decoder.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShardMap {
    cluster_id: u128,
    epoch: u64,
    route_v: u8,
    /// Sorted by `lo_block`, starting at 0.
    entries: Vec<(u64, u32)>,
    /// Sorted by id; every entry's shard is here.
    shards: Vec<ShardInfo>,
    digest: u64,
}

impl ShardMap {
    /// A map from its parts; refused unless the entries cover every block
    /// with no gap or overlap and name only listed shards.
    pub fn new(
        cluster_id: u128,
        epoch: u64,
        entries: Vec<(u64, u32)>,
        mut shards: Vec<ShardInfo>,
    ) -> Result<ShardMap, String> {
        shards.sort_by_key(|s| s.id);
        let mut map = ShardMap {
            cluster_id,
            epoch,
            route_v: ROUTE_V1,
            entries,
            shards,
            digest: 0,
        };
        map.validate()?;
        map.digest = map.compute_digest();
        Ok(map)
    }

    /// `shards.len()` shards with ids `0..n`, each owning one equal range of
    /// blocks, as `RangeMap::initial` lays out partitions.
    pub fn initial(cluster_id: u128, shards: Vec<ShardInfo>) -> Result<ShardMap, String> {
        let n = shards.len();
        if n == 0 || n > MAX_SHARDS {
            return Err(format!("a shard map has 1 to {MAX_SHARDS} shards, not {n}"));
        }
        let mut ids: Vec<u32> = shards.iter().map(|s| s.id).collect();
        ids.sort_unstable();
        let entries = ids
            .iter()
            .enumerate()
            .map(|(i, &id)| (((i as u128 * BLOCKS as u128) / n as u128) as u64, id))
            .collect();
        ShardMap::new(cluster_id, 1, entries, shards)
    }

    fn validate(&self) -> Result<(), String> {
        if self.route_v != ROUTE_V1 {
            return Err(format!("shard map uses route_v {}, this build knows only {ROUTE_V1}", self.route_v));
        }
        if self.epoch == 0 {
            return Err("shard map epoch must be at least 1".to_string());
        }
        if self.shards.is_empty() || self.shards.len() > MAX_SHARDS {
            return Err(format!("a shard map has 1 to {MAX_SHARDS} shards, not {}", self.shards.len()));
        }
        for w in self.shards.windows(2) {
            if w[0].id == w[1].id {
                return Err(format!("shard map lists shard {} twice", w[0].id));
            }
        }
        for s in &self.shards {
            for a in std::iter::once(&s.addr).chain(&s.standbys) {
                if a.len() > MAX_ADDR || a.chars().any(|c| c.is_whitespace() || c == ',' || c.is_control()) {
                    return Err(format!("shard {} has a malformed address {a:?}", s.id));
                }
            }
            if s.addr.is_empty() && !s.standbys.is_empty() {
                return Err(format!("shard {} has standbys but no primary address", s.id));
            }
        }
        match self.entries.first() {
            None => return Err("shard map has no entries".to_string()),
            Some(&(0, _)) => {}
            Some(&(lo, _)) => return Err(format!("shard map starts at block {lo:#x}, not 0")),
        }
        if self.entries.len() > MAX_ENTRIES {
            return Err(format!("shard map has {} entries, more than {MAX_ENTRIES}", self.entries.len()));
        }
        for w in self.entries.windows(2) {
            if w[1].0 <= w[0].0 {
                return Err(format!(
                    "shard map is not strictly increasing: block {:#x} then {:#x}",
                    w[0].0, w[1].0
                ));
            }
        }
        for &(lo, id) in &self.entries {
            if lo >= BLOCKS {
                return Err(format!("shard map entry at block {lo:#x} is past the last block"));
            }
            if self.shard(id).is_none() {
                return Err(format!("shard map assigns block {lo:#x} to unlisted shard {id}"));
            }
        }
        Ok(())
    }

    pub fn cluster_id(&self) -> u128 {
        self.cluster_id
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    pub fn route_v(&self) -> u8 {
        self.route_v
    }

    /// The xxh64 of the canonical content; equal maps have equal digests.
    pub fn digest(&self) -> u64 {
        self.digest
    }

    pub fn entries(&self) -> &[(u64, u32)] {
        &self.entries
    }

    pub fn shards(&self) -> &[ShardInfo] {
        &self.shards
    }

    pub fn shard(&self, id: u32) -> Option<&ShardInfo> {
        self.shards.binary_search_by_key(&id, |s| s.id).ok().map(|i| &self.shards[i])
    }

    /// The shard owning level-0 block `block` (`< 2⁴⁸`; higher bits ignored).
    #[inline]
    pub fn shard_of_block(&self, block: u64) -> u32 {
        let block = block & (BLOCKS - 1);
        // Entry 0 starts at 0, so at least one entry is <= block.
        let j = self.entries.partition_point(|&(lo, _)| lo <= block) - 1;
        self.entries[j].1
    }

    /// The shard owning routing point `point`.
    #[inline]
    pub fn shard_of_point(&self, point: u64) -> u32 {
        self.shard_of_block(block_of(point))
    }

    /// The block ranges `[lo, hi)` shard `id` owns, in order.
    pub fn ranges_of(&self, id: u32) -> Vec<(u64, u64)> {
        let mut out = Vec::new();
        for (j, &(lo, s)) in self.entries.iter().enumerate() {
            if s == id {
                let hi = self.entries.get(j + 1).map_or(BLOCKS, |e| e.0);
                match out.last_mut() {
                    Some((_, h)) if *h == lo => *h = hi,
                    _ => out.push((lo, hi)),
                }
            }
        }
        out
    }

    /// A copy with `[lo, hi)` reassigned to shard `to`, adjacent entries of
    /// one shard merged, and the epoch raised by one: the edit a tenant move
    /// publishes.
    /// This map with shard `info` added, owning nothing yet, one epoch up:
    /// a split's first step (B6.2).
    pub fn with_shard(&self, info: ShardInfo) -> Result<ShardMap, String> {
        if self.shard(info.id).is_some() {
            return Err(format!("shard {} is in the map already", info.id));
        }
        let mut shards = self.shards.clone();
        shards.push(info);
        ShardMap::new(self.cluster_id, self.epoch + 1, self.entries.clone(), shards)
    }

    pub fn reassign(&self, lo: u64, hi: u64, to: u32) -> Result<ShardMap, String> {
        if lo >= hi || hi > BLOCKS {
            return Err(format!("block range [{lo:#x}, {hi:#x}) is empty or past the end"));
        }
        if self.shard(to).is_none() {
            return Err(format!("shard {to} is not in the map"));
        }
        // Owner at every boundary, as a step function.
        let mut steps: BTreeMap<u64, u32> = self.entries.iter().copied().collect();
        let after = if hi < BLOCKS { Some(self.shard_of_block(hi)) } else { None };
        steps.retain(|&b, _| b < lo || b >= hi);
        steps.insert(lo, to);
        if let Some(owner) = after {
            steps.entry(hi).or_insert(owner);
        }
        let mut entries: Vec<(u64, u32)> = Vec::with_capacity(steps.len());
        for (b, s) in steps {
            if entries.last().is_some_and(|&(_, p)| p == s) {
                continue;
            }
            entries.push((b, s));
        }
        ShardMap::new(self.cluster_id, self.epoch + 1, entries, self.shards.clone())
    }

    fn compute_digest(&self) -> u64 {
        // The canonical bytes: the binary body without its frame.
        xxh64(&self.body(), DIGEST_SEED)
    }

    /// Canonical body: everything but the magic, the digest and the frame
    /// checksum.
    fn body(&self) -> Vec<u8> {
        let mut b = Vec::with_capacity(32 + self.entries.len() * 12 + self.shards.len() * 32);
        b.extend_from_slice(&self.cluster_id.to_le_bytes());
        b.extend_from_slice(&self.epoch.to_le_bytes());
        b.push(self.route_v);
        b.extend_from_slice(&(self.shards.len() as u32).to_le_bytes());
        for s in &self.shards {
            b.extend_from_slice(&s.id.to_le_bytes());
            put_str(&mut b, &s.addr);
            b.extend_from_slice(&(s.standbys.len() as u16).to_le_bytes());
            for a in &s.standbys {
                put_str(&mut b, a);
            }
        }
        b.extend_from_slice(&(self.entries.len() as u32).to_le_bytes());
        for &(lo, id) in &self.entries {
            b.extend_from_slice(&lo.to_le_bytes());
            b.extend_from_slice(&id.to_le_bytes());
        }
        b
    }

    /// `magic ‖ format:u16 ‖ body_len:u32 ‖ body ‖ digest:u64 ‖ check:u64`,
    /// all little-endian; `check` is xxh64 of everything before it.
    pub fn to_binary(&self) -> Vec<u8> {
        let body = self.body();
        let mut out = Vec::with_capacity(body.len() + 30);
        out.extend_from_slice(&BINARY_MAGIC);
        out.extend_from_slice(&FORMAT_V1.to_le_bytes());
        out.extend_from_slice(&(body.len() as u32).to_le_bytes());
        out.extend_from_slice(&body);
        out.extend_from_slice(&self.digest.to_le_bytes());
        let check = xxh64(&out, FRAME_SEED);
        out.extend_from_slice(&check.to_le_bytes());
        out
    }

    pub fn from_binary(bytes: &[u8]) -> Result<ShardMap, String> {
        let mut r = Reader { b: bytes, at: 0 };
        if r.take(8)? != BINARY_MAGIC {
            return Err("not a binary shard map".to_string());
        }
        let format = r.u16()?;
        if format != FORMAT_V1 {
            return Err(format!("shard map format {format} is not known to this build"));
        }
        let body_len = r.u32()? as usize;
        let body_start = r.at;
        let body_end = body_start.checked_add(body_len).filter(|&e| e + 16 == bytes.len());
        let Some(body_end) = body_end else {
            return Err("binary shard map has the wrong length".to_string());
        };
        let check = u64::from_le_bytes(bytes[body_end + 8..].try_into().unwrap());
        if xxh64(&bytes[..body_end + 8], FRAME_SEED) != check {
            return Err("binary shard map failed its checksum".to_string());
        }
        let mut b = Reader { b: &bytes[..body_end], at: body_start };
        let cluster_id = u128::from_le_bytes(b.take(16)?.try_into().unwrap());
        let epoch = b.u64()?;
        let route_v = b.take(1)?[0];
        let n_shards = b.u32()? as usize;
        if n_shards > MAX_SHARDS {
            return Err(format!("binary shard map names {n_shards} shards"));
        }
        let mut shards = Vec::with_capacity(n_shards);
        for _ in 0..n_shards {
            let id = b.u32()?;
            let addr = b.str()?;
            let n = b.u16()? as usize;
            let mut standbys = Vec::with_capacity(n.min(16));
            for _ in 0..n {
                standbys.push(b.str()?);
            }
            shards.push(ShardInfo { id, addr, standbys });
        }
        let n_entries = b.u32()? as usize;
        if n_entries > MAX_ENTRIES {
            return Err(format!("binary shard map has {n_entries} entries"));
        }
        let mut entries = Vec::with_capacity(n_entries);
        for _ in 0..n_entries {
            entries.push((b.u64()?, b.u32()?));
        }
        if b.at != body_end {
            return Err("binary shard map has trailing bytes in its body".to_string());
        }
        let digest = u64::from_le_bytes(bytes[body_end..body_end + 8].try_into().unwrap());
        let map = ShardMap::parts(cluster_id, epoch, route_v, entries, shards)?;
        if map.digest != digest {
            return Err("binary shard map's digest does not match its content".to_string());
        }
        Ok(map)
    }

    fn parts(
        cluster_id: u128,
        epoch: u64,
        route_v: u8,
        entries: Vec<(u64, u32)>,
        mut shards: Vec<ShardInfo>,
    ) -> Result<ShardMap, String> {
        shards.sort_by_key(|s| s.id);
        let mut map = ShardMap {
            cluster_id,
            epoch,
            route_v,
            entries,
            shards,
            digest: 0,
        };
        map.validate()?;
        map.digest = map.compute_digest();
        Ok(map)
    }

    /// The text form:
    ///
    /// ```text
    /// rasterizeddb shard map
    /// format 1
    /// cluster 0123…(32 hex)
    /// epoch 7
    /// route_v 1
    /// shard 0 10.0.0.1:5433 10.0.0.2:5433
    /// shard 1 10.0.0.3:5433
    /// range 0x0 0
    /// range 0x800000000000 1
    /// digest 89ab…(16 hex)
    /// ```
    ///
    /// `#` starts a comment; blank lines are ignored. `digest` is checked
    /// when present, so a hand edit must drop or recompute it.
    pub fn to_text(&self) -> String {
        let mut s = String::new();
        let _ = writeln!(s, "{TEXT_MAGIC}");
        let _ = writeln!(s, "format {FORMAT_V1}");
        let _ = writeln!(s, "cluster {:032x}", self.cluster_id);
        let _ = writeln!(s, "epoch {}", self.epoch);
        let _ = writeln!(s, "route_v {}", self.route_v);
        for sh in &self.shards {
            let _ = write!(s, "shard {}", sh.id);
            if !sh.addr.is_empty() {
                let _ = write!(s, " {}", sh.addr);
            }
            for a in &sh.standbys {
                let _ = write!(s, " {a}");
            }
            s.push('\n');
        }
        for &(lo, id) in &self.entries {
            let _ = writeln!(s, "range {lo:#x} {id}");
        }
        let _ = writeln!(s, "digest {:016x}", self.digest);
        s
    }

    pub fn from_text(text: &str) -> Result<ShardMap, String> {
        let mut lines = text
            .lines()
            .enumerate()
            .map(|(i, l)| (i + 1, l.split('#').next().unwrap_or("").trim()))
            .filter(|(_, l)| !l.is_empty());
        match lines.next() {
            Some((_, l)) if l == TEXT_MAGIC => {}
            _ => return Err(format!("not a shard map: the first line must be {TEXT_MAGIC:?}")),
        }
        let (mut format, mut cluster, mut epoch, mut route_v, mut digest) = (None, None, None, None, None);
        let mut shards = Vec::new();
        let mut entries = Vec::new();
        for (n, line) in lines {
            let mut words = line.split_whitespace();
            let key = words.next().unwrap_or("");
            let bad = |what: &str| format!("shard map line {n}: {what}: {line:?}");
            let one = |w: &mut std::str::SplitWhitespace| -> Result<String, String> {
                let v = w.next().ok_or_else(|| bad("missing value"))?.to_string();
                if w.next().is_some() {
                    return Err(bad("too many values"));
                }
                Ok(v)
            };
            match key {
                "format" => {
                    format = Some(one(&mut words)?.parse::<u16>().map_err(|_| bad("bad format"))?);
                }
                "cluster" => {
                    cluster = Some(u128::from_str_radix(&one(&mut words)?, 16).map_err(|_| bad("bad cluster id"))?);
                }
                "epoch" => epoch = Some(one(&mut words)?.parse::<u64>().map_err(|_| bad("bad epoch"))?),
                "route_v" => route_v = Some(one(&mut words)?.parse::<u8>().map_err(|_| bad("bad route_v"))?),
                "digest" => {
                    digest = Some(u64::from_str_radix(&one(&mut words)?, 16).map_err(|_| bad("bad digest"))?);
                }
                "shard" => {
                    let id = words
                        .next()
                        .and_then(|w| w.parse::<u32>().ok())
                        .ok_or_else(|| bad("bad shard id"))?;
                    let addr = words.next().unwrap_or("").to_string();
                    let standbys = words.map(str::to_string).collect();
                    shards.push(ShardInfo { id, addr, standbys });
                }
                "range" => {
                    let lo = words.next().ok_or_else(|| bad("missing block"))?;
                    let lo = match lo.strip_prefix("0x") {
                        Some(h) => u64::from_str_radix(h, 16),
                        None => lo.parse::<u64>(),
                    }
                    .map_err(|_| bad("bad block"))?;
                    let id = one(&mut words)?.parse::<u32>().map_err(|_| bad("bad shard id"))?;
                    entries.push((lo, id));
                }
                _ => return Err(bad("unknown key")),
            }
        }
        if format.unwrap_or(FORMAT_V1) != FORMAT_V1 {
            return Err(format!("shard map format {} is not known to this build", format.unwrap()));
        }
        let cluster = cluster.ok_or("shard map has no cluster line")?;
        let epoch = epoch.ok_or("shard map has no epoch line")?;
        let map = ShardMap::parts(cluster, epoch, route_v.unwrap_or(ROUTE_V1), entries, shards)?;
        if let Some(d) = digest
            && d != map.digest
        {
            return Err(format!(
                "shard map digest {d:016x} does not match its content ({:016x}); drop or recompute the digest line after an edit",
                map.digest
            ));
        }
        Ok(map)
    }
}

fn put_str(b: &mut Vec<u8>, s: &str) {
    b.extend_from_slice(&(s.len() as u16).to_le_bytes());
    b.extend_from_slice(s.as_bytes());
}

struct Reader<'a> {
    b: &'a [u8],
    at: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], String> {
        let end = self.at.checked_add(n).filter(|&e| e <= self.b.len());
        let Some(end) = end else {
            return Err("binary shard map is truncated".to_string());
        };
        let s = &self.b[self.at..end];
        self.at = end;
        Ok(s)
    }
    fn u16(&mut self) -> Result<u16, String> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> Result<u32, String> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64, String> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn str(&mut self) -> Result<String, String> {
        let n = self.u16()? as usize;
        if n > MAX_ADDR {
            return Err("binary shard map has an over-long address".to_string());
        }
        String::from_utf8(self.take(n)?.to_vec()).map_err(|_| "binary shard map has a non-UTF-8 address".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::key::{KeyFamily, KeyValue, shard_block};

    fn shards(n: u32) -> Vec<ShardInfo> {
        (0..n)
            .map(|i| ShardInfo {
                id: i,
                addr: format!("10.0.0.{i}:5433"),
                standbys: if i % 2 == 0 { vec![format!("10.0.1.{i}:5433")] } else { vec![] },
            })
            .collect()
    }

    #[test]
    fn initial_covers_every_block_evenly() {
        for n in [1u32, 2, 3, 7, 64, 1000] {
            let m = ShardMap::initial(0xABCD, shards(n)).unwrap();
            assert_eq!(m.entries().len(), n as usize);
            assert_eq!(m.shard_of_block(0), 0);
            assert_eq!(m.shard_of_block(BLOCKS - 1), n - 1);
            let total: u64 = (0..n).flat_map(|i| m.ranges_of(i)).map(|(lo, hi)| hi - lo).sum();
            assert_eq!(total, BLOCKS);
        }
        assert!(ShardMap::initial(1, vec![]).is_err());
    }

    #[test]
    fn both_encodings_round_trip_and_agree_on_the_digest() {
        let m = ShardMap::initial(0x1234_5678_9ABC_DEF0_0FED_CBA9_8765_4321, shards(5))
            .unwrap()
            .reassign(10, 20, 3)
            .unwrap();
        let t = m.to_text();
        let b = m.to_binary();
        let from_t = ShardMap::from_text(&t).unwrap();
        let from_b = ShardMap::from_binary(&b).unwrap();
        assert_eq!(from_t, m);
        assert_eq!(from_b, m);
        assert_eq!(from_t.digest(), from_b.digest());
        // A digestless text is accepted and gets the same digest.
        let no_digest: String = t.lines().filter(|l| !l.starts_with("digest")).map(|l| format!("{l}\n")).collect();
        assert_eq!(ShardMap::from_text(&no_digest).unwrap().digest(), m.digest());
    }

    #[test]
    fn corruption_is_refused() {
        let m = ShardMap::initial(9, shards(3)).unwrap();
        let b = m.to_binary();
        for i in 0..b.len() {
            let mut c = b.clone();
            c[i] ^= 0x40;
            assert!(ShardMap::from_binary(&c).is_err(), "flip at {i} accepted");
        }
        for n in 0..b.len() {
            assert!(ShardMap::from_binary(&b[..n]).is_err(), "truncation to {n} accepted");
        }
        let t = m.to_text().replace("range 0x0 0", "range 0x0 1");
        assert!(ShardMap::from_text(&t).unwrap_err().contains("digest"));
    }

    #[test]
    fn malformed_maps_are_refused() {
        let s = shards(2);
        assert!(ShardMap::new(1, 1, vec![], s.clone()).is_err());
        assert!(ShardMap::new(1, 1, vec![(1, 0)], s.clone()).is_err(), "must start at 0");
        assert!(ShardMap::new(1, 1, vec![(0, 0), (0, 1)], s.clone()).is_err(), "duplicate lo");
        assert!(ShardMap::new(1, 1, vec![(0, 0), (BLOCKS, 1)], s.clone()).is_err(), "past the end");
        assert!(ShardMap::new(1, 1, vec![(0, 7)], s.clone()).is_err(), "unlisted shard");
        assert!(ShardMap::new(1, 0, vec![(0, 0)], s.clone()).is_err(), "epoch 0");
        let mut dup = s.clone();
        dup.push(s[0].clone());
        assert!(ShardMap::new(1, 1, vec![(0, 0)], dup).is_err(), "shard listed twice");
        let mut bad = s.clone();
        bad[0].addr = "a b".into();
        assert!(ShardMap::new(1, 1, vec![(0, 0)], bad).is_err(), "space in address");
        // One shard may own many ranges.
        assert!(ShardMap::new(1, 1, vec![(0, 0), (5, 1), (9, 0)], s).is_ok());
        assert!(ShardMap::from_text("nope").is_err());
        assert!(ShardMap::from_text(&format!("{TEXT_MAGIC}\nepoch 1\n")).is_err(), "no cluster");
    }

    #[test]
    fn b62_a_split_adds_a_shard_then_moves_half_a_range_to_it() {
        let m = ShardMap::initial(1, shards(2)).unwrap();
        let grown = m.with_shard(ShardInfo { id: 7, addr: "x:1".into(), standbys: vec![] }).unwrap();
        assert_eq!(grown.epoch(), m.epoch() + 1);
        assert!(grown.ranges_of(7).is_empty(), "joins owning nothing");
        assert_eq!(grown.entries(), m.entries());
        assert!(grown.with_shard(ShardInfo { id: 7, addr: "y:1".into(), standbys: vec![] }).is_err());
        let (lo, hi) = grown.ranges_of(0)[0];
        let mid = lo + (hi - lo) / 2;
        let split = grown.reassign(mid, hi, 7).unwrap();
        assert_eq!(split.ranges_of(7), vec![(mid, hi)]);
        assert_eq!(split.ranges_of(0), vec![(lo, mid)]);
    }

    #[test]
    fn reassign_moves_one_range_and_raises_the_epoch() {
        let m = ShardMap::initial(1, shards(2)).unwrap();
        let half = BLOCKS / 2;
        let k = shard_block(KeyValue::Int(42), KeyFamily::Integer).unwrap();
        let owner = m.shard_of_block(k);
        let to = 1 - owner;
        let moved = m.reassign(k, k + 1, to).unwrap();
        assert_eq!(moved.epoch(), m.epoch() + 1);
        assert_eq!(moved.shard_of_block(k), to);
        assert_eq!(moved.shard_of_block(k.wrapping_sub(1) & (BLOCKS - 1)), m.shard_of_block(k.wrapping_sub(1)));
        assert_eq!(moved.shard_of_block(k + 1), m.shard_of_block(k + 1));
        assert_ne!(moved.digest(), m.digest());
        // Moving it back merges the entries again.
        let back = moved.reassign(k, k + 1, owner).unwrap();
        assert_eq!(back.entries(), m.entries());
        // Whole halves.
        let all0 = m.reassign(half, BLOCKS, 0).unwrap();
        assert_eq!(all0.entries(), &[(0, 0)]);
        assert!(m.reassign(5, 5, 0).is_err());
        assert!(m.reassign(0, 1, 9).is_err());
        // Every point of a block goes to the block's shard.
        let p = k << 16;
        assert_eq!(moved.shard_of_point(p), to);
        assert_eq!(moved.shard_of_point(p | 0xFFFF), to);
    }
}
