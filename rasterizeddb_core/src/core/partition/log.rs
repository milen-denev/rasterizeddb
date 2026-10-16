//! The partition map log: its record codec and replay (§3.2, §1.1, §16 I5).
//! Pure: no file IO and no WAL calls. The caller appends [`encode`]d records
//! and hands the bytes it reads back to [`replay`].
//!
//! # Framing
//!
//! `len:u32 LE ‖ crc32:u32 LE ‖ kind:u8 ‖ payload`. `len` counts `kind ‖
//! payload` (1 to [`MAX_RECORD_LEN`]) and the CRC (IEEE, `crc32fast`) covers
//! the same bytes. Every payload opens with its layout version, [`LAYOUT_V1`].
//! Integers are little-endian. `opt(T)` is a flag byte (0 or 1) then `T`,
//! which must be zero when the flag is 0, so every field has a fixed width.
//!
//! | kind | record | payload after the layout byte |
//! |---|---|---|
//! | 0x01 | HEADER | uid u64, route_v u16, n u8, n × (write_order u32, family u8), w u8, w × width u8, co-location group u64 (0 = none) |
//! | 0x02 | LOCATIONS | n u32, n × (len u32, UTF-8 path): the whole interned list |
//! | 0x03 | PLACE | hi u128, n u32, n × (lo u64, pid u32) |
//! | 0x04 | MATERIALIZE | pid u32, floor u64, loc u32 |
//! | 0x05 | DIRTY | pid u32, floor u64 |
//! | 0x06 | SWEPT | pid u32, swept_to u64, clean u8 |
//! | 0x07 | STATS | pid u32, rows u64, bytes u64 |
//! | 0x08 | RESHAPE_BEGIN | id u64, n u32, n × source pid u32, m u32, m × (lo u64, pid u32, loc u32) |
//! | 0x09 | RESHAPE_COMMIT | id u64, m u32, m × (pid u32, opt(pin u64)) |
//! | 0x0A | RESHAPE_ABORT | id u64 |
//! | 0x0B | RETIRED_GONE | n u32, n × pid u32 |
//! | 0x0C | DROPPED | none |
//! | 0x0D | SNAPSHOT | epoch u64, next_pid u64, last_reshape u64, n u32, n × (lo u64, pid u32), p u32, p × (pid u32, retired u8, opt(loc u32), opt(pin u64), opt(rows u64, bytes u64)), r u32, r × RESHAPE_BEGIN's fields |
//! | 0x0E | SEALED | none |
//!
//! # What each record does to the state
//!
//! - `HEADER` comes first and only once. Its `route_v` must be one this binary
//!   routes with, and its widths those `route_v` prescribes for the arity.
//! - `LOCATIONS` carries the whole interned list, which must extend the
//!   current one: an index, once handed out, never moves.
//! - `PLACE { hi, entries }` replaces every entry covering `[entries[0].lo,
//!   hi)` with `entries`. Both ends must already be partition boundaries (the
//!   first `PLACE` must span the whole space), every new pid must be fresh,
//!   and a replaced partition must hold no files and be no reshape's source:
//!   rows move only by reshape. Replaced pids leave the state outright.
//! - `MATERIALIZE` gives a live partition its location and sets its pin to
//!   `floor`; at most once per pid.
//! - `DIRTY` sets the pin of a clean, live, materialized partition to its
//!   floor, which can be above the last `SWEPT` floor §7.2's restart rule
//!   names but is sound by its first-write rule: every stamp after a clean
//!   `SWEPT` comes from a writer whose id is at least `forgotten_below()` at
//!   that moment. Otherwise it lowers the pin to the minimum, which is never
//!   higher than the last `SWEPT` or `MATERIALIZE` floor.
//! - `SWEPT` sets the pin to `swept_to`, or clears it when `clean`.
//! - `STATS` replaces a materialized partition's row and byte counts.
//! - `RESHAPE_BEGIN` names live sources, contiguous and in no other open
//!   reshape, and fresh targets that start at the sources' first point and
//!   tile their span. The target pids are spent at once.
//! - `RESHAPE_COMMIT` lists the targets' pids in `BEGIN` order with each
//!   one's inherited pin, at or below the lowest pin of the sources it
//!   overlaps: the targets hold the sources' raw-copied aborted stamps
//!   (§6.2, §7.2), and a higher pin would let them read as committed once
//!   the sources are gone. It swaps the sources' entries for the targets',
//!   retires the sources (they keep their location and pin until
//!   `RETIRED_GONE`) and materializes the targets at their `BEGIN` locations.
//! - `RESHAPE_ABORT` forgets an open reshape; its target pids stay spent.
//! - `RETIRED_GONE` removes retired partitions whose files are deleted.
//! - `SNAPSHOT` sets the whole state, and only opens a generation: it must
//!   follow `HEADER` and `LOCATIONS` with no `PLACE` before it.
//! - `SEALED` marks a compacted generation complete; once only.
//! - `DROPPED` ends the log: any record after it is an error.
//!
//! `PLACE` and `RESHAPE_COMMIT` start a new epoch, and every epoch's map
//! passes [`RangeMap::validate`].
//!
//! # Replay
//!
//! [`replay`] applies records in order and stops at the first whose framing
//! fails: too short to hold its header, an implausible length, running past
//! the end, or a CRC mismatch. When no intact record starts anywhere after
//! it (a partial record, or the zeros of a file a crash extended), that is a
//! torn tail; [`Replay::torn`] says where, and the caller truncates to
//! [`Replay::valid_len`] before appending again and does not reap on a start
//! that saw one (§12). An intact record after the damage means the log lost
//! a record from its middle, which a crash does not do: dropping what follows
//! could lose a `MATERIALIZE` whose partition then looks unwritten and has its
//! files deleted (§12), so the table is refused instead. (A crash whose
//! unsynced pages land out of order can look the same; it is refused too.)
//! A record that passes its
//! CRC but cannot be understood (unknown kind or layout version, a malformed
//! payload) or does not apply to the state is an error: the table is refused,
//! never half-read.

use std::collections::{BTreeMap, HashSet};

use super::key::{KeyFamily, MAX_ARITY};
use super::map::{PointRange, RangeMap, SPACE_END};

/// The only routing version this binary knows (§1.3).
pub const ROUTE_V1: u16 = 1;

/// The payload layout every record kind is written in.
pub const LAYOUT_V1: u8 = 1;

/// `len ‖ crc32`.
pub const FRAME_LEN: usize = 8;

/// The largest `kind ‖ payload` a record may have. A snapshot of 65,536
/// partitions is about 3 MB; a larger length is a corrupt one.
pub const MAX_RECORD_LEN: u32 = 64 << 20;

/// A record's kind byte.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Kind {
    Header = 0x01,
    Locations = 0x02,
    Place = 0x03,
    Materialize = 0x04,
    Dirty = 0x05,
    Swept = 0x06,
    Stats = 0x07,
    ReshapeBegin = 0x08,
    ReshapeCommit = 0x09,
    ReshapeAbort = 0x0A,
    RetiredGone = 0x0B,
    Dropped = 0x0C,
    Snapshot = 0x0D,
    Sealed = 0x0E,
}

impl Kind {
    const ALL: [Kind; 14] = [
        Kind::Header,
        Kind::Locations,
        Kind::Place,
        Kind::Materialize,
        Kind::Dirty,
        Kind::Swept,
        Kind::Stats,
        Kind::ReshapeBegin,
        Kind::ReshapeCommit,
        Kind::ReshapeAbort,
        Kind::RetiredGone,
        Kind::Dropped,
        Kind::Snapshot,
        Kind::Sealed,
    ];

    pub fn from_tag(tag: u8) -> Option<Kind> {
        Self::ALL.into_iter().find(|k| *k as u8 == tag)
    }

    pub fn name(self) -> &'static str {
        match self {
            Kind::Header => "HEADER",
            Kind::Locations => "LOCATIONS",
            Kind::Place => "PLACE",
            Kind::Materialize => "MATERIALIZE",
            Kind::Dirty => "DIRTY",
            Kind::Swept => "SWEPT",
            Kind::Stats => "STATS",
            Kind::ReshapeBegin => "RESHAPE_BEGIN",
            Kind::ReshapeCommit => "RESHAPE_COMMIT",
            Kind::ReshapeAbort => "RESHAPE_ABORT",
            Kind::RetiredGone => "RETIRED_GONE",
            Kind::Dropped => "DROPPED",
            Kind::Snapshot => "SNAPSHOT",
            Kind::Sealed => "SEALED",
        }
    }
}

/// Widths `route_v1` gives each level; `KeySpec::widths` must agree (tested).
fn route_v1_widths(arity: usize) -> &'static [u8] {
    match arity {
        1 => &[48],
        2 => &[48, 16],
        3 => &[48, 8, 8],
        _ => &[],
    }
}

fn family_from_tag(tag: u8) -> Option<KeyFamily> {
    [KeyFamily::Integer, KeyFamily::Uuid, KeyFamily::Text]
        .into_iter()
        .find(|f| f.tag() == tag)
}

/// What a table routes by. The caller checks `key` against the schema.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MapHeader {
    /// Nonzero; names the table's files.
    pub uid: u64,
    pub route_v: u16,
    /// `(write_order, family)` per level, leading level first.
    pub key: Vec<(u32, KeyFamily)>,
    /// Bits of the point each level fills.
    pub widths: Vec<u8>,
    /// The co-location group's uid, 0 for none.
    pub colocation: u64,
}

impl MapHeader {
    pub fn validate(&self) -> Result<(), String> {
        if self.uid == 0 {
            return Err("partition map header has uid 0".to_string());
        }
        if self.route_v != ROUTE_V1 {
            return Err(format!(
                "partitioned table {:016x} routes with route_v{}; this binary knows only route_v{ROUTE_V1}",
                self.uid, self.route_v
            ));
        }
        if self.key.is_empty() || self.key.len() > MAX_ARITY {
            return Err(format!(
                "partition map header has {} key columns, not 1 to {MAX_ARITY}",
                self.key.len()
            ));
        }
        for (i, (wo, _)) in self.key.iter().enumerate() {
            if self.key[..i].iter().any(|(w, _)| w == wo) {
                return Err(format!("partition map header names key column {wo} twice"));
            }
        }
        let want = route_v1_widths(self.key.len());
        if self.widths.as_slice() != want {
            return Err(format!(
                "partition map header has widths {:?}; route_v1 with {} key columns uses {want:?}",
                self.widths,
                self.key.len()
            ));
        }
        Ok(())
    }
}

/// One new partition of a reshape.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReshapeTarget {
    pub lo: u64,
    pub pid: u32,
    /// Index into the interned locations.
    pub loc: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReshapeBegin {
    pub id: u64,
    pub sources: Vec<u32>,
    /// Sorted by `lo`, tiling the sources' span.
    pub targets: Vec<ReshapeTarget>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PartStats {
    pub rows: u64,
    pub bytes: u64,
}

/// What the log says about one partition.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PartState {
    /// Replaced by a reshape; its files remain until `RETIRED_GONE`.
    pub retired: bool,
    /// Where its files are; `None` until it is materialized.
    pub location: Option<u32>,
    /// Unswept aborted stamps may exist at or above this (§7.2); `None` is
    /// clean, or never written.
    pub pin: Option<u64>,
    pub stats: Option<PartStats>,
}

/// The whole state, for opening a compacted generation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Snapshot {
    pub epoch: u64,
    pub next_pid: u64,
    pub last_reshape: u64,
    pub entries: Vec<(u64, u32)>,
    /// Sorted by pid.
    pub parts: Vec<(u32, PartState)>,
    /// Open reshapes, sorted by id.
    pub reshapes: Vec<ReshapeBegin>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Record {
    Header(MapHeader),
    Locations(Vec<String>),
    Place { hi: u128, entries: Vec<(u64, u32)> },
    Materialize { pid: u32, floor: u64, loc: u32 },
    Dirty { pid: u32, floor: u64 },
    Swept { pid: u32, swept_to: u64, clean: bool },
    Stats { pid: u32, rows: u64, bytes: u64 },
    ReshapeBegin(ReshapeBegin),
    ReshapeCommit { id: u64, pins: Vec<(u32, Option<u64>)> },
    ReshapeAbort { id: u64 },
    RetiredGone(Vec<u32>),
    Dropped,
    Snapshot(Snapshot),
    Sealed,
}

impl Record {
    pub fn kind(&self) -> Kind {
        match self {
            Record::Header(_) => Kind::Header,
            Record::Locations(_) => Kind::Locations,
            Record::Place { .. } => Kind::Place,
            Record::Materialize { .. } => Kind::Materialize,
            Record::Dirty { .. } => Kind::Dirty,
            Record::Swept { .. } => Kind::Swept,
            Record::Stats { .. } => Kind::Stats,
            Record::ReshapeBegin(_) => Kind::ReshapeBegin,
            Record::ReshapeCommit { .. } => Kind::ReshapeCommit,
            Record::ReshapeAbort { .. } => Kind::ReshapeAbort,
            Record::RetiredGone(_) => Kind::RetiredGone,
            Record::Dropped => Kind::Dropped,
            Record::Snapshot(_) => Kind::Snapshot,
            Record::Sealed => Kind::Sealed,
        }
    }
}

// ---------------------------------------------------------------- encoding

struct W(Vec<u8>);

impl W {
    fn u8(&mut self, v: u8) {
        self.0.push(v);
    }
    fn u16(&mut self, v: u16) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    fn u32(&mut self, v: u32) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    fn u64(&mut self, v: u64) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    fn u128(&mut self, v: u128) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    fn count(&mut self, n: usize) -> Result<(), String> {
        let n = u32::try_from(n).map_err(|_| format!("{n} items do not fit a map log record"))?;
        self.u32(n);
        Ok(())
    }
    fn opt_u32(&mut self, v: Option<u32>) {
        self.u8(v.is_some() as u8);
        self.u32(v.unwrap_or(0));
    }
    fn opt_u64(&mut self, v: Option<u64>) {
        self.u8(v.is_some() as u8);
        self.u64(v.unwrap_or(0));
    }
    fn reshape_begin(&mut self, b: &ReshapeBegin) -> Result<(), String> {
        self.u64(b.id);
        self.count(b.sources.len())?;
        for &pid in &b.sources {
            self.u32(pid);
        }
        self.count(b.targets.len())?;
        for t in &b.targets {
            self.u64(t.lo);
            self.u32(t.pid);
            self.u32(t.loc);
        }
        Ok(())
    }
}

/// `rec`, framed and ready to append. Refuses a record [`replay`] would read
/// as torn.
pub fn encode(rec: &Record) -> Result<Vec<u8>, String> {
    let mut w = W(vec![0; FRAME_LEN]);
    w.u8(rec.kind() as u8);
    w.u8(LAYOUT_V1);
    match rec {
        Record::Header(h) => {
            w.u64(h.uid);
            w.u16(h.route_v);
            w.u8(u8::try_from(h.key.len()).map_err(|_| "too many key columns".to_string())?);
            for &(wo, family) in &h.key {
                w.u32(wo);
                w.u8(family.tag());
            }
            w.u8(u8::try_from(h.widths.len()).map_err(|_| "too many widths".to_string())?);
            for &width in &h.widths {
                w.u8(width);
            }
            w.u64(h.colocation);
        }
        Record::Locations(paths) => {
            w.count(paths.len())?;
            for p in paths {
                w.count(p.len())?;
                w.0.extend_from_slice(p.as_bytes());
            }
        }
        Record::Place { hi, entries } => {
            w.u128(*hi);
            w.count(entries.len())?;
            for &(lo, pid) in entries {
                w.u64(lo);
                w.u32(pid);
            }
        }
        Record::Materialize { pid, floor, loc } => {
            w.u32(*pid);
            w.u64(*floor);
            w.u32(*loc);
        }
        Record::Dirty { pid, floor } => {
            w.u32(*pid);
            w.u64(*floor);
        }
        Record::Swept { pid, swept_to, clean } => {
            w.u32(*pid);
            w.u64(*swept_to);
            w.u8(*clean as u8);
        }
        Record::Stats { pid, rows, bytes } => {
            w.u32(*pid);
            w.u64(*rows);
            w.u64(*bytes);
        }
        Record::ReshapeBegin(b) => w.reshape_begin(b)?,
        Record::ReshapeCommit { id, pins } => {
            w.u64(*id);
            w.count(pins.len())?;
            for &(pid, pin) in pins {
                w.u32(pid);
                w.opt_u64(pin);
            }
        }
        Record::ReshapeAbort { id } => w.u64(*id),
        Record::RetiredGone(pids) => {
            w.count(pids.len())?;
            for &pid in pids {
                w.u32(pid);
            }
        }
        Record::Dropped | Record::Sealed => {}
        Record::Snapshot(s) => {
            w.u64(s.epoch);
            w.u64(s.next_pid);
            w.u64(s.last_reshape);
            w.count(s.entries.len())?;
            for &(lo, pid) in &s.entries {
                w.u64(lo);
                w.u32(pid);
            }
            w.count(s.parts.len())?;
            for (pid, p) in &s.parts {
                w.u32(*pid);
                w.u8(p.retired as u8);
                w.opt_u32(p.location);
                w.opt_u64(p.pin);
                w.u8(p.stats.is_some() as u8);
                let st = p.stats.unwrap_or(PartStats { rows: 0, bytes: 0 });
                w.u64(st.rows);
                w.u64(st.bytes);
            }
            w.count(s.reshapes.len())?;
            for b in &s.reshapes {
                w.reshape_begin(b)?;
            }
        }
    }
    let mut buf = w.0;
    let body = buf.len() - FRAME_LEN;
    if body > MAX_RECORD_LEN as usize {
        return Err(format!(
            "a {} record of {body} bytes exceeds the map log's {MAX_RECORD_LEN}-byte limit",
            rec.kind().name()
        ));
    }
    let crc = crc32fast::hash(&buf[FRAME_LEN..]);
    buf[0..4].copy_from_slice(&(body as u32).to_le_bytes());
    buf[4..8].copy_from_slice(&crc.to_le_bytes());
    Ok(buf)
}

// ---------------------------------------------------------------- decoding

struct R<'a> {
    b: &'a [u8],
    at: usize,
}

impl<'a> R<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], String> {
        if self.b.len() - self.at < n {
            return Err("payload ends early".to_string());
        }
        let s = &self.b[self.at..self.at + n];
        self.at += n;
        Ok(s)
    }
    fn u8(&mut self) -> Result<u8, String> {
        Ok(self.take(1)?[0])
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
    fn u128(&mut self) -> Result<u128, String> {
        Ok(u128::from_le_bytes(self.take(16)?.try_into().unwrap()))
    }
    fn flag(&mut self) -> Result<bool, String> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            b => Err(format!("flag byte {b:#04x} is neither 0 nor 1")),
        }
    }
    fn absent_is_zero(present: bool, zero: bool) -> Result<(), String> {
        if !present && !zero {
            return Err("an absent value is not zero".to_string());
        }
        Ok(())
    }
    fn opt_u32(&mut self) -> Result<Option<u32>, String> {
        let present = self.flag()?;
        let v = self.u32()?;
        Self::absent_is_zero(present, v == 0)?;
        Ok(present.then_some(v))
    }
    fn opt_u64(&mut self) -> Result<Option<u64>, String> {
        let present = self.flag()?;
        let v = self.u64()?;
        Self::absent_is_zero(present, v == 0)?;
        Ok(present.then_some(v))
    }
    /// A count of items at least `each` bytes long, bounded by what is left,
    /// so a corrupt count cannot ask for a huge allocation.
    fn count(&mut self, each: usize) -> Result<usize, String> {
        let n = self.u32()? as usize;
        if n.saturating_mul(each) > self.b.len() - self.at {
            return Err(format!("count {n} overruns the payload"));
        }
        Ok(n)
    }
    fn reshape_begin(&mut self) -> Result<ReshapeBegin, String> {
        let id = self.u64()?;
        let n = self.count(4)?;
        let sources = (0..n).map(|_| self.u32()).collect::<Result<_, _>>()?;
        let m = self.count(16)?;
        let mut targets = Vec::with_capacity(m);
        for _ in 0..m {
            targets.push(ReshapeTarget {
                lo: self.u64()?,
                pid: self.u32()?,
                loc: self.u32()?,
            });
        }
        Ok(ReshapeBegin { id, sources, targets })
    }
    fn end(&self) -> Result<(), String> {
        match self.b.len() - self.at {
            0 => Ok(()),
            n => Err(format!("{n} bytes after the payload")),
        }
    }
}

/// Decodes one record from its kind byte and payload (layout byte included).
pub fn decode(tag: u8, payload: &[u8]) -> Result<Record, String> {
    let kind = Kind::from_tag(tag).ok_or_else(|| {
        format!("unknown record kind {tag:#04x}: written by a newer binary, or corrupt under a valid CRC")
    })?;
    let mut r = R { b: payload, at: 0 };
    let layout = r.u8()?;
    if layout != LAYOUT_V1 {
        return Err(format!(
            "{} has layout version {layout}; this binary reads only {LAYOUT_V1}",
            kind.name()
        ));
    }
    let rec = match kind {
        Kind::Header => {
            let uid = r.u64()?;
            let route_v = r.u16()?;
            let n = r.u8()? as usize;
            let mut key = Vec::with_capacity(n);
            for _ in 0..n {
                let wo = r.u32()?;
                let tag = r.u8()?;
                let family = family_from_tag(tag).ok_or_else(|| format!("unknown key family {tag:#04x}"))?;
                key.push((wo, family));
            }
            let w = r.u8()? as usize;
            let widths = r.take(w)?.to_vec();
            let colocation = r.u64()?;
            Record::Header(MapHeader {
                uid,
                route_v,
                key,
                widths,
                colocation,
            })
        }
        Kind::Locations => {
            let n = r.count(4)?;
            let mut paths = Vec::with_capacity(n);
            for _ in 0..n {
                let len = r.count(1)?;
                let bytes = r.take(len)?;
                let p = std::str::from_utf8(bytes).map_err(|_| "a location path is not UTF-8".to_string())?;
                paths.push(p.to_string());
            }
            Record::Locations(paths)
        }
        Kind::Place => {
            let hi = r.u128()?;
            let n = r.count(12)?;
            let mut entries = Vec::with_capacity(n);
            for _ in 0..n {
                entries.push((r.u64()?, r.u32()?));
            }
            Record::Place { hi, entries }
        }
        Kind::Materialize => Record::Materialize {
            pid: r.u32()?,
            floor: r.u64()?,
            loc: r.u32()?,
        },
        Kind::Dirty => Record::Dirty {
            pid: r.u32()?,
            floor: r.u64()?,
        },
        Kind::Swept => Record::Swept {
            pid: r.u32()?,
            swept_to: r.u64()?,
            clean: r.flag()?,
        },
        Kind::Stats => Record::Stats {
            pid: r.u32()?,
            rows: r.u64()?,
            bytes: r.u64()?,
        },
        Kind::ReshapeBegin => Record::ReshapeBegin(r.reshape_begin()?),
        Kind::ReshapeCommit => {
            let id = r.u64()?;
            let n = r.count(13)?;
            let mut pins = Vec::with_capacity(n);
            for _ in 0..n {
                pins.push((r.u32()?, r.opt_u64()?));
            }
            Record::ReshapeCommit { id, pins }
        }
        Kind::ReshapeAbort => Record::ReshapeAbort { id: r.u64()? },
        Kind::RetiredGone => {
            let n = r.count(4)?;
            Record::RetiredGone((0..n).map(|_| r.u32()).collect::<Result<_, _>>()?)
        }
        Kind::Dropped => Record::Dropped,
        Kind::Sealed => Record::Sealed,
        Kind::Snapshot => {
            let epoch = r.u64()?;
            let next_pid = r.u64()?;
            let last_reshape = r.u64()?;
            let n = r.count(12)?;
            let mut entries = Vec::with_capacity(n);
            for _ in 0..n {
                entries.push((r.u64()?, r.u32()?));
            }
            let p = r.count(36)?;
            let mut parts = Vec::with_capacity(p);
            for _ in 0..p {
                let pid = r.u32()?;
                let retired = r.flag()?;
                let location = r.opt_u32()?;
                let pin = r.opt_u64()?;
                let has_stats = r.flag()?;
                let rows = r.u64()?;
                let bytes = r.u64()?;
                R::absent_is_zero(has_stats, rows == 0 && bytes == 0)?;
                let stats = has_stats.then_some(PartStats { rows, bytes });
                parts.push((
                    pid,
                    PartState {
                        retired,
                        location,
                        pin,
                        stats,
                    },
                ));
            }
            let m = r.count(16)?;
            let mut reshapes = Vec::with_capacity(m);
            for _ in 0..m {
                reshapes.push(r.reshape_begin()?);
            }
            Record::Snapshot(Snapshot {
                epoch,
                next_pid,
                last_reshape,
                entries,
                parts,
                reshapes,
            })
        }
    };
    r.end()?;
    Ok(rec)
}

enum Frame<'a> {
    End,
    Torn(String),
    Record { tag: u8, payload: &'a [u8], next: usize },
}

fn read_frame(buf: &[u8], at: usize) -> Frame<'_> {
    let rest = &buf[at..];
    if rest.is_empty() {
        return Frame::End;
    }
    if rest.len() < FRAME_LEN {
        return Frame::Torn(format!("{} bytes, too few for a record header", rest.len()));
    }
    let len = u32::from_le_bytes(rest[0..4].try_into().unwrap());
    let crc = u32::from_le_bytes(rest[4..8].try_into().unwrap());
    if len == 0 || len > MAX_RECORD_LEN {
        return Frame::Torn(format!("implausible record length {len}"));
    }
    let len = len as usize;
    if rest.len() - FRAME_LEN < len {
        return Frame::Torn(format!(
            "record of {len} bytes runs past the end ({} left)",
            rest.len() - FRAME_LEN
        ));
    }
    let body = &rest[FRAME_LEN..FRAME_LEN + len];
    if crc32fast::hash(body) != crc {
        return Frame::Torn("CRC mismatch".to_string());
    }
    Frame::Record {
        tag: body[0],
        payload: &body[1..],
        next: at + FRAME_LEN + len,
    }
}

/// The first offset after `at` where an intact record starts: a plausible
/// length that fits, a known kind, [`LAYOUT_V1`], then the CRC. The cheap
/// checks come first, and keep a chance CRC match inside a torn record's own
/// bytes far less likely than real damage.
fn intact_frame_after(buf: &[u8], at: usize) -> Option<usize> {
    (at + 1..buf.len()).find(|&y| {
        let rest = &buf[y..];
        if rest.len() < FRAME_LEN + 2 {
            return false;
        }
        let len = u32::from_le_bytes(rest[0..4].try_into().unwrap());
        if len < 2 || len > MAX_RECORD_LEN || rest.len() - FRAME_LEN < len as usize {
            return false;
        }
        let body = &rest[FRAME_LEN..FRAME_LEN + len as usize];
        Kind::from_tag(body[0]).is_some()
            && body[1] == LAYOUT_V1
            && crc32fast::hash(body) == u32::from_le_bytes(rest[4..8].try_into().unwrap())
    })
}

// ---------------------------------------------------------------- state

/// A partitioned table's map as its log describes it. Built only by
/// [`replay`], then kept current with [`MapState::apply`], so every instance
/// satisfies [`MapState::check`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MapState {
    header: MapHeader,
    locations: Vec<String>,
    map: RangeMap,
    epoch: u64,
    /// Every pid below this was handed out; pids are never reused.
    next_pid: u64,
    /// Reshape ids are never reused either.
    last_reshape: u64,
    /// Live partitions (exactly the map's pids) and retired ones not yet gone.
    parts: BTreeMap<u32, PartState>,
    /// Open reshapes by id, with the span their sources cover.
    reshapes: BTreeMap<u64, (ReshapeBegin, PointRange)>,
    sealed: bool,
    dropped: bool,
}

fn extend_locations(cur: &mut Vec<String>, new: &[String]) -> Result<(), String> {
    if new.len() < cur.len() || new[..cur.len()] != cur[..] {
        return Err("LOCATIONS does not extend the interned list, so indices would move".to_string());
    }
    for (i, p) in new.iter().enumerate().skip(cur.len()) {
        if p.is_empty() {
            return Err(format!("location {i} is an empty path"));
        }
        if new[..i].contains(p) {
            return Err(format!("location {p:?} is interned twice"));
        }
    }
    cur.extend_from_slice(&new[cur.len()..]);
    Ok(())
}

/// The span a `PLACE` covers, `[entries[0].lo, hi)`, with its entries in order.
fn place_span(hi: u128, entries: &[(u64, u32)]) -> Result<PointRange, String> {
    let Some(&(lo, _)) = entries.first() else {
        return Err("PLACE has no entries".to_string());
    };
    if hi > SPACE_END {
        return Err(format!("PLACE ends at {hi:#x}, past the routing space"));
    }
    if entries.windows(2).any(|w| w[1].0 <= w[0].0) {
        return Err("PLACE entries are not strictly increasing".to_string());
    }
    let last = entries[entries.len() - 1].0;
    if last as u128 >= hi {
        return Err(format!("PLACE entry at {last:#018x} is not below its end {hi:#x}"));
    }
    Ok(PointRange { lo, hi })
}

fn check_part(pid: u32, p: &PartState, locations: usize) -> Result<(), String> {
    match p.location {
        Some(l) if l as usize >= locations => {
            Err(format!("partition {pid} is at location {l}, but only {locations} are interned"))
        }
        None if p.pin.is_some() || p.stats.is_some() => {
            Err(format!("partition {pid} has a pin or stats but no files"))
        }
        _ => Ok(()),
    }
}

impl MapState {
    /// The state after the first `PLACE`, which must cover the whole space.
    fn placed(header: MapHeader, locations: Vec<String>, hi: u128, entries: &[(u64, u32)]) -> Result<Self, String> {
        let span = place_span(hi, entries)?;
        if span != PointRange::FULL {
            return Err("the first PLACE does not cover the whole routing space".to_string());
        }
        let map = RangeMap::new(entries.to_vec())?;
        let parts: BTreeMap<u32, PartState> = entries.iter().map(|&(_, pid)| (pid, PartState::default())).collect();
        let next_pid = entries.iter().map(|&(_, pid)| pid as u64 + 1).max().unwrap_or(0);
        Ok(MapState {
            header,
            locations,
            map,
            epoch: 1,
            next_pid,
            last_reshape: 0,
            parts,
            reshapes: BTreeMap::new(),
            sealed: false,
            dropped: false,
        })
    }

    /// The state a `SNAPSHOT` describes, checked whole.
    fn from_snapshot(header: MapHeader, locations: Vec<String>, s: &Snapshot) -> Result<Self, String> {
        if s.epoch == 0 {
            return Err("snapshot at epoch 0".to_string());
        }
        let map = RangeMap::new(s.entries.clone())?;
        let mut parts = BTreeMap::new();
        for (i, (pid, p)) in s.parts.iter().enumerate() {
            if i > 0 && *pid <= s.parts[i - 1].0 {
                return Err("snapshot partitions are not sorted by pid".to_string());
            }
            if *pid as u64 >= s.next_pid {
                return Err(format!("snapshot partition {pid} was never handed out (next pid {})", s.next_pid));
            }
            check_part(*pid, p, locations.len())?;
            parts.insert(*pid, *p);
        }
        for &(_, pid) in map.entries() {
            match parts.get(&pid) {
                Some(p) if !p.retired => {}
                _ => return Err(format!("snapshot map names partition {pid}, which is not live")),
            }
        }
        if parts.values().filter(|p| !p.retired).count() != map.len() {
            return Err("snapshot has live partitions the map does not place".to_string());
        }
        let mut st = MapState {
            header,
            locations,
            map,
            epoch: s.epoch,
            next_pid: s.next_pid,
            last_reshape: s.last_reshape,
            parts,
            reshapes: BTreeMap::new(),
            sealed: false,
            dropped: false,
        };
        let mut targets = HashSet::new();
        for b in &s.reshapes {
            if b.id == 0 || b.id > s.last_reshape || st.reshapes.range(b.id..).next().is_some() {
                return Err(format!("snapshot reshape id {} is out of order", b.id));
            }
            let span = st.reshape_span(b)?;
            for t in &b.targets {
                if t.pid as u64 >= st.next_pid || st.parts.contains_key(&t.pid) || !targets.insert(t.pid) {
                    return Err(format!("snapshot reshape {} target {} is not a spent, unused pid", b.id, t.pid));
                }
            }
            st.reshapes.insert(b.id, (b.clone(), span));
        }
        Ok(st)
    }

    pub fn header(&self) -> &MapHeader {
        &self.header
    }
    pub fn locations(&self) -> &[String] {
        &self.locations
    }
    pub fn map(&self) -> &RangeMap {
        &self.map
    }
    pub fn epoch(&self) -> u64 {
        self.epoch
    }
    /// The next fresh pid; every pid below it was handed out once.
    pub fn next_pid(&self) -> u64 {
        self.next_pid
    }
    pub fn last_reshape(&self) -> u64 {
        self.last_reshape
    }
    pub fn part(&self, pid: u32) -> Option<&PartState> {
        self.parts.get(&pid)
    }
    /// Live and retired partitions, by pid.
    pub fn parts(&self) -> impl Iterator<Item = (u32, &PartState)> {
        self.parts.iter().map(|(&pid, p)| (pid, p))
    }
    /// Open reshapes, by id. After a restart each is rolled back (§12).
    pub fn reshapes(&self) -> impl Iterator<Item = &ReshapeBegin> {
        self.reshapes.values().map(|(b, _)| b)
    }
    pub fn sealed(&self) -> bool {
        self.sealed
    }
    pub fn dropped(&self) -> bool {
        self.dropped
    }
    /// The table's forget pin: the lowest of its partitions', retired ones
    /// included, since readers of an older epoch may still visit them.
    pub fn forget_pin(&self) -> Option<u64> {
        self.parts.values().filter_map(|p| p.pin).min()
    }

    /// The whole state, for a compacted generation's `SNAPSHOT`.
    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            epoch: self.epoch,
            next_pid: self.next_pid,
            last_reshape: self.last_reshape,
            entries: self.map.entries().to_vec(),
            parts: self.parts.iter().map(|(&pid, &p)| (pid, p)).collect(),
            reshapes: self.reshapes.values().map(|(b, _)| b.clone()).collect(),
        }
    }

    /// Re-checks every invariant from scratch, as loading a snapshot of
    /// this state would.
    pub fn check(&self) -> Result<(), String> {
        self.header.validate()?;
        extend_locations(&mut Vec::new(), &self.locations)?;
        let mut rebuilt = MapState::from_snapshot(self.header.clone(), self.locations.clone(), &self.snapshot())?;
        rebuilt.sealed = self.sealed;
        rebuilt.dropped = self.dropped;
        if rebuilt != *self {
            return Err("partition map state disagrees with its own snapshot".to_string());
        }
        Ok(())
    }

    fn part_mut(&mut self, pid: u32, live: bool) -> Result<&mut PartState, String> {
        match self.parts.get_mut(&pid) {
            Some(p) if live && p.retired => Err(format!("partition {pid} is retired")),
            Some(p) => Ok(p),
            None => Err(format!("no partition {pid}")),
        }
    }

    fn materialized_mut(&mut self, pid: u32, live: bool) -> Result<&mut PartState, String> {
        let p = self.part_mut(pid, live)?;
        if p.location.is_none() {
            return Err(format!("partition {pid} has no files"));
        }
        Ok(p)
    }

    /// Indices `[first, end)` of the entries covering `span`, whose ends must
    /// be partition boundaries.
    fn span_indices(&self, span: PointRange) -> Result<(usize, usize), String> {
        let e = self.map.entries();
        let first = e.partition_point(|&(lo, _)| lo < span.lo);
        if e.get(first).map(|&(lo, _)| lo) != Some(span.lo) {
            return Err(format!("{:#018x} is not a partition boundary", span.lo));
        }
        let end = e.partition_point(|&(lo, _)| (lo as u128) < span.hi);
        if span.hi != SPACE_END && e.get(end).map(|&(lo, _)| lo as u128) != Some(span.hi) {
            return Err(format!("{:#x} is not a partition boundary", span.hi));
        }
        Ok((first, end))
    }

    /// Checks `pids` are distinct and never handed out; the new `next_pid`.
    fn fresh(&self, pids: impl Iterator<Item = u32>) -> Result<u64, String> {
        let mut seen = HashSet::new();
        let mut next = self.next_pid;
        for pid in pids {
            if (pid as u64) < self.next_pid {
                return Err(format!(
                    "pid {pid} is not fresh: every pid below {} was handed out",
                    self.next_pid
                ));
            }
            if !seen.insert(pid) {
                return Err(format!("pid {pid} is placed twice"));
            }
            next = next.max(pid as u64 + 1);
        }
        Ok(next)
    }

    fn reshape_source_of(&self, pid: u32) -> Option<u64> {
        self.reshapes
            .values()
            .find(|(b, _)| b.sources.contains(&pid))
            .map(|(b, _)| b.id)
    }

    /// The span `b`'s sources cover, after checking its sources and targets
    /// (not the target pids' freshness, which depends on the caller).
    fn reshape_span(&self, b: &ReshapeBegin) -> Result<PointRange, String> {
        if b.sources.is_empty() || b.targets.is_empty() {
            return Err(format!("reshape {} has no sources or no targets", b.id));
        }
        let mut ranges = Vec::with_capacity(b.sources.len());
        for (i, &pid) in b.sources.iter().enumerate() {
            if b.sources[..i].contains(&pid) {
                return Err(format!("reshape {} names source {pid} twice", b.id));
            }
            match self.parts.get(&pid) {
                Some(p) if !p.retired => {}
                _ => return Err(format!("reshape {} source {pid} is not a live partition", b.id)),
            }
            if let Some(other) = self.reshape_source_of(pid) {
                return Err(format!("partition {pid} is already a source of reshape {other}"));
            }
            ranges.push(
                self.map
                    .range_of(pid)
                    .ok_or_else(|| format!("live partition {pid} is not in the map"))?,
            );
        }
        ranges.sort_by_key(|r| r.lo);
        if ranges.windows(2).any(|w| w[0].hi != w[1].lo as u128) {
            return Err(format!("reshape {} sources are not contiguous", b.id));
        }
        let span = PointRange {
            lo: ranges[0].lo,
            hi: ranges[ranges.len() - 1].hi,
        };
        if b.targets[0].lo != span.lo {
            return Err(format!("reshape {} targets do not start where its sources do", b.id));
        }
        if b.targets.windows(2).any(|w| w[1].lo <= w[0].lo) {
            return Err(format!("reshape {} targets are not strictly increasing", b.id));
        }
        if b.targets[b.targets.len() - 1].lo as u128 >= span.hi {
            return Err(format!("reshape {} has a target past its sources' span", b.id));
        }
        if let Some(t) = b.targets.iter().find(|t| t.loc as usize >= self.locations.len()) {
            return Err(format!("reshape {} target {} is at unknown location {}", b.id, t.pid, t.loc));
        }
        Ok(span)
    }

    /// Applies one record after the map exists. On error the state is
    /// unchanged and the record must not be appended.
    pub fn apply(&mut self, rec: &Record) -> Result<(), String> {
        if self.dropped {
            return Err(format!("{} after DROPPED", rec.kind().name()));
        }
        match rec {
            Record::Header(_) => Err("a second HEADER".to_string()),
            Record::Snapshot(_) => Err("SNAPSHOT after the map was placed; a snapshot only opens a generation".to_string()),
            Record::Locations(paths) => extend_locations(&mut self.locations, paths),
            Record::Place { hi, entries } => self.apply_place(*hi, entries),
            &Record::Materialize { pid, floor, loc } => {
                if loc as usize >= self.locations.len() {
                    return Err(format!("partition {pid} materialized at unknown location {loc}"));
                }
                let p = self.part_mut(pid, true)?;
                if p.location.is_some() {
                    return Err(format!("partition {pid} materialized twice"));
                }
                p.location = Some(loc);
                p.pin = Some(floor);
                Ok(())
            }
            &Record::Dirty { pid, floor } => {
                let p = self.materialized_mut(pid, true)?;
                p.pin = Some(p.pin.map_or(floor, |pin| pin.min(floor)));
                Ok(())
            }
            &Record::Swept { pid, swept_to, clean } => {
                let p = self.materialized_mut(pid, false)?;
                p.pin = (!clean).then_some(swept_to);
                Ok(())
            }
            &Record::Stats { pid, rows, bytes } => {
                self.materialized_mut(pid, false)?.stats = Some(PartStats { rows, bytes });
                Ok(())
            }
            Record::ReshapeBegin(b) => {
                if b.id <= self.last_reshape {
                    return Err(format!("reshape id {} was already used (last {})", b.id, self.last_reshape));
                }
                let span = self.reshape_span(b)?;
                let next = self.fresh(b.targets.iter().map(|t| t.pid))?;
                self.next_pid = next;
                self.last_reshape = b.id;
                self.reshapes.insert(b.id, (b.clone(), span));
                Ok(())
            }
            Record::ReshapeCommit { id, pins } => self.apply_commit(*id, pins),
            Record::ReshapeAbort { id } => match self.reshapes.remove(id) {
                Some(_) => Ok(()),
                None => Err(format!("RESHAPE_ABORT of reshape {id}, which is not open")),
            },
            Record::RetiredGone(pids) => {
                let mut seen = HashSet::new();
                for &pid in pids {
                    if !seen.insert(pid) {
                        return Err(format!("RETIRED_GONE names partition {pid} twice"));
                    }
                    match self.parts.get(&pid) {
                        Some(p) if p.retired => {}
                        Some(_) => return Err(format!("RETIRED_GONE names live partition {pid}")),
                        None => return Err(format!("RETIRED_GONE names unknown partition {pid}")),
                    }
                }
                for pid in pids {
                    self.parts.remove(pid);
                }
                Ok(())
            }
            Record::Dropped => {
                self.dropped = true;
                Ok(())
            }
            Record::Sealed => {
                if self.sealed {
                    return Err("a second SEALED".to_string());
                }
                self.sealed = true;
                Ok(())
            }
        }
    }

    fn apply_place(&mut self, hi: u128, entries: &[(u64, u32)]) -> Result<(), String> {
        let span = place_span(hi, entries)?;
        let (first, end) = self.span_indices(span)?;
        let replaced: Vec<u32> = self.map.entries()[first..end].iter().map(|&(_, pid)| pid).collect();
        for &pid in &replaced {
            if self.parts.get(&pid).is_some_and(|p| p.location.is_some()) {
                return Err(format!("PLACE would drop partition {pid}, which holds files; rows move only by reshape"));
            }
            if let Some(id) = self.reshape_source_of(pid) {
                return Err(format!("PLACE would drop partition {pid}, a source of open reshape {id}"));
            }
        }
        let next_pid = self.fresh(entries.iter().map(|&(_, pid)| pid))?;
        let cur = self.map.entries();
        let mut next = Vec::with_capacity(cur.len() - (end - first) + entries.len());
        next.extend_from_slice(&cur[..first]);
        next.extend_from_slice(entries);
        next.extend_from_slice(&cur[end..]);
        let map = RangeMap::new(next)?;
        let epoch = self.next_epoch()?;
        for pid in replaced {
            self.parts.remove(&pid);
        }
        for &(_, pid) in entries {
            self.parts.insert(pid, PartState::default());
        }
        self.map = map;
        self.next_pid = next_pid;
        self.epoch = epoch;
        Ok(())
    }

    fn apply_commit(&mut self, id: u64, pins: &[(u32, Option<u64>)]) -> Result<(), String> {
        let (b, span) = self
            .reshapes
            .get(&id)
            .cloned()
            .ok_or_else(|| format!("RESHAPE_COMMIT of reshape {id}, which is not open"))?;
        if pins.len() != b.targets.len() || pins.iter().zip(&b.targets).any(|(&(pid, _), t)| pid != t.pid) {
            return Err(format!("RESHAPE_COMMIT of reshape {id} does not list its targets in order"));
        }
        let (first, end) = self.span_indices(span)?;
        let cur = self.map.entries();
        let mut owners: Vec<u32> = cur[first..end].iter().map(|&(_, pid)| pid).collect();
        let mut sources = b.sources.clone();
        owners.sort_unstable();
        sources.sort_unstable();
        if owners != sources {
            return Err(format!("reshape {id}'s sources no longer own its span"));
        }
        // A target holds the raw-copied stamps of every source it overlaps,
        // so it must pin at or below the lowest of their pins (§7.2). Sweeps
        // of the sources pause from BEGIN, so their pins here are final.
        let src = &cur[first..end];
        for (i, (t, &(_, pin))) in b.targets.iter().zip(pins).enumerate() {
            let t_hi = b.targets.get(i + 1).map_or(span.hi, |n| n.lo as u128);
            // Sources are sorted by lo and tile the span, which starts at
            // targets[0].lo, so the one holding t.lo exists.
            let mut j = src.partition_point(|&(lo, _)| lo <= t.lo) - 1;
            let mut need: Option<u64> = None;
            while j < src.len() && (src[j].0 as u128) < t_hi {
                if let Some(p) = self.parts.get(&src[j].1).and_then(|p| p.pin) {
                    need = Some(need.map_or(p, |n| n.min(p)));
                }
                j += 1;
            }
            if let Some(m) = need {
                if !pin.is_some_and(|p| p <= m) {
                    return Err(format!(
                        "RESHAPE_COMMIT {id} target {} pins above its sources ({pin:?}, sources at {m})",
                        t.pid
                    ));
                }
            }
        }
        let mut next = Vec::with_capacity(cur.len() - (end - first) + b.targets.len());
        next.extend_from_slice(&cur[..first]);
        next.extend(b.targets.iter().map(|t| (t.lo, t.pid)));
        next.extend_from_slice(&cur[end..]);
        let map = RangeMap::new(next)?;
        let epoch = self.next_epoch()?;
        for pid in &sources {
            if let Some(p) = self.parts.get_mut(pid) {
                p.retired = true;
            }
        }
        for (t, &(_, pin)) in b.targets.iter().zip(pins) {
            self.parts.insert(
                t.pid,
                PartState {
                    retired: false,
                    location: Some(t.loc),
                    pin,
                    stats: None,
                },
            );
        }
        self.map = map;
        self.epoch = epoch;
        self.reshapes.remove(&id);
        Ok(())
    }

    /// A snapshot may carry any epoch, so the increment is checked.
    fn next_epoch(&self) -> Result<u64, String> {
        self.epoch
            .checked_add(1)
            .ok_or_else(|| "partition map epoch overflow".to_string())
    }
}

// ---------------------------------------------------------------- replay

/// Where a replay stopped short of the end.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TornTail {
    /// Byte offset of the record that failed its framing.
    pub offset: usize,
    pub reason: String,
    /// Bytes from `offset` to the end, all ignored.
    pub trailing: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Replay {
    pub state: MapState,
    /// Bytes of whole records applied: truncate here before appending.
    pub valid_len: usize,
    /// `Some` when bytes past `valid_len` were ignored; reaping must then
    /// wait (§12).
    pub torn: Option<TornTail>,
}

enum Loader {
    Empty,
    Placing { header: MapHeader, locations: Vec<String> },
    Mapped(MapState),
}

impl Loader {
    fn feed(&mut self, rec: &Record) -> Result<(), String> {
        let next = match self {
            Loader::Empty => match rec {
                Record::Header(h) => {
                    h.validate()?;
                    Loader::Placing {
                        header: h.clone(),
                        locations: Vec::new(),
                    }
                }
                other => return Err(format!("the first record is {}, not HEADER", other.kind().name())),
            },
            Loader::Placing { header, locations } => match rec {
                Record::Header(_) => return Err("a second HEADER".to_string()),
                Record::Locations(paths) => return extend_locations(locations, paths),
                Record::Place { hi, entries } => {
                    Loader::Mapped(MapState::placed(header.clone(), locations.clone(), *hi, entries)?)
                }
                Record::Snapshot(s) => Loader::Mapped(MapState::from_snapshot(header.clone(), locations.clone(), s)?),
                other => return Err(format!("{} before the map's first PLACE or SNAPSHOT", other.kind().name())),
            },
            Loader::Mapped(st) => return st.apply(rec),
        };
        *self = next;
        Ok(())
    }
}

/// Replays a map log (§3.2). Stops at a torn tail; refuses a log that is
/// empty, does not open with `HEADER`, never places a map, is damaged before
/// an intact record, or holds a record that passes its CRC but cannot be
/// understood or applied.
pub fn replay(buf: &[u8]) -> Result<Replay, String> {
    let mut loader = Loader::Empty;
    let mut at = 0;
    let mut torn = None;
    loop {
        let (tag, payload, next) = match read_frame(buf, at) {
            Frame::End => break,
            Frame::Torn(reason) => {
                if let Some(y) = intact_frame_after(buf, at) {
                    return Err(format!(
                        "partition map log: damaged record at byte {at} ({reason}) followed by a valid record at byte {y}; refusing"
                    ));
                }
                torn = Some(TornTail {
                    offset: at,
                    reason,
                    trailing: buf.len() - at,
                });
                break;
            }
            Frame::Record { tag, payload, next } => (tag, payload, next),
        };
        decode(tag, payload)
            .and_then(|rec| loader.feed(&rec))
            .map_err(|e| format!("partition map log, record at byte {at}: {e}"))?;
        at = next;
    }
    match loader {
        Loader::Mapped(state) => Ok(Replay {
            state,
            valid_len: at,
            torn,
        }),
        Loader::Empty => Err(match torn {
            Some(t) => format!("partition map log has no readable HEADER: {}", t.reason),
            None => "partition map log is empty".to_string(),
        }),
        Loader::Placing { .. } => Err(format!(
            "partition map log ends at byte {at} before its first PLACE or SNAPSHOT"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const UID: u64 = 0x1234_5678_9abc_def0;
    const HALF: u64 = 1 << 60;

    /// Start of initial(8) partition `i`.
    fn lo(i: u64) -> u64 {
        i << 61
    }

    fn header() -> MapHeader {
        MapHeader {
            uid: UID,
            route_v: ROUTE_V1,
            key: vec![(0, KeyFamily::Integer)],
            widths: vec![48],
            colocation: 0,
        }
    }

    fn locs(n: usize) -> Record {
        Record::Locations(["/data/root", "/mnt/disk2"][..n].iter().map(|s| s.to_string()).collect())
    }

    fn begin(id: u64, sources: &[u32], targets: &[(u64, u32, u32)]) -> Record {
        Record::ReshapeBegin(ReshapeBegin {
            id,
            sources: sources.to_vec(),
            targets: targets
                .iter()
                .map(|&(lo, pid, loc)| ReshapeTarget { lo, pid, loc })
                .collect(),
        })
    }

    /// A generation-0 log of 40 records using every kind but SNAPSHOT and
    /// SEALED: create, materializations on two disks, sweeps, a split, an
    /// aborted then committed merge, a move, a PLACE splitting an unwritten
    /// partition, reclaims, an open reshape, and the drop.
    fn gen0() -> Vec<Record> {
        use Record::*;
        vec![
            Header(header()),
            locs(1),
            Place {
                hi: SPACE_END,
                entries: RangeMap::initial(8).unwrap().entries().to_vec(),
            },
            Materialize { pid: 3, floor: 100, loc: 0 },
            Materialize { pid: 5, floor: 110, loc: 0 },
            locs(2),
            Materialize { pid: 0, floor: 120, loc: 1 },
            Stats { pid: 3, rows: 10, bytes: 4096 },
            Swept { pid: 3, swept_to: 200, clean: false },
            Swept { pid: 3, swept_to: 250, clean: true },
            Dirty { pid: 3, floor: 260 },
            Stats { pid: 5, rows: 500, bytes: 65536 },
            Swept { pid: 5, swept_to: 300, clean: true },
            begin(1, &[3], &[(lo(3), 8, 1), (lo(3) + HALF, 9, 0)]),
            Materialize { pid: 6, floor: 130, loc: 1 },
            ReshapeCommit { id: 1, pins: vec![(8, Some(260)), (9, Some(260))] },
            Stats { pid: 8, rows: 5, bytes: 2048 },
            begin(2, &[4, 5], &[(lo(4), 10, 1)]),
            ReshapeAbort { id: 2 },
            begin(3, &[5, 4], &[(lo(4), 11, 0)]),
            Dirty { pid: 5, floor: 310 },
            ReshapeCommit { id: 3, pins: vec![(11, Some(310))] },
            RetiredGone(vec![3]),
            Swept { pid: 8, swept_to: 400, clean: false },
            Place {
                hi: SPACE_END,
                entries: vec![(lo(7), 12), (lo(7) + HALF, 13)],
            },
            Materialize { pid: 13, floor: 500, loc: 1 },
            RetiredGone(vec![4, 5]),
            Stats { pid: 0, rows: 1, bytes: 100 },
            Swept { pid: 0, swept_to: 600, clean: true },
            Dirty { pid: 0, floor: 610 },
            Materialize { pid: 1, floor: 620, loc: 0 },
            Swept { pid: 9, swept_to: 630, clean: false },
            Stats { pid: 9, rows: 7, bytes: 700 },
            begin(4, &[6], &[(lo(6), 14, 0)]),
            Stats { pid: 6, rows: 2, bytes: 200 },
            ReshapeCommit { id: 4, pins: vec![(14, Some(130))] },
            Swept { pid: 14, swept_to: 700, clean: true },
            begin(5, &[2], &[(lo(2), 15, 1)]),
            Stats { pid: 14, rows: 2, bytes: 200 },
            Dropped,
        ]
    }

    fn log_of(records: &[Record]) -> (Vec<u8>, Vec<usize>) {
        let mut log = Vec::new();
        let mut ends = vec![0];
        for r in records {
            log.extend(encode(r).unwrap());
            ends.push(log.len());
        }
        (log, ends)
    }

    fn state_after(records: &[Record]) -> MapState {
        let r = replay(&log_of(records).0).unwrap();
        assert!(r.torn.is_none());
        r.state
    }

    /// A compacted generation of gen0's state before its drop, then more.
    fn gen1() -> Vec<Record> {
        let before_drop = state_after(&gen0()[..39]);
        use Record::*;
        vec![
            Header(header()),
            locs(2),
            Snapshot(before_drop.snapshot()),
            Sealed,
            ReshapeCommit { id: 5, pins: vec![(15, None)] },
            RetiredGone(vec![6]),
            Materialize { pid: 12, floor: 800, loc: 0 },
            Dirty { pid: 14, floor: 810 },
            Stats { pid: 15, rows: 0, bytes: 0 },
        ]
    }

    fn raw_frame(body: &[u8]) -> Vec<u8> {
        let mut f = (body.len() as u32).to_le_bytes().to_vec();
        f.extend(crc32fast::hash(body).to_le_bytes());
        f.extend_from_slice(body);
        f
    }

    #[test]
    fn route_v1_widths_match_the_key_spec() {
        use crate::core::db_type::DbType;
        for arity in 1..=MAX_ARITY {
            let cols: Vec<(u32, DbType)> = (0..arity as u32).map(|i| (i, DbType::I64)).collect();
            let spec = super::super::KeySpec::new(&cols).unwrap();
            let want: Vec<u8> = spec.widths().iter().map(|&w| w as u8).collect();
            assert_eq!(route_v1_widths(arity), want.as_slice());
        }
    }

    #[test]
    fn every_record_kind_round_trips() {
        let mut records = gen0();
        records.extend(gen1());
        records.push(Record::Header(MapHeader {
            uid: u64::MAX,
            route_v: ROUTE_V1,
            key: vec![(7, KeyFamily::Uuid), (2, KeyFamily::Text), (9, KeyFamily::Integer)],
            widths: vec![48, 8, 8],
            colocation: 0xfeed,
        }));
        records.push(Record::Place {
            hi: SPACE_END - 1,
            entries: vec![(0, u32::MAX)],
        });
        records.push(Record::Locations(vec!["/ünïcode/路径".to_string(), String::new()]));
        records.push(Record::ReshapeCommit { id: u64::MAX, pins: vec![] });
        for kind in Kind::ALL {
            assert!(records.iter().any(|r| r.kind() == kind), "{} not covered", kind.name());
            assert_eq!(Kind::from_tag(kind as u8), Some(kind));
        }
        for r in &records {
            let bytes = encode(r).unwrap();
            let Frame::Record { tag, payload, next } = read_frame(&bytes, 0) else {
                panic!("{} did not frame", r.kind().name());
            };
            assert_eq!(next, bytes.len());
            assert_eq!(payload[0], LAYOUT_V1);
            let back = decode(tag, payload).unwrap();
            assert_eq!(&back, r);
            assert_eq!(encode(&back).unwrap(), bytes);
        }
    }

    #[test]
    fn a_realistic_log_replays_to_the_expected_state() {
        let records = gen0();
        assert_eq!(records.len(), 40);
        let st = state_after(&records[..39]);
        let entries: Vec<(u64, u32)> = vec![
            (lo(0), 0),
            (lo(1), 1),
            (lo(2), 2),
            (lo(3), 8),
            (lo(3) + HALF, 9),
            (lo(4), 11),
            (lo(6), 14),
            (lo(7), 12),
            (lo(7) + HALF, 13),
        ];
        assert_eq!(st.map().entries(), entries.as_slice());
        assert_eq!(st.epoch(), 5);
        assert_eq!(st.next_pid(), 16);
        assert_eq!(st.last_reshape(), 5);
        assert_eq!(st.locations(), ["/data/root", "/mnt/disk2"]);
        let part = |pid| *st.part(pid).unwrap();
        let p = |loc, pin, stats: Option<(u64, u64)>| PartState {
            retired: false,
            location: loc,
            pin,
            stats: stats.map(|(rows, bytes)| PartStats { rows, bytes }),
        };
        assert_eq!(part(0), p(Some(1), Some(610), Some((1, 100))));
        assert_eq!(part(1), p(Some(0), Some(620), None));
        assert_eq!(part(2), p(None, None, None));
        assert_eq!(part(6), PartState { retired: true, ..p(Some(1), Some(130), Some((2, 200))) });
        assert_eq!(part(8), p(Some(1), Some(400), Some((5, 2048))));
        assert_eq!(part(9), p(Some(0), Some(630), Some((7, 700))));
        assert_eq!(part(11), p(Some(0), Some(310), None));
        assert_eq!(part(12), p(None, None, None));
        assert_eq!(part(13), p(Some(1), Some(500), None));
        assert_eq!(part(14), p(Some(0), None, Some((2, 200))));
        for gone in [3, 4, 5, 7, 10, 15] {
            assert!(st.part(gone).is_none(), "pid {gone}");
        }
        assert_eq!(st.forget_pin(), Some(130));
        assert_eq!(st.reshapes().map(|b| b.id).collect::<Vec<_>>(), vec![5]);
        assert!(!st.sealed() && !st.dropped());
        st.check().unwrap();
        independent_check(&st).unwrap();

        // A clean SWEPT then DIRTY pins at the DIRTY floor (§7.2).
        assert_eq!(state_after(&records[..11]).part(3).unwrap().pin, Some(260));
        assert_eq!(state_after(&records[..10]).part(3).unwrap().pin, None);

        let done = state_after(&records);
        assert!(done.dropped());
        let mut after = done.clone();
        assert!(after.apply(&Record::Stats { pid: 0, rows: 1, bytes: 1 }).is_err());
    }

    #[test]
    fn dirty_never_raises_a_pin() {
        let records = gen0();
        let mut st = state_after(&records[..9]);
        assert_eq!(st.part(3).unwrap().pin, Some(200), "unclean SWEPT");
        st.apply(&Record::Dirty { pid: 3, floor: 900 }).unwrap();
        assert_eq!(st.part(3).unwrap().pin, Some(200));
        st.apply(&Record::Dirty { pid: 3, floor: 150 }).unwrap();
        assert_eq!(st.part(3).unwrap().pin, Some(150));
    }

    #[test]
    fn a_compacted_generation_reproduces_the_state() {
        let records = gen1();
        let mut want = state_after(&gen0()[..39]);
        want.sealed = true;
        assert_eq!(state_after(&records[..4]), want);

        let st = state_after(&records);
        assert!(st.sealed());
        assert_eq!(st.reshapes().count(), 0);
        assert_eq!(st.map().pid_of(lo(2)), 15);
        assert_eq!(st.part(15).unwrap().location, Some(1));
        assert_eq!(st.part(15).unwrap().pin, None);
        assert!(st.part(6).is_none());
        assert_eq!(st.part(14).unwrap().pin, Some(810));
        assert_eq!(st.epoch(), 6);
        st.check().unwrap();
        independent_check(&st).unwrap();

        // Compacting again gives the same state.
        let again = vec![Record::Header(header()), locs(2), Record::Snapshot(st.snapshot()), Record::Sealed];
        assert_eq!(state_after(&again), st);
    }

    fn refused(records: &[Record]) -> String {
        replay(&log_of(records).0).unwrap_err()
    }

    #[test]
    fn malformed_logs_are_refused() {
        use Record::*;
        let full = Place {
            hi: SPACE_END,
            entries: RangeMap::initial(4).unwrap().entries().to_vec(),
        };
        let ok = || vec![Header(header()), locs(1), full.clone()];
        assert!(replay(&[]).unwrap_err().contains("empty"));
        assert!(refused(&[locs(1)]).contains("not HEADER"));
        assert!(refused(&[Header(header()), locs(1)]).contains("before its first PLACE"));
        assert!(refused(&[Header(header()), Header(header())]).contains("second HEADER"));
        assert!(refused(&[ok(), vec![Header(header())]].concat()).contains("second HEADER"));
        assert!(refused(&[Header(header()), Stats { pid: 0, rows: 0, bytes: 0 }]).contains("before the map"));
        let bad_header = |h: MapHeader| refused(&[Header(h), locs(1), full.clone()]);
        assert!(bad_header(MapHeader { route_v: 2, ..header() }).contains("route_v2"));
        assert!(bad_header(MapHeader { uid: 0, ..header() }).contains("uid 0"));
        assert!(bad_header(MapHeader { widths: vec![32], ..header() }).contains("widths"));
        assert!(bad_header(MapHeader { key: vec![], widths: vec![], ..header() }).contains("key columns"));
        assert!(bad_header(MapHeader {
            key: vec![(1, KeyFamily::Integer), (1, KeyFamily::Text)],
            widths: vec![48, 16],
            ..header()
        })
        .contains("twice"));

        let partial = Place { hi: (lo(4)) as u128, entries: vec![(0, 0)] };
        assert!(refused(&[Header(header()), locs(1), partial]).contains("whole routing space"));
        let after = |rec: Record| refused(&[ok(), vec![rec]].concat());
        assert!(after(Snapshot(state_after(&ok()).snapshot())).contains("only opens a generation"));
        assert!(after(Materialize { pid: 0, floor: 1, loc: 1 }).contains("unknown location"));
        assert!(after(Materialize { pid: 9, floor: 1, loc: 0 }).contains("no partition"));
        assert!(after(Dirty { pid: 0, floor: 1 }).contains("no files"));
        assert!(after(Swept { pid: 0, swept_to: 1, clean: true }).contains("no files"));
        assert!(after(Stats { pid: 0, rows: 1, bytes: 1 }).contains("no files"));
        assert!(after(Locations(vec!["/other".into()])).contains("does not extend"));
        assert!(after(Locations(vec!["/data/root".into(), "/data/root".into()])).contains("twice"));
        assert!(after(Place { hi: (lo(4) + 1) as u128, entries: vec![(lo(4), 4)] }).contains("not a partition boundary"));
        // initial(4): partition i owns [lo(2i), lo(2i + 2)).
        assert!(after(Place { hi: (lo(6)) as u128, entries: vec![(lo(4), 3)] }).contains("not fresh"));
        assert!(after(Place { hi: (lo(6)) as u128, entries: vec![(lo(4), 9), (lo(4) + 5, 9)] }).contains("twice"));
        assert!(after(begin(1, &[0, 2], &[(0, 9, 0)])).contains("not contiguous"));
        assert!(after(begin(1, &[0], &[(1, 9, 0)])).contains("do not start"));
        assert!(after(begin(1, &[0], &[(0, 9, 0), (lo(2), 10, 0)])).contains("past"));
        assert!(after(begin(1, &[0], &[(0, 3, 0)])).contains("not fresh"));
        assert!(after(begin(1, &[0], &[(0, 9, 1)])).contains("unknown location"));
        assert!(after(begin(0, &[0], &[(0, 9, 0)])).contains("already used"));
        assert!(after(ReshapeCommit { id: 1, pins: vec![] }).contains("not open"));
        assert!(after(ReshapeAbort { id: 1 }).contains("not open"));
        assert!(after(RetiredGone(vec![0])).contains("live"));
        assert!(after(RetiredGone(vec![9])).contains("unknown"));

        let materialized = || [ok(), vec![Materialize { pid: 1, floor: 5, loc: 0 }]].concat();
        let after_m = |recs: Vec<Record>| refused(&[materialized(), recs].concat());
        assert!(after_m(vec![Materialize { pid: 1, floor: 6, loc: 0 }]).contains("twice"));
        assert!(after_m(vec![Place { hi: (lo(4)) as u128, entries: vec![(lo(2), 9)] }]).contains("holds files"));
        let open = begin(1, &[1], &[(lo(2), 9, 0)]);
        assert!(after_m(vec![open.clone(), begin(2, &[1, 2], &[(lo(2), 10, 0)])]).contains("already a source"));
        assert!(after_m(vec![open.clone(), begin(1, &[2], &[(lo(2), 10, 0)])]).contains("already used"));
        assert!(after_m(vec![open.clone(), ReshapeCommit { id: 1, pins: vec![(10, None)] }]).contains("in order"));
        let unwritten = begin(1, &[2], &[(lo(4), 9, 0)]);
        assert!(after_m(vec![unwritten, Place { hi: (lo(6)) as u128, entries: vec![(lo(4), 10)] }]).contains("source of open reshape"));
        let commit = ReshapeCommit { id: 1, pins: vec![(9, Some(5))] };
        assert!(after_m(vec![open.clone(), commit.clone(), Dirty { pid: 1, floor: 1 }]).contains("retired"));
        // A retired, never-written source must not take files after its
        // range moved to the targets.
        let moved = vec![
            begin(1, &[2], &[(lo(4), 9, 0)]),
            ReshapeCommit { id: 1, pins: vec![(9, None)] },
            Materialize { pid: 2, floor: 1, loc: 0 },
        ];
        assert!(after_m(moved).contains("retired"));
        // Targets inherit their sources' pin (§7.2): pid 1 is pinned at 5.
        assert!(after_m(vec![open.clone(), ReshapeCommit { id: 1, pins: vec![(9, None)] }]).contains("pins above"));
        assert!(after_m(vec![open.clone(), ReshapeCommit { id: 1, pins: vec![(9, Some(6))] }]).contains("pins above"));
        assert!(after_m(vec![open.clone(), commit.clone(), RetiredGone(vec![1]), Stats { pid: 1, rows: 0, bytes: 0 }])
            .contains("no partition"));
        assert!(after_m(vec![open, ReshapeAbort { id: 1 }, ReshapeAbort { id: 1 }]).contains("not open"));
        assert!(after_m(vec![Sealed, Sealed]).contains("second SEALED"));
        assert!(after_m(vec![Dropped, Sealed]).contains("after DROPPED"));
    }

    #[test]
    fn records_valid_under_their_crc_but_not_understood_are_refused() {
        let (good, _) = log_of(&gen0()[..3]);
        let with = |body: &[u8]| {
            let mut log = good.clone();
            log.extend(raw_frame(body));
            replay(&log)
        };
        assert!(with(&[0x0F, LAYOUT_V1]).unwrap_err().contains("unknown record kind"));
        assert!(with(&[0x00]).unwrap_err().contains("unknown record kind"));
        assert!(with(&[Kind::Sealed as u8, 2]).unwrap_err().contains("layout version 2"));
        assert!(with(&[Kind::Sealed as u8]).unwrap_err().contains("ends early"));
        assert!(with(&[Kind::Sealed as u8, LAYOUT_V1, 0]).unwrap_err().contains("after the payload"));
        let mut swept = encode(&Record::Swept { pid: 3, swept_to: 1, clean: true }).unwrap()[FRAME_LEN..].to_vec();
        *swept.last_mut().unwrap() = 2;
        assert!(with(&swept).unwrap_err().contains("flag byte"));
        let mut huge = vec![Kind::RetiredGone as u8, LAYOUT_V1];
        huge.extend(u32::MAX.to_le_bytes());
        assert!(with(&huge).unwrap_err().contains("overruns"));
        assert!(with(&[Kind::Sealed as u8, LAYOUT_V1]).is_ok());
    }

    #[test]
    fn a_torn_tail_is_reported_and_ignored() {
        let records = gen0();
        let (mut log, ends) = log_of(&records[..10]);
        let want = state_after(&records[..10]);
        // A crash that extended the file with zeros.
        log.extend(vec![0u8; 4096]);
        let r = replay(&log).unwrap();
        assert_eq!(r.state, want);
        assert_eq!(r.valid_len, ends[10]);
        let torn = r.torn.unwrap();
        assert_eq!((torn.offset, torn.trailing), (ends[10], 4096));
        assert!(torn.reason.contains("implausible"), "{}", torn.reason);
    }

    #[test]
    fn apply_leaves_the_state_unchanged_on_error() {
        let st = state_after(&gen0()[..39]);
        let bad = [
            Record::Materialize { pid: 1, floor: 1, loc: 0 },
            Record::ReshapeCommit { id: 5, pins: vec![(16, None)] },
            Record::Place { hi: (lo(1)) as u128, entries: vec![(0, 20)] },
            Record::Place { hi: (lo(7) + HALF) as u128, entries: vec![(lo(7), 20), (lo(7) + 1, 20)] },
            Record::RetiredGone(vec![6, 0]),
            Record::RetiredGone(vec![6, 6]),
            Record::Locations(vec!["/mnt/disk2".into(), "/data/root".into()]),
            Record::Locations(vec!["/data/root".into(), "/mnt/disk2".into(), "".into()]),
            begin(6, &[12, 2], &[(lo(7), 20, 0)]),
            begin(6, &[12], &[(lo(7), 20, 0), (lo(7) + 1, 21, 0), (lo(7) + 1, 22, 0)]),
            begin(6, &[12], &[(lo(7), 15, 0)]),
            Record::Header(header()),
        ];
        for rec in &bad {
            let mut s = st.clone();
            assert!(s.apply(rec).is_err(), "{rec:?} applied");
            assert_eq!(s, st, "{rec:?} changed the state");
        }

        // Pid 9 is pinned at 630; a split must pin both halves at or below it.
        let mut split = st.clone();
        split
            .apply(&begin(6, &[9], &[(lo(3) + HALF, 20, 0), (lo(3) + HALF + 1, 21, 1)]))
            .unwrap();
        for pins in [
            vec![(20, Some(630)), (21, None)],
            vec![(20, Some(631)), (21, Some(630))],
        ] {
            let mut s = split.clone();
            let err = s.apply(&Record::ReshapeCommit { id: 6, pins }).unwrap_err();
            assert!(err.contains("pins above"), "{err}");
            assert_eq!(s, split);
        }
        split
            .apply(&Record::ReshapeCommit { id: 6, pins: vec![(20, Some(630)), (21, Some(1))] })
            .unwrap();
    }

    #[test]
    fn a_reshape_target_pins_at_most_the_sources_it_overlaps() {
        use Record::*;
        let base = vec![
            Header(header()),
            locs(1),
            Place {
                hi: SPACE_END,
                entries: RangeMap::initial(4).unwrap().entries().to_vec(),
            },
            Materialize { pid: 0, floor: 5, loc: 0 },
            Materialize { pid: 1, floor: 50, loc: 0 },
            // Pid 9 takes source 0's range, pid 10 source 1's.
            begin(1, &[0, 1], &[(0, 9, 0), (lo(2), 10, 0)]),
        ];
        let commit = |a, b| replay(&log_of(&[base.clone(), vec![ReshapeCommit { id: 1, pins: vec![(9, a), (10, b)] }]].concat()).0);
        let st = commit(Some(5), Some(50)).unwrap().state;
        assert_eq!((st.part(9).unwrap().pin, st.part(10).unwrap().pin), (Some(5), Some(50)));
        assert!(commit(Some(5), Some(51)).unwrap_err().contains("target 10 pins above"));
        assert!(commit(Some(6), Some(50)).unwrap_err().contains("target 9 pins above"));
        assert!(commit(Some(5), None).unwrap_err().contains("target 10 pins above"));
        // A merge's one target overlaps both and takes the lower pin.
        let merge = [
            base[..5].to_vec(),
            vec![begin(1, &[0, 1], &[(0, 9, 0)]), ReshapeCommit { id: 1, pins: vec![(9, Some(6))] }],
        ]
        .concat();
        assert!(refused(&merge).contains("target 9 pins above"));
    }

    #[test]
    fn the_epoch_cannot_overflow() {
        use Record::*;
        let ok = vec![
            Header(header()),
            locs(1),
            Place {
                hi: SPACE_END,
                entries: RangeMap::initial(4).unwrap().entries().to_vec(),
            },
        ];
        let mut s = state_after(&ok).snapshot();
        s.epoch = u64::MAX;
        let st = state_after(&[Header(header()), locs(1), Snapshot(s.clone())]);
        let place = Place { hi: lo(2) as u128, entries: vec![(0, 9)] };
        let mut t = st.clone();
        assert!(t.apply(&place).unwrap_err().contains("epoch overflow"));
        assert_eq!(t, st);
        assert!(refused(&[Header(header()), locs(1), Snapshot(s), place]).contains("epoch overflow"));
        t.apply(&begin(1, &[0], &[(0, 9, 0)])).unwrap();
        let open = t.clone();
        assert!(t.apply(&ReshapeCommit { id: 1, pins: vec![(9, None)] }).unwrap_err().contains("epoch overflow"));
        assert_eq!(t, open);
    }

    #[test]
    fn bad_snapshots_are_refused() {
        let good = state_after(&gen0()[..39]).snapshot();
        let load = |s: Snapshot| replay(&log_of(&[Record::Header(header()), locs(2), Record::Snapshot(s)]).0);
        load(good.clone()).unwrap();
        let bad = |what: &str, edit: &dyn Fn(&mut Snapshot)| {
            let mut s = good.clone();
            edit(&mut s);
            let err = load(s).unwrap_err();
            assert!(err.contains(what), "{what}: {err}");
        };
        // Parts by index: 0 1 2 6 8 9 11 12 13 14; reshape 5 splits 2 into 15.
        assert_eq!(good.parts.iter().map(|p| p.0).collect::<Vec<_>>(), [0, 1, 2, 6, 8, 9, 11, 12, 13, 14]);
        bad("never handed out", &|s| s.next_pid = 14);
        bad("epoch 0", &|s| s.epoch = 0);
        bad("not sorted", &|s| s.parts.swap(0, 1));
        bad("not live", &|s| {
            s.parts.remove(4);
        });
        bad("not live", &|s| s.parts[4].1.retired = true);
        bad("does not place", &|s| s.parts.insert(4, (7, PartState::default())));
        bad("only 2 are interned", &|s| s.parts[0].1.location = Some(2));
        bad("no files", &|s| s.parts[2].1.pin = Some(1));
        bad("no files", &|s| s.parts[2].1.stats = Some(PartStats { rows: 1, bytes: 1 }));
        bad("out of order", &|s| s.last_reshape = 4);
        bad("out of order", &|s| s.reshapes[0].id = 0);
        bad("not a spent, unused pid", &|s| s.reshapes[0].targets[0].pid = 13);
        bad("not a spent, unused pid", &|s| s.reshapes[0].targets[0].pid = 16);
        bad("not a live partition", &|s| s.reshapes[0].sources = vec![3]);
        bad("already a source of reshape 4", &|s| {
            s.reshapes.insert(
                0,
                ReshapeBegin {
                    id: 4,
                    sources: vec![2],
                    targets: vec![ReshapeTarget { lo: lo(2), pid: 10, loc: 0 }],
                },
            )
        });
    }

    /// Hex with spaces between fields.
    fn unhex(s: &str) -> Vec<u8> {
        let s: String = s.split_whitespace().collect();
        (0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap()).collect()
    }

    /// The bytes on disk, written out independently of [`encode`] (CRCs from
    /// zlib): a change made alike to the writer and the reader would reread
    /// every existing log differently, and only these catch it.
    #[test]
    fn the_on_disk_format_is_frozen() {
        use Record::*;
        let tags = [
            (Kind::Header, 0x01),
            (Kind::Locations, 0x02),
            (Kind::Place, 0x03),
            (Kind::Materialize, 0x04),
            (Kind::Dirty, 0x05),
            (Kind::Swept, 0x06),
            (Kind::Stats, 0x07),
            (Kind::ReshapeBegin, 0x08),
            (Kind::ReshapeCommit, 0x09),
            (Kind::ReshapeAbort, 0x0A),
            (Kind::RetiredGone, 0x0B),
            (Kind::Dropped, 0x0C),
            (Kind::Snapshot, 0x0D),
            (Kind::Sealed, 0x0E),
        ];
        for (kind, tag) in tags {
            assert_eq!(kind as u8, tag, "{}", kind.name());
        }
        let golden = [
            (
                Header(MapHeader {
                    uid: 0x1234_5678_9abc_def0,
                    route_v: 1,
                    key: vec![(3, KeyFamily::Integer), (9, KeyFamily::Text)],
                    widths: vec![48, 16],
                    colocation: 0x0102,
                }),
                "22000000 e72fbf64 0101 f0debc9a78563412 0100 02 0300000001 0900000003 023010 0201000000000000",
            ),
            (
                Locations(vec!["/a".into(), "/bc".into()]),
                "13000000 23930e35 0201 02000000 020000002f61 030000002f6263",
            ),
            (
                Place { hi: SPACE_END, entries: vec![(0, 0), (1 << 63, 1)] },
                "2e000000 4a75f2bf 0301 00000000000000000100000000000000 02000000 000000000000000000000000 000000000000008001000000",
            ),
            (
                Materialize { pid: 0x0102_0304, floor: 0x1112_1314_1516_1718, loc: 0x2122_2324 },
                "12000000 8b664610 0401 04030201 1817161514131211 24232221",
            ),
            (Dirty { pid: 5, floor: 0x0a0b }, "0e000000 27fa37e9 0501 05000000 0b0a000000000000"),
            (
                Swept { pid: 6, swept_to: 0x0c0d, clean: true },
                "0f000000 302b2419 0601 06000000 0d0c000000000000 01",
            ),
            (
                Stats { pid: 7, rows: 0x0102, bytes: 0x0a_0b0c },
                "16000000 f5360679 0701 07000000 0201000000000000 0c0b0a0000000000",
            ),
            (
                begin(3, &[1, 2], &[(1 << 63, 4, 0), (3 << 62, 5, 1)]),
                "3a000000 815bf46a 0801 0300000000000000 02000000 01000000 02000000 02000000 \
                 00000000000000800400000000000000 00000000000000c00500000001000000",
            ),
            (
                ReshapeCommit { id: 5, pins: vec![(15, None), (16, Some(0x0102_0304_0506_0708))] },
                "28000000 5635d332 0901 0500000000000000 02000000 0f000000000000000000000000 10000000010807060504030201",
            ),
            (ReshapeAbort { id: 7 }, "0a000000 e5e516e9 0a01 0700000000000000"),
            (RetiredGone(vec![4, 5]), "0e000000 f8fbba1c 0b01 02000000 04000000 05000000"),
            (Dropped, "02000000 656d6b9a 0c01"),
            (
                Snapshot(super::Snapshot {
                    epoch: 2,
                    next_pid: 6,
                    last_reshape: 3,
                    entries: vec![(0, 0), (1 << 63, 1)],
                    parts: vec![
                        (0, PartState::default()),
                        (
                            1,
                            PartState {
                                retired: false,
                                location: Some(1),
                                pin: Some(9),
                                stats: Some(PartStats { rows: 3, bytes: 4 }),
                            },
                        ),
                    ],
                    reshapes: vec![super::ReshapeBegin {
                        id: 3,
                        sources: vec![1],
                        targets: vec![ReshapeTarget { lo: 1 << 63, pid: 5, loc: 0 }],
                    }],
                }),
                "aa000000 e8de6673 0d01 020000000000000006000000000000000300000000000000 \
                 02000000 000000000000000000000000 000000000000008001000000 \
                 02000000 000000000000000000000000000000000000000000000000000000000000000000000000 \
                 010000000001010000000109000000000000000103000000000000000400000000000000 \
                 01000000 0300000000000000 01000000 01000000 01000000 00000000000000800500000000000000",
            ),
            (Sealed, "02000000 e70f5da8 0e01"),
        ];
        for (rec, hex) in golden {
            let bytes = unhex(hex);
            assert_eq!(encode(&rec).unwrap(), bytes, "{}", rec.kind().name());
            assert_eq!(decode(bytes[8], &bytes[9..]).unwrap(), rec, "{}", rec.kind().name());
        }
    }

    /// A log damaged before its last record lost a record from its middle:
    /// refused, never truncated to the damage (§12 would then delete the
    /// files of a partition whose MATERIALIZE came after it).
    #[test]
    fn damage_before_an_intact_record_is_refused() {
        use Record::*;
        let records = vec![
            Header(header()),
            locs(1),
            Place {
                hi: SPACE_END,
                entries: RangeMap::initial(4).unwrap().entries().to_vec(),
            },
            Materialize { pid: 0, floor: 10, loc: 0 },
            Materialize { pid: 1, floor: 11, loc: 0 },
            Stats { pid: 1, rows: 1, bytes: 1 },
        ];
        let (log, ends) = log_of(&records);
        let mut flipped = log.clone();
        flipped[ends[3] + FRAME_LEN + 3] ^= 0x01;
        let err = replay(&flipped).unwrap_err();
        assert!(err.contains(&format!("damaged record at byte {}", ends[3])), "{err}");
        assert!(err.contains(&format!("valid record at byte {}", ends[4])), "{err}");
        // A hole of zeros where a middle record was.
        let mut hole = log.clone();
        hole[ends[3]..ends[4]].fill(0);
        assert!(replay(&hole).unwrap_err().contains("followed by a valid record"));

        // A partial last record, then the zeros of a file a crash extended,
        // is a torn tail.
        let mut torn = log[..ends[5] + 5].to_vec();
        torn.extend(vec![0u8; 512]);
        let r = replay(&torn).unwrap();
        assert_eq!(r.valid_len, ends[5]);
        assert_eq!(r.state, state_after(&records[..5]));
        assert_eq!(r.torn.unwrap().trailing, 5 + 512);
    }

    /// Invariants checked without [`MapState::from_snapshot`], which
    /// [`MapState::check`] goes through and so cannot judge.
    fn independent_check(st: &MapState) -> Result<(), String> {
        st.map.validate()?;
        if st.epoch == 0 {
            return Err("epoch 0".into());
        }
        let mut live: Vec<u32> = Vec::new();
        for (&pid, p) in &st.parts {
            if pid as u64 >= st.next_pid {
                return Err(format!("pid {pid} at or above next_pid {}", st.next_pid));
            }
            if p.location.is_some_and(|l| l as usize >= st.locations.len()) {
                return Err(format!("pid {pid} at an unknown location"));
            }
            if p.location.is_none() && (p.pin.is_some() || p.stats.is_some()) {
                return Err(format!("pid {pid} has a pin or stats but no files"));
            }
            if !p.retired {
                live.push(pid);
            }
        }
        let mut placed: Vec<u32> = st.map.entries().iter().map(|&(_, pid)| pid).collect();
        placed.sort_unstable();
        if live != placed {
            return Err(format!("live pids {live:?} are not the map's {placed:?}"));
        }
        for (&id, (b, _)) in &st.reshapes {
            if id == 0 || id > st.last_reshape || b.id != id {
                return Err(format!("reshape id {id}"));
            }
            if b.sources.iter().any(|s| st.parts.get(s).is_none_or(|p| p.retired)) {
                return Err(format!("reshape {id} has a source that is not live"));
            }
            if b.targets.iter().any(|t| t.pid as u64 >= st.next_pid || st.parts.contains_key(&t.pid)) {
                return Err(format!("reshape {id} has a target that is not spent and unused"));
            }
        }
        Ok(())
    }

    /// Every outcome a damaged log may have: exactly the state of the last
    /// whole prefix, or an error where that prefix is itself refused.
    fn assert_recovers(got: Result<Replay, String>, want: &Result<MapState, String>, valid_len: usize, what: &str) {
        match (got, want) {
            (Ok(r), Ok(w)) => {
                r.state.check().unwrap_or_else(|e| panic!("{what}: {e}"));
                independent_check(&r.state).unwrap_or_else(|e| panic!("{what}: {e}"));
                r.state.map().validate().unwrap();
                assert_eq!(&r.state, w, "{what}");
                assert_eq!(r.valid_len, valid_len, "{what}");
            }
            (Err(_), Err(_)) => {}
            (Ok(_), Err(e)) => panic!("{what}: recovered a state where the prefix is refused ({e})"),
            (Err(e), Ok(_)) => panic!("{what}: refused ({e}) where the prefix replays"),
        }
    }

    fn fuzz(records: &[Record]) {
        let (log, ends) = log_of(records);
        let prefixes: Vec<Result<MapState, String>> =
            (0..=records.len()).map(|k| replay(&log[..ends[k]]).map(|r| r.state)).collect();
        assert!(prefixes.last().unwrap().is_ok());
        // The prefix whose records end at or before byte `t`.
        let whole = |t: usize| ends.iter().rposition(|&e| e <= t).unwrap();

        for t in 0..=log.len() {
            let k = whole(t);
            let got = replay(&log[..t]);
            if let Ok(r) = &got {
                assert_eq!(r.torn.is_some(), t != ends[k], "truncated at {t}");
            }
            assert_recovers(got, &prefixes[k], ends[k], &format!("truncated at {t}"));
        }

        // Damage to any record but the last leaves an intact record after it,
        // which a crash cannot: refused. Damage to the last is a torn tail.
        let last = records.len() - 1;
        let mut damaged = log.clone();
        for at in 0..log.len() {
            let k = whole(at);
            for mask in [0x01u8, 0x80, 0xFF] {
                damaged[at] ^= mask;
                let got = replay(&damaged);
                damaged[at] ^= mask;
                let what = format!("byte {at} (record {k}) ^ {mask:#x}");
                if k < last {
                    match got {
                        Err(e) => assert!(e.contains("followed by a valid record"), "{what}: {e}"),
                        Ok(r) => panic!("{what}: recovered {} bytes past mid-log damage", r.valid_len),
                    }
                    continue;
                }
                if let Ok(r) = &got {
                    assert!(r.torn.is_some(), "{what} went unnoticed");
                }
                assert_recovers(got, &prefixes[k], ends[k], &what);
            }
        }

        // Damage the CRC cannot see: re-sign each corrupted record. Any
        // outcome but a panic or an invalid state is acceptable.
        for i in 0..records.len() {
            let body = &log[ends[i] + FRAME_LEN..ends[i + 1]];
            for at in 0..body.len() {
                for mask in [0x01u8, 0xFF] {
                    let mut b = body.to_vec();
                    b[at] ^= mask;
                    let mut l = log[..ends[i]].to_vec();
                    l.extend(raw_frame(&b));
                    l.extend_from_slice(&log[ends[i + 1]..]);
                    if let Ok(r) = replay(&l) {
                        r.state
                            .check()
                            .and_then(|()| independent_check(&r.state))
                            .unwrap_or_else(|e| panic!("record {i} byte {at} ^ {mask:#x}: {e}"));
                    }
                }
            }
        }
    }

    #[test]
    fn fuzz_a_generation_zero_log() {
        fuzz(&gen0());
    }

    #[test]
    fn fuzz_a_compacted_log() {
        fuzz(&gen1());
    }

    #[test]
    fn random_bytes_never_panic() {
        let (prefix, _) = log_of(&gen0()[..3]);
        let mut x: u64 = 0x2545_F491_4F6C_DD1D;
        let mut next = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        for round in 0..4000 {
            let len = (next() % 160) as usize;
            let mut buf = if round % 2 == 0 { Vec::new() } else { prefix.clone() };
            for _ in 0..len {
                buf.push(next() as u8);
            }
            if round % 4 == 3 && len > 0 {
                // A CRC-valid frame of random contents.
                let body: Vec<u8> = buf.split_off(prefix.len());
                buf.extend(raw_frame(&body));
            }
            if let Ok(r) = replay(&buf) {
                r.state.check().unwrap();
                independent_check(&r.state).unwrap();
            }
        }
    }
}
