//! Sidecar persistence for the filter index.
//!
//! # Why this is safe to lose
//!
//! The sidecar is **derived data**. It is never on the durability critical path
//! and never needs `fsync`: any file we cannot fully trust is discarded and the
//! filter falls back to "everything is a candidate", which is exactly the
//! behaviour of a table with no index. Correctness never depends on it.
//!
//! # The staleness hazard, and the one byte that closes it
//!
//! A checkpoint records each column's `valid_upto` frontier: slots below it were
//! correctly indexed *at that moment*. Rows appended afterwards are no problem —
//! they sit above the frontier and are treated as candidates on reload.
//!
//! Rows **updated or deleted** afterwards are a real problem. The checkpoint
//! still holds the old fingerprint, so a query for the row's *new* value would
//! find no match at that slot and prune a row that does match — a false
//! negative, the one failure this design must never produce.
//!
//! So the superblock carries a `dirty` byte. `Table` sets it before any delete
//! or update, and a checkpoint clears it. A file loaded with `dirty != 0` is
//! rejected wholesale. That costs one byte written per delete/update *statement*
//! (not per row, and never on the insert path) and makes staleness impossible
//! rather than merely unlikely.

use crate::core::db_type::DbType;
use crate::core::row::row_pointer::ROW_POINTER_RECORD_LEN;
use crate::core::storage_providers::traits::StorageIO;

use super::bloom::SuperBloom;
use super::column::{ColumnDelta, ColumnFilter, Generation, Hll};
use super::hash::HashSeed;
use super::kernels::ZONE_ROWS;
use super::zonemap::ZoneMap;

const MAGIC: &[u8; 8] = b"RDBFLT01";
// v2 added per-column zone min/max bounds; v3 brought `BOOL` into the index as
// a bounds-only column, which changes *which* columns a file describes. Older
// files are rejected rather than upgraded: rebuilding costs one scan and keeps
// a single code path.
const FORMAT_VER: u16 = 3;

/// Fixed-size superblock. `dirty` lives at a known offset so it can be flipped
/// with an 8-byte positioned write, without rewriting anything else.
pub const SUPERBLOCK_LEN: usize = 64;
/// Byte offset of the `dirty` flag inside the superblock.
pub const DIRTY_OFFSET: u64 = 13;
/// Byte offset of `base_len` inside the superblock.
const BASE_LEN_OFFSET: usize = 40;

/// Marks the start of a delta record. Deliberately not the file magic: a reader
/// that finds this where a base image should be, or vice versa, is reading a
/// file whose layout it does not understand.
const DELTA_MAGIC: &[u8; 4] = b"RDLD";
/// Header bytes of a delta record ahead of its payload.
const DELTA_HEADER_LEN: usize = 16;

/// How many deltas may follow a base before the next checkpoint rewrites it.
///
/// Deltas are replayed in order at open, so an unbounded chain turns a cheap
/// load into a long one. The bound also caps how much a torn tail can cost.
const MAX_DELTAS: u32 = 64;

struct Writer {
    buf: Vec<u8>,
}

impl Writer {
    fn new() -> Self {
        Self {
            buf: Vec::with_capacity(1 << 16),
        }
    }
    #[inline]
    fn u8(&mut self, v: u8) {
        self.buf.push(v);
    }
    #[inline]
    fn u16(&mut self, v: u16) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }
    #[inline]
    fn u32(&mut self, v: u32) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }
    #[inline]
    fn u64(&mut self, v: u64) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }
    #[inline]
    fn bytes(&mut self, v: &[u8]) {
        self.buf.extend_from_slice(v);
    }
    #[inline]
    fn pad(&mut self, n: usize) {
        self.buf.resize(self.buf.len() + n, 0);
    }
    fn words(&mut self, w: &[u64]) {
        self.buf.reserve(w.len() * 8);
        for v in w {
            self.buf.extend_from_slice(&v.to_le_bytes());
        }
    }
    fn words32(&mut self, w: &[u32]) {
        self.buf.reserve(w.len() * 4);
        for v in w {
            self.buf.extend_from_slice(&v.to_le_bytes());
        }
    }
}

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }
    /// Every length in the file is attacker-controlled once the bytes are
    /// corrupt, so the arithmetic here must not overflow before the bounds
    /// check gets a chance to fail. `pos + n` on a corrupted `u64` length
    /// panics in a debug build rather than rejecting the checkpoint.
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let end = self.pos.checked_add(n)?;
        let s = self.buf.get(self.pos..end)?;
        self.pos = end;
        Some(s)
    }
    fn u8(&mut self) -> Option<u8> {
        Some(self.take(1)?[0])
    }
    fn u16(&mut self) -> Option<u16> {
        Some(u16::from_le_bytes(self.take(2)?.try_into().ok()?))
    }
    fn u32(&mut self) -> Option<u32> {
        Some(u32::from_le_bytes(self.take(4)?.try_into().ok()?))
    }
    fn u64(&mut self) -> Option<u64> {
        Some(u64::from_le_bytes(self.take(8)?.try_into().ok()?))
    }
    fn skip(&mut self, n: usize) -> Option<()> {
        self.take(n).map(|_| ())
    }
    fn words(&mut self, n: usize) -> Option<Vec<u64>> {
        // `n * 8` overflows for a corrupt length long before `take` could
        // reject it out of bounds.
        let raw = self.take(n.checked_mul(8)?)?;
        Some(
            raw.chunks_exact(8)
                .map(|c| u64::from_le_bytes(c.try_into().unwrap()))
                .collect(),
        )
    }
    fn words32(&mut self, n: usize) -> Option<Vec<u32>> {
        let raw = self.take(n.checked_mul(4)?)?;
        Some(
            raw.chunks_exact(4)
                .map(|c| u32::from_le_bytes(c.try_into().unwrap()))
                .collect(),
        )
    }
}

fn write_superblock(w: &mut Writer, seed: HashSeed, column_count: u16, n_slots: u64) {
    let start = w.buf.len();
    w.bytes(MAGIC);
    w.u16(FORMAT_VER);
    w.u16(ZONE_ROWS as u16);
    w.u8(ROW_POINTER_RECORD_LEN as u8);
    w.u8(0); // dirty — cleared by construction; DIRTY_OFFSET points here
    w.u16(column_count);
    w.u64(seed.k0);
    w.u64(seed.k1);
    w.u64(n_slots);
    debug_assert_eq!(w.buf.len() - start, DIRTY_OFFSET as usize + 27);
    debug_assert_eq!(w.buf.len() - start, BASE_LEN_OFFSET);
    w.u64(0); // base_len — patched once the base image is complete
    w.pad(SUPERBLOCK_LEN - (w.buf.len() - start));
}

/// Serialize the whole catalog. Columns are written in the caller's order and
/// validated against the live schema on load.
pub fn serialize(columns: &[&ColumnFilter], seed: HashSeed, n_slots: u64) -> Vec<u8> {
    let mut w = Writer::new();
    write_superblock(&mut w, seed, columns.len() as u16, n_slots);

    for col in columns {
        w.u32(col.write_order);
        w.u8(col.db_type.to_byte());
        w.u8(col.current_f());
        w.u16(0);
        // The persisted frontier is the *effective* one: a slot modified since
        // the last checkpoint drags it back so the stale region is never trusted.
        w.u64(col.persistable_valid_upto());
        // And the prefix below it that was never indexed at all, which is a
        // separate fact: a table opened with `backfill_on_open = off` has one
        // for its whole life, and a checkpoint that forgot to say so would come
        // back claiming those rows were indexed.
        w.u64(col.unknown_below());
        w.u64(col.rows_seen());
        w.bytes(col.hll_regs());

        let gens = col.generations();
        w.u32(gens.len() as u32);
        w.u32(0);
        for g in gens {
            w.u64(g.start_zone);
            w.u8(g.f);
            w.pad(7);
            w.u64(g.words.len() as u64);
            w.words(&g.words);
        }

        match col.zonemap() {
            Some(zm) => {
                w.u8(1);
                w.pad(7);
                w.u64(zm.data().len() as u64);
                w.bytes(zm.data());
                w.u64(zm.valid_bits().len() as u64);
                w.words(zm.valid_bits());
            }
            None => {
                w.u8(0);
                w.pad(7);
            }
        }

        match col.span_summary() {
            Some(b) => {
                w.u8(1);
                w.pad(7);
                w.u64(b.words().len() as u64);
                w.words32(b.words());
            }
            None => {
                w.u8(0);
                w.pad(7);
            }
        }
    }

    // The base ends here; everything past it is a delta record.
    let base_len = w.buf.len() as u64;
    w.buf[BASE_LEN_OFFSET..BASE_LEN_OFFSET + 8].copy_from_slice(&base_len.to_le_bytes());
    w.buf
}

/// Serialize one incremental record: the state of every column from its own
/// `from_zone` onward, to be appended after the base and any earlier deltas.
///
/// `from_zones` is parallel to `columns`.
pub fn serialize_delta(columns: &[&ColumnFilter], from_zones: &[u64], n_slots: u64) -> Vec<u8> {
    debug_assert_eq!(columns.len(), from_zones.len());
    let mut w = Writer::new();
    w.bytes(DELTA_MAGIC);
    w.u16(FORMAT_VER);
    w.u16(0);
    w.u64(0); // payload_len — patched below
    let payload_start = w.buf.len();
    debug_assert_eq!(payload_start, DELTA_HEADER_LEN);

    w.u64(n_slots);
    w.u32(columns.len() as u32);
    w.u32(0);

    for (col, &from_zone) in columns.iter().zip(from_zones) {
        let d = col.delta(from_zone);
        w.u32(col.write_order);
        w.u8(col.db_type.to_byte());
        w.u8(d.next_f);
        w.u16(0);
        w.u64(d.valid_upto);
        w.u64(d.unknown_below);
        w.u64(d.rows_seen);
        w.bytes(d.hll.regs());

        w.u32(d.gens.len() as u32);
        w.u32(0);
        for (start_zone, f, first_zone, words) in &d.gens {
            w.u64(*start_zone);
            w.u8(*f);
            w.pad(7);
            w.u64(*first_zone);
            w.u64(words.len() as u64);
            w.words(words);
        }

        match &d.zonemap {
            Some((first_zone, data, valid)) => {
                w.u8(1);
                w.pad(7);
                w.u64(*first_zone);
                w.u64(data.len() as u64);
                w.bytes(data);
                w.u64(valid.len() as u64);
                w.words(valid);
            }
            None => {
                w.u8(0);
                w.pad(7);
            }
        }

        match &d.bloom {
            Some((first_superzone, words)) => {
                w.u8(1);
                w.pad(7);
                w.u64(*first_superzone);
                w.u64(words.len() as u64);
                w.words32(words);
            }
            None => {
                w.u8(0);
                w.pad(7);
            }
        }
    }

    let payload_len = (w.buf.len() - payload_start) as u64;
    w.buf[8..16].copy_from_slice(&payload_len.to_le_bytes());
    w.buf
}

/// Outcome of parsing a sidecar.
pub struct Loaded {
    pub columns: Vec<ColumnFilter>,
    pub n_slots: u64,
    /// Bytes of the file that were actually understood: the base image plus
    /// every delta that replayed cleanly.
    ///
    /// The next checkpoint writes from here, so a torn or rejected tail is
    /// overwritten rather than left in front of the record that supersedes it —
    /// otherwise the next open would stop at the same bad record again and never
    /// reach anything written after it.
    pub valid_len: u64,
    /// Delta records replayed on top of the base image.
    pub deltas: u32,
}

/// Append a stopper so replay ends here even if a longer file preceded this
/// write.
///
/// A base image can be *shorter* than the file it replaces: a delta re-sends a
/// whole HLL and validity vector per column, so a long chain of them can outrun
/// the growth of the planes they describe. Whatever is left beyond the new end
/// is bytes from an older layout, and the one thing they must not do is parse.
/// Sixteen zeroes cannot: the magic check fails on the first of them.
///
/// The stopper is written but not counted — the next record overwrites it.
pub fn terminate(buf: &mut Vec<u8>) {
    buf.extend_from_slice(&[0u8; DELTA_HEADER_LEN]);
}

/// Bytes [`terminate`] adds beyond the meaningful end of a write.
pub const TERMINATOR_LEN: u64 = DELTA_HEADER_LEN as u64;

/// Ceiling on how many deltas may follow a base image.
pub const MAX_DELTAS_PER_BASE: u32 = MAX_DELTAS;

/// Reasons a sidecar is discarded. Every one falls back to an unindexed table,
/// never to a wrong answer.
#[derive(Debug, PartialEq, Eq)]
pub enum LoadReject {
    Empty,
    BadMagic,
    Version(u16),
    /// `ZONE_ROWS` changed: slot-to-zone arithmetic would differ.
    Geometry,
    /// `enable_long_row` toggled: `slot = offset / RECORD_LEN` would differ, so
    /// every stored fingerprint describes the wrong row.
    RecordLen,
    /// Written by a different hash seed; every fingerprint would be meaningless.
    Seed,
    /// A delete or update landed after the last checkpoint.
    Dirty,
    Truncated,
    /// Schema no longer matches what was persisted.
    SchemaMismatch,
    /// The checkpoint describes more slots than the pointer file now holds, so
    /// it cannot be describing this file. See `deserialize`'s `now_slots`.
    StaleSlotCount {
        persisted: u64,
        actual: u64,
    },
}

/// Parse a sidecar, validating everything that could make its contents lie.
/// `now_slots` is the pointer file's current slot count, used to reject a
/// checkpoint that cannot belong to it.
pub fn deserialize(
    buf: &[u8],
    seed: HashSeed,
    expected: &[(u32, DbType)],
    cfg: super::FilterConfig,
    now_slots: u64,
) -> Result<Loaded, LoadReject> {
    if buf.is_empty() {
        return Err(LoadReject::Empty);
    }
    let mut r = Reader::new(buf);

    if r.take(8).ok_or(LoadReject::Truncated)? != MAGIC {
        return Err(LoadReject::BadMagic);
    }
    let ver = r.u16().ok_or(LoadReject::Truncated)?;
    if ver != FORMAT_VER {
        return Err(LoadReject::Version(ver));
    }
    if r.u16().ok_or(LoadReject::Truncated)? as usize != ZONE_ROWS {
        return Err(LoadReject::Geometry);
    }
    if r.u8().ok_or(LoadReject::Truncated)? as usize != ROW_POINTER_RECORD_LEN {
        return Err(LoadReject::RecordLen);
    }
    if r.u8().ok_or(LoadReject::Truncated)? != 0 {
        return Err(LoadReject::Dirty);
    }
    let column_count = r.u16().ok_or(LoadReject::Truncated)? as usize;
    let k0 = r.u64().ok_or(LoadReject::Truncated)?;
    let k1 = r.u64().ok_or(LoadReject::Truncated)?;
    if k0 != seed.k0 || k1 != seed.k1 {
        return Err(LoadReject::Seed);
    }
    let n_slots = r.u64().ok_or(LoadReject::Truncated)?;
    let base_len = r.u64().ok_or(LoadReject::Truncated)? as usize;
    r.skip(SUPERBLOCK_LEN - BASE_LEN_OFFSET - 8)
        .ok_or(LoadReject::Truncated)?;

    // The base image must at least contain the superblock and must fit in what
    // we were handed. A corrupt length here would otherwise decide how much of
    // the file counts as a delta log.
    if base_len < SUPERBLOCK_LEN || base_len > buf.len() {
        return Err(LoadReject::Truncated);
    }

    // A checkpoint may describe fewer slots than exist — rows appended since
    // sit above its frontier and are backfilled. It must never describe *more*.
    //
    // Between checkpoints a pointer file only grows: deletes tombstone in
    // place, and compaction rebuilds and re-checkpoints after truncating. So a
    // shorter file means these bytes belong to a different file that happens to
    // share the name — the table was dropped and recreated, which is what
    // `ALTER TABLE ... ADD COLUMN` does when it rewrites rows.
    //
    // The schema check below catches that too whenever the *indexable* columns
    // change, but adding a column the filter does not index (`BYTEA`, `ARRAY`)
    // leaves them identical. File removal on drop is best-effort, so this is the
    // check that does not depend on it.
    if n_slots > now_slots {
        return Err(LoadReject::StaleSlotCount {
            persisted: n_slots,
            actual: now_slots,
        });
    }

    if column_count != expected.len() {
        return Err(LoadReject::SchemaMismatch);
    }

    let mut columns = Vec::with_capacity(column_count);
    for (exp_wo, exp_ty) in expected.iter() {
        let write_order = r.u32().ok_or(LoadReject::Truncated)?;
        // Compare the raw discriminant rather than decoding it: `from_byte`
        // panics on a value it does not recognise, and these bytes come from a
        // file that may be arbitrarily corrupt. Checking against what the schema
        // expects is also the stronger test — it rejects a valid-but-wrong type
        // as well as an invalid one.
        let db_type_byte = r.u8().ok_or(LoadReject::Truncated)?;
        let next_f = r.u8().ok_or(LoadReject::Truncated)?;
        r.skip(2).ok_or(LoadReject::Truncated)?;

        if write_order != *exp_wo || db_type_byte != exp_ty.to_byte() {
            return Err(LoadReject::SchemaMismatch);
        }
        let db_type = exp_ty.clone();

        let valid_upto = r.u64().ok_or(LoadReject::Truncated)?;
        let unknown_below = r.u64().ok_or(LoadReject::Truncated)?;
        let rows_seen = r.u64().ok_or(LoadReject::Truncated)?;
        let mut regs = [0u8; 256];
        regs.copy_from_slice(r.take(256).ok_or(LoadReject::Truncated)?);

        let gen_count = r.u32().ok_or(LoadReject::Truncated)? as usize;
        r.skip(4).ok_or(LoadReject::Truncated)?;

        let mut gens = Vec::with_capacity(gen_count);
        for _ in 0..gen_count {
            let start_zone = r.u64().ok_or(LoadReject::Truncated)?;
            let f = r.u8().ok_or(LoadReject::Truncated)?;
            r.skip(7).ok_or(LoadReject::Truncated)?;
            let word_count = r.u64().ok_or(LoadReject::Truncated)? as usize;

            // A corrupt length must not drive a huge allocation.
            if f == 0 || word_count % (super::kernels::ZONE_WORDS * f as usize) != 0 {
                return Err(LoadReject::Truncated);
            }
            let words = r.words(word_count).ok_or(LoadReject::Truncated)?;
            gens.push(Generation {
                start_zone,
                f,
                words,
            });
        }

        let zonemap = match r.u8().ok_or(LoadReject::Truncated)? {
            0 => {
                r.skip(7).ok_or(LoadReject::Truncated)?;
                None
            }
            1 => {
                r.skip(7).ok_or(LoadReject::Truncated)?;
                let data_len = r.u64().ok_or(LoadReject::Truncated)? as usize;
                let data = r.take(data_len).ok_or(LoadReject::Truncated)?.to_vec();
                let valid_len = r.u64().ok_or(LoadReject::Truncated)? as usize;
                let valid = r.words(valid_len).ok_or(LoadReject::Truncated)?;
                // A width or length that does not divide cleanly means the file
                // does not describe this column; reject rather than guess.
                Some(
                    ZoneMap::from_parts(db_type.clone(), data, valid)
                        .ok_or(LoadReject::SchemaMismatch)?,
                )
            }
            _ => return Err(LoadReject::Truncated),
        };

        let bloom = match r.u8().ok_or(LoadReject::Truncated)? {
            0 => {
                r.skip(7).ok_or(LoadReject::Truncated)?;
                None
            }
            1 => {
                r.skip(7).ok_or(LoadReject::Truncated)?;
                let n = r.u64().ok_or(LoadReject::Truncated)? as usize;
                let words = r.words32(n).ok_or(LoadReject::Truncated)?;
                Some(SuperBloom::from_words(words).ok_or(LoadReject::SchemaMismatch)?)
            }
            _ => return Err(LoadReject::Truncated),
        };

        columns.push(ColumnFilter::restore(
            write_order,
            db_type,
            seed,
            cfg,
            gens,
            valid_upto,
            unknown_below,
            rows_seen,
            next_f,
            Hll::from_regs(regs),
            zonemap,
            bloom,
        ));
    }

    // Everything past the base is an append log. Replaying it brings the
    // catalog forward one record at a time; the first record that does not
    // parse or does not fit ends the replay and we keep what we have.
    //
    // Stopping early is safe rather than merely tolerable. Base plus a prefix of
    // the deltas is exactly the checkpoint that existed at some earlier moment,
    // and every slot above its frontier is treated as a candidate and
    // backfilled. That is the same position a whole-blob checkpoint leaves us in
    // when rows were appended after it, so there is one recovery path and not
    // two — which is what makes a torn final append a non-event.
    let mut n_slots = n_slots;
    let mut at = base_len;
    let mut applied = 0u32;
    while applied < MAX_DELTAS {
        let Some(rec) = read_delta(buf, &mut at, &mut columns) else {
            break;
        };
        n_slots = rec;
        applied += 1;
    }

    Ok(Loaded {
        columns,
        n_slots,
        valid_len: at as u64,
        deltas: applied,
    })
}

/// Parse and apply one delta record at `*at`, advancing it past the record.
///
/// `None` for anything that is not a whole, well-formed record — end of file, a
/// torn tail, a record for a different schema. `columns` may have been partly
/// updated when a record turns out to be inconsistent partway through; that is
/// still a state some earlier delta produced for the columns ahead of the fault
/// and the base state for the ones behind it, and every column's own
/// `valid_upto` is only advanced at the very end of applying its record, so no
/// column ever claims a frontier it does not have the bits for.
fn read_delta(buf: &[u8], at: &mut usize, columns: &mut [ColumnFilter]) -> Option<u64> {
    let header = buf.get(*at..at.checked_add(DELTA_HEADER_LEN)?)?;
    if &header[0..4] != DELTA_MAGIC {
        return None;
    }
    if u16::from_le_bytes(header[4..6].try_into().ok()?) != FORMAT_VER {
        return None;
    }
    let payload_len = u64::from_le_bytes(header[8..16].try_into().ok()?) as usize;
    let start = at.checked_add(DELTA_HEADER_LEN)?;
    let end = start.checked_add(payload_len)?;
    let payload = buf.get(start..end)?;

    let mut r = Reader::new(payload);
    let n_slots = r.u64()?;
    let column_count = r.u32()? as usize;
    r.skip(4)?;
    if column_count != columns.len() {
        return None;
    }

    for col in columns.iter_mut() {
        let write_order = r.u32()?;
        let db_type_byte = r.u8()?;
        let next_f = r.u8()?;
        r.skip(2)?;
        if write_order != col.write_order || db_type_byte != col.db_type.to_byte() {
            return None;
        }

        let valid_upto = r.u64()?;
        let unknown_below = r.u64()?;
        let rows_seen = r.u64()?;
        let mut regs = [0u8; 256];
        regs.copy_from_slice(r.take(256)?);

        let gen_count = r.u32()? as usize;
        r.skip(4)?;
        let mut gens = Vec::with_capacity(gen_count.min(64));
        for _ in 0..gen_count {
            let start_zone = r.u64()?;
            let f = r.u8()?;
            r.skip(7)?;
            let first_zone = r.u64()?;
            let word_count = r.u64()? as usize;
            if f == 0 || word_count % (super::kernels::ZONE_WORDS * f as usize) != 0 {
                return None;
            }
            gens.push((start_zone, f, first_zone, r.words(word_count)?));
        }

        let zonemap = match r.u8()? {
            0 => {
                r.skip(7)?;
                None
            }
            1 => {
                r.skip(7)?;
                let first_zone = r.u64()?;
                let data_len = r.u64()? as usize;
                let data = r.take(data_len)?.to_vec();
                let valid_len = r.u64()? as usize;
                let valid = r.words(valid_len)?;
                Some((first_zone, data, valid))
            }
            _ => return None,
        };

        let bloom = match r.u8()? {
            0 => {
                r.skip(7)?;
                None
            }
            1 => {
                r.skip(7)?;
                let first_superzone = r.u64()?;
                let n = r.u64()? as usize;
                Some((first_superzone, r.words32(n)?))
            }
            _ => return None,
        };

        if !col.apply_delta(ColumnDelta {
            valid_upto,
            unknown_below,
            rows_seen,
            next_f,
            hll: Hll::from_regs(regs),
            gens,
            zonemap,
            bloom,
        }) {
            return None;
        }
    }

    *at = end;
    Some(n_slots)
}

/// Flip the on-disk `dirty` byte without rewriting the file.
///
/// Called before a delete or update touches rows the last checkpoint already
/// covered. One 8-byte write per statement; never on the insert path.
///
/// Deliberately does **not** guard on `io.get_len()`. `LocalStorageProvider`
/// tracks length in an atomic that `write_data` does not update, so a sidecar
/// created by `write_data` reports length 0 in-process until the provider is
/// reopened — a length guard here silently disables dirty marking and lets a
/// stale checkpoint load. The caller decides whether a checkpoint exists to
/// invalidate; see `TableFilters::mark_disk_dirty`.
pub async fn mark_dirty_on_disk<S: StorageIO>(io: &S) {
    io.write_data(DIRTY_OFFSET, &[1u8]).await;
}
