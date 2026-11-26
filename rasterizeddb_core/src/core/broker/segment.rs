//! A partition's segment files and their sparse index (docs/broker.md §2.3).
//!
//! `<base offset, 20 digits>.seg` holds batches back to back, exactly as they
//! were stamped. `<base>.idx` holds 16-byte entries `(offset - base u32,
//! position u32, base_time i64)`, one per [`INDEX_INTERVAL`] bytes of batches.
//! The index is a hint, written behind the data and never synced. At open it
//! is used only as far as it checks out against the segment, and the newest
//! segment is always scanned from its last good entry to its end.

use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rasterizeddb_mq::batch::{self, BatchView, HEADER};

use crate::core::pos_io::FileExt;

/// Bytes of batches between two index entries.
pub const INDEX_INTERVAL: u64 = 4096;
pub const INDEX_ENTRY: usize = 16;
/// How much of a segment a recovery scan reads per call.
const SCAN_CHUNK: usize = 1 << 20;

pub fn seg_name(base: u64) -> String {
    format!("{base:020}.seg")
}

pub fn idx_name(base: u64) -> String {
    format!("{base:020}.idx")
}

/// The base offset a segment file is named for, or `None` for any other file.
pub fn parse_seg_name(name: &str) -> Option<u64> {
    let stem = name.strip_suffix(".seg")?;
    (stem.len() == 20 && stem.bytes().all(|b| b.is_ascii_digit()))
        .then(|| stem.parse().ok())
        .flatten()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IndexEntry {
    pub rel: u32,
    pub pos: u32,
    pub time: i64,
}

impl IndexEntry {
    fn encode(&self) -> [u8; INDEX_ENTRY] {
        let mut b = [0u8; INDEX_ENTRY];
        b[0..4].copy_from_slice(&self.rel.to_le_bytes());
        b[4..8].copy_from_slice(&self.pos.to_le_bytes());
        b[8..16].copy_from_slice(&self.time.to_le_bytes());
        b
    }
    fn decode(b: &[u8]) -> Self {
        IndexEntry {
            rel: u32::from_le_bytes(b[0..4].try_into().unwrap()),
            pos: u32::from_le_bytes(b[4..8].try_into().unwrap()),
            time: i64::from_le_bytes(b[8..16].try_into().unwrap()),
        }
    }
}

/// One segment file.
pub struct Segment {
    pub base: u64,
    /// One past the last offset appended to it (including bytes not yet
    /// written to the file).
    pub next: u64,
    /// Bytes appended to it, including bytes not yet written.
    pub len: u64,
    /// Bytes known to be in the file (written by the flusher).
    pub written: u64,
    pub first_time: i64,
    pub max_time: i64,
    /// Sparse index, in memory.
    pub index: Vec<IndexEntry>,
    /// Entries of `index` already in the `.idx` file.
    pub index_written: usize,
    /// Bytes since the last index entry.
    pub since_index: u64,
    pub dir: PathBuf,
    pub file: Option<Arc<File>>,
}

impl Segment {
    pub fn new(dir: &Path, base: u64) -> Segment {
        Segment {
            base,
            next: base,
            len: 0,
            written: 0,
            first_time: i64::MAX,
            max_time: i64::MIN,
            index: Vec::new(),
            index_written: 0,
            since_index: INDEX_INTERVAL,
            dir: dir.to_path_buf(),
            file: None,
        }
    }

    pub fn path(&self) -> PathBuf {
        self.dir.join(seg_name(self.base))
    }

    pub fn idx_path(&self) -> PathBuf {
        self.dir.join(idx_name(self.base))
    }

    /// Notes a batch appended at `self.len`, adding an index entry when one
    /// is due.
    #[inline]
    pub fn note_append(&mut self, base_offset: u64, count: u32, bytes: u64, base_time: i64, max_time: i64) {
        if self.since_index >= INDEX_INTERVAL {
            self.index.push(IndexEntry {
                rel: (base_offset - self.base) as u32,
                pos: self.len as u32,
                time: base_time,
            });
            self.since_index = 0;
        }
        self.since_index += bytes;
        self.len += bytes;
        self.next = base_offset + count as u64;
        self.first_time = self.first_time.min(base_time);
        self.max_time = self.max_time.max(max_time);
    }

    /// The file, opened (and created) on first use.
    pub fn file(&mut self) -> io::Result<Arc<File>> {
        if let Some(f) = &self.file {
            return Ok(f.clone());
        }
        let f = Arc::new(OpenOptions::new().read(true).write(true).create(true).truncate(false).open(self.path())?);
        self.file = Some(f.clone());
        Ok(f)
    }

    /// The position of the last indexed batch at or before `offset`.
    pub fn position_for(&self, offset: u64) -> u64 {
        if offset <= self.base {
            return 0;
        }
        let rel = (offset - self.base).min(u32::MAX as u64) as u32;
        match self.index.partition_point(|e| e.rel <= rel) {
            0 => 0,
            i => self.index[i - 1].pos as u64,
        }
    }

    /// The position of the first indexed batch whose time is at or after
    /// `time`, stepping back one entry so a batch straddling it is included.
    pub fn position_for_time(&self, time: i64) -> u64 {
        match self.index.partition_point(|e| e.time < time) {
            0 => 0,
            i => self.index[i - 1].pos as u64,
        }
    }

    /// Writes index entries not in the `.idx` file yet. Not synced: the
    /// index is a hint (module docs).
    pub fn write_index(&mut self) -> io::Result<()> {
        if self.index_written >= self.index.len() {
            return Ok(());
        }
        let mut buf = Vec::with_capacity((self.index.len() - self.index_written) * INDEX_ENTRY);
        for e in &self.index[self.index_written..] {
            buf.extend_from_slice(&e.encode());
        }
        let f = OpenOptions::new().create(true).append(true).open(self.idx_path())?;
        std::io::Write::write_all(&mut &f, &buf)?;
        self.index_written = self.index.len();
        Ok(())
    }
}

/// Reads up to `max` bytes of `file` from `pos`, stopping at `end`.
pub fn read_at(file: &File, pos: u64, end: u64, max: usize) -> io::Result<Vec<u8>> {
    let n = (end.saturating_sub(pos) as usize).min(max);
    let mut buf = vec![0u8; n];
    if n > 0 {
        file.read_exact_at(&mut buf, pos)?;
    }
    Ok(buf)
}

/// A batch's identity, as a recovery scan saw it: what the idempotence table
/// is rebuilt from.
#[derive(Clone, Copy, Debug)]
pub struct ScannedBatch {
    pub base_offset: u64,
    pub count: u32,
    pub producer_id: u64,
    pub epoch: u16,
    pub base_sequence: u32,
}

/// What opening a segment found.
pub struct Opened {
    pub segment: Segment,
    /// The batches the scan walked (the newest segment's tail), oldest first.
    pub scanned: Vec<ScannedBatch>,
    /// Bytes cut from a torn tail.
    pub cut: u64,
}

/// Opens a segment. With `scan_tail` (the newest segment), every batch past
/// the last good index entry is read and both its CRCs checked, and the file
/// is cut at the first that fails: a crash can leave a partial last batch,
/// and nothing after it was acknowledged. A closed segment is walked the same
/// way from its last good entry, by framing only, and damage there is refused
/// rather than cut.
pub fn open(dir: &Path, base: u64, scan_tail: bool) -> io::Result<Opened> {
    let mut seg = Segment::new(dir, base);
    let file = OpenOptions::new().read(true).write(true).open(seg.path())?;
    let file_len = file.metadata()?.len();
    let mut index = load_index(&seg.idx_path(), file_len);
    let good = index
        .iter()
        .position(|e| !entry_checks_out(&file, e, base, file_len))
        .unwrap_or(index.len());
    index.truncate(good);
    // Walk from the last entry that checked out. The entries before it stay;
    // the walk adds the one at `from` (and any after) again.
    let from = index.last().map_or(0, |e| e.pos as u64);
    let mut expected: Option<u64> = None;
    if let Some(last) = index.pop() {
        expected = Some(base + last.rel as u64);
        seg.first_time = index.first().map_or(last.time, |e| e.time);
        seg.max_time = last.time;
    }
    seg.index = index;
    seg.len = from;
    seg.since_index = INDEX_INTERVAL;

    let mut scanned = Vec::new();
    let mut pos = from;
    let mut buf: Vec<u8> = Vec::new();
    let mut buf_start = pos;
    let mut cut = 0u64;
    while pos < file_len {
        let off = (pos - buf_start) as usize;
        let avail = buf.len() - off;
        let need = if avail >= HEADER { batch::claimed_len(&buf[off..]).unwrap_or(0) } else { HEADER };
        if need < HEADER || need > batch::MAX_BATCH || pos + need as u64 > file_len {
            cut = file_len - pos;
            break;
        }
        if avail < need {
            buf.drain(..off);
            buf_start = pos;
            let target = need.max(SCAN_CHUNK).min((file_len - pos) as usize);
            let at = buf.len();
            buf.resize(target, 0);
            file.read_exact_at(&mut buf[at..], pos + at as u64)?;
            continue;
        }
        let here = &buf[off..off + need];
        let framed = if scan_tail {
            batch::check(here, batch::MAX_BATCH, true).is_ok()
        } else {
            here[..4] == batch::MAGIC
        };
        let v = BatchView::trusted(here);
        if !framed || v.base_offset() < base || expected.is_some_and(|e| v.base_offset() != e) {
            cut = file_len - pos;
            break;
        }
        if scan_tail {
            scanned.push(ScannedBatch {
                base_offset: v.base_offset(),
                count: v.count(),
                producer_id: v.producer_id(),
                epoch: v.epoch(),
                base_sequence: v.base_sequence(),
            });
        }
        seg.note_append(v.base_offset(), v.count(), need as u64, v.base_time(), v.max_time());
        expected = Some(v.next_offset());
        pos += need as u64;
    }
    seg.next = expected.unwrap_or(base);
    if cut > 0 {
        if !scan_tail {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{}: damaged batch at byte {pos} of a closed segment", seg.path().display()),
            ));
        }
        log::warn!(
            "broker: {} ends in {cut} bytes that are not a whole batch (a write a crash cut short); cutting them",
            seg.path().display()
        );
        file.set_len(pos)?;
        file.sync_data()?;
    }
    seg.written = seg.len;
    // The index is rewritten from what was rebuilt, so the next open trusts
    // all of it and nothing past a cut.
    let _ = std::fs::remove_file(seg.idx_path());
    seg.index_written = 0;
    seg.file = Some(Arc::new(file));
    Ok(Opened { segment: seg, scanned, cut })
}

fn load_index(path: &Path, file_len: u64) -> Vec<IndexEntry> {
    let Ok(bytes) = std::fs::read(path) else {
        return Vec::new();
    };
    let mut out = Vec::with_capacity(bytes.len() / INDEX_ENTRY);
    let mut last: Option<IndexEntry> = None;
    for chunk in bytes.chunks_exact(INDEX_ENTRY) {
        let e = IndexEntry::decode(chunk);
        // Entries rise in both offset and position, and stay inside the file.
        if e.pos as u64 >= file_len || last.is_some_and(|l| e.rel < l.rel || e.pos <= l.pos) {
            break;
        }
        out.push(e);
        last = Some(e);
    }
    out
}

fn entry_checks_out(file: &File, e: &IndexEntry, base: u64, file_len: u64) -> bool {
    if e.pos as u64 + HEADER as u64 > file_len {
        return false;
    }
    let mut hdr = [0u8; HEADER];
    if file.read_exact_at(&mut hdr, e.pos as u64).is_err() || hdr[..4] != batch::MAGIC {
        return false;
    }
    BatchView::trusted_header(&hdr).0 == base + e.rel as u64
}

trait HeaderPeek {
    fn trusted_header(hdr: &[u8; HEADER]) -> (u64, u32);
}

impl HeaderPeek for BatchView<'_> {
    /// `(base_offset, count)` of a header read alone.
    fn trusted_header(hdr: &[u8; HEADER]) -> (u64, u32) {
        (
            u64::from_le_bytes(hdr[batch::O_BASE_OFFSET..batch::O_BASE_OFFSET + 8].try_into().unwrap()),
            u32::from_le_bytes(hdr[batch::O_COUNT..batch::O_COUNT + 4].try_into().unwrap()),
        )
    }
}

/// The segment files in a partition's directory, by base offset.
pub fn list(dir: &Path) -> io::Result<Vec<u64>> {
    let mut bases = Vec::new();
    match std::fs::read_dir(dir) {
        Ok(rd) => {
            for entry in rd {
                let entry = entry?;
                if let Some(b) = entry.file_name().to_str().and_then(parse_seg_name) {
                    bases.push(b);
                }
            }
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    bases.sort_unstable();
    Ok(bases)
}

/// Removes a segment's files.
pub fn remove(dir: &Path, base: u64) {
    let _ = std::fs::remove_file(dir.join(seg_name(base)));
    let _ = std::fs::remove_file(dir.join(idx_name(base)));
}
