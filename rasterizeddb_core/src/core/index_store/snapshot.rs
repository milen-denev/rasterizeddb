//! The `.ixs` snapshot container's codec (docs/persistent-indexes.md §1,
//! §4): a header naming the identity and the sections, the sections, and a
//! footer checksum over everything before it.
//!
//! Pure: sections are opaque bytes here. What a FILTER, INDEX or GIN section
//! holds is B13's and B17's; writing and reading the file is theirs too.
//!
//! # Layout, version 1 (little-endian)
//!
//! ```text
//! magic "RZIXSNP1"          8
//! format_ver u32            4   SNAPSHOT_FORMAT_VER
//! header_len u32            4   everything up to and including header_crc
//! identity                 92   super::identity
//! section_count u32         4   at most MAX_SECTIONS
//! sections, each           24   kind u16 ‖ ordinal u16 ‖ offset u64 ‖ len u64 ‖ crc32c u32
//! header_crc u32            4   crc32c of every header byte before it
//! section bytes …               at their offsets, in ascending order, no overlap
//! footer u64                8   xxh64 (seed 0, core::hash) of every byte before it
//! ```
//!
//! The header checksum is not in the design's list. It is there so the
//! cheap checks at open, and a fan-out open that `pread`s only the FILTER
//! section (§4), can trust the section table without reading the whole file
//! for the footer: a rotted offset would otherwise send the read elsewhere
//! and only the section's own crc32c would notice.
//!
//! `(kind, ordinal)` is unique: the FILTER section has ordinal 0, an INDEX
//! or GIN section the index's position in `for_schema` order.

use super::codec::{FormatError, Reader, crc32c};
use super::identity::{IDENTITY_LEN, Identity};
use crate::core::hash::{Xxh64, xxh64};

/// The container's magic.
pub const SNAPSHOT_MAGIC: [u8; 8] = *b"RZIXSNP1";
/// The container's format version (also `VersionTuple::snapshot_format_ver`).
pub const SNAPSHOT_FORMAT_VER: u32 = 1;
/// One section-table entry.
pub const SECTION_ENTRY_LEN: usize = 2 + 2 + 8 + 8 + 4;
/// The footer.
pub const FOOTER_LEN: usize = 8;
/// More sections than any table has indexes, and few enough that a rotted
/// count cannot make a reader allocate much.
pub const MAX_SECTIONS: u32 = 4096;

const FIXED_HEAD: usize = 8 + 4 + 4 + IDENTITY_LEN + 4;
const WHAT: &str = "index snapshot";

/// What a section holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(u16)]
pub enum SectionKind {
    /// The filter's base image (`persist::serialize_sealed`).
    Filter = 1,
    /// A B-tree, hash or path index.
    Index = 2,
    /// A gin index.
    Gin = 3,
}

impl SectionKind {
    fn from_u16(v: u16) -> Option<SectionKind> {
        match v {
            1 => Some(SectionKind::Filter),
            2 => Some(SectionKind::Index),
            3 => Some(SectionKind::Gin),
            _ => None,
        }
    }
}

/// One section-table entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SectionEntry {
    pub kind: SectionKind,
    pub ordinal: u16,
    pub offset: u64,
    pub len: u64,
    pub crc32c: u32,
}

/// A decoded header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotHeader {
    pub identity: Identity,
    pub sections: Vec<SectionEntry>,
}

/// The header's encoded length for `sections` entries.
pub fn header_len(sections: usize) -> u64 {
    (FIXED_HEAD + sections * SECTION_ENTRY_LEN + 4) as u64
}

impl SnapshotHeader {
    /// The header for `sections`, laid out contiguously right after it, each
    /// with its crc32c computed.
    pub fn layout(identity: Identity, sections: &[(SectionKind, u16, &[u8])]) -> SnapshotHeader {
        let mut offset = header_len(sections.len());
        let sections = sections
            .iter()
            .map(|(kind, ordinal, bytes)| {
                let entry = SectionEntry {
                    kind: *kind,
                    ordinal: *ordinal,
                    offset,
                    len: bytes.len() as u64,
                    crc32c: crc32c(bytes),
                };
                offset += bytes.len() as u64;
                entry
            })
            .collect();
        SnapshotHeader { identity, sections }
    }

    pub fn encode(&self) -> Vec<u8> {
        let len = header_len(self.sections.len());
        let mut out = Vec::with_capacity(len as usize);
        out.extend_from_slice(&SNAPSHOT_MAGIC);
        out.extend_from_slice(&SNAPSHOT_FORMAT_VER.to_le_bytes());
        out.extend_from_slice(&(len as u32).to_le_bytes());
        self.identity.encode_into(&mut out);
        out.extend_from_slice(&(self.sections.len() as u32).to_le_bytes());
        for s in &self.sections {
            out.extend_from_slice(&(s.kind as u16).to_le_bytes());
            out.extend_from_slice(&s.ordinal.to_le_bytes());
            out.extend_from_slice(&s.offset.to_le_bytes());
            out.extend_from_slice(&s.len.to_le_bytes());
            out.extend_from_slice(&s.crc32c.to_le_bytes());
        }
        let crc = crc32c(&out);
        out.extend_from_slice(&crc.to_le_bytes());
        out
    }

    /// Decodes and checks the header at the start of `prefix`, a file of
    /// `file_len` bytes: magic, version, header checksum, and a section table
    /// whose entries are unique, ascending, non-overlapping and end before
    /// the footer. Needs only the header's bytes, not the sections'.
    pub fn decode(prefix: &[u8], file_len: u64) -> Result<SnapshotHeader, FormatError> {
        let mut r = Reader::new(prefix, WHAT);
        if r.array::<8>()? != SNAPSHOT_MAGIC {
            return Err(FormatError::BadMagic { what: WHAT });
        }
        let ver = r.u32()?;
        if ver != SNAPSHOT_FORMAT_VER {
            return Err(FormatError::UnsupportedVersion { what: WHAT, found: ver });
        }
        let len = r.u32()? as u64;
        let malformed = |at: u64, why| FormatError::Malformed { what: WHAT, at, why };
        if len < header_len(0) || len > header_len(MAX_SECTIONS as usize) {
            return Err(malformed(12, "header length out of range"));
        }
        let header = Reader::new(prefix, WHAT).take(len as usize)?;
        let (body, crc) = header.split_at(header.len() - 4);
        if crc32c(body) != u32::from_le_bytes(crc.try_into().expect("four bytes")) {
            return Err(FormatError::BadChecksum { what: WHAT, at: 0 });
        }
        let identity = Identity::read(&mut r)?;
        let count = r.u32()?;
        if count > MAX_SECTIONS || header_len(count as usize) != len {
            return Err(malformed(FIXED_HEAD as u64 - 4, "section count disagrees with the header length"));
        }
        let footer_at = file_len
            .checked_sub(FOOTER_LEN as u64)
            .filter(|f| *f >= len)
            .ok_or(FormatError::Truncated {
                what: WHAT,
                at: file_len,
                need: (len + FOOTER_LEN as u64).saturating_sub(file_len),
            })?;
        let mut sections: Vec<SectionEntry> = Vec::with_capacity(count as usize);
        let mut end = len;
        for _ in 0..count {
            let at = r.pos();
            let kind = SectionKind::from_u16(r.u16()?).ok_or_else(|| malformed(at, "unknown section kind"))?;
            let entry = SectionEntry {
                kind,
                ordinal: r.u16()?,
                offset: r.u64()?,
                len: r.u64()?,
                crc32c: r.u32()?,
            };
            let stop = entry.offset.checked_add(entry.len);
            if entry.offset < end || stop.is_none_or(|s| s > footer_at) {
                return Err(malformed(at, "section out of order, overlapping or past the footer"));
            }
            if sections.iter().any(|s| (s.kind, s.ordinal) == (entry.kind, entry.ordinal)) {
                return Err(malformed(at, "duplicate section"));
            }
            end = stop.expect("checked above");
            sections.push(entry);
        }
        Ok(SnapshotHeader { identity, sections })
    }

    pub fn section(&self, kind: SectionKind, ordinal: u16) -> Option<&SectionEntry> {
        self.sections.iter().find(|s| s.kind == kind && s.ordinal == ordinal)
    }
}

/// Checks one section's bytes against its entry: what a fan-out open does
/// after `pread`ing the FILTER section alone.
pub fn verify_section(entry: &SectionEntry, bytes: &[u8]) -> Result<(), FormatError> {
    if bytes.len() as u64 != entry.len {
        return Err(FormatError::Truncated {
            what: WHAT,
            at: entry.offset + bytes.len() as u64,
            need: entry.len.saturating_sub(bytes.len() as u64),
        });
    }
    if crc32c(bytes) != entry.crc32c {
        return Err(FormatError::BadChecksum {
            what: WHAT,
            at: entry.offset,
        });
    }
    Ok(())
}

/// The footer of a file whose preceding bytes are `preceding`. A streaming
/// writer feeds the same bytes through [`Xxh64::new`]`(0)` instead.
pub fn footer_of(preceding: &[u8]) -> u64 {
    xxh64(preceding, 0)
}

/// A whole snapshot in memory: header, sections, footer.
pub fn encode(identity: Identity, sections: &[(SectionKind, u16, &[u8])]) -> Vec<u8> {
    let mut out = SnapshotHeader::layout(identity, sections).encode();
    for (_, _, bytes) in sections {
        out.extend_from_slice(bytes);
    }
    let mut h = Xxh64::new(0);
    h.update(&out);
    out.extend_from_slice(&h.digest().to_le_bytes());
    out
}

/// A snapshot decoded and fully checked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot<'a> {
    pub header: SnapshotHeader,
    /// Each entry with its bytes, in table order.
    pub sections: Vec<(SectionEntry, &'a [u8])>,
    /// The footer, which a `CLEAN` records.
    pub footer: u64,
}

/// Decodes `bytes` as a whole snapshot and checks everything: the header,
/// every section's crc32c, and the footer.
pub fn decode(bytes: &[u8]) -> Result<Snapshot<'_>, FormatError> {
    let header = SnapshotHeader::decode(bytes, bytes.len() as u64)?;
    let footer_at = bytes.len() - FOOTER_LEN;
    let footer = u64::from_le_bytes(bytes[footer_at..].try_into().expect("eight bytes"));
    if footer_of(&bytes[..footer_at]) != footer {
        return Err(FormatError::BadChecksum {
            what: WHAT,
            at: footer_at as u64,
        });
    }
    let mut sections = Vec::with_capacity(header.sections.len());
    for entry in &header.sections {
        let s = entry.offset as usize;
        let slice = &bytes[s..s + entry.len as usize];
        verify_section(entry, slice)?;
        sections.push((*entry, slice));
    }
    Ok(Snapshot {
        header,
        sections,
        footer,
    })
}

/// A snapshot read once, front to back, and fully checked without holding
/// it: the header, every section's crc32c and the footer xxh64, as
/// [`decode`] checks them. Only the sections `keep` selects are kept.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Streamed {
    pub header: SnapshotHeader,
    pub footer: u64,
    /// The kept sections' entries and bytes, in table order.
    pub kept: Vec<(SectionEntry, Vec<u8>)>,
}

/// How much of the file a streaming check reads at a time.
const STREAM_CHUNK: usize = 1 << 20;

/// Checks a snapshot of `file_len` bytes read from `src`, front to back,
/// as [`decode`] would check it whole (B17: an open no longer buffers the
/// file, §4 "The load"). `src` must yield exactly the file's bytes.
pub fn verify_stream(
    src: &mut impl std::io::Read,
    file_len: u64,
    keep: impl Fn(&SectionEntry) -> bool,
) -> Result<Streamed, StreamError> {
    let mut footer_hash = Xxh64::new(0);
    // The fixed head, for the header's length; then the rest of the header.
    let head = header_len(0) as usize;
    if file_len < (head + FOOTER_LEN) as u64 {
        return Err(FormatError::Truncated { what: WHAT, at: file_len, need: (head + FOOTER_LEN) as u64 - file_len }.into());
    }
    let mut header_bytes = vec![0u8; 16];
    src.read_exact(&mut header_bytes)?;
    let claimed = u32::from_le_bytes(header_bytes[12..16].try_into().expect("four bytes")) as u64;
    let hlen = claimed.clamp(head as u64, header_len(MAX_SECTIONS as usize)).min(file_len - FOOTER_LEN as u64);
    header_bytes.resize(hlen as usize, 0);
    src.read_exact(&mut header_bytes[16..])?;
    let header = SnapshotHeader::decode(&header_bytes, file_len)?;
    footer_hash.update(&header_bytes);
    let mut at = hlen;
    let mut kept = Vec::new();
    let mut buf = vec![0u8; STREAM_CHUNK];
    let footer_at = file_len - FOOTER_LEN as u64;
    // Sections are ascending and do not overlap (the header checked that);
    // the bytes between them, if any, are hashed and skipped.
    let mut feed = |src: &mut dyn std::io::Read, upto: u64, at: &mut u64, sink: &mut dyn FnMut(&[u8])| -> std::io::Result<()> {
        while *at < upto {
            let n = ((upto - *at) as usize).min(STREAM_CHUNK);
            src.read_exact(&mut buf[..n])?;
            footer_hash.update(&buf[..n]);
            sink(&buf[..n]);
            *at += n as u64;
        }
        Ok(())
    };
    for entry in &header.sections {
        feed(src, entry.offset, &mut at, &mut |_| {})?;
        let mut crc = super::codec::crc32c_digest();
        let keep_it = keep(entry);
        let mut bytes = Vec::new();
        if keep_it {
            bytes.reserve_exact(entry.len as usize);
        }
        feed(src, entry.offset + entry.len, &mut at, &mut |b| {
            crc.update(b);
            if keep_it {
                bytes.extend_from_slice(b);
            }
        })?;
        if crc.finalize() != entry.crc32c {
            return Err(FormatError::BadChecksum { what: WHAT, at: entry.offset }.into());
        }
        if keep_it {
            kept.push((*entry, bytes));
        }
    }
    feed(src, footer_at, &mut at, &mut |_| {})?;
    let mut tail = [0u8; FOOTER_LEN];
    src.read_exact(&mut tail)?;
    let footer = u64::from_le_bytes(tail);
    if footer_hash.digest() != footer {
        return Err(FormatError::BadChecksum { what: WHAT, at: footer_at }.into());
    }
    Ok(Streamed { header, footer, kept })
}

/// Why a streaming check failed: the bytes, or reading them.
#[derive(Debug)]
pub enum StreamError {
    Format(FormatError),
    Io(std::io::Error),
}

impl From<FormatError> for StreamError {
    fn from(e: FormatError) -> StreamError {
        StreamError::Format(e)
    }
}

impl From<std::io::Error> for StreamError {
    fn from(e: std::io::Error) -> StreamError {
        // A file shorter than it said is a truncation, like `decode`'s.
        StreamError::Io(e)
    }
}

impl std::fmt::Display for StreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StreamError::Format(e) => write!(f, "{e}"),
            StreamError::Io(e) => write!(f, "{WHAT}: {e}"),
        }
    }
}

/// Reads one section of the snapshot at `path` and checks it against its
/// entry (length and crc32c): what the index load does per section, after
/// the open's proof streamed the whole file once.
pub fn read_section(path: &std::path::Path, entry: &SectionEntry) -> std::io::Result<Vec<u8>> {
    use std::os::unix::fs::FileExt;
    let file = std::fs::File::open(path)?;
    let mut bytes = vec![0u8; entry.len as usize];
    file.read_exact_at(&mut bytes, entry.offset)?;
    verify_section(entry, &bytes).map_err(std::io::Error::other)?;
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::super::codec::{digest_hex, from_hex, to_hex};
    use super::super::identity::sample;
    use super::*;
    use crate::core::index::vectors::{HistoryRow, check_history};

    const FILTER: &[u8] = b"filter section";
    const INDEX0: &[u8] = &[0x00, 0x01, 0x02, 0x03, 0xfe, 0xff];
    const GIN1: &[u8] = b"";

    fn sections() -> Vec<(SectionKind, u16, &'static [u8])> {
        vec![
            (SectionKind::Filter, 0, FILTER),
            (SectionKind::Index, 0, INDEX0),
            (SectionKind::Gin, 1, GIN1),
        ]
    }

    /// [`encode`]`(sample(), sections())` under `SNAPSHOT_FORMAT_VER` 1,
    /// frozen.
    const SNAPSHOT_V1: &str = "525a4958534e503101000000bc00000001001032547698badcfeefcdab896745\
        230107000000000000009058170000000000d0cc5f0300000000c0388b170000\
        000001000000010000000100000002000000020000000100000005000df0feca\
        efbeadde78563412dec0ad0b0300000001000000bc000000000000000e000000\
        000000004505184e02000000ca000000000000000600000000000000711f4e27\
        03000100d00000000000000000000000000000000000000097d2101866696c74\
        65722073656374696f6e00010203feff26a382f0f09e4b4e";

    fn v1_digest() -> u64 {
        digest_hex(&[SNAPSHOT_V1])
    }

    /// `SNAPSHOT_FORMAT_VER`'s frozen vectors, oldest first.
    const HISTORY: &[HistoryRow] = &[(1, v1_digest, 0x0c17_2d82_0bff_0370)];

    #[test]
    fn a_snapshot_encodes_to_its_frozen_bytes() {
        assert_eq!(
            to_hex(&encode(sample(), &sections())),
            to_hex(&from_hex(SNAPSHOT_V1)),
            "the snapshot container layout moved"
        );
    }

    #[test]
    fn snapshot_vectors_are_pinned_to_snapshot_format_ver() {
        check_history("snapshot::SNAPSHOT_FORMAT_VER", HISTORY, SNAPSHOT_FORMAT_VER);
    }

    #[test]
    fn a_snapshot_round_trips_and_its_header_reads_alone() {
        let bytes = encode(sample(), &sections());
        let snap = decode(&bytes).unwrap();
        assert_eq!(snap.header.identity, sample());
        let got: Vec<(SectionKind, u16, &[u8])> = snap.sections.iter().map(|(e, b)| (e.kind, e.ordinal, *b)).collect();
        assert_eq!(got, sections());
        assert_eq!(snap.footer, footer_of(&bytes[..bytes.len() - FOOTER_LEN]));
        // The header alone, as the cheap checks and a fan-out open read it.
        let hlen = header_len(3) as usize;
        let header = SnapshotHeader::decode(&bytes[..hlen], bytes.len() as u64).unwrap();
        let filter = header.section(SectionKind::Filter, 0).unwrap();
        let s = filter.offset as usize;
        verify_section(filter, &bytes[s..s + filter.len as usize]).unwrap();
        assert!(verify_section(filter, &bytes[s..s + filter.len as usize - 1]).is_err());
        assert_eq!(header.section(SectionKind::Gin, 0), None);
        // No sections at all.
        let empty = encode(sample(), &[]);
        assert!(decode(&empty).unwrap().sections.is_empty());
    }

    #[test]
    fn every_truncation_and_every_corrupted_byte_is_an_error() {
        let bytes = encode(sample(), &sections());
        for n in 0..bytes.len() {
            assert!(decode(&bytes[..n]).is_err(), "{n} bytes");
        }
        for i in 0..bytes.len() {
            for flip in [0x01u8, 0x80, 0xff] {
                let mut bad = bytes.clone();
                bad[i] ^= flip;
                assert!(decode(&bad).is_err(), "byte {i} ^ {flip:#x} was accepted");
            }
        }
        // The header alone, as a fan-out open reads it, catches its own rot.
        let hlen = header_len(3) as usize;
        for i in 0..hlen {
            let mut bad = bytes.clone();
            bad[i] ^= 0x01;
            assert!(
                SnapshotHeader::decode(&bad[..hlen], bytes.len() as u64).is_err(),
                "header byte {i} was accepted without the footer"
            );
        }
        let mut longer = bytes.clone();
        longer.insert(bytes.len() - FOOTER_LEN, 0);
        assert!(decode(&longer).is_err(), "a byte inserted before the footer");
    }

    /// The streaming check accepts exactly what `decode` accepts, keeps the
    /// sections asked for, and refuses every truncation and every corrupted
    /// byte, as the whole-file decode does (B17).
    #[test]
    fn the_streaming_check_agrees_with_decode() {
        let stream = |b: &[u8], keep: &dyn Fn(&SectionEntry) -> bool| {
            verify_stream(&mut std::io::Cursor::new(b), b.len() as u64, keep)
        };
        let bytes = encode(sample(), &sections());
        let whole = decode(&bytes).unwrap();
        let got = stream(&bytes, &|e| e.kind != SectionKind::Gin).unwrap();
        assert_eq!(got.header, whole.header);
        assert_eq!(got.footer, whole.footer);
        let kept: Vec<(SectionKind, u16, &[u8])> = got.kept.iter().map(|(e, b)| (e.kind, e.ordinal, b.as_slice())).collect();
        assert_eq!(kept, vec![(SectionKind::Filter, 0, FILTER), (SectionKind::Index, 0, INDEX0)]);
        for n in 0..bytes.len() {
            assert!(stream(&bytes[..n], &|_| true).is_err(), "{n} bytes");
        }
        for i in 0..bytes.len() {
            let mut bad = bytes.clone();
            bad[i] ^= 0x01;
            assert!(stream(&bad, &|_| false).is_err(), "byte {i} was accepted");
        }
        // A section larger than a chunk, and a gap-free multi-chunk file.
        let big = vec![0x5au8; (STREAM_CHUNK * 2) + 17];
        let bytes = encode(sample(), &[(SectionKind::Index, 0, &big), (SectionKind::Index, 1, b"x")]);
        let got = stream(&bytes, &|e| e.ordinal == 0).unwrap();
        assert_eq!(got.kept.len(), 1);
        assert_eq!(got.kept[0].1, big);
        let mut bad = bytes.clone();
        bad[STREAM_CHUNK + 100] ^= 0x80;
        assert!(stream(&bad, &|_| false).is_err());
        // One section read back alone.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.ixs");
        std::fs::write(&path, &bytes).unwrap();
        let entry = got.header.sections[1];
        assert_eq!(read_section(&path, &entry).unwrap(), b"x");
        let mut wrong = entry;
        wrong.crc32c ^= 1;
        assert!(read_section(&path, &wrong).is_err());
    }

    #[test]
    fn a_well_checksummed_header_that_breaks_a_rule_is_refused() {
        let id = sample();
        let mut h = SnapshotHeader::layout(id, &sections());
        let file_len = |h: &SnapshotHeader| {
            h.sections.iter().map(|s| s.offset + s.len).max().unwrap_or(header_len(h.sections.len())) + FOOTER_LEN as u64
        };
        let ok_len = file_len(&h);
        assert!(SnapshotHeader::decode(&h.encode(), ok_len).is_ok());
        // Overlap.
        h.sections[1].offset -= 1;
        assert!(SnapshotHeader::decode(&h.encode(), ok_len).is_err());
        // Duplicate (kind, ordinal).
        let mut h = SnapshotHeader::layout(id, &sections());
        h.sections[2].kind = SectionKind::Index;
        h.sections[2].ordinal = 0;
        assert!(SnapshotHeader::decode(&h.encode(), ok_len).is_err());
        // Past the footer.
        let h = SnapshotHeader::layout(id, &sections());
        assert!(SnapshotHeader::decode(&h.encode(), ok_len - 1).is_err());
        // A length that overflows.
        let mut h = SnapshotHeader::layout(id, &sections());
        h.sections[2].len = u64::MAX;
        assert!(SnapshotHeader::decode(&h.encode(), ok_len).is_err());
    }

    #[test]
    fn arbitrary_bytes_never_panic() {
        let mut rng = fastrand::Rng::with_seed(0x5a9);
        let good = encode(sample(), &sections());
        for round in 0..20_000 {
            let mut b: Vec<u8> = (0..rng.usize(0..400)).map(|_| rng.u8(..)).collect();
            match round % 3 {
                // A valid magic and version, so the parser goes further.
                0 if b.len() >= 12 => {
                    b[..8].copy_from_slice(&SNAPSHOT_MAGIC);
                    b[8..12].copy_from_slice(&SNAPSHOT_FORMAT_VER.to_le_bytes());
                }
                // A good file with a few random bytes overwritten.
                1 => {
                    b = good.clone();
                    for _ in 0..rng.usize(1..4) {
                        let i = rng.usize(0..b.len());
                        b[i] = rng.u8(..);
                    }
                }
                _ => {}
            }
            let _ = decode(&b);
            let _ = SnapshotHeader::decode(&b, rng.u64(..));
        }
    }
}
