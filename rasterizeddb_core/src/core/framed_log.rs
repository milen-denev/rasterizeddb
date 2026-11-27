//! Framed append-only logs: the record framing, torn-tail detection and the
//! generation rule shared by every log of this shape (the partition map log
//! today; sharding's ownership and shard-map logs next, sharding-plan.md
//! B0.4).
//!
//! # Framing
//!
//! `len:u32 LE ‖ crc32:u32 LE ‖ kind:u8 ‖ payload`. `len` counts `kind ‖
//! payload` (1 to the log's [`Format::max_record_len`]) and the CRC (IEEE,
//! `crc32fast`) covers the same bytes. Each log decides its own kinds and
//! payloads; this module only frames them, and asks the log's
//! [`Format::known_kind`] and [`Format::layout`] when it searches for an
//! intact record after damage.
//!
//! # Torn tails
//!
//! [`scan`] reads records in order and stops at the first whose framing
//! fails: too short to hold its header, an implausible length, running past
//! the end, or a CRC mismatch. When no intact record starts anywhere after
//! it (a partial record, or the zeros of a file a crash extended), that is a
//! torn tail: [`ScanEnd::torn`] says where, and the caller truncates to
//! [`ScanEnd::valid_len`] before appending again. An intact record after the
//! damage means the log lost a record from its middle, which a crash does not
//! do, so the scan refuses the log instead.
//!
//! # Generations
//!
//! A log is compacted by writing a new generation (a higher number) that
//! opens with the whole state and ends with the log's `SEALED` record, then
//! switching appends to it. The live generation is the highest that replays
//! sealed; a higher one that does not is an unfinished compaction.
//! [`live_generation`] applies that rule over any log's replay.

use std::fmt::Display;

/// `len ‖ crc32`.
pub const FRAME_LEN: usize = 8;

/// What the framing needs to know about one log's records.
#[derive(Clone, Copy, Debug)]
pub struct Format {
    /// Names the log in messages: "partition map log".
    pub name: &'static str,
    /// The largest `kind ‖ payload` a record may have; a larger length is a
    /// corrupt one.
    pub max_record_len: u32,
    /// The layout byte every payload opens with.
    pub layout: u8,
    /// Whether a kind byte is one this log writes.
    pub known_kind: fn(u8) -> bool,
}

/// A buffer to write one record's `kind ‖ payload` into after the frame
/// header's room; [`finish`] fills the header.
pub fn begin(kind: u8, layout: u8) -> Vec<u8> {
    let mut buf = vec![0; FRAME_LEN];
    buf.push(kind);
    buf.push(layout);
    buf
}

/// Fills the frame header of `buf` (made by [`begin`]). `Err` with the body
/// length when the record is longer than the format allows, which a scan
/// would read as torn.
pub fn finish(format: &Format, mut buf: Vec<u8>) -> Result<Vec<u8>, usize> {
    let body = buf.len() - FRAME_LEN;
    if body == 0 || body > format.max_record_len as usize {
        return Err(body);
    }
    let crc = crc32fast::hash(&buf[FRAME_LEN..]);
    buf[0..4].copy_from_slice(&(body as u32).to_le_bytes());
    buf[4..8].copy_from_slice(&crc.to_le_bytes());
    Ok(buf)
}

/// One step of reading a log.
pub enum Frame<'a> {
    End,
    Torn(String),
    Record { tag: u8, payload: &'a [u8], next: usize },
}

/// The frame at `at`.
pub fn read_frame<'a>(format: &Format, buf: &'a [u8], at: usize) -> Frame<'a> {
    let rest = &buf[at..];
    if rest.is_empty() {
        return Frame::End;
    }
    if rest.len() < FRAME_LEN {
        return Frame::Torn(format!("{} bytes, too few for a record header", rest.len()));
    }
    let len = u32::from_le_bytes(rest[0..4].try_into().unwrap());
    let crc = u32::from_le_bytes(rest[4..8].try_into().unwrap());
    if len == 0 || len > format.max_record_len {
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
/// length that fits, a known kind, the format's layout byte, then the CRC.
/// The cheap checks come first, and keep a chance CRC match inside a torn
/// record's own bytes far less likely than real damage.
pub fn intact_frame_after(format: &Format, buf: &[u8], at: usize) -> Option<usize> {
    (at + 1..buf.len()).find(|&y| {
        let rest = &buf[y..];
        if rest.len() < FRAME_LEN + 2 {
            return false;
        }
        let len = u32::from_le_bytes(rest[0..4].try_into().unwrap());
        if len < 2 || len > format.max_record_len || rest.len() - FRAME_LEN < len as usize {
            return false;
        }
        let body = &rest[FRAME_LEN..FRAME_LEN + len as usize];
        (format.known_kind)(body[0])
            && body[1] == format.layout
            && crc32fast::hash(body) == u32::from_le_bytes(rest[4..8].try_into().unwrap())
    })
}

/// Where a scan stopped short of the end.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TornTail {
    /// Byte offset of the record that failed its framing.
    pub offset: usize,
    pub reason: String,
    /// Bytes from `offset` to the end, all ignored.
    pub trailing: usize,
}

/// How a [`scan`] ended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScanEnd {
    /// Bytes of whole records read: truncate here before appending.
    pub valid_len: usize,
    /// `Some` when bytes past `valid_len` were ignored.
    pub torn: Option<TornTail>,
}

/// Reads every intact record of `buf` in order, handing each to `record`
/// with its offset, kind byte and payload (layout byte included). Stops at a
/// torn tail; refuses damage followed by an intact record, and any error
/// `record` returns (prefixed with the log's name and the record's offset).
pub fn scan(
    format: &Format,
    buf: &[u8],
    mut record: impl FnMut(usize, u8, &[u8]) -> Result<(), String>,
) -> Result<ScanEnd, String> {
    let mut at = 0;
    loop {
        match read_frame(format, buf, at) {
            Frame::End => return Ok(ScanEnd { valid_len: at, torn: None }),
            Frame::Torn(reason) => {
                if let Some(y) = intact_frame_after(format, buf, at) {
                    return Err(format!(
                        "{}: damaged record at byte {at} ({reason}) followed by a valid record at byte {y}; refusing",
                        format.name
                    ));
                }
                return Ok(ScanEnd {
                    valid_len: at,
                    torn: Some(TornTail {
                        offset: at,
                        reason,
                        trailing: buf.len() - at,
                    }),
                });
            }
            Frame::Record { tag, payload, next } => {
                record(at, tag, payload).map_err(|e| format!("{}, record at byte {at}: {e}", format.name))?;
                at = next;
            }
        }
    }
}

/// What one generation's log says, for [`live_generation`].
pub enum GenerationReplay<E> {
    /// It replays and ends sealed: a finished compaction.
    Sealed,
    /// It replays, but its compaction never reached `SEALED`.
    Unsealed,
    /// It does not replay at all (an unfinished compaction whose pages were
    /// written out of order, or a later version's records).
    Unreadable(E),
}

/// The live generation among `generations` (highest first): the highest
/// that replays sealed, else 0, which never needs a `SEALED` (it is the log
/// as created). Also the higher generations that did not replay at all, with
/// why, so a caller deletes none of them. `replay` is asked for each
/// generation above 0 in turn until one is sealed; an `Err` from it (the file
/// cannot be read) ends the search.
pub fn live_generation<E: Display, F>(generations: &[u64], mut replay: F) -> Result<(u64, Vec<(u64, E)>), String>
where
    F: FnMut(u64) -> Result<GenerationReplay<E>, String>,
{
    let mut unreadable = Vec::new();
    for &g in generations {
        if g == 0 {
            return Ok((0, unreadable));
        }
        match replay(g)? {
            GenerationReplay::Sealed => return Ok((g, unreadable)),
            GenerationReplay::Unsealed => {}
            GenerationReplay::Unreadable(e) => unreadable.push((g, e)),
        }
    }
    Ok((0, unreadable))
}
