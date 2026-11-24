//! Compression of stored values (docs/compression.md).
//!
//! Nothing is compressed unless a table asks for it. Two ways to ask:
//!
//! - **A column** (`COMPRESSION zstd(9)` on a variable-width column: text,
//!   JSONB, BYTEA, arrays, ranges, …). Each of its values of at least
//!   [`MIN_VALUE_BYTES`] is stored as its own zstd frame when that saves at
//!   least an eighth of the bytes.
//! - **A row** (`WITH (row_compression = 'zstd(9)')`). Every variable-width
//!   value of a row is stored in one frame, the row's *tail*, so a row of
//!   many short strings compresses where none of them would alone.
//!
//! Either way the fixed-width region of a row stays as it is. The prefilter,
//! the cell folds, top-N key cells and the in-place UPDATE read its cells at
//! `position + offset`, straight from storage.
//!
//! # Where the mark is
//!
//! A string's 12-byte header in a row is a `u64` offset from the row's start
//! and a `u32` length. Every reader refuses a length over 64 MiB, so bits
//! 27–31 of that length are never set in a row a build without this can
//! read; such a build errors on the marks below, never reads a frame as
//! text, and the directory's `compressed_values_v1` flag refuses it at open
//! anyway.
//!
//! - Bit 31 ([`COMPRESSED`]): the stored bytes are a frame of this value; the
//!   length under the flags is the frame's.
//! - Bit 30 ([`IN_TAIL`]): the value is in the row's tail frame. The length
//!   under the flags is the value's own; the offset's high 32 bits are where
//!   the frame starts in the row, its low 32 bits where the value starts in
//!   the frame's decompressed bytes.
//!
//! # A frame
//!
//! `codec u8 | raw length u32 | [dictionary u32] | zstd frame`, little-endian.
//! The codec is [`CODEC_ZSTD`], or [`CODEC_ZSTD_DICT`] followed by the
//! dictionary's id (zstd's own dictionary id). The raw length is checked
//! against the 64 MiB cap before anything is allocated for it, and every
//! zstd frame carries a content checksum: damaged bytes are an error, never
//! another value.
//!
//! # Dictionaries
//!
//! `ALTER TABLE … RECOMPRESS` trains a zstd dictionary per compressed column
//! (and one for the row tail) from the values the table holds, and keeps it
//! when it shrinks them by a tenth more than compressing without one. A
//! dictionary is held in the table's `_schema_ZD.db` sidecar and named by its
//! id in a marker record (`type_modifier::DICTIONARY_MARKER`). Read, it is
//! registered process-wide by id ([`register`]), which is how a reader that
//! knows only the row's bytes finds it.
//!
//! # Only two readers
//!
//! `RowPointer::fetch_row` / `fetch_row_reuse_async` and
//! `processor::column_scan` are the only code that reads a string header
//! (`row_pointer::string_cell`), and both decode through [`decompress`].
//! Everything above them sees the value as it was written.

use std::io;
use std::sync::atomic::{AtomicU64, Ordering};

use rclite::Arc;

use crate::memory_pool::MemoryBlock;

/// Bit 31 of a string header's length: the stored bytes are a frame.
pub const COMPRESSED: u32 = 1 << 31;

/// Bit 30 of a string header's length: the value is in the row's tail frame.
pub const IN_TAIL: u32 = 1 << 30;

/// The length under the flags. Only the two flags are masked: a stray bit
/// from 27 to 29 is still a length past [`MAX_VALUE_BYTES`], which a reader
/// refuses as the misread header it is.
pub const LENGTH_MASK: u32 = !(COMPRESSED | IN_TAIL);

/// The largest value a reader returns, compressed or not: what
/// `row_pointer::string_extent` has always refused above.
pub const MAX_VALUE_BYTES: usize = 64 * 1024 * 1024;

/// zstd, no dictionary: `codec u8 | raw length u32`.
pub const CODEC_ZSTD: u8 = 1;
/// zstd with a dictionary: `codec u8 | raw length u32 | dictionary id u32`.
pub const CODEC_ZSTD_DICT: u8 = 2;

/// The preamble of a frame without a dictionary.
pub const PREAMBLE_BYTES: usize = 5;
/// The preamble of a frame with one.
pub const DICT_PREAMBLE_BYTES: usize = 9;

/// A value shorter than this is stored as it is: a zstd frame of a few dozen
/// bytes is rarely shorter than its input plus the preamble, and trying costs
/// a compression call per value. A row's tail is held to it as a whole.
pub const MIN_VALUE_BYTES: usize = 64;

/// The level `zstd` without one means.
pub const DEFAULT_LEVEL: i32 = 3;

/// The levels a column or table may declare: zstd's 1 (fastest) to 22
/// (smallest).
pub const MIN_LEVEL: i32 = 1;
/// See [`MIN_LEVEL`].
pub const MAX_LEVEL: i32 = 22;

/// Whether this build can compress and decompress. A build without the
/// `storage_compression` feature stores every value as it is and refuses a
/// frame it reads.
pub const ENABLED: bool = cfg!(feature = "storage_compression");

// ------------------------------------------------------------------ policy

/// A trained zstd dictionary, by zstd's own id for it.
pub struct Dictionary {
    pub id: u32,
    pub bytes: Vec<u8>,
}

impl std::fmt::Debug for Dictionary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Dictionary({:#010x}, {} bytes)", self.id, self.bytes.len())
    }
}

/// How one column, or a row's tail, is compressed.
#[derive(Debug, Clone)]
pub struct Codec {
    /// A zstd level, [`MIN_LEVEL`]..=[`MAX_LEVEL`].
    pub level: i32,
    /// The dictionary new frames use, if one was trained and kept.
    pub dictionary: Option<Arc<Dictionary>>,
}

/// What one column asks for, by its `write_order`.
#[derive(Debug, Clone)]
pub struct ColumnCompression {
    pub write_order: u64,
    pub codec: Codec,
}

/// What a table's writes did, since it opened: for `SHOW TABLE STATS`.
#[derive(Debug, Default)]
pub struct Counters {
    /// Values stored as their own frame.
    pub values_compressed: AtomicU64,
    /// Row tails stored as one frame.
    pub tails_compressed: AtomicU64,
    /// The bytes those values and tails held.
    pub raw_bytes: AtomicU64,
    /// The bytes their frames took.
    pub stored_bytes: AtomicU64,
}

impl Counters {
    pub fn record(&self, tail: bool, raw: usize, stored: usize) {
        if tail {
            self.tails_compressed.fetch_add(1, Ordering::Relaxed);
        } else {
            self.values_compressed.fetch_add(1, Ordering::Relaxed);
        }
        self.raw_bytes.fetch_add(raw as u64, Ordering::Relaxed);
        self.stored_bytes.fetch_add(stored as u64, Ordering::Relaxed);
    }
}

/// A table's compression: [`crate::core::row::schema::TableSchema::compression`],
/// rebuilt from its marker records by `refresh_compression`. Empty for a
/// table that asked for none, which is every table unless one did.
#[derive(Debug, Clone, Default)]
pub struct StoragePolicy {
    /// The variable-width columns that compress their values.
    pub columns: Vec<ColumnCompression>,
    /// The row tail's codec, when the table compresses whole tails. A column
    /// codec is then not used: the tail frame holds every value.
    pub row: Option<Codec>,
    /// Shared by every clone, so a partitioned table counts once.
    pub counters: Arc<Counters>,
}

impl StoragePolicy {
    /// Whether nothing is compressed: the write path makes no copy.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty() && self.row.is_none()
    }

    /// The codec of the column at `write_order`, if it compresses.
    #[inline]
    pub fn column(&self, write_order: u64) -> Option<&Codec> {
        self.columns.iter().find(|c| c.write_order == write_order).map(|c| &c.codec)
    }
}

/// The level of `write_order` in `policy`, if that column compresses.
#[inline]
pub fn level_of(policy: &StoragePolicy, write_order: u64) -> Option<i32> {
    policy.column(write_order).map(|c| c.level)
}

// ------------------------------------------------------------------ syntax

/// Parses the method of a `COMPRESSION` clause or a `row_compression`
/// option: `zstd`, `zstd(9)`, `lz4`, `pglz` (both from `pg_dump`, stored as
/// zstd at the default level), or `default` / `none` / `uncompressed`.
/// `Ok(None)` for one that stores values as they are.
pub fn parse_method(text: &str) -> Result<Option<i32>, String> {
    let t = text.trim().trim_matches('\'').trim_matches('"').trim();
    let lower: String = t.to_ascii_lowercase().chars().filter(|c| !c.is_whitespace()).collect();
    let (name, arg) = match lower.find('(') {
        Some(open) => {
            let Some(inner) = lower[open + 1..].strip_suffix(')') else {
                return Err(format!("invalid compression method \"{t}\""));
            };
            (&lower[..open], Some(inner))
        }
        None => (lower.as_str(), None),
    };
    match (name, arg) {
        ("default" | "none" | "uncompressed", None) => Ok(None),
        ("lz4" | "pglz" | "zstd", None) => Ok(Some(DEFAULT_LEVEL)),
        ("zstd", Some(level)) => {
            let level: i32 = level
                .parse()
                .map_err(|_| format!("invalid compression level \"{level}\""))?;
            if !(MIN_LEVEL..=MAX_LEVEL).contains(&level) {
                return Err(format!(
                    "compression level {level} is out of range: zstd levels are {MIN_LEVEL} to {MAX_LEVEL}"
                ));
            }
            Ok(Some(level))
        }
        _ => Err(format!("invalid compression method \"{t}\"")),
    }
}

/// The `COMPRESSION method` clause among a column definition's constraint
/// tokens (split on whitespace, so `zstd (9)` and `zstd( 9 )` arrive in
/// pieces): `None` when there is none, else what [`parse_method`] makes of
/// the method.
pub fn clause_in(tokens: &[&str]) -> Option<Result<Option<i32>, String>> {
    let at = tokens.iter().position(|t| t.eq_ignore_ascii_case("COMPRESSION"))?;
    let mut method = String::new();
    let mut depth = 0i32;
    for token in &tokens[at + 1..] {
        // The method's name, then a level group if one follows it.
        if !method.is_empty() && depth == 0 && !token.starts_with('(') {
            break;
        }
        let token = token.trim_end_matches(',');
        depth += token.matches('(').count() as i32 - token.matches(')').count() as i32;
        method.push_str(token);
        if depth <= 0 && method.contains('(') {
            break;
        }
    }
    if method.is_empty() {
        return Some(Err("syntax error at end of input: COMPRESSION needs a method".to_string()));
    }
    Some(parse_method(&method))
}

/// What a table's storage options (`CREATE TABLE … WITH (…)`, `ALTER TABLE
/// … SET (…)`) ask of compression: `Some(level)` / `Some(None)` when they
/// name `row_compression` (a method, or `none`), `None` when they do not.
/// PostgreSQL's own options (`fillfactor`, `autovacuum_*`, `toast.*`, …)
/// describe machinery this engine does not have and are accepted and
/// ignored; any other is refused, as PostgreSQL refuses it.
///
/// `inner` is the text between the group's parentheses.
pub fn row_compression_option(inner: &str) -> Result<Option<Option<i32>>, String> {
    const IGNORED: &[&str] = &[
        "fillfactor",
        "parallel_workers",
        "user_catalog_table",
        "vacuum_index_cleanup",
        "vacuum_truncate",
        "log_autovacuum_min_duration",
        "oids",
        "toast_tuple_target",
        "security_barrier",
        "security_invoker",
        "check_option",
    ];
    let mut found = None;
    for option in crate::core::rql::lexer_ct::split_definitions(inner) {
        let option = option.trim();
        if option.is_empty() {
            continue;
        }
        let (key, value) = match option.split_once('=') {
            Some((k, v)) => (k.trim(), Some(v.trim())),
            None => (option, None),
        };
        let key = key.trim_matches('"').to_ascii_lowercase();
        if key == "row_compression" {
            // `RESET (row_compression)` names it with no value: none.
            found = Some(match value {
                Some(v) => parse_method(v)?,
                None => None,
            });
        } else if !(IGNORED.contains(&key.as_str()) || key.starts_with("autovacuum_") || key.starts_with("toast.")) {
            return Err(format!("unrecognized parameter \"{key}\""));
        }
    }
    Ok(found)
}

/// The method as `pg_attribute.attcompression` and `pg_class.reloptions`
/// show it: `zstd` at the default level, else `zstd(n)`.
pub fn method_name(level: Option<i32>) -> Option<String> {
    level.map(|l| if l == DEFAULT_LEVEL { "zstd".to_string() } else { format!("zstd({l})") })
}

// --------------------------------------------------------------- the codec

/// The frame `raw` is stored as under `codec`, or `None` when it is stored as
/// it is: too short, not shortened by an eighth, or this build cannot
/// compress.
pub fn compress(raw: &[u8], codec: &Codec) -> Option<Vec<u8>> {
    if raw.len() < MIN_VALUE_BYTES || raw.len() > MAX_VALUE_BYTES {
        return None;
    }
    imp::compress(raw, codec)
}

/// The value `frame` holds, into a block of its raw length.
pub fn decompress(frame: &[u8]) -> io::Result<MemoryBlock> {
    let (body, raw_len, dictionary) = preamble(frame)?;
    let mut out = crate::memory_pool::MEMORY_POOL.acquire(raw_len);
    if raw_len > 0 || !body.is_empty() {
        imp::decompress_into(body, dictionary, out.into_slice_mut())?;
    }
    Ok(out)
}

/// The zstd bytes of `frame`, its raw length (checked against
/// [`MAX_VALUE_BYTES`] before anything is sized by it) and its dictionary.
pub fn preamble(frame: &[u8]) -> io::Result<(&[u8], usize, Option<u32>)> {
    let Some(&codec) = frame.first() else {
        return Err(invalid("compressed value of 0 bytes".to_string()));
    };
    let need = match codec {
        CODEC_ZSTD => PREAMBLE_BYTES,
        CODEC_ZSTD_DICT => DICT_PREAMBLE_BYTES,
        other => return Err(invalid(format!("unknown compression codec {other} in a stored value"))),
    };
    if frame.len() < need {
        return Err(invalid(format!("compressed value of {} bytes is shorter than its preamble", frame.len())));
    }
    let raw_len = u32::from_le_bytes([frame[1], frame[2], frame[3], frame[4]]) as usize;
    if raw_len > MAX_VALUE_BYTES {
        return Err(invalid(format!("compressed value claims {raw_len} bytes (too large)")));
    }
    let dictionary = (codec == CODEC_ZSTD_DICT).then(|| u32::from_le_bytes([frame[5], frame[6], frame[7], frame[8]]));
    Ok((&frame[need..], raw_len, dictionary))
}

fn invalid(message: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

// ------------------------------------------------------------ dictionaries

static REGISTRY: std::sync::LazyLock<parking_lot::RwLock<std::collections::HashMap<u32, Arc<Dictionary>>>> =
    std::sync::LazyLock::new(Default::default);

/// Makes `dictionary` findable by its id, for every reader in the process.
/// Refused when another dictionary already holds the id: a frame naming it
/// could then be decoded with the wrong one (zstd would refuse it, but the
/// table could not be read).
pub fn register(dictionary: Arc<Dictionary>) -> Result<(), String> {
    let mut registry = REGISTRY.write();
    match registry.get(&dictionary.id) {
        Some(held) if held.bytes != dictionary.bytes => Err(format!(
            "compression dictionary {:#010x} is already held by another dictionary",
            dictionary.id
        )),
        Some(_) => Ok(()),
        None => {
            registry.insert(dictionary.id, dictionary);
            Ok(())
        }
    }
}

/// The registered dictionary of `id`.
pub fn dictionary(id: u32) -> Option<Arc<Dictionary>> {
    REGISTRY.read().get(&id).cloned()
}

/// A dictionary trained from `samples` for compression at `level`, kept
/// (and registered) only when it shrinks them by at least a tenth more than
/// compressing them without one. `None` for too few samples to train on, a
/// dictionary that does not pay, or a build that cannot compress.
pub fn train(samples: &[&[u8]], level: i32) -> Option<Arc<Dictionary>> {
    const MIN_SAMPLES: usize = 32;
    const MIN_SAMPLE_BYTES: usize = 8 * 1024;
    let total: usize = samples.iter().map(|s| s.len()).sum();
    if samples.len() < MIN_SAMPLES || total < MIN_SAMPLE_BYTES {
        return None;
    }
    let bytes = imp::train(samples, (total / 10).clamp(1024, 64 * 1024))?;
    let id = imp::dictionary_id(&bytes)?;
    let dictionary = Arc::new(Dictionary { id, bytes });
    if register(dictionary.clone()).is_err() {
        return None;
    }
    // Judged on the samples themselves, each as a value would be stored.
    let plain = Codec { level, dictionary: None };
    let with = Codec { level, dictionary: Some(dictionary.clone()) };
    let size = |codec: &Codec| -> usize {
        samples.iter().take(512).map(|s| imp::compress(s, codec).map_or(s.len(), |f| f.len())).sum()
    };
    let (without, using) = (size(&plain), size(&with));
    (using * 10 <= without * 9).then_some(dictionary)
}

/// The dictionaries of a table: `<t>_schema_ZD.db`, records of
/// `magic | id u32 | length u32 | crc32 u32 | bytes`, appended through the
/// log like every sidecar. Variable-length, so `torn_tail` does not cut it;
/// a record that does not verify (zeros a power cut left) is stepped over by
/// looking for the next magic, so a record appended after it still loads.
pub mod sidecar {
    use super::{Arc, Dictionary};
    use crate::core::storage_providers::traits::StorageIO;

    /// The sidecar's suffix after the schema file's stem.
    pub const SUFFIX: &str = "ZD.db";
    const MAGIC: [u8; 4] = *b"RZD1";
    const HEADER: usize = 16;

    /// The record holding `dictionary`.
    pub fn record(dictionary: &Dictionary) -> Vec<u8> {
        let mut out = Vec::with_capacity(HEADER + dictionary.bytes.len());
        out.extend_from_slice(&MAGIC);
        out.extend_from_slice(&dictionary.id.to_le_bytes());
        out.extend_from_slice(&(dictionary.bytes.len() as u32).to_le_bytes());
        out.extend_from_slice(&crc32fast::hash(&dictionary.bytes).to_le_bytes());
        out.extend_from_slice(&dictionary.bytes);
        out
    }

    /// Every whole, verifying record in `bytes`.
    pub fn parse(bytes: &[u8]) -> Vec<Dictionary> {
        let mut out = Vec::new();
        let mut at = 0usize;
        while at + HEADER <= bytes.len() {
            if bytes[at..at + 4] != MAGIC {
                at += 1;
                continue;
            }
            let id = u32::from_le_bytes(bytes[at + 4..at + 8].try_into().unwrap());
            let len = u32::from_le_bytes(bytes[at + 8..at + 12].try_into().unwrap()) as usize;
            let crc = u32::from_le_bytes(bytes[at + 12..at + 16].try_into().unwrap());
            let end = at + HEADER + len;
            match bytes.get(at + HEADER..end) {
                Some(body) if crc32fast::hash(body) == crc && id != 0 => {
                    out.push(Dictionary { id, bytes: body.to_vec() });
                    at = end;
                }
                _ => at += 1,
            }
        }
        out
    }

    /// Reads a table's dictionaries and registers each.
    pub async fn load<S: StorageIO>(schema_io: &Arc<S>) {
        let io = schema_io.create_new(crate::core::torn_tail::sidecar_name(&**schema_io, SUFFIX)).await;
        let len = io.get_len().await;
        if len == 0 {
            return;
        }
        let mut bytes = vec![0u8; len as usize];
        if let Err(e) = io.read_data_into_buffer(&mut 0, &mut bytes).await {
            log::error!("Could not read the compression dictionaries of {}: {}", schema_io.get_name(), e);
            return;
        }
        for dictionary in parse(&bytes) {
            if let Err(e) = super::register(Arc::new(dictionary)) {
                log::error!("{} ({})", e, schema_io.get_name());
            }
        }
    }

    /// Adds `dictionary`'s record to the statement's sidecar writes.
    pub fn stage<S: StorageIO>(schema_io: &S, writes: &mut crate::core::torn_tail::SidecarWrites, dictionary: &Dictionary) {
        writes.add(crate::core::torn_tail::sidecar_name(schema_io, SUFFIX), &record(dictionary));
    }
}

#[cfg(feature = "storage_compression")]
mod imp {
    use std::cell::RefCell;
    use std::io;

    use super::{invalid, Codec, CODEC_ZSTD, CODEC_ZSTD_DICT, DICT_PREAMBLE_BYTES, PREAMBLE_BYTES};

    /// Contexts kept per thread, by dictionary (0 for none) and level:
    /// creating one allocates its tables, and loading a dictionary into it
    /// digests the dictionary, which would otherwise be paid per value.
    const CACHED: usize = 8;

    thread_local! {
        static COMPRESSORS: RefCell<Vec<(u32, i32, zstd::bulk::Compressor<'static>)>> = const { RefCell::new(Vec::new()) };
        static DECOMPRESSORS: RefCell<Vec<(u32, zstd::bulk::Decompressor<'static>)>> = const { RefCell::new(Vec::new()) };
    }

    pub(super) fn compress(raw: &[u8], codec: &Codec) -> Option<Vec<u8>> {
        // Stored only when it saves an eighth, preamble included.
        let budget = raw.len() - raw.len() / 8;
        let (dictionary_id, preamble) = match &codec.dictionary {
            Some(d) => (d.id, DICT_PREAMBLE_BYTES),
            None => (0, PREAMBLE_BYTES),
        };
        if budget <= preamble {
            return None;
        }
        let mut out = vec![0u8; preamble + zstd::zstd_safe::compress_bound(raw.len())];
        out[0] = if codec.dictionary.is_some() { CODEC_ZSTD_DICT } else { CODEC_ZSTD };
        out[1..5].copy_from_slice(&(raw.len() as u32).to_le_bytes());
        if codec.dictionary.is_some() {
            out[5..9].copy_from_slice(&dictionary_id.to_le_bytes());
        }
        let written = COMPRESSORS.with(|cell| {
            let mut cached = cell.borrow_mut();
            let at = match cached.iter().position(|(d, l, _)| *d == dictionary_id && *l == codec.level) {
                Some(at) => at,
                None => {
                    let mut c = match &codec.dictionary {
                        Some(d) => zstd::bulk::Compressor::with_dictionary(codec.level, &d.bytes).ok()?,
                        None => zstd::bulk::Compressor::new(codec.level).ok()?,
                    };
                    c.set_parameter(zstd::zstd_safe::CParameter::ChecksumFlag(true)).ok()?;
                    if cached.len() == CACHED {
                        cached.remove(0);
                    }
                    cached.push((dictionary_id, codec.level, c));
                    cached.len() - 1
                }
            };
            cached[at].2.compress_to_buffer(raw, &mut out[preamble..]).ok()
        })?;
        out.truncate(preamble + written);
        (out.len() <= budget).then_some(out)
    }

    pub(super) fn decompress_into(body: &[u8], dictionary: Option<u32>, out: &mut [u8]) -> io::Result<()> {
        let id = dictionary.unwrap_or(0);
        DECOMPRESSORS.with(|cell| {
            let mut cached = cell.borrow_mut();
            let at = match cached.iter().position(|(d, _)| *d == id) {
                Some(at) => at,
                None => {
                    let d = match dictionary {
                        Some(id) => {
                            let Some(found) = super::dictionary(id) else {
                                return Err(invalid(format!(
                                    "a stored value names compression dictionary {id:#010x}, which no open table holds"
                                )));
                            };
                            zstd::bulk::Decompressor::with_dictionary(&found.bytes)?
                        }
                        None => zstd::bulk::Decompressor::new()?,
                    };
                    if cached.len() == CACHED {
                        cached.remove(0);
                    }
                    cached.push((id, d));
                    cached.len() - 1
                }
            };
            let n = cached[at].1.decompress_to_buffer(body, &mut out[..])?;
            if n != out.len() {
                return Err(invalid(format!(
                    "compressed value decoded to {n} bytes, its preamble says {}",
                    out.len()
                )));
            }
            Ok(())
        })
    }

    pub(super) fn train(samples: &[&[u8]], max_size: usize) -> Option<Vec<u8>> {
        zstd::dict::from_samples(samples, max_size).ok()
    }

    pub(super) fn dictionary_id(bytes: &[u8]) -> Option<u32> {
        zstd::zstd_safe::get_dict_id(bytes).map(|id| id.get())
    }
}

#[cfg(not(feature = "storage_compression"))]
mod imp {
    use std::io;

    use super::Codec;

    pub(super) fn compress(_raw: &[u8], _codec: &Codec) -> Option<Vec<u8>> {
        None
    }

    pub(super) fn decompress_into(_body: &[u8], _dictionary: Option<u32>, _out: &mut [u8]) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "a stored value is compressed and this build has no `storage_compression` feature",
        ))
    }

    pub(super) fn train(_samples: &[&[u8]], _max_size: usize) -> Option<Vec<u8>> {
        None
    }

    pub(super) fn dictionary_id(_bytes: &[u8]) -> Option<u32> {
        None
    }
}
