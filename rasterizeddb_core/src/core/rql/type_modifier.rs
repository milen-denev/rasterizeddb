//! A column's declared precision and scale — the `(p, s)` of `NUMERIC(p, s)`.
//!
//! `DbType` says a column holds a decimal; it does not say how many digits the
//! author asked for. That was discarded at `CREATE TABLE`, with two visible
//! consequences: `10.5000` in a `NUMERIC(10,4)` column read back as `10.5`,
//! because nothing knew four places had been asked for; and every decimal was
//! stored at one fixed scale, so a `NUMERIC(38,16)` column could not hold the
//! sixteen places it declares.
//!
//! Recorded in a sidecar rather than in the field record. The field record is a
//! fixed 153-byte stride with every byte spoken for, so widening it would
//! change where every existing table's columns are read from. A sidecar is
//! additive: a table written before this existed simply has none, and a column
//! with no recorded scale keeps the engine's default — which is what every row
//! already on disk was written with.

use std::io;

use rclite::Arc;

use crate::core::storage_providers::traits::StorageIO;

/// Bytes one persisted modifier occupies: the column name, precision, scale,
/// and a tombstone. Fixed-stride, so the file reads as an array.
pub const TYPE_MODIFIER_SIZE: u64 = 129 + 2 + 2 + 1;

/// The scale a decimal column uses when none was declared.
///
/// The value every decimal already on disk was written with, which is why an
/// unrecorded column must keep it: reinterpreting stored digits at a different
/// scale would silently change what they mean.
pub const DEFAULT_DECIMAL_SCALE: u16 = crate::core::value_codec::DECIMAL_SCALE as u16;

/// The most decimal places a column may declare.
///
/// A value is held in an `i128`, which carries 38 decimal digits in total. A
/// scale beyond this leaves no room for an integer part.
pub const MAX_DECIMAL_SCALE: u16 = 30;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypeModifier {
    pub column: String,
    /// Total digits, as declared. Recorded for reporting; the storage width is
    /// the same either way.
    pub precision: u16,
    /// Digits after the decimal point.
    pub scale: u16,
    pub is_deleted: bool,
}

impl TypeModifier {
    pub fn into_vec(&self) -> Vec<u8> {
        let mut out = vec![0u8; TYPE_MODIFIER_SIZE as usize];
        let name = self.column.as_bytes();
        let len = name.len().min(128);
        out[..len].copy_from_slice(&name[..len]);
        out[129..131].copy_from_slice(&self.precision.to_le_bytes());
        out[131..133].copy_from_slice(&self.scale.to_le_bytes());
        out[133] = if self.is_deleted { 1 } else { 0 };
        out
    }

    pub fn from_vec(bytes: &[u8]) -> io::Result<Self> {
        if bytes.len() < TYPE_MODIFIER_SIZE as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "type modifier record is too short",
            ));
        }
        let end = bytes[..129].iter().position(|b| *b == 0).unwrap_or(129);
        let column = String::from_utf8_lossy(&bytes[..end]).to_string();
        Ok(TypeModifier {
            column,
            precision: u16::from_le_bytes([bytes[129], bytes[130]]),
            scale: u16::from_le_bytes([bytes[131], bytes[132]]),
            is_deleted: bytes[133] != 0,
        })
    }
}

fn sidecar_name<S: StorageIO>(schema_io: &Arc<S>) -> String {
    format!("{}_{}", schema_io.get_name().replace(".db", ""), "TYP.db")
}

/// Reads a table's declared type modifiers.
pub async fn load<S: StorageIO>(schema_io: Arc<S>) -> Vec<TypeModifier> {
    let io = schema_io.create_new(sidecar_name(&schema_io)).await;
    let len = io.get_len().await;

    let mut out: Vec<TypeModifier> = Vec::new();
    if len < TYPE_MODIFIER_SIZE {
        return out;
    }

    let mut buffer = vec![0u8; TYPE_MODIFIER_SIZE as usize];
    for i in 0..(len / TYPE_MODIFIER_SIZE) {
        let mut position = i * TYPE_MODIFIER_SIZE;
        if io
            .read_data_into_buffer(&mut position, &mut buffer)
            .await
            .is_err()
        {
            break;
        }
        // A record of zeros is an append a power cut kept the length of and
        // dropped (`torn_tail::is_unwritten`), never a declaration.
        if crate::core::torn_tail::is_unwritten(&buffer) {
            continue;
        }
        match TypeModifier::from_vec(&buffer) {
            // The newest record for a column wins, so altering a type is an
            // append rather than a rewrite.
            Ok(entry) => {
                out.retain(|e| !e.column.eq_ignore_ascii_case(&entry.column));
                if !entry.is_deleted {
                    out.push(entry);
                }
            }
            Err(e) => log::error!("Skipping unreadable type modifier record: {}", e),
        }
    }
    out
}

/// Appends a modifier, superseding any earlier one for the same column.
pub async fn append<S: StorageIO>(schema_io: Arc<S>, entry: &TypeModifier) -> io::Result<()> {
    let mut writes = crate::core::torn_tail::SidecarWrites::default();
    stage(&*schema_io, &mut writes, entry)?;
    writes.commit(&*schema_io).await
}

/// Adds `entry`'s record to `writes`, the statement's sidecar records, which
/// commit as one transaction (`torn_tail::SidecarWrites`): one log sync for
/// all of them, and after a cut all of them or none.
pub fn stage<S: StorageIO>(
    schema_io: &S,
    writes: &mut crate::core::torn_tail::SidecarWrites,
    entry: &TypeModifier,
) -> io::Result<()> {
    writes.add(crate::core::torn_tail::sidecar_name(schema_io, "TYP.db"), &entry.into_vec());
    Ok(())
}

/// The prefix of a record that is no modifier but says a column was
/// declared `character(n)` (or `char`, `bpchar`). Recorded in this sidecar,
/// keyed by a name no column can have, so every reader that looks a modifier
/// up by its column never meets one.
///
/// The record's `precision` and `scale` carry the declared length `n`, low
/// and high sixteen bits ([`TextTypmod::Bpchar`]); [`BPCHAR_UNBOUNDED`] marks
/// a bare `bpchar`. A record with both zero was written by a binary that
/// stored `character(n)` as plain text and kept no length (see
/// [`is_legacy_bpchar`]): such a column keeps text semantics, as it had.
pub const BPCHAR_MARKER: &str = "\u{1}bpchar:";

/// The prefix of a record saying a column was declared `character
/// varying(n)`, its length carried as [`BPCHAR_MARKER`]'s is. A binary that
/// predates it never reads it: no lookup by column name meets it, and the
/// column was unbounded text there, as it always was.
pub const VARCHAR_MARKER: &str = "\u{1}varchar:";

/// The prefix of a record saying a `timestamp(p)`, `timestamptz(p)`,
/// `time(p)`, `timetz(p)` or `interval(p)` column rounds what it is given to
/// `p` fractional digits, stored as `p + 1` as the lengths are.
pub const PRECISION_MARKER: &str = "\u{1}precision:";

/// The stored length of a bare `bpchar` column, which has none.
pub const BPCHAR_UNBOUNDED: u32 = u32::MAX;

/// The longest `character(n)` / `character varying(n)` PostgreSQL accepts.
pub const MAX_TEXT_LENGTH: u32 = 10_485_760;

/// A declared text length: `character(n)` (blank-padded, compared without
/// its trailing spaces) or `character varying(n)`.
///
/// A `character(n)` value is *stored* without its trailing spaces. That is
/// the form every comparison PostgreSQL makes on the type reads (each of its
/// operators strips them first), so two values the type calls equal are
/// equal bytes: a key, a hash, an index entry, a row lock and a filter all
/// treat `'b'`, `'b '` and `'b  '` as the one value they are. The padding is
/// put back where the value leaves as itself: the wire (`DbType::BPCHAR`),
/// a `::char(n)` in the select list, `LIKE`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TextTypmod {
    /// `character(n)`; `n` is 0 for a bare `bpchar`, which has no length.
    Bpchar(u32),
    /// `character varying(n)`; `n` is 0 for a `varchar` with none, which
    /// is otherwise text but compares blank-padded with `character(n)`.
    Varchar(u32),
    /// The fractional-second digits of `timestamp(p)`, `timestamptz(p)`,
    /// `time(p)`, `timetz(p)` or `interval(p)` (0 to 5; six is the type's
    /// own and needs no record): a value assigned is rounded to them.
    Precision(u32),
}

/// A [`TextTypmod`] in four bytes, as a `SchemaField` carries it: the kind in
/// the top two bits, the length plus one below. An enum of three `u32`
/// variants is eight, and every field of every inline schema copy paid for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PackedTypmod(std::num::NonZeroU32);

impl PackedTypmod {
    const VALUE: u32 = (1 << 30) - 1;

    pub fn pack(t: TextTypmod) -> Option<PackedTypmod> {
        let (kind, n) = match t {
            TextTypmod::Bpchar(n) => (0u32, n),
            TextTypmod::Varchar(n) => (1, n),
            TextTypmod::Precision(p) => (2, p),
        };
        // Lengths are at most `MAX_TEXT_LENGTH`, far below the 30 bits.
        let value = n.checked_add(1).filter(|v| *v <= Self::VALUE)?;
        std::num::NonZeroU32::new((kind << 30) | value).map(PackedTypmod)
    }

    pub fn unpack(self) -> TextTypmod {
        let raw = self.0.get();
        let n = (raw & Self::VALUE) - 1;
        match raw >> 30 {
            0 => TextTypmod::Bpchar(n),
            1 => TextTypmod::Varchar(n),
            _ => TextTypmod::Precision(n),
        }
    }
}

impl TextTypmod {
    /// The type's name as PostgreSQL writes it in an error.
    pub fn type_name(&self) -> String {
        match self {
            TextTypmod::Bpchar(0) => "bpchar".to_string(),
            TextTypmod::Bpchar(n) => format!("character({n})"),
            TextTypmod::Varchar(0) => "character varying".to_string(),
            TextTypmod::Varchar(n) => format!("character varying({n})"),
            TextTypmod::Precision(p) => format!("precision({p})"),
        }
    }

    /// The declared length, when there is one.
    pub fn length(&self) -> Option<u32> {
        match self {
            TextTypmod::Bpchar(0) | TextTypmod::Varchar(0) | TextTypmod::Precision(_) => None,
            TextTypmod::Bpchar(n) | TextTypmod::Varchar(n) => Some(*n),
        }
    }

    /// `atttypmod` as the wire carries it: the length plus four, or -1.
    pub fn wire_typmod(&self) -> i32 {
        self.length().map_or(-1, |n| n.saturating_add(4).min(i32::MAX as u32) as i32)
    }

    /// `text` assigned to a column of this type, as PostgreSQL's assignment
    /// does it: past the length is an error (22001) unless every character
    /// past it is a space, which is then cut. A `character(n)` value comes
    /// back in its stored form, its trailing spaces gone.
    pub fn assign<'a>(&self, text: &'a str) -> Result<&'a str, String> {
        match *self {
            TextTypmod::Bpchar(n) => {
                let stripped = text.trim_end_matches(' ');
                if n > 0 && exceeds(stripped, n) {
                    return Err(format!("value too long for type character({n})"));
                }
                Ok(stripped)
            }
            TextTypmod::Varchar(0) | TextTypmod::Precision(_) => Ok(text),
            TextTypmod::Varchar(n) => match char_boundary(text, n) {
                None => Ok(text),
                Some(at) if text[at..].bytes().all(|b| b == b' ') => Ok(&text[..at]),
                Some(_) => Err(format!("value too long for type character varying({n})")),
            },
        }
    }

    /// `text::type(n)`, an explicit cast: cut to the length, never an error
    /// (`'abcd'::varchar(2)` is `ab`). A `character(n)` value comes back in
    /// its stored form.
    pub fn cast<'a>(&self, text: &'a str) -> &'a str {
        let cut = match self.length().and_then(|n| char_boundary(text, n)) {
            Some(at) => &text[..at],
            None => text,
        };
        match self {
            TextTypmod::Bpchar(_) => cut.trim_end_matches(' '),
            TextTypmod::Varchar(_) | TextTypmod::Precision(_) => cut,
        }
    }

    /// The stored form of a `character(n)` value as it is shown: blank-padded
    /// to the length. Anything else, and a bare `bpchar`, as it is.
    pub fn pad<'a>(&self, text: &'a str) -> std::borrow::Cow<'a, str> {
        match *self {
            TextTypmod::Bpchar(n) if n > 0 => pad_to(text, n),
            _ => std::borrow::Cow::Borrowed(text),
        }
    }

    /// The marker record for a column of this type.
    pub fn marker(&self, column: &str) -> TypeModifier {
        let (prefix, stored) = match *self {
            TextTypmod::Bpchar(0) => (BPCHAR_MARKER, BPCHAR_UNBOUNDED),
            TextTypmod::Bpchar(n) => (BPCHAR_MARKER, n),
            TextTypmod::Varchar(0) => (VARCHAR_MARKER, BPCHAR_UNBOUNDED),
            TextTypmod::Varchar(n) => (VARCHAR_MARKER, n),
            // Plus one, so `(0)` is a record and not the zeros of none.
            TextTypmod::Precision(p) => (PRECISION_MARKER, p + 1),
        };
        TypeModifier {
            column: format!("{prefix}{column}"),
            precision: (stored & 0xFFFF) as u16,
            scale: (stored >> 16) as u16,
            is_deleted: false,
        }
    }
}

/// A stored text value of `field` held to the field's declared length, as an
/// assignment does it ([`TextTypmod::assign`]): `Ok(None)` when it is already
/// in its stored form, `Ok(Some(bytes))` when it changed, `Err` (22001) when
/// it is too long. Anything but a text column with a declared length, and
/// bytes that are not UTF-8, are left alone.
pub fn assign_bytes(field: &crate::core::row::schema::SchemaField, bytes: &[u8]) -> Result<Option<Vec<u8>>, String> {
    let Some(typmod) = field.text_typmod() else { return Ok(None) };
    if let TextTypmod::Precision(p) = typmod {
        // A time value assigned to a column of declared precision is rounded
        // to it, as PostgreSQL's assignment does.
        let rounded = round_time_bytes(&field.db_type, bytes, p);
        return Ok(rounded.filter(|r| r.as_slice() != bytes));
    }
    if field.db_type != crate::core::db_type::DbType::STRING {
        return Ok(None);
    }
    let Ok(text) = std::str::from_utf8(bytes) else { return Ok(None) };
    let held = typmod.assign(text)?;
    Ok((held.len() != text.len()).then(|| held.as_bytes().to_vec()))
}

/// Rounds a count of microseconds to `precision` fractional digits of a
/// second, half away from zero — PostgreSQL's `AdjustTimestampForTypmod`,
/// `AdjustTimeForTypmod` and `AdjustIntervalForTypmod`.
pub fn round_micros(micros: i64, precision: u32) -> i64 {
    if precision >= 6 {
        return micros;
    }
    let scale = 10i64.pow(6 - precision);
    let offset = scale / 2;
    if micros >= 0 {
        micros.saturating_add(offset) / scale * scale
    } else {
        -((micros.saturating_neg().saturating_add(offset)) / scale * scale)
    }
}

/// The stored bytes of a time value of `db_type` rounded to `precision`
/// fractional digits, or `None` when `db_type` is no time type or the bytes
/// are not one. A timestamp is rounded on PostgreSQL's epoch, 2000-01-01,
/// where its half-away-from-zero meets the sign: `1999-12-31 23:59:59.5` to
/// no digits is `23:59:59`.
pub fn round_time_bytes(db_type: &crate::core::db_type::DbType, bytes: &[u8], precision: u32) -> Option<Vec<u8>> {
    use crate::core::db_type::DbType;
    match db_type {
        DbType::DATETIME => {
            let raw = <[u8; 8]>::try_from(bytes).ok()?;
            let offset = crate::core::value_codec::PG_EPOCH_OFFSET_US;
            let pg = i64::from_le_bytes(raw).wrapping_sub(offset);
            Some(round_micros(pg, precision).wrapping_add(offset).to_le_bytes().to_vec())
        }
        DbType::TIME => {
            let raw = <[u8; 8]>::try_from(bytes).ok()?;
            Some(round_micros(i64::from_be_bytes(raw), precision).to_be_bytes().to_vec())
        }
        DbType::TIMETZ | DbType::INTERVAL if bytes.len() == if *db_type == DbType::TIMETZ { 12 } else { 16 } => {
            let micros = i64::from_be_bytes(bytes[..8].try_into().ok()?);
            let mut out = round_micros(micros, precision).to_be_bytes().to_vec();
            out.extend_from_slice(&bytes[8..]);
            Some(out)
        }
        _ => None,
    }
}

/// The fractional-second precision a declared time type, as written, gives
/// its column: `timestamp(0)`, `timestamp(3) with time zone`, `timestamptz(2)`,
/// `time(1)`, `timetz(2)`, `interval(0)`. `None` without a modifier and for
/// six or more (the type's own precision, nothing to round).
pub fn parse_time_precision(declared: &str) -> Option<Result<TextTypmod, String>> {
    let text = declared.trim();
    let open = text.find('(')?;
    let close = text[open..].find(')')? + open;
    let base = text[..open].trim().to_ascii_lowercase();
    if !matches!(base.as_str(), "timestamp" | "timestamptz" | "time" | "timetz" | "interval") {
        return None;
    }
    let digits = text[open + 1..close].trim();
    match digits.parse::<i64>() {
        Ok(p) if p < 0 => Some(Err(format!("{base}({p}) precision must not be negative"))),
        Ok(p) if p >= 6 => None,
        Ok(p) => Some(Ok(TextTypmod::Precision(p as u32))),
        Err(_) => Some(Err(format!("invalid type modifier '{digits}'"))),
    }
}

/// `text` blank-padded to `n` characters (PostgreSQL counts characters, not
/// bytes).
pub fn pad_to(text: &str, n: u32) -> std::borrow::Cow<'_, str> {
    let have = text.chars().count();
    if have >= n as usize {
        return std::borrow::Cow::Borrowed(text);
    }
    let mut out = String::with_capacity(text.len() + n as usize - have);
    out.push_str(text);
    out.extend(std::iter::repeat_n(' ', n as usize - have));
    std::borrow::Cow::Owned(out)
}

/// Whether `text` holds more than `n` characters.
fn exceeds(text: &str, n: u32) -> bool {
    char_boundary(text, n).is_some()
}

/// The byte offset of character `n` of `text`, when it has more than `n`.
fn char_boundary(text: &str, n: u32) -> Option<usize> {
    if text.len() <= n as usize {
        return None;
    }
    text.char_indices().nth(n as usize).map(|(at, _)| at)
}

/// The length a marker record carries: `None` for one an older binary wrote
/// (no length), `Some(0)` for a bare `bpchar`.
fn marker_length(m: &TypeModifier) -> Option<u32> {
    let stored = u32::from(m.precision) | (u32::from(m.scale) << 16);
    match stored {
        0 => None,
        BPCHAR_UNBOUNDED => Some(0),
        n => Some(n),
    }
}

/// The record marking `column` as `character(n)` with no length, as a binary
/// that kept none wrote it. Kept for the tests that build such a table.
pub fn bpchar_marker(column: &str) -> TypeModifier {
    TypeModifier { column: format!("{BPCHAR_MARKER}{column}"), precision: 0, scale: 0, is_deleted: false }
}

/// The record retiring `column`'s `character(n)` marker: what `DROP COLUMN`,
/// `ALTER COLUMN … TYPE` to another type and a re-`ADD` under another type
/// leave, so a later check on a column of that name is judged by the
/// column's own type, not by the one a former column of the name had.
pub fn bpchar_marker_retired(column: &str) -> TypeModifier {
    TypeModifier { is_deleted: true, ..bpchar_marker(column) }
}

/// The record retiring `column`'s `character varying(n)` marker.
pub fn varchar_marker_retired(column: &str) -> TypeModifier {
    TypeModifier { column: format!("{VARCHAR_MARKER}{column}"), precision: 0, scale: 0, is_deleted: true }
}

/// The live marker of `prefix` for `column`: the last record of the key,
/// unless that is a tombstone, as the sidecar is read (`load`).
fn live_marker<'a>(prefix: &str, column: &str, modifiers: &'a [TypeModifier]) -> Option<&'a TypeModifier> {
    modifiers
        .iter()
        .rev()
        .find(|m| m.column.strip_prefix(prefix).is_some_and(|c| c.eq_ignore_ascii_case(column)))
        .filter(|m| !m.is_deleted)
}

/// The declared length `modifiers` record for `column`, when its type has
/// one this engine enforces. A `character(n)` column an older binary created
/// has none (see [`is_legacy_bpchar`]).
pub fn text_typmod_of(column: &str, modifiers: &[TypeModifier]) -> Option<TextTypmod> {
    if let Some(m) = live_marker(BPCHAR_MARKER, column, modifiers) {
        return marker_length(m).map(TextTypmod::Bpchar);
    }
    if let Some(m) = live_marker(PRECISION_MARKER, column, modifiers) {
        let stored = u32::from(m.precision) | (u32::from(m.scale) << 16);
        return stored.checked_sub(1).map(TextTypmod::Precision);
    }
    live_marker(VARCHAR_MARKER, column, modifiers)
        .and_then(marker_length)
        .map(TextTypmod::Varchar)
}

/// Whether `column` is a `character(n)` column an older binary created: it
/// stored the values as text, as written, and kept no length, so it keeps
/// text semantics (and its checks stay unenforced) until `ALTER COLUMN …
/// TYPE character(n)` converts it.
pub fn is_legacy_bpchar(column: &str, modifiers: &[TypeModifier]) -> bool {
    live_marker(BPCHAR_MARKER, column, modifiers).is_some_and(|m| marker_length(m).is_none())
}

/// Whether `modifiers` mark `column` as `character(n)`, with a length or not.
pub fn is_marked_bpchar(column: &str, modifiers: &[TypeModifier]) -> bool {
    live_marker(BPCHAR_MARKER, column, modifiers).is_some()
}

/// The columns `modifiers` mark as `character(n)` an older binary created
/// ([`is_legacy_bpchar`]).
pub fn legacy_bpchar_columns(modifiers: &[TypeModifier]) -> Vec<String> {
    modifiers
        .iter()
        .filter(|m| !m.is_deleted && marker_length(m).is_none())
        .filter_map(|m| m.column.strip_prefix(BPCHAR_MARKER).map(str::to_string))
        .collect()
}

/// The records that give `column` the declared text length `typmod` (none
/// for any other type): its marker, and the retirement of a marker of the
/// other kind a former type, or a former column of the name, left.
pub fn text_typmod_records(column: &str, typmod: Option<TextTypmod>, modifiers: &[TypeModifier]) -> Vec<TypeModifier> {
    let mut out = Vec::new();
    let bpchar_now = matches!(typmod, Some(TextTypmod::Bpchar(_)));
    let varchar_now = matches!(typmod, Some(TextTypmod::Varchar(_)));
    let precision_now = matches!(typmod, Some(TextTypmod::Precision(_)));
    if !bpchar_now && live_marker(BPCHAR_MARKER, column, modifiers).is_some() {
        out.push(bpchar_marker_retired(column));
    }
    if !varchar_now && live_marker(VARCHAR_MARKER, column, modifiers).is_some() {
        out.push(varchar_marker_retired(column));
    }
    if !precision_now && live_marker(PRECISION_MARKER, column, modifiers).is_some() {
        out.push(TypeModifier { column: format!("{PRECISION_MARKER}{column}"), precision: 0, scale: 0, is_deleted: true });
    }
    if let Some(t) = typmod {
        out.push(t.marker(column));
    }
    out
}

/// Whether a declared type, as written (`CHAR(3)`, `character`, `bpchar`,
/// `char`), is `character(n)`; `character varying` is not.
pub fn is_bpchar_type(declared: &str) -> bool {
    matches!(parse_text_typmod(declared), Some(Ok(TextTypmod::Bpchar(_))))
}

/// The length a declared type, as written, gives a text column:
/// `character(3)`, `char`, `nchar(2)`, `bpchar` (no length), `varchar(10)`,
/// `character varying(3)`, `varchar` (no length). `None` for any other type
/// and an array of either.
pub fn parse_text_typmod(declared: &str) -> Option<Result<TextTypmod, String>> {
    let text = declared.trim();
    if text.contains('[') {
        return None;
    }
    let (name, args) = match text.find('(') {
        Some(open) => {
            let close = text.rfind(')')?;
            if close < open || !text[close + 1..].trim().is_empty() {
                return None;
            }
            (&text[..open], Some(&text[open + 1..close]))
        }
        None => (text, None),
    };
    let words: Vec<String> = name.split_whitespace().map(|w| w.trim_matches('"').to_ascii_lowercase()).collect();
    let words: Vec<&str> = words.iter().map(String::as_str).collect();
    let varying = match words.as_slice() {
        ["varchar"] | ["character", "varying"] | ["char", "varying"] | ["nvarchar"]
        | ["national", "character", "varying"] | ["national", "char", "varying"] => true,
        ["bpchar"] | ["char"] | ["character"] | ["nchar"] | ["national", "character"] | ["national", "char"] => false,
        _ => return None,
    };
    let length = match args {
        None => None,
        Some(a) => {
            let a = a.trim();
            match a.parse::<i64>() {
                Ok(n) if n < 1 => {
                    return Some(Err(format!(
                        "length for type {} must be at least 1",
                        if varying { "varchar" } else { "char" }
                    )));
                }
                Ok(n) if n > i64::from(MAX_TEXT_LENGTH) => {
                    return Some(Err(format!(
                        "length for type {} cannot exceed {}",
                        if varying { "varchar" } else { "char" },
                        MAX_TEXT_LENGTH
                    )));
                }
                Ok(n) => Some(n as u32),
                Err(_) => return Some(Err(format!("invalid type modifier '{a}'"))),
            }
        }
    };
    Some(Ok(match (varying, length) {
        (true, Some(n)) => TextTypmod::Varchar(n),
        // Recorded with no length: text that compares blank-padded with
        // `character(n)`, where text compares exactly.
        (true, None) => TextTypmod::Varchar(0),
        (false, Some(n)) => TextTypmod::Bpchar(n),
        // `character` alone is `character(1)`; `bpchar` alone has no length.
        (false, None) if words == ["bpchar"] => TextTypmod::Bpchar(0),
        (false, None) => TextTypmod::Bpchar(1),
    }))
}

/// The scale a decimal column stores at.
///
/// An unrecorded column keeps the engine's default, which is what its rows were
/// written with.
pub fn scale_of(column: &str, modifiers: &[TypeModifier]) -> u16 {
    modifiers
        .iter()
        .find(|m| m.column.eq_ignore_ascii_case(column) && !m.is_deleted)
        .map(|m| m.scale)
        .unwrap_or(DEFAULT_DECIMAL_SCALE)
}

/// The precision a column declared, when it declared one.
pub fn precision_of(column: &str, modifiers: &[TypeModifier]) -> Option<u32> {
    modifiers
        .iter()
        .find(|m| m.column.eq_ignore_ascii_case(column) && !m.is_deleted)
        .map(|m| m.precision as u32)
}

/// Splits `NUMERIC(38, 16)` into its precision and scale.
///
/// Returns `None` when the type carries no modifier, which is the ordinary
/// case and means the column keeps the default scale.
pub fn parse_numeric_modifier(declared: &str) -> Option<Result<(u16, u16), String>> {
    let text = declared.trim();
    let open = text.find('(')?;
    let close = text.rfind(')')?;
    if close < open {
        return None;
    }
    let name = text[..open].trim().to_ascii_uppercase();
    if !matches!(name.as_str(), "NUMERIC" | "DECIMAL") {
        return None;
    }

    let mut parts = text[open + 1..close].split(',');
    let precision = parts.next()?.trim().parse::<u16>().ok()?;
    let scale = match parts.next() {
        Some(s) => match s.trim().parse::<u16>() {
            Ok(v) => v,
            Err(_) => return Some(Err(format!("'{}' is not a scale", s.trim()))),
        },
        // `NUMERIC(10)` means ten digits and no fractional part.
        None => 0,
    };
    if parts.next().is_some() {
        return Some(Err(format!("'{}' has too many type modifiers", text)));
    }
    if scale > precision {
        return Some(Err(format!(
            "NUMERIC scale {} is larger than its precision {}",
            scale, precision
        )));
    }
    if scale > MAX_DECIMAL_SCALE {
        return Some(Err(format!(
            "a scale of {} is more than this engine stores: a decimal is held in 38 digits, \
             so at most {} may be fractional",
            scale, MAX_DECIMAL_SCALE
        )));
    }
    Some(Ok((precision, scale)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_record_round_trips() {
        let entry = TypeModifier {
            column: "quantity".to_string(),
            precision: 38,
            scale: 16,
            is_deleted: false,
        };
        let decoded = TypeModifier::from_vec(&entry.into_vec()).expect("decodes");
        assert_eq!(decoded, entry);
    }

    #[test]
    fn a_declared_modifier_is_read() {
        assert_eq!(
            parse_numeric_modifier("NUMERIC(38, 16)"),
            Some(Ok((38, 16)))
        );
        assert_eq!(parse_numeric_modifier("numeric(10,2)"), Some(Ok((10, 2))));
        // No fractional part declared means none.
        assert_eq!(parse_numeric_modifier("DECIMAL(10)"), Some(Ok((10, 0))));
        // No modifier at all is not an error; the column keeps the default.
        assert!(parse_numeric_modifier("NUMERIC").is_none());
        assert!(parse_numeric_modifier("INT").is_none());
    }

    #[test]
    fn an_impossible_modifier_is_refused() {
        // Refused rather than clamped: storing fewer places than the column
        // declares would round every value on the way in and report success.
        assert!(parse_numeric_modifier("NUMERIC(38, 37)").unwrap().is_err());
        assert!(parse_numeric_modifier("NUMERIC(4, 6)").unwrap().is_err());
    }

    /// PostgreSQL 17's assignment and explicit-cast rules for `character(n)`
    /// and `character varying(n)`, and the stored form: no trailing spaces.
    #[test]
    fn a_text_length_is_held_as_postgresql_holds_it() {
        let c3 = TextTypmod::Bpchar(3);
        let v3 = TextTypmod::Varchar(3);
        // Assignment: past the length only spaces may be cut.
        assert_eq!(c3.assign("b"), Ok("b"));
        assert_eq!(c3.assign("b  "), Ok("b"));
        assert_eq!(c3.assign("abc     "), Ok("abc"));
        assert_eq!(c3.assign(" a"), Ok(" a"));
        assert_eq!(c3.assign("   "), Ok(""));
        assert!(c3.assign("abcd").unwrap_err().contains("character(3)"));
        assert!(c3.assign("ab c").is_err());
        assert_eq!(v3.assign("ab   "), Ok("ab "));
        assert_eq!(v3.assign("abc"), Ok("abc"));
        assert!(v3.assign("abcd").unwrap_err().contains("character varying(3)"));
        // Characters, not bytes.
        assert_eq!(TextTypmod::Bpchar(2).assign("éü "), Ok("éü"));
        assert!(TextTypmod::Varchar(1).assign("éü").is_err());
        // An explicit cast cuts and never fails.
        assert_eq!(v3.cast("abcd"), "abc");
        assert_eq!(TextTypmod::Bpchar(2).cast("abcd"), "ab");
        assert_eq!(c3.cast("ab  "), "ab");
        assert_eq!(TextTypmod::Bpchar(0).cast("ab  "), "ab");
        // Shown padded to the length.
        assert_eq!(c3.pad("b"), "b  ");
        assert_eq!(c3.pad(""), "   ");
        assert_eq!(TextTypmod::Bpchar(2).pad("é"), "é ");
        assert_eq!(TextTypmod::Bpchar(0).pad("b"), "b");
        assert_eq!(c3.wire_typmod(), 7);
        assert_eq!(TextTypmod::Bpchar(0).wire_typmod(), -1);
    }

    #[test]
    fn a_declared_text_length_is_read_from_the_type_as_written() {
        let read = |t: &str| parse_text_typmod(t).map(|r| r.map_err(|_| ()));
        assert_eq!(read("CHAR(3)"), Some(Ok(TextTypmod::Bpchar(3))));
        assert_eq!(read("character(10)"), Some(Ok(TextTypmod::Bpchar(10))));
        assert_eq!(read("char"), Some(Ok(TextTypmod::Bpchar(1))));
        assert_eq!(read("CHARACTER"), Some(Ok(TextTypmod::Bpchar(1))));
        assert_eq!(read("nchar(2)"), Some(Ok(TextTypmod::Bpchar(2))));
        assert_eq!(read("bpchar"), Some(Ok(TextTypmod::Bpchar(0))));
        assert_eq!(read("bpchar(4)"), Some(Ok(TextTypmod::Bpchar(4))));
        assert_eq!(read("varchar(5)"), Some(Ok(TextTypmod::Varchar(5))));
        assert_eq!(read("character varying(3)"), Some(Ok(TextTypmod::Varchar(3))));
        assert_eq!(read("CHARACTER  VARYING ( 3 )"), Some(Ok(TextTypmod::Varchar(3))));
        assert_eq!(read("varchar"), Some(Ok(TextTypmod::Varchar(0))));
        assert_eq!(read("text"), None);
        assert_eq!(read("char(3)[]"), None);
        assert_eq!(read("char(0)"), Some(Err(())));
        assert_eq!(read("varchar(10485761)"), Some(Err(())));
        assert_eq!(read("char(x)"), Some(Err(())));
    }

    /// The marker carries the length; one with none is an older binary's,
    /// read as no length (the column keeps text semantics).
    #[test]
    fn a_text_length_round_trips_through_its_marker() {
        for t in [TextTypmod::Bpchar(1), TextTypmod::Bpchar(0), TextTypmod::Bpchar(70_000), TextTypmod::Varchar(255)] {
            let record = TypeModifier::from_vec(&t.marker("naïve col").into_vec()).expect("decodes");
            assert_eq!(text_typmod_of("naïve col", &[record]), Some(t), "{t:?}");
        }
        let legacy = [bpchar_marker("k")];
        assert_eq!(text_typmod_of("k", &legacy), None);
        assert!(is_legacy_bpchar("k", &legacy));
        assert!(!is_legacy_bpchar("k", &[TextTypmod::Bpchar(3).marker("k")]));
        // The newest record of a name wins, a tombstone included.
        let changed = [TextTypmod::Bpchar(3).marker("k"), bpchar_marker_retired("k"), TextTypmod::Varchar(4).marker("k")];
        assert_eq!(text_typmod_of("k", &changed), Some(TextTypmod::Varchar(4)));
        // A marker never answers a lookup by column name.
        assert_eq!(scale_of("k", &changed), DEFAULT_DECIMAL_SCALE);
        assert_eq!(precision_of("k", &changed), None);
        // Giving a column a type retires the other kind's marker.
        let records = text_typmod_records("k", Some(TextTypmod::Bpchar(2)), &[TextTypmod::Varchar(4).marker("k")]);
        assert!(records.iter().any(|r| r.is_deleted && r.column.starts_with(VARCHAR_MARKER)));
        assert!(records.iter().any(|r| !r.is_deleted && r.column.starts_with(BPCHAR_MARKER)));
    }

    #[test]
    fn an_unrecorded_column_keeps_the_default_scale() {
        // Every decimal already on disk was written at the default; reading one
        // at a different scale would change what its digits mean.
        assert_eq!(scale_of("anything", &[]), DEFAULT_DECIMAL_SCALE);
        let recorded = [TypeModifier {
            column: "q".to_string(),
            precision: 38,
            scale: 16,
            is_deleted: false,
        }];
        assert_eq!(scale_of("q", &recorded), 16);
        assert_eq!(scale_of("other", &recorded), DEFAULT_DECIMAL_SCALE);
    }
}
