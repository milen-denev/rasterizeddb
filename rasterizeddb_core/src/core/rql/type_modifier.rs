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
    /// `character varying(n)`.
    Varchar(u32),
}

impl TextTypmod {
    /// The type's name as PostgreSQL writes it in an error.
    pub fn type_name(&self) -> String {
        match self {
            TextTypmod::Bpchar(0) => "bpchar".to_string(),
            TextTypmod::Bpchar(n) => format!("character({n})"),
            TextTypmod::Varchar(n) => format!("character varying({n})"),
        }
    }

    /// The declared length, when there is one.
    pub fn length(&self) -> Option<u32> {
        match self {
            TextTypmod::Bpchar(0) => None,
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
            TextTypmod::Varchar(_) => cut,
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
            TextTypmod::Varchar(n) => (VARCHAR_MARKER, n),
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
    let Some(typmod) = field.text_typmod else { return Ok(None) };
    if field.db_type != crate::core::db_type::DbType::STRING {
        return Ok(None);
    }
    let Ok(text) = std::str::from_utf8(bytes) else { return Ok(None) };
    let held = typmod.assign(text)?;
    Ok((held.len() != text.len()).then(|| held.as_bytes().to_vec()))
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
    if !bpchar_now && live_marker(BPCHAR_MARKER, column, modifiers).is_some() {
        out.push(bpchar_marker_retired(column));
    }
    if !varchar_now && live_marker(VARCHAR_MARKER, column, modifiers).is_some() {
        out.push(varchar_marker_retired(column));
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
/// `character varying(3)`. `None` for any other type, an unbounded
/// `varchar`, and an array of either.
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
        (true, None) => return None,
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
        assert_eq!(read("varchar"), None);
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
