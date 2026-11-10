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

/// The prefix of a record giving an array column its element type
/// ([`TextTypmod::Element`]), the type's byte code plus one. A binary that
/// predates it never reads it, and the column is the untyped array it was
/// there.
pub const ELEMENT_MARKER: &str = "\u{1}element:";

/// The element type `INTEGER[]`, `int4[]`, `boolean ARRAY` or
/// `numeric(10,2)[]` declares, as a [`TextTypmod::Element`]; `None` for a type
/// that is not an array, and for an array of text (`text[]`, `varchar(n)[]`),
/// of `json`/`jsonb`, or of a type this cannot read, which stay the untyped
/// array of text elements every array once was.
pub fn parse_array_element(declared: &str) -> Option<TextTypmod> {
    use crate::core::db_type::DbType;
    let upper = declared.trim().to_ascii_uppercase();
    let base = array_base(&upper)?;
    // The element's type without its modifier: every parenthesised group
    // out, so `TIMESTAMP(3) WITH TIME ZONE` is still the zoned type. Cut at
    // the `(`, it was a `timestamp` without one.
    let mut without_modifier = String::with_capacity(base.len());
    let mut depth = 0usize;
    for c in base.chars() {
        match c {
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            c if depth == 0 => without_modifier.push(c),
            _ => {}
        }
    }
    let tokens: Vec<&str> = without_modifier.split_whitespace().collect();
    let (db_type, used) = crate::core::rql::lexer_alter::parse_column_type(&tokens).ok()?;
    if used != tokens.len() {
        return None;
    }
    // The element's modifier, applied to every element as the column's own
    // would be to a value (`ElementModifier`).
    let modifier = match &db_type {
        DbType::DECIMAL => match parse_numeric_modifier(base) {
            Some(Ok((precision, scale))) => {
                Some(ElementModifier::Numeric { precision: u32::from(precision), scale: u32::from(scale) })
            }
            _ => None,
        },
        DbType::STRING | DbType::CHAR => match parse_text_typmod(base) {
            Some(Ok(TextTypmod::Varchar(n))) if n > 0 => Some(ElementModifier::Varchar(n)),
            Some(Ok(TextTypmod::Bpchar(n))) if n > 0 => Some(ElementModifier::Bpchar(n)),
            _ => None,
        },
        _ => match parse_time_precision(base) {
            Some(Ok(TextTypmod::Precision(p))) => Some(ElementModifier::Precision(p)),
            _ => None,
        },
    };
    match db_type {
        DbType::ARRAY | DbType::JSONTEXT | DbType::JSON | DbType::JSONB => None,
        // Text elements are kept as written, the untyped array every array
        // once was, unless a length holds them (`varchar(5)[]`).
        DbType::STRING | DbType::CHAR => match modifier {
            Some(m) => element_typmod(DbType::STRING.to_byte(), Some(m)),
            None => None,
        },
        t => element_typmod(t.to_byte(), modifier).or_else(|| element_typmod(t.to_byte(), None)),
    }
}

/// An array column's element modifier (`numeric(10,2)[]`, `varchar(5)[]`,
/// `timestamp(0)[]`), applied to each element written as the column's own
/// modifier is to a value: a numeric rounded to its scale and refused past
/// its precision (22003), a text longer than its length refused (22001), a
/// time rounded to its fractional digits. Kept in the
/// [`TextTypmod::Element`] value above the type's code
/// ([`element_typmod`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ElementModifier {
    Numeric { precision: u32, scale: u32 },
    Varchar(u32),
    /// The length is held; the element is not blank-padded, as an element
    /// read back (`a[1]`) is text here, not `character(n)`, and would then
    /// compare unequal to its own unpadded spelling.
    Bpchar(u32),
    Precision(u32),
}

/// [`TextTypmod::Element`]'s value: the element type's code in the low
/// eight bits, a modifier's kind in the next three and its value in the
/// nineteen above. A binary that predates modifiers reads a value past a
/// byte as no type it knows, and the column as the untyped array it was.
const ELEMENT_KIND_SHIFT: u32 = 8;
const ELEMENT_VALUE_SHIFT: u32 = 11;
/// Below `PackedTypmod::VALUE` with the one it adds.
const ELEMENT_VALUE_MAX: u32 = (1 << 19) - 2;

/// The [`TextTypmod::Element`] for an array of the type `code` names, with
/// `modifier`; `None` when the modifier's value does not fit (a
/// `varchar(n)` past half a million characters).
pub fn element_typmod(code: u8, modifier: Option<ElementModifier>) -> Option<TextTypmod> {
    let (kind, value) = match modifier {
        None => (0u32, 0u32),
        // A precision is at most 1000 and a scale at most 38 here.
        Some(ElementModifier::Numeric { precision, scale }) => {
            if precision > 1023 || scale > 63 {
                return None;
            }
            (1, (precision << 6) | scale)
        }
        Some(ElementModifier::Varchar(n)) => (2, n),
        Some(ElementModifier::Bpchar(n)) => (3, n),
        Some(ElementModifier::Precision(p)) => (4, p),
    };
    if value > ELEMENT_VALUE_MAX {
        return None;
    }
    Some(TextTypmod::Element(u32::from(code) | (kind << ELEMENT_KIND_SHIFT) | (value << ELEMENT_VALUE_SHIFT)))
}

/// The element type's code a [`TextTypmod::Element`] value holds.
pub fn element_code(raw: u32) -> u8 {
    (raw & 0xFF) as u8
}

/// The element type a typed array column is described and sent as, by its
/// code: `None` for text elements (`varchar(5)[]`, typed only to hold their
/// length), which go out as the untyped `text[]` they always were.
pub fn described_element(raw: u32) -> Option<u8> {
    let code = element_code(raw);
    (code != crate::core::db_type::DbType::STRING.to_byte()).then_some(code)
}

/// The element modifier a [`TextTypmod::Element`] value holds.
pub fn element_modifier(raw: u32) -> Option<ElementModifier> {
    let value = raw >> ELEMENT_VALUE_SHIFT;
    match (raw >> ELEMENT_KIND_SHIFT) & 0b111 {
        1 => Some(ElementModifier::Numeric { precision: value >> 6, scale: value & 0x3F }),
        2 => Some(ElementModifier::Varchar(value)),
        3 => Some(ElementModifier::Bpchar(value)),
        4 => Some(ElementModifier::Precision(value)),
        _ => None,
    }
}

/// One element's canonical text held to `modifier`: `Err` as the column's
/// own type refuses a value (22003, 22001). `explicit` for a cast
/// (`::varchar(3)[]`), which cuts a text to its length where an
/// assignment refuses it, as PostgreSQL's do.
pub fn hold_element(
    element: &crate::core::db_type::DbType,
    encoded: &[u8],
    canonical: String,
    modifier: ElementModifier,
    explicit: bool,
) -> Result<String, String> {
    use crate::core::value_codec as vc;
    match modifier {
        ElementModifier::Numeric { precision, scale } => {
            // `NaN` holds in any `numeric(p, s)`; the infinities do not.
            if canonical.eq_ignore_ascii_case("nan") {
                return Ok(canonical);
            }
            if canonical.to_ascii_lowercase().contains("infinity") {
                return Err(
                    "numeric field overflow: a field with a declared precision and scale cannot hold an infinite value"
                        .to_string(),
                );
            }
            let unscaled = vc::parse_decimal_rounded(&canonical, scale)
                .ok_or_else(|| format!("numeric field overflow: {} does not fit", vc::double_quoted(&canonical)))?;
            vc::check_numeric_precision(unscaled, precision, scale)?;
            Ok(vc::format_decimal_scaled(unscaled, scale))
        }
        ElementModifier::Varchar(n) if explicit => Ok(TextTypmod::Varchar(n).cast(&canonical).to_string()),
        ElementModifier::Bpchar(n) if explicit => Ok(TextTypmod::Varchar(n).cast(&canonical).to_string()),
        ElementModifier::Varchar(n) => TextTypmod::Varchar(n).assign(&canonical).map(str::to_string),
        ElementModifier::Bpchar(n) => TextTypmod::Bpchar(n).assign(&canonical).map(str::to_string),
        ElementModifier::Precision(p) => Ok(match round_time_bytes(element, encoded, p) {
            Some(rounded) => crate::pgwire::format_column_text_scaled(element.clone(), &rounded, None).unwrap_or(canonical),
            None => canonical,
        }),
    }
}

/// The element type's name in an array type's (`integer[]`, `float8[][]`,
/// `int[3]`, `int ARRAY`, `int ARRAY[3]`); `None` when `declared` names no
/// array. PostgreSQL ignores both a declared bound and the number of
/// brackets — `float8[][]` and `float8[]` are one type, whose values may
/// have any number of dimensions — so every one of these names the same
/// array of its element type.
pub fn array_base(declared: &str) -> Option<&str> {
    let mut rest = declared.trim_end();
    let mut groups = 0usize;
    // Trailing `[]` / `[n]` groups.
    while let Some(open) = rest.strip_suffix(']').and_then(|r| r.rfind('[')) {
        let bound = rest[open + 1..rest.len() - 1].trim();
        if !bound.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        rest = rest[..open].trim_end();
        groups += 1;
    }
    // `ARRAY`, after which only one group may come.
    let word = rest.len().checked_sub(5).filter(|&at| rest.get(at..).is_some_and(|w| w.eq_ignore_ascii_case("ARRAY")));
    if let Some(at) = word.filter(|&at| at > 0 && rest.as_bytes()[at - 1].is_ascii_whitespace()) {
        if groups > 1 {
            return None;
        }
        rest = rest[..at].trim_end();
    } else if groups == 0 {
        return None;
    }
    (!rest.is_empty() && !rest.contains(['[', ']'])).then_some(rest)
}

/// The element type a [`TextTypmod::Element`] code names.
pub fn element_db_type(code: u32) -> Option<crate::core::db_type::DbType> {
    Some(crate::core::db_type::DbType::from_byte(element_code(code)))
}

/// An element type's name as PostgreSQL writes it (`integer`), for errors.
fn element_type_name(code: u32) -> &'static str {
    element_db_type(code).map_or("unknown", |t| crate::core::value_codec::pg_input_type_name(&t))
}

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
    /// The element type of an array column (`integer[]`), as its
    /// [`DbType::to_byte`](crate::core::db_type::DbType::to_byte) code.
    /// Every element written is read as that type, refused as it refuses
    /// one, and stored in its canonical text under the type's OID
    /// ([`crate::core::value_codec::hold_array_elements`]). A column with
    /// none is the untyped array every array once was: text elements, as
    /// written.
    Element(u32),
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
            TextTypmod::Element(b) => (3, b),
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
            2 => TextTypmod::Precision(n),
            _ => TextTypmod::Element(n),
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
            TextTypmod::Element(b) => match element_modifier(*b) {
                Some(ElementModifier::Numeric { precision, scale }) => format!("numeric({precision},{scale})[]"),
                Some(ElementModifier::Varchar(n)) => format!("character varying({n})[]"),
                Some(ElementModifier::Bpchar(n)) => format!("character({n})[]"),
                Some(ElementModifier::Precision(p)) => format!("{}({p})[]", element_type_name(*b)),
                None => format!("{}[]", element_type_name(*b)),
            },
        }
    }

    /// The declared length, when there is one.
    pub fn length(&self) -> Option<u32> {
        match self {
            TextTypmod::Bpchar(0) | TextTypmod::Varchar(0) | TextTypmod::Precision(_) | TextTypmod::Element(_) => None,
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
            TextTypmod::Varchar(0) | TextTypmod::Precision(_) | TextTypmod::Element(_) => Ok(text),
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
            TextTypmod::Varchar(_) | TextTypmod::Precision(_) | TextTypmod::Element(_) => cut,
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
            // Plus one too: no type's code is relied on to be non-zero.
            TextTypmod::Element(b) => (ELEMENT_MARKER, b + 1),
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
    // A legacy `CHAR` column (`value_codec::legacy_char_assign`): the text,
    // or a cell already in its stored form, as the four bytes it is stored
    // as. The text was copied into the cell and cut at four bytes, so
    // `'abcde'` stored `'abcd'` and reported success. No bytes, or a slot of
    // zeros, is a NULL here, its companion saying so, and is left as it is:
    // an encoder that can tell `''` from NULL stores `''` as `' '` itself
    // (`lexer_ir`, `lexer_ur`, `expr`).
    if field.db_type == crate::core::db_type::DbType::CHAR {
        if bytes.iter().all(|b| *b == 0) {
            return Ok(None);
        }
        let end = bytes.iter().rposition(|b| *b != 0).map_or(0, |at| at + 1);
        let text = std::str::from_utf8(&bytes[..end])
            .map_err(|_| "value too long for type character(1)".to_string())?;
        let held = crate::core::value_codec::legacy_char_assign(text)?;
        return Ok((held.as_slice() != bytes).then(|| held.to_vec()));
    }
    // An untyped array column (`text[]`, or one declared before element
    // types were kept) holds its arrays as text: an array built under
    // another element OID (`array_agg` of integers) is relabelled text's, so
    // it orders as the column's other values do.
    if field.db_type == crate::core::db_type::DbType::ARRAY
        && !matches!(field.text_typmod(), Some(TextTypmod::Element(_)))
    {
        return Ok(crate::core::value_codec::array_element_type(bytes)
            .is_some()
            .then(|| crate::core::value_codec::as_text_array(bytes)));
    }
    let Some(typmod) = field.text_typmod() else { return Ok(None) };
    if let TextTypmod::Element(code) = typmod {
        if field.db_type != crate::core::db_type::DbType::ARRAY {
            return Ok(None);
        }
        let Some(element) = element_db_type(code) else { return Ok(None) };
        return crate::core::value_codec::hold_array_elements_to(bytes, &element, element_modifier(code), false);
    }
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
        // `timestamp`, one storage type with `timestamptz` (an array's
        // `timestamp(0)[]` elements are typed so; a column is `DATETIME`).
        DbType::DATETIME | DbType::TIMESTAMP => {
            let raw = <[u8; 8]>::try_from(bytes).ok()?;
            let offset = crate::core::value_codec::PG_EPOCH_OFFSET_US;
            let pg = i64::from_le_bytes(raw).wrapping_sub(offset);
            Some(round_micros(pg, precision & PRECISION_DIGITS).wrapping_add(offset).to_le_bytes().to_vec())
        }
        DbType::TIME => {
            let raw = <[u8; 8]>::try_from(bytes).ok()?;
            Some(round_micros(i64::from_be_bytes(raw), precision & PRECISION_DIGITS).to_be_bytes().to_vec())
        }
        DbType::INTERVAL if bytes.len() == 16 => {
            let mut iv = crate::core::pg_types::decode_interval(bytes)?;
            restrict_interval_fields(&mut iv, precision >> INTERVAL_FIELDS_SHIFT);
            iv.micros = round_micros(iv.micros, precision & PRECISION_DIGITS);
            Some(crate::core::pg_types::encode_interval(iv))
        }
        DbType::TIMETZ if bytes.len() == 12 => {
            let micros = i64::from_be_bytes(bytes[..8].try_into().ok()?);
            let mut out = round_micros(micros, precision & PRECISION_DIGITS).to_be_bytes().to_vec();
            out.extend_from_slice(&bytes[8..]);
            Some(out)
        }
        _ => None,
    }
}

/// The low bits of a [`TextTypmod::Precision`]: the fractional-second
/// digits, six for the type's own.
pub const PRECISION_DIGITS: u32 = 0xFF;
/// Where an interval's field restriction sits in a [`TextTypmod::Precision`]
/// (`interval hour to minute`): one of PostgreSQL's ranges, numbered as
/// [`parse_interval_fields`] numbers them, 0 for none.
pub const INTERVAL_FIELDS_SHIFT: u32 = 8;

/// `interval <fields> [(p)]` and `interval <fields> second(p)`: the column's
/// precision with its field restriction above it. `None` for anything else,
/// an `interval(p)` without fields among it (read as every other type's
/// precision is), and fields this does not know (left unrestricted, as
/// before).
fn parse_interval_fields(text: &str) -> Option<Result<TextTypmod, String>> {
    let lower = text.to_ascii_lowercase();
    let rest = lower.strip_prefix("interval")?;
    if !rest.starts_with(|c: char| c.is_whitespace()) {
        return None;
    }
    let (words, digits) = match (rest.find('('), rest.find(')')) {
        (Some(open), Some(close)) if close > open => {
            (format!("{} {}", &rest[..open], &rest[close + 1..]), Some(rest[open + 1..close].trim().to_string()))
        }
        _ => (rest.to_string(), None),
    };
    let words: Vec<&str> = words.split_whitespace().collect();
    let range = match words.as_slice() {
        ["year"] => 1,
        ["month"] => 2,
        ["day"] => 3,
        ["hour"] => 4,
        ["minute"] => 5,
        ["second"] => 6,
        ["year", "to", "month"] => 7,
        ["day", "to", "hour"] => 8,
        ["day", "to", "minute"] => 9,
        ["day", "to", "second"] => 10,
        ["hour", "to", "minute"] => 11,
        ["hour", "to", "second"] => 12,
        ["minute", "to", "second"] => 13,
        _ => return None,
    };
    let precision = match digits.as_deref().map(str::parse::<i64>) {
        None => 6,
        Some(Ok(p)) if p < 0 => return Some(Err(format!("interval({p}) precision must not be negative"))),
        Some(Ok(p)) => p.min(6) as u32,
        Some(Err(_)) => return Some(Err(format!("invalid type modifier '{}'", digits.unwrap_or_default()))),
    };
    Some(Ok(TextTypmod::Precision(precision | (range << INTERVAL_FIELDS_SHIFT))))
}

/// `iv` cut to the fields `range` keeps, as PostgreSQL's assignment to
/// `interval <fields>` cuts it (`AdjustIntervalForTypmod`): the parts below
/// the last field dropped, toward zero. Seconds are left to the precision.
fn restrict_interval_fields(iv: &mut crate::core::pg_types::Interval, range: u32) {
    const HOUR: i64 = 3_600_000_000;
    const MINUTE: i64 = 60_000_000;
    match range {
        // YEAR
        1 => {
            iv.months = iv.months / 12 * 12;
            iv.days = 0;
            iv.micros = 0;
        }
        // MONTH, YEAR TO MONTH
        2 | 7 => {
            iv.days = 0;
            iv.micros = 0;
        }
        // DAY
        3 => iv.micros = 0,
        // HOUR, DAY TO HOUR
        4 | 8 => iv.micros = iv.micros / HOUR * HOUR,
        // MINUTE, DAY TO MINUTE, HOUR TO MINUTE
        5 | 9 | 11 => iv.micros = iv.micros / MINUTE * MINUTE,
        _ => {}
    }
}

/// The fractional-second precision a declared time type, as written, gives
/// its column: `timestamp(0)`, `timestamp(3) with time zone`, `timestamptz(2)`,
/// `time(1)`, `timetz(2)`, `interval(0)`. `None` without a modifier and for
/// six or more (the type's own precision, nothing to round).
pub fn parse_time_precision(declared: &str) -> Option<Result<TextTypmod, String>> {
    let text = declared.trim();
    // `timestamp(0)[]` is an array, whose element type and modifier
    // `parse_array_element` reads. Claimed here first, the column took the
    // scalar's precision and was left an untyped array of text.
    if array_base(text).is_some() {
        return None;
    }
    if let Some(fields) = parse_interval_fields(text) {
        return Some(fields);
    }
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
    if let Some(m) = live_marker(ELEMENT_MARKER, column, modifiers) {
        let stored = u32::from(m.precision) | (u32::from(m.scale) << 16);
        return stored.checked_sub(1).map(TextTypmod::Element);
    }
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
    let element_now = matches!(typmod, Some(TextTypmod::Element(_)));
    if !element_now && live_marker(ELEMENT_MARKER, column, modifiers).is_some() {
        out.push(TypeModifier { column: format!("{ELEMENT_MARKER}{column}"), precision: 0, scale: 0, is_deleted: true });
    }
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
    fn an_element_modifier_round_trips_through_its_typmod() {
        use crate::core::db_type::DbType;
        let element = |t: &str| match parse_array_element(t) {
            Some(TextTypmod::Element(raw)) => (DbType::from_byte(element_code(raw)), element_modifier(raw)),
            other => panic!("{t}: {other:?}"),
        };
        assert_eq!(element("numeric(10,2)[]"), (DbType::DECIMAL, Some(ElementModifier::Numeric { precision: 10, scale: 2 })));
        assert_eq!(element("numeric(1000,30)[]"), (DbType::DECIMAL, Some(ElementModifier::Numeric { precision: 1000, scale: 30 })));
        assert_eq!(element("decimal(4)[]"), (DbType::DECIMAL, Some(ElementModifier::Numeric { precision: 4, scale: 0 })));
        assert_eq!(element("varchar(5)[]"), (DbType::STRING, Some(ElementModifier::Varchar(5))));
        assert_eq!(element("character(3) ARRAY"), (DbType::STRING, Some(ElementModifier::Bpchar(3))));
        assert_eq!(element("timestamp(0)[]").1, Some(ElementModifier::Precision(0)));
        assert_eq!(element("timestamp(3) with time zone[]"), (DbType::DATETIME, Some(ElementModifier::Precision(3))));
        assert_eq!(element("int[]"), (DbType::I32, None));
        // The code alone, as every array typed before modifiers were kept.
        assert!(matches!(parse_array_element("int[]"), Some(TextTypmod::Element(c)) if c == u32::from(DbType::I32.to_byte())));
        // Text with no length stays the untyped array.
        assert_eq!(parse_array_element("text[]"), None);
        assert_eq!(parse_array_element("varchar[]"), None);
        // And every typmod still packs into a field's four bytes.
        for t in ["numeric(1000,30)[]", "varchar(524286)[]", "timestamp(5)[]"] {
            let typmod = parse_array_element(t).unwrap();
            assert_eq!(PackedTypmod::pack(typmod).map(PackedTypmod::unpack), Some(typmod), "{t}");
        }
        // Past what the value holds: no modifier kept, so no typed column.
        assert_eq!(parse_array_element("varchar(600000)[]"), None);
        // `timestamp(0)[]` is the array's, not a scalar precision.
        assert!(parse_time_precision("timestamp(0)[]").is_none());
    }

    #[test]
    fn every_array_spelling_names_its_element_type() {
        for spelled in ["integer[]", "integer[][]", "integer [3]", "integer[3][4]", "integer ARRAY", "integer array[3]", "INTEGER ARRAY [ ]"] {
            assert!(array_base(spelled).is_some_and(|b| b.trim().eq_ignore_ascii_case("integer")), "{spelled}");
            assert_eq!(parse_array_element(spelled), parse_array_element("int[]"), "{spelled}");
        }
        assert_eq!(array_base("numeric(10,2)[]"), Some("numeric(10,2)"));
        assert_eq!(array_base("double precision[][]"), Some("double precision"));
        for not_one in ["integer", "integer[x]", "ARRAY", "[]", "integer ARRAY[][]", "integerARRAY", "int[-1]"] {
            assert_eq!(array_base(not_one), None, "{not_one}");
        }
    }

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
