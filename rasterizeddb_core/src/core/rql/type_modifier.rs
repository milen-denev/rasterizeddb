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
/// declared `character(n)` (or `char`, `bpchar`): PostgreSQL compares such a
/// column blank-padded, and this engine stores it as text, so a check that
/// reads it is kept unenforced (`check::cast_columns_to_bpchar`). Recorded in
/// this sidecar, keyed by a name no column can have, so every reader that
/// looks a modifier up by its column never meets one, and a later `ALTER
/// TABLE … ADD CONSTRAINT` still knows the column's type.
pub const BPCHAR_MARKER: &str = "\u{1}bpchar:";

/// The record marking `column` as `character(n)`.
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

/// Whether `modifiers` mark `column` as `character(n)`.
pub fn is_marked_bpchar(column: &str, modifiers: &[TypeModifier]) -> bool {
    bpchar_columns(modifiers).iter().any(|c| c.eq_ignore_ascii_case(column))
}

/// The columns `modifiers` mark as `character(n)`.
pub fn bpchar_columns(modifiers: &[TypeModifier]) -> Vec<String> {
    modifiers
        .iter()
        .filter(|m| !m.is_deleted)
        .filter_map(|m| m.column.strip_prefix(BPCHAR_MARKER).map(str::to_string))
        .collect()
}

/// Whether a declared type, as written (`CHAR(3)`, `character`, `bpchar`,
/// `char`), is `character(n)`; `character varying` is not.
pub fn is_bpchar_type(declared: &str) -> bool {
    let upper = declared.trim().to_ascii_uppercase();
    let base = upper.split('(').next().unwrap_or("").trim();
    (base == "BPCHAR" || base == "CHAR" || base == "CHARACTER" || base == "NCHAR") && !upper.contains("VARYING")
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
