//! Partition keys and the `route_v1` hash (§1.3, §1.4, §1b).
//!
//! The input is a column's stored bytes, never literal text, so an insert and
//! a query agree on a value's point exactly when they agree on its bytes.
//! Stored layouts this relies on: integers are little-endian at the column's
//! width (`encode_integer`/`decode_integer`, `core/value_codec.rs:1532-1595`),
//! a UUID is its 16 octets in network order (`DbType::UUID`,
//! `core/db_type.rs:22-25`; `parse_uuid`, `value_codec.rs:30`), and a string
//! is its UTF-8 bytes.

use smallvec::SmallVec;

use crate::core::db_type::DbType;
use crate::core::row::schema::TableSchema;

use super::map::PointRange;
use crate::core::hash::xxh64;

/// Seed of level 0; level `i` uses `LEVEL_SEED + i` ("RDBPRT").
pub const LEVEL_SEED: u64 = 0x5244_4250_5254_0000;

/// At most three key columns (§1b, "Widths").
pub const MAX_ARITY: usize = 3;

/// A normalisation, and the tag that keeps families apart in the frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum KeyFamily {
    /// I8–I128 and U8–U64, as an `i128`, 16 bytes little-endian.
    Integer = 0x01,
    /// The 16 stored octets.
    Uuid = 0x02,
    /// The UTF-8 bytes, as stored.
    Text = 0x03,
}

impl KeyFamily {
    pub fn tag(self) -> u8 {
        self as u8
    }
}

/// The family a column of type `t` hashes in, or why it may not be a key.
///
/// `t` must be the schema field's declared type. Two things it cannot see,
/// and that a caller using it directly must refuse first (prefer
/// [`KeySpec::from_schema`], which does the second itself):
///
/// - `STRING` also stands for CITEXT, BPCHAR, CHARACTER, CHAR(n > 1) and
///   NAME: the caller checks the declared type text (or the sidecar
///   recording it).
/// - `UUID` is also what a shredded JSON column is declared as, holding a
///   per-row reference rather than the document. Only `schema.json_columns`
///   tells them apart.
///
/// Never pass `physical()` or a `RowWrite` column type: those turn BYTEA,
/// ARRAY, JSONB, BIT, VARBIT, PATH, POLYGON and every range into `STRING`,
/// and this would accept them as text.
pub fn family_of(t: &DbType) -> Result<KeyFamily, String> {
    // Exhaustive, so a new type is refused until someone decides otherwise.
    let reason = match t {
        DbType::I8
        | DbType::I16
        | DbType::I32
        | DbType::I64
        | DbType::I128
        | DbType::U8
        | DbType::U16
        | DbType::U32
        | DbType::U64 => return Ok(KeyFamily::Integer),
        DbType::UUID => return Ok(KeyFamily::Uuid),
        DbType::STRING => return Ok(KeyFamily::Text),
        DbType::U128 => "it does not fit in i128",
        DbType::F32 | DbType::F64 => "NaN, -0 and lossy coercion make float equality unsafe to route on",
        DbType::DECIMAL => "NUMERIC bytes depend on the scale, and parsing a literal truncates it",
        DbType::JSON | DbType::JSONB => "its = is not PostgreSQL's, and is not property-tested",
        DbType::CHAR => "CHAR is blank-padded in PostgreSQL and is not on the allowlist; use TEXT or VARCHAR",
        DbType::INTERVAL
        | DbType::TIMETZ
        | DbType::POINT
        | DbType::LINE
        | DbType::LSEG
        | DbType::BOX
        | DbType::PATH
        | DbType::POLYGON
        | DbType::CIRCLE
        | DbType::INT4RANGE
        | DbType::INT8RANGE
        | DbType::NUMRANGE
        | DbType::TSRANGE
        | DbType::TSTZRANGE
        | DbType::DATERANGE => "its equality is not byte equality",
        DbType::BOOL => "it has too few distinct values",
        DbType::DATE
        | DbType::TIME
        | DbType::DATETIME
        | DbType::INET
        | DbType::MACADDR
        | DbType::MACADDR8 => "it is not supported as a partition key yet",
        DbType::BYTEA | DbType::ARRAY | DbType::BIT | DbType::VARBIT => "it is not on the partition-key allowlist",
        DbType::UNKNOWN | DbType::NULL | DbType::START | DbType::END => "it is an internal type",
    };
    Err(format!("a partition key cannot be of type {t}: {reason}"))
}

/// A value's normalised bytes, borrowed where normalisation is the identity.
enum Norm<'a> {
    Int([u8; 16]),
    Bytes(&'a [u8]),
}

impl Norm<'_> {
    fn as_slice(&self) -> &[u8] {
        match self {
            Norm::Int(b) => b,
            Norm::Bytes(b) => b,
        }
    }
}

fn normalise<'a>(t: &DbType, column: &'a [u8]) -> Result<(KeyFamily, Norm<'a>), String> {
    let family = family_of(t)?;
    // A wrong width is a caller bug; guessing would place the row somewhere
    // no query looks.
    let width = |n: usize| -> Result<(), String> {
        if column.len() == n {
            Ok(())
        } else {
            Err(format!("a {t} partition key value must be {n} bytes, got {}", column.len()))
        }
    };
    fn le<const N: usize>(b: &[u8]) -> [u8; N] {
        b.try_into().unwrap()
    }
    let norm = match t {
        DbType::I8 => {
            width(1)?;
            Norm::Int((i8::from_le_bytes(le(column)) as i128).to_le_bytes())
        }
        DbType::I16 => {
            width(2)?;
            Norm::Int((i16::from_le_bytes(le(column)) as i128).to_le_bytes())
        }
        DbType::I32 => {
            width(4)?;
            Norm::Int((i32::from_le_bytes(le(column)) as i128).to_le_bytes())
        }
        DbType::I64 => {
            width(8)?;
            Norm::Int((i64::from_le_bytes(le(column)) as i128).to_le_bytes())
        }
        DbType::I128 => {
            width(16)?;
            Norm::Int(le(column))
        }
        DbType::U8 => {
            width(1)?;
            Norm::Int((column[0] as i128).to_le_bytes())
        }
        DbType::U16 => {
            width(2)?;
            Norm::Int((u16::from_le_bytes(le(column)) as i128).to_le_bytes())
        }
        DbType::U32 => {
            width(4)?;
            Norm::Int((u32::from_le_bytes(le(column)) as i128).to_le_bytes())
        }
        DbType::U64 => {
            width(8)?;
            Norm::Int((u64::from_le_bytes(le(column)) as i128).to_le_bytes())
        }
        DbType::UUID => {
            width(16)?;
            Norm::Bytes(column)
        }
        // Byte equality is the evaluator's string equality, so the stored
        // bytes are hashed as they are, valid UTF-8 or not.
        DbType::STRING => Norm::Bytes(column),
        _ => unreachable!("family_of admitted {t}"),
    };
    Ok((family, norm))
}

/// The normalised bytes of one key value: what `frame` wraps. The one entry
/// point for insert and query alike (§1.3).
pub fn route_bytes(t: &DbType, column: &[u8]) -> Result<Vec<u8>, String> {
    normalise(t, column).map(|(_, n)| n.as_slice().to_vec())
}

/// `family:u8 ‖ len:u32 LE ‖ bytes`.
pub fn frame(family: KeyFamily, normalised: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 + normalised.len());
    push_frame(family, normalised, &mut out);
    out
}

fn push_frame(family: KeyFamily, normalised: &[u8], out: &mut impl Extend<u8>) {
    // Callers check the length fits; a frame never truncates it.
    let len = u32::try_from(normalised.len()).expect("frame length checked by caller");
    out.extend([family.tag()]);
    out.extend(len.to_le_bytes());
    out.extend(normalised.iter().copied());
}

/// `H_i = XXH64(frame(family, normalised), LEVEL_SEED + i)`.
pub fn level_hash(level: usize, family: KeyFamily, normalised: &[u8]) -> Result<u64, String> {
    if level >= MAX_ARITY {
        return Err(format!("partition key level {level} is past the last ({})", MAX_ARITY - 1));
    }
    if u32::try_from(normalised.len()).is_err() {
        return Err(format!("a partition key value of {} bytes is too long to hash", normalised.len()));
    }
    // Integer and UUID frames fit inline; only long text spills.
    let mut buf: SmallVec<[u8; 64]> = SmallVec::new();
    push_frame(family, normalised, &mut buf);
    Ok(xxh64(&buf, LEVEL_SEED + level as u64))
}

/// One key column: where it is in the row, its type, and its family.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyLevel {
    pub write_order: u32,
    pub db_type: DbType,
    pub family: KeyFamily,
}

/// The ordered key columns of a partitioned table, and the hierarchical
/// layout of their hashes in a point.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeySpec {
    levels: Vec<KeyLevel>,
}

impl KeySpec {
    /// A spec over `(write_order, type)` columns, leading column first.
    /// Refuses no columns, more than three, a column named twice, or a type
    /// off the allowlist.
    pub fn new(columns: &[(u32, DbType)]) -> Result<Self, String> {
        if columns.is_empty() || columns.len() > MAX_ARITY {
            return Err(format!(
                "a partition key has 1 to {MAX_ARITY} columns, not {}",
                columns.len()
            ));
        }
        let mut levels = Vec::with_capacity(columns.len());
        for (i, (wo, t)) in columns.iter().enumerate() {
            if columns[..i].iter().any(|(w, _)| w == wo) {
                return Err(format!("partition key names column {wo} twice"));
            }
            levels.push(KeyLevel {
                write_order: *wo,
                db_type: t.clone(),
                family: family_of(t)?,
            });
        }
        Ok(KeySpec { levels })
    }

    /// A spec over the named columns of `schema`, leading column first. Uses
    /// each field's declared type, and refuses a column that is unknown,
    /// dropped, or a shredded JSON column (stored as a `UUID` reference, so
    /// [`family_of`] would accept it). STRING aliases remain the caller's to
    /// refuse; see [`family_of`].
    pub fn from_schema(schema: &TableSchema, key: &[&str]) -> Result<Self, String> {
        let mut columns = Vec::with_capacity(key.len());
        for name in key {
            // Any record, dropped or not: refusing a reused name is an
            // inconvenience, routing on a reference is a wrong answer.
            if schema.json_columns.iter().any(|c| c.column.eq_ignore_ascii_case(name)) {
                return Err(format!(
                    "partition key column \"{name}\" is a JSON column, which cannot be a partition key"
                ));
            }
            let mut live = schema
                .fields
                .iter()
                .filter(|f| !f.is_deleted && f.name.eq_ignore_ascii_case(name));
            let field = live
                .next()
                .ok_or_else(|| format!("partition key column \"{name}\" does not exist"))?;
            if live.next().is_some() {
                return Err(format!("partition key column \"{name}\" is ambiguous"));
            }
            let write_order = u32::try_from(field.write_order)
                .map_err(|_| format!("partition key column \"{name}\" has write order {}", field.write_order))?;
            columns.push((write_order, field.db_type.clone()));
        }
        Self::new(&columns)
    }

    pub fn levels(&self) -> &[KeyLevel] {
        &self.levels
    }

    pub fn arity(&self) -> usize {
        self.levels.len()
    }

    /// Bits of the point each level fills, leading level first: `[48]`,
    /// `[48, 16]` or `[48, 8, 8]`. Level 0 is 48 bits at every arity, so a
    /// leading `tenant_id` gives the same block in every table.
    pub fn widths(&self) -> &'static [u32] {
        widths(self.arity())
    }

    /// The routing point of a row whose key columns hold `columns` (stored
    /// bytes, one per level).
    pub fn point_of(&self, columns: &[&[u8]]) -> Result<u64, String> {
        if columns.len() != self.arity() {
            return Err(format!(
                "a {}-column partition key was given {} values",
                self.arity(),
                columns.len()
            ));
        }
        Ok(self.prefix_range(columns)?.lo)
    }

    /// The points rows can occupy once the first `prefix.len()` key columns
    /// are fixed: a whole tenant block for the leading column alone, a single
    /// point for every column, the whole space for none.
    pub fn prefix_range(&self, prefix: &[&[u8]]) -> Result<PointRange, String> {
        if prefix.len() > self.arity() {
            return Err(format!(
                "a {}-column partition key was given {} values",
                self.arity(),
                prefix.len()
            ));
        }
        let mut hashes: SmallVec<[u64; MAX_ARITY]> = SmallVec::new();
        for (i, (level, column)) in self.levels.iter().zip(prefix).enumerate() {
            let (family, norm) = normalise(&level.db_type, column)?;
            hashes.push(level_hash(i, family, norm.as_slice())?);
        }
        Ok(assemble(&hashes, self.widths()))
    }
}

/// The points under fixed level hashes: each fills the top `widths[i]` bits
/// of its level, and the levels not given span everything below. Split out
/// so the top block, which no known value hashes into, can be tested: its
/// `hi` is 2⁶⁴, one past what a `u64` holds.
fn assemble(hashes: &[u64], widths: &[u32]) -> PointRange {
    let mut lo: u64 = 0;
    let mut low_bits: u32 = 64;
    for (&h, &w) in hashes.iter().zip(widths) {
        low_bits -= w;
        lo |= (h >> (64 - w)) << low_bits;
    }
    PointRange {
        lo,
        hi: lo as u128 + (1u128 << low_bits),
    }
}

fn widths(arity: usize) -> &'static [u32] {
    match arity {
        1 => &[48],
        2 => &[48, 16],
        3 => &[48, 8, 8],
        _ => unreachable!("KeySpec::new bounds the arity"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const UUID: [u8; 16] = [
        0x12, 0x3e, 0x45, 0x67, 0xe8, 0x9b, 0x12, 0xd3, 0xa4, 0x56, 0x42, 0x66, 0x14, 0x17, 0x40, 0x00,
    ];
    const NON_ASCII: &str = "Grüße, 世界";

    fn spec(cols: &[DbType]) -> KeySpec {
        let cols: Vec<(u32, DbType)> = cols.iter().cloned().enumerate().map(|(i, t)| (i as u32, t)).collect();
        KeySpec::new(&cols).unwrap()
    }

    fn h(level: usize, t: &DbType, column: &[u8]) -> u64 {
        let family = family_of(t).unwrap();
        level_hash(level, family, &route_bytes(t, column).unwrap()).unwrap()
    }

    /// route_v1, frozen. These were computed independently of this code,
    /// with libxxhash 0.8.3 over hand-built frames. CHANGING ANY OF THEM
    /// MOVES PERSISTED DATA: rows already placed would sit in partitions
    /// that queries no longer route to. A new hash is a new route_v.
    #[test]
    fn golden_level_hashes_route_v1() {
        let cases: &[(&str, DbType, Vec<u8>, [u64; 3])] = &[
            (
                "0",
                DbType::I64,
                0i64.to_le_bytes().to_vec(),
                [0x1BA1_1803_D99A_C171, 0x49AB_8810_C191_ABA6, 0x0A10_1EF8_74F9_A5E0],
            ),
            (
                "-1",
                DbType::I64,
                (-1i64).to_le_bytes().to_vec(),
                [0x4AF7_0157_F8F3_7A75, 0x426C_2346_D3CA_84C0, 0xA7DB_CC17_3330_2798],
            ),
            (
                "i64::MIN",
                DbType::I64,
                i64::MIN.to_le_bytes().to_vec(),
                [0x2E0D_F99E_C878_379B, 0x74BD_BB98_0FBF_6F9E, 0x2FF0_F3F2_F1D6_CE38],
            ),
            (
                "u64::MAX",
                DbType::U64,
                u64::MAX.to_le_bytes().to_vec(),
                [0xFE66_E209_5495_3637, 0x191B_723A_EA37_61CB, 0x94B3_0807_D42B_9733],
            ),
            (
                "''",
                DbType::STRING,
                Vec::new(),
                [0x56DC_78BE_6C06_8B70, 0xF88D_A7F3_8616_65FE, 0xA8B8_D2E6_8F39_CE2F],
            ),
            (
                "non-ASCII",
                DbType::STRING,
                NON_ASCII.as_bytes().to_vec(),
                [0x617E_6C8A_2E13_3B99, 0x3881_2D56_41AD_A493, 0xB167_4C38_CB0F_C2F6],
            ),
            (
                "uuid",
                DbType::UUID,
                UUID.to_vec(),
                [0xE4F6_A69D_CE3B_A653, 0xE60B_7699_3646_AF9E, 0x6590_48C1_ADFA_F8DA],
            ),
        ];
        for (name, t, column, want) in cases {
            for (level, &w) in want.iter().enumerate() {
                assert_eq!(h(level, t, column), w, "{name} at level {level}");
            }
        }
    }

    /// route_v1 points, frozen like the hashes above (same provenance).
    #[test]
    fn golden_points_route_v1() {
        let i42 = 42i64.to_le_bytes();
        assert_eq!(spec(&[DbType::I64]).point_of(&[&i42]).unwrap(), 0xE53D_EF32_5ADD_0000);
        assert_eq!(
            spec(&[DbType::I64, DbType::STRING]).point_of(&[&i42, b"abc"]).unwrap(),
            0xE53D_EF32_5ADD_3F9E
        );
        assert_eq!(
            spec(&[DbType::I64, DbType::STRING, DbType::UUID])
                .point_of(&[&i42, b"abc", &UUID])
                .unwrap(),
            0xE53D_EF32_5ADD_3F65
        );
        let text2 = spec(&[DbType::STRING, DbType::STRING]);
        assert_eq!(text2.point_of(&[b"ab", b"c"]).unwrap(), 0x1390_20A4_A5A4_3037);
        assert_eq!(text2.point_of(&[b"a", b"bc"]).unwrap(), 0x0A0C_405D_7C26_5DFF);
    }

    #[test]
    fn integer_widths_agree_on_a_value() {
        // INT 42 and BIGINT 42 hold different bytes but the same value.
        let as_i32 = spec(&[DbType::I32]).point_of(&[&42i32.to_le_bytes()]).unwrap();
        let as_i64 = spec(&[DbType::I64]).point_of(&[&42i64.to_le_bytes()]).unwrap();
        assert_eq!(as_i32, as_i64);
        let same = [
            route_bytes(&DbType::I8, &42i8.to_le_bytes()).unwrap(),
            route_bytes(&DbType::I16, &42i16.to_le_bytes()).unwrap(),
            route_bytes(&DbType::I32, &42i32.to_le_bytes()).unwrap(),
            route_bytes(&DbType::I64, &42i64.to_le_bytes()).unwrap(),
            route_bytes(&DbType::I128, &42i128.to_le_bytes()).unwrap(),
            route_bytes(&DbType::U8, &42u8.to_le_bytes()).unwrap(),
            route_bytes(&DbType::U16, &42u16.to_le_bytes()).unwrap(),
            route_bytes(&DbType::U32, &42u32.to_le_bytes()).unwrap(),
            route_bytes(&DbType::U64, &42u64.to_le_bytes()).unwrap(),
        ];
        assert!(same.iter().all(|b| b == &42i128.to_le_bytes()));
        // Sign extension, not zero extension, for signed types; the reverse
        // for unsigned.
        assert_eq!(route_bytes(&DbType::I8, &[0xFF]).unwrap(), (-1i128).to_le_bytes());
        assert_eq!(route_bytes(&DbType::U8, &[0xFF]).unwrap(), 255i128.to_le_bytes());
        assert_eq!(
            route_bytes(&DbType::U64, &u64::MAX.to_le_bytes()).unwrap(),
            (u64::MAX as i128).to_le_bytes()
        );
        // The high bit at every width, where sign and zero extension differ.
        let signed: [(DbType, Vec<u8>, i128); 7] = [
            (DbType::I16, (-1i16).to_le_bytes().to_vec(), -1),
            (DbType::I16, i16::MIN.to_le_bytes().to_vec(), i16::MIN as i128),
            (DbType::I32, (-1i32).to_le_bytes().to_vec(), -1),
            (DbType::I32, i32::MIN.to_le_bytes().to_vec(), i32::MIN as i128),
            (DbType::I64, i64::MIN.to_le_bytes().to_vec(), i64::MIN as i128),
            (DbType::I128, (-1i128).to_le_bytes().to_vec(), -1),
            (DbType::I8, i8::MIN.to_le_bytes().to_vec(), i8::MIN as i128),
        ];
        let unsigned: [(DbType, Vec<u8>, i128); 2] = [
            (DbType::U16, vec![0xFF, 0xFF], 65_535),
            (DbType::U32, u32::MAX.to_le_bytes().to_vec(), 4_294_967_295),
        ];
        for (t, column, want) in signed.iter().chain(&unsigned) {
            assert_eq!(route_bytes(t, column).unwrap(), want.to_le_bytes(), "{t} {column:02x?}");
        }
        // A negative INT lands where the frozen BIGINT -1 does, literally.
        assert_eq!(h(0, &DbType::I32, &(-1i32).to_le_bytes()), 0x4AF7_0157_F8F3_7A75);
        assert_eq!(h(0, &DbType::I16, &(-1i16).to_le_bytes()), 0x4AF7_0157_F8F3_7A75);
    }

    #[test]
    fn levels_hash_apart_so_concatenations_differ() {
        let s = spec(&[DbType::STRING, DbType::STRING]);
        let a = s.point_of(&[b"ab", b"c"]).unwrap();
        let b = s.point_of(&[b"a", b"bc"]).unwrap();
        assert_ne!(a >> 16, b >> 16, "level 0 must differ");
        // Families are tagged: the same bytes as text and as a UUID differ.
        assert_ne!(h(0, &DbType::STRING, &UUID), h(0, &DbType::UUID, &UUID));
        // The seed depends on the level.
        assert_ne!(h(0, &DbType::I64, &[0; 8]), h(1, &DbType::I64, &[0; 8]));
    }

    #[test]
    fn a_leading_value_gives_the_same_block_at_every_arity() {
        let t = 7i64.to_le_bytes();
        let one = spec(&[DbType::I64]).prefix_range(&[&t]).unwrap();
        let two = spec(&[DbType::I64, DbType::STRING]).prefix_range(&[&t]).unwrap();
        let three = spec(&[DbType::I64, DbType::I32, DbType::UUID]).prefix_range(&[&t]).unwrap();
        assert_eq!(one, two);
        assert_eq!(one, three);
        assert_eq!(one.lo % (1 << 16), 0);
        assert_eq!(one.hi - one.lo as u128, 1 << 16);
    }

    #[test]
    fn prefix_ranges_nest_and_hold_their_points() {
        let s = spec(&[DbType::I64, DbType::STRING, DbType::UUID]);
        let t = 42i64.to_le_bytes();
        let full = s.prefix_range(&[]).unwrap();
        let tenant = s.prefix_range(&[&t]).unwrap();
        let group = s.prefix_range(&[&t, b"abc"]).unwrap();
        let row = s.prefix_range(&[&t, b"abc", &UUID]).unwrap();
        assert_eq!(full, PointRange::FULL);
        assert_eq!(tenant.hi - tenant.lo as u128, 1 << 16);
        assert_eq!(group.hi - group.lo as u128, 1 << 8);
        assert_eq!(row, PointRange::point(s.point_of(&[&t, b"abc", &UUID]).unwrap()));
        for (outer, inner) in [(full, tenant), (tenant, group), (group, row)] {
            assert!(outer.lo <= inner.lo && inner.hi <= outer.hi, "{outer:?} ⊉ {inner:?}");
        }
        let two = spec(&[DbType::I64, DbType::STRING]);
        let p = two.point_of(&[&t, b"x"]).unwrap();
        assert_eq!(two.prefix_range(&[&t, b"x"]).unwrap(), PointRange::point(p));
        assert!(two.prefix_range(&[&t]).unwrap().contains(p));
    }

    #[test]
    fn the_top_block_ends_at_the_end_of_the_space() {
        // No value is known to hash there, so assemble all-ones hashes.
        use super::super::map::SPACE_END;
        for arity in 1..=MAX_ARITY {
            let w = widths(arity);
            let tenant = assemble(&[u64::MAX], w);
            assert_eq!(tenant, PointRange { lo: 0xFFFF_FFFF_FFFF_0000, hi: SPACE_END }, "arity {arity}");
            if arity > 1 {
                let last = assemble(&vec![u64::MAX; arity], w);
                assert_eq!(last, PointRange { lo: u64::MAX, hi: SPACE_END }, "arity {arity}");
            }
        }
        assert_eq!(assemble(&[], widths(2)), PointRange::FULL);
        // Only the top bits of each hash are used.
        assert_eq!(assemble(&[u64::MAX, 0x0001_FFFF_FFFF_FFFF], widths(2)), PointRange::point(0xFFFF_FFFF_FFFF_0001));
    }

    fn table(fields: &[(&str, DbType)]) -> TableSchema {
        use crate::core::row::schema::SchemaField;
        let mut schema = TableSchema::new("t".to_string(), false);
        for (i, (name, t)) in fields.iter().enumerate() {
            schema
                .fields
                .push(SchemaField::new(name.to_string(), t.clone(), 0, 0, 10 + i as u64, false));
        }
        schema
    }

    #[test]
    fn from_schema_uses_declared_types_and_refuses_json_references() {
        use crate::core::rql::json_map::JsonColumn;
        let mut schema = table(&[("tenant", DbType::I32), ("id", DbType::UUID), ("doc", DbType::UUID)]);
        // A shredded JSON column is declared UUID; only this record says so.
        schema.json_columns.push(JsonColumn {
            column: "doc".to_string(),
            shadow_table: "t__json_doc".to_string(),
            is_deleted: false,
        });
        let s = KeySpec::from_schema(&schema, &["tenant", "ID"]).unwrap();
        assert_eq!(s, KeySpec::new(&[(10, DbType::I32), (11, DbType::UUID)]).unwrap());
        let err = KeySpec::from_schema(&schema, &["tenant", "doc"]).unwrap_err();
        assert!(err.contains("JSON"), "{err}");
        assert!(KeySpec::from_schema(&schema, &["DOC"]).is_err());
        assert!(KeySpec::from_schema(&schema, &["nope"]).unwrap_err().contains("does not exist"));

        // Declared, not physical: these are all STRING to storage.
        for t in [DbType::JSONB, DbType::BYTEA, DbType::ARRAY, DbType::VARBIT, DbType::INT4RANGE] {
            let schema = table(&[("k", t.clone())]);
            assert_eq!(t.physical(), DbType::STRING);
            assert!(KeySpec::from_schema(&schema, &["k"]).is_err(), "{t}");
        }

        // A dropped column is not a key; its live namesake is.
        let mut schema = table(&[("k", DbType::STRING), ("k", DbType::I64)]);
        schema.fields[0].is_deleted = true;
        assert_eq!(KeySpec::from_schema(&schema, &["k"]).unwrap().levels()[0].db_type, DbType::I64);
        schema.fields[0].is_deleted = false;
        assert!(KeySpec::from_schema(&schema, &["k"]).unwrap_err().contains("ambiguous"));
    }

    #[test]
    fn refusals_carry_a_reason() {
        for t in [
            DbType::U128,
            DbType::F32,
            DbType::F64,
            DbType::DECIMAL,
            DbType::JSONB,
            DbType::JSON,
            DbType::CHAR,
            DbType::INTERVAL,
            DbType::TIMETZ,
            DbType::POINT,
            DbType::INT4RANGE,
            DbType::BOOL,
            DbType::DATE,
            DbType::TIME,
            DbType::DATETIME,
            DbType::INET,
            DbType::MACADDR,
            DbType::BYTEA,
            DbType::ARRAY,
            DbType::NULL,
        ] {
            let err = family_of(&t).unwrap_err();
            assert!(err.contains(&format!("{t}")) && err.contains(": "), "{err}");
            assert!(KeySpec::new(&[(0, t.clone())]).is_err());
            assert!(route_bytes(&t, &[0; 16]).is_err());
        }
    }

    #[test]
    fn malformed_specs_and_values_are_refused() {
        assert!(KeySpec::new(&[]).is_err());
        assert!(KeySpec::new(&[(0, DbType::I64), (1, DbType::I64), (2, DbType::I64), (3, DbType::I64)]).is_err());
        assert!(KeySpec::new(&[(4, DbType::I64), (4, DbType::STRING)]).is_err());
        let s = spec(&[DbType::I32, DbType::UUID]);
        assert!(s.point_of(&[&42i32.to_le_bytes()]).is_err(), "too few values");
        assert!(s.point_of(&[&42i64.to_le_bytes(), &UUID]).is_err(), "wrong integer width");
        assert!(s.point_of(&[&42i32.to_le_bytes(), &UUID[..15]]).is_err(), "short uuid");
        assert!(s.prefix_range(&[&[0; 4], &UUID, b"x"]).is_err(), "too many values");
        assert!(level_hash(3, KeyFamily::Text, b"").is_err());
    }

    #[test]
    fn frame_layout() {
        assert_eq!(frame(KeyFamily::Text, b"ab"), vec![0x03, 2, 0, 0, 0, b'a', b'b']);
        assert_eq!(frame(KeyFamily::Uuid, &[]), vec![0x02, 0, 0, 0, 0]);
        // A text value long enough to spill the inline buffer hashes the same
        // as hashing its frame directly.
        let long = vec![b'z'; 300];
        assert_eq!(
            level_hash(1, KeyFamily::Text, &long).unwrap(),
            xxh64(&frame(KeyFamily::Text, &long), LEVEL_SEED + 1)
        );
    }
}
