//! route_v1's key hashing: the families, the frame, the level hashes and the
//! point layout (partitioning.md §1.3, §1.4, §1b).
//!
//! The engine normalises a column's *stored bytes* (`core::partition::key`)
//! and the client normalises a typed value ([`KeyValue`]); both end in the
//! same normalised bytes, so both reach the same point. Everything here is
//! frozen: changing any of it moves persisted rows. A new hash is a new
//! [`ROUTE_V1`].

use crate::map::PointRange;
use crate::xxh64::xxh64;

/// The routing function's version, as the partition log and the shard map
/// record it.
pub const ROUTE_V1: u8 = 1;

/// Seed of level 0; level `i` uses `LEVEL_SEED + i` ("RDBPRT").
pub const LEVEL_SEED: u64 = 0x5244_4250_5254_0000;

/// At most three key columns (§1b, "Widths").
pub const MAX_ARITY: usize = 3;

/// The bits of a point that level 0 (the shard key) fills at every arity.
pub const LEVEL0_BITS: u32 = 48;

/// A normalisation, and the tag that keeps families apart in the frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum KeyFamily {
    /// I8–I128 and U8–U64, as an `i128`, 16 bytes little-endian.
    Integer = 0x01,
    /// The 16 octets, in network order.
    Uuid = 0x02,
    /// The UTF-8 bytes, as stored.
    Text = 0x03,
}

impl KeyFamily {
    pub fn tag(self) -> u8 {
        self as u8
    }

    /// The family a tag names, for decoders.
    pub fn from_tag(tag: u8) -> Option<KeyFamily> {
        match tag {
            0x01 => Some(KeyFamily::Integer),
            0x02 => Some(KeyFamily::Uuid),
            0x03 => Some(KeyFamily::Text),
            _ => None,
        }
    }
}

/// `family:u8 ‖ len:u32 LE ‖ bytes`.
pub fn frame(family: KeyFamily, normalised: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 + normalised.len());
    push_frame(family, normalised, &mut out);
    out
}

/// Appends [`frame`]'s bytes to `out`. The caller has checked that the length
/// fits a `u32`; a frame never truncates it.
pub fn push_frame(family: KeyFamily, normalised: &[u8], out: &mut impl Extend<u8>) {
    let len = u32::try_from(normalised.len()).expect("frame length checked by caller");
    out.extend([family.tag()]);
    out.extend(len.to_le_bytes());
    out.extend(normalised.iter().copied());
}

/// A frame of up to this many bytes is built on the stack.
const INLINE_FRAME: usize = 64;

/// `H_i = XXH64(frame(family, normalised), LEVEL_SEED + i)`.
///
/// The engine's `core::partition::key::level_hash` computes the same value
/// through `core::hash::xxh64` (this crate's), behind the engine's hash fence;
/// `core_and_router_agree` in that module pins the two together.
pub fn level_hash(level: usize, family: KeyFamily, normalised: &[u8]) -> Result<u64, String> {
    if level >= MAX_ARITY {
        return Err(format!("partition key level {level} is past the last ({})", MAX_ARITY - 1));
    }
    if u32::try_from(normalised.len()).is_err() {
        return Err(format!("a partition key value of {} bytes is too long to hash", normalised.len()));
    }
    let seed = LEVEL_SEED + level as u64;
    let n = 5 + normalised.len();
    if n <= INLINE_FRAME {
        // Integer and UUID frames, and short text: no allocation.
        let mut buf = [0u8; INLINE_FRAME];
        buf[0] = family.tag();
        buf[1..5].copy_from_slice(&(normalised.len() as u32).to_le_bytes());
        buf[5..n].copy_from_slice(normalised);
        Ok(xxh64(&buf[..n], seed))
    } else {
        Ok(xxh64(&frame(family, normalised), seed))
    }
}

/// Bits of the point each level fills, leading level first: `[48]`,
/// `[48, 16]` or `[48, 8, 8]`. Level 0 is 48 bits at every arity, so a
/// leading `tenant_id` gives the same block in every table, and so on the
/// same shard.
pub fn widths(arity: usize) -> Result<&'static [u32], String> {
    match arity {
        1 => Ok(&[48]),
        2 => Ok(&[48, 16]),
        3 => Ok(&[48, 8, 8]),
        _ => Err(format!("a partition key has 1 to {MAX_ARITY} columns, not {arity}")),
    }
}

/// The points under fixed level hashes: each fills the top `widths[i]` bits
/// of its level, and the levels not given span everything below. The top
/// block's `hi` is 2⁶⁴, one past what a `u64` holds.
pub fn assemble(hashes: &[u64], widths: &[u32]) -> PointRange {
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

/// The level-0 block a point lies in: its top 48 bits. A shard map assigns
/// blocks, so this is all a router needs from a point.
#[inline]
pub fn block_of(point: u64) -> u64 {
    point >> (64 - LEVEL0_BITS)
}

/// A typed key value, as an application holds it. Normalises to the bytes
/// the engine normalises the stored column to, so the client and the engine
/// reach the same point.
///
/// The column's family decides, never the value's: a UUID held in a TEXT
/// column is routed as [`KeyValue::Text`] of its text. Types the engine
/// refuses as keys (NUMERIC, floats, dates, CHAR, JSON) have no variant, so
/// no collation or float rule is ever needed here.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeyValue<'a> {
    /// Any of `i8…i128` or `u8…u64`: `INT 42` and `BIGINT 42` route alike.
    Int(i128),
    /// A UUID's 16 octets in network order.
    Uuid([u8; 16]),
    /// Text, as its UTF-8 bytes.
    Text(&'a str),
}

impl KeyValue<'_> {
    /// The family this value normalises in.
    pub fn family(&self) -> KeyFamily {
        match self {
            KeyValue::Int(_) => KeyFamily::Integer,
            KeyValue::Uuid(_) => KeyFamily::Uuid,
            KeyValue::Text(_) => KeyFamily::Text,
        }
    }

    /// Level `level`'s hash of this value in a column of `family`. A value of
    /// another family is refused: guessing would route to a shard no query
    /// for the stored row looks at.
    pub fn level_hash(&self, level: usize, family: KeyFamily) -> Result<u64, String> {
        if self.family() != family {
            return Err(format!(
                "a {:?} key value cannot route a {:?} key column",
                self.family(),
                family
            ));
        }
        match self {
            KeyValue::Int(v) => level_hash(level, family, &v.to_le_bytes()),
            KeyValue::Uuid(b) => level_hash(level, family, b),
            KeyValue::Text(s) => level_hash(level, family, s.as_bytes()),
        }
    }
}

macro_rules! int_key {
    ($($t:ty),*) => {$(
        impl From<$t> for KeyValue<'_> {
            fn from(v: $t) -> Self {
                KeyValue::Int(v as i128)
            }
        }
    )*};
}
int_key!(i8, i16, i32, i64, i128, u8, u16, u32, u64);

impl<'a> From<&'a str> for KeyValue<'a> {
    fn from(v: &'a str) -> Self {
        KeyValue::Text(v)
    }
}

impl<'a> From<&'a String> for KeyValue<'a> {
    fn from(v: &'a String) -> Self {
        KeyValue::Text(v)
    }
}

impl From<[u8; 16]> for KeyValue<'_> {
    fn from(v: [u8; 16]) -> Self {
        KeyValue::Uuid(v)
    }
}

/// The routing point of a row whose key columns, of `families`, hold
/// `values`, leading column first.
pub fn point_of(families: &[KeyFamily], values: &[KeyValue<'_>]) -> Result<u64, String> {
    if values.len() != families.len() {
        return Err(format!(
            "a {}-column partition key was given {} values",
            families.len(),
            values.len()
        ));
    }
    Ok(prefix_range(families, values)?.lo)
}

/// The points rows can occupy once the first `values.len()` key columns are
/// fixed: a whole level-0 block for the leading column alone (one shard),
/// a single point for every column.
pub fn prefix_range(families: &[KeyFamily], values: &[KeyValue<'_>]) -> Result<PointRange, String> {
    let w = widths(families.len())?;
    if values.len() > families.len() {
        return Err(format!(
            "a {}-column partition key was given {} values",
            families.len(),
            values.len()
        ));
    }
    let mut hashes = [0u64; MAX_ARITY];
    for (i, (v, f)) in values.iter().zip(families).enumerate() {
        hashes[i] = v.level_hash(i, *f)?;
    }
    Ok(assemble(&hashes[..values.len()], w))
}

/// The level-0 block of a shard key value: the only thing a shard map needs.
/// Equal for every arity, so one value lands on one shard in every table.
pub fn shard_block(value: KeyValue<'_>, family: KeyFamily) -> Result<u64, String> {
    Ok(block_of(assemble(&[value.level_hash(0, family)?], &[LEVEL0_BITS]).lo))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::map::SPACE_END;

    const UUID: [u8; 16] = [
        0x12, 0x3e, 0x45, 0x67, 0xe8, 0x9b, 0x12, 0xd3, 0xa4, 0x56, 0x42, 0x66, 0x14, 0x17, 0x40, 0x00,
    ];
    const NON_ASCII: &str = "Grüße, 世界";

    /// route_v1, frozen: the same vectors as the engine's
    /// `golden_level_hashes_route_v1`, computed independently with libxxhash
    /// 0.8.3 over hand-built frames. CHANGING ANY OF THEM MOVES PERSISTED DATA.
    #[test]
    fn golden_level_hashes_route_v1() {
        let cases: &[(&str, KeyValue, [u64; 3])] = &[
            ("0", KeyValue::Int(0), [0x1BA1_1803_D99A_C171, 0x49AB_8810_C191_ABA6, 0x0A10_1EF8_74F9_A5E0]),
            ("-1", KeyValue::Int(-1), [0x4AF7_0157_F8F3_7A75, 0x426C_2346_D3CA_84C0, 0xA7DB_CC17_3330_2798]),
            (
                "i64::MIN",
                KeyValue::Int(i64::MIN as i128),
                [0x2E0D_F99E_C878_379B, 0x74BD_BB98_0FBF_6F9E, 0x2FF0_F3F2_F1D6_CE38],
            ),
            (
                "u64::MAX",
                KeyValue::Int(u64::MAX as i128),
                [0xFE66_E209_5495_3637, 0x191B_723A_EA37_61CB, 0x94B3_0807_D42B_9733],
            ),
            ("''", KeyValue::Text(""), [0x56DC_78BE_6C06_8B70, 0xF88D_A7F3_8616_65FE, 0xA8B8_D2E6_8F39_CE2F]),
            (
                "non-ASCII",
                KeyValue::Text(NON_ASCII),
                [0x617E_6C8A_2E13_3B99, 0x3881_2D56_41AD_A493, 0xB167_4C38_CB0F_C2F6],
            ),
            ("uuid", KeyValue::Uuid(UUID), [0xE4F6_A69D_CE3B_A653, 0xE60B_7699_3646_AF9E, 0x6590_48C1_ADFA_F8DA]),
        ];
        for (name, v, want) in cases {
            for (level, &w) in want.iter().enumerate() {
                assert_eq!(v.level_hash(level, v.family()).unwrap(), w, "{name} at level {level}");
            }
        }
    }

    /// route_v1 points, frozen like the hashes (the engine's
    /// `golden_points_route_v1`).
    #[test]
    fn golden_points_route_v1() {
        use KeyFamily::*;
        let i42 = KeyValue::Int(42);
        assert_eq!(point_of(&[Integer], &[i42]).unwrap(), 0xE53D_EF32_5ADD_0000);
        assert_eq!(point_of(&[Integer, Text], &[i42, "abc".into()]).unwrap(), 0xE53D_EF32_5ADD_3F9E);
        assert_eq!(
            point_of(&[Integer, Text, Uuid], &[i42, "abc".into(), UUID.into()]).unwrap(),
            0xE53D_EF32_5ADD_3F65
        );
        assert_eq!(point_of(&[Text, Text], &["ab".into(), "c".into()]).unwrap(), 0x1390_20A4_A5A4_3037);
        assert_eq!(point_of(&[Text, Text], &["a".into(), "bc".into()]).unwrap(), 0x0A0C_405D_7C26_5DFF);
        assert_eq!(shard_block(i42, Integer).unwrap(), 0xE53D_EF32_5ADD);
    }

    #[test]
    fn integer_widths_agree_on_a_value() {
        let all: [KeyValue; 9] = [
            42i8.into(),
            42i16.into(),
            42i32.into(),
            42i64.into(),
            42i128.into(),
            42u8.into(),
            42u16.into(),
            42u32.into(),
            42u64.into(),
        ];
        let want = shard_block(KeyValue::Int(42), KeyFamily::Integer).unwrap();
        for v in all {
            assert_eq!(shard_block(v, KeyFamily::Integer).unwrap(), want);
        }
        // Sign extension for signed types, zero extension for unsigned.
        assert_eq!(KeyValue::from(-1i8), KeyValue::Int(-1));
        assert_eq!(KeyValue::from(255u8), KeyValue::Int(255));
        assert_eq!(KeyValue::from(u64::MAX), KeyValue::Int(u64::MAX as i128));
    }

    #[test]
    fn the_column_family_decides() {
        assert!(KeyValue::Int(1).level_hash(0, KeyFamily::Text).is_err());
        assert!(KeyValue::Text("1").level_hash(0, KeyFamily::Integer).is_err());
        assert!(KeyValue::Uuid(UUID).level_hash(0, KeyFamily::Text).is_err());
        assert!(point_of(&[KeyFamily::Integer], &[]).is_err());
        assert!(prefix_range(&[], &[]).is_err());
        assert!(level_hash(3, KeyFamily::Text, b"").is_err());
    }

    #[test]
    fn a_leading_value_gives_the_same_block_at_every_arity() {
        use KeyFamily::*;
        let t = KeyValue::Int(7);
        let one = prefix_range(&[Integer], &[t]).unwrap();
        let two = prefix_range(&[Integer, Text], &[t]).unwrap();
        let three = prefix_range(&[Integer, Integer, Uuid], &[t]).unwrap();
        assert_eq!(one, two);
        assert_eq!(one, three);
        assert_eq!(one.hi - one.lo as u128, 1 << 16);
        assert_eq!(block_of(one.lo), shard_block(t, Integer).unwrap());
    }

    #[test]
    fn the_top_block_ends_at_the_end_of_the_space() {
        for arity in 1..=MAX_ARITY {
            let w = widths(arity).unwrap();
            assert_eq!(assemble(&[u64::MAX], w), PointRange { lo: 0xFFFF_FFFF_FFFF_0000, hi: SPACE_END });
        }
        assert_eq!(assemble(&[], widths(2).unwrap()), PointRange::FULL);
    }

    #[test]
    fn frame_layout_and_long_frames() {
        assert_eq!(frame(KeyFamily::Text, b"ab"), vec![0x03, 2, 0, 0, 0, b'a', b'b']);
        assert_eq!(frame(KeyFamily::Uuid, &[]), vec![0x02, 0, 0, 0, 0]);
        // Either side of the stack frame, the hash is of the frame itself.
        for n in [0, INLINE_FRAME - 6, INLINE_FRAME - 5, INLINE_FRAME - 4, 300] {
            let v = vec![b'z'; n];
            assert_eq!(
                level_hash(1, KeyFamily::Text, &v).unwrap(),
                xxh64(&frame(KeyFamily::Text, &v), LEVEL_SEED + 1),
                "{n} bytes"
            );
        }
        for tag in 0..=255u8 {
            if let Some(f) = KeyFamily::from_tag(tag) {
                assert_eq!(f.tag(), tag);
            }
        }
    }
}
