use crate::core::db_type::DbType;

/// Supported semantic rule operations.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SemanticRuleOp {
    /// Special record used as a file header/metadata record.
    /// Stored as a normal fixed-size record with `rule_id = 0`.
    Meta = 0,
    BelowExclusive = 1,
    AboveOrEqual = 2,
    BetweenInclusive = 3,

    // ---- String-derived semantic rules ----
    /// Bitset (256 bits) of first bytes observed in the window.
    /// Stored as lower[0..16] + upper[0..16].
    StrFirstByteSet = 10,
    /// Bitset (256 bits) of last bytes observed in the window.
    /// Stored as lower[0..16] + upper[0..16].
    StrLastByteSet = 11,
    /// Bitset (256 bits) of bytes observed anywhere in the window's strings.
    /// Stored as lower[0..16] + upper[0..16].
    StrCharSet = 12,
    /// Min/max string byte length observed in the window.
    /// Stored as u64 LE in lower[0..8] and upper[0..8].
    StrLenRange = 13,
    /// Max run length (consecutive repeats) observed in the window.
    /// Stored as [0, max] as u64 LE in lower/upper.
    StrMaxRunLen = 14,
    /// Max count of any single byte observed in the window (frequency within one string).
    /// Stored as [0, max] as u64 LE in lower/upper.
    StrMaxCharCount = 15,
}

impl SemanticRuleOp {
    #[inline]
    pub fn from_byte(b: u8) -> Self {
        match b {
            0 => Self::Meta,
            1 => Self::BelowExclusive,
            2 => Self::AboveOrEqual,
            3 => Self::BetweenInclusive,
            10 => Self::StrFirstByteSet,
            11 => Self::StrLastByteSet,
            12 => Self::StrCharSet,
            13 => Self::StrLenRange,
            14 => Self::StrMaxRunLen,
            15 => Self::StrMaxCharCount,
            _ => panic!("Invalid SemanticRuleOp byte: {}", b),
        }
    }
}

/// A binary-storable semantic rule.
///
/// Encoding is fixed-size to avoid length-prefix parsing overhead.
/// Record layout (72 bytes total):
/// - rule_id: u64
/// - column_schema_id: u64
/// - column_type: u8 (DbType::to_byte)
/// - op: u8 (SemanticRuleOp)
/// - reserved: [u8; 6]
/// - lower: [u8; 16] (type-dependent)
/// - upper: [u8; 16] (type-dependent)
/// - start_row_id: u64
/// - end_row_id: u64
#[derive(Debug, Clone, PartialEq, Eq)]
#[repr(C)]
pub struct SemanticRule {
    pub rule_id: u64,
    pub column_schema_id: u64,
    pub column_type: DbType,
    pub op: SemanticRuleOp,

    /// Reserved padding to keep this struct's layout stable and aligned with the on-disk
    /// 72-byte record format.
    pub reserved: [u8; 6],

    /// Type-dependent bounds.
    ///
    /// For integers, store sign-extended i128 in LE bytes.
    /// For unsigned, store u128 in LE bytes.
    /// For floats, store IEEE bits in the lowest 8 bytes (upper bytes zero).
    pub lower: [u8; 16],
    pub upper: [u8; 16],

    pub start_row_id: u64,
    pub end_row_id: u64,
}

impl SemanticRule {
    pub const BYTE_LEN: usize = 72;

    #[inline]
    pub fn encode(&self) -> [u8; Self::BYTE_LEN] {
        let mut out = [0u8; Self::BYTE_LEN];

        out[0..8].copy_from_slice(&self.rule_id.to_le_bytes());
        out[8..16].copy_from_slice(&self.column_schema_id.to_le_bytes());
        out[16] = self.column_type.to_byte();
        out[17] = self.op as u8;
        out[18..24].copy_from_slice(&self.reserved);
        out[24..40].copy_from_slice(&self.lower);
        out[40..56].copy_from_slice(&self.upper);
        out[56..64].copy_from_slice(&self.start_row_id.to_le_bytes());
        out[64..72].copy_from_slice(&self.end_row_id.to_le_bytes());

        out
    }

    #[inline]
    pub fn decode(bytes: &[u8; Self::BYTE_LEN]) -> Self {
        let rule_id = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let column_schema_id = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
        let column_type = DbType::from_byte(bytes[16]);
        let op = SemanticRuleOp::from_byte(bytes[17]);

        let mut reserved = [0u8; 6];
        reserved.copy_from_slice(&bytes[18..24]);

        let mut lower = [0u8; 16];
        let mut upper = [0u8; 16];
        lower.copy_from_slice(&bytes[24..40]);
        upper.copy_from_slice(&bytes[40..56]);

        let start_row_id = u64::from_le_bytes(bytes[56..64].try_into().unwrap());
        let end_row_id = u64::from_le_bytes(bytes[64..72].try_into().unwrap());

        Self {
            rule_id,
            column_schema_id,
            column_type,
            op,
            reserved,
            lower,
            upper,
            start_row_id,
            end_row_id,
        }
    }

    #[inline]
    pub fn lower_as_i128(&self) -> i128 {
        i128::from_le_bytes(self.lower)
    }

    #[inline]
    pub fn upper_as_i128(&self) -> i128 {
        i128::from_le_bytes(self.upper)
    }

    #[inline]
    pub fn lower_as_u128(&self) -> u128 {
        u128::from_le_bytes(self.lower)
    }

    #[inline]
    pub fn upper_as_u128(&self) -> u128 {
        u128::from_le_bytes(self.upper)
    }

    #[inline]
    pub fn lower_as_f64_bits(&self) -> u64 {
        u64::from_le_bytes(self.lower[0..8].try_into().unwrap())
    }

    #[inline]
    pub fn upper_as_f64_bits(&self) -> u64 {
        u64::from_le_bytes(self.upper[0..8].try_into().unwrap())
    }
}

#[inline]
pub fn i128_to_16le(v: i128) -> [u8; 16] {
    v.to_le_bytes()
}

#[inline]
pub fn u128_to_16le(v: u128) -> [u8; 16] {
    v.to_le_bytes()
}

#[inline]
pub fn f64_bits_to_16le(bits: u64) -> [u8; 16] {
    let mut out = [0u8; 16];
    out[0..8].copy_from_slice(&bits.to_le_bytes());
    out
}
