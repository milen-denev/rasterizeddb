use crate::core::db_type::DbType;

/// Numeric correlation rule operation.
///
/// Interpreted as a *predicate on the query value* when selecting candidate rules:
/// - `LessThan`: match if `query < threshold`.
/// - `GreaterThan`: match if `query > threshold`.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NumericRuleOp {
    LessThan = 1,
    GreaterThan = 2,
}

impl NumericRuleOp {
    #[inline]
    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::LessThan),
            2 => Some(Self::GreaterThan),
            _ => None,
        }
    }
}

/// Row-id range (inclusive).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowRange {
    pub start_row_id: u64,
    pub end_row_id: u64,
}

/// In-memory numeric correlation rule.
#[derive(Debug, Clone, PartialEq)]
pub struct NumericCorrelationRule {
    pub column_schema_id: u64,
    pub column_type: DbType,
    pub op: NumericRuleOp,
    pub value: NumericScalar,
    pub ranges: Vec<RowRange>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum NumericScalar {
    Signed(i128),
    Unsigned(u128),
    Float(f64),
}

impl NumericScalar {
    #[inline]
    pub fn encode_16le(self) -> [u8; 16] {
        match self {
            Self::Signed(v) => v.to_le_bytes(),
            Self::Unsigned(v) => v.to_le_bytes(),
            Self::Float(v) => {
                let mut out = [0u8; 16];
                out[0..8].copy_from_slice(&v.to_bits().to_le_bytes());
                out
            }
        }
    }
}

/// Fixed-size on-disk rule record.
///
/// This record does *not* inline ranges; instead it points to a range-list pool.
///
/// Per-record metadata like `column_schema_id`, `column_type`, and rule op are
/// stored in the per-column header (or implied by section ordering). This keeps
/// per-record bytes minimal.
///
/// Layout (28 bytes):
/// - value: [u8; 16] (i128/u128 LE or f64 bits in [0..8])
/// - ranges_offset: u64 (absolute file offset)
/// - ranges_count: u32 (number of (u64,u64) pairs)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NumericRuleRecordDisk {
    pub value: [u8; 16],
    pub ranges_offset: u64,
    pub ranges_count: u32,
}

impl NumericRuleRecordDisk {
    pub const BYTE_LEN: usize = 28;

    #[inline]
    pub fn encode(&self) -> [u8; Self::BYTE_LEN] {
        let mut out = [0u8; Self::BYTE_LEN];
        out[0..16].copy_from_slice(&self.value);
        out[16..24].copy_from_slice(&self.ranges_offset.to_le_bytes());
        out[24..28].copy_from_slice(&self.ranges_count.to_le_bytes());
        out
    }

    #[inline]
    pub fn decode(bytes: &[u8; Self::BYTE_LEN]) -> Option<Self> {
        let mut value = [0u8; 16];
        value.copy_from_slice(&bytes[0..16]);

        let ranges_offset = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
        let ranges_count = u32::from_le_bytes(bytes[24..28].try_into().unwrap());

        Some(Self {
            value,
            ranges_offset,
            ranges_count,
        })
    }

    #[inline]
    pub fn decoded_scalar(&self, column_type: &DbType) -> NumericScalar {
        match column_type {
            DbType::F32 | DbType::F64 => {
                let bits = u64::from_le_bytes(self.value[0..8].try_into().unwrap());
                NumericScalar::Float(f64::from_bits(bits))
            }
            DbType::I8
            | DbType::I16
            | DbType::I32
            | DbType::I64
            | DbType::I128 => NumericScalar::Signed(i128::from_le_bytes(self.value)),
            DbType::U8 | DbType::U16 | DbType::U32 | DbType::U64 | DbType::U128 => {
                NumericScalar::Unsigned(u128::from_le_bytes(self.value))
            }
            _ => NumericScalar::Unsigned(u128::from_le_bytes(self.value)),
        }
    }
}

#[inline]
pub fn normalize_ranges(mut ranges: Vec<RowRange>) -> Vec<RowRange> {
    if ranges.len() <= 1 {
        return ranges;
    }
    ranges.sort_by_key(|r| r.start_row_id);
    let mut out = Vec::with_capacity(ranges.len());

    let mut cur = ranges[0];
    for r in ranges.into_iter().skip(1) {
        if r.start_row_id <= cur.end_row_id.saturating_add(1) {
            cur.end_row_id = cur.end_row_id.max(r.end_row_id);
        } else {
            out.push(cur);
            cur = r;
        }
    }
    out.push(cur);
    out
}

#[inline]
pub fn intersect_ranges(a: &[RowRange], b: &[RowRange]) -> Vec<RowRange> {
    let mut out = Vec::new();
    let mut i = 0usize;
    let mut j = 0usize;
    while i < a.len() && j < b.len() {
        let lo = a[i].start_row_id.max(b[j].start_row_id);
        let hi = a[i].end_row_id.min(b[j].end_row_id);
        if lo <= hi {
            out.push(RowRange {
                start_row_id: lo,
                end_row_id: hi,
            });
        }

        if a[i].end_row_id < b[j].end_row_id {
            i += 1;
        } else {
            j += 1;
        }
    }
    out
}
