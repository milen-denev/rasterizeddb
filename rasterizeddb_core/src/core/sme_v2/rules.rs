use crate::core::{db_type::DbType, row::row_pointer::ROW_POINTER_RECORD_LEN};

pub type RangeVec = smallvec::SmallVec<[RowRange; 64]>;

/// Numeric correlation rule operation.
///
/// Interpreted as a *predicate on the stored row value* when building rule ranges:
/// - `LessThan`: rows where `value < threshold`.
/// - `GreaterThan`: rows where `value > threshold`.
///
/// Query-time candidate selection should choose thresholds such that the selected
/// rule range sets are a superset of the query predicate (to avoid false negatives).
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
    /// Starting byte offset inside the row-pointer file.
    ///
    /// Must be aligned to `ROW_POINTER_RECORD_LEN`.
    pub start_pointer_pos: u64,
    /// Number of row-pointer records to read starting at `start_pointer_pos`.
    pub row_count: u64,
}

impl RowRange {
    #[inline]
    pub fn end_pointer_pos_exclusive(self) -> u64 {
        use crate::core::row::row_pointer::ROW_POINTER_RECORD_LEN;
        let len = ROW_POINTER_RECORD_LEN as u64;
        self.start_pointer_pos
            .saturating_add(self.row_count.saturating_mul(len))
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.row_count == 0
    }

    /// Returns the inclusive end pointer position (byte offset) covered by this range.
    ///
    /// If `row_count == u64::MAX`, this is treated as an unbounded range and returns `u64::MAX`.
    #[inline]
    pub fn end_pointer_pos_inclusive(&self) -> u64 {
        if self.row_count == 0 {
            return self.start_pointer_pos;
        }
        if self.row_count == u64::MAX {
            return u64::MAX;
        }

        let rec = ROW_POINTER_RECORD_LEN as u64;
        debug_assert!(rec > 0);
        debug_assert!(self.start_pointer_pos % rec == 0);

        // end = start + (row_count - 1) * rec
        let delta = self.row_count.saturating_sub(1).saturating_mul(rec);
        self.start_pointer_pos.saturating_add(delta)
    }

    /// Constructs a range from inclusive pointer bounds.
    ///
    /// If `end_pointer_pos_inclusive == u64::MAX`, the resulting range is treated as unbounded
    /// and will have `row_count == u64::MAX`.
    #[inline]
    pub fn from_pointer_bounds_inclusive(start_pointer_pos: u64, end_pointer_pos_inclusive: u64) -> Self {
        let rec = ROW_POINTER_RECORD_LEN as u64;
        debug_assert!(rec > 0);
        debug_assert!(start_pointer_pos % rec == 0);

        if end_pointer_pos_inclusive == u64::MAX {
            return Self {
                start_pointer_pos,
                row_count: u64::MAX,
            };
        }

        debug_assert!(end_pointer_pos_inclusive >= start_pointer_pos);
        debug_assert!((end_pointer_pos_inclusive - start_pointer_pos) % rec == 0);

        let rows_minus_1 = (end_pointer_pos_inclusive - start_pointer_pos) / rec;
        Self {
            start_pointer_pos,
            row_count: rows_minus_1.saturating_add(1),
        }
    }
    
    /// Convenience for tests/fixtures: construct from inclusive *virtual row indices*.
    ///
    /// The virtual row index `i` maps to pointer position `i * ROW_POINTER_RECORD_LEN`.
    #[inline]
    pub fn from_row_id_range_inclusive(start_row_id: u64, end_row_id: u64) -> Self {
        let rec = ROW_POINTER_RECORD_LEN as u64;
        debug_assert!(rec > 0);

        let start_pointer_pos = match start_row_id.checked_mul(rec) {
            Some(v) => v,
            None => {
                // Saturate to the largest aligned pointer position.
                u64::MAX - (u64::MAX % rec)
            }
        };
        if end_row_id == u64::MAX {
            return Self {
                start_pointer_pos,
                row_count: u64::MAX,
            };
        }

        debug_assert!(end_row_id >= start_row_id);
        let row_count = end_row_id
            .saturating_sub(start_row_id)
            .saturating_add(1);

        Self {
            start_pointer_pos,
            row_count,
        }
    }
}

/// In-memory numeric correlation rule.
#[derive(Debug, Clone, PartialEq)]
pub struct NumericCorrelationRule {
    pub column_schema_id: u64,
    pub column_type: DbType,
    pub op: NumericRuleOp,
    pub value: NumericScalar,
    pub ranges: RangeVec,
}

/// String correlation rule operation.
///
/// Interpreted as a predicate on the stored row value when building ranges.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StringRuleOp {
    StartsWith = 1,
    EndsWith = 2,
    Contains = 3,
}

impl StringRuleOp {
    #[inline]
    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::StartsWith),
            2 => Some(Self::EndsWith),
            3 => Some(Self::Contains),
            _ => None,
        }
    }
}

pub const STRING_RULE_VALUE_MAX_BYTES: usize = 16;

/// In-memory string correlation rule.
///
/// `count` is primarily meaningful for `Contains`:
/// - `Contains` + `value="A"` + `count=1` => contains at least 1 'A'
/// - `Contains` + `value="A"` + `count=5` => contains at least 5 'A'
#[derive(Debug, Clone, PartialEq)]
pub struct StringCorrelationRule {
    pub column_schema_id: u64,
    pub column_type: DbType,
    pub op: StringRuleOp,
    pub count: u8,
    pub value: String,
    pub ranges: RangeVec,
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

/// Fixed-size on-disk string rule record.
///
/// Layout (32 bytes):
/// - op: u8
/// - count: u8
/// - value_len: u8 (0..=16, UTF-8 bytes)
/// - padding: u8
/// - value: [u8; 16] (UTF-8 bytes, remainder zeroed)
/// - ranges_offset: u64 (absolute file offset)
/// - ranges_count: u32
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StringRuleRecordDisk {
    pub op: u8,
    pub count: u8,
    pub value_len: u8,
    pub value: [u8; 16],
    pub ranges_offset: u64,
    pub ranges_count: u32,
}

impl StringRuleRecordDisk {
    pub const BYTE_LEN: usize = 32;

    #[inline]
    pub fn encode(&self) -> [u8; Self::BYTE_LEN] {
        let mut out = [0u8; Self::BYTE_LEN];
        out[0] = self.op;
        out[1] = self.count;
        out[2] = self.value_len;
        out[3] = 0;
        out[4..20].copy_from_slice(&self.value);
        out[20..28].copy_from_slice(&self.ranges_offset.to_le_bytes());
        out[28..32].copy_from_slice(&self.ranges_count.to_le_bytes());
        out
    }

    #[inline]
    pub fn decode(bytes: &[u8; Self::BYTE_LEN]) -> Option<Self> {
        let op = bytes[0];
        let count = bytes[1];
        let value_len = bytes[2];
        if value_len as usize > STRING_RULE_VALUE_MAX_BYTES {
            return None;
        }
        if StringRuleOp::from_byte(op).is_none() {
            return None;
        }

        let mut value = [0u8; 16];
        value.copy_from_slice(&bytes[4..20]);

        let ranges_offset = u64::from_le_bytes(bytes[20..28].try_into().unwrap());
        let ranges_count = u32::from_le_bytes(bytes[28..32].try_into().unwrap());

        Some(Self {
            op,
            count,
            value_len,
            value,
            ranges_offset,
            ranges_count,
        })
    }

    #[inline]
    pub fn decoded_value(&self) -> Option<String> {
        let len = self.value_len as usize;
        let bytes = &self.value[..len];
        std::str::from_utf8(bytes).ok().map(|s| s.to_string())
    }
}

#[inline]
pub fn encode_string_rule_value(value: &str) -> Option<(u8, [u8; 16])> {
    let bytes = value.as_bytes();
    if bytes.len() > STRING_RULE_VALUE_MAX_BYTES {
        return None;
    }
    let mut out = [0u8; 16];
    out[..bytes.len()].copy_from_slice(bytes);
    Some((bytes.len() as u8, out))
}

#[inline]
pub fn normalize_ranges(ranges: Vec<RowRange>) -> Vec<RowRange> {
    use crate::core::sme_v2::sme_range_processor_common::merge_row_ranges;
    use smallvec::SmallVec;

    // Delegate to the shared merging implementation used by the SME range processors.
    // This also drops any empty ranges.
    merge_row_ranges(SmallVec::<[RowRange; 64]>::from_vec(ranges)).into_vec()
}

#[inline]
pub fn normalize_ranges_smallvec(ranges: Vec<RowRange>) -> smallvec::SmallVec<[RowRange; 64]> {
    use crate::core::sme_v2::sme_range_processor_common::merge_row_ranges;
    use smallvec::SmallVec;

    merge_row_ranges(SmallVec::<[RowRange; 64]>::from_vec(ranges))
}

#[inline]
pub fn normalize_range_vec(ranges: RangeVec) -> RangeVec {
    use crate::core::sme_v2::sme_range_processor_common::merge_row_ranges;
    merge_row_ranges(ranges)
}

#[inline]
pub fn intersect_ranges(a: &[RowRange], b: &[RowRange]) -> RangeVec {
    use crate::core::row::row_pointer::ROW_POINTER_RECORD_LEN;
    use smallvec::SmallVec;
    let len = ROW_POINTER_RECORD_LEN as u64;

    let mut out: RangeVec = SmallVec::new();
    let mut i = 0usize;
    let mut j = 0usize;
    while i < a.len() && j < b.len() {
        let ra = a[i];
        let rb = b[j];

        // Convert to record-index intervals (inclusive) to avoid alignment issues.
        if ra.row_count == 0 {
            i += 1;
            continue;
        }
        if rb.row_count == 0 {
            j += 1;
            continue;
        }

        let a_start = ra.start_pointer_pos / len;
        let a_end = ra.end_pointer_pos_inclusive() / len;
        let b_start = rb.start_pointer_pos / len;
        let b_end = rb.end_pointer_pos_inclusive() / len;

        let lo = a_start.max(b_start);
        let hi = a_end.min(b_end);
        if lo <= hi {
            let count = hi - lo + 1;
            out.push(RowRange {
                start_pointer_pos: lo.saturating_mul(len),
                row_count: count,
            });
        }

        if a_end < b_end {
            i += 1;
        } else {
            j += 1;
        }
    }
    out
}
