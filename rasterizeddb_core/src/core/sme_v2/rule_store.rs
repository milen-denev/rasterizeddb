use std::{collections::BTreeMap, io, io::SeekFrom};

use rclite::Arc;

use crate::core::storage_providers::traits::StorageIO;

use super::rules::{
    normalize_range_vec,
    RangeVec,
    NumericCorrelationRule, 
    NumericRuleOp, 
    NumericRuleRecordDisk,
    StringCorrelationRule,
    StringRuleOp,
    StringRuleRecordDisk,
    encode_string_rule_value,
    RowRange,
};

/// Rules file format (v2):
/// - u32 numeric_header_len_bytes (NOT including these u32s)
/// - u32 string_header_len_bytes  (NOT including these u32s)
/// - numeric header bytes: repeated per-column numeric entries
/// - string header bytes: repeated per-column string entries
/// - body bytes: per-column contiguous section(s)
///
/// Numeric header entry layout (40 bytes):
/// - column_schema_id: u64
/// - column_type: u8 (DbType::to_byte)
/// - padding: [u8; 7]
/// - counts_packed: u64 (lt_count in low32, gt_count in high32)
/// - start_offset: u64 (absolute file offset)
/// - end_offset: u64   (absolute file offset)
///
/// String header entry layout (48 bytes):
/// - column_schema_id: u64
/// - column_type: u8 (DbType::to_byte)
/// - padding: [u8; 7]
/// - startswith_count: u32
/// - endswith_count: u32
/// - contains_count: u32
/// - padding: u32
/// - start_offset: u64 (absolute file offset)
/// - end_offset: u64   (absolute file offset)
///
/// Offsets are stored as absolute positions from the start of the file.
const FILE_PREFIX_LEN_V2: u64 = 8;
const NUMERIC_COLUMN_HEADER_ENTRY_LEN: usize = 8 + 1 + 7 + 8 + 8 + 8;
const STRING_COLUMN_HEADER_ENTRY_LEN: usize = 8 + 1 + 7 + 4 + 4 + 4 + 4 + 8 + 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RulesHeaderFormatV2 {
    Numeric40String48,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColumnHeaderEntryV2 {
    pub column_schema_id: u64,
    pub column_type: u8,
    pub lt_rule_count: u32,
    pub gt_rule_count: u32,
    pub rule_count: u32,
    pub start_offset: u64,
    pub end_offset: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StringColumnHeaderEntryV2 {
    pub column_schema_id: u64,
    pub column_type: u8,
    pub startswith_rule_count: u32,
    pub endswith_rule_count: u32,
    pub contains_rule_count: u32,
    pub rule_count: u32,
    pub start_offset: u64,
    pub end_offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RulesFileHeaderV2 {
    pub numeric_header_len_bytes: u32,
    pub string_header_len_bytes: u32,
    pub format: RulesHeaderFormatV2,
    pub numeric_columns: Vec<ColumnHeaderEntryV2>,
    pub string_columns: Vec<StringColumnHeaderEntryV2>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NumericColumnRulesIndex {
    pub column_schema_id: u64,
    pub column_type: crate::core::db_type::DbType,
    pub lt: Vec<NumericRuleRecordDisk>,
    pub gt: Vec<NumericRuleRecordDisk>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StringColumnRulesIndex {
    pub column_schema_id: u64,
    pub column_type: crate::core::db_type::DbType,
    pub startswith: Vec<StringRuleRecordDisk>,
    pub endswith: Vec<StringRuleRecordDisk>,
    pub contains: Vec<StringRuleRecordDisk>,
}

pub struct CorrelationRuleStore;

impl CorrelationRuleStore {
    /// Creates/opens the correlation rules file for a table.
    ///
    /// Kept separate from the existing SME v1 rule file name.
    pub async fn open_rules_io<S: StorageIO>(base_io: Arc<S>, table_name: &str) -> Arc<S> {
        Arc::new(base_io.create_new(format!("{}_rules_v2.db", table_name)).await)
    }

    pub async fn try_load_header_v2<S: StorageIO>(
        rules_io: Arc<S>,
    ) -> io::Result<Option<RulesFileHeaderV2>> {
        let file_len = rules_io.get_len().await;
        if file_len == 0 {
            return Ok(None);
        }
        if file_len < FILE_PREFIX_LEN_V2 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Rules file too small for v2 prefix size: {}", file_len),
            ));
        }

        let mut pos = 0u64;
        let mut prefix_buf = [0u8; 8];
        rules_io
            .read_data_into_buffer(&mut pos, &mut prefix_buf)
            .await?;

        let numeric_header_len_bytes = u32::from_le_bytes(prefix_buf[0..4].try_into().unwrap());
        let string_header_len_bytes = u32::from_le_bytes(prefix_buf[4..8].try_into().unwrap());

        let numeric_header_start = FILE_PREFIX_LEN_V2;
        let numeric_header_end = numeric_header_start + (numeric_header_len_bytes as u64);
        let string_header_start = numeric_header_end;
        let string_header_end = string_header_start + (string_header_len_bytes as u64);
        let header_end = string_header_end;

        if header_end > file_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Headers extend beyond file: header_end={} file_len={} (numeric_len={} string_len={})",
                    header_end,
                    file_len,
                    numeric_header_len_bytes,
                    string_header_len_bytes
                ),
            ));
        }

        // Determine header entry format by divisibility.
        if (numeric_header_len_bytes as usize) % NUMERIC_COLUMN_HEADER_ENTRY_LEN != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Numeric header length not divisible by {}: {}",
                    NUMERIC_COLUMN_HEADER_ENTRY_LEN, numeric_header_len_bytes
                ),
            ));
        }
        if (string_header_len_bytes as usize) % STRING_COLUMN_HEADER_ENTRY_LEN != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "String header length not divisible by {}: {}",
                    STRING_COLUMN_HEADER_ENTRY_LEN, string_header_len_bytes
                ),
            ));
        }

        let header_format = RulesHeaderFormatV2::Numeric40String48;

        // Read numeric header.
        let numeric_entry_count = (numeric_header_len_bytes as usize) / NUMERIC_COLUMN_HEADER_ENTRY_LEN;
        let mut numeric_header_buf = vec![0u8; numeric_header_len_bytes as usize];
        let mut numeric_pos = numeric_header_start;
        if numeric_header_len_bytes != 0 {
            rules_io
                .read_data_into_buffer(&mut numeric_pos, &mut numeric_header_buf)
                .await?;
        }

        let mut numeric_columns = Vec::with_capacity(numeric_entry_count);
        for chunk in numeric_header_buf.chunks_exact(NUMERIC_COLUMN_HEADER_ENTRY_LEN) {
            let column_schema_id = u64::from_le_bytes(chunk[0..8].try_into().unwrap());
            let column_type = chunk[8];
            // padding at 9..16
            let counts_packed = u64::from_le_bytes(chunk[16..24].try_into().unwrap());
            let lt_rule_count = (counts_packed & 0xFFFF_FFFF) as u32;
            let gt_rule_count = (counts_packed >> 32) as u32;
            let start_offset = u64::from_le_bytes(chunk[24..32].try_into().unwrap());
            let end_offset = u64::from_le_bytes(chunk[32..40].try_into().unwrap());
            let rule_count = lt_rule_count.saturating_add(gt_rule_count);

            // Basic validation.
            if start_offset > end_offset {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Invalid numeric column range for col {}: start={} end={}",
                        column_schema_id, start_offset, end_offset
                    ),
                ));
            }
            if start_offset < header_end {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Numeric column rules start inside headers for col {}: start={} header_end={} ",
                        column_schema_id, start_offset, header_end
                    ),
                ));
            }
            if end_offset > file_len {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Numeric column rules end beyond file for col {}: end={} file_len={} ",
                        column_schema_id, end_offset, file_len
                    ),
                ));
            }

            numeric_columns.push(ColumnHeaderEntryV2 {
                column_schema_id,
                column_type,
                lt_rule_count,
                gt_rule_count,
                rule_count,
                start_offset,
                end_offset,
            });
        }

        // Read string header.
        let string_entry_count = (string_header_len_bytes as usize) / STRING_COLUMN_HEADER_ENTRY_LEN;
        let mut string_header_buf = vec![0u8; string_header_len_bytes as usize];
        let mut string_pos = string_header_start;
        if string_header_len_bytes != 0 {
            rules_io
                .read_data_into_buffer(&mut string_pos, &mut string_header_buf)
                .await?;
        }

        let mut string_columns = Vec::with_capacity(string_entry_count);
        for chunk in string_header_buf.chunks_exact(STRING_COLUMN_HEADER_ENTRY_LEN) {
            let column_schema_id = u64::from_le_bytes(chunk[0..8].try_into().unwrap());
            let column_type = chunk[8];
            // padding at 9..16
            let startswith_rule_count = u32::from_le_bytes(chunk[16..20].try_into().unwrap());
            let endswith_rule_count = u32::from_le_bytes(chunk[20..24].try_into().unwrap());
            let contains_rule_count = u32::from_le_bytes(chunk[24..28].try_into().unwrap());
            // padding 28..32
            let start_offset = u64::from_le_bytes(chunk[32..40].try_into().unwrap());
            let end_offset = u64::from_le_bytes(chunk[40..48].try_into().unwrap());
            let rule_count = startswith_rule_count
                .saturating_add(endswith_rule_count)
                .saturating_add(contains_rule_count);

            if start_offset > end_offset {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Invalid string column range for col {}: start={} end={} ",
                        column_schema_id, start_offset, end_offset
                    ),
                ));
            }
            if start_offset < header_end {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "String column rules start inside headers for col {}: start={} header_end={} ",
                        column_schema_id, start_offset, header_end
                    ),
                ));
            }
            if end_offset > file_len {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "String column rules end beyond file for col {}: end={} file_len={} ",
                        column_schema_id, end_offset, file_len
                    ),
                ));
            }

            string_columns.push(StringColumnHeaderEntryV2 {
                column_schema_id,
                column_type,
                startswith_rule_count,
                endswith_rule_count,
                contains_rule_count,
                rule_count,
                start_offset,
                end_offset,
            });
        }

        Ok(Some(RulesFileHeaderV2 {
            numeric_header_len_bytes,
            string_header_len_bytes,
            format: header_format,
            numeric_columns,
            string_columns,
        }))
    }

    pub async fn load_numeric_column_rules_index_for_column<S: StorageIO>(
        rules_io: Arc<S>,
        header: &RulesFileHeaderV2,
        column_schema_id: u64,
    ) -> io::Result<Option<NumericColumnRulesIndex>> {
        let Some(col) = header
            .numeric_columns
            .iter()
            .find(|c| c.column_schema_id == column_schema_id)
            .copied()
        else {
            return Ok(None);
        };

        if col.rule_count == 0 {
            return Ok(Some(NumericColumnRulesIndex {
                column_schema_id,
                column_type: if col.column_type == 0 {
                    crate::core::db_type::DbType::UNKNOWN
                } else {
                    crate::core::db_type::DbType::from_byte(col.column_type)
                },
                lt: Vec::new(),
                gt: Vec::new(),
            }));
        }

        // Simplified logic (legacy formats removed)
        if col.column_type == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Missing numeric column_type for col {}", column_schema_id),
            ));
        }
        let column_type = crate::core::db_type::DbType::from_byte(col.column_type);

        let lt_count = col.lt_rule_count as usize;
        let gt_count = col.gt_rule_count as usize;
        let total_count = lt_count.saturating_add(gt_count);

        let records_span = total_count * NumericRuleRecordDisk::BYTE_LEN;
        let total_span = (col.end_offset - col.start_offset) as usize;
        if total_span < records_span {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Column section too small for rule records: total_span={} records_span={} col={}",
                    total_span, records_span, column_schema_id
                ),
            ));
        }

        let mut buf = vec![0u8; records_span];
        let mut pos = col.start_offset;
        rules_io.read_data_into_buffer(&mut pos, &mut buf).await?;

        let mut all = Vec::with_capacity(total_count);
        for chunk in buf.chunks_exact(NumericRuleRecordDisk::BYTE_LEN) {
            let bytes: &[u8; NumericRuleRecordDisk::BYTE_LEN] = chunk.try_into().unwrap();
            let rec = NumericRuleRecordDisk::decode(bytes).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Failed to decode numeric rule record for col {}",
                        column_schema_id
                    ),
                )
            })?;
            all.push(rec);
        }

        let lt = all[..lt_count].to_vec();
        let gt = all[lt_count..].to_vec();

        Ok(Some(NumericColumnRulesIndex {
            column_schema_id,
            column_type,
            lt,
            gt,
        }))
    }

    pub async fn load_numeric_rules_for_column<S: StorageIO>(
        rules_io: Arc<S>,
        header: &RulesFileHeaderV2,
        column_schema_id: u64,
    ) -> io::Result<Vec<NumericCorrelationRule>> {
        let Some(idx) =
            Self::load_numeric_column_rules_index_for_column(rules_io.clone(), header, column_schema_id)
                .await?
        else {
            return Ok(Vec::new());
        };

        let column_type = idx.column_type.clone();

        let total = idx.lt.len().saturating_add(idx.gt.len());
        let mut out = Vec::with_capacity(total);

        for rec in idx.lt.iter().copied() {
            let ranges = RangeVec::from_vec(Self::load_ranges_for_rule(rules_io.clone(), rec).await?);
            out.push(NumericCorrelationRule {
                column_schema_id,
                column_type: column_type.clone(),
                op: NumericRuleOp::LessThan,
                value: rec.decoded_scalar(&column_type),
                ranges,
            });
        }

        for rec in idx.gt.iter().copied() {
            let ranges = RangeVec::from_vec(Self::load_ranges_for_rule(rules_io.clone(), rec).await?);
            out.push(NumericCorrelationRule {
                column_schema_id,
                column_type: column_type.clone(),
                op: NumericRuleOp::GreaterThan,
                value: rec.decoded_scalar(&column_type),
                ranges,
            });
        }

        Ok(out)
    }

    pub async fn load_string_column_rules_index_for_column<S: StorageIO>(
        rules_io: Arc<S>,
        header: &RulesFileHeaderV2,
        column_schema_id: u64,
    ) -> io::Result<Option<StringColumnRulesIndex>> {
        let Some(col) = header
            .string_columns
            .iter()
            .find(|c| c.column_schema_id == column_schema_id)
            .copied()
        else {
            return Ok(None);
        };

        let column_type = if col.column_type == 0 {
            crate::core::db_type::DbType::UNKNOWN
        } else {
            crate::core::db_type::DbType::from_byte(col.column_type)
        };

        if col.rule_count == 0 {
            return Ok(Some(StringColumnRulesIndex {
                column_schema_id,
                column_type,
                startswith: Vec::new(),
                endswith: Vec::new(),
                contains: Vec::new(),
            }));
        }

        let sw_count = col.startswith_rule_count as usize;
        let ew_count = col.endswith_rule_count as usize;
        let c_count = col.contains_rule_count as usize;
        let total_count = sw_count.saturating_add(ew_count).saturating_add(c_count);

        let records_span = total_count * StringRuleRecordDisk::BYTE_LEN;
        let total_span = (col.end_offset - col.start_offset) as usize;
        if total_span < records_span {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "String column section too small for rule records: total_span={} records_span={} col={}",
                    total_span, records_span, column_schema_id
                ),
            ));
        }

        let mut buf = vec![0u8; records_span];
        let mut pos = col.start_offset;
        rules_io.read_data_into_buffer(&mut pos, &mut buf).await?;

        let mut all = Vec::with_capacity(total_count);
        for chunk in buf.chunks_exact(StringRuleRecordDisk::BYTE_LEN) {
            let bytes: &[u8; StringRuleRecordDisk::BYTE_LEN] = chunk.try_into().unwrap();
            let rec = StringRuleRecordDisk::decode(bytes).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Failed to decode string rule record for col {}",
                        column_schema_id
                    ),
                )
            })?;
            all.push(rec);
        }

        let startswith = all[..sw_count].to_vec();
        let endswith = all[sw_count..sw_count.saturating_add(ew_count)].to_vec();
        let contains = all[sw_count.saturating_add(ew_count)..].to_vec();

        Ok(Some(StringColumnRulesIndex {
            column_schema_id,
            column_type,
            startswith,
            endswith,
            contains,
        }))
    }

    pub async fn load_string_rules_for_column<S: StorageIO>(
        rules_io: Arc<S>,
        header: &RulesFileHeaderV2,
        column_schema_id: u64,
    ) -> io::Result<Vec<StringCorrelationRule>> {
        let Some(idx) =
            Self::load_string_column_rules_index_for_column(rules_io.clone(), header, column_schema_id)
                .await?
        else {
            return Ok(Vec::new());
        };

        let total = idx
            .startswith
            .len()
            .saturating_add(idx.endswith.len())
            .saturating_add(idx.contains.len());
        let mut out = Vec::with_capacity(total);

        let column_type = idx.column_type.clone();

        for rec in idx.startswith.iter().copied() {
            let ranges = RangeVec::from_vec(
                Self::load_ranges_for_offset_count(rules_io.clone(), rec.ranges_offset, rec.ranges_count).await?,
            );
            let value = rec.decoded_value().ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "Invalid UTF-8 in string rule value")
            })?;
            out.push(StringCorrelationRule {
                column_schema_id,
                column_type: column_type.clone(),
                op: StringRuleOp::StartsWith,
                count: rec.count,
                value,
                ranges,
            });
        }

        for rec in idx.endswith.iter().copied() {
            let ranges = RangeVec::from_vec(
                Self::load_ranges_for_offset_count(rules_io.clone(), rec.ranges_offset, rec.ranges_count).await?,
            );
            let value = rec.decoded_value().ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "Invalid UTF-8 in string rule value")
            })?;
            out.push(StringCorrelationRule {
                column_schema_id,
                column_type: column_type.clone(),
                op: StringRuleOp::EndsWith,
                count: rec.count,
                value,
                ranges,
            });
        }

        for rec in idx.contains.iter().copied() {
            let ranges = RangeVec::from_vec(
                Self::load_ranges_for_offset_count(rules_io.clone(), rec.ranges_offset, rec.ranges_count).await?,
            );
            let value = rec.decoded_value().ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "Invalid UTF-8 in string rule value")
            })?;
            out.push(StringCorrelationRule {
                column_schema_id,
                column_type: column_type.clone(),
                op: StringRuleOp::Contains,
                count: rec.count,
                value,
                ranges,
            });
        }

        Ok(out)
    }

    pub async fn load_ranges_for_rule<S: StorageIO>(
        rules_io: Arc<S>,
        rec: NumericRuleRecordDisk,
    ) -> io::Result<Vec<RowRange>> {
        Self::load_ranges_for_offset_count(rules_io, rec.ranges_offset, rec.ranges_count).await
    }

    pub async fn load_ranges_for_offset_count<S: StorageIO>(
        rules_io: Arc<S>,
        ranges_offset: u64,
        ranges_count: u32,
    ) -> io::Result<Vec<RowRange>> {
        use crate::core::row::row_pointer::ROW_POINTER_RECORD_LEN;

        if ranges_count == 0 {
            return Ok(Vec::new());
        }
        let span = (ranges_count as usize) * 16;
        let mut buf = vec![0u8; span];
        let mut pos = ranges_offset;
        rules_io.read_data_into_buffer(&mut pos, &mut buf).await?;

        let mut ranges = Vec::with_capacity(ranges_count as usize);
        for chunk in buf.chunks_exact(16) {
            let start_pointer_pos = u64::from_le_bytes(chunk[0..8].try_into().unwrap());
            let row_count = u64::from_le_bytes(chunk[8..16].try_into().unwrap());
            if row_count == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Invalid row range (row_count == 0)",
                ));
            }
            if (start_pointer_pos as usize) % ROW_POINTER_RECORD_LEN != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Invalid row range (start_pointer_pos not aligned)",
                ));
            }
            ranges.push(RowRange {
                start_pointer_pos,
                row_count,
            });
        }

        Ok(ranges)
    }

    pub async fn save_rules_atomic<S: StorageIO>(
        rules_io: Arc<S>,
        numeric_rules_by_col: &[(u64, Vec<NumericCorrelationRule>)],
        string_rules_by_col: &[(u64, Vec<StringCorrelationRule>)],
    ) -> io::Result<()> {
        // Sort columns for stable file layout.
        let mut numeric_by_col: BTreeMap<u64, &Vec<NumericCorrelationRule>> = BTreeMap::new();
        for (col, rules) in numeric_rules_by_col {
            numeric_by_col.insert(*col, rules);
        }
        let mut string_by_col: BTreeMap<u64, &Vec<StringCorrelationRule>> = BTreeMap::new();
        for (col, rules) in string_rules_by_col {
            string_by_col.insert(*col, rules);
        }

        let numeric_entry_count = numeric_by_col.len();
        let string_entry_count = string_by_col.len();

        let numeric_header_len_bytes = (numeric_entry_count * NUMERIC_COLUMN_HEADER_ENTRY_LEN) as u32;
        let string_header_len_bytes = (string_entry_count * STRING_COLUMN_HEADER_ENTRY_LEN) as u32;
        let header_end = FILE_PREFIX_LEN_V2 + (numeric_header_len_bytes as u64) + (string_header_len_bytes as u64);

        let mut numeric_entries: Vec<ColumnHeaderEntryV2> = Vec::with_capacity(numeric_entry_count);
        let mut numeric_sections: Vec<(u64, Vec<NumericRuleRecordDisk>, Vec<u8>)> = Vec::with_capacity(numeric_entry_count);

        let mut string_entries: Vec<StringColumnHeaderEntryV2> = Vec::with_capacity(string_entry_count);
        let mut string_sections: Vec<(u64, Vec<StringRuleRecordDisk>, Vec<u8>)> = Vec::with_capacity(string_entry_count);

        let mut offset = header_end;

        // Numeric columns first.
        for (col, rules) in numeric_by_col.iter() {
            if rules.is_empty() {
                let start_offset = offset;
                let end_offset = offset;
                numeric_entries.push(ColumnHeaderEntryV2 {
                    column_schema_id: *col,
                    column_type: 0,
                    lt_rule_count: 0,
                    gt_rule_count: 0,
                    rule_count: 0,
                    start_offset,
                    end_offset,
                });
                numeric_sections.push((*col, Vec::new(), Vec::new()));
                continue;
            }

            let column_type = rules[0].column_type.clone();
            if rules.iter().any(|r| r.column_type != column_type) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Mixed numeric column types in a single column's rule list",
                ));
            }

            let mut lt_rules: Vec<&NumericCorrelationRule> = Vec::new();
            let mut gt_rules: Vec<&NumericCorrelationRule> = Vec::new();
            for r in rules.iter() {
                match r.op {
                    NumericRuleOp::LessThan => lt_rules.push(r),
                    NumericRuleOp::GreaterThan => gt_rules.push(r),
                }
            }

            let lt_count: u32 = lt_rules
                .len()
                .try_into()
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Too many LT rules"))?;
            let gt_count: u32 = gt_rules
                .len()
                .try_into()
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Too many GT rules"))?;
            let rule_count: u32 = lt_count
                .checked_add(gt_count)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Too many rules"))?;

            let start_offset = offset;
            let records_len = (rule_count as u64) * (NumericRuleRecordDisk::BYTE_LEN as u64);
            let mut range_pool_offset = start_offset + records_len;

            let mut records: Vec<NumericRuleRecordDisk> = Vec::with_capacity(rule_count as usize);
            let mut range_pool: Vec<u8> = Vec::new();

            for r in lt_rules.into_iter().chain(gt_rules.into_iter()) {
                let ranges = normalize_range_vec(r.ranges.clone());
                let ranges_count: u32 = ranges
                    .len()
                    .try_into()
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Too many ranges"))?;

                let ranges_offset = range_pool_offset;
                for rr in ranges.iter() {
                    range_pool.extend_from_slice(&rr.start_pointer_pos.to_le_bytes());
                    range_pool.extend_from_slice(&rr.row_count.to_le_bytes());
                }
                range_pool_offset = range_pool_offset + (ranges.len() as u64) * 16;

                records.push(NumericRuleRecordDisk {
                    value: r.value.encode_16le(),
                    ranges_offset,
                    ranges_count,
                });
            }

            let end_offset = range_pool_offset;

            numeric_entries.push(ColumnHeaderEntryV2 {
                column_schema_id: *col,
                column_type: column_type.to_byte(),
                lt_rule_count: lt_count,
                gt_rule_count: gt_count,
                rule_count,
                start_offset,
                end_offset,
            });

            numeric_sections.push((*col, records, range_pool));
            offset = end_offset;
        }

        // String columns next.
        for (col, rules) in string_by_col.iter() {
            if rules.is_empty() {
                let start_offset = offset;
                let end_offset = offset;
                string_entries.push(StringColumnHeaderEntryV2 {
                    column_schema_id: *col,
                    column_type: 0,
                    startswith_rule_count: 0,
                    endswith_rule_count: 0,
                    contains_rule_count: 0,
                    rule_count: 0,
                    start_offset,
                    end_offset,
                });
                string_sections.push((*col, Vec::new(), Vec::new()));
                continue;
            }

            let column_type = rules[0].column_type.clone();
            if rules.iter().any(|r| r.column_type != column_type) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Mixed string column types in a single column's rule list",
                ));
            }

            let mut sw_rules: Vec<&StringCorrelationRule> = Vec::new();
            let mut ew_rules: Vec<&StringCorrelationRule> = Vec::new();
            let mut c_rules: Vec<&StringCorrelationRule> = Vec::new();
            for r in rules.iter() {
                match r.op {
                    StringRuleOp::StartsWith => sw_rules.push(r),
                    StringRuleOp::EndsWith => ew_rules.push(r),
                    StringRuleOp::Contains => c_rules.push(r),
                }
            }

            let sw_count: u32 = sw_rules
                .len()
                .try_into()
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Too many StartsWith rules"))?;
            let ew_count: u32 = ew_rules
                .len()
                .try_into()
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Too many EndsWith rules"))?;
            let c_count: u32 = c_rules
                .len()
                .try_into()
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Too many Contains rules"))?;
            let rule_count: u32 = sw_count
                .checked_add(ew_count)
                .and_then(|v| v.checked_add(c_count))
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Too many rules"))?;

            let start_offset = offset;
            let records_len = (rule_count as u64) * (StringRuleRecordDisk::BYTE_LEN as u64);
            let mut range_pool_offset = start_offset + records_len;

            let mut records: Vec<StringRuleRecordDisk> = Vec::with_capacity(rule_count as usize);
            let mut range_pool: Vec<u8> = Vec::new();

            for (op, iter) in [
                (StringRuleOp::StartsWith, sw_rules.into_iter()),
                (StringRuleOp::EndsWith, ew_rules.into_iter()),
                (StringRuleOp::Contains, c_rules.into_iter()),
            ] {
                for r in iter {
                    let (value_len, value_bytes) = encode_string_rule_value(&r.value).ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            "String rule value exceeds 16 UTF-8 bytes",
                        )
                    })?;

                    let ranges = normalize_range_vec(r.ranges.clone());
                    let ranges_count: u32 = ranges
                        .len()
                        .try_into()
                        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Too many ranges"))?;

                    let ranges_offset = range_pool_offset;
                    for rr in ranges.iter() {
                        range_pool.extend_from_slice(&rr.start_pointer_pos.to_le_bytes());
                        range_pool.extend_from_slice(&rr.row_count.to_le_bytes());
                    }
                    range_pool_offset = range_pool_offset + (ranges.len() as u64) * 16;

                    records.push(StringRuleRecordDisk {
                        op: op as u8,
                        count: r.count,
                        value_len,
                        value: value_bytes,
                        ranges_offset,
                        ranges_count,
                    });
                }
            }

            let end_offset = range_pool_offset;

            string_entries.push(StringColumnHeaderEntryV2 {
                column_schema_id: *col,
                column_type: column_type.to_byte(),
                startswith_rule_count: sw_count,
                endswith_rule_count: ew_count,
                contains_rule_count: c_count,
                rule_count,
                start_offset,
                end_offset,
            });

            string_sections.push((*col, records, range_pool));
            offset = end_offset;
        }

        let total_len = offset as usize;
        let mut bytes = Vec::with_capacity(total_len);

        // Prefix: two header sizes.
        bytes.extend_from_slice(&numeric_header_len_bytes.to_le_bytes());
        bytes.extend_from_slice(&string_header_len_bytes.to_le_bytes());

        // Numeric header.
        for e in numeric_entries.iter() {
            bytes.extend_from_slice(&e.column_schema_id.to_le_bytes());
            bytes.push(e.column_type);
            bytes.extend_from_slice(&[0u8; 7]);
            let lt = e.lt_rule_count as u64;
            let gt = e.gt_rule_count as u64;
            let packed = lt | (gt << 32);
            bytes.extend_from_slice(&packed.to_le_bytes());
            bytes.extend_from_slice(&e.start_offset.to_le_bytes());
            bytes.extend_from_slice(&e.end_offset.to_le_bytes());
        }

        // String header.
        for e in string_entries.iter() {
            bytes.extend_from_slice(&e.column_schema_id.to_le_bytes());
            bytes.push(e.column_type);
            bytes.extend_from_slice(&[0u8; 7]);
            bytes.extend_from_slice(&e.startswith_rule_count.to_le_bytes());
            bytes.extend_from_slice(&e.endswith_rule_count.to_le_bytes());
            bytes.extend_from_slice(&e.contains_rule_count.to_le_bytes());
            bytes.extend_from_slice(&0u32.to_le_bytes());
            bytes.extend_from_slice(&e.start_offset.to_le_bytes());
            bytes.extend_from_slice(&e.end_offset.to_le_bytes());
        }

        debug_assert_eq!(bytes.len(), header_end as usize);

        // Body: numeric sections.
        for (col_id, records, range_pool) in numeric_sections.into_iter() {
            let entry = numeric_entries
                .iter()
                .find(|e| e.column_schema_id == col_id)
                .copied()
                .unwrap();
            debug_assert_eq!(bytes.len() as u64, entry.start_offset);

            for rec in records.iter() {
                bytes.extend_from_slice(&rec.encode());
            }
            bytes.extend_from_slice(&range_pool);
            debug_assert_eq!(bytes.len() as u64, entry.end_offset);
        }

        // Body: string sections.
        for (col_id, records, range_pool) in string_sections.into_iter() {
            let entry = string_entries
                .iter()
                .find(|e| e.column_schema_id == col_id)
                .copied()
                .unwrap();
            debug_assert_eq!(bytes.len() as u64, entry.start_offset);

            for rec in records.iter() {
                bytes.extend_from_slice(&rec.encode());
            }
            bytes.extend_from_slice(&range_pool);
            debug_assert_eq!(bytes.len() as u64, entry.end_offset);
        }

        // Atomic swap via temp file.
        let mut temp = rules_io.create_temp().await;
        temp.write_data_seek(SeekFrom::Start(0), &bytes).await;
        rules_io.swap_temp(&mut temp).await;
        Ok(())
    }

    pub async fn save_numeric_rules_atomic<S: StorageIO>(
        rules_io: Arc<S>,
        rules_by_col: &[(u64, Vec<NumericCorrelationRule>)],
    ) -> io::Result<()> {
        Self::save_rules_atomic(rules_io, rules_by_col, &[]).await
    }
}
