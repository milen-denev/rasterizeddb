use std::{collections::BTreeMap, io, io::SeekFrom};

use rclite::Arc;

use crate::core::storage_providers::traits::StorageIO;

use super::rules::{
    normalize_ranges, 
    NumericCorrelationRule, 
    NumericRuleOp, 
    NumericRuleRecordDisk,
    RowRange,
};

/// Rules file format (v2):
/// - u32 header_len_bytes (NOT including this u32)
/// - header bytes: repeated per-column entries
/// - body bytes: per-column contiguous section
///
/// Header entry layout (40 bytes):
/// - column_schema_id: u64
/// - column_type: u8 (DbType::to_byte)
/// - padding: [u8; 7]
/// - counts_packed: u64 (lt_count in low32, gt_count in high32)
/// - start_offset: u64 (absolute file offset)
/// - end_offset: u64   (absolute file offset)
///
/// Offsets are stored as absolute positions from the start of the file.
const COLUMN_HEADER_ENTRY_LEN_SPLIT: usize = 8 + 1 + 7 + 8 + 8 + 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RulesHeaderFormatV2 {
    Split40,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RulesFileHeaderV2 {
    pub header_len_bytes: u32,
    pub format: RulesHeaderFormatV2,
    pub columns: Vec<ColumnHeaderEntryV2>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NumericColumnRulesIndex {
    pub column_schema_id: u64,
    pub column_type: crate::core::db_type::DbType,
    pub lt: Vec<NumericRuleRecordDisk>,
    pub gt: Vec<NumericRuleRecordDisk>,
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
        if file_len < 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Rules file too small for v2 header size: {}", file_len),
            ));
        }

        let mut pos = 0u64;
        let mut header_len_buf = [0u8; 4];
        rules_io
            .read_data_into_buffer(&mut pos, &mut header_len_buf)
            .await?;
        let header_len_bytes = u32::from_le_bytes(header_len_buf);

        let header_start = 4u64;
        let header_end = header_start + (header_len_bytes as u64);

        if header_end > file_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Header extends beyond file: header_end={} file_len={}",
                    header_end, file_len
                ),
            ));
        }

        // Determine header entry format by divisibility.
        let header_format = {
            let h = header_len_bytes as usize;
            if h % COLUMN_HEADER_ENTRY_LEN_SPLIT == 0 {
                RulesHeaderFormatV2::Split40
            } else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Header length not divisible by any known entry size: {} (split={})",
                        header_len_bytes, COLUMN_HEADER_ENTRY_LEN_SPLIT
                    ),
                ));
            }
        };

        let entry_len = match header_format {
            RulesHeaderFormatV2::Split40 => COLUMN_HEADER_ENTRY_LEN_SPLIT,
        };

        let entry_count = (header_len_bytes as usize) / entry_len;
        let mut header_buf = vec![0u8; header_len_bytes as usize];
        let mut header_pos = header_start;
        if header_len_bytes != 0 {
            rules_io
                .read_data_into_buffer(&mut header_pos, &mut header_buf)
                .await?;
        }

        let mut columns = Vec::with_capacity(entry_count);
        for chunk in header_buf.chunks_exact(entry_len) {
            // RulesHeaderFormatV2::Split40 logic directly
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
                        "Invalid column range for col {}: start={} end={}",
                        column_schema_id, start_offset, end_offset
                    ),
                ));
            }
            if start_offset < header_end {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Column rules start inside header for col {}: start={} header_end={}",
                        column_schema_id, start_offset, header_end
                    ),
                ));
            }
            if end_offset > file_len {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Column rules end beyond file for col {}: end={} file_len={}",
                        column_schema_id, end_offset, file_len
                    ),
                ));
            }

            columns.push(ColumnHeaderEntryV2 {
                column_schema_id,
                column_type,
                lt_rule_count,
                gt_rule_count,
                rule_count,
                start_offset,
                end_offset,
            });
        }

        Ok(Some(RulesFileHeaderV2 {
            header_len_bytes,
            format: header_format,
            columns,
        }))
    }

    pub async fn load_numeric_column_rules_index_for_column<S: StorageIO>(
        rules_io: Arc<S>,
        header: &RulesFileHeaderV2,
        column_schema_id: u64,
    ) -> io::Result<Option<NumericColumnRulesIndex>> {
        let Some(col) = header
            .columns
            .iter()
            .find(|c| c.column_schema_id == column_schema_id)
            .copied()
        else {
            return Ok(None);
        };

        if col.rule_count == 0 {
            return Ok(Some(NumericColumnRulesIndex {
                column_schema_id,
                column_type: crate::core::db_type::DbType::U8,
                lt: Vec::new(),
                gt: Vec::new(),
            }));
        }

        // Simplified Logic for Split40 (Legacy28 removed)
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
            let ranges = Self::load_ranges_for_rule(rules_io.clone(), rec).await?;
            out.push(NumericCorrelationRule {
                column_schema_id,
                column_type: column_type.clone(),
                op: NumericRuleOp::LessThan,
                value: rec.decoded_scalar(&column_type),
                ranges,
            });
        }

        for rec in idx.gt.iter().copied() {
            let ranges = Self::load_ranges_for_rule(rules_io.clone(), rec).await?;
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

    pub async fn load_ranges_for_rule<S: StorageIO>(
        rules_io: Arc<S>,
        rec: NumericRuleRecordDisk,
    ) -> io::Result<Vec<RowRange>> {
        if rec.ranges_count == 0 {
            return Ok(Vec::new());
        }
        let span = (rec.ranges_count as usize) * 16;
        let mut buf = vec![0u8; span];
        let mut pos = rec.ranges_offset;
        rules_io.read_data_into_buffer(&mut pos, &mut buf).await?;

        let mut ranges = Vec::with_capacity(rec.ranges_count as usize);
        for chunk in buf.chunks_exact(16) {
            let start_row_id = u64::from_le_bytes(chunk[0..8].try_into().unwrap());
            let end_row_id = u64::from_le_bytes(chunk[8..16].try_into().unwrap());
            if start_row_id > end_row_id {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Invalid row range (start > end)",
                ));
            }
            ranges.push(RowRange {
                start_row_id,
                end_row_id,
            });
        }

        Ok(ranges)
    }

    pub async fn save_numeric_rules_atomic<S: StorageIO>(
        rules_io: Arc<S>,
        rules_by_col: &[(u64, Vec<NumericCorrelationRule>)],
    ) -> io::Result<()> {
        // Sort columns for stable file layout.
        let mut by_col: BTreeMap<u64, &Vec<NumericCorrelationRule>> = BTreeMap::new();
        for (col, rules) in rules_by_col {
            by_col.insert(*col, rules);
        }

        let entry_count = by_col.len();
        let header_len_bytes = (entry_count * COLUMN_HEADER_ENTRY_LEN_SPLIT) as u32;
        let header_end = 4u64 + (header_len_bytes as u64);

        let mut entries: Vec<ColumnHeaderEntryV2> = Vec::with_capacity(entry_count);
        let mut col_sections: Vec<(u64, Vec<NumericRuleRecordDisk>, Vec<u8>)> =
            Vec::with_capacity(entry_count);

        let mut offset = header_end;

        for (col, rules) in by_col.iter() {
            if rules.is_empty() {
                let start_offset = offset;
                let end_offset = offset;
                entries.push(ColumnHeaderEntryV2 {
                    column_schema_id: *col,
                    column_type: 0,
                    lt_rule_count: 0,
                    gt_rule_count: 0,
                    rule_count: 0,
                    start_offset,
                    end_offset,
                });
                col_sections.push((*col, Vec::new(), Vec::new()));
                continue;
            }

            let column_type = rules[0].column_type.clone();
            if rules.iter().any(|r| r.column_type != column_type) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Mixed column types in a single column's rule list",
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
                // Normalize to keep the range pool compact and intersection-friendly.
                let ranges = normalize_ranges(r.ranges.clone());
                let ranges_count: u32 = ranges
                    .len()
                    .try_into()
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Too many ranges"))?;

                let ranges_offset = range_pool_offset;
                for rr in ranges.iter() {
                    range_pool.extend_from_slice(&rr.start_row_id.to_le_bytes());
                    range_pool.extend_from_slice(&rr.end_row_id.to_le_bytes());
                }

                range_pool_offset = range_pool_offset + (ranges.len() as u64) * 16;

                let rec = NumericRuleRecordDisk {
                    value: r.value.encode_16le(),
                    ranges_offset,
                    ranges_count,
                };
                records.push(rec);
            }

            let end_offset = range_pool_offset;

            entries.push(ColumnHeaderEntryV2 {
                column_schema_id: *col,
                column_type: column_type.to_byte(),
                lt_rule_count: lt_count,
                gt_rule_count: gt_count,
                rule_count,
                start_offset,
                end_offset,
            });

            col_sections.push((*col, records, range_pool));
            offset = end_offset;
        }

        let total_len = offset as usize;
        let mut bytes = Vec::with_capacity(total_len);

        // Prefix: header size.
        bytes.extend_from_slice(&header_len_bytes.to_le_bytes());

        // Header: column entries.
        for e in entries.iter() {
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

        debug_assert_eq!(bytes.len(), header_end as usize);

        // Body: per-column [records][range_pool].
        for (col_id, records, range_pool) in col_sections.into_iter() {
            let entry = entries
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
}
