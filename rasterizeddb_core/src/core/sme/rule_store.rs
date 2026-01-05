use std::{io, io::SeekFrom};

use rclite::Arc;

use crate::core::storage_providers::traits::StorageIO;

use super::rules::SemanticRule;

/// Scan metadata stored as the first record in `TABLENAME_rules.db`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScanMeta {
    /// Length of the pointers IO (bytes) at the end of the scan.
    pub pointers_len_bytes: u64,
    /// Fingerprint of the pointers IO contents at the end of the scan.
    ///
    /// This allows detecting in-place pointer updates (updates/deletes) that
    /// don't change file length.
    pub pointers_fingerprint: u64,
    /// Last scanned row pointer id at the end of the scan.
    pub last_row_id: u64,
}

// On-disk format (v2-only):
// [header][rules-by-column contiguous]
// Header enables loading only specific column rule ranges.
const RULES_FILE_VERSION_V2: u32 = 2;
const RULES_HEADER_FIXED_LEN_V2: usize = 32;

// v3 adds pointers_fingerprint (u64) to ScanMeta.
const RULES_FILE_VERSION_V3: u32 = 3;
const RULES_HEADER_FIXED_LEN_V3: usize = 40;

const RULES_HEADER_FLAG_META_PRESENT: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColumnRulesRange {
    pub column_schema_id: u64,
    pub start_offset: u64,
    pub end_offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RulesFileHeaderV2 {
    pub meta: Option<ScanMeta>,
    pub ranges: Vec<ColumnRulesRange>,
    pub header_len: u64,
}

/// Loads/saves SME rule files using a fixed-size binary format.
///
/// File format: concatenation of `SemanticRule::BYTE_LEN` records.
///
/// Per your requirement, each record begins with `rule_id` as an 8-byte (u64)
/// field, followed by the remaining bytes needed for the rule.
pub struct SemanticRuleStore;

impl SemanticRuleStore {
    pub async fn open_rules_io<S: StorageIO>(base_io: Arc<S>, table_name: &str) -> Arc<S> {
        Arc::new(
            base_io
                .create_new(format!("{}_rules.db", table_name))
                .await,
        )
    }

    pub async fn load_meta_and_rules<S: StorageIO>(
        rules_io: Arc<S>,
    ) -> io::Result<(Option<ScanMeta>, Vec<SemanticRule>)> {
        let header = match Self::try_load_header_v2(rules_io.clone()).await? {
            Some(h) => h,
            None => return Ok((None, Vec::new())),
        };

        let mut all_rules = Vec::new();
        for r in header.ranges.iter() {
            let mut rules = Self::load_rules_for_range(rules_io.clone(), *r).await?;
            all_rules.append(&mut rules);
        }

        Ok((header.meta, all_rules))
    }

    pub async fn load_rules<S: StorageIO>(rules_io: Arc<S>) -> io::Result<Vec<SemanticRule>> {
        Ok(Self::load_meta_and_rules(rules_io).await?.1)
    }

    pub async fn save_rules_atomic_with_meta<S: StorageIO>(
        rules_io: Arc<S>,
        meta: Option<ScanMeta>,
        rules: &[SemanticRule],
    ) -> io::Result<()> {
        // v2 format: header + per-column contiguous rule ranges.
        // This allows the SME to load only the columns referenced by a query.
        use std::collections::BTreeMap;

        let mut by_col: BTreeMap<u64, Vec<&SemanticRule>> = BTreeMap::new();
        for r in rules {
            by_col.entry(r.column_schema_id).or_default().push(r);
        }

        let entry_count = by_col.len() as u32;
        let header_len = (RULES_HEADER_FIXED_LEN_V3 + (entry_count as usize) * 24) as u32;

        let mut flags: u32 = 0;
        let mut pointers_len_bytes: u64 = 0;
        let mut pointers_fingerprint: u64 = 0;
        let mut last_row_id: u64 = 0;
        if let Some(m) = meta {
            flags |= RULES_HEADER_FLAG_META_PRESENT;
            pointers_len_bytes = m.pointers_len_bytes;
            pointers_fingerprint = m.pointers_fingerprint;
            last_row_id = m.last_row_id;
        }

        // Precompute ranges.
        let mut ranges: Vec<ColumnRulesRange> = Vec::with_capacity(entry_count as usize);
        let mut offset: u64 = header_len as u64;
        for (col_id, col_rules) in by_col.iter() {
            let bytes_len = (col_rules.len() * SemanticRule::BYTE_LEN) as u64;
            let start = offset;
            let end = offset + bytes_len;
            ranges.push(ColumnRulesRange {
                column_schema_id: *col_id,
                start_offset: start,
                end_offset: end,
            });
            offset = end;
        }

        let total_len = header_len as usize + (rules.len() * SemanticRule::BYTE_LEN);
        let mut bytes = Vec::with_capacity(total_len);

        bytes.extend_from_slice(&RULES_FILE_VERSION_V3.to_le_bytes());
        bytes.extend_from_slice(&header_len.to_le_bytes());
        bytes.extend_from_slice(&pointers_len_bytes.to_le_bytes());
        bytes.extend_from_slice(&pointers_fingerprint.to_le_bytes());
        bytes.extend_from_slice(&last_row_id.to_le_bytes());
        bytes.extend_from_slice(&entry_count.to_le_bytes());
        bytes.extend_from_slice(&flags.to_le_bytes());

        for r in ranges.iter() {
            bytes.extend_from_slice(&r.column_schema_id.to_le_bytes());
            bytes.extend_from_slice(&r.start_offset.to_le_bytes());
            bytes.extend_from_slice(&r.end_offset.to_le_bytes());
        }

        debug_assert_eq!(bytes.len(), header_len as usize);

        // Write rules in the same order as the ranges.
        for r in ranges.iter() {
            if let Some(col_rules) = by_col.get(&r.column_schema_id) {
                for rule in col_rules.iter() {
                    bytes.extend_from_slice(&rule.encode());
                }
            }
        }

        // Write into a temp IO then atomically swap.
        let mut temp = rules_io.create_temp().await;
        temp.write_data_seek(SeekFrom::Start(0), &bytes).await;
        rules_io.swap_temp(&mut temp).await;

        Ok(())
    }

    pub async fn save_rules_atomic<S: StorageIO>(rules_io: Arc<S>, rules: &[SemanticRule]) -> io::Result<()> {
        Self::save_rules_atomic_with_meta(rules_io, None, rules).await
    }

    pub async fn try_load_header_v2<S: StorageIO>(
        rules_io: Arc<S>,
    ) -> io::Result<Option<RulesFileHeaderV2>> {
        let file_len = rules_io.get_len().await;
        if file_len == 0 {
            return Ok(None);
        }

        let mut pos = 0u64;
        let mut ver = [0u8; 4];
        rules_io.read_data_into_buffer(&mut pos, &mut ver).await?;
        let version = u32::from_le_bytes(ver);

        let (header_fixed_len, pointers_len_bytes, pointers_fingerprint, last_row_id, entry_count, flags, header_len) =
            if version == RULES_FILE_VERSION_V2 {
                if file_len < RULES_HEADER_FIXED_LEN_V2 as u64 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Rules file too small for v2 header: {}", file_len),
                    ));
                }

                let mut rest = [0u8; RULES_HEADER_FIXED_LEN_V2 - 4];
                rules_io.read_data_into_buffer(&mut pos, &mut rest).await?;
                let mut fixed = [0u8; RULES_HEADER_FIXED_LEN_V2];
                fixed[0..4].copy_from_slice(&ver);
                fixed[4..].copy_from_slice(&rest);

                let header_len = u32::from_le_bytes(fixed[4..8].try_into().unwrap()) as u64;
                let pointers_len_bytes = u64::from_le_bytes(fixed[8..16].try_into().unwrap());
                let last_row_id = u64::from_le_bytes(fixed[16..24].try_into().unwrap());
                let entry_count = u32::from_le_bytes(fixed[24..28].try_into().unwrap()) as usize;
                let flags = u32::from_le_bytes(fixed[28..32].try_into().unwrap());

                (
                    RULES_HEADER_FIXED_LEN_V2 as u64,
                    pointers_len_bytes,
                    0u64,
                    last_row_id,
                    entry_count,
                    flags,
                    header_len,
                )
            } else if version == RULES_FILE_VERSION_V3 {
                if file_len < RULES_HEADER_FIXED_LEN_V3 as u64 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Rules file too small for v3 header: {}", file_len),
                    ));
                }

                let mut rest = [0u8; RULES_HEADER_FIXED_LEN_V3 - 4];
                rules_io.read_data_into_buffer(&mut pos, &mut rest).await?;
                let mut fixed = [0u8; RULES_HEADER_FIXED_LEN_V3];
                fixed[0..4].copy_from_slice(&ver);
                fixed[4..].copy_from_slice(&rest);

                let header_len = u32::from_le_bytes(fixed[4..8].try_into().unwrap()) as u64;
                let pointers_len_bytes = u64::from_le_bytes(fixed[8..16].try_into().unwrap());
                let pointers_fingerprint = u64::from_le_bytes(fixed[16..24].try_into().unwrap());
                let last_row_id = u64::from_le_bytes(fixed[24..32].try_into().unwrap());
                let entry_count = u32::from_le_bytes(fixed[32..36].try_into().unwrap()) as usize;
                let flags = u32::from_le_bytes(fixed[36..40].try_into().unwrap());

                (
                    RULES_HEADER_FIXED_LEN_V3 as u64,
                    pointers_len_bytes,
                    pointers_fingerprint,
                    last_row_id,
                    entry_count,
                    flags,
                    header_len,
                )
            } else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Unsupported rules file version: {}", version),
                ));
            };

        if header_len < RULES_HEADER_FIXED_LEN_V2 as u64 || header_len > file_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid rules header length: {}", header_len),
            ));
        }

        let expected_header_len = header_fixed_len + (entry_count as u64) * 24;
        if header_len != expected_header_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Rules header length mismatch: got {}, expected {}",
                    header_len, expected_header_len
                ),
            ));
        }

        let mut ranges: Vec<ColumnRulesRange> = Vec::with_capacity(entry_count);
        if entry_count != 0 {
            let mut buf = vec![0u8; (entry_count * 24) as usize];
            let mut pos = header_fixed_len;
            rules_io.read_data_into_buffer(&mut pos, &mut buf).await?;
            for chunk in buf.chunks_exact(24) {
                let column_schema_id = u64::from_le_bytes(chunk[0..8].try_into().unwrap());
                let start_offset = u64::from_le_bytes(chunk[8..16].try_into().unwrap());
                let end_offset = u64::from_le_bytes(chunk[16..24].try_into().unwrap());

                if start_offset < header_len || end_offset < start_offset || end_offset > file_len {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "Invalid rules range for column {}: {}..{} (file_len={}, header_len={})",
                            column_schema_id, start_offset, end_offset, file_len, header_len
                        ),
                    ));
                }

                let span = end_offset - start_offset;
                if span % (SemanticRule::BYTE_LEN as u64) != 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "Rules range for column {} not aligned to record size: span {}",
                            column_schema_id, span
                        ),
                    ));
                }

                ranges.push(ColumnRulesRange {
                    column_schema_id,
                    start_offset,
                    end_offset,
                });
            }
        }

        let meta = if (flags & RULES_HEADER_FLAG_META_PRESENT) != 0 {
            Some(ScanMeta {
                pointers_len_bytes,
                pointers_fingerprint,
                last_row_id,
            })
        } else {
            None
        };

        Ok(Some(RulesFileHeaderV2 {
            meta,
            ranges,
            header_len,
        }))
    }

    pub async fn load_rules_for_columns_v2<S: StorageIO>(
        rules_io: Arc<S>,
        header: &RulesFileHeaderV2,
        column_schema_ids: &[u64],
    ) -> io::Result<Vec<(u64, Vec<SemanticRule>)>> {
        let mut out: Vec<(u64, Vec<SemanticRule>)> = Vec::new();

        // Header ranges are sorted by column id (writer uses BTreeMap), so linear scan is ok.
        for &col in column_schema_ids {
            if let Some(r) = header.ranges.iter().find(|r| r.column_schema_id == col) {
                let rules = Self::load_rules_for_range(rules_io.clone(), *r).await?;
                out.push((col, rules));
            }
        }

        Ok(out)
    }

    async fn load_rules_for_range<S: StorageIO>(
        rules_io: Arc<S>,
        range: ColumnRulesRange,
    ) -> io::Result<Vec<SemanticRule>> {
        let span = (range.end_offset - range.start_offset) as usize;
        if span == 0 {
            return Ok(Vec::new());
        }
        if span % SemanticRule::BYTE_LEN != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid range span {}", span),
            ));
        }

        let mut buf = vec![0u8; span];
        let mut pos = range.start_offset;
        rules_io.read_data_into_buffer(&mut pos, &mut buf).await?;

        let mut rules: Vec<SemanticRule> = Vec::with_capacity(span / SemanticRule::BYTE_LEN);
        for chunk in buf.chunks_exact(SemanticRule::BYTE_LEN) {
            let bytes: &[u8; SemanticRule::BYTE_LEN] = chunk.try_into().unwrap();
            rules.push(SemanticRule::decode(bytes));
        }
        Ok(rules)
    }
}
