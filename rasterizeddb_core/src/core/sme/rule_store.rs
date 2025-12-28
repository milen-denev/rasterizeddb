use std::{io, io::SeekFrom, sync::Arc};

use crate::core::storage_providers::traits::StorageIO;

use super::rules::{SemanticRule, SemanticRuleOp};

/// Scan metadata stored as the first record in `TABLENAME_rules.db`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScanMeta {
    /// Length of the pointers IO (bytes) at the end of the scan.
    pub pointers_len_bytes: u64,
    /// Last scanned row pointer id at the end of the scan.
    pub last_row_id: u64,
}

const META_RULE_ID: u64 = 0;

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
        let len = rules_io.get_len().await as usize;
        if len == 0 {
            return Ok((None, Vec::new()));
        }
        if len % SemanticRule::BYTE_LEN != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Invalid rules file length: {} (not a multiple of {})",
                    len,
                    SemanticRule::BYTE_LEN
                ),
            ));
        }

        let mut buf = vec![0u8; len];
        let mut pos = 0u64;
        rules_io
            .read_data_into_buffer(&mut pos, &mut buf)
            .await?;

        let mut meta: Option<ScanMeta> = None;
        let mut rules = Vec::with_capacity(len / SemanticRule::BYTE_LEN);
        for chunk in buf.chunks_exact(SemanticRule::BYTE_LEN) {
            let bytes: &[u8; SemanticRule::BYTE_LEN] = chunk.try_into().unwrap();
            let rule = SemanticRule::decode(bytes);
            if rule.rule_id == META_RULE_ID && rule.op == SemanticRuleOp::Meta {
                // lower packs (pointers_len_bytes, last_row_id)
                let pointers_len_bytes = u64::from_le_bytes(rule.lower[0..8].try_into().unwrap());
                let last_row_id = u64::from_le_bytes(rule.lower[8..16].try_into().unwrap());
                meta = Some(ScanMeta {
                    pointers_len_bytes,
                    last_row_id,
                });
            } else {
                rules.push(rule);
            }
        }

        Ok((meta, rules))
    }

    pub async fn load_rules<S: StorageIO>(rules_io: Arc<S>) -> io::Result<Vec<SemanticRule>> {
        Ok(Self::load_meta_and_rules(rules_io).await?.1)
    }

    pub async fn save_rules_atomic_with_meta<S: StorageIO>(
        rules_io: Arc<S>,
        meta: Option<ScanMeta>,
        rules: &[SemanticRule],
    ) -> io::Result<()> {
        let mut bytes = Vec::with_capacity((rules.len() + 1) * SemanticRule::BYTE_LEN);

        if let Some(meta) = meta {
            let mut lower = [0u8; 16];
            lower[0..8].copy_from_slice(&meta.pointers_len_bytes.to_le_bytes());
            lower[8..16].copy_from_slice(&meta.last_row_id.to_le_bytes());

            // Meta record uses fixed layout like any other record.
            let meta_rule = SemanticRule {
                rule_id: META_RULE_ID,
                column_schema_id: 0,
                column_type: crate::core::db_type::DbType::UNKNOWN,
                op: SemanticRuleOp::Meta,
                lower,
                upper: [0u8; 16],
                start_row_id: 0,
                end_row_id: 0,
            };
            bytes.extend_from_slice(&meta_rule.encode());
        }

        for r in rules {
            bytes.extend_from_slice(&r.encode());
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
}
