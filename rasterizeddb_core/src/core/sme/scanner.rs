use std::{sync::Arc, time::Duration};

use smallvec::SmallVec;

use crate::{
    core::{
        db_type::DbType,
        row::{
            row::{ColumnFetchingData, Row, RowFetch},
            row_pointer::RowPointerIterator,
            schema::SchemaField,
        },
        storage_providers::traits::StorageIO,
    },
    memory_pool::MemoryBlock,
};

use super::{
    rule_store::{ScanMeta, SemanticRuleStore},
    rules::{f64_bits_to_16le, i128_to_16le, u128_to_16le, SemanticRule, SemanticRuleOp},
};

#[cfg(debug_assertions)]
use serde::Serialize;

const SAMPLE_CAP: usize = 4096;
/// Hard cap: semantic rules file must not exceed this fraction of the table's on-disk bytes.
///
/// Table bytes are approximated as `rows_io.len + pointers_io.len`.
const MAX_RULES_FILE_RATIO: f64 = 0.10;
/// Base granularity knob: how many rows are summarized into a single rule (per numeric column).
///
/// Larger values produce fewer, wider rules (more false positives, smaller file).
const BASE_ROWS_PER_RULE: u64 = 256;

/// How many semantic rule records we emit per STRING column per window.
///
/// See `StringWindowRuleBuilder::flush_window_rules`.
const STRING_RULES_PER_COL: u64 = 6;

/// If a string is longer than this, we avoid scanning it byte-by-byte for some
/// metrics and instead conservatively over-approximate (to avoid false negatives).
const STRING_METRIC_SCAN_CAP: usize = 8192;

/// Spawns a periodic background task that scans a table and writes semantic rules to
/// `TABLENAME_rules.db`.
///
/// Design:
/// - Scan all numeric columns and compute min/max + a reservoir sample (streaming).
/// - Build windowed min/max rules: every N rows, emit exactly one rule per numeric column
///   covering that window's min/max. This guarantees bounded rule counts.
/// - Automatically widen N (rows_per_rule) to keep rules <= MAX_RULES_FILE_RATIO.
/// - Include newly appended rows while scanning by refreshing pointer IO length.
pub fn spawn_table_rules_scanner<S: StorageIO>(
    base_io: Arc<S>,
    table_name: String,
    table_schema_fields: SmallVec<[SchemaField; 20]>,
    table_pointers_io: Arc<S>,
    table_rows_io: Arc<S>,
    interval: Duration,
) {
    tokio::spawn(async move {
        let rules_io = SemanticRuleStore::open_rules_io(base_io.clone(), &table_name).await;

        loop {
            if let Err(e) = scan_once_build_rules(
                &table_name,
                table_schema_fields.as_slice(),
                table_pointers_io.clone(),
                table_rows_io.clone(),
                rules_io.clone(),
            )
            .await
            {
                log::error!("SME scan failed for table {}: {}", table_name, e);
            }

            tokio::time::sleep(interval).await;
        }
    });
}

#[derive(Debug, Clone)]
struct NumericColumn {
    schema_idx: usize,
    db_type: DbType,
    stats: NumericStats,
}

#[derive(Debug, Clone)]
struct StringColumn {
    schema_idx: usize,
}

#[derive(Debug, Clone)]
enum NumericStats {
    Signed {
        count: u64,
        min: i128,
        max: i128,
        samples: Vec<i128>,
    },
    Unsigned {
        count: u64,
        min: u128,
        max: u128,
        samples: Vec<u128>,
    },
    Float {
        count: u64,
        min: f64,
        max: f64,
        samples: Vec<f64>,
    },
}

impl NumericStats {
    #[inline]
    fn count(&self) -> u64 {
        match self {
            NumericStats::Signed { count, .. }
            | NumericStats::Unsigned { count, .. }
            | NumericStats::Float { count, .. } => *count,
        }
    }

    fn push_sample_i128(samples: &mut Vec<i128>, seen: u64, v: i128) {
        if samples.len() < SAMPLE_CAP {
            samples.push(v);
            return;
        }
        // Reservoir sampling.
        let j = fastrand::u64(..seen);
        if (j as usize) < SAMPLE_CAP {
            samples[j as usize] = v;
        }
    }

    fn push_sample_u128(samples: &mut Vec<u128>, seen: u64, v: u128) {
        if samples.len() < SAMPLE_CAP {
            samples.push(v);
            return;
        }
        let j = fastrand::u64(..seen);
        if (j as usize) < SAMPLE_CAP {
            samples[j as usize] = v;
        }
    }

    fn push_sample_f64(samples: &mut Vec<f64>, seen: u64, v: f64) {
        if samples.len() < SAMPLE_CAP {
            samples.push(v);
            return;
        }
        let j = fastrand::u64(..seen);
        if (j as usize) < SAMPLE_CAP {
            samples[j as usize] = v;
        }
    }

    #[inline]
    fn update_from_mb(&mut self, db_type: &DbType, mb: &MemoryBlock) {
        let s = mb.into_slice();

        match (self, db_type) {
            (NumericStats::Signed { count, min, max, samples }, DbType::I8) => {
                *count += 1;
                let v = i8::from_le_bytes(s[0..1].try_into().unwrap()) as i128;
                *min = (*min).min(v);
                *max = (*max).max(v);
                Self::push_sample_i128(samples, *count, v);
            }
            (NumericStats::Signed { count, min, max, samples }, DbType::I16) => {
                *count += 1;
                let v = i16::from_le_bytes(s[0..2].try_into().unwrap()) as i128;
                *min = (*min).min(v);
                *max = (*max).max(v);
                Self::push_sample_i128(samples, *count, v);
            }
            (NumericStats::Signed { count, min, max, samples }, DbType::I32) => {
                *count += 1;
                let v = i32::from_le_bytes(s[0..4].try_into().unwrap()) as i128;
                *min = (*min).min(v);
                *max = (*max).max(v);
                Self::push_sample_i128(samples, *count, v);
            }
            (NumericStats::Signed { count, min, max, samples }, DbType::I64) => {
                *count += 1;
                let v = i64::from_le_bytes(s[0..8].try_into().unwrap()) as i128;
                *min = (*min).min(v);
                *max = (*max).max(v);
                Self::push_sample_i128(samples, *count, v);
            }
            (NumericStats::Signed { count, min, max, samples }, DbType::I128) => {
                *count += 1;
                let v = i128::from_le_bytes(s[0..16].try_into().unwrap());
                *min = (*min).min(v);
                *max = (*max).max(v);
                Self::push_sample_i128(samples, *count, v);
            }
            (NumericStats::Unsigned { count, min, max, samples }, DbType::U8) => {
                *count += 1;
                let v = u8::from_le_bytes(s[0..1].try_into().unwrap()) as u128;
                *min = (*min).min(v);
                *max = (*max).max(v);
                Self::push_sample_u128(samples, *count, v);
            }
            (NumericStats::Unsigned { count, min, max, samples }, DbType::U16) => {
                *count += 1;
                let v = u16::from_le_bytes(s[0..2].try_into().unwrap()) as u128;
                *min = (*min).min(v);
                *max = (*max).max(v);
                Self::push_sample_u128(samples, *count, v);
            }
            (NumericStats::Unsigned { count, min, max, samples }, DbType::U32) => {
                *count += 1;
                let v = u32::from_le_bytes(s[0..4].try_into().unwrap()) as u128;
                *min = (*min).min(v);
                *max = (*max).max(v);
                Self::push_sample_u128(samples, *count, v);
            }
            (NumericStats::Unsigned { count, min, max, samples }, DbType::U64) => {
                *count += 1;
                let v = u64::from_le_bytes(s[0..8].try_into().unwrap()) as u128;
                *min = (*min).min(v);
                *max = (*max).max(v);
                Self::push_sample_u128(samples, *count, v);
            }
            (NumericStats::Unsigned { count, min, max, samples }, DbType::U128) => {
                *count += 1;
                let v = u128::from_le_bytes(s[0..16].try_into().unwrap());
                *min = (*min).min(v);
                *max = (*max).max(v);
                Self::push_sample_u128(samples, *count, v);
            }
            (NumericStats::Float { count, min, max, samples }, DbType::F32) => {
                *count += 1;
                let v = f32::from_le_bytes(s[0..4].try_into().unwrap()) as f64;
                if v.is_nan() {
                    return;
                }
                *min = (*min).min(v);
                *max = (*max).max(v);
                Self::push_sample_f64(samples, *count, v);
            }
            (NumericStats::Float { count, min, max, samples }, DbType::F64) => {
                *count += 1;
                let v = f64::from_le_bytes(s[0..8].try_into().unwrap());
                if v.is_nan() {
                    return;
                }
                *min = (*min).min(v);
                *max = (*max).max(v);
                Self::push_sample_f64(samples, *count, v);
            }
            _ => {}
        }
    }
}

async fn scan_once_build_rules<S: StorageIO>(
    table_name: &str,
    schema: &[SchemaField],
    pointers_io: Arc<S>,
    rows_io: Arc<S>,
    rules_io: Arc<S>,
) -> std::io::Result<()> {
    let mut numeric_cols = build_numeric_columns(schema);
    let string_cols = build_string_columns(schema);
    if numeric_cols.is_empty() && string_cols.is_empty() {
        // Still write a meta record so downstream knows we scanned.
        let meta = ScanMeta {
            pointers_len_bytes: pointers_io.get_len().await,
            last_row_id: 0,
        };
        SemanticRuleStore::save_rules_atomic_with_meta(rules_io, Some(meta), &[]).await?;
        return Ok(());
    }

    // PASS 1 (numeric only): collect min/max + reservoir samples for ALL numeric columns.
    // For string-only tables, skip this pass.
    let meta_stats = if numeric_cols.is_empty() {
        ScanMeta {
            pointers_len_bytes: pointers_io.get_len().await,
            last_row_id: 0,
        }
    } else {
        let (meta, _row_fetch) =
            collect_numeric_stats(schema, &mut numeric_cols, pointers_io.clone(), rows_io.clone())
                .await?;
        meta
    };

    // Enforce an on-disk budget for the rule file.
    let table_bytes = pointers_io.get_len().await.saturating_add(rows_io.get_len().await);
    let max_rules_bytes = ((table_bytes as f64) * MAX_RULES_FILE_RATIO).floor() as u64;
    let max_rules = std::cmp::max(1, max_rules_bytes / (SemanticRule::BYTE_LEN as u64));

    let (row_count, last_row_id_fast) = if !numeric_cols.is_empty() {
        (
            numeric_cols
                .first()
                .map(|c| c.stats.count())
                .unwrap_or(0),
            meta_stats.last_row_id,
        )
    } else {
        count_rows_fast(pointers_io.clone()).await?
    };

    let numeric_col_count = numeric_cols.len() as u64;
    let string_col_count = string_cols.len() as u64;
    let signals_per_window = numeric_col_count
        .saturating_add(string_col_count.saturating_mul(STRING_RULES_PER_COL));

    // Choose a window size (rows_per_rule) that will keep:
    // total_rules ~= signals_per_window * ceil(rows/rows_per_rule) <= max_rules.
    let required_rows_per_rule = if signals_per_window == 0 {
        BASE_ROWS_PER_RULE
    } else {
        ceil_div_u64(signals_per_window.saturating_mul(row_count), max_rules)
    };
    let rows_per_rule = std::cmp::max(1, std::cmp::max(BASE_ROWS_PER_RULE, required_rows_per_rule));

    // PASS 2: build bounded windowed rules (numeric + string-derived).
    let (rules_by_col, string_rules, last_row_id_seen) = build_window_rules(
        table_name,
        schema,
        &numeric_cols,
        &string_cols,
        rows_per_rule,
        pointers_io.clone(),
        rows_io.clone(),
    )
    .await?;

    let mut flat_rules = Vec::new();
    for rules in rules_by_col.into_iter() {
        flat_rules.extend(rules);
    }
    flat_rules.extend(string_rules);

    // Use pointers IO len at *end* of scan (may have grown during scanning).
    let meta = ScanMeta {
        pointers_len_bytes: pointers_io.get_len().await,
        last_row_id: std::cmp::max(last_row_id_fast, last_row_id_seen),
    };

    SemanticRuleStore::save_rules_atomic_with_meta(rules_io.clone(), Some(meta), &flat_rules).await?;

    #[cfg(debug_assertions)]
    {
        if let Err(e) = write_rules_debug_json(table_name, schema, rules_io.clone(), meta, &flat_rules).await {
            log::warn!("Failed to write SME debug rules JSON for table {}: {}", table_name, e);
        }
    }

    Ok(())
}

#[inline]
fn ceil_div_u64(n: u64, d: u64) -> u64 {
    if d == 0 {
        return n;
    }
    (n / d) + u64::from(n % d != 0)
}

#[cfg(debug_assertions)]
#[derive(Debug, Serialize)]
struct DebugRulesDump {
    table: String,
    meta: DebugMeta,
    rules: Vec<DebugRule>,
}

#[cfg(debug_assertions)]
#[derive(Debug, Serialize)]
struct DebugMeta {
    pointers_len_bytes: u64,
    last_row_id: u64,
}

#[cfg(debug_assertions)]
#[derive(Debug, Serialize)]
struct DebugRule {
    rule_id: u64,
    column_schema_id: u64,
    column_name: String,
    column_type: String,
    op: String,
    start_row_id: u64,
    end_row_id: u64,
    lower: String,
    upper: String,
    lower_hex: String,
    upper_hex: String,
}

#[cfg(debug_assertions)]
async fn write_rules_debug_json<S: StorageIO>(
    table_name: &str,
    schema: &[SchemaField],
    rules_io: Arc<S>,
    meta: ScanMeta,
    rules: &[SemanticRule],
) -> std::io::Result<()> {
    let Some(dir) = rules_io.get_location() else {
        return Ok(());
    };
    let file_name = rules_io.get_name();

    let delimiter = if cfg!(unix) {
        "/"
    } else if cfg!(windows) {
        "\\"
    } else {
        "/"
    };

    let json_name = if file_name.ends_with(".db") {
        format!("{}{}", &file_name[..file_name.len() - 3], "debug.json")
    } else {
        format!("{}.debug.json", file_name)
    };

    let path = format!("{}{}{}", dir, delimiter, json_name);

    let mut out_rules = Vec::with_capacity(rules.len());
    for r in rules {
        let col_name = schema
            .get(r.column_schema_id as usize)
            .map(|s| s.name.clone())
            .unwrap_or_else(|| "<unknown>".to_string());

        let (lower_str, upper_str) = format_bounds(&r.column_type, r.op, &r.lower, &r.upper);
        out_rules.push(DebugRule {
            rule_id: r.rule_id,
            column_schema_id: r.column_schema_id,
            column_name: col_name,
            column_type: format!("{}", r.column_type),
            op: format!("{:?}", r.op),
            start_row_id: r.start_row_id,
            end_row_id: r.end_row_id,
            lower: lower_str,
            upper: upper_str,
            lower_hex: hex_16(&r.lower),
            upper_hex: hex_16(&r.upper),
        });
    }

    let dump = DebugRulesDump {
        table: table_name.to_string(),
        meta: DebugMeta {
            pointers_len_bytes: meta.pointers_len_bytes,
            last_row_id: meta.last_row_id,
        },
        rules: out_rules,
    };

    let json = serde_json::to_string_pretty(&dump)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

    tokio::fs::write(path, json).await
}

#[cfg(debug_assertions)]
fn hex_16(b: &[u8; 16]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(32);
    for byte in b.iter() {
        s.push(HEX[(byte >> 4) as usize] as char);
        s.push(HEX[(byte & 0x0F) as usize] as char);
    }
    s
}

#[cfg(debug_assertions)]
fn format_bounds(
    db_type: &DbType,
    op: SemanticRuleOp,
    lower: &[u8; 16],
    upper: &[u8; 16],
) -> (String, String) {
    // JSON numbers can't represent u64/i128 precisely, so stringify.
    match db_type {
        DbType::I8 | DbType::I16 | DbType::I32 | DbType::I64 | DbType::I128 => {
            let lo = i128::from_le_bytes(*lower);
            let hi = i128::from_le_bytes(*upper);
            (format!("{} ({:?})", lo, op), format!("{}", hi))
        }
        DbType::U8 | DbType::U16 | DbType::U32 | DbType::U64 | DbType::U128 => {
            let lo = u128::from_le_bytes(*lower);
            let hi = u128::from_le_bytes(*upper);
            (format!("{} ({:?})", lo, op), format!("{}", hi))
        }
        DbType::F32 | DbType::F64 => {
            let lo_bits = u64::from_le_bytes(lower[0..8].try_into().unwrap());
            let hi_bits = u64::from_le_bytes(upper[0..8].try_into().unwrap());
            (format!("{} ({:?})", f64::from_bits(lo_bits), op), format!("{}", f64::from_bits(hi_bits)))
        }
        DbType::STRING => match op {
            SemanticRuleOp::StrFirstByteSet
            | SemanticRuleOp::StrLastByteSet
            | SemanticRuleOp::StrCharSet => (format!("<bitset {:?}>", op), "".to_string()),
            SemanticRuleOp::StrLenRange
            | SemanticRuleOp::StrMaxRunLen
            | SemanticRuleOp::StrMaxCharCount => {
                let lo = u64::from_le_bytes(lower[0..8].try_into().unwrap());
                let hi = u64::from_le_bytes(upper[0..8].try_into().unwrap());
                (format!("{} ({:?})", lo, op), format!("{}", hi))
            }
            _ => ("<n/a>".to_string(), "<n/a>".to_string()),
        },
        _ => ("<n/a>".to_string(), "<n/a>".to_string()),
    }
}

fn build_numeric_columns(schema: &[SchemaField]) -> Vec<NumericColumn> {
    schema
        .iter()
        .enumerate()
        .filter_map(|(schema_idx, f)| {
            let stats = match f.db_type {
                DbType::I8 | DbType::I16 | DbType::I32 | DbType::I64 | DbType::I128 => {
                    Some(NumericStats::Signed {
                        count: 0,
                        min: i128::MAX,
                        max: i128::MIN,
                        samples: Vec::with_capacity(std::cmp::min(256, SAMPLE_CAP)),
                    })
                }
                DbType::U8 | DbType::U16 | DbType::U32 | DbType::U64 | DbType::U128 => {
                    Some(NumericStats::Unsigned {
                        count: 0,
                        min: u128::MAX,
                        max: u128::MIN,
                        samples: Vec::with_capacity(std::cmp::min(256, SAMPLE_CAP)),
                    })
                }
                DbType::F32 | DbType::F64 => Some(NumericStats::Float {
                    count: 0,
                    min: f64::INFINITY,
                    max: f64::NEG_INFINITY,
                    samples: Vec::with_capacity(std::cmp::min(256, SAMPLE_CAP)),
                }),
                _ => None,
            };
            stats.map(|stats| NumericColumn {
                schema_idx,
                db_type: f.db_type.clone(),
                stats,
            })
        })
        .collect()
}

fn build_row_fetch_for_columns(schema: &[SchemaField], columns: &[NumericColumn]) -> RowFetch {
    let mut v = smallvec::SmallVec::<[ColumnFetchingData; 32]>::new();
    for c in columns {
        let f = &schema[c.schema_idx];
        v.push(ColumnFetchingData {
            column_offset: f.offset as u32,
            column_type: f.db_type.clone(),
            size: f.size,
            schema_id: c.schema_idx as u64,
        });
    }
    RowFetch {
        columns_fetching_data: v,
    }
}

fn build_string_columns(schema: &[SchemaField]) -> Vec<StringColumn> {
    schema
        .iter()
        .enumerate()
        .filter_map(|(schema_idx, f)| {
            if f.db_type == DbType::STRING {
                Some(StringColumn { schema_idx })
            } else {
                None
            }
        })
        .collect()
}

async fn count_rows_fast<S: StorageIO>(pointers_io: Arc<S>) -> std::io::Result<(u64, u64)> {
    let mut iterator = RowPointerIterator::new(pointers_io.clone()).await.map_err(|e| {
        std::io::Error::new(std::io::ErrorKind::Other, format!("RowPointerIterator: {}", e))
    })?;

    let mut count = 0u64;
    let mut last_row_id = 0u64;
    let mut empty_rounds: u8 = 0;

    loop {
        let pointers = iterator.next_row_pointers().await.map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::Other, format!("RowPointerIterator: {}", e))
        })?;

        if pointers.is_empty() {
            iterator.refresh_total_length().await;
            empty_rounds += 1;
            if empty_rounds >= 2 {
                break;
            }
            continue;
        }
        empty_rounds = 0;

        for p in pointers.iter() {
            if p.deleted {
                continue;
            }
            count += 1;
            last_row_id = p.id;
        }
    }

    Ok((count, last_row_id))
}

async fn collect_numeric_stats<S: StorageIO>(
    schema: &[SchemaField],
    numeric_cols: &mut [NumericColumn],
    pointers_io: Arc<S>,
    rows_io: Arc<S>,
) -> std::io::Result<(ScanMeta, RowFetch)> {
    let row_fetch = build_row_fetch_for_columns(schema, numeric_cols);

    let mut iterator = RowPointerIterator::new(pointers_io.clone()).await.map_err(|e| {
        std::io::Error::new(std::io::ErrorKind::Other, format!("RowPointerIterator: {}", e))
    })?;

    // Map schema_idx -> numeric_cols index for fast updates.
    let mut index_map = vec![usize::MAX; schema.len()];
    for (i, c) in numeric_cols.iter().enumerate() {
        index_map[c.schema_idx] = i;
    }

    let mut row = Row::default();
    let mut last_row_id: u64 = 0;
    let mut empty_rounds: u8 = 0;

    loop {
        let pointers = iterator.next_row_pointers().await.map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::Other, format!("RowPointerIterator: {}", e))
        })?;

        if pointers.is_empty() {
            iterator.refresh_total_length().await;
            empty_rounds += 1;
            if empty_rounds >= 2 {
                break;
            }
            continue;
        }
        empty_rounds = 0;

        for pointer in pointers.iter() {
            if pointer.deleted {
                continue;
            }
            last_row_id = pointer.id;

            if pointer
                .fetch_row_reuse_async(rows_io.clone(), &row_fetch, &mut row)
                .await
                .is_err()
            {
                continue;
            }

            for col in row.columns.iter() {
                let schema_idx = col.schema_id as usize;
                let mapped = *index_map.get(schema_idx).unwrap_or(&usize::MAX);
                if mapped == usize::MAX {
                    continue;
                }
                let db_type = &schema[schema_idx].db_type;
                numeric_cols[mapped]
                    .stats
                    .update_from_mb(db_type, &col.data);
            }
        }
    }

    Ok((
        ScanMeta {
            pointers_len_bytes: pointers_io.get_len().await,
            last_row_id,
        },
        row_fetch,
    ))
}

// Old bucket/quantile rule building removed in favor of size-bounded windowed rules.

fn decode_numeric_value(db_type: &DbType, mb: &MemoryBlock) -> Option<NumericValue> {
    let s = mb.into_slice();
    match db_type {
        DbType::I8 => Some(NumericValue::Signed(i8::from_le_bytes(s[0..1].try_into().unwrap()) as i128)),
        DbType::I16 => Some(NumericValue::Signed(i16::from_le_bytes(s[0..2].try_into().unwrap()) as i128)),
        DbType::I32 => Some(NumericValue::Signed(i32::from_le_bytes(s[0..4].try_into().unwrap()) as i128)),
        DbType::I64 => Some(NumericValue::Signed(i64::from_le_bytes(s[0..8].try_into().unwrap()) as i128)),
        DbType::I128 => Some(NumericValue::Signed(i128::from_le_bytes(s[0..16].try_into().unwrap()))),
        DbType::U8 => Some(NumericValue::Unsigned(u8::from_le_bytes(s[0..1].try_into().unwrap()) as u128)),
        DbType::U16 => Some(NumericValue::Unsigned(u16::from_le_bytes(s[0..2].try_into().unwrap()) as u128)),
        DbType::U32 => Some(NumericValue::Unsigned(u32::from_le_bytes(s[0..4].try_into().unwrap()) as u128)),
        DbType::U64 => Some(NumericValue::Unsigned(u64::from_le_bytes(s[0..8].try_into().unwrap()) as u128)),
        DbType::U128 => Some(NumericValue::Unsigned(u128::from_le_bytes(s[0..16].try_into().unwrap()))),
        DbType::F32 => {
            let v = f32::from_le_bytes(s[0..4].try_into().unwrap()) as f64;
            if v.is_nan() { None } else { Some(NumericValue::Float(v)) }
        }
        DbType::F64 => {
            let v = f64::from_le_bytes(s[0..8].try_into().unwrap());
            if v.is_nan() { None } else { Some(NumericValue::Float(v)) }
        }
        _ => None,
    }
}

#[derive(Debug, Clone, Copy)]
enum NumericValue {
    Signed(i128),
    Unsigned(u128),
    Float(f64),
}

#[derive(Debug)]
struct ColumnWindowRuleBuilder {
    schema_idx: usize,
    db_type: DbType,
    op: SemanticRuleOp,
    // Global bounds (fallback when a window contains invalid/un-decodable values).
    global_min_s: i128,
    global_max_s: i128,
    global_min_u: u128,
    global_max_u: u128,
    global_min_f: f64,
    global_max_f: f64,

    // Window bounds.
    win_min_s: Option<i128>,
    win_max_s: Option<i128>,
    win_min_u: Option<u128>,
    win_max_u: Option<u128>,
    win_min_f: Option<f64>,
    win_max_f: Option<f64>,
    win_invalid: bool,

    seq: u64,
    rules: Vec<SemanticRule>,
}

impl ColumnWindowRuleBuilder {
    fn new(col: &NumericColumn) -> Self {
        let (global_min_s, global_max_s, global_min_u, global_max_u, global_min_f, global_max_f) =
            match &col.stats {
                NumericStats::Signed { min, max, .. } => (*min, *max, 0, 0, 0.0, 0.0),
                NumericStats::Unsigned { min, max, .. } => (0, 0, *min, *max, 0.0, 0.0),
                NumericStats::Float { min, max, .. } => (0, 0, 0, 0, *min, *max),
            };

        Self {
            schema_idx: col.schema_idx,
            db_type: col.db_type.clone(),
            op: SemanticRuleOp::BetweenInclusive,
            global_min_s,
            global_max_s,
            global_min_u,
            global_max_u,
            global_min_f,
            global_max_f,
            win_min_s: None,
            win_max_s: None,
            win_min_u: None,
            win_max_u: None,
            win_min_f: None,
            win_max_f: None,
            win_invalid: false,
            seq: 1,
            rules: Vec::new(),
        }
    }

    fn push_value(&mut self, value: Option<NumericValue>) {
        let Some(value) = value else {
            self.win_invalid = true;
            return;
        };

        match value {
            NumericValue::Signed(v) => {
                self.win_min_s = Some(self.win_min_s.map_or(v, |cur| cur.min(v)));
                self.win_max_s = Some(self.win_max_s.map_or(v, |cur| cur.max(v)));
            }
            NumericValue::Unsigned(v) => {
                self.win_min_u = Some(self.win_min_u.map_or(v, |cur| cur.min(v)));
                self.win_max_u = Some(self.win_max_u.map_or(v, |cur| cur.max(v)));
            }
            NumericValue::Float(v) => {
                self.win_min_f = Some(self.win_min_f.map_or(v, |cur| cur.min(v)));
                self.win_max_f = Some(self.win_max_f.map_or(v, |cur| cur.max(v)));
            }
        }
    }

    fn flush_window(&mut self, table_name: &str, start_row_id: u64, end_row_id: u64) {
        if start_row_id > end_row_id {
            self.reset_window();
            return;
        }

        let (lower, upper) = if self.win_invalid {
            self.global_bounds_16le()
        } else {
            match self.db_type {
                DbType::I8 | DbType::I16 | DbType::I32 | DbType::I64 | DbType::I128 => {
                    let lo = self.win_min_s.unwrap_or(self.global_min_s);
                    let hi = self.win_max_s.unwrap_or(self.global_max_s);
                    (i128_to_16le(lo), i128_to_16le(hi))
                }
                DbType::U8 | DbType::U16 | DbType::U32 | DbType::U64 | DbType::U128 => {
                    let lo = self.win_min_u.unwrap_or(self.global_min_u);
                    let hi = self.win_max_u.unwrap_or(self.global_max_u);
                    (u128_to_16le(lo), u128_to_16le(hi))
                }
                DbType::F32 | DbType::F64 => {
                    let lo = self.win_min_f.unwrap_or(self.global_min_f);
                    let hi = self.win_max_f.unwrap_or(self.global_max_f);
                    (f64_bits_to_16le(lo.to_bits()), f64_bits_to_16le(hi.to_bits()))
                }
                _ => self.global_bounds_16le(),
            }
        };

        let rule_id = make_rule_id(table_name, self.schema_idx as u64, self.seq);
        self.seq = self.seq.wrapping_add(1);
        self.rules.push(SemanticRule {
            rule_id,
            column_schema_id: self.schema_idx as u64,
            column_type: self.db_type.clone(),
            op: self.op,
            reserved: [0u8; 6],
            lower,
            upper,
            start_row_id,
            end_row_id,
        });

        self.reset_window();
    }

    fn reset_window(&mut self) {
        self.win_min_s = None;
        self.win_max_s = None;
        self.win_min_u = None;
        self.win_max_u = None;
        self.win_min_f = None;
        self.win_max_f = None;
        self.win_invalid = false;
    }

    fn global_bounds_16le(&self) -> ([u8; 16], [u8; 16]) {
        match self.db_type {
            DbType::I8 | DbType::I16 | DbType::I32 | DbType::I64 | DbType::I128 => {
                (i128_to_16le(self.global_min_s), i128_to_16le(self.global_max_s))
            }
            DbType::U8 | DbType::U16 | DbType::U32 | DbType::U64 | DbType::U128 => {
                (u128_to_16le(self.global_min_u), u128_to_16le(self.global_max_u))
            }
            DbType::F32 | DbType::F64 => (
                f64_bits_to_16le(self.global_min_f.to_bits()),
                f64_bits_to_16le(self.global_max_f.to_bits()),
            ),
            _ => ([0u8; 16], [0u8; 16]),
        }
    }
}

fn build_row_fetch_for_mixed_columns(
    schema: &[SchemaField],
    numeric_cols: &[NumericColumn],
    string_cols: &[StringColumn],
) -> RowFetch {
    let mut v = smallvec::SmallVec::<[ColumnFetchingData; 32]>::new();

    for c in numeric_cols {
        let f = &schema[c.schema_idx];
        v.push(ColumnFetchingData {
            column_offset: f.offset as u32,
            column_type: f.db_type.clone(),
            size: f.size,
            schema_id: c.schema_idx as u64,
        });
    }

    for c in string_cols {
        let f = &schema[c.schema_idx];
        v.push(ColumnFetchingData {
            column_offset: f.offset as u32,
            column_type: f.db_type.clone(),
            size: f.size,
            schema_id: c.schema_idx as u64,
        });
    }

    RowFetch {
        columns_fetching_data: v,
    }
}

#[derive(Debug)]
struct StringWindowRuleBuilder {
    table_name: String,
    column_schema_id: u64,
    seq: u64,

    win_first_set: [u8; 32],
    win_last_set: [u8; 32],
    win_charset: [u8; 32],
    win_min_len: Option<u64>,
    win_max_len: Option<u64>,
    win_max_run_len: u64,
    win_max_char_count: u64,

    rules: Vec<SemanticRule>,
}

impl StringWindowRuleBuilder {
    fn new(table_name: &str, column_schema_id: u64) -> Self {
        Self {
            table_name: table_name.to_string(),
            column_schema_id,
            seq: 0,
            win_first_set: [0u8; 32],
            win_last_set: [0u8; 32],
            win_charset: [0u8; 32],
            win_min_len: None,
            win_max_len: None,
            win_max_run_len: 0,
            win_max_char_count: 0,
            rules: Vec::new(),
        }
    }

    #[inline]
    fn set_bit(mask: &mut [u8; 32], byte: u8) {
        let idx = byte as usize;
        mask[idx >> 3] |= 1u8 << (idx & 7);
    }

    fn push_bytes(&mut self, bytes: &[u8]) {
        let len = bytes.len() as u64;
        self.win_min_len = Some(self.win_min_len.map(|v| v.min(len)).unwrap_or(len));
        self.win_max_len = Some(self.win_max_len.map(|v| v.max(len)).unwrap_or(len));

        if bytes.is_empty() {
            return;
        }

        Self::set_bit(&mut self.win_first_set, bytes[0]);
        Self::set_bit(&mut self.win_last_set, bytes[bytes.len() - 1]);

        if bytes.len() > STRING_METRIC_SCAN_CAP {
            // Conservative over-approx to avoid false negatives.
            self.win_charset = [0xFFu8; 32];
            self.win_max_run_len = self.win_max_run_len.max(len);
            self.win_max_char_count = self.win_max_char_count.max(len);
            return;
        }

        for &b in bytes {
            Self::set_bit(&mut self.win_charset, b);
        }

        // Max run length (consecutive repeats)
        let mut run_max: u64 = 1;
        let mut run: u64 = 1;
        for i in 1..bytes.len() {
            if bytes[i] == bytes[i - 1] {
                run += 1;
            } else {
                run_max = run_max.max(run);
                run = 1;
            }
        }
        run_max = run_max.max(run);
        self.win_max_run_len = self.win_max_run_len.max(run_max);

        // Max count of a single byte in the whole string
        let mut counts = [0u16; 256];
        let mut max_count: u16 = 0;
        for &b in bytes {
            let c = &mut counts[b as usize];
            *c = c.saturating_add(1);
            if *c > max_count {
                max_count = *c;
            }
        }
        self.win_max_char_count = self.win_max_char_count.max(max_count as u64);
    }

    fn flush_window_rules(&mut self, start_row_id: u64, end_row_id: u64) {
        if start_row_id > end_row_id {
            self.reset_window();
            return;
        }

        let min_len = self.win_min_len.unwrap_or(0);
        let max_len = self.win_max_len.unwrap_or(0);

        let rule_id_base = make_rule_id(&self.table_name, self.column_schema_id, self.seq);
        self.seq = self.seq.wrapping_add(1);

        let mut push_rule = |op: SemanticRuleOp, lower: [u8; 16], upper: [u8; 16]| {
            self.rules.push(SemanticRule {
                rule_id: rule_id_base ^ (op as u8 as u64),
                column_schema_id: self.column_schema_id,
                column_type: DbType::STRING,
                op,
                reserved: [0u8; 6],
                lower,
                upper,
                start_row_id,
                end_row_id,
            });
        };

        // First byte set
        let mut lo = [0u8; 16];
        let mut hi = [0u8; 16];
        lo.copy_from_slice(&self.win_first_set[0..16]);
        hi.copy_from_slice(&self.win_first_set[16..32]);
        push_rule(SemanticRuleOp::StrFirstByteSet, lo, hi);

        // Last byte set
        let mut lo = [0u8; 16];
        let mut hi = [0u8; 16];
        lo.copy_from_slice(&self.win_last_set[0..16]);
        hi.copy_from_slice(&self.win_last_set[16..32]);
        push_rule(SemanticRuleOp::StrLastByteSet, lo, hi);

        // Charset
        let mut lo = [0u8; 16];
        let mut hi = [0u8; 16];
        lo.copy_from_slice(&self.win_charset[0..16]);
        hi.copy_from_slice(&self.win_charset[16..32]);
        push_rule(SemanticRuleOp::StrCharSet, lo, hi);

        // Length range
        let mut lo = [0u8; 16];
        let mut hi = [0u8; 16];
        lo[0..8].copy_from_slice(&min_len.to_le_bytes());
        hi[0..8].copy_from_slice(&max_len.to_le_bytes());
        push_rule(SemanticRuleOp::StrLenRange, lo, hi);

        // Max run length (store [0, max])
        let lo = [0u8; 16];
        let mut hi = [0u8; 16];
        hi[0..8].copy_from_slice(&self.win_max_run_len.to_le_bytes());
        push_rule(SemanticRuleOp::StrMaxRunLen, lo, hi);

        // Max char count (store [0, max])
        let lo = [0u8; 16];
        let mut hi = [0u8; 16];
        hi[0..8].copy_from_slice(&self.win_max_char_count.to_le_bytes());
        push_rule(SemanticRuleOp::StrMaxCharCount, lo, hi);

        self.reset_window();
    }

    fn reset_window(&mut self) {
        self.win_first_set = [0u8; 32];
        self.win_last_set = [0u8; 32];
        self.win_charset = [0u8; 32];
        self.win_min_len = None;
        self.win_max_len = None;
        self.win_max_run_len = 0;
        self.win_max_char_count = 0;
    }
}

async fn build_window_rules<S: StorageIO>(
    table_name: &str,
    schema: &[SchemaField],
    numeric_cols: &[NumericColumn],
    string_cols: &[StringColumn],
    rows_per_rule: u64,
    pointers_io: Arc<S>,
    rows_io: Arc<S>,
) -> std::io::Result<(Vec<Vec<SemanticRule>>, Vec<SemanticRule>, u64)> {
    let row_fetch = build_row_fetch_for_mixed_columns(schema, numeric_cols, string_cols);

    // schema_idx -> numeric builder index
    let mut numeric_index_map = vec![usize::MAX; schema.len()];
    for (i, c) in numeric_cols.iter().enumerate() {
        numeric_index_map[c.schema_idx] = i;
    }

    // schema_idx -> string builder index
    let mut string_index_map = vec![usize::MAX; schema.len()];
    for (i, c) in string_cols.iter().enumerate() {
        string_index_map[c.schema_idx] = i;
    }

    let mut builders: Vec<ColumnWindowRuleBuilder> =
        numeric_cols.iter().map(ColumnWindowRuleBuilder::new).collect();

    let mut string_builders: Vec<StringWindowRuleBuilder> = string_cols
        .iter()
        .map(|c| StringWindowRuleBuilder::new(table_name, c.schema_idx as u64))
        .collect();

    let mut iterator = RowPointerIterator::new(pointers_io.clone()).await.map_err(|e| {
        std::io::Error::new(std::io::ErrorKind::Other, format!("RowPointerIterator: {}", e))
    })?;

    let mut row = Row::default();
    let mut empty_rounds: u8 = 0;

    let mut row_index: u64 = 0;
    let mut window_start_row_id: Option<u64> = None;
    let mut prev_row_id: u64 = 0;
    let mut last_row_id_seen: u64 = 0;

    loop {
        let pointers = iterator.next_row_pointers().await.map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::Other, format!("RowPointerIterator: {}", e))
        })?;

        if pointers.is_empty() {
            iterator.refresh_total_length().await;
            empty_rounds += 1;
            if empty_rounds >= 2 {
                break;
            }
            continue;
        }
        empty_rounds = 0;

        for pointer in pointers.iter() {
            if pointer.deleted {
                continue;
            }

            let row_id = pointer.id;
            last_row_id_seen = row_id;

            if window_start_row_id.is_none() {
                window_start_row_id = Some(row_id);
            }

            if pointer
                .fetch_row_reuse_async(rows_io.clone(), &row_fetch, &mut row)
                .await
                .is_err()
            {
                continue;
            }

            for col in row.columns.iter() {
                let schema_idx = col.schema_id as usize;

                let ni = *numeric_index_map.get(schema_idx).unwrap_or(&usize::MAX);
                if ni != usize::MAX {
                    let db_type = &schema[schema_idx].db_type;
                    let v = decode_numeric_value(db_type, &col.data);
                    builders[ni].push_value(v);
                    continue;
                }

                let si = *string_index_map.get(schema_idx).unwrap_or(&usize::MAX);
                if si != usize::MAX {
                    string_builders[si].push_bytes(col.data.into_slice());
                }
            }

            row_index = row_index.saturating_add(1);
            prev_row_id = row_id;

            if rows_per_rule != 0 && row_index % rows_per_rule == 0 {
                let start = window_start_row_id.unwrap_or(prev_row_id);
                let end = prev_row_id;
                for b in builders.iter_mut() {
                    b.flush_window(table_name, start, end);
                }
                for sb in string_builders.iter_mut() {
                    sb.flush_window_rules(start, end);
                }
                window_start_row_id = None;
            }
        }
    }

    if row_index > 0 {
        let start = window_start_row_id.unwrap_or(prev_row_id);
        let end = prev_row_id;
        for b in builders.iter_mut() {
            b.flush_window(table_name, start, end);
        }
        for sb in string_builders.iter_mut() {
            sb.flush_window_rules(start, end);
        }
    }

    let numeric_rules = builders.into_iter().map(|b| b.rules).collect();
    let mut string_rules = Vec::new();
    for sb in string_builders.into_iter() {
        string_rules.extend(sb.rules);
    }

    Ok((numeric_rules, string_rules, last_row_id_seen))
}

#[inline]
fn make_rule_id(table_name: &str, column_schema_id: u64, seq: u64) -> u64 {
    let mut h = ahash::AHasher::default();
    use std::hash::Hasher;
    h.write(table_name.as_bytes());
    h.write_u64(column_schema_id);
    h.write_u64(seq);
    h.finish()
}
