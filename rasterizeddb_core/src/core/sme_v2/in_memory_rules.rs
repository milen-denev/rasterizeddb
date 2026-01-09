use std::collections::HashMap;
use std::sync::OnceLock;
use parking_lot::RwLock;

use rclite::Arc;

use super::rule_store::{
    ColumnHeaderEntryV2, RulesFileHeaderV2, RulesHeaderFormatV2, StringColumnHeaderEntryV2,
};
use super::rules::{NumericCorrelationRule, StringCorrelationRule};

#[derive(Clone)]
pub struct TableRulesV2 {
    pub header: Arc<RulesFileHeaderV2>,
    pub numeric_by_col: HashMap<u64, Arc<Vec<NumericCorrelationRule>>, ahash::RandomState>,
    pub string_by_col: HashMap<u64, Arc<Vec<StringCorrelationRule>>, ahash::RandomState>,
}

static IN_MEMORY_RULES_V2: OnceLock<Arc<RwLock<HashMap<String, Arc<TableRulesV2>, ahash::RandomState>>>> = OnceLock::new();

fn store() -> &'static RwLock<HashMap<String, Arc<TableRulesV2>, ahash::RandomState>> {
    IN_MEMORY_RULES_V2.get_or_init(|| Arc::new(RwLock::new(HashMap::with_hasher(Default::default()))))
}

pub fn set_table_rules(
    table_name: &str,
    numeric_rules_by_col: Vec<(u64, Vec<NumericCorrelationRule>)>,
    string_rules_by_col: Vec<(u64, Vec<StringCorrelationRule>)>,
) {
    let mut numeric_by_col: HashMap<u64, Arc<Vec<NumericCorrelationRule>>, ahash::RandomState> = HashMap::with_hasher(Default::default());
    for (col, rules) in numeric_rules_by_col.into_iter() {
        numeric_by_col.insert(col, Arc::new(rules));
    }

    let mut string_by_col: HashMap<u64, Arc<Vec<StringCorrelationRule>>, ahash::RandomState> = HashMap::with_hasher(Default::default());
    for (col, rules) in string_rules_by_col.into_iter() {
        string_by_col.insert(col, Arc::new(rules));
    }

    let mut numeric_columns: Vec<ColumnHeaderEntryV2> = Vec::with_capacity(numeric_by_col.len());
    for (col, rules) in numeric_by_col.iter() {
        let mut lt = 0u32;
        let mut gt = 0u32;
        let mut col_type = 0u8;
        if let Some(first) = rules.first() {
            col_type = first.column_type.to_byte();
            for r in rules.iter() {
                match r.op {
                    super::rules::NumericRuleOp::LessThan => lt = lt.saturating_add(1),
                    super::rules::NumericRuleOp::GreaterThan => gt = gt.saturating_add(1),
                }
            }
        }

        numeric_columns.push(ColumnHeaderEntryV2 {
            column_schema_id: *col,
            column_type: col_type,
            lt_rule_count: lt,
            gt_rule_count: gt,
            rule_count: lt.saturating_add(gt),
            start_offset: 0,
            end_offset: 0,
        });
    }
    numeric_columns.sort_by_key(|e| e.column_schema_id);

    let mut string_columns: Vec<StringColumnHeaderEntryV2> = Vec::with_capacity(string_by_col.len());
    for (col, rules) in string_by_col.iter() {
        let mut sw = 0u32;
        let mut ew = 0u32;
        let mut c = 0u32;
        let mut col_type = 0u8;
        if let Some(first) = rules.first() {
            col_type = first.column_type.to_byte();
            for r in rules.iter() {
                match r.op {
                    super::rules::StringRuleOp::StartsWith => sw = sw.saturating_add(1),
                    super::rules::StringRuleOp::EndsWith => ew = ew.saturating_add(1),
                    super::rules::StringRuleOp::Contains => c = c.saturating_add(1),
                }
            }
        }

        string_columns.push(StringColumnHeaderEntryV2 {
            column_schema_id: *col,
            column_type: col_type,
            startswith_rule_count: sw,
            endswith_rule_count: ew,
            contains_rule_count: c,
            rule_count: sw.saturating_add(ew).saturating_add(c),
            start_offset: 0,
            end_offset: 0,
        });
    }
    string_columns.sort_by_key(|e| e.column_schema_id);

    let header = RulesFileHeaderV2 {
        numeric_header_len_bytes: (numeric_columns.len() * 40) as u32,
        string_header_len_bytes: (string_columns.len() * 48) as u32,
        format: RulesHeaderFormatV2::Numeric40String48,
        numeric_columns,
        string_columns,
    };

    let table_rules = Arc::new(TableRulesV2 {
        header: Arc::new(header),
        numeric_by_col,
        string_by_col,
    });

    let mut guard = store().write();
    guard.insert(table_name.to_string(), table_rules);
}

pub fn get_table_rules(table_name: &str) -> Option<Arc<TableRulesV2>> {
    let guard = store().read();
    guard.get(table_name).cloned()
}

pub fn get_header(table_name: &str) -> Option<Arc<RulesFileHeaderV2>> {
    get_table_rules(table_name).map(|t| Arc::clone(&t.header))
}

pub fn get_numeric_rules(table_name: &str, column_schema_id: u64) -> Option<Arc<Vec<NumericCorrelationRule>>> {
    get_table_rules(table_name)
        .and_then(|t| t.numeric_by_col.get(&column_schema_id).cloned())
}

pub fn get_string_rules(table_name: &str, column_schema_id: u64) -> Option<Arc<Vec<StringCorrelationRule>>> {
    get_table_rules(table_name)
        .and_then(|t| t.string_by_col.get(&column_schema_id).cloned())
}

pub fn has_any_rules(table_name: &str) -> bool {
    let Some(t) = get_table_rules(table_name) else {
        return false;
    };
    t.numeric_by_col.values().any(|v| !v.is_empty()) || t.string_by_col.values().any(|v| !v.is_empty())
}
