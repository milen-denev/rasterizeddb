use itertools::Itertools;
use smallvec::SmallVec;

use crate::core::row_v2::schema::{SchemaField, SchemaCalculator};
use crate::core::row_v2::row::{RowFetch, ColumnFetchingData};
use crate::core::row_v2::transformer::ComparerOperation;
use crate::core::rql_v2::lexer_s1::{QueryPurpose};
use crate::core::row_v2::common::simd_compare_strings;

pub struct QueryRows {
    pub table_name: String,
    pub row_fetch: RowFetch,
    pub where_clause: String,
}

/// Parses a QueryPurpose::QueryRows variant and returns a RowFetch (for the columns) and the WHERE clause (without the WHERE word)
///
/// # Arguments
/// * `schema` - Vec<SchemaField> representing the table schema
/// * `query_purpose` - QueryPurpose enum (should be QueryPurpose::QueryRows)
///
/// # Returns
/// (RowFetch, String) - RowFetch for the columns, and the WHERE clause (without the WHERE word)
pub fn row_fetch_from_select_query(query_purpose: &QueryPurpose, schema: &SmallVec<[SchemaField; 20]>) -> Result<QueryRows, String> {
	let sql = match query_purpose {
		QueryPurpose::QueryRows(qr_data) => qr_data.query.trim(),
		_ => return Err("Not a SELECT/QueryRows query".to_string()),
	};
	let sql_bytes = sql.as_bytes();
	let select_prefix = b"SELECT ";
	let from_kw = b" FROM ";
	if !simd_compare_strings(&sql_bytes[..select_prefix.len().min(sql_bytes.len())], select_prefix, &ComparerOperation::Equals) {
		return Err("Not a SELECT query".to_string());
	}
	let mut from_pos = None;
	for i in 0..sql_bytes.len().saturating_sub(from_kw.len()) {
		if simd_compare_strings(&sql_bytes[i..i+from_kw.len()], from_kw, &ComparerOperation::Equals) {
			from_pos = Some(i);
			break;
		}
	}
	let from_pos = from_pos.ok_or("Missing FROM in SELECT query")?;

	let columns_part = sql[select_prefix.len()..from_pos].trim();
	let after_from = &sql[from_pos + from_kw.len()..];
	let where_kw = b" WHERE ";
	let mut where_pos = None;
	let after_from_bytes = after_from.as_bytes();
	for i in 0..after_from_bytes.len().saturating_sub(where_kw.len()) {
		if simd_compare_strings(&after_from_bytes[i..i+where_kw.len()], where_kw, &ComparerOperation::Equals) {
			where_pos = Some(i);
			break;
		}
	}
	let (table_part, where_part) = if let Some(idx) = where_pos {
		(after_from[..idx].trim(), after_from[idx + where_kw.len()..].trim())
	} else {
		(after_from.trim(), "")
	};

	// Parse columns
	let columns: Vec<&str> = if columns_part == "*" {
		schema.iter().sorted_by(|a, b| a.write_order.cmp(&b.write_order)).map(|f| f.name.as_str()).collect()
	} else {
		columns_part.split(',').map(|s| s.trim()).collect()
	};

	let schema_calc = SchemaCalculator::default();
	let mut columns_fetching_data = SmallVec::new();

	for col in columns {
		let field = schema.iter().find(|f| simd_compare_strings(f.name.as_bytes(), col.as_bytes(), &ComparerOperation::Equals))
			.ok_or_else(|| format!("Column '{}' not found in schema", col))?;
		let offset = schema_calc.calculate_schema_offset(&field.name, schema);
		columns_fetching_data.push(ColumnFetchingData {
			column_offset: offset,
			column_type: field.db_type.clone(),
			size: field.size,
		});
	}

	Ok(QueryRows {
		table_name: table_part.to_string(),
		row_fetch: RowFetch { columns_fetching_data },
		where_clause: where_part.to_string(),
	})
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::core::row_v2::schema::SchemaField;
	use crate::core::db_type::DbType;
	use crate::core::row_v2::common::simd_compare_strings;
	use crate::core::row_v2::transformer::ComparerOperation;

	fn make_schema() -> SmallVec<[SchemaField; 20]> {
		smallvec::smallvec![
			SchemaField { name: "id".to_string(), db_type: DbType::U64, size: 8, offset: 0, write_order: 0, is_unique: true, is_deleted: false },
			SchemaField { name: "name".to_string(), db_type: DbType::STRING, size: 32, offset: 8, write_order: 1, is_unique: false, is_deleted: false },
			SchemaField { name: "age".to_string(), db_type: DbType::U8, size: 1, offset: 40, write_order: 2, is_unique: false, is_deleted: false },
			SchemaField { name: "email".to_string(), db_type: DbType::STRING, size: 64, offset: 41, write_order: 3, is_unique: false, is_deleted: false },
		]
	}

	#[test]
	fn test_select_all_columns() {
		let schema = make_schema();
		let query = "SELECT * FROM users WHERE age > 18";
		let qp = crate::core::rql_v2::lexer_s1::QueryPurpose::QueryRows(
			crate::core::rql_v2::lexer_s1::QueryRowsData {
				table_name: "users".to_string(),
				query: query.to_string(),
			}
		);
		let qr = row_fetch_from_select_query(&qp, &schema).unwrap();
		assert_eq!(qr.row_fetch.columns_fetching_data.len(), schema.len());
		assert_eq!(qr.where_clause, "age > 18");
		assert_eq!(qr.table_name, "users");
	}

	#[test]
	fn test_select_specific_columns() {
		let schema = make_schema();
		let query = "SELECT id, name FROM users WHERE name = 'John'";
		let qp = crate::core::rql_v2::lexer_s1::QueryPurpose::QueryRows(
			crate::core::rql_v2::lexer_s1::QueryRowsData {
				table_name: "users".to_string(),
				query: query.to_string(),
			}
		);
		let qr = row_fetch_from_select_query(&qp, &schema).unwrap();
		assert_eq!(qr.row_fetch.columns_fetching_data.len(), 2);
		assert_eq!(qr.row_fetch.columns_fetching_data[0].column_type, DbType::U64);
		assert_eq!(qr.row_fetch.columns_fetching_data[1].column_type, DbType::STRING);
		assert_eq!(qr.where_clause, "name = 'John'");
		assert_eq!(qr.table_name, "users");
	}

	#[test]
	fn test_select_no_where() {
		let schema = make_schema();
		let query = "SELECT id, email FROM users";
		let qp = crate::core::rql_v2::lexer_s1::QueryPurpose::QueryRows(
			crate::core::rql_v2::lexer_s1::QueryRowsData {
				table_name: "users".to_string(),
				query: query.to_string(),
			}
		);
		let qr = row_fetch_from_select_query(&qp, &schema).unwrap();
		assert_eq!(qr.row_fetch.columns_fetching_data.len(), 2);
		assert_eq!(qr.where_clause, "");
		assert_eq!(qr.table_name, "users");
	}

	#[test]
	fn test_column_not_found() {
		let schema = make_schema();
		let query = "SELECT id, not_a_column FROM users";
		let qp = crate::core::rql_v2::lexer_s1::QueryPurpose::QueryRows(
			crate::core::rql_v2::lexer_s1::QueryRowsData {
				table_name: "users".to_string(),
				query: query.to_string(),
			}
		);
		let result = row_fetch_from_select_query(&qp, &schema);
		assert!(result.is_err());
	}

	#[test]
	fn test_simd_compare_strings_for_column_names() {
		let schema = make_schema();
		let col_name = "name";
		let col_bytes = col_name.as_bytes();
		let found = schema.iter().any(|f| {
			simd_compare_strings(f.name.as_bytes(), col_bytes, &ComparerOperation::Equals)
		});
		assert!(found);
		let not_found = schema.iter().any(|f| {
			simd_compare_strings(f.name.as_bytes(), b"not_a_column", &ComparerOperation::Equals)
		});
		assert!(!not_found);
	}

	#[test]
	fn test_simd_compare_strings_for_where_clause() {
		// Simulate a WHERE clause comparison using SIMD
		let clause = "name = 'John'";
		let target = "name = 'John'";
		let result = simd_compare_strings(clause.as_bytes(), target.as_bytes(), &ComparerOperation::Equals);
		assert!(result);
		let not_result = simd_compare_strings(clause.as_bytes(), b"age > 18", &ComparerOperation::Equals);
		assert!(!not_result);
	}
}
