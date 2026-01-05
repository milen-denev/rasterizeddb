use smallvec::SmallVec;

use crate::{
	core::{
		db_type::DbType, processor::transformer::ComparerOperation, row::{
			common::simd_compare_strings,
			row::{ColumnFetchingData, ColumnWritePayload, RowFetch},
			schema::{SchemaCalculator, SchemaField},
		}, rql::lexer_s1::QueryPurpose
	},
	memory_pool::{MEMORY_POOL, MemoryBlock},
};

pub struct UpdateRow {
	pub table_name: String,
	pub where_clause: String,
	pub query_row_fetch: RowFetch,
	pub updates: SmallVec<[ColumnWritePayload; 32]>,
}

fn extract_where_columns(where_clause: &str, schema: &SmallVec<[SchemaField; 20]>) -> std::collections::HashSet<String> {
	let mut where_columns = std::collections::HashSet::new();
	if where_clause.is_empty() {
		return where_columns;
	}

	let where_bytes = where_clause.as_bytes();
	for field in schema.iter() {
		let field_bytes = field.name.as_bytes();
		for i in 0..where_bytes.len().saturating_sub(field_bytes.len()) {
			if i + field_bytes.len() <= where_bytes.len() {
				let slice = &where_bytes[i..i + field_bytes.len()];
				if simd_compare_strings(slice, field_bytes, &ComparerOperation::Equals) {
					let start_ok = i == 0 || !where_bytes[i - 1].is_ascii_alphanumeric();
					let end_ok = i + field_bytes.len() == where_bytes.len()
						|| !where_bytes[i + field_bytes.len()].is_ascii_alphanumeric();
					if start_ok && end_ok {
						where_columns.insert(field.name.clone());
						break;
					}
				}
			}
		}
	}

	where_columns
}

#[inline(always)]
fn strip_surrounding_quotes(s: &str) -> &str {
	let s = s.trim();
	if (s.starts_with('"') && s.ends_with('"')) || (s.starts_with('\'') && s.ends_with('\'')) {
		if s.len() >= 2 {
			return &s[1..s.len() - 1];
		}
	}
	s
}

#[inline(always)]
fn parse_postgres_bool(s: &str) -> Option<u8> {
	let lower = s.trim().to_ascii_lowercase();
	match lower.as_str() {
		"true" | "t" | "yes" | "y" | "on" | "1" => Some(1),
		"false" | "f" | "no" | "n" | "off" | "0" => Some(0),
		_ => None,
	}
}

fn value_to_mb(value_str: &str, db_type: &DbType) -> MemoryBlock {
	let value_str = value_str.trim();

	// "NULL" becomes empty.
	if value_str.eq_ignore_ascii_case("NULL") {
		return MemoryBlock::default();
	}

	match db_type {
		DbType::STRING | DbType::CHAR | DbType::UNKNOWN | DbType::START | DbType::END => {
			let stripped = strip_surrounding_quotes(value_str);
			let bytes = stripped.as_bytes();
			let mut mb = MEMORY_POOL.acquire(bytes.len());
			mb.into_slice_mut().copy_from_slice(bytes);
			mb
		}
		DbType::I8 => {
			let v: i8 = value_str.parse().unwrap_or_default();
			let bytes = v.to_le_bytes();
			let mut mb = MEMORY_POOL.acquire(bytes.len());
			mb.into_slice_mut().copy_from_slice(&bytes);
			mb
		}
		DbType::I16 => {
			let v: i16 = value_str.parse().unwrap_or_default();
			let bytes = v.to_le_bytes();
			let mut mb = MEMORY_POOL.acquire(bytes.len());
			mb.into_slice_mut().copy_from_slice(&bytes);
			mb
		}
		DbType::I32 => {
			let v: i32 = value_str.parse().unwrap_or_default();
			let bytes = v.to_le_bytes();
			let mut mb = MEMORY_POOL.acquire(bytes.len());
			mb.into_slice_mut().copy_from_slice(&bytes);
			mb
		}
		DbType::I64 => {
			let v: i64 = value_str.parse().unwrap_or_default();
			let bytes = v.to_le_bytes();
			let mut mb = MEMORY_POOL.acquire(bytes.len());
			mb.into_slice_mut().copy_from_slice(&bytes);
			mb
		}
		DbType::I128 => {
			let v: i128 = value_str.parse().unwrap_or_default();
			let bytes = v.to_le_bytes();
			let mut mb = MEMORY_POOL.acquire(bytes.len());
			mb.into_slice_mut().copy_from_slice(&bytes);
			mb
		}
		DbType::U8 => {
			let v: u8 = value_str.parse().unwrap_or_default();
			let bytes = v.to_le_bytes();
			let mut mb = MEMORY_POOL.acquire(bytes.len());
			mb.into_slice_mut().copy_from_slice(&bytes);
			mb
		}
		DbType::U16 => {
			let v: u16 = value_str.parse().unwrap_or_default();
			let bytes = v.to_le_bytes();
			let mut mb = MEMORY_POOL.acquire(bytes.len());
			mb.into_slice_mut().copy_from_slice(&bytes);
			mb
		}
		DbType::U32 => {
			let v: u32 = value_str.parse().unwrap_or_default();
			let bytes = v.to_le_bytes();
			let mut mb = MEMORY_POOL.acquire(bytes.len());
			mb.into_slice_mut().copy_from_slice(&bytes);
			mb
		}
		DbType::U64 => {
			let v: u64 = value_str.parse().unwrap_or_default();
			let bytes = v.to_le_bytes();
			let mut mb = MEMORY_POOL.acquire(bytes.len());
			mb.into_slice_mut().copy_from_slice(&bytes);
			mb
		}
		DbType::U128 => {
			let v: u128 = value_str.parse().unwrap_or_default();
			let bytes = v.to_le_bytes();
			let mut mb = MEMORY_POOL.acquire(bytes.len());
			mb.into_slice_mut().copy_from_slice(&bytes);
			mb
		}
		DbType::F32 => {
			let v: f32 = value_str.parse().unwrap_or_default();
			let bytes = v.to_le_bytes();
			let mut mb = MEMORY_POOL.acquire(bytes.len());
			mb.into_slice_mut().copy_from_slice(&bytes);
			mb
		}
		DbType::F64 => {
			let v: f64 = value_str.parse().unwrap_or_default();
			let bytes = v.to_le_bytes();
			let mut mb = MEMORY_POOL.acquire(bytes.len());
			mb.into_slice_mut().copy_from_slice(&bytes);
			mb
		}
		DbType::BOOL => {
			let stripped = strip_surrounding_quotes(value_str);
			let v = parse_postgres_bool(stripped).unwrap_or(0);
			let bytes = v.to_le_bytes();
			let mut mb = MEMORY_POOL.acquire(bytes.len());
			mb.into_slice_mut().copy_from_slice(&bytes);
			mb
		}
		DbType::DATETIME => {
			// Stored as i64 epoch for now.
			let v: i64 = value_str.parse().unwrap_or_default();
			let bytes = v.to_le_bytes();
			let mut mb = MEMORY_POOL.acquire(bytes.len());
			mb.into_slice_mut().copy_from_slice(&bytes);
			mb
		}
		DbType::NULL => MemoryBlock::default(),
	}
}

/// Attempts to parse a QueryPurpose::UpdateRow variant into an UpdateRow plan.
///
/// Supported form (simple):
/// `UPDATE table_name SET col1 = val1, col2 = val2 WHERE <expr>`
pub fn update_row_from_query_purpose(
	qp: &QueryPurpose,
	schema: &SmallVec<[SchemaField; 20]>,
) -> Option<UpdateRow> {
	let sql = match qp {
		QueryPurpose::UpdateRow(sql) => sql.trim(),
		_ => return None,
	};

	let sql_upper = sql.to_ascii_uppercase();
	if !sql_upper.starts_with("UPDATE") {
		return None;
	}

	// Find SET
	let set_pos = sql_upper.find(" SET ")?;
	let before_set = sql[..set_pos].trim();
	let after_set = sql[set_pos + 5..].trim();

	// Table name is after UPDATE
	let mut parts = before_set.split_whitespace();
	parts.next()?; // UPDATE
	let table_name = parts.next()?.trim_matches('"');
	if table_name.is_empty() {
		return None;
	}

	// Split optional WHERE
	let after_set_upper = after_set.to_ascii_uppercase();
	let where_pos = after_set_upper.find(" WHERE ");
	let (set_clause, where_clause) = if let Some(pos) = where_pos {
		(after_set[..pos].trim(), after_set[pos + 7..].trim())
	} else {
		(after_set.trim(), "")
	};

	if set_clause.is_empty() {
		return None;
	}

	// Parse assignments
	let mut updates: SmallVec<[ColumnWritePayload; 32]> = SmallVec::new();
	for assignment in set_clause.split(',') {
		let assignment = assignment.trim();
		if assignment.is_empty() {
			continue;
		}

		let mut kv = assignment.splitn(2, '=');
		let key = kv.next()?.trim().trim_matches('"');
		let value = kv.next()?.trim();
		if key.is_empty() {
			return None;
		}

		let field = schema.iter().find(|f| f.name.eq_ignore_ascii_case(key))?;
		let data = value_to_mb(value, &field.db_type);
		updates.push(ColumnWritePayload {
			data,
			write_order: field.write_order as u32,
			column_type: field.db_type.clone(),
			size: field.size,
		});
	}

	if updates.is_empty() {
		return None;
	}

	// Build query_row_fetch for WHERE clause
	let where_columns = extract_where_columns(where_clause, schema);
	let schema_calc = SchemaCalculator::default();
	let mut query_columns_fetching_data = SmallVec::new();
	for col_name in &where_columns {
		let field = schema
			.iter()
			.find(|f| f.name.eq_ignore_ascii_case(col_name.as_str()))?;
		let offset = schema_calc.calculate_schema_offset(&field.name, schema);
		query_columns_fetching_data.push(ColumnFetchingData {
			column_offset: offset.0,
			column_type: field.db_type.clone(),
			size: field.size,
			schema_id: offset.1,
		});
	}

	Some(UpdateRow {
		table_name: table_name.to_string(),
		where_clause: where_clause.to_string(),
		query_row_fetch: RowFetch {
			columns_fetching_data: query_columns_fetching_data,
		},
		updates,
	})
}

#[cfg(test)]
mod tests {
	use super::*;

	fn make_schema() -> SmallVec<[SchemaField; 20]> {
		smallvec::smallvec![
			SchemaField {
				name: "id".to_string(),
				db_type: DbType::U64,
				size: 8,
				offset: 0,
				write_order: 0,
				is_unique: true,
				is_deleted: false,
			},
			SchemaField {
				name: "name".to_string(),
				db_type: DbType::STRING,
				size: 12,
				offset: 8,
				write_order: 1,
				is_unique: false,
				is_deleted: false,
			},
		]
	}

	#[test]
	fn test_update_row_basic() {
		let schema = make_schema();
		let sql = r#"UPDATE users SET name = 'Alice' WHERE id = 1"#;
		let qp = QueryPurpose::UpdateRow(sql.to_string());
		let ur = update_row_from_query_purpose(&qp, &schema).expect("should parse");
		assert_eq!(ur.table_name, "users");
		assert_eq!(ur.where_clause, "id = 1");
		assert_eq!(ur.updates.len(), 1);
		assert_eq!(ur.updates[0].write_order, 1);
	}
}
