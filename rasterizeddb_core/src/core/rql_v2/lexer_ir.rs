use smallvec::SmallVec;

use crate::core::{rql_v2::lexer_s1::QueryPurpose, row_v2::{row::{RowWrite, ColumnWritePayload}, schema::SchemaField}};
use crate::memory_pool::MemoryBlock;

/// Attempts to parse a QueryPurpose::InsertRow variant into a RowWrite object, using the provided schema.
/// Returns None if the QueryPurpose is not InsertRow or parsing fails.
pub fn insert_row_from_query_purpose(qp: &QueryPurpose, schema: &[SchemaField]) -> Option<RowWrite> {
	if let QueryPurpose::InsertRow(sql) = qp {
		let sql = sql.as_str();
		let sql_upper = sql.to_ascii_uppercase();
		if !sql_upper.starts_with("INSERT INTO") {
			return None;
		}
		// Parse: INSERT INTO table_name (col1, col2, ...) VALUES (val1, val2, ...)
		let paren_start = sql.find('(')?;
		let paren_end = sql.find(')')?;
		let columns_str = &sql[paren_start+1..paren_end];
		let columns: SmallVec<[&str; 32]> = columns_str.split(',').map(|s| s.trim().trim_matches('"')).collect();
		let values_kw = sql_upper.find("VALUES")?;
		let values_paren_start = sql[values_kw..].find('(')? + values_kw;
		let values_paren_end = sql[values_paren_start..].find(')')? + values_paren_start;
		let values_str = &sql[values_paren_start+1..values_paren_end];
		let values: SmallVec<[&str; 32]> = values_str.split(',').map(|s| s.trim().trim_matches('"')).collect();
		if columns.len() != values.len() { return None; }
		let mut columns_writing_data = SmallVec::<[ColumnWritePayload; 32]>::new();

        for (col_name, value) in columns.iter().zip(values.iter()) {
            // Find schema field
            let schema_field = schema.iter().find(|f| f.name == *col_name)?;
            // Convert value to MemoryBlock (for now, just store as bytes, real impl may differ)
            let data = MemoryBlock::from_vec(value.as_bytes().to_vec());
            let payload = ColumnWritePayload {
            data,
            write_order: schema_field.write_order as u32, // Could be index or other logic
            column_type: schema_field.db_type.clone(),
            size: schema_field.size as u32,
            };
            columns_writing_data.push(payload);
        }
        
        // Sort by write_order before creating the RowWrite
        columns_writing_data.sort_by_key(|col| col.write_order);
		Some(RowWrite { columns_writing_data })
	} else {
		None
	}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::row_v2::schema::SchemaField;
    use crate::core::db_type::DbType;

    #[test]
    fn test_insert_row_basic() {
        let schema = vec![
            SchemaField { name: "id".to_string(), db_type: DbType::I32, size: 4, offset: 0, write_order: 0, is_unique: false, is_deleted: false },
            SchemaField { name: "name".to_string(), db_type: DbType::STRING, size: 4 + 8, offset: 4, write_order: 1, is_unique: false, is_deleted: false },
        ];
        let sql = r#"INSERT INTO users (id, name) VALUES (1, "Alice")"#;
        let qp = QueryPurpose::InsertRow(sql.to_string());
        let row_write = insert_row_from_query_purpose(&qp, &schema).expect("Should parse successfully");
        assert_eq!(row_write.columns_writing_data.len(), 2);
        assert_eq!(row_write.columns_writing_data[0].column_type, DbType::I32);
        assert_eq!(row_write.columns_writing_data[1].column_type, DbType::STRING);
        assert_eq!(row_write.columns_writing_data[0].size, 4);
        assert_eq!(row_write.columns_writing_data[1].size, 12);
    }

    #[test]
    fn test_insert_row_mismatched_columns() {
        let schema = vec![
            SchemaField { name: "id".to_string(), db_type: DbType::I32, size: 4, offset: 0, write_order: 0, is_unique: false, is_deleted: false },
        ];
        let sql = r#"INSERT INTO users (id, name) VALUES (1, "Alice")"#;
        let qp = QueryPurpose::InsertRow(sql.to_string());
        assert!(insert_row_from_query_purpose(&qp, &schema).is_none());
    }

    #[test]
    fn test_insert_row_many_columns_full_check() {
        let schema = vec![
            SchemaField { name: "id".to_string(), db_type: DbType::I32, size: 4, offset: 0, write_order: 0, is_unique: true, is_deleted: false },
            SchemaField { name: "name".to_string(), db_type: DbType::STRING, size: 16, offset: 4, write_order: 1, is_unique: false, is_deleted: false },
            SchemaField { name: "age".to_string(), db_type: DbType::I16, size: 2, offset: 20, write_order: 2, is_unique: false, is_deleted: false },
            SchemaField { name: "email".to_string(), db_type: DbType::STRING, size: 32, offset: 22, write_order: 3, is_unique: true, is_deleted: false },
            SchemaField { name: "active".to_string(), db_type: DbType::U8, size: 1, offset: 54, write_order: 4, is_unique: false, is_deleted: false },
            SchemaField { name: "score".to_string(), db_type: DbType::F64, size: 8, offset: 55, write_order: 5, is_unique: false, is_deleted: false },
            SchemaField { name: "created_at".to_string(), db_type: DbType::I64, size: 8, offset: 63, write_order: 6, is_unique: false, is_deleted: false },
            SchemaField { name: "notes".to_string(), db_type: DbType::STRING, size: 64, offset: 71, write_order: 7, is_unique: false, is_deleted: false },
        ];

        let sql = r#"INSERT INTO users (id, name, age, email, active, score, created_at, notes) VALUES (42, "Bob", 27, "bob@example.com", 1, 99.5, 1680000000, "Test user")"#;
        let qp = QueryPurpose::InsertRow(sql.to_string());
        let row_write = insert_row_from_query_purpose(&qp, &schema).expect("Should parse successfully");
        assert_eq!(row_write.columns_writing_data.len(), schema.len());

        for (i, payload) in row_write.columns_writing_data.iter().enumerate() {
            let field = &schema[i];
            assert_eq!(payload.write_order, field.write_order as u32, "write_order mismatch at col {}", i);
            assert_eq!(payload.column_type, field.db_type, "db_type mismatch at col {}", i);
            assert_eq!(payload.size, field.size as u32, "size mismatch at col {}", i);
            // Check data bytes (string columns will be quoted, others as stringified numbers or bools)
            let expected_value = match field.name.as_str() {
                "id" => b"42".as_ref(),
                "name" => b"Bob".as_ref(),
                "age" => b"27".as_ref(),
                "email" => b"bob@example.com".as_ref(),
                "active" => b"1".as_ref(),
                "score" => b"99.5".as_ref(),
                "created_at" => b"1680000000".as_ref(),
                "notes" => b"Test user".as_ref(),
                _ => panic!("Unexpected column"),
            };
            let actual_bytes = payload.data.into_slice();
            assert_eq!(actual_bytes, expected_value, "data bytes mismatch at col {}", i);
        }

        // Check that write_order is sorted
        let mut last_order = 0;
        for payload in &row_write.columns_writing_data {
            assert!(payload.write_order >= last_order, "write_order not sorted");
            last_order = payload.write_order;
        }
    }

    #[test]
    fn test_insert_row_column_order_mixed() {
        let schema = vec![
            SchemaField { name: "id".to_string(), db_type: DbType::I32, size: 4, offset: 0, write_order: 0, is_unique: true, is_deleted: false },
            SchemaField { name: "name".to_string(), db_type: DbType::STRING, size: 16, offset: 4, write_order: 1, is_unique: false, is_deleted: false },
            SchemaField { name: "age".to_string(), db_type: DbType::I16, size: 2, offset: 20, write_order: 2, is_unique: false, is_deleted: false },
        ];
        let sql = r#"INSERT INTO users (age, id, name) VALUES (30, 100, "Charlie")"#;
        let qp = QueryPurpose::InsertRow(sql.to_string());
        let row_write = insert_row_from_query_purpose(&qp, &schema).expect("Should parse successfully");
        assert_eq!(row_write.columns_writing_data.len(), 3);

        // Should be sorted by write_order: id (0), name (1), age (2)
        assert_eq!(row_write.columns_writing_data[0].write_order, 0);
        assert_eq!(row_write.columns_writing_data[0].column_type, DbType::I32);
        assert_eq!(row_write.columns_writing_data[0].data.into_slice(), b"100");

        assert_eq!(row_write.columns_writing_data[1].write_order, 1);
        assert_eq!(row_write.columns_writing_data[1].column_type, DbType::STRING);
        assert_eq!(row_write.columns_writing_data[1].data.into_slice(), b"Charlie");

        assert_eq!(row_write.columns_writing_data[2].write_order, 2);
        assert_eq!(row_write.columns_writing_data[2].column_type, DbType::I16);
        assert_eq!(row_write.columns_writing_data[2].data.into_slice(), b"30");
    }
}
