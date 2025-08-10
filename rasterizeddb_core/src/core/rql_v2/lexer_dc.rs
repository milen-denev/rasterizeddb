use crate::core::rql_v2::lexer_s1::QueryPurpose;

pub struct DropColumn {
	pub table_name: String,
	pub column_name: String,
}

/// Attempts to parse a QueryPurpose::DropColumn variant into a DropColumn struct.
/// Returns None if the QueryPurpose is not DropColumn or parsing fails.
pub fn drop_column_from_query_purpose(qp: &QueryPurpose) -> Option<DropColumn> {
	if let QueryPurpose::DropColumn(sql) = qp {
		let sql = sql.as_str();
		let sql_upper = sql.to_ascii_uppercase();
		if !sql_upper.starts_with("ALTER TABLE") {
			return None;
		}
		// ALTER TABLE table_name DROP COLUMN column_name;
		let mut parts = sql.split_whitespace();
		parts.next(); // ALTER
		parts.next(); // TABLE
		let table_name = parts.next()?.trim_matches('"');
		// Find "DROP" and "COLUMN"
		while let Some(word) = parts.next() {
			if word.to_ascii_uppercase() == "DROP" {
				if let Some(next_word) = parts.next() {
					if next_word.to_ascii_uppercase() == "COLUMN" {
						break;
					}
				}
			}
		}
		let col_name = parts.next()?.trim_matches('"');
		let mut parts_check = parts.clone();
		if col_name.is_empty() || parts_check.next().is_some() {
			return None;
		}
		Some(DropColumn { table_name: table_name.to_string(), column_name: col_name.to_string() })
	} else {
		None
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::core::rql_v2::lexer_s1::QueryPurpose;

	#[test]
	fn test_drop_column_alter_table_form() {
		let sql = r#"ALTER TABLE users DROP COLUMN age"#;
		let qp = QueryPurpose::DropColumn(sql.to_string());
		let result = drop_column_from_query_purpose(&qp).expect("Should parse successfully");
		assert_eq!(result.table_name, "users");
		assert_eq!(result.column_name, "age");
	}

	#[test]
	fn test_drop_column_with_quotes() {
		let sql = r#"ALTER TABLE "my_table" DROP COLUMN "flag""#;
		let qp = QueryPurpose::DropColumn(sql.to_string());
		let result = drop_column_from_query_purpose(&qp).expect("Should parse successfully");
		assert_eq!(result.table_name, "my_table");
		assert_eq!(result.column_name, "flag");
	}

	#[test]
	fn test_drop_column_add_column_form_should_fail() {
		let sql = r#"DROP COLUMN users age"#;
		let qp = QueryPurpose::DropColumn(sql.to_string());
		assert!(drop_column_from_query_purpose(&qp).is_none());
	}

	#[test]
	fn test_drop_column_invalid_sql() {
		let sql = r#"ALTER TABLE t1 DROP COLUMN"#;
		let qp = QueryPurpose::DropColumn(sql.to_string());
		assert!(drop_column_from_query_purpose(&qp).is_none());
	}
}
