use rclite::Arc;

use log::{debug, error, info};

use crate::core::{
    database::Database, row::row::rows_into_vec, rql::{
        lexer_ct::create_table_from_query_purpose, lexer_ir::insert_row_from_query_purpose,
        lexer_query::row_fetch_from_select_query, lexer_s1::QueryPurpose,
        lexer_ur::update_row_from_query_purpose,
        lexer_dr::delete_row_from_query_purpose,
    }
};

pub async fn execute(query: QueryPurpose, database: Arc<Database>) -> Vec<u8> {
    match &query {
        QueryPurpose::CreateTable(_) => {
            // Handle create table
            if let Some(create_table) = create_table_from_query_purpose(&query) {
                if database.add_table(create_table).await.is_ok() {
                    info!("Table created successfully");
                    let mut result: [u8; 1] = [0u8; 1];
                    result[0] = 0;
                    return result.to_vec();
                } else {
                    error!("Failed to add table");
                    let mut result: [u8; 1] = [1u8; 1];
                    result[0] = 3;
                    let mut result = result.to_vec();
                    let error_message = "Error occurred";
                    let mut error_message_bytes = Vec::with_capacity(error_message.len());
                    error_message_bytes.extend_from_slice(error_message.as_bytes());
                    result.extend_from_slice(&mut error_message_bytes);
                    return result.to_vec();
                }
            } else {
                error!("Table already exists");
                let mut result: [u8; 1] = [1u8; 1];
                result[0] = 3;
                let mut result = result.to_vec();
                let error_message = "Table already exists";
                let mut error_message_bytes = Vec::with_capacity(error_message.len());
                error_message_bytes.extend_from_slice(error_message.as_bytes());
                result.extend_from_slice(&mut error_message_bytes);
                return result.to_vec();
            }
        }
        QueryPurpose::DropTable(_) => {
            // Handle drop table
            todo!("Handle drop table");
        }
        QueryPurpose::AddColumn(_) => {
            // Handle add column
            todo!("Handle add column");
        }
        QueryPurpose::DropColumn(_) => {
            // Handle drop column
            todo!("Handle drop column");
        }
        QueryPurpose::RenameColumn(_) => {
            // Handle rename column
            todo!("Handle rename column");
        }
        QueryPurpose::InsertRow(row_data) => {
            let table_name = &row_data.table_name;
            debug!("Insert row into table: {}", table_name);
            let table_ref = database.tables.get(table_name);
            let table_clone = table_ref.as_ref().clone();

            let table = if let Some(table) = table_clone {
                table
            } else {
                error!("Table not found");
                let mut result: [u8; 1] = [1u8; 1];
                result[0] = 3;
                let mut result = result.to_vec();
                let error_message = "Table not found";
                let mut error_message_bytes = Vec::with_capacity(error_message.len());
                error_message_bytes.extend_from_slice(error_message.as_bytes());
                result.extend_from_slice(&mut error_message_bytes);
                return result.to_vec();
            };

            let schema = &table.schema;

            // Handle insert row
            if let Some(insert_row) = insert_row_from_query_purpose(&query, &schema.fields) {
                if table.insert_row(insert_row).await.is_ok() {
                    // Keep status=0 for backward compatibility, but also include rows-affected.
                    // pgwire can use this to emit INSERT 0 <n>.
                    let mut result = vec![0u8];
                    result.extend_from_slice(&1u64.to_le_bytes());
                    return result;
                } else {
                    error!("Failed to insert row");
                    let mut result: [u8; 1] = [1u8; 1];
                    result[0] = 3;
                    let mut result = result.to_vec();
                    let error_message = "Error occurred";
                    let mut error_message_bytes = Vec::with_capacity(error_message.len());
                    error_message_bytes.extend_from_slice(error_message.as_bytes());
                    result.extend_from_slice(&mut error_message_bytes);
                    return result.to_vec();
                }
            } else {
                error!("Cannot insert row due to schema mismatch");
                let mut result: [u8; 1] = [1u8; 1];
                result[0] = 3;
                let mut result = result.to_vec();
                let error_message = "Cannot insert row due to schema mismatch";
                let mut error_message_bytes = Vec::with_capacity(error_message.len());
                error_message_bytes.extend_from_slice(error_message.as_bytes());
                result.extend_from_slice(&mut error_message_bytes);
                return result.to_vec();
            }
        }
        QueryPurpose::UpdateRow(_) => {
            let schema_fields;

            // Parse UPDATE ... using schema
            // We need the table to obtain schema, so first read the table name in a lightweight way.
            let table_name = match &query {
                QueryPurpose::UpdateRow(sql) => sql.split_whitespace().nth(1).unwrap_or("").trim_matches('"').to_string(),
                _ => String::new(),
            };

            if table_name.is_empty() {
                error!("UPDATE missing table name");
                let mut result: [u8; 1] = [1u8; 1];
                result[0] = 3;
                let mut result = result.to_vec();
                let error_message = "UPDATE missing table name";
                result.extend_from_slice(error_message.as_bytes());
                return result;
            }

            let table_ref = database.tables.get(&table_name);
            let table_clone = table_ref.as_ref().clone();
            let table = if let Some(table) = table_clone {
                table
            } else {
                error!("Table not found");
                let mut result: [u8; 1] = [1u8; 1];
                result[0] = 3;
                let mut result = result.to_vec();
                let error_message = "Table not found";
                result.extend_from_slice(error_message.as_bytes());
                return result;
            };

            schema_fields = &table.schema.fields;

            let schema_sv = schema_fields
                .iter()
                .cloned()
                .collect::<smallvec::SmallVec<[crate::core::row::schema::SchemaField; 20]>>();

            let update_plan = if let Some(plan) = update_row_from_query_purpose(&query, &schema_sv) {
                plan
            } else {
                error!("Cannot parse UPDATE query");
                let mut result: [u8; 1] = [1u8; 1];
                result[0] = 3;
                let mut result = result.to_vec();
                let error_message = "Cannot parse UPDATE query";
                result.extend_from_slice(error_message.as_bytes());
                return result;
            };

            match table
                .update_rows(
                    &update_plan.where_clause,
                    update_plan.query_row_fetch,
                    update_plan.updates,
                    None,
                )
                .await
            {
                Ok(n) => {
                    info!("Updated {} rows", n);
                    // Keep status=0 for backward compatibility, but also include rows-affected.
                    let mut result = vec![0u8];
                    result.extend_from_slice(&n.to_le_bytes());
                    return result;
                }
                Err(e) => {
                    error!("Failed to update rows: {}", e);
                    let mut result: [u8; 1] = [1u8; 1];
                    result[0] = 3;
                    let mut result = result.to_vec();
                    let error_message = "Error occurred";
                    result.extend_from_slice(error_message.as_bytes());
                    return result;
                }
            }
        }
        QueryPurpose::DeleteRow(_) => {
            // Parse DELETE ... using schema
            let table_name = match &query {
                QueryPurpose::DeleteRow(sql) => sql
                    .split_whitespace()
                    .nth(2)
                    .unwrap_or("")
                    .trim_matches('"')
                    .to_string(),
                _ => String::new(),
            };

            if table_name.is_empty() {
                error!("DELETE missing table name");
                let mut result: [u8; 1] = [1u8; 1];
                result[0] = 3;
                let mut result = result.to_vec();
                let error_message = "DELETE missing table name";
                result.extend_from_slice(error_message.as_bytes());
                return result;
            }

            let table_ref = database.tables.get(&table_name);
            let table_clone = table_ref.as_ref().clone();
            let table = if let Some(table) = table_clone {
                table
            } else {
                error!("Table not found");
                let mut result: [u8; 1] = [1u8; 1];
                result[0] = 3;
                let mut result = result.to_vec();
                let error_message = "Table not found";
                result.extend_from_slice(error_message.as_bytes());
                return result;
            };

            let schema_sv = table
                .schema
                .fields
                .iter()
                .cloned()
                .collect::<smallvec::SmallVec<[crate::core::row::schema::SchemaField; 20]>>();

            let delete_plan = if let Some(plan) = delete_row_from_query_purpose(&query, &schema_sv) {
                plan
            } else {
                error!("Cannot parse DELETE query");
                let mut result: [u8; 1] = [1u8; 1];
                result[0] = 3;
                let mut result = result.to_vec();
                let error_message = "Cannot parse DELETE query";
                result.extend_from_slice(error_message.as_bytes());
                return result;
            };

            match table
                .delete_rows(&delete_plan.where_clause, delete_plan.query_row_fetch, None)
                .await
            {
                Ok(n) => {
                    info!("Deleted {} rows", n);
                    // Keep status=0 for backward compatibility, but also include rows-affected.
                    let mut result = vec![0u8];
                    result.extend_from_slice(&n.to_le_bytes());
                    return result;
                }
                Err(e) => {
                    error!("Failed to delete rows: {}", e);
                    let mut result: [u8; 1] = [1u8; 1];
                    result[0] = 3;
                    let mut result = result.to_vec();
                    let error_message = "Error occurred";
                    result.extend_from_slice(error_message.as_bytes());
                    return result;
                }
            }
        }
        QueryPurpose::QueryRows(query_rows) => {
            // Handle query rows
            let table_name = &query_rows.table_name;
            debug!("Query rows from table: {}", table_name);
            let table_ref = database.tables.get(table_name);
            let table_clone = table_ref.as_ref().clone();
            let table = if let Some(table) = table_clone {
                table
            } else {
                error!("Table not found");
                let mut result: [u8; 1] = [1u8; 1];
                result[0] = 3;
                let mut result = result.to_vec();
                let error_message = "Table not found";
                let mut error_message_bytes = Vec::with_capacity(error_message.len());
                error_message_bytes.extend_from_slice(error_message.as_bytes());
                result.extend_from_slice(&mut error_message_bytes);
                return result.to_vec();
            };
            let schema = &table.schema;
            // Handle insert row
            if let Ok(query_row) = row_fetch_from_select_query(&query, &schema.fields) {
                if let Ok(rows) = table
                    .query_row(
                        &query_row.where_clause,
                        &schema.fields.to_vec(),
                        query_row.query_row_fetch,
                        query_row.requested_row_fetch,
                        query_row.limit,
                        query_row.order_by,
                    )
                    .await
                {
                    info!("Query executed successfully, returning {} rows", rows.len());
                    let mut result: [u8; 1] = [2u8; 1];
                    let mut final_vec = Vec::with_capacity(1 + rows.len());
                    let mut rows_vec = rows_into_vec(rows);
                    final_vec.extend_from_slice(&mut result);
                    final_vec.append(&mut rows_vec);
                    return final_vec;
                } else {
                    error!("Failed to execute query");
                    let mut result: [u8; 1] = [1u8; 1];
                    result[0] = 3;
                    let mut result = result.to_vec();
                    let error_message = "Error occurred";
                    let mut error_message_bytes = Vec::with_capacity(error_message.len());
                    error_message_bytes.extend_from_slice(error_message.as_bytes());
                    result.extend_from_slice(&mut error_message_bytes);
                    return result.to_vec();
                }
            } else {
                error!("Cannot query rows due to schema mismatch");
                let mut result: [u8; 1] = [1u8; 1];
                result[0] = 3;
                let mut result = result.to_vec();
                let error_message = "Cannot query rows due to schema mismatch";
                let mut error_message_bytes = Vec::with_capacity(error_message.len());
                error_message_bytes.extend_from_slice(error_message.as_bytes());
                result.extend_from_slice(&mut error_message_bytes);
                return result.to_vec();
            }
        }
    }
}
