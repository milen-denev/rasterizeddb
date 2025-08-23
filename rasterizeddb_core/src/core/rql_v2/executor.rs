use std::sync::Arc;

use log::{debug, error, info};

use crate::core::{database_v2::Database, row_v2::row::rows_into_vec, rql_v2::{lexer_ct::create_table_from_query_purpose, lexer_ir::insert_row_from_query_purpose, lexer_query::row_fetch_from_select_query, lexer_s1::QueryPurpose}};

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
                    let mut result: [u8; 1] = [0u8; 1];
                    result[0] = 0;
                    return result.to_vec();
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
            // Handle update row
            todo!("Handle update row");
        }
        QueryPurpose::DeleteRow(_) => {
            // Handle delete row
            todo!("Handle delete row");
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
                if let Ok(rows) = table.query_row(&query_row.where_clause, &schema.fields.to_vec(), query_row.query_row_fetch, query_row.requested_row_fetch).await {
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