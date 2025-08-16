use std::{io, sync::Arc, usize, vec};

use ahash::RandomState;
use dashmap::{DashMap, Map};
use log::info;
use rastcp::server::{TcpServer, TcpServerBuilder};
use tokio::sync::RwLock;

use crate::{
    rql::{self, parser::{InsertEvaluationResult, ParserResult}},
    SERVER_PORT,
};

use super::{
    column::Column, row::{self, InsertOrUpdateRow}, storage_providers::traits::StorageIO, support_types::ReturnResult, table::Table
};

static TCP_SERVER: async_lazy::Lazy<Arc<TcpServer>> = async_lazy::Lazy::new(|| {
    Box::pin(async {
        let server = TcpServerBuilder::new("127.0.0.1", SERVER_PORT)
            .max_connections(usize::MAX)
            .build()
            .await
            .unwrap();

        Arc::new(server)
    })
});

pub struct Database<S: StorageIO + Send + Sync + 'static> {
    config_table: RwLock<Table<S>>,
    tables: Arc<DashMap<String, Table<S>, RandomState>>,
}

unsafe impl<S: StorageIO> Send for Database<S> {}
unsafe impl<S: StorageIO> Sync for Database<S> {}

impl<S: StorageIO + Send + Sync> Database<S> {
    pub async fn new(io_sync: S) -> io::Result<Database<S>> {
        let config_io_sync = io_sync.create_new("CONFIG_TABLE.db".to_string()).await;
        let config_table = Table::init("CONFIG_TABLE".into(), config_io_sync.clone(), false, false).await?;

        let all_rows_result = rql::parser::parse_rql(
            r#"
            BEGIN
            SELECT FROM CONFIG_TABLE
            END
        "#,
        )
        .unwrap();

        let table_rows = config_table
            .execute_query(all_rows_result.parser_result)
            .await?;

        let tables: DashMap<String, Table<S>, RandomState> = if table_rows.is_some() {
            let rows = table_rows.unwrap();
            let mut table_specs: Vec<(String, bool, bool)> = Vec::default();

            let rows = match rows {
                ReturnResult::Rows(rows) => rows,
                ReturnResult::HtmlView(_) => {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid data type returned from query."));
                }
            };

            for row in rows {
                let columns = Column::from_buffer(&row.columns_data)?;
                let name = columns[0].into_value();
                info!("Table name found: {}", name);
                let compress: bool = columns[1].content.as_slice()[0] == 1;
                let immutable: bool = columns[2].content.as_slice()[0] == 1;
                table_specs.push((name, compress, immutable));
            }

            let inited_tables: DashMap<String, Table<S>, RandomState> = DashMap::default();

            for table_spec in table_specs {
                let table_name = table_spec.0.trim().replace("\0", "").to_string();
                let file_name = format!("{}.db", table_name);
                let new_io_sync = io_sync.create_new(file_name).await;
                let table = Table::<S>::init_inner(table_name.clone(), new_io_sync, table_spec.1, table_spec.2).await?;
                inited_tables.insert(table_name, table);
            }

            inited_tables
        } else {
            DashMap::default()
        };

        Ok(Database {
            config_table: RwLock::new(config_table),
            tables: Arc::new(tables),
        })
    }

    pub async fn create_table(
        &self,
        name: String,
        compress: bool,
        immutable: bool,
    ) -> io::Result<()> {
        if self.tables.contains_key(&name) {
            return Err(io::Error::new(io::ErrorKind::AlreadyExists, "Table already exists"));
        }

        let mut config_table = self.config_table.write().await;

        let new_io_sync = config_table.io_sync.create_new(format!("{}.db", name)).await;
        let table = Table::<S>::init_inner(name.clone(), new_io_sync, compress, immutable).await?;

        let name_column = Column::new(name.clone()).unwrap();
        let compress_column = Column::new(compress).unwrap();
        let immutable_column = Column::new(immutable).unwrap();

        let mut columns_buffer_update: Vec<u8> =
            Vec::with_capacity(name_column.len() + compress_column.len() + immutable_column.len());

        columns_buffer_update.append(&mut name_column.content.to_vec());
        columns_buffer_update.append(&mut compress_column.content.to_vec());
        columns_buffer_update.append(&mut immutable_column.content.to_vec());

        self.tables.insert(name, table);

        config_table
            .insert_row(InsertOrUpdateRow {
                columns_data: columns_buffer_update,
            })
            .await;

        Ok(())
    }

    pub async fn drop_table(&self, name: String) -> io::Result<()> {        
        let mut config_table = self.config_table.write().await;

        let mut table = self.tables.try_get_mut(&name);

        while table.is_locked() || table.is_absent() {
            table = self.tables.try_get_mut(&name);
        }

        let table = table.unwrap();

        table.io_sync.drop_io();

        drop(table);

        _ = self.tables.remove(&name).unwrap();

        let select_table_to_drop = rql::parser::parse_rql(&format!(
        r#"
            BEGIN
            SELECT FROM CONFIG_TABLE
            WHERE name = {name}
            END
        "#,
        ))
        .unwrap();

        let table_row = config_table
            .execute_query(select_table_to_drop.parser_result)
            .await?;

        match table_row {
            Some(ReturnResult::Rows(rows)) => {
                for row in rows {
                    let columns = Column::from_buffer(&row.columns_data)?;
                    let table_name = columns[0].into_value();
                    if table_name == name {
                        let row_id = row.id;
                        config_table.delete_row_by_id(row_id).await?;
                        config_table.vacuum_table().await;
                        break;
                    }
                }
            },
            Some(ReturnResult::HtmlView(_)) => {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid data type returned from query."));
            },
            None => {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid data type returned from query."));
            }
        }

        Ok(())
    }

    pub async fn execute_cached_query(&self, name: String, parser_cached_result: ParserResult) -> io::Result<Vec<u8>> {
        let mut table = self.tables.try_get(&name);

        while table.is_locked() || table.is_absent() {
            table = self.tables.try_get(&name);
        }

        let table = table.unwrap();
        let rows: Option<ReturnResult> = table.execute_query(parser_cached_result).await.unwrap();

        Ok(row::Row::serialize_rows(rows))
    }

    pub async fn execute_query(&self, name: String, parser_result: ParserResult) -> io::Result<Vec<u8>> {
        let mut table = self.tables.try_get(&name);

        while table.is_locked() || table.is_absent() {
            table = self.tables.try_get(&name);
        }

        let table = table.unwrap();

        let rows: Option<ReturnResult> = table.execute_query(parser_result).await.unwrap();

        Ok(row::Row::serialize_rows(rows))
    }

    pub async fn execute_insert_query(&self, name: String, insert_evaluation_result: InsertEvaluationResult) -> std::result::Result<u64, String> {
        let mut table = self.tables._try_get_mut(&name);

        while table.is_locked() || table.is_absent() {
            table = self.tables.try_get_mut(&name);
        }

        let mut table = table.unwrap();

        let mut rows_affected = 0;

        for row in insert_evaluation_result.rows {
            table.insert_row(row).await;
            rows_affected += 1;
        }

        Ok(rows_affected)
    }

    pub async fn execute_delete_query(&self, name: String, delete_evaluation_tokens: ParserResult) -> io::Result<()> {
        let mut table = self.tables.try_get_mut(&name);

        while table.is_locked() || table.is_absent() {
            table = self.tables.try_get_mut(&name);
        }

        let mut table = table.unwrap();
        
        // Find rows matching the delete criteria
        let rows = table.execute_query(delete_evaluation_tokens).await?;
        
        if let Some(rows) = rows {
            match rows {
                ReturnResult::Rows(rows) => {
                    for row in rows {
                        table.delete_row_by_id(row.id).await?;
                    }
                },
                ReturnResult::HtmlView(_) => {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid data type returned from query."));
                }
            }
        }
        
        Ok(())
    }

    pub async fn start_async(database: Arc<Database<S>>) -> io::Result<()> {
        let db = database.clone();
        let receiver = TCP_SERVER.force().await;

        let fut = async move {
            let receiver_clone = receiver.clone();
            _ = receiver_clone.run_with_context(
                db,
                process_incoming_queries
            ).await;
        };
        
        _ = tokio::spawn(fut).await;

        Ok(())
    }
}

#[allow(unused_variables)]
pub(crate) async fn process_incoming_queries<S: StorageIO>(
    database: Arc<Database<S>>,
    request_vec: Vec<u8>,
) -> Vec<u8> {
    let query = String::from_utf8_lossy(&request_vec);
    
    info!("Received query: {}", query);

    let database_operation = rql::parser::parse_rql(&query).unwrap();

    match database_operation.parser_result {
        ParserResult::CreateTable((name, compress, immutable)) => {
            if let Ok(_) = database.create_table(name, compress, immutable).await {
                let mut result: [u8; 1] = [0u8; 1];
                result[0] = 0;
                return result.to_vec();
            } else {
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
        ParserResult::DropTable(name) => {
            database.drop_table(name).await.unwrap();
            let mut result: [u8; 1] = [0u8; 1];
            result[0] = 0;
            return result.to_vec();
        }
        ParserResult::InsertEvaluationTokens(insert_evaluation_result) => {
            let rows_affected = database.execute_insert_query(database_operation.table_name, insert_evaluation_result).await.unwrap();
            let mut result = vec![0u8; 1];
            result[0] = 1;
            result.extend_from_slice(&mut rows_affected.to_le_bytes());
            return result.to_vec();
        },
        ParserResult::UpdateEvaluationTokens(tokens) => todo!(),
        ParserResult::DeleteEvaluationTokens(tokens) => {
            database.execute_delete_query(database_operation.table_name, ParserResult::QueryEvaluationTokens(tokens)).await.unwrap();
            let mut result: [u8; 1] = [0u8; 1];
            result[0] = 0;
            return result.to_vec();
        },
        ParserResult::QueryEvaluationTokens(tokens) => {
            let mut rows_result = database.execute_query(database_operation.table_name, ParserResult::QueryEvaluationTokens(tokens)).await.unwrap();
            let mut result: [u8; 1] = [2u8; 1];
            let mut final_vec = Vec::with_capacity(1 + rows_result.len());

            final_vec.extend_from_slice(&mut result);
            final_vec.append(&mut rows_result);

            return final_vec;
        },
        ParserResult::CachedHashIndexes(indexes) => {
            let mut rows_result = database.execute_cached_query(database_operation.table_name, ParserResult::CachedHashIndexes(indexes)).await.unwrap();
            let mut result: [u8; 1] = [2u8; 1];
            let mut final_vec = Vec::with_capacity(1 + rows_result.len());

            final_vec.extend_from_slice(&mut result);
            final_vec.append(&mut rows_result);

            return final_vec;
        },
        ParserResult::RebuildIndexes(name) => {
            let mut table = database.tables.try_get_mut(&name);

            while table.is_locked() || table.is_absent() {
                table = database.tables.try_get_mut(&name);
            }

            let mut table = table.unwrap();
            table.rebuild_in_memory_indexes().await;
            let mut result: [u8; 1] = [0u8; 1];
            result[0] = 0;
            return result.to_vec();
        }
    }
}

#[derive(Debug)]
#[repr(u8)]
pub enum QueryExecutionResult {
    Ok = 0,
    RowsAffected(u64) = 1,
    RowsResult(Box<Vec<u8>>) = 2,
    Error(String) = 3,
}
