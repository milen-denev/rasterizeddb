use std::{io, sync::Arc};

use ahash::RandomState;
use dashmap::DashMap;
use log::info;
use rastcp::server::TcpServer;
use tokio::sync::RwLock;

use crate::{
    rql::{self, parser::ParserResult},
    SERVER_PORT,
};

use super::{
    column::Column, row::{self, InsertOrUpdateRow, Row}, storage_providers::traits::IOOperationsSync,
    table::Table,
};

static TCP_SERVER: async_lazy::Lazy<Arc<TcpServer>> = async_lazy::Lazy::new(|| {
    Box::pin(async {
        let server = TcpServer::new(&format!("127.0.0.1:{}", SERVER_PORT)).await.unwrap();
        Arc::new(server)
    })
});

pub struct Database<S: IOOperationsSync + Send + Sync + 'static> {
    config_table: Table<S>,
    tables: Arc<DashMap<String, Table<S>, RandomState>>,
}

unsafe impl<S: IOOperationsSync> Send for Database<S> {}
unsafe impl<S: IOOperationsSync> Sync for Database<S> {}

impl<S: IOOperationsSync + Send + Sync> Database<S> {
    pub async fn new(io_sync: S) -> io::Result<Database<S>> {
        let config_io_sync = io_sync.create_new("CONFIG_TABLE.db".to_string()).await;
        let config_table = Table::init(config_io_sync.clone(), false, false).await?;

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
                let table = Table::<S>::init_inner(new_io_sync, table_spec.1, table_spec.2).await?;
                inited_tables.insert(table_name, table);
            }

            inited_tables
        } else {
            DashMap::default()
        };

        Ok(Database {
            config_table: config_table,
            tables: Arc::new(tables),
        })
    }

    pub async fn create_table(
        &mut self,
        name: String,
        compress: bool,
        immutable: bool,
    ) -> io::Result<()> {
        // Optimize table creation by reserving memory upfront
        if self.tables.contains_key(&name) {
            return Err(io::Error::new(io::ErrorKind::AlreadyExists, "Table already exists"));
        }

        // Create columns buffer with exact capacity to avoid reallocations
        let name_column = Column::new(name.clone())?;
        let compress_column = Column::new(compress)?;
        let immutable_column = Column::new(immutable)?;
        
        let total_capacity = name_column.len() + compress_column.len() + immutable_column.len();
        let mut columns_buffer: Vec<u8> = Vec::with_capacity(total_capacity);
        
        // Use extend_from_slice instead of append where possible
        columns_buffer.extend_from_slice(&name_column.content.as_slice());
        columns_buffer.extend_from_slice(&compress_column.content.as_slice());
        columns_buffer.extend_from_slice(&immutable_column.content.as_slice());

        // Create the new table file and initialize it in one transaction
        let new_io_sync = self.config_table.io_sync.create_new(format!("{}.db", name)).await;
        let table = Table::<S>::init_inner(new_io_sync, compress, immutable).await?;
        
        // Insert the table descriptor and update in-memory collection in one operation
        self.tables.insert(name, table);
        
        // Insert config record
        self.config_table
            .insert_row(InsertOrUpdateRow {
                columns_data: columns_buffer,
            })
            .await;

        Ok(())
    }

    pub async fn drop_table(&mut self, name: String) -> io::Result<()> {
        // Use optimization: remove from hashmap first to prevent new queries, then clean up resources
        let mut table = match self.tables.remove(&name) {
            Some((_, table)) => table,
            None => return Err(io::Error::new(io::ErrorKind::NotFound, "Table not found")),
        };

        // Drop IO resources
        table.io_sync.drop_io();

        // Find and delete config entry efficiently
        let select_query = format!(
            r#"
                BEGIN
                SELECT FROM CONFIG_TABLE
                WHERE name = {name}
                END
            "#,
        );
        
        let select_table_to_drop = rql::parser::parse_rql(&select_query).unwrap();

        let table_row_result = self.config_table
            .execute_query(select_table_to_drop.parser_result)
            .await?;
            
        if let Some(mut rows) = table_row_result {
            if let Some(row) = rows.pop() {
                // Delete the config record
                self.config_table.delete_row_by_id(row.id).await?;
                
                // Schedule vacuum as a background task to avoid blocking caller
                let mut config_table_clone = self.config_table.clone();
                tokio::spawn(async move {
                    config_table_clone.vacuum_table().await;
                });
            }
        }

        Ok(())
    }

    pub async fn execute_cached_query(&self, name: String, parser_cached_result: ParserResult) -> io::Result<Vec<u8>> {
        // Use the same pattern as execute_query for consistency
        match self.tables.get(&name) {
            Some(table) => {
                let rows: Option<Vec<Row>> = table.execute_query(parser_cached_result).await?;
                Ok(row::Row::serialize_rows(rows))
            },
            None => Err(io::Error::new(io::ErrorKind::NotFound, format!("Table not found: {}", name)))
        }
    }

    pub async fn execute_query(&self, name: String, parser_cached_result: ParserResult) -> io::Result<Vec<u8>> {
        // Replace busy waiting with a more efficient approach using dashmap's entry API
        match self.tables.get(&name) {
            Some(table) => {
                let rows: Option<Vec<Row>> = table.execute_query(parser_cached_result).await?;
                Ok(row::Row::serialize_rows(rows))
            },
            None => Err(io::Error::new(io::ErrorKind::NotFound, format!("Table not found: {}", name)))
        }
    }

    pub async fn start_async(database: Arc<RwLock<Database<S>>>) -> io::Result<()> {
        let db = database.clone();
        let receiver = TCP_SERVER.force().await;

        let future = receiver.run_with_context(
            db,
            process_incoming_queries
        );

        _ = tokio::spawn(future).await;

        Ok(())
    }
}

#[allow(unused_variables)]
pub(crate) async fn process_incoming_queries<S: IOOperationsSync>(
    database: Arc<RwLock<Database<S>>>,
    request_vec: Vec<u8>,
) -> Vec<u8> {
    // Optimize: avoid converting to string if we only need to parse it 
    // (Future optimization: implement zero-copy parsing directly from bytes)
    let query = String::from_utf8_lossy(&request_vec);
    
    info!("Received query: {}", query);

    let database_operation = match rql::parser::parse_rql(&query) {
        Ok(op) => op,
        Err(e) => {
            let mut result = Vec::with_capacity(1 + e.len());
            result.push(3); // Error code
            result.extend_from_slice(e.as_bytes());
            return result;
        }
    };

    // Reuse buffers for common result types
    let create_ok_result = [0u8; 1];
    let rows_result_prefix = [2u8; 1];

    match database_operation.parser_result {
        ParserResult::CreateTable((name, compress, immutable)) => {
            let mut db = database.write().await;
            match db.create_table(name, compress, immutable).await {
                Ok(_) => create_ok_result.to_vec(),
                Err(e) => {
                    let mut result = Vec::with_capacity(1 + e.to_string().len());
                    result.push(3); // Error code
                    result.extend_from_slice(e.to_string().as_bytes());
                    result
                }
            }
        }
        ParserResult::DropTable(name) => {
            let mut db = database.write().await;
            match db.drop_table(name).await {
                Ok(_) => create_ok_result.to_vec(),
                Err(e) => {
                    let mut result = Vec::with_capacity(1 + e.to_string().len());
                    result.push(3); // Error code
                    result.extend_from_slice(e.to_string().as_bytes());
                    result
                }
            }
        }
        ParserResult::QueryEvaluationTokens(tokens) => {
            // Optimize: Use read lock instead of write lock for queries
            let db = database.read().await;
            
            match db.execute_query(database_operation.table_name, ParserResult::QueryEvaluationTokens(tokens)).await {
                Ok(mut rows_result) => {
                    // Pre-allocate result vector with exact size
                    let mut final_vec = Vec::with_capacity(1 + rows_result.len());
                    final_vec.extend_from_slice(&rows_result_prefix);
                    final_vec.append(&mut rows_result);
                    final_vec
                },
                Err(e) => {
                    let mut result = Vec::with_capacity(1 + e.to_string().len());
                    result.push(3); // Error code
                    result.extend_from_slice(e.to_string().as_bytes());
                    result
                }
            }
        },
        ParserResult::CachedHashIndexes(indexes) => {
            let db = database.read().await;
            
            match db.execute_cached_query(database_operation.table_name, 
                                        ParserResult::CachedHashIndexes(indexes)).await {
                Ok(mut rows_result) => {
                    // Pre-allocate result vector with exact size
                    let mut final_vec = Vec::with_capacity(1 + rows_result.len());
                    final_vec.extend_from_slice(&rows_result_prefix);
                    final_vec.append(&mut rows_result);
                    final_vec
                },
                Err(e) => {
                    let mut result = Vec::with_capacity(1 + e.to_string().len());
                    result.push(3); // Error code
                    result.extend_from_slice(e.to_string().as_bytes());
                    result
                }
            }
        },
        ParserResult::RebuildIndexes(name) => {
            // Use entry API to safely get and modify the table
            let db = database.read().await;
            
            let mut table = db.tables.try_get_mut(&name);

            if table.is_absent() {
                let error_msg = format!("Table not found: {}", name);
                let mut result = Vec::with_capacity(1 + error_msg.len());
                result.push(3); // Error code
                result.extend_from_slice(error_msg.as_bytes());
                return result;
            }

            loop {
                if table.is_locked() {
                    table = db.tables.try_get_mut(&name);
                } else {
                    break;
                }
            }

            let table = table.unwrap();

            // Optimize index rebuilding as a background task
            let mut table_clone = table.clone();
            tokio::spawn(async move {
                table_clone.rebuild_in_memory_indexes().await;
            });
            create_ok_result.to_vec()
        },
        ParserResult::InsertEvaluationTokens(tokens) => todo!(),
        ParserResult::UpdateEvaluationTokens(tokens) => todo!(),
        ParserResult::DeleteEvaluationTokens(tokens) => todo!(),
    }
}

#[repr(u8)]
pub enum QueryExecutionResult {
    Ok = 0,
    RowsAffected(u64) = 1,
    RowsResult(Box<Vec<u8>>) = 2,
    Error(String) = 3,
}
