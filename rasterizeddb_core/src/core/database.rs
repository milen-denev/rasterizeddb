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
        if self.tables.contains_key(&name) {
            return Err(io::Error::new(io::ErrorKind::AlreadyExists, "Table already exists"));
        }

        let new_io_sync = self.config_table.io_sync.create_new(format!("{}.db", name)).await;
        let table = Table::<S>::init_inner(new_io_sync, compress, immutable).await?;

        let name_column = Column::new(name.clone()).unwrap();
        let compress_column = Column::new(compress).unwrap();
        let immutable_column = Column::new(immutable).unwrap();

        let mut columns_buffer_update: Vec<u8> =
            Vec::with_capacity(name_column.len() + compress_column.len() + immutable_column.len());

        columns_buffer_update.append(&mut name_column.content.to_vec());
        columns_buffer_update.append(&mut compress_column.content.to_vec());
        columns_buffer_update.append(&mut immutable_column.content.to_vec());

        self.tables.insert(name, table);

        self.config_table
            .insert_row(InsertOrUpdateRow {
                columns_data: columns_buffer_update,
            })
            .await;

        Ok(())
    }

    pub async fn drop_table(&mut self, name: String) -> io::Result<()> {
        let mut table = self.tables.try_get_mut(&name);

        while table.is_locked() || table.is_absent() {
            table = self.tables.try_get_mut(&name);
        }

        let mut table = table.unwrap();

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

        let table_row = self.config_table
            .execute_query(select_table_to_drop.parser_result)
            .await?;

        let row_id = table_row.unwrap().pop().unwrap().id;
        self.config_table.delete_row_by_id(row_id).await?;
        self.config_table.vacuum_table().await;

        Ok(())
    }

    pub async fn execute_cached_query(&self, name: String, parser_cached_result: ParserResult) -> io::Result<Vec<u8>> {
        let mut table = self.tables.try_get(&name);

        while table.is_locked() || table.is_absent() {
            table = self.tables.try_get(&name);
        }

        let table = table.unwrap();
        let rows: Option<Vec<Row>> = table.execute_query(parser_cached_result).await.unwrap();

        Ok(row::Row::serialize_rows(rows))
    }

    pub async fn execute_query(&self, name: String, parser_cached_result: ParserResult) -> io::Result<Vec<u8>> {
        let mut table = self.tables.try_get(&name);

        while table.is_locked() || table.is_absent() {
            table = self.tables.try_get(&name);
        }

        let table = table.unwrap();
        let rows: Option<Vec<Row>> = table.execute_query(parser_cached_result).await.unwrap();

        Ok(row::Row::serialize_rows(rows))
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
    let query = String::from_utf8_lossy(&request_vec);
    
    info!("Received query: {}", query);

    let database_operation = rql::parser::parse_rql(&query).unwrap();

    match database_operation.parser_result {
        ParserResult::CreateTable((name, compress, immutable)) => {
            let mut db = database.write().await;
            db.create_table(name, compress, immutable).await.unwrap();
            let mut result: [u8; 1] = [0u8; 1];
            result[0] = 0;
            return result.to_vec();
        }
        ParserResult::DropTable(name) => {
            let mut db = database.write().await;
            db.drop_table(name).await.unwrap();
            let mut result: [u8; 1] = [0u8; 1];
            result[0] = 0;
            return result.to_vec();
        }
        ParserResult::InsertEvaluationTokens(tokens) => todo!(),
        ParserResult::UpdateEvaluationTokens(tokens) => todo!(),
        ParserResult::DeleteEvaluationTokens(tokens) => todo!(),
        ParserResult::QueryEvaluationTokens(tokens) => {
            let db = database.read().await;
            let mut rows_result = db.execute_query(database_operation.table_name, ParserResult::QueryEvaluationTokens(tokens)).await.unwrap();
            let mut result: [u8; 1] = [2u8; 1];
            let mut final_vec = Vec::with_capacity(1 + rows_result.len());

            final_vec.extend_from_slice(&mut result);
            final_vec.append(&mut rows_result);

            return final_vec;
        },
        ParserResult::CachedHashIndexes(indexes) => {
            let db = database.read().await;
            let mut rows_result = db.execute_cached_query(database_operation.table_name, ParserResult::CachedHashIndexes(indexes)).await.unwrap();
            let mut result: [u8; 1] = [2u8; 1];
            let mut final_vec = Vec::with_capacity(1 + rows_result.len());

            final_vec.extend_from_slice(&mut result);
            final_vec.append(&mut rows_result);

            return final_vec;
        },
        ParserResult::RebuildIndexes(name) => {
            let db = database.read().await;
          
            let mut table = db.tables.try_get_mut(&name);

            while table.is_locked() || table.is_absent() {
                table = db.tables.try_get_mut(&name);
            }

            let mut table = table.unwrap();
            table.rebuild_in_memory_indexes().await;
            let mut result: [u8; 1] = [0u8; 1];
            result[0] = 0;
            return result.to_vec();
        }
    }
}

#[repr(u8)]
pub enum QueryExecutionResult {
    Ok = 0,
    RowsAffected(u64) = 1,
    RowsResult(Box<Vec<u8>>) = 2,
    Error(String) = 3,
}
