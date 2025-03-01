use std::{io, sync::Arc};

use ahash::RandomState;
use dashmap::DashMap;
use rastdp::receiver::Receiver;
use tokio::sync::RwLock;

use crate::{
    rql::{self, parser::ParserResult},
    SERVER_PORT,
};

use super::{
    column::Column, row::InsertOrUpdateRow, storage_providers::traits::IOOperationsSync,
    table::Table,
};

static RECEIVER: async_lazy::Lazy<Arc<Receiver>> = async_lazy::Lazy::new(|| {
    Box::pin(async {
        let receiver = Receiver::new(&format!("127.0.0.1:{}", SERVER_PORT))
            .await
            .unwrap();
        Arc::new(receiver)
    })
});

pub struct Database<S: IOOperationsSync + Send + Sync + 'static> {
    config_table: Table<S>,
    tables: Arc<DashMap<String, Table<S>, RandomState>>,
}

unsafe impl<S: IOOperationsSync> Send for Database<S> {}
unsafe impl<S: IOOperationsSync> Sync for Database<S> {}

impl<S: IOOperationsSync + Send + Sync> Database<S> {
    // pub async fn new(io_sync: S) -> io::Result<Database<S>> {
    //     let io_sync = io_sync.create_new("CONFIG_TABLE.db".to_string()).await;
    //     let mut config_table = Table::init(io_sync.clone(), false, false).await?;

    //     let all_rows_result = rql::parser::parse_rql(
    //         r#"
    //         BEGIN
    //         SELECT FROM CONFIG_TABLE
    //         END
    //     "#,
    //     )
    //     .unwrap();

    //     // let table_rows = config_table
    //     //     .execute_query(all_rows_result.parser_result)
    //     //     .await?;

    //     // let tables: DashMap<String, Table<S>, RandomState> = if table_rows.is_some() {
    //     //     let rows = table_rows.unwrap();

    //     //     let mut table_specs: Vec<(String, bool, bool)> = Vec::default();

    //     //     for row in rows {
    //     //         let columns = Column::from_buffer(&row.columns_data)?;
    //     //         let name = columns[0].into_value();
    //     //         let compress: bool = columns[1].into_value().parse().unwrap();
    //     //         let immutable: bool = columns[2].into_value().parse().unwrap();
    //     //         table_specs.push((name, compress, immutable));
    //     //     }

    //     //     let inited_tables: DashMap<String, Table<S>, RandomState> = DashMap::default();

    //     //     for table_spec in table_specs {
    //     //         let new_io_sync = io_sync.create_new(table_spec.0.clone()).await;
    //     //         let table = Table::<S>::init_inner(new_io_sync, table_spec.1, table_spec.2).await?;
    //     //         inited_tables.insert(table_spec.0, table);
    //     //     }

    //     //     inited_tables
    //     // } else {
    //     //     DashMap::default()
    //     // };

    //     // Ok(Database {
    //     //     config_table: config_table,
    //     //     tables: Arc::new(tables), //server: receiver
    //     // })
    // }

    pub async fn create_table(
        &mut self,
        name: String,
        compress: bool,
        immutable: bool,
    ) -> io::Result<()> {
        let new_io_sync = self.config_table.io_sync.create_new(name.clone()).await;
        let table = Table::<S>::init_inner(new_io_sync, compress, immutable).await?;

        self.tables.insert(name.clone(), table);

        let name_column = Column::new(name).unwrap();
        let compress_column = Column::new(compress).unwrap();
        let immutable_column = Column::new(immutable).unwrap();

        let mut columns_buffer_update: Vec<u8> =
            Vec::with_capacity(name_column.len() + compress_column.len() + immutable_column.len());

        columns_buffer_update.append(&mut name_column.content.to_vec());
        columns_buffer_update.append(&mut compress_column.content.to_vec());
        columns_buffer_update.append(&mut immutable_column.content.to_vec());

        self.config_table
            .insert_row(InsertOrUpdateRow {
                columns_data: columns_buffer_update,
            })
            .await;

        Ok(())
    }

    pub async fn drop_table(&mut self, name: String) -> io::Result<()> {
        let mut table = self.tables.try_get_mut(&name);

        while table.is_locked() {
            table = self.tables.try_get_mut(&name);
        }

        let mut table = table.unwrap();

        table.io_sync.drop_io();

        drop(table);

        _ = self.tables.remove(&name).unwrap();

        //let row = self.config_table.first_or_default_by_column(0, name).await?.unwrap();
        //self.config_table.delete_row_by_id(row.id).await?;

        Ok(())
    }

    pub async fn start_async(database: Arc<RwLock<Database<S>>>) -> io::Result<()> {
        let db = database.clone();
        let receiver = RECEIVER.force().await;

        let future = receiver.start_processing_function_result_object(
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
    let database_operation =
        rql::parser::parse_rql(&String::from_utf8_lossy(&request_vec)).unwrap();

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
        ParserResult::QueryEvaluationTokens(tokens) => todo!(),
        ParserResult::CachedHashIndexes(indexes) => todo!(),
    }
}

#[repr(u8)]
pub enum QueryExecutionResult {
    Ok = 0,
    RowsAffected(u64) = 1,
    RowsResult(Box<Vec<u8>>) = 2,
    Error(String) = 3,
}
