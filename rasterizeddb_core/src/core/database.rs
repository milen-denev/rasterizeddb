use std::io;

use ahash::RandomState;
use dashmap::DashMap;
use rastdp::receiver::Receiver;

use crate::rql::{self, parser::{DatabaseAction, ParserResult}};

use super::{column::Column, storage_providers::traits::IOOperationsSync, table::Table};

pub struct Database<S: IOOperationsSync> {
    config_table: Table<S>,
    tables: DashMap<String, Table<S>, RandomState>,
    server: Receiver
}

impl<S: IOOperationsSync> Database<S> {
    pub async fn new(io_sync: S) -> io::Result<Database<S>> {
        let mut config_table = Table::init(io_sync.clone(), false, false).await?;
        let receiver = Receiver::new("127.0.0.1:63660").await.unwrap();

        let all_rows_result = rql::parser::parse_rql(r#"
            BEGIN
            SELECT FROM CONFIG_TABLE
            END
        "#).unwrap();

        let table_rows = config_table.execute_query(all_rows_result.parser_result).await?;

        let tables: DashMap<String, Table<S>, RandomState> = if table_rows.is_some() {
            let rows = table_rows.unwrap();

            let mut table_specs: Vec<(String, bool, bool)> = Vec::default();

            for row in rows {
                let columns = Column::from_buffer(&row.columns_data)?;
                let name = columns[0].into_value();
                let compress: bool = columns[1].into_value().parse().unwrap();
                let immutable: bool = columns[2].into_value().parse().unwrap();
                table_specs.push((name, compress, immutable));
            }

            let inited_tables: DashMap<String, Table<S>, RandomState> = DashMap::default();

            for table_spec in table_specs {
                let new_io_sync = io_sync.create_new(table_spec.0.clone()).await;
                let table = Table::<S>::init_inner(new_io_sync, table_spec.1, table_spec.2).await?;
                inited_tables.insert(table_spec.0, table);
            }

            inited_tables
        } else {
            DashMap::default()
        };

        Ok(Database {
            config_table: config_table,
            tables: tables,
            server: receiver
        })
    }

    pub async fn create_table(&self, name: String, compress: bool, immutable: bool) -> io::Result<()>  {
        let new_io_sync = self.config_table.io_sync.create_new(name.clone()).await;
        let table = Table::<S>::init_inner(new_io_sync, compress, immutable).await?;
        self.tables.insert(name, table);
        Ok(())
    }

    pub async fn drop_table(&self, name: String) -> io::Result<()>  {
        let mut table = self.tables.try_get_mut(&name);

        while table.is_locked() {
            table = self.tables.try_get_mut(&name);
        }

        let mut table = table.unwrap();

        table.io_sync.drop();

        drop(table);

        _ = self.tables.remove(&name).unwrap();

        Ok(())
    }

    pub async fn start_async(&self) -> io::Result<()> {
        

        Ok(())
    }

    pub(crate) async fn process_incoming_queries(&self, request_vec: Vec<u8>) -> Vec<u8> {
        let database_operation = rql::parser::parse_rql(&String::from_utf8_lossy(&request_vec)).unwrap();
        
        match database_operation.parser_result {
            ParserResult::CreateTable((name, compress, immutable)) => { 
                self.create_table(name, compress, immutable).await.unwrap();
                let mut result: [u8; 1] = [0u8; 1];
                result[0] = 0;
                return result.to_vec();
            },
            ParserResult::DropTable(name) => {
                self.drop_table(name).await.unwrap();
                let mut result: [u8; 1] = [0u8; 1];
                result[0] = 0;
                return result.to_vec();
            },
            ParserResult::InsertEvaluationTokens(tokens) => todo!(),
            ParserResult::UpdateEvaluationTokens(tokens) => todo!(),
            ParserResult::DeleteEvaluationTokens(tokens) => todo!(),
            ParserResult::QueryEvaluationTokens(tokens) => todo!(),
            ParserResult::CachedHashIndexes(indexes) => todo!(),
        }
    }
}

#[repr(u8)]
pub enum QueryExecutionResult {
    Ok = 0,
    RowsAffected(u64) = 1,
    RowsResult(Box<Vec<u8>>) = 2
}