use log::{error, info};
use std::io::Result;
use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64},
    },
};

use super::schema::TableSchema;
use crate::core::processor::concurrent_processor::ConcurrentProcessor;
use crate::core::rql::lexer_ct::CreateColumnData;
use crate::core::{
    row::{
        row::{Row, RowFetch, RowWrite},
        row_pointer::{RowPointer, RowPointerIterator},
        schema::SchemaField,
    },
    storage_providers::traits::StorageIO,
};

pub struct Table<S: StorageIO> {
    pub schema: TableSchema,
    pub io_pointers: Arc<S>,
    pub io_rows: Arc<S>,
    pub io_schema: Arc<S>,
    pub hard_locked: AtomicBool,
    pub len: AtomicU64,
    pub last_row_id: AtomicU64,
    pub concurrent_processor: ConcurrentProcessor,
}

unsafe impl<S: StorageIO> Send for Table<S> {}
unsafe impl<S: StorageIO> Sync for Table<S> {}

impl<S: StorageIO> Table<S> {
    pub async fn new(table_name: &str, initial_io: Arc<S>, columns: Vec<CreateColumnData>) -> Self {
        let io_pointers = Arc::new(
            initial_io
                .create_new(format!("{}_pointers.db", table_name))
                .await,
        );
        let io_rows = Arc::new(initial_io.create_new(format!("{}.db", table_name)).await);
        let io_schema = Arc::new(
            initial_io
                .create_new(format!("{}_schema.db", table_name))
                .await,
        );

        let io_pointers_clone = io_pointers.clone();
        let io_rows_clone = io_rows.clone();
        let io_schema_clone = io_schema.clone();

        tokio::spawn(async move {
            let clone_io_rows = io_rows_clone.clone();
            clone_io_rows.start_service().await
        });
        tokio::spawn(async move {
            let clone_io_pointers = io_pointers_clone.clone();
            clone_io_pointers.start_service().await
        });
        tokio::spawn(async move {
            let clone_io_schema = io_schema_clone.clone();
            clone_io_schema.start_service().await
        });

        let schema = {
            if let Ok(schema) = TableSchema::load(io_schema.clone()).await {
                schema
            } else {
                let mut schema = TableSchema::new(table_name.to_string(), false);

                schema
                    .save(io_schema.clone())
                    .await
                    .expect("Failed to save initial schema");

                for column in columns {
                    //TODO is_unique
                    schema
                        .add_field(io_schema.clone(), column.name, column.data_type, false)
                        .await;
                }

                schema
            }
        };

        info!("Loaded schema: {:?}", schema);

        let mut pointer_iterator = RowPointerIterator::new(io_pointers.clone()).await.unwrap();

        let (atomic_last_id, atomic_table_length) =
            if let Some(row_pointer) = pointer_iterator.read_last().await {
                let last_id = row_pointer.id;
                let atomic_last_id = AtomicU64::new(last_id);

                let table_length = row_pointer.position;
                let atomic_table_length = AtomicU64::new(table_length);

                (atomic_last_id, atomic_table_length)
            } else {
                let atomic_last_id = AtomicU64::new(0);
                let atomic_table_length = AtomicU64::new(0);
                (atomic_last_id, atomic_table_length)
            };

        Self {
            schema,
            io_pointers,
            io_rows,
            io_schema,
            hard_locked: AtomicBool::new(false),
            len: atomic_table_length,
            last_row_id: atomic_last_id,
            concurrent_processor: ConcurrentProcessor::new(),
        }
    }

    pub async fn insert_row(&self, row_write: RowWrite) -> Result<()> {
        // Implementation for inserting a row into the table

        info!(
            "Inserting row with {} columns",
            row_write.columns_writing_data.len()
        );

        let result = RowPointer::write_row(
            self.io_pointers.clone(),
            self.io_rows.clone(),
            &self.last_row_id,
            &self.len,
            #[cfg(feature = "enable_long_row")]
            cluster,
            &row_write,
        )
        .await;

        if let Err(e) = result {
            error!("Failed to insert row: {}", e);
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("Failed to insert row: {}", e),
            ));
        } else if let Ok(row_pointer) = result {
            info!("Successfully inserted row with pointer: {:?}", row_pointer);
        }

        return Ok(());
    }

    pub async fn query_row(
        &self,
        query: &str,
        schema_fields: &Vec<SchemaField>,
        query_row_fetch: RowFetch,
        requested_row_fetch: RowFetch,
    ) -> Result<Vec<Row>> {
        // Implementation for querying a row from the table

        info!("Querying row");

        let mut iterator = RowPointerIterator::new(self.io_pointers.clone())
            .await
            .unwrap();

        let rows = self
            .concurrent_processor
            .process(
                query,
                query_row_fetch,
                requested_row_fetch,
                schema_fields,
                self.io_rows.clone(),
                &mut iterator,
            )
            .await;

        info!("Query executed, returning {} rows", rows.len());

        return Ok(rows);
    }
}
