use std::{
    io,
    sync::{Arc, atomic::AtomicU64},
};

use dashmap::DashMap;
use log::{debug, error, info};
use rastcp::server::{TcpServer, TcpServerBuilder};
use smallvec::{SmallVec, smallvec};

use crate::{
    core::{
        db_type::DbType, processor::concurrent_processor::ConcurrentProcessor, row::{
            row::{ColumnFetchingData, ColumnWritePayload, RowFetch, RowWrite},
            row_pointer::{RowPointer, RowPointerIterator},
            schema::{SchemaCalculator, SchemaField, TableSchema},
            table::Table,
        }, rql::{executor::execute, lexer_ct::CreateTable, lexer_s1::recognize_query_purpose}, storage_providers::{file_sync::LocalStorageProvider, traits::StorageIO}
    }, memory_pool::MEMORY_POOL, SERVER_PORT
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

pub struct Database {
    pub tables: DashMap<String, Arc<Table<LocalStorageProvider>>>,
    pub db_io_pointers: Arc<LocalStorageProvider>,
    pub db_io_rows: Arc<LocalStorageProvider>,
    pub db_io_schema: Arc<LocalStorageProvider>,
}

impl Database {
    pub async fn new(location: &str) -> Self {
        let dir_exists = tokio::fs::metadata(location).await.is_ok();

        if !dir_exists {
            tokio::fs::create_dir_all(location).await.unwrap();
        }

        let io_pointers =
            Arc::new(LocalStorageProvider::new(location, Some("db_data_pointers.db")).await);
        let io_rows = Arc::new(LocalStorageProvider::new(location, Some("db_data_rows.db")).await);
        let io_schema =
            Arc::new(LocalStorageProvider::new(location, Some("db_data_schema.db")).await);

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

        let schema = load_db_data_schema(io_schema.clone()).await;
        let mut iterator = RowPointerIterator::new(io_pointers.clone()).await.unwrap();
        let tables_names: Vec<(String, u64)> =
            load_db_tables(&schema, &mut iterator, io_rows.clone()).await;

        debug!(
            "Loaded {} tables from database: {:?}",
            tables_names.len(),
            tables_names
        );

        let tables = DashMap::new();

        for (table_name, _) in tables_names {
            let table = Table::new(&table_name, io_pointers.clone(), vec![]).await;
            tables.insert(table_name, Arc::new(table));
        }

        Database {
            tables,
            db_io_pointers: io_pointers,
            db_io_rows: io_rows,
            db_io_schema: io_schema,
        }
    }

    pub async fn add_table(&self, create_table: CreateTable) -> io::Result<()> {
        let table_name = &create_table.table_name;
        let columns = create_table.columns;

        if self.tables.contains_key(table_name) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "Table already exists",
            ));
        }

        let name_mb = MEMORY_POOL.acquire(table_name.len());
        let name_data = name_mb.into_slice_mut();
        name_data.copy_from_slice(table_name.as_bytes());

        let clusters_mb = MEMORY_POOL.acquire(8);
        let clusters_data = clusters_mb.into_slice_mut();
        clusters_data.copy_from_slice(&[0u8; 8]);

        let row_write = RowWrite {
            columns_writing_data: smallvec![
                ColumnWritePayload {
                    column_type: DbType::STRING,
                    size: 8 + 4,
                    data: name_mb,
                    write_order: 0
                },
                ColumnWritePayload {
                    column_type: DbType::U64,
                    size: 8,
                    data: clusters_mb,
                    write_order: 1
                }
            ],
        };

        let mut pointer_iterator = RowPointerIterator::new(self.db_io_pointers.clone())
            .await
            .unwrap();

        if let Some(row_pointer) = pointer_iterator.read_last().await {
            let last_id = row_pointer.id;
            let atomic_last_id = AtomicU64::new(last_id);

            let table_length = row_pointer.position;
            let atomic_table_length = AtomicU64::new(table_length);

            #[cfg(feature = "enable_long_row")]
            let cluster = 0;

            let result = RowPointer::write_row(
                self.db_io_pointers.clone(),
                self.db_io_rows.clone(),
                &atomic_last_id,
                &atomic_table_length,
                #[cfg(feature = "enable_long_row")]
                cluster,
                &row_write,
            )
            .await;

            if result.is_err() {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "Failed to write row pointer.",
                ));
            }
        } else {
            let atomic_last_id = AtomicU64::new(0);
            let atomic_table_length = AtomicU64::new(0);

            #[cfg(feature = "enable_long_row")]
            let cluster = 0;

            let result = RowPointer::write_row(
                self.db_io_pointers.clone(),
                self.db_io_rows.clone(),
                &atomic_last_id,
                &atomic_table_length,
                #[cfg(feature = "enable_long_row")]
                cluster,
                &row_write,
            )
            .await;

            if result.is_err() {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "Failed to write initial row pointer.",
                ));
            }
        }

        let table = Table::new(&table_name, self.db_io_pointers.clone(), columns).await;

        self.tables.insert(create_table.table_name, Arc::new(table));

        Ok(())
    }

    pub async fn start_db(database: Arc<Database>) -> io::Result<()> {
        info!("Starting database");

        let db = database.clone();
        let receiver = TCP_SERVER.force().await;

        let fut = async move {
            info!("Database started, listening on port {}", SERVER_PORT);
            let receiver_clone = receiver.clone();
            _ = receiver_clone
                .run_with_context(db, process_incoming_queries)
                .await;
        };

        _ = tokio::spawn(fut).await;

        Ok(())
    }
}

async fn load_db_data_schema<S: StorageIO>(schema_io: Arc<S>) -> TableSchema {
    let schema = {
        if let Ok(schema) = TableSchema::load(schema_io.clone()).await {
            info!("Loaded schema: {:?}", schema);
            schema
        } else {
            info!("Schema not found, creating new schema");
            let mut table = TableSchema::new("db_data".to_string(), false);

            table
                .add_field(
                    schema_io.clone(),
                    "table_name".to_string(),
                    DbType::STRING,
                    true,
                )
                .await;
            table
                .add_field(
                    schema_io.clone(),
                    "clusters".to_string(),
                    DbType::U64,
                    false,
                )
                .await;

            table.save(schema_io).await.unwrap();

            info!("Created new schema and saved it");

            table
        }
    };

    schema
}

async fn load_db_tables<S: StorageIO>(
    schema: &TableSchema,
    row_pointer_iterator: &mut RowPointerIterator<S>,
    io_rows: Arc<S>,
) -> Vec<(String, u64)> {
    info!("Loading database tables");

    let mut tables = Vec::new();

    let schema_fields = &schema.fields;

    let row_fetch = create_db_data_row_fetch(schema_fields);
    let row_fetch2 = create_db_data_row_fetch(schema_fields);

    let concurrent_processor = ConcurrentProcessor::new();

    let all_rows = concurrent_processor
        .process(
            &format!(
                r##"
            1 = 1
        "##
            ),
            row_fetch,
            row_fetch2,
            &schema_fields.to_vec(),
            io_rows,
            row_pointer_iterator,
        )
        .await;

    let small_vec_fields = SmallVec::from_vec(schema_fields.to_vec());

    info!("Found {} tables in database", all_rows.len());

    for row in all_rows {
        // Process each row and extract table information
        if let Some(table_name) = row.get_column("table_name", &small_vec_fields) {
            if let Some(cluster_id) = row.get_column("clusters", &small_vec_fields) {
                let table_name = String::from_utf8(table_name.into_slice().to_vec()).unwrap();
                let cluster_id = u64::from_le_bytes(cluster_id.into_slice().try_into().unwrap());
                info!("Found table: {}, cluster: {}", table_name, cluster_id);
                tables.push((table_name.clone(), cluster_id.clone()));
            } else {
                panic!("Cluster ID not found in row: {:?}", row);
            }
        } else {
            panic!("Table name not found in row: {:?}", row);
        }
    }

    tables
}

fn create_db_data_row_fetch(schema_fields: &SmallVec<[SchemaField; 20]>) -> RowFetch {
    let schema_calculator = SchemaCalculator::default();

    RowFetch {
        columns_fetching_data: smallvec::smallvec![
            ColumnFetchingData {
                column_offset: schema_calculator
                    .calculate_schema_offset("table_name", schema_fields)
                    .0,
                column_type: DbType::STRING,
                size: 8 + 4,
                schema_id: 0,
            },
            ColumnFetchingData {
                column_offset: schema_calculator
                    .calculate_schema_offset("clusters", schema_fields)
                    .0,
                column_type: DbType::U64,
                size: 8,
                schema_id: 1,
            },
        ],
    }
}

#[allow(unused_variables)]
pub(crate) async fn process_incoming_queries(
    database: Arc<Database>,
    request_vec: Vec<u8>,
) -> Vec<u8> {
    let query = String::from_utf8_lossy(&request_vec);

    info!("Received query: {}", query);

    if let Some(query_purpose) = recognize_query_purpose(&query) {
        info!("Recognized query purpose: {:?}", query_purpose);
        execute(query_purpose, database).await
    } else {
        error!("Could not recognize query purpose");
        let mut result: [u8; 1] = [1u8; 1];
        result[0] = 3;
        let mut result = result.to_vec();
        let error_message = "Unrecognized query";
        let mut error_message_bytes = Vec::with_capacity(error_message.len());
        error_message_bytes.extend_from_slice(error_message.as_bytes());
        result.extend_from_slice(&mut error_message_bytes);
        return result.to_vec();
    }
}
