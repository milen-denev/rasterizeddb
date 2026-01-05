use std::sync::atomic::AtomicU64;

use rclite::Arc;

use smallvec::SmallVec;
use stopwatch::Stopwatch;

use crate::{
    core::{processor::concurrent_processor::ConcurrentProcessor, row::schema::TableSchemaIterator},
    memory_pool::MEMORY_POOL,
};

use super::{
    db_type::DbType,
    row::{
        row::{ColumnFetchingData, ColumnWritePayload, RowFetch, RowWrite},
        row_pointer::{RowPointer, RowPointerIterator},
        schema::{SchemaCalculator, SchemaField, TableSchema},
    },
    storage_providers::{file_sync::LocalStorageProvider, traits::StorageIO},
};

pub static mut IO_SCHEMA: async_lazy::Lazy<Arc<LocalStorageProvider>> =
    async_lazy::Lazy::new(|| {
        Box::pin(async {
            let io = LocalStorageProvider::new("G:\\Databases\\Test_Database", Some("table_5M.db"))
                .await;
            Arc::new(io)
        })
    });

#[allow(static_mut_refs)]
pub static mut IO_POINTERS: async_lazy::Lazy<Arc<LocalStorageProvider>> =
    async_lazy::Lazy::new(|| {
        Box::pin(async {
            let schema_io = unsafe { IO_SCHEMA.force().await };
            let io_location = schema_io.get_location().unwrap();
            let name = schema_io.get_name();
            let name = format!("{}_{}", name.replace(".db", ""), "POINTERS.db");
            let io = LocalStorageProvider::new(&io_location, Some(&name)).await;
            Arc::new(io)
        })
    });

#[allow(static_mut_refs)]
pub static mut IO_ROWS: async_lazy::Lazy<Arc<LocalStorageProvider>> = async_lazy::Lazy::new(|| {
    Box::pin(async {
        let schema_io = unsafe { IO_SCHEMA.force().await };
        let io_location = schema_io.get_location().unwrap();
        let name = schema_io.get_name();
        let name = format!("{}_{}", name.replace(".db", ""), "ROWS.db");
        let io = LocalStorageProvider::new(&io_location, Some(&name)).await;
        Arc::new(io)
    })
});

#[allow(static_mut_refs)]
pub async fn consolidated_write_data_function(times: u64) {
    let io_schema = unsafe { IO_SCHEMA.force().await };
    let io_pointers = unsafe { IO_POINTERS.force().await };
    let io_rows = unsafe { IO_ROWS.force().await };

    let io_schema = io_schema.clone();

    create_schema(io_schema, "test_table").await;

    for i in 0..times {
        let io_pointers = io_pointers.clone();
        let io_rows = io_rows.clone();

        fill_data(i, io_pointers, io_rows, times).await;
    }
}

#[allow(static_mut_refs)]
pub async fn get_schema() -> TableSchema {
    let io_schema = unsafe { IO_SCHEMA.force().await };
    let io_schema = io_schema.clone();
    let mut iterator = TableSchemaIterator::new(io_schema).await.unwrap();
    let schema = iterator.next_table_schema().await.unwrap();
    if let Some(schema) = schema {
        return schema;
    }
    panic!("No schema found")
}

#[allow(static_mut_refs)]
pub async fn consolidated_read_data_function(schema: TableSchema, _id: u64) {
    let io_pointers = unsafe { IO_POINTERS.force().await };
    let io_rows = unsafe { IO_ROWS.force().await };

    let io_pointers = io_pointers.clone();
    let io_rows = io_rows.clone();

    let mut stopwatch = Stopwatch::start_new();

    // let schema_fields = schema.fields.clone();
    // let row_fetch = create_row_fetch(&schema_fields);

    let mut iterator = RowPointerIterator::new(io_pointers).await.unwrap();
    let concurrent_processor = ConcurrentProcessor::new();

    // let all_rows = concurrent_processor.process(
    //     &format!(
    //     r##"
    //         id = {}
    //         AND
    //         age < 40 AND
    //         bank_balance > 500.25 AND
    //         name != 'Jane Doe' AND
    //         last_purchase_category CONTAINS 'Elec' AND
    //         last_purchase_amount >= 50.0 AND
    //         is_active = 1 AND
    //         last_purchase_amount < 200.0 AND
    //         last_purchase_notes ENDSWITH 'notes' AND
    //         last_purchase_date = '2023-10-01'
    //         OR
    //         id < 1 AND
    //         age > 25 AND
    //         bank_balance < 5000.00 AND
    //         name STARTSWITH 'J' AND
    //         last_purchase_category = 'Electronics' AND
    //         last_purchase_amount <= 150.0 AND
    //         is_active = 1 AND
    //         last_purchase_amount >= 75.5 AND
    //         last_purchase_notes CONTAINS 'No' AND
    //         last_purchase_date = '2023-10-01'
    //     "##, _id),
    //     row_fetch,
    //     schema_fields,
    //     io_rows,
    //     &mut iterator
    // ).await;

    let schema_fields = schema.fields.clone();
    let row_fetch = create_row_fetch(&schema_fields);
    let row_fetch2 = create_row_fetch(&schema_fields);

    let all_rows = concurrent_processor
        .process(
            &schema.name,
            &format!(
                r##"
            id < 2
        "##
            ),
            row_fetch,
            row_fetch2,
            &schema_fields.to_vec(),
            io_rows,
            &mut iterator,
            None,
            None,
        )
        .await;

    stopwatch.stop();

    println!("Total rows collected: {}", all_rows.len());
    println!("Row fetch took: {:?}", stopwatch.elapsed());

    //}

    // for row in all_rows {
    //     for column in row.columns {
    //         match column.column_type {
    //             DbType::STRING => {
    //                 let string_data = column.data.into_slice();
    //                 let string_value = String::from_utf8_lossy(string_data);
    //                 println!("Column: {}, Value: {}", column.column_type, string_value);
    //             }
    //             DbType::U64 => {
    //                 let u64_data = column.data.into_slice();
    //                 let u64_value = u64::from_le_bytes(u64_data.try_into().unwrap());
    //                 println!("Column: {}, Value: {}", column.column_type, u64_value);
    //             }
    //             _ => {
    //                 let other_data = column.data.into_slice();
    //                 let other_value = other_data.to_vec();
    //                 println!("Column: {}, Value: {:?}", column.column_type, other_value);
    //             }
    //         }
    //     }
    // }
}

fn create_row_fetch(schema_fields: &SmallVec<[SchemaField; 20]>) -> RowFetch {
    let schema_calculator = SchemaCalculator::default();

    RowFetch {
        columns_fetching_data: smallvec::smallvec![
            ColumnFetchingData {
                column_offset: schema_calculator
                    .calculate_schema_offset("id", schema_fields)
                    .0,
                column_type: DbType::U64,
                size: 8,
                schema_id: schema_calculator
                    .calculate_schema_offset("id", schema_fields)
                    .1
            },
            // ColumnFetchingData {
            //     column_offset: schema_calculator.calculate_schema_offset("name", schema_fields),
            //     column_type: DbType::STRING,
            //     size: 4 + 8
            // },
            // ColumnFetchingData {
            //     column_offset: schema_calculator.calculate_schema_offset("age", schema_fields),
            //     column_type: DbType::U8,
            //     size: 1
            // },
            // ColumnFetchingData {
            //     column_offset: schema_calculator.calculate_schema_offset("email", schema_fields),
            //     column_type: DbType::STRING,
            //     size: 4 + 8
            // },
            // ColumnFetchingData {
            //     column_offset: schema_calculator.calculate_schema_offset("address", schema_fields),
            //     column_type: DbType::STRING,
            //     size: 4 + 8
            // },
            // ColumnFetchingData {
            //     column_offset: schema_calculator.calculate_schema_offset("phone", schema_fields),
            //     column_type: DbType::STRING,
            //     size: 4 + 8
            // },
            // ColumnFetchingData {
            //     column_offset: schema_calculator.calculate_schema_offset("is_active", schema_fields),
            //     column_type: DbType::U8,
            //     size: 1
            // },
            // ColumnFetchingData {
            //     column_offset: schema_calculator.calculate_schema_offset("bank_balance", schema_fields),
            //     column_type: DbType::F64,
            //     size: 8
            // },
            // ColumnFetchingData {
            //     column_offset: schema_calculator.calculate_schema_offset("married", schema_fields),
            //     column_type: DbType::U8,
            //     size: 1
            // },
            // ColumnFetchingData {
            //     column_offset: schema_calculator.calculate_schema_offset("birth_date", schema_fields),
            //     column_type: DbType::STRING,
            //     size: 4 + 8
            // },
            // ColumnFetchingData {
            //     column_offset: schema_calculator.calculate_schema_offset("last_login", schema_fields),
            //     column_type: DbType::STRING,
            //     size: 4 + 8
            // },
            // ColumnFetchingData {
            //     column_offset: schema_calculator.calculate_schema_offset("last_purchase", schema_fields),
            //     column_type: DbType::STRING,
            //     size: 4 + 8
            // },
            // ColumnFetchingData {
            //     column_offset: schema_calculator.calculate_schema_offset("last_purchase_amount", schema_fields),
            //     column_type: DbType::F32,
            //     size: 4
            // },
            // ColumnFetchingData {
            //     column_offset: schema_calculator.calculate_schema_offset("last_purchase_date", schema_fields),
            //     column_type: DbType::STRING,
            //     size: 4 + 8
            // },
            // ColumnFetchingData {
            //     column_offset: schema_calculator.calculate_schema_offset("last_purchase_location", schema_fields),
            //     column_type: DbType::STRING,
            //     size: 4 + 8
            // },
            // ColumnFetchingData {
            //     column_offset: schema_calculator.calculate_schema_offset("last_purchase_method", schema_fields),
            //     column_type: DbType::STRING,
            //     size: 4 + 8
            // },
            // ColumnFetchingData {
            //     column_offset: schema_calculator.calculate_schema_offset("last_purchase_category", schema_fields),
            //     column_type: DbType::STRING,
            //     size: 4 + 8
            // },
            // ColumnFetchingData {
            //     column_offset: schema_calculator.calculate_schema_offset("last_purchase_subcategory", schema_fields),
            //     column_type: DbType::STRING,
            //     size: 4 + 8
            // },
            // ColumnFetchingData {
            //     column_offset: schema_calculator.calculate_schema_offset("last_purchase_description", schema_fields),
            //     column_type: DbType::STRING,
            //     size: 4 + 8
            // },
            // ColumnFetchingData {
            //     column_offset: schema_calculator.calculate_schema_offset("last_purchase_notes", schema_fields),
            //     column_type: DbType::STRING,
            //     size: 4 + 8
            // },
        ],
    }
}

// Helper function to create a test TableSchema
fn create_test_table_schema(name: &str, primary_key: Option<&str>) -> TableSchema {
    let mut table = TableSchema::new(name.to_string(), false);
    if let Some(pk) = primary_key {
        table.set_primary_key(pk.to_string());
    }
    table
}

async fn create_schema<S: StorageIO>(mock_io: Arc<S>, table_name: &str) -> TableSchema {
    let mut schema = create_test_table_schema(table_name, None);
    schema.save(mock_io.clone()).await.unwrap();
    schema
        .add_field(mock_io.clone(), "id".into(), DbType::U64, true)
        .await;
    schema
        .add_field(mock_io.clone(), "name".into(), DbType::STRING, false)
        .await;
    schema
        .add_field(mock_io.clone(), "age".into(), DbType::U8, false)
        .await;
    schema
        .add_field(mock_io.clone(), "email".into(), DbType::STRING, false)
        .await;
    schema
        .add_field(mock_io.clone(), "address".into(), DbType::STRING, false)
        .await;
    schema
        .add_field(mock_io.clone(), "phone".into(), DbType::STRING, false)
        .await;
    schema
        .add_field(mock_io.clone(), "is_active".into(), DbType::U8, false)
        .await;
    schema
        .add_field(mock_io.clone(), "bank_balance".into(), DbType::F64, false)
        .await;
    schema
        .add_field(mock_io.clone(), "married".into(), DbType::U8, false)
        .await;
    schema
        .add_field(mock_io.clone(), "birth_date".into(), DbType::STRING, false)
        .await;
    schema
        .add_field(mock_io.clone(), "last_login".into(), DbType::STRING, false)
        .await;
    schema
        .add_field(
            mock_io.clone(),
            "last_purchase".into(),
            DbType::STRING,
            false,
        )
        .await;
    schema
        .add_field(
            mock_io.clone(),
            "last_purchase_amount".into(),
            DbType::F32,
            false,
        )
        .await;
    schema
        .add_field(
            mock_io.clone(),
            "last_purchase_date".into(),
            DbType::STRING,
            false,
        )
        .await;
    schema
        .add_field(
            mock_io.clone(),
            "last_purchase_location".into(),
            DbType::STRING,
            false,
        )
        .await;
    schema
        .add_field(
            mock_io.clone(),
            "last_purchase_method".into(),
            DbType::STRING,
            false,
        )
        .await;
    schema
        .add_field(
            mock_io.clone(),
            "last_purchase_category".into(),
            DbType::STRING,
            false,
        )
        .await;
    schema
        .add_field(
            mock_io.clone(),
            "last_purchase_subcategory".into(),
            DbType::STRING,
            false,
        )
        .await;
    schema
        .add_field(
            mock_io.clone(),
            "last_purchase_description".into(),
            DbType::STRING,
            false,
        )
        .await;
    schema
        .add_field(
            mock_io.clone(),
            "last_purchase_notes".into(),
            DbType::STRING,
            false,
        )
        .await;
    schema
}

static LAST_ID: AtomicU64 = AtomicU64::new(0);
static TABLE_LENGTH: AtomicU64 = AtomicU64::new(0);

async fn fill_data<S: StorageIO>(id: u64, io_pointers: Arc<S>, io_rows: Arc<S>, times: u64) {
    #[cfg(feature = "enable_long_row")]
    let cluster = 0;

    let row_write = if id == times - 1 {
        create_row_write(id, Some("Test row with notes".to_string()))
    } else {
        create_row_write(id, None)
    };

    let _result = RowPointer::write_row(
        io_pointers,
        io_rows,
        &LAST_ID,
        &TABLE_LENGTH,
        #[cfg(feature = "enable_long_row")]
        cluster,
        &row_write,
    )
    .await;

    if _result.is_err() {
        panic!("Error writing row");
    }
}

fn create_string_column(data: &str, write_order: u32) -> ColumnWritePayload {
    let string_bytes = data.as_bytes();

    let mut string_data = MEMORY_POOL.acquire(string_bytes.len());
    let string_slice = string_data.into_slice_mut();

    string_slice[0..].copy_from_slice(string_bytes);

    ColumnWritePayload {
        data: string_data,
        write_order,
        column_type: DbType::STRING,
        size: 4 + 8,
    }
}

fn create_u8_column(data: u8, write_order: u32) -> ColumnWritePayload {
    let mut u8_data = MEMORY_POOL.acquire(1);
    let u8_slice = u8_data.into_slice_mut();
    u8_slice.copy_from_slice(&data.to_le_bytes());

    ColumnWritePayload {
        data: u8_data,
        write_order,
        column_type: DbType::U8,
        size: 1,
    }
}

fn create_u64_column(data: u64, write_order: u32) -> ColumnWritePayload {
    let mut u64_data = MEMORY_POOL.acquire(8);
    let u64_slice = u64_data.into_slice_mut();
    u64_slice.copy_from_slice(&data.to_le_bytes());

    ColumnWritePayload {
        data: u64_data,
        write_order,
        column_type: DbType::U64,
        size: 8,
    }
}

fn create_f32_column(data: f32, write_order: u32) -> ColumnWritePayload {
    let mut f32_data = MEMORY_POOL.acquire(4);
    let f32_slice = f32_data.into_slice_mut();
    f32_slice.copy_from_slice(&data.to_le_bytes());

    ColumnWritePayload {
        data: f32_data,
        write_order,
        column_type: DbType::F32,
        size: 4,
    }
}

fn create_f64_column(data: f64, write_order: u32) -> ColumnWritePayload {
    let mut f64_data = MEMORY_POOL.acquire(8);
    let f64_slice = f64_data.into_slice_mut();
    f64_slice.copy_from_slice(&data.to_le_bytes());

    ColumnWritePayload {
        data: f64_data,
        write_order,
        column_type: DbType::F64,
        size: 8,
    }
}

fn create_row_write(id: u64, notes: Option<String>) -> RowWrite {
    RowWrite {
        columns_writing_data: smallvec::smallvec![
            create_u64_column(id, 0),
            create_string_column("John Doe", 1),
            create_u8_column(30, 2),
            create_string_column("john.doe@example.com", 3),
            create_string_column("123 Main St", 4),
            create_string_column("555-1234", 5),
            create_u8_column(1, 6),
            create_f64_column(1000.50, 7),
            create_u8_column(0, 8),
            create_string_column("1990-01-01", 9),
            create_string_column("2023-10-01", 10),
            create_string_column("Sunsung Phone Andomega 10", 11),
            create_f32_column(100.0, 12),
            create_string_column("2023-10-01", 13),
            create_string_column("New York", 14),
            create_string_column("Credit Card", 15),
            create_string_column("Electronics", 16),
            create_string_column("Smartphones", 17),
            create_string_column("Latest model", 18),
            notes.map_or_else(
                || create_string_column("No notes", 19),
                |n| create_string_column(&n, 19)
            )
        ],
    }
}

fn _print_process_memory_stats(label: &str) {
    if let Some(usage) = memory_stats::memory_stats() {
        println!("--- Memory Stats ({}) ---", label);
        println!(
            "  Physical Memory: {} bytes ({} MB)",
            usage.physical_mem,
            usage.physical_mem / (1024 * 1024)
        );
        println!(
            "  Virtual Memory:  {} bytes ({} MB)",
            usage.virtual_mem,
            usage.virtual_mem / (1024 * 1024)
        );
        println!("----------------------------");
    } else {
        println!("--- Memory Stats ({}) ---", label);
        println!("  Could not get memory statistics.");
        println!("----------------------------");
    }
}
