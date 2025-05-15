use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};

use log::LevelFilter;
use rasterizeddb_core::core::database::Database;

use rasterizeddb_core::core::db_type::DbType;
use rasterizeddb_core::core::row_v2::row::{ColumnFetchingData, ColumnWritePayload, RowFetch, RowWrite};
use rasterizeddb_core::core::row_v2::row_pointer::{RowPointer, RowPointerIterator};
use rasterizeddb_core::core::storage_providers::mock_file_sync::MockStorageProvider;
use rasterizeddb_core::core::storage_providers::traits::StorageIO;
use rasterizeddb_core::core::table::Table;
use rasterizeddb_core::memory_pool::MEMORY_POOL;
use rasterizeddb_core::rql::parser::{parse_rql, ParserResult};
use rasterizeddb_core::{
    core::storage_providers::file_sync::LocalStorageProvider,
    EMPTY_BUFFER,
};

use stopwatch::Stopwatch;
use tokio::fs::remove_file;

#[tokio::main(flavor = "multi_thread")]
#[allow(unreachable_code)]
async fn main() -> std::io::Result<()> {
    let mut mock_io_pointers = MockStorageProvider::new().await;
    let mut mock_io_rows = MockStorageProvider::new().await;

    println!("{}", mock_io_pointers.get_location().unwrap());
    println!("{}", mock_io_rows.get_location().unwrap());

    #[cfg(feature = "enable_long_row")]
    let cluster = 0;

    let last_id = AtomicU64::new(0);
    let table_length = AtomicU64::new(0);

    // Column 0
    let string_bytes = b"Hello, world!";

    let string_data = MEMORY_POOL.acquire(string_bytes.len());
    let mut string_data_wrapper = unsafe { string_data.into_wrapper() };
    let string_vec = string_data_wrapper.as_vec_mut();

    string_vec[0..].copy_from_slice(string_bytes);

    // Column 1
    
    let large_string = "Hello, world!".repeat(50);
    let large_string_bytes = large_string.as_bytes();

    let large_string_data = MEMORY_POOL.acquire(large_string_bytes.len());
    let mut large_string_data_wrapper = unsafe { large_string_data.into_wrapper() };
    let large_string_vec = large_string_data_wrapper.as_vec_mut();

    large_string_vec[0..].copy_from_slice(large_string_bytes);

    // Column 2

    let i32_bytes = 42_i32.to_le_bytes();
    let i32_data = MEMORY_POOL.acquire(i32_bytes.len());
    let mut i32_data_wrapper = unsafe { i32_data.into_wrapper() };
    let i32_vec = i32_data_wrapper.as_vec_mut();
    i32_vec.copy_from_slice(&i32_bytes);

    let row_write = RowWrite {
        columns_writing_data: vec![
            ColumnWritePayload {
                data: string_data,
                write_order: 0,
                column_type: DbType::STRING,
                size: 4 + 8 // String Size + String Pointer
            },
            ColumnWritePayload {
                data: i32_data,
                write_order: 1,
                column_type: DbType::I32,
                size: i32_bytes.len() as u32
            },
            ColumnWritePayload {
                data: large_string_data,
                write_order: 2,
                column_type: DbType::STRING,
                size: 4 + 8 // String Size + String Pointer
            },
        ],
    };

    _ = RowPointer::write_row(
        &mut mock_io_pointers, 
        &mut mock_io_rows, 
        &last_id, 
        &table_length, 
        #[cfg(feature = "enable_long_row")]
        cluster, 
        &row_write
    ).await;

    _ = RowPointer::write_row(
        &mut mock_io_pointers, 
        &mut mock_io_rows, 
        &last_id, 
        &table_length, 
        #[cfg(feature = "enable_long_row")]
        cluster, 
        &row_write
    ).await;

    _ = RowPointer::write_row(
        &mut mock_io_pointers, 
        &mut mock_io_rows, 
        &last_id, 
        &table_length, 
        #[cfg(feature = "enable_long_row")]
        cluster, 
        &row_write
    ).await;

    _ = RowPointer::write_row(
        &mut mock_io_pointers, 
        &mut mock_io_rows, 
        &last_id, 
        &table_length, 
        #[cfg(feature = "enable_long_row")]
        cluster, 
        &row_write
    ).await;

    let result = RowPointer::write_row(
        &mut mock_io_pointers, 
        &mut mock_io_rows, 
        &last_id, 
        &table_length, 
        #[cfg(feature = "enable_long_row")]
        cluster, 
        &row_write
    ).await;

    println!("Row write result: {:?}", result);

    assert!(result.is_ok());

    let mut iterator = RowPointerIterator::new(&mut mock_io_pointers).await.unwrap();

    let mut stopwatch = Stopwatch::start_new();

    let next_pointer = iterator.next_row_pointer().await.unwrap().unwrap();

    let row_fetch = RowFetch {
        columns_fetching_data: vec![
            ColumnFetchingData {
                column_offset: 0,
                column_type: DbType::STRING,
                size: 0 // String size is not known yet
            },
            ColumnFetchingData {
                column_offset: 8 + 4,
                column_type: DbType::I32,
                size: i32_bytes.len() as u32
            },
            // ColumnFetchingData {
            //     column_offset: 8 + 4 + 4,
            //     column_type: DbType::STRING,
            //     size: 0 // String size is not known yet
            // },
        ],
    };

    let row = next_pointer.fetch_row(
        &mut mock_io_rows, 
        &row_fetch
    ).await.unwrap();

    stopwatch.stop();

    println!("Row fetch took: {:?}", stopwatch.elapsed());
    
    row.columns.iter().for_each(|column| {
        match column.column_type {
            DbType::STRING => {
                println!("STRING column: {:?}", column.data);
                let wrapper = unsafe { column.data.into_wrapper() };
                let string_data = wrapper.as_vec();
                if string_data.len() < 100 {
                    assert_eq!(string_data, string_bytes);
                } else {
                    assert_eq!(string_data, large_string_bytes);
                } 
            },
            DbType::I32 => {
                println!("I32 column: {:?}", column.data);
                let wrapper = unsafe { column.data.into_wrapper() };
                let i32_data = wrapper.as_slice();
                assert_eq!(i32_data, i32_bytes);
            },
            _ => {}
        }
    });

    return Ok(());

    env_logger::Builder::new()
        .filter_level(LevelFilter::Error)
        .init();
    
    #[cfg(target_arch = "x86_64")]
    {
        let empty_buffer_ptr = EMPTY_BUFFER.as_ptr();
        unsafe { _mm_prefetch::<_MM_HINT_T0>(empty_buffer_ptr as *const i8) };
    }

    let db_file = "C:\\db\\";
    let io_sync = LocalStorageProvider::new(
        db_file,
        None,
    )
    .await;

    let _database = Database::new(io_sync).await?;
    _ = tokio::spawn(Database::start_async(Arc::new(_database))).await?;

    return Ok(());

    _ = remove_file("C:\\db\\test.db").await;

    let io_sync = LocalStorageProvider::new(
        db_file,
        Some("test.db"),
    )
    .await;

    let mut table = Table::init("test_db".into(), io_sync, false, false).await.unwrap();

    let insert_query_evaluation = format!(
        r#"
        BEGIN
        INSERT INTO test_db (COL(I32), COL(STRING))
        VALUES (5882, 'This is a test2')
        VALUES (5882, 'This is a test3')
        VALUES (5882, 'This is a test4')
        VALUES (5882, 'This is a test5')
        END
    "#);

    let query_evaluation = parse_rql(&insert_query_evaluation).unwrap();
  
    match query_evaluation.parser_result {
        ParserResult::InsertEvaluationTokens(insert) => {
            for row in insert.rows {
                table.insert_row(row).await;
            }
        },
        _ => {
            println!("Unsupported database action.");
        }
    };

    println!("DONE inserting rows.");

    table.rebuild_in_memory_indexes().await;

    println!("DONE building indexes.");

    let query_evaluation = parse_rql(&format!(
        r#"
        BEGIN
        SELECT FROM test_db
        WHERE COL(1,STRING) = 'Foo'
        LIMIT 50
        END
    "#
    )).unwrap();

    let _result = table.execute_query(query_evaluation.parser_result).await.unwrap();

    //println!("total rows from query {:?}", rows.unwrap().len());

    return Ok(());
}