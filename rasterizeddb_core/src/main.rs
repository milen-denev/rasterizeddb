use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};

use log::LevelFilter;
use rasterizeddb_core::core::database::Database;

use rasterizeddb_core::core::db_type::DbType;
use rasterizeddb_core::core::mock_helpers::{create_row_write, create_row_write_custom_i32, get_row_fetch_i32, i32_column};
use rasterizeddb_core::core::row_v2::concurrent_processor::ConcurrentProcessor;
use rasterizeddb_core::core::row_v2::row_pointer::{RowPointer, RowPointerIterator};
use rasterizeddb_core::core::row_v2::transformer::{ColumnTransformer, ColumnTransformerType, ComparerOperation};

use rasterizeddb_core::core::storage_providers::traits::StorageIO;
use rasterizeddb_core::{
    core::storage_providers::file_sync::LocalStorageProvider,
    EMPTY_BUFFER,
};

use stopwatch::Stopwatch;

static mut IO_ROWS: async_lazy::Lazy<LocalStorageProvider> =
    async_lazy::Lazy::new(|| {
        Box::pin(async {
            //let io = LocalStorageProvider::new("G:\\Databases\\Test_Database", Some("rows3.db")).await;
            let loc = "/home/milen-denev/Database/";
            let io = LocalStorageProvider::new(loc, Some("rows3.db")).await;
            io
        })
    });

static mut IO_POINTERS: async_lazy::Lazy<LocalStorageProvider> =
    async_lazy::Lazy::new(|| {
        Box::pin(async {
            //let io = LocalStorageProvider::new("G:\\Databases\\Test_Database", Some("pointers3.db")).await;
            let loc = "/home/milen-denev/Database/";
            let io = LocalStorageProvider::new(loc, Some("pointers3.db")).await;
            io
        })
    });

#[tokio::main(flavor = "multi_thread")]
#[allow(unreachable_code)]
#[allow(static_mut_refs)]
async fn main() -> std::io::Result<()> {
    const SEARCH_VALUE: i32 = 1000;

    let io_rows = unsafe { IO_ROWS.force_mut().await };
    let io_pointers = unsafe { IO_POINTERS.force_mut().await };

    tokio::spawn(io_rows.start_service());
    tokio::spawn(io_pointers.start_service());

    let io_rows = unsafe { IO_ROWS.force_mut().await };
    let io_pointers = unsafe { IO_POINTERS.force_mut().await };

    let stdin = std::io::stdin();
    let mut buffer = String::new();
    println!("Press Enter to start...");
    stdin.read_line(&mut buffer).unwrap();

    #[cfg(feature = "enable_long_row")]
    let cluster = 0;

    // let last_id = AtomicU64::new(0);
    // let table_length = AtomicU64::new(io_rows.get_len().await);

    // let row_write = create_row_write(true);
    // let custom_row = create_row_write_custom_i32(SEARCH_VALUE, true);

    // for _i in 0..1000 {
    //     _ = RowPointer::write_row(
    //         io_pointers,
    //         io_rows, 
    //         &last_id, 
    //         &table_length, 
    //         #[cfg(feature = "enable_long_row")]
    //         cluster, 
    //         &row_write
    //     ).await;
    // }

    // let _result = RowPointer::write_row(
    //     io_pointers,
    //     io_rows, 
    //     &last_id, 
    //     &table_length, 
    //     #[cfg(feature = "enable_long_row")]
    //     cluster, 
    //     &custom_row
    // ).await;

    // if let Err(e) = result {
    //     println!("Error writing row: {}", e);
    // }

    //tokio::time::sleep(tokio::time::Duration::from_millis(1500)).await;

    let mut stopwatch = Stopwatch::start_new();

    let concurrent_processor = ConcurrentProcessor::new();

    let mut iterator = RowPointerIterator::new(io_pointers).await.unwrap();
    let row_fetch = get_row_fetch_i32();
    let search_value = SEARCH_VALUE;
    let search_value_block = i32_column(search_value);

    let transformer = ColumnTransformer::new(
        DbType::I32,
        search_value_block,
        ColumnTransformerType::ComparerOperation(ComparerOperation::Equals),
        None
    );

    // Await the aggregator to finish collecting rows.
    let all_rows = concurrent_processor.process(row_fetch, io_rows, &mut iterator, transformer).await;

    stopwatch.stop();

    println!("Total rows collected: {}", all_rows.len());
    println!("Row fetch took: {:?}", stopwatch.elapsed());
    
    stopwatch.reset();
    stopwatch.start();
    
    let row_fetch = get_row_fetch_i32();
    let search_value = SEARCH_VALUE;
    let search_value_block = i32_column(search_value);

    let transformer = ColumnTransformer::new(
        DbType::I32,
        search_value_block,
        ColumnTransformerType::ComparerOperation(ComparerOperation::Equals),
        None
    );

    let all_rows = concurrent_processor.process(row_fetch, io_rows, &mut iterator, transformer).await;
    stopwatch.stop();

    println!("Total rows collected: {}", all_rows.len());
    println!("Second row fetch took: {:?}", stopwatch.elapsed());

    let stdin = std::io::stdin();
    let mut buffer = String::new();
    println!("Press Enter to continue...");
    stdin.read_line(&mut buffer).unwrap();

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
}