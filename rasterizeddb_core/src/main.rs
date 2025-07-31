use std::sync::Arc;
use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
use log::LevelFilter;
use rasterizeddb_core::core::database::Database;
use rasterizeddb_core::core::mock_table::{consolidated_read_data_function, get_schema, IO_POINTERS, IO_ROWS};
use rasterizeddb_core::core::storage_providers::traits::StorageIO;
use rasterizeddb_core::{
    core::storage_providers::file_sync::LocalStorageProvider,
    EMPTY_BUFFER,
};

#[tokio::main(flavor = "multi_thread")]
#[allow(unreachable_code)]
#[allow(static_mut_refs)]
async fn main() -> std::io::Result<()> {
    let io_rows = unsafe { IO_ROWS.force_mut().await };
    let io_pointers = unsafe { IO_POINTERS.force_mut().await };

    tokio::spawn(io_rows.start_service());
    tokio::spawn(io_pointers.start_service());

    // rasterizeddb_core::core::mock_table::
    //     consolidated_write_data_function(5_000_000 * 2).await;

    let stdin = std::io::stdin();
    let mut buffer = String::new();
    println!("Press Enter to start...");
    stdin.read_line(&mut buffer).unwrap();

    for i in 0..100 {
        println!("Iteration: {}", i);
        let schema = get_schema().await;
        let _ = consolidated_read_data_function(schema, 599_999_999).await;
    }
    
    let stdin = std::io::stdin();
    let mut buffer = String::new();
    println!("Press Enter to terminate...");
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
