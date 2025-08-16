use std::sync::Arc;

use log::LevelFilter;
use rasterizeddb_core::core::database_v2::Database;

#[tokio::main(flavor = "multi_thread")]
#[allow(unreachable_code)]
#[allow(static_mut_refs)]
async fn main() -> std::io::Result<()> {
    // let io_rows = unsafe { IO_ROWS.force_mut().await };
    // let io_pointers = unsafe { IO_POINTERS.force_mut().await };
    // let io_schema = unsafe { IO_SCHEMA.force_mut().await };

    // tokio::spawn(io_rows.start_service());
    // tokio::spawn(io_pointers.start_service());
    // tokio::spawn(io_schema.start_service());

    // rasterizeddb_core::core::mock_table::
    //     consolidated_write_data_function(5_000_000 * 2).await;

    // let stdin = std::io::stdin();
    // let mut buffer = String::new();
    // println!("Press Enter to start...");
    // stdin.read_line(&mut buffer).unwrap();

    // for i in 0..100 {
    //     println!("Iteration: {}", i);
    //     let schema = get_schema().await;
    //     let _ = consolidated_read_data_function(schema, 599_999_999).await;
    // }
    
    // let stdin = std::io::stdin();
    // let mut buffer = String::new();
    // println!("Press Enter to terminate...");
    // stdin.read_line(&mut buffer).unwrap();

    // return Ok(());

    env_logger::Builder::new()
        .filter_level(LevelFilter::Debug)
        .init();

    let database = Database::new("G:\\Databases\\Production").await;
    let arc_database = Arc::new(database);
    _ = Database::start_db(arc_database).await;

    return Ok(());
}
