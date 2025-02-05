use std::{io::stdin, sync::Arc};

use rasterizeddb_core::{
    core::{
        column::Column, database::{self, Database}, row::InsertOrUpdateRow, storage_providers::{file_sync::LocalStorageProvider, memory::MemoryStorageProvider}, table::Table
    }, rql::parser::parse_rql
};

use stopwatch::Stopwatch;
use tokio::sync::RwLock;
use std::fs::remove_file;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_BACKTRACE","0");

    //_ = remove_file("C:\\Users\\mspc6\\OneDrive\\Professional\\Desktop\\database.db");

    let io_sync = LocalStorageProvider::new(
        "C:\\Users\\mspc6\\OneDrive\\Professional\\Desktop",
        "database.db"
    ).await;

    //let database = Database::new(io_sync).await?;

    // Database::start_async(Arc::new(RwLock::new(database))).await?;

    // loop {
    //     tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    // }

    //let io_sync = MemoryStorageProvider::new();

    let mut table = Table::init(io_sync, false, false).await.unwrap();

    // for i in 0..200_000 {
    //     if i == 199_998 {
    //         let mut c1 = Column::new(5_000_000).unwrap();
    //         let mut c2 = Column::new(-5_000_000).unwrap();
    //         let mut c3 = Column::new("This is awesome 5_000_000.").unwrap();
    
    //         let mut columns_buffer: Vec<u8> = Vec::with_capacity(
    //             c1.len() + 
    //             c2.len() +
    //             c3.len() 
    //         );
        
    //         columns_buffer.append(&mut c1.into_vec().unwrap());
    //         columns_buffer.append(&mut c2.into_vec().unwrap());
    //         columns_buffer.append(&mut c3.into_vec().unwrap());
        
    //         let insert_row = InsertOrUpdateRow {
    //             columns_data: columns_buffer
    //         };
        
    //         table.insert_row(insert_row).await;
    //     } else {
    //         let mut c1 = Column::new(i).unwrap();
    //         let mut c2 = Column::new(i * -1).unwrap();
    //         let mut c3 = Column::new("This is also awesome.").unwrap();
    
    //         let mut columns_buffer: Vec<u8> = Vec::with_capacity(
    //             c1.len() + 
    //             c2.len() +
    //             c3.len() 
    //         );
        
    //         columns_buffer.append(&mut c1.into_vec().unwrap());
    //         columns_buffer.append(&mut c2.into_vec().unwrap());
    //         columns_buffer.append(&mut c3.into_vec().unwrap());
        
    //         let insert_row = InsertOrUpdateRow {
    //             columns_data: columns_buffer
    //         };
        
    //         table.insert_row(insert_row).await;
    //     }
    // }

    // println!("DONE inserting rows.");

    table.rebuild_in_memory_indexes().await;

    println!("DONE building indexes.");

    let row = table.first_or_default_by_id(1).await?.unwrap();

    for column in Column::from_buffer(&row.columns_data).unwrap() {
        println!("{}", column.into_value());
    }

    let row = table.first_or_default_by_id(60).await?.unwrap();

    for column in Column::from_buffer(&row.columns_data).unwrap() {
        println!("{}", column.into_value());
    }

    let mut stopwatch = Stopwatch::new();

    // WHERE COL(0) >= 6000000 LIMIT 20

    let query_evaluation = parse_rql(&format!(r#"
        BEGIN
        SELECT FROM NAME_DOESNT_MATTER_FOR_NOW
        WHERE COL(2) = 'This is awesome 5_000_000.'
        LIMIT 1
        END
    "#)).unwrap();

    stopwatch.start();
    let rows = table.execute_query(query_evaluation.parser_result).await?;
    stopwatch.stop();

    println!("elapsed {:?}", stopwatch.elapsed_ms());
    println!("total rows {:?}", rows.unwrap().len());

    //table.delete_row_by_id(3).await.unwrap();

    // UPDATE ROW(3)

    //let x = 48;

    //let mut c1_update = Column::new(21).unwrap();
    //let mut c2_update = Column::new(x * -1).unwrap();
    //let large_string = "A".repeat(1024);
    //let mut c3_update = Column::new(&large_string).unwrap();

    // let mut columns_buffer_update: Vec<u8> = Vec::with_capacity(
    //     c1_update.len() + 
    //     c2_update.len()
        //c3_update.len() 
    //);

    // columns_buffer_update.append(&mut c1_update.into_vec().unwrap());
    // columns_buffer_update.append(&mut c2_update.into_vec().unwrap());
    //columns_buffer_update.append(&mut c3_update.into_vec().unwrap());

    // let update_row = InsertOrUpdateRow {
    //     columns_data: columns_buffer_update
    // };

    //table.update_row_by_id(3, update_row).await;

    //table.delete_row_by_id(3).unwrap();

    //table.vacuum_table().await;

    //table.first_or_default_by_id(3)?.unwrap();

    // END UPDATE ROW(3)

    //let row = table.first_or_default_by_id(4).await?.unwrap();

    // for column in Column::from_buffer(&row.columns_data).unwrap() {
    //     println!("{}", column.into_value());
    // }

    //let row = table.first_or_default_by_column(0, 21)?.unwrap();

    // for column in Column::from_buffer(&row.columns_data).unwrap() {
    //     println!("{}", column.into_value());
    // }

    // let mut stopwatch = Stopwatch::new();

    // for _ in 0..100 {
    //     stopwatch.start();
    //     let query_evaluation = parse_rql(&format!(r#"
    //         BEGIN
    //         SELECT FROM NAME_DOESNT_MATTER_FOR_NOW
    //         WHERE COL(0) >= 0 LIMIT 20
    //         END
    //     "#)).unwrap();

    //     //let _rows = table.execute_query(query_evaluation).await?.unwrap();
    //     stopwatch.stop();
    //     println!("{}ms", stopwatch.elapsed_ms());
    //     stopwatch.reset();
    //     println!("DONE");
    // }

    // for row in rows {
    //     for column in Column::from_buffer(&row.columns_data).unwrap() {
    //         println!("{}", column.into_value());
    //     }
    // }

    println!("Press any key to continue...");

    let mut buffer = String::new();
    stdin().read_line(&mut buffer).unwrap();

    Ok(())
}