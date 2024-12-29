use std::io::stdin;

use rasterizeddb_core::{
    core::{
        column::Column, 
        row::InsertOrUpdateRow, 
        storage_providers::{file_sync::LocalStorageProvider, memory::MemoryStorageProvider}, 
        table::Table
    }, rql::parser::parse_rql
};

use stopwatch::Stopwatch;
use std::fs::remove_file;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_BACKTRACE","0");

    //_ = remove_file("C:\\Users\\mspc6\\OneDrive\\Professional\\Desktop\\database.db");

    let io_sync = LocalStorageProvider::new(
        "C:\\Users\\mspc6\\OneDrive\\Professional\\Desktop",
        "database.db"
    ).await;

    //let io_sync = MemoryStorageProvider::new();

    let mut table = Table::init(io_sync, false, false).await.unwrap();

    // for i in 0..5 {
    //     if i == 3 {
    //         let mut c1 = Column::new(1000).unwrap();
    //         let mut c2 = Column::new(i * -1).unwrap();
    //         //let mut c3 = Column::new("This is awesome 2.").unwrap();
    
    //         let mut columns_buffer: Vec<u8> = Vec::with_capacity(
    //             c1.len() + 
    //             c2.len() //+
    //             //c3.len() 
    //         );
        
    //         columns_buffer.append(&mut c1.into_vec().unwrap());
    //         columns_buffer.append(&mut c2.into_vec().unwrap());
    //         //columns_buffer.append(&mut c3.into_vec().unwrap());
        
    //         let insert_row = InsertOrUpdateRow {
    //             columns_data: columns_buffer
    //         };
        
    //         table.insert_row(insert_row).await;
    //     } else {
    //         let mut c1 = Column::new(i).unwrap();
    //         let mut c2 = Column::new(i * -1).unwrap();
    //         //let mut c3 = Column::new("This is also awesome.").unwrap();
    
    //         let mut columns_buffer: Vec<u8> = Vec::with_capacity(
    //             c1.len() + 
    //             c2.len() //+
    //             //c3.len() 
    //         );
        
    //         columns_buffer.append(&mut c1.into_vec().unwrap());
    //         columns_buffer.append(&mut c2.into_vec().unwrap());
    //         //columns_buffer.append(&mut c3.into_vec().unwrap());
        
    //         let insert_row = InsertOrUpdateRow {
    //             columns_data: columns_buffer
    //         };
        
    //         table.insert_row(insert_row).await;
    //     }
    // }

    //table.rebuild_in_memory_indexes();

    let row = table.first_or_default_by_id(1).await?.unwrap();

    // for column in Column::from_buffer(&row.columns_data).unwrap() {
    //     println!("{}", column.into_value());
    // }

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

    let row = table.first_or_default_by_id(4).await?.unwrap();

    // for column in Column::from_buffer(&row.columns_data).unwrap() {
    //     println!("{}", column.into_value());
    // }

    //let row = table.first_or_default_by_column(0, 21)?.unwrap();

    // for column in Column::from_buffer(&row.columns_data).unwrap() {
    //     println!("{}", column.into_value());
    // }

    let mut stopwatch = Stopwatch::new();

    stopwatch.start();
    for _ in 0..5 {
        let query_evaluation = parse_rql(&format!(r#"
            BEGIN
            SELECT FROM NAME_DOESNT_MATTER_FOR_NOW
            WHERE COL(0) = 21 + 13905
            END
        "#)).unwrap();

        let _row = table.first_or_default_by_query(query_evaluation).await?;
        // for column in Column::from_buffer(&row.columns_data).unwrap() {
        //     println!("{}", column.into_value());
        // }
    }
    stopwatch.stop();
    println!("{}ms", stopwatch.elapsed_ms());
    stopwatch.reset();
    println!("DONE");

    let mut buffer = String::new();
    stdin().read_line(&mut buffer).unwrap();

    Ok(())
}