use std::io::stdin;

use rasterizeddb_core::{
    core::{
        column::Column, row::InsertRow, storage_providers::{file_sync::LocalStorageProvider, memory::MemoryStorageProvider}, table::Table
    }, rql::parser::parse_rql
};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_BACKTRACE","0");

    let io_sync = LocalStorageProvider::new(
        "C:\\Users\\mspc6\\OneDrive\\Professional\\Desktop",
        "database.db"
    );

    //let io_sync = MemoryStorageProvider::new();

    let mut table = Table::init(io_sync, false, false).unwrap();

    for i in 0..100 {
        let mut c1 = Column::new(i).unwrap();
        let mut c2 = Column::new(i * -1).unwrap();

        let mut columns_buffer: Vec<u8> = Vec::with_capacity(
            c1.len() + 
            c2.len()
        );
    
        columns_buffer.append(&mut c1.into_vec().unwrap());
        columns_buffer.append(&mut c2.into_vec().unwrap());
    
        let insert_row = InsertRow {
            columns_data: columns_buffer
        };
    
        table.insert_row(insert_row).await;
    }

    table.rebuild_in_memory_indexes();

    let row = table.first_or_default_by_id(10)?.unwrap();

    for column in Column::from_buffer(&row.columns_data).unwrap() {
        println!("{}", column.into_value());
    }

    let row = table.first_or_default_by_column(0, 10)?.unwrap();

    for column in Column::from_buffer(&row.columns_data).unwrap() {
        println!("{}", column.into_value());
    }

    let query_evaluation = parse_rql(&format!(r#"
        BEGIN
        SELECT FROM NAME_DOESNT_MATTER_FOR_NOW
        WHERE COL(0) > 10
        END
    "#)).unwrap();

    let row = table.first_or_default_by_query(query_evaluation).await?.unwrap();

    for column in Column::from_buffer(&row.columns_data).unwrap() {
        println!("{}", column.into_value());
    }

    println!("DONE");

    let mut buffer = String::new();
    stdin().read_line(&mut buffer).unwrap();

    Ok(())
}