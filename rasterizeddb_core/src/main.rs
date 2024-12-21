use rasterizeddb_core::core::{column::Column, row::InsertRow, storage_providers::{
    file_async::LocalStorageProviderAsync, 
    file_sync::LocalStorageProvider}, 
    table::Table
};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_BACKTRACE","0");

    let io_sync = LocalStorageProvider::new(
        "C:\\Users\\mspc6\\OneDrive\\Professional\\Desktop",
        "database.db"
    );

    let io_async = LocalStorageProviderAsync::new(
        "C:\\Users\\mspc6\\OneDrive\\Professional\\Desktop",
        "database.db"
    ).await;

    let mut table = Table::init(io_sync, io_async, false, false).unwrap();

    let mut c1 = Column::new(50).unwrap();

    let mut columns_buffer: Vec<u8> = Vec::with_capacity(
        c1.len()
    );

    columns_buffer.append(&mut c1.into_vec().unwrap());

    let mut insert_row = InsertRow {
        columns_data: columns_buffer
    };

    table.insert_row(&mut insert_row).await;

    let io_sync = LocalStorageProvider::new(
        "C:\\Users\\mspc6\\OneDrive\\Professional\\Desktop",
        "database.db"
    );

    let io_async = LocalStorageProviderAsync::new(
        "C:\\Users\\mspc6\\OneDrive\\Professional\\Desktop",
        "database.db"
    ).await;

    let mut table = Table::init(io_sync, io_async, false, false).unwrap();

    let row = table.first_or_default_by_id(2)?.unwrap();

    for column in Column::from_buffer(&row.columns_data).unwrap() {
        println!("{}", column.into_value());
    }

    Ok(())
}