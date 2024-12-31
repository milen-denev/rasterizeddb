use std::fs::remove_file;
use tokio::runtime;

use rasterizeddb_core::core::{
    column::Column, 
    row::InsertOrUpdateRow, 
    storage_providers::{file_sync::LocalStorageProvider, memory::MemoryStorageProvider}, 
    table::Table
};

pub fn rebuild_indexes_file() {
    _ = remove_file("C:\\Tests\\database.db");

    let rt = runtime::Builder::new_current_thread().build().unwrap();

    rt.block_on(async { 
        let io_sync = LocalStorageProvider::new(
            "C:\\Tests",
            "database.db"
        ).await;

        let mut table = Table::init(io_sync, false, false).await.unwrap();

        for i in 0..500 {
            if i == 450 {
                let mut c1 = Column::new(1000).unwrap();
                let mut c2 = Column::new(i * -1).unwrap();
                let mut c3 = Column::new("This is awesome.").unwrap();
        
                let mut columns_buffer: Vec<u8> = Vec::with_capacity(
                    c1.len() + 
                    c2.len() + 
                    c3.len() 
                );
            
                columns_buffer.append(&mut c1.into_vec().unwrap());
                columns_buffer.append(&mut c2.into_vec().unwrap());
                columns_buffer.append(&mut c3.into_vec().unwrap());
            
                let insert_row = InsertOrUpdateRow {
                    columns_data: columns_buffer
                };
            
                table.insert_row(insert_row).await;
            } else {
                let mut c1 = Column::new(i).unwrap();
                let mut c2 = Column::new(i * -1).unwrap();
                let mut c3 = Column::new("This is also awesome.").unwrap();
        
                let mut columns_buffer: Vec<u8> = Vec::with_capacity(
                    c1.len() + 
                    c2.len() + 
                    c3.len() 
                );
            
                columns_buffer.append(&mut c1.into_vec().unwrap());
                columns_buffer.append(&mut c2.into_vec().unwrap());
                columns_buffer.append(&mut c3.into_vec().unwrap());
            
                let insert_row = InsertOrUpdateRow {
                    columns_data: columns_buffer
                };
            
                table.insert_row(insert_row).await;
            }
        }

        table.rebuild_in_memory_indexes().await;
    });
}

pub fn rebuild_indexes_memory() {
    let rt = runtime::Builder::new_current_thread().build().unwrap();

    rt.block_on(async { 
        let io_sync = MemoryStorageProvider::new();

        let mut table = Table::init(io_sync, false, false).await.unwrap();

        for i in 0..500 {
            if i == 450 {
                let mut c1 = Column::new(1000).unwrap();
                let mut c2 = Column::new(i * -1).unwrap();
                let mut c3 = Column::new("This is awesome.").unwrap();
        
                let mut columns_buffer: Vec<u8> = Vec::with_capacity(
                    c1.len() + 
                    c2.len() + 
                    c3.len() 
                );
            
                columns_buffer.append(&mut c1.into_vec().unwrap());
                columns_buffer.append(&mut c2.into_vec().unwrap());
                columns_buffer.append(&mut c3.into_vec().unwrap());
            
                let insert_row = InsertOrUpdateRow {
                    columns_data: columns_buffer
                };
            
                table.insert_row(insert_row).await;
            } else {
                let mut c1 = Column::new(i).unwrap();
                let mut c2 = Column::new(i * -1).unwrap();
                let mut c3 = Column::new("This is also awesome.").unwrap();
        
                let mut columns_buffer: Vec<u8> = Vec::with_capacity(
                    c1.len() + 
                    c2.len() + 
                    c3.len() 
                );
            
                columns_buffer.append(&mut c1.into_vec().unwrap());
                columns_buffer.append(&mut c2.into_vec().unwrap());
                columns_buffer.append(&mut c3.into_vec().unwrap());
            
                let insert_row = InsertOrUpdateRow {
                    columns_data: columns_buffer
                };
            
                table.insert_row(insert_row).await;
            }
        }

        table.rebuild_in_memory_indexes().await;
    });
}