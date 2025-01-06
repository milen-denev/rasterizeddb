use std::sync::Arc;

use once_cell::sync::Lazy;
use rand::rngs::ThreadRng;
use rand::Rng;
use rasterizeddb_core::core::{
    column::Column, 
    row::InsertOrUpdateRow, 
    storage_providers::file_sync::LocalStorageProvider, 
    table::Table
};
use axum::{response::Response, routing::get};
use axum::{Extension, Router};
use rasterizeddb_core::rql::parser::parse_rql;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::RwLock;

// static mut TABLE: Lazy<Table<LocalStorageProvider>> = Lazy::new(|| {
    
// });

static mut TABLE: async_lazy::Lazy<Table<LocalStorageProvider>> = async_lazy::Lazy::const_new(|| Box::pin(async {
    let io_sync = LocalStorageProvider::new(
        "C:\\Users\\mspc6\\OneDrive\\Professional\\Desktop",
        "database.db"
    ).await;

    let mut table = Table::init(io_sync, false, false).await.unwrap();

    table.rebuild_in_memory_indexes().await;

    table
}));

static mut RANDOM: Lazy<ThreadRng> = Lazy::new(|| {
    rand::rngs::ThreadRng::default()
});

#[tokio::main]
async fn main() {
    let file = Arc::new(RwLock::new(
        tokio::fs::File::open("C:\\Users\\mspc6\\OneDrive\\Professional\\Desktop\\column_data.png")
        .await
        .unwrap()
    ));

    let app = Router::new()
        .route("/", get(index))
        .route("/get", get(query))
        .route("/test", get(test_reading))
        .layer(Extension(file));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:80")
        .await
        .unwrap();

    axum::serve(listener, app).await.unwrap();
}

#[allow(static_mut_refs)]
async fn index() -> Response {
    let x = unsafe { RANDOM.gen_range(-10..10 as i32) } as i32;
    let y = unsafe { RANDOM.gen_range(-10..10 as i32) } as i32;
    let z = unsafe { RANDOM.gen_range(-10..10 as i32) } as i32;

    let mut c1_update = Column::new(x + y).unwrap();
    let mut c2_update = Column::new((((x as f64) / (y as f64) as f64) * z as f64) as f64).unwrap();
    //let large_string = "A".repeat(1024);
    //let mut c3_update = Column::new(&large_string).unwrap();

    let mut columns_buffer_update: Vec<u8> = Vec::with_capacity(
        c1_update.len() + 
        c2_update.len()
        //c3_update.len() 
    );

    columns_buffer_update.append(&mut c1_update.into_vec().unwrap());
    columns_buffer_update.append(&mut c2_update.into_vec().unwrap());
    //columns_buffer_update.append(&mut c3_update.into_vec().unwrap());

    let update_row = InsertOrUpdateRow {
        columns_data: columns_buffer_update
    };

    let table = unsafe { TABLE.force_mut().await };

    _ = table.insert_row(update_row).await;

    Response::new("".into())
}

#[allow(static_mut_refs)]
async fn query() -> Response {
    //let x = unsafe { RANDOM.gen_range(-10..10 as i32) } as i32;
    //let y = unsafe { RANDOM.gen_range(-10..10 as i32) } as i32;
    //let z = unsafe { RANDOM.gen_range(-100_000..100_000) } as f64;

    //OR COL(1) = {x} / {y} * {z}
    
    let query_evaluation = parse_rql(&format!(r#"
            BEGIN
            SELECT FROM NAME_DOESNT_MATTER_FOR_NOW
            WHERE COL(0) > 0
            END
        "#)).unwrap();

    let table = unsafe { TABLE.force_mut().await };

    //let result = table.first_or_default_by_query(query_evaluation).await.unwrap();

    if true {//if result.is_some() {
        Response::builder().status(200).body("".into()).unwrap()
    } else {
        Response::builder().status(404).body("".into()).unwrap()
    }
}

#[allow(static_mut_refs)]
async fn test_reading(reader: Extension<Arc<RwLock<tokio::fs::File>>>) -> Response {
    let reader_clone = reader.clone();
    let mut write_reader = reader_clone.write().await;

    _ = write_reader.seek(std::io::SeekFrom::Start(6)).await;
    let mut vec = vec![0; 8];
    _ = write_reader.read(&mut vec).await;

    Response::new(format!("{:?}", vec).into())
}