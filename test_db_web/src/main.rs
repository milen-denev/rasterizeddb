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
use axum::Router;
use rasterizeddb_core::rql::parser::parse_rql;

static mut TABLE: Lazy<Table<LocalStorageProvider>> = Lazy::new(|| {
    let io_sync = LocalStorageProvider::new(
        "C:\\Users\\mspc6\\OneDrive\\Professional\\Desktop",
        "database.db"
    );

    Table::init(io_sync, false, false).unwrap()
});

static mut RANDOM: Lazy<ThreadRng> = Lazy::new(|| {
    rand::rngs::ThreadRng::default()
});

#[tokio::main]
async fn main() {
    let app = Router::new()
        .route("/", get(index))
        .route("/get", get(query));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:80")
        .await
        .unwrap();

    axum::serve(listener, app).await.unwrap();
}

#[allow(static_mut_refs)]
async fn index() -> Response {
    let x = unsafe { RANDOM.gen_range(-100_000..100_000) } as f64;
    let y = unsafe { RANDOM.gen_range(-100_000..100_000) } as f64;
    let z = unsafe { RANDOM.gen_range(-100_000..100_000) } as f64;

    let mut c1_update = Column::new((x + y) as u64).unwrap();
    let mut c2_update = Column::new((x / y) * z).unwrap();
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

    unsafe { _ = TABLE.insert_row(update_row).await };

    Response::new("".into())
}

#[allow(static_mut_refs)]
async fn query() -> Response {
    let x = unsafe { RANDOM.gen_range(-100_000..100_000) } as f64;
    let y = unsafe { RANDOM.gen_range(-100_000..100_000) } as f64;
    let z = unsafe { RANDOM.gen_range(-100_000..100_000) } as f64;

    let query_evaluation = parse_rql(&format!(r#"
            BEGIN
            SELECT FROM NAME_DOESNT_MATTER_FOR_NOW
            WHERE COL(0) = {x} + {y} OR COL(1) = {x} / {y} * {z}
            END
        "#)).unwrap();

    unsafe { _ = TABLE.first_or_default_by_query(query_evaluation).await; };

    Response::new("".into())
}