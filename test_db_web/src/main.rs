use axum::Router;
use axum::{response::Response, routing::get};
use rasterizeddb_core::core::{
    column::Column, row::InsertOrUpdateRow, storage_providers::file_sync::LocalStorageProvider,
    table::Table,
};

use rasterizeddb_core::rql::parser::parse_rql;

static mut TABLE: async_lazy::Lazy<Table<LocalStorageProvider>> =
    async_lazy::Lazy::const_new(|| {
        Box::pin(async {
            let io_sync = LocalStorageProvider::new(
                "C:\\Users\\mspc6\\OneDrive\\Professional\\Desktop",
                "database.db",
            )
            .await;

            let mut table = Table::init(io_sync, false, false).await.unwrap();

            table.rebuild_in_memory_indexes().await;

            table
        })
    });

#[tokio::main]
async fn main() {
    let app = Router::new()
        .route("/", get(index));
        //.route("/get", get(query));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:80").await.unwrap();

    axum::serve(listener, app).await.unwrap();
}

#[allow(static_mut_refs)]
async fn index() -> Response {
    let x = rand::random_range(-10..10 as i32);
    let y = rand::random_range(-10..10 as i32);
    let z = rand::random_range(-100_000.0..100_000.0 as f64);

    let c1_update = Column::new(x + y).unwrap();
    let c2_update = Column::new((((x as f64) / (y as f64) as f64) * z as f64) as f64).unwrap();
    let large_string = "A".repeat(1024);
    let c3_update = Column::new(&large_string).unwrap();

    let mut columns_buffer_update: Vec<u8> = Vec::with_capacity(
        c1_update.len() + c2_update.len(), //c3_update.len()
    );
    columns_buffer_update.append(&mut c1_update.content.to_vec());
    columns_buffer_update.append(&mut c2_update.content.to_vec());
    columns_buffer_update.append(&mut c3_update.content.to_vec());

    let update_row = InsertOrUpdateRow {
        columns_data: columns_buffer_update,
    };

    let table = unsafe { TABLE.force_mut().await };

    _ = table.insert_row(update_row).await;

    Response::new("".into())
}

#[allow(static_mut_refs)]
async fn query() -> Response {
    let x = rand::random_range(-10..10 as i32);
    let y = rand::random_range(-10..10 as i32);

    //OR COL(1) = {x} / {y} * {z}

    let query_evaluation = parse_rql(&format!(
        r#"
        BEGIN
        SELECT FROM NAME_DOESNT_MATTER_FOR_NOW
        WHERE COL(0) > {x} OR COL(1) > {y} OR COL(0) > -1000
        LIMIT 1000
        END
    "#
    ))
    .unwrap();

    let table = unsafe { TABLE.force_mut().await };

    let result = table
        .execute_query(query_evaluation.parser_result)
        .await
        .unwrap();

    if result.is_some() {
        Response::builder().status(200).body("".into()).unwrap()
    } else {
        Response::builder().status(404).body("".into()).unwrap()
    }
}
