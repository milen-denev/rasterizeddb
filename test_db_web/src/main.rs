use axum::body::Body;
use axum::http::header;
use axum::Router;
use axum::{response::Response, routing::get};
use rasterizeddb_core::client::DbClient;

static CLIENT: async_lazy::Lazy<DbClient> =
    async_lazy::Lazy::new(|| {
        Box::pin(async {
            let client = DbClient::with_pool_settings(Some("127.0.0.1"), 10, 1000).await.unwrap();
            client.execute_query("BEGIN CREATE TABLE test_db (FALSE, FALSE) END").await.unwrap();
            client.execute_query("BEGIN SELECT FROM test_db REBUILD_INDEXES END").await.unwrap();
            client
        })
    });

#[tokio::main]
async fn main() {
    let app = Router::new()
        .route("/", get(index))
        .route("/get", get(query));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:80").await.unwrap();

    axum::serve(listener, app).await.unwrap();
}

#[allow(static_mut_refs)]
async fn index() -> Response {
    let x = rand::random_range(-30000..50000 as i32);
    let y = rand::random_range(-20000..20000 as i32);
    let z = rand::random_range(-100_000.0..100_000.0 as f64);

    let client = CLIENT.force().await;

    let query = format!(
        r#"
        BEGIN
        INSERT INTO test_db (COL(I32), COL(I32), COL(STRING), COL(F64))
        VALUES ({}, {}, '{}', {})
        END
    "#, x as f64, y, "Hello, World!", z);
                
    let db_response = client.execute_query(&query).await;

    if db_response.is_ok() {
        return Response::new("".into());
    } else {
        return Response::new("Error occurred".into());
    }
}

#[allow(static_mut_refs)]
async fn query() -> Response {
    let x = rand::random_range(-30000..50000 as i32);
    //let y = rand::random_range(-10..10 as i32);

    //OR COL(1) = {x} / {y} * {z}

    let query_evaluation = &format!(
        r#"
        BEGIN
        SELECT FROM test_db
        WHERE COL(0,I32) < {x}
        LIMIT 1000
        END
    "#
    );

    let client = CLIENT.force().await;

    let db_response2 = client.execute_query(&query_evaluation).await.unwrap();
    let result = DbClient::extract_rows(db_response2).unwrap().unwrap_or_default();

    let mut output = String::new();

    for row in result.iter() {
        let columns = row.columns().unwrap();
        for (i, column) in columns.iter().enumerate() {
            output.push_str(&format!("Column ({}): {} | ", i, column.into_value()));
        }
        output.push_str("\n");
    }

    Response::builder().header(header::CONTENT_TYPE, "text/html; charset=UTF-8").body(Body::new(output)).unwrap()
}
