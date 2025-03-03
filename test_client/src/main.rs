use std::io::stdin;

use rasterizeddb_core::client::DbClient;
use stopwatch::Stopwatch;


#[tokio::main(flavor = "multi_thread")]
#[allow(unreachable_code)]
async fn main() -> std::io::Result<()> {
    let mut client = DbClient::new(Some("127.0.0.1")).await.unwrap();

    //client.execute_query("BEGIN CREATE TABLE database (FALSE, FALSE) END").await.unwrap();
    client.execute_query("BEGIN SELECT FROM database REBUILD_INDEXES END").await.unwrap();

    let mut stopwatch = Stopwatch::new();
    stopwatch.start();

    let query = format!(
        r#"
        BEGIN
        SELECT FROM database
        WHERE COL(0) = 4999999
        LIMIT 50
        END
    "#);

    let db_response = client.execute_query(&query).await.unwrap();
    let result = DbClient::extract_rows(db_response).unwrap();

    stopwatch.stop();

    // for row in result.unwrap().unwrap().iter() {
    //     let columns = row.columns().unwrap();
    //     for (i, column) in columns.iter().enumerate() {
    //         println!("Column ({}): {}", i, column.into_value());
    //     }
    // }

    println!("Elapsed {:?}", stopwatch.elapsed());

    println!("Total rows: {}", result.unwrap().len());

    println!("Press any key to continue...");

    let mut buffer = String::new();
    stdin().read_line(&mut buffer).unwrap();

    return Ok(());
}
