use std::io::stdin;

use rasterizeddb_core::client::DbClient;
use stopwatch::Stopwatch;

#[tokio::main(flavor = "multi_thread")]
#[allow(unreachable_code)]
async fn main() -> std::io::Result<()> {
    let mut client = DbClient::new(Some("127.0.0.1")).await.unwrap();

    let create_result = client.execute_query("BEGIN CREATE TABLE test_db (FALSE, FALSE) END").await;

    println!("Create result: {:?}", create_result);

    client.execute_query("BEGIN SELECT FROM test_db REBUILD_INDEXES END").await.unwrap();

    let mut stopwatch = Stopwatch::new();
    stopwatch.start();

    let query1 = format!(
        r#"
        BEGIN
        INSERT INTO test_db (COL(I32), COL(STRING))
        VALUES (5882, 'This is a test2')
        END
    "#);

    let query2 = format!(
        r#"
        BEGIN
        SELECT FROM test_db
        WHERE COL(0) = 5882
        LIMIT 50
        END
    "#);

    let db_response1 = client.execute_query(&query1).await.unwrap();

    println!("Insert result: {:?}", db_response1);

    let db_response2 = client.execute_query(&query2).await.unwrap();
    let result = DbClient::extract_rows(db_response2).unwrap().unwrap();

    stopwatch.stop();

    for row in result.iter() {
        let columns = row.columns().unwrap();
        for (i, column) in columns.iter().enumerate() {
            println!("Column ({}): {}", i, column.into_value());
        }
    }

    println!("Elapsed {:?}", stopwatch.elapsed());

    println!("Total rows: {}", result.len());

    println!("Press any key to continue...");

    let mut buffer = String::new();
    stdin().read_line(&mut buffer).unwrap();

    return Ok(());
}
