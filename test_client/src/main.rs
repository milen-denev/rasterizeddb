use std::io::stdin;

use rasterizeddb_core::{client::DbClient, core::support_types::ReturnResult};
use stopwatch::Stopwatch;

#[tokio::main(flavor = "multi_thread")]
#[allow(unreachable_code)]
async fn main() -> std::io::Result<()> {
    let client = DbClient::new(Some("127.0.0.1")).await.unwrap();

    let create_result = client.execute_query("BEGIN CREATE TABLE test_db (FALSE, FALSE) END").await;

    println!("Create result: {:?}", create_result);

    let rebuild_result = client.execute_query("BEGIN SELECT FROM test_db REBUILD_INDEXES END").await.unwrap();

    println!("Rebuild indexes result: {:?}", rebuild_result);

    // for i in 0..1_500_000 {
    //     let query = format!(
    //         r#"
    //         BEGIN
    //         INSERT INTO test_db (COL(F64), COL(F32), COL(I32), COL(STRING), COL(CHAR), COL(U128))
    //         VALUES ({}, {}, {}, {}, 'D', {})
    //         END
    //     "#, i, i as f64 * -1 as f64, i as i32, "Milen Denev", u128::MAX - i);
                
    //     let db_response = client.execute_query(&query).await.unwrap();

    //     // println!("Insert result: {:?}", db_response);
    // }

    println!("Done inserting rows.");

    let mut stopwatch = Stopwatch::new();
    stopwatch.start();

    let query2 = format!(
        r#"
        BEGIN
        SELECT FROM test_db
        WHERE COL(0,I32) = 12440 
        LIMIT 1000000
        END
    "#);

    let db_response2 = client.execute_query(&query2).await.unwrap();
    let result = DbClient::extract_rows(db_response2).unwrap().unwrap();

    stopwatch.stop();

    // for row in result.iter() {
    //     let columns = row.columns().unwrap();
    //     for (i, column) in columns.iter().enumerate() {
    //         println!("Column ({}): {}", i, column.into_value());
    //     }
    // }

    println!("Elapsed {:?}", stopwatch.elapsed());

    match result {
        ReturnResult::Rows(rows) => {
            println!("Total rows: {}", rows.len());
        },
        ReturnResult::HtmlView(html_string) => {
            println!("Html string: {}", html_string);
        }
    }

    println!("Press any key to continue...");

    let mut buffer = String::new();
    stdin().read_line(&mut buffer).unwrap();

    return Ok(());
}
