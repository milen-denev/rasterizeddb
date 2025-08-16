use log::LevelFilter;
use rasterizeddb_core::{client::DbClient, core::{database::QueryExecutionResult, db_type::DbType, row_v2::row::vec_into_rows}};

#[tokio::main(flavor = "multi_thread")]
#[allow(unreachable_code)]
async fn main() -> std::io::Result<()> {
    env_logger::Builder::new()
        .filter_level(LevelFilter::Debug)
        .init();

    let client = DbClient::new(Some("127.0.0.1")).await.unwrap();

    // let _query = r##"
    //     CREATE TABLE employees (
    //         id UBIGINT,
    //         name VARCHAR,
    //         position VARCHAR,
    //         salary REAL
    //     );
    // "##;

    // let create_result = client.execute_query(_query).await;

    // println!("Create result: {:?}", create_result);

    // let query = r##"
    //     INSERT INTO employees (id, name, position, salary)
    //     VALUES (1, 'Alice', 'Engineer', 75000.0);
    // "##;

    // let insert_result = client.execute_query(query).await;

    // println!("Insert result: {:?}", insert_result);

    // let query = r##"
    //     INSERT INTO employees (id, name, position, salary)
    //     VALUES (2, 'Ben', 'Electrician', 65000.0);
    // "##;

    // let insert_result = client.execute_query(query).await;

    // println!("Insert result: {:?}", insert_result);

    let query = r##"
        SELECT id, name, position, salary FROM employees
        WHERE id >= 0
    "##;

    let select_result = client.execute_query(query).await;

    println!("Select result: {:?}", select_result);

    match select_result.unwrap() {
        QueryExecutionResult::RowsResult(rows) => {
            let rows = vec_into_rows(&rows).unwrap();
            for row in rows {
                for column in &row.columns {
                    if column.column_type == DbType::STRING {
                        let value = String::from_utf8(column.data.into_slice().to_vec()).unwrap();
                        println!("Name / Position: {}", value);
                    } else if column.column_type == DbType::U64 {
                        println!("Id: {}", u64::from_le_bytes(column.data.into_slice().try_into().unwrap()));
                    } else if column.column_type == DbType::F32 {
                        println!("Salary: {}", f32::from_le_bytes(column.data.into_slice().try_into().unwrap()));
                    }
                }
            }
        }
        QueryExecutionResult::Error(err) => {
            eprintln!("Error occurred: {}", err);
        }
        _ => {
            eprintln!("Unexpected result type");
        }
    }

    // let rebuild_result = client.execute_query("BEGIN SELECT FROM test_db REBUILD_INDEXES END").await.unwrap();

    // println!("Rebuild indexes result: {:?}", rebuild_result);

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

    // println!("Done inserting rows.");

    // let mut stopwatch = Stopwatch::new();
    // stopwatch.start();

    // let query2 = format!(
    //     r#"
    //     BEGIN
    //     SELECT FROM test_db
    //     WHERE COL(0,I32) = 12440 
    //     LIMIT 1000000
    //     END
    // "#);

    // let db_response2 = client.execute_query(&query2).await.unwrap();
    // let result = DbClient::extract_rows(db_response2).unwrap().unwrap();

    // stopwatch.stop();

    // for row in result.iter() {
    //     let columns = row.columns().unwrap();
    //     for (i, column) in columns.iter().enumerate() {
    //         println!("Column ({}): {}", i, column.into_value());
    //     }
    // }

    // println!("Elapsed {:?}", stopwatch.elapsed());

    // match result {
    //     ReturnResult::Rows(rows) => {
    //         println!("Total rows: {}", rows.len());
    //     },
    //     ReturnResult::HtmlView(html_string) => {
    //         println!("Html string: {}", html_string);
    //     }
    // }

    // println!("Press any key to continue...");

    // let mut buffer = String::new();
    // stdin().read_line(&mut buffer).unwrap();

    return Ok(());
}
