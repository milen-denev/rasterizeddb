use std::sync::Arc;
use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};

use log::LevelFilter;
use rasterizeddb_core::core::database::Database;

use rasterizeddb_core::core::table::Table;
use rasterizeddb_core::rql::parser::{parse_rql, ParserResult};
use rasterizeddb_core::{
    core::storage_providers::file_sync::LocalStorageProvider,
    EMPTY_BUFFER,
};

use tokio::fs::remove_file;

#[tokio::main(flavor = "multi_thread")]
#[allow(unreachable_code)]
async fn main() -> std::io::Result<()> {
    env_logger::Builder::new()
        .filter_level(LevelFilter::Error)
        .init();
    
    #[cfg(target_arch = "x86_64")]
    {
        let empty_buffer_ptr = EMPTY_BUFFER.as_ptr();
        unsafe { _mm_prefetch::<_MM_HINT_T0>(empty_buffer_ptr as *const i8) };
    }

    let db_file = "C:\\db\\";
    let io_sync = LocalStorageProvider::new(
        db_file,
        None,
    )
    .await;

    let _database = Database::new(io_sync).await?;
    _ = tokio::spawn(Database::start_async(Arc::new(_database))).await?;

    return Ok(());

    _ = remove_file("C:\\db\\test.db").await;

    let io_sync = LocalStorageProvider::new(
        db_file,
        Some("test.db"),
    )
    .await;

    let mut table = Table::init("test_db".into(), io_sync, false, false).await.unwrap();

    let insert_query_evaluation = format!(
        r#"
        BEGIN
        INSERT INTO test_db (COL(I32), COL(STRING))
        VALUES (5882, 'This is a test2')
        VALUES (5882, 'This is a test3')
        VALUES (5882, 'This is a test4')
        VALUES (5882, 'This is a test5')
        END
    "#);

    let query_evaluation = parse_rql(&insert_query_evaluation).unwrap();
  
    match query_evaluation.parser_result {
        ParserResult::InsertEvaluationTokens(insert) => {
            for row in insert.rows {
                table.insert_row(row).await;
            }
        },
        _ => {
            println!("Unsupported database action.");
        }
    };

    println!("DONE inserting rows.");

    table.rebuild_in_memory_indexes().await;

    println!("DONE building indexes.");

    let query_evaluation = parse_rql(&format!(
        r#"
        BEGIN
        SELECT FROM test_db
        WHERE COL(1,STRING) = 'Foo'
        LIMIT 50
        END
    "#
    )).unwrap();

    let _result = table.execute_query(query_evaluation.parser_result).await.unwrap();

    //println!("total rows from query {:?}", rows.unwrap().len());

    return Ok(());
}