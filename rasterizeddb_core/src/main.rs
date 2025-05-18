use std::sync::Arc;
use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};

use futures::future::join_all;
use itertools::Either;
use log::LevelFilter;
use rasterizeddb_core::core::database::Database;

use rasterizeddb_core::core::db_type::DbType;
use rasterizeddb_core::core::mock_helpers::{get_row_fetch_i32, i32_column};
use rasterizeddb_core::core::row_v2::row::Row;
use rasterizeddb_core::core::row_v2::row_pointer::RowPointerIterator;
use rasterizeddb_core::core::row_v2::transformer::{ColumnTransformer, ColumnTransformerType, ComparerOperation};

use rasterizeddb_core::core::table::Table;
use rasterizeddb_core::rql::parser::{parse_rql, ParserResult};
use rasterizeddb_core::{
    core::storage_providers::file_sync::LocalStorageProvider,
    EMPTY_BUFFER,
};

use stopwatch::Stopwatch;
use tokio::fs::remove_file;
use tokio::sync::{mpsc, Semaphore};
use tokio::task;

// CREATE Arc<(TUPLE)>
// TO REDUCE REFERENCE COUNTING

#[tokio::main(flavor = "multi_thread")]
#[allow(unreachable_code)]
async fn main() -> std::io::Result<()> {
    let stdin = std::io::stdin();
    let mut buffer = String::new();
    println!("Press Enter to start...");
    stdin.read_line(&mut buffer).unwrap();

    const SEARCH_VALUE: i32 = 1000;

    //let mut mock_io_pointers = MockStorageProvider::new().await;
    //let mut mock_io_rows = MockStorageProvider::new().await;

    let mut mock_io_pointers = LocalStorageProvider::new("G:\\Databases\\Test_Database", Some("pointers.db")).await;
    let mock_io_rows = LocalStorageProvider::new("G:\\Databases\\Test_Database", Some("rows.db")).await;

    // println!("{}", mock_io_pointers.get_location().unwrap());
    // println!("{}", mock_io_rows.get_location().unwrap());

    #[cfg(feature = "enable_long_row")]
    let cluster = 0;

    // let last_id = AtomicU64::new(1_000_000);
    // let table_length = AtomicU64::new(mock_io_rows.get_len().await);

    //let row_write = create_row_write(true);
    //let custom_row = create_row_write_custom_i32(SEARCH_VALUE, true);

    // for _i in 0..10_000_000 {
    //     _ = RowPointer::write_row(
    //         &mut mock_io_pointers, 
    //         &mut mock_io_rows, 
    //         &last_id, 
    //         &table_length, 
    //         #[cfg(feature = "enable_long_row")]
    //         cluster, 
    //         &row_write
    //     ).await;
    // }

    // let result = RowPointer::write_row(
    //     &mut mock_io_pointers, 
    //     &mut mock_io_rows, 
    //     &last_id, 
    //     &table_length, 
    //     #[cfg(feature = "enable_long_row")]
    //     cluster, 
    //     &custom_row
    // ).await;

    // if let Err(e) = result {
    //     println!("Error writing row: {}", e);
    // }

    let mut iterator = RowPointerIterator::new(&mut mock_io_pointers).await.unwrap();
    let mut stopwatch = Stopwatch::start_new();
    let row_fetch = get_row_fetch_i32();
    let search_value = SEARCH_VALUE;
    let search_value_block = i32_column(search_value);
    let (tx, mut rx) = mpsc::unbounded_channel::<Row>();

    let arc_tuple = Arc::new((Semaphore::new(16), mock_io_rows, row_fetch));

    // Collect handles for all batch tasks
    let mut batch_handles = Vec::new();

    // Process batches
    while let Ok(pointers) = iterator.next_row_pointers().await {
        if pointers.is_empty() {
            break;
        }

        let tuple_clone = arc_tuple.clone();

        let tx_clone = tx.clone();
        let search_value_block_clone = search_value_block.clone();
        //let i_clone = i.clone();

        // Spawn a task for this entire batch of pointers
        let batch_handle = task::spawn(async move {
            // Acquire a permit for this batch
            let batch_permit = tuple_clone.0.acquire().await.unwrap();

            let mut row = Row::default();

            // Process each pointer in this batch (same as your original code)
            for pointer in pointers {
                let tuple_clone_2 = tuple_clone.clone();
                let (_, io_clone, row_fetch) = &*tuple_clone_2;

                let search_value_block_clone_2 = search_value_block_clone.clone();
             
                pointer.fetch_row_reuse_async(io_clone, row_fetch, &mut row).await;
                
                let transformer = ColumnTransformer::new(
                    DbType::I32,
                    search_value_block_clone_2,
                    row.columns[0].data.clone(),
                    ColumnTransformerType::ComparerOperation(ComparerOperation::Equals),
                    None
                );
                
                let result = transformer.transform_single();
                
                if let Either::Right(found) = result {
                    if found {
                        println!("Found row with ID: {}", pointer.id);
                        tx_clone.send(Row::clone_from_mut_row(&row)).unwrap();
                    }
                }

            }

            // Release the batch semaphore permit
            drop(batch_permit);
        });
        
        batch_handles.push(batch_handle);
    }

    // Wait for all batch tasks to complete
    join_all(batch_handles).await;

    // Drop the sender to signal that no more rows will be sent
    drop(tx);

    // Aggregator remains the same
    let aggregator_handle = tokio::spawn(async move {
        let mut collected_rows = Vec::with_capacity(50);
        
        if !rx.is_empty() {
            rx.recv_many(&mut collected_rows, usize::MAX).await;
        }
        
        drop(rx);
        collected_rows
    });

    // Await the aggregator to finish collecting rows.
    let all_rows = aggregator_handle.await.unwrap();

    println!("Total rows collected: {}", all_rows.len());

    stopwatch.stop();

    println!("Row fetch took: {:?}", stopwatch.elapsed());
    
    let stdin = std::io::stdin();
    let mut buffer = String::new();
    println!("Press Enter to continue...");
    stdin.read_line(&mut buffer).unwrap();

    return Ok(());

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