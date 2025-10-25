use std::{num::NonZero, sync::Arc};
use std::env;

use log::LevelFilter;
use rasterizeddb_core::{core::database::Database, BATCH_SIZE, MAX_PERMITS_THREADS};

use rasterizeddb_core::configuration::Configuration;

#[allow(unreachable_code)]
#[allow(static_mut_refs)]
#[compio::main]
async fn main() -> std::io::Result<()> {
    // 32 MiB
    let stack_size = 32 * 1024 * 1024;

    // Parse command line arguments
    let args: Vec<String> = env::args().collect();
    let mut config = Configuration {
        location: None,
        batch_size: None,
        concurrent_threads: None,
    };

    // Simple argument parsing: --location <path> --batch_size <n> --concurrent_threads <n>
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--location" => {
                if i + 1 < args.len() {
                    config.location = Some(args[i + 1].clone());
                    i += 1;
                }
            }
            "--batch_size" => {
                if i + 1 < args.len() {
                    if let Ok(val) = args[i + 1].parse::<usize>() {
                        config.batch_size = Some(val);
                    }
                    i += 1;
                }
            }
            "--concurrent_threads" => {
                if i + 1 < args.len() {
                    if let Ok(val) = args[i + 1].parse::<usize>() {
                        config.concurrent_threads = Some(val);
                    }
                    i += 1;
                }
            }
            _ => {}
        }
        i += 1;
    }

    unsafe { MAX_PERMITS_THREADS = config.concurrent_threads.unwrap_or(16) };

    let batch_ptr: *const usize = &BATCH_SIZE;

    #[allow(invalid_reference_casting)]
    let batch_ref: &mut usize = unsafe { &mut *(batch_ptr as *mut usize) };

    *batch_ref = config.batch_size.unwrap_or(1024 * 64);

    let dispatcher = compio::dispatcher::DispatcherBuilder::new()
        .concurrent(true)
        .stack_size(stack_size)
        .worker_threads(NonZero::new(config.concurrent_threads.unwrap_or(unsafe { MAX_PERMITS_THREADS })).unwrap())
        .build()?;

    let config_clone = config.clone();
    let location_clone = config_clone.location.expect("Database location must be provided with --location <path>").clone();

    let result = dispatcher.dispatch(move || {
        async {
            env_logger::Builder::new()
                .filter_level(LevelFilter::Error)
                .init();

            let db_location = location_clone;
            let database = Database::new(&db_location).await;
            let arc_database = Arc::new(database);
            _ = Database::start_db(arc_database).await;
        }
    }).unwrap();

    result.await.expect("Dispatcher task failed");

    Ok(())
}
