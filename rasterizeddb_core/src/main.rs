use std::sync::Arc;

use log::LevelFilter;
use rasterizeddb_core::core::database_v2::Database;
use tokio::runtime::Builder;

#[allow(unreachable_code)]
#[allow(static_mut_refs)]
fn main() -> std::io::Result<()> {
    // 64 MiB
    let stack_size = 64 * 1024 * 1024;

    let rt = Builder::new_multi_thread()
        .worker_threads(16)
        .thread_stack_size(stack_size)
        .enable_all()
        .build()?;

     rt.block_on(async {
        env_logger::Builder::new()
            .filter_level(LevelFilter::Error)
            .init();

            
        let database = Database::new("G:\\Databases\\Production").await;
        let arc_database = Arc::new(database);
        _ = Database::start_db(arc_database).await;
    });

    return Ok(());
}
