use std::sync::Arc;
use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
use rasterizeddb_core::core::database::Database;
use rasterizeddb_core::{
    core::storage_providers::file_sync::LocalStorageProvider,
    EMPTY_BUFFER,
};

use tokio::sync::RwLock;

#[tokio::main(flavor = "multi_thread")]
#[allow(unreachable_code)]
async fn main() -> std::io::Result<()> {
    #[cfg(target_arch = "x86_64")]
    {
        let empty_buffer_ptr = EMPTY_BUFFER.as_ptr();
        unsafe { _mm_prefetch::<_MM_HINT_T0>(empty_buffer_ptr as *const i8) };
    }

    unsafe { std::env::set_var("RUST_BACKTRACE", "0") };
    unsafe { std::env::set_var("RUST_LOG", "error") };

    env_logger::init();

    let db_file = "C:\\db\\";
    let io_sync = LocalStorageProvider::new(
        db_file,
        "database.db",
    )
    .await;

    let database = Database::new(io_sync).await?;
    _ = tokio::spawn(Database::start_async(Arc::new(RwLock::new(database)))).await?;

    return Ok(());
}