# rasterizeddb_embedded

Run [RasterizedDB](https://github.com/milen-denev/rasterizeddb) inside your Rust application.

The full engine runs in-process, on a thread of its own with the 32 MiB stacks the server uses. You talk to it through [`rasterizeddb_client`](https://docs.rs/rasterizeddb_client), so typed queries, transactions, the ORM, migrations and the key/value API all work unchanged. You can start with an embedded database and later point the same code at a standalone server.

```toml
[dependencies]
rasterizeddb_embedded = "1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

```rust
use rasterizeddb_embedded::Builder;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Builder::open("./data").start().await?;   // or Builder::in_memory()
    let client = db.connect().await?;

    client.execute("CREATE TABLE IF NOT EXISTS notes (id BIGINT, body TEXT)", &[]).await?;
    client.execute("INSERT INTO notes VALUES (1, 'hello')", &[]).await?;
    let rows = client.query("SELECT body FROM notes WHERE id = 1", &[]).await?;
    println!("{}", rows.iter().next().unwrap().get::<String>(0)?);

    db.shutdown().await?;   // drains, seals the index snapshots, checkpoints the WAL
    Ok(())
}
```

## Settings

Everything the server binary takes as a flag, `Builder` takes as a method. A method's documentation names the flag it stands for and what it costs.

```rust
use rasterizeddb_embedded::{Builder, IndexResidency};

let db = Builder::open("./data")
    .worker_threads(4)                        // the engine's runtime threads
    .query_threads(4)                         // workers one query may use
    .malloc_arenas(4)                         // fewer allocator arenas: less memory, less throughput
    .index_residency(IndexResidency::Disk)    // indexes on disk behind a 64 MiB cache
    .join_memory_mb(256)                      // a join past this fails instead of exhausting memory
    .synchronous_commit(false)                // commits return before their fdatasync
    .config("auto_compact.interval_secs", "300")
    .start()
    .await?;
```

| Area | Methods |
|---|---|
| Memory | `low_memory` (a preset: 4 arenas, indexes on disk, 256 MiB a join), `malloc_arenas`, `idle_trim`, `tune_allocator`, `index_residency`, `index_cache_mb`, `index_delta_mb`, `join_memory_mb`, `batch_size`, `query_cache`, `query_cache_entries`, `kv_max_memory`, `kv_eviction` |
| Durability | `synchronous_commit`, `wal_writer_delay`, `wal_checkpoint_bytes`, `wal_prealloc_kb`, `kv_persist`, `kv_save_interval` |
| Threads and CPU | `worker_threads`, `query_threads`, `simd`, `scan_prefilter` |
| Storage | `index_storage`, `auto_compact`, `json_shadow_tables`, `settle_status`, `partition_open_budget`, `raise_descriptor_limit` |
| Everything else | `config(key, value)`: any key `SHOW CONFIG KEYS` lists (`filter.*`, `auto_compact.*`, `index_snapshot.*`, `partition.*`, `query_stats.*`, `vacuum.enabled`, …) |

Three things to know:

- **Hot keys change on the running engine.** `SET CONFIG wal.synchronous_commit = off` through any client works as it does on a server, and `SHOW CONFIG` lists every key with its value. With a database directory the change is also saved to `rasterizeddb.conf` there, so the next start keeps it, except for `replication.*` and `vacuum.enabled`, which act on one database. The Builder only sets where a hot key starts, and is not saved; a key the Builder gives wins over a saved one.
- **Startup keys are read once.** `SET CONFIG` on one (`server.*`, `index.*`, `wal.checkpoint_bytes`, `wal.prealloc_kb`, `cache.enabled`, `cache.size`) saves it to `rasterizeddb.conf` in the database directory, and the next start on that directory applies it. A value given to the Builder wins over a saved one.
- **One engine at a time in a process.** Settings are the process's, and `SET CONFIG` saves to the running engine's directory, so `start` refuses while another engine runs (`ErrorKind::AlreadyExists`); serve several databases from one engine with `CREATE DATABASE`. A start after a stop is fine, but a startup value the first engine fixed (batch size, worker permits, …) stays fixed: asking for another fails rather than running with a value nobody asked for.
- **Your signals stay yours.** Starting turns the engine's own Ctrl+C/SIGTERM handling off for the process, including a replication listener started later by `REPLICATION ENABLE`. Stop the engine with `shutdown()` (or `shutdown_blocking()`), or by dropping it, which blocks until the log is checkpointed.

Memory the engine frees goes back to the operating system once it has been idle for a few seconds (`idle_trim`, on by default; glibc only).

## Platforms

| | Endpoint | Authentication |
|---|---|---|
| Linux, other unix | a unix socket in a fresh `0700` directory (socket `0600`) | none needed: only your OS user can open it |
| Windows | TLS on `127.0.0.1:<port>` | `Builder::password(user, password)` (required), or `allow_unauthenticated_loopback(true)` |

On x86-64 the engine needs an AVX2 CPU (x86-64-v3). `start` refuses on older CPUs rather than crash. For full speed, build with `RUSTFLAGS="-C target-cpu=x86-64-v3"`. ARM64 (aarch64) needs nothing extra.

A database directory written by the `rasterizeddb` server binary opens here, and the reverse. Only one engine may have a directory open at a time: it holds an operating-system lock on `rasterizeddb.lock` there, so a second process, the server binary, or another engine in this process is refused with "already open" instead of corrupting the directory. The lock ends with its holder, even a killed one.

## License

GPL-3.0-only.
