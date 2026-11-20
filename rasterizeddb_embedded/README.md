# rasterizeddb_embedded

Run [RasterizedDB](https://github.com/milen-denev/rasterizeddb) inside your Rust application.

The full engine runs in-process, on a thread of its own with the 32 MiB stacks the server uses. You talk to it through [`rasterizeddb_client`](https://docs.rs/rasterizeddb_client), so typed queries, transactions, the ORM, migrations and the key/value API all work unchanged. You can start with an embedded database and later point the same code at a standalone server.

```toml
[dependencies]
rasterizeddb_embedded = "0.2"
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

## Platforms

| | Endpoint | Authentication |
|---|---|---|
| Linux, other unix | a unix socket in a fresh `0700` directory (socket `0600`) | none needed: only your OS user can open it |
| Windows | TLS on `127.0.0.1:<port>` | `Builder::password(user, password)` (required), or `allow_unauthenticated_loopback(true)` |

On x86-64 the engine needs an AVX2 CPU (x86-64-v3). `start` refuses on older CPUs rather than crash. For full speed, build with `RUSTFLAGS="-C target-cpu=x86-64-v3"`. ARM64 (aarch64) needs nothing extra.

A database directory written by the `rasterizeddb` server binary opens here, and the reverse. Only one process may have a directory open at a time.

## License

GPL-3.0-only.
