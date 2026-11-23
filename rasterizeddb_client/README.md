# rasterizeddb_client

A client SDK and ORM for [RasterizedDB](https://github.com/milen-denev/rasterizeddb)'s
native protocol, without compiling the engine.

- **SQL**: `$n` parameters rendered safely, typed rows (`fetch_all::<(i64, String)>`),
  every common PostgreSQL type including UUIDs, timestamps, intervals, exact
  decimals, JSON and arrays.
- **Administration**: S3 and directory stores, table placement, indexes,
  partitions, roles, backups and configuration as typed calls.
- **ORM**: `#[derive(Model)]`, a typed query builder, inserts and upserts
  that return what was stored, relations loaded without N+1, aggregates,
  keyset pages and streams, optimistic locking, server timestamps, soft
  delete, hooks, partitioned tables, a read-through cache and atomic bulk
  updates, on a client or (coming) inside a transaction.
- **Migrations**: steps, baselines and rollbacks, with tables kept in sync
  with their structs. They can run when the client connects (`auto_migrate`).
- **Key/value**: the server's Redis-compatible cache, typed, with closure
  queries and models stored as hashes.

```rust
use rasterizeddb_client::prelude::*;

#[derive(Model, Debug)]
struct Note {
    #[rdb(primary_key, serial)]
    id: i64,
    body: String,
    pinned: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::connect("unix:/run/rdb/rdb.sock", Options::default()).await?;
    client.create_table::<Note>().await?;

    let note = client.insert(&Note { id: 0, body: "hello".into(), pinned: true }).await?;
    let pinned = Note::query().filter(Note::pinned.eq(true)).all(&client).await?;

    let hits = client.kv().map::<String, i64>("hits");
    hits.incr_by(&"/".to_string(), 1).await?;
    Ok(())
}
```

The [documentation](https://docs.rs/rasterizeddb_client) has a guide chapter
for each layer. [`examples/`](examples) holds runnable programs:
`RDB_ADDR=unix:/path/to/rdb.sock cargo run --example orm`.

| feature | default | adds |
|---|---|---|
| `derive` | yes | `#[derive(Model)]`, `#[derive(FromRow)]`, `#[derive(SqlEnum)]` |
| `chrono` | | `chrono` date and time types |
| `uuid` | | `uuid::Uuid` |
| `serde_json` | | `serde_json::Value`, `#[rdb(json)]` fields, JSON cache values |

## Testing

`cargo test` runs the unit tests and every documentation example (the
examples use `#[derive(Model)]`, so they need the default features). The
integration tests in `tests/` run against a real server when
`RDB_SERVER_BIN` names a server binary; without it they say so and pass.
