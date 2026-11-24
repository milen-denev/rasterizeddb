# Client-side sharding

Spreading an application's tenants over several RasterizedDB servers, with
the routing in the client (sharding plan, Track A). Each server is an
ordinary RasterizedDB; nothing in the engine changes. The router adds no
hop: it hands out the shard's own `Client`.

The routing function is route_v1 from `rasterizeddb_router`, the same code
the engine's hash partitioning uses. A tenant's shard here is therefore the
shard the engine will place it on when it shards by itself (Track B), so
moving between the two changes no data.

Feature `sharding` of `rasterizeddb_client`, on by default and dormant until
a `Router` is built.

## The map

A shard map assigns route_v1's level-0 blocks (2⁴⁸ of them, one per shard
key value's hash) to shards. A shard key value is always exactly one block,
so a tenant is never split between shards.

```text
rasterizeddb shard map
format 1
cluster 0f1e2d3c4b5a69788796a5b4c3d2e1f0
epoch 1
route_v 1
shard 0 10.0.0.1:5433 10.0.1.1:5433
shard 1 10.0.0.2:5433
range 0x0 0
range 0x800000000000 1
digest 89abcdef01234567
```

- `shard ID ADDR [STANDBY…]`: the primary first, then warm standbys tried in
  order.
- `range LO_BLOCK SHARD`: the shard owns from `LO_BLOCK` up to the next
  range.
- `digest` is checked when present. Drop it after a hand edit, or recompute
  it with `ShardMap::to_text`.
- `ShardMap::initial(cluster, shards)` lays out equal ranges.

## Setting up

1. Start one server per shard. Each keeps its data in whatever database you
   choose. Replication now covers every database of an instance, so a
   standby per shard follows all of them.
2. Write the map, then run `rdb-shard init --map shards.map`. It writes each
   shard's `rdb_shard_identity` row: cluster, shard id, map epoch and digest.
3. Connect:

```rust,ignore
use rasterizeddb_client::prelude::*;
use rasterizeddb_client::shard::{Router, ShardMap};

let map = ShardMap::from_text(&std::fs::read_to_string("shards.map")?)?;
let router = Router::connect(map, Options::default()).await?;
```

On connect, the router reads every shard's identity row. It refuses a shard
whose row names another cluster or shard (`Error::IdentityMismatch`), or a
newer map (`Error::MapStale`, which means reload). A wrong server behind an
address is therefore a connect error, not a misrouted write.

## Models

```rust,ignore
#[derive(Model)]
#[rdb(table = "orders", partition_by = "store_id, id", shard_by = "store_id")]
struct Order {
    #[rdb(primary_key)] store_id: String,
    #[rdb(primary_key)] id: Uuid,
    total: i64,
}

#[derive(Model)]
#[rdb(table = "stores", shard = "global")]      // on the home shard only
struct Store { #[rdb(primary_key)] id: String, name: String }
```

- **The default shard key.** `shard_by` defaults to the leading
  `partition_by` column, so the shard key and the engine's level-0 partition
  key are the same column.
  - A model with neither attribute is global.
  - A partitioned model that breaks a rule below is `Unroutable`. It still
    compiles, and a router refuses it with the reason.
  - `shard = "unsharded"` makes a router refuse the model outright.
- **Compile-time rules for `shard_by`:**
  - The key field is an integer, a `String` or a `Uuid`, and never an
    `Option`.
  - The primary key and every unique key contain the shard column, because
    uniqueness is only exact per shard.
  - No `serial` column: a sequence is per shard, so its values collide. Use
    UUIDs (v7), or a shard-prefixed `BIGINT` (`shard_id << 40 | counter`).

## Routing

| Call | Where it goes |
|---|---|
| `router.insert(&m)`, `update`, `upsert`, `delete`, `force_delete`, `restore` | the row's shard, by `m.shard_key()`; a global model goes to `home()` |
| `router.insert_many(&rows)` | grouped by shard, one transaction per shard. Rows of several shards need `.split_across_shards()`, else `Error::CrossShard`. A failure is `Error::FanOutPartial`, which names the shards that applied |
| `router.update_many(&rows)` | one shard only, else `Error::CrossShard` |
| `router.find::<M>(key)`, `find_by_key` | the key's shard (the primary key holds the shard column) |
| `router.all::<M>()` | home for a global model, every shard's rows concatenated for a sharded one |
| `router.select(&q).all()` | the shards the filter pins: `M::store_id.eq(k)` or `.is_in(…)`. `and` keeps a pin, `or`/`not`/`Expr::raw` drop it, and two different keys under `and` send nothing |
| no pin | `Error::NeedsRouteKey`, unless the query says `.on_shard(key)` or `.all_shards()` |
| `.all_shards()` | every shard. Rows are merged in `order_by` order (column values decoded, NULLs as declared, text by its bytes), and each shard is asked for `limit + offset` rows before the cut. `count` sums, `exists` is any. Keyset `after`/`before` merge the same way; `page()` reads one shard only |
| `router.select(&q).delete()`, `router.update(&u).run()` | the pinned shards; several only with `.split_across_shards()` on the query |
| relations `with`, `with_count`, `where_has` | a child sharing the parent's shard column loads on the parent's shard, and a global child loads from `home()`. Any other pairing is `Error::CrossShard` |
| `router.begin_for(key)`, `router.transaction_for(key, …)` | one shard. Inside, the ORM checks each row against that shard before sending (`Error::CrossShard`) |
| `router.for_key(k)`, `for_shard(id)`, `home()` | the shard's own `Client`, for plain SQL, `admin()` and `kv()` |
| `router.fan_out(\|id, client\| …)` | every shard at once; each shard's answer, in shard order |
| `router.cached::<M>(ttl)`, `router.kv_repo::<M>()` | each entry on its row's shard, so an invalidation reaches the same server as the write it follows |
| `router.kv_for(key)` | the shard of `key`, for KV keys that carry the tenant |

`router.shard_of(key)` and `rdb_route_block(type, value)` on any server
give a key's block the same way. Compare them to check a client build
against a server build.

## Rules a sharded application follows

- **Tenant keys come first.** Every tenant table leads its key with the
  tenant column, and so does every unique key and primary key. A table
  holding tenant rows without that column is not sharded: it lives on the
  home shard.
- **Global tables** (stores, owners, reference data) live on `home()`. Every
  shard has the table, and it stays empty off the home shard. A
  cross-tenant admin query is `.all_shards()` or a `fan_out` plus a merge in
  the application.
- **Ids:** no `serial` on sharded models.
- **Migrations.** `Options::migrations` is not run per shard inside a
  router. `Router::connect` runs it once with `Migrator::apply_sharded`: the
  home shard first, then every shard in id order, each recording its own
  ledger.
  - A failure stops at the shard it names.
  - The next run resumes from each shard's ledger, and nothing is undone,
    since DDL is not transactional.
  - `router.migrate(&migrator)` does the same by hand.
- **KV:** keys carry the tenant (`<tenant>:…`) and go through
  `kv_for(tenant)`. Global keys go to `home()`. `keys()`/SCAN is a fan-out.
- **Limits:**
  - no cross-shard transactions or joins;
  - the application merges what a fan-out returns, beyond the ORM's merges;
  - tenant moves are offline and fenced by the application;
  - HA is per shard, with manual failover to a standby listed in the map.

## Health and reloads

- **Health.** A background `SELECT 1` probes each shard (every 2 s by
  default; `RouterOptions`).
  - A shard that fails is marked down, and calls routed to it return
    `Error::ShardDown`.
  - A down shard is probed again at a doubling interval, up to 30 s, until it
    answers.
- **Reloads.** `router.reload(map)` takes only a higher epoch of the same
  cluster.
  - Shards new to the map are connected and their identity checked before
    it takes effect.
  - A failure leaves the old map in force.
  - A shard whose address changed needs a new router.

## Moving a tenant: `rdb-shard`

```text
rdb-shard init        --map MAP
rdb-shard show        --map MAP
rdb-shard move-tenant --map MAP --out NEW_MAP --state STATE \
                      --key-type int|text|uuid --key VALUE --to SHARD \
                      --table orders:store_id --table items:store_id --fenced
rdb-shard rollback    --map NEW_MAP --out MAP --state STATE
rdb-shard retire      --map NEW_MAP --state STATE [--retention-hours 24] [--force]
```

Logins use `--user U` with `RDB_PASSWORD`. TLS uses `--ca PEM` or
`--pin SHA256`; the default accepts any certificate. `--database` picks the
database.

A move takes these steps:

1. **Fence.** The application stops writing the tenant (a flag it checks,
   or `ALTER ROLE … NOLOGIN` for a per-tenant role). The tool refuses to run
   without `--fenced`.
2. **Copy.** It reads each table's rows of the tenant at one snapshot
   (`REPEATABLE READ READ ONLY`). It refuses a destination that already holds
   rows of the tenant, and inserts in 10k-row transactions.
3. **Verify.** It compares the row count and `Σ xxh64(row)` on both sides,
   and aborts on any difference.
4. **Hand off.** It writes the map with the tenant's block on the new shard
   (epoch + 1), updates both shards' identity rows, and records the move in
   the state file.
   - Reload every router, then lift the fence.
   - A router still on the old map is refused (`MapStale`) the next time it
     connects to either shard.
5. **Retire.** It deletes the source rows after the retention (24 h). Until
   then, `rollback` puts the tenant back and removes the copied rows.

## sqlx and tokio-postgres applications

An application that does not use the native client keeps a pool per shard
and routes with `rasterizeddb_router` itself:

```rust,ignore
use rasterizeddb_router::{KeyFamily, KeyValue, ShardMap, key::shard_block};

fn pool_for<'p>(map: &ShardMap, pools: &'p [(u32, sqlx::PgPool)], store_id: &str) -> &'p sqlx::PgPool {
    let block = shard_block(KeyValue::Text(store_id), KeyFamily::Text).expect("a text key routes");
    let shard = map.shard_of_block(block);
    &pools.iter().find(|(id, _)| *id == shard).expect("every shard has a pool").1
}
```

Hash the Rust value under the column's family: an integer column takes
`KeyValue::Int`, whatever its width, and a text column takes
`KeyValue::Text`. The server coerces a bound parameter to the column's type,
so this matches what the engine stores.
