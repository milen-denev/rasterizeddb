# Sharding: implementation plan

Status: **plan, nothing built.** Written 2026-11-23 against `dev` at
`219ea81f`; anchors are relative to `rasterizeddb_core/src/` unless a crate
is named. The design is [sharding.md](sharding.md) (2026-10-17, at
`952b3c4`). Its anchors and several of its premises are stale, and its refresh
is deferred: §9 lists what this plan found wrong in it. Where the two
disagree, this plan is the newer reading of the tree.

Everything here ships behind Cargo features (§2), in the client and its ORM
as well as the engine (§3, A4b).

**Where this lives.** Branch `wip/sharding`, worktree
`.claude/worktrees/sharding`, off local `dev` at `c02baf88`. It stays there
until the owner decides to merge it. **No slice starts until the owner says
so.**

## 0. Summary

Two tracks, in this order:

- **Track A, client-side sharding (§3).** A shared routing crate, a
  shard-aware router in `rasterizeddb_client` with its ORM routing models by
  their shard key (`#[rdb(shard_by)]`), a shard identity row on every server
  and an offline tenant-move tool. N independent servers, each with the
  existing replica for HA. There is no engine change on the statement path. It
  is a few weeks of work and covers apps whose every query is scoped by a
  tenant key (pms by `domain_id`, Kostas by `store_id`).
- **Track B, engine sharding (§4).** The design's phases 0 to 6, revised
  against the tree. A gateway forwards statements and shards enforce
  ownership. Fan-out reads come next, then an authority with offline moves,
  then per-shard Raft. Each phase is safe to stop at.

Track A's map format and routing function are Track B's from day one, so
moving from A to B changes no data and no tenant placement.

**What the tree changes about the design:**

1. **pgwire is a rewrite engine.** Views, triggers, sequences, CTE and
   derived-table temps, MERGE, DO blocks and COPY all become internal
   statements, and subqueries run inside pgwire, even at Describe. A gateway
   that tried to plan or execute statements would have to move all of that.
   So the plan's gateway **forwards the client's statement text to a full
   session on the shard** (the way `pgwire/proxied.rs` bridges MySQL and TDS)
   and classifies only to choose the shard (§4.2).
2. **Enforcement fits the seams that already exist.** Every internal
   statement routes on its own through `route_where`
   (`partition/table.rs:1717`), and FK cascades and `ON CONFLICT` re-enter the
   executor. An ownership check in `route_where`/`open_partition` plus guards
   on the storage write paths therefore covers rewritten statements too. One
   hole: **the delete path has no guard hook today** (`table.rs:5963`).
3. **Replication ships statements**, and only for the default database
   (`main.rs:1829`). The only piece of row-image capture that exists is
   `Table::returning_image` (`table.rs:6151`), so phase 3's capture is new work.
4. **Partial aggregates cannot be serialized, and there is no streaming
   merge.** Partition fan-out concatenates and then folds once. Phase 1c builds
   the codec.
5. **Most phase-0 asks are still open:**
   - **Open:** the `0b11` refusal; a persisted `is_primary`; the `FOREIGN`
     pid state; `RangeMap::within`; span records; a shared log-framing module;
     the resident-bytes budget.
   - **Done:** per-database sequences; durable partitioned `SERIAL`; composite
     identifying keys (local to the executor); `StatementClock`; persistent
     index v1 (off by default).

## 1. Decisions for the owner

Each decision comes with a recommendation. Nothing in Track B starts before
D1 and D6.

| # | Decision | Recommendation |
|---|---|---|
| D1 | Which ceiling are we buying for? Sharding pays off only above one node's commit rate, RAM or disk. The current consumers (pms at ~10M rows and 40–50K tenants; Kostas) are far below that. | Build **Track A** now and gate Track B on a measured need (§7). |
| D2 | Which clients Track A covers | The native client gets the router. sqlx apps (pms) use the shared routing crate and an app-level `pool_for(key)`, documented with an example, not productised. |
| D3 | Phase 3 consensus | **openraft pinned to an exact version** behind `sharding_raft`, as the design argues. A home-grown Raft reproduces the classic bugs. This is a new third-party crate in a tree that hand-rolls its infrastructure, so it is the owner's call. |
| D4 | Default feature list | Follow the convention: `sharding` and the client's `sharding` go in `default`, dormant until configured. `sharding_raft` and `sharding_2pc` join `default` only once each phase meets its gate. |
| D5 | Persistent index default | Flip `index_snapshot.index_storage` to `snapshot` before phase 3. Follower apply and relocation depend on a cheap reopen. |
| D6 | Where Track B stops | Phase **1c** gives write scale-out without HA. Phase **3** is the first production topology for a large target. Decide this before B1a starts, because phases 1–2 alone have no replicas. |
| D7 | HA before phase 3 | Allow the **existing replication one-way per shard** as a warm standby with manual failover. It is statement-based, with known divergence risks (§4.3 B1a.13). The alternative is RF1 plus S3 restore. |

## 2. Cargo features

The convention, from [[new-features-behind-cargo-features]] and
`rasterizeddb_core/Cargo.toml`:
- Each subsystem is a feature in the `default` list, dormant until configured.
- A build without the feature carries none of its code or crates.
- An on-disk format that such a build cannot keep correct is guarded by an
  `incompat_features` marker (`core/database/incompat.rs`), as
  `persistent_indexes` does.

| Crate | Feature | Gates | New crates | Default |
|---|---|---|---|---|
| **`rasterized_router`** (new workspace member) | none | `xxh64`, key framing, `level_hash`, point assembly, `PointRange`, `RangeMap`, the shard-map codec | none | always built: core's partitioning depends on it |
| `rasterizeddb_client` | `sharding` | the `shard` module (map, router, fan-out, health, epoch check); the ORM's sharded surface (`ShardedModelExt` on `Router`, fan-out `Select`, sharded `Cached`/`KvRepo`, `Migrator::run_sharded`); the `rdb-shard` tool (behind `required-features`) | `rasterized_router` only | yes, beside `derive` |
| `rasterizeddb_client_derive` | none | emits `Model::SHARD` and `shard_key()` for every model; plain data with no routing code, so a client built without `sharding` still compiles derived models | none | — |
| `rasterizeddb_core` | `sharding` | `core/shard/*` (map, ownership log, admission, classifier, rshard, gateway), the ownership guard, `--role`/`--cluster-map`/`--shard-id`, `SHOW SHARDS`, cluster DDL | none (rastcp, rustls and the tree's xxh64 cover it) | yes; dormant without `--role` |
| `rasterizeddb_core` | `sharding_raft` = `["sharding", "dep:openraft"]` | phase 3: log store, state machine, capture sink, apply loop, ReadIndex, learners | `openraft` (exact pin) | after phase 3's gate |
| `rasterizeddb_core` | `sharding_2pc` = `["sharding_raft"]` | phase 5: Prepared outcome writers, `__dist_txn`, the recovery driver | none | after phase 5's gate |

**Rules that keep the features honest:**

- **The ownership guard compiles away without `sharding`.** Its type
  collapses to `()` and the check is a `#[cfg]`-ed no-op, the way
  `storage_pools` collapses its pool type. The hot paths pay nothing in a
  build without the feature, and one relaxed load when the feature is
  compiled in but no role is configured.
- **On-disk formats and their markers:**
  - A shard directory writes `shard_v1` to `incompat_features`, so a build
    without `sharding` refuses to open it. It does not open a shard as a
    plain database that serves every range.
  - A phase-3 replica writes `shard_raft_v1`, and a phase-5 one writes
    `shard_2pc_v1`.
- **Safety fixes are not gated.**
  - The `0b11` refusal at status open (B0.1) and the persisted `is_primary`
    flag (B0.2) are correctness and schema work that benefits every build.
  - The partition map codec decodes the new kinds (`FOREIGN`, span records)
    unconditionally, so there is one format. Only the writers are gated.
- **`--role`** in a build without `sharding` is refused at flag parse with
  "built without the sharding feature", never silently ignored.
- **The build check matrix** for every sharding slice:
  - `cargo check --workspace --all-targets`
  - `cargo check -p rasterizeddb_core --no-default-features`, alone, because
    a workspace check unifies features ([[workspace-check-unifies-features]])
  - `cargo check -p rasterizeddb_core --no-default-features --features sharding`
  - `cargo check -p rasterizeddb_client --no-default-features --features sharding`

## 3. Track A: client-side sharding

```
 app ──► rasterizeddb_client::shard::Router ──► shard 0: rdb server (+ replica)
             │  for_key(k) → &Client             shard 1: rdb server (+ replica)
             │  home() / all() / fan_out()        shard N: …
             └─ ShardMap (epoch, digest, level-0 blocks → shard)
```

### A1. The `rasterized_router` crate

- **What moves out of core:**
  - `core/hash/xxh64.rs` (the in-tree reference XXH64);
  - from `partition/key.rs`: `KeyFamily`, `frame`, `level_hash`,
    `LEVEL_SEED`, `assemble` and the widths `[48]`, `[48,16]`, `[48,8,8]`;
  - `PointRange`, and `RangeMap` with `initial`/`validate`/`pid_of`/`range_of`
    from `partition/map.rs`.
- **What stays in core:**
  - the `DbType`→family mapping;
  - `normalise` over stored bytes, which sign- or zero-extends integers to
    i128 LE, writes UUIDs in network order and passes text as raw UTF-8;
  - width checks and `KeySpec::from_schema`.

  Core re-exports through `core::hash` so the fence in
  `partition/hash_source_tests.rs` keeps passing.
- **Frozen and versioned:**
  - family tags (1 int, 2 uuid, 3 text);
  - the frame `tag:u8 ‖ len:u32 LE ‖ bytes`;
  - `LEVEL_SEED = 0x5244_4250_5254_0000`;
  - the widths;
  - `ROUTE_V1 = 1` (`partition/log.rs:95`).
- **A client-side normaliser for the three families**, mirroring core's:
  - `i8…i128` and `u8…u64` (int4 42 and int8 42 hash alike);
  - `[u8;16]` and `uuid::Uuid` behind the client's `uuid` feature;
  - `&str`.
- **What a client never has to replicate:** a refused key type (NUMERIC,
  floats, dates, CHAR/citext, JSON), and therefore no collation. A UUID held
  in a TEXT column hashes as text. The router takes the *column's* family,
  never the value's.
- **The shard-map codec.** A versioned text form (one line per entry,
  `lo_block shard_id`, with a header carrying `cluster_id`, `epoch`, `route_v`
  and a digest) and a binary form with CRC framing. **This is the static map
  of Track B's phase 1a** (§4.3 B1a.3), so nothing is translated later.
- **Golden vectors:** `key.rs:399-460`'s tests move with the code, and a new
  test asserts that core and the crate produce byte-identical points for every
  family and width.
- **Size:** about 600 LOC, half of it tests. About 50 LOC change in core.

### A2. `rdb_route_point(regtype, value)` and `rdb_shard_of(value)`

- **What:** one SQL function returns the level-0 point and block of a value
  under a column type. It is a drift check between a client build and a
  server build, and it serves partitioning too, so it is not gated. Today
  nothing exposes a point (`SHOW PARTITIONS` gives ranges only;
  `pg_partition_tree` at `pgwire.rs:8129` is a stub).
- **Where:** beside the other `rdb_*` functions; no executor seam.
- **Test:** for each family, the function's answer equals the crate's.

### A3. Shard identity on the server

- **The row:** an ordinary table, `rdb_shard_identity(cluster_id text,
  shard_id int8, map_epoch int8, map_digest bytea)`, created by `rdb-shard
  init`. There is no engine change. There is no free-form server slot today:
  `SET CONFIG` takes only known keys (`introspect.rs:2940-2951`), and
  `--node-id` is replication-only and hidden from clients
  (`introspect.rs:3744`).
- **Why a table works:** backups, restores and replicas carry it, which is
  correct.
- **Connect check:** on connect the router reads the row and refuses a shard
  whose `cluster_id`/`shard_id` differ from its map, or whose `map_epoch` is
  newer than its map ("map is stale: reload"). A wrong shard behind an address
  is therefore a connect error, not a misrouted write.

### A4. The router (`rasterizeddb_client`, feature `sharding`)

```rust
pub struct Router { map: ArcSwap<ShardMap>, shards: Vec<Arc<Client>>, home: ShardId }
impl Router {
    pub async fn connect(map: ShardMap, opts: Options) -> Result<Router>;  // per-shard Client, identity check
    pub fn for_key<K: RouteKey>(&self, key: K) -> &Client;   // tenant-scoped work, incl. begin()/kv()/orm
    pub fn for_point(&self, point: u64) -> &Client;
    pub fn home(&self) -> &Client;                           // global tables, sequences, global KV keys
    pub fn all(&self) -> &[Arc<Client>];
    pub async fn fan_out<T, F, Fut>(&self, f: F) -> Vec<(ShardId, Result<T>)>;
    pub async fn reload(&self, map: ShardMap) -> Result<()>;  // epoch must rise; identity re-checked
}
```

- **Handing out `&Client`.** `Executor` uses `impl Future` methods and a
  hidden `send_checked`, so a routing executor cannot be a trait object.
  Returning the real `Client` keeps the ORM, `admin()`, `kv()`, `begin()` and
  `transaction(..)` unchanged. Transactions are single-shard by construction.
- **Health:** a background `SELECT 1` per shard with backoff marks a shard
  down. `Client::is_connected` (`client.rs:629`) is not a health check.
- **Errors:** `ShardDown`, `MapStale`, `IdentityMismatch` and `FanOutPartial`
  (which lists the shards that applied).
- **Size:** about 1.0–1.3k LOC plus about 600 LOC of unit and live tests.

### A4b. ORM integration (`rasterizeddb_client` + `rasterizeddb_client_derive`, feature `sharding`)

The ORM already carries partitioning: `Model::PARTITION` from
`#[rdb(partition_by = "…", partitions = n)]` (`orm/mod.rs:205`, `:810`;
derive `lib.rs:79`, `:622`), and the migrator plans conversions
(`orm/migrate.rs:1266`). Sharding builds on it, so one model declaration
drives the table's DDL, its partitioning and its routing.

**Model metadata (derive, ungated):**

```rust
#[derive(Model)]
#[rdb(table = "orders", partition_by = "store_id, id", shard_by = "store_id")]
struct Order { #[rdb(primary_key)] store_id: String, #[rdb(primary_key)] id: Uuid, total: i64 }

#[derive(Model)]
#[rdb(table = "stores", shard = "global")]          // lives on the home shard
struct Store { #[rdb(primary_key)] id: String, name: String }

// generated:
const SHARD: Sharding = Sharding::By { column: "store_id", family: KeyFamily::Text };   // or Sharding::Global
fn shard_key(&self) -> Option<ShardKey<'_>>;  // the field's family + normalized bytes, never hashed here
```

- **Default `shard_by`.** It defaults to the leading `partition_by` column, so
  the shard key and the engine's level-0 partition key are the same column,
  which is what lets Track B adopt the data unchanged. A model with neither
  `shard_by` nor `partition_by` is `Global`, unless it says
  `shard = "unsharded"` (refused by `Router`; for apps that want an explicit
  error).
- **Compile-time checks:**
  - The key field's Rust type must map to a routable family: `i8…i128`,
    `u8…u64`, `Uuid` (client feature `uuid`), `String`/`&str`. `Option<_>`,
    floats, decimals, dates, JSON and enums are a compile error naming the
    attribute.
  - Every `primary_key` and `unique` set of a sharded model must contain the
    shard column (a compile error otherwise), because per-shard uniqueness is
    only exact then.
  - `#[rdb(serial)]` on a sharded model is a compile error suggesting `Uuid`
    (v7) or, once Track B lands, the engine's shard-prefixed `BIGSERIAL`
    (B1a.9).
- **No hashing in the derive.** `ShardKey` is a small ungated type in the
  client (`family` + bytes, mirroring `rasterized_router`'s families). The
  hashing happens only in the `sharding` module, so a build without the
  feature carries no routing code.

**Routing ORM calls.** `Router` gets `ShardedModelExt`, the same method
names as today's `ModelExt: Executor` (`orm/mod.rs:980`: `insert`,
`insert_many`, `find`, `update`, `upsert`, `delete`, …), so a call site
changes only its receiver (`db` to `router`).

| Call | Routing |
|---|---|
| `router.insert(&m)`, `update`, `upsert`, `delete`, `restore` | `m.shard_key()`; `Global` models go to `home()` |
| `router.insert_many(&rows)` | grouped by shard, one transaction per shard; atomic per shard only. Rows spanning shards need `.split_across_shards()`, else `Error::CrossShard`; with it, a failure returns `FanOutPartial` naming the shards that applied |
| `router.find::<M>(key)` | the PK contains the shard column (checked by the derive), so the key routes |
| `Select::<M>::new().filter(M::store_id.eq(k))…all(&router)` | **routed by a hint**: `Column::eq`/`is_in` (`orm/query.rs:64`, `:133`) on the model's shard column attach a route hint to `Expr` beside its SQL text. `and` keeps a hint, `or` and `not` drop it, and two different keys under `and` give an empty route (no rows, no query). `Expr::raw` carries none |
| `Select` with no hint | `Error::NeedsRouteKey`, fail-closed, unless the caller says `.on_shard(key)` or `.all_shards()` |
| `.all_shards()` | fan-out. `all`: concatenate; with `order_by` + `limit`/`offset`, each shard returns `limit + offset` rows and the client merges on the same ordering (decoded column values, `NULLS FIRST/LAST` as declared) before cutting. `count`: sum. `exists`: any. `first`/`one` with `order_by`: the merged top. Keyset `after`/`before` (`orm/keyset.rs`) works per shard and merges the same way. `delete` across shards: refused unless `.split_across_shards()` |
| Relations `with`, `with_count`, `where_has` (`orm/relations.rs`) | a child that shares the parent's shard column loads on the parent's shard. A child that is `Global` loads from `home()` as a second query. Two sharded models on different shard columns: `Error::CrossShard` at `with(..)` |
| `router.transaction_for(key, \|tx\| …)`, `router.begin_for(key)` | one shard. Inside, the ORM checks each model's key against the transaction's shard and refuses a mismatch before sending (`Error::CrossShard`), so a wrong key is a client error, not a misrouted row |

**ORM cache and KV.**
- `Cached<M>` (`orm/cache.rs:45`; keys `rdb_orm:<table>:<pk>`) and
  `KvRepo<M>` (`kv.rs:1012`) take a `&Router` in their sharded constructors
  and keep each entry on the row's own shard. Invalidation therefore goes to
  the same server as the write it follows.
- `Global` models cache on `home()`.
- `router.kv_for(key)` returns today's `Kv` on that shard.
- `KvMap`/`KvQuery` scans over a router are fan-outs.

**Migrations.** `Migrator::run_sharded(&router)` beside `run(&client)`:
1. Run on `home()` first, then on every shard in shard order, recording a
   per-shard ledger in each shard's migration table.
2. A failure stops, names the shard, and the next run resumes from each
   shard's own ledger (DDL is not transactional).
3. Every model's table exists on every shard, so schemas stay identical,
   which Track B's fingerprints need. `Global` tables on non-home shards stay
   empty, and the router never sends them writes.
4. `Options::migrations` with `auto_migrate` inside `Client::connect`
   (`client.rs:268-281`) is refused when the `Client` belongs to a `Router`.
   The router migrates once, at `Router::connect`.

**When Track B arrives.** Connected to a gateway (a plain `Client`), the ORM
sends `SET LOCAL rdb.route_key = …` from the same hints, so the gateway's
classifier always has an explicit key (B1a.6). The model code does not
change.

**Size and tests.**
- **Size:** derive about 300 LOC; ORM sharded surface about 1.5–2k LOC.
- **Unit tests:**
  - hint propagation through `and`/`or`/`not`/`raw`;
  - compile-fail tests (`trybuild`, dev-dependency only) for bad key types,
    a unique key without the shard column, and `serial` on a sharded model.
- **Live tests:**
  - three servers;
  - insert, find and update per tenant;
  - a fan-out `order_by … limit` against a single-server twin holding the
    same rows;
  - relations: co-located, global and refused;
  - cache invalidation landing on the write's shard;
  - a migration failing on shard 2, then resuming.

### A5. Rules a sharded app must follow (written into the client docs)

- **Every tenant table leads its key with the tenant column**, and so does
  every unique key and PK. A plain table holding tenant rows is not sharded;
  it lives on the home shard.
- **Global tables** (Kostas `stores`/`stores_owner`, pms reference data) live
  on `home()`. A cross-store admin query is a `fan_out` plus an app merge.
- **Ids:** `#[rdb(serial)]` collides across shards. Use UUIDs, or a
  shard-prefixed `BIGINT` (`shard_id << 40 | counter`).
- **Migrations:**
  - `Options::migrations` run per `Client` inside `connect`, so with a router
    they move to `router.migrate()`. It applies to `home` first, then every
    shard, and records a per-shard ledger so a failure resumes rather than
    skews.
  - DDL is not transactional ([[ddl-is-not-transactional]]), so a failed
    shard is reported by name.
- **KV:** keys carry the tenant (`<tenant>:…`) and route with it. Global keys
  (Kostas `platform_key`, `src/cache.rs:134`) go to `home()`. `keys()`/SCAN is
  a fan-out.
- **LISTEN/NOTIFY:** the native client has none, so there is no conflict.

### A6. Moving a tenant (offline, `rdb-shard move-tenant`)

1. **Carve.** A tenant's level-0 prefix is one 2¹⁶ block, so it is always
   exactly one map entry; carving only splits an entry. Publish the map at
   epoch e+1 with the tenant still on its source shard.
2. **Fence.** The app's write fence for that tenant (a flag the app checks,
   or `ALTER ROLE … NOLOGIN` for a per-tenant role). Track A has no engine
   fence; the tool refuses to run without the app's acknowledgement.
3. **Copy.** For each sharded table, select `WHERE tenant = $1` at one
   snapshot (`BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY`) and insert
   into the destination in 10k-row transactions.
4. **Verify.** Compare count and `Σ xxh64(canonical row) mod 2⁶⁴` per table,
   computed client-side from the same encoding on both sides. Abort on any
   difference.
5. **Hand off.** Publish epoch e+2 with the tenant on the destination, and
   update the identity rows on both shards. Routers reload and refuse older
   epochs.
6. **Retire.** Delete the source rows after `move_retention` (24 h). Until
   then, the tool's `rollback` reverses the move.

- **Size:** about 800 LOC.
- **Test:** a live 3-server move under a concurrent reader, then a digest
  check. A mutation that skips the verify step must turn it red.

### A7. HA per shard

Each shard is a primary plus the existing replication peer
([replication.md](replication.md)), with manual failover. One caveat: replication covers
**the default database only** (`main.rs:1829`), so each shard keeps its data
in the default database.

### A8. sqlx / tokio-postgres consumers

- An app holds `Vec<PgPool>` plus `rasterized_router`, and calls
  `pool_for(map, key)`.
- Bound parameters are coerced to the column type on the server, so hashing
  the Rust value under the column's family matches the engine.
- The deliverable is an example in `rasterized_router/examples/`, not a
  crate.

### A9. Track A tests and exit

- **Unit:** golden vectors; map codec fuzzing at every offset (refuse
  gaps, overlaps, mid-block boundaries and a falling epoch).
- **Live (bounded, temp directories removed, judged on exit code):**
  - three servers on unused ports (check the ports first,
    [[probe-ports-may-be-a-peers]]);
  - routing, fan-out and a refused identity mismatch;
  - map reload;
  - tenant move and rollback.
- **Exit:** Kostas or a pms staging copy runs on two shards for a week. The
  router adds no hop, so measure only that connect and reload stay under a
  second.
- **Limits, stated in the docs:**
  - no cross-shard transactions or joins;
  - the app merges fan-out results;
  - tenant moves are offline and fenced by the app;
  - HA is per shard with manual failover.

## 4. Track B: engine sharding

### 4.1 Shape, revised

```
 client ─pgwire─► gateway (--role gateway)                 shard (--role shard, one Database)
                   classify: route key / tenant predicate ──rshard──► admission: map/owner/schema epoch
                   per-session backend sessions, lazily     │        full pgwire session: rewrites, views,
                   fan-out merge (1b/1c)                    │        triggers, CTE temps run HERE
                                                            │        ownership: route_where ∩ Owned,
                   Track A routers connect to shards ───────┘        FOREIGN refuses, write guard
```

- **The gateway forwards text and does not execute.** It holds catalogue
  shells only to classify.
- **Correctness never depends on the classifier.** Each shard refuses any
  read reaching a range it does not own (`RDB02`) and any write that would
  stamp a row outside `Owned ∪ Incoming(own move)`.
- **Track A routers and the gateway coexist.** Both reach the same
  shards, and the shards enforce ownership.

### 4.2 Phase B0: prerequisites (single node, mostly ungated)

| # | Item | Where (today) | Feature | Test (mutation it must catch) |
|---|---|---|---|---|
| B0.1 | Refuse a `0b11` status code at open, in the full-byte scan and the settled sidecar load; `decode` stays total | `mvcc/status.rs:430-438` (`open_with`), `:1768` (`open_settled`), decoders `:590-595`, `:1583-1588` | none | `0b11` in a plain and in a settled file fails the open *(skip either check)* |
| B0.2 | Persist `is_primary` on `UniqueKey`; set it for every PK, composite included; bump the sidecar version; old sidecars infer it from the `<t>_pkey` name. Then make `pkey_name`/`identifying_key`/`composite_primary_key` read the flag | `rql/unique_key.rs:81-92`, `:843`; `rql/lexer_ct.rs:287-290` (one-column only); `executor.rs:19883`; `database/foreign_keys.rs:190`; the test at `lexer_ct.rs:2317` changes | none | composite PK round-trips the sidecar; old sidecar reopens with the flag inferred *(drop the flag on write)* |
| B0.3 | Move `IdentifyingKey`/`row_key` out of the executor into `core/row/identifying_key.rs` so storage-level apply and moves can address rows by PK | `executor.rs:19624-19700`; uses `index::key::encode_canonical` (`index/key.rs:275`) | none | existing lock/UPDATE…FROM tests unchanged |
| B0.4 | Partition map: a `FOREIGN` pid state; `RangeMap::within(owned, n)`; `ACQUIRE_SPAN`/`RELEASE_SPAN` kinds (0x0F/0x10); `open_partition` refuses `FOREIGN`; the framing (`len ‖ crc32 ‖ kind ‖ payload`, generations, `SEALED`) pulled into `core/framed_log.rs` with a per-log kind validator | `partition/log.rs:258-267` (`PartState`), `:109-125` (kinds), `:372`/`:718`/`:752` (framing); `map.rs:51-146`; `PLACE` (`log.rs:36-40`) already replaces a sub-span but refuses spans holding files | codec none; writers `sharding` | fuzz every offset; a `FOREIGN` pid opened by any path errors *(empty `FOREIGN` span reads as empty)*; within() block-aligned, covers exactly the owned ranges |
| B0.5 | A resident-bytes term in the open budget (`partition_resident_budget`) | `partition/budget.rs` (count/fd only: `min(max_open, (NOFILE−2048)/10)`) | none | budget closes by bytes before count |
| B0.6 | A public "wait until this partition's PK index is ready" covering the rebuild path, not only snapshot loads; decide D5 | `index/mod.rs` `TableIndexes::exact` (None until ready); `index_store/load.rs:365-389` (`LoadBarrier`); readers wait 600 ms then scan (`row/table.rs:4895`) | none | wait returns after a rebuild; a timeout errors, never scans silently in apply |
| B0.7 | Guard hooks on every stamping path, as no-ops until B1a: delete before `pointer.delete`, `insert_version`, and the P9 reshape `append` | `table.rs:5963` (**no guard today**), `:5126`; `partition/reshape.rs:166` | hook `sharding`, compiled away without it | — (tested in B1a) |
| B0.8 | Cluster mode asserts `wal_enabled()` | `lib.rs:64-74` (always on; only an embedder can turn it off) | `sharding` | embedded start with WAL off and a role refuses |
| B0.9 | Measure durable commits/s per `Database` on .7, ext4, release, 8 and 64 clients | — | — | the number goes into §7's trigger table |

**Already done, and dropped from the design's phase 0:**
- Sequences are keyed per database (`rql/sequence.rs:46`).
- Partitioned `SERIAL` reserves durably in steps of 1024 (`partition/serial.rs:65`).
- The composite identifying key exists (it needs B0.3's move).
- `StatementClock { transaction_us, statement_us }` (`statement_clock.rs:24`) is
  the design's `txn_start_time` input, applied by `scope`.
- The partition cap is 2²⁰ per database (`partition/table.rs:115`), not
  65,536. The comment at `partition/mod.rs:60` is stale.

**Size:** about 2.5–3.5k LOC across six slices. B0.1, B0.2 and B0.4 are
independent of one another.

### 4.3 Phase B1a: single-shard routing, RF1 (feature `sharding`)

**B1a.1 Roles and flags.** `--role shard|gateway`, `--cluster-map <file>`,
`--shard-id`, `--cluster-id`; all under `#[cfg(feature = "sharding")]` in
`main.rs`. A shard is one `Database` per process: the data-directory lock and
`EngineSlot` already enforce one engine per process (a5d81376). Several
shards on one machine are several processes, as the design says.

**B1a.2 `core/shard/` module layout:**

| File | What |
|---|---|
| `map.rs` | `ShardMap` over `rasterized_router::RangeMap`: epoch, `route_v`, ranges → `(ShardId, owner_epoch)`, `validate` (total, disjoint, `BLOCK`-aligned) |
| `ownership.rs` | `db_shard_ownership.log` on `core/framed_log.rs`: `ACQUIRE`, `RELEASE`, `INCOMING`, `FENCE`, `EVICTED`; `ArcSwap<OwnedRanges>` published only from accepted map + ownership records together |
| `admission.rs` | one function every request passes: placement generation, `(range, owner_epoch)` Owned and named by the accepted map, schema epoch, per-range admission lease |
| `guard.rs` | the ownership write guard (one comparison against the `ArcSwap`); `()` without the feature |
| `classify.rs` | the gateway classifier (B1a.6) |
| `rshard.rs` | the protocol over rastcp, pipelined (`TcpClient::pipeline()`, as replication uses since 9c090782) |
| `gateway.rs` | per-client session, backend sessions per shard, forwarding, refusals, cancel map |
| `ddl.rs` | cluster DDL procedure (B1a.10) |

**B1a.3 Static map.** The file is Track A's map codec (A1), with CRC and
digest and no signature until phase 2. Nodes persist the highest epoch they
have seen and refuse lower ones. Two nodes reporting one epoch with different
digests stop serving sharded tables (`57P03`).

**B1a.4 Shard-side read enforcement:**
- **Local map.** At shard open, every sharded table's partition map is built
  with `RangeMap::within(owned, n)` plus `FOREIGN` gaps (B0.4).
- **`route_where`** (`partition/table.rs:1717`) intersects each routed set
  with `Owned` and fails `RDB02 "statement reaches ranges not on this shard"`
  **before reading**.
- **Why that covers rewrites:** it runs per table reference and per internal
  statement. That includes:
  - single-table selects (`execute_partitioned_select` → `partitioned_source`,
    executor `:23164`/`:23200`);
  - join relations (`partitioned_with`, `:34`);
  - FK probes (`referenced_values_now` `:2100`, `relation_rows_as_of` `:2350`);
  - learned prepared binds (`prepared.rs:138`);
  - every statement pgwire issues internally (CTE temps, views, triggers,
    DO-block pieces).
- **Backstop:** `open_partition` refusing `FOREIGN` catches any path that
  bypasses `route_where`.

**B1a.5 Shard-side write enforcement.** The ownership guard sits beside
partitioning's `RangeGuard` (`partition/guard.rs:32-92`). That guard is a
static `Option` per `TableData`; the ownership guard is a separate `ArcSwap`
layer. It covers:
- insert (`table.rs:5026`);
- update at the single guard site before both in-place branches (`:6725`;
  `write_columns_in_place` `:6854`, `update_with_pointer_record_pos` `:6876`);
- delete (B0.7, before `:5963`);
- `insert_version` (`:5126`);
- reshape `append`.

FK cascades, `ON CONFLICT`, `UPDATE … FROM`, COPY, MERGE and import all
re-enter the executor and end on these paths. **`FENCE` publishes the fenced
guard before the drain samples the running set**, as in sharding.md §6.

**B1a.6 The classifier, explicit first.** Routing in order:
1. **An explicit route key.** `SET rdb.route_key = '<value>'` per session, or
   `SET LOCAL` per transaction. pms already scopes every query by
   `domain_id`, so this is one statement per connection checkout.
2. **Automatic** only for statements whose every sharded reference is a
   base table with the leading key fixed by `=` or a capped `IN`. This uses
   the engine parser and `sarg::extract_gated` (`filter/sarg.rs:555`) against
   shells, never a text scan ([[scanners-must-track-nesting]]).
3. **Only unsharded tables:** the home shard.
4. **Anything else:** `0A000 "statement needs a route key in cluster mode"`.
   Fail-closed ([[a-guard-is-an-allowlist]]). A wrong guess costs the
   client an `RDB02` from the shard, never wrong rows.

Views, functions and triggers are expanded only on the shard. A statement
whose route the gateway cannot see through a view needs the route key.

**B1a.7 Gateway session model:**
- **Forwarding point.** The gateway's pgwire listener branches at the top of
  `execute_recorded_inner` (`pgwire.rs:8002`), before any rewrite: before the
  DO block (`:8412`), temp-table (`:8476`), MERGE (`:8510`), sequence
  (`:8542`), view (`:8590`), trigger (`:8616`) and CTE (`:8863`/`:8886`)
  rewrites, and before `catalog_guards` (`:8255`).
- **Backend sessions.** One per shard per client session, opened lazily over
  rshard. Each is a real `SessionState` on the shard, bridged like
  `pgwire/proxied.rs`'s `PgBridge`. Session `SET`s are recorded at the
  gateway and replayed to a backend session when it opens.
- **Prepared statements.** The gateway keeps Parse/Bind. At Execute it
  inlines parameters with `pg_params::inline_params` (`pg_params.rs:97`, as
  the shard would at `pgwire.rs:4512`), routes, and forwards text. **It never
  routes at Parse** (S12).
- **Describe.** Forwarded to the session's current shard, or the home shard.
  Schemas are identical under the schema epoch. The describe cache key
  (`pgwire.rs:3719`) gains the schema epoch.
- **Transactions:**
  - `BEGIN` is recorded at the gateway, and the first routed statement opens
    the backend transaction.
  - The first write fixes the write shard. A write reaching another shard is
    `0A000` and the block fails.
  - Under READ COMMITTED, a non-locking read may go to another shard as its
    own read-only snapshot. Under REPEATABLE READ or SERIALIZABLE, a second
    shard is refused.
  - A transaction pins the owner epochs it touched; a change gives `40001`.
  - Gateway-supplied `StatementClock.transaction_us` rides each request, so
    `now()` agrees across shards.
- **Temp tables** (`__cte_tmp_*`, `__rdb_tmp_*`) pin the session to the
  shard that created them. A later statement routing elsewhere is refused.
- **Retries.** Only `before_effect` refusals (`Moved`, `SchemaBehind`,
  `Busy`) retry, transparently, in autocommit or on a transaction's first
  statement, at most three times. A connection lost after dispatching a write
  or commit is `08007`, never retried (S22).
- **Cancel.** The gateway issues its own BackendKeyData from `cancel.rs`
  (`register_session`, `:333`) and maps it to each backend's `(pid, key)`. A
  cancel fans out to every shard running the statement.

**B1a.8 Front ends in cluster mode:**

| Front end | Phase 1a | Later |
|---|---|---|
| pgwire | through the gateway | — |
| native (rastcp) | direct to shards with the Track A router; native autocommit (`execute_as`, `database/mod.rs:6722`) bypasses pgwire, so a native gateway is separate work | native gateway in 1b if asked |
| MySQL, TDS | refused on gateways (they bridge to pgwire, so they can follow the pgwire gateway later); allowed direct to a shard | after 1c |
| Redis / `kv_cache` | node-local; refused on gateways; direct to a shard is that shard's cache | route by key prefix, phase 4 |
| `LISTEN`/`NOTIFY` | refused on gateways (the hub is per process, `pgwire/notify.rs`) | gateway hub, phase 4 |
| COPY / multi-row INSERT | one shard only (route key); spanning shards `0A000` | `rdb.split_bulk_writes`, opt-in, non-atomic (sharding.md §8.5) |
| DO / PL/pgSQL | forwarded whole to the routed shard; pieces reaching another shard fail there with `RDB02` | — |

**B1a.9 Unsharded tables and global objects:**
- **Plain tables** live on the home shard. Every other shard refuses them
  at catalogue resolve (a home-only flag in the cluster catalogue), so they
  are not read as empty.
- **Global schema objects** (`__rdb_views`, `__rdb_sequences`,
  `__rdb_functions`, `__rdb_triggers`, `__rdb_domains`, `__rdb_types`,
  `rdb_roles`, `__rdb_acl`) are copied to **every** shard by cluster DDL
  (B1a.10). Each shard reloads its name registry as today (every 2 s).
  Fingerprints cover them.
- **Sequences:**
  - `BIGSERIAL` on sharded tables draws `(shard_id << 40) | counter` from
    `partition/serial.rs`'s durable reservations.
  - `SERIAL` (int4) on a sharded table is refused.
  - Standalone sequences live on the home shard, so `nextval` is one hop
    there.
  - `setval` on a sharded sequence is refused.
  - All of this is tested by S10 and S27.

**B1a.10 Cluster DDL.** A resumable procedure, as in sharding.md §8.4:
1. Take the lock.
2. Run `DdlApply{ddl_id, epoch, sql}` on each shard, idempotently.
3. Each shard acks a 128-bit `TableSchema` fingerprint, covering every
   `write_order`, dropped-column history, `__null_` and `__ci_`/`__cia_`
   companions.
4. If all match, publish epoch n+1; otherwise mark the table `PARTIAL` and
   refuse it until `CLUSTER DDL RESUME`.

Phase 1a runs it from an operator tool (`rdb-cluster ddl apply <file>`).
DDL sent through the gateway is refused. Physical-rewrite DDL (P8), reshape
(P7) and conversion stay refused in cluster mode until phase 4.

**B1a.11 Refused in cluster mode:**
- branching and PITR branches (`db_branching`);
- `--json-shadow-tables`;
- statement replication between shards;
- any directory whose map was copied from another node without the restore
  alias.

All are explicit errors.

**B1a.12 Observability:**
- `SHOW SHARDS` and `SHOW CLUSTER`;
- alarms on `shard_ownership_mismatch` and write-guard trips;
- counters for refusals by reason, retries, `08007`s and the gateway hop
  p50/p99;
- `pg_stat_statements` gains `shards_touched` and `forwarded`;
- statement-text redaction extends to rshard ([[statement-text-reaches-four-surfaces]]).

**B1a.13 HA option (D7).** If the owner chooses it, each shard may run the
existing replication **one-way** to a standby, with manual failover. The
caveats are stated:
- it ships statements (`note_statement` at depth 0);
- `replicated_values.rs` pins `now()`/`random()` or refuses per-row values;
- last-writer-wins is per table;
- the standby must be fenced by a placement-generation bump before promotion.

**Tests (sharding.md §17 numbering):**
- S1: the map;
- S3: another shard's tenant via a subquery, CTE, `IN (SELECT …)`, join, view,
  trigger body and DO block gives `RDB02`, never rows;
- S4: the guard on every path, including the new delete and `insert_version`
  guards;
- S6: multi-shard writes;
- S10, S12, S22 and S27;
- a **twin model test**: three shards in one process against one plain table
  under random single-tenant transactions, rollbacks and prepared/simple
  mixes.

**Gate:** gateway hop on a point read under 150 µs on the LAN (.7 ↔ .4).

**Size:** about 7–10k LOC. The gateway session model is the largest part.

**Shared hot files:** `pgwire.rs` (the forwarding branch), `table.rs` (guard
calls), `partition/table.rs` (`route_where`). Each hunk goes to the file's
current owner before commit ([[parallel-sessions-coordinate]],
[[table-pub-methods-seam-rule]]).

### 4.4 Phase B1b: fan-out `COUNT` and rows

- **Gateway.** It scatters `Exec{Part{ranges}}`. Fan-out statements are a
  narrow allowlist:
  - a `SELECT` over sharded base tables and views;
  - autocommit or `BEGIN READ ONLY`, at READ COMMITTED;
  - no temp tables, no DO block, no locking clause.
- **Shard:**
  - `Part` mode bounds each partition call to its point ranges. A partition
    straddling an edge gets a residual point predicate (one xxh64 per row).
  - `served` is derived from the ownership snapshot taken atomically with the
    read snapshot, never echoed from the request.
- **Fragment mode.** Fan-out needs engine row bytes back, not pgwire
  encoding. The shard runs pgwire's rewrites, then the executor in a
  **fragment mode** that returns `Count` or `Rows` bodies. The merge reuses
  the in-process fan-out's shapes (`row_source.rs:241` for count sums, `:271`
  for chunk concatenation). The schema fingerprint is checked before decoding.
- **Coverage check.** The served lists must be pairwise disjoint and union to
  the routed set at the dispatched owner epochs, else `XX000` plus the
  `coverage_mismatch` alarm (S5).
- **Caps:**
  - `shard_gather_max_bytes`; `54000` past it, never a truncation;
  - a gateway gather semaphore (`53200`);
  - unrouted statements are refused above `shard_unrouted_max_partitions`,
    estimated from each shard's `totals()` (`partition/table.rs:2037`;
    `PartitionInfo` has no live-row count, so the estimate uses bytes);
  - a NOTICE past `shard_fanout_warn`.
- **Empty routes still run the pipeline** (S11).
- **Native gateway**, if D2 asks for it: `serve_native_request`
  (`database/mod.rs:6320`) gains the same branch.
- **Size:** about 3–4k LOC.
- **Gate:** fan-out latency over 8 and 32 shards; S5 and S11 green.

### 4.5 Phase B1c: top-N and aggregates

- **Partial codec** (`core/shard/merge.rs`):
  - It encodes `(group key bytes, shown, Vec<Acc>)` with a version byte.
  - It destructures **every** `Acc` variant (`rql/aggregate.rs:1123`) with no
    `..`, so a new field fails to compile.
  - `ExactSum` travels whole: `{total, n, scaled, scale, overflow, dscale,
    track_dscale, own_scale, base}`. That is two more fields than the design
    lists.
  - `Distinct` (a `HashSet`) and `Collect` (vectors) are encoded by value, up
    to the gather cap.
  - The shard merge asserts that `scale`/`scaled` agree in release builds,
    else `XX000` ([[the-number-that-looks-right]],
    [[numeric-scale-must-reach-the-planner]]).
- **Fold-only executor mode.** It stops before `HAVING`/`ORDER`/`OFFSET`/
  `LIMIT`. The gateway builds its `Folder` from its own plan template, merges
  (`Folder::merge`, `:3510`) and finishes once (`finish_into`, `:3535`).
  Today `Folder::merge` serves only thread-parallel cell folds
  (`executor.rs:13333`).
- **Top-N:**
  - Shards return scan-schema rows carrying every sort key and its `__null_`
    companion, capped at k+o.
  - The gateway re-cuts with `sort_rows_for_merge`
    (`processor/concurrent_processor.rs:2282`), which re-sorts the union.
    That is fine at k × shards rows, and there is still no streaming k-way
    merge.
  - Resolution is **strict**: an unresolved key is `XX000`, never
    skip-and-warn.
  - Alias and expression keys are refused until phase 4.
- **Tests:**
  - S21, a codec property test: overflow; `NUMERIC(p,s)` and unconstrained
    `AVG`; jsonb group keys `{"a":1}` and `{"a":1.0}` split across shards;
  - S25: nullable, unprojected and alias keys;
  - float `MIN`/`MAX` with `-0`/`0` across shards ([[float-min-max-signed-zero-order]]).
- **Size:** about 3k LOC.
- **Gate:** S21 and S25 green.

### 4.6 Phase B2: authority, signed maps, offline moves

- **Authority** (`--role authority`, one process):
  - It owns the map, signs it (Ed25519 through the crypto provider rustls
    already pulls, with no new crate if the provider exposes it; otherwise
    `ed25519-dalek` under `sharding`, decided at the slice), runs the DDL lock
    and records backup marks.
  - Nodes refuse unsigned maps from phase 2 on.
  - `rdb-authority rebuild --from-shards` rebuilds a lost authority with the
    total-and-disjoint check.
- **Offline range move** (sharding.md §10.2 steps 1–7):
  1. Carve.
  2. `INCOMING` plus adopting the `FOREIGN` span (`ACQUIRE_SPAN`); `FENCE`
     with the guard published first.
  3. Drain transactions and admission leases; past 30 s the **move** aborts,
     not the transactions.
  4. Copy through the MVCC insert path with fresh row ids.
  5. Verify count and `Σ xxh64(pk ‖ canonical row)` per table and tenant.
  6. Two-party handoff with the signed token.
  7. `Departed`, then `SHARD EVICT RANGE` (file unlink for whole partitions,
     MVCC delete for straddlers, `RELEASE_SPAN`, `EVICTED`).
- **Offline relocation** of a whole shard: stop, copy with the exclusions
  (`db_replication.*`, `backup_instance.id`, identity, `.ixs*`/`.ixm*`),
  rewrite the pool configuration, write the `RELOCATED` tombstone, start at
  `placement_gen + 1`.
- **`EXECUTE ON EACH SHARD $$…$$`** runs as `Part{Owned}` on each shard,
  with the guard. It errors naming the shards that applied.
- **Backup marks** under the handoff lock. Each shard backs up to its own
  prefix `…/<cluster>/<shard>/<generation>/` ([[s3-prefix-guard-crash-loop]]).
  Note that `backup/mod.rs`'s `COMPACTION_GATE`/`DELETION_GATE`/
  `RELOCATION_GATE` are process-global, which is fine with one shard per
  process.
- **Tests:**
  - S2: the ownership crash matrix at every record;
  - S7: moves lose and duplicate nothing, through `SET n = n + 1` and
    non-unique inserts, with a crash at every record;
  - S8, S20 and S24;
  - the ownership log under the power-loss harness, which must announce file
    creation ([[sigkill-is-not-power-loss]]).
- **Gate:** insert-path move rate measured on ext4 on .7, published before any
  move time is promised.
- **Size:** about 6–8k LOC.

### 4.7 Phase B3: per-shard Raft over row images (feature `sharding_raft`)

The design's §9, with these concrete seams:

- **Capture.** A `ChangeSink` on `Table`, called from:
  - insert (`table.rs:5063`);
  - update (merged row at `:6725`, both in-place branches);
  - delete (`:5963`);
  - `insert_version`.

  Ops are buffered per transaction family and tagged with the writing sub-id.
  `rollback_savepoint` and abandoned autocommit attempts discard theirs.
  `__ci_`/`__cia_` companions and `__null_` columns travel as stored values;
  a follower never recomputes them. Temp relations are excluded. Row images
  build on `Table::returning_image` (`:6151`).
- **Propose instead of commit.** Inside `commit_written_transaction`
  (`database/mod.rs:2237`) at the point where `prepare_replication` runs
  (`:2274`). Every committer passes through it:
  - the executor autocommit tail (`:20711`/`:20721`);
  - pgwire's implicit transaction (`:8997`/`:9010`);
  - `commit_block_inner` (`:11464`);
  - native autocommit.

  The exception is the PG import (`migrate/run.rs:717`), which is refused in
  cluster mode or routed to the per-shard bulk mode (sharding.md §8.5).
- **The commit path** follows sharding.md §9.2:
  - visibility only after quorum;
  - the sequential apply loop commits by `origin`;
  - `__shard_applied` is insert-only;
  - outcomes are decided by log id, never TxId;
  - on step-down, every proposed-but-unapplied transaction aborts;
  - on restart, entries re-apply past the durable applied id before serving.
- **The in-place path is off in cluster mode:** `try_begin_in_place_for`
  returns `None`.
- **Follower apply at the storage layer:**
  - capture off and index hooks on;
  - no FK actions, defaults or sequences;
  - rows addressed by PK through B0.3's identifying key and B0.6's
    wait-for-ready probe;
  - every op asserts `old_h`;
  - a failed apply halts the replica.
- **Also built:**
  - `SeqReserve` entries;
  - `Schema` entries behind a table barrier, with DDL idempotent by
    fingerprint;
  - ReadIndex reads, batched;
  - incremental per-partition replica digests;
  - learner relocation through the backup snapshot routine
    (`compaction_permit`, the declined-checkpoint condition,
    [[backup-wal-declined-checkpoint]]).
- **What carries over from replication:**
  - rastcp pipelining;
  - HLC (`replication/hlc.rs`);
  - the CRC framing (via `core/framed_log.rs`);
  - the power-loss crash-image harness (`replication_crash_tests.rs` as the
    template);
  - `REPLICATION CHECK`'s digest idea.

  Nothing of its statement capture carries over.
- **Gates, all before any throughput claim:**
  - a stateright or TLA+ model of commit, step-down, restart re-apply and
    handoff;
  - `rasterizeddb_cluster_sim` (in-memory network, seeded scheduler,
    `FaultyStorageProvider` discarding unsynced writes);
  - openraft's storage conformance suite;
  - follower apply ≥ 1.5× the leader's peak under a cold-partition mix, else
    parallel apply first;
  - RF3 commit and read p50/p99 on ext4 with the full placement;
  - a one-shard restore including index readiness.

  Tests: S13–S18.
- **Size:** about 15–20k LOC. It is the riskiest phase: visibility moves into
  the apply loop on the hottest path (sharding.md §18.1).

### 4.8 Phases B4 to B6 (outline only, replanned when B3 lands)

- **B4:**
  - online range moves on the Raft log (`Watch`/`Drained`/`TxnData`);
  - an openraft authority;
  - a gather for `DISTINCT`, windows and joins (`RowSet::Serialized`,
    `join.rs:374`, under `RDB_JOIN_MEMORY_MB`);
  - correlated tenant-predicate propagation;
  - routing `kv_cache` by key;
  - a gateway hub for `LISTEN`/`NOTIFY`;
  - MySQL/TDS on gateways;
  - reshape and physical ALTER under the schema epoch.
- **B5 (`sharding_2pc`):**
  - opt-in 2PC with the anchor-log decision;
  - every rule in sharding.md §7's table: a Prepared outcome and WAL kind,
    restart rebuilding `running`, `settle_bound`/forget/settled-block
    exclusions;
  - reference tables;
  - decision-pausing marks.
- **B6:**
  - closed timestamps and consistent cross-shard snapshots;
  - follower and lease reads;
  - distributed deadlock detection;
  - automatic rebalancing;
  - clone splits.

## 5. Sequencing

```
            weeks →   1   2   3   4   5   6   7   8   9  10  11  12 …
Track A   A1 route    ███
          A2–A4           ██████
          A4b ORM             ████████
          A5–A9                 ██████            (exit: 1 week staging)
Track B   B0.1/.2/.3  ████████                    (independent slices)
          B0.4/.5/.6/.7   ██████████
          B0.9 measure        ██
          B1a                       ████████████████████
          B1b                                         ████████
          B1c                                               ████████
          B2 …                                                     ██████████████
          B3 …                                     (gated on D3, D6 and §7's trigger)
```

The weeks are rough and assume one or two worker sessions per track. The
critical path is B0.4 → B1a → B1b → B1c → B2 → B3.

**Parallel streams that do not share files:**
- A1–A9 (route crate, client);
- B0.1 (`mvcc/status.rs`);
- B0.2/B0.3 (`unique_key`, `lexer_ct`, executor key code);
- B0.4/B0.5 (`partition/*`);
- B0.6 (`index_store`).

B0.7 and B1a.5 touch `table.rs`; B1a.7 touches `pgwire.rs`.

## 6. How the work is run

- **Slice discipline:**
  - **Nothing starts until the owner says so.**
  - All sharding work stays on `wip/sharding` in
    `.claude/worktrees/sharding`. A parallel worker uses a branch stacked on
    it (`wip/sharding-<item>`) and merges back into `wip/sharding`, never
    into `dev`.
  - **Nothing goes to the integrator or into `dev` until the owner decides
    to merge.** At that point the usual hand-off applies
    ([[local-wip-branch-workflow]]): `ready: wip/sharding @ <hash>` naming
    the integration tests the work moves.
  - Rebase `wip/sharding` onto local `dev` when `dev` moves, and rerun the
    `cargo check` matrix.
  - Run `cargo check` across §2's matrix and only the slice's own new tests.
  - Never push.
  - The worktree builds into its own `target/` with `DEBUG=0`; check
    `df -h /mnt/Volume4` first. It was at 92% on 2026-11-23
    ([[parallel-agents-fill-target-dir]]).
- **Every invariant gets its mutation.** S1–S27 each name a mutation that
  must turn the test red; grep for `MUTATED` after a round
  ([[grep-mutated-after-mutation-rounds]]).
- **Tests are bounded and judged on exit code**, and remove their temp
  directories ([[tests-must-be-bounded]]). Multi-process tests choose free
  ports and kill by the port's PID ([[setsid-pid-is-not-the-server]]).
- **Durability claims come from the power-loss harness on ext4, not tmpfs**
  ([[tmpfs-hides-disk-cost]], [[sigkill-is-not-power-loss]]). This covers the
  ownership log, the shard map file, the Raft log store and the vote file.
- **Hot files:** `pgwire.rs`, `executor.rs`, `row/table.rs`,
  `partition/table.rs`. Message the current owner with each hunk before
  committing. Prefer additive seams (a new `cfg`-gated call) over changing
  signatures.
- **Docs:** each phase adds an "as built" section to sharding.md, as
  partitioning.md does. The full refresh (§9) is its own slice.

## 7. When Track B is worth starting (the trigger)

Track B starts when **one** of these is measured on production hardware,
with the number recorded:

| Ceiling | Measure | Trigger |
|---|---|---|
| Commit rate | durable commits/s per `Database` (B0.9) against the peak hour | peak > 60% of the measured ceiling |
| RAM | resident bytes of open partitions and indexes per idle window (`SHOW TABLE STATS`, budget counters) | > 50% of RAM with snapshot indexes on |
| Disk | data + WAL + vacuum headroom | > 60% of usable disk on the largest node |
| Tenant isolation | one tenant's load degrading others' p99 | not fixable by `ISOLATE KEY` or storage pools |

Until then, Track A plus partitioning (`ISOLATE KEY`, placement over pools)
covers the growth path.

## 8. Risks and stop points

1. **The forwarding gateway's session model is the largest unknown in B1a.**
   This covers SET replay, temp pinning, prepared statements and Describe. If
   it grows past ~10k LOC, stop at "native/Track A routers plus shard-side
   enforcement" (B0 + B1a.1–.5 + B1a.9–.11), which is useful on its own: the
   engine then guarantees a misrouted client gets errors, never rows.
2. **The classifier will refuse legitimate ORM SQL.** The explicit route key
   is the escape hatch. Collect refusals from pms traffic before widening the
   automatic path.
3. **Capture coverage in B3.** An unlisted write path diverges followers
   silently. `insert_version` and the P9 reshape `append` are already known
   paths outside insert/update/delete. S16's twin diff per path plus `old_h`
   and the digests are the backstop, not a proof.
4. **openraft** is version churn and an external crate in a hand-rolled tree
   (D3).
5. **Process-global state limits a machine to one shard per process.** The
   statics that matter:
   - `WALS`, `SEQUENCES`, `MEMORY_POOL`, the partition budget's `PROCESS`;
   - the backup gates;
   - `QUERY_CACHE`, `catalog_model::MODELS`, the `cancel` `REGISTRY`;
   - saved configuration.

   Several `Database`s per process (design phase 4) needs an audit of each.
6. **Persistent indexes are off by default (D5).** Phase 3's follower apply
   stalls on cold rebuilds without them.

## 9. What sharding.md gets wrong today (for the deferred refresh)

- **Status.** No sharding code exists; the only sharding commit is the design
  (`2205b992`).
- **§8.2.** The `SEQUENCES` dependency is resolved (keyed by database id), and
  partitioned `SERIAL` reserves in steps of 1024.
- **§8.1.** A composite PK is recognised by the `<t>_pkey` name, and a custom
  name is refused (`9e31d838`). Its `_pkey` index comes from the
  `unique_keys` loop (`index/mod.rs:2176`). What is missing is the persisted
  flag, not recognition.
- **§11.1, §16.** The partition cap is 2²⁰ per database, not 65,536.
- **Persistent indexes.** v1 is built (phases 0–2), with
  `index_storage = rebuild` by default. Every "with persistent-index v1"
  figure assumes the snapshot setting.
- **§3.2 and §5.** A gateway cannot reuse pgwire's planning: rewrites issue
  internal statements, and subqueries run in pgwire. Hence §4.1's forwarding
  gateway.
- **§5.3 and §16 phase 1c:**
  - `Acc`'s `ExactSum` has two more fields (`own_scale`, `base`).
  - The approved top-N wrapper is `sort_rows_for_merge`, which re-sorts; a
    streaming merge is not built.
  - The in-process fan-out folds once over the union; it does not merge
    partials.
- **§6.** Delete has no guard hook. `insert_version` and the P9 reshape
  `append` stamp versions outside the three paths the design lists.
- **§9.1.** Replication was hardened (record-before-commit, pipelining at
  depth 8, `REPLICATION CHECK`). It is still statement-based and
  default-database-only, so the refusal in cluster mode stands, for the
  reasons now in replication.md §9.
- **§3.4 and §1.** `txn_start_time` exists as `StatementClock`.
  `WAL_ENABLED` cannot be turned off except by an embedder.
- **New surfaces the design does not mention:**
  - native protocol and the client crate;
  - MySQL/TDS;
  - `kv_cache`/Redis and KV storage;
  - schema objects in `__rdb_*`;
  - PL/pgSQL DO blocks;
  - LISTEN/NOTIFY;
  - full-text and collation companions;
  - branching;
  - the data-directory lock and embedded mode.

  §4.3 B1a.8–.11 says what each does in cluster mode.
- **Line anchors.** All of them are stale: `route()` is `route.rs:68`; the
  status decoders are `status.rs:590`/`:1584`; the table write paths are as
  in §4.3 here.
