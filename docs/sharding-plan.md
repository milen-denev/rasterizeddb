# Sharding: implementation plan

Status (2026-11-24, branch `dev-sharding`): **B0 to B3 built, B5 in part,
T1 to T9 in part. Not yet tested;** the tests are written after the whole
plan.

| Phase | State |
|---|---|
| B0–B1c | Built: `route_v1`, the shard map and its signing, ownership hooks on every stamping path, the gateway (rshard over rastcp/2 mTLS, virtual sessions, classification), fan-out with Concat, CountSum, Any, TopN and the aggregate, top-N and DISTINCT coordinator, and Redis routing by key |
| B2 | Built: `rdb-shard` (init, show, move-tenant, rollback, retire, rebalance with `--apply`/`--every`, ddl, keygen, sign, exec, resolve), engine-fenced range moves, the offline balancer |
| B3 | Built: our own Raft (`rasterizeddb_raft`), row-image capture, follower apply, `Op::Ddl`, leader-only serving |
| B4 | **Not built:** online range moves, the gather for windows and joins, the LISTEN/NOTIFY hub, MySQL/TDS on gateways, reshape under the schema epoch. §4.8 is an outline to be replanned now that B3 has landed |
| B5 | Built: 2PC (`sharding_2pc`), the anchor decision log, `resolve`, reference tables (sharding.md §8.3). **Not built:** decision-pausing marks; reference-table writes atomic by 2PC |
| B6 | **Not built:** closed timestamps, follower and lease reads, distributed deadlock detection, clone splits. Outline only (§4.8) |
| T1–T10 | Built: the policy hooks (GPL), strict tenancy in the licensed crates (T1–T7, T8 directory adoption, T9 outbound allowlist, T10). **Not built:** T8's separate tenants root; disk quotas |

Written 2026-11-23 against `dev` at
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

**Two sharding paths (owner, 2026-11-23).** A deployment uses either or both:

1. **Key sharding.** Rows are spread over shards by a column's value
   (`#[rdb(shard_by = "store_id")]`). Each statement goes to the shard that
   owns its key. Either the client (Track A `Router`, ORM) or the gateway
   (Track B) does the routing, with the same `rasterizeddb_router` function
   and map.
2. **Per-user shards.** Who logs in decides the shard. The gateway maps the
   login role to one shard at authentication and **locks** the session
   there: no other shard, no fan-out, no way to switch. The user's data
   lives whole on that shard; tables need no tenant column (G11).

Built in two tracks, in this order:

- **Track A, client-side sharding (§3).** A shared routing crate, a
  shard-aware router in `rasterizeddb_client` with its ORM routing models by
  their shard key (`#[rdb(shard_by)]`), a shard identity row on every server
  and an offline tenant-move tool. N independent servers, each with the
  existing replica for HA. There is no engine change on the statement path. It
  is a few weeks of work and covers apps whose every query is scoped by a
  tenant key (pms by `domain_id`, Kostas by `store_id`).
- **Track B, engine sharding (§4).** The design's phases 0 to 6, revised
  against the tree. A dedicated gateway binary (§4.9) routes by hashing one
  value and relays bytes. It is built to stay within a few percent of a raw
  byte relay, with zero allocations per statement. Shards enforce
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
| D3 | Phase 3 consensus | **Decided 2026-11-24: our own Raft**, no third-party crate. It is held to the plan's correctness bar: the power-loss harness on every log and vote write, a deterministic simulation (seeded network partitions, reorderings, crashes) checking election safety, log matching and state-machine safety, and no merge until that runs clean |
| D4 | Default feature list | Follow the convention: `sharding` and the client's `sharding` go in `default`, dormant until configured. `sharding_raft` and `sharding_2pc` join `default` only once each phase meets its gate. |
| D5 | Persistent index default | Flip `index_snapshot.index_storage` to `snapshot` before phase 3. Follower apply and relocation depend on a cheap reopen. |
| D6 | Where Track B stops | Phase **1c** gives write scale-out without HA. Phase **3** is the first production topology for a large target. Decide this before B1a starts, because phases 1–2 alone have no replicas. |
| D7 | HA before phase 3 | Allow the **existing replication one-way per shard** as a warm standby with manual failover. It is statement-based, with known divergence risks (§4.3 B1a.13). The alternative is RF1 plus S3 restore. |
| D8 | Default for a statement with no route key (`unrouted`, B1b) | **`refuse` by default.** Turn on `fan_out` per role (reporting, admin) or per gateway where the app expects it. A forgotten key then surfaces as an error instead of a silent whole-cluster scan. `unrouted_writes = each_shard` is never a gateway-wide default, because it is not atomic before 2PC |
| D9 | User-based routing (G11) | **Decided 2026-11-23:** two paths only. Key sharding (`shard_by`, routed by client or gateway) and per-user shards (login → one shard, locked). The earlier `user_key` mode, a login carrying a key into key sharding, is dropped. `user_routing = off` by default |
| D10 | Licensing | **Decided 2026-11-23:** only the service and enterprise features are dual-licensed: `tenant_isolation`, `segregation_policies` and G11 user routing, in their engine and gateway parts. Everything else stays GPL-3.0-only: the engine, partitioning, sharding, `rasterizeddb_router`, the gateway's routing, the client/ORM, R-ALL replication, and the T0 security fixes. Under the dual licence, an organisation with more than 100 employees, more than 1 million in revenue or more than 5 million in assets must obtain a licence from **Denev Operations EOOD**. The layout this needs is in §10 |
| D11 | HA for customer databases | **Decided 2026-11-23:** replication covers **every database in an instance, any number of them**, not only the default one. The work items are in §4.10 (R-ALL) |
| D10b | Licence of the licensed crates | **Decided 2026-11-24: PolyForm Small Business 1.0.0**, adapted by the owner's lawyer to D10's thresholds (more than 100 employees, 1 million revenue or 5 million assets; the stock text has a people and revenue limit only, and no currency). The rest of the tree stays GPL-3.0-only |
| D12 | Are the dual-licensed features in the default build? | **No (recommended).** They break the "default all features" rule on purpose: the default binary and images stay pure GPL, and a separate `enterprise` build (binary, Docker tag, package) carries them. Then nobody runs licensed code without choosing to |

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
| **`rasterizeddb_router`** (new workspace member) | none | `xxh64`, key framing, `level_hash`, point assembly, `PointRange`, `RangeMap`, the shard-map codec | none | always built: core's partitioning depends on it |
| `rasterizeddb_client` | `sharding` | the `shard` module (map, router, fan-out, health, epoch check); the ORM's sharded surface (`ShardedModelExt` on `Router`, fan-out `Select`, sharded `Cached`/`KvRepo`, `Migrator::run_sharded`); the `rdb-shard` tool (behind `required-features`) | `rasterizeddb_router` only | yes, beside `derive` |
| `rasterizeddb_client_derive` | none | emits `Model::SHARD` and `shard_key()` for every model; plain data with no routing code, so a client built without `sharding` still compiles derived models | none | — |
| `rasterizeddb_core` | `sharding` | `core/shard/*` (map, ownership log, admission, `Classify`, the shard side of rshard, the virtual-session backend endpoint), the ownership guard, `--role shard`/`--cluster-map`/`--shard-id`, `SHOW SHARDS`, cluster DDL | none (rastcp, rustls and `rasterizeddb_router` cover it) | yes; dormant without `--role` |
| **`rasterizeddb_gateway`** (new binary crate) | `tls` (default), `native` (default), `fanout` (default from 1b) | the gateway (§4.9). **Does not depend on `rasterizeddb_core`**: no engine, no parser, no catalogue | `rastcp`, `rasterizeddb_router`, `tokio`, `rustls` (all already in the tree) | its own binary, built only when asked |
| `rasterizeddb_core` | none (GPL) | the **hook traits** the licensed crates implement: `TenancyPolicy` (admission, introspection scope, limits, process-admin split) and `PlacementPolicy`, plus the T0 security fixes. The default implementation is today's behaviour. Small, and always built | none | always |
| **`rasterizeddb_enterprise`** (new crate, **dual-licensed**) | `tenant_isolation`, `segregation_policies` | T1–T10, `CREATE TENANT`, the tenant directory, shard labels, placement policies, the placement audit. Depends on `rasterizeddb_core` and implements its hooks | none | not in default (D12) |
| **`rasterizeddb_gateway_enterprise`** (new crate, **dual-licensed**) | `user_routing`, `tenant_isolation`, `segregation` | G11, `tenancy = database`, gateway label checks. Depends on `rasterizeddb_gateway`'s hook traits, never on core | none | not in default (D12) |
| **`rasterizeddb_server`** (new binary crate, GPL) | `enterprise` = `["dep:rasterizeddb_enterprise"]` | today's `rasterizeddb_core/src/main.rs`, moved, with its binary still named `rasterizeddb_core` so deploy scripts, Docker and the Arch package keep working. With `enterprise`, it installs the licensed hooks at startup | — | `enterprise` off by default |
| `rasterizeddb_core` | `sharding_raft` = `["sharding"]` | phase 3: our own Raft (D3), log store, state machine, capture sink, apply loop, ReadIndex, learners | none | after phase 3's gate |
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

### A1. The `rasterizeddb_router` crate

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
  client (`family` + bytes, mirroring `rasterizeddb_router`'s families). The
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

- An app holds `Vec<PgPool>` plus `rasterizeddb_router`, and calls
  `pool_for(map, key)`.
- Bound parameters are coerced to the column type on the server, so hashing
  the Rust value under the column's family matches the engine.
- The deliverable is an example in `rasterizeddb_router/examples/`, not a
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
 client ─pgwire/native─► rasterizeddb_gateway (own binary, no Database)      shard (--role shard, one Database)
                          route template cache: stmt shape → "key = $2, int"  ┌► backend endpoint: virtual sessions
                          hash 1 value → range map → shard   ──rastcp/2 mux──┤   (pgwire session per client session)
                          relay pgwire bytes both ways, no row decode         │   admission: map/owner/schema epoch
                          first sight of a shape: Classify{sql} ─────────────┘   rewrites, views, triggers, CTEs run HERE
                          no key + unrouted=fan_out: tier-1 merge here,           ownership: route_where ∩ Owned,
                          tier-2 (aggregates, top-N) on a coordinator shard
                   Track A routers connect to shards directly ─────────────►      FOREIGN refuses, write guard
```

- **The gateway forwards and does not execute.** It is a separate,
  lean binary (§4.9) with no `Database`, no SQL parser and no catalogue
  shells. A shard classifies each statement shape once, and the gateway
  caches the answer.
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

**B1a.1 Roles and flags.** `--role shard`, `--cluster-map <file>`,
`--shard-id`, `--cluster-id`; all under `#[cfg(feature = "sharding")]` in
`main.rs`. The gateway is its own binary, `rasterizeddb_gateway` (§4.9),
not a role of the server. A shard is one `Database` per process: the data-directory lock and
`EngineSlot` already enforce one engine per process (a5d81376). Several
shards on one machine are several processes, as the design says.

**B1a.2 `core/shard/` module layout:**

| File | What |
|---|---|
| `map.rs` | `ShardMap` over `rasterizeddb_router::RangeMap`: epoch, `route_v`, ranges → `(ShardId, owner_epoch)`, `validate` (total, disjoint, `BLOCK`-aligned) |
| `ownership.rs` | `db_shard_ownership.log` on `core/framed_log.rs`: `ACQUIRE`, `RELEASE`, `INCOMING`, `FENCE`, `EVICTED`; `ArcSwap<OwnedRanges>` published only from accepted map + ownership records together |
| `admission.rs` | one function every request passes: placement generation, `(range, owner_epoch)` Owned and named by the accepted map, schema epoch, per-range admission lease |
| `guard.rs` | the ownership write guard (one comparison against the `ArcSwap`); `()` without the feature |
| `classify.rs` | answers the gateway's `Classify{sql}` with a route template (B1a.6), using the engine's own parser, views and `route_where` |
| `rshard.rs` | the shard side of the protocol over rastcp/2: virtual sessions, `Classify`, fan-out `Exec{Part}`, map and epoch exchange |
| `backend.rs` | the virtual-session endpoint: one pgwire session per gateway client session, driven over a mux stream and writing replies straight into mux frames (§4.9, G5) |
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

**B1a.6 Classification: on a shard, once per statement shape.** The
gateway routes in this order:
0. **The login role's shard** (G11, per-user shards), resolved once at
   authentication. A pinned user skips steps 1–4 entirely and is locked.
1. **A route key carried with the request.** The native request header
   field (set by the ORM and `Router`, A4b), or `SET rdb.route_key` /
   `SET LOCAL rdb.route_key`. No inspection of the SQL at all.
2. **A cached route template** for the statement's shape:
   - A prepared statement's shape is its Parse text.
   - A simple query's shape is its text with literals replaced by slots, by
     the shared literal tokenizer (G3).
   - The template says where the level-0 key is: `Param(i, family)`,
     `Slot(k, family)`, `Home`, `FanOut(shape)` or `NeedsKey`.
3. **On a cache miss**, the gateway sends `Classify{sql}` to any live shard
   (its home shard by default) and caches the answer under the cluster
   schema epoch.

   **The shard classifies with the engine's real machinery:** its parser,
   its views, functions and triggers, and the same `sarg::extract_gated`
   (`filter/sarg.rs:555`) and `route()` that `route_where` enforces. So the
   gateway carries no SQL parser and no catalogue, and statements through
   views route automatically when their key is visible after expansion.
4. **No key:**
   - `NeedsKey`: `0A000 "statement needs a route key in cluster mode"`.
     This is fail-closed ([[a-guard-is-an-allowlist]]).
   - `FanOut`: decided by the `unrouted` option (B1b): refuse, or send to
     every shard and combine the results.

A wrong template costs an `RDB02` from the shard, never wrong rows. A
template that produced an `RDB02` is dropped from the cache and counted
(`route_template_refuted`), so it does not keep failing.

**B1a.7 Gateway session semantics.** The mechanics (relay, threads,
buffers) are in §4.9; this item fixes what a session means.
- **Backend sessions.** One virtual session per shard per client session,
  opened lazily on the shard's mux connection.
  - Each is a real pgwire session on the shard. Every rewrite (DO blocks,
    temp tables, MERGE, sequences, views, triggers, CTEs, `catalog_guards`)
    therefore runs there, unchanged.
  - Session `SET`s are recorded at the gateway as raw messages and replayed
    to a backend session when it opens.
- **Prepared statements.**
  - Parse/Bind/Describe/Execute are forwarded **verbatim**. Parameters are
    never inlined at the gateway; the shard inlines as it does today
    (`pg_params::inline_params`, `pg_params.rs:97`).
  - A statement Parsed on one backend is re-Parsed lazily on another the
    first time it routes there, cached per backend session.
  - Routing happens **at every Bind/Execute** by hashing the bound key
    value, never at Parse (S12).
- **Describe.** Goes to the session's current shard, or the home shard.
  Schemas are identical under the schema epoch. The shard's describe cache
  key (`pgwire.rs:3719`) gains the schema epoch.
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
| native (rastcp) | through the gateway: the request header carries the route key (ORM/`Router` fill it), so routing parses nothing. On the shard the virtual session runs today's native path (`serve_native_request`, `database/mod.rs:6320`). Track A clients may still go direct | — |
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

### 4.4 Phase B1b: the no-key option, fan-out `COUNT` and rows

**The option: what happens when no key is given.** A statement may carry no
route key while its template is not single-shard (`FanOut`). The gateway's
`unrouted` setting then decides:

| Setting | Set where | Reads | Writes |
|---|---|---|---|
| `unrouted = refuse` | gateway config; per role (`ALTER ROLE … SET rdb.unrouted`); per session (`SET rdb.unrouted`) | `0A000 "statement needs a route key"` | `0A000` |
| `unrouted = fan_out` | same | Sent to every shard owning a routed range, and the results combined **exactly** (the operators below). A shape the merge cannot make exact is refused, never approximated | still refused unless `unrouted_writes` says otherwise |
| `unrouted_writes = each_shard` | per session or role only, never as a gateway-wide default | — | Autocommit only. `UPDATE`/`DELETE` run on each shard as that shard's own transaction (`EXECUTE ON EACH SHARD` semantics): row counts are summed, and any failure errors naming the shards that already applied. Inside `BEGIN`: refused until 2PC (B5) makes it atomic |

- **`INSERT` always routes by the row's key values.** Rows for several
  shards are a multi-shard write (`rdb.split_bulk_writes`, sharding.md §8.5).
- **Fan-out runs only under certain sessions:**
  - autocommit or `BEGIN READ ONLY`, at READ COMMITTED;
  - refused in REPEATABLE READ, SERIALIZABLE and write transactions;
  - a role can forbid it with `rdb.cross_shard_read = refuse`.
- **What a fan-out result means:**
  - Each shard reads at its own snapshot.
  - Every transaction is seen whole, because there are no cross-shard
    transactions before B5.
  - Transactions committing concurrently on different shards may appear in
    any combination.
  - This text is in the docs and in `SHOW SHARDING`.
- **Cost stays bounded even with `fan_out`.** A statement without a key
  visits every partition on every shard (sharding.md §11.6). So
  `shard_unrouted_max_partitions` still refuses one whose estimate is too
  large (`54000`), and a NOTICE fires past `shard_fanout_warn`.
- **The default is decision D8.**

**Where results are combined.**
- **Tier 1, in the gateway: shapes that need no decoding.**
  - A plain row stream with no `ORDER BY` and no aggregate: each shard's
    DataRows are relayed in turn under one RowDescription, and the
    CommandComplete counts are summed.
  - `LIMIT k` without `ORDER BY`: stop after k rows and cancel the rest.
  - `COUNT(*)` alone: sum one int8 per shard.

  This is the gateway's `fanout` feature. It keeps the gateway free of
  engine code (§4.9 G1).
- **Tier 2, on a coordinator shard: everything that needs engine
  semantics.** That covers aggregates, `GROUP BY`/`HAVING` and
  `ORDER BY … LIMIT` (B1c), and later `DISTINCT`, windows and joins (B4).
  - The gateway forwards the statement to one coordinator: the home shard,
    or the least-loaded shard.
  - The coordinator scatters `Exec{Part}` to its peers over rshard, merges
    with the engine's own `Folder`, sort and post-scan pipeline, and returns
    pgwire bytes that the gateway relays as usual.
  - **Cost:** one extra hop, on a path that is already a whole-cluster
    operation.
  - **Benefit:** the merge logic exists once, in the engine. A second
    implementation in the gateway is how a merged `NUMERIC` sum ends up
    looking right and being wrong ([[the-number-that-looks-right]]).

**Mechanics, whichever tier scatters:**
- **The scatterer** sends `Exec{Part{ranges}}`. Fan-out statements are a
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
  - a gather semaphore on each scatterer, gateway or coordinator (`53200`);
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
  `LIMIT`. The coordinator shard (tier 2, B1b) builds its `Folder` from its
  own plan of the statement, merges
  (`Folder::merge`, `:3510`) and finishes once (`finish_into`, `:3535`).
  Today `Folder::merge` serves only thread-parallel cell folds
  (`executor.rs:13333`).
- **Top-N:**
  - Shards return scan-schema rows carrying every sort key and its `__null_`
    companion, capped at k+o.
  - The coordinator re-cuts with `sort_rows_for_merge`
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

### 4.6b Auto-balancing (owner, 2026-11-24)

The owner asked for automatic balancing between shards. It moves up from
B6 to sit on B2's moves, and covers both sharding paths.

**What is measured.** Each shard reports its load per partition range of
every sharded table (`SHOW SHARD LOAD`): the range's level-0 blocks, its
stored bytes and rows (the partition map's `STATS`), and its statement
count since the last report. A range is the unit a move carries; a hot
range wider than one block is split at a block boundary first (a carve,
B2 step 1) so the next round can move half of it.

**The planner** (`rasterizeddb_router::balance`, pure and dependency-free, so
the authority, `rdb-shard` and tests share it):
- A shard's load is `w_bytes × bytes + w_ops × ops`, normalised by the
  cluster's totals.
- Balanced when every shard is within `tolerance` (default 10%) of the
  mean. Hysteresis: a round starts only past `tolerance`, and stops once
  inside `tolerance / 2`, so ranges do not flap.
- Greedy: move from the heaviest shard to the lightest the range that best
  closes their gap without overshooting it; repeat while the caps allow.
- Caps per round: `max_moves` (default 4) and `max_bytes` (default 64 GiB),
  so a round is bounded in time and disk.
- Never moves a range pinned by a placement policy (§4.10) or by an
  operator (`PIN RANGE`), nor onto a shard marked `draining` or full.
- A range bigger than the gap it could close is proposed for a split
  instead.
- Output: a list of moves `(blocks, from, to, bytes)` and splits; applying
  them is `ShardMap::reassign` per move (one epoch each) plus the data copy.

**Per-user shards (G11).** A pinned user is never split. New users are
placed on the least-loaded shard (`balance::place`). Existing users move
only when a shard is past `user_shard_max_load` (default 90% of the
cluster mean × (1 + tolerance)), whole and drained first, as G11 moves are.

**Who runs it.**
- Track A, offline: `rdb-shard rebalance --plan` prints the round;
  `--apply` runs each move with the offline range move (fence, copy,
  verify, hand-off) one at a time.
- Track B from B2: the authority runs a round every `balance_interval`
  (default 10 min) when `auto_balance = on` (off by default), using the
  B2 move procedure; from B4, online moves.
- Either way a move that fails verification is rolled back and the
  balancer pauses until an operator clears it.

**Tests:** the planner converges and never violates a pin, on generated
skewed clusters; moves lose and duplicate nothing (S7); a round under
load keeps p99 within the move-rate gate.

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

**B3 as built so far (2026-11-24), behind `sharding_raft`, off by default:**
- `rasterizeddb_raft`: our own Raft core (D3), deterministic and I/O-free:
  pre-vote, leader stickiness, check-quorum, ReadIndex, snapshots,
  single-server membership changes with learners, leadership transfer,
  flow control; its codec.
- `core::shard::raft_store`: the durable log, hard state and snapshot.
- `core::shard::raft_group`: the runtime (persist, send, apply, answer by
  index and term) and the `StateMachine`/`Transport` seams.
- `core::shard::raft_net`: the mTLS transport.

**Still to build, together with its gate** (the deterministic simulation
over `RaftNode`s with seeded partitions, reorderings and crashes; the
power-loss harness on the store; S13–S18), because it moves visibility onto
the hottest path and cannot be trusted without them:
1. *Capture.* A per-transaction-family op buffer (insert image; update as
   key + new image; delete by key), filled at the four stamping paths
   beside `shard_hooks::check_stamp`, keyed by root TxId so a block's
   statements accumulate, discarding a rolled-back savepoint's ops by the
   family's outcomes.
2. *Propose at commit.* In `commit_written_transaction` where
   `prepare_replication` runs: the family's ops proposed and awaited
   (the group's `propose`, on a multi-thread runtime), and the local commit
   only after it applied; `Lost`/`NotLeader` aborts the transaction.
3. *Apply.* On every replica, by log index: skip at or below the durable
   applied index; the leader commits the waiting origin transaction; a
   follower (or a leader whose origin transaction died with a restart)
   writes the images under a fresh transaction at the storage layer, by PK
   (B0.3's identifying key, B0.6's index wait), asserting the old image,
   with capture off, no defaults, FK actions or sequences; the applied
   index written in the same storage transaction.
4. *Reads:* ReadIndex for linearizable reads; followers serve none until
   then.
5. `SeqReserve` and `Schema` entries; the in-place update path off;
   learner relocation through the backup snapshot routine.

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
  - online automatic rebalancing (the offline balancer is §4.6b);
  - clone splits.

### 4.9 The gateway (`rasterizeddb_gateway`)

The gateway sits on every statement's path, so it gets the hardest budget
in this plan: be invisible next to the shard's own cost, and never be the
reason a statement fails or returns wrong rows. It is built in B1a and
extended by B1b/B1c's fan-out.

**Budgets.** These are merge gates, measured as in §6 and
[[how-to-profile-this-repo]]: instructions and cycles per statement with
`perf stat`, alternating A B B A, never wall-clock on a loaded box.

| # | Budget | Why |
|---|---|---|
| B-1 | Routed statement, steady state: gateway cycles (user + kernel, TLS on both legs) ≤ **1.25×** a plain byte relay doing the same socket work (the floor, measured by G0) | Everything above the relay is routing, which should cost tens of nanoseconds |
| B-2 | Gateway user-space instructions per routed prepared point read ≤ **10%** of the shard's (today ~95k server instructions, [[native-point-query-cost]]) | The hop must not change capacity planning |
| B-3 | **Zero heap allocations** per routed statement in steady state | Checked by a counting global allocator in a dev-only bench. Allocation was this repo's largest flat cost ([[allocator-page-fault-churn]]) |
| B-4 | ≤ **1 context switch** per statement per leg (`sysp.sh`) | The `yield_now` lesson ([[yield-now-wakes-a-second-worker]]) |
| B-5 | Added latency over a direct connection, on loopback: p50 ≤ **30 µs**, p99 ≤ **100 µs** at 50% load. On the LAN: B1a's gate (< 150 µs) | — |
| B-6 | ≤ **8 KiB** per idle client session, ≤ **64 KiB** per active one | 100k idle connections in under 1 GiB |
| B-7 | Cycles per statement ≤ **PgBouncer's** (transaction pooling) on the same box and workload | The canonical lightweight proxy is the yardstick. Measured once in G0 and recorded |

**G1. A lean binary.**
- **Own crate.** Its only dependencies are `rastcp`, `rasterizeddb_router`,
  `tokio` and `rustls`, all already in the tree.
- **No `rasterizeddb_core`:**
  - none of the engine's statics (`MEMORY_POOL`, `WALS`, the query cache);
  - no parser;
  - no catalogue.
- `#![forbid(unsafe_code)]`.
- It starts in milliseconds, reading its map from a file or from any shard.

**G2. Thread-per-core.**
- **N workers.** Each is a `current_thread` tokio runtime with its own
  `SO_REUSEPORT` listener (rastcp `reuse_port`, f1c6a2be). Each has its own
  mux connections to every shard, its own route-template cache and its own
  buffer pool.
- **A client connection stays on one core for life.** Nothing on the hot
  path crosses threads.
- **Shared read-mostly state:** the map (an `ArcSwap`, one load per
  statement) and the template cache's shared second level, filled on a
  per-core miss.
- **Why here:** rastcp's measurements found thread-per-core worth 4–11% only
  for echo-like handlers ([[rastcp-perf-phases]]). A relay is exactly that
  case; the database handler on a shard was not.

**G3. Routing without parsing.**
- **The per-statement work:**
  1. Find the route value: a request header field, a Bind parameter by
     index, or literal slot k.
  2. Normalise it to its family's bytes.
  3. Hash it with `xxh64`.
  4. Binary-search the range map (4,096 ranges is 12 comparisons).

  Target: ≤ 100 ns.
- **Bind formats.** Both binary and text, normalised exactly as the server
  coerces (int2/4/8 by width, UUID octets, text bytes). Property-tested
  against `rdb_route_point` (A2).
- **The literal-slot tokenizer** for simple queries handles strings, `E''`,
  dollar quotes, comments and nested parentheses. It lives in
  `rasterizeddb_router`, shared with core's text template (663c509f) so the
  two cannot drift, and is differential-fuzzed against core's
  ([[scanners-must-track-nesting]], [[byte-walk-scanners-cut-utf8]]).

**G4. Byte relay, no decode.**
- **What the gateway reads:** pgwire framing (type and length), plus the few
  messages it must understand:
  - Parse, Bind, Execute, Sync, Query, Terminate, CancelRequest;
  - ReadyForQuery's transaction-status byte;
  - ErrorResponse's SQLSTATE, for the retry rules.
- **Everything else is copied as bytes** from the shard's mux frames to the
  client socket, coalesced into one `writev` per batch. That includes
  DataRow, RowDescription and CommandComplete. Nothing is decoded and
  nothing is re-encoded.

**G5. The shard writes into the frame.**
- **The endpoint.** `core/shard/backend.rs` drives a pgwire session whose
  I/O is a message source and sink over a mux stream. It is neither a
  socket nor the in-memory duplex `PgBridge` uses.
- **Why not the duplex:** it re-encodes every row, which costs ~1–5.7k
  instructions per row for MySQL/TDS ([[four-protocol-comparison]]).
- **The change it needs:** pgwire's session I/O behind a small trait. This
  is a `pgwire.rs` change, sent to its owner first.
- **Native sessions** use today's native path, with replies written into
  frames the same way.

**G6. Pipelining and batching.**
- Everything a client has pipelined is forwarded in one mux write, and
  replies stream back as they arrive.
- One mux connection per core per shard by default (rastcp/2 multiplexes up
  to `max_in_flight`). More only if measurement shows head-of-line blocking.

**G7. Per-connection machinery, never per-statement.**
- One `Deadline` per connection (rastcp's interrupt pattern, a2e0beb0) and no
  per-statement timers.
- No task spawn per statement: the session is driven on its connection task,
  as `drive_alone` does.
- No `select!` over a `watch` per statement, and no `yield_now`.
- Small futures: rare branches are boxed so large futures are not moved at
  every await (the memmove lesson from the native rounds).

**G8. Buffers.**
- Per-core pooled read and write buffers, sized from each session's recent
  high-water mark and returned when the session goes idle (B-6).
- A large result streams through a bounded buffer and is never held whole.

**G9. Authentication.**
- The gateway verifies SCRAM-SHA-256 and SCRAM-SHA-256-PLUS itself, using
  each role's StoredKey and ServerKey.
- The keys are fetched from the home shard over mTLS and cached under a role
  epoch. The gateway holds no plaintext secrets. Channel binding works
  because the gateway terminates the client's TLS.
- Backend virtual sessions open with the authenticated role, asserted by the
  gateway's mTLS identity, and shards re-check grants (sharding.md §13).

**G10. Observability at no hot-path cost.**
- **Per-core counters:** plain integers, no atomics on the hot path.
- **Summed on read:** by `SHOW GATEWAY` and a metrics endpoint.
- **What is counted:**
  - statements routed;
  - template hits and misses, and `route_template_refuted`;
  - refusals by SQLSTATE, retries and `08007`s;
  - bytes relayed and sessions;
  - a hop-latency histogram in per-core buckets.

**G11. Per-user shards (gateway only), the second sharding path.** The
gateway routes by **who logs in** instead of by what the statement says.
When authentication completes (G9), the login role is resolved to its shard,
and the session is **locked** to that shard for life. From then on every
statement on that connection goes there with no per-statement work at all:
no template lookup, no hash. Key sharding (path 1) is the other path; a
deployment can run both, with tenant users pinned and service roles routed
by key. The Track A `Router` has no equivalent; this is a gateway feature.

| | Per-user shard |
|---|---|
| What it means | The role is pinned to a shard (`shard_id`). Every session of that user goes to that shard, whole, and cannot leave it |
| The data | Tables need no tenant column. A shard holds the data of the users placed on it, like a database per group of users but spread over instances |
| Moves | Allowed only if the user's rows can be identified: the user's own database, or an owner column declared for the table in the cluster catalogue, else refused. A move is copy, verify, then re-pin, with the user's sessions drained first |

(An earlier draft also had a `user_key` mode, where the login carried a
tenant key into key sharding. The owner dropped it on 2026-11-23: the two
paths stay separate.)

*The mapping:*
- **Storage.** It lives in a cluster catalogue table,
  `rdb_user_routes(role, shard_id, epoch)`.
  It is on the home shard in phases 1–2 and under the authority from B2,
  versioned with the map epoch.
- **Gateway cache.** The gateway caches the mapping beside each role's SCRAM
  keys (G9) under a role epoch. A changed mapping takes effect at the user's
  next connection, and immediately for a move (below).
- **SQL.** `ALTER USER bob ROUTE SHARD 3` and `ALTER USER carol ROUTE
  NONE`, by `cluster_admin` only. `SHOW USER
  ROUTES`.
- **Gateway setting `user_routing`:**
  - `off` (default): statement routing only.
  - `prefer`: mapped users are pinned, unmapped users use statement routing.
  - `require`: an unmapped user is refused at login with `28000`. This is
    for gateways that serve only tenant users.

*Rules:*
- **The lock cannot be escaped.** For a pinned role, `SET rdb.route_key`,
  `SET LOCAL rdb.route_key` and a route key in the native request header are
  refused (`42501`).
- **No fan-out, ever, for a pinned role.** `unrouted` is `refuse` for it
  with no per-role override, so a pinned user never reaches another shard.
- **Service and admin roles stay unmapped** and use statement routing and
  fan-out as configured.
- **A pinned session refuses a statement it cannot keep on the user's shard.**
  - Sharded tables reaching other ranges get `RDB02` from the shard, as
    always.
  - Global tables are readable if they live on the user's shard or are
    reference tables (B5); otherwise they are refused.
- **The shard checks too (R1 holds).** It admits a pinned virtual
  session only if its own placement list names that role at the request's
  epoch, else `Moved` (`before_effect`, so the gateway refreshes and
  retries). A gateway with a stale mapping therefore costs a retry, never a
  session on the wrong shard.
- **A move drains first.** Moving a pinned user fences new sessions
  for that role with a retryable `57P03` for up to 5 s, waits for open
  transactions to finish, copies, verifies, bumps the placement epoch and
  releases.
- **Pinning is placement, not authorisation.** Users co-located on one shard
  are kept apart by that shard's grants (and row policies, where the engine
  has them), exactly as on a single node today. The plan does not claim
  more. Where separation must be a **guarantee**, as for a service with many
  customers or company-policy segregation, use §4.10's models. Those come
  with the engine hardening that makes the guarantee true.

*Cost:* one lookup per connection, at login; zero per statement.

*Tests:*
- A pinned user with a forged `SET rdb.route_key`, a header route key, or an
  unrouted statement is refused.
- A pinned session arriving at a shard that does not host the role
  gets `Moved` *(skip the shard-side placement check)*.
- A remapped user's next connection lands on the new shard.
- Moving a pinned user under load loses and duplicates nothing (the
  digest equals the twin's).
- `require` refuses an unmapped login.

*Size:* about 1.5k LOC in the gateway and on the shard, plus the catalogue
table.

**Rock solid: what must never happen, and what prevents it.**

| # | Never | Prevented by | Test (mutation) |
|---|---|---|---|
| R1 | Wrong rows from a misroute | Shard ownership (B1a.4–.5). The gateway is never trusted for correctness | A gateway with a deliberately wrong map gets only `RDB02`/`Moved`, never rows *(skip the shard check)* |
| R2 | A write applied twice | Retry only on `before_effect` refusals. A connection lost after dispatch is `08007` | Kill the shard after a commit is sent: the counter rises exactly once *(retry on connection loss)* |
| R3 | One slow client stalling others | Per-stream flow control on rastcp/2 (verify it exists; add credit windows if not) and bounded per-session buffers. A session whose client stops reading stops reading its own stream, not the shared connection | A client that never reads a 1 GB result; other sessions' p99 unchanged *(unbounded buffer)* |
| R4 | One dead shard affecting other tenants | Per-shard circuit state: requests for that shard fail fast (`08006`/`57P03`), others are untouched. One dial attempt speaks for every waiter (rastcp 2c352d34) | Kill shard 2 under load: shards 1 and 3 keep their p99 |
| R5 | Unbounded memory | B-6's budgets, the gather caps and semaphore (B1b), max sessions and max in-flight per core; past them, shed with `53300` | A connection and request flood stays under its RSS bound |
| R6 | A crash losing state | No durable state except the highest map epoch seen (one small file). Any number of gateways run, with no coordination between them; a restarted gateway is simply a new one | kill -9 mid-transaction: the shard rolls back the orphaned virtual session, via a session lease on the mux connection |
| R7 | A stale map serving writes | Every shard reply carries its map epoch. A newer epoch makes the gateway fetch the signed map from that shard before routing more writes, and epochs never go down | Shard at epoch e+1, gateway at e: the next write waits for the fetch *(route on the old map)* |
| R8 | A panic killing the process | Each session's task runs under `catch_unwind`; a panic ends that session with `XX000` and is counted | An injected panic in one session; the others continue |
| R9 | A malformed client harming anything | Length-checked framing, a maximum message size, and a fuzzed parser (bounded, seeded corpus) | Fuzz the frame reader and the Bind decoder |
| R10 | Downtime on upgrade | `SO_REUSEPORT` hand-over: the new process binds, the old one stops accepting and drains up to a deadline. Open transactions finish; idle sessions close with `57P01` | A rolling restart under load: no failed statements beyond the idle closes |

**Build order, inside B1a and before any feature work:**
1. **G0, the floor.** A spike kept as a bench: a pure byte relay over the
   same sockets, TLS and mux, with a fixed shard. It measures B-1's floor,
   B-7's PgBouncer comparison and B-5 on .7/.4. If the relay itself misses
   B-5, fix the transport (rastcp) first, not the gateway.
2. **G1–G4 and G6–G8** on top. Each slice is re-measured against the floor
   with the A/B harness (the `.claude/reviews/rastcp-perf/` and `protobench/`
   methods).
3. **G5** on the shard side, in parallel.
4. **G9 and G10** next, then **G11**: user-based routing needs G9's role
   cache.

A slice that breaks a budget does not merge until it is back within it.

**Size:** the gateway crate is about 5–7k LOC plus ~3k of tests and benches.
The shard endpoint (G5) is about 1.5k.

### 4.10 Two tenancy models: database as a service, and enterprise segregation

The owner's goal is to run RasterizedDB **as a service** for many customers,
each with their own credentials, where no customer can see, change or even
learn about another's database. The same machinery serves an **enterprise
feature**: segregating data by company policy (region, dedicated hardware,
compliance tier). Each model is its own Cargo feature and its own runtime
switch, dormant until configured.

| Model | Cargo feature | Runtime switch | Needs |
|---|---|---|---|
| **Database as a service** (one database per customer, strict isolation) | `tenant_isolation` in `rasterizeddb_enterprise` and `rasterizeddb_gateway_enterprise` (dual-licensed, §10) | core `--tenant-mode strict` (`off` by default); gateway `tenancy = database` | Works on **one** shard too (a single server plus gateway); sharding spreads customers over shards |
| **Enterprise segregation** (placement by policy) | `segregation_policies` (needs `sharding`) in `rasterizeddb_enterprise`; `segregation` in `rasterizeddb_gateway_enterprise` (dual-licensed, §10) | `placement_policies = enforce` (`off` by default) | The authority and moves (B2); `tenant_isolation` when policies apply to databases |
| (base for both) per-user shards (G11) | `user_routing` in `rasterizeddb_gateway_enterprise` (dual-licensed, §10) | `user_routing = off \| prefer \| require` (G11) | — |

**The security floor comes first, in every build.** It is now planned in full as **R0 of the role system plan** (`docs/roles-plan.md`, branch `wip/roles`, GPL and standard). T1's process-admin split uses that plan's R1 `PROCESS ADMIN` attribute. An isolation audit on
2026-11-23 found that today **any login role can make itself superuser**.
This was reproduced live: a non-superuser ran
`UPDATE rdb_roles SET is_superuser = true WHERE name = 'me'`, reconnected,
and `CREATE DATABASE` succeeded. The same role could read every role's
password verifier. These are bugs, not missing features, and their fixes
(T0) are **not** feature-gated:

| # | Fix (ungated) | Where |
|---|---|---|
| T0.1 | User SQL cannot read or write `rdb_roles` or any `__rdb_*` catalogue table; only the auth and catalogue paths may | `pgwire/privilege_statements.rs:209-215` (early return with no ACL/owner), `core/rql/privileges.rs:392-395` (`allows` is true without an owner), `:26` / `privilege_statements.rs:253` (skips `__*`) |
| T0.2 | `IMPORT` is gated by `is_import_statement`, not by the words `IMPORT DATABASE`. `DATABASE` is optional, so `IMPORT FROM POSTGRES …` passes the gate today | `pgwire.rs:2436` vs `core/migrate/sql.rs:57-63`; native path `database/mod.rs:6498-6504` |
| T0.3 | MySQL `USE`/`COM_INIT_DB` and TDS `USE` to another database re-authenticate against it, or are refused | `mysql/conn.rs:915-944`, `mssql/conn.rs:2185-2206`, `pgwire/proxied.rs:236-275` |

**Database as a service (`tenant_isolation`).**

*How a customer connects:*
- It is standard PostgreSQL: `user=alice dbname=acme`.
- The gateway's **tenant directory** maps `acme` to its shard.
- The gateway verifies alice's SCRAM credentials against the roles **inside
  acme's own database**, fetched from that shard and cached (G9).
- It opens a virtual session locked to `(shard, acme)`. Any database switch
  is refused, on every protocol.
- User names may repeat across customers (two customers may both have
  `admin`), because credentials live inside each customer's database.
- Unknown database, unknown user and wrong password all give the same
  `28P01`, so customer names cannot be enumerated.

*Provisioning* is an operator action through the gateway's admin endpoint,
or the authority from B2:

```sql
CREATE TENANT acme OWNER acme_admin PASSWORD '…' [ON SHARD 3 | POLICY eu_dedicated]
       [LIMITS (connections = 50, storage = '20GB', statement_timeout = '60s')];
ALTER TENANT acme SET LIMITS (…);   MOVE TENANT acme TO SHARD 5;   DROP TENANT acme;
SHOW TENANTS;
```

`CREATE TENANT` creates the database on the chosen shard. It seeds **only**
`acme_admin`: a database-scoped administrator who may manage roles and
grants inside `acme` and nothing else. Today's `CREATE DATABASE` copies every
role and password verifier from the default database (`catalog.rs:453-467`,
`auth.rs:832-910`); tenant mode stops that.

*The tenant-safe mode*, under `--tenant-mode strict`, has these items:

| # | Item | Today |
|---|---|---|
| T1 | **Process admin** is a separate flag, granted only in the operator's database and never in a customer database. Every process-wide verb requires it: `CREATE/DROP DATABASE`, `BRANCH`, `SET CONFIG`, `STORAGE`, `BACKUP`/`RESTORE`, `REPLICATION`, `KV CONFIG/SAVE/FLUSHALL`, `IMPORT` | superuser of *any* database administers the process (`pgwire.rs:2396-2456`) |
| T2 | Customer introspection is scoped to its own database: `SHOW QUERIES`, `SHOW QUERY STATS`, `pg_stat_statements`, MySQL `SHOW PROCESSLIST` (redacted), `pg_stat_activity` | process-wide, with literals (`introspect.rs:3370-3409`, `mysql/conn.rs:2355-2370`) |
| T3 | Other databases are invisible: `SHOW DATABASES`, `pg_database`, MySQL/TDS listings and `SHOW BRANCHES` show only the current database, with no `location` path | every database listed, with absolute paths (`pgwire.rs:6503-6525`, `catalog_views.rs:1822`) |
| T4 | `SHOW BACKUP STATUS`, `SHOW REPLICATION` and `SHOW STORAGE` require process admin | ungated (`introspect.rs:1864-1885`, `storage/sql.rs:711`) |
| T5 | Session control is scoped to the database: MySQL `KILL` and native cancel tokens include the database id | `KILL` checks the user name only (`mysql/conn.rs:946-967`); cancel tokens keyed by (user, token) (`cancel.rs:443-470`) |
| T6 | Refuse to start with `trust`, or with native authentication failing open | trust administers; native auth fails open with no password role (`database/mod.rs:4711`) |
| T7 | Per-tenant limits (B-budgets for customers): connections (enforced at the gateway, cheap), a server-side `statement_timeout` ceiling, idle timeouts on by default, a disk quota per database, KV `maxmemory` bounded by default, the session counted only after authentication | one process-wide `max_connections` (500); `statement_timeout` client-set and 0-able; no quotas (`pgwire.rs:160-190`, `:12977`, `kv_cache/mod.rs:101`) |
| T8 | Customer databases live under a separate tenants root, never adopting an existing directory; `STORAGE … LOCATION` and `PLACE TABLE AT` are confined to the customer's own root | inside the default root; any path accepted once escalated (`storage/sql.rs:319-358`) |
| T9 | Outbound connections (PG import, S3 settings) are process-admin only, through an allowlist of hosts | — |
| T10 | The Redis port stays off in tenant mode, or `AUTH` names the database | serves the default database only (`main.rs:1955`) |

*Moves, backups and HA per customer:*
- **Moves.** A customer moves between shards whole, with `MOVE TENANT`:
  1. The gateway drains the customer's sessions (new ones wait up to 5 s,
     retryably).
  2. Copy the database through the backup snapshot routine. Each database
     has its own WAL.
  3. Verify, flip the tenant directory, release.
- **Backups.** Each customer's database already archives to its own prefix.
- **HA for every database (D11, decided): R-ALL. Largely BUILT since this plan was written:** `0d65afd8` (merged into local `dev` as `21a4cc83`, 2026-11-23) makes every database replicate, routed by the greeting's database through the one listener, with `CREATE`/`DROP DATABASE` carried in the default stream. Re-check the items below against it before starting; what it already covers is done. The items as first planned: Replication must cover
  every database in an instance, any number of them. Today it covers the
  default database only (`main.rs:1829`), and a `CREATE DATABASE` database
  gets a "not replicated" warning. Work items, in today's replication
  (statement-based, [replication.md](replication.md)), until B3's Raft
  replaces it per shard:

  | # | Item |
  |---|---|
  | R-ALL.1 | **One stream per database.** Each `Database` keeps its own `db_replication.*` log, intents and state in its own directory, with its own sequence. Streams never mix. Apply runs in that database's context; [[task-locals-cross-databases]] is the known trap. |
  | R-ALL.2 | **One link per peer pair, carrying every database's stream.** Each transaction record reserves an opaque, typed extension payload (L1b), so licensed features such as the ledger can carry per-transaction metadata without changing the GPL frame format. Frames are tagged with the database's **name plus a creation uid**, never `Database::id`, which is a per-process counter. Pipelining (depth 8) runs per database, so one busy customer cannot stall another's stream (the same fairness rule as gateway R3). |
  | R-ALL.3 | **The database set replicates too.** `CREATE DATABASE`, `CREATE TENANT`, `DROP DATABASE`/`DROP TENANT` and renames go into a small instance-level stream, ordered so a database exists on the peer before its first record applies, and its stream stops before the drop. |
  | R-ALL.4 | **Seeding.** The handshake exchanges the database list with each stream's position. A database the peer lacks, or one that reports `needs reseed`, is seeded from the backup snapshot routine for that one database, with the per-database state files kept or stripped as [[replication-reseed-state-files]] says. |
  | R-ALL.5 | **Per-database control and status.** `SHOW REPLICATION` and `SHOW REPLICATION STATS` gain a database column. `REPLICATION CHECK [DATABASE d]` runs per database. A setting `replication.databases = all` (the default when replication is on) \| `default` \| a list, as a runtime knob. In tenant mode, all of these are process-admin only (T1). |
  | R-ALL.6 | **Branches** are databases too and replicate as ordinary databases. Branch operations (`RESET`/`PROMOTE`/`MERGE BRANCH`) are captured as instance-stream events, or refused on a replicating instance until they are. Today branches are documented as not replicated. |
  | R-ALL.7 | **Tests.** Two real nodes (the `.claude/reviews/repl-hardening/e2e.py` pattern): N databases created and dropped during load, kill -9 of each node, then a per-database verify and sum. Power-loss crash images per database. A slow database does not delay another's apply. Mutations: an untagged frame, stream order across databases, a create applied after its first record. |

  *Size:* about 3–4k LOC. It has no sharding dependency and can run in
  parallel with T1–T10.
- **Noisy neighbours.** A customer that outgrows a shared shard moves to its
  own shard, or to a dedicated one under a policy (below).

*Tests:*
- Every T-item has a mutation-checked test, run over each protocol:
  pgwire, native, MySQL, TDS.
- A **cross-tenant attack suite** runs as customer A against customer B on
  the same shard and must fail every time:
  - escalation through `rdb_roles` and `__rdb_acl`;
  - `IMPORT` SSRF;
  - `USE` switches;
  - reading `SHOW QUERIES`, `PROCESSLIST` and `pg_database`;
  - `KILL` and cancel;
  - enumeration before login;
  - storage paths;
  - exhausting connections, disk or KV memory.

*Size:* T0 about 0.5k LOC; T1–T10 about 4–5k LOC; tenant directory and
provisioning about 2k LOC.

**Enterprise segregation (`segregation_policies`).**

- **Shard labels.** Set with `ALTER SHARD 3 SET LABELS (region = 'eu-west',
  tier = 'dedicated', compliance = 'hipaa')`. A gateway has labels too.
- **Policies.**

  ```sql
  CREATE PLACEMENT POLICY eu_dedicated REQUIRE (region = 'eu-west') EXCLUSIVE;
  ```

  `EXCLUSIVE` means no other customer or policy may share those shards. A
  policy attaches to a tenant (`ALTER TENANT acme SET POLICY …`), a role
  (per-user shard) or a route key.
- **Enforcement, at every layer, so one bug is not a breach:**
  1. The authority refuses any map edit, move, rebalance or `CREATE TENANT`
     that would place data against its policy.
  2. Each shard checks at admission that the data it serves matches its own
     labels, and refuses otherwise. This covers a stale or forged map.
  3. A gateway refuses to route a policy's traffic unless its own labels
     satisfy the policy, so EU data never transits a gateway outside the EU.
  4. Backups go to the policy's backup target, such as a bucket in the same
     region.
- **Audit.** Policy changes, and every refusal a policy causes, go to an
  append-only audit table on the authority: who, when, what. `SHOW PLACEMENT
  AUDIT` reads it.
- **Out of scope here (stated):** per-customer encryption keys at rest. If
  wanted, that is a design of its own.
- **Tests:** a move or rebalance that would break a policy is refused at the
  authority, and again at the shard, with the authority check removed
  *(mutation)*. A gateway with the wrong region refuses. `EXCLUSIVE` keeps
  other customers off.
- **Size:** about 3k LOC.

**Order.**
1. **T0 now**, as bug fixes, independent of everything else, on the
   owner's go.
2. **`tenant_isolation` (T1–T10)** next. It is useful even before sharding:
   one server plus the gateway as a single-shard service.
3. **G11 user routing** after the gateway's G9.
4. **`segregation_policies`** after B2's authority and moves.

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
          G0 relay floor        ███                   (spike + bench, before B1a features)
          B1a (+ gateway G1–G10)    ████████████████████
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

1. **The gateway is the largest unknown in B1a**, for two reasons:
   - its session model: SET replay, temp pinning, prepared statements across
     backends, Describe;
   - its budgets (§4.9).

   G0 measures the floor before any feature is built, so a transport problem
   shows up first. If the gateway cannot meet B-1/B-5, or grows past ~10k
   LOC, stop at "native/Track A routers plus shard-side
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

## 10. Licensing (D10 and D12, decided 2026-11-23)

**The owner's decision.**
- **Dual-licensed:** only the service and enterprise features:
  - `tenant_isolation` (database as a service, T1–T10, `CREATE TENANT`);
  - `segregation_policies` (labels, placement policies, audit);
  - G11 user routing.

  Each covers its engine part and its gateway part. An organisation with
  **more than 100 employees, more than 1 million in revenue or more than 5
  million in assets** must obtain a licence from **Denev Operations EOOD**,
  which is responsible for licensing.
- **Everything else stays GPL-3.0-only, as today:**
  - the engine, partitioning and sharding;
  - `rasterizeddb_router`, the gateway's routing and relay;
  - the client and ORM;
  - R-ALL replication;
  - the T0 security fixes. T0 is a security floor for every user, never a
    paid feature.

**The code layout this needs.** A licence boundary has to be a crate
boundary. One crate mixing GPL and licensed code is hard to reason about
and hard to distribute.

```
rasterizeddb_core (GPL)                 hook traits: TenancyPolicy, PlacementPolicy; default impl = today
   ▲                ▲
   │                └── rasterizeddb_enterprise (dual)          T1–T10, CREATE TENANT, labels, policies, audit
   │                          ▲
rasterizeddb_server (GPL)  ───┘ only with feature `enterprise`  the binary, still named rasterizeddb_core
rasterizeddb_gateway (GPL)              hook traits for routing by user/tenant/policy
   ▲
   └── rasterizeddb_gateway_enterprise (dual)                   G11, tenancy = database, gateway labels
```

- **Hooks live in core and are GPL.** The licensed crates only implement
  them. Each hook is a trait object installed once at startup and read
  through one load, so a build without them pays nothing measurable on the
  statement path, and the plan's B-budgets are untouched.
- **The binary moves out of core.** Today `rasterizeddb_core/src/main.rs` is
  the server. A licensed crate that depends on core cannot also be linked
  by core's own binary (a dependency cycle). So `main.rs` moves to
  `rasterizeddb_server`, a thin binary crate whose binary keeps the name
  `rasterizeddb_core`.
- **Two artifacts (D12).**
  - The default build is pure GPL.
  - The `enterprise` build (`--features enterprise`) is the one that
    contains licensed code, shipped as its own binary, Docker tag
    (`…:<version>-enterprise`) and package (`rasterizeddb-enterprise-bin`).

**Caveat for the owner's legal review (this is not legal advice):**
- **A GPL threshold is not enforceable.** The threshold must sit in the
  dual-licensed crates' free licence, which therefore cannot be GPL. It
  would be a source-available licence such as PolyForm Small Business, or a
  custom one, paired with the commercial licence.
- **The combined binary needs a GPL linking exception.** The `enterprise`
  binary combines GPL core with code under that licence, and GPLv3 does not
  allow that combination by default. As the copyright holder, Denev
  Operations EOOD can add an **additional permission** to the core's GPL
  (GPLv3 §7) allowing it to be combined with the dual-licensed crates. The
  lawyer should draft that wording.
- **Copyright ownership.** All 2,518 commits are by the owner, with AI
  co-author trailers on many. The copyright should be assigned to Denev
  Operations EOOD (the licensor), and the lawyer should say how they treat
  the AI-assisted parts.
- **Dependencies are no obstacle.** All 739 third-party crates are
  permissive or offer a permissive option, and `option-ext` is MPL-2.0
  (file-level, unmodified).
- **Thresholds need a currency** (EUR or USD) and a counting rule: group or
  affiliates, which year.
- **Releases keep their licence.** Nothing released so far contains these
  features, so no GPL release is affected.

**Engineering items, on the owner's word:**

| # | Item |
|---|---|
| L1 | The hook traits in core (GPL), with today's behaviour as the default; the T0 fixes stay in core. **Shared with the ledger plan (rasterizeddb-17, agreed 2026-11-23):**<br>(a) `incompat` markers registered at runtime by installed hooks before any catalogue opens (`ledger_v1`, later `encrypted_v1`, and any format a licensed tenancy crate writes). A marker registered too late is refused, which fails closed.<br>(b) Per-table hooks besides the process-wide ones: `TableWritePolicy`, `CommitParticipant`, `ColumnCipher`, a hidden-columns provider, and a per-transaction replication payload. Each is an `Option<Arc<dyn …>>` cached on the `Table` at open, so a plain table pays one `None` check per row or commit, never per value.<br>(c) A table whose header names a policy with no installed hook refuses to open, even if the marker file was deleted |
| L2 | Move `main.rs` into `rasterizeddb_server`, keeping the binary name; update `deploy.sh`, `docker/`, `packaging/` and the PGO/BOLT release workflows to build that crate. `rasterizeddb_embedded` gains the same `enterprise` feature, linking `rasterizeddb_enterprise` and installing its hooks in `Builder`. **One slice shared with the ledger plan:** whichever gets the owner's go first does it, and the other rebases |
| L3 | Create `rasterizeddb_enterprise` and `rasterizeddb_gateway_enterprise`, each with its own `LICENSE` (the threshold licence) and `LICENSE-COMMERCIAL` (contact: Denev Operations EOOD), plus `license = "LicenseRef-…"` in their `Cargo.toml`; mark them `publish = false` until decided |
| L4 | The GPL additional-permission text, once drafted by the lawyer, in `LICENSE-GPL`'s companion file and in the core crates' `Cargo.toml` (`GPL-3.0-only WITH LicenseRef-…-exception`) |
| L5 | SPDX headers in every file of the licensed crates; a CI check that no file under a GPL crate imports from them and that the default build links neither (`cargo tree -e normal` on the default features) |
| L6 | README and docs: which features are dual-licensed, the threshold, how to obtain a licence; `SHOW VERSION` reports `enterprise` when it is built in |
| L7 | No licence-enforcement code (keys, phone-home) is planned. If wanted, it is a separate design |
| L8 | **The licensed crates, all plans together:**<br>- `rasterizeddb_enterprise`, features `tenant_isolation`, `segregation_policies`, `ledger`, `ledger_s3`, `encryption`;<br>- `rasterizeddb_gateway_enterprise`, features `user_routing`, `tenant_isolation`, `segregation`;<br>- `rasterizeddb_client_enterprise`, the ledger's client/ORM API. The derive emits only plain-data consts (`Model::SHARD`, `Model::LEDGER`) in GPL;<br>- `rdb_studio_enterprise`, the Studio panels for tenants, policies and the ledger, linked by a Studio `enterprise` feature. UI dependencies stay out of the client crates |
