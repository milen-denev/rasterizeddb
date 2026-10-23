# Multi-node sharding

Status: **designed, not built.** Line anchors were read on 2026-10-17 at
`952b3c4` (`feat/partitioning`) and are relative to `rasterizeddb_core/src/`.
`core/row/table.rs` and `core/index/*` are being edited by other sessions, so
anchors there will drift; the function names are the stable part.

```sql
SELECT * FROM assets WHERE tenant_id = 42;          -- one shard, today's semantics
SELECT tenant_id, count(*) FROM assets WHERE tenant_id IN (42, 77, 901) GROUP BY tenant_id;  -- fan-out, merged
UPDATE assets SET archived = true WHERE created_at < '2020-01-01';        -- 0A000 until phase 5
EXECUTE ON EACH SHARD $$ DELETE FROM audit_log WHERE at < now() - interval '90 days' $$;
SHARD MOVE KEY (tenant_id = 42) TO SHARD 77;
```

A sharded database is one logical database spread over many **shards**. Each
shard is an ordinary single-node `Database` with its own transaction manager,
status log, outcome log and WAL, and it owns ranges of the frozen `route_v1`
point space that [partitioning](partitioning.md) already hashes tenants into.
Inside a shard the table is an ordinary `PartitionedTable`. A **gateway**
speaks pgwire, routes each statement with the partition router, and forwards
it to the owning shard, or scatters it and merges the results. From phase 3
each shard is replicated by its own Raft group over **row images**, not
statements.

This is one of three designs for one target, and it depends on the other two:
[partitioning phase 3](partitioning.md#72-lazy-open-and-idle-close) (lazy open,
idle close, file budget) and [persistent indexes](persistent-indexes.md),
designed alongside it. All three size the target with this document's layout
(§11.1).

## What was asked, and what this gives

The target is one partitioned table of 1,000,000 tenants × ~800,000 rows:
8×10¹¹ rows at ~259 B per row on disk including pointer records and filter
checkpoints, about 207 TB. One node fails it three ways: every index and the
filter are resident and rebuilt by a full scan at open (17.6 TB of RAM for the
filter alone); there is one transaction manager per database
(`core/mvcc/transaction.rs:518`); and there is one root outcome log.

| Ask | What this design does |
|---|---|
| Serve 207 TB | **Yes, on about 64 machines** holding 128 shards at three replicas (§11). Requires partitioning phase 3 with persistent-index v1 (a cheap reopen keeps idle windows short) and, with three indexes per table above roughly 1-2% of tenants active per idle window (row-weighted; 0.9% on a 512 GB machine at the high per-row cost), or one index above ~5%, persistent-index v2, which is requirements only today (persistent-indexes §16). Sharding alone leaves 3.5–15.5 TB of resident index and filter per machine. |
| Lift the one-TM, one-outcome-log ceiling | **Yes, for tenant-local work.** 128 shard leaders are 128 commit pipelines sharing nothing on the commit path. Cluster throughput is 128 times one shard's *measured* figure (§11.4). |
| Cross-tenant reads | **Read-only fan-out** at READ COMMITTED, one snapshot per shard, exact merges for `COUNT`, aggregates with `GROUP BY`, top-N and rows. Anything else is refused, never approximated. A statement with no tenant predicate visits every partition of every shard (§11.6), so it is capped and refused by default above a partition count. |
| Cross-tenant writes | **Refused (0A000) before any shard applies anything**, until opt-in 2PC in phase 5. `EXECUTE ON EACH SHARD` covers maintenance, explicitly non-atomic. |
| High availability | **From phase 3, Raft per shard over row images**: visible only after quorum durability, failover by election, fenced by term, no acknowledged write lost. Before phase 3 there are no replicas. |
| Rebalancing | Whole shards move between machines as Raft learners at disk speed. Tenants and ranges move between shards by copy, catch-up, verify, fence and a two-party handoff (§10). |

**Honest caveats.**

1. **Sharding divides RAM by machine count; it does not change the cost per
   row.** Feasibility is decided by partitioning phase 3 and persistent
   indexes. §11 does the arithmetic at the granularity lazy open works at.
2. **No consistent cross-shard snapshot before phase 6.** Until phase 5 every
   transaction lives in one shard, so a fan-out still sees each transaction
   whole. With 2PC it can see one half applied, as in Citus and Vitess `TWOPC`.
   That is refused where it breaks a promise and announced elsewhere.
3. **Phase 3 changes the hottest code.** A commit becomes visible only after a
   quorum holds it: on the critical path, the leader's Raft append fsync in
   parallel with a round trip and a follower's append fsync. That is roughly
   today's one fsync plus a round trip, but every replica still syncs its rows
   and outcomes off the critical path, so a replicated commit costs the group
   three append fsyncs plus group-commit flushes, against one fsync today
   (§9.2, §11.4).
4. **The gateway adds a LAN round trip** (100–300 µs), except when a client
   connects to the gateway beside its tenant's leader. From phase 3 every leader
   read also waits for a ReadIndex quorum round (batched), and followers serve
   no reads before phase 6, so read capacity is the 128 leaders (§11.4).
5. **The PostgreSQL compatibility cost is real**, and listed below.

## What it is not

- **Not a distributed SQL planner.** Cross-shard joins, `DISTINCT`, windows and
  unlimited `ORDER BY` in a fan-out are refused until phase 4, then gathered
  under a byte cap. A 207 TB unrouted scan is hours of I/O: a bounded escape
  hatch, not an OLAP path.
- **Not global uniqueness.** Every `UNIQUE` and `PRIMARY KEY` contains the
  partition key (partitioning §1b).
- **Not multi-primary.** Each range has one writer, its shard's Raft leader.
  Statement replication's last-writer-wins is refused in cluster mode (§9.1).
- **Not automatic before phase 6.** Moves are statements an operator approves.

| Restriction | Why | Migration path | Lifted |
|---|---|---|---|
| A sharded table needs a `PRIMARY KEY` containing the partition key | Follower apply and moves address rows by it (§9.4, §10). Today a composite `PRIMARY KEY` is recorded as a plain unique key (§8.1), so this needs a phase 0 change | `PRIMARY KEY (tenant_id, id)` | Never; load-bearing |
| `COPY` and multi-row `INSERT` spanning shards refused unless the session opts into a per-shard, non-atomic split | Same as above | `SET rdb.split_bulk_writes = on` in autocommit (§8.5) | Phase 5, opt-in |
| `SERIAL` (int4) refused on sharded tables | 2³¹ values cannot be split disjointly across shards | `BIGSERIAL` (§8.2) or `UUID` | Never |
| Writes reaching several shards refused | No atomic commit before 2PC; Vitess's default `MULTI` commits these partially, this engine's characteristic bug | Add the tenant predicate, or `EXECUTE ON EACH SHARD` | Phase 5, opt-in |
| Subqueries must repeat the tenant predicate | The node checks every table reference (§5.2) | `WHERE tenant_id = 42` in the subquery | Phase 4: correlated key propagation |
| Unknown statement shapes refused | A fail-closed allowlist ("a guard is an allowlist") | Report it; added with a test | Every phase |
| Fan-out in REPEATABLE READ, SERIALIZABLE, or a write transaction refused; a second shard in a block only for RC non-locking reads (§3.2) | It cannot keep that isolation | Autocommit or `BEGIN READ ONLY` | Phase 6 |
| Cross-shard joins, `DISTINCT`, windows in a fan-out | No gather pipeline | Co-locate on the key | Phase 4, capped |

Non-deterministic defaults (`now()`, `random()`, `nextval()`) are **not**
refused: phases 1–2 have no replicas, and from phase 3 replication carries the
row the leader wrote, not the statement.

## 1. Topology and node roles

All roles are one binary, chosen by `--role`.

- **Shard process.** One process holds one shard replica: one `Database`, one
  node identity. Replication, backups and rastcp act only on the default
  database (`main.rs:998-1002`), and the process-global `SEQUENCES` map is keyed
  without the database name (`core/rql/sequence.rs:29-31`). One shard per
  process reuses all of them unchanged. **A machine runs several shard
  processes** (six at the target), each on its own drive where possible: the
  cheapest way to multiply transaction managers and outcome logs on one
  machine. Per-node budgets of other designs (partitioning's resident-bytes
  open budget; persistent-index v2's `machine_index_budget` and `block_cache`)
  are **machine budgets divided among
  the processes**: the authority hands each process `processes_per_machine`
  (a flag in phase 1) and each takes `budget / N` (persistent-indexes §9, §16),
  never a per-process default six times over. Several `Database`s per process comes
  in phase 4, after the `SEQUENCES` fix (§8.2).
- **Gateway** (`--role gateway`, or `--gateway` on a shard process). It parses,
  classifies, forwards over **rshard** and merges. It holds **catalogue
  shells** (schemas without data, as partitioning §3.3), the cached map and
  sessions, and nothing else. A co-located gateway runs statements for its own
  shard in process, through the same admission as a remote request (§4).
- **Map authority.** Owns the cluster map, plans moves, coordinates DDL, records
  backup marks. Phase 1 has none (a static file, unsigned, §2.2); phases 2–3 run one
  process; phase 4 replaces it with a three-voter openraft group. It is never on
  the statement path (§15).
- **Ports:** SQL on P, rshard on P+2, shard Raft on P+3, all rastcp over mutual
  TLS 1.3 (§13).

## 2. The shard map

### 2.1 Shape: two levels

The cluster map sends a point in `[0, 2⁶⁴)` to a **range** and a range to a
shard; each shard's partition map (partitioning §3.2) sends the same point to a
local partition. Both use the frozen `route_v1` point, so every node computes
the same point for a value. This is CockroachDB's `meta1`/`meta2`: the cluster
level is small and replicated, the local level stays physical and local
(partitioning §12).

```rust
// core/shard/map.rs
pub struct ShardMap {
    epoch: u64,                     // bumped on every committed change
    route_v: u16,                   // must be 1 (log::ROUTE_V1), else refuse to load
    key_family: Family,             // level-0 family every sharded table shares
    ranges: partition::RangeMap,    // [lo,hi) → RangeId, total over [0, 2^64)
    owner: Vec<(ShardId, u64)>,     // by RangeId: owner, owner_epoch
    shards: BTreeMap<ShardId, ShardDesc /* replicas, placement_gen, slot */>,
    schema_epoch: u64,
    moves: Vec<MoveDesc>,
}
```

- **`RangeMap` and `route()` are reused unchanged.** `route(&KeySpec, mask,
  constant_false)` (`core/partition/route.rs:65`) gives a superset of ranges,
  with the 256-point cap and the widening allowlist. The payload is a
  `RangeId`, because `RangeMap::validate` refuses a repeated id
  (`core/partition/map.rs:74`) and a shard owns many ranges.
- **Alignment is a new check.** `validate` checks only start, monotonicity and
  distinct ids; alignment comes from `initial()` alone. `ShardMap::validate`
  requires every boundary to be a multiple of `BLOCK = 2¹⁶` (`map.rs:10`), so a
  tenant is never split across shards by accident.
- **One key family.** Every sharded table leads with the same level-0 family.
  `route_v1` seeds by key level, not table, so a tenant lands on the same shard
  in every table. Other tables are **unsharded** (§8.3).
- **Layout.** `SHARD MAP INIT shards = 128, ranges = 4096` gives 4,096 equal,
  block-aligned ranges, 32 per shard. The map is tens of KB, never O(tenants);
  Citus needed 11–20 minutes to sync 480,000 shard rows. Moving one tenant
  first **carves** its block into its own range, a map edit with no owner change.
- **The shard's local map is carved, not uniform.** `RangeMap::initial(n)`
  spreads n partitions over all of `[0, 2⁶⁴)` (`map.rs:57-69`), which would put
  only 32 of 4,096 in owned ranges. A shard needs `RangeMap::within(owned, n)`:
  n/32 partitions per owned range, block-aligned, and one **`FOREIGN`** entry
  per unowned gap (§5.2). That constructor, the `FOREIGN` state and the span
  edits a move needs are asks of partitioning, listed in §16 phase 0.

### 2.2 Storage, epochs and authority

`db_cluster.map` reuses the partition map log's **framing** (`len ‖ crc32 ‖
kind ‖ payload`, durable appends) and its generation/`SEALED` replay, factored
out of `core/partition/log.rs` into a shared pure module with records of its
own; the partition `Record` kinds and `MapState` rules do not carry over
(partitioning §3.2 says the same). That small refactor is one of phase 0's
asks of partitioning (§16), needed by phase 1a's static map. In phase 1
the map is a static operator-written file checked by CRC and digest; from phase
2 it is signed with the authority's Ed25519 key and nodes refuse unsigned maps.
Nodes and gateways refuse lower epochs, and **persist the highest epoch they
have seen**; from phase 2 a gateway that lost its cache fetches a fresh map
from the authority before routing writes.

Every rshard request carries the **map epoch** (routing freshness), the
**owner epoch** of each range it targets (the epoch its owner last changed, so a
move of range 900 does not invalidate requests to range 17), and the **schema
epoch** (§8.4). From phase 3 each shard group also has a **Raft term**.

**The map is advisory for routing and authoritative only through ownership.**
A shard serves a range only if its own durable ownership state says `Owned` at
the request's owner epoch **and** its accepted map agrees; on disagreement it
refuses and raises `shard_ownership_mismatch`. A stale gateway costs a retry,
never a misplaced or missing row. Ownership lives in the shard: phases 1–2 in
`db_shard_ownership.log` (CRC-framed, fsynced per append), phase 3 on as
entries in the shard's Raft log, ordered with the rows it governs (§6). Two
nodes reporting the same map epoch with different digests stop serving sharded
tables (`57P03`) until an operator resolves it.

## 3. Routing

### 3.1 Classifying a statement

Clients connect to any gateway; pgwire startup is unchanged. A tenant-aware
pool can call `SELECT rdb_shard_of(42)` once and connect beside that shard's
leader. PostgreSQL clients do not follow redirects, so there are none.

The gateway runs the engine's parser and SARG extraction against its shells,
never a text scan (five bugs in one day came from scanners that did not track
nesting). For **every table reference**, including subqueries, CTEs, join
relations and `IN (SELECT …)`, it computes `route()` and a shard set.

| Class | Condition | Action |
|---|---|---|
| Single-shard | every sharded reference routes to shard s; unsharded ones only if s is their home | `Exec{Single}` to s |
| Fan-out read | a `SELECT` reaching several shards, autocommit or `BEGIN READ ONLY`, READ COMMITTED, supported shape (§5.3) | scatter `Exec{Part{ranges}}`, merge |
| Multi-shard write | any write reaching several shards | **0A000 before anything runs** (opt-in bulk split: §8.5) |
| No table reference | `SELECT nextval('s')`, `currval`, `lastval`, constants | `nextval` to the gateway's co-located shard, else a fixed shard per sequence: values are disjoint by construction (§8.2), so they may come from another shard's space; `currval`/`lastval` are gateway session state; `setval` on a sharded sequence refused |
| Unknown shape | any construct the classifier does not enumerate | **refuse**, fail-closed |
| DDL / session commands / `pg_catalog` | | §8.4 / kept at the gateway, applied lazily / answered from shells and `STATS` |

Routing is a superset: an `IN` list past `POINT_CAP` degrades to a range that
may cover every shard. A read then fans out; a write is refused even if one
shard holds every match. That is a conservative refusal, never a wrong answer.

### 3.2 Prepared statements, sessions, transactions

pgwire already inlines bound parameters at Execute "because the engine parses
SQL only" (`pgwire.rs:2204`). The gateway does Parse, Bind and Describe against
its shells and **routes at every Execute**, never at Parse: partitioning's I11
one level up. The describe cache's generation key gains the schema epoch.

`BEGIN` is recorded at the gateway. The first statement that routes to one shard
opens a remote transaction there with `{isolation, read_only, timeouts,
synchronous_commit, txn_start_time}`; the last is a new executor input, so
`now()` agrees across a read-only transaction's fan-outs without rewriting text.

- **The first write fixes the block's write shard.** A write reaching another
  shard is refused 0A000 and the block fails (25P02).
- **READ COMMITTED lets non-locking reads go elsewhere.** Every RC statement
  takes its own snapshot, and none of the block's writes exist on another shard,
  so a read-only statement without `FOR UPDATE/SHARE` that reaches a shard other
  than the write shard runs there as its own read-only snapshot. That is exactly
  RC, and it admits the common ORM block that reads a global table on shard 0
  and then writes a tenant elsewhere. Under REPEATABLE READ or SERIALIZABLE any
  statement reaching a second shard is refused, as is any locking read.
- **A transaction pins the owner epochs it has touched.** A later statement
  reaching a range whose owner changed since gets 40001. Otherwise a REPEATABLE
  READ transaction could read tenant 42 on shard A, then again on B under a
  snapshot B never gave it.
- Fan-out inside a write, REPEATABLE READ or SERIALIZABLE transaction is refused.
- **`synchronous_commit = off`** in cluster mode skips only the replica's local
  outcome fsync, which phase 3 takes off the critical path anyway; the reply
  still follows quorum and apply (§9.2), so a session always reads its own
  acknowledged write. Today's meaning, a reply before durability, would put the
  reply before visibility.

### 3.3 Retries and cancellation

A shard refuses (`Moved`, `SchemaBehind`, `NotLeader`, `Fenced`, `Busy`) only
**at admission, before execution**, and the frame says `before_effect`. Only
those are retried, transparently, in autocommit or on a transaction's first
statement: refresh the map, resend the whole statement, at most three times. A
connection lost after a commit or autocommit write was dispatched is **never**
retried; the client gets `08007`, like FoundationDB's `commit_result_unknown`.
Retrying there is how `SET n = n + 1` lands twice. **From phase 3, `40001` is
allowed only if no `Commit` entry was handed to openraft.** Once one was, the
outcome is unknown until the log resolves it, so the error is `08007`, never a
retryable class, and the engine's autocommit 40001 retry loop must not see it
("two conflict retry loops").

BackendKeyData's pid becomes `gateway_slot:10 ‖ counter:21`, with a random key;
today's pid is unique only per process (`register_session`, `core/cancel.rs:245`).
A cancel reaching any gateway is forwarded to the slot's owner, which cancels
every shard running the statement. Cancellation still always ends in an error,
never a short result (`core/cancel.rs:20-25`).

### 3.4 The rshard protocol

```
Exec{req_id, txn, ctx, role, map_epoch, schema_epoch,
     ranges: Vec<(RangeId, owner_epoch)>, mode: Single | Part{ranges}, sql}
ExecOk{req_id, served: Vec<(RangeId, owner_epoch, rows_read)>, schema_fp: Vec<(table_uid, [u8;16])>, body}
Refused{req_id, before_effect: true, why}
body = Count | Rows | TopN{k} | Partials{codec_ver, …} | Affected{n, returning}
```

Bodies carry engine row bytes; the gateway merges with the engine's own
functions and encodes pgwire once. The gateway refuses to merge rows whose
schema fingerprint differs from its shell's (§8.4): the post-scan pipeline
decodes by `write_order`, and a wrong field type corrupts the payload, not just
the label ("describe and payload share one source").

## 4. Data-node admission

Correctness is enforced on the shard. Every statement, over rshard or from a
co-located gateway in process, passes **one** admission function, before it
reads anything:

1. **Leadership.** From phase 3, this replica believes it leads the current term
   and has applied through its term's first entry, else `NotLeader{hint}`. A
   deposed leader still believing it leads cannot make a write visible (§9.2),
   and reads use ReadIndex (§9.3). Before phase 3, the process must hold the
   placement generation the map names; an older copy refuses to serve
   (`57P03`), and a relocated copy refuses even to start (§10.1).
2. **Ownership.** Each `(range, owner_epoch)` is `Owned` at exactly that epoch
   **and** the shard's accepted map names it owner at that epoch (§2.2), else
   `Moved` (after waiting up to 1 s if its map is older than the request's).
   Admission takes a **per-range lease** that `FENCE`'s drain waits for (§10.2),
   so no statement admitted before a fence begins its transaction after it.
3. **Schema.** Each touched table is at the request's schema epoch and not
   `PARTIAL`, else `SchemaBehind`.
4. **Re-route** of every table reference (§5.2).
5. **Snapshot admission** for newly acquired ranges (§10.4).

## 5. Statement execution

### 5.1 Single-shard

After admission the shard runs today's code unchanged: first-updater-wins at
every level, the autocommit retry, `ON CONFLICT`, `RETURNING`, the unique check,
co-located FK actions, the point-update key stripes, the local deadlock
detector.

### 5.2 The node re-checks every table reference

- **In `Single` mode**, the partition routing hook (partitioning §4.1)
  intersects every partitioned table's route with this shard's `Owned` ranges.
  Any reference reaching outside fails with `RDB02 "statement reaches ranges
  not on this shard"` **before reading**. Without it, `(SELECT max(x) FROM t
  WHERE tenant_id = 99)` misclassified to tenant 42's shard reads zero rows and
  reports success.
- **Unowned spans refuse; they never read as empty.** The local partition map
  still spans `[0, 2⁶⁴)`, and an unmaterialized partition reads as empty by
  design. So each unowned gap is its own entry with a fresh pid in state
  **`FOREIGN`**, a per-pid flag like `retired`, so `RangeMap::validate`'s
  distinct-pid rule (`map.rs:89-95`) still holds. `open_partition` refuses a
  `FOREIGN` pid with an error, as the shell refuses data (partitioning §3.3): a
  read path that bypasses the hook errors, including on a `Departed` span after
  eviction turned it back to `FOREIGN`.
- **Ownership changes are local map edits.** Acquiring a range adopts a
  `FOREIGN` span (`ACQUIRE_SPAN{range, pids}`: a `PLACE` of fresh pids over an
  empty `FOREIGN` entry); evicting one releases it (`RELEASE_SPAN{range}`).
  Partitioning's map log has no such record today (`core/partition/log.rs:283-298`;
  its only map edits are `PLACE` and phase 5's reshape), so these are named asks
  in §16 phase 0, and range moves wait for them.
- **Unsharded tables** live on their home shard; every other shard holds a
  **stub** that errors on access.
- **In `Part{ranges}` mode**, each partition call is bounded to its point
  ranges. Partitions wholly inside are read as they are; one straddling an edge
  gets a residual point predicate (one xxh64 per row). Reads need no partition
  reshape (phase 5, unbuilt), and this keeps the fan-out union exact while a
  source still holds rows it handed off (§10).
- **`EXECUTE ON EACH SHARD`** runs as `Part{Owned ranges at their owner epochs}`
  on each shard, with the same bounding and the write guard, never as an
  unbounded statement that bypasses the re-route.

### 5.3 Fan-out reads

| Operator | Shard returns | Gateway merge |
|---|---|---|
| `COUNT(*)` | `u64` | sum, then `row_count_result` |
| COUNT/SUM/MIN/MAX/AVG, `GROUP BY`, `HAVING` | the unfinished `Folder` state from a new **fold-only** executor mode (no `HAVING`, `ORDER`, `OFFSET`, `LIMIT`; `finish_into` applies those, `aggregate.rs:1699`) | merged into a `Folder` built from the gateway's own plan template, never a decoded one (`Folder::merge`, `core/rql/aggregate.rs:1674`); `finish` once |
| `LIMIT k OFFSET o`, no `ORDER BY` | rows, capped at k+o | concatenate, truncate, stop dispatching |
| `ORDER BY … LIMIT k OFFSET o` | sorted top k+o as **scan-schema rows** carrying every sort key and its `__null_` companion | re-cut with `sort_rows_if_needed` (`core/processor/concurrent_processor.rs:1800`) through the `pub(crate)` wrapper rasterizeddb-99 approved on its top-N superset condition, then project. The wrapper resolves **strictly**: an unresolved key is `XX000`, never the in-process skip-and-warn (`:1665-1671`) or the no-keys truncate (`:1823-1828`). Alias and expression keys refused until phase 4 |
| Rows, no `LIMIT` | rows | concatenate under `shard_gather_max_bytes` (1 GiB) |
| `DISTINCT`, windows, unlimited `ORDER BY`, joins | — | refused in phases 1–3; from phase 4 gathered into the post-scan pipeline under the cap, joins via `RowSet::Serialized` and the 4 GiB cap (`core/rql/join.rs:343`) |
| `FOR UPDATE`, `SKIP LOCKED` | — | refused across shards |

**The partial codec is new.** `Acc` is `pub(crate)` and in memory
(`aggregate.rs:669`). `core/shard/merge.rs` encodes `(group key bytes, shown,
Vec<Acc>)` with a version byte, destructuring every `Acc` variant exhaustively
with no `..`, so a new field fails to compile until the codec carries it.
`ExactSum` travels whole: `{total, n, scaled, scale, overflow, dscale,
track_dscale}`. Without `overflow`, an overflowed sum merges into a plausible
number ("the number that looks right"); without `scale` or `scaled`, `AVG` is
off by 10^scale, the NUMERIC bug that had to reach the planner. `Acc::merge`
ignores `scale` and `scaled` (`aggregate.rs:842-870`), so the shard merge path
asserts they agree, in release builds, else `XX000`. `shown` is the group's
plain first-row key (jsonb groups by value since `377f36b`); without it a
merged jsonb `GROUP BY` would emit canonical key bytes.

**Each routed range is answered exactly once.** A shard derives `served` from
its ownership snapshot, taken atomically with the read snapshot, never by
echoing the request, and reports rows read per range. The gateway fails with
`XX000` and alarms (`coverage_mismatch`) unless the lists are pairwise disjoint,
their union is the routed set, and each owner epoch is the one it dispatched.
With the node's `Part` bounding that is two independent layers at O(ranges)
per statement; S5 tests each with a fault only it catches.

**Gathers never truncate** (`54000` past the cap). **Empty routes still run the
pipeline**: `COUNT(*)` gives one row of 0, `MAX` one NULL. A gateway-wide
gather-memory semaphore bounds every gather, top-N and join buffer together;
exhaustion is `53200`. **Unrouted statements are refused by default** above
`shard_unrouted_max_partitions` (default 4,096, one shard's worth), estimated
before dispatch from each shard's `STATS` (materialized partitions × bytes);
`shard_max_fanout_bytes_scanned` caps the same estimate per role, and a NOTICE
fires past `shard_fanout_warn` (8 shards). §11.6 gives the cost they guard.

**Snapshot semantics, exactly.** Each shard snapshots when its sub-request
arrives, after the statement began. While transactions stay single-shard
(phases 1–4) the result includes every transaction committed before the
statement began, shows each transaction whole or not at all, and shows an
arbitrary per-shard subset of transactions committing concurrently, so
real-time order among concurrent transactions on different shards is not kept.
This text is in the REPEATABLE READ refusal and in `SHOW SHARDING`;
`pg_stat_statements` counts `fractured_reads`; `rdb.cross_shard_read = refuse`
refuses them outright.

### 5.4 Writes

Phases 1–4 allow one shard per write transaction, with exactly the single-node
isolation. `EXECUTE ON EACH SHARD $$ … $$` (phase 2) is refused inside `BEGIN`,
runs an independent autocommit transaction per shard, returns
`(shard, status, rowcount)` per shard, and **errors** if any shard fails, naming
those that applied: Vitess `MULTI`, explicit and loud. The gateway's HLC
(`replication/hlc.rs`) rides every request and orders diagnostics and marks only,
never visibility, before phase 6.

## 6. Ownership and the handoff

```
ACQUIRE{range, owner_epoch, from, token}   RELEASE{range, owner_epoch, to, token, final_index}
INCOMING{range, move_id} / INCOMING_ABORT   FENCE{range, move_id} / UNFENCE   EVICTED{range}
```

A range on a shard is `Owned`, `Incoming` (only the move applier writes; never
served), `Fencing` (writes refused, retryable) or `Departed` (rows may remain;
never served, for any snapshot).

**Two-party handoff.** The source appends `FENCE`, stops writes, then `RELEASE`
with a token signed for the new owner epoch; the destination appends `ACQUIRE`
only once it holds that token durably. At most one shard accepts writes for any
point at any time, whatever map any gateway holds. The gap between the records
is unavailability, never wrong data.

**The write guard.** Beside partitioning's range guard (§4.5 there), in
`insert_row_in_txn` (`core/row/table.rs:3049`), on the merged row in
`update_rows_locked` (`table.rs:3841`), **including both in-place branches**
(`write_columns_in_place`, `table.rs:1693`, and the overwrite), and on **every
`xmax`-stamping path** (`delete_rows`, `delete_rows_in_txn`, `delete_rows_as_of`,
`table.rs:3502-3535`, and FK cascade deletes), the shard errors unless
`point(row) ∈ Owned ∪ Incoming(own move)`. Release builds, one comparison
against an `ArcSwap` of sorted ranges, updated only from accepted maps and
ownership records together. A guard on the relocating branch alone is no
guard. `FENCE` publishes the fenced `ArcSwap` **before** the drain samples the
running set and the admission leases (§4), so a statement admitted before the
fence either finishes inside the drain or fails at the guard.

## 7. Two-phase commit (phase 5, opt-in)

`SET rdb.cross_shard_writes = on` per session; refused for tables an operator
marks as having a fan-out read SLA, since fan-outs can then see a half-applied
transaction until phase 6.

**The decision lives on a participant**, as CockroachDB's transaction record
does. The **anchor** is the first writing shard. At commit its branch also
inserts `(gtid, participants)` into `__dist_txn` in the same local transaction,
so **the anchor's commit is the decision**. It goes through the anchor's Raft
commit path, so it is visible only once a quorum of the anchor's group holds it
(§9.2): no reader sees a decision that failover erases.

1. **Prepare.** Each other participant proposes `Prepare{gtid, family, ops}` to
   its group and votes yes only once it is quorum-durable. Every replica's apply
   records the **whole family** in one durable write: `0b11` PREPARED for every
   member not rolled back, Aborted for rolled-back savepoints. None may abort it
   afterwards on its own; resolution marks the whole recorded family. PREPARED on
   the parent alone would lose every row written inside a savepoint at restart
   (`retire_family`, `transaction.rs:658-679`).
2. **Decide**: commit the anchor branch. 3. **Finish**: `Decide{gtid}` to the
   rest, after the reply. A failure before step 2 rolls back every branch.

**Recovery is driven by participants, and only the anchor's log says
Aborted.** A participant leader holding a branch prepared for over 30 s asks the
anchor `TxnStatus{gtid}`. The anchor's branch may still be in flight as a
`Commit` entry, which the current leader, or a new leader's first commit of
prior-term entries, can still commit; an answer read from visibility alone could
abort a participant while the anchor commits. So the anchor answers from
**applied state after a ReadIndex in the current term**: **Committed** if the
`__dist_txn` row is applied, **Running** if the session is alive and no
decision is applied; otherwise it proposes `Decide{gtid, commit: false}` and
answers **Aborted** once that is applied. Every replica's apply loop resolves
the race deterministically: whichever of the branch `Commit` and that `Decide`
is first in the log wins, and the later one is rejected. `__dist_txn` rows are
kept until every participant has acknowledged `Decide`.

**PREPARED must survive everything that forgets outcomes.** None of this holds
today:

| Today | Required | Test (mutation) |
|---|---|---|
| Two decoders map `0b11` to InProgress: settled `decode` (`core/mvcc/status.rs:1157-1162`) and the plain `outcome_in` (`status.rs:534-539`) | Phase 0: **open** refuses a `0b11` code with a named error, in the existing full-byte scan (`status.rs:395-401`) and the settled sidecar load; `decode` stays a total `#[inline]` function | `0b11` in a plain and in a settled status file both fail the open *(skip either open check)* |
| `StatusLog::record` takes `TxOutcome`, which has no Prepared (`status.rs:460-466`); the WAL outcome record has no kind for it | A `Prepared` outcome and WAL record kind, both decoders taught it | round trip through both representations and WAL replay *(map Prepared to InProgress)* |
| A crashed InProgress id reads Aborted (`status.rs:386-393`) | PREPARED ids are not aborted at restart | prepare, SIGKILL, restart: still prepared *(treat as InProgress)* |
| `TransactionManager::new` starts with `running` empty (`transaction.rs:518-531`) | Restart rebuilds `running` with PREPARED ids | a post-restart snapshot lists it *(clear `running`)* |
| `settle_bound` counts only running and snapshots (`transaction.rs:930-934`) | Prepared ids hold settle, forget and vacuum horizons below them | prepare, restart, advance several `FORGET_STEP`s, vacuum, commit: rows intact *(omit them from `settle_bound`)* |
| Ids below the forgotten horizon read Committed (`status.rs:1090-1091`) | Forgetting never passes a prepared id | an aborted prepared branch never reads Committed *(forget past it)* |
| A missing settled block reads Committed (`status.rs:1096-1097`) | A prepared id never lands in a settled block | settle while prepared: block stays dense *(settle past it)* |

These rules are built in phase 5, for 2PC only. Phase 3's commit path never
records PREPARED (§9.2), so it leaves the status-log and vacuum horizons
untouched. Cross-shard transactions run with `lock_timeout = 10s`; a distributed
deadlock detector is phase 6.

## 8. Constraints, sequences, unsharded tables, DDL

### 8.1 Constraints

`UNIQUE` and `PRIMARY KEY` contain every partition key column, so per-shard
checks are exact and `ON CONFLICT` reaches one shard. Sharded tables require a
`PRIMARY KEY`, which **the engine does not record when it is composite**: the
parser sets `schema.primary_key` only for one column (`core/rql/lexer_ct.rs:240-247`,
`core/row/schema.rs:380`) and files a composite key in `unique_keys`,
indistinguishable from a `UNIQUE`; `_pkey` is built only from `primary_key`
(`core/index/mod.rs:1245`). Phase 0 adds `is_primary` to `UniqueKey`, persisted
in the sidecar and covered by the §8.4 fingerprint, and the DDL check reads that
flag (test: `PRIMARY KEY (tenant_id, id)` accepted, `PRIMARY KEY (id)` and a
table with only `UNIQUE (tenant_id, x)` refused). Addressing rows by that key
(§9.4, §10) also needs partitioning phase 3's composite identifying key
(`encode_canonical` framed tuple), a hard dependency of sharding phases 2–3.

FKs between sharded tables must contain the referenced table's partition key,
so they co-locate; others are refused at DDL. `CHECK`, `DEFAULT` and generated
columns run on the leader and replicate as values.

### 8.2 Sequences and row ids

- **`BIGSERIAL`** draws from `(shard_id << 40) | counter`, the high-water
  reserved durably in steps (partitioning §10), never seeded from `MAX`. From
  phase 3 each reservation is a `SeqReserve` entry in the shard's log, committed
  before any value is issued, so no leader, deposed or new, reissues one. Moved
  rows keep their values and the source never reissues its space. Global order
  is lost, as in Citus.
- **Row ids are replica-local.** They are drawn inside `RowPointer::new` by
  `last_id.fetch_add` with no way to pass one in (`core/row/row_pointer.rs:946-956`),
  and nothing needs them equal: follower apply and moves address rows by PK,
  and `locate_row_by_id` has no caller outside a seam test. Each `Database`'s id
  space stays self-consistent, which is partitioning I12 per `Database`.
  Persistent-index snapshots are per replica and never copied. No row ids
  travel in the log.
- **External dependency.** `SEQUENCES` is process-global, keyed
  `"{table}_{col}_seq"`, seeded with `or_insert_with`
  (`core/rql/sequence.rs:29-31, 88-92`), so two databases in one process with
  `t(id SERIAL)` share a counter (inferred). Another session owns that fix
  (bug #7, `fix/stress-bugs`); phase 4 waits for it and keeps only the
  regression test (two `Database`s in one process, `t(id SERIAL)`) as an
  acceptance check.

### 8.3 Unsharded tables

They live on a **home shard** (shard 0 by default), with stubs elsewhere.
Joining one with a sharded table waits for phase 4's gather. Reference tables,
copied everywhere and written by 2PC, are phase 5.

### 8.4 DDL: a cluster schema epoch

DDL is not transactional ("DDL is not transactional"), so a fan-out to 128
shards can half-apply. It is a resumable procedure.

1. The gateway takes the authority's DDL lock (`55P03` on timeout); the
   authority appends `SCHEMA{epoch = n+1, ddl_id, sql}`. Physical-rewrite DDL
   stays refused, as on one node until partitioning phase 6.
2. Each shard leader applies `DdlApply{ddl_id, epoch, sql}` idempotently, records
   it, and acks a **schema fingerprint** per table: a 128-bit hash of
   `TableSchema` including every `write_order`, the dropped-column history and
   the `__null_` companions ("schema_id is a write_order"). **From phase 3 the
   DDL is a `Schema` entry in the shard's own Raft log**, so followers apply it
   at the same position relative to the rows, never on a separate channel. A
   follower computing a different fingerprint, or failing the apply, halts
   (§9.4).
3. All fingerprints equal: the authority publishes epoch n+1; gateways refresh.
4. Otherwise the DDL is `PARTIAL`, and the table is refused on every shard until
   `CLUSTER DDL RESUME ddl_id` or an operator revert. No automatic undo: a
   compensating `DROP` is DDL that can half-apply too.

Fingerprints are checked at apply (divergence caught when it happens) and at
every merge (no row decoded under another `write_order` reaches a client).

**From phase 3, three more rules.**
- **Rows carry their schema.** Each `Commit` names the fingerprint of every
  table its ops touch (`schema_fp`); a replica whose table has another
  fingerprint halts. On the leader, a `Schema` entry applies only from the apply
  loop, behind a **table barrier** that first drains every executed-but-unapplied
  writer of that table, so no v1 row image is proposed after a v2 `Schema` entry.
- **DDL apply is idempotent by fingerprint**, since the DDL and the applied log
  cannot commit atomically: at the target fingerprint, skip; at the prior one,
  apply; at neither, halt. A crash between DDL and applied log neither errors
  nor half-applies twice.
- **JSON shadow-table mode is refused in cluster mode.** `--json-shadow-tables`
  runs implicit DDL inside an `INSERT` (shadow `CREATE`, `ADD COLUMN`, a
  populated-table rewrite), outside the cluster schema epoch. Native JSONB
  storage needs no DDL.

### 8.5 Bulk ingest

Loading 8×10¹¹ rows one per commit at the §11.4 design point (256,000 commits/s
over 128 leaders) is about 36 days with every leader saturated, and a `COPY` or
multi-row `INSERT` spanning tenants is a multi-shard write. The PostgreSQL import
commits in memory and bypasses `commit_written_transaction`
(`core/database/mod.rs:1373-1375`), so it bypasses capture too.

- **Split, opt-in and loud.** With `SET rdb.split_bulk_writes = on`, in
  autocommit only, the gateway splits `COPY` and multi-row `INSERT` by shard into
  batched per-shard autocommits (~10k rows), returns per-shard row counts and,
  like `EXECUTE ON EACH SHARD`, errors naming the shards that applied if any
  fails. Off, it is 0A000.
- **Per-shard bulk mode** for an initial load into an empty table: replicated
  as batched row-image entries, indexes built once at the end (persistent
  indexes refuse a bulk load in progress), the table refused to readers until
  then. Built only if phase 3's measured batched rate makes the load time
  unacceptable.
- **Gate.** Phase 3 measures rows/s through batched replicated inserts on ext4
  and publishes a time-to-load estimate for the target.

## 9. Replication and failover per shard (phase 3)

### 9.1 Why not statement replication

Statement replication is refused in cluster mode. Making it safe would need six
fixes, each a silent divergence today: `seq` taken after the commit is visible
(`replication/manager.rs:687-688`), so a replica applies T2's `UPDATE` before
T1's `INSERT` and matches 0 rows; an applied position buffered outside the
apply; an apply rastcp's timeout can drop midway; no per-origin apply mutex;
per-table last-writer-wins, which on a million-tenant table drops nearly every
remote record; and backups carrying both state files ("replication reseed state
files"). Even then `NOW()`, `RANDOM()`, omitted `DEFAULT`s and `nextval()` would
evaluate on the replica. Those fixes stay worth doing for whole-database
replication, each with a red-on-mutation test, but nothing in cluster mode uses
that stream.

### 9.2 Row images through Raft

Each shard is one Raft group, normally three replicas, on **openraft** pinned to
an exact release. A home-grown Raft is refused: it reproduces the classic bugs
(votes not persisted before replying, commit indexes applied from older terms,
overlapping membership changes). The log store, state machine and snapshot
install are ours, and §17 tests them hardest.

```rust
// core/shard/log.rs; every entry has a log id (term, index) from openraft
enum ShardEntry {
    Commit { origin: (NodeId, Incarnation, ProposalSeq), ops: Vec<RowOp>,
             schema_fp: Vec<(table_uid, [u8;16])>, hlc },
    TxnData { origin, ops },  Abort { origin },     // large transactions only, below
    SeqReserve { seq, upto },
    Schema { epoch, ddl_id, sql, fingerprint }, Ownership(OwnershipRecord),
    Watch { range, move_id }, Drained { range, move_id }, Mark { name },
    RetirePartitions { pids }, Prepare { gtid, family, ops }, Decide { gtid, commit },
    Noop { term },
}
enum RowOp {                                             // no row ids: replica-local (§8.2)
    Insert { table_uid, point, pk, row },
    Update { table_uid, point, old_pk, pk, old_h, row }, // full new image
    Delete { table_uid, point, pk, old_h },              // old_h = xxh64(canonical old row)
}
```

**`TxnData`.** A transaction whose ops exceed one entry ships them ahead in
`TxnData` entries; every replica buffers them by `origin`, unapplied, until the
`Commit` (apply all) or `Abort` (discard) with that origin. Nothing in a
`TxnData` is ever applied on its own.

**Capture** is a `ChangeSink` on `Table`, called from the three storage write
paths (`insert_row_in_txn`, `update_rows_locked` with both in-place branches,
delete), buffered **per transaction family**, each op tagged with the id that
wrote it. `rollback_savepoint` discards the ops tagged with the doomed sub-ids;
a failed statement, or an attempt the autocommit retry loop abandons, discards
that attempt's ops. Otherwise followers apply writes the leader threw away.
Capturing at storage rather than at `note_write` (`core/rql/executor.rs:9523`,
which ships only depth 0) covers FK cascades. A PK-changing update within one
point emits `Update{old_pk, pk, row}`. Point-changing updates are refused today
(partitioning I4); when partitioning phase 6 allows them, capture must emit
`Delete` plus `Insert`, with its own test. Temp relations are excluded. A path
that bypasses capture diverges followers silently, so each path has a twin-diff
mutation (S16), and `old_h` plus replica diffing (§9.6) are the backstop.

**The leader's commit path.** No step commits before quorum, and none records
PREPARED.

1. Execute; versions are InProgress under a leader-local TxId; row locks held.
   The rows are **not** synced here: the Raft log is what makes them durable.
2. Propose `Commit{origin, ops, schema_fp, hlc}`, the HLC assigned under the
   proposal mutex. `origin` maps, in this process's memory only, to the local
   transaction.
3. Once openraft reports it committed (quorum-durable), the sequential apply
   loop finds the local transaction by `origin`, inserts the entry's log id into
   the applied log (§9.4) **inside that transaction**, then calls
   `commit_transaction` (marks the family Committed in memory, releases locks)
   and replies.
4. The local rows, applied-log row and outcomes flush lazily by group commit, in
   `commit_written_transaction`'s order: other locations first, then the root
   log, rows before outcome. A family's sub-transaction outcomes become durable
   no later than its parent's, in the same flush.

`commit_written_transaction` (`core/database/mod.rs:1392-1403`) is
`sync_other_locations()` then `commit_transaction`, which makes the transaction
visible and releases its locks, so calling it before step 3 is exactly S13's
mutation. In cluster mode the executor's autocommit tail
(`core/rql/executor.rs:9368-9430`) and pgwire's `COMMIT` hand the transaction to
`propose_written_transaction(tx)` instead.

**Visibility only after quorum durability.** A deposed leader cannot reach a
quorum, so it cannot make anything visible, whatever its clock says: the fence
is on the **visibility point**, not write admission. The apply loop is
sequential, so visibility order equals log order and the T1/T2 inversion cannot
happen.

**The in-place fast path is off in cluster mode.** `write_columns_in_place`
keeps no previous version, and `try_begin_in_place_for` is safe only because an
autocommit "can never be rolled back after its statement returns"
(`core/mvcc/transaction.rs:596-626`). Here it can: a failed proposal aborts it.
One flag set at shard open makes `try_begin_in_place_for` return `None` (test:
an idle leader, an autocommit fixed-width `UPDATE`, the proposal failed by a
failpoint; the old value survives and no reader saw the new one).

**Every outcome is decided by the log, by log id, never by TxId.** TxIds are
replica-local (`transaction.rs:520-521` seeds each counter from its own status
log), so a new leader's TxId N says nothing about an old leader's N.
- **A failed proposal** leaves the local transaction running, locks held, and
  the client gets `08007`. If the entry commits while this node still leads,
  step 3 applies it.
- **On every leader→follower transition**, the node aborts every local
  transaction it proposed and has not applied, and drops the `origin` map.
  Whatever of those entries commits is applied later by follower apply, from its
  ops, exactly like any other entry. Nothing matches a local transaction to a
  foreign one.
- **A restarted leader** finds its unflushed transactions InProgress, which
  today's rule reads as aborted (`status.rs:386-393`), and its applied log below
  them. Before it serves, it re-applies every committed entry past its durable
  applied log id through follower apply (§9.4); admission waits for that (§4).
  No PREPARED state, no change to `running`, `settle_bound`, forgetting or
  settled blocks.

**Retention.** A shard's log is retained to the minimum of the lowest index
whose local apply is not yet durable on this replica, each follower's match
index, each learner's progress, and any `Watch`. No crash leaves an applied-but-
unflushed entry whose ops are gone.

**Cost.** The critical path is the leader's Raft append fsync in parallel with a
round trip plus a follower's append fsync: about today's one fsync plus the
round trip. Every replica still syncs its Raft append and, amortized by group
commit off the critical path, its local rows and outcomes, so a replicated
commit costs three append fsyncs plus three shares of a group flush, against one
fsync today (§11.4). The Raft log is its own file on the shard's
drive, so today's rows-and-outcome merge in one log does not extend to it.
Phase 3 measures latency and fsync rate on ext4, never tmpfs.

### 9.3 Reads

Leader reads use openraft's ReadIndex-style check, batched (one heartbeat round
per batch). Clock-based leader leases are phase 6, off by default, behind a
skew-and-pause simulation. Followers serve no reads before phase 6's closed
timestamps.

### 9.4 Follower apply

- **One local transaction per `Commit`, under a fresh follower TxId**, which also
  **inserts** the entry's log id `(term, index)` into `__shard_applied`, so the
  applied position is durable exactly when the apply is. The applied log is
  insert-only, one row per entry (or per apply lane), applied = the max visible
  log id, trimmed behind the durable horizon by batched deletes. A single row
  updated per entry would grow a version per commit forever, since the in-place
  path never fires under traffic ("in-place update never fires"), and would make
  every parallel apply conflict first-updater-wins. A crash leaves an aborted
  id, never reused, and replay resumes after the durable log id.
- **Non-durable apply.** The apply transaction commits in memory and flushes by
  group commit, as the leader's does (§9.2): the Raft log is the durability, and
  a lost apply is replayed because its applied-log row was lost with it.
- **Apply writes at the `Table` storage layer**, with capture off, the four
  index maintenance hooks on, and no executor side effects: no FK actions,
  defaults, sequences or constraint checks. The leader already decided those,
  and their effects are in the ops. Re-running them on the follower would apply
  cascades twice. A test pins that a follower with a violating op still applies
  it *(mutation: run the unique check at apply)*.
- **Every op asserts what it finds**, else the replica halts:
  - `Update`/`Delete` require exactly one visible version with that PK, whose
    canonical row hashes to `old_h`;
  - `Insert` requires none.
  A zero-match update reported as success is the characteristic bug.
- **Rows are addressed by primary key.** `locate_row_by_id` is "Linear in the
  pointer file" (`core/row/table.rs:2609`): at 1.53M rows and 37 B per pointer
  record, about 57 MB read per replicated `UPDATE`. The follower probes the
  partition's PK index, a superset, then requires the exact PK match above. An
  `Insert`'s absence check asks the filter first, also a superset: a filter
  miss proves absence without the index. A PK probe waits for that index to be
  ready, so with in-memory indexes a first apply to a cold partition stalls the
  sequential loop for a 1.53M-row rebuild. **Phase 3's gate requires persistent
  indexes or the `core/shard/locator.rs` row-id locator** (sharding's, outside
  `core/index/*`) before any throughput claim. Persistent-index v1 loads a
  partition's PK index from its snapshot when it was closed clean; one written
  since its last snapshot (after a crash) still rebuilds (persistent-indexes
  §4).
- **A failed apply halts the replica.** I/O error, full disk, DDL error,
  fingerprint mismatch or a failed assertion stops its state machine at that
  index, loudly (`shard_replica_halted`). It never skips or continues. The
  group serves while it has a quorum; the replica is replaced as a learner
  (§10.1).
- **Followers keep warm indexes for what they write.** The hooks run on every
  partition the follower writes, and partitions written in the last
  `shard_follower_warm_secs` are exempt from idle close. Without
  persistent-index v1 it defaults to 30 min, because a cold first apply stalls
  on a rebuild; with v1 a cold first apply loads a snapshot (~0.2 s), so it
  defaults to the leader's idle window, which is what §11.2's per-machine
  figures assume (persistent-indexes §9). Follower RAM is the written-partition
  set per follower window (§11.2). Phase 3's follower-apply gate includes the
  per-cold-partition load stall. A promoted follower rebuilds unwritten
  partitions on first touch: correct, and with persistent-index v1 a partition
  closed clean loads its snapshot instead.

**The replay ceiling.** Sequential apply caps a shard's sustained write rate at
what one follower replays. Phase 3's gate: a follower sustains at least 1.5× the
leader's measured peak commit rate **under a cold-partition write mix** (a = 5%,
idle close on). If not, parallel apply comes before any per-shard throughput
claim: entries whose conflict keys are disjoint apply in separate local
transactions concurrently and **commit strictly in index order**, so visibility
still follows the log. The conflict key is every unique-key value an op writes
or removes, not only `(table, pk)`, plus the referenced key of any FK. Either way
the leader delays proposals past `shard_follower_lag_max`, so lag is bounded.

### 9.5 Failover and fencing

The **fencing token is the Raft term**; there are no clock leases for
correctness. Failover is ordinary election inside the group (200 ms heartbeat,
1–2 s election timeout), without the authority. In-flight transactions on the
old leader end with `40001` if no `Commit` entry was proposed for them, else
`08007` (§3.3); nothing partial survives. Losing a
majority makes the shard unavailable for writes and linearizable reads. `ALTER
SHARD s FORCE RECOVER ON NODE n ACCEPT DATA LOSS` needs `cluster_admin`, logs
loudly and bumps the term; the engine never trusts a bare assertion that an old
primary is down, because the term bump is what fences it.

### 9.6 Continuous replica diffing

Two layers, both alarming on any mismatch (`shard_replica_diff_mismatch`):
- **Per op.** Apply checks every replaced row against `old_h` (§9.4), so a
  divergent row halts the replica the first time it is written.
- **Per partition, incremental.** Each replica's apply loop maintains a rolling
  `(visible rows, Σ xxh64(pk ‖ canonical row) mod 2⁶⁴)` per partition, adding on
  insert and subtracting `old_h` on update and delete, and records it in the
  applied log every N entries. Comparing replicas is O(partitions) values at
  one log id, never a scan. A full per-tenant recount runs only on demand or
  after a crash lost the rolling state: one short snapshot per tenant, never
  one per shard, which would pin the vacuum and settle horizons
  ("vacuum frees visible versions"), under an I/O token bucket
  (`shard_diff_bandwidth`).

## 10. Moving shards, ranges and tenants

| Move | Mechanism | Ids | Speed | Used for |
|---|---|---|---|---|
| **A shard replica between machines** | Raft learner: fuzzy copy plus WAL replay, log catch-up, verify, promote, remove | travel with the `Database`; nothing translated | disk and network rate | rebalancing, capacity, replacing machines |
| **A range or tenant between shards** | copy through the MVCC insert path, catch-up from the source's log, fence, verify, two-party handoff | fresh at the destination | insert-path rate, to be measured | hot tenants, splitting a shard |

Shard relocation carries the bulk of rebalancing at byte speed and translates
nothing, because a shard is a whole `Database`. Range moves reinsert, keeping
partitioning I12 and letting the index hooks see copied rows as client inserts.
Copying raw rows into a `Database` that holds other ranges of the same table
would carry the source's row ids into a foreign id space, where
`locate_row_by_id` and the unique check's `exclude_id` could pick the wrong row:
a silent wrong update. That is refused.

### 10.1 Shard relocation: the learner snapshot

1. The authority records `Relocate{shard, from, to}`; the leader adds `to` as a
   learner, which never counts toward quorum.
2. **Fuzzy copy plus WAL replay**, the argument S3 backup already rests on
   ([S3 backup §2](s3-backup.md#2-why-the-snapshot-may-be-taken-while-writes-are-in-flight)):
   WAL records are physical redo, so replay is idempotent. A follower, still
   applying, records its commit sequence `start_lsn`, streams its files, records
   `end_lsn`, and stages that window's WAL. **Relocation calls the backup's
   snapshot routine**, not a re-description of it, because the argument holds
   only with its interlocks: `compaction_permit` held exclusive for the copy
   (`core/backup/mod.rs:254`; compaction truncates without a redo record), a
   `.tmp` fold racing the copy fails it, and the window is proven archived by
   condition, since a declined checkpoint returns `Ok` without staging
   ([S3 backup §10.7](s3-backup.md)). WAL `next_txn` resumes only past ids in the
   live log (`core/wal.rs:2457-2461`), so a source restart inside the window
   aborts the relocation. The learner replays the window to `end_lsn`; its
   applied log then gives the snapshot's last log id `(term, index)`, which is
   what openraft's snapshot install takes.
   - **Nothing is hard-linked.** The engine writes in place (in-place updates,
     `xmax` stamps), so later applies would mutate a hard-linked inode. Apply
     never pauses, so the group never loses its quorum during a copy.
   - **Checks are on installed bytes.** Each chunk's CRC is computed on the
     bytes sent and checked on the bytes written; logical equality is step 4's
     job, since a fuzzy copy matches no instant of the source files.
   - **Excluded:** `db_replication.*`, `backup_instance.id`, the shard identity
     file, **the Raft log store and vote/hard state**, and the persistent-index
     snapshots and marker logs (`.ixs*`, `.ixm*`). Those are fdatasync'd but not
     WAL'd, so the redo argument does not cover them, and a fuzzy copy could
     pair a `CLEAN` marker with later data (persistent-indexes §10); the
     learner rebuilds. Adopting copied indexes is a v2 requirement. The pool
     configuration is rewritten for the destination ("a copied directory reads
     the original"). Cluster mode requires `WAL_ENABLED`.
3. **Catch up** from the learner's applied log id; retention is pinned by it.
4. **Verify** the per-partition digests of §9.6 against the leader at one log
   id, O(partitions). Any difference aborts.
5. **Index readiness.** The learner counts toward "redundancy restored" only
   once its indexes are ready: rebuilt after the copy (persistent-index v1
   never copies snapshots) or, in the in-memory regime, warm for the
   partitions it writes.
   Before that it is a slow failover target, and §11.3's times include it.
6. **Promote, then remove** with joint consensus. Before this step the move is
   undone by removing the learner. Writers never pause; the copy is throttled by
   `cluster.move_bandwidth` (500 MB/s per stream).

**Before phase 3** relocation is offline: stop the process, copy the directory
(same exclusions and rewrite), and, while the old process is still stopped,
write a durable `RELOCATED{to, placement_gen + 1}` tombstone into the old
directory, which refuses startup. Then start the new copy at
`placement_gen + 1`. A placement generation alone is not enough: the old copy's
own ownership log says `Owned`, and a restarted old process with its cached map
and a gateway that loaded an old static map would agree on the old generation.
The shard is down about 27 minutes for 1.62 TB at 1 GB/s (test: restart the old
copy after relocation; it refuses to open).

### 10.2 Range moves between shards

**Offline (phase 2), after the span edits of §5.2 exist.**

1. Carve the tenant's block into its own range if needed.
2. The destination appends `INCOMING` and adopts the `FOREIGN` span; the source
   appends `FENCE`, publishing the fenced write guard first (§6), and new
   statements on the range get retryable `RDB03`, buffered at gateways up to
   5 s.
3. **Drain**: wait for transactions active at `FENCE` **and** admission leases
   taken before it (§4) to end. Past 30 s the **move** aborts, never the
   transactions: ORMs holding transactions open must not pay.
4. **Copy**: the source reads every sharded table at one snapshot, bounded to
   the range, and streams rows by PK; the destination inserts them through the
   MVCC path in ~10k-row transactions, with fresh row ids and `xmin`.
5. **Verify** per table and tenant: visible count and
   `Σ xxh64(pk ‖ canonical row) mod 2⁶⁴`, source snapshot vs destination. Any
   difference aborts.
6. **Handoff**: `RELEASE`, `ACQUIRE`, the authority publishes the owner epoch.
7. The source range is `Departed`; `SHARD EVICT RANGE` later retires every
   local partition wholly inside the span by a file unlink (`RetirePartitions`
   from phase 3, as partitioning's `RETIRED_GONE`), deletes rows through MVCC
   only in partitions straddling an edge, releases the span back to `FOREIGN`
   and appends `EVICTED`. A whole range is ~195M rows; 195M replicated deletes
   would compete with the commit pipeline and the replay ceiling.

**Online (phase 4).** By then every source commit is a Raft entry with row
images, so **the catch-up stream is the source's own log, filtered by point**.
No separate change log and no LSN assigned under the transaction manager's
lock: the Raft index is the LSN, in visibility order by construction.

1. The source appends `Watch{range, move_id}`, pinning retention.
2. **Snapshot and index, atomically.** The leader takes snapshot S between two
   applies, under the apply loop's mutex. Only apply makes anything visible, so S
   is exactly the log prefix through index I_S. The copy runs at S while the
   source keeps writing.
3. The destination applies committed entries that touch the range, in index
   order, starting at min(I_S + 1, the first `TxnData` index of any transaction
   unresolved at I_S); `Watch` pins retention from there. `TxnData` is buffered
   by origin until its `Commit` or `Abort`, as on followers. Each op is
   idempotent: upsert the after-image by PK, delete by PK, and an `Update` with
   `old_pk ≠ pk` as delete(old_pk) plus upsert(pk) in one transaction. Each
   apply inserts its log id into `__shard_move_<id>`, insert-only like the
   applied log (§9.4).
4. Under 10k entries of lag: fence; the source appends `Drained{range, move_id}`
   once the last transaction and admission lease active at `FENCE` has ended;
   apply **through the `Drained` index**, verify, hand off. Transactions open
   across `FENCE` commit after it, so a cut at `FENCE` loses acknowledged
   commits. For large ranges, verify twice: a full digest before the fence
   (source at S and I_S, destination at I_S), then only PKs touched since,
   through `Drained`, old and new PKs both.

**Cost.** Bounded by the destination's insert path (WAL, index and filter work
per row), unmeasured; phase 2 measures it on ext4 before any time is promised.
An average tenant (~800k rows, ~200 MB) moves quickly at any plausible rate; a
100M-row tenant at an assumed 100k–300k rows/s is 6–17 minutes of copy. Range
moves are off the capacity-change path, which uses relocation (§11.3).

### 10.3 Rollback and failure

Before `RELEASE`, abort: `INCOMING_ABORT`, evict, unfence. After the handoff,
`Departed` rows stay for `move_retention` (24 h) and `SHARD MOVE … ROLLBACK`
runs the procedure in reverse.

**If the destination is lost between `RELEASE` and `ACQUIRE`**, the authority
may grant the source `ACQUIRE` at owner epoch + 2, **only if its own log shows it
never published owner epoch + 1**; the source's data is complete since it
stopped writing at `FENCE`. The authority cannot see whether the destination
appended `ACQUIRE` before it was cut off, so the destination's safety rests on
admission's second conjunct (§4): it serves only when its accepted signed map
names it owner at that epoch, which a map it never accepted cannot. Its incoming
rows were never served. When it returns
it sees the range owned by the source at a higher epoch, appends
`INCOMING_ABORT` and evicts before it can ever append `ACQUIRE` for that range
again. S2's crash matrix covers every point.

### 10.4 Snapshot admission

On the destination, a statement touching a newly acquired range is admitted only
if its snapshot was taken after `ACQUIRE` was applied, else 40001. Otherwise a
REPEATABLE READ snapshot predating the copy would see the tenant empty:
partitioning's "snapshot first, then the map" (§4.4) across nodes. On the
source, `Departed` ranges are refused for every snapshot.

## 11. Capacity planning against the target

### 11.1 Layout

| Quantity | Value |
|---|---|
| Shards | **128**: 1.62 TB, 7,812 tenants, 6.25×10⁹ rows each |
| Replicas, machines | 384 at RF3; **64 machines**, six replicas (about two leaders) each, 9.7 TB on 16 TB usable NVMe (61%, room for moves and vacuum) |
| Local partitions per shard | **4,096**: 1.9 tenants, 1.53M rows, ~395 MB each |
| Files per shard | ~12,300 today; **~16,400 with persistent-index v1** (`.ixs` replaces `_flt.db`, plus `.ixm`, per partition, persistent-indexes §1); ~24,600 directory entries with the two data files' `.tmp` siblings (`_flt.db`'s goes with it; `.ixs.tmp` exists only during a close) |

**Why 4,096 per shard.** Lazy open helps only if open partitions track active
tenants. With k tenants per partition and fraction a active in the idle window,
partitions touched are `f = 1 − (1 − a)^k`:

| a | k = 1.9 (4,096/shard) | k = 15 (512/shard) | k = 30 (256/shard) | k = 61 (coarse) |
|---|---|---|---|---|
| 1% | **1.9%** (78 partitions) | 14% | 26% | 46% |
| 5% | **9.3%** (382) | 54% | 79% | 96% |
| 10% | **18.2%** (745) | 79% | 96% | 99.8% |

At k ≥ 30 lazy open degenerates to everything open. 16,384 per shard (k ≈ 0.5)
buys little for four times the files, near the 600k entries at which
partitioning §7.4 expects (unmeasured) `read_dir`, backup walks and checkpoints
to reach seconds.

**Against the file limits.** Partitioning's open budget is `min(4096,
(RLIMIT_NOFILE − 2048 − …) / per-partition fds)` (partitioning §7.1).
Persistent-index v1 drops the `_flt.db` provider and holds no fd for its own
files while a partition is open, so the term falls to **10** (two providers ×
5), plus 5 fds per directory for `rdb.ixe` (persistent-indexes §1): at
`LimitNOFILE=65535` the budget is about (65535 − 2048) / 10 ≈ 6,350, capped at
4,096, enough for a whole shard. Persistent-index v2's run files would raise
the term to 11, giving ≈ 5,770, still above 4,096. The deployment still sets
**131072** in `deploy.sh` (65535 today, `deploy.sh:213`) for headroom (plain
tables at ~25 fds each, sockets, a v2 without v1's provider removal at 16, or
≈ 3,967), so fds never thrash. RAM binds first, so this design asks partitioning phase 3 for
a **resident-bytes term** in the budget (`partition_resident_budget`). 4,096
per shard is above phase 2a's cap of 2,048 declared partitions per database, so
this layout needs partitioning phase 3, whose cap is 65,536 partitions per
node, sixteen times this. Directory entries are about a twenty-fourth of
partitioning's 600k. Checkpoints
sync only dirty files since `b0935eb`, but sequentially: a burst over 382
partitions dirties ~1,150 files, a 2.6 s pass at the 2.3 ms per fsync measured
on .7's LUKS consumer disk, far less on power-loss-protected NVMe, measured in
partitioning phase 3.

### 11.2 RAM per machine

**Per open partition, in-memory indexes:** `rows × (C·f/8 + k·(70–130)) B`, for
C filtered columns at f bits per row and k indexes. The reference is 22 B of
filter per row (a measured `_flt` file beside its pointer file, which is C ≈ 22
at f = 8, inferred), so at 1.53M rows:
- **k = 1** (PK only): 34 + 107–199 = **141–233 MB**.
- **k = 3**: 34 + 321–597 = **355–631 MB**.

Leaders hold what their tenants touch; followers hold what they write in
their warm window (§9.4). The machine column is six replicas at the leader
figure, which holds only when the follower window equals the leader's idle
window, the default with persistent-index v1. With the 30-minute window
(no v1), a follower's set is the partitions written in 30 minutes, which can
exceed a leader's at a short window; size followers then at `a_follower`, the
fraction written per follower window (persistent-indexes §9).

| Regime | Per replica, k = 1 | Per machine, k = 1 | Per machine, k = 3 | Feasible? |
|---|---|---|---|---|
| Today: eager, all resident | 575–950 GB | 3.5–5.7 TB | 8.7–15.5 TB | **No** |
| Partitioning phase 3, in-memory indexes, a = 1% | 78 partitions: 11–18 GB | 66–109 GB | 166–295 GB | k = 1 yes; k = 3 yes on 768 GB, marginal on 512 GB (295 GB exceeds half of RAM) |
| Same, a = 5% | 382: 54–89 GB | 324–534 GB | 0.8–1.45 TB | k = 1 marginal on 512–768 GB; **k = 3 no** |
| Same, a = 10% | 745: 105–174 GB | 630 GB–1 TB | 1.6–2.8 TB | **No** |
| Phase 3 + persistent-index v1, any a | as the in-memory rows above at the same a: v1 keeps an open partition's index in memory, and its cheap reopen lets a shorter idle window lower a (persistent-indexes §9) | same | same | as above |
| Phase 3 + persistent-index v2 (requirements only, persistent-indexes §16; figures from its previous design), any a | its share of the machine budgets, plus fixed and pinned terms of tens of MB at a = 5%, under 1 GB with all 4,096 open (the previous design's §10.1–10.2, in git history: ~4 KB per open partition, pinned IndexBlocks at ~0.03 B per entry, filter segment directories and HLLs, 64 B per closed one) | the machine's `block_cache` + `machine_index_budget` (4 GiB) + 0.3–4 GB of fixed and pinned terms | same | Yes; a sets the cache hit rate, not the RAM |

**The v2 row is budgets, not page cache.** Persistent-index v2 would read
through a `pread` block cache with hard limits, and their defaults (25% of RAM
for the cache, `machine_index_budget` = 4 GiB of write buffer) are **per
machine**, divided by `processes_per_machine`.
Six processes left at that setting's default of 1 would claim 150% of RAM plus
24 GiB, so the authority sets it to six (§1): 25% of RAM shared by the six
caches, and 4 GiB / 6 ≈ 0.67 GiB of write buffer each. Hit rate would then
depend on key locality: only tenant-led keys keep RAM per active row.

**The active fraction is row-weighted, not tenant-weighted.** Per-partition
RAM scales with the partition's rows, and activity usually correlates with
size. Resident bytes are `Σ over touched partitions p of rows_p × per-row cost`.
One 100M-row tenant's partition alone is 9.2–15.2 GB at k = 1 and 23–41 GB at
k = 3. So large tenants are isolated into partitions of their own
(partitioning's `ISOLATE KEY`, a reshape), sizing uses the row-weighted
fraction, and §14's sizing counter is resident bytes of partitions touched per
idle window, not a tenant count.

**One reference layout.** Persistent-indexes §9 sizes this layout: 1/128 of
the target per shard, 4,096 partitions of 1.53M rows (1.9 tenants), six
processes per machine, so its per-shard and per-machine figures compare
directly with this section's. (An earlier draft there sized 1/16 per node at
~15 tenants per partition, the k = 15 column of §11.1, at which the in-memory
regime fails: 54% touched at a = 5%.)

The target needs all three asks: sharding fits the disk; lazy open makes RAM
follow touched partitions, and the 4,096 layout makes those follow active
tenants; persistent-index v1 makes reopening cheap enough that idle windows
stay short; above a few percent active, only v2 would turn the index and
filter terms into a cache budget, and §14's counters are its build trigger
(persistent-indexes §16). **The active fraction is an assumption**, and §14
exposes the counters to measure it.

### 11.3 Re-replication and capacity changes

- **A failed machine** loses six replicas of six different shards. Each is
  rebuilt as a learner from a surviving follower, placed so the six sources are
  on six machines. Six 1.62 TB copies at 500 MB/s run in parallel: **about 55
  minutes**, plus catch-up, the O(partitions) verify, and **index readiness**.
  Persistent-index v1 never copies snapshots, so every copied index is
  rebuilt: ~1.3 s per index per 1.53M-row partition (persistent-indexes §9), a
  borrowed rate its phase 0 (e) replaces (allow up to 3× until then), is
  4.4–13 core-hours per replica at k = 3, **0.6–1.7 hours on 8 cores**. Those
  shards survive no further loss for **about 1.5–3 hours**, until the learner
  is index-ready (§10.1 step 5). In the in-memory regime there is no
  up-front build, but the new replica is a slow failover target until warm.
- **64 to 96 machines** relocates a third of the replicas (~207 TB), four per
  new machine: **about 3.6 hours** of copy at 500 MB/s per destination plus
  index readiness, writers never pausing. Add capacity at 60–70% full.
- **Beyond 384 machines** shards must split: range moves at insert-path speed
  until clone splits exist (§18).
- **Before phase 3** relocation is offline and a lost machine means restoring
  from S3, so phases 1–2 are not a production topology for this target.

### 11.4 Commit and read throughput

The single-transaction-manager CPU ceiling is per `Database`: about three global
write-lock acquisitions and one read per statement, `SELECT` included, plus
`SnapshotRegistry`, `RowLocks` and the process-global query-stats mutex. So **the
number of commit pipelines scales, not the number of machines**; each shard
process has its own outcome log, WAL and query-stats mutex.

- **Measured:** 705 durable `INSERT`s/s from 8 clients on one `Database`,
  consumer NVMe without power-loss protection, ext4, ~5 ms per commit, nearly all
  `fdatasync` ("commit path fsync cost").
- **Design point, not a measurement:** 2,000 durable commits/s per leader on
  power-loss-protected NVMe at 64 clients, so 256,000/s across 128 leaders
  before the Raft factor.
- **The fsync budget is per machine.** At per-shard commit rate r, a machine
  with two leaders and four followers syncs **6r Raft appends per second**, plus
  each replica's group-commit flushes of rows and outcomes (§9.2), all on the
  drives the leaders use. Follower apply is non-durable (§9.4), so it adds no
  fsync of its own.
- **The published figure is measured under RF3 with the full six-replica
  placement per machine** (phase 3's gate), never 128 times a phase-0
  single-process number.
- **Reads.** Only leaders serve before phase 6: 128 pipelines on about two per
  machine, while 256 follower replicas serve none. Each read batch waits a
  ReadIndex quorum round, and every `SELECT` takes its shard's
  transaction-manager locks, so read capacity is the per-TM `SELECT` ceiling ×
  128. Phase 3 gates on read p50/p99 as well as commit latency.
- **If the CPU ceiling binds** first, add shards (256 on the same machines), not
  machines.

### 11.5 Startup and recovery per shard process

Estimates; phase 3 measures each on ext4.

| Step | Bound | Estimate |
|---|---|---|
| `register_directory` `read_dir` (partitioning §7.1) | ~25,000 entries with persistent-index v1 (four files and two `.tmp` siblings per partition, §11.1) | tens of ms |
| WAL recovery, closed partitions included | live log since the last checkpoint | under 1 s |
| Shell open, map replay | O(partitions) records | under 1 s |
| Raft tail; re-apply past the durable applied log id (§9.2) | one election and catch-up; one group-commit window | 1–5 s |
| **Time to first write** | | **target under 10 s** |
| Forget sweeps of partitions dirty at the crash (partitioning §7.2) | every partition written since its last clean `SWEPT`. Cold dirty partitions are background-swept within `partition_sweep_max_lag` (10 min), so the set is at most those written in that window: 57 MB × 382 at a = 5% ≈ 22 GB; worst case 57 MB × 4,096 ≈ 233 GB | **~40 s** typical, **~6.5 min** worst, to forgetting resumed at 600 MB/s |
| Vacuum's first-tick full pointer sweep per opened partition ([space reclamation](space-reclamation.md)) | 57 MB per partition opened | ~40 s of background reads for 382 opens |
| Persistent-index v1 proof per open (persistent-indexes §4) | marker log, snapshot header and file lengths: a few reads | well under 1 s for 382 opens |
| Index warm-up | persistent-index v1 after a clean shutdown: one ~120 MB (98–144 MB) snapshot load per first touch, started at the routed open; after a crash, or for what the shutdown deadline skipped, partitions written since their last snapshot rebuild (1.53M rows each, queries scanning meanwhile, correctly) | ~75 s at 600 MB/s for 382 clean opens; 2–5 min for a rebuilt active set at a = 5% |
| Clean shutdown (persistent-indexes §3) | snapshots of partitions written since their last one, in parallel, unbucketed, under `index_snapshot.shutdown_deadline` (120 s) below `TimeoutStopSec`: 382 × ~120 MB ≈ 46 GB per process, ~275 GB per machine at a = 5% | ~90 s at 3 GB/s per machine; on a 600 MB/s disk the deadline skips most, which then rebuild |

### 11.6 Unrouted fan-out

A statement without the tenant predicate (`WHERE id = $1` when the PK is
`(tenant_id, id)`, `WHERE email = …`, a `created_at` range) visits every
partition of every shard: **128 × 4,096 = 524,288 partition visits per
statement**. In the in-memory regime each closed partition opens and loads its
whole 34 MB filter (under persistent-index v1, the snapshot's FILTER section
when the partition closed clean, else an unpruned scan of up to ~395 MB;
persistent-indexes §4), **17.6 TB of filter read** before any row and a full churn
of every shard's open budget; a scan that the filter does not prune reads up to
207 TB. With the paged filter it is the segments the predicate touches, still
O(rows). Hours, not a query. That is why unrouted statements are refused by
default above `shard_unrouted_max_partitions` (§5.3), and why non-tenant-led
lookups belong in a tenant-led index.

### 11.7 Backup bandwidth and cluster restore

- **Every S3 snapshot is full** ([S3 backup §13](s3-backup.md)), daily by default
  (`--s3-snapshot-interval-secs 86400`). At the target that is 207 TB a day,
  **about 2.4 GB/s sustained cluster-wide**, and each designated follower
  re-reads 1.62 TB a day (~19 MB/s) while it applies. **Incremental snapshots,
  or weekly full plus archived WAL, are a prerequisite for production at this
  size.**
- **Cluster restore** = download (207 TB; at 10 GB/s aggregate S3 egress,
  5.75 hours), WAL replay, index readiness (rebuilt, since backups exclude
  persistent-index snapshots: 0.6–1.7 hours per replica on 8 cores, §11.3), RF1, then learners
  (§11.3). Roughly **7–8 hours to a serving RF1 cluster**, and another 1.5–3 to RF3,
  bounded by egress. Phase 3 gates on a measured restore of one 1.62 TB shard,
  index readiness included.

## 12. Backup and restore

- **One designated replica per shard** (a follower from phase 3) runs S3 backup
  as today to `s3://…/<cluster>/<shard>/<generation>/`, guarded by
  `backup_instance.id`, covering every pool location (since `bd84cee`). Moving
  the designation starts a new generation prefix, so the prefix guard never sees
  a foreign instance ("S3 prefix guard crash loop").
- **Cluster marks.** `CLUSTER BACKUP MARK 'name'` proposes `Mark{name}` to every
  shard's log (an rshard call before phase 3). Applying it commits a marker row
  with the ownership state and map epoch, a point on each shard's commit
  sequence. **The authority holds the move and handoff lock from the first
  marker to the last ack** and records the map epoch it pinned: a `RELEASE` or
  `ACQUIRE` between two shards' markers would restore a range with no owner (or
  two), and GC of `Departed` or `Incoming` rows would then delete its data. `CLUSTER RESTORE AT MARK` restores each shard `--restore-at-lsn` its
  marker, with the map of that epoch. Before phase 5 any per-shard cut keeps
  transactions whole. From phase 5, marking briefly pauses only new 2PC
  **decisions**, so every decision is before every marker or after every marker,
  and 2PC recovery resolves restored branches consistently from `__dist_txn`,
  which is inside each snapshot.
- No mark during a move's fence; marks during other move phases record the move
  as aborted. `CLUSTER RESTORE AT MARK` rebuilds the map from the restored
  ownership states with `rebuild --from-shards`'s total-and-disjoint check (§15)
  and refuses on any gap or overlap **before** GC can run. Restored `Departed`
  ranges stay unserved until GC.
- Costs and restore time are in §11.7.
- **Restore goes to a fresh cluster** (new `cluster_id`), RF1, then learners.
  Restored nodes can never rejoin the old groups. A single-shard restore is
  allowed with a warning; ownership decides what is served.

## 13. Security

- **rshard and shard Raft use mutual TLS 1.3, `ca` mode only**; `insecure` is
  refused off loopback. Each certificate's SAN carries node id and role only;
  shard membership is authorized from the map at Hello, which also closes
  replication's gap where duplicate node ids go unnoticed. Putting the shard in
  the SAN would make every relocation a certificate issuance.
- **The gateway authenticates (SCRAM) and forwards the role.** A shard accepts it
  only from an authenticated peer and **re-checks grants** against its own
  catalogue, kept identical by DDL fan-out; a stale catalogue refuses, never
  grants.
- **The map is signed** (Ed25519) from phase 2; phase 1's static file is
  unsigned and checked by CRC and digest (§2.2). Moves, relocation, force
  recovery and map edits need `cluster_admin`. The join token is separate.
- **Statement text reaches two new surfaces**, the shard's log and ring, and
  crosses the network. Redaction covers them ("statement text reaches four
  surfaces"). Row images in the Raft log are protected like data files.

## 14. Observability

- **`SHOW SHARDS`**: ranges, leader, term, replicas with match index, apply lag
  and rate, rows, bytes, open partitions and resident bytes, schema epoch and
  fingerprint status, fence state. **`SHOW SHARD MOVES`**: phase, bytes, log lag,
  fence time, verify result. **`SHOW CLUSTER`**: nodes, slots, roles, authority,
  map epoch per node.
- **Alarms on any non-zero value:** `coverage_mismatch`,
  `shard_ownership_mismatch`, write-guard trips, `shard_replica_diff_mismatch`,
  `shard_replica_halted`, fingerprint mismatches.
- **Counters:** refusals by reason; transparent retries; `08007`s; gateway hop
  p50/p99; gather bytes and gather-semaphore waits; quorum-wait and ReadIndex
  histograms; learner bytes/s and index readiness; unresolved PREPARED count
  and age (phase 5); follower backpressure; fsyncs/s per drive.
- **Sizing inputs:** distinct tenants and partitions touched per idle window per
  shard, and the **resident bytes** of those partitions, so §11.2's row-weighted
  active fraction is measured.
- `pg_stat_statements` and the ring gain `shards_touched`, `forwarded`,
  `fractured_reads`.

## 15. The authority as a single point of failure (phases 2–3)

While it is down, **every statement, transparent retries and Raft failover keep
working**. Moves, relocations, DDL, map edits, new nodes and backup marks stop.
A lost authority is rebuilt, not restored: `rdb-authority rebuild --from-shards`
collects each shard's ownership state and owner epochs from its own log, refuses
any gap or overlap, and signs the result above every epoch seen. Phase 4
replaces it with an openraft group of three.

## 16. Phased plan

Each phase is safe to stop at and refuses what it does not support.

| Phase | Ships | Still refuses | Measures; gate |
|---|---|---|---|
| **0** (single node) | `0b11` refused at status open (both representations); composite `PRIMARY KEY` recorded (`is_primary`, §8.1); node-id uniqueness; cluster mode requires `WAL_ENABLED`. **Asks of partitioning:** 2a–3 with the resident-bytes budget; the `FOREIGN` per-pid state, `RangeMap::within(owned, n)`, and `ACQUIRE_SPAN`/`RELEASE_SPAN` map-log edits (§5.2); the shared log-framing module (§2.2); composite identifying keys (its phase 3). **External:** bug #7, `SEQUENCES` per database (§8.2) | all sharding | commits/s per `Database` on target hardware, ext4, 8 and 64 clients |
| **1a** | Static map (CRC and digest); RF1 shard processes; remote-only gateways; classifier; admission, re-route, `FOREIGN` spans, stubs, write guard on every write and delete path; single-shard execution; lazy binding with epoch pinning; RC cross-shard reads in blocks; cancel slots; shard-disjoint `BIGSERIAL`, table-less `nextval`; cluster DDL by operator tool with fingerprints | all fan-out; multi-shard writes; moves; replicas | gateway hop on a point read (target < 150 µs, LAN) |
| **1b** | Fan-out of `COUNT` and rows; coverage check; caps, unrouted refusal, gather semaphore | top-N, aggregates | fan-out latency over 128 shards |
| **1c** | Fan-out top-N (strict wrapper) and aggregates (fold-only mode, codec) | RR/SERIALIZABLE/write-transaction fan-out; joins, `DISTINCT`, windows | S21 property tests green |
| **2** | Authority; signed maps; offline range moves with drain leases, verify and handoff; offline relocation with tombstone; `EXECUTE ON EACH SHARD`; rebalance plan; backup marks under the handoff lock. **Needs** phase 0's span edits and composite keys | online moves; replicas | insert-path move rate on ext4 |
| **3** | Per-shard openraft over row images: family capture with discard, §9.2's commit path (no PREPARED), in-place fast path off, re-apply on restart, non-durable asserting apply at the storage layer, insert-only applied log, `SeqReserve`, DDL with barrier and `schema_fp`, ownership in the log, ReadIndex, election failover, learner relocation through the backup snapshot routine, incremental replica diffing, halting apply; statement replication and JSON shadow tables refused in cluster mode | follower reads; automatic rebalancing | **Gates:** a stateright or TLA+ model of §9.2–9.5 and the simulation suite pass first; persistent-index v1 or the shard locator (§9.4); follower apply ≥ 1.5× leader peak under a cold-partition mix, else parallel apply first; commit and read p50/p99 and throughput under RF3 with six replicas per machine; batched ingest rate (§8.5); one-shard restore with index readiness; failover and re-replication time |
| **4** | Online range moves on the Raft log (`Drained`, `TxnData` start); openraft authority; gather for `DISTINCT`, windows, joins, alias and expression `ORDER BY`; correlated tenant predicates; several `Database`s per process | cross-shard writes | fence time vs range size; per-`Database` overhead |
| **5** | Opt-in 2PC, family-wide PREPARED with §7's rules and S17, anchor-log decisions, recovery; reference tables; decision-pausing marks | — (fan-outs may see 2PC partially, documented) | 2PC latency; in-doubt resolution time |
| **6** | Closed timestamps and consistent cross-shard snapshots; follower and lease reads; distributed deadlock detection; automatic rebalancing; `SPLIT_TENANT`; clone splits | — | — |

The co-located in-process gateway path, and S19 with it, is deferred until the
phase 1a hop measurement shows it is needed.

**Production for the 207 TB target** needs phase 3 here, partitioning phase 3,
persistent-index v1, persistent-index v2 if §14's counters meet its trigger
(above ~1-2% active with three indexes, ~5% with one; persistent-indexes §16),
and incremental snapshots (§11.7).

**Coordination.** No sharding code lands in `core/index/*`, which
rasterizeddb-99 owns and is changing; indexes stay local per shard, so the
superset invariant holds per node with no cross-node argument. Sharding asks
for a narrow, additive interface on that work's schedule: (1) an index openable
without a full scan; (2) the existing PK probe plus a way to wait for a
partition's PK index to be ready; (3) nothing else, since apply reaches the
ordinary maintenance hooks through the storage layer. The edits that do collide
with other sessions are elsewhere, and each hunk goes to its owner before
commit (rasterizeddb-99 requires seeing any `executor.rs` or `table.rs` hunk):

| Phase | File: function | Change | Additive? |
|---|---|---|---|
| 0 | `core/mvcc/status.rs`: open, settled load | refuse `0b11` | yes |
| 0 | `core/rql/lexer_ct.rs`, `unique_key.rs`, sidecar | `is_primary` | field added |
| 0 | `core/partition/map.rs`, `log.rs` (partitioning) | `FOREIGN`, `within`, span records, framing module | yes, partitioning's schedule |
| 0 | `core/hash/xxh64.rs` (moved there by persistent-indexes §15, phase 0) | the residual point predicate, `old_h` and the digests hash with it, never ahash | none: uses the moved module |
| 1a | `core/row/table.rs`: `insert_row_in_txn`, `update_rows_locked`, `delete_rows*` | write guard | one call per path |
| 1a | `core/rql/executor.rs`: routing hook | per-reference re-route; `txn_start_time` input | hook, new input |
| 1a | `pgwire.rs` (10 direct executor calls, 13 direct table reads) | remote-execution seam for the gateway | new seam |
| 1b | `executor.rs`: partition calls | `Part{ranges}` bounding, residual xxh64 | yes |
| 1c | `concurrent_processor.rs` | strict `pub(crate)` top-N wrapper | yes, the agreed seam |
| 1c | `core/rql/aggregate.rs`, `executor.rs` | fold-only mode | new mode |
| 3 | `table.rs` | `ChangeSink` on the three paths | one call per path |
| 3 | `executor.rs:9365-9430`, pgwire `COMMIT` | `propose_written_transaction` in cluster mode | branch, today's path untouched |
| 3 | `core/mvcc/transaction.rs`: `try_begin_in_place_for` | cluster flag | one check |
| 5 | `status.rs`, `transaction.rs` | Prepared outcome and §7's rules | not additive; dedicated review |

## 17. Invariants and the tests that pin them

Each mutation must turn its test red; afterwards, grep the tree for `MUTATED`.
Tests are bounded, remove their temp directories, and are judged on exit code.

| # | Invariant | Enforced by | Test (mutation it must catch) |
|---|---|---|---|
| S1 | The map is total, disjoint, block-aligned, on `route_v1`; a node's epoch never decreases | §2 | fuzz `db_cluster.map` at every offset; a mid-block boundary is refused *(drop the alignment check)* |
| S2 | At most one shard accepts writes for a point, over all time | §6, §10.3 | sim: partitions and crashes at every ownership record, destination loss, the epoch + 2 re-grant; the destination `ACQUIRE`s and is partitioned, the source is re-granted, and the destination refuses every request *(ACQUIRE before RELEASE is durable; re-grant without INCOMING_ABORT; drop the map-agreement conjunct)* |
| S3 | A `Single` request runs only if every table reference is within `Owned`; unowned spans and stubs error | §5.2 | another shard's tenant in a subquery, CTE, `IN (SELECT …)`, join; unsharded table off its home: `RDB02` or error, never rows *(skip the re-route; empty `FOREIGN` span; empty stub)* |
| S4 | No write stamps a row outside `Owned ∪ Incoming(own move)` | §4, §6 | injected wrong route; key-assigning `UPDATE` on each in-place branch; an injected admission → `FENCE` → begin interleaving on an autocommit `DELETE` *(guard the relocating branch only; no guard on delete; sample the running set before publishing the fence)* |
| S5 | A fan-out answers each routed range exactly once | §5.2, §5.3 | after a move, **no GC**: `COUNT`, `SUM` equal the twin *(unbounded partition calls)*; a faulty gateway dispatching one range to both old and new owner fails the coverage check *(skip the coverage check; echo `served` from the request)* |
| S6 | No write touches two shards before phase 5 | §3.1 | `UPDATE` without tenant; `IN` over the cap: 0A000, no shard changed *(refuse after the first shard)* |
| S7 | A move loses and duplicates nothing | §10.2 | `SET n = n + 1` and non-unique inserts through offline and online moves, crash at every record; a transaction open across `FENCE` committing in the drain; a large transaction straddling I_S; `TxnData` then `Abort`; a PK-changing update; eviction of whole and straddling partitions: digest and twin equal *(apply out of order; cut at `FENCE`; start at I_S + 1; upsert without deleting `old_pk`; flip on mismatch)* |
| S8 | No snapshot sees a moved range empty or one range under two owners | §3.2, §10.4 | RR straddling `ACQUIRE`: 40001; RR reads tenant 42, move, reads again: 40001 *(no admission; no epoch pin)* |
| S9 | Rows decode only under a matching fingerprint | §8.4 | shard with a different `write_order` history: refused at apply and at merge; a v1 transaction proposed after a v2 `Schema` entry; a crash between DDL and applied log *(skip the merge check; omit `schema_fp`; apply DDL without the fingerprint test)* |
| S10 | Sequence values never repeat across shards, terms, moves | §8.2 | inserts on 4 shards with moves and leader kills; a deposed leader rejoins *(seed from `MAX`; issue before the reservation commits)* |
| S11 | Gathers never truncate; empty routes aggregate; cancel errors | §5.3, §3.3 | over-cap; `COUNT` 0 and `MAX` NULL on an empty tenant; cancel a 4-shard fan-out *(truncate at the cap)* |
| S12 | Prepared statements never pin a shard | §3.2 | 10 executes alternating tenants on different shards *(route at Parse)* |
| S13 | Visible implies quorum-durable | §9.2 | sim: kill leader and a follower right after an ack; every acked write survives, no reader saw a lost one; a reader on the leader cannot see a row while its proposal is blocked by a failpoint; restart with unflushed outcomes: every acked write is visible before the node serves; an autocommit fixed-width `UPDATE` whose proposal fails keeps the old value *(mark Committed at proposal; call `commit_written_transaction` before quorum; serve before re-apply reaches the commit index; leave the in-place path on)* |
| S14 | Visibility order = log order; follower = leader at every index | §9.2, §9.4 | T1/T2 inversion; parallel apply under conflicting and disjoint loads *(commit out of order in parallel apply)* |
| S15 | Follower apply is idempotent and exact; a failed apply halts | §9.4 | crash at every apply point, diff empty; inject I/O and DDL errors and a divergent row: halts at that index; parallel apply of 10k disjoint entries: zero 40001; the applied log's slots stay bounded after vacuum *(applied log id outside the transaction; skip the failing entry; a zero-match update succeeds; a single-row applied UPDATE)* |
| S16 | Capture covers every write path and only surviving writes | §9.2 | leader/follower twin diff per path: insert, relocating update, each in-place branch, delete, FK cascade, PK-changing update; `ROLLBACK TO` a savepoint and a failed statement attempt leave the diff empty; a savepoint's rows survive a leader crash after the ack *(skip capture on that path, one mutation each; capture without discard; flush the parent's outcome before its subs')* |
| S17 | (Phase 5) PREPARED is never aborted locally, forgotten, settled or vacuumed past, family-wide | §7 | §7's table; a savepoint plus a crash while prepared keeps its rows *(record PREPARED on the parent only)* |
| S18 | Retention never drops an entry whose local apply is not durable; outcomes resolve by log id | §9.2 | crash with applies unflushed after log compaction: all re-applied; a leader proposes, fails over, the new leader issues its own TxId N, the old node rejoins: diff empty *(retain by follower match only; resolve by TxId)* |
| S19 | The in-process path passes the same admission | §4 | co-located gateway on a deposed leader and on a source after `RELEASE`: refused *(bypass admission when local)* |
| S20 | A learner equals its source at the installed index; excluded files stay out | §10.1 | relocation under writes, then diff and index answers against the twin; first writes after seeding visible; an old copy restarted after offline relocation refuses to open *(skip the WAL window; copy `db_replication.state`; skip the compaction permit; copy the vote file; trust a declined checkpoint; no tombstone)* |
| S21 | The partial codec is exact | §5.3 | encode/decode/merge property test, overflow, `DECIMAL` and `NUMERIC(p,s)` and unconstrained `NUMERIC` `AVG` across shards, jsonb group keys `{"a":1}` and `{"a":1.0}` split across shards *(drop `overflow`; drop `scaled`; drop `shown`)* |
| S22 | Retries happen only before effect | §3.3 | drop the connection after commit dispatch: `08007`, applied once; kill the leader after the proposal, before the quorum: the counter rises exactly once *(retry on connection loss; map a proposal failure to 40001)* |
| S23 | 2PC is atomic; no decision is visible before quorum durability | §7 | failpoints at every step with gateway, anchor, participant crashes; session death while the anchor's `Commit` is in flight; anchor failover with the branch entry in the new leader's log *(answer Aborted from visibility without a log decision; decision visible at local commit)* |
| S24 | Restore at a mark keeps transactions whole | §12 | 2PC traffic, mark, restore all shards; a handoff racing a mark: restore refuses on a gap or overlap *(skip the decision pause; release the handoff lock per shard)* |
| S25 | A fan-out top-N never degrades to unsorted | §5.3 | `ORDER BY` an unprojected column, a nullable key and an alias, `LIMIT 3`, equal the twin or refused *(skip unresolved keys)* |
| S26 | A session reads its own acknowledged writes | §3.2 | `synchronous_commit = off`, write, read on the next statement *(reply before apply)* |
| S27 | A table-less `nextval` never collides | §3.1, §8.2 | ORM-prefetched ids inserted for tenants on several shards *(route `nextval` to a random shard's counter without the shard prefix)* |

**The twin model test** lifts partitioning's I6 one level: three shards of three
replicas in one process against one plain table, under random single-tenant
transactions, conflicts, rollbacks, fan-outs, prepared and simple queries, every
§5.3 operator, empty routes, DDL, moves, relocations and leader kills, every read
compared with the twin.

**The deterministic harness** (`rasterizeddb_cluster_sim`):

- `ShardTransport` and openraft's network trait run over an in-memory network
  with a seeded scheduler that drops, delays, duplicates, reorders and
  partitions, and a `Clock` trait with skew and pauses.
- **A `FaultyStorageProvider` discards every unsynced write at a simulated
  crash**, which SIGKILL cannot model because the page cache survives it. This
  is what tests re-apply on restart, the family's flush order, the log store's fsync order and retention.
- Named failpoints at every ownership record, move, DDL, apply and 2PC step. A
  failing seed is pinned as a regression; CI runs fixed seeds, nightly runs
  random ones. Tokio is not fully deterministic, so the harness owns message
  order and failpoints, and real-process SIGKILL tests recheck on disk.
- openraft's storage conformance suite runs against our log store; a
  Jepsen-style harness on real processes (bounded writers, explicit cleanup)
  feeds Elle's list-append checker; and a stateright or TLA+ model of the commit
  path, step-down and restart re-apply, and the handoff gates phase 3.

## 18. Risks and open questions

1. **The phase 3 commit path** moves visibility to the apply loop and local
   durability behind group commit, in the hottest code, beside the
   per-statement durability invariants. Step-down abort and restart re-apply
   carry the correctness; they need a dedicated review and the model gate.
2. **Capture coverage.** An unlisted write path diverges followers silently. S16
   and diffing narrow this; they do not close it.
3. **The replay ceiling.** If parallel apply is needed and conflicts are common,
   a shard's write rate sits below its leader's.
4. **Insert-path move rate** is unmeasured. Slow means slow hot-tenant moves and
   splits, not slow capacity changes.
5. **openraft**: version churn, a task set per group (six per machine now, more
   with several `Database`s per process), and our adapter's fsync order.
6. **The authority** is a single point of failure for changes until phase 4.
7. **The classifier will refuse legitimate ORM SQL at first.** The node re-route
   is the safety net, not a substitute.
8. **The active fraction is assumed, and row-weighted.** With three indexes
   above ~1-2% active, or one above ~5%, the target needs persistent-index v2,
   not just lazy open and v1; a few large active tenants can outweigh many
   small ones.
9. **Clone splits** (a learner detaching at a `Split` entry into a new group)
   would split at byte speed, but openraft has no native split and the detach
   must be crash-idempotent: phase 6 at the earliest.
10. **2PC weakens fan-out semantics** until phase 6. Proposed: opt-in, documented,
    refused for tables with a fan-out read SLA.
11. **Mixed versions.** `route_v1` is frozen, but literal coercion must agree
    across binaries (partitioning I2); rolling upgrades need a cluster-wide
    minimum version in the Hello.
12. **Six processes per machine** share CPU, memory, cache budgets and
    possibly drives; each replicated commit costs every replica an append fsync
    (§11.4). Only phase 3's RF3 measurement with full placement counts.
13. **Composite keys and span edits** are prerequisites owned by other work
    (§16 phase 0). Range moves cannot ship before them.
14. **Index readiness bounds recovery.** Persistent-index v1 never copies
    snapshots, so a relocated or restored replica rebuilds every index, hours
    per replica; adoption is a v2 requirement.
15. **Replicas must share one partition map.** Per-partition digests (§9.6,
    §10.1 step 4) compare replicas partition by partition, and span edits and
    `RetirePartitions` are log entries, but partitioning's reshapes and
    conversions are node-local, with per-node partition counts (partitioning
    §2, §12). Open: propose reshapes through the shard's log in cluster mode,
    or keep the digests per range.
