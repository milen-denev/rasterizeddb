# Hash partitioning

Status: **designed, not built.** Line anchors were read on 2026-10-16 at
`61e9200` and are relative to `rasterizeddb_core/src/`. `core/row/table.rs`,
`core/vacuum.rs` and `core/filter/sarg.rs` are being edited by another session,
so anchors there will drift; the function names are the stable part.

```sql
CREATE TABLE assets (
  tenant_id      BIGINT NOT NULL,
  asset_group_id BIGINT NOT NULL,
  asset_id       UUID   NOT NULL,
  body           TEXT,
  PRIMARY KEY (tenant_id, asset_group_id, asset_id)
) PARTITION BY HASH (tenant_id, asset_group_id);

SELECT * FROM assets WHERE tenant_id = 42;                        -- one partition
SELECT * FROM assets WHERE tenant_id = 42 AND asset_group_id = 7; -- one partition
ALTER TABLE assets ISOLATE KEY (tenant_id = 42) TO '/mnt/nvme2';
ALTER TABLE assets SPLIT KEY (tenant_id = 42) INTO 8;          -- by asset_group_id
```

A partitioned table is one logical table whose rows are spread over many
physical file sets, **partitions**, chosen by a frozen hash of the row's
**partition key**. A statement that fixes the key, or its leading column,
touches only the partitions that can hold matching rows; any other reads all of
them under one snapshot. Each partition has its own files, locks, indexes and
vacuum state (§5), and can sit on its own disk.

## What was asked, and what this gives

| Ask | What this design does |
|---|---|
| A distinct subtable per tenant | **Not by default.** Tenants map many-to-one onto 1024 partitions by default; a tenant's rows are always in one partition. `ISOLATE KEY` gives one tenant a partition of its own. Isolating every tenant, or `PARTITIONS` near the tenant count, gives the literal version at 3 files and, when open, 15 fds per tenant, affordable to about 10⁵ partitions per node (§7). |
| 100 tenants at once touch 100 files | **Mostly.** 100 random tenants land on about 95 distinct partitions of 1024, about 91 of them alone. 64 partitions give about 51 distinct, 256 give 83, 4096 give 99. Isolated tenants always get their own. |
| No contention | **Partly.** Today one tenant's `DELETE` blocks every `UPDATE` and `DELETE` of the table for its whole scan and rewrite (not `INSERT` or `SELECT`); here it blocks only its own partition. `INSERT` and point-`SELECT` throughput should not change: they never took that lock. Until phase 4 all partitions share one disk's log and fsync stream, and every commit shares the transaction manager and root outcome log (§5). |
| Practically infinite scale | **No, not in one process.** One outcome log and one transaction manager per database bound commit throughput. "Infinite" means sharding across nodes, a separate design that would reuse this one's frozen hash, range map and statement-level routing. |
| Balance subtables across disks | **Yes**, per partition, chosen at first write and recorded in the table's own map (§6.1). First for databases without S3 backup; with backup it waits for snapshots covering pool locations (phase 4b). Gated on a crash test of a durability gap that multi-disk transactions make routine (§12). |
| Several columns in one key | **Yes, hierarchically.** `(tenant_id, asset_group_id)` behaves like `tenant_id` until a large tenant is sub-split by `asset_group_id`; a query fixing only `tenant_id` always routes to that tenant's contiguous sub-range (§1b). |

**Why not a subtable per tenant by default.** A table today costs about 25
file descriptors, 5 mmaps, 30 files and 3–4 background tasks waking about 5.6
times a second idle, and opens eagerly at startup with a full index rebuild.
`deploy.sh:213` sets `LimitNOFILE=65535`, capping a process near 2,600 tables,
and provider opens `.unwrap()` and panic on EMFILE, in
`LocalStorageProvider::new` (`core/storage_providers/file_sync.rs:643-717`) and
in `create_new` (`file_sync.rs:2054-2124`), which opens every table file. So
tenants hash into a bounded set of partitions, made cheap enough that the set
can be large, and the tenants that matter are isolated; the long tail pays no
per-tenant cost.

**The "index" framing.** An index here is a superset accelerator: in memory,
rebuilt at open, droppable for free ([Indexes](indexes.md)). A partition key
decides where bytes live, so creating one rewrites the table, as removing it
would. `CREATE INDEX … USING partition` is accepted as the user's spelling of
`ALTER TABLE … PARTITION BY` (§2); it replies that rows were moved, is not
listed in `pg_indexes`, and refuses `DROP INDEX`.

## What it is not

- **Not PostgreSQL's hash partitioning.** The hash is ours, so a PostgreSQL
  `MODULUS 4, REMAINDER 0` child would hold different rows; children are never
  addressable by name.
- **Not a value directory.** A subtable per key value routes exactly, but
  unrouted queries fan out over every tenant (minutes at 10⁶), a mistyped
  tenant id creates a subtable, and a per-tenant drop unlinks files under open
  snapshots. The hash map gives the same prefix routing and sub-splitting
  without those costs.
- **Not global uniqueness** (§1b), **not automatic** (splits and moves are
  statements, like `STORAGE REBALANCE`), **not multi-node sharding**, and not a
  way around the shared commit points of §5.

## 1. Model

### 1.1 What a partition is

**Logically**, the rows whose **routing point** (a `u64`, §1.3) lies in a
half-open `[lo, hi)`. The **range map**, a sorted `Vec<(lo: u64, pid: u32)>`,
covers `[0, 2⁶⁴)` with no gap or overlap, checked by `RangeMap::validate` on
every load and new map epoch. Partition ids are monotonic and never reused.

**Physically**, three files in one pool location, named from a random nonzero
64-bit table id `uid` fixed at `CREATE`, each with today's `.tmp` sibling:

```
__pt_<uid:016x>_<pid:08x>.db            row bytes
__pt_<uid:016x>_<pid:08x>_pointers.db   row pointers (xmin, xmax live here)
__pt_<uid:016x>_<pid:08x>_flt.db        filter checkpoint
```

Names use only `[A-Za-z0-9_]`, well under 128 bytes, to fit the schema
header's name filter (`core/row/schema.rs:540-555`) and avoid the `$` trap. No
table name, so `RENAME` touches no partition file; no tenant value, so no
customer data reaches a file name. Flat in the location directory, since a
subdirectory has no write-ahead log and falls back to an fsync per append,
about ten times slower ([storage pools](storage-pools.md), "Write-ahead logs").
No schema or sequence file: a partition opens with a clone of the shared schema
and a handle on the shared row-id sequence (§3.4, §3.5).

**The logical table** is a `PartitionedTable` in a new `Database.partitioned:
DashMap<String, Arc<PartitionedTable>>` (schema, key spec, map with each
partition's placement and slot, row-id sequence), plus a **shell** in
`Database.tables` (`core/database/mod.rs:671`): a `Table` with the shared
schema that **refuses every data access with an error** (§3.3). About 120
non-test uses of `tables.get`, `iter`, `contains_key` and `remove` span 18
files, 49 in `executor.rs` (a few name other maps). The shell keeps schema-only
sites working (pgwire describe, `param_infer`, `index_name_taken` at
`core/rql/executor.rs:7939`, `referencing_keys` at `executor.rs:1236`, which
`ON DELETE CASCADE` uses) and makes unconverted data sites fail loudly; absent,
some would miss silently: `DROP TABLE IF EXISTS t` would succeed dropping
nothing (`drop_table`'s `Ok(false)` becomes success,
`executor.rs:6626-6629`), and `ALTER TABLE IF EXISTS t …` would report
"relation absent, nothing to do" (`executor.rs:7066-7082`).

### 1.2 How tenants map to partitions

A tenant, one value of the leading key column, occupies one contiguous
**block** of 2¹⁶ points (§1.3). Every boundary that `CREATE`, an ordinary
split, a merge or a move sets is a multiple of 2¹⁶, so a tenant not sub-split
by `SPLIT KEY` is always in exactly one partition. With N partitions the
initial boundaries are `lo_i = ⌊i · 2⁴⁸ / N⌋ << 16`.

**The proposed default N is 1024**, for the arithmetic above; an unused
partition costs a map entry, since files and location come at first write
(§3.2, §6.1). The price, unrouted fan-out, is unmeasured, so phase 2a fixes the
default (§8).

### 1.3 The hash

Version `route_v1`, frozen and recorded in the map header; a binary that does
not know a table's version refuses to open it.

```
frame(f, b)  = f:u8 ‖ len:u32 LE ‖ b
H_i          = XXH64(frame(family_i, norm_i(v_i)), seed = 0x5244_4250_5254_0000 + i)   // "RDBPRT"
point, 1 col = top48(H_0) << 16
point, 2 col = top48(H_0) << 16 | top16(H_1)
point, 3 col = top48(H_0) << 16 | top8(H_1) << 8 | top8(H_2)
```

- **XXH64 is in-tree**, about 80 lines, so no crate update can move data,
  pinned by reference vectors and framed golden vectors per family and level:
  `0`, `-1`, `i64::MIN`, `u64::MAX`, `''`, non-ASCII text, a fixed UUID.
- **Not ahash**, which `KeyLocks::stripe_of` (`core/row/key_locks.rs:53-62`)
  uses with `write_usize`: its output can change with crate version, target
  features and pointer width, harmless for lock stripes, catastrophic for
  persisted placement.
- **Seeds depend on the level, not the table**, so `tenant_id` gives the same
  block in every table leading with it, all co-location needs. Levels hash
  separately, so `('ab','c')` and `('a','bc')` differ at level 0; the length
  prefix also keeps frames self-delimiting.
- **The input is column-typed bytes after coercion, never literal text**: at
  insert, `RowWrite.columns_writing_data[k].data` (`core/row/row.rs:29-35`)
  after defaults and generated values; at query, `SargPredicate.value`
  (`core/filter/sarg.rs:57`), coerced by `conjunct_to_pred` (`sarg.rs:158`) with
  the evaluator's own `coerce_literal` (`core/value_codec.rs:1453`). Both go
  through one `route_bytes(family, &[u8])`, the pairing the hash index relies
  on and property-tests (`core/index/key.rs:434`). Normalisation is per family,
  unlike `encode_canonical`'s per-width bytes (`key.rs:222`).
- **Collisions are harmless.** Two tenants share a 48-bit block with expected
  count n²/2⁴⁹, about 0.002 pairs at 10⁶ tenants. Routing is a superset and
  rows are re-evaluated against the whole `WHERE`, so colliding tenants merely
  cannot be isolated from each other.

### 1.4 Allowed key types

An allowlist, not `hash_supported` (`core/index/key.rs:83`):

| Family | Types | Normalised bytes |
|---|---|---|
| `0x01` integer | I8–I128, U8–U64 | the value as i128, 16 bytes LE, so `INT 42` and `BIGINT 42` hash alike |
| `0x02` uuid | UUID | the 16 stored bytes |
| `0x03` text | TEXT, VARCHAR(n) | the UTF-8 bytes, in the evaluator's binary order |

All else is refused at DDL time with its reason, by one rule: if the engine's
`=` for a type may yet change to match PostgreSQL, rows placed under the old
semantics would sit in the wrong partition.

| Refused | Reason |
|---|---|
| U128 | does not fit in i128 |
| F32, F64 | NaN, −0, lossy coercion |
| NUMERIC | bytes depend on scale; `parse_decimal_scaled` (`value_codec.rs:500`) truncates |
| JSONB | its `=` is not PostgreSQL's, and is not property-tested |
| CITEXT, BPCHAR, CHARACTER, CHAR(n > 1), NAME | map to STRING without their semantics (case folding, blank padding, NAME's 63-byte truncation; `core/rql/lexer_alter.rs:527-532`). Bare `CHAR` and `CHAR(1)` map to `DbType::CHAR`, not on the allowlist. |
| INTERVAL, TIMETZ, geometric and range types | equality is not byte equality |
| BOOL | too few distinct values |
| a non-default `COLLATE` | the rule above |
| date, time, INET, MAC | v1 only: sound, but unasked for |

`CREATE TABLE … PARTITION BY` catches the STRING aliases by declared text. At
conversion the text is gone (neither the field record nor the `TYP` sidecar
keeps it; `core/rql/type_modifier.rs` holds only precision and scale), so
conversion refuses a STRING key unless a new additive sidecar, which must join
`RENAMED_SUFFIXES`, recorded it as `TEXT` or `VARCHAR`. Older tables cannot
convert on a text key; use `CREATE … PARTITION BY` plus `INSERT … SELECT`. The
PostgreSQL importer decides from the source's `pg_type` (`citext`, `bpchar`,
`name`), never the mapped type.

### 1.5 NULL

Key columns must be `NOT NULL`; `ALTER … DROP NOT NULL` on one is refused. A
NULL is zero bytes plus a `__null_` companion (`core/rql/nullable.rs:48`), so it
would hash like `0` or `''`, and `IS NULL` names the companion, so it could
never route. On a NOT NULL column the rewriter makes `x IS NULL` into `1 = 0`
(`sarg.rs:506`), routing to zero partitions by proof. Key columns cannot be
generated, retyped or dropped; `RENAME COLUMN` is fine, as the hash never sees
names.

## 1b. Composite keys

### Flat or hierarchical

| | Flat: `H(frame(t) ‖ frame(g))` | Hierarchical: `top48(H₀(t)) ‖ top16(H₁(g))` |
|---|---|---|
| A query fixes `(t, g)` | 1 partition | 1 partition |
| A query fixes `t` only | **every partition** | the partitions overlapping t's block, normally 1 |
| Where one tenant's rows sit | sprayed over all partitions | contiguous, so the tenant can be isolated |
| A big tenant | already spread, but every one of its queries fans out | starts in one partition and is split by g on demand |
| Low-cardinality leading column | spreads evenly | concentrates rows in few partitions |

**Only hierarchical in v1**: "supply `tenant_id` and touch only that tenant's
subtable" is prefix routing, which flat cannot do. Flat, PostgreSQL's choice
for `PARTITION BY HASH (a, b)`, suits only a low-cardinality leading column, a
layout the documentation should advise against ("put the tenant column
first"). `WITH (key_layout = 'flat')` can come later without touching v1
tables.

**Widths.** At most three columns: `[48]`, `[48,16]` or `[48,8,8]`, frozen in
the map header. Level 0 is 48 bits at every arity, so tables leading with
`tenant_id` put a tenant in the same block. The 16 low bits allow 65,536
sub-ranges per tenant.

### What routes

| `WHERE` fixes by equality | Routed to |
|---|---|
| Every key column | One point, so one partition |
| A leading prefix: `tenant_id` alone, or the first two of three columns | That prefix's interval: normally one partition, or the tenant's sub-split pieces |
| Only a non-leading column, such as `asset_group_id` | All partitions |
| A range, `!=`, a function or a cast on a key column | Nothing at that level; it is unconstrained |

**Multi-column IN.** `tenant_id IN (1,2) AND asset_group_id IN (7,8,9)` is
lowered by `rewrite_where` (`core/tokenizer/predicate_rewrite.rs:1673`) to an
`AND` of `OR`s: 6 points (§4.2). Each `IN` is one box holding a value set, so no
product is built to be thrown away. Past **256 points** the expansion
**degrades from the right**, dropping the last constrained level from every box,
de-duplicating, repeating; 3 tenants × 500 groups route to the 3 tenant blocks.
The cap applies **during** the fold (§4.2, step 4), so two or three
10,000-value lists cost O(cap × levels), never 10⁸ boxes. Only dropping level
0, or level 0 alone exceeding the cap, reaches all partitions; degrading only
widens. A tuple-`IN` such as `(t, g) IN ((1,7),(2,9))` is not lowered by the
rewriter, so it fans out.

### Sub-splitting a big tenant

`SPLIT KEY (tenant_id = 42) INTO k`, k a power of two up to 1024, places `k − 1`
boundaries in the tenant's block B at `B + j·2¹⁶/k`, dividing its rows by the
hash of `asset_group_id`: `(42, g)` hits one piece, `42` alone all k (still not
the whole table), other tenants nothing. A single-column key cannot split a
tenant, as one hash value is atomic, which is also Citus's and Yugabyte's limit.
Declare `(tenant_id, <natural sub-key>)` from the start; the second level costs
nothing until used.

### Uniqueness

Every `UNIQUE` and `PRIMARY KEY` must contain **every** key column,
PostgreSQL's rule, so equal unique values share a partition and per-table
enforcement (`unique_lock`, `check_unique`, the unique index) is exact. The
rejected alternative, a leading-column unique key with sub-splitting refused
while it exists, makes uniqueness depend on layout history: every
boundary-creating path (split, merge, rebalance, isolate, co-location joins)
must enforce it, and one miss stores a silent duplicate. The static rule is one
DDL check nothing later can break. Its cost: a `(tenant_id, asset_group_id)`
key rules out `PRIMARY KEY (tenant_id, asset_id)`. The error says so and names
the alternative, partitioning by `(tenant_id)` alone (isolatable, not
sub-splittable). An opt-in is §17.6.

**Row-addressing statements need a composite identifying key.** `FOR
UPDATE`/`SHARE`, `SKIP LOCKED`, `UPDATE … FROM` and `DELETE … USING` name each
row by one column (`identifying_field`, `core/rql/executor.rs:8968`: an inline
`UNIQUE` column, a one-column `PRIMARY KEY` or unique key), refusing composite
keys; `PRIMARY KEY (a, b)` leaves `schema.primary_key` empty
(`core/rql/lexer_ct.rs:229-233`). By the rule above a composite-keyed table has
no one-column unique key, and a one-column key's only candidate is the key
itself, one row per tenant. So these are refused on essentially every
partitioned table, as on plain tables with a composite primary key (so the twin
test cannot notice), until `identifying_field` accepts a composite key, with
`RowLocks` keyed by the framed tuple (`encode_canonical` is already
length-prefixed) and `refuse_write_to_locked_rows` using it: a phase 3
prerequisite that fixes plain tables too.

### Co-location

`PARTITION BY HASH (tenant_id) COLOCATE WITH accounts` shares `accounts`' map
and placement; reshapes apply to the group under one epoch. Members need the
same level-0 family and `route_v` (INT and BIGINT co-locate). A group sub-split
cuts every member at the same points; a one-column member's rows for that
tenant land in the first piece (low bits zero), still correct. It puts a
tenant's rows in every member on one disk and enables partition-wise joins.
Phase 6.

## 2. SQL surface

```sql
CREATE TABLE t (...) PARTITION BY HASH (c1 [, c2 [, c3]]) [PARTITIONS n] [COLOCATE WITH u];

-- The user's framing; the same conversion as the ALTER (phase 5)
CREATE INDEX t_by_tenant ON t USING partition (tenant_id, asset_group_id) [WITH (partitions = n)];
ALTER TABLE t PARTITION BY HASH (tenant_id, asset_group_id) [PARTITIONS n];

-- Reshaping (phase 5); local to this node, not replicated
ALTER TABLE t SPLIT PARTITION FOR (tenant_id = 42);
ALTER TABLE t ISOLATE KEY (tenant_id = 42) [TO '/mnt/nvme2'];
ALTER TABLE t SPLIT KEY (tenant_id = 42) INTO 8;
ALTER TABLE t MERGE PARTITIONS FOR (tenant_id = 42), FOR (tenant_id = 77);   -- adjacent only
STORAGE MOVE PARTITION t FOR (tenant_id = 42) TO '/mnt/nvme2';
STORAGE REBALANCE PARTITIONS [DRY RUN];

SHOW PARTITIONS t;
SET partition_trace = on;   -- a NOTICE per statement: partitions routed, and why
```

**Refused**, with `0A000` or `42P16` and a reason:

| Refused | Reason |
|---|---|
| `PARTITION BY RANGE` or `LIST`; `DEFAULT` partitions | not designed |
| `PARTITION OF … FOR VALUES WITH (MODULUS m, REMAINDER r)`, `ATTACH`, `DETACH` | the child would hold different rows than in PostgreSQL. A later phase may accept `PARTITION OF` as a **write-only alias** (`INSERT` and `COPY` route through the parent, as a `pg_dump` restore needs; `SELECT`, `UPDATE`, `DELETE` stay refused). |
| A key breaking §1.4 or §1.5, or over three columns; a unique key without every key column | §1.4, §1.5, §1b |
| Naming a partition; `CREATE TABLE` or `RENAME` to a `__pt_` name | `add_table` checks only `contains_key` (`core/database/mod.rs:2100`); rename refuses only the JSON shadow prefix (`database/mod.rs:1408`) |
| `STORAGE MOVE TABLE t` | points at `MOVE PARTITION` |
| JSON columns in shredded mode | shadow tables would be unpartitioned; native JSONB is fine |
| Temporary partitioned tables; KV-backed partitions | KV until measured |
| `DROP INDEX` on a `USING partition` index | not an index (above) |
| Until their phase: `ALTER`, `RENAME`, `CREATE`/`DROP INDEX` on partitioned tables (phase 3; physical-rewrite `ALTER` phase 6, §11); point-changing key assignment (phase 6, §9); FKs referencing a partitioned table (phase 3, §10); `FOR UPDATE`/`SHARE`, `SKIP LOCKED`, partitioned targets of `UPDATE … FROM`/`DELETE … USING` (composite identifying keys, §1b); reshapes (phase 5) | §15 |

**Today's mis-parse, fixed first.** `parse_create_table` takes the body between
the first `(` and the **last** `)` (`core/rql/lexer_ct.rs:169-171`), so `… k INT
NOT NULL) PARTITION BY HASH (k)` **creates an unpartitioned table and reports
success**. Phase 0 replaces it with a scan tracking nesting and quotes that
finds the column list's matching paren and refuses a top-level `PARTITION`,
`INHERITS` or `PARTITION OF` after it, on word boundaries. Other trailers
(`WITH`, `TABLESPACE`) keep today's behaviour, a separate latent bug.

**PostgreSQL import** already reads partitioned sources through the parent
(`core/migrate/source.rs:185-194`). It emits `PARTITION BY HASH (<same
columns>)` when every key column is NOT NULL and allowed and every unique
constraint contains the key; otherwise a plain table, warning with the reason.
It never reproduces PostgreSQL's remainders.

## 3. Storage layout and metadata

### 3.1 Parent files and the catalogue

All parent files live in the root location:

| File | Contents |
|---|---|
| `__pt_<uid>_schema.db` and its sidecars `__pt_<uid>_schema_{FIELDS,FK,DEF,UK,JSON,TYP,CHK,GEN,PIX,IDX}.db` | the one shared schema. Sidecar names derive from the schema provider's name (`core/row/schema.rs:459`), so the load code needs no change. |
| `__pt_<uid>_map_<gen>.db` | the partition map log, one live generation (§3.2) |
| `__pt_<uid>_ids.db` | the shared row-id reservation (§3.5) |
| `__pt_<uid>_seq.db` | durable high-water marks of the table's `SERIAL` columns (§10, phase 3) |

**The catalogue row** keeps `table_name` and sets `clusters = uid` (written 0
today, `database/mod.rs:2111-2113`, only logged at load,
`database/mod.rs:2892-2895`); nonzero means partitioned, files `__pt_<uid>_*`.

**Downgrade guard.** An older binary ignores `clusters`: `Table::new` creates an
empty `t_schema.db`, `TableSchema::load` refuses a file shorter than its header
(`core/row/schema.rs:436-445`), the table is marked `schema_unreadable`
(`core/row/table.rs:560-574`) and left out of the map
(`database/mod.rs:958-964`): missing, not empty. That chain was read, not run;
it must be **tested against the previous release's binary** before phase 2
ships (I18). There `CREATE TABLE t` then succeeds (`add_table` checks only
`contains_key`) with a second live row `t`, `clusters = 0`, and since startup
inserts once per row (`database/mod.rs:950-967`), after re-upgrading load order
would pick the `t` served. So the new binary **refuses to start when two live
catalogue rows share a name**, naming both.

**`RENAME`** avoids `rename_table`, which copies every `<old>*` file, rewrites
`<new>_schema.db` by name, and commits via `remember_in_catalogue`, hardcoding
`clusters = 0` (`database/mod.rs:2003-2010`, called at `:1469`): the next start
would open the table as plain, and with no row carrying the uid the reaper
would delete every `__pt_<uid>_*` file. A partitioned rename (1) records the
rename intent (`record_rename_intent`, `database/mod.rs:2073`); (2) writes a new
row with the **same** `clusters = uid` via a `remember_in_catalogue` variant
taking the cluster value, **the commit point**; (3) rewrites the name in
`__pt_<uid>_schema.db`'s header; (4) tombstones the old row. No file is copied
or deleted. Recovery reads two live rows with one uid as "rename committed",
keeps the newer name and finishes steps 3–4; the reaper sees a live uid
throughout (I25).

**`db_storage.cfg` never names a partition**, even in memory:
`StorageManager::render` serialises every `placements` entry
(`core/storage/mod.rs:933`) and round-robin `choose` counts them under the
write lock (`storage/mod.rs:1306`), which would bring back O(N²) placement. A
partition opens from its map entry's location via `seed_at(path)`, never
`pool.location_of` (`storage/mod.rs:495`) or `seed_in`'s lookup
(`database/mod.rs:471`), which answer "root" for unknown names and would open
empty files there. Pool guards consult `Database::partition_locations_in_use()`,
counting materialized partitions only: a location holding any cannot be
dropped or made a backup. `STORAGE DISABLE LOCATION` stops new materializations
there at once and drains the rest via `REBALANCE PARTITIONS`.

### 3.2 The map log

`__pt_<uid>_map_<gen>.db`, in the root directory, is an append-only log of
`len:u32 ‖ crc32:u32 ‖ kind:u8 ‖ payload` records (the existing `crc32fast`);
load replays to the last record whose CRC passes. **Every record is appended
with `append_data(buf, true)`**, through `append_data_txn`, issuing its WAL
record at append time (`file_sync.rs:1637-1645`, `:1529-1530`). The default
deferred append stages into `.tmp` and lands later with `sync_all` and no WAL
record (`file_sync.rs:1647-1700`, `:955-1035`), so nothing would order it before
an outcome.

| Record | Payload | Durability |
|---|---|---|
| `HEADER` | `uid`, `route_v`, key `(write_order, family)` list, widths, co-location group | fsync |
| `LOCATIONS` | interned location paths, so removing a location cannot shift indices | fsync |
| `PLACE` | `[(pid, lo)]`: one record per `CREATE` or reshape. No location: an unwritten partition has none (§6.1). | fsync |
| `MATERIALIZE` | `(pid, floor, loc_idx)`: the files exist at that location; the forget floor at that moment (§7.2) | WAL record at append; group commit |
| `DIRTY` | `(pid, floor)`: a clean partition is about to take its first stamp (§7.2) | root log durable before the stamp |
| `SWEPT` | `(pid, swept_to, clean)`, appended after the sweep's writes are durable | group commit |
| `STATS` | `(pid, rows, bytes)`, after vacuum and on a close that wrote something | group commit |
| `RESHAPE_BEGIN` / `_COMMIT` / `_ABORT` | sources and new entries, with targets' locations | fsync |
| `RETIRED_GONE`, `DROPPED` | pids / none | group commit / fsync |

**`MATERIALIZE` needs no fsync of its own.** Order: delete leftover
`__pt_<uid>_<pid>*` files (§12), create the files exclusively, fsync the
directory once; append `MATERIALIZE` with its WAL record; write the first row;
at commit, append its outcome to the same log. Replay stops at the first torn
record, so a durable outcome implies a durable `MATERIALIZE` (consequences:
§12). `bulk_load` takes no WAL records (`file_sync.rs:1523-1528`); there a load
that dies is discarded whole, that mode's existing contract.

**Materializing cost is unmeasured.** `create_new` fsyncs the directory per
file and per `.tmp` (`file_sync.rs:2077-2111`), `LocalStorageProvider::new`
three times per file (`:634-671`): six to nine for three files. The partition
path's `create_partition_files` creates every file, fsyncs the directory once,
and propagates that error, which today's constructors discard (`let _ =`).
Phase 2a measures first-write latency per new tenant on ext4.

**Compaction** triggers past twice the last snapshot plus 1 MiB (a fixed
threshold would undercut one snapshot of 65,536 slots and compact on every
append): (1) clone the map under the map mutex, release; (2) write `HEADER`,
`LOCATIONS` and a `SNAPSHOT` of the clone to a new generation; (3) retake the
mutex, append records taken since the clone and `SEALED`, fsync file and
directory, switch appends; (4) delete the old generation. Load takes the
highest generation ending in `SEALED`, reaping the rest. WAL file ids hash the
file name (FNV, `core/wal.rs:164-173`), so old records never replay onto a new
generation and compaction need not wait for a checkpoint. The mutex, also
taken by `MATERIALIZE` on INSERT, is held only for step 3's tail.

### 3.3 The shell refuses data by construction

`Table::partition_shell(name, schema_io)` builds it. Guarding a method list
would be an allowlist, and anything off it would answer from emptiness; three
rules close that:

- **Data fields become private** (`io_pointers`, `io_rows`, `io_filters`,
  `io_sequence`, `len`, `last_row_id`, `filters`, `indexes`, `free_space`,
  `readers`, `vacuum_stats`; `table.rs:48-150`), behind accessors that return
  `Err` on a shell. The compiler lists every outside read: 29 non-test ones of
  `io_rows` and `io_pointers` alone, in `vacuum.rs`, `introspect.rs`, `wal.rs`,
  `catalog_views.rs`, `storage/sql.rs` and `database/mod.rs`. `pg_index` and
  `pg_indexes` treat `indexes == None` as "no indexes" (`catalog_views.rs:988`,
  `:1053`), so a shell would hide every declared index from the view a
  migration tool reads to decide whether to re-issue `CREATE INDEX` (§14).
- **Every data method checks first**: each `pub`/`pub(crate)` method of `impl
  Table` starts with `self.data()?` or is on a named schema-only allowlist; a
  test parsing the `impl` block fails on any other. This catches, among others,
  `find_row_with_value`, `locate_row_by_id` (`table.rs:2452`), index probes,
  `vacuum_now` (`:1100`), `compact` (`:1166`), `sizes_of`, `relation_size`, and
  the executor's `seed_sequences` (`executor.rs:6865`), `lock_selected_rows`
  (`:4427`) and `returning_rows_for_write` (`:8617`), each erroring until
  converted to dispatch on `Database::relation(name) ->
  Relation::{Plain(Arc<Table>), Partitioned(Arc<PartitionedTable>)}`.
- **Every name-keyed `Database` method starts with `self.relation(name)`**,
  dispatching or erring before removing anything from `tables`; the `impl` scan
  covers `impl Database` too. These reach files via `seed_in`, bypassing the
  shell: `table_bytes` (`database/mod.rs:1584`), whose `create_new` for 15
  suffixes would litter the root with empty `<logical>*` files and report 0;
  `reload_table` (`:2493`), which removes the map entry before `Table::new(name)`
  fails, so `DROP TABLE IF EXISTS` then silently succeeds and `CREATE TABLE t`
  duplicates the catalogue row (`CREATE INDEX` and metadata `ALTER`s reach it);
  `move_table_to_backend` (`:1642`); `rename_table` and helpers
  (`copy_table_files` `:1512`, `rewrite_schema_name` `:1948`);
  `drop_table_to_depth`; `vacuum_all` (`:2337`), which logs a failed table and
  returns the rest, so it and bare `VACUUM` now fail the statement instead.
  Whole-table `STORAGE REBALANCE` (`storage/relocate.rs:214-227`) picks from
  `tables`, smallest first, which a shell would be; it skips shells.

### 3.4 The partition `Table`

`Table::open_partition(physical, seed_at(location), schema clone, ids, guard,
mode)` runs `new_with_constraints`'s body (`table.rs:482`) but loads no schema
file or `_seq.db`, spawns no per-table tasks in `mode = External` (phase 3 on),
installs the range guard (§4.5), and sets **`cache_scope` explicitly** to
`"<db_id>\0<physical name>"`; derived from the shared `schema.name`
(`table.rs:467`), one partition's cached rows would answer for another (I20).
The seam sits in files another session is editing (`table.rs`, `vacuum.rs`, the
processor) and the field change is mechanical but wide, so it lands as one
reviewed change, announced first, after that session's index work.

### 3.5 One row-id sequence for the logical table

`locate_row_by_id`, the unique check's `exclude_id` (`table.rs:1981`, `:2375`)
and "at most one live pointer carries a given id" (`table.rs:2474`) need
table-wide unique ids. One sequence per logical table is reserved durably in
`__pt_<uid>_ids.db` in large steps and handed out in chunks of 1024 (one
contended `fetch_add` each), so no raw copy (§6.2), merges included, gives
`exclude_id` two rows with one id. A missing or unreadable ids file refuses the
table, never falling back to the last record's id, which the code calls exact
only for legacy ascending-id files (`table.rs:603-611`). As `create_new` and
`LocalStorageProvider::new` silently create missing files
(`file_sync.rs:2077-2080`, `:634-641`), the parent's ids and map files and every
file of a materialized partition are checked by a never-creating open
(`File::options().read(true).write(true).open`) before any provider is built;
a missing one fails with a named error (I9).

## 4. Routing and pruning

### 4.1 Where it hooks

Routing runs on **exactly the `WHERE` text the partitions' scans will
evaluate**, the post-rewrite clause tokenized against the shared schema as
`build_candidates` does (`table.rs:2730`), late, at the executor arms that
resolve the table name and dispatch on `relation()`:

| Statement | Hook |
|---|---|
| SELECT | the `QueryRows` arm (`executor.rs:10911`) and `execute_query_rows` (`:10954`), taking a `RowSource::{Single, Partitioned}` |
| Prepared SELECT | the `PreparedRows` arm (`executor.rs:9591-9593`); `PreparedRows.table_name` (`core/rql/prepared.rs:38`) stays **logical**. pgwire renders each execution's literals into the template (`pgwire.rs:3378-3386`) and that `WHERE` routes fresh, so a learned parse cannot pin the first tenant's partition (I11). |
| INSERT | after defaults, generated values and CHECKs (`executor.rs:9917`), from the final `RowWrite` |
| UPDATE / DELETE | `executor.rs:10318` / `:10737` |
| Joins | `execute_join`'s relation loop (`executor.rs:24`): each relation's pushed-down predicate routes it |
| UNION, CTEs, `IN (SELECT …)` | re-dispatched as SQL text (`substitute_in_subqueries`, `executor.rs:645`) |
| `$n` | already inlined as literal text (`pg_params.rs:97`) |

### 4.2 The algorithm

1. If `sarg::clause_is_constant_false` (`sarg.rs:665`) holds, route to **zero**
   partitions: the only zero exit, a proof the scan already trusts.
2. Run `sarg::extract_gated(tokens, |wo| key_wos.contains(wo), &scales)`
   (`sarg.rs:473`), shaped like `point_update_stripe` (`table.rs:2162`); `None`
   means all partitions. Use this entry point, not `extract_gated_with_paths`.
   The path-aware variant gives `doc->>'k'` predicates virtual write_orders
   counting down from `u32::MAX - 1` (`index::FIRST_PATH_COLUMN`). The gate
   above already rejects them, but any code that walks the predicates must
   treat a write_order that is not a real field as unconstrained.
3. Fold the `MaskExpr` (`sarg.rs:66`) into **boxes**, one optional value set
   per key level (unset is unconstrained). The match is an allowlist; any
   variant it does not name widens.

   | `MaskExpr` | Boxes |
   |---|---|
   | `Pred { op: Eq, wo: level i, value }` | one box, level i = `{value}` |
   | any other `Pred`, or a predicate on a non-key column | one box, all levels unconstrained |
   | `All`, or any unlisted variant | one box, all levels unconstrained |
   | `Or` whose every branch is `Pred(Eq)` on one key level (a lowered `IN`) | **one** box, that level = the set of values |
   | `And(children)` | the product of the children's boxes. Two sets for one level are **unioned, not intersected**: an empty intersection would route to nothing without proof, and would hide an evaluation error the scan would raise. |
   | `Or(children)`, otherwise | the union of the children's boxes |

4. Check the 256-point cap (§1b) **as the fold goes**: when a partial `And`
   product, a union, or one box's level-set product would exceed it, degrade
   right-to-left at once, in the limit dropping level 0 (the whole space).
   Memory stays O(cap × levels).
5. Expand each box's leading constrained levels into point intervals (level 0
   unconstrained covers everything).
6. Binary-search the pinned map for overlapping partitions; union them.

`!=` (no `SargOp`), `NOT`, `LIKE`, functions, casts (`tenant_id::text = '5'`),
`OR` across columns, and `id = 1.5` on an integer key (the evaluator compares
floats; `coerce_literal` refuses it) all widen. Fan-out is always correct, only
slower.

### 4.3 Skipping without opening

A partition without `MATERIALIZE` is **known empty** and skipped unopened, by
proof, never by row-count estimate: per §4.4 the flag is set before the first
row is written, the writer commits after, a reader seeing the row snapshotted
after the commit, and it reads the flag after its snapshot. A resident bitmap
of materialized partitions makes the skip O(1). A bit is set at `MATERIALIZE`
and cleared only when the pid is retired, never
because vacuum found no live rows, since nothing proves that survives a
concurrent insert.

### 4.4 Snapshot first, then the map

Every statement, each in an explicit transaction included, (1) takes its
`Visibility` (`Table::visibility_for`, `table.rs:1065`); (2) loads the map epoch
with `ArcSwap::load_full`, holding a counted `Arc<MapEpoch>` (a plain `load`
guard is invisible to the strong count reclamation waits on, §6.2), and reads
the materialized flags; (3) holds both throughout, the `Visibility` registered
until the last partition call returns. Reversed, rows are lost: a statement
pins epoch E, a reshape flips to E+1, a writer inserts into an E+1 target and
commits, and a snapshot taken after that commit sees the row while the
statement reads the E source, which never had it. With the snapshot first this
cannot happen: any write to an E+1 target happened after the flip, so it
committed after any snapshot taken before the flip. Swapping the steps must fail
I7.

**Single-partition writes keep today's behaviour**: an `UPDATE` or `DELETE`
without foreign keys passes no `Visibility` (`executor.rs:10416-10422`), so the
table snapshots under `write_lock` (`table.rs:3343`, `:3649`) and a queued
writer sees the previous one's committed result instead of raising 40001. A
one-partition write does the same under that partition's `write_lock`, with the
slot's `write_gate` held shared, which still precedes any write to an E+1
target of that range because a reshape's fence waits for the gate.
Multi-partition statements use the one `Visibility`, so fan-out writes raise
more 40001s than on a plain table.

### 4.5 The range guard

Every partition carries `(KeySpec, lo, hi)`; the guard is on the row, not a
branch:

- **`insert_row_in_txn`** (`table.rs:2829`) errors if the recomputed point lies
  outside `[lo, hi)`.
- **`update_rows_locked`** recomputes the point from the merged `row_write`
  once assignments and expressions apply (`table.rs:3750-3856`), before
  `check_unique` or any branch, and fails with 0A000 if it changed. Two
  branches never relocate: the fixed-width fast path (`write_columns_in_place`,
  `table.rs:1548`, taken at `:3925-3931` whenever no other transaction runs, as
  in a quiet-server batch job or migration) and the in-place overwrite
  (`relocated == None`, `:3991`). Guarding only the relocating branch would let
  `SET tenant_id = 43` overwrite a row in tenant 42's partition, report
  success, and hide it from routed queries. Every `UPDATE`, the point-update
  fast path and `ON CONFLICT DO UPDATE` (`apply_conflict_update`) included,
  reaches this function.
- **Reshape raw-copy and delta writes** check the target's `[lo, hi)`.

The rule is "the point is unchanged", which depends only on the frozen hash so
every node agrees, not "the row stays in this partition", which depends on this
node's count and sub-splits and could pass on the primary but fail on a
replica (§12). It runs in release builds, not as a `debug_assert`, at one
XXH64 per key level per written row, so a routing or reshape bug fails a
statement rather than storing a row no query finds. With agreement (I2), the
superset rule (I3) and snapshot-first (I7), the guard (I4) is why routing
cannot silently miss a row; the twin test (I6) checks all four.

## 5. Concurrency and contention

**Per partition**, so unshared across partitions: files, mmaps, `.tmp`
staging; `write_lock`, taken exclusively by `DELETE`, general `UPDATE` and the
aborted-write sweep ([MVCC](mvcc.md), "Which writers take turns"); the 64
`unique_lock` stripes and `compaction_gate`; free space, reader epochs,
`dead_stamps`, cache scope and generation; filters, indexes, vacuum. `INSERT`
and point `SELECT` never took `write_lock`: expect parity, not a gain.

**Still shared, under any partition design:**

| Shared point | Per statement | Held across I/O | Fix, cheapest first |
|---|---|---|---|
| `TransactionManager` (`core/mvcc/transaction.rs:296`) | ~3 write-lock takes (begin, `mark_wrote`, commit), a running-set copy per snapshot (`:1058`), 2 registry-mutex trips; SELECTs included | no | a lock-free running set or epoch snapshots: deep, a separate design |
| `StatusLog` mutex around the **root** WAL `writev` (`core/mvcc/status.rs:662-703`) | one per writing commit | `writev` | stage bytes under the mutex, append outside (small). Per-disk outcome logs need cross-log commit ordering; not proposed. |
| One WAL per directory: `writer` across `writev`, `gate` across `fdatasync` | every partition on a disk | yes, deliberately | WAL shards keyed `(dir, shard)`, each registered by recovery; never a log per partition (an fsync each) |
| `Database::sync_durable` syncs **every** pool path (`database/mod.rs:1027-1075`) | a commit pays another disk's fsync debt | yes | sync only logs this transaction touched, root last; required before phase 4 |
| `RowLocks::release_all` | two global mutexes per commit | no | atomic fast path at `row_count == 0`; cheap, independent of this work |
| The query-stats ring | one mutex | no | shard it |
| `Arc<PartitionedTable>` refcount, epoch `load_full` | one `fetch_add` each on one cache line per logical table | no | borrow from the map guard |
| Per-logical-table DDL lock (§11), new | one shared acquire, an atomic RMW | across the statement, never blocking one | none; reader-preferring, so DDL waits, never statements |
| WAL checkpoint sync pass and keep-tail (`core/wal.rs:1328-1347`, `:1418-1455`) | sequential `sync_data` per dirty file, about 2.3 ms each on .7's disk; then the tail is copied, fsynced and renamed **under the writer lock** | yes | parallelise the pass (bounded threads) or `syncfs` past a threshold; rotate segments instead of copying the tail |

Only the checkpoint row worsens with partitioning: about 3 dirty files per
active partition instead of per table, so 1000 active partitions mean a
multi-second pass and a large tail copied under the lock, stalling every writer
in the location. Phase 2a measures it on ext4 (tmpfs hides it). Each cheap fix
is done when a phase 2 profile names it; the deep ones are not proposed, and
nothing here depends on or rules them out.

## 6. Disk balancing and moving partitions

### 6.1 Placement

**An unwritten partition has no location.** Most of 1024 may stay empty for
months; placing at `CREATE` would balance map entries rather than data, give a
later disk nothing until a rebalance, and aim a new tenant's first write at a
since-filled disk. The location is chosen **at materialization**, recorded in
`MATERIALIZE` (already the durability point), among enabled primaries then
above `min_free_bytes`, by weight (and available bytes under `free-space`).
**The dealer is cross-table**: it picks the location furthest below its share
in materialized count and `STATS` bytes **summed over every partitioned
table**, ties to the lowest index, so small tables cannot pile onto one disk;
never `choose`'s argmax (`storage/mod.rs:1314`), which sends every new
partition to the emptiest disk. Reshape targets use the same dealer, recorded
in `RESHAPE_COMMIT`. A new location gets its share of later materializations
at once; recorded placement is never recomputed ([storage
pools](storage-pools.md)). Until phase 4, all is in root.

### 6.2 One primitive: reshape

Move, split, isolate (a three-way cut at the tenant's block edges), sub-split,
merge and conversion are all `reshape(sources → [(range, new_pid,
location)])`. **Every target is a new pid and every source is retired**, so no
live partition holds versions outside its range.

1. **Begin.** Append `RESHAPE_BEGIN`, fsync. Sources go append-only (no
   free-space reuse or in-place overwrite; vacuum, compaction and forget sweeps
   paused), so a source slot can change only its stamps, never point at
   another row.
2. **Copy, writers running.** Copy every record below `L0`, each source's
   pointer-file length, with its row, to the target holding `point(row)`:
   **raw** (`xmin`, `xmax`, `deleted` kept, as VACUUM FULL moves rows, no new
   MVCC transaction) and range-guarded. Uncommitted versions are copied, so a
   transaction idle between statements keeps its rows; waiting on
   `oldest_snapshot` to filter them would lose those (`settle_bound` tracks
   running transactions and snapshots separately, `transaction.rs:930`) and
   could starve behind a long transaction.
3. **Fence.** Take the source's `write_gate` exclusively; it belongs to the
   slot, not the `Table`, and every write path holds it shared. Writers drain;
   readers are unaffected.
4. **Delta.** Re-read every source record: rewrite changed ones below `L0`,
   copy those at or above it, each **routed by its own recomputed point**,
   never a remembered slot mapping; disagreement aborts. The fence lasts one
   pointer-file pass plus new rows, at about 40 B per record roughly a second
   for 10M rows (inferred; measured in phase 5). Very large partitions need a
   repeated delta: copy until it is small, then fence.
5. **Verify** per-range record counts against the source and a sampled
   checksum of row bytes; build the targets' filters and indexes.
6. **Durable.** `sync_durable` the targets' locations, append `RESHAPE_COMMIT`
   with the new entries and each target's forget pin (§7.2), fsync: **the commit
   point**.
7. **Flip.** Store epoch + 1, mark sources `Retired`, release the fence. A
   writer queued on a source gate wakes, sees `Retired`, and **re-routes its
   share against the new epoch**; it held nothing of the source, so the
   statement does not restart.

   **A re-routed visit is bounded to its share's points.** Each `UPDATE` or
   `DELETE` partition call carries the point ranges it owns (normally `[lo,
   hi)`); a re-routed call to T carries the retired source's ranges ∩ T's and
   skips, uncounted, matched rows outside them. Else, in a merge `S1 + S2 → T`,
   a statement that finished S1 before the fence would rescan in T the S1 rows
   the delta copied with its own new versions, which it sees
   (`core/mvcc/transaction.rs:246-258`; there are no command ids): `SET n = n +
   1` applies twice and count and `RETURNING` double, reporting success. Key
   assignment never moves a point (§4.5), so ranges stay disjoint across
   visits, two reshapes, or two sources into one target. Phase 6's row moves
   break that premise and need their own exclusion (§9).
8. **Reclaim** waits on epochs, not the `Table`: a statement clones a
   partition's `Arc<Table>` only on visiting it, 8 at a time, so it may yet
   visit retired source S while S's strong count is 1. The map keeps each
   superseded `Arc<MapEpoch>` where S was live, plus S's slot, and reclaims S
   (deletes its files, appends `RETIRED_GONE`) once all have strong count 1,
   held by that list alone. Until then E readers read S correctly; their
   snapshots predate the flip (§4.4).

Row locks key on logical name plus key bytes (`core/mvcc/row_locks.rs`), or
the framed tuple with composite identifying keys (§1b), so no reshape drops
them.

**No MVCC-mover fallback.** Delete-and-insert movers lose rows under
`REPEATABLE READ`: their inserts commit before the flip, a transaction whose
snapshot predates them and whose next statement loads E+1 cannot see them, and
the retired source holding its visible version is never read again. The raw
copy keeps `xmin` and `xmax`, so every snapshot sees in a target exactly what it
saw in the source.

### 6.3 Rebalancing

`REBALANCE PARTITIONS [DRY RUN]` plans from `STATS`, never `table_bytes` on an
unopened partition (§3.3). Moves run **in bounded parallel**, by default one
per location and at most 4, on disjoint sources with their own fences and
serialised flips, so adding a disk under 10⁴ partitions is not hours of
sequential copies. The first failure stops new moves; a re-plan resumes.
Partitions never use `move_table_to_backend`, which has no write fence: a writer
holding the old `Arc` can write past the copy into files about to be deleted.

## 7. Scale to many partitions

### 7.1 Resident cost

| State | fds | VMAs | Heap | Tasks |
|---|---|---|---|---|
| Unmaterialized | 0 | 0 | one map entry, ~16 B | 0 |
| Closed | 0 | 0 | one slot, ~64 B: state, location, pin, stats, last use | 0 |
| Open | 15 (3 providers × 5) | 3 | ~8 KB, plus filters and indexes in proportion to rows | 0 from phase 3 |
| A `Table` today | ~25 | 5 | 15–30 KB, plus filters and indexes | 3–4, ~5.6 wakeups/s idle |

**Startup** from phase 3 opens the shell and replays the map, nothing more; WAL
recovery still replays closed partitions, since `register_directory`
registers every file it finds (`core/wal.rs:1938`).

**The open budget**, at most `min(4096, (RLIMIT_NOFILE − 2048 − 25 × open
plain tables) / 15)` partitions per process across tables, is a **sharded
CLOCK** (no global lock on the hot path), scan-resistant (fan-out opens with
the reference bit clear). Provider open and map return `Result` on the partition path,
never panic; on EMFILE the budget evicts and retries once, then fails the
statement with `53000` (insufficient resources), and replica apply treats
`53xxx` as transient and retries, never skips (§12). **A write is never refused for
the budget**, since success must not depend on this node's `RLIMIT_NOFILE` or
open count (§12); past it, an open evicts and waits.

**Phase 2 has no idle close** and opens every materialized partition at
startup, so it caps declared partitions at 2,048 across all tables (about 31k
fds when all are written), checked by `CREATE`: a constant decides alike on
every node, unlike an `RLIMIT`-derived budget, and no write hits it. Plain
tables open first; a table whose partitions cannot all open stays a shell,
erroring loudly (§3.3), and the rest of the database starts.

**Close writes nothing if `write_generation` did not move**, and **a fan-out
open is scan-only** (filter read-only, index rebuild deferred to the first
routed access or write, §7.2, point 4), so an unrouted `SELECT` wider than the
budget rewrites no O(P) checkpoints, feeds no map-log compaction, and starts no
rebuilds it then discards.

### 7.2 Lazy open and idle close

**1. The forget horizon.** Getting this wrong silently resurrects rows. Today
dropping a `ForgetHandle` leaves the minimum (`transaction.rs:412`), a reopen
registers at the current floor (`:938`), and ids below the forgotten horizon
read as **Committed** (`status.rs:1086`): a closed partition with an unswept
aborted stamp would stop pinning, and its aborted rows would return on reopen.
So the `PartitionedTable` holds **one** `ForgetHandle` at the minimum of its
partitions' **pins**:

- **A pin** means unswept aborted stamps may exist at or above it. It lives in
  the slot, so **a closed partition keeps pinning**: dirty partitions can close,
  and a long transaction cannot grow the open set without bound.
- **Clean** partitions pin nothing. A forget sweep makes one clean if it
  reached `swept_to = h`, saw only stamps below h, and `write_generation` did
  not move meanwhile.
- **After any sweep**, clean or not: once its writes are durable
  (`sync_durable` over the location), append `SWEPT(pid, h, clean)` and raise
  the pin to h.
- **First write after clean**: under the registry lock, before the first stamp,
  pin at `forgotten_below()`, sound since the writer's id ≥ `oldest_active` ≥
  the forgotten horizon. This needs `ForgetHandle::lower_to`, as `forget_below`
  never moves back (`status.rs:1588-1589`). The edge is also **durable**:
  append `DIRTY(pid, floor)` and wait for the root log. Log order alone fails,
  because an aborted writer has no outcome and kernel writeback can land its
  stamp with no record ordering it. Cost: one group-commit wait on a clean,
  hence cold, partition's first write.
- **After a restart**, a partition whose last of the three records is
  `MATERIALIZE`, `DIRTY` or an unclean `SWEPT` is pinned at its last `SWEPT` or
  `MATERIALIZE` floor until swept again; a clean `SWEPT` pins nothing. Sound:
  stamps below the pin were neutralized, later ones are newer, and nothing
  stamps after a clean `SWEPT` without a durable `DIRTY`. A lost `SWEPT` only
  pins lower, so it needs no fsync.
- **Restart cost**: forgetting waits until every partition dirty at the crash
  (the recently written set, not all materialized) is swept once, by
  pointer-file-only opens. At 2 bits per transaction the status bitmap grows
  about 18 MB per 72M transactions meanwhile (inferred).
- **Reshape targets** inherit the source's pin in `RESHAPE_COMMIT`, under
  `forget_pause`, before it is retired.

**2. Exclusive close.** `Open` → `Closing` only at the slot's `Arc<Table>`
strong count 1 (statements and scheduler jobs hold clones; newcomers wait on
the slot's `Notify`). With no per-partition tasks, one live instance per file
set is structural, unlike today's `reload_table`.

**3. Close** lands `.tmp` and, if written since open, writes the filter
checkpoint and `STATS`; one rejected as dirty is backfilled (correct, unpruned
meanwhile).

**4. Index rebuild on open** is O(partition rows) on the scheduler, affordable
at 1/N of the table; until ready, queries scan and unique checks use filter
plus scan, which is correct.

**The WAL file map.** `Wal::register_file` silently overwrites a mapped id
(`wal.rs:488-490`) and checkpoints silently skip unresolvable ids
(`wal.rs:1326-1334`). So **close never unregisters** (about 100 B per file ever
opened), and **registering an id mapped to another path is a hard error** from
phase 0: at 10⁶ files a 64-bit FNV collision is small but not negligible, and
today it silently replays into the wrong file.

### 7.3 Shared maintenance

One scheduler per database (`MaintenanceMode::External`), driven by a dirty
queue that writes push rather than per-table timers, replaces each partition's
services, flush, filter and vacuum tasks. Jobs: landing `.tmp`; filter
checkpoints; vacuum (`vacuum::run_once`, `core/vacuum.rs:1314`, under the
`SweepScheduler` permits, `vacuum.rs:1857`); forget sweeps of pinned
partitions, cold ones round-robin at low priority; close preparation. Idle
partitions cost no wakeups: clean ones are no longer swept every 2²⁰ ids
(`vacuum.rs:1523-1532`).

### 7.4 Nothing O(P) per statement

- **Routing** is O(log P + k).
- **Database-wide walks stay O(logical tables)** (`referencing_keys`, catalog
  walks, bare `VACUUM`, `index_name_taken`), as partitions are not in
  `Database.tables`. `pg_class`, `SHOW TABLES` and `pg_stat_user_tables`,
  rebuilt per statement, read `PartitionedTable` running totals kept from
  `STATS` and live deltas, O(1) each. `referencing_keys` still walks every
  table per `DELETE` (`executor.rs:1236-1249`); phase 3 replaces it with a
  `Database.fk_reverse` index maintained at DDL, for all tables.
- **`schema_generation`** moves once per logical DDL, never on materialize,
  close or reshape; `add_table` bumps it for non-internal names
  (`database/mod.rs:2094`), and partitions bypass `add_table`.

**Literal one-subtable-per-tenant scale.** 10⁵ partitions × 3 files × 2 is 600k
directory entries in one location (a lazy `.tmp` halves it). There
`register_directory`'s startup `read_dir`, backup snapshot directory walks, and
post-burst checkpoints (§5) grow from milliseconds to seconds; the phase that
raises N measures them.

## 8. Multi-partition queries

**One snapshot** (§4.4) needs new entry points: only
`query_row_with_visibility` (`table.rs:3010`) and the write paths accept a
`Visibility`, so `count_rows_in_txn` (`:2944`) and `query_row_bytes_in_txn`
(`:3133`) gain `_with_visibility` variants delegating as `query_row_in_txn`
(`:2981`) does. Otherwise a READ COMMITTED fan-out could see a cross-tenant
transfer half applied (I7).

**Phase 2 fan-out** keeps at most 8 partitions in flight, with workers from
**one statement budget** of `max_permits_threads()`, so the variants take a
worker cap: each call now starts `min(threads, batches)` workers itself
(`core/processor/concurrent_processor.rs:904`), up to all of them per large
partition. A single-partition statement calls straight through: a tenant-scoped point
read must cost no more than an unpartitioned one plus one route. Phase 6's `process_many` runs
`spawn_batch_tasks` per partition on one channel with a shared
`limit_counter`, `stop`, `eval_error`, `cancel`, `count_only` and `TopN`.

| Operator | Across partitions |
|---|---|
| `COUNT(*)` | sum the counts, then `row_count_result` (`core/rql/aggregate.rs:584`) |
| Aggregates, GROUP BY, HAVING, FILTER | phase 2: concatenate and fold once. Later: a `Folder` per partition and `Folder::merge` (`aggregate.rs:1634`), keeping AVG as `(total, n)` |
| LIMIT without ORDER BY | stop launching partitions once enough rows exist, then truncate |
| ORDER BY … LIMIT k OFFSET o | each partition returns its sorted top `k + o` (`QueryRows.limit` already includes OFFSET); concatenate and re-cut with `sort_rows_if_needed` (`concurrent_processor.rs:1800`, made `pub(crate)`) |
| ORDER BY without LIMIT, DISTINCT, windows, expression sorts | gather, then the existing post-scan pipeline; valid because every partition's schema is a clone of one, so `write_order`s agree |
| Joins | concatenated bytes in one `RowSet::Serialized` (`core/rql/join.rs:331`); the 4 GiB cap (`join.rs:343`) applies to the sum, and still errors |
| FOR UPDATE, SKIP LOCKED | refused until composite identifying keys (§1b); then gathered |

**An empty route still runs the pipeline.** A constant-false clause, or only
unmaterialized partitions (a new tenant's first `SELECT COUNT(*) … WHERE
tenant_id = 42`), runs the post-scan pipeline over an empty source:
`row_count_result(0)` and a `Folder` finished with no input, so `COUNT(*)` gives
one row of 0 and `MAX(x)` one row of NULL, as PostgreSQL and the twin do. "No
partitions, so no rows" is a silent wrong answer for every aggregate without
`GROUP BY`.

**Query cache.** Each partition caches its share under its own scope and
generation; pids are never reused, so a scope never returns meaning other rows.
No cached result spans partitions, and only statements routing to 4 or fewer
are admitted, so a wide fan-out cannot flush the cache.

**Unrouted fan-out cost is unmeasured.** The code's one figure, `COUNT(*)` on a
one-row table at about 0.46 ms against 0.15 ms for `SELECT 1`
(`concurrent_processor.rs:875`), predates the `min(threads, batches)` cap and no
longer holds. Per partition remain the visibility, `build_candidates`, one
worker, and an open if closed. Phase 2a measures fan-out over 1024 one-row
partitions and phase 3 the open cost, before default N is fixed. Such queries
show in `partitions_touched` (`pg_stat_statements` and the statement ring), a
`partition_fanout_warn` counter (default: over 256 partitions), and
`partition_trace`.

## 9. Writes

- **INSERT** routes from the final `RowWrite`, materializes if needed (placing,
  §6.1), takes the `write_gate` shared, re-checks the epoch, and runs
  `insert_row_in_txn`, whose in-partition unique check under its stripes is
  exact by §1b. Multi-row `INSERT` already re-dispatches each tuple
  (`insert_values_batch`, `executor.rs:5661`); `INSERT … SELECT` routes each
  row.
- **UPDATE and DELETE** visit routed partitions in ascending pid order, with one
  `Visibility` if several (§4.4), each call bounded to its point ranges (§6.2,
  step 7) and taking only its own `write_lock`, none held across partitions, so
  no lock-order deadlock. `point_update_stripe` works unchanged in the one
  partition a fully keyed `WHERE` reaches. Counts sum; RETURNING runs
  `returning_rows_for_write` over the same route and `Visibility` and
  concatenates.
- **Conflicts.** A partition's conflict is retryable only if it applied
  nothing, the statement's only if no partition did; otherwise 40001,
  non-retryable. Both retry loops honour this: autocommit's and
  `execute_in_txn`'s (`executor.rs:9445`), the one ORM traffic uses. A failed
  statement in `BEGIN` poisons the block (25P02), so no partial apply commits.
- **Assigning a key column** with the point unchanged is ordinary, as it must
  be: ORMs assign every column, usually its current value. Any other key change
  fails the whole statement at the guard (§4.5, which also says why the rule is
  not "stays in its partition") with 0A000 ("changing a partition key value is
  not supported yet"). Phase 6 adds the move, allowing every key change:
  retire the row in the source under its locks, release them, insert into the
  target under its stripes, in one transaction; first-updater-wins guards
  concurrent movers, RETURNING shows the new row. A moved version has a new
  point and the mover's `xmin`, so a later visit of the same statement would
  match it again; phase 6 must design a per-statement record of versions
  written.
- **ON CONFLICT.** The target is a unique key, so contains the partition key:
  probe, insert and follow-up update all hit the inserted row's partition, where
  `apply_conflict_update` (`executor.rs:4319`) runs unchanged. Targetless `DO
  NOTHING` is covered too, as every unique key contains the partition key.
- **Replication capture** (`note_write`, `executor.rs:9383`) records the
  logical name.

## 10. Constraints

- **UNIQUE and PRIMARY KEY**: per partition, global by §1b's rule, which
  `ALTER TABLE ADD UNIQUE` and `CREATE UNIQUE INDEX` check before validating
  each partition.
- **Foreign keys.** Partitioned to plain: unchanged. Plain parent, partitioned
  child: the shell's schema carries the child's FKs for `referencing_keys`, and
  `apply_on_delete` (`executor.rs:1301`) cascades as SQL through
  `execute_with_context`, which routes; its existence checks read the child by
  name, so they error on the shell until converted, never skipping silently
  (I24). Referencing a partitioned table: refused until phase 3; the referenced
  key is unique, so contains the key, so each check then routes to one
  partition.
- **SERIAL**: one sequence per logical table (`sequence_name(table, col)`,
  `core/rql/sequence.rs:33`). Phase 2, with every materialized partition open,
  seeds it once per process from a `MAX` over them (`seed_sequences`); that
  `MAX` is unroutable, so under lazy open the first post-restart insert would
  open and scan every partition. From phase 3 the high-water is reserved
  durably in `__pt_<uid>_seq.db` in steps, like row ids (§3.5), never from
  `MAX`; a crash leaves a gap, as in PostgreSQL.
- **CHECK, DEFAULT and generated columns** live on the shared sidecars and run
  before routing, so a `DEFAULT` on a key column works.

## 11. DDL fan-out

**One schema.** Every partition's `TableSchema` clones the shell's, so write
orders, dropped-column history and `__null_` companions match by construction,
and the "schema_id is a write_order" corruption class cannot occur between
partitions.

**Metadata-only DDL** (rename column, defaults, CHECKs, `CREATE`/`DROP INDEX`,
marking `DROP COLUMN`, table `RENAME`) reopens partitions, safe only once
`MaintenanceMode::External` and exclusive close exist, so it ships in **phase
3**: in phase 2 each partition's own tasks hold provider clones and
write until they notice `alive = false` (`table.rs:239-244`), so a reopen would
put two live instances on one file set, `reload_table`'s hazard times the
partition count. Steps: (1) write the shell's sidecars once; (2) rebuild the
shell's schema in place, never via `reload_table` (§3.3); (3) bump
`schema_epoch`; (4) exclusively close and reopen each **open** partition with
the new clone, O(open partitions) and never O(P), with an index rebuild each,
which a new index needs anyway; closed ones take it at open. Statements hold the table's DDL lock shared from routing
to their last partition call and step 3 holds it exclusively, so none mixes
epochs. **It is reader-preferring, and DDL try-acquires it** against
`lock_timeout`, failing with `55P03`: statements never queue behind DDL, so one
long fan-out cannot stall every tenant, and no wait cycle forms through a
statement holding it while waiting on a row lock (the row-lock deadlock detector
sees only transaction edges, `core/mvcc/row_locks.rs:607-635`). The cost: DDL
can starve under load and must be retried.

**Physical-rewrite DDL** (populated `ADD COLUMN`, `ALTER … TYPE`, rewriting
`DROP COLUMN`; `rewrite_table_with_columns`, `executor.rs:2125`;
`retype_column`, `:3068`) is refused until phase 6. It reads the whole table
into memory and recreates it; per partition it is not transactional, and a
crash midway leaves two layouts under one schema, silently corrupting data.
Phase 6 makes it a **generational reshape**: copy every partition to new pids
under the new schema, flip map and schema generation in one `RESHAPE_COMMIT`,
keep the old generation until then, at twice the disk meanwhile.

**Conversion** (`ALTER TABLE … PARTITION BY`, `CREATE INDEX … USING partition`)
is a reshape from the plain table, fencing writers only for the delta; it
commits at `RESHAPE_COMMIT`, then rewrites the catalogue row to `clusters =
uid`. At startup a map with `RESHAPE_COMMIT` and a row still at 0 rolls forward;
one with only `RESHAPE_BEGIN` rolls back, deleting the new files, in the shape
of `recover_interrupted_rename` (`database/mod.rs:2597`). DDL is not
transactional here, so every window resumes or refuses to open; none is half
applied silently.

## 12. Durability, recovery, backup, replication

| Operation | Order | A crash leaves |
|---|---|---|
| CREATE | (1) delete any orphan `__pt_<uid>_*`; (2) shell schema and sidecars, `HEADER`, `LOCATIONS` and `PLACE`, fsynced; (3) the catalogue row with `clusters = uid`, **the commit point**; (4) insert into both maps | before (3): orphans with no catalogue row, which are reaped. After (3): a complete, empty table. |
| Materialize | §3.2 | files without `MATERIALIZE`: **deleted at startup** (below), never reused. `MATERIALIZE` with files missing: **the table refuses to open** (I9). |
| Reshape | §6.2 | before `RESHAPE_COMMIT`: sources untouched (pausing only sets in-memory flags); replay deletes targets, appends `RESHAPE_ABORT`. After: replay deletes retired sources. |
| Rename, conversion | §3.1, §11 | resumes or rolls back |
| DROP | (1) tombstone the catalogue row, **the commit point**; (2) remove from both maps; (3) `DROPPED`; (4) partition files; (5) parent files last | files of a uid with no catalogue row, which are reaped |

`CREATE` avoids `add_table` (`database/mod.rs:2093`), which writes the row first
(`:2105-2188`) and never rolls back, leaving a row with no map. Map recovery
runs after WAL recovery, before any partition opens.

**The reaper** touches only names matching
`^__pt_[0-9a-f]{16}_([0-9a-f]{8}(_pointers|_flt)?|schema(_[A-Z]+)?|map_[0-9]+|ids|seq)\.db(\.tmp)?$`,
and deletes one only if its uid has no catalogue row, its pid is retired or
lacks `MATERIALIZE`, it sits where its map does not place it, or it is a
non-live map generation (§3.2). It runs **only if every map parsed cleanly**;
one unreadable map disables reaping for that start, with a warning, so a
corrupt log never gets live files deleted.

**Files without `MATERIALIZE` are never reused.** Data files are written
through the page cache before their log records are durable, so writeback can
persist rows of a transaction whose outcome and `MATERIALIZE` were lost. The
partition is then unmaterialized (skipped, unpinned, never swept), the
forgotten horizon passes the aborted id X, and reusing the files with a floor
above X would read X's rows as committed (`core/mvcc/status.rs:1084-1087`). So
the reaper deletes them **before forgetting resumes**, and materializing deletes
leftovers again. Deleting is safe: a durable outcome implies a durable
`MATERIALIZE` (§3.2), so such files hold only versions no snapshot can see.

**S3 backup.** Snapshots walk only the database directory, and other locations'
logs are deliberately not streams (`core/backup/mod.rs:428-435`, `:1016`): root-located
partitions and their parent files are covered, but off-root partitions would be
**silently left out**, a gap plain tables on pool
disks already have. Refusing placement alone misses a backup enabled later, so
both: under `s3_backup`, off-root placement is refused with the reason; and the
backup service refuses to start, and each snapshot fails loudly, when
`Database::partition_locations_in_use()` reports a materialized partition
outside the database directory. Its only check today reads `db_storage.cfg`
(`warn_about_excluded_tables`, `core/backup/mod.rs:987-1010`), which never names
a partition, and only warns. Both lift in phase 4b.

**The cross-session ordering gap** (inferred, untested): session C syncs disk
D; A writes to D and stages its outcome in the root log; C's root sync makes A's
outcome durable before A's rows on D; a crash breaks A's all-or-nothing.
Spread partitions make multi-directory transactions normal, so phase 4 is
gated on a page-cache-drop crash test (SIGKILL cannot reproduce it) and the
fix: `stage()` writes only outcomes whose transactions finished their non-root
syncs.

**Replication.** Statements ship with logical names
(`replication/manager.rs:563-577`), and each replica routes with its own map.
`CREATE … PARTITION BY` replicates, with per-node partition counts. Conversion
replicates as logical DDL, each node using its own count, so the refusals it
adds (unique key without the key columns, `DROP NOT NULL`) match everywhere.
Reshapes are physical and local, like `STORAGE MOVE TABLE`. LWW `table_stamps`
stay per logical table. **Every refusal of a logical statement depends only on
replicated state**: replica apply skips a failed statement under `Skip` and
`LastWriterWins`, commits the rest and advances
(`replication/server.rs:413-428`), silently diverging the nodes, and under
`Abort` stalls; hence §4.5's rule and §7.1's never-refused writes. `53xxx`
resource failures are the one local failure left; replica apply must retry
them, which `server.rs` does not yet distinguish. The reseed path is unread; a
test must show a reseeded node returns identical rows.

## 13. Hot tenants

`ISOLATE KEY (tenant_id = 42) TO '/mnt/nvme3'` reshapes the tenant's block into
its own partition, as Citus's `isolate_tenant_to_new_shard` does (own files,
write lock, disk): the ceiling for a single-column key. `SPLIT KEY` spreads a
tenant over k partitions and up to k disks (§1b). Not provided: splitting one
atomic `(tenant, group)` value, and automatic splitting; `SHOW PARTITIONS`
(§14) and `partition_trace`, which samples the hottest level-0 hashes, find
candidates. An automatic isolate past a size
threshold could come later, one concurrent reshape at a time. **Dropping a
tenant** is a routed, MVCC, replicated `DELETE … WHERE tenant_id = 42`; a
physical drop of an isolated tenant would need a fence, a wait until no
snapshot or transaction predates it, and replication as the logical `DELETE`,
and is not designed here.

## 14. Observability

- **`pg_class`** lists the logical table as relkind `'r'`; its size and tuple
  counts are the running totals (§7.4). `'p'` would send psql and pg_dump to `pg_partitioned_table`,
  which `catalog_views.rs` lacks, and to children they cannot query.
- **`pg_inherits`** stays empty (`core/rql/catalog_views.rs:1122`).
- **`pg_index`, `pg_indexes`** list declared indexes from the shell's
  `schema.secondary_indexes`, not live `indexes` (§3.3), and omit the partition
  key.
- **Partition and parent files** start with `__`, so `is_internal`
  (`catalog_views.rs:1265`) hides them everywhere.
- **`SHOW TABLES`, `pg_stat_user_tables`**: running totals.
- **`SHOW PARTITIONS t`**, per partition: pid, `[lo, hi)`, location, state
  (unmaterialized, closed, open, fenced, retired), rows, bytes, dead stamps,
  writes per second, last use, pin, blocks shared by colliding tenants; plus map
  epoch, `route_v` and open-budget use.
- **Metrics:** partitions touched per statement, fan-out warnings, budget
  evictions, reopens within 10 s (thrash), p99 open latency, materializations
  per second, reaper deletions.

## 15. Phased plan

Each phase is safe alone and refuses what it does not support.

| Phase | Ships | Still refuses | Measures |
|---|---|---|---|
| **0** | Nesting-aware `CREATE TABLE` trailer scan, making `PARTITION BY`, `PARTITION OF`, `INHERITS`, `ATTACH`, `DETACH` explicit refusals; refusal of `__pt_` names; hard error on a `Wal::register_file` id collision | all partitioning | the three mis-parse shapes, each asserting no catalogue row is written |
| **1** | Pure, unwired modules: `partition::hash`, `RangeMap`, the map-log codec, `route(MaskExpr)` | — | route ns/op; I1, I2-pure, I3, I5 |
| **2a** | **The `table.rs` seam (§3.4), one reviewed change:** private data fields, the shell, `open_partition`, the range guard on the merged row, `_with_visibility` variants with a worker cap, the shared id source.<br>**The table:** `PartitionedTable`, the map, `CREATE … PARTITION BY HASH`, ≤ 2,048 declared partitions across tables (§7.1), root only, lazy materialization with exclusive creation, eager `Result` opens, existence checks (I9).<br>**Statements:** routing, single-snapshot fan-out for SELECT and COUNT, routed INSERT, UPDATE, DELETE. **Prepared SELECTs** skip the learned-parse cache (`note_rewritten`), using the text path.<br>**Also:** the uniqueness rule, catalogs with running totals, `SHOW PARTITIONS`, DROP, the reaper, the duplicate-name startup refusal, the name-keyed `Database` guards (§3.3). **Not advertised.** | everything else, incl. joins, ON CONFLICT and RETURNING on partitioned tables, and §2's phased refusals | on .7, ext4 not tmpfs: 100 concurrent tenants' DELETE/UPDATE mix vs one plain table (expect a large gain); INSERT and point SELECT (expect parity); concurrent same-row increments in one tenant, 40001s vs plain; fan-out latency over 1024 one-row partitions; first-write latency per new tenant; writer stall p99 during a checkpoint with 1024 active partitions; point-read routing cost (< 5 µs); idle wakeups at 1k open partitions. **2b proceeds only if the first shows the gain.** |
| **2b** | Joins, the prepared-SELECT cache with late routing (I11), ON CONFLICT and RETURNING on partitioned tables | as 2a, less these | prepared point-read cost vs plain |
| **3** | Lazy open, scan-only fan-out opens, idle close, sharded CLOCK, pins, `SWEPT` and `DIRTY`, `MaintenanceMode::External`, lazy `.tmp`, persisted `SERIAL` high-water, metadata DDL and RENAME (§11), the reader-preferring DDL lock, map-log generations, `fk_reverse`, FKs referencing partitioned tables, composite identifying keys (§1b) with FOR UPDATE, SKIP LOCKED, UPDATE FROM, DELETE USING; up to 65,536 partitions. **Advertised from here.** | as 2b, less these | fds, heap, wakeups and open cost per partition; startup at 10⁴ partitions with 100 active; time to first forget and to first `SERIAL` insert after restart at 10⁴; first-touch latency |
| **4** | Placement at materialize time across locations, weighted across tables; pool guards counting materialized partitions; touched-only `sync_durable`. **Gated on** the cross-session crash test and fix (§12). Databases without `s3_backup` only. | off-root placement under `s3_backup`; backup refuses to run beside off-root partitions (§12) | per-disk group-commit size and throughput at 1, 2, 4 disks |
| **4b** | S3 snapshots and log archive stream every pool location, with a multi-location restore test; phase-4 refusals lift | — | snapshot time vs location count |
| **5** | Reshape: MOVE, SPLIT, ISOLATE KEY, SPLIT KEY, MERGE, bounded-parallel REBALANCE, conversion; each with crash recovery, range-bounded re-routes, epoch-gated reclamation | — | fence duration vs partition size; throughput during a reshape |
| **6** | Row moves on key UPDATE, generational physical ALTER, co-location and partition-wise joins, `process_many` and `Folder` merge, `PARTITION OF` write-only alias | — | conversion time per GB |
| **7** | The §5 shared-point fixes that phase 2a's measurements single out: StatusLog staging, RowLocks fast path, WAL shards | — | commits/s vs cores and disks |

## 16. Invariants and the tests that pin them

Each listed mutation must fail its test; afterwards, grep the tree for
`MUTATED`. Crash tests are bounded, remove their temp directories, and are
judged on exit code, not a grep of the summary line. "Enforced by" names the
section specifying the mechanism.

| # | Invariant | Enforced by | Test (and the mutation it must catch) |
|---|---|---|---|
| I1 | The hash never changes within a `route_v` | §1.3 | reference and framed golden vectors per family and level |
| I2 | Insert and query agree on a value's point | §1.3 | property test through the real tokenizer, `insert_row_with_defaults` and `extract_gated`, for every spelling the evaluator calls equal: `42`, `'42'`, `42::bigint`, `$1` text and binary, the prepared render; INT = BIGINT. *(Mutation: hash the literal text.)* |
| I3 | Routing is a superset, in bounded memory | §4.2, §4.3 | random `WHERE` trees (AND, OR, IN, ranges, casts, functions, contradictions) over random rows: every brute-force-matching partition is routed; two and three 10k-value `IN` lists route in bounded allocation and time, and still as a superset. *(Mutations: intersect inside `And`; prune an `Or` with an unconstrained branch; accept casts; keep only the first box; truncate at the cap instead of degrading; cap after the product; clear the bitmap bit when vacuum finds no live rows.)* |
| I4 | No row outside its range; no update changes a point | §4.5 | an injected wrong route errors; a key-changing `UPDATE` with no other transaction running (in-place fast path eligible) returns 0A000, the in-place counter stays, I6 finds no stray row. *(Mutations: remove the guard; move it back onto the relocating branch only, which I6 must catch.)* |
| I5 | The map is total and disjoint | §1.1 | fuzz the map log, truncating or corrupting at every offset: the last valid epoch is recovered, or the table refused loudly |
| I6 | A partitioned table answers like a plain twin | everything | **twin-table model test**, extending the index model test: N = 7 and 1024, open budget 1, random reshapes between; random inserts, updates (in place, relocating, same-value key assignment), deletes, rollbacks, conflicts, ON CONFLICT, RETURNING, VACUUM, VACUUM FULL, IN, OR, `$n`, prepared runs; every read compared (counts, aggregates, ORDER BY LIMIT OFFSET, DISTINCT, windows, joins). Empty routes: `COUNT(*)`, `MAX`, other aggregates without `GROUP BY`, `RETURNING`, `UPDATE … RETURNING`, on a never-written tenant and on `WHERE tenant_id = 1 AND tenant_id = 2`. |
| I7 | One snapshot per multi-partition statement, taken before the map | §4.4, §8 | cross-tenant transfers beside a fan-out `SUM(balance)` that stays constant, across reshapes and in `REPEATABLE READ`; a fan-out `SELECT` at budget 1 with a slow lane, a reshape flipped and the reclaimer run mid-statement, returns the twin's rows. *(Mutations: a snapshot per partition; map read before snapshot; reclaim on the `Table`'s strong count.)* |
| I8 | The shell never serves data | §3.3 | a site catalogue, each statement correct or an error, never empty or a subset, the shell still in `Database.tables` after: DROP and ALTER `IF EXISTS`, `CREATE IF NOT EXISTS`, CREATE INDEX (reaching `reload_table`), VACUUM t, bare VACUUM (table in the report), LOCK, FOR UPDATE and SKIP LOCKED (refused until phase 3, then succeeding on a `PRIMARY KEY (tenant_id, asset_group_id, asset_id)` table), COPY, every ALTER verb, `STORAGE MOVE TABLE`, `STORAGE REBALANCE` and `DRY RUN`, `SHOW STORAGE`, every `pg_*` view (declared indexes in `pg_indexes`), FK checks, `seed_sequences`; no `<logical>*` file in root. *(Mutation: delete one routing call; that statement must error.)* |
| I9 | A partition opens only from its map location; missing files of a materialized partition, or the parent's ids or map file, are an error | §3.1, §3.5 | delete the files: a loud error; make the pool answer root: it still opens from its map entry. *(Mutation: open through `create_new` without the check.)* |
| I10 | Uniqueness holds | §1b | concurrent duplicate inserts and upserts across tenants and through reshapes; DDL refusals |
| I11 | A prepared statement never pins a partition | §4.1 | prepared SELECT, UPDATE, DELETE, 10 runs alternating tenants, past the learned third. *(Mutation: route before `note_plain`.)* |
| I12 | Row ids are unique across the logical table | §3.5 | merge two partitions: no repeated id; ids file missing: table refused |
| I13 | Close, reshape, restart and crash never resurrect an aborted row | §7.2, §12 | abort a write in p, close p, forget past it for several `FORGET_STEP`s, reopen: invisible; again across a restart and a reshape. Write in a new partition, SIGKILL before group commit with data pages forced out by a data-file-fsync failpoint; restart, force forgetting past the id, write the same tenant: the old row stays invisible. *(Mutations: unpin on close; all partitions clean on restart; `SWEPT` before durability; `DIRTY` group-commit only; reuse leftover files at materialize.)* |
| I14 | At most one live instance per file set | §7.2 | open/close storm at budget 1 under unique-key writers |
| I15 | The reaper never deletes live data | §12 | corrupt one map, restart: every other table's files intact |
| I16 | WAL registration is never silently wrong | §7.2 | a colliding registration errors; checkpoint after close and a crash, compare with the twin |
| I17 | No retry after a partial apply | §9 | a conflict in the second partition of a fan-out UPDATE, in autocommit and in BEGIN |
| I18 | Downgrade fails loudly, leaving no shadowing duplicate | §3.1 | the previous release opens a phase-2 database: table absent, not empty; `CREATE TABLE t` there, insert, re-upgrade: the new binary refuses to start, naming both rows |
| I19 | Every crash window leaks rather than loses | §12, §3.2 | failpoints at each step of create, materialize, rename, reshape, conversion and drop, under SIGKILL and the page-cache-drop harness, including a page-cache drop after the outcome is durable: rows equal before or after, never neither. *(Mutation: append `MATERIALIZE` with `append_data(_, false)`.)* |
| I20 | Cache scopes are distinct | §3.4 | one `WHERE` on two partitions with different rows. *(Mutation: scope from `schema.name`.)* |
| I21 | Placement is balanced and never reaches `db_storage.cfg` | §6.1, §3.1 | 1024 materializations over 3 locations: within ±1 (round-robin) or proportional (free-space); a location added mid-run gets its share of later ones, one below `min_free_bytes` none; `db_storage.cfg` unchanged |
| I22 | Reshape writes are neither lost nor applied twice | §6.2 | writers through `MOVE PARTITION`; a `MERGE` with a fan-out `UPDATE … SET n = n + 1` parked between its sources; a statement crossing a split then a merge; all vs the twin, row count and `RETURNING` included. *(Mutations: skip the re-route; re-route without the range bound.)* |
| I23 | Replicas route on their own and refuse alike | §12 | primary 1024, replica 64: identical results; primary 64, replica 1024 plus a `SPLIT KEY`, key-assigning `UPDATE`s (same value, same point, different point) under each conflict policy: identical rows, nothing skipped on one side only |
| I24 | FK actions reach partitioned children | §10 | a parent delete cascades into every partition holding children |
| I25 | A rename never loses a partitioned table | §3.1 | rename, restart: rows intact, nothing reaped; crash at each step, restart. *(Mutation: `clusters = 0` on rename.)* |
| I26 | DDL never hangs the table | §11 | a `REPEATABLE READ` transaction holding `FOR UPDATE`, a second waiting on it, then an `ALTER`: it completes or fails with `55P03`, both transactions finish; a DDL storm under unique-key writers |
| I27 | A backup never silently omits a partition | §12 | off-root partitions, then `s3_backup`: the service refuses, no snapshot succeeds with a subset |

## 17. Risks and open questions

1. **The seam collides with work in progress** (§3.4).
2. **The downgrade guard rests on today's code**, read at `61e9200` (I18).
3. **The fence grows with the source's pointer file**: the repeated delta
   (§6.2, step 4), never the MVCC mover.
4. **Forgetting pauses after a restart** until partitions dirty at the crash
   are swept (§7.2).
5. **Shared CPU points may cap the gain** at high client counts (§5); phase 2a
   measures before anything promises more.
6. **The static uniqueness rule** will surprise `PRIMARY KEY (tenant_id, id)`
   under a two-column key. A possible opt-in, `WITH (split_floor = 1)`, would
   accept a unique key on a leading prefix, refuse a boundary inside such a
   prefix on every boundary-creating path, take the minimum across a
   co-location group, and need its own tests.
7. **Unrouted fan-out at 10⁴–10⁵ partitions** stays costly even with skipping.
   Open: does a `max_fanout` refusal beat a slow success? Proposed: a warning
   plus a setting.
8. **Index memory** is rebuilt on every reopen; if that churns, use a longer
   idle window or persisted indexes.
9. **`pg_dump`** emits a plain table, relkind being `'r'` (§14); a re-import
   loses partitioning unless the importer is told.
10. **Out of scope:** a physical tenant drop (§13), and global uniqueness on a
    non-key column, which would need a hidden table partitioned by it and a
    two-partition write protocol.
11. **One map per co-location group** couples members' reshape costs; separate
    maps with enforced equal boundaries would not, for more code.
12. **Checkpoints grow with active partitions** and can stall a location's
    writers for seconds (§5).
13. **Row-addressing statements wait on composite identifying keys**, an
    every-table change (§1b); ORMs and job queues use them, so phase 3 cannot
    be advertised without it.
14. **Replica apply skips a transient `53xxx`** under `Skip` and
    `LastWriterWins` until `replication/server.rs` retries it (§12).
