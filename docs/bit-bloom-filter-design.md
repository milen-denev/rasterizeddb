# Bit-Level Adaptive Filter Index — Design & Implementation Plan

Status: **implemented, integrated, persistent, runtime-tunable, and covering equality *and* range predicates** (`rasterizeddb_core::core::filter`). Enabled by default. L0/L1 tiers not built — see §0.
Target: `rasterizeddb_core`, edition 2024, baseline `target-cpu=x86-64-v3` (AVX2 + BMI1/BMI2 guaranteed at compile time; AVX-512 runtime-detected).

## 0. What exists

| Component | State |
|---|---|
| `filter/hash.rs` — canonical hashing | **Done.** Agreement with `perform_comparison_operation` asserted per type |
| `filter/kernels.rs` — bit-sliced planes, AVX-512 / AVX2 / scalar | **Done.** SIMD paths differentially tested against the scalar oracle |
| `filter/column.rs` — generations, adaptive `f`, watermark, HLL | **Done** |
| `filter/persist.rs` — sidecar checkpoints, validation, dirty protocol | **Done** |
| `filter/zonemap.rs` — per-zone min/max bounds | **Done.** Covers `<`/`>`/`<=`/`>=` at 0.25 bits/row, numeric types exactly and `STRING` by truncated prefix (§4.7) |
| `filter/bloom.rs` — per-superzone span summary (L0/L1) | **Done.** Off by default; 0.25 bits/row, 2.5× on absent-value probes for low-cardinality spans and 0.8× when saturated (§11.1) |
| `filter/sarg.rs` — predicate extraction | **Done.** Recursive-descent over `AND`/`OR`/parens at SQL precedence; equality, ranges, `IN`, `BETWEEN`, `STARTSWITH` lowered to a half-open range; literals coerced to the column's type |
| `filter/mod.rs` — `TableFilters`, candidate bitmaps | **Done** |
| `filter/settings.rs` — layered runtime settings | **Done.** Global → table → column, partial overrides |
| `filter/sql.rs` — `SET`/`SHOW`/`RESET FILTER` | **Done.** 11 keys, three scopes (§16) |
| CLI (`--filter-*`) | **Done.** Nine flags seeding the same runtime state |
| Scan integration (`RowPointerIterator`, `Table`) | **Done.** Insert/update/delete maintenance, mask on read, whole-word skip |
| Persistence lifecycle | **Done.** Load at open, delta backfill, amortised auto-checkpoint |
| Maintenance service | **Done.** One background task per table owns backfill and checkpointing; no caller ever pays for either |
| Tests | **384** — 98 in-crate + **286 in `rasterizeddb_filter_tests`**, an external black-box suite across 20 files; green in both the default and `enable_long_row` configurations, and under CPU load at `--test-threads=32` |
| Benchmarks | `benches/filter_benchmarks.rs`, numbers in §11.1 |

**Defaults: everything on.** `FILTER_ENABLED = true`, `FILTER_BACKFILL_ON_OPEN = true`. A table opens by loading its sidecar and delta-scanning only what the checkpoint does not cover; with a valid checkpoint that is near zero work. `FILTER_ENABLED = false` still yields byte-identical results, which the differential tests assert.

**Not built:**

- **A separate whole-table L0.** The span summary (§4.3) serves both roles: "absent from the table" is "absent from every superzone", at one cache line per 16 384 rows. A second tier above it would be another 32× reduction in bytes streamed and is only worth considering past a billion rows.
- **A staged insert queue.** Concurrent single-row inserts still take one lock per column per row. Batched paths do not (§8), but there is no batch to join for a row arriving alone on its own connection.

**Resolved since first draft:**

- ~~`IS NULL`.~~ Resolved, and not the way this document anticipated. Nullability landed as a **companion `BOOL` column** per nullable column (`core/rql/nullable.rs`), with `predicate_rewrite` desugaring `x IS NULL` to `__null_x = 1` before the tokenizer sees it. So the filter never meets a unary operator, and the `CODE_NULL` encoding slot stays unused. `BOOL` is now indexed — for **zone bounds only**, never fingerprints (`hash::is_fingerprintable`) — which makes a sparsely-null column prune almost perfectly at 0.03 bits/row. The `IsNotNull` trap this document warned about **does not arise**: `IS NOT NULL` desugars to `__null_x = 0`, an ordinary equality, and equality masks are supersets in both directions. Nothing here negates anything, which is the only thing that made `!=` unsafe.
- ~~Prefix filtering for `STARTSWITH` / `LIKE 'abc%'`.~~ Built as a **range lowering**, not as the second fingerprint plane §13.2 proposed. `sarg::starts_with_to_range` rewrites `col STARTSWITH 'abc'` to `col >= 'abc' AND col < 'abd'`, which is exact rather than approximate and needs no extra storage at all. See §13.2 for why the reserved-plane design does not work.
- ~~String zone bounds.~~ Built, on the back of the above. `ZoneMap` now carries two bound shapes (§4.7): exact values for fixed-width types, and a length-prefixed 16-byte truncation for `STRING` whose `max` bound rounds **up** with carry.
- ~~Incremental sidecar writes.~~ Built. The sidecar is a base image followed by an append log (§4.4); a checkpoint describes only the zones written since the last one and falls back to rewriting the base when the delta would span the file anyway. A torn tail replays to the state before it.
- ~~Lock-free open zone.~~ Resolved by **not** building it. Measurement says the §8 atomic-plane shape is the wrong answer for this contention, and batching is the right one — see §8.
- ~~A checkpoint could claim to cover rows nothing had indexed.~~ `unknown_below` was not persisted and `restore` assumed zero, so a checkpoint taken while an un-indexed prefix existed came back claiming those rows were indexed and pruned them. Reachable whenever a checkpoint lands before the backfill finishes, and permanently with `backfill_on_open = off`. The prefix is now part of the column record; `a_checkpoint_remembers_its_unindexed_prefix` pins it.
- ~~Parenthesised predicates.~~ The evaluator flattened boolean structure and ignored grouping, so `A AND (B OR C)` evaluated as `(A AND B) OR C`. Found by the disabled-filter differential; fixed elsewhere by compiling a postfix program that reconstructs the bracketing. Extraction is re-widened and `a = 1 AND b IN (…)` filters again. The lesson is recorded in §9.3: an earlier revision of this document claimed the engine handled parens correctly, based on four shapes that all used **two-element** OR groups — which pass under flattening by coincidence. A lucky pass reported as a measurement.
- ~~MVCC update-in-place.~~ Resolved, though not the way §15.8 anticipated. See §4.10: under versioning a delete no longer clears fingerprints, because the filter prunes *before* visibility is consulted; reclamation clears them once nothing can reach the version.

**Known-good properties, by test:** no false negatives under random data, out-of-order inserts, deletes, updates, generation boundaries, un-backfilled prefixes, checkpoint round-trips and reopens; identical results with and without filtering for `OR`, `!=`, ranges and parens; every sidecar rejection path (bad magic, version, geometry, record length, seed, dirty, truncation at eight offsets, schema add/remove/retype) falls back to a rebuild; measured FPP within 2× of theory.

---

## 0.1 Tuning

Three surfaces, one runtime state — a CLI flag is the starting value, not a
separate mechanism.

```sql
SET FILTER max_f = 16                          -- global
SET FILTER max_f = 12 FOR TABLE users          -- one table
SET FILTER max_f = 24 FOR COLUMN users.email   -- one column
SET FILTER zone_bounds = off FOR COLUMN users.uuid
SET FILTER indexed  = off FOR TABLE audit_log

SHOW FILTER [FOR TABLE t | FOR COLUMN t.c]     -- effective values at that scope
SHOW FILTER KEYS
RESET FILTER [FOR TABLE t | FOR COLUMN t.c]    -- unwind one layer
```

Scopes layer, most specific winning, and every override is **partial** — a
column names only the knob it changes and inherits the rest. That is what makes
per-column tuning practical: a table usually wants one column widened, not a
whole config restated.

Two keys exist only because per-column control is the point:

- **`indexed = off`** drops a column (or a whole table) from the index.
- **`zone_bounds = off`** keeps fingerprints and drops range bounds. The right
  trade for a column with no locality — random UUIDs, hashes — where every
  zone's bounds span the space and prune nothing, so the two values per zone are
  pure overhead. See §11.1: UUIDv4 gets *no* range pruning at any setting.
- **`bloom = on`** adds the per-superzone span summary (§4.3). Off everywhere by
  default, because it reduces *bytes streamed* and not *rows fetched*, and
  because whether it pays depends entirely on the column: measured at 2.5× on
  absent-value probes for a few hundred distinct values per 16 384-row span, and
  **0.8×** — a loss — when every row is distinct and the block saturates.

**Tuning is node-local and does not replicate.** `SET FILTER` parses to
`QueryPurpose::FilterControl` and returns before the replication capture hook, so
an operator has to run it on every node; applying it on one and expecting the
cluster to follow will silently leave the others on defaults. That is deliberate
rather than an oversight — the same argument as `VACUUM`. These are per-node
performance knobs, and nodes in a cluster legitimately differ in memory and read
pattern, so forcing one node's `max_f` onto all of them would be worse than
leaving it local. DDL *does* replicate, so a `CREATE TABLE` reaches every node
and each builds its own sidecar from its own settings.

A change applies to filters built afterwards, and to widths chosen at the next
zone seal. Existing generations keep the width they were sealed with — the same
way growth already works — so tuning a large table takes effect from that point
rather than rewriting what exists. `Table::backfill_filters()` applies it
retroactively.

### The selectivity gate has teeth

`choose_f` returns 0 for a column whose predicates would match too much of the
table to be worth narrowing. That verdict used to be **computed and discarded**:
the column kept its previous width and carried on probing, so `min_selectivity`
was a setting with no effect. A gated-out column now stops being probed.
Maintenance continues, so the decision reverses when cardinality grows; freeing
the planes would save the storage too, but re-enabling would then need a
backfill.

---

## 1. Goals and non-goals

**Goals**

| # | Goal | Success criterion |
|---|------|-------------------|
| G1 | Eliminate row fetches for equality predicates | `SELECT … WHERE col = v` touches O(matches) rows instead of O(table) |
| G2 | Bit granularity, never byte granularity | Every per-row cost is a tunable number of *bits*, `f ∈ [3,16]`, not rounded up to a byte |
| G3 | Scale up and down with table size | No global rebuild on growth; storage and RAM shrink when rows are deleted or a column goes cold |
| G4 | SIMD-first probe | Probe kernel is bandwidth-bound, not instruction-bound, on both AVX2 and AVX-512 |
| G5 | Minimal overhead | 1–2 bytes/row/indexed column on disk (adaptive, §7.3), near-zero resident RAM, zero writes on the row-insert hot path; columns that cannot benefit cost **0** |
| G6 | No false negatives, ever | A row that matches is *never* pruned, under concurrency, crash, or partial rebuild |

**Non-goals**

- `CONTAINS` / `ENDSWITH`. Unsupportable by any hash filter, and unsupportable by an ordered bound either — neither is bracketed by any prefix of the value. `STARTSWITH` **is** supportable and is built, as a range lowering rather than as a hash (§13.2).
- ~~Ordered bounds for `STRING`.~~ Built (§4.7). This entry read "the engine implements only `>=` for strings"; `Less`, `Greater` and `LessOrEquals` landed since, and the remaining obstacle was structural rather than a capability — `ZoneMap` had one stride and range probes gated on exact length equality against the probe value. Both changed. The bounds are truncated to 16 bytes rather than exact, which is what keeps the array strided.

**Indexed types:** everything the evaluator can compare — all integers, `F32`/`F64`, `STRING`, `DATETIME`, `CHAR`, `DECIMAL`, `UUID`, `INET` and `BOOL`. Only `BYTEA` and `ARRAY` are left out, and by capability rather than policy: `perform_comparison_operation` has no arm for them, so indexing them would store values no query could ever match.

**Fingerprinted types** are that set minus `BOOL`. Two distinct values can never clear the selectivity gate (§7.3), so `f` bits per row would buy a mask that hands back half the table — but two *bytes per zone* is a fiftieth of that and rules out a whole zone for a rarely-set flag, which is what makes `IS NULL` prune (§4.7). All of `UUID`, `INET`, `STRING` and `BOOL` carry zone bounds; `memcmp` over the fixed octet layouts of the first two is a total order, and the last two are covered in §4.7.

> **UUID byte order matters more than the type choice.** Canonical UUID octets are big-endian; a `U128` column is read little-endian. Measured over 1 M keys: UUIDv7 written as a straight byte copy prunes *nothing* on ranges (1,000,000 of 1,000,000 survive) and needs 25 candidates for a point lookup, because the timestamp lands in the least significant bytes. The same keys byte-reversed give **1 candidate** for a point lookup and **10.04 %** survivors on a range. The native `UUID` type stores network order and compares by `memcmp`, so it gets the good case by construction. UUIDv4 gets no range benefit either way — every zone's bounds span the space.
- Replacing the scan. This is a *pruning* layer that sits in front of the existing scan, not a B-tree.

---

## 2. Where this plugs into the current engine

Today's read path, per [concurrent_processor.rs:241-500](rasterizeddb_core/src/core/processor/concurrent_processor.rs#L241-L500):

```
RowPointerIterator ──▶ Vec<RowPointer> (BATCH_SIZE = 10K)
       │                      │
       │ sequential read      └─▶ kanal MPMC ──▶ 16 workers
       │ of *_pointers.db                          │
       │ 21 B/row                                  ├─ fetch_row_reuse_async  ◀── random I/O into *.db
       └───────────────────────────────────────────┴─ TransformerProcessor::execute  (WHERE eval)
```

Two costs dominate a filtered scan:

1. **Sequential** read of `{table}_pointers.db` — 21 B/row ([row_pointer.rs:42-48](rasterizeddb_core/src/core/row/row_pointer.rs#L42-L48)).
2. **Random** `fetch_row_reuse_async` per surviving pointer — the real killer, one per row.

The critical structural fact this design exploits:

> **`{table}_pointers.db` is a dense, fixed-stride array.** Slot index `i = byte_offset / ROW_POINTER_RECORD_LEN`. There is no indirection between "row number" and "position in file".

That means a filter can be a **positional shadow** of the pointer file — bit `i` of the filter describes slot `i` of the pointer file. No key→slot map, no indirection, no per-entry pointer. This is what makes the whole thing cost *bits* instead of bytes, and it is why the design below beats a textbook Bloom filter.

New path:

```
                 ┌── L0 table filter (one cache line)  ── "value absent from table" → return []
FilterCatalog ───┤
                 └── L2 fingerprint planes ── SIMD ──▶ 512-bit candidate mask per zone
                                                            │
RowPointerIterator::next_row_pointers_masked ◀──────────────┘
       │  decodes ONLY masked slots, read_vectored for scattered pointer pages
       └──▶ workers ──▶ fetch_row_reuse_async (survivors only) ──▶ WHERE eval (unchanged)
```

The WHERE evaluator is **not** modified. The filter is conservative; the transformer still decides truth. That keeps the blast radius small and makes correctness testing easy (§12).

Aggregates and joins inherit this for free. `COUNT`/`SUM`/`MIN`/`MAX`/`AVG` take the row-mode path through `Table::query_row`, and a joined `SELECT` scans each relation separately through the same call with its own share of the `WHERE` pushed down — both are where the mask is attached. So `SELECT COUNT(*) … WHERE col IN (…)` and `… JOIN … WHERE d.name = 'Acme'` are both pruned before any row is fetched. Because the mask is a superset and the evaluator still decides per row, the fold and the hash join see exactly the rows they would have seen unfiltered.

---

## 3. The core decision: two structures, one hierarchy

A classic Bloom filter is the right tool when you *don't know which slot* a key lives in — you must hash into a shared bit array. But we do know: every row has a dense slot index. Once slot identity is free, a **bit-sliced fingerprint** strictly dominates Bloom on both axes.

Per-row cost vs. false-positive probability:

| bits / row | Split-block Bloom (Impala/Parquet style) | Bit-sliced fingerprint (this design) |
|---|---|---|
| 4 | ~30 % | **7.1 %**  (1/14) |
| 6 | ~7 % | **1.6 %**  (1/62) |
| 8 | ~3.1 % | **0.39 %** (1/254) |
| 10 | ~1.6 % | **0.098 %** (1/1022) |
| 12 | ~0.93 % | **0.024 %** (1/4094) |
| 16 | ~0.39 % | **0.0015 %** (1/65534) |

Fingerprints are ~40× better at 8 bits, **and** they return *which rows* are candidates rather than "somewhere in this group". A Bloom filter at the row tier would be strictly wasted bits.

So the design uses each structure where it is actually optimal:

| Tier | Structure | Granularity | Answers | Probe cost |
|------|-----------|-------------|---------|------------|
| **L0** | Split-block Bloom (register-blocked, bit-level) | whole table | "value absent from table" | **1 cache line**, O(1) |
| **L1** *(optional)* | Split-block Bloom | superzone = 32 zones = 16 384 rows | "absent from this 16K-row span" | 1 cache line per superzone |
| **L2** | Bit-sliced fingerprint planes | zone = 512 rows, per-row | 512-bit candidate mask | f cache lines per zone |

Bloom is used exactly where slot identity does not exist (L0/L1 summarise *sets*, not positions), fingerprints where it does (L2). Both are pure bit structures; there is no byte-per-slot anywhere in the format.

**Default configuration is L0 + L2 only.**

The critical thing to understand about the hierarchy, and the easiest thing to get wrong: **L0/L1 reduce how many filter bytes you must *stream*; only `f` reduces how many rows you falsely *fetch*.** Total false row fetches is `rows × 2^-f`, full stop — adding a Bloom tier above L2 does not improve it, because within any span that survives L1 the L2 fingerprints alone decide. L1 is therefore opt-in per column (§7.4): it converts a 4 ms bandwidth-bound plane sweep into a ~40 µs pointer-chase, which is worth real money only on very large tables under heavy point-lookup traffic. It buys latency, not accuracy.

---

## 4. Data structures and bit layouts

### 4.1 Geometry constants

```rust
pub const ZONE_ROWS: usize = 512;          // one plane == exactly one 64-byte cache line
pub const ZONE_PLANE_BYTES: usize = 64;
pub const SUPERZONE_ZONES: usize = 32;     // 16 384 rows
pub const L0_BLOCK_BITS: usize = 512;      // one cache line per Bloom block
```

`ZONE_ROWS = 512` is load-bearing: a plane is *exactly* one cache line and *exactly* one AVX-512 ZMM register. A zone's whole L2 for `f = 8` is 512 bytes — 8 cache lines, guaranteed inside one 4 KiB page.

### 4.2 L2 — bit-sliced fingerprint planes

For a zone of 512 rows and fingerprint width `f`, store **`f` bitplanes of 512 bits each**, plane-major:

```
zone L2 layout (f = 8, 512 bytes):

           row slot →  0    1    2   ...  511
plane 0 (64 B):        g0₀  g0₁  g0₂ ...  g0₅₁₁     ← bit 0 of every row's fingerprint
plane 1 (64 B):        g1₀  g1₁  g1₂ ...  g1₅₁₁
  ⋮
plane 7 (64 B):        g7₀  g7₁  g7₂ ...  g7₅₁₁
```

Row `i`'s fingerprint is **not contiguous** — it is one bit taken from each of `f` different cache lines. That is the entire trick: it lets a single SIMD register test bit `j` of 512 rows simultaneously, and it makes `f = 5` or `f = 11` cost exactly 5 or 11 bits per row with no padding.

Reserved codes (fingerprint is never 0 or 1 for a real value):

| code | meaning |
|------|---------|
| `0` | empty / deleted / not-yet-indexed slot — matches nothing |
| `1` | Reserved for SQL `NULL`. **Unused:** nullability is a companion column and never reaches the fingerprints (§0) |
| `2 … 2^f − 1` | real value fingerprints |

Code 0 falling out of an all-zero region means **an unwritten or zeroed region is automatically "matches nothing"**, and a deleted row is erased by clearing `f` bits. Both are free correctness properties.

### 4.3 L0/L1 — split-block Bloom, one summary per superzone

Built as **one structure serving both tiers** (`filter/bloom.rs`), because "absent from the table" is just "absent from every superzone".

- **Superzone** = 32 zones = 16 384 rows.
- **Block** = 4096 bits (512 bytes) per superzone = **0.25 bits/row**, split into 16 × 256-bit sub-blocks of 8 × `u32` lanes.
- A probe picks one sub-block from the hash's high half and tests one bit per lane, keyed from the low half. Eight word tests, one cache line, no plane read for a superzone it rules out.
- The Bloom hash is `mix(h ^ φ)` rather than a half of `h` directly, so the sub-block choice cannot share structure with `fingerprint`, which takes the high half.

**Why a fixed block size and not one sized from cardinality.** This is the arithmetic that decides whether a Bloom tier is worth anything at all, and it is easy to get backwards. A Bloom costs ~5–10 bits per *distinct value*; the planes cost `f` bits per *row*. Streaming the summary beats streaming the planes only when `10 × distinct/rows < f` — for `f = 8`, when fewer than about four rows in five hold a distinct value. **Sizing each block from its span's cardinality would make the structure track that ratio and grow until it cost as much as what it was meant to avoid reading.**

A fixed 512 bytes per superzone inverts the trade so it can only be cheap:

| Distinct values per span | Block state | Effect |
|---|---|---|
| a few hundred | sparse | rules out ~99 % of superzones; **2.5× faster** absent-value probe |
| 16 384 (all distinct) | saturated | answers "maybe" to everything; **0.8×** — a 19 % loss |

The failure mode is a small constant overhead, not an index that quietly doubles. Both numbers are measured — §11.1, `report_span_summary`.

**Deletes are safe by construction.** A Bloom cannot forget, so a deleted row leaves its key behind and the summary gets *more* conservative, never less — the same asymmetry the zone bounds rely on.

### 4.4 On-disk sidecar: `{table}_flt.db`

Written through the existing `StorageIO` trait ([traits.rs](rasterizeddb_core/src/core/storage_providers/traits.rs)), so it inherits the mmap-backed `file_sync` provider for free.

```
┌──────────────────────────────────────────────────────────────┐
│ Superblock (4 KiB, page aligned)                             │
│   magic "RDBFLT01" | format_ver u16 | zone_rows u16          │
│   hash_seed[4] u64        ← PERSISTED. see §5.1              │
│   pointer_record_len u8   ← guards against enable_long_row   │
│   column_count u16 | catalog_offset u64 | catalog_len u32    │
├──────────────────────────────────────────────────────────────┤
│ Column directory (one 64 B entry per indexed column)         │
│   write_order u32 | db_type u8 | f u8 | flags u8             │
│   valid_from_slot u64   ← for ALTER-added columns, §7.5      │
│   valid_upto_slot  u64  ← the no-false-negative watermark    │
│   l2_offset u64 | l1_offset u64 (0 = absent)                 │
│   distinct_est u64 (HLL) | probe_counter u64 (adaptivity)    │
├──────────────────────────────────────────────────────────────┤
│ Column 0 · L2 planes   [zone 0 ‖ zone 1 ‖ …]  ← contiguous   │
│ Column 0 · L1 blocks   (optional)                            │
│ Column 0 · L0 block    (512 B incl. header)                  │
├──────────────────────────────────────────────────────────────┤
│ Column 1 · …                                                 │
└──────────────────────────────────────────────────────────────┘
```

**Column-partitioned, not row-partitioned.** A scan filtering on one column reads one perfectly sequential extent. This is the single most important layout decision for bandwidth.

**Base image plus an append log.** The superblock carries `base_len`; everything past it is a sequence of delta records, each `"RDLD" | ver | payload_len | payload`. A delta re-sends every zone from the lowest slot written since the last checkpoint, so an append-only table pays for the writes rather than for the table. A checkpoint rewrites the base instead when there is nothing to append to, when the superblock's `dirty` byte has to be cleared (an append cannot reach it), when the chain reaches 64 records, or when the delta would already be half the file — which a `DELETE` hits by construction, since it invalidates slots near the start.

Three details that are load-bearing:

- **The length is tracked in memory, never asked of the provider.** `LocalStorageProvider` keeps its length in an atomic that only `append_data` updates, so a file created or extended by `write_data` reports length 0 in-process until reopened (§15.15). Every byte of this file was written from `TableFilters`, so counting them there is both cheaper and the only answer that is right.
- **Every write ends with 16 zero bytes** that are written but not counted. A base image can be *shorter* than the file it replaces — a delta re-sends a whole HLL and validity vector per column, so a long chain can outrun the growth of the planes it describes — and whatever is left beyond the new end must not parse as a record. The next write overwrites the stopper.
- **A torn tail is a non-event.** Replay stops at the first record that does not parse, and base-plus-a-prefix is exactly the checkpoint that existed at an earlier moment: rows above its frontier are candidates and get backfilled. That is the same position an ordinary checkpoint leaves us in when rows were appended after it, so there is one recovery path rather than two.

### 4.5 In-memory catalog

```rust
pub struct FilterCatalog {
    columns: SmallVec<[ColumnFilter; 8]>,
    seed: HashSeed,
}

pub struct ColumnFilter {
    write_order: u32,
    db_type: DbType,
    f: u8,                        // fingerprint bits, 3..=16
    l2: MmapSegment,              // borrowed from StorageIO, NOT heap-copied
    l1: Option<MmapSegment>,
    l0: Option<Block512>,         // 64 B, worth keeping resident
    valid_from_slot: u64,
    valid_upto_slot: AtomicU64,   // Release-stored; see §8
    open_zone: OpenZone,          // the only mutable part
}

/// Tail zone, RAM-only until sealed. 16 × f AtomicU64 = 512 B at f = 8.
pub struct OpenZone {
    planes: [AtomicU64; 8 * 16],  // f planes × 8 u64 per 512-bit plane
    base_slot: u64,
    hll: [u8; 64],                // 64-register HLL, ~13 % stderr — sizes the NEXT zone
}
```

Note `l2: MmapSegment`. Filters are **never copied to the heap.** Resident RAM is whatever the OS page cache decides to keep, which is exactly the right policy and requires zero eviction code from us.

Publication uses `arc-swap` (already a dependency): `ArcSwap<FilterCatalog>`. Sealed zones are immutable, so readers take no locks at all.

### 4.8 The maintenance service

Filter *maintenance* — the bit writes on insert, update and delete — is inline
and unavoidable: it is a handful of writes under a per-column lock, and the data
has to be indexed as it arrives. Everything else runs on a **background task,
one per table**, spawned by `Table::new`:

- **Initial backfill.** `Table::new` returns immediately and the task indexes
  whatever the checkpoint does not already cover. Until it finishes, the filter
  reports an unindexed prefix, so queries are correct — just not yet narrowed.
  `Table::wait_filters_ready()` awaits it when a caller needs determinism.
- **Checkpointing.** The task wakes every `FILTER_CHECKPOINT_INTERVAL_SECS`
  (default 10) and writes a checkpoint if enough has changed or a mutation
  requested one. Doing this inline meant one unlucky insert paid the full blob
  rewrite — ~325 µs on a million rows (§11.1) — and a one-row `DELETE` paid for
  the entire index.

`Table` implements `Drop` to signal the task; without it a per-table task would
outlive its storage and keep ticking against files that are going away.

Deletes and updates therefore only *request* a checkpoint. If the process exits
before the task honours the request, the sidecar is left dirty and is discarded
on the next open — costing a rebuild scan, never an answer.

---

### 4.9 Three ways the positional shadow can be invalidated

The index is a positional shadow of the pointer file — bit `i` describes slot
`i` — so anything that changes *which row lives in which slot* invalidates it
wholesale. A stale positional index prunes matching rows **silently**, so each
path needs an explicit answer:

1. **Compaction** moves live records down over tombstones. `Table::compact`
   marks the sidecar dirty *before* anything moves, then rebuilds the in-memory
   catalog and re-checkpoints after truncating — in that order, so a crash
   anywhere leaves a sidecar that is rejected rather than one indexing rows by
   addresses they no longer have. Covered by `tests/compaction.rs`.
2. **A table rewrite** (`ALTER TABLE … ADD COLUMN` drops and recreates the
   files, re-inserting only live rows). The schema check catches this whenever
   the *indexable* columns change — but adding a column the filter does not
   index (`BOOL`, `BYTEA`, `ARRAY`) leaves them identical, and file removal on
   drop is best-effort. So loading also rejects a checkpoint claiming **more
   slots than the pointer file holds** (`LoadReject::StaleSlotCount`). The
   invariant: a pointer file only grows between checkpoints, because deletes
   tombstone in place and compaction re-checkpoints after truncating.
3. **A same-length rewrite** that changes which row occupies which slot would
   defeat both checks. No such path exists today; one would need an explicit
   invalidation call.

### 4.10 Deletes under MVCC

On an unversioned table a delete clears the slot's fingerprints immediately, and
the false-positive rate *improves*.

Under versioning it must not. The filter prunes **before visibility is
consulted**, so a cleared fingerprint does not make a version invisible — it
makes it *unreachable*. The scan never decodes the pointer, the visibility rule
never runs, and an older snapshot loses a row it is entitled to. Reclamation
clears the fingerprints instead, once nothing can reach the version.

The consequence, stated plainly: **on a versioned table a delete no longer
improves the false-positive rate.** Between delete and reclaim those slots are
extra candidates the evaluator rejects on visibility. Correct, slightly less
selective, self-correcting.

---

### 4.7 Zone min/max — the other half of the index

> **Two bound shapes.** A fixed-width type stores its bounds exactly: two values per zone, and the bound *is* the value. `STRING` cannot, because the array has to have a stride, so it keeps the first 16 bytes of each and accepts looser bounds — `2 × (1 + 16)` bytes per zone, **0.53 bits/row**. A length byte accompanies each bound because zero-padding would change what it compares as: `"ab"` is *less* than `"ab\0\0"` under the engine's ordering, which breaks a `min` bound in the unsafe direction.
>
> **Truncation is asymmetric, and this is the one bug here that returns wrong answers rather than merely pruning badly.** Cutting a string short makes it lexicographically *smaller*. For `min` that moves the bound down and widens the range — safe. For `max` it moves the bound down too, which *narrows* the range, and a range too narrow prunes rows that match. So `max` rounds **up**: increment the truncated prefix as a big-endian integer with carry, and mark the zone unbounded above when the prefix is every byte `0xFF` and nothing above it can be named. Note this runs opposite to the "bounds only ever widen" invariant the rest of the module is built on.
>
> **`BOOL` is here and nowhere else.** It carries no fingerprints — two distinct values can never clear the selectivity gate — but two bytes per 512-row zone is **0.03 bits/row** and rules out a whole zone for a flag that is rarely set. That case arrives on every nullable column, since `x IS NULL` lowers to `__null_x = 1`.


A fingerprint answers "might this row hold exactly `v`?" and says nothing about
`>` or `<`. Two bounds per 512-row zone do, at **0.25 bits/row for a `u64`
column** — a thirty-second of what an `f = 8` fingerprint costs.

Three properties make it safe:

- **Bounds only ever widen.** A delete cannot shrink a zone's range, because we
  would have to know whether the removed value was the sole occupant of a bound.
  An update widens if the new value falls outside. Both leave the range
  conservatively wide — pruning less, never wrongly — and a rebuild tightens it.
  This is the one place the design behaves like a Bloom filter rather than like
  the exact fingerprints.
- **NaN never enters the bounds.** Every IEEE comparison against NaN is false, so
  a NaN that seeded `min`/`max` would stick there permanently — nothing could
  displace it — and every range test on that zone would then answer "cannot
  match" and prune rows that do. Excluding NaN is also free in the other
  direction: a NaN row can never satisfy a range predicate anyway.
- **Comparisons come from the evaluator.** Every bound test calls
  `perform_comparison_operation`, the same function `WHERE` uses. There is
  deliberately no second comparison implementation to drift out of agreement —
  the discipline §5.2 applies to equality, applied to ordering.

Equality consults **both** structures: bounds reject whole zones outside the
range, fingerprints then cut within the surviving ones. They prune
independently, so intersecting is strictly better than either alone.

---

### 4.6 Checkpoint staleness and the dirty byte

The sidecar is derived data, so it never needs `fsync` and any unreadable file is
simply discarded. The subtle failure is not corruption but **staleness**.

A checkpoint records each column's `valid_upto` frontier: slots below it were
correctly indexed *at that moment*. Rows appended afterwards are harmless — they
sit above the frontier and reload as candidates. Rows **updated or deleted**
afterwards are not: the checkpoint still holds the old fingerprint, so a query
for the row's new value finds no match at that slot and prunes a row that does
match. That is a false negative, the one outcome this design must never produce.

Two mechanisms close it, and both are needed:

1. **In memory, `dirty_min`.** Overwriting a slot the frontier already covers
   drags the *persistable* frontier back to that slot, so a checkpoint never
   claims to cover a region it might describe wrongly. Backfill writes use
   `set_reindex` and deliberately skip this — a backfill restores known-good
   state, and treating it as a modification would pull `dirty_min` to 0 and make
   every later checkpoint claim to cover nothing.
2. **On disk, a `dirty` byte** at a fixed superblock offset. `Table` sets it
   before any delete or update and a checkpoint clears it. A file loaded with
   `dirty != 0` is rejected wholesale. This costs one byte written per
   delete/update *statement* — not per row, and never on the insert path — and
   makes staleness impossible across a crash rather than merely unlikely.

Everything else the loader checks would make the stored bytes describe something
other than the current table, and each is a hard rejection: magic, format
version, `ZONE_ROWS`, `ROW_POINTER_RECORD_LEN` (i.e. the `enable_long_row`
feature), hash seed, column set and column types. A rejected sidecar costs one
rebuild scan; it can never cost an answer.

---

## 5. Hashing and fingerprinting

### 5.1 The stable-seed requirement

The seed **must be persisted in the superblock and reused across restarts.** `ahash::RandomState::new()` is randomised per process; using it would silently invalidate every persisted fingerprint on restart and produce false negatives — the one failure mode we cannot tolerate. Use `RandomState::with_seeds(s0,s1,s2,s3)` with the four `u64` from the superblock.

### 5.2 Canonicalisation invariant

`perform_comparison_operation` ([transformer.rs:191](rasterizeddb_core/src/core/processor/transformer.rs#L191)) is dispatched on a single `column_type`, i.e. the literal is coerced to the column's type before comparison. **The filter must hash the same canonical byte form.** Concretely:

```rust
#[inline(always)]
fn canonical_hash(db_type: DbType, bytes: &[u8], seed: &HashSeed) -> u64 {
    match db_type {
        // fixed-width: zero-extend to u64/u128 in the column's endianness,
        // then one multiply-shift. ~3 cycles.
        I8|I16|I32|I64|U8|U16|U32|U64|BOOL|CHAR|DATETIME =>
            mix64(widen_to_u64(db_type, bytes) ^ seed.0),
        I128|U128 => mix128(bytes, seed),
        // F32/F64: canonicalise -0.0 → +0.0 and all NaN → one quiet NaN
        // BEFORE hashing, or `x = -0.0` misses rows storing +0.0.
        F32|F64  => mix64(canon_float(db_type, bytes) ^ seed.0),
        STRING   => ahash_with_seed(bytes, seed),   // full value, AES-NI when present
        NULL     => NULL_SENTINEL,
        _        => unreachable!(),
    }
}

#[inline(always)]
fn mix64(x: u64) -> u64 {                 // fibonacci multiply + xorshift finaliser
    let h = x.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    h ^ (h >> 29)
}
```

**This must be enforced by a test that walks every `DbType` and asserts `canonical_hash(a) == canonical_hash(b)` for every pair where `perform_comparison_operation(a, b, Equals)` is true.** It is the one place where a silent divergence yields wrong query results, so it gets a dedicated property test (§12.2).

### 5.3 Fingerprint extraction

```rust
#[inline(always)]
fn fingerprint(h: u64, f: u8) -> u32 {
    // Take from the LOW bits; the HIGH bits address the L0/L1 block, so the
    // two uses stay independent.
    let g = (h as u32) & ((1u32 << f) - 1);
    // Fold reserved codes 0 and 1 into the live range without biasing:
    if g < 2 { g + 2 } else { g }        // cmov, no branch
}
```

The `< 2` fold slightly overloads codes 2 and 3; FPP becomes `≈ 1/(2^f − 2)` rather than `1/2^f`. The table in §3 already accounts for this.

---

## 6. SIMD kernels

### 6.1 L2 zone probe — AVX-512 (the fast path)

Test one value against 512 rows:

```rust
#[target_feature(enable = "avx512f,avx512bw")]
unsafe fn probe_zone_avx512(planes: *const __m512i, f: u8, g: u32) -> __m512i {
    let mut acc = _mm512_set1_epi64(-1i64);           // all candidates alive
    for j in 0..f as usize {
        let plane = _mm512_loadu_si512(planes.add(j).cast());
        // want = plane      if bit j of g is 1
        // want = NOT plane  if bit j of g is 0
        // acc &= want   →  one vpternlogq, zero branches:
        // Branchless select: 0 or -1 from bit j of g, no unpredictable branch.
        let want = _mm512_set1_epi64((((g >> j) & 1) as i64).wrapping_neg());
        acc = _mm512_ternarylogic_epi64::<0x90>(acc, plane, want);
    }
    acc   // zone is empty iff _mm512_test_epi64_mask(acc, acc) == 0
}
```

> `0x90` is the imm8 whose bit at index `4A + 2B + C` equals `A & (B XNOR C)` — i.e. bits 4 and 7 set. Only **AVX512F** is needed; use `_mm512_test_epi64_mask` for the empty check rather than the AVX512BW byte variant. Test 12.2 re-derives the constant exhaustively instead of trusting it.

**Cost:** `f` loads + `f` `vpternlogq` + 1 `vptest`, for **512 rows**. At `f = 8`: 17 µops, ~6 cycles throughput. That is ~0.012 cycles/row — three orders of magnitude below the memory system's ability to feed it. The kernel is *unconditionally memory-bandwidth bound*, which is the correct place to land (G4): at 1 byte/row (`f = 8`) and ~25 GB/s of achievable single-socket streaming bandwidth, this is ~25 billion rows/second; at `f = 16` it halves to ~12 billion. Bandwidth, not the kernel, is the only thing `f` trades against on the CPU side.

`vpternlogq` is the reason to care about AVX-512 here: it fuses the select-and-mask into one instruction, halving the op count versus AVX2.

### 6.2 L2 zone probe — AVX2 (baseline, always available at `x86-64-v3`)

A 512-bit plane is 2 × `__m256i`:

```rust
#[target_feature(enable = "avx2")]
unsafe fn probe_zone_avx2(planes: *const __m256i, f: u8, g: u32) -> (u32, [u64; 8]) {
    let (mut lo, mut hi) = (_mm256_set1_epi64x(-1), _mm256_set1_epi64x(-1));
    for j in 0..f as usize {
        let sel = _mm256_set1_epi64x(if (g >> j) & 1 == 1 { 0 } else { -1 });
        let p_lo = _mm256_loadu_si256(planes.add(2 * j));
        let p_hi = _mm256_loadu_si256(planes.add(2 * j + 1));
        lo = _mm256_and_si256(lo, _mm256_xor_si256(p_lo, sel));
        hi = _mm256_and_si256(hi, _mm256_xor_si256(p_hi, sel));
    }
    let empty = _mm256_testz_si256(lo, lo) & _mm256_testz_si256(hi, hi);
    (empty as u32, store_mask(lo, hi))
}
```

2× the ops (`vpxor` + `vpand` instead of one `vpternlogq`), still ~12 cycles per 512 rows, still bandwidth bound. **The AVX2 path is not a degraded fallback** — it hits the same bandwidth ceiling. AVX-512 buys headroom, not throughput.

Dispatch once at startup into a `static PROBE_FN: fn(...)`, resolved by `is_x86_feature_detected!`, in the style already used in [query_tokenizer.rs:113](rasterizeddb_core/src/core/tokenizer/query_tokenizer.rs#L113).

### 6.3 Mask → row indices

BMI1, guaranteed by `x86-64-v3`:

```rust
let mut w = mask_words[k];
while w != 0 {
    let bit = w.trailing_zeros();          // tzcnt
    let slot = zone_base + k * 64 + bit as u64;
    emit(slot);
    w &= w - 1;                            // blsr
}
```

Two µops per surviving row. For a selective query this loop essentially never executes.

### 6.4 Insert / update / delete kernels

All three are the same primitive — write `f` bits at slot `i`:

```rust
#[inline(always)]
fn write_slot(planes: &[AtomicU64], f: u8, i: usize, g: u32) {
    let (w, b) = (i >> 6, i & 63);
    let m = 1u64 << b;
    for j in 0..f as usize {
        let word = &planes[j * 8 + w];
        if (g >> j) & 1 == 1 { word.fetch_or(m, Relaxed); }
        else                 { word.fetch_and(!m, Relaxed); }
    }
}
```

- **Insert**: `write_slot(g)`.
- **Update** ([table.rs:302](rasterizeddb_core/src/core/row/table.rs#L302) rewrites the row but keeps `pointer_record_pos`): `write_slot(new_g)` — *in place, O(f) bit ops, no rebuild.* A Bloom filter fundamentally cannot do this.
- **Delete** ([table.rs:252](rasterizeddb_core/src/core/row/table.rs#L252)): `write_slot(0)` — the slot stops matching everything, immediately, and deleted rows are now pruned *before* their pointer is even decoded.

`f ≤ 16` `fetch_or`s on 8-byte-apart words. At `f = 8` that is 8 locked ops per row insert — measurable, so §8 specifies the batched non-atomic variant used when the open zone is uncontended.

### 6.5 Batched multi-value probe (`IN`-lists, semi-joins)

For `col IN (v1..vn)`: compute all `n` fingerprints once, then per zone run the plane loop once per value but reuse the loaded planes — the planes stay in L1d, so the marginal cost of value `k+1` is `f` ALU ops with no loads. `n` values over a zone cost `f` loads + `n·f` ternlogs. Union the masks. Up to `n ≈ 32` this is still cheaper than the load traffic.

---

## 7. Dynamic scaling

### 7.1 The promotion ladder (scale up)

Tiers materialise as the table grows, so a small table pays literally nothing:

| Table rows | Structure | Disk | Resident RAM |
|---|---|---|---|
| < 4 096 | **nothing** — full scan is already cheaper than a probe | 0 | 0 |
| 4 096 … 1 M | L2 only | `f/8` B/row | page cache only |
| 1 M … 64 M | L2 + L0 | `f/8` B/row + 512 B | + 64 B |
| > 64 M, or planner-demanded | L2 + L0 + L1 | `+ b/8` B/row | + page cache |

Promotion happens in a background task; until it completes, the missing tier is simply not consulted. Every tier is independently optional, so there is no flag-day.

### 7.2 Growth without rebuild — zone sealing

```
insert row → slot i, zone z = i / 512
    if z == open_zone.base_slot / 512:
        write_slot(open_zone.planes, f, i % 512, g)      // RAM only, no I/O
        hll_update(open_zone.hll, h)
    when i % 512 == 511:                                  // SEAL
        append 512·f/8 bytes to the sidecar               // ONE write per 512 rows
        distinct_est ← hll_estimate(open_zone.hll)
        f_next ← choose_f(distinct_est)                   // §7.3
        zero open_zone, base_slot += 512
```

**The append-only sidecar never needs a global rebuild as the table grows.** Growth is O(1) amortised, and the row-insert hot path performs *zero* I/O — the sidecar is written once per 512 rows (G5).

L0 is the one structure that must cover the whole table. It grows by the **doubling-without-rehash** trick: block index comes from the *high* bits of the hash, so doubling the block count means each old block `b` is duplicated into new blocks `2b` and `2b+1`. Every key that was found is still found — no false negatives, no rehash, no reading the base data, just a `memcpy`. FPP inflates slightly with each doubling; a background clean rebuild (from a full L2 sweep, no row reads needed if we keep hashes… otherwise from a row scan) runs when inflation exceeds 2× target. Until it completes, the old L0 stays live and correct.

### 7.3 Adaptive width — the "dynamic" in dynamic scaling

Two knobs adapt. The per-zone sketch feeds a running **global** distinct estimate for the column (per-zone counts alone are the wrong signal — a column with 1 M distinct values still looks near-unique inside any 512-row window):

**(a) Fingerprint width `f`.** The rule that matters:

```
false fetches  = N_rows × 2^-f          (independent of cardinality!)
true matches   = N_rows / distinct
⇒ balanced when 2^-f ≈ 1/distinct  ⇒  f ≈ log2(distinct)
```

`f` is driven by **global distinct count**, and therefore grows with the table — this is the literal sense in which the structure scales with table size. Cardinality decides *whether* to index; it also, separately, decides *how wide*.

```rust
fn choose_f(distinct_global: u64, n_rows: u64, cfg: &FilterCfg) -> u8 {
    // Gate: if the predicate would match too large a fraction of the table,
    // pruning cannot pay for the bytes. A `status` column with 5 values matches
    // 20 % of rows either way — DON'T INDEX IT. This gate is where most of the
    // "minimal overhead" actually comes from.
    if distinct_global < (n_rows as f64 * cfg.min_selectivity) as u64 { return 0; }

    // Balance false fetches against true matches, then clamp.
    let f = (distinct_global as f64).log2().ceil() as u8;
    f.clamp(cfg.min_f /* 4 */, cfg.max_f /* 16 */)
}
```

Note what the clamp costs. `max_f = 16` on a 100 M-row table means a unique-key lookup still yields **~1 500 false fetches** (§11). Raising to 20 cuts that to 95 at 2.5 B/row. There is no setting at which this becomes a B-tree, and it should not be sold as one.

**Generations.** Because `f` grows as the table grows, zones sealed early carry a smaller `f` than zones sealed later. Rather than a per-zone descriptor (which would break arithmetic offset computation), zones are grouped into **generations** — a contiguous run of zones sharing one `f`:

```rust
struct Generation { start_zone: u32, zone_count: u32, f: u8, byte_offset: u64 }
```

A few dozen entries covers any table. Offsets stay arithmetic *within* a generation, old generations are never rewritten, and each generation is correctly sized for the table as it was when sealed. Since generation sizes grow geometrically, total false fetches stay bounded by the newest generation's `f`. Old generations may be upgraded opportunistically during compaction; leaving them alone is always correct.

**(b) Which columns exist at all.** `probe_counter` in the directory tracks how often the planner asked for each column. A column with zero probes over `N` queries is dropped (§7.4); a non-indexed column that the planner *wanted* `N` times is queued for a background build. The index converges on the actual workload rather than on the schema.

### 7.4 Scale **down** — the part most designs skip

Four independent mechanisms:

1. **Delete → immediate bit reclamation.** `write_slot(0)` erases the fingerprint. Unlike Bloom, this is exact: FPP *improves* on delete. It also means a heavily-deleted zone's planes go to all-zeros and compress/sparsify naturally.
2. **Truncation / compaction → physical shrink.** When trailing zones are dropped, truncate the sidecar extent and `fallocate(FALLOC_FL_PUNCH_HOLE)` any interior all-zero regions. Disk usage actually falls; the file becomes sparse.
3. **Cold column → segment eviction.** A column whose `probe_counter` stays flat is demoted: its L2 extent is hole-punched and `flags.materialised = 0`. The directory entry survives (64 bytes), so re-materialising is a background rebuild, not a schema change. This reclaims *both* disk and page cache.
4. **RAM pressure → `madvise`.** Because segments are mmap'd and never heap-copied, RAM scale-down is the kernel's job. We only advise: `MADV_SEQUENTIAL` + `MADV_WILLNEED` on the extent a scan is about to stream, `MADV_DONTNEED` on cold columns. There is no eviction policy to write and no way for the filter to OOM the process.

Additionally, when the promotion ladder in §7.1 is crossed *downward* (a table shrinks below 1 M rows), L1 and L0 extents are simply dropped.

### 7.5 Late-added columns

`ALTER TABLE ADD COLUMN` sets `valid_from_slot = current_slot_count`. Probes treat slots below it as unconditional candidates, so the answer is correct from the first instant. A background backfill walks slots `[0, valid_from_slot)` and lowers the value to 0 when done.

---

## 8. Concurrency and the no-false-negative invariant

This is the correctness core (G6). The hazard: a scanner sees row pointer `i` (already appended) but reads filter slot `i` before the inserter wrote it → the row is pruned → **wrong answer**.

**Invariant.** A scanner may trust filter slot `i` only if `i < valid_upto_slot`. Slots at or above the watermark are unconditional candidates.

```rust
// Writer (insert path, after append_data_returning_offset gives the offset):
let slot = offset / ROW_POINTER_RECORD_LEN as u64;
write_slot(&open.planes, f, (slot % 512) as usize, g);   // Relaxed stores
core::sync::atomic::fence(Ordering::Release);
advance_watermark(&col.valid_upto_slot, slot);            // CAS-if-contiguous

// Reader (scan path, ONCE per query):
let w = col.valid_upto_slot.load(Ordering::Acquire);
// slots >= w: mask bit forced to 1
```

`advance_watermark` CASes `valid_upto_slot` from `slot` to `slot + 1`. Concurrent inserters complete out of order, so the watermark can lag behind a gap; the consequence is that a handful of tail slots are treated as candidates. **Lag costs performance, never correctness** — that asymmetry is what makes this design safe under any interleaving. A tiny pending-bitset lets a later writer close the gap opportunistically.

Everything else is lock-free by construction:

- Sealed zones are **immutable**. Readers need no synchronisation whatsoever — just a raw slice of mmap'd bytes.
- The catalog is swapped whole via `ArcSwap` on seal / rebuild / drop.
- The open zone is `[AtomicU64]` with `fetch_or`/`fetch_and`. When the table has a single writer (the common case), a `#[cfg]`-free fast path takes a `parking_lot` mutex around the open zone once per batch and uses plain non-atomic writes — 8 locked ops/row is worth avoiding.

> **Measured, and the atomic open zone is the wrong answer here.** Two of the three claims above do not survive contact with the engine.
>
> **Sealed zones are not immutable.** `UPDATE`, `DELETE`, backfill and reclamation all write into zones long past the frontier, so there is no immutable tail to publish by pointer swap and no version of this design where readers need no synchronisation.
>
> **Atomics would be slower than the lock, not faster.** Concurrent appends all land in the same open zone, so its words are the contended cache lines either way. Going lock-free replaces *one* lock acquisition per row with `2f` atomic read-modify-writes on those same lines — sixteen ping-pongs instead of one, at `f = 8`. The paragraph above concedes exactly this in its own last clause.
>
> What the measurement says (`report_insert_contention`, 4 indexed columns):
>
> | writers | per-row locking | batched (64) |
> |---|---|---|
> | 1 | 9.0 M rows/s | 9.0 M rows/s |
> | 2 | 3.6 M | 5.1 M |
> | 4 | 3.3 M | 6.6 M |
> | 8 | 2.3 M | 5.6 M |
>
> Per-row locking scales *negatively*. Shortening the critical section does not fix it: hashing and payload lookup were moved out of the lock — the payload scan had made maintenance quadratic in column count while holding it — and the contended numbers did not move, which is what says the cost is the acquisition rather than the work inside. Batching does fix it, 2.4× at eight writers, and it is what the paragraph above already proposed beside the atomics.
>
> So `on_insert_batch` and `backfill_batch` take each column's lock once per run, and the whole-table backfill uses runs of 256. What is left uncovered is a single row arriving alone on its own connection, which has no batch to join. For scale: the engine's own insert path costs ~1220 ns/row single-threaded, against ~110 ns/row of filter maintenance — so the filter is ~9 % of an insert, and the uncovered case is worth a staged queue only if that ratio changes.

**Crash safety.** The sidecar is *derived data*, so it is never on the durability critical path and never needs `fsync`. On open:

```
persisted_watermark = superblock.valid_upto_slot
actual_slots        = io_pointers.get_len() / ROW_POINTER_RECORD_LEN
if persisted_watermark < actual_slots:
    valid_upto_slot = persisted_watermark      // correct immediately, just less selective
    spawn background backfill of the delta     // reads rows, writes bits, advances watermark
```

A torn or truncated sidecar is always recoverable, and a completely deleted sidecar degrades to today's behaviour. `pointer_record_len` in the superblock guards against the `enable_long_row` feature changing the stride out from under a stale file.

---

## 9. Planner integration — mask algebra

### 9.1 Sargable extraction

During `compile_query_plan` ([concurrent_processor.rs:204](rasterizeddb_core/src/core/processor/concurrent_processor.rs#L204)), walk the token stream and build a parallel *mask program* over the boolean structure:

| WHERE construct | Mask contribution |
|---|---|
| `col = <literal>` on an indexed column | `probe(col, g)` |
| `col IN (a, b, c)` | `probe(a) ∣ probe(b) ∣ probe(c)` |
| `col IS NULL` | via `predicate_rewrite` → `__null_col = 1`, an ordinary equality on the companion `BOOL` column, answered by its zone bounds |
| `col IS NOT NULL` | via `predicate_rewrite` → `__null_col = 0`. Sound, unlike `!=`: this is an equality mask, not the complement of one |
| `col STARTSWITH 'abc'` | `mask(col >= 'abc') & mask(col < 'abd')` — see §13.2 |
| `col > v`, `col >= v`, `col < v`, `col <= v` | zone-bounds test (§4.7) |
| `A AND B` | `mask(A) & mask(B)` |
| `A OR B` | `mask(A) ∣ mask(B)` |
| `col != v`, `CONTAINS`, `ENDSWITH`, col-to-col compare, arithmetic operands | `ALL_ONES` (unknown) |
| `(`…`)` | parsed as written; grouping overrides precedence |
| `col IN (a, b, c)` | via `predicate_rewrite` → `(col = a OR col = b OR col = c)` |
| `col BETWEEN lo AND hi` | via `predicate_rewrite` → `(col >= lo AND col <= hi)` |
| `col NOT BETWEEN lo AND hi` | via `predicate_rewrite` → `(col < lo OR col > hi)` — two ranges, genuinely filterable |
| `col NOT IN (a, b)` | via `predicate_rewrite` → `(col != a AND col != b)` — all inequalities, so extraction declines |
| unbalanced parens | extraction declines entirely |

Only **monotone (negation-free)** contexts are usable. `col != v` is the trap: `probe(v)` is a *superset* of matching rows, so its complement is a *subset* of non-matching rows — using it would drop real results. It must yield `ALL_ONES`. There is no `NOT` in the tokenizer, so that case cannot arise.

**Precedence is not invented here, it is mirrored.** `TransformerProcessor::evaluate_comparison_results` ([transformer.rs](rasterizeddb_core/src/core/processor/transformer.rs)) walks the flat `(bool, Option<Next>)` chain accumulating an AND-group and short-circuiting to `true` when a group succeeds before an `OR` — i.e. **sum-of-products with `AND` binding tighter than `OR`**, standard SQL precedence. `sarg::extract` reproduces exactly that grouping from the token stream, so `A OR B AND C` becomes `A ∨ (B∧C)`. Folding left-to-right as `(A∨B) ∧ C` would produce a mask that is not a superset — the failure this mirroring exists to prevent, and the reason the property test `or_masks_are_supersets_of_truth` checks 300 random expressions against brute-force truth.

An AND-group containing no usable predicate would contribute `ALL_ONES` to the disjunction and make the whole mask `ALL_ONES`; `extract` detects that and reports `None` so no useless bitmap is built.

### 9.5 Operators are an allowlist

`op_of` recognises exactly five operators and returns `None` for everything
else, so any operator added to the engine later is opaque to the filter until
someone deliberately teaches this module about it.

That is what makes case-insensitive comparison safe to add elsewhere. `ILIKE`
would break the §5.2 invariant outright — `'ACME'` and `'acme'` compare equal but
hash differently, so a mask built from one would prune rows holding the other.
Being outside the allowlist, such a predicate collapses to `All` and the query
scans in full rather than losing rows.

The allowlist cannot protect against the other shape of this hazard: a
**case-insensitive collation** would change what `=` itself means, and `=` *is*
on the allowlist. `string_equality_is_byte_equality` states that dependency
executably — if equality ever stops being byte equality, either `canonical_hash`
folds identically or `STRING` leaves `is_indexable`.

---

### 9.4 Literals are coerced, not type-checked

The tokenizer types a numeric literal from the **preceding** column, so a literal rarely arrives already matching the column it is compared against: `1990 < id` yields an `I32` literal against a `U64` column, `amount > 200` an `I32` against `DECIMAL`, and a text literal against a `UUID` column is simply how a UUID is written.

An earlier version *checked* the literal's type and declined on mismatch. That was silently catastrophic for coverage — the evaluator answered those queries correctly (the parser reconciles operand types), so nothing looked wrong, while **every one of them ran unfiltered**. Between that and the paren refusal, essentially all ORM traffic bypassed the index.

`sarg` now re-encodes the literal through the same `value_codec::coerce_literal` the parser uses. That is what keeps the bytes hashed identical to the bytes compared — the same invariant §5.2 enforces for equality. A literal that cannot be coerced (`uid = 'not-a-uuid'`) contributes nothing rather than hashing garbage.

### 9.2 Free compounding on AND

Two **independent** `f = 8` columns ANDed give FPP ≈ 2⁻⁸ × 2⁻⁸ ≈ 1.5 × 10⁻⁵ on the pair. This is why **composite/multi-column filters are not needed** — intersecting per-column masks already beats them, at no extra storage, for every conjunction rather than only the pre-declared ones.

**Caveat: independence is an assumption, not a guarantee.** Correlated columns (`city`/`zip`, `first_name`/`email`) do not multiply; in the degenerate case where one column functionally determines the other, the pair's FPP is that of the *better* single column, not the product. This is a performance risk only — the mask is still a superset — but the planner must not treat multiplied FPP as a promise when estimating whether a fetch is worth doing.

### 9.3 Early exit

If L0 rejects, the query returns immediately without opening the pointer iterator at all. `SELECT … WHERE email = 'nope@x.com'` on a 100 M-row table resolves in **one cache-line read**.

---

## 10. Scan integration

Add to `RowPointerIterator`:

```rust
pub async fn next_row_pointers_masked(
    &mut self,
    masks: &mut ZoneMaskStream<'_>,   // yields a 512-bit mask per zone
    out: &mut Vec<RowPointer>,
) -> Result<()>;
```

Semantics, replacing the inner drain loop at [row_pointer.rs:311-328](rasterizeddb_core/src/core/row/row_pointer.rs#L311-L328):

1. Pull the next zone's mask (sequential mmap read of `f × 64` bytes).
2. If the mask is zero → **skip 512 slots without decoding a single pointer record**, and if 8 consecutive zones are all zero, skip the pointer-file page entirely.
3. Otherwise `tzcnt`/`blsr` over the mask, decoding only the surviving records.
4. Batch scattered survivors through the existing `try_read_vectored_sync` / `read_vectored` ([traits.rs](rasterizeddb_core/src/core/storage_providers/traits.rs)) so a sparse zone costs one vectored read rather than 512 random ones.

The workers in `spawn_batch_tasks` need **no changes** — they simply receive far fewer pointers. That is the whole integration.

### 10.1 When a mask is worth using — two questions, not one

`min_pruning_ratio` asks what fraction of the table a mask excludes, and declines
one that excludes too little on the grounds that a bitmap test per record costs
more than the fetches it saves. **That is the right question only for a scan that
runs to completion.**

A scan that stops early — `LIMIT`, or any other early exit — never reaches most
of the table. What it pays is not how much was excluded but **how far it walks
before its first match**, and every one of those steps is a row fetch the mask
would have turned into a bit test.

`WHERE id >= 99995 LIMIT 5` over a million rows is the case that made this
concrete: ~90 % of the table matches, so the ratio test threw the mask away, and
the scan then fetched row bytes for ~100 000 non-matching rows before its first
hit. The mask excluded every one of them — a leading dead run of 99,840 slots,
195 zones exactly, since bounds prune at zone granularity.

So `build_candidates` keeps a mask that prunes broadly **or** one whose leading
run of excluded slots is long enough to matter. Measured on the same shape at
200,000 rows with a 20,000-slot dead prefix: **0.43 ms -> 0.09 ms**, ~4.8x.

### 10.2 The ratio both thresholds rest on, measured

`min_pruning_ratio` and the leading-run override trade the same two costs — what
a masked-out slot costs against what a visited one costs — and **neither
constant was chosen from a measurement of it**. That is how a heuristic ends up
tuned around a wrong number: the tuning absorbs the error, and what you see is a
guard that looks delicate for no visible reason.

Measured by `tests/cost_ratio.rs` in `rasterizeddb_filter_tests` (a scan that
walks everything and matches nothing, filtered table against the same table with
`indexed = off`, best of 5 per configuration, repeated):

| row width | masked-out slot | visited slot | ratio | break-even pruning ratio |
|---|---|---|---|---|
| ~8 B | 0.17–0.37 ns | 12.1–15.8 ns | 34–77× | 0.970–0.987 |
| ~400 B | 0.17–0.22 ns | 14.3–18.3 ns | 66–108× | 0.985–0.991 |
| ~2000 B | 0.20–0.41 ns | 46.9–51.7 ns | 116–252× | 0.991–0.996 |

Three things follow.

**A skipped slot costs about a fifth of a nanosecond, not "a bitmap test".** The
whole-word skip in `next_row_pointers_into` clears 64 slots per word test, so a
*contiguous* excluded region — exactly what a range predicate produces — costs
about 1/64 of a test per row. The premise the guard was written against is
pessimistic by that factor in precisely the case it most needs to get right.

**The break-even pruning ratio is 0.970–0.996; the default was 0.5 and is now
0.90.** Every measurement puts break-even above 0.97, so the old default was
discarding masks that paid for themselves several times over — the figure had
been picked before either cost had a number under it. 0.90 sits under the whole
measured range with margin, deliberately short of the 0.970 floor rather than
level with it, since 0.970 is the worst *observed* value and not a bound derived
with slack. Whoever runs this on a quiet machine, or against a table that does
not fit in page cache, can justify going closer.

**Wider rows push the ratio up, and steeply.** At ~2000 B the row file is 400 MB
and the fetch cost triples to ~48 ns while the masked-out slot stays flat at
~0.2 ns — the mask is one bit per row and stays resident whatever the rows cost.
That is the direction that matters for sizing a threshold: **break-even can only
move away from 0.5, never toward it**, because nothing about a larger or colder
table makes the mask dearer. The worst configuration measured — narrow rows,
cached, on a contended machine — still puts break-even at 0.970, and that is the
floor the decision needs rather than a precise ratio.

That also settles the scaling edge in the absolute leading-run floor. One batch
of avoided fetches is right for the early-exit case it targets; on a very large
table it will also accept a mask whose dead prefix is a tiny fraction of the
whole, which is harmless under `LIMIT` and — at 34× and up rather than the 32×
that would make it marginal — not a loss for a full scan either.

Prefetch `_mm_prefetch::<_MM_HINT_T0>` 4 zones ahead of the plane stream; the access pattern is perfectly linear, so the hardware prefetcher mostly handles it anyway.

---

## 11. Cost model

Worked example: **100 M rows, ~100 B/row, 3 indexed columns, `f = 8`, default config (L0 + L2).**

**The central trade.** Everything hinges on `f`, so it gets its own table rather than a single headline number:

| `f` | B/row/col | 3 cols on 100 M rows | overhead vs 10 GB | **false row fetches per probe** |
|---|---|---|---|---|
| 8 | 1.00 | 300 MB | 3.0 % | 393,701 |
| 12 | 1.50 | 450 MB | 4.5 % | 24,426 |
| 16 | 2.00 | 600 MB | 6.0 % | 1,526 |
| 20 | 2.50 | 750 MB | 7.5 % | 95 |
| 24 | 3.00 | 900 MB | 9.0 % | 6 |

False fetches are `N × 2^-f` and scale **linearly with table size**. On a 1 M-row table `f = 8` costs 3,900 false fetches and is fine; on 100 M rows the same setting costs 394,000 and is useless for a point lookup. Columns failing the `choose_f` gate cost **0**.

**RAM:** 64 B per column resident (L0) + whatever the page cache holds. No configured filter cache, no eviction thread, no heap allocation proportional to table size.

**I/O per query** — 100 M rows, high-cardinality columns at `f = 16`:

| Query | Today | With filters |
|---|---|---|
| Point lookup, value **absent** | 2.1 GB pointers + ~10 GB rows | **64 B** — one L0 cache line |
| Point lookup, 1 match | 2.1 GB + ~10 GB | 200 MB planes + **~1 526** false fetches + 1 real |
| `a = x AND b = y` (independent) | 2.1 GB + ~10 GB | 400 MB planes + **~1** fetch |
| Scan matching 30 % of rows | 2.1 GB + ~10 GB | no benefit — planner skips the filter |

So: **~60× fewer bytes and ~65,000× fewer random row fetches** on a selective single-column lookup, and absent values resolve in a single cache line. Multi-predicate conjunctions are where the design genuinely shines — two independent filters compound to ~1 fetch. Single-column unique-key lookups remain ~1,500 fetches, which is excellent against a 100 M-row scan and poor against a real index; that gap is the honest boundary of this structure.

### 11.1 Measured (AVX-512 host, `cargo bench --bench filter_benchmarks`)

**Storage is exactly `f` bits per row**, which is the whole point of bit-slicing — no byte rounding anywhere:

| `f` | measured bytes for 1 M rows | bits/row |
|---|---|---|
| 4 | 500,224 | **4.002** |
| 8 | 1,000,448 | **8.004** |
| 12 | 1,500,672 | **12.005** |
| 16 | 2,000,896 | **16.007** |

The 0.002 is generation bookkeeping.

**Probe kernel**, one zone = 512 rows, planes resident in L1:

| `f` | scalar | AVX-512 | speedup | rows/s (AVX-512) |
|---|---|---|---|---|
| 4 | 12.0 ns | 4.52 ns | 2.7× | 113 G |
| 8 | 21.3 ns | 5.57 ns | 3.8× | 92 G |
| 12 | 29.7 ns | 6.08 ns | 4.9× | 84 G |
| 16 | 42.3 ns | 7.94 ns | 5.3× | 64 G |

`vpternlogq` earns its keep: the AVX-512 advantage *widens* with `f`, because each extra plane costs one fused instruction instead of an xor plus an and.

**Full-column sweep** — building a complete candidate bitmap over 1 M rows:

| `f` | time | throughput |
|---|---|---|
| 8 | 20.6 µs | **48.5 G rows/s** |
| 16 | 31.8 µs | 31.4 G rows/s |

Faster than the 25 G rows/s estimate above because 1 MB of planes is cache-resident; expect the estimate to hold once the working set exceeds LLC.

**Insert maintenance:** ~15 ns per row per indexed column at `f = 8`, ~17 ns at `f = 16`, uncontended. That is the real cost added to a row write, and it is dominated by `canonical_hash` plus the HLL update rather than by the 8–16 bit writes. Under concurrent writers it is dominated by neither — see the contention table below.

**Hashing:** 4.4 ns (`u64`), 4.3 ns (`f64`), 5.0 ns (8-byte string), 5.8 ns (32 B), 30.7 ns (128 B).

**Zone bounds**, `i64` column, 1 M rows, predicate `col > 900000`:

| | |
|---|---|
| Storage | 31,512 B = **0.252 bits/row** (16 B per 512-row zone, as predicted) |
| Sweep | 8.24 µs = **121 G rows/s** — 2.5× the fingerprint sweep, on 32× less data |
| Append-ordered data | leaves **100,416** of 1 M candidates (10.04 %; the true answer is 99,999) |
| Random data | leaves 1,000,000 (no pruning) |

That contrast is the whole story of zone maps: on anything with locality —
auto-increment ids, timestamps, append-ordered anything — a quarter of a bit per
row gets within 0.4 % of the exact answer. On randomly distributed data the
bounds of every zone overlap and it prunes nothing at all, which is why it
complements the fingerprints rather than replacing them.

**Checkpoint round-trip**, 1 M rows × 2 near-unique columns (both adapted to `f = 16`):

| | |
|---|---|
| Sidecar size | 3,985,624 B = **15.94 bits/row/column** |
| Serialize | 325 µs (11.4 GiB/s) |
| Deserialize | **120 µs** (31.0 GiB/s) |

That last number is the point of phase 4: reopening a million-row table restores
its whole index in **~120 µs** instead of rescanning every row. Checkpoints are
written on an amortised schedule whose threshold grows with the table, so the
total rewrite cost stays linear rather than quadratic in the number of them.

**Span summary** (`bloom = on`), `u64` column, 1 M rows, `f = 8`, probing for a value that is absent:

| Distinct values per 16 384-row span | Probe, summary off | Summary on | |
|---|---|---|---|
| 250 | 18 µs | **7 µs** | **2.54×** |
| 16 384 (all distinct) | 18 µs | 23 µs | **0.81×** |

Storage is 31,744 B = **0.254 bits/row** in both cases. This is the whole argument for the tier being opt-in: it is a 2.5× win exactly where the cost model of §4.3 says it should be, and a 19 % *loss* where the block saturates and it can rule nothing out. The default is off.

**Insert maintenance under concurrency**, 4 indexed columns:

| writers | per-row locking | batched (64) |
|---|---|---|
| 1 | 9.0 M rows/s | 9.0 M rows/s |
| 2 | 3.6 M | 5.1 M |
| 4 | 3.3 M | 6.6 M |
| 8 | 2.3 M | 5.6 M |

Per-row locking scales negatively; batching is 2.4× at eight writers and roughly flat. For scale, the engine's own `insert_row` costs ~1220 ns/row single-threaded against ~110 ns/row of filter maintenance — the filter is ~9 % of an insert. See §8 for why the atomic open zone of that section is *not* the answer.

**CPU, projected:** at 1 byte/row and ~25 GB/s of streaming bandwidth once out of cache, 100 M rows of filtering ≈ **4 ms single-threaded**, ~0.4 ms across the existing 16-worker pool — against 100 M random row fetches.

---

## 12. Correctness and benchmarking plan

### 12.1 The safety property

> For every query and every row, if the WHERE clause evaluates true for that row, the row's mask bit is 1.

Everything else is a performance property. Tests are structured around exactly that split.

### 12.2 Tests

| Test | Shape |
|---|---|
| **Hash/comparison agreement** | For each `DbType`, generate pairs; assert `perform_comparison_operation(a,b,Equals) ⇒ canonical_hash(a)==canonical_hash(b)`. Explicit cases for `-0.0`/`+0.0`, NaN, empty string, max-width ints. |
| **No false negatives (property test)** | Random schema, 100 K random rows, 1 K random sargable WHERE clauses. Assert `mask ⊇ brute_force_matches` for every one. Run with delete/update interleavings. |
| **Differential vs. unfiltered** | Same query with filters on and off must return byte-identical result sets. Run the whole existing query test suite in both modes. |
| **Concurrency** | 16 writer tasks + 16 scanner tasks against one table; assert no scan ever misses a committed row. Watermark lag is allowed, misses are not. |
| **Crash recovery** | Truncate the sidecar at random offsets, kill mid-seal, delete it entirely; assert correct results in every case and that backfill converges. |
| **`enable_long_row` guard** | Open a sidecar built with the other `ROW_POINTER_RECORD_LEN`; assert clean rejection + rebuild. |
| **FPP calibration** | Measured FPP within 1.5× of `1/(2^f − 2)` for `f ∈ {4,6,8,10,12}`. |
| **`imm8` derivation** | Exhaustive 8-bit truth-table check of the `vpternlogq` constant against a scalar reference. |
| **Storage bound** | Assert sidecar size ≤ `f/8 · rows · cols · 1.02`. Catches format bloat regressions. |

### 12.3 Benchmarks — `benches/filter_benchmarks.rs`

Following the existing criterion setup:

1. `canonical_hash` throughput per `DbType`.
2. `probe_zone` — AVX-512 vs AVX2 vs scalar, ns per 512 rows, `f ∈ {4,8,12}`.
3. Plane-stream throughput, rows/s, warm vs cold page cache.
4. `write_slot` insert cost, atomic vs non-atomic path.
5. End-to-end query latency across selectivity `10⁻⁷ … 1.0`, with and without filters — **the crossover point where filtering stops paying is the number that sets `min_pruning_ratio`.** Measured in §10.2 by isolating the two per-record costs instead; the end-to-end sweep would confirm it against real query shapes.
6. Insert throughput regression check against the current baseline (must be within noise, since the hot path adds only `f` bit ops).

Add `hotspot_benchmarks.rs`-style profiling for the sealing path.

---

## 13. Configuration

Following the existing `OnceLock` convention in [lib.rs](rasterizeddb_core/src/lib.rs):

| Knob | Default | Effect |
|---|---|---|
| `FILTER_ENABLED` | `true` | Master switch |
| `FILTER_ZONE_ROWS` | `512` | Do not change without re-deriving the kernels |
| `FILTER_MIN_F` | `4` | Floor once a column passes the gate |
| `FILTER_DEFAULT_F` | `8` | Used only when cardinality is still unknown |
| `FILTER_MAX_F` | `16` | Ceiling. Raise to 20–24 to make unique-key lookups viable, at 2.5–3 B/row |
| `FILTER_MIN_SELECTIVITY` | `0.02` | Below this distinct-ratio, don't index the column |
| `FILTER_MIN_TABLE_ROWS` | `4096` | Below this, no sidecar at all |
| `FILTER_ENABLE_L1` | `false` | Opt-in superzone tier |
| `FILTER_L0_MIN_ROWS` | `1_000_000` | Table-filter promotion threshold |
| `FILTER_COLD_QUERIES` | `10_000` | Unprobed-query count before a column is demoted |
| `FILTER_MIN_PRUNING_RATIO` | `0.90` | Planner skips the filter if it prunes less than this — **unless** the mask lets the scan start late (§10.1). Was `0.5`; raised once break-even was measured at 0.97–0.996 (§10.2) |
| `FILTER_CHECKPOINT_INTERVAL_SECS` | `10` | How often the per-table maintenance task considers a checkpoint |

### 13.1 Failure policy

Every failure mode degrades to "no filter": corrupt sidecar, unknown format version, mismatched `pointer_record_len`, allocation failure, unsupported CPU. `FILTER_ENABLED = false` must produce byte-identical results to `true`, which is exactly what test 12.2/"differential" asserts.

### 13.2 Prefix filtering for `STARTSWITH` — built, but not this way

The design above **does not work**, and the reason is worth keeping. A stored value would be hashed over its first `min(len, 8)` bytes while a query for `STARTSWITH('abc')` hashes three; the two hashes never meet. Making them meet needs one plane set per prefix length, which is eight times the storage for a predicate that is not eight times as common.

What is built instead costs nothing at all. `sarg::starts_with_to_range` lowers the predicate to a **half-open range**:

```
col STARTSWITH 'abc'   =>   col >= 'abc' AND col < 'abd'
```

`zonemap::successor` produces the upper bound by incrementing the last byte that can be incremented, rolling trailing `0xFF` bytes to `0x00` as the carry propagates. That value is above every extension of the prefix and below nothing else that starts with it, so the range is **exactly** the predicate rather than a superset of it, and it reuses the string zone bounds of §4.7 with no new structure.

Deliberately not lowered, each for a reason that would otherwise lose rows:

- **`ISTARTSWITH`** (from `ILIKE 'abc%'`). Case folding is not a byte range: `'a'` and `'A'` are 32 apart, and every range containing both contains most of ASCII besides.
- **`CONTAINS` / `ENDSWITH`.** Neither is bounded by any prefix of the value, so no range brackets them.
- **A non-`STRING` column.** `perform_comparison_operation` returns `UnsupportedType` for a prefix test on a number, which fails the query; pruning on that basis would hide a query error behind a short result set.
- **An all-`0xFF` prefix**, which has no successor, keeps only its lower bound. Unreachable from SQL — a literal is a `String` and valid UTF-8 tops out at `0xF4` — but the stored-bytes path needs it.

---

## 14. Phasing

| Phase | Deliverable | State |
|---|---|---|
| **1** | `canonical_hash` + agreement tests | **done** |
| **2** | Bit-sliced planes, scalar probe, `ColumnFilter` | **done** |
| **3** | AVX2 + AVX-512 kernels, runtime dispatch, benchmarks | **done** |
| **4** | Sidecar format, checkpoints, validation, dirty protocol, delta backfill | **done** — base image plus an append log (§4.4) |
| **5** | Planner mask algebra, scan integration | **done** — equality, ranges, `AND`/`OR`/parens, `IN`, `BETWEEN`, `IS NULL`, `STARTSWITH` |
| **6** | HLL sizing, adaptive `f`, generations | **done** — column demotion/promotion and hole punching outstanding |
| **7** | Zone min/max for ranges | **done** — numeric exactly, `STRING` by truncated prefix (§4.7) |
| **8** | L0/L1 tiers, prefix filter for `STARTSWITH` | **done** — one summary serving both tiers (§4.3), off by default; prefix as a range lowering (§13.2) |

What is left is smaller than what is done. **Column demotion and hole punching** (§7.4) would return storage when a column goes cold, which nothing does today. **A staged insert queue** would cover the one contention case batching cannot reach (§8), and the measurement says it is not yet worth it. **A second tier above the span summary** is another 32× reduction in bytes streamed and only becomes interesting past a billion rows. Parenthesised boolean groups are resolved; §15.14 is kept for the lesson rather than the issue.

---

## 15. Open questions and risks

1. **Hash/comparison agreement is the only true correctness risk.** If `perform_comparison_operation` does any coercion the filter doesn't replicate, queries return wrong results silently. Mitigation: phase 1 ships the agreement test *before* anything depends on it. Worth auditing `perform_comparison_operation` for cross-type paths before starting.
2. **Float equality semantics.** Confirm what the engine intends for `NaN = NaN` and `-0.0 = 0.0`, then make the hash match. Currently unverified.
3. **`enable_long_row`.** Changes `ROW_POINTER_RECORD_LEN` from 21 to 92, so the slot↔offset map changes. Guarded by the superblock field, but the feature must not be toggled on a live directory.
4. **Slot density.** The whole design assumes tombstoned pointers keep their slot (they do today — `delete` sets a flag). If compaction is ever added that *removes* pointer records, every slot below the compaction point shifts and the sidecar must be rebuilt or remapped. Worth deciding now, since a compaction design that preserves slots costs nothing extra.
5. **`ORDER BY` + `LIMIT`.** The early-limit path ([concurrent_processor.rs:357-408](rasterizeddb_core/src/core/processor/concurrent_processor.rs#L357-L408)) already stops early; filters compose fine, but the interaction with `enforce_limit_early` across workers needs a test.
6. **Write amplification on wide updates.** An update touching `n` indexed columns costs `n × f` bit writes. Bounded and small, but measure it in phase 2's insert benchmark.
7. **Is L1 ever worth it?** It converts a ~4 ms bandwidth-bound plane sweep into a ~40 µs pointer-chase but does **not** reduce false fetches (§3). Defer to phase 7 and let the phase-5 benchmarks decide.
8. **MVCC would break update-in-place — the sharpest forward-compat risk.** `write_slot(new_g)` on update is safe *only* because no reader can currently see the superseded row version. Under snapshot isolation, an old version still visible to an open transaction would be pruned by a filter that now holds only the new fingerprint — a false negative, the one thing this design promises never to produce. If MVCC is on the roadmap, decide now: either the slot must hold the union of live versions (and updates become or-only, forfeiting the delete/update reclamation in §7.4), or each version needs its own slot. **This constrains the storage engine's future far more than it constrains the filter, so it deserves a decision before phase 4 writes a file format.**
9. **x86-only.** `vpternlogq` has no NEON equivalent and there are no 512-bit registers on ARM. The format itself is portable (`Z = 512` is 4 × 128-bit on NEON, and a plain `[u64; 8]` scalar loop auto-vectorises acceptably), but nothing here is designed or benchmarked for it. Matches the existing posture of [instructions.rs](rasterizeddb_core/src/instructions.rs) and `.cargo/config.toml`, so it is a pre-existing constraint rather than a new one — but "future proof" it is not.
10. **`ALTER COLUMN … TYPE` invalidates every fingerprint** for that column, since `canonical_hash` is type-dispatched. Needs an explicit drop-and-rebuild path; currently unspecified. Same for any change to the hash function itself, which is why `format_ver` is in the superblock.
11. **`MemoryStorageProvider::create_new` returns an unshared buffer** for the same name, so `TableSchema::add_field` — which re-opens `{schema}_FIELDS.db` on every call to read back prior fields — never sees them and assigns `write_order = 0` to every column. Any `Table` built on the in-memory provider therefore gets a broken schema. Pre-existing and unrelated to the filter, but the filter keys on `write_order`, so it surfaced here; the integration tests use `LocalStorageProvider` for this reason.
12. **Insert maintenance costs ~15 ns per row per indexed column** (§11.1), more than the "handful of bit ops" this document originally implied — `canonical_hash` and the HLL update dominate, not the bit writes. Worth revisiting if insert throughput becomes the binding constraint; the HLL update could be sampled rather than run on every row.
15. **`LocalStorageProvider::get_len()` under-reports after `write_data`.** `load_file_len` returns an atomic that only appends update, so a file created or extended by `write_data` reports length 0 in-process until the provider is reopened — despite the comment above `get_len` claiming it re-reads filesystem metadata. This bit hard: guarding the dirty-byte write on `get_len()` silently disabled it, a stale checkpoint then loaded happily, and a row updated after that checkpoint was pruned when queried by its new value — a false negative. The filter no longer consults `get_len` for that decision (it tracks whether a checkpoint exists itself), and `mutation_marks_the_sidecar_dirty_on_disk` pins the behaviour. The provider inconsistency itself is untouched and worth fixing on its own merits.
14. **Parenthesised boolean groups may not be evaluated as written.** The engine flattens boolean structure into a flat `(bool, Option<Next>)` chain, which cannot represent `(A OR B) AND C` without distribution. Whether such a query is currently evaluated correctly is **unverified** — the filter simply declines to prune it, so filtering neither introduces nor hides the issue. Worth settling independently of this work.
13. **Literal-first comparisons are rejected by the engine.** `tokenize` types a numeric literal from the *preceding* column, so `1990 < id` types the literal before `id` is seen and `perform_comparison_operation` fails with an operand-size mismatch; `id > 1990` is fine. Pre-existing and unrelated to filtering — `sarg::extract` mirrors reversed operands correctly and declines to filter when the literal's type does not match the column, so the filter neither causes this nor masks it. Pinned by `literal_first_comparisons_are_rejected_by_the_evaluator`.
