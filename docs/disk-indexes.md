# Disk-resident indexes

Status: **phases 1-3 built.** B-tree, hash and path indexes (`LiveIndex`)
page and survive a clean restart; gin indexes page too, and rebuild at open
as they always have.

Today every index lives in memory, whole ([Indexes](indexes.md), "Lifetime").
[Persistent indexes](persistent-indexes.md) v1 snapshots that memory at clean
points, so an open need not rebuild it, but RAM still holds every index of
every open table. An index of 20 GB needs 20 GB of RAM.

This adds a second residency: an index whose entries live in sorted files on
disk and are searched there through a bounded block cache. Only its recent
changes are kept in memory.

## The knob

`--index-residency memory|hybrid|disk`, process-wide, read at each table
open. It is a startup setting, shown by `SHOW CONFIG` as `index.residency`
(with `index.cache_mb` and `index.delta_mb`), and `SET CONFIG` saves any of
the three for the next start (`rasterizeddb.conf`; a flag still wins).

| Residency | Entries live in | Block cache | Memory part spills at |
|---|---|---|---|
| `memory` (default) | RAM, as today | none | never |
| `hybrid` | runs on disk + memory part | `--index-cache-mb`, default 1024 | `--index-delta-mb`, default 256 |
| `disk` | runs on disk + memory part | `--index-cache-mb`, default 64 | `--index-delta-mb`, default 16 |

`hybrid` and `disk` are one mechanism with different budgets. An explicit
`--index-cache-mb` or `--index-delta-mb` overrides the residency's default.
`memory` changes nothing that exists.

## The one rule this relies on

Every index candidate is re-checked: the scan re-reads the row, checks its
visibility and evaluates the whole `WHERE`, and a UNIQUE probe compares the
row's bytes. **An index may list too much, never too little.** A stale entry
costs one fetch; a missing one is a silent wrong answer.

A paged index lists every slot the in-memory index would, and possibly more.
It never removes an entry from a file in place. An entry made stale (by an
update that changed the key, or by vacuum) stays in its file until the next
merge or rebuild drops it, and the re-check filters it out meanwhile.

## Structure

```
 lookups ──▶ memory part (BTreeMap<key, slots>)      newest
         ──▶ frozen parts (immutable, being written)
         ──▶ run N (file)
         ──▶ …
         ──▶ run 0 (file)                            oldest
```

- **Memory part.** Today's map: inserts land here, and `remove` removes from
  here. It holds the index's recent changes, budgeted by `--index-delta-mb`.
- **Frozen parts.** When the memory part reaches its budget it is swapped
  for an empty one, and the full one, now immutable, is written out as a new
  run by a background thread. Lookups read it until the run replaces it.
- **Runs.** Immutable files of `(key, slots)` entries sorted by key bytes,
  which is the order every index kind already sorts in (the hash's too, as v1
  snapshots write it). A key may appear in several runs; a lookup takes the
  union.
- **Merge.** More than `RUNS_MAX` (8) runs are merged into one by a
  background k-way merge. A merge drops every slot whose presence bit is
  clear (a slot vacuum forgot, see below). It keeps everything else,
  stale-but-present entries included.

The presence bitmap (one bit per slot) and the unkeyed list stay in memory, as
today: 125 MB at a billion rows.

### Why dropping on the presence bit is safe

`forget` is the only thing that clears a slot's bit, and vacuum calls it for a
version gone for everyone. `insert` lists the entry before it sets the bit. A
merge reads each slot's bit as it writes the slot's entries:

- **Bit clear:** the slot was forgotten, so its entries in older runs are dead.
- **Slot reused since:** a reused slot gets a fresh `insert`, whose entry
  lands in the memory part, not in the runs being merged, so dropping the old
  runs' entries loses nothing. The bit read is then set, so the old entries
  are kept as stale. That is harmless.

## Run file

```
header   magic "RDBIDXR1", version u32, method u8, n_entries u64, n_blocks u64
blocks   each: n u32, then n × (key_len varint, key, n_slots varint,
         slots as varint deltas), crc32c u32
fences   per block: key_len varint, first key, offset u64, length u32
footer   fences offset u64, fences length u64, crc32c of fences u32, magic
```

- **Blocks** target 16 KB (`hybrid`) or 64 KB (`disk`). A posting list too
  long for one block continues in the next under the same key, so a key may
  open several consecutive blocks.
- **Fences** are read into memory at open: about 50 bytes per block, so 65 MB
  for a 20 GB index at 16 KB blocks, or 16 MB at 64 KB.
- **Reads** use `pread` against an open handle. Each block's checksum is
  verified when it is first read, and a mismatch fails the lookup with an
  error rather than returning a short answer.

### Lookups

- **Equality:** find the first block whose fence key is below the key, then
  scan blocks while their fence key is at most the key.
- **Range** (B-tree, leading columns): the same, from the low bound to the
  high bound.
- **The union:** memory part, then frozen parts, then runs newest first. The
  existing caps (`candidate_cap`) apply to the union, and a cap reached stops
  early, as today.
- **`listed_under`** (planner costing) sums the posting lengths across
  sources.

### Block cache

A process-wide CLOCK cache of verified block bytes, keyed by
`(run id, block no)`, sharded 64 ways, bounded by `--index-cache-mb`. Runs
are never modified, so an entry is never stale. A dropped run's blocks
simply age out of the cache.

## Lifetime and crash

Run files live beside the table, in `<table>.idxr/`, named
`<index>.<generation>.run`, and are written to a temporary name and renamed.

**Working runs are trusted only by the process that wrote them:** they are
not synced, and every open deletes what an earlier process left in its
instance directories. Without a snapshot (`--index-storage rebuild`, the
default, or after a crash) a paged table rebuilds from rows. The build is the
normal insert path, so it spills runs and merges like any other write load:
an external sort, never the whole index in RAM. Queries scan until the build
has covered the existing rows, exactly as today.

### Across a clean restart (phase 2)

Under `--index-storage snapshot`, a seal captures a paged index as a **run
reference** rather than its entries, in the same INDEX section a memory index
fills ([Persistent indexes](persistent-indexes.md) §3), so the snapshot's
format, proof and plan are unchanged:

- **The seal** (writers held off by its gate) merges every source into one
  run in `<stem>.idxr/sealed/<index>.<n>.run`, written whole, synced, renamed
  and its directory synced before the snapshot that names it is. It is the
  index's sorted stream exactly, dropping nothing, as a memory index's section
  is. The section holds the file's name, length and CRC-32C, then the unkeyed
  slots and presence words. A cancelled seal leaves no file.
- **The open** whose v1 proof passes reads each section. A paged index takes a
  run reference whole: the length and the whole file's CRC-32C must match, the
  bits and unkeyed slots must lie inside the table, then the bits are restored
  before the run is adopted (the order a paged insert keeps). Anything else
  (a missing or short file, a mismatch) fails the load, and the table
  rebuilds, as every v1 load failure does.
- **Mixed residencies:** a run reference starts with a magic that is not an
  INDEX-section version, so a memory index refuses it and rebuilds; a paged
  index given a memory index's entries writes them as a run.
- **Sealed runs are kept, not deleted when dropped.** A seal keeps the two
  newest earlier ones (a seal may be skipped after writing its run, so the
  published snapshot can name the one before) and removes any older. A
  missing one costs a rebuild, never an answer: an adoption checks the file.

A crash is still a rebuild: a write after the seal makes the snapshot
untrusted (`DIRTY`, made durable before the write), exactly as in v1.

## Phases

1. **`LiveIndex` paged.** B-tree, hash and path indexes: runs, memory and
   frozen parts, merge, block cache, the knob and the budgets. Rebuild at
   every open. A model test drives the same random operations into a
   `memory` and a `disk` index with tiny budgets (many runs and merges) and
   checks every lookup: the paged answer contains the memory answer, and
   anything beyond it was once listed under that key. The compat model test
   for secondary indexes runs under `disk` residency as well.
2. **Persist across clean restarts** (built). A seal writes one sealed run
   and its reference in the INDEX section; an open whose v1 proof passes
   adopts it instead of rebuilding. A crash, or any write after the seal,
   rebuilds as v1 does. That is the crash model chosen: **rebuild after a
   crash**, no log replay. Under `RASTERIZEDDB_INDEX_RESIDENCY=disk` the v1
   snapshot, load and power-loss suites pass, the loads adopting runs; three
   audit tests that plant faults by editing the memory format's entries run
   with memory forced (`paged::force_memory_under`).
3. **Gin indexes paged** (built), on the same run format with each `u64`
   item's big-endian bytes as the key. Insert sets the bit first; forget
   only clears it (merges drop the slot, lookups skip it), so compaction and
   its forgotten-slot list stay in-memory only. Every list is read through one
   helper; one that cannot be read declines the lookup. List lengths are upper
   bounds, which only decline more or leave a list out of a walk. Gin indexes
   are still not snapshotted.
4. **Measured tuning:** block size, merge fan-in, and a second fence level
   if fence memory matters at the target.

## What this is not

- Not a WAL-logged B+tree: no index bytes in the log, and no recovery. A
  crash rebuilds.
- Not mmap. Reads are `pread` through the cache, so memory stays bounded by
  the knobs, not by the page cache's view of the process.
- Not a change to what an index answers. The planner, the re-check and the
  superset rule are untouched.
