# Row and column compression

Status: branch `wip/compression` off dev 219ea81f (2026-11-23). **Phases 1–4 are
built** (column compression, RECOMPRESS, row compression, dictionaries): §11
says what exists, how to use it, and where it departs from the plan below.
Phase 5 (columnar segments) is not started.

Decided by the user, 2026-11-23:
- it is a Cargo feature (`storage_compression`);
- **nothing is compressed by default**: only a column that asks;
- the degree of compression is chosen per column (a zstd level);
- the client's ORM declares it too.

## 1. What the storage looks like today (what the plan has to fit)

- A row is one extent in `{t}.db` at `RowPointer.position`, `length` bytes
  (`row/row_pointer.rs:845-887`). The pointer record is 37 bytes
  (`id u64 | length u32 | position u64 | deleted u8 | xmin u64 | xmax u64`)
  and has **no spare bit**: `deleted` is read as `!= 0`.
- Inside a row: fixed-width cells at schema offsets
  (`SchemaCalculator::calculate_schema_offset`, `row/schema.rs:353`), then the
  variable tail. A variable-width column (`DbType::physical() == STRING`:
  TEXT, BYTEA, JSONB, arrays, ranges, BIT/VARBIT, PATH/POLYGON, BPCHAR…) has a
  12-byte header cell: `u64 offset from row start | u32 length`
  (`STRING_HEADER_BYTES`, `row_pointer.rs:805`). Readers refuse a length over
  64 MiB (`string_extent`, `row_pointer.rs:815`), so **bits 26–31 of that
  u32 are never set in a readable row**.
- NULL is a hidden BOOL companion cell `__null_x`, not a bitmap.
- The hot read paths read **cells, not rows**, at `position + column_offset`:
  - the batch prefilter (`processor/prefilter.rs:487-550`, fixed-width only;
    with `decides()` its mask is the answer and the row is never read);
  - cell-fold aggregates and top-N key cells (`processor/column_scan.rs`,
    `try_read_cells_sync`);
  - `fetch_row_reuse_async` / `fetch_row` (two vectored passes: fixed cells +
    string headers, then string bytes);
  - the fixed-width in-place UPDATE (`Table::write_columns_in_place`,
    `table.rs:3193`).
- Only `row/row_pointer.rs` and `processor/column_scan.rs` decode string
  headers (`string_extent` / `STRING_HEADER_BYTES`). Everything above them
  (executor, indexes, filter backfill, unique probes, FK checks, vacuum's
  index forget, catalogue rows, reshape's partition-key fetch) receives the
  decoded `Row`.
- Rows are always copied into pooled `MemoryBlock`s. Nothing borrows row
  cells from the mmap (only pointer records are borrowed), so decompressing
  into the block loses no zero-copy path.
- The WAL is physical redo of the stored bytes. Backup, branching, STORAGE
  MOVE, mirror resync, partition reshape's raw copy, KV storage and
  `Table::compact` phase 1 all move stored bytes without decoding them.
  Replication ships SQL. So **every one of these is compression-agnostic** as
  long as the stored row stays self-describing.
- Field records (`schema.rs:13-34`, 153 bytes): byte 130 is a flag byte;
  bit 0 = TIMESTAMP w/o TZ, bit 1 = JSON text, **bits 2–7 free**.
- `incompat_features` (`database/incompat.rs`) already fences directories a
  build cannot keep correct (`index_store_v1`, `numeric_own_scale_v1` — the
  latter is the precedent for a per-column storage change).
- zstd 0.14 is already a dependency (optional, behind `replication_compression`
  and `s3_backup`); there is no shared codec helper.

## 2. The switch: a Cargo feature

The whole subsystem is the Cargo feature **`storage_compression`**, defined
in `rasterizeddb_core/Cargo.toml`:

```toml
storage_compression = ["dep:zstd"]   # in `default`, like every other subsystem
```

- **Built without it:** no `core/compression` module and no compression
  branch in the read and write paths. zstd is not linked unless backup or
  replication pulls it in.
  - The SQL `COMPRESSION` clauses are still **parsed and accepted as
    no-ops**, so pg_dump output and the PG importer keep working. Nothing is
    stored.
  - A directory carrying `compressed_values_v1` is refused at open (§3.4).
- **Built with it:** the per-column and per-table settings below are
  available.
- Check both builds every time:
  - `cargo check -p rasterizeddb_core --no-default-features --all-targets`
    (with the other defaults re-listed);
  - the default build;

  as [workspace-check-unifies-features] describes.

## 2b. The decision: compress variable-width values, keep the fixed region raw

**Column compression** means per-value compression of variable-width columns
(TOAST-style). **Row compression** means compressing a row's whole variable
tail as one frame. In both, the **fixed-width region stays raw**.

Why not compress whole rows (fixed cells included):

1. It would break every cell path above: the prefilter's SIMD compares, the
   exact-mask shortcut, cell-fold aggregates, top-N key cells,
   `try_read_cells_sync` and the in-place fixed-width UPDATE. Each would need
   a "compressed row ⇒ fall back to whole-row decode" branch. The exact-mask
   shortcut is the dangerous one: it returns a mask without reading the row,
   so a missed branch there is a silent wrong answer, not an error.
2. It gains almost nothing. The fixed region is 1–8-byte numbers, BOOL
   companions and 12-byte string headers. A single row is tens to a few
   hundred bytes, and zstd/LZ4 on that alone has no history to match against.
   The bytes worth compressing are the text, JSONB, BYTEA and arrays in the tail.

Real compression of numeric columns needs many rows of one column side by
side (delta, frame-of-reference, bit-packing). That is a columnar segment
format, which is a separate project (§8, Phase 5).

## 3. On-disk format

### 3.1 Compressed value (column compression)

String header: `u64 offset | u32 length`. **Bit 31 of `length` set ⇒ the
payload is a compressed frame**, and the low 26 bits are the stored
(compressed) length.

```
payload = codec u8 | dict u16 (0 = none) | raw_len u32 | frame bytes
```

- `raw_len ≤ 64 MiB` is checked **before** allocating (see
  [ci-runners-expose-oversized-allocations]: an input-sized allocation
  aborts on 16 GB runners).
- Codec ids: `1 = zstd`, `2 = lz4` (reserved), `0` invalid.
- A value is stored raw unless all of these hold:
  - `raw_len ≥ min_value_bytes` (default 128, or 32 with a dictionary);
  - compressing saves at least 1/8 of the bytes;
  - the column's codec is not `none`.

  So incompressible or small values cost nothing, and one column holds a mix
  of raw and compressed values.
- An older build reading bit 31 sees a length over 64 MiB and **errors**. It
  never reads garbage. The `incompat_features` flag (§3.4) makes that refusal
  happen at open instead.

### 3.2 Compressed tail (row compression)

For tables with many small strings per row, where no single value reaches
the threshold:

- **Bit 30 of a header's length set ⇒ the offset/length address the row's
  *decompressed* tail**, not stored bytes.
- The stored tail starts at the fixed region's end (known from the schema)
  and is one frame: `codec u8 | dict u16 | raw_len u32 | frame`.
- A row is compressed whole-tail or not at all. If any header has bit 30,
  all of them do, and the reader decompresses the tail once per row fetch.
- Trade-off: reading one string column decompresses every string of the row.
  That is why this is a **table option**, not the default.

### 3.3 Per-column and per-table settings

- **Per column:** field byte 130 bits 2–4 hold a 3-bit codec choice:
  `0 = default` (follows the table/global setting), `1 = none`, `2 = zstd`,
  `3 = lz4`. These are rewrites of the field record, which DDL already does.
- **Per table:** a new sidecar `{t}_OPT.db` holding the table options:
  `row_compression`, `min_value_bytes`, `level`. It is the first persisted
  table-options record, and it is also where a parsed `WITH (…)` can land
  later. **The new suffix must be added to `RENAMED_SUFFIXES`, the
  copy/branch/STORAGE MOVE file lists and the drop path**
  ([renamed-suffixes-misses-lazy-sidecars]).
- **Global:** none. Nothing is compressed unless a column or table says so.

### 3.4 Fencing

- `incompat_features` gains `compressed_values_v1`, required durably the
  first time a column or table is set to anything other than `none`. Mark at
  DDL, not at first compressed write: the DDL is rare, and a write-path
  check would be per row.
- A build without the `storage_compression` Cargo feature refuses to open a
  directory carrying the flag, the same way `persistent_indexes` does.
- Dictionaries (Phase 4) get their own flag, `compression_dicts_v1`.

## 4. Code shape

- **New module `core/compression/`**, behind Cargo feature
  `storage_compression = ["dep:zstd"]`, in `default`:
  - `codec.rs`: `encode(col_cfg, raw) -> Option<Frame>` and
    `decode_into(frame, &mut MemoryBlock)`. Thread-local reused
    `zstd::bulk::{Compressor, Decompressor}` contexts, as `FrameCodec` does
    in `replication/protocol.rs:851`.
  - `format.rs`: header bit constants and parse/validate of the frame
    preamble. This is the only place that knows bits 30/31.
  - Without the feature: `encode` is `None` and `decode_into` returns an
    "unsupported" error. The incompat fence makes that path unreachable on a
    real directory.
- **Write path:**
  - First, **unify the two row encoders**: the inline encoder in
    `write_row_with_pointer_record_pos` (`row_pointer.rs:2328-2401`) and
    `build_row_data` (`1401-1469`, the in-place rewrite). Today they
    duplicate the layout. Compression must happen in exactly one of them.
  - Then compress in that encoder, before `RowPointer::new` computes
    `length`. Compression runs outside every lock: it happens while the row
    bytes are built, before the extent is allocated.
  - Fix a pre-existing bug on the way: the writer stores a string over
    64 MiB that no reader will return. Refuse it with `54000`.
- **Read path (the two choke points):**
  - `fetch_row_reuse_async` / `fetch_row` pass 2. After `string_extent`,
    if bit 31 is set, read the stored bytes into scratch and decompress
    into the row's block. If bit 30 is set, read the stored tail once,
    decompress it, and serve every string column from it.
  - `column_scan::read_onto`'s string pass gets the same treatment, through
    the same helper. One function, called from both places, so they cannot
    disagree.
  - `string_extent` learns to mask bits 26–31 before its 64 MiB check and to
    return the flags.
- **Nothing else decodes headers**, so the executor, indexes, filter
  backfill, unique/FK probes, vacuum forget, reshape and catalogue reads need
  no change. Phase 1 adds a test that greps for any new reader of
  `STRING_HEADER_BYTES` outside these files, an allowlist in the style of
  [a-guard-is-an-allowlist].
- **Unaffected by construction** (prove each with a test, do not assume):
  - the prefilter and exact mask (fixed-width only);
  - cell-fold aggregates (refuse `STRING` columns);
  - the fixed-width in-place UPDATE;
  - the WAL, backup, branching, mirror, KV storage and compaction's raw move.
- **Point keys** bind literal bytes and compare them after the normal fetch,
  which decompresses. A string primary key or unique column must therefore
  never reach a path that compares *stored* bytes. Audit
  `point_key::key_type_allowed` and the unique probes for that. Any byte
  comparison of a string against storage is a finding.

## 5. SQL surface (PostgreSQL syntax, so pg_dump, ORMs and the importer work)

```sql
CREATE TABLE t (doc JSONB COMPRESSION zstd, note TEXT COMPRESSION none);
ALTER TABLE t ALTER COLUMN doc SET COMPRESSION zstd;   -- also lz4 | pglz | default
ALTER TABLE t SET (row_compression = zstd);            -- tail mode, §3.2
CREATE TABLE t (...) WITH (row_compression = zstd, compression_min_bytes = 64);
ALTER TABLE t RECOMPRESS;        -- rewrite existing rows under the current settings
VACUUM (FULL, RECOMPRESS) t;     -- same, as part of compaction
SELECT pg_column_compression(doc) FROM t;              -- 'zstd' / 'lz4' / NULL
```

- As in PostgreSQL, `SET COMPRESSION` affects **new values only**. Existing
  rows change on `RECOMPRESS`, `VACUUM FULL … RECOMPRESS`, or when they are
  next updated.
- `pglz`/`lz4` from `pg_dump` and the PG importer are accepted and mapped to
  the engine codec. Rejecting them would break `pg_import` on any PG14+ dump
  that names a compression method.
- `SET COMPRESSION` on a fixed-width column is accepted and is a no-op, as
  PostgreSQL does for plain-storage types. It must not error, because
  pg_dump emits it.
- Catalogues: `pg_attribute.attcompression` (`'z'`/`'l'`/`''`),
  `attstorage`, and `psql \d+`'s Compression column.
- `SHOW TABLE STATS` adds `raw_value_bytes`, `stored_value_bytes` and
  `compressed_values`, counted by the writer and re-measured by the vacuum
  sweep the way `live_rows` is.
- `CREATE TABLE … WITH (…)` is currently mis-parsed (pinned in
  `rql/lexer_ct.rs:2167-2180`, "so the fix, when it comes, is a decision").
  Phase 3 is that decision: parse the group, accept `row_compression` and
  `compression_min_bytes`, and accept-and-ignore `fillfactor` and the PG
  storage options pg_dump emits. Update that pinned test deliberately.

## 6. Rewriting existing rows

- `Table::compact` phase 1 (`table.rs:2787-2849`) moves raw bytes, and
  vacuum's move plan requires target ≤ source. A recompressing compaction
  decodes the tail, re-encodes it under the current settings, and writes the
  result only when it is **not longer** than the original. Otherwise it moves
  the row unchanged. That keeps the move plan valid without a second plan.
- `ALTER TABLE … RECOMPRESS` reuses the ALTER rewrite machinery
  (`read_rewrite_versions` → `insert_version`), which already decodes and
  re-encodes. That is the simple, correct path. Compaction is the cheap,
  online one.
- Free-space extents are matched by exact size (`alloc_after`). Compressed
  rows vary more in length, so expect reuse to drop. Measure fragmentation
  (`reclaimable_bytes`) in the benchmark before calling the feature done.

## 7. Defaults and codec choice

- **Codec: zstd first.** It is already in the tree, so there is no new
  crate. Level 1 by default (`level` table option, 1–9): at level 1 it
  decompresses at ~1–1.5 GB/s per core and compresses JSON/text 3–6×.
- `lz4` keeps codec id 2 reserved and is accepted in SQL as an alias for
  zstd. Add `lz4_flex` (pure Rust, small) only if the profile shows
  decompression is a top-5 cost on a text scan.
- **Nothing is compressed by default** (decided 2026-11-23). A column or
  table asks, and says how hard (a zstd level).

## 8. Phases (each a local commit on `wip/compression`, `cargo check --all-targets`, handed to the integrator)

1. **Codec + column compression.** Feature, module, header bits, the
   encoder unification, both read choke points, field-byte codec, incompat
   flag, `COMPRESSION` in CREATE and `ALTER COLUMN SET COMPRESSION`,
   `pg_column_compression`/`attcompression`, the >64 MiB writer refusal, and
   the stats counters.
2. **Rewriting.** `ALTER TABLE … RECOMPRESS`, and `VACUUM (FULL, RECOMPRESS)`
   with the not-longer rule. Auto-compact does not recompress unless
   `auto_compact.recompress = on`.
3. **Row (tail) compression.** Bit 30, the `{t}_OPT.db` sidecar,
   `WITH (…)` / `ALTER TABLE SET (…)` parsing, and `RENAMED_SUFFIXES` /
   copy lists.
4. **Dictionaries.** Per-column zstd dictionaries trained from a sample
   during `RECOMPRESS` / `VACUUM FULL RECOMPRESS` (`zstd::dict`), stored
   append-only in `{t}_DICT.db` by id and never deleted while referenced.
   This is what makes 30–120 B JSON/text values compressible, and it is the
   biggest ratio lever for a row store. Own incompat flag.
5. **(Separate design, not started)** Columnar segments for frozen data:
   delta/FOR/bit-packing for fixed-width columns. It needs its own pointer
   semantics and its own cell-read paths, so it is out of scope here.

## 9. Tests and measurements

- **Differential, the main guard:** run the same workload with compression
  on and off. Every SELECT must return byte-identical results through
  `execute()` and pgwire, across:
  - TEXT, JSONB, BYTEA, arrays and ranges, NULLs and empty strings;
  - non-ASCII text;
  - values at the threshold ± 1;
  - incompressible (random) values;
  - mixed raw/compressed rows after `SET COMPRESSION` mid-table.

  Also run the PG17 diff harness (`.claude/reviews/pgdiff`) with the global
  default flipped on.
- **Reopen tests:** write, close, reopen, read ([reopen-exposes-restart-only-bugs]).
- **Downgrade test:** a directory with `compressed_values_v1` is refused by
  a build without the feature (`downgrade_tests.rs`).
- **Path tests**, each with compressed rows, each asserting it engaged
  (counter) and matched the oracle:
  - unique and PK probes on compressed string keys;
  - index build and `forget_reclaimed`;
  - filter backfill;
  - the in-place UPDATE (both kinds);
  - the UPDATE that grows a row;
  - compaction moves;
  - partition reshape;
  - branch create, then read;
  - STORAGE MOVE, then read;
  - WAL replay after SIGKILL;
  - power-loss images.
- **Seam tests** for any new `pub` method on `Table` ([table-pub-methods-seam-rule]).
- **Mutation round** on `compression/format.rs` and the two choke points.
- **Benchmarks.** Use `perf stat -e instructions:u,cycles:u`, not wall time
  ([how-to-profile-this-repo]). Measure:
  - JSONB/TEXT scan and point read (cached and out-of-cache);
  - INSERT throughput;
  - bytes on disk and `reclaimable_bytes` after an update-heavy run.

  For data, use a 1M-row synthetic table, plus a real import: the Kostas
  app's tables or a PMS dump through `pg_import`.

## 10. Open decisions (for the user)

- Decided 2026-11-23: a Cargo feature (`storage_compression`, §2); nothing
  compressed by default; a level per column; the ORM declares it.
- zstd only, or also add `lz4_flex` for a faster-decode codec?
- Is Phase 5 (columnar segments for numeric data) wanted, or out of scope?

## 11. As built (Phases 1–4)

Phases 1–4 are built. Phase 5 (columnar segments) is not: it was always a
separate design.

### Using it

```sql
-- Per column: each value its own frame. 1 = fastest .. 22 = smallest; zstd = zstd(3).
CREATE TABLE docs (id INT PRIMARY KEY, body JSONB COMPRESSION zstd(9), note TEXT COMPRESSION zstd);
ALTER TABLE docs ALTER COLUMN note SET COMPRESSION zstd(19);
ALTER TABLE docs ALTER COLUMN note SET COMPRESSION default;      -- stop (new values)

-- Per row: every text/jsonb/bytea/array value of a row in one frame.
CREATE TABLE events (id INT, kind TEXT, src TEXT, msg TEXT) WITH (row_compression = 'zstd(6)');
ALTER TABLE events SET (row_compression = 'zstd');
ALTER TABLE events RESET (row_compression);

-- Rewrite existing rows under the current settings, training dictionaries.
ALTER TABLE docs RECOMPRESS;
VACUUM (FULL, RECOMPRESS) docs;

-- What is set, and what it gets.
SELECT attname, attcompression FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid WHERE c.relname = 'docs';
SELECT reloptions FROM pg_class WHERE relname = 'events';        -- {row_compression=zstd(6)}
SHOW TABLE STATS docs;  -- compressed_columns, row_compression, compressed_values,
                        -- compressed_row_tails, compression_raw_bytes, compression_stored_bytes
```

```rust
#[derive(Model)]
#[rdb(table = "events", row_compression = "zstd(6)")]   // WITH (row_compression = 'zstd(6)')
struct Event { #[rdb(primary_key)] id: i32, kind: String, src: String, msg: String }

#[derive(Model)]
struct Doc {
    #[rdb(primary_key)] id: i32,
    #[rdb(json, compression = "zstd(9)")] body: serde_json::Value,   // COMPRESSION zstd(9)
    #[rdb(compression)] note: String,                                 // COMPRESSION zstd
}
```

The migrator compares the model with `attcompression` (columns) and
`reloptions` (tables). It issues `SET COMPRESSION` or `SET`/`RESET
(row_compression)` under `SyncPolicy::compression`, which is on by default and
in `exact()`, and off in `additive()` and `report_only()`. It never issues
RECOMPRESS: a rewrite is the operator's call.

### Behaviour

- **Nothing is compressed by default.** A build with the feature and no
  setting writes byte-identical rows, and the write path makes no copy.
- `zstd` means level 3. `lz4` and `pglz` (from pg_dump) are taken as zstd(3).
- **Thresholds:** a value, or a row tail, is stored compressed only when it
  is at least 64 bytes and the frame saves at least 1/8 of its size.
  Otherwise it is stored as it was.
- **Row compression wins** over column compression for a row: the tail frame
  holds every value.
- **What is refused:**
  - a fixed-width column with `COMPRESSION zstd` (as PostgreSQL refuses it);
  - a level outside 1–22;
  - an unknown method;
  - an unknown `WITH` option.

  PostgreSQL's own storage options (fillfactor, autovacuum_*, toast.*, …)
  are accepted and ignored.
- **New values only:** `SET COMPRESSION` and `SET (row_compression)` affect
  values written from then on. Old rows keep their bytes until RECOMPRESS,
  and a table holds every kind at once.
- **RECOMPRESS** is the ALTER rewrite: every visible version is rewritten
  with its own xmin/xmax under ACCESS EXCLUSIVE. Before writing, it trains
  a dictionary per compressed column and one for the row tail, from up to
  4096 values or 8 MiB. It keeps a dictionary only when it beats
  no-dictionary by at least 10% on the samples, and retires the old ones.
  `VACUUM (FULL, RECOMPRESS)` runs it per table; a rewrite also compacts.
- **Through DDL:** RENAME COLUMN keeps a column's setting and dictionary,
  DROP COLUMN retires them, and ALTER rewrites carry them.
- **Without the feature,** every clause is accepted and ignored, so pg_dump
  output and the importer still load.

### Format

- **Settings** are marker records in `_schema_TYP.db` (never the field byte
  §3.3 proposed, and no `_OPT.db`):

  | Key | `precision` | `scale` |
  |---|---|---|
  | `\u{1}compression:<col>` | codec 1 | level |
  | `\u{1}row_compression:` | codec 1 | level |
  | `\u{1}dictionary:<col>`, or `\u{1}dictionary:` for the tail | id, low 16 bits | id, high 16 bits |

  Binaries that predate them never meet them. `column_declarations::other_marker_keys`
  carries the per-column ones through rename and drop.
- **In memory:** `TableSchema::compression`, a `StoragePolicy` holding the
  columns, the row codec and shared counters. `refresh_compression` rebuilds
  it at load and at CREATE.
- **Dictionaries** are stored in `_schema_ZD.db`. Each record is
  `RZD1 | id | len | crc32 | bytes`, and a reader scans for the magic, so a
  torn record is stepped over. The sidecar is in `RENAMED_SUFFIXES`, and is
  opened only for a table whose markers name a dictionary. The id is zstd's
  own content-derived dictionary id.
  - Ids are registered process-wide (`compression::register`), which is how
    a reader that only has a row's bytes finds the dictionary. A conflicting
    id is refused.
  - `write_schema_sidecars` copies the dictionaries a definition names into
    the new table's sidecar, which covers rewrites, partition shells and
    copies.
- **String header, length word:**
  - bit 31 set: the stored bytes are a frame of this value, and the length is
    the frame's;
  - bit 30 set: the value is in the row's tail frame, and the length is the
    value's own. The offset holds the frame start relative to the row in its
    high 32 bits, and the value's position in the decompressed tail in its
    low 32 bits.

  Both bits set, or a length over 64 MiB, is an error. A build without the
  feature fails on either bit, never reads wrong text.
- **Frame:** `codec u8 | raw_len u32 | [dict id u32 when codec = 2] | zstd frame`,
  with zstd's content checksum on.
- **Fence:** `compressed_values_v1` in `incompat_features`. It is set at:
  - CREATE (plain, partitioned and rewrite paths);
  - `SET COMPRESSION` and `SET (row_compression)`;
  - table open;
  - `fence_restored_directory`, which scans `_schema_TYP.db` files.

### Code

- `core/compression.rs` holds the codec, the policy, the parsers, the
  dictionary registry, training and the sidecar.
- The `RowPointer` write paths:
  - `RowPointer::encode_row` is the one place a row's stored bytes are
    built: plain, per-value frames, or a tail frame;
  - `write_row_with_pointer_record_pos_compressed` and
    `update_with_pointer_record_pos_compressed` are additive. The old
    signatures never compress; the WAL and catalogue writers use them;
  - the write and update bodies take the stored bytes, so a relocating
    update is encoded once.
- Reads decode in `fetch_row`, `fetch_row_reuse_async` (`fill_from_tails`)
  and `column_scan::read_onto` (tail values, then `decode_frames`). All three
  go through `string_cell`, and nothing else reads string headers.
- `build_row_data` is the one encoder: the inline copy in the write path was
  folded into it.
- The writer refuses a value over 64 MiB, which no reader would return. This
  was a pre-existing bug.

### Dropped or not built

- **`auto_compact.recompress`:** recompression is the offline rewrite, so the
  automatic compactor never does it.
- **The `compression_min_bytes` option:** the threshold is fixed at 64 bytes.
- **`pg_column_compression(value)`:** the executor only ever sees decoded
  values, so this needs storage-level information it does not have.
  `attcompression` and `SHOW TABLE STATS` say what is set and what it got.
- **Online (in-compaction) recompression:** see above.
- **Phase 5, columnar segments:** not started.
