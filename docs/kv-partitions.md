# Partitioned tables on a key/value store

Status: **§1 (placement), §2 (snapshots validated by the store) and §3
(auto-compaction per partition) built** (branch
`feat/kv-partitions`, from `feat/secondary-indexes` at 538d4590,
2026-11-15). See "§1 as built" at the end.

Asked for by oxyredox-v2: its per-PoP logger keeps `charges`,
`cache_results` and `access_logs`, each `PARTITION BY HASH (domain)`, on an
S3 store (`kv:logs`, one writer per prefix). Three things are needed, and the
user chose to build all three on one branch:

1. a partitioned table placed on `kv:<store>`;
2. boot without re-reading every row from the store: index snapshots and the
   persistent filter for KV-backed tables;
3. retention deletes that eventually give storage back, without a manual
   `VACUUM FULL`.

Line references below were read at 538d4590 and are relative to
`rasterizeddb_core/src/core/`.

## What stands in the way today

**1. Partitions are directory-only by construction.** No partition goes
through `seed_in`. Every partition and parent file is opened from the root
seed `db_io_pointers` (`partition/table.rs:450`, `database/partitioned.rs:368`),
created with `std::fs` by `files::create_partition_files` (`partition/files.rs:357`),
checked by `files::require_existing` (`files.rs:279`), sized by
`closed_lens` (`partition/table.rs:2442`, `std::fs::metadata`), deleted by
`files::remove_all` and found by the start-up reaper through directory scans
(`partitioned.rs:216, 672`). A partition's place is an interned absolute path in
the map log (`Record::Locations`, `Record::Materialize { loc }`).
`try_create_new_at` is `Unsupported` off files (`storage_providers/backend.rs:418`).
CREATE after a PLACE is refused by name (`partitioned.rs:451`), PLACE/MOVE of a
partitioned table by `refuse_partitioned`, reshape targets go through
`location_dir` (`partition/table.rs:2045`). Backup's cold copy finds KV tables
by name in `db_storage.cfg` (`backup/cold.rs:100`), which never lists a
partition, so a KV partition would be silently left out of a snapshot.

**2. The persistent-index protocol is built on directory files.** A KV table
gets `FileGate::detached()` because `StorageIO::index_store_dir()` is `None`
off files (`storage_providers/traits.rs:311`, `backend.rs:493`), and then seal
reports `Excluded` and the open proof `Reject::Excluded`. The protocol itself
(`index_store/`) is raw `File` I/O by design: a DIRTY append to `.ixm`
fdatasynced before the first post-snapshot write, `.ixs.tmp` + rename + dir
fsync before CLEAN, a WAL barrier, `rdb.ixe` read with `std::fs`. A KV store
has no rename, no WAL, and background writes (the 200 ms flush, spill,
compaction) that no gate sees.

**3. Nothing gives storage back but `VACUUM FULL`.** The free list is in
memory only and byte-granular (`vacuum.rs:179`); plain vacuum never moves a
row or a slot. The pointers file grows by 37 bytes on every INSERT and UPDATE
forever. For `access_logs` that is ~1 GB a day per node, on files as much as
on S3. Only `Table::compact` (VACUUM FULL, `row/table.rs:2545`) packs rows and
slots and truncates, and KV `truncate_to` deletes the chunk objects past the
new end (`kv/provider.rs:2501`). Auto-compaction exists (`auto_compact.rs`:
slots ≥ 20 000 and ≥ 8 × live, 600 s cooldown) but **skips partitioned
tables** (`auto_compact.rs:63`).

## Design

### 1. Placement

- `STORAGE PLACE TABLE t AT 'kv:<store>'` before `CREATE TABLE t … PARTITION BY
  HASH` places the **partitions' row and pointer files** on the store. The
  parent's files (schema, map log, ids, seq) stay in the root: they are small,
  WAL-logged, and the map is what says where everything else is.
- The map's location list gains backend specs beside paths: a location string
  `kv:<store>` names a store. `Materialize { loc }` points at it like any other
  location. A partition location is therefore one of `Dir(path)` or
  `Store(name)`, and every directory-only helper above gets a store arm that
  goes through the store's seed (`seed_on_other_backend`) by file name:
  create, exists (from the family's recovered names, never "reads as empty"),
  length, delete (`drop_io`), and the reaper (the family's listed names under
  `__pt_<uid>`).
- `STORAGE MOVE PARTITION … TO 'kv:<store>'` and back, and reshape `TO`, accept
  a spec; the copy is already `StorageIO` to `StorageIO`.
- The open budget keeps counting a KV partition (it holds memory, not fds).
- Backup: a snapshot copies KV partitions as it copies KV tables, or records
  them as not contained; never silently.

### 2. Snapshots for KV tables: validated by the store, not by markers

The file protocol's markers exist because a directory cannot say whether a
file changed after a snapshot. A KV file can: every stored record carries a
per-file sequence number, monotonic across restarts (`next_seq` resumes past
`compacted_upto` and every stored delta, `kv/provider.rs:1256`), and a
compaction reserves one too. So:

- **Identity.** A KV snapshot records, for every file of the table, the
  file's durable state `(base_gen, last stored seq, logical length)`, plus the
  schema incarnation and the index shape hash.
- **Seal** (clean shutdown, a partition's idle close, the idle checkpoint):
  under the table's exclusive lock, with **no open transaction anywhere in the
  family** (a transaction's records in this table's files can be declared
  committed by an object in another table's file), flush every pending record,
  read the identity, serialize the INDEX and FILTER sections with the existing
  codecs, and write them as one file `<stem>.ixs` in the same family
  (`write_data_txn` + `commit_durable`: visible whole or not at all).
- **Open:** recompute the identity from the recovered family state. Equal:
  load. Different, missing or unreadable: rebuild, as today.
- **Why it is safe.** Any write after the seal that reached the store is a
  record with a higher seq (or a new generation) in one of the table's files, so
  the identity differs and the snapshot is not used. A write lost before it was
  stored leaves the content the snapshot describes. Spurious mismatches (a
  compaction after the seal, an aborted spill) cost a rebuild, never an answer.
  A dropped-and-recreated name differs in incarnation.
- File tables keep the marker protocol unchanged.

### 3. Reclaim

- **Auto-compaction per partition.** Lift the skip: each partition is
  measured and compacted on its own (`VACUUM FULL` of one partition), under
  that partition's lock, so a retention-heavy partition shrinks both files and
  its KV tail chunks are deleted by `truncate_to`. At the 8× ratio the rows
  moved are at most an eighth of the slots, so the amortised rewrite is small
  for a log with retention. This fixes the pointers-file growth on files too.
- `VACUUM FULL` on a KV-backed table has never been tested; it gets tests.
- **Hole punching (stretch).** A freed extent covering whole chunks could be
  punched (a `punch` record, then delete the chunk objects; missing chunks
  already read as zeros). Equivalent to reusing the range with zeros, which
  the engine already tolerates. Not needed once auto-compaction runs; built only
  if measurement shows it matters.

## Order of work

1. Placement (§1) with the partition twin running on a directory-backed KV
   store; restart, DROP, MOVE PARTITION both ways, backup.
2. Auto-compaction per partition (§3), on files and on KV.
3. KV snapshots (§2), with a twin of reopen-after-write-after-seal and a
   mutation that skips the identity check.

## §1 as built

`partition/store.rs` holds the store arm: `PartStore`, an object-safe trait
the partition code reaches a store through (so `partition/` builds without
`kv_storage`), implemented by `KvPartStore` over a `KvFamily`. The pool's
`Placement` resolves a `kv:<store>` spec to one (`Placement::store`), deals a
table placed on a store that store (`Placement::deal(uid, table)`, by the
table's name in `db_storage.cfg`), and lists the stores this process opened
(`Placement::open_stores`).

- **Location.** A map location string that parses as `kv:<store>`
  (`store::store_spec`) is a store; anything else is a directory, as before.
  A slot's place is `Place::Dir(path)` or `Place::Store(StoreFiles)`, the
  latter holding the partition's two files open, so its lengths are atomics
  (`closed_lens`, `rows_len`, `totals`, `SHOW PARTITIONS` read them without
  awaiting).
- **Exists** (I9). `KvFamily::stands` / `standing_names` answer from the
  names recovered from the store and the files opened since;
  `KvStorageProvider::open_existing` never makes a name. The table's open
  (`slots_of`), `materialize`'s recorded branch and `open_instance` all
  open through it, and a missing file refuses the table with "partition
  file … is missing from key/value store '…'".
- **Create.** `store::create_partition_files` drops any standing leftover of
  the pid (the reopen waits out its sweep), opens each file fresh, and
  `KvStorageProvider::persist_existence` stores an object for it (a
  truncation to its own length) before `MATERIALIZE` is appended: a partition
  materialized with no row still stands after a restart.
- **Open.** The partition's providers are the store files themselves (the
  seed is its rows file). `External` partitions get no provider services, so
  `PartStore::start_service` starts the KV flush/compaction loop on the
  runtime the store was resolved on.
- **Delete.** DROP (`discard_uid_files`, now async) drops every standing
  name under the uid on every store the process opened, before the
  directories and the root; reshape (`discard_pid_files`) drops by pid.
  Drops are the family's: recorded on local disk before the sweep starts,
  resumed by the next start. DROP also forgets the name's placement.
- **Reaper.** `reap_stores_at_startup` runs before the directory reaper
  (which deletes a dead uid's map log, the only record of its stores), on
  every store a map in the root interns or a partitioned table is placed on:
  drops a live uid's data file the map does not place on that store, and
  every file of a uid with parent files in the root but no catalogue row. A
  uid with no trace in the root is never touched (one prefix, one
  database). Off while any live map cannot be relied on.
- **uid.** `CREATE` opens the placed store first (an unopenable store
  refuses the CREATE, not the first INSERT) and avoids uids already named
  there.
- **Reshape / MOVE.** `ReshapePlan` and `ReshapeFiles` carry
  `reshape::PartLoc` (`Dir` or `Store`); `TO 'kv:<store>'` is accepted when
  the store is configured; targets inherit a store source's place; the
  copy is `StorageIO` to `StorageIO` unchanged. `STORAGE MOVE PARTITION t
  FOR (…) TO 'kv:logs'` and back to a directory work.
- **Refusals kept.** `CREATE … PARTITION BY` after a directory or memory
  placement is still refused by name; `STORAGE PLACE TABLE` / `MOVE TABLE`
  of an existing partitioned table are still refused. `STORAGE DROP STORE`
  is refused while any partitioned table has a partition on the store.
  `REBALANCE PARTITIONS` skips store partitions.
- **RENAME** carries the placement to the new name after the catalogue
  commit point (lost to a crash there, later partitions go to directories).
- **Backup: not contained.** A snapshot does not copy store partitions. The
  cold copy reads every map log in the database directory and records each
  partitioned table with a store location as uncovered ("its partitions on
  key/value store(s) … are not copied by backups yet"), with a warning; a
  restore in place reads whatever the store holds then, as for any
  uncovered key/value table.
- **Not built** in §1 (§2 below). Opening a table
  opens every materialized store partition's two files (the family already
  holds their recovered deltas in memory).

Tests: `database/partition_store_tests.rs` (placement, restart, close and
reopen, a missing store file refused, DROP, the reaper, a store with
partitions not dropped, an empty materialization surviving a restart),
`tests/partition_kv.rs` (the statement kinds against a plain twin over the
wire, restart, idle close, MOVE PARTITION both ways, DROP), and the twin's
`*_on_kv_store` configurations.

## §3 as built

`auto_compact` measures every partition open at the pass
(`measure_partitions`: `Table::data`'s pointer length against the sweep's
live count, as for a plain table) and compacts a due one alone through
`executor::compact_partition`. A closed partition is never opened to be
measured or compacted.

- **Lock.** ACCESS EXCLUSIVE on the partitioned table, under an owner of
  its own, waiting at most `LOCK_WAIT_MS` (100 ms). Locks are per table
  name, and a transaction that wrote a partition holds ROW EXCLUSIVE on the
  name until it ends with that partition's slots in its write set, so
  nothing weaker keeps the compaction from moving them (a mutation to ROW
  EXCLUSIVE fails `a_busy_partitioned_table_is_skipped`). The hold is one
  partition's compaction, not the table's.
- **Cooldown** per `(database, table, pid)`.
- **On a store.** `Table::compact` truncates both files, and `truncate_to`
  deletes the chunk objects wholly past the new end. The chunk straddling
  the end keeps its tail (by design: rewriting it would cost a request to
  change bytes nothing reads), so a compacted file holds at most one chunk
  past its live bytes: 4 MiB at the default `CHUNK_SIZE`, per file, not
  growing.
- **Hole punching** is not built: with the partition compacted the
  remaining waste is that one chunk.

Tests: `auto_compact_tests.rs` (update-heavy partitions on files and on a
directory store, each compacted by one pass, the cooldown, the store's
bytes bounded by a chunk per file; a locked table and an open writer each
make the pass skip, not wait).

## §2 as built

Any table whose rows and pointers are key/value files is sealed this way,
partition or plain table: the store arm is the provider's, not the
partitions'.

- **State.** `StorageIO::kv_state_now` / `kv_settled_state` give a key/value
  file's `KvState` `(generation, next_seq, len)`. The settled read flushes
  the file's pending records under its flush lock, then answers only when
  nothing is pending, committing or aborting, no open or in-doubt
  transaction wrote the file, and no compaction is folding it. A recovered
  file's `next_seq` is one past the highest stored batch, which is what the
  running file's is once everything is stored; a reserved sequence that
  never landed (a failed put) makes the two differ, and the open rebuilds.
- **Seal** (`SealParts::seal_on_store`, from `Table::seal_snapshot` under
  the instance's `maintenance_tick`, so no vacuum sweep changes the
  structures without a write): structures ready and no revocation since
  open; both files settled (`before`); already clean when `before` is what
  the snapshot this instance stands on recorded and its sections are
  current; the sections captured; the outcomes barrier (the status log
  flushed and the root log synced: a version the structures dropped as dead
  stays dead); both files settled again and equal to `before`, else
  `Busy`. Any write in that window either left a transaction open (not
  settled) or stored a batch (a state moved). Stored as
  `kv_snapshot::encode`: magic, both states, crc32c, then the ordinary
  snapshot bytes, with the slot epoch fixed at 0 (a detached gate's epoch
  is per process; the sequences already move with every renumbering).
- **Where.** `<rows file>.ixs` in the table's family, written by
  `kv_write_sidecar`: cut to nothing, then one transaction only the sidecar
  holds, committed durably (so neither table file takes a sequence for
  it), then folded into base chunks so a restart does not hold it in memory
  as a recovered delta. A crash in between leaves a sidecar that fails its
  checks.
- **When.** Shutdown's seal, a partition's close, and
  `rdb_index_checkpoint`. Not the idle checkpoint: its watcher keys on the
  marker word a detached gate does not keep, so a store table idle for a
  long time is sealed when it closes or the server stops.
- **Open** (`Table::assemble`): both files' `kv_state_now`, read before
  this instance writes; the sidecar read whole (`kv_read_sidecar`, never
  made); `kv_snapshot::prove`: the header's crc, the recorded states equal
  to the recovered ones (`Reject::StoreState`, counted as `store_state`),
  the snapshot streamed and checked as a file's is, its identity equal to
  the files' and this binary's, and the FILTER section present exactly when
  the table has a filter. The INDEX sections are read from the held bytes
  (`SnapshotSource::Bytes`) by the ordinary load, so the snapshot's size is
  held in memory from the open until the load ends.
- **Drop.** `KvStorageProvider::drop_io` drops the file's sidecar with it,
  synchronously (a registered sidecar is dropped, a recovered one swept),
  so its sweep is recorded in `pending` before the drop returns: a sidecar
  never outlives its file to describe a later one of the same name. Should
  a lost sweep record leave one anyway, a partition's create drops its
  pid's sidecars with its other leftover names, and the store reaper drops
  a sidecar whose file does not stand.
- **INDEX sections** are written and loaded only under
  `index_snapshot.index_storage = snapshot` (`--index-storage snapshot`),
  as for file sets; the FILTER section always.
- **Rebuilds that cost only time.** A store compaction of a file after the
  seal (it reserves a sequence), any `VACUUM FULL` (every compacted
  partition writes), a failed put, or a write to a file: the open finds
  other states and rebuilds.
- **Why the states and not the lengths.** A delete, a `VACUUM FULL` and an
  insert of a row of the same size leave both files at the lengths the
  seal recorded, slots renumbered under them. With the state check removed,
  `sealed_store_partitions_load_their_indexes_and_a_later_write_rebuilds`
  loads all four compacted partitions' stale snapshots.

Tests: `kv_snapshot` (the codec and the proof's refusals),
`partition_store_tests` (sealed partitions load; a write after the seal
rebuilds and answers through its index; the same-lengths rewrite; a plain
table on a store; DROP sweeps the sidecar), `tests/partition_kv_snapshot.rs`
(over the wire at the store's default tuning: the shutdown seal, two
restarts, written partitions rebuild, a reseal loads again).
