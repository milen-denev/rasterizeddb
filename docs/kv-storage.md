# Key/Value Storage Backend — Design & Operation

Status: **implemented and selectable per table, off by default.** Build with
`--features kv_storage` (in-process and local-directory stores) or
`--features kv_storage_s3` (adds the S3 backing store). Per-table placement also
needs `storage_pools`, which is on by default.
Target: `rasterizeddb_core::core::storage_providers::kv`, edition 2024.

## 0. What exists

| Component | State |
|---|---|
| `kv::store` — the `KvStore` trait, `MemoryKvStore`, `LocalDirStore` | **Done.** §2 |
| `kv::s3_store` — `S3KvStore` over the existing SigV4 client | **Done.** §7 |
| `kv::layout` — key naming, delta and metadata codecs | **Done.** §3 |
| `kv::overlay` — the non-overlapping range map writes land in | **Done.** §4 |
| `kv::cache` — the shared, byte-budgeted page cache | **Done.** §5 |
| `kv::provider` — `KvConfig`, `KvFamily`, `KvStorageProvider` | **Done.** §4–6 |
| `DbStorageProvider::KeyValue` variant | **Done.** §9 |
| Per-table placement, `db_kv.cfg`, `STORAGE PLACE TABLE … AT 'kv:…'` | **Done.** §9 |
| Crash-safe commit spanning several files | **Done.** §6 |
| Compaction, delta retirement, recovery | **Done.** §4.4, §4.5 |
| `create_temp` / `swap_temp` without a rename | **Done.** §4.6 |

Not done: a multi-writer lease (§8) and a local disk cache tier (§5). Backup and
relocation deliberately do **not** cover a key/value table — see §9.

## 1. Why an object store needs a different layout

The `StorageIO` trait is a byte-addressable random-access file: write at an
offset, append and learn the offset, read a range, truncate. An object store
offers none of those. It offers whole-object put, ranged get, and delete.

Three properties of the engine's write pattern decide the design:

- The most common write is tiny and in the middle of a file — flipping a status
  byte on a row pointer, or stamping a version. Storing a file as one object
  would re-upload the table to change one byte; storing it as N chunk objects
  would re-upload a chunk.
- A transaction touches two files (row bytes and pointer) and commits on one of
  them. Neither half means anything alone.
- Reads are a mix of dense sequential (pointer scans) and scattered small
  (rows), and an object store answers any request after a latency floor that has
  nothing to do with how much was asked for.

So: base chunks are never written on the commit path, a commit is one object per
file describing the bytes that changed, and reads are served from a page cache
in front of ranged gets.

## 2. What a backing store has to provide

```rust
trait KvStore {
    fn get(&self, key) -> Option<Vec<u8>>;
    fn get_range(&self, key, offset, len) -> Option<Vec<u8>>;
    fn put(&self, key, body);
    fn delete(&self, keys);
    fn list(&self, prefix) -> Vec<ObjectInfo>;
    fn concurrency(&self) -> usize;
}
```

Deliberately the smallest interface an object store satisfies. No rename, no
append, no partial write, no compare-and-swap — S3 has none of them, so a trait
that assumed them could only be implemented by something that is not an object
store. Everything that makes the driver usable sits above this line and is
written once.

Two contract points carry weight:

- **`put` is atomic.** A reader sees the whole previous value or the whole new
  one. All crash safety rests on this; every S3-compatible store gives it.
  `LocalDirStore` earns it by writing a sibling and renaming.
- **A missing key is `Ok(None)`, not an error.** A sparse file's unwritten
  chunks are simply absent, and treating that as a failure would make every hole
  an outage.

Three implementations ship: `MemoryKvStore` (a map), `LocalDirStore` (a file per
object), and `S3KvStore`. The first two are how the driver is tested — the whole
of it, including recovery and compaction, runs under `cargo test` with no
network.

## 3. Key layout

```text
<root>/<file>/META                current base generation and length
<root>/<file>/g<gen>/c<index>     base chunk, chunk_size bytes
<root>/<file>/d/<seq>             delta batch
```

Sequences and chunk indexes are zero-padded hex so a lexicographic listing — the
only ordering an object store offers — is also the numeric one.

A **delta batch** holds records (`write at offset`, `truncate to length`), each
tagged with the transaction that produced it, plus the list of transactions this
batch declares committed. Header carries a CRC over the payload; a batch that
does not verify is treated as *uncommitted*, not as an error, because the only
way to see a partial batch is a store that broke its put-atomicity contract and
the safe reading of a batch we cannot trust is that its transactions never
finished.

## 4. The three layers

Every read is answered newest-first from:

```text
  live overlay    writes since the last compaction started
  frozen overlay  writes the compaction in flight is folding
  base chunks     objects in the store
```

### 4.1 Writing

A write inserts into the live overlay and queues a record. Nothing is read to be
written — there is no read-modify-write of a chunk anywhere on the write path.
The append offset comes from an atomic `fetch_add` on the logical length, the
same allocator discipline `file_sync` uses and for the same reason: two
concurrent appends must not describe each other's bytes.

### 4.2 Committing

`commit_durable` writes one delta object per file the transaction touched. One
put per commit per file, whatever the commit changed and however far apart the
changed bytes were — a transaction poking one byte into each of ten chunks is
still one object.

### 4.3 Reading

The read path resolves *pages*, not chunks. A chunk is the object; a page
(default 64 KiB) is the unit of caching and of ranged reads. Per chunk, the read
path counts how many of its pages a batch actually wants: a few scattered ones
are fetched as ranged reads, most of them as one whole-object get that is split
into pages on arrival. A batch's fetches all go out together, up to the store's
concurrency.

Sequential reads arm read-ahead, which fetches the next chunks whole behind the
scan.

### 4.4 Compaction

Runs in the background, and folds overlay bytes into base chunks:

1. **Freeze.** The live overlay's committed portion is set aside and writes carry
   on against a fresh one, so nothing below has to reason about a range changing
   underneath it. Only ranges below the lowest offset any open transaction has
   written are frozen — folding an uncommitted byte into a base chunk would
   publish a transaction that never finished.
2. **Rewrite.** Chunks carrying enough overlay are read, overlaid, and put back.
3. **Publish**, in this order: whatever could not be folded goes out as one
   consolidated batch, then `META` (advancing `base_len`, and the watermark below
   which deltas may be ignored), then the superseded deltas are deleted.

Order is load-bearing. Dying anywhere in it recovers, because a record is
physical redo: base chunks written but not yet published are simply re-covered by
the deltas that are still there, and applying a record twice is applying it once.

Delta retirement waits for a moment with no open transaction. An older delta may
still hold records of a transaction that is open right now, and deleting it would
leave a commit declaration with nothing to commit. The chunks and the leftovers
are published either way, so a pass under load still makes progress.

### 4.5 Recovery

Family-wide and up front: one listing plus one get per delta object for the whole
database, before any file is opened. Per-file recovery could not work — see §6.

Batches are ordered by sequence and applied, keeping a record only if some batch
*anywhere in the database* named its transaction. The transaction counter resumes
past everything the store has seen; restarting it would let a fresh transaction
take an id a surviving batch already declared committed.

### 4.6 Scratch files and `swap_temp`

`create_temp` returns a new **generation of the same file**, not a differently
named one, and `swap_temp` publishes that generation by writing `META` and then
deleting the old generation's objects.

The obvious implementation — scratch under its own name, then swap the two names
— cannot work: an object store has no rename, so adopting the scratch file means
either copying every object into place or leaving the live data under a name
nothing reopens. The second is what the first version of this did, and it reads
as a table that comes back empty after a restart.

Sharing a key prefix between a file and its scratch is safe because every delta
batch records the generation it describes and recovery skips the ones that do not
match the live generation: before a swap the scratch batches are inert, after it
the originals are. The two share one sequence counter so they cannot take the
same delta key. `erase` on scratch is scoped to its own generation for the same
reason — sweeping the prefix would take the table with it.

Order in `swap_temp` is the same bargain as compaction: `META` first, then the
deletes. Dying in between orphans objects, which costs storage; the other order
would delete the chunks the live metadata still points at.

## 5. The cache

Budgeted in bytes and shared across every file of a database, because a working
set is not spread evenly over tables — a per-file budget would hold an idle
table's cold pages while evicting the hot table's. Eviction is least-recently-used
by scan, which costs nothing on a hit and only runs when the cache is full.

Without it the driver would be unusable rather than merely slow: a scan reads
pointers densely and rows randomly, so every row would be a round trip.

There is currently no local *disk* tier, so a restart starts cold. That is the
single largest thing left undone for a production deployment.

## 6. Why a commit is a family operation

The engine begins a transaction on one file and commits it on another. An INSERT
writes row bytes through `rows`, the pointer through `pointers`, and calls
`commit_durable` on `pointers` alone. The file backend gets away with that
because both files share one write-ahead log.

Committing per file here would make the pointer durable and the row bytes not —
a pointer naming bytes that were never stored, which is exactly what the engine's
own comment at that call site says the transaction exists to prevent.

So a commit flushes every file holding records for the transaction, concurrently,
and **only the last object written declares it committed**. Recovery unions those
declarations across the whole database before applying anything. The transaction
is therefore either everywhere or nowhere: if the final put fails, no object names
it and every file discards its half. `a_commit_that_fails_leaves_neither_half_behind`
pins this.

The write-ahead log is not used and cannot be: it maps file ids to *paths* and
replays by writing to them, so its recovery would write into local files rather
than the store. `get_location()` returning `None` is what tells the engine this
file has no log — the in-memory provider already answers the same way, and both
call sites handle it.

### Durability window

`commit_durable` returns only after the delta object is stored. Writes made
*outside* a transaction — schema headers, the id sequence — are flushed by the
background service within `flush_interval` (200 ms). That is the same bargain the
file backend makes with the page cache, with one difference worth stating: the
page cache survives a process crash and this overlay does not. `KvFamily::flush_all`
is the clean-shutdown hook the trait does not have.

## 7. The S3 store

Thin on purpose. The SigV4 signing, connection pool and retry policy already
exist for `s3_backup`; three methods were added to that client — `get_object_opt`,
`get_object_range`, `list_prefix` — and nothing in it changed.

A 200 response to a ranged request (some stores ignore `Range` for small objects)
is sliced client-side rather than treated as an error: the answer is still
correct, only more expensive. A put over 64 MiB goes multipart, and a failed part
aborts the upload rather than leaving parts billing until a lifecycle rule reaps
them.

Works against anything speaking the S3 REST API: AWS, MinIO, Ceph RGW, Cloudflare
R2, Backblaze B2, Wasabi.

## 8. One writer

The append allocator is an in-process atomic and an object store offers no
compare-and-swap to replace it with. Two servers pointed at one prefix will
interleave offsets and corrupt each other. There is no lease yet; the deployment
has to guarantee it.

This matters because "S3-compatible storage" usually gets reached for in order to
run several stateless nodes, and that is precisely what is not yet safe.

## 9. Configuration and what is left

`KvConfig` defaults, all tunable:

| Field | Default | Trades |
|---|---|---|
| `chunk_size` | 4 MiB | Fewer requests per scan against more bytes rewritten per fold |
| `page_size` | 64 KiB | Bandwidth per point read; below ~32 KiB the saving vanishes next to the round trip |
| `cache_bytes` | 256 MiB | Resident base data, shared across the database |
| `chunk_fetch_pages` | 4 | Pages of one chunk wanted before the object is fetched whole |
| `readahead_chunks` | 2 | Chunks pulled in behind a sequential scan |
| `max_deltas` | 64 | Delta objects tolerated before compaction is forced |
| `compact_chunk_bytes` | 1 MiB | Overlay in one chunk that makes folding it worthwhile |
| `flush_interval_ms` | 200 | Exposure window for non-transactional writes |
| `spill_bytes` | 8 MiB | Unstored records held before they are written out early |

### Choosing it for a table

A table's placement in `db_storage.cfg` is a *backend spec*, not only a
directory. A bare path still means a directory, so every file written before this
existed means what it meant; `memory:` and `kv:<store>` are the new ones.

```sql
STORAGE PLACE TABLE events AT 'kv:archive';   -- before the table is created
CREATE TABLE events (...);
```

The stores themselves are named in `db_kv.cfg` beside `db_storage.cfg`:

```text
version 1
store   archive  s3   https://s3.us-east-1.amazonaws.com  us-east-1  my-bucket  rdb/archive  virtual
store   scratch  dir  /mnt/fast/kv                        scratch
```

Credentials come from `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` /
`AWS_SESSION_TOKEN`, never from the file — it is rewritten by the server, copied
into backups, and readable by anyone who can list the database directory.
Trailing `key=value` fields override any `KvConfig` setting for that store.

One database can hold file, in-memory and key/value tables at once. Nothing else
about such a table differs: it is the same `Table` over the same
`DbStorageProvider`, and only the bytes go somewhere else.

### What a non-file table does not get

Backup, relocation, the write-ahead log and the free-space policy are features
*about files*, and a table without files does not get them:

- **Not in a backup.** A snapshot is a copy of a directory. Every snapshot logs
  the tables it could not include and says a restore will not bring them back —
  the one thing worse than not backing them up is finding out at a restore.
- **`STORAGE MOVE TABLE` is refused**, naming the backend, rather than reporting
  a copy of zero bytes.
- **No write-ahead log.** Durability comes from the delta object instead (§6),
  and `get_location()` returning `None` is what tells the engine so.
- **Not a rebalance candidate**, and not counted against any location's free
  space.

A table placed on a store this server cannot open — the entry is gone from
`db_kv.cfg`, or the build lacks the feature — is **left out of the live table
map**, so a query for it says the table does not exist and the log says why.
Opening it anyway would answer with no rows, which is indistinguishable from a
table someone emptied.

### Still open

One writer per prefix (§8) applies per store, so per-table placement does not
make a second server safe. A single long transaction holds its writes in memory
until it commits, bounded by the transaction's size;
`should_break_transaction` — the engine's own valve — keys off the write-ahead
log and so returns false here.

## 10. Tests

`cargo test -p rasterizeddb_core --features kv_storage storage_providers::kv` —
28 tests, no network.

Worth knowing what they cover, because the interesting ones are not the obvious ones:

- **Request shape.** A two-file commit is asserted to be exactly two puts; a
  commit spanning ten chunks exactly one. A change that turns one put per commit
  into three is invisible to every correctness test and obvious here.
- **The torn commit.** A store armed to fail the final put, then a reopen
  asserting *neither* half survived.
- **Compaction against an open transaction**, which is one of the two bugs the
  first version had: freezing captured uncommitted bytes and re-emitted them as
  committed.
- **A swapped scratch file across a restart**, which is the other: the swap left
  the live data under a scratch key and the table reopened empty. Also that
  dropping scratch does not erase the table it shares a prefix with.
- **A differential fuzz** running random appends, in-place writes, transactions,
  truncations and compactions against both this backend and the in-memory
  provider, comparing bytes at every step.
- **The engine on top of it** — a real `Table` doing 200 inserts, a full scan, a
  filtered query, and a restart that recovers the rows from the store.
