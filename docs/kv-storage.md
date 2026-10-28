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
| `STORAGE ADD/ALTER/DROP STORE`, `SHOW STORAGE STORES` | **Done.** §9 |
| Per-store credentials in `db_kv_secrets.cfg` (0600) | **Done.** §9 |
| `STORAGE MOVE TABLE` converting between every backend | **Done.** §9 |
| Crash-safe commit spanning several files | **Done.** §6 |
| Compaction, delta retirement, recovery | **Done.** §4.4, §4.5 |
| `create_temp` / `swap_temp` without a rename | **Done.** §4.6 |

Not done: a multi-writer lease (§8) and a local disk cache tier (§5). Backup and
the free-space policy deliberately do **not** cover a key/value table — see §9.
Relocation does, but as a conversion rather than a directory move.

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

### 4.7 Dropping a file

`drop_io` is synchronous and deleting is a round trip, so the sweep (`erase`)
is spawned, and a reopen of the name waits for it. Three things have to hold or
a dropped table comes back:

- **Nothing written outlives the sweep.** `erase` takes the maintenance gate,
  then the compaction guard, then the flush lock — the order `adopt` takes
  them — so a flush or a compaction in flight finishes first and what it wrote
  is in the listing. Afterwards a flush, a compaction or a truncation of a
  dropped file writes nothing. Otherwise its objects land after the sweep, and
  a table later made under the same name (generation 0, sequence 0 again)
  reads them as its own or replays them at the next restart; an untransacted
  record — a schema header, the id sequence — applies with no declaration.
- **Declarations other files need survive it.** A transaction declared
  committed in the dropped file's deltas may have its other half in another
  file. Before the sweep each such declaration is written again in a file that
  holds those records (the [`Ledger`] that compaction and `adopt` consult),
  or that table loses the rows at the next restart.
- **A reopen never gets the dropped file.** The sweep's gate is registered
  before the file leaves the map, and a dropped file still in the map is
  waited out, not returned.

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

### Defining a store

Everything here is a statement. Nothing needs the server stopped, and nothing
needs a file edited by hand:

```sql
STORAGE ADD STORE archive S3 WITH (
    ENDPOINT   = 'https://s3.us-east-1.amazonaws.com',
    REGION     = 'us-east-1',
    BUCKET     = 'my-bucket',
    PREFIX     = 'rdb/archive',
    ADDRESSING = 'virtual',        -- 'path' for MinIO, or a non-DNS-safe bucket
    CHUNK_SIZE = '4MiB');

STORAGE ADD STORE scratch DIRECTORY WITH (PATH = '/mnt/fast/kv', PREFIX = 'scratch');

STORAGE ALTER STORE archive SET CACHE_BYTES = '512MiB';
STORAGE DROP STORE scratch;
SHOW STORAGE STORES;
```

Only `ENDPOINT` and `BUCKET` are required for S3; `REGION` defaults to
`us-east-1` (it is signed over, and the stores that ignore it — MinIO, Ceph, R2
— are the ones set up first), `PREFIX` to `rdb`, and `ADDRESSING` to `virtual`.
Any of the settings in the table above may appear in `WITH (...)` or in
`ALTER STORE ... SET`, and sizes take `4MiB` as readily as a byte count.

`STORAGE ADD STORE IF NOT EXISTS <name> ...` is for a deployment that configures
its own store and issues the statement on every start-up: there is no good moment
for it to decide whether this is the first one, and wrapping the statement in a
`SHOW` and a branch is a race as well as a nuisance. Re-issuing the same
definition is a no-op. Re-issuing a **different** one is refused and names the
fields that differ — leaving the old address in place would make the statement a
lie, and adopting the new one would repoint a store out from under whatever is
already on it. Credentials given on that path are still applied, because
re-issuing with a key read from the environment is how such a deployment rotates
one.

### Credentials

A store may carry its own, which is what lets two stores in one database live in
two accounts:

```sql
STORAGE ADD STORE archive S3 WITH (
    ENDPOINT = '...', BUCKET = '...',
    ACCESS_KEY_ID     = 'AKIA...',
    SECRET_ACCESS_KEY = '...',
    SESSION_TOKEN     = '...');          -- for STS / IRSA / instance roles

STORAGE ALTER STORE archive SET ACCESS_KEY_ID = '...', SECRET_ACCESS_KEY = '...';
```

A store given none signs with `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` /
`AWS_SESSION_TOKEN` from the server's environment, as before. `SHOW STORAGE
STORES` has a `credentials` column saying which of the two a store is on — never
the credentials themselves.

**They are not written to `db_kv.cfg`.** That file is rewritten by the server,
quoted in errors and readable by anyone who can list the database directory, so
a secret in it is a secret in the logs. Credentials go to `db_kv_secrets.cfg`
beside it, created `0600`, and nothing renders them back: not `SHOW`, not
`describe`, not a `Debug` impl, not a parse error — that parser reports line
numbers and never the line. The split also keeps `db_kv.cfg` safe to paste into
a ticket.

What the split does **not** protect is a copy of the directory: a backup carries
`db_kv_secrets.cfg` like any other file, so a backup of this database is as
sensitive as the key in it. That is the one place to think before pointing a
snapshot somewhere less private.

Rotation is allowed where an address change is not — a new key moves no bytes,
so `ALTER STORE ... SET SECRET_ACCESS_KEY` works while tables are placed on the
store. A store this server already has **open** keeps signing with the
credentials it opened with until the server restarts; the statement says so.
Live rotation would mean swapping the signer under a shared `S3Client`, which is
a change to the client `s3_backup` also uses, and is not done.

`WITH (...)` tracks quotes, so a value may contain a comma, a `)`, an `=` or the
word `WITH`. It has **no escape sequence**: a value cannot contain the quote
character it is wrapped in, and an unterminated one is refused rather than read
short. Parse errors in a `WITH` list name the key and never quote the offending
text, because that text can be a credential and an error message is a log line.

Half a credential — a key id with no secret, or a session token alone — is an
error rather than a partial update: it would leave a store that looks configured
and fails at its first request, by which time the failure reads as a wrong
endpoint. A `DIRECTORY` store refuses credentials outright, since it signs
nothing.

**Adding a store does not contact it.** No request is made until a table is
placed there, so a wrong endpoint, a missing bucket or an absent credential is
still silent at `ADD STORE` time and shows up as the first table failing to
open. `SHOW STORAGE STORES` has an `opened` column that says which of the two a
store is in.

Three things are refused rather than allowed to become silent:

- **Dropping a store a table is placed on.** A placement naming a store that is
  gone is what stops the *database* opening (§9), so this would be a statement
  that succeeds now and refuses to start later. It names the tables.
- **Repointing a store a table is placed on** — `ENDPOINT`, `BUCKET`, `PREFIX`
  and the rest. Changing an address does not move any bytes: the old prefix
  keeps them and the table reads an empty one. Move the tables off first. The
  settings that only tune it can be changed at any time.
- **Writing a store file this server could not parse.** A file that fails to
  parse and a file that is absent both leave the registry empty, and rewriting
  the first would replace an operator's stores with nothing.

A store already open keeps the geometry and the page budget it was opened with
until the server restarts — said in the statement's own output, because
`chunk_size` is not a setting so much as a different layout for the same bytes,
and the family holding it is shared by every open table on that store.

### Choosing it for a table

A table's placement in `db_storage.cfg` is a *backend spec*, not only a
directory. A bare path still means a directory, so every file written before this
existed means what it meant; `memory:` and `kv:<store>` are the new ones.

```sql
STORAGE PLACE TABLE events AT 'kv:archive';   -- before the table is created
CREATE TABLE events (...);
```

Once it exists, `SHOW TABLES` says which backend each table ended up on — `s3`,
`kv-directory` or `kv-memory` for these, against `file` or `memory` for the rest
— and `SHOW TABLE STATS events` reports the store's page cache beside it, where
the miss count is the number of requests the store has actually been asked for.
See [introspection.md](introspection.md#what-is-holding-a-table).

### Converting a table between backends

`STORAGE MOVE TABLE` takes the same spec, so moving a table between disks and
converting it from files to an object store are one statement:

```sql
STORAGE MOVE TABLE events TO 'kv:archive';    -- files      -> S3
STORAGE MOVE TABLE events TO 'kv:scratch';    -- S3         -> a local kv store
STORAGE MOVE TABLE events TO 'memory:';       -- ... or into this process
STORAGE MOVE TABLE events TO '/mnt/disk2';    -- ... and back onto files
```

The copy is `StorageIO` to `StorageIO`, one named file at a time. It never reads
a directory entry, an object key or a buffer address, so a directory, an
in-memory registry and a key/value family are all just a provider to it — which
is what makes every pairing one operation rather than twelve. The ordering is
the one the directory-to-directory move already used: copy, flip the placement,
delete the originals, so every window leaves an orphan rather than a loss.

What is *not* backend-agnostic is what the table gains or loses on arrival, and
each move says so in its own output. Crossing the file boundary in either
direction is otherwise invisible: the rows read back identically, and the next
snapshot is the first hint that a table is no longer in one.

Two checks happen before a byte is copied — a directory destination must be a
storage location of this database, and a store destination must be configured —
because after that the table has left the live map.

### The file it writes

The statements rewrite `db_kv.cfg`, beside `db_storage.cfg`, atomically. It can
still be read and hand-edited while the server is stopped:

```text
version	1
store	archive	s3	https://s3.us-east-1.amazonaws.com	us-east-1	my-bucket	rdb/archive	virtual
store	scratch	dir	/mnt/fast/kv	scratch
```

Every gap is a **single TAB**. Spaces do not parse, and they fail quietly:
`version 1` written with a space is not recognised as the version line, the file
is then rejected for having none, and every store it defines silently ceases to
exist. Trailing `key=value` fields override any `KvConfig` setting for that
store. A store of a kind this build cannot open — a newer server's, say — is
kept verbatim when the file is rewritten rather than reduced to a stub.

One database can hold file, in-memory and key/value tables at once. Nothing else
about such a table differs: it is the same `Table` over the same
`DbStorageProvider`, and only the bytes go somewhere else.

### What a non-file table does not get

Relocation, the write-ahead log and the free-space policy are features *about
files*, and a table without files does not get them. A backup is the exception:

- **In a backup, as of the snapshot.** A snapshot copies every table on a store
  out as one crash-consistent cut and carries its files like any other table's
  ([s3-backup.md §10.14](s3-backup.md)). A restore brings it back as a table in
  the restored directory — never onto the store, which a clone would share with
  the live server — and as of that copy, since it has no log a point-in-time
  target could replay. An in-memory table cannot be copied; a snapshot records
  it as not contained, and `SHOW BACKUPS` and `SHOW BACKUP STATUS` say so.
- **Not a `STORAGE MOVE TABLE` between directories.** It has no files to put in
  one. `STORAGE MOVE TABLE t TO '/mnt/disk2'` converts it back into a file table
  instead, which is a different operation and says so.
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

The statements are covered separately, in
`core::storage::backend_tests` (`cargo test -p rasterizeddb_core --lib
storage::backend_tests`). The two worth knowing there:

- **A table round-tripping every backend** — files to memory to a store to
  memory to files to a store — asserting the same rows at every step, that the
  database directory is left empty, and that a restart still finds them. It is
  also what catches a sidecar missing from the copy list, as leftover files: a
  table's own files are the only complete statement of what a table is.
- **A store definition the file could not hold** — a credential, an unreadable
  file, and a store of a kind this build cannot open being rewritten by a server
  that does not understand it.
