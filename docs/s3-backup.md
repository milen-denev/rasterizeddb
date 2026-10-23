# S3 Backup & Point-in-Time Recovery — Design & Operation

Status: **implemented, off by default.** Build with `--features s3_backup`.
Target: `rasterizeddb_core::core::backup`, edition 2024.

## 0. What exists

| Component | State |
|---|---|
| `backup::s3` — SigV4 signing, pooled HTTP/1.1, multipart, batch delete | **Done.** §5 |
| `backup::archive` — the snapshot container, CRC per entry | **Done.** §3 |
| `backup::manifest` — the catalogue, recovery-target resolution | **Done.** §4 |
| `backup::restore` — download, unpack, bounded replay, atomic swap-in | **Done.** §6 |
| Log-coverage verification on both sides | **Done.** §10.7 |
| Resumable downloads, orphaned-upload reaping, staging ceiling | **Done.** §10.5–10.7 |
| `Wal::set_archive_dir` — checkpoints stage instead of discarding | **Done.** §2 |
| `Wal::recover_upto` — replay stopping at a commit sequence | **Done.** §6.3 |
| `Wal::scan_commit_range` — label a segment without replaying it | **Done.** §4 |
| `BACKUP` / `RESTORE` / `SHOW BACKUPS` statements | **Done.** §8 |
| CLI flags, including restore-before-open | **Done.** §7 |
| Scheduled snapshots, scheduled archive flush, retention | **Done.** §9 |
| Compaction interlock | **Done.** §10.1 |

Not done: cross-region replication of the prefix, encryption at rest beyond
whatever the bucket applies, and incremental (delta) snapshots. §12.

## 1. The shape of a backup

Two kinds of object, and one catalogue.

- A **snapshot** is the whole database directory packed into one archive,
  compressed with zstd, and written as a single object.
- A **segment bundle** is the write-ahead log records cut since the last
  bundle, concatenated and compressed the same way.
- `MANIFEST.json` describes every one of them.

Together they answer two different questions: the snapshot says what the
database looked like at some point, and the bundles say everything that
happened after it.

```
s3://bucket/prefix/
    MANIFEST.json
    snapshots/snap-20260820T134737Z-b57e1f06.rdba.zst
    wal/0000000000000000-0000000000000003.wseg.zst
    wal/0000000000000001-000000000000002f.wseg.zst
```

## 2. Why the snapshot may be taken while writes are in flight

Stopping the database to copy it would make every backup a maintenance window,
and the window would grow with the data. Instead the copy is **fuzzy**: files
are read while writers keep appending, so the result is a mix of states that is
not, on its own, consistent.

What makes it consistent is the log. Records are physical redo — "these bytes
belong at this offset in this file" — so replaying one is idempotent, and it
does not matter whether the copy already caught a given write. A snapshot
therefore records the commit sequence at the moment the copy started
(`start_lsn`) and again when it finished (`end_lsn`), and a restore replays the
archived log across that whole window. Below `end_lsn` the copy is not yet
usable; at or above it, it is exactly the database.

This is the same argument PostgreSQL's base backups rest on, and it is why the
log has to be *archived* rather than discarded at a checkpoint.

### 2.1 Staging, not discarding

A checkpoint proves every record is in its data file and then truncates the
log. With an archive directory set, it first copies the log there:

```rust
// Wal::checkpoint, after fsyncing every registered data file
let mut file = self.writer.lock();
self.stage_locked(&mut file)?;   // copy to <db>/wal_archive/<seq>.wseg
self.truncate_locked(&mut file)  // then, and only then, discard
```

The copy happens under the same lock that truncates, or a record appended in
between would be discarded having never been archived. `std::fs::copy` is
served by `copy_file_range` on Linux, so the bytes do not travel through user
space, and on a reflinking filesystem it is close to free.

A failure to stage is logged, not propagated: the data is already in the data
files, so losing the segment costs point-in-time coverage rather than
durability, and failing the checkpoint would let the log grow without bound on
what is most likely a full disk.

## 3. The archive container

Tar spends 512 bytes of header per file on metadata that a restore does not
use, and reading it back means tolerating its historical dialects. A snapshot
is a flat set of files in one directory, so the container carries the two facts
that matter:

```
"RDBA" u32 | version u8 | pad u8 x3 | entry_count u32
per entry: name_len u16 | name utf-8 | data_len u64 | data | crc32 u32
```

Entries stream one after another with no index and no seeking, which is what
lets the archive be produced and uploaded at the same time. Names are validated
on unpack — an archive is a file fetched from a network service, and a name
containing a separator or `..` would otherwise write outside the target
directory.

Each entry's length is taken once and honoured exactly. A file being appended
to during the snapshot is captured at the length it had when its turn came, and
the log supplies everything after that. Reading to EOF instead would record a
length that no longer matched the bytes already declared.

The per-entry CRC is checked on unpack, on top of zstd's frame checksum. A
snapshot is the last copy of the data and is worth doubting twice.

## 4. The catalogue

`ListObjectsV2` returns 1000 keys per request and says nothing about what is in
them, so planning a restore from a listing costs one request per thousand
objects plus a `GET` per segment just to learn its LSN range. A database
archiving for a year would spend hundreds of requests deciding what to
download.

Keeping the catalogue in one object makes planning exactly one `GET`. Each
bundle is labelled with the commit range it covers, computed by
`wal::scan_commit_range`, which walks record headers and skips payloads — so a
64 MiB segment is labelled without a second pass over its data.

The cost is that the manifest is rewritten on every archive flush, which is one
small `PUT` alongside an upload that was happening anyway.

## 5. The S3 client

Hand-rolled rather than an SDK, for two reasons that are the whole point of the
feature: CPU and request count.

- **`UNSIGNED-PAYLOAD`.** Bodies are never hashed for the signature. This is a
  documented SigV4 mode every implementation accepts, and it is the single
  biggest CPU saving in the upload path — otherwise a multi-gigabyte snapshot
  would be read twice, once to compress and once to sign. Integrity does not
  rest on it: TLS covers the wire, and the archive carries a CRC per entry plus
  a zstd frame checksum, both verified on restore.
- **Pooled connections and a cached signing key.** A burst of part uploads pays
  one TLS handshake and one key derivation between them.
- **No hidden retry layer.** Every request is one the backup code chose.

It costs nothing to carry: the transport is `rustls`/`tokio-rustls` and the
signing is `sha2`/`hmac`/`base64`, all of which the pgwire listener already
required. Enabling `s3_backup` adds `zstd` and `webpki-roots` — a compressor and
a root certificate bundle — not a dependency tree.

Plain SigV4 against the REST API, so anything speaking S3 works: AWS, MinIO,
Ceph RGW, Cloudflare R2, Backblaze B2, Wasabi. Use `--s3-path-style` for the
ones that do not do virtual-host buckets.

## 6. Restore

### 6.1 The two halves

Unpacking the snapshot gives a directory that is *nearly* the database. Replay
of the archived log over it is what removes the "nearly".

### 6.2 Why replay runs over one concatenated file

Segments could be replayed one at a time, but a transaction whose records were
written just before a checkpoint and whose commit marker landed just after
would be split across two of them — and per-segment replay, which discards
records belonging to transactions it saw no commit for, would throw it away.
Concatenating first makes that transaction whole again, at the cost of one
sequential write of the log's bytes.

### 6.3 Nothing half-restored is ever visible

The restore is assembled in a sibling directory and moved into place with a
single rename once it is complete. Two things follow, and both matter more than
the rename costs:

- A restore that fails part way leaves nothing behind, so it can simply be run
  again. Without this the partial directory is non-empty, and the emptiness
  check in §6 refuses the retry — a trap rather than a safeguard.
- The target directory never exists in a half-built state, so nothing can open
  it and find a database missing the log records it needed.

A sibling rather than a temp directory, because the rename has to stay on one
filesystem to be atomic and cheap.

### 6.4 Where it stops

`Wal::recover_upto` cuts on the **commit sequence**, so a transaction is either
wholly applied or wholly absent — a record whose transaction commits past the
ceiling is left out even though the record itself was written before it.

Cutting on a wall-clock instant is coarser by construction. Log records carry
no timestamp, because stamping each one would put a clock read on the write
path, so time is only known at the granularity a bundle was uploaded at, and
`AT TIME` resolves to the end of the last flush at or before it. If you need
exactness, use `AT LSN`.

## 7. Command line

Backups are enabled by naming a bucket.

```sh
rasterizeddb_core --location /var/lib/rasterized \
  --s3-bucket my-backups --s3-region eu-central-1 \
  --s3-prefix prod/orders
```

Credentials come from `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` /
`AWS_SESSION_TOKEN`, or from `--s3-access-key` / `--s3-secret-key`. Prefer the
environment variables: an argument is visible to anything that can read the
process list.

| Flag | Default | Notes |
|---|---|---|
| `--s3-bucket` | — | Setting it enables backups |
| `--s3-prefix` | `""` | One database per prefix; a clash is refused at start-up |
| `--s3-region` | `$AWS_REGION`, else `us-east-1` | |
| `--s3-endpoint` | regional AWS | Point at MinIO/R2/B2/Wasabi |
| `--s3-path-style` | off | Required by MinIO |
| `--s3-snapshot-level` | 12 | zstd, 1..22 |
| `--s3-segment-level` | 6 | zstd, 1..22 |
| `--s3-compression-threads` | 0 | 0 = half the cores, max 8 |
| `--s3-no-long-range` | off | Turns off long-distance matching |
| `--s3-part-size-mb` | 32 | Min 5. Raise beyond ~320 GB |
| `--s3-upload-concurrency` | 3 | Peak memory ≈ this × part size |
| `--s3-max-attempts` | 4 | |
| `--s3-snapshot-interval-secs` | 86400 | 0 = manual only. See §10.9 |
| `--s3-wal-flush-secs` | 300 | The recovery point objective |
| `--s3-wal-bundle-mb` | 64 | Flush early when staging reaches this |
| `--s3-retain-snapshots` | 7 | 0 = keep everything |
| `--s3-rollover-hours` | 0 | Fresh snapshot, then delete every earlier generation. 0 = never |
| `--s3-staging-max-mb` | 4096 | Safety valve; see §10.7. 0 = no ceiling |
| `--s3-backup-now` | off | Snapshot at *every* start, on top of the schedule |

Restore:

| Flag | Notes |
|---|---|
| `--restore-from-s3` | Rebuild `--location` before opening anything, then serve |
| `--restore-to <DIR>` | Restore into `DIR` and exit |
| `--restore-at-lsn <N>` | Exact cut on the commit sequence |
| `--restore-at-time <TS>` | Cut at the last flush at or before `TS` |
| `--restore-snapshot <ID>` | That snapshot, and only enough log to make it consistent |
| `--restore-replace` | Allow in-place restore over a non-empty `--location` |

`--restore-replace` **renames** the existing directory to
`<name>.pre-restore-<timestamp>` rather than deleting it. A restore is usually
run because something has already gone wrong, and the directory being replaced
may be the last local copy.

### 7.1 Recovering a lost host

```sh
# Rebuild onto a fresh machine and start serving.
rasterizeddb_core --location /var/lib/rasterized \
  --s3-bucket my-backups --s3-prefix prod/orders \
  --restore-from-s3

# Or rewind past a bad migration, keeping the damaged copy.
rasterizeddb_core --location /var/lib/rasterized \
  --s3-bucket my-backups --s3-prefix prod/orders \
  --restore-from-s3 --restore-replace \
  --restore-at-time '2026-08-20T09:55:00Z'
```

## 8. SQL

```sql
BACKUP DATABASE;                  -- snapshot, then archive, so it is restorable
BACKUP DATABASE TO S3;            -- same; naming the destination reads better
BACKUP WAL;                       -- point-in-time barrier: archive everything so far
BACKUP PRUNE;                     -- apply the retention policy now
SHOW BACKUPS;                     -- one row per snapshot and per segment bundle

RESTORE DATABASE FROM S3 TO '/var/lib/rasterized-restored';
RESTORE DATABASE FROM S3 TO '/tmp/verify' AT LSN 918273;
RESTORE DATABASE FROM S3 TO '/tmp/verify' AT TIME '2026-08-20T10:00:00Z';
RESTORE DATABASE FROM SNAPSHOT 'snap-20260820T134737Z-b57e1f06' TO '/tmp/verify';
```

These work over both the rastcp protocol and pgwire, because both route through
the same statement recogniser.

`RESTORE` from SQL always writes to a **new** directory. A statement that
swapped the files underneath the server executing it would leave every open
table reading storage that no longer describes what it thinks it does. In-place
restore is `--restore-from-s3`, which runs before any table is opened.

## 9. What it costs in requests

The design target is that request count scales with **time**, not with write
volume or object count.

| Operation | Requests |
|---|---|
| Snapshot ≤ one part | 1 `PUT` + 1 manifest `PUT` |
| Snapshot, *n* parts | 1 create + *n* parts + 1 complete + 1 manifest |
| Archive flush | 1 `PUT` per bundle + 1 manifest `PUT` |
| Retention prune | 1 manifest `PUT` + 1 batch delete per 1000 objects |
| Restore | 1 manifest `GET` + 1 snapshot `GET` + 1 `GET` per needed bundle |

A database writing ten times as fast makes the same number of requests: the
flush bundles every segment cut since the last one into a single object. At the
defaults — a daily snapshot and a five-minute flush — that is roughly 600
requests a day.

## 10. Interlocks and limits

The engine is MVCC, which the backup inherits rather than participates in: a
row's visibility depends on how its transaction ended, and that outcome lives
in `db_transactions.status`, not in the row. A snapshot that captured the rows
but not that file would restore a database unable to tell a committed row from
a rolled-back one — and the rows would still be physically present, so it would
look fine. The file is an ordinary member of the database directory and is
captured like any other; `transaction_outcomes_survive_a_restore` pins it.

Its neighbour `db_transactions.next` holds the transaction-id reservation: no
id at or above it has been handed out, and an open resumes there. Without it a
restored database could hand out again the id of a writer whose rows the
replay brought back but whose outcome it did not, and its new owner would
adopt those rows. Each raise of the mark is a logged write, so the replay that
restores the rows restores the mark past them;
`a_restore_resumes_transaction_ids_past_every_one_the_window_issued` pins it.
While the mark cannot be raised (a full disk), ids are still handed out so
reads go on, but a transaction that wrote under an id at or above the mark is
refused at commit and rolled back, as `53100 disk_full` when the disk is full
and `58030 io_error` otherwise, until a later `begin` raises it.


### 10.1 Compaction

Compaction moves rows and then shortens the file, and a truncation has no redo
record. Running underneath a fuzzy copy it could produce an archive that no
amount of replay repairs. `backup::compaction_permit` closes that: `Table::compact`
takes the gate shared, a snapshot takes it exclusively for the duration of the
copy. Uncontended it is one atomic, and it is compiled out entirely without the
feature.

### 10.2 Staged appends

`StorageIO::append_data` with `immediate` false stages bytes in a `.tmp`
sidecar which a background task later folds into the main file — neither step
writes a log record. Snapshots capture the sidecars, so staged bytes are
restored and folded in when the restored database is opened. What that does not
survive is a fold landing in the middle of the copy. No write path in the
engine currently stages, so this cannot happen today; rather than leave a trap,
the snapshot compares the sidecars before and after the copy and fails instead
of uploading a lossy backup.

### 10.3 Without the write-ahead log

With `WAL_ENABLED` false there is nothing to archive. Snapshots still work, but
they are only as consistent as a fuzzy copy is on its own, and there is no
point-in-time recovery. The service logs a warning at start-up saying so.

### 10.4 Interrupted downloads

A dropped connection part-way through a restore is resumed with a ranged
request rather than restarted, because the caller has already consumed what
arrived and cannot un-consume it. Without that a single blip fails a whole
restore, and the larger the database the likelier that becomes — exactly
backwards from what a restore needs.

A resumed request that comes back `200` instead of `206` is refused rather than
consumed: the server is sending the whole object again, and appending it would
duplicate everything already delivered. A failed restore is recoverable; a
silently corrupt one is not.

### 10.5 Orphaned multipart uploads

A process killed mid-snapshot leaves its parts in the bucket, and S3 bills for
them until something reaps them. A bucket lifecycle rule is still worth having,
but the service also clears them itself at start-up: by that point the prefix is
known to belong to this database and none of its own uploads can be in flight,
so anything listed is unambiguously an orphan.

This is best-effort — a provider without the call, or credentials without
permission for it, must not stop the database from starting.

Note that the listing is filtered client-side. `prefix` is a documented
parameter of `ListMultipartUploads`, but MinIO echoes it back and then ignores
it, returning nothing; a caller trusting the server-side filter would conclude
there were no orphans and leave every one of them in place.

### 10.6 The staging ceiling

The failure this exists for is an outage, not data loss. Uploads start failing —
expired credentials, a bucket policy change, the network — and every checkpoint
keeps handing segments to a directory nobody is draining. Left alone that fills
the disk, and a full disk stops the database.

Past `--s3-staging-max-mb`, archiving is **suspended**: checkpoints go back to
discarding the log, and the operator is told loudly that point-in-time recovery
is broken from that moment. It resumes on its own once staging drains to half
the ceiling — half rather than the ceiling itself, so a directory hovering at
the limit does not flap once per tick. After a suspension, take a snapshot to
establish a new baseline; the gap in the archived log cannot be filled
retroactively.

Losing point-in-time coverage is bad. Taking the database down to preserve it
is worse.

### 10.7 A snapshot is only recorded once its log is archived

`Wal::checkpoint` returns `Ok(())` when it *declines* to run: a transaction
with pending in-place writes points into the log for its payload, so truncating
underneath it would strand them. That is right for the log, but it means a
snapshot cannot conclude from a successful call that its window was cut and
staged.

There are **two** such returns, and both look identical to a caller:

1. A cheap early-out at the top of the function.
2. A re-check under the writer lock, after the data files are synced. The
   first check is only advisory — a transaction can register a pending
   overwrite between it and the lock being taken — and records are appended
   under that same lock, so once it is held the answer cannot change.

Staging runs under the lock, *after* (2), so a decline there skips it entirely
and still reports success. This is why the check below tests the condition
rather than the return value, and why simplifying it back to trusting the `Ok`
would be a regression with two ways to bite.
`a_declined_checkpoint_stages_nothing_and_still_reports_success` pins the
behaviour; `a_snapshot_whose_log_cannot_be_archived_is_discarded` pins the
response to it.

Left unchecked, that produces the worst failure this feature has: a snapshot
records an `end_lsn` whose records were never archived, the catalogue entry
looks healthy, and the restore stops short of consistency without saying so.

So the condition is checked rather than inferred. After the closing flush, the
live log must be empty and the staging directory drained; it is retried, since
a pending transaction is usually brief, and if it still does not hold the
snapshot is **discarded** rather than recorded. Losing a snapshot costs a
retry. Recording one that cannot be restored costs whatever depended on it.

The restore side checks the same property from the other end: if the catalogue
does not reach a snapshot's `end_lsn` — retention that pruned too far, an
object deleted by hand — it refuses and says which older snapshot to use
instead, rather than replaying what it has and handing back a database that
opens and answers queries and is wrong.

### 10.8 One database per prefix

Two servers sharing a prefix would overwrite each other's catalogue and leave
both unrestorable. Each database writes a `backup_instance.id` into its
directory, and a server whose id does not match the one in the manifest refuses
to start.

### 10.9 The schedule is measured from the catalogue, not from start-up

Every background loop originally slept a whole interval before its first run,
which quietly made each schedule relative to the *process*:

```rust
loop { sleep(interval).await; snapshot().await }   // the bug
```

A server restarted more often than its interval therefore never reached a run
at all. At the default daily snapshot that is any host redeployed during a
working day — and nothing reports it, because the loop is running, the log is
being archived on its own five-minute schedule, and the bucket simply never
gains a snapshot. What accumulates instead is segments with nothing underneath
them, which restore nothing. The manual `BACKUP NOW` was the only thing that
ever produced a base.

Each loop now reads what the catalogue says was last done and waits out the
remainder:

* **Snapshots** measure from the newest `SnapshotEntry.finished_at`. An empty
  catalogue is due *immediately*, whatever `--s3-snapshot-interval-secs` says
  and including `0` — "manual" is a choice about the schedule, not a request to
  keep paying for log that cannot be replayed. A base snapshot that fails is
  retried with a doubling backoff, from a minute up to an hour, rather than
  being left until tomorrow.
* **Archive flushes** measure from the newest `SegmentEntry.archived_at`. A
  restart no longer widens the recovery point objective, and segments a crashed
  run left staged go up at once instead of after another five minutes.
* **Rollovers** measure from `Manifest.last_rollover_at`, which is why that
  field exists: a rollover leaves an ordinary snapshot behind, so nothing else
  in the catalogue distinguishes one after the fact. A catalogue written before
  the field existed reads 0 and falls back to the newest snapshot — otherwise
  the first start after an upgrade would read 1970, roll over immediately, and
  delete every earlier generation. A catalogue with no rollover *and* no
  snapshot waits a full interval, because the snapshot task is already taking
  the base and there is nothing yet to reclaim.

Overdue in either direction is due now: a clock stepped backwards by NTP, or a
manifest stamped by a machine running ahead of this one, would otherwise park a
task centuries out.

### 10.10 Which bundles belong to a snapshot is decided by sequence, not by LSN

The commit sequence lives in the log, and a checkpoint discards the log once
its records are in the data files. A server that stops cleanly therefore leaves
nothing to recover the counter from, and the next process starts it again at
zero. Two things followed from that, and both were seen in a live bucket:

* A snapshot taken before the first commit of a run recorded `start_lsn: 0`.
  Retention kept every bundle whose `last_lsn >= 0` — which is all of them — so
  `BACKUP ROLLOVER` deleted the older snapshots, reported success, and left
  every archived log bundle behind it in the prefix.
* A bundle written *before* a restart carried sequences from the previous run
  of the counter, so it could compare as newer than a snapshot taken after it.
  A restore would then replay records describing file offsets that mean
  something else now, over a copy that already contained the writes they
  describe.

Two changes close it. The counter is now persisted: a truncation records the
sequence in `<log>.lsn` before dropping the log, and an open resumes from it —
written before rather than after, so a crash in between leaves a mark that is
merely ahead of the log, which recovery reconciles by taking the higher of the
two. And a snapshot now records `first_segment_sequence`, the catalogue's
bundle counter at the moment its copy began. Everything its window produces
lands at or above that number, and `manifest::segment_needed_by` — the one rule
both retention and restore ask, so they cannot disagree — compares against it
instead of against a commit sequence.

The bundle counter is a high-water mark on the manifest rather than
`max(sequence) + 1` over the list, because retention deletes from that list: a
counter derived from it alone would rewind after a prune and hand a new bundle
a sequence below a surviving snapshot's floor, where the restore that needs it
would never look.

Catalogues written before the field carry `null` and keep the old
commit-sequence rule, which over-keeps rather than deleting something a restore
would download. The first snapshot taken after an upgrade carries a sequence,
and the first rollover after that clears what they left behind.

### 10.11 Every database has its own log, and each is archived separately

`CREATE DATABASE` makes a subdirectory with a write-ahead log of its own. The
service used to attach only to the log at the location itself, and nothing
re-attached when a database appeared later, so those logs were staged by nobody.
Their checkpoints discarded records that had reached no archive — and since the
snapshot excludes live logs by basename *at every depth*, the same records were
excluded from the copy. They were in neither place, and the backup reported
success. Exposure was bounded by each database's own checkpoint cadence, which
is not something the backup controls.

The logs cannot simply be merged into one. A file id is a hash of a **table
name** (`FileSync::wal_file_id`), so two databases each holding a table called
`users` name it identically. One replay stream over both would write one
database's records into the other's file, and nothing would report it. So the
streams stay separate the whole way:

* **Staging** — `wal_archive/` holds the root's cut segments, exactly where an
  older build left them, and one subdirectory per created database holds
  theirs. Read back from disk rather than from the registry, so a database
  dropped with segments still staged has them uploaded rather than stranded.
* **Bundles** — never concatenated across logs. `SegmentEntry.database` names
  which one a bundle came from, and its key is `wal/<database>/…` (the root
  keeps the flat `wal/…` layout).
* **Sequence** — one counter for the whole installation, so a snapshot's
  `first_segment_sequence` bounds every log at once and §10.10's rule needs no
  per-database variant.
* **Windows** — `SnapshotEntry.windows` carries one `start_lsn`/`end_lsn` per
  log. The old scalar fields still carry the root's, which is what a catalogue
  written before this change holds and what an older build understands.
* **Restore** — one replay per database, into that database's own directory,
  against a resolver built from that directory's files.
* **Attaching** — the WAL registry calls a hook on every install, and the
  service attaches any log inside its location. Routing this through `CREATE
  DATABASE` instead would have covered one path and missed the others, and put
  a backup dependency in the catalogue.

`stream_key` decides what "inside its location" means, and refuses two cases on
purpose: a sibling directory whose path merely starts with the same characters
(`/var/lib/rasterizeddb2` next to `/var/lib/rasterizeddb`), and a storage
location on another disk. The second is still a stream, but under a key of its
own rather than a relative path — see §10.13.

#### What `AT LSN` means with more than one log

Each database counts its own commits, so no single number means the same thing
to all of them. `AT TIME` and `AT SNAPSHOT` translate directly — an instant is
an instant, and a snapshot records every log's window. `AT LSN` names a commit
of the database at the location itself, and reaches the others through the
moment that commit was archived: the same granularity `AT TIME` already has,
for the same reason (§6.4).

### 10.12 A restored copy must not read the directory it was copied from

`db_storage.cfg` is a file in the database directory like any other, so a
snapshot captures it and a restore writes it back out. It names **absolute
paths**: the database's own directory as a storage location, and every table
placed on it. Restored somewhere else, it still named the source — so the
restored database opened the *live* database's files, and its own were never
read at all.

That is the worst shape a backup bug can take. The obvious way to check a
restore is to open it and run a query, and that query passed by consulting the
very database the copy was supposed to be able to replace. On a host where the
source is gone — the case a restore exists for — the same query returns nothing.
And a write against the "clone" would have landed in the original, which is the
one thing §6.3 promises cannot happen.

So `storage::repoint_pool_config` rewrites the file before the restored
directory is ever opened as a database:

* the source root becomes the directory this restore is being published into —
  the final one, not the sibling it is staged in, because the paths have to be
  true after the rename;
* every database created with `CREATE DATABASE` has a pool file of its own,
  naming its own subdirectory by an absolute path, and is rewritten the same
  way — anything under the source root maps to the same place under the
  restore. Until 2026-10 only the root's file was, so a restored sub-database
  read the source's files;
* a storage location the snapshot carried (§10.13) is rewritten to wherever its
  files were restored;
* any other storage location is dropped, along with the tables placed on it: a
  backup location, whose files mirror a primary the snapshot already has, or
  any location of a snapshot taken before locations were copied. A restored
  database that resolves those tables to its own root and reports them missing
  is loud; one that reads the source's disk is not. The restore logs which
  tables those were;
* an **in-place** restore — the CLI's `--restore-from-s3`, landing on the
  directory the backup came from — is left alone. Every path in the file is
  still true there, and rewriting would drop placements that host can still
  serve.

`Manifest.source_location` is what makes the in-place case recognisable; it is
refreshed on every publish, so a database directory that moves does not leave a
restore repointing at a path this installation no longer uses. A catalogue
written before that field falls back to the first `location` line, which
`Pool::render` always writes as the root.

### 10.13 Storage locations on other disks

A table can be placed on a storage location outside the database directory
([storage-pools.md](storage-pools.md)). Until 2026-10 the snapshot walked only
the database directory and the archive took only the logs under it, so such a
table was in neither — and the backup reported success. Now:

* **Which locations.** Every primary location named by a pool file under the
  backed-up directory — the root's and each created database's — plus any
  directory a placement names. Read from the files, so the service still has no
  dependency on a live `Database`. A location shared by two databases is copied
  once. Backup locations are not copied: they mirror a primary that is. A
  location inside the backed-up directory is not listed either; the walk
  already takes it.
* **Keys.** Each is identified by `~loc-` and a hash of its path. That is its
  stream key (staging subdirectory, `SegmentEntry.database`,
  `LogWindow.database`) and the directory its files are named under in the
  archive. Never a relative path, so it cannot collide with a database created
  inside the directory. `SnapshotEntry.locations` maps each key back to the
  path.
* **The copy** is flat: tables sit directly in a location, and a recursive walk
  would take in whatever else is kept beneath it. The whole directory is taken,
  not a list of a table's files — the suffix list has been short twice.
* **The log.** Each location has its own log (§10.11's reasons apply: file ids
  are names), checkpointed around the copy with every other. A location that
  starts holding tables later is attached at the next cut, since the install
  hook only saw its log while it held nothing. A location that appears *while*
  a copy runs fails that snapshot rather than being recorded without it.
* **Restore in place** (`--restore-from-s3` onto the source directory) copies
  each location's files back to its own path — another disk, so a copy, done
  before the root is renamed into place and undone if any fails. The location
  must be empty or absent; a restore never merges into what is there, and says
  so before downloading anything. Move it aside first.
* **Restore elsewhere** puts each location's files in
  `<target>/pool-location-<hash>` and repoints the pool file there. The source's
  location is still the source's, and a clone writing into it would overwrite
  live tables. The clone therefore holds everything on one disk; move tables
  back out with `STORAGE MOVE TABLE` once it is verified.
* **Format.** A catalogue with any such snapshot is written as version 3, which
  an older build refuses: it would unpack the locations into stray directories
  and restore a database whose tables on them read as missing. A catalogue
  without one stays at the version it was, and snapshots taken before this
  restore exactly as they did.

## 11. Tests

| Suite | Covers |
|---|---|
| `core::backup::*` unit tests | Manifest resolution, archive round-trip and corruption, part cutting, bundling, timestamps |
| `core::backup::pitr_tests` | Staging, concatenation, exact-LSN cuts, transactions split across a checkpoint, and the fuzzy-copy-plus-replay claim |
| `tests/s3_backup_minio.rs` | SigV4 against a real server: signing, path escaping, multipart, batch delete, streaming, pooling; plus an opt-in probe of the TLS path against real AWS |
| `tests/s3_backup_end_to_end.rs` | Backup and restore a live database, reopen it, and query it — including under concurrent writes, with a point-in-time target, after retention has pruned, after a corrupted snapshot failed a previous attempt, driven only by the background scheduler, and across a staging suspension |
| `tests/s3_backup_scale.rs` | Opt-in volume test: a many-part multipart upload of incompressible data, restored and compared byte for byte |

The last two are skipped unless `RDB_S3_TEST_ENDPOINT` is set:

```sh
docker run -d --rm -p 19000:9000 \
  -e MINIO_ROOT_USER=testkey -e MINIO_ROOT_PASSWORD=testsecret123 \
  quay.io/minio/minio:latest server /data
curl -s -X PUT --aws-sigv4 "aws:amz:us-east-1:s3" \
  --user testkey:testsecret123 http://127.0.0.1:19000/rdbtest

RDB_S3_TEST_ENDPOINT=http://127.0.0.1:19000 \
RDB_S3_TEST_BUCKET=rdbtest \
AWS_ACCESS_KEY_ID=testkey AWS_SECRET_ACCESS_KEY=testsecret123 \
  cargo test -p rasterizeddb_core --features s3_backup
```

MinIO is reached over plain HTTP, so the encrypted transport is covered
separately by `reaches_real_s3_over_tls`, which is opt-in because it talks to a
third party. It needs no credentials — it uses the published example key, whose
rejection *as a parsed S3 error* is what proves the handshake, virtual-host
addressing and response parsing all worked:

```sh
RDB_S3_TLS_PROBE=1 cargo test -p rasterizeddb_core --features s3_backup \
  --test s3_backup_minio reaches_real_s3_over_tls
```

## 12. Before running this in production

Things the implementation cannot do for you:

- **Set a lifecycle rule for incomplete multipart uploads.** The service aborts
  its own uploads on any error it survives to handle, and reaps orphans from a
  previous run at start-up (§10.5) — but a database that is killed and never
  restarted has nobody to do that for it. A one-line bucket rule covers the
  remainder.
- **Restrict the credentials.** The client needs `s3:PutObject`,
  `s3:GetObject`, `s3:DeleteObject`, `s3:AbortMultipartUpload` and
  `s3:ListBucketMultipartUploads` on the prefix, and nothing else.
- **Verify a restore.** `--restore-to` exists for exactly this: it rebuilds
  into a scratch directory without touching the live database, and the
  restored directory can be opened and queried. A backup nobody has restored
  is a hypothesis.
- **Decide the recovery point objective.** `--s3-wal-flush-secs` is it: the
  default of 300 means a lost host can lose up to five minutes of write
  history. Lowering it costs two more requests per flush.

### 12.1 Scale

`tests/s3_backup_scale.rs` pushes deliberately incompressible data through the
real upload and download paths and compares the result byte for byte — not by
length, because a multipart upload that assembled its parts out of order
produces an object of exactly the right size. It has been run at 853 MiB across
171 parts.

What that does not cover is duration. A multi-hour snapshot against a remote
endpoint crosses credential expiry, and temporary credentials are refreshed by
the process that supplied them, not by this client. Run a snapshot and a
`--restore-to` against a production-sized copy over your real endpoint before
relying on it.

## 13. Not done

- **Incremental snapshots.** Every snapshot is full. For a large database with
  a low write rate the archived log already provides most of the benefit, but a
  delta snapshot would cut the daily upload substantially.
- **Encryption at rest beyond the bucket's own.** No SSE-C or client-side key.
- **Restoring a single table.** The unit is the database directory.
- **Parallel segment download on restore.** Bundles are fetched in order; a
  restore is bounded by decompression and disk rather than by round-trip
  latency, but a very long archived history would benefit.
