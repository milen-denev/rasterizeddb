# Multiple storage directories

Behind the off-by-default `storage_pools` Cargo feature. Without it the pool
type is `()`, every question asked of it is a one-line shim that the optimiser
erases, and `DbStorageProvider` has one fewer variant.

A database that has only ever been given one directory behaves identically with
the feature on: its pool holds exactly that directory, no backups, and the
`primary` policy — which is what a single-directory database has always done.

## The model

A pool is an ordered list of locations. The first is the database's own
directory and cannot be removed: it holds the catalogue, the transaction status
log, and the pool file itself.

Each location has a role:

- **primary** — eligible to hold a table's files. A policy decides which primary
  a new table lands in.
- **backup** — receives a mirrored copy of every write. Never chosen for
  placement, so a table is never *only* on a backup.

The roles compose rather than being alternatives. "Primary + backup" is one
primary and one backup. "Balance across four disks, all mirrored to a fifth" is
four primaries and one backup. Neither has a mode switch of its own.

Each location also has a **weight** (multiplies its share under `free-space`)
and an **enabled** flag. A disabled location keeps every table already in it —
those files stay open and readable — and takes no new ones. That is how a disk
is drained before it is removed.

## Policies

| policy | choice |
| --- | --- |
| `primary` | the first enabled primary |
| `round-robin` | the enabled primary holding the fewest tables, ties by declaration order |
| `free-space` | the enabled primary with the greatest `available_bytes × weight` |

Free space comes from `statvfs` through the `libc` already in the tree. On a
platform where it cannot be determined the probe returns "unknown", which
`choose` treats as *not known to be full* — so `free-space` degrades to picking
the first candidate rather than to a wrong answer.

`round-robin` counts tables rather than keeping a rotating cursor. A cursor
would reset to zero at every restart and pile the next several tables into the
first directory; a count is a function of state on disk, so a restart changes
nothing.

`min_free_bytes` (default 256 MiB) is a floor: a location below it is skipped.
If *every* location is below it the floor is ignored and the emptiest is used,
with a warning. Refusing the `CREATE TABLE` is the other option and is worse —
the operator loses a table because a different directory is full.

## Where a table actually is

`db_storage.cfg`, in the database directory, is tab-separated text:

```
# rasterizeddb storage pool. Rewritten by the server; edit while it is stopped.
version	1
policy	free-space
min_free_bytes	4294967296
location	primary	1	enabled	/var/lib/rasterized
location	primary	2	enabled	/mnt/nvme2
location	backup	1	enabled	/mnt/backup
table	/mnt/nvme2	orders
table	/var/lib/rasterized	users
```

Tab-separated rather than a serialisation format: paths contain spaces and
almost never tabs, the file has to stay hand-readable for an operator recovering
a database onto different mount points, and it is a dozen lines of parsing
against a dependency and a derive.

**Placement is recorded, not recomputed.** Recomputing on open would be a
disaster in slow motion: change the policy, or add a disk, and every existing
table would be looked for in a directory its files are not in — reported not as
an error but as an empty table. A table with no `table` line is in the database
directory, which is what every table written before this feature existed is, so
an existing installation opens unchanged.

The file is rewritten through a temporary file and a rename, with the bytes
synced before the rename. A truncated pool file is not a lost setting; it is
every table on a second disk becoming unfindable at the next start-up.

A pool file that cannot be parsed is logged and the database degrades to a
single directory — loud, in a way that inventing placements would not be. A
`table` line naming a directory the pool no longer lists is warned about and
left pointing where it says, because dropping it would make the table resolve to
the root and open as empty.

## Write-ahead logs

Every file this engine writes takes its durability from `wal_for(<its
directory>)`. A table in a directory with no log falls back to an `fsync` per
append — correct, and roughly an order of magnitude slower than the group commit
the root enjoys. So the pool creates each location and opens (and recovers) a
log there when the database starts.

`Database::sync_durable` walks every pool directory, not just the database's
own. A statement that touched a table on a second disk logged that write to
*that* directory's log, and the root's `sync_pending` knows nothing about it —
syncing only the root would report durability for bytes still in the page cache.

## Mirroring

`MirrorStorageProvider` wraps one primary `LocalStorageProvider` and any number
of backup ones. Reads go to the primary; every write goes to all of them. The
catalogue, the row store and the schema are mirrored too, so a backup directory
that held every table but not the list of them cannot happen — that directory
opens as a database.

Every provider receives the same operations in the same order and each file
starts empty, so a record lands at the same offset in all of them. Row pointers
record absolute offsets, so they are valid against any copy. That is what makes
a backup directory directly usable rather than only an archive.

Backup appends always take the immediate path, whatever the caller asked for.
The deferred path stages into a per-file temp buffer that a background task
lands later — a backup would hold bytes in a `.tmp` that the primary already has
in its data file, and the two directories would agree only once that task
happened to run. The `fsync` is still deferred, so this is a write, not a flush.

### What it is not

A **warm copy**, not a synchronous replica. The primary's write is the one a
commit waits on; a backup's is issued at the same time and flushed by its own
directory's log, so a crash can leave a backup a few records behind — a window
of one group commit. The copy is repaired at the next open, not continuously:
`MirrorStorageProvider::attach` compares lengths when a file is opened and
rewrites any backup that disagrees.

Lengths, not contents. A full comparison would read both files end to end every
time a table opens, which on a large table costs more than the copy it is trying
to avoid. Lengths can only diverge through a crash, and a crash that leaves them
equal has left the same records in both. When they do disagree the backup is
emptied and rewritten rather than having its tail appended: appending is only
correct if the shared prefix is identical, which is exactly what a length
mismatch gives no grounds to assume.

It is also not a replacement for replication, which ships transactions to
another *process* and survives this machine. A backup location survives the
primary disk. Those are different failures.

Adding a backup location mirrors tables **created or opened afterwards**. Tables
already open are mirrored from the next restart — their providers were built
before the location existed and cannot be swapped underneath a live table. The
`STORAGE ADD LOCATION` output says so.

## Moving tables

`STORAGE MOVE TABLE <name> TO '<path>'` copies, flips the placement, then
deletes the originals. Every window between those steps leaks rather than loses:

- crash after the copy — the placement still names the old directory, the table
  opens from it unchanged, and the new copies are orphans. `db_storage.cfg` is
  the only thing that decides where a table is, so files it does not name are
  invisible.
- crash after the flip — the table opens from the new directory and the
  originals are the orphans.

The flip is a rename over `db_storage.cfg`, so there is no state in which it is
half-applied. That is the whole reason the pool file is rewritten atomically
rather than appended to.

The table handle is removed from the map before the files are read, so its
maintenance task stops before the copy starts — a background flush landing
mid-copy would put bytes in the source that the copy has already passed. This is
the same exposure `ALTER TABLE ... RENAME` has, and statements in flight against
the table briefly see it as absent.

A rename does not move a table: the new name inherits the old one's directory,
pinned *before* the copy so that the cleanup of a failed one and the recovery
sweep after a crash both resolve the new name to the directory its files are
actually in.

## Rebalancing

The engine never moves a table on its own. A several-gigabyte copy is not
something to start behind an operator's back, so `STORAGE REBALANCE` is a
statement, and `STORAGE REBALANCE DRY RUN` prints the plan — table, size, from,
to, and why — without moving anything.

The plan, in order:

1. **Evacuation.** Any table in a location that is disabled, or that is no
   longer a primary, is moved out. This is what makes `STORAGE DISABLE LOCATION`
   a drain rather than a freeze.
2. **Policy.** Under `round-robin`, tables move from the fullest location to the
   emptiest — smallest table first, since evening out a count should cost as
   little copying as it can — until the counts differ by at most one. Under
   `free-space`, tables move off any location below the floor, *largest* first,
   since one copy that frees the most beats several that free the same between
   them. Under `primary`, nothing: that policy is a statement that placement is
   manual.

Free space is decremented as the plan is built, so two moves in one plan do not
both aim at the same headroom, and nothing is moved onto a location that would
itself drop below the floor. The plan is sorted deterministically: a dry run
that disagreed with the run after it would be worse than no dry run at all.

Moves run sequentially and a failure stops the run. Each move is individually
atomic, so stopping leaves a consistent database with part of the plan applied,
and the next `STORAGE REBALANCE` re-plans from where it got to.

## Guards

- The database's own directory cannot be dropped from the pool or made a backup.
- A location that still holds tables cannot be dropped; the error names them.
- A location that overlaps an existing one — either direction — is refused.
  Containment is compared per path component, so `/data/db2` is not "inside"
  `/data/db`. A backup nested in the data directory is on the same filesystem as
  the thing it is backing up, and a sweep of the outer directory takes the inner
  with it.
- A relative path is refused: the server's working directory is not part of its
  configuration.
- A new location is created and write-probed on the spot. A location added
  successfully and unusable later is a support call; one that refuses
  immediately names its own problem.
- A location holding tables cannot be turned into a backup — it is written by
  mirroring, and would be overwritten.
- A table cannot be moved into a backup location, for the same reason.

## Per-database, not per-catalogue

Each database in a catalogue has its own pool and its own `db_storage.cfg`.
Locations are used verbatim: a non-default database does *not* get an implicit
subdirectory of its own under an added location, because guessing one would put
files somewhere the operator did not name. Give `analytics` its own
`/mnt/nvme2/analytics` if that is what you want.
