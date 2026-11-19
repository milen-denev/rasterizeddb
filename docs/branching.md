# Database branches

Status: **implemented**, Cargo feature `db_branching` (in the default list).
Code: `core/database/branch.rs` (copy, catalogue, statements),
`pgwire/branch_merge.rs` (diff and merge), `rdb_studio/src/views/branches.rs`.

A branch is a database made as a consistent copy of another — its *parent* —
that then diverges from it. It can be refreshed from the parent, swapped in
for it, compared with it, and merged back into it.

## 1. Statements

| Statement | What it does |
|---|---|
| `CREATE BRANCH dev [FROM prod]` | `dev` holds `prod` as of now, copied while `prod` keeps taking writes. `FROM` defaults to the session's database. |
| `… AS OF TIMESTAMP '2026-11-19 10:00+00'` / `AS OF LSN n` / `AS OF SNAPSHOT 'id'` | A past point, restored from the S3 backup (§4). |
| `… WITHOUT BASE` | Keeps no merge base (§5): half the space, but the branch cannot be merged. |
| `CREATE DATABASE dev [WITH] TEMPLATE [=] prod` | PostgreSQL's spelling of the same copy. `template0`/`template1` are a plain empty create. |
| `SHOW BRANCHES` | branch, parent, parent_exists, point, parent_lsn, method, created_at, merged_at, size_bytes, base_bytes, sessions, then the tree: depth, path (`prod/dev/feature`), children, via (§8). Rows are in tree order. |
| `RESET BRANCH dev` | Copies the parent again, as of now. Everything written on `dev` is discarded. |
| `PROMOTE BRANCH dev` | Swaps `dev` and its parent by name. The parent's old contents stay, as branch `dev`. |
| `SHOW BRANCH DIFF dev` | What a merge would do, per table: schema changes, inserts/updates/deletes, conflicts. |
| `MERGE BRANCH dev [INTO prod] [ON CONFLICT FAIL \| BRANCH WINS \| PARENT WINS] [ALLOW DROP]` | Applies what `dev` changed since its base to the parent, in one transaction (§6). |
| `DROP BRANCH [IF EXISTS] dev` | `DROP DATABASE`, refused for a database that is not a branch. Branches made from `dev` move to `dev`'s parent (§8). |

A branch is an ordinary database: connect to it by name; its roles, sequences,
indexes and partitions came with it. Every form but `SHOW BRANCHES` needs a
superuser — `SHOW BRANCH DIFF` too, since it reads and names rows of databases
the session may hold no account in. The forms that change something are also
refused on a read-only session (25006) and inside a transaction block (25001),
like `CREATE DATABASE`.

## 2. On disk

```text
<root>/                       the default database
  prod/                       a database
  dev/                        a branch of prod
    rdb_branch.meta           parent, point, LSN, method, base generation (JSON)
    rdb_branch.ddl            schema changes since the base, one JSON string per line
  .rdb_branch_bases/
    dev.0/                    dev's merge base (generation 0), closed
  .branching-<name>/          a branch being built (removed at start-up if left)
```

Hidden names start with a dot, which no database name may, so the catalogue
never discovers them as databases. The bases are left out of S3 snapshots
(they are copies of databases the snapshot holds); a restored server's branches
therefore come back without bases, and `RESET BRANCH` makes new ones.

## 3. How a live database is copied

The parent keeps taking writes while it is copied, so the copy is fuzzy and
the write-ahead log makes it consistent — the backup snapshot's argument
(`s3-backup.md` §2), applied to a local copy:

1. **Gates.** The snapshot's gates are taken (`backup::CopyGates`): no
   compaction, table rename or move — writers with no log record behind them —
   runs during the copy. The deletion gate only until the starting point is
   fixed. A backup snapshot and a branch therefore never copy at once.
2. **Hold checkpoints.** Every log the parent writes — its own and each storage
   location's — is held (`Wal::hold_checkpoints`): from here it keeps every
   record since its last truncation. A checkpoint already running is waited
   out. Commits go on; the log only grows.
3. **Copy the files**, flat: a database's files sit directly in its directory.
   A subdirectory is another database (the default database is the root),
   index runs that the branch rebuilds, or a storage location, copied as one.
   Left out: everything a snapshot leaves out (the live log, the durable floors,
   compaction scratch, index-store snapshots — rebuilt at open), plus
   replication's log and state, which belong to this node.
4. **Copy the log**, whole records only (`Wal::copy_log_to`), last.
5. **Replay** it over the copy with the restore's barrier-aware replay
   (`Wal::recover_restoring`), after cutting torn tails. Records are physical
   redo, so it does not matter which writes the copy already caught; a file
   created during the copy is created from its records; a table dropped during
   the copy is voided by its barrier. The branch is then the parent as of the
   last commit the copied log holds — `parent_lsn` in `SHOW BRANCHES`.
6. **Fix up**: `db_storage.cfg` is rewritten to name the branch's directories
   (a copy that kept the parent's absolute paths would read and write the
   parent's files — see `s3-backup.md` §10.12); a table on another disk is
   copied into `pool-location-<hash>` inside the branch; the NUMERIC fence is
   applied; the metadata is written; the staging directory is renamed into
   place and opened.

**Clone or copy.** Each file is cloned with `FICLONE` where the filesystem
shares blocks (btrfs, XFS, ZFS 2.2+, bcachefs) — near-free whatever the size —
and copied in full otherwise (ext4, NTFS). `method` says which. Before a full
copy, free space is checked for the copy, its base, and 256 MiB to spare, and
the branch is refused (53100) if it does not fit.

**Tables on a key/value store** have no files to copy and no log to replay.
After the replay, each store's tables are copied out of it as one
crash-consistent cut (`kv::export`, what a backup snapshot uses) into the
branch's directory: on the branch they are ordinary tables. Never onto the
store itself, where the branch would write among the parent's objects; for the
same reason the store definitions (`db_kv.cfg`, `db_kv_secrets.cfg`) are not
copied. Each store is cut at its own moment, just after the directories: a
transaction that wrote both a store's table and a directory's may be in one
copy and not the other. In-memory tables come over empty, with a warning.

**Refused:** the partitions of a partitioned table placed on a key/value store
(not copied by backups either; the message names the stores), and a server
running without the write-ahead log.

## 4. A past point

`AS OF` restores the S3 backup (`backup::restore_from`) into a hidden
directory beside the root, takes out the database asked for and its storage
locations, repoints its pool file, and continues as §3 step 6. It needs the
backup configured for this catalogue's root. Costs to know:

- the whole snapshot archive is downloaded — it holds every database — plus the
  log bundles up to the point;
- `TIMESTAMP` resolves to the last archive flush at or before it (log records
  carry no time), so it is as precise as the flush interval; `LSN` and
  `SNAPSHOT` are exact;
- tables that were on a key/value store come back as of the snapshot's copy.

## 5. The merge base

A three-way merge needs the state at the branch point: without it, "the branch
deleted this row" and "the parent inserted this row" look the same. So a branch
keeps a frozen copy of itself as it was made — on ext4/NTFS a second full copy,
with reflinks free. `WITHOUT BASE` opts out.

The base is opened once and kept open, and replaced only when a merge commits
or the branch is reset. A retired copy (an old base, a reset branch's old
copy, a dropped database) closes all its files when it is dropped — its
catalogue's background service ends with it — and its directory is removed
once they are closed (on ntfs-3g, removing open files leaves `.fuse_hidden`
leftovers).

## 6. Merge

`MERGE BRANCH dev`:

1. **The branch side** is a fresh live copy of `dev` (§3), so it is read at one
   point, and becomes the next base once the merge commits. Writes to `dev`
   after that point go to the next merge.
2. **Schema.** Each table's shape — columns with their types from
   `information_schema.columns`, index definitions from `pg_indexes` — is
   compared across base, branch and parent:
   - changed only on the branch: its DDL is replayed;
   - changed the same way on both: nothing to do;
   - changed differently on both: a conflict, which always refuses;
   - a dropped table or column needs `ALLOW DROP` (2BP01 otherwise). A column
     renamed on the branch reads as a drop and an add.

   The DDL replayed is what the branch *ran*: every successful `CREATE`,
   `ALTER`, `DROP` and `COMMENT` on a branch is appended to `rdb_branch.ddl`,
   inside a block only once the block commits. After replaying it, every table
   the branch changed must look on the parent exactly as on the branch, or the
   merge rolls back. A schema changed by a path that did not record its DDL is
   refused, not merged around.
3. **Rows**, compared as text as a client reads them:
   - a table with a primary or full unique key, row by row on the key:
     unchanged on the branch → skip; changed only on the branch → applied;
     the same on both → skip; otherwise a conflict. A key with NULLs repeated
     on any side falls back to the next rule;
   - a table with no key, as a multiset of whole rows. A keyless table whose
     columns the branch changed cannot be matched and is a conflict;
   - a column the base has and the parent does not (dropped there, or added
     by a branch this one was re-parented past, §8) is left out of what is
     written, and the diff's detail says so.
4. **Paged.** A table whose key has no nullable column is compared a page
   (20 000 rows) at a time on all three sides. A round reads the branch's next
   page, then the base's and the parent's within its range; a side whose page
   comes back full ends the round's range at its last row, and the sides read
   before it are read again up to there. So each round holds every side's rows
   of one range, and the range's bound is the engine's own ordering — nothing
   is compared by an ordering of the merge's own. Every read is in one
   `REPEATABLE READ` block per side: one snapshot, and a write over a parent
   row another session changed meanwhile is refused, not lost.
5. **Apply.** One transaction on the parent: the DDL; then a first pass,
   children-first by foreign key, which compares every table, counts its
   changes and conflicts, and runs its deletes; then, unless that found
   conflicts under `FAIL`, a second pass, parents-first, running the updates
   and inserts. As ordinary SQL — so constraints, foreign keys, indexes and
   triggers apply. Generated columns are not written. With `ON CONFLICT FAIL`
   (the default) any conflict rolls the whole merge back (40001) and names up
   to 20 of them; `BRANCH WINS` writes the branch's version, `PARENT WINS`
   keeps the parent's.
6. **Sequence keys.** In a table keyed on one `SERIAL` (or `DEFAULT
   nextval(…)`) column, both sides hand out the same numbers to different rows.
   So a row only the branch has is never matched to a parent row by number: it
   is inserted without its key (`INSERT … RETURNING`), taking the parent's next
   one, and every merged reference to it (a foreign key, in another table or
   its own) is rewritten from the branch's number to the new one — once, from
   the numbers the branch holds. Rows referencing each other are inserted in
   an order their references allow; a cycle among new rows is refused. The
   pairs are recorded in the parent's `__rdb_branch_keymap` (branch id, table,
   branch key, parent key) in the merge's own transaction, so a later merge
   compares those rows by their recorded keys (read after the ranges, by key),
   and a merge that committed before its base moved (a crash) re-offers
   nothing. `DROP BRANCH` deletes its records. The engine has no
   `GENERATED … AS IDENTITY`, so `SERIAL` is the one sequence key.
7. **After commit** the base moves to the branch side's copy and the replayed
   DDL is trimmed from the log, so a second merge offers only later changes.

The parent's role catalogue (`rdb_roles`), `__rdb_branch_keymap` and internal
tables are never merged. The `__rdb_*` tables holding views, sequences,
functions and triggers are merged as data.

**Limits.** A table with no key, or one whose key may hold NULLs, is read
whole into memory, as is a sequence-keyed table whose rows reference each
other, and one whose key holds a reference to renumbered rows. A sequence
key's recorded pairs are held while a merge runs (one per row ever
renumbered). DDL a transaction cannot roll back here (a statement `block_ddl_covers` does
not cover) is refused rather than applied outside the merge's atomicity — except
`CREATE INDEX`, which a rolled-back merge may leave behind harmlessly.

## 7. Reset, promote, drop

- **RESET** builds the new copy (and base) first, then swaps directories under
  the catalogue: the old branch is renamed aside, the new one into place, and
  opened; if it will not open, the old one is put back. A crash between the
  renames is repaired at start-up from the `.retiring-<name>` directory.
- **PROMOTE** detaches both, swaps the directories by three renames, rewrites
  each pool file for its new path, and keeps the parent's own lineage. The old
  parent becomes a branch without a base. Refused for the default database (it
  is the root directory) and while either side has sessions.
- **DROP BRANCH** is `DROP DATABASE` plus every base generation.

## 8. Branches of branches

A branch can be the parent of others: `CREATE BRANCH feature FROM dev`. The
copy leaves out `dev`'s own branch metadata and DDL log, so `feature` records
`dev` as its parent and starts with an empty log. Merges go one level at a
time: `MERGE BRANCH feature` into `dev` replays `feature`'s DDL there, where
`dev` records it as its own, so `MERGE BRANCH dev` carries it on to `prod`.

- **Dropping a middle branch** (`DROP BRANCH dev`, or `DROP DATABASE dev`)
  moves each branch made from it to `dev`'s parent, keeping its base — still
  the copy taken from `dev`. A later merge into `prod` therefore carries only
  `feature`'s own changes: rows `dev` changed and `feature` did not compare as
  "changed on the parent" and are left as `prod` has them; a column only `dev`
  had is left out (§6). `via` records the route. A branch of a database that
  was not a branch keeps naming it, and cannot be merged any more. Rows an
  earlier merge into `dev` renumbered (§6.6) were recorded in `dev`, which is
  gone: a later merge into `prod` no longer matches them by their new keys.
- **Promoting a middle branch** moves the promoted branch's own branches with
  its data: their parent becomes the name it now has. Branches of the old
  parent keep the name.
- `SHOW BRANCHES` gives each branch's `depth`, `path` and `children`, in tree
  order.

## 9. Studio

The BRANCHES tab draws the branches as a tree under the databases they grow
from, lists them (names indented under their parents while the grid is
unsorted), makes one (name, source, point, base), and per branch offers DIFF,
MERGE (conflict policy, allow drops; the statement is shown and confirmed),
RESET, PROMOTE and DROP, each behind a confirmation.

## 10. Not done

Replicating branches (replication carries the default database only);
partitions placed on a key/value store; merging between siblings (a merge goes
into the branch's own parent).
