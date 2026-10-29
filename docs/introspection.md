# Asking a database about itself

RasterizedDB answers a small statement family that describes the database, its
tables, its schema and its runtime configuration. It exists because a console —
`rdb_studio`, or anything else — needs those answers, and the two obvious ways
to provide them are both worse.

```text
SHOW DATABASE STATS          -- one metric per row, for the whole database
SHOW STATS                   -- the same thing, shorter
SHOW TABLES                  -- a row per table, with its sizes
SHOW TABLE STATS <table>     -- one metric per row, for one table
SHOW COLUMNS FROM <table>    -- a row per column   (also: DESCRIBE <table>)
SHOW SCHEMA MAP              -- a row per relationship, for drawing a graph
SHOW DATABASES               -- what the catalogue holds
SHOW CONFIG                  -- every runtime knob, its value, and how to change it
SHOW CONFIG KEYS             -- what SET CONFIG accepts
SET CONFIG <key> = <value>   -- change one, where the engine can do it live

SHOW QUERIES [n]             -- the last n statements, newest first
SHOW QUERY STATS             -- a row per statement shape, with its totals
RESET QUERY STATS            -- forget both

SHOW BACKUP STATUS           -- can this server back up, where to, and is it working
SHOW REPLICATION STATS       -- node state and counters, as rows
SHOW REPLICATION PEER STATUS -- a row per peer
```

Everything is in `core/introspect.rs`, except `SHOW DATABASES`, which is
answered by `pgwire` because the catalogue sits above the engine and the
executor is only ever handed one database.

## Why not `information_schema`

PostgreSQL's catalogue is a set of tables, so a client introspects by selecting
from them. Nothing here is stored that way: the schema lives in a per-table
sidecar, the sizes live in the filesystem, and the tunables live in process
memory. Emulating `pg_class` would mean materialising all three into rows a
query planner then filters — the whole cost of a catalogue, to answer questions
that are a field read each.

So these are statements rather than relations. They take no `WHERE`, they return
a fixed shape, and the shape is the one a console renders directly.

## Why not just parse the log

The other obvious answer is to expose nothing and let tooling scrape. That makes
every figure a format, and a format nobody declared is a format nobody can
change. A result set has column names and types the wire protocol describes, so
a client binds to those and a new metric is an added row rather than a broken
parser.

## What is holding a table

`SHOW TABLES` names the backend of every table, and `SHOW TABLE STATS` says
where it is:

```
table_name | storage      | row_slots | ...
customers  | file         |         2 | ...
archive    | kv-directory |         1 | ...
sessions   | memory       |    10_412 | ...
```

| `storage` | what it means |
|---|---|
| `memory` | in this process, and gone when it exits |
| `file` | ordinary files in a directory |
| `mirrored` | files, written to a primary directory and to backup ones |
| `s3` | objects in an S3-compatible store |
| `kv-directory` | the object layout, in a local directory |
| `kv-memory` | the object layout, in a map |

One database can hold tables on several of these at once — `STORAGE PLACE TABLE
t AT 'kv:archive'` puts one table on an object store while the rest of the
database stays on files — which is why this is a column of the table list and
not a property of the database. `SHOW DATABASE STATS` reports the database's own
backend under the same names.

The three `kv-*` answers are one storage engine over three backing stores, and
they are deliberately not folded into a single `object`: what an operator *does*
about a slow table differs completely between them, since a local directory is a
disk to look at and S3 is a request count to pay for. For the same reason the
kind is a fixed one-word vocabulary rather than the store's description —
`storage_location` carries the path or the bucket, and it is free to be reworded;
`storage` is what a console groups by, and it is not.

For a table on a key/value store, `SHOW TABLE STATS` also reports the page cache
in front of it — `kv_cache_hits`, `kv_cache_misses`, `kv_cache_bytes`. Those are
the store's rather than the table's, shared by every table placed on it, and the
`detail` column says so. On an object store the miss count is the figure that
matters: each one is a request, and requests are what an object store charges
for.

## What the figures mean

Every size is read from the storage layer rather than counted, so `SHOW TABLES`
over a hundred tables is a hundred `stat` calls and not a hundred scans.

The consequence is that **`row_slots` counts pointer records, not live rows**. A
deleted row keeps its slot until a vacuum reclaims it, and an updated row that
had to move leaves one behind. It is an upper bound, and it is named `row_slots`
rather than `rows` so that nothing reads it as the latter. A client that wants
the exact figure runs `SELECT COUNT(*)`, which scans.

`avg_row_bytes` is the measured average — row data divided by pointer records —
where `fixed_row_width` is the declared minimum. The gap between them is what the
variable-width columns actually cost, which is the figure capacity planning wants
and the one that cannot be worked out from the schema.

`reclaimable_bytes` is what a `VACUUM` would return, and `dead_ratio` is that as
a proportion of the row data. The ratio is the one to read: the decision it
informs — is a `VACUUM` worth taking — is about the proportion and not the size,
since a gigabyte of dead space in a terabyte table is noise and the same gigabyte
in a two-gigabyte table is half the disk. `vacuum_enabled` says whether
reclamation is running *for this table*, which is the question `SHOW CONFIG`
cannot answer: the switch is per table, so all it can report there is the words
"per table". `index_bytes` is the
persisted bit-sliced filter checkpoint, not a B-tree.

`forgotten_below` is the transaction id below which outcomes are no longer
looked up (every row stamped below it has been proven free of aborted ids by
vacuum's forget sweeps), and `forgetting_held_by` names the hash-partitioned
tables that cannot be opened and therefore hold it where it was when they were
refused: until such a table opens or is dropped the transaction status log
grows (2 bits per transaction), and the server also logs a warning naming it
every ten minutes. `partition_compaction_held_by` names the hash-partitioned
tables whose map log is not compacted because torn-tail evidence
(`<map log>.torn`) is still beside it: their map log grows until an operator
examines and removes that file (the server logs it once per open, at the first
compaction it holds off). `partition_maintenance_panics` counts panics the
shared partition maintenance caught and survived.

## What `SHOW DATABASE STATS` does not report

Two kinds of row used to be there and are not.

**Settings.** `wal_enabled`, `wal_checkpoint_bytes`, `filter_enabled`,
`filter_min_pruning_ratio`, `query_cache_enabled`, `query_stats_enabled`,
`replication_active`, `batch_size`, `concurrent_threads`. They are not
measurements: they say what this server was told to do, they do not move while
it runs, and every one of them is already reported by `SHOW CONFIG` — which is
also the only place they can be changed. Reported in both places they were most
of the list, which pushed the figures that actually move off the page, and a
constant sitting beside the sizes invites being read as a statistic.

**Internal ids and internal relations.** `database_id` and
`replication_node_id` name this process to the engine and nothing a client does
depends on either. And the `__…` relations — a JSON column's shadow table, the
temporary relation a CTE or a catalogue view materialises into — are counted by
neither `tables` nor `columns`, because `SHOW TABLES` does not list them and a
console whose front page disagrees with its table list about how many tables
exist is a console nobody trusts.

The byte totals still cover **every** file the database owns, shadow tables
included. That is what the disk holds, and a size that left them out would be
wrong in the direction that matters.

## Configuration, and what "hot reload" means

`SHOW CONFIG` returns `(section, key, value, hot_reload, set_with, help)`. The
`hot_reload` column is the whole point of the statement: some knobs a running
server can change and some it cannot, and the difference is not cosmetic.
`batch_size` is read once into a `OnceLock` that every later reader assumes is
stable, so there is no safe way to change it mid-flight — `SET CONFIG` refuses
it and names the flag instead of failing obscurely.

`SET CONFIG` delegates every hot key to the subsystem that already owns it:

| section | goes to |
|---|---|
| `filter.*` | `SET FILTER …` (`core::filter::sql`) |
| `replication.*` | `REPLICATION SET …` (`replication::sql`) |
| `vacuum.enabled` | `Table::set_vacuum_enabled`, for every table in the database |

That is what keeps `SET CONFIG filter.max_f = 20` and `SET FILTER max_f = 20`
from drifting into meaning different things.

A change is **not persisted**. It takes effect immediately and is gone after a
restart unless the matching command-line flag is added too — the same rule the
replication surface already follows, so that what a node comes up as is always
what its command line says.

## The schema map

`SHOW SCHEMA MAP` returns one row per relationship:

```
kind | from_table | from_column | to_table | to_column | constraint_name | on_delete | on_update
```

`kind` is `fk` for a declared foreign key and `json` for a column mapped onto a
shadow table. The second is a real relationship here rather than an
implementation detail: that table is where a document's fields are queried from.

## The primary key is now recorded

`CREATE TABLE … PRIMARY KEY` used to collapse into `UNIQUE NOT NULL` on the
column, and `TableSchema.primary_key` — a field that exists, is persisted, and
is read back — stayed `None` forever. Enforcement never needed it, because a
primary key is a unique constraint and was already enforced as one.

Every client that addresses a single row does need it. "The column this table
calls its key" and "some column that happens to be unique" are different
questions, and only the first has one answer. A console that guessed would build
`UPDATE … WHERE <some unique column> = …`, which is right until a table has two.

So `CREATE TABLE` now records it, in the schema header where the field always
was. A composite `PRIMARY KEY (a, b)` is left unrecorded: the header holds one
name, and recording half of a two-column key would say something false. The
constraint is still enforced — it lives in the unique-key sidecar like any other
composite.

Tables created by an older binary have no key recorded and will report none.
Recreating the table records it; nothing else needs to change.

## Over the wire

Each statement describes its own columns, for both the simple and the extended
query protocol. That second one is not a detail: without a `Describe` arm the
extended protocol answers `NoData` and then sends `DataRow`s anyway, which every
driver reads as a protocol violation — so a statement works under `psql` and
fails under any ORM. `SHOW REPLICATION` had exactly that gap and it was fixed at
the same time.

## `SHOW BACKUP STATUS`

`SHOW BACKUPS` lists the catalogue. It cannot answer the question asked before
that one — *is there a destination at all, and is it working* — because an empty
catalogue and a missing destination look identical from the outside, and so does
a binary built without the feature. Three states, three different things to do
about them:

| `feature_compiled` | `configured` | what to do |
|---|---|---|
| `false` | — | rebuild with `--features s3_backup` |
| `true` | `false` | start with `--s3-bucket` |
| `true` | `true` | look at `archiving_suspended` |

That last one is the failure this statement exists for. When uploads start
failing, segments pile up in the staging directory; past `staging_max_bytes` the
service goes back to discarding the log, because losing point-in-time coverage
is bad and taking the database down with a full disk is worse. From that moment
recovery has a hole in it — and nothing in `SHOW BACKUPS` says so, because the
catalogue simply stops growing, which is exactly what a quiet database looks
like.

`recoverable_to_lsn` is the other figure worth promoting: the highest commit any
archived bundle covers, which is the real answer to "how far forward can I
recover", and which `SHOW BACKUPS` makes you work out by reading every row.

## `SHOW REPLICATION STATS` and `SHOW REPLICATION PEER STATUS`

`SHOW REPLICATION`, `... STATUS`, `... PEERS` and `... SETTINGS` return lines of
text. That is right for `psql` and for a runbook — they are read, not parsed.

A console wants columns. It aligns them, sorts them, and colours a peer that is
disconnected, and turning `peer berlin: connected, lag 3` back into fields with
a regular expression works until somebody improves the wording. So the two
structured forms are added *beside* the text ones rather than replacing them:
nothing that reads the text output changes, and no client has to parse prose.

`STATS` is `(metric, value, detail)` like the other status statements.
`PEER STATUS` is a row per peer:

```
peer | address | remote_node | connected | paused | acked_seq | lag | queued
     | records_sent | batches_sent | dropped | codec | last_ratio | last_error
```

Two things there are worth knowing. `dropped` counts records that could not be
queued — they are **not lost**, since the sender backfills from the log, but a
queue that fills is a peer that is not keeping up. And `max_peer_lag` in `STATS`
is the figure an operator actually watches: how far behind the furthest-behind
peer is, which otherwise means reading every row and taking a maximum.

Every value that has a spelling the parser accepts is printed in that spelling —
`auto:zstd:1`, not `Compression { mode: Auto, codec: Zstd, ... }` — so anything
copied out of a status result can be pasted into a `REPLICATION SET`.

## `SHOW QUERIES` and `SHOW QUERY STATS`

Two questions, and they are not the same one. *What has this server been doing*
is a list in time order, which is what an operator watching a slow page wants.
*Which statement is the expensive one* is a list in cost order, which is what
someone with a week of traffic behind them wants. `SHOW QUERIES` answers the
first, `SHOW QUERY STATS` the second, and both are built from one record written
when a statement finishes.

```
started_at | duration_ms | rows | rows_scanned | materialised | materialised_ms | status | database | username | statement | error
queryid | calls | total_ms | mean_ms | min_ms | max_ms | stddev_ms | rows | rows_scanned | materialised | materialised_ms | errors | last_seen | statement
```

### `rows_scanned` says *why* — when the statement is scan-bound

`rows` is what the client received. `rows_scanned` is what the scan had to look
at to find it, counted where every scan dispatches its work — after the filter
index has excluded what it can, so a statement that reads a tenth of a table
reports a tenth of it. Four readings:

| `rows` vs `rows_scanned` | what it means |
|---|---|
| 3 of 10,000,000 | a predicate nothing indexes; the whole table was read to find three rows |
| 10,000,000 of 10,000,000 | the statement asked for the table, and got it |
| 3 of 0 | answered from the query cache, which read no pointers at all |
| 3 of 0, and slow | **the cost was not scanning** — see below |

That last row is the one to be careful with, because the column answers it
honestly and uselessly. `rows_scanned` is a scan-cost proxy, and not every
expensive statement is expensive because of a scan: a CTE or a derived table is
materialised into a real durable relation — `CREATE TABLE __cte_…`, a multi-row
`INSERT`, then `DROP TABLE` — so wrapping a trivial query in `WITH … AS` pays
three durable round trips before it reads anything. Measured on a live node,
`SELECT COUNT(*) FROM t` at 0.25 ms became 136 ms as
`WITH s AS (SELECT id FROM t) SELECT COUNT(*) FROM s`, flat and independent of
how much data `t` holds — while reporting almost no rows scanned.

That figure is a **disk** measurement and does not reproduce from the test
suite: the same materialisation costs about 1 ms on tmpfs against 200–695 ms on
ext4, and `tempfile::tempdir()` puts every test on tmpfs. A reader who measures
it there will get a millisecond and conclude this paragraph is wrong.

So read a high `duration_ms` against a low `rows_scanned` as "the cost was not
scanning", never as "this cannot be the slow one". Today that difference is
usually materialisation, and nothing here counts it yet.

A join counts every relation it scanned, because each one dispatches through the
same place.

### `materialised` is the cost `rows_scanned` cannot see

A CTE and a derived table are each built as a real relation before the outer
statement runs — `CREATE TABLE __cte_…`, a multi-row `INSERT`, `DROP TABLE`
through the ordinary executor. No scan happens, so `rows_scanned` reports such a
statement honestly and uselessly: the column that exists to say *why* says "it
did no work" for what may be the most expensive shape on the server. A correct
number that means nothing is harder to distrust than a wrong one.

`materialised` counts the relations one statement built and `materialised_ms`
says what they cost. They are separate columns rather than one summed "cost",
because they are different kinds of number: `rows_scanned` is read against
`rows` as a ratio, while a materialisation count means something alone and has
no denominator. A statement can be expensive for either reason or both, and one
column would hide the mix.

Counted at the two places a relation is actually built — the `WITH` path and
`materialize_query` — rather than at the entry points that walk them, because a
derived table inside a CTE body goes through both and would otherwise be counted
twice. A materialisation that *failed* is not counted: it produced nothing to
have been slow for, so on a failed statement the figure is a floor.

These relations used to be durable, at a flat ~130 ms each on a real filesystem
regardless of row count. They are memory-backed now, since a statement-scoped
relation has no reader that could outlive it, and that cost is gone — the
counter is how you confirm that is still true rather than taking it on trust.
Which matters more than usual, because no test could have caught the original
problem: the same materialisation is ~1 ms on tmpfs, and `tempfile::tempdir()`
puts every test there.

### One shape, not one statement

`WHERE id = 1` and `WHERE id = 2` are the same statement asked twice, so the
aggregate folds them together: literals become `$1`, `$2`, whitespace collapses,
comments go, and the `queryid` is a hash of what is left. Quoted identifiers and
bare names are kept — `col1` is a name, not a name and a number.

The hash is FNV-1a rather than the engine's usual `ahash`, which is randomly
seeded: a `queryid` has to name the same statement after a restart, or a
dashboard keyed on it starts a new series every time the server comes back.

### `pg_stat_statements`

The same aggregate under PostgreSQL's own name, produced on demand like every
other catalogue relation:

```sql
SELECT query, calls, mean_exec_time, rows, rows_scanned
FROM pg_stat_statements
ORDER BY total_exec_time DESC
LIMIT 10;
```

Times are milliseconds, as the extension reports them. `shared_blks_hit` and
`shared_blks_read` are reported as zero rather than omitted, so a dashboard that
selects them parses; this engine has no shared buffer pool to count. The
`rows_scanned` column is this engine's addition. `SELECT pg_stat_statements_reset()`
works too, and is the same thing as `RESET QUERY STATS`.

Built without `--features query_stats`, the name is not claimed at all — so
selecting from it fails the way PostgreSQL fails when the extension is not
installed, rather than returning an empty relation a monitor would read as "this
server has run nothing".

### What is not recorded at all

A statement that describes the server rather than using it is not written to
either half of the log when it succeeds — not to `SHOW QUERIES`, not to `SHOW
QUERY STATS`, not to `pg_stat_statements`. One that **failed** is kept: what
the server refused and why is half of what that surface answers, and a refused
`SHOW COLUMNS FROM …` is exactly the row somebody looks for when a console has
gone blank. It cannot flood the log either — an introspection statement that
fails is rare by construction. Three families count: `SHOW`/`SET`/`RESET`/`DISCARD`
statements; anything naming `pg_catalog.…`, `information_schema.…` or a bare
`pg_…` relation; and anything naming a `__…` internal relation. Names inside
string literals do not count — `WHERE relname = 'pg_class'` is a query *about*
the catalogue name, run against a table the application owns.

This is not tidiness. A console refreshes its table list, its column lists and
its statistics every few seconds, and each refresh is a statement: recorded,
they are the overwhelming majority of what the log holds. The ring fills with
`SHOW COLUMNS FROM …` before one statement the user typed reaches the bottom of
it, and the costliest-shape list ranks the console's own polling above the
application's slowest query. The log answers "what has this server been doing
for the application", and a client asking the server about itself is not that.

The filter is applied when the statement finishes rather than before it runs,
because that is the one point where the text is known; what an ignored
statement costs is an `Instant` and an `Arc`.

### What it costs, and what it keeps

A ring of 256 statements and a map of at most 5,000 shapes, both in memory and
both lost on restart. Per statement that is one mutex acquisition; per scan it is
one relaxed atomic add per batch of ten thousand rows, which is why the counter
is not per row and not per worker. `SET CONFIG query_stats.enabled = off` stops
recording without forgetting what was recorded, and `SET CONFIG
query_stats.recent = N` resizes the ring.

Two things are deliberately not stored. A statement's text is cut at the first
keyword that means a secret follows — `PASSWORD`, and the S3 credential options
a store carries (`ACCESS_KEY_ID`, `SECRET_ACCESS_KEY`, `SESSION_TOKEN` and their
shorter spellings) — whatever the statement is, because this surface is readable
by any session that can run `SHOW QUERIES` and a credential that reached the ring
would outlive the statement that set it. Per-store keys are deliberately kept out
of `db_kv.cfg` and written to a second file at mode 0600; recording the statement
that set them would put them straight back into a surface anyone can read. The
cut takes everything after the earliest match, and the bare words `SECRET` and
`TOKEN` are included on purpose: truncating an ordinary query whose column is
called `token` costs a log line, and not truncating one that is carrying a
session token costs the token. And nothing is written to disk: a
statement that cost a second statement would make the log the busiest relation
in the database and the write path both the thing being measured and the thing
doing the measuring.
