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

## What the figures mean

Every size is read from the storage layer rather than counted, so `SHOW TABLES`
over a hundred tables is a hundred `stat` calls and not a hundred scans.

The consequence is that **`row_slots` counts pointer records, not live rows**. A
deleted row keeps its slot until a vacuum reclaims it, and an updated row that
had to move leaves one behind. It is an upper bound, and it is named `row_slots`
rather than `rows` so that nothing reads it as the latter. A client that wants
the exact figure runs `SELECT COUNT(*)`, which scans.

`reclaimable_bytes` is what a `VACUUM` would return. `index_bytes` is the
persisted bit-sliced filter checkpoint, not a B-tree.

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
