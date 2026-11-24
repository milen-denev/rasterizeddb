# Releases

## Unreleased

### Statements that failed, now answered

- A JSON path written as an array constructor, `data #>> ARRAY['0', 'k']` or `#> ARRAY[…]`, failed ("unexpected character '['" in a `WHERE`, "Column … not found" in a select list, and no columns described once a driver's parameter was in it). A path of constants is now read as the path literal `'{0,k}'`; an element that is a column or an expression is still unsupported.
- SQL-level `PREPARE p(text) AS … WHERE data -> $1 ->> 'k' = …` then `EXECUTE p('0')` failed ("is neither a column nor a value"): a string for a text parameter now goes in as written, not as `CAST('0' AS text)`.
- A statement with more than 32,767 bound parameters (a 1,000-row `INSERT … VALUES` of 39 columns) dropped its connection with nothing in the server's log; the client read "expected to read 5 bytes, got 0 bytes at EOF". Parse and Bind now read their counts unsigned, as PostgreSQL does, up to 65,535 parameters. A Parse or Bind the server cannot read is answered with FATAL 08P01 naming it, and logged, before the connection closes.
- `GROUP BY 1` over a join failed with "Column '1' not found in schema", and so did `GROUP BY k` naming an output column (`t.system_key AS k`). Both now group by that select-list item, a real column of the same name winning, as in PostgreSQL; a position past the select list fails with 42P10. On one table both already worked.

### New features

- SQL Server front end: EF Core 10 runs unchanged, its Identity schema, migrations and `__EFMigrationsLock` included. Added: `CROSS/OUTER APPLY`, `TOP` in a derived table, a unique index filtered by `key IS NOT NULL`, `sp_getapplock`/`sp_releaseapplock`/`APPLOCK_TEST`, `sp_rename … 'INDEX'`, `ALTER DATABASE … SET SINGLE_USER`, and bracketed table names holding dots (`[dbo].[DenevCloud.ERP.Colors]`). Database names are matched without case, and a pooled connection's reset goes back to the login's database.
- Case-insensitive text columns (the engine's `rdb_ci` collation) group, deduplicate, join, sort in windows, and take `min`/`max`, foreign keys, `ALTER TABLE ADD COLUMN` and `ALTER COLUMN TYPE` by key.
- Opt-in zstd compression (`storage_compression`, a default feature): a column takes `COMPRESSION zstd(n)` and a table `WITH (row_compression = …)`, levels 1–22. Nothing is compressed unless asked, and fixed-width values stay raw. `ALTER TABLE … RECOMPRESS` and `VACUUM (FULL, RECOMPRESS)` rewrite rows and train dictionaries. The client's ORM takes `#[rdb(compression)]` and `#[rdb(row_compression)]`. See `docs/compression.md`.

### Behaviour changes

- **Over the SQL Server protocol, text columns are case-insensitive by default**, as under SQL Server's default `SQL_Latin1_General_CP1_CI_AS`: a `CREATE TABLE` or `ADD COLUMN` without a `COLLATE` gives a text column `rdb_ci`. `--mssql-collation bin` keeps the old byte comparison. Tables made over PostgreSQL, MySQL or the native protocol are unchanged.
- A `CREATE UNIQUE INDEX … WHERE …` over the SQL Server protocol ignored its filter and refused rows the filter leaves out; the filter is honoured now.
- `STORAGE MOVE … TO 'kv:…'` said the table was no longer covered by backups. A backup copies it as of each snapshot, with no point-in-time replay, and the message now says so.

### Upgrade notes

- **No downgrade after a compressed value:** the first one adds `compressed_values_v1` to the directory's `incompat_features`, and an older build refuses to open that directory.

## 1.0.1 (2026-11-24: server, `rasterizeddb_core`, `rasterizeddb_client`) and `rasterizeddb_embedded` 1.1.0

### Silent wrong answers fixed

- **Replicated defaults diverged.** On a replicating node, a column whose `DEFAULT` is not deterministic
  (`gen_random_uuid()`, `uuidv7()`, `clock_timestamp()`, `random()`, and also `now()`) was evaluated again
  by every peer, so an `id uuid DEFAULT gen_random_uuid()` row had a different key on every node. The
  value is now written into the replicated `INSERT … VALUES` / `DEFAULT VALUES`, once per row. Where no
  single value can stand for it, the statement is refused (0A000) with what to write instead: an
  `INSERT … SELECT` that leaves such a column to a per-row default, `UPDATE … SET c = DEFAULT`, and
  `ALTER TABLE … ADD COLUMN … DEFAULT gen_random_uuid()` on a table that has rows (its backfill). An
  `ADD COLUMN … DEFAULT now()` backfills one instant on every node and keeps `now()` as the default; on an
  empty table, or with `IF NOT EXISTS` for a column that exists, `ADD COLUMN` runs as written.
  `uuidv7()` written in a statement is now folded like `gen_random_uuid()`. Only replicating nodes change.
- `SELECT -id AS id … ORDER BY id` (an output alias shadowing a column), and `ORDER BY 1`, sorted by the stored column, including under `LIMIT`, `DISTINCT` and window functions.
- Set operations mixing type families (`UNION` of `text` and `bigint`) answered with blank, lost, NULL or garbage values. They now fail with 42804, as in PostgreSQL.
- `id IN ((correlated scalar subquery), 3)` in a select list was true for every row.
- An uncorrelated `EXISTS (…)::int` in arithmetic was folded to a wrong value.
- `ARRAY(SELECT x FROM t ORDER BY 1)` ignored the `ORDER BY`.
- A `regtype` column sorted and compared by name rather than by OID.
- A second `CHECK` on one column was dropped and never enforced.
- Replication carried only the default database. Every database now replicates, `CREATE`/`DROP DATABASE` included.
- A copied, moved or mirrored data directory read and wrote the original directory's files, because `db_storage.cfg` names absolute paths. A copy with one storage location is now rewritten for its new path; any other copy is refused at open.
- Two processes, or two embedded engines, could open one data directory at once: the second replayed and truncated the first's live log. The directory is now locked (`rasterizeddb.lock`), and the refusal names the holder's pid.
- Client: a `NUMERIC` with more than 38 decimal places read as a wrong integer.
- `SHOW TABLES` and the table statistics named only the first column of a composite primary key.
- `SHOW REPLICATION STATUS` described `applied` as what a node received; it counts what peers applied of this node's own transactions, so a replica read `applied 0`. `duplicates` and `conflicts` were described the same way. The descriptions now say so; what a node received is the `applied through` line.

### Statements that failed, now answered

Each was met migrating an application onto 1.0.0.

- Over the extended protocol (every driver's prepared statements), a CTE whose body reads a derived table (`WITH r AS (SELECT … FROM (SELECT …) x) SELECT … FROM r`) was described with no columns and its rows sent anyway, so drivers failed on every row.
- `BEGIN; DROP TABLE child; DROP TABLE parent; COMMIT` was refused because of the child's foreign key, although the child's drop was pending in the same block.
- A correlated reference in an inner `JOIN … ON` inside `EXISTS` (`… JOIN m ON m.space_id = t.space_id`) failed with "missing FROM-clause entry".
- A window function in a query filtered by a correlated `EXISTS` that compares with `<` or `>` was refused.
- `make_interval(hours => t.h)` over a column was refused; it now computes per row, `NULL` in giving `NULL` out.
- A `DEFAULT` written as a parenthesised cast (`DEFAULT ('[]'::jsonb)`, `('{}'::text[])`), as ORMs and `pg_dump` write it, was refused.
- A select-list alias written without `AS` (`ROW_NUMBER() OVER (…) rn`) did not name a CTE's or derived table's column.
- A bound parameter cast to a multi-word type (`$1::timestamp with time zone`, `double precision`, `character varying`) took only the type's first word, and the rest joined the value.
- SQL-level `PREPARE … LIMIT $1` then `EXECUTE` failed with "Invalid LIMIT value 'CAST(8'".

### Crashes fixed

- A scheduled backup failed ("db_storage.cfg.tmp was listed for the snapshot and is gone") when it listed the directory while the server rewrote `db_storage.cfg`, `db_kv.cfg` or `db_kv_secrets.cfg`.
- A multibyte character (`Δ`, `😀`) next to a parenthesis or a keyword in a `WHERE` (`ΔANY(…)`, `Δ(b) > 5`) or a `JOIN LATERAL` (`t😀 AS l`) panicked the statement's handler.

### New features

- Composite foreign keys, including on and onto hash-partitioned tables.
- `DO $$ … $$` blocks.
- `DROP INDEX` drops an index made by `CREATE UNIQUE INDEX`. It used to be refused.
- A primary key can take any name, inline keys included.
- `pg_dump` 16, 17 and 18 dump a RasterizedDB database, including the rows of partitioned tables and composite foreign keys on them.
- Redis-compatible KV cache: replication between nodes, and TLS on the Redis port.
- Replication: `REPLICATION DISABLE` covers every database, and `SHOW REPLICATION STATUS` has a row per database.
- Partitioned tables:
  - the partition key is carried across join equalities, so fewer partitions are read;
  - the open-partition ceiling is a setting (`--partition-max-open`);
  - partitions and indexes are fast on key/value and S3 tables.
- `SET CONFIG` changes made while running are saved across a restart.
- Native protocol:
  - notices;
  - `SHOW` in autocommit;
  - a statement over `--rdb-request-timeout` is answered with 57014 and the connection survives.
- SQL:
  - `date + timetz`, `CREATE TABLE … AS VALUES`, `pg_get_viewdef`, `::regtype` over expressions, `generate_subscripts`;
  - `pg_index.indclass` and `indoption`;
  - aggregates over `jsonb`, `text` and `bool`;
  - `(x LIKE y)::text`;
  - a row that references itself in its own `INSERT`;
  - correlated subqueries in more places: `EXISTS` with non-`=` predicates, `IN`, row `IN`, `UPDATE … SET v = (…) RETURNING`, set operations, derived-table `UNION`.
- Speed:
  - `UPDATE … FROM` no longer syncs the log once per row;
  - a correlated subquery no longer scans its table once per outer row;
  - `FROM a, b WHERE a.x = b.y` is planned as a join, not a cross join.
- Packaging:
  - Docker images on Docker Hub (`milendenev/rasterizeddb`) and GHCR: Ubuntu 24.04, Ubuntu 26.04, Alpine, and a `rustfs` tag with RustFS for backups and S3 tables;
  - an Arch Linux package with a systemd service;
  - static musl builds for Linux x86-64 and ARM64.
- `rasterizeddb_embedded` 1.1.0:
  - every server setting is a `Builder` option: `config(key, value)` for every `SHOW CONFIG` key, typed methods, and `low_memory()`;
  - one engine per process;
  - the host keeps its own signal handlers.
- `rasterizeddb_client` 1.0.1:
  - no panic on hostile text (time zones, cursors, keys);
  - SCRAM refuses iteration counts above 1,000,000;
  - sends are safe to drop mid-flight;
  - notices.

### Behaviour changes

- Errors on partitioned tables carry PostgreSQL's codes: 42P16 where XX000 or 0A000 was answered. `pg_class.relkind` is `'p'`.
- A second primary key, under any name, fails with 42P16.
- These are refused with 0A000 and a message naming the cast to write:
  - set operations with a `date` column beside a `timestamp`;
  - a string constant in the first branch beside a typed later branch;
  - a `regtype` sort key in grouped, `DISTINCT` or set queries.
- An `UPDATE` matching no row no longer fails a `CHECK` its constant `SET` would break; it answers `UPDATE 0`.
- A row breaking several `CHECK`s reports the first by name, as PostgreSQL does.
- An unnamed `CHECK` is named as PostgreSQL names it: `<table>_<column>_check`, or `<table>_check` for a table-level one, which used to be `<table>_check1`. Tables created before keep their names.
- On a server started without `--node-id`, `SET CONFIG replication.<key>` and `REPLICATION SET` change the setting instead of failing with "replication needs a node id". Enabling replication and adding a peer still need an id.
- A throttled S3 write (429/503) is retried ten times, and can take about 25 seconds before it fails.

### Upgrade notes

- Tables replicated by 1.0.0 that use non-deterministic defaults may already differ between nodes
  (different UUIDs or timestamps for the same row). Compare them (`SELECT … ORDER BY` on each node) and
  re-copy the affected tables from one node.
- **Replicating clusters:** upgrade the receiving nodes first and the node taking writes last. Every node must run this release before any of these:
  - `CREATE UNIQUE INDEX`: its replicated text carries `AS INDEX`, which 1.0.0 cannot parse;
  - KV cache writes on a replicating node;
  - replicating a database other than the default one.

  A database that existed before replication was turned on still needs a base copy (a restore) on each node.
- **No downgrade after a composite foreign key:** the first one adds `composite_foreign_key_v1` to the directory's `incompat_features`, and 1.0.0 refuses to open that directory.
- **`CREATE UNIQUE INDEX` keys after a downgrade:** 1.0.0 reads them as unique constraints, so `DROP INDEX` on them is refused again.
- **`rasterizeddb_core`'s Rust API changed** and is not covered by semver: it is the server's internals. The server's flags, wire protocols and SQL are what is stable. Applications should use `rasterizeddb_embedded` or `rasterizeddb_client`, which follow semver.

## 1.0.0 (2026-11-23)

The first stable release:
- a SQL database server speaking the PostgreSQL, native (rastcp), MySQL / MariaDB, SQL Server (TDS) and Redis protocols;
- MVCC transactions and a write-ahead log;
- B-tree, hash, GIN and JSON path indexes, plus the SIMD filter index;
- hash partitioning;
- multi-primary replication;
- S3 backup and point-in-time recovery;
- database branches, storage pools and key/value storage;
- PostgreSQL import;
- the rdb_studio desktop console.

Executables for Linux and Windows (x86-64 and ARM64). Crates: `rasterizeddb_core`, `rasterizeddb_embedded`, `rasterizeddb_client`, `rastcp`.

## 0.1.0 (2025-09-08)

- Initial build: Includes creating table, creating rows, quering rows.
