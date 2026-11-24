
# Rasterized DB
## A high-performance database written from scratch in Rust — now fully rewritten.

![rdb_studio connected to a RasterizedDB server](docs/images/rdb_studio.png)

*[rdb_studio](#rdb_studio--the-desktop-console), the desktop console, watching a
live server: size and row counts, reclaimable space, transaction rate, cache
hits against scans, and where the bytes actually are.*

RasterizedDB is a schema-full storage engine that speaks the PostgreSQL wire
protocol, so `psql`, any driver and any ORM connect to it unmodified. It is
transactional (MVCC, real `BEGIN`/`COMMIT`/`ROLLBACK`, savepoints), crash-safe
through a write-ahead log, and it reclaims its own dead space. Rows are scanned
through a bit-sliced SIMD filter index rather than a B-tree.

### What is in it

| | |
|---|---|
| **PostgreSQL wire protocol** | Simple and extended query protocols, bound parameters, SCRAM-SHA-256, TLS. Drivers and ORMs work as-is — see [SQL surface](#sql-surface) |
| **Type system** | 40 PostgreSQL types, including intervals, MAC and bit strings, the seven geometric types and the six ranges — each with its real wire OID |
| **Transactions** | MVCC snapshots, `READ COMMITTED` / `REPEATABLE READ`, savepoints, first-updater-wins conflicts |
| **Durability** | Write-ahead log with checkpoints, and a `synchronous_commit` knob with the trade stated exactly |
| **Filter index** | Bit-sliced fingerprints, 512 rows per SIMD register, plus zone maps for ranges. Adaptive and runtime-tunable |
| **Space reclamation** | Background vacuum publishing a free list the write path allocates from; `VACUUM` and `VACUUM FULL` |
| **JSON** | Documents stored as binary values, with path indexing |
| **Replication** | Multi-primary, statement-shipping, HLC-ordered, over TLS 1.3 |
| **Backup** | Snapshot and point-in-time recovery to any S3-compatible store |
| **Branches** | A database copied live (or from a past point), then reset, promoted, diffed and three-way merged back |
| **Storage pools** | One database across several directories, with a placement policy and mirrored backups |
| **Key/value storage** | A database kept in an object store instead of files, per table, alongside file and in-memory tables |
| **PostgreSQL import** | One-shot migration of a live PostgreSQL database |
| **rdb_studio** | A desktop console that also speaks to PostgreSQL |

Design notes for each subsystem live in [docs/](docs/) — they cover the
reasoning, the alternatives that were measured and rejected, and the parts
deliberately not built.

### Guide on how to run

Download the archive for your platform from the [releases](https://github.com/milen-denev/rasterizeddb/releases) (tags `v*`):

| Platform | Archive | CPU |
|---|---|---|
| Linux x86-64 | `rasterizeddb-server-<version>-linux-x86_64.tar.gz` | x86-64-v3 (AVX2: Intel Haswell / AMD Excavator or later) |
| Linux ARM64 | `rasterizeddb-server-<version>-linux-arm64.tar.gz` | any ARMv8-A (Graviton, Ampere, Raspberry Pi 4/5, ...) |
| Windows x86-64 | `rasterizeddb-server-<version>-windows-x86_64.zip` | x86-64-v3 (AVX2) |
| Windows ARM64 | `rasterizeddb-server-<version>-windows-arm64.zip` | any ARMv8-A (Snapdragon X, ...) |
| Linux x86-64, static (musl) | `rasterizeddb-server-<version>-linux-x86_64-musl.tar.gz` | x86-64-v3 (AVX2) |
| Linux ARM64, static (musl) | `rasterizeddb-server-<version>-linux-arm64-musl.tar.gz` | any ARMv8-A |

The glibc Linux builds need glibc 2.39 or newer; the static musl builds run on any Linux, Alpine included.

**Docker** (linux/amd64 and linux/arm64; Ubuntu 24.04 by default, also `-ubuntu26.04` and `-alpine` tags; [docs/docker.md](docs/docker.md)):

```sh
docker run -d --name rasterizeddb -e RASTERIZEDDB_PG_PASSWORD=change-me \
  -p 5432:5432 -p 61170:61170 -v rasterizeddb:/var/lib/rasterizeddb \
  milendenev/rasterizeddb:latest          # or ghcr.io/milen-denev/rasterizeddb:latest
```

The `rustfs` tag runs [RustFS](https://github.com/rustfs/rustfs) in the same container, with the server's backups and S3-backed tables already pointed at it ([docs/docker.md](docs/docker.md#rustfs-backups-and-s3-tables-in-one-container)).

**Arch Linux / CachyOS** (x86-64-v3): each release carries `rasterizeddb-bin-<version>-1-x86_64.pkg.tar.zst`, with a systemd service:

```sh
sudo pacman -U rasterizeddb-bin-*.pkg.tar.zst
sudoedit /etc/rasterizeddb/rasterizeddb.env     # RASTERIZEDDB_PG_PASSWORD=...
sudo systemctl enable --now rasterizeddb
```

Unix sockets (`--rdb-socket`, `--pg-socket-dir`, peer authentication) exist on Linux only; on Windows use TCP.

To run an archive's executable yourself, give it a data directory:

```sh
rasterizeddb --location /var/lib/rasterized --protocol both --pg-password <password>
```

`--concurrent-threads` sets the number of worker threads (default 16) and `--batch-size` the rows read per batch (default 10 000). `rasterizeddb --help` lists every flag; the sections below cover them by subsystem. Built with `cargo install rasterizeddb_core`, the same executable is named `rasterizeddb_core`; on x86-64 build it with `RUSTFLAGS="-C target-cpu=x86-64-v3"`.

### Build features

The heavier subsystems are Cargo features, so a build that does not want one
does not carry its code, its dependencies, or its match arms. The current
default set turns most of them on:

```toml
default = ["memory_pool_reuse", "replication_compression", "s3_backup",
           "pg_import", "storage_pools", "kv_storage", "kv_storage_s3",
           "query_stats", "kv_cache", "persistent_indexes", "mysql_wire",
           "mssql_wire", "db_branching"]
```

| Feature | Default | What it adds |
|---|---|---|
| `s3_backup` | on | Backup, restore and PITR to an object store, and the `BACKUP`/`RESTORE` statements. Implies `storage_pools` |
| `pg_import` | on | The PostgreSQL frontend client and `IMPORT DATABASE FROM POSTGRES` |
| `storage_pools` | on | Multi-directory placement, mirroring, and the `STORAGE` statements |
| `kv_storage` | on | A table kept in a key/value store rather than in files, and the `kv:` placement spec. Implies `storage_pools` |
| `kv_storage_s3` | on | The S3 backing store for `kv_storage`. Implies `kv_storage` and `s3_backup` |
| `kv_cache` | on | A Redis-compatible key/value store: `KV …` statements, and the Redis protocol on `--kv-port` |
| `mysql_wire` | on | The MySQL / MariaDB client protocol on `--mysql-port` / `--mysql-socket` |
| `mssql_wire` | on | The SQL Server (TDS) client protocol on `--mssql-port` / `--mssql-socket` |
| `query_stats` | on | `SHOW QUERIES`, `SHOW QUERY STATS` and `pg_stat_statements` |
| `db_branching` | on | `CREATE BRANCH`, `RESET`/`PROMOTE`/`DROP BRANCH`, `SHOW BRANCH DIFF`, `MERGE BRANCH`, `AS OF`. Implies `s3_backup` |
| `replication_compression` | on | Zstd on replication links; a node without it still interoperates, uncompressed |
| `memory_pool_reuse` | on | Thread-local free lists in the buffer pool |
| `enable_long_row` | off | Rows above the standard width cap |
| `enable_data_verification` | off | Extra checksums on the read path |

Replication itself needs no feature — it is compiled in and switched on with
`--replication`. To build without the optional subsystems:

```sh
cargo build --release -p rasterizeddb_core --no-default-features \
    --features memory_pool_reuse,replication_compression
```

### PostgreSQL network compatibility (pgwire)

The server speaks the PostgreSQL v3 wire protocol ("pgwire"), so `psql`, any
driver and any ORM can connect to it. See `docs/postgres-compatibility.md` for
what the SQL layer does and does not support.

Start it, and connect with an ordinary connection string:

```sh
cargo run -p rasterizeddb_core -- --location <PATH> --protocol pgwire \
    --pg-addr 0.0.0.0 --pg-tls disable \
    --pg-user superuser --pg-password <password>

psql "postgresql://superuser:<password>@127.0.0.1:5432/postgres?sslmode=disable"
```

Every part of that string matters:

- **`/postgres`** names a database. `--pg-default-db` sets which one the
  location directory holds (`postgres` by default); others are created with
  `CREATE DATABASE` and live in subdirectories of it. A name that does not exist
  is refused rather than silently served the default.
- **`sslmode=disable`** needs `--pg-tls disable` or `prefer`. The default is
  `require`, which refuses a cleartext connection instead of downgrading it. With
  no certificate given, a self-signed one is generated at start-up.
- **`superuser`** must be a role. `--pg-user`/`--pg-password` seed one the first
  time a database's role catalogue is empty; after that, use `CREATE USER`.
  Roles are per database.
- **`--pg-addr`** defaults to `127.0.0.1`, which is not reachable from another
  host.

Authentication defaults to SCRAM-SHA-256; `--pg-auth` also accepts `md5`,
`password` and `trust`. Both the simple and the extended query protocols are
implemented, which is what lets prepared statements and ORMs work.

`--protocol both` serves this and the native protocol (`rasterizeddb_client`)
from one process, on one catalogue. The `--pg-*` flags configure this listener
and `--rdb-*` the native one (`--rdb-addr 0.0.0.0 --rdb-port 61170` to reach it
from another host). The two share the role catalogue, which is seeded from
`--pg-user`/`--pg-password`, so the same role logs in on either; the native
protocol answers SCRAM only, so keep `--pg-auth scram-sha-256` there. A
native connection uses `--pg-default-db` unless its client names another
database (`Options::database`); as on pgwire, a login is checked against the
roles of the database it names.

### MySQL and SQL Server network compatibility

Beside PostgreSQL, the server speaks the **MySQL / MariaDB** client protocol
and the **SQL Server** protocol (TDS 7.4, and TDS 8.0's TLS-first mode). Each
is off until it is given a port or a unix socket, and all of them run at once,
on the same databases and the same roles:

```sh
cargo run -p rasterizeddb_core -- --location <PATH> --protocol all \
    --pg-port 5432 --pg-password <password> \
    --mysql-port 3306 --mysql-socket /run/rdb/mysql.sock \
    --mssql-port 1433

mysql -h 127.0.0.1 -P 3306 -u rasterized -p postgres
sqlcmd -S 127.0.0.1,1433 -U rasterized -P <password> -d postgres -C
```

`--protocol all` (or `both`) also serves the native protocol beside pgwire.
A MySQL "database" and a SQL Server database are the engine's databases;
`master` (and the other SQL Server system databases) is the default one.

| Flag | Default | |
|---|---|---|
| `--mysql-port`, `--mssql-port` | off | TCP port to serve on |
| `--mysql-addr`, `--mssql-addr` | `127.0.0.1` | Address to listen on |
| `--mysql-socket`, `--mssql-socket` | off | Unix socket (`--socket-mode`, `--socket-group` apply) |
| `--mysql-auth`, `--mssql-auth` | `password` | `password` or `trust` |
| `--mysql-socket-auth`, `--mssql-socket-auth` | the TCP method | Authentication on the socket |
| `--mysql-tls`, `--mssql-tls` | `prefer` | `disable`, `prefer` or `require` |
| `--mysql-cert`/`--mysql-key`, `--mssql-cert`/`--mssql-key` | `--pg-cert`/`--pg-key`, else self-signed | TLS certificate |

Logins are checked against the role catalogue the PostgreSQL listener uses
(`CREATE USER` there, or over any protocol). MySQL clients authenticate with
`caching_sha2_password`, MySQL 8's default: a role's first login sends its
password over TLS (or the unix socket), later ones use the fast scramble. SQL
Server clients use SQL Server authentication; Windows authentication is not
offered.

Each connection is translated onto an engine session, so transactions,
isolation, prepared statements and cancellation behave as they do over
PostgreSQL. Both dialects are rewritten into the engine's: MySQL's backticks,
`LIMIT a, b`, `AUTO_INCREMENT`, `ON DUPLICATE KEY UPDATE`, `INSERT IGNORE`,
`SHOW …`, `@@variables` and functions; T-SQL's `[brackets]`, `TOP`,
`OFFSET … FETCH`, `IDENTITY`, `OUTPUT INSERTED`, `+` concatenation, batches
with variables, `IF`/`WHILE`/`TRY…CATCH`, `sp_executesql` and the RPC calls
drivers make, and the `sys.*` / `INFORMATION_SCHEMA` views. An error inside a
transaction fails only its statement, as in both of those databases.

What the engine does not have, the front ends keep: stored procedures (T-SQL
and MySQL's language, with cursors and handlers), temporary tables (`#t`,
table variables, `CREATE TEMPORARY TABLE`), cursors (T-SQL's and MySQL's
`COM_STMT_FETCH`), T-SQL's `MERGE`, `FOR JSON` / `FOR XML PATH`, bulk load
(`SqlBulkCopy`), MySQL's `LOAD DATA LOCAL INFILE` and
`ON UPDATE CURRENT_TIMESTAMP`. See
[docs/mysql-and-sql-server.md](docs/mysql-and-sql-server.md) for what each
dialect is translated into, and what is refused rather than answered
differently.

Verified with the `mysql` 8.4 and `mariadb` 11.4 clients, a `mysqldump`
loaded as is, sqlx-mysql, SQLAlchemy 2 + PyMySQL, Microsoft.Data.SqlClient,
Dapper, EF Core 10, tiberius and ODBC `sqlcmd`. Text compares as bytes (the
servers report `utf8mb4_bin` / `Latin1_General_BIN2`), not case-insensitively.
See [`docs/mysql-and-sql-server.md`](docs/mysql-and-sql-server.md) for what is
translated, what is refused, and why.

### SQL surface

The dialect is PostgreSQL's. What is implemented:

- **Projection** — `*`, column lists, `AS` aliases, qualified and quoted names,
  case-insensitive resolution, arithmetic, `SELECT DISTINCT`.
- **Filtering** — every comparison operator on every scalar type, `AND`/`OR`
  with parentheses, `IN`, `NOT IN`, `BETWEEN`, `LIKE` and `ILIKE` with the full
  pattern language, and `SIMILAR TO` with alternation, grouping, quantifiers
  and bracket expressions.
- **Structural operators** — `@>`, `<@` and `&&` over documents, arrays, ranges
  and geometry; `<<`, `<<=`, `>>` and `>>=` over networks; `@@` for text search.
  An operator with no meaning for its operands is an **error**, not `false` —
  answering `false` would drop every row and look like an empty table.
- **Joins** — `INNER` and `LEFT JOIN`, any number of relations, aliases, and
  **composite keys** (`ON a.x = b.x AND a.y = b.y`), which every junction table
  needs. Executed as a hash join over materialised rows, each relation scanned
  once with its own share of the `WHERE` pushed down, so filter pruning still
  applies per relation.
- **Aggregates** — `COUNT`, `SUM`, `MIN`, `MAX`, `AVG` with `GROUP BY` and
  `HAVING`, over a single table or a join. `SUM` and `AVG` over an exact type
  stay exact, so a money column keeps its cents.
- **Window functions** — the ranking calls (`ROW_NUMBER`, `RANK`, `DENSE_RANK`,
  `PERCENT_RANK`, `CUME_DIST`), the value calls (`LAG`, `LEAD`, `FIRST_VALUE`,
  `LAST_VALUE`, `NTH_VALUE`) and the aggregates over a window, with `ROWS` and
  `RANGE` frames. Not an aggregate: every row survives and gains a column
  computed from the rows around it. The default frame follows PostgreSQL's, so
  `SUM(x) OVER (ORDER BY t)` is a running total and `SUM(x) OVER ()` is the
  partition total.
- **Full-text search** — `to_tsvector`, `to_tsquery`, `plainto_tsquery`,
  `phraseto_tsquery`, `websearch_to_tsquery`, `ts_rank` and `@@`, with
  PostgreSQL's `english` stop-word list and Snowball stemmer, so a query for
  `runs` finds a document that said `running`. `CREATE INDEX … USING gin
  (to_tsvector('english', title || ' ' || body))` indexes it: a match over
  any of those columns in that configuration is answered from the index.
- **CTEs** — `WITH name AS (SELECT …)`, materialised, which gives PostgreSQL's
  pre-12 semantics: an optimisation fence, evaluated once however many times it
  is referenced.
- **Subqueries** — uncorrelated `IN (SELECT …)`, folded into the outer statement
  as a literal list so filter pruning still applies.
- **Ordering and paging** — `ORDER BY` over several keys each with its own
  direction, including expressions and keys that are not selected; `LIMIT`,
  `OFFSET`, and the `FETCH FIRST … ROWS ONLY` spellings.
- **Writes** — `INSERT` (single and multi-row), `INSERT … SELECT`, `UPDATE`,
  `DELETE`, `DELETE … USING`, `ON CONFLICT DO UPDATE`, and `RETURNING` on all of
  them — where a returned item may be an expression, not only a column.
- **Expressions** — `CASE WHEN`, `COALESCE`, `NULLIF`, `LEAST`, `GREATEST`,
  `MOD`, `array_length`, `array_ndims`, and `::` casts — over a column as well
  as a literal — in projections, predicates and `RETURNING`.
- **Interval arithmetic** — `timestamp - timestamp` is an `interval`, and
  `timestamp ± interval` moves the **calendar**: 31 January plus a month is
  28 February, not 3 March.
- **Constraints** — `NOT NULL`, defaults (kept as text, so
  `DEFAULT CURRENT_TIMESTAMP` means the time of the insert, and likewise
  `clock_timestamp()`, `CURRENT_DATE` and `CURRENT_TIME`), primary keys,
  single- and multi-column `UNIQUE`, foreign keys, and `CHECK` — all
  **enforced** rather than merely accepted. A `CHECK` that evaluates to NULL
  passes, which is PostgreSQL's rule and the opposite of a `WHERE` clause.
- **Generated columns** — `GENERATED ALWAYS AS (…) STORED`, computed from the
  row as it will be stored, so it sees the values defaults filled in. A
  client-supplied value for one is refused, as PostgreSQL refuses it.
- **Serial columns** — `SERIAL`, `SMALLSERIAL` and `BIGSERIAL`, expanded to
  what PostgreSQL expands them to: an integer column, `NOT NULL`, defaulting to
  the next value of a sequence made with the table. As in PostgreSQL, a value
  written explicitly into the column does not advance the sequence, so a later
  default can repeat it. After a restart a plain table's sequence resumes above
  the largest value stored, while a hash-partitioned table's resumes from a
  durable reservation: exactly after a clean shutdown, and up to 1,024 values
  further after a crash (a gap, as PostgreSQL's sequences leave). Only the
  table whose `SERIAL` made the sequence reserves it: a second table that shares
  it through a hand-written `DEFAULT nextval('…')` does not.
- **DDL** — `CREATE`/`DROP TABLE`, `CREATE DATABASE`, `ALTER TABLE` including
  `ADD`/`DROP`/`RENAME COLUMN` and `ALTER … TYPE … USING`.
- **Catalogue** — PostgreSQL's catalogue relations are produced on demand when a
  statement names one, so tools that introspect `pg_catalog` work.
- **`pg_stat_statements`** — the same view PostgreSQL's extension provides,
  built from the engine's own statement log, plus a `rows_scanned` column
  PostgreSQL has no counterpart for.

The notable gaps, each pinned by a test in `rasterizeddb_compat` that names it
rather than being left to be discovered:

- `LATERAL` set-returning functions in `FROM`, and `WITH ORDINALITY`.
- Function calls inside a `WHERE` clause. The scan's comparison path takes
  column bytes and literals and has nowhere to evaluate an expression per row —
  which is why `to_tsvector(…)` works in a select list but not in a predicate.
- A join `ON` that is an `OR` of predicates.
- The `jsonb_path_query` family, and multi-dimensional array subscripts.

`cargo test -p rasterizeddb_compat -- --ignored` runs them as a work list.

See [docs/postgres-compatibility.md](docs/postgres-compatibility.md) for the
type mapping, the exact semantics, and what is not supported.

### Transactions

`BEGIN`, `COMMIT` and `ROLLBACK` do what they say. They used to be accepted and
ignored — each statement committed as it ran, so an application that rolled a
transaction back had already committed every statement in it, with no way to
find out.

The engine is MVCC: an update writes a new row version rather than overwriting
one a reader might still be looking at, and each statement reads through a
snapshot. Uncommitted work is invisible to other sessions, a rollback undoes
inserts, updates and deletes alike, and two transactions updating the same row
get a serialization failure (`40001`) rather than one silently losing.

`READ COMMITTED` (the default) takes a fresh snapshot per statement;
`REPEATABLE READ` fixes one for the transaction. `SERIALIZABLE` is accepted and
behaves as `REPEATABLE READ` — it does **not** detect write skew, which needs
predicate locking.

Savepoints work — `SAVEPOINT`, `ROLLBACK TO SAVEPOINT`, `RELEASE` — which is
what an ORM's nested transactions are built from. A failed statement poisons
the block until it is closed or rolled back to a savepoint. `SELECT … FOR
UPDATE` and `FOR SHARE` take row locks.

See [docs/mvcc.md](docs/mvcc.md).

### Durability

Writes go through a write-ahead log. Without one, every durable write has to
reach storage on its own: an `INSERT` appends row bytes to one file and a
pointer record to another, paying two device flushes, and a crash between them
leaves the row half-written with no way to tell. The log turns that into one
sequential append plus one flush of a single file — and because a flush
persists everything written before it, concurrent writers share it.

`synchronous_commit` is PostgreSQL's knob and the same trade:

```sh
rasterizeddb_core --location <PATH> --synchronous-commit off --wal-writer-delay-ms 200
```

**On** (default), nothing is acknowledged until its records are durable, which
costs one `fdatasync` per writing statement — on a drive without a
power-loss-protected cache that is most of what the statement takes.

**Off**, a commit returns once the bytes are in the *kernel's* page cache. The
server process dying then loses nothing — `kill -9` and a panic are both
survivable — and only an operating system crash or power loss costs the last
`--wal-writer-delay-ms` of transactions. Records are appended in order and
replay stops at the first one it cannot read whole, so what is lost is whole
transactions off the end of the log, never half of one. It is a durability
knob, not a consistency one.

### Indexes

`CREATE INDEX` builds a real B-tree or hash index over one or more columns,
and every `PRIMARY KEY`, `UNIQUE` column and composite unique constraint gets
one implicitly. With no `USING`, the method is chosen from the column types:
a hash where only equality is ever asked (UUID, JSONB), a B-tree everywhere
else. A query uses an index for `=`, `IN`, ranges, `BETWEEN` and prefix
`LIKE`, and a `UNIQUE` check probes it rather than scanning. Point reads by
primary key are 5–6.6× faster than without, and upserts 4.8×.

An index may only ever return too much, never too little: the scan rechecks
every candidate. An end-to-end test runs an indexed and an unindexed table
through the same random writes and requires identical answers. See
[docs/indexes.md](docs/indexes.md).

### The filter index

Scans are pruned by a bit-level index rather than a B-tree. It is a positional
shadow of the row-pointer file — bit *i* describes slot *i* — so no
key-to-slot map is needed and the structure costs *bits* per row rather than
bytes per entry. Values are stored as fingerprints, bit-sliced across bitplanes
per 512-row zone, so one SIMD register tests 512 rows at once (AVX-512 where the
CPU has it, AVX2 otherwise). Zone maps carry min/max bounds for the ranges a
fingerprint cannot answer.

The safety property is one-sided: the index may say a zone *might* match when it
does not, and the scan then confirms. It must never say a zone cannot match when
a matching row is there — so a predicate the extractor is not certain it models
exactly is dropped rather than approximated.

It is adaptive and tunable while the server runs:

```sql
SHOW CONFIG;                    -- every knob, and whether it is hot-reloadable
SET CONFIG filter.max_f = 14;   -- applied live
```

```sh
rasterizeddb_core --location <PATH> --filter true --filter-max-f 14 \
    --filter-min-table-rows 50000 --filter-min-selectivity 0.05
```

See [docs/bit-bloom-filter-design.md](docs/bit-bloom-filter-design.md) for the
cost model, the measured skip-versus-fetch ratio, and the tiers not built.

### Space reclamation

A `DELETE` tombstones a pointer record, and an `UPDATE` that outgrows its slot
tombstones the old one and appends a replacement. Neither touches the row bytes,
so without reclamation the rows file only ever grows: a table that deletes and
reinserts the same working set doubles on disk every cycle even though its live
size never changes.

A background task scans the pointer file, works out which byte ranges no pointer
refers to any more, and publishes them as a free list the write path allocates
from before falling back to appending. Nothing moves, so no offset changes, and
the pass can be interrupted at any point. Reclamation is snapshot-aware: an
extent is not reused while any open query could still read it.

```sql
VACUUM;             -- reclaim into the free list
VACUUM FULL;        -- compact in place and return space to the filesystem
SHOW TABLE STATS users;   -- includes how much is reclaimable
```

Pointer slots are given back only by `VACUUM FULL`, so a background task runs
it on a table or partition whose slots are mostly dead: at least 2 048 slots,
and either 8× its live rows or a quarter dead (the latter up to 131 072 slots,
so it never stops a large table for long), each at most once per 10 minutes,
skipping one that is in use. Every threshold is a hot `SET CONFIG` key and a
flag:

```sql
SET CONFIG auto_compact.dead_fraction = 0.3;   -- also enabled, interval_secs,
                                               -- min_slots, ratio, cooldown_secs,
                                               -- dead_fraction_max_slots
```

```sh
rasterizeddb_core --location <PATH> --auto-compact on --auto-compact-min-slots 4096 \
    --auto-compact-dead-fraction 0.3 --auto-compact-cooldown-secs 900
```

See [docs/space-reclamation.md](docs/space-reclamation.md).

### JSON documents

A JSON column is stored as one binary value. It used to be *shredded* — each key
became a column, each nested object a further table — which does not survive
contact with real documents, for a structural reason: shredding makes the schema
a function of the data. A document keyed by data (statistics keyed by column
name, a row keyed by a user-defined field) has unboundedly many keys, so the
table grows a column per distinct key.

Shredding survives as a knob (`--json-shadow-tables`) for the workloads it did
suit, but it is no longer the default. Reaching into a document from SQL, and
indexing a particular path so a predicate over it does not parse every document,
are both supported — the path index reuses the same filter machinery rather than
adding a second index structure.

### Native protocol authentication

The native protocol reads the same role catalogue as pgwire, so one
`CREATE USER` works for both. Its client is the
[`rasterizeddb_client`](rasterizeddb_client) crate, which does not compile the
engine:

```sh
cargo run -p rasterizeddb_core -- --location <PATH> \
    --rdb-user superuser --rdb-password <password>
```

```rust
use rasterizeddb_client::{Client, Login, Options};

let client = Client::connect(
    "127.0.0.1:61170",
    Options {
        login: Login::password("superuser", "<password>"),
        ..Options::default()
    },
)
.await?;
```

`Login::Password` runs SCRAM-SHA-256 bound to the connection, so the password
never crosses the wire, and it refuses a server that lets the connection in
without proving it holds the verifier. On a unix socket (`--rdb-socket`),
`Login::Peer` logs in as the operating-system user, and `Login::Trust` sends no
credentials.

`Options::database` picks the database, as the `/name` of a PostgreSQL URL
does. Password and peer logins name it in the login and are checked against
that database's roles; a trust connection selects it with its first request.
Every connection the client opens uses it, a reopened one and each
transaction's included. A name that does not exist fails `connect` (3D000),
and so does a server too old to select a database: neither quietly gives you
the default database.

TCP is TLS. The server makes itself a self-signed certificate at every start
unless given one with `--rdb-cert`/`--rdb-key`; a client that should verify it
sets `Options::trust` (`rasterizeddb_client::load_ca("ca.pem")?` for a private
CA, `Trust::WebPki` for a public one). The default encrypts without
authenticating the server.

The client shares its connections (`Options::connections`, one by default)
between all the statements in flight, which is the faster arrangement whenever
more than one is: 4 concurrent callers ran 12% more point queries a second over
one shared connection than over a pool of their own, and 128 ran 33% more. A
statement that fails in transit (a dropped connection, a timeout) is reported,
never sent again: the server may already have carried it out, and a repeated
`INSERT` is a duplicate row.

Prefer the `RASTERIZEDDB_PASSWORD` environment variable to `--rdb-password`: a
password on the command line is visible in `ps` to every other user on the
machine.

What it insists on, and why:

- **TLS 1.3 only**, on both ends. Nothing but this crate speaks the protocol, so
  there is no legacy peer to accommodate and no reason to keep TLS 1.2's
  negotiable ciphers and downgrade dances. 0-RTT early data is off, because a
  replayed request here is a repeated write.
- **SCRAM-SHA-256 only.** `md5` and cleartext exist in the PostgreSQL listener
  for drivers that speak nothing else; `--rdb-auth` takes `scram-sha-256`
  (default) or `trust` and refuses the other two.
- **Channel binding is mandatory** (RFC 9266 `tls-exporter`). This is what makes
  the exchange safe even though the client does not verify the server's
  self-signed certificate by default: an attacker who intercepts the connection
  cannot relay the exchange onto their own connection to the real server,
  because the proof is bound to the connection it was made on.
- **The client checks the server too**, and refuses a server that waves it
  through without a challenge.
- Role statements over this protocol require a superuser, and are never written
  to the log in full — their text contains a password.

With no password configured and no account in the database holding one, the
loopback listener accepts any caller and says so loudly at start-up: a database
nobody can open is not a secure database. It starts requiring a password the
moment there is one to require, and never stops again.

### Unix sockets

Every listener can also be served on a unix socket, for clients on the same
machine, and its TCP port can be turned off. On a socket there is no TCP stack
and no TLS: only root can watch a local socket, and who may connect is decided
by the socket file's permissions and group.

| Listener | Socket | Turn TCP off | How callers on the socket log in |
|---|---|---|---|
| Native (`rasterizeddb_client`) | `--rdb-socket PATH` | `--rdb-tcp off` | `--rdb-socket-auth` (defaults to `--rdb-auth`) |
| Redis protocol | `--kv-socket PATH` | leave out `--kv-port` | `--kv-auth` |
| PostgreSQL | `--pg-socket-dir DIR` | `--pg-tcp off` | `--pg-socket-auth` (defaults to `--pg-auth`) |

`--pg-socket-dir` creates `DIR/.s.PGSQL.<pg-port>`, the name every PostgreSQL
client looks for: `psql "host=DIR port=5432"`, tokio-postgres's `host_path`,
sqlx's `socket`, and rdb_studio with the directory as its host. A TLS request
there is answered "no" whatever `--pg-tls` says, as PostgreSQL does, so the
default `--pg-tls require` applies to TCP only.

`--socket-mode` (octal, default `600`: this server's user only) and
`--socket-group NAME` apply to every socket. Connecting needs write permission,
so `--socket-mode 660 --socket-group app` admits the members of `app`, which is
how a service running as another user is given access.

- **`peer`**, offered on sockets only: a role may log in without a password when
  its name is the operating-system user connecting, which the kernel reports.
  The role must exist and be allowed to log in.
- **`trust`** is allowed on a socket only when its mode admits nobody outside
  its owner and group (`660` yes, `666` no), as it is allowed on TCP only on
  loopback.
- A socket file left behind by a server that did not shut down is replaced at
  start-up. One another server is listening on, or a path that is not a socket,
  is refused rather than taken. The file is removed at shutdown.
- A socket's path has to fit a socket address: about 107 bytes on Linux.

Measured on one machine with 64-byte requests, a unix socket doubled the
requests one connection could carry (81,000 to 160,000 a second, median
latency 12 µs to 5 µs). A single point query's median latency fell from 67 µs
to 56 µs. Under heavy concurrent load the query engine, not the transport, is
the limit, and the two perform the same.

### rdb_studio — the desktop console

A pgAdmin-style console for RasterizedDB, written in [iced](https://iced.rs).
It also speaks to PostgreSQL, because both answer the same wire protocol.

```sh
cargo run -p rdb_studio                # pick a connection
cargo run -p rdb_studio -- my-profile  # open a saved one straight away
```

Live dashboard, database and table statistics, a schema map drawn from the
foreign keys, an editable data browser, a SQL editor, an activity screen showing
what the server has been running and how much of each table it had to read, S3
backup and restore, a storage view for placing tables across disks, a
replication view, and a configuration screen that knows which settings a running
server can actually change. See [rdb_studio/README.md](rdb_studio/README.md).

It reads the introspection statements the engine gained for it —
`SHOW TABLES`, `SHOW DATABASE STATS`, `SHOW SCHEMA MAP`, `SHOW CONFIG`,
`SET CONFIG` and the rest — which work from `psql` too:

```sql
SHOW DATABASE STATS;
SHOW TABLES;                  -- with a storage column: memory, file, s3, ...
SHOW TABLE STATS users;
SHOW COLUMNS FROM users;      -- or DESCRIBE users
SHOW SCHEMA MAP;
SHOW DATABASES;
SHOW CONFIG;                  -- with a hot_reload column per setting
SET CONFIG filter.max_f = 14; -- applied live
SHOW QUERIES;                 -- the last statements, with time and rows scanned
SHOW QUERIES 20;              -- just the last twenty
SHOW QUERY STATS;             -- one row per statement shape, slowest first
RESET QUERY STATS;            -- forget both
SHOW BACKUP STATUS;           -- configured? healthy? how far can a restore reach?
SHOW REPLICATION STATS;       -- node state and counters, as rows
SHOW REPLICATION PEER STATUS; -- a row per peer
SHOW STORAGE STATS;           -- the pool, as rows
SHOW STORAGE LOCATIONS;       -- a row per directory, with free space
SHOW STORAGE PLACEMENTS;      -- which directory holds each table
```

See [docs/introspection.md](docs/introspection.md) for what each figure means
and why the surface is statements rather than an `information_schema`.

### Multi-primary replication

Every node accepts reads and writes. A transaction committed on one is shipped
to the others and applied there, so the cluster converges without any node being
designated the writer and without a failover step when one goes away.

```sh
# node 1
rasterizeddb_core --location /data --replication --node-id 1 \
    --peer 'two@10.0.0.2:61171' --peer 'three@10.0.0.3:61171'

# node 2
rasterizeddb_core --location /data --replication --node-id 2 \
    --peer 'one@10.0.0.1:61171' --peer 'three@10.0.0.3:61171'
```

What travels is a committed transaction as the statements that made it up,
stamped with the node that took it, its sequence number, and a hybrid logical
clock reading. Statements rather than pages, so the nodes are not tied to one
physical layout. The clock is what makes the ordering agreeable: wall-clock time
alone does not give one, since two nodes a second apart disagree about which
commit came first, and a clock that steps backwards makes a node disagree with
itself.

Links run over TLS 1.3 with mutual certificate pinning or a cluster CA, batch
and compress (zstd, negotiated per link, falling back to uncompressed against a
node built without it), and catch a peer up from the replication log after a
disconnect. Conflict policy is `abort` by default, with `skip` and
`last-writer-wins` available.

From SQL, over either protocol:

```sql
REPLICATION ENABLE;
REPLICATION ADD PEER 'berlin' ADDRESS '10.0.0.2:61171';
REPLICATION ADD PEER 'tokyo' ADDRESS '10.0.1.7:61171' WITH (COMPRESSION = 'zstd:3');
REPLICATION PAUSE PEER 'berlin';
REPLICATION RESYNC PEER 'berlin' FROM 0;
REPLICATION SET conflict = 'last-writer-wins';

SHOW REPLICATION;           -- summary and a line per peer
SHOW REPLICATION STATUS;    -- counters
SHOW REPLICATION PEERS;
SHOW REPLICATION SETTINGS;
```

Unlike the storage statements these are **not** persisted: a peer added with a
statement is gone after a restart unless the flag is added too, so what a node
comes up as is always what its command line says.

See [docs/replication.md](docs/replication.md) for the wire format, the
measured compression and batching figures, the authentication setup, and — the
part worth reading before relying on it — what it guarantees and what it does
not.

### Multiple storage directories

On in the default feature set (`storage_pools`). A database that has only
ever been given one directory keeps behaving exactly as it did — the whole
subsystem compiles away without the feature.

Name the disks on the command line:

```sh
rasterizeddb_core --location /var/lib/rasterized \
  --storage-location /mnt/nvme1 \
  --storage-location /mnt/nvme2:primary:2 \
  --storage-location /mnt/backup:backup \
  --storage-policy free-space --storage-min-free 4GiB
```

Each location is `PATH`, `PATH:ROLE` or `PATH:ROLE:WEIGHT`. A **primary** holds
tables; a **backup** receives a mirrored copy of every write, including the
catalogue, so the directory opens as a database on its own rather than as an
archive that has to be restored first. `WEIGHT` multiplies a primary's share
under the `free-space` policy — a location with weight 2 is chosen while it has
half as much free space as one with weight 1.

Three policies decide where a new table's files are created:

| policy | what it does |
| --- | --- |
| `primary` (default) | always the first enabled primary |
| `round-robin` | the enabled primary holding the fewest tables |
| `free-space` | the enabled primary with the most available bytes, weighted |

`--storage-min-free` is a floor: a location below it is skipped. "Primary +
backup" is one primary and one backup; "balance across four disks, all mirrored
to a fifth" is four primaries and one backup. There is no separate mode switch
for either.

From SQL, over either protocol:

```sql
STORAGE ADD LOCATION '/mnt/nvme2' WITH (ROLE = 'primary', WEIGHT = 2);
STORAGE ADD LOCATION '/mnt/backup' WITH (ROLE = 'backup');
STORAGE SET POLICY = 'free-space';        -- primary | round-robin | free-space
STORAGE SET MIN_FREE = '4GiB';

STORAGE DISABLE LOCATION '/mnt/nvme1';    -- drain: keeps its tables, takes no new ones
STORAGE MOVE TABLE orders TO '/mnt/nvme2';
STORAGE REBALANCE DRY RUN;                -- what it would move, and why
STORAGE REBALANCE;                        -- do it
STORAGE DROP LOCATION '/mnt/nvme1';       -- refused while it still holds tables

SHOW STORAGE;                             -- a line per location and store, with free space
SHOW STORAGE STATS;                       -- pool-wide figures as columns
SHOW STORAGE LOCATIONS;                   -- a row per location
SHOW STORAGE STORES;                      -- a row per key/value store
SHOW STORAGE PLACEMENTS;                  -- which directory each table is in
SHOW STORAGE POLICY;
SHOW STORAGE KEYS;
```

A directory is not the only thing a table can sit on. An object store is defined
by statement too, and then a table is placed on it or converted onto it:

```sql
STORAGE ADD STORE archive S3 WITH (
    ENDPOINT          = 'https://s3.us-east-1.amazonaws.com',
    BUCKET            = 'my-bucket',
    PREFIX            = 'rdb/archive',
    ACCESS_KEY_ID     = 'AKIA...',        -- or leave both out and it signs with
    SECRET_ACCESS_KEY = '...');           -- the server's AWS_* environment

STORAGE PLACE TABLE events AT 'kv:archive';   -- before CREATE TABLE
STORAGE MOVE TABLE orders TO 'kv:archive';    -- ... or convert one that exists
STORAGE MOVE TABLE orders TO 'memory:';       -- files, memory, kv, S3: one statement
STORAGE MOVE TABLE orders TO '/mnt/nvme2';    -- and back
```

The copy is provider to provider, so every pairing of backends is the same
operation. What differs is what the table gains or loses by arriving — backups,
the write-ahead log, the free-space policy are features about *files* — and each
move says which. See [docs/kv-storage.md](docs/kv-storage.md).

Unlike the replication statements, these are **persisted**: where each table
lives is recorded in `db_storage.cfg` in the database directory, and a location
forgotten at restart would leave every table created into it unfindable. The
flags are applied on top of that file rather than replacing it, so a service
unit can name its disks on every start.

Placement is recorded, never recomputed. Changing the policy or adding a disk
moves nothing on its own — `STORAGE REBALANCE` is the statement that moves
tables, and it can be asked what it would do first. Nothing is moved onto a
location that would itself drop below the free-space floor as a result.

The STORAGE tab in [rdb_studio](#rdb_studio--the-desktop-console) is the same
thing with the free space drawn in: a row per directory with how full it is, a
row per table saying which directory holds it and whether its writes are
reaching a backup, and buttons for the policy, the floor, draining a disk,
moving one table, and running a rebalance — with the plan shown before it runs.
Against PostgreSQL the same page reports tablespaces, read-only, because
PostgreSQL has no placement policy to change.

See [docs/storage-pools.md](docs/storage-pools.md) for the on-disk format, what
a crash leaves behind at each step, and what a mirrored backup does and does not
guarantee.

### Key/value storage

On in the default feature set (`kv_storage`, and `kv_storage_s3` for the S3
backing store), and unused until a table is placed on it. A whole second
storage backend — chunked objects, a delta log, a page cache and a compactor —
so a build without the features, which only ever writes files, does not carry
it or the extra match arm on every storage call.

Placement is **per table**, so one database can hold file, in-memory and
object-store tables side by side. A table's spec in `db_storage.cfg` widens
from a directory path to a backend:

```sql
STORAGE PLACE TABLE events AT 'kv:archive';
STORAGE PLACE TABLE scratch AT 'memory:';
STORAGE PLACE TABLE orders  AT '/mnt/nvme1';
```

A non-file table gets **no file-based features**: no backup, no relocation, no
WAL, and the free-space policy skips it. Those are guards rather than
omissions — a snapshot logs what it excluded instead of quietly leaving it out.

See [docs/kv-storage.md](docs/kv-storage.md) for the object layout, the
compactor, and the two limits stated rather than left implicit: one writer per
store prefix, and no local disk cache tier.

### Redis-compatible key/value store

The `kv_cache` feature, in the default build and dormant until used. Any Redis
client connects to `--kv-port` unchanged: RESP2 and RESP3, pipelining,
`MULTI`/`EXEC`/`WATCH`, pub/sub, blocking list pops, 16 databases, and 157
commands across strings, hashes, lists, sets and sorted sets. `AUTH` checks the
database's own roles. The same store answers SQL on the PostgreSQL port:

```sql
KV SET session:42 '{"user":7}' EX 3600;
KV GET session:42;
KV ZRANGE board 0 9 REV WITHSCORES;   -- (member, score) rows
```

It runs every core where Redis runs one: 1024 lock-striped shards, each command
atomic on its key, and multi-key commands and `EXEC` atomic across theirs.
`maxmemory` and Redis's eviction policies bound it, and `SAVE` and
`--kv-persist` snapshot it. See [docs/kv-cache.md](docs/kv-cache.md) for the
command list, what is not implemented, the design, and measurements against
Redis and Valkey.

### S3 backup and point-in-time recovery

On in the default feature set (`s3_backup`). Name a bucket:

```sh
rasterizeddb_core --location /var/lib/rasterized \
  --s3-bucket my-backups --s3-region eu-central-1 --s3-prefix prod/orders
```

That takes a daily snapshot of the whole database (zstd, single object) and
archives the write-ahead log every five minutes, so recovery to any commit in
between is possible. Credentials come from the usual `AWS_ACCESS_KEY_ID` /
`AWS_SECRET_ACCESS_KEY` environment variables. `--s3-endpoint` points it at
MinIO, Cloudflare R2, Backblaze B2 or Wasabi instead of AWS.

From SQL, over either protocol:

```sql
BACKUP DATABASE;                  -- snapshot now
BACKUP WAL;                       -- point-in-time barrier
SHOW BACKUPS;                     -- what the catalogue holds
RESTORE DATABASE FROM S3 TO '/var/lib/rasterized-restored';
RESTORE DATABASE FROM S3 TO '/tmp/verify' AT LSN 918273;
RESTORE DATABASE FROM S3 TO '/tmp/verify' AT TIME '2026-08-20T10:00:00Z';
```

To replace a database after a major fault, restore before the server opens
anything:

```sh
rasterizeddb_core --location /var/lib/rasterized \
  --s3-bucket my-backups --s3-prefix prod/orders \
  --restore-from-s3 --restore-replace --restore-at-time '2026-08-20T09:55:00Z'
```

`--restore-replace` renames the existing directory aside rather than deleting
it. See [docs/s3-backup.md](docs/s3-backup.md) for the design, the full flag
list, and what it costs in API requests.

### Database branches

A branch is a database made as a consistent copy of another while it keeps
taking writes, which then diverges from it:

```sql
CREATE BRANCH dev FROM prod;                 -- or CREATE DATABASE dev TEMPLATE prod
CREATE BRANCH yesterday FROM prod AS OF TIMESTAMP '2026-11-18 09:00+00';  -- from the S3 backup
SHOW BRANCHES;
SHOW BRANCH DIFF dev;                        -- what a merge would do, per table
MERGE BRANCH dev;                            -- ON CONFLICT FAIL | BRANCH WINS | PARENT WINS, ALLOW DROP
RESET BRANCH dev;                            -- copy the parent again
PROMOTE BRANCH dev;                          -- swap it in under the parent's name
DROP BRANCH dev;
```

Clients connect to a branch by name. Files are cloned where the filesystem
shares blocks (btrfs, XFS) and copied in full elsewhere; each branch also keeps
a merge base, a copy of itself as made, unless created `WITHOUT BASE`. A merge
replays the schema changes the branch recorded, then compares rows three ways
and writes them to the parent in one transaction, a page at a time; any
conflict fails it whole by default. Rows the branch inserted under a `SERIAL`
key take new numbers from the parent's sequence, with their references. A
branch can be branched again (`CREATE BRANCH feature FROM dev`); dropping a
middle branch moves its branches to its parent. Tables on a key/value store
come over as the branch's own. rdb_studio has a BRANCHES tab, with the tree,
for all of it. See [docs/branching.md](docs/branching.md).

### Importing a PostgreSQL database

On in the default feature set (`pg_import`).

**Against a server that is already running** — the usual case, and the one that
needs no coordination — send it a statement over either protocol:

```sql
IMPORT DATABASE FROM POSTGRES 'postgresql://user:password@db.internal:5432/shop'
    INTO analytics;

IMPORT DATABASE FROM POSTGRES
    HOST 'db.internal' DATABASE 'shop'
    USER 'migrator' PASSWORD 'secret' SSLMODE 'verify-full'
    EXCLUDE TABLES (audit_log, sessions)
    INTO analytics;
```

That reads the schema, creates a database called `analytics`, and streams every
row of every table into it — about 87 000 rows a second, primary keys and unique
constraints included. Add `DRY RUN` to see what it would do without creating
anything.

**Or as a one-shot command**, on a host where nothing is serving that data
directory — from the tree:

```sh
cargo run --release -p rasterizeddb_core --features pg_import -- \
  --location /var/lib/rasterized \
  --pg-import-from 'postgresql://user:password@db.internal:5432/shop' \
  --pg-import-into analytics
```

or, on a machine `deploy.sh` has installed (where the binary is
`/usr/local/bin/rasterizeddb` and the service owns the data directory, so it has
to be stopped first):

```sh
sudo systemctl stop rasterizeddb
sudo -u "$(stat -c %U /var/lib/rasterizeddb)" rasterizeddb \
  --location /var/lib/rasterizeddb \
  --pg-import-from 'postgresql://user:password@db.internal:5432/shop' \
  --pg-import-into analytics
sudo systemctl start rasterizeddb
```

The process exits when the import is done, unless `--pg-import-and-serve` is
given. `deploy.sh` builds with `pg_import` by default (`RDB_FEATURES` overrides
it); a binary built without the feature has no `--pg-import-*` flags at all.

It carries over tables, columns, types, `NOT NULL`, constant defaults, primary
keys, unique constraints and single-column foreign keys. Everything that does
**not** come across unchanged — an enum that became `TEXT`, a sequence default
that could not be reproduced, a partial index — is named in the report at the
end rather than left for you to find. Values are validated against the storage
encoder before they are written, so a number this engine cannot hold exactly
fails the import instead of being quietly stored as zero.

See [docs/postgres-import.md](docs/postgres-import.md) for the type mapping, the
full grammar, and what it does not do.

### Guide on how to run the client-side

The native-protocol client SDK is [`rasterizeddb_client`](rasterizeddb_client/README.md) on crates.io: typed queries, transactions, an ORM, migrations and the key/value API. Any PostgreSQL, MySQL or SQL Server driver works against the matching listener too.

### Vision

#### PostgreSQL Dialect Compatibility
To make adoption easier, Rasterized DB aims for compatibility with the PostgreSQL SQL dialect, the most widely used and supported SQL standard in production environments. Not every PostgreSQL feature is implemented ([docs/postgres-compatibility.md](docs/postgres-compatibility.md) says which), but familiar syntax and tools work without learning a proprietary query language.

#### Performance Philosophy
Most work is avoided rather than done faster. The [filter index](#the-filter-index) rules out whole runs of rows before they are read, B-tree and hash [indexes](#indexes) answer point and range lookups, and an opt-in query cache (`--enable-cache`, `--cache-size`) remembers where a repeated query's rows are, so it does not scan again. For data that should not touch the disk at all, the [Redis-compatible key/value store](#redis-compatible-keyvalue-store) runs in the same process and answers both Redis clients and SQL.

#### Why Rust?
Rust combines zero-cost abstractions with low-level performance control, making it ideal for building a database that is both fast and safe. Its memory safety guarantees reduce the risk of crashes or data corruption, while still enabling optimizations close to the hardware.

#### Not built
These are not part of any release:

- Sharding across nodes. A design exists in [docs/sharding.md](docs/sharding.md); nothing of it is built.
- Compression of table files. Replication links and backups are compressed (zstd); the data files are not.
- Vector and tensor types. Arrays are supported.
- Immutable (append-only) tables.

#### Clients
[`rasterizeddb_client`](rasterizeddb_client/README.md) is the Rust SDK for the native protocol, with an ORM and migrations built in. There is no dedicated client for other languages: they connect with any PostgreSQL, MySQL or SQL Server driver.

#### Stability
Rasterized DB is at version 1.x. What is stable is the server: its command-line flags, its wire protocols and the SQL it answers, along with the [`rasterizeddb_client`](rasterizeddb_client/README.md) and [`rasterizeddb_embedded`](rasterizeddb_embedded/README.md) crates, which follow semver. The Rust API of `rasterizeddb_core` is the server's internals and may change in any release.

### Running the database inside a Rust application

[`rasterizeddb_embedded`](rasterizeddb_embedded/README.md) runs the whole engine in-process and hands you a `rasterizeddb_client::Client` for it:

```rust
let db = rasterizeddb_embedded::Builder::open("./data").start().await?;
let client = db.connect().await?;
client.execute("CREATE TABLE IF NOT EXISTS notes (id BIGINT, body TEXT)", &[]).await?;
db.shutdown().await?;
```

The server's flags are `Builder` methods there (memory, durability, threads, and `config(key, value)` for every `SHOW CONFIG` key); see its README, "Settings".

How releases are cut (crates and executables): [docs/releasing.md](docs/releasing.md).

##### Sponsor

[![Buy me a coffe](https://raw.githubusercontent.com/vasundhasauras/badge-bmc/1bf9f937862f918818d3528cce12256be0116570/badges/coffee/buy%20me%20a%20coffee/bm_coffee.svg "Buy me a coffe")](https://buymeacoffee.com/milen.denev)

### License
Everything in this directory is distributed under GNU GENERAL PUBLIC LICENSE version 3.
