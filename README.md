
# Rasterized DB
## A high-performance database written from scratch in Rust — now fully rewritten.

### Guide on how to run

rasterizeddb_server-PLATFORM-x64.exe --location /your-location/bla-bla --concurrent_threads 16 --batch_size 16000

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

### Native protocol authentication

The native protocol — what `DbClient` speaks — reads the same role catalogue as
pgwire, so one `CREATE USER` works for both:

```sh
cargo run -p rasterizeddb_core -- --location <PATH> \
    --rdb-user superuser --rdb-password <password>
```

```rust
let client = DbClient::new_authenticated(Some("127.0.0.1"), "superuser", "<password>").await?;
```

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

*batch_size*: recommended value is 16K, but you can play around with more or less depending on your case. The lesser the number of rows, the lower the batch size must be.
*concurrent_threads*: must equal to the number of CPU threads of lower.

### rdb_studio — the desktop console

A pgAdmin-style console for RasterizedDB, written in [iced](https://iced.rs).
It also speaks to PostgreSQL, because both answer the same wire protocol.

```sh
cargo run -p rdb_studio                # pick a connection
cargo run -p rdb_studio -- my-profile  # open a saved one straight away
```

Live dashboard, database and table statistics, a schema map drawn from the
foreign keys, an editable data browser, a SQL editor, S3 backup and restore, a
storage view for placing tables across disks, a replication view, and a
configuration screen that knows which settings a running server can actually
change. See [rdb_studio/README.md](rdb_studio/README.md).

It reads the introspection statements the engine gained for it —
`SHOW TABLES`, `SHOW DATABASE STATS`, `SHOW SCHEMA MAP`, `SHOW CONFIG`,
`SET CONFIG` and the rest — which work from `psql` too:

```sql
SHOW DATABASE STATS;
SHOW TABLES;
SHOW TABLE STATS users;
SHOW COLUMNS FROM users;      -- or DESCRIBE users
SHOW SCHEMA MAP;
SHOW DATABASES;
SHOW CONFIG;                  -- with a hot_reload column per setting
SET CONFIG filter.max_f = 14; -- applied live
SHOW BACKUP STATUS;           -- configured? healthy? how far can a restore reach?
SHOW REPLICATION STATS;       -- node state and counters, as rows
SHOW REPLICATION PEER STATUS; -- a row per peer
SHOW STORAGE STATS;           -- the pool, as rows
SHOW STORAGE LOCATIONS;       -- a row per directory, with free space
SHOW STORAGE PLACEMENTS;      -- which directory holds each table
```

See [docs/introspection.md](docs/introspection.md) for what each figure means
and why the surface is statements rather than an `information_schema`.

### Multiple storage directories

Off by default. Build with `--features storage_pools`. A database that has only
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

SHOW STORAGE;                             -- a line per location, with free space
SHOW STORAGE STATS;                       -- pool-wide figures as columns
SHOW STORAGE LOCATIONS;                   -- a row per location
SHOW STORAGE PLACEMENTS;                  -- which directory each table is in
SHOW STORAGE POLICY;
SHOW STORAGE KEYS;
```

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

### S3 backup and point-in-time recovery

Off by default. Build with `--features s3_backup`, then name a bucket:

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

### Importing a PostgreSQL database

Off by default. Build with `--features pg_import`.

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

Refer to *test_client/src/main.rs* for examples! 

### Vision

#### Complete Rewrite
Rasterized DB has recently undergone a complete rewrite from the ground up and is still in an active state of redevelopment. This major overhaul has brought significant architectural changes to improve stability, performance, and developer experience. Many components are being redesigned for long-term maintainability, and the database will continue to evolve rapidly until its core design reaches maturity.

#### From Schemaless to Schema-Full
Originally conceived as a schemaless database, Rasterized DB is now schema-full. The database itself now manages and enforces the schema, reducing complexity for client applications and enabling stronger data consistency. This change also opens the door to richer query capabilities and deeper optimizations at the storage and execution layers.

#### PostgreSQL Dialect Compatibility
To make adoption easier, Rasterized DB aims for compatibility with the PostgreSQL SQL dialect, the most widely used and supported SQL standard in production environments. While not all PostgreSQL features are currently implemented, this compatibility goal ensures developers can leverage familiar syntax and tools without learning a proprietary query language from scratch.

#### Performance Philosophy
Rasterized DB takes inspiration from how the web handles caching. Just like a browser reuses files when their cache headers indicate they’re still valid, Rasterized DB hashes each query so repeated requests can instantly fetch results from a known storage offset without rescanning the dataset.

Future updates will introduce in-memory row caching, allowing certain queries to bypass disk entirely. This approach will merge the speed of in-memory stores like Redis with the persistence of a traditional database.

#### Why Rust?
Rust combines zero-cost abstractions with low-level performance control, making it ideal for building a database that is both fast and safe. Its memory safety guarantees reduce the risk of crashes or data corruption, while still enabling optimizations close to the hardware.

#### Scalability and Future Features

Rasterized DB is designed to handle virtually unlimited data sizes, row counts, and column counts. Planned features include:

- Advanced data types: arrays, vectors, tensors, and more [PARTIALLY DONE]
- Row insertion, updates, and deletions via SQL [DONE]
- Vacuuming unused space [DONE]
- UUID (GUID) support [DONE]
- Fully functional RETURN, LIMIT, and advanced SELECT capabilities [DONE]
- Server mode with network access [DONE]
- Sharding for horizontal scalability
- Compression for storage efficiency
- Table immutability options

#### ORM & Ecosystem
An official Rust ORM is in development, with a C# ORM planned afterward. These tools will provide a seamless experience when integrating Rasterized DB into applications.

#### Stability
Currently, Rasterized DB is not stable. Table formats, storage engines, and query processing internals are likely to change until version 1.0.0. Use it at your own risk in production environments.

### How to use the current API?

```rust
// COMING SOON
// Please refer to the main.rs and core/mock_table.rs and core/mock_helpers.rs to see API in use.
```

##### Sponsor

[![Buy me a coffe](https://raw.githubusercontent.com/vasundhasauras/badge-bmc/1bf9f937862f918818d3528cce12256be0116570/badges/coffee/buy%20me%20a%20coffee/bm_coffee.svg "Buy me a coffe")](https://buymeacoffee.com/milen.denev)

### License
Everything in this directory is distributed under GNU GENERAL PUBLIC LICENSE version 3.
