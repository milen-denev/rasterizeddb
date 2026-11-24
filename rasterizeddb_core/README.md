# Rasterized DB

> **API stability.** What is stable is the server: the `rasterizeddb` binary,
> its command-line flags, its wire protocols (PostgreSQL, native, MySQL, SQL
> Server, RESP) and the SQL it answers. The Rust API of this crate,
> `rasterizeddb_core`, is the server's internals: it is published so the server
> and the embedded SDK can be built from crates.io, and **any of it may change
> in any release, a patch release included**. To run the database inside an
> application, use [`rasterizeddb_embedded`](https://crates.io/crates/rasterizeddb_embedded);
> to talk to a server, [`rasterizeddb_client`](https://crates.io/crates/rasterizeddb_client).
> Both follow semver.

RasterizedDB is a SQL database server written in Rust. It speaks the
PostgreSQL wire protocol, so `psql`, any PostgreSQL driver and any ORM connect
to it unmodified. It also speaks its own native protocol (rastcp), the MySQL /
MariaDB client protocol and the SQL Server (TDS) protocol, all on the same
databases and the same roles, and serves a Redis-compatible key/value store.

This crate is the engine and the server. The full documentation is in the
[repository](https://github.com/milen-denev/rasterizeddb).

## What is in it

| | |
|---|---|
| **Protocols** | PostgreSQL v3 (simple and extended query, SCRAM-SHA-256, TLS); native rastcp; MySQL / MariaDB; SQL Server (TDS 7.4 and 8.0); Redis RESP2/RESP3 |
| **Types** | 40 PostgreSQL types, including intervals, MAC and bit strings, the geometric types and the ranges, each with its real wire OID |
| **Transactions** | MVCC snapshots, `READ COMMITTED` / `REPEATABLE READ`, savepoints, first-updater-wins conflicts |
| **Durability** | Write-ahead log with checkpoints, and a `synchronous_commit` setting |
| **Indexes** | B-tree and hash indexes (`CREATE INDEX`, and one per primary key and unique constraint), GIN full-text indexes, JSON path indexes, plus a bit-sliced SIMD filter index that prunes every scan |
| **Space reclamation** | Background vacuum, `VACUUM` and `VACUUM FULL` |
| **JSON** | Documents stored as binary values, with path indexing |
| **Partitioning** | Hash-partitioned tables, with split, merge and per-partition placement |
| **Replication** | Multi-primary, HLC-ordered, over TLS 1.3 |
| **Backup** | Snapshots and point-in-time recovery to any S3-compatible store |
| **Branches** | A database copied live (or from a past point), then reset, promoted, diffed and merged back |
| **Storage pools** | One database across several directories, with a placement policy and mirrored backups |
| **Key/value storage** | A table kept in an object store instead of files, beside file and in-memory tables |
| **Key/value cache** | A Redis-compatible store, reachable from Redis clients and from SQL (`KV …`) |
| **PostgreSQL import** | One-shot migration of a live PostgreSQL database |

## Install and run

Prebuilt servers for Linux and Windows (x86-64 and ARM64) and an Arch Linux
package are on the
[releases page](https://github.com/milen-denev/rasterizeddb/releases). There,
and in the Docker images, the executable is named `rasterizeddb`.

**Docker** (`milendenev/rasterizeddb` or `ghcr.io/milen-denev/rasterizeddb`;
Ubuntu 24.04 by default, also `-ubuntu26.04` and `-alpine` tags):

```sh
docker run -d --name rasterizeddb -e RASTERIZEDDB_PG_PASSWORD=change-me \
  -p 5432:5432 -p 61170:61170 -v rasterizeddb:/var/lib/rasterizeddb \
  milendenev/rasterizeddb:latest
```

**From crates.io:**

```sh
RUSTFLAGS="-C target-cpu=x86-64-v3" cargo install rasterizeddb_core   # on x86-64
cargo install rasterizeddb_core                                       # on ARM64
```

This installs the binary as `rasterizeddb_core`. On x86-64 the server needs an
AVX2 CPU (x86-64-v3) and refuses to start on an older one.

**Running it:**

```sh
rasterizeddb_core --location /var/lib/rasterized --protocol both \
    --pg-addr 0.0.0.0 --pg-user superuser --pg-password <password>

psql "postgresql://superuser:<password>@127.0.0.1:5432/postgres?sslmode=require"
```

- `--location` is the data directory, and is required.
- `--protocol` is `rastcp` (the default), `pgwire`, or `both`. `both` serves
  PostgreSQL on `--pg-port` (5432) and the native protocol on `--rdb-port`
  (61170) from one process.
- `--mysql-port`, `--mssql-port` and `--kv-port` turn on the MySQL, SQL Server
  and Redis listeners. Each is off until it is given a port.
- Every listener binds `127.0.0.1` unless told otherwise (`--pg-addr`,
  `--rdb-addr`, ...).
- PostgreSQL connections need TLS by default (`--pg-tls require`). Without a
  certificate the server generates a self-signed one at start-up.
- `--pg-user`/`--pg-password` seed the first role of an empty database. After
  that, use `CREATE USER`.

`rasterizeddb_core --help` lists every flag.

The heavier subsystems are Cargo features, and the default build turns most of
them on. The
[feature table](https://github.com/milen-denev/rasterizeddb#build-features)
says what each one adds.

## Embedding

To run the database inside a Rust application, with no server process, use
[`rasterizeddb_embedded`](https://crates.io/crates/rasterizeddb_embedded). It
starts the engine in-process and hands back a `rasterizeddb_client::Client`
for it. Over the network, use
[`rasterizeddb_client`](https://crates.io/crates/rasterizeddb_client), the
native-protocol SDK and ORM, or any PostgreSQL, MySQL or SQL Server driver.

## Documentation

- [Repository README](https://github.com/milen-denev/rasterizeddb#readme):
  the SQL surface, transactions, durability and every subsystem in more depth
- [PostgreSQL compatibility](https://github.com/milen-denev/rasterizeddb/blob/main/docs/postgres-compatibility.md)
- [MySQL and SQL Server](https://github.com/milen-denev/rasterizeddb/blob/main/docs/mysql-and-sql-server.md)
- [Indexes](https://github.com/milen-denev/rasterizeddb/blob/main/docs/indexes.md),
  [persistent indexes](https://github.com/milen-denev/rasterizeddb/blob/main/docs/persistent-indexes.md)
- [Partitioning](https://github.com/milen-denev/rasterizeddb/blob/main/docs/partitioning.md)
- [Replication](https://github.com/milen-denev/rasterizeddb/blob/main/docs/replication.md)
- [S3 backup and point-in-time recovery](https://github.com/milen-denev/rasterizeddb/blob/main/docs/s3-backup.md)
- [Database branches](https://github.com/milen-denev/rasterizeddb/blob/main/docs/branching.md)
- [Storage pools](https://github.com/milen-denev/rasterizeddb/blob/main/docs/storage-pools.md),
  [key/value storage](https://github.com/milen-denev/rasterizeddb/blob/main/docs/kv-storage.md),
  [Redis-compatible cache](https://github.com/milen-denev/rasterizeddb/blob/main/docs/kv-cache.md)
- [PostgreSQL import](https://github.com/milen-denev/rasterizeddb/blob/main/docs/postgres-import.md)
- [Docker](https://github.com/milen-denev/rasterizeddb/blob/main/docs/docker.md)

## License

GNU General Public License version 3 (`GPL-3.0-only`).
