# Importing a PostgreSQL database — design and operation

Status: **implemented, off by default.** Build with `--features pg_import`.
Target: `rasterizeddb_core::core::migrate`, edition 2024.

One direction, one shot. This reads a live PostgreSQL server and builds a new
RasterizedDB database from what it finds: the schema first, then every row. It
does not write back to PostgreSQL, it does not stay in sync, and it will not
import into a database that already exists.

## 0. What exists

| Component | State |
|---|---|
| `migrate::pg_client` — protocol v3, TLS, SCRAM/MD5/cleartext, `COPY` | **Done.** §3 |
| `migrate::source` — the catalogue queries | **Done.** §4 |
| `migrate::types` — type mapping and per-value validation | **Done.** §5 |
| `migrate::plan` — DDL, constraint deferral, the `COPY` projection | **Done.** §6 |
| `migrate::run` — create, load, put the keys back | **Done.** §7 |
| `IMPORT DATABASE FROM POSTGRES … INTO …` | **Done.** §2 |
| CLI flags, import-then-exit | **Done.** §2 |
| End-to-end tests against a real server | **Done.** §9 |

Not done: incremental or repeated sync, importing several schemas at once,
sequences, views, and `COPY FROM` in the other direction. §10.

## 1. Two commands, and which one to use

Everything below is reachable two ways, and they run the same code.

```sql
IMPORT DATABASE FROM POSTGRES 'postgresql://user:pass@host:5432/shop'
    INTO analytics;
```

```sh
cargo run --release -p rasterizeddb_core --features pg_import -- \
  --location /var/lib/rasterized \
  --pg-import-from 'postgresql://user:pass@host:5432/shop' \
  --pg-import-into analytics
```

The SQL form is answered by the pgwire session rather than by the executor,
alongside `CREATE DATABASE`, because it creates a database and the executor is
handed one. The CLI form runs after the catalogue opens and before any listener
starts, and the process exits when it is done unless `--pg-import-and-serve`
is given.

**Use the SQL form against a server that is running.** There is no lock on the
data directory: a second process pointed at a directory a live server is already
serving will open it, memory-map the same files and write to them, and neither
process will notice. The SQL form runs inside the server that owns the
directory, which is why it exists.

The CLI form is for a host where nothing is serving that directory yet — a fresh
machine, or a deployed one with the service stopped.

### On a deployed host

`deploy.sh` installs the binary as `/usr/local/bin/rasterizeddb` — not
`rasterizeddb_core`, which is only the cargo target name — and runs it from a
systemd unit that owns `/var/lib/rasterizeddb`. So:

```sh
sudo systemctl stop rasterizeddb
sudo -u "$(stat -c %U /var/lib/rasterizeddb)" rasterizeddb \
  --location /var/lib/rasterizeddb \
  --pg-import-from 'postgresql://user:pass@host:5432/shop' \
  --pg-import-into analytics
sudo systemctl start rasterizeddb
```

Running it as the user that owns the data directory matters: root would create
the new database's files root-owned, and the service — which runs as the login
user — would then fail to open them.

`deploy.sh` builds with `--features pg_import` by default, overridable with
`RDB_FEATURES`. A binary built without it has no `--pg-import-*` flags and
answers `IMPORT DATABASE` with a syntax error — which is not a start-up failure
but a surprise at the moment someone needs it.

## 2. The full grammar

```text
IMPORT [DATABASE] FROM POSTGRES[QL] '<connection uri>'
    [SCHEMA '<schema>']
    [ONLY TABLES (a, b)] [EXCLUDE TABLES (c)]
    [BATCH <rows>] [JSON AS TEXT|NATIVE] [WIDE NUMERIC AS FAIL|TEXT]
    [WITHOUT CONSTRAINTS] [DRY RUN]
    INTO <new database>
```

or, spelled out instead of as a URI:

```text
IMPORT DATABASE FROM POSTGRES
    HOST '<host>' [PORT <n>] DATABASE '<name>'
    USER '<user>' PASSWORD '<password>' [SSLMODE '<mode>']
    …
    INTO <new database>
```

Every clause has a flag: `--pg-import-schema`, `--pg-import-only-tables`,
`--pg-import-exclude-tables`, `--pg-import-batch-rows`, `--pg-import-json`,
`--pg-import-wide-numeric`, `--pg-import-without-constraints`,
`--pg-import-dry-run`. The connection can also come from the libpq environment
variables — `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, `PGDATABASE`,
`PGSSLMODE` — and `PGPASSWORD` is read even when a URI is given, since a
password is the field most often kept out of a written-down DSN.

`DRY RUN` connects, reads the catalogue, plans the whole import and reports what
it *would* do, creating nothing. It is the same code path as a real import minus
the execution, so what it reports is what will happen.

## 3. The client

`migrate::pg_client` is a hand-rolled PostgreSQL frontend, for the same reason
`backup::s3` is a hand-rolled S3 client: `tokio-postgres` would bring a
dependency tree an order of magnitude larger than the code it replaces, and
everything hard about it already exists in this tree. The SCRAM exchange is
`auth::ScramClient` — the very code the pgwire *listener* is the other side of —
the transport is the `rustls`/`tokio-rustls` that listener needs anyway, and MD5
comes from the `md-5` this crate already carries for old clients. The feature
adds no crate of its own beyond `webpki-roots`, which `s3_backup` also pulls.

What it speaks: the start-up packet, `SSLRequest`, cleartext / MD5 /
SCRAM-SHA-256 authentication, the simple query protocol, and `COPY … TO STDOUT`.
What it does not: the extended query protocol, prepared statements, pipelining,
`COPY FROM`, notifications, cancellation. A migration asks a handful of
catalogue questions and then streams tables, and every one of those is a simple
query.

`sslmode` follows libpq exactly, including the part people misread: `require`
encrypts but does **not** check who the server is, so anyone able to intercept
the connection can present their own certificate and read the password and every
row. `verify-full` checks the chain and the host name. `verify-ca` is accepted
as `verify-full`, because this client cannot check a chain without also checking
the name and quietly doing less than was asked for is the wrong direction to
round in.

SCRAM-SHA-256-PLUS is deliberately not offered. Channel binding would tie the
exchange to the TLS connection, but that is only worth anything under
`verify-full` — and a server offering PLUS over an unverified connection is
exactly the case where the binding value came from the attacker.

### The settings the connection pins

Everything downstream converts *text*, so the settings that decide how
PostgreSQL renders a value are pinned in the start-up packet rather than left at
the server's defaults:

| Setting | Value | Why |
|---|---|---|
| `client_encoding` | `UTF8` | Values are stored as UTF-8. |
| `DateStyle` | `ISO, YMD` | A server set to `SQL, DMY` renders `21/08/2026`, which reads as an impossible month — or a real one. |
| `TimeZone` | `UTC` | No zone arithmetic on this side, so none can be got wrong. |
| `extra_float_digits` | `3` | Shortest-round-trip floats. Servers before 12 truncate to six significant digits and lose the low bits of every double. |
| `bytea_output` | `hex` | Set after connecting, since it is not a start-up parameter. Under `escape` a `bytea` arrives as octal escapes. |

## 4. Reading the source

Four queries answer for a whole database — tables, columns, constraints, unique
indexes — rather than one per table, so a schema with a few hundred tables does
not spend the first part of a migration on round trips.

`information_schema` is not used. It is defined in terms of the standard, so it
hides everything PostgreSQL added: array element types, domains' base types,
`typcategory`, identity columns and partitioning are all either absent or
reshaped. `pg_catalog` says what is actually there.

Two details that are easy to get wrong and are not:

- **Partitioned tables.** `relkind` `p` is included and `relispartition` is
  excluded, so a partitioned table is read once through its parent — not once
  per partition *and* again through the parent, which would double every row.
- **Domains.** A recursive term walks a domain down to whatever it is
  ultimately built on, however many domains deep. Without it a column of a
  domain over `numeric` would fall through to the unrecognised-type case and
  become `TEXT`: the values would survive, but a number would have stopped being
  a number.

Views, materialised views, sequences and foreign tables are counted and
reported, not imported.

## 5. Types, and the reason every value is validated

| PostgreSQL | Here | Note |
|---|---|---|
| `bool` | `BOOLEAN` | |
| `int2` / `int4` / `int8` | `SMALLINT` / `INTEGER` / `BIGINT` | |
| `oid`, `xid`, `cid` | `BIGINT` | unsigned 32-bit needs the wider column |
| `float4` / `float8` | `REAL` / `DOUBLE PRECISION` | `NaN` and the infinities survive |
| `numeric` | `NUMERIC` | see below |
| `money` | `NUMERIC` | selected as `::numeric`, since the text form depends on `lc_monetary` |
| `text`, `varchar`, `bpchar`, `name`, `citext`, `xml` | `TEXT` | |
| `"char"` | `TEXT` | not this engine's `CHAR`, which is one Unicode scalar in four bytes |
| `uuid` | `UUID` | |
| `inet`, `cidr` | `INET` | same address and netmask |
| `bytea` | `BYTEA` | |
| `timestamp`, `timestamptz` | `TIMESTAMP` | microseconds since the epoch, UTC |
| `date` | `TIMESTAMP` | at midnight; there is no date-only type |
| any array | `TEXT[]` | see below |
| `json`, `jsonb` | `TEXT`, or `JSONB` under `JSON AS NATIVE` | |
| enums, composites, ranges, `interval`, `time`, `macaddr`, `bit`, `tsvector`, geometry, anything unrecognised | `TEXT` | holding PostgreSQL's own rendering, which is exact and reversible |

Nothing is dropped and nothing is guessed. A type with no counterpart becomes
`TEXT` carrying PostgreSQL's rendering rather than being skipped, and every such
decision is named in the report.

**Every array becomes `TEXT[]`.** This engine stores an array's elements as text
and tags the value `text[]` whatever the column was declared as — a
`DOUBLE PRECISION[]` column refuses its own values — so declaring the element
type would be declaring something untrue.

**`numeric` is the one type that can refuse a value.** This engine's `DECIMAL`
is an `i128` scaled by nine decimal places. A column declared finer than that —
`numeric(30,18)`, which is what a token balance looks like — is refused when the
import is *planned*, naming the column, rather than a million rows into the
copy; `WIDE NUMERIC AS TEXT` imports it as `TEXT` instead, which keeps every
digit but stops it being a number. An unconstrained `numeric` says nothing about
its values, so those are checked one at a time as they arrive.

### Why the values are validated at all

This is the load-bearing decision of the whole feature.

The insert path *coerces* rather than refuses. Measured against the current
engine, each of these is stored as a zero and the statement reports success:

| Written | Stored |
|---|---|
| `numeric '0.1234567891'` | `0` |
| `numeric 'NaN'` | `0` |
| `numeric '1e40'` | `0` |
| `uuid 'not-a-uuid'` | the nil UUID |
| `inet 'garbage'` | all zeroes |
| `timestamp 'infinity'` | the epoch |
| `timestamp '0001-01-01 BC'` | the epoch |

An import that turned a balance of `0.1234567891` into `0` and reported
`INSERT 1` would be the worst thing this feature could do. So every value is put
through `value_codec::encode_literal` — the same encoder the storage layer uses
— *before* a statement is built from it, and anything that will not encode fails
the import naming the table, the row and the column. The check is in
`types::literal`, and `types::tests::values_the_engine_would_silently_zero_are_refused`
is the test that keeps it there.

## 6. Why the constraints go on last

A `UNIQUE` constraint is checked here by scanning the table, so inserting into a
table that already has one costs time proportional to the rows already in it.
Measured on this engine, release build:

| Rows already present | Cost per insert |
|---|---|
| 10 000 | 24 µs |
| 20 000 | 37 µs |
| 30 000 | 50 µs |
| 60 000 | 88 µs |

It climbs without limit — loading a million-row table that way does not finish.
Without the constraint the same inserts cost a flat 6.3 µs whatever the table
already holds.

So a table is created **with** its keys, they are dropped before the data goes
in, and they are added back afterwards, which validates the whole table in one
pass: 51 ms for the primary key and 64 ms for a unique constraint on those same
60 000 rows. Foreign keys are added after every table holds its rows, so import
order does not matter and a cycle of references is not a problem.

Creating the table with the keys and dropping them — rather than simply adding
them at the end — is what keeps the schema's record of *which* column is the
primary key. That is written when the table is created, and there is no
statement that sets it later.

The result, on a 200 000-row table with a primary key and a unique constraint,
end to end including putting both keys back: **2.3 s**, about 87 000 rows a
second.

## 7. What the import does, in order

1. Connect and pin the rendering settings (§3).
2. Read the catalogue and plan (§4, §5, §6). Both are pure reads; nothing
   exists on this side yet, so a schema this engine cannot hold costs nothing to
   discover.
3. Create the target database and every table, keys included, then drop the
   keys.
4. Stream each table in with `COPY`, a batch of rows to a transaction
   (`BATCH`, default 5 000). A batch bounds what one open transaction holds;
   it is not a unit of durability — see below.
5. Put the keys back, then add the foreign keys.
6. Make it all durable, once.

**A failure at any point after step 3 drops the target database.** A database
that holds some of a table is indistinguishable, once the process has exited,
from one that holds all of it; the only safe thing to leave behind is nothing.
The error says whether the removal succeeded.

**Nothing between steps 3 and 6 is made durable, and that is deliberate.** An
ordinary write costs its bytes, a write-ahead log record, a commit marker, and
a device flush at commit; a row with a JSON column used to cost that several
times over, once per nested object. None of it buys anything for a database
that nobody has been handed yet and that is dropped whole if the process dies.
So the target directory is put into *bulk-load* mode for the duration: an
append is one `write` and nothing else, a batch's commit is an in-memory
outcome, and the shadow rows a JSON document decomposes into join the batch's
transaction rather than committing on their own. Step 6 then syncs every file
the target holds, flushes every transaction outcome, and only then reports the
import finished. Every other database on the server keeps its guarantees
throughout; the mode is scoped to the target's directory.

An import is a point-in-time copy *per table*. A read-only repeatable-read
transaction covering the whole import was considered and rejected: it would hold
one snapshot for hours on a busy source, pinning dead tuples. A source still
taking writes should be quiesced by whoever is migrating it.

## 8. What does not come across, and how you are told

The report at the end names every difference. Nothing is silent. The classes:

- **Sequences.** `nextval(...)` defaults are dropped — there are no sequences
  here — but the *values* come over, since they are ordinary data. A column that
  was `bigserial` will not allocate new ids.
- **Defaults that call into PostgreSQL.** Constants come across; `now()` and
  `CURRENT_TIMESTAMP` become `CURRENT_TIMESTAMP`; an expression over other
  columns is reported and dropped. Data is unaffected either way — an import
  supplies every value explicitly.
- **Partial and expression unique indexes.** They constrain something other than
  "these columns are unique together", which is the only shape this engine has,
  so they are reported rather than approximated into a different constraint.
- **Composite foreign keys.** This engine's foreign keys are single-column, and
  several single-column keys forbid different rows from what one composite key
  forbids.
- **Foreign keys leaving the schema**, and keys spanning more than eight
  columns.
- **`NOT NULL`** is carried onto the columns, but this engine does not yet
  refuse a null on `INSERT` or `UPDATE`. Every imported row honours it because
  the source did; writes made afterwards are not checked.
- **`NUMERIC` display.** The value is exact; the declared precision and scale
  are not kept, so `10.5000` reads back as `10.5`.
- **`TIMESTAMP` display.** Reported to clients as `timestamptz` in UTC, so a
  `timestamp without time zone` reads back with a `+00` on the end. Same
  instant.
- **Views, materialised views, sequences and foreign tables**, counted and named.

Two things stop an import rather than being reported, because carrying on would
mean silently storing something else:

- A table or column name that needs quoting, or two columns differing only in
  case — this engine compares names case-insensitively, so `Name` and `name`
  would become one column with whichever value was written last.
- A `numeric` declared finer than nine decimal places, unless
  `WIDE NUMERIC AS TEXT` was asked for.

Both are reported together, all of them at once, so a rename is done in one pass
rather than three.

## 9. Testing

Unit tests cover the parser, the type mapping, the value conversion, the `COPY`
text decoder and the planner, and run with an ordinary `cargo test`.

The end-to-end tests need a real server and are skipped without
`RDB_PG_IMPORT_TEST_DSN`:

```sh
docker run -d --rm --name rdb-migrate-pg \
    -e POSTGRES_PASSWORD=migr8pw -e POSTGRES_USER=migruser -e POSTGRES_DB=sourcedb \
    -p 55444:5432 postgres:17

RDB_PG_IMPORT_TEST_DSN='postgresql://migruser:migr8pw@127.0.0.1:55444/sourcedb' \
    cargo test -p rasterizeddb_core --features pg_import --test pg_import_end_to_end
```

They work in a schema of their own (`rdb_import_test`) and drop it first, so
they cannot disturb anything else in the database they are pointed at. Every
assertion is on a **value**, never on the absence of an error — the
characteristic failure in this engine is a statement that reports success and
stores something else, and a test that only checks for errors is blind to all of
it.

## 10. Not done

- Repeating an import, or keeping the two in sync. This is a one-shot copy.
- More than one schema per import.
- Sequences, views and materialised views.
- `NOT NULL` enforcement on the target, which is an engine gap rather than an
  import one (§8).
- A lock on the data directory, so that a CLI import into a directory a live
  server is serving is refused rather than merely documented (§1). Also an
  engine gap: nothing anywhere takes one.
- Exporting the other way.
