# The ORM

`#[derive(Model)]` maps a struct to a table. What it generates:

* [`Model`](crate::orm::Model): the table's name and columns, and how to
  write each field as a literal;
* [`FromRow`](crate::FromRow), reading the model's select list;
* one associated constant per column, named as the field
  (`Account::email`), typed [`Column<Account, String>`](crate::orm::Column),
  for the query builder.

```rust,no_run
use rasterizeddb_client::prelude::*;

#[derive(SqlEnum, Debug, Clone, Copy, PartialEq)]
enum Plan { Free, Pro }

#[derive(Model, Debug, Clone)]
#[rdb(table = "accounts")]
struct Account {
    #[rdb(primary_key, serial)]
    id: i64,
    #[rdb(unique)]
    email: String,
    #[rdb(index)]
    name: Option<String>,
    plan: Plan,
    balance: Numeric,
    tags: Vec<String>,
    #[rdb(default = "0")]
    logins: i32,
    created_at: Timestamp,
}
# fn main() {}
```

## Attributes

| on a field | meaning |
|---|---|
| `primary_key` | part of the primary key; several fields make a composite key |
| `serial` | assigned by the server's sequence: left out of inserts, read back |
| `unique` | a `UNIQUE` constraint |
| `index`, `index = "hash"` | a secondary index: `btree`, `hash`, `gin`, `jsonb_path_ops` |
| `column = "name"` | the column's name, when it is not the field's |
| `sql_type = "VARCHAR(64)"` | the column's DDL type, when the field type's default is not wanted |
| `default = "…"` | a `DEFAULT` expression for the DDL (also used when the migrator adds the column) |
| `renamed_from = "old"` | an earlier name, for the migrator to rename (may repeat) |
| `json` | any `Serialize + Deserialize` type, stored as `jsonb` (feature `serde_json`) |
| `text` | read as `col::text` and parsed, for a type of your own |
| `skip` | not a column; `Default::default()` on reads |
| `version` | an integer for optimistic locking (below) |
| `created_at`, `updated_at` | timestamps the server sets (below) |
| `soft_delete` | an `Option` timestamp: `delete` marks the row instead of removing it (below) |
| `references = "users(id)"` | a foreign key, with `on_delete` / `on_update` ([relations](crate::guide::relations#foreign-keys)) |
| `has_many(…)`, `has_one(…)`, `belongs_to(…)`, `many_to_many(…)`, `count_of = "…"` | a relation field, not a column ([relations](crate::guide::relations)) |

| on the struct | meaning |
|---|---|
| `table = "name"` | the table (otherwise the struct's name in snake case) |
| `renamed_from = "old"` | an earlier table name, for the migrator to rename |
| `hooks` | run the struct's [`ModelHooks`](crate::orm::ModelHooks) (below) |
| `partition_by = "tenant_id, group_id"`, `partitions = 16` | a hash-partitioned table (below) |

A field's type gives its column type through [`SqlType`](crate::orm::SqlType)
(`i64` is `BIGINT`, `Option<T>` is nullable, `Vec<String>` is `TEXT[]`, …).

## Create, read, update, delete

[`ModelExt`](crate::orm::ModelExt) gives every
[`Executor`](crate::Executor) the calls: a [`Client`](crate::Client), a
reference to one, or a transaction. Each is one statement. Code written
against `E: Executor` runs on its own or inside a transaction unchanged, and
every future it returns is `Send`.

```rust,no_run
# use rasterizeddb_client::prelude::*;
# #[derive(Model, Debug, Clone)] #[rdb(table = "accounts")]
# struct Account { #[rdb(primary_key, serial)] id: i64, #[rdb(unique)] email: String, name: Option<String> }
# async fn demo(client: &Client) -> rasterizeddb_client::Result<()> {
client.create_table::<Account>().await?;            // IF NOT EXISTS, keys, indexes

let ada = client
    .insert(&Account { id: 0, email: "ada@example.com".into(), name: None })
    .await?;                                        // the stored row: id assigned
let many = client.insert_many(&[ada.clone(), ada.clone()]).await; // chunked multi-row INSERTs
# let _ = many;

let found: Option<Account> = client.find(ada.id).await?;
let mut changed = ada.clone();
changed.name = Some("Ada".into());
let stored = client.update(&changed).await?;        // every non-key column, RETURNING
let upserted = client.upsert(&stored).await?;        // ON CONFLICT (primary key) DO UPDATE (below)
let existed = client.delete(&upserted).await?;
# let _ = (found, existed); Ok(()) }
```

## Rows as stored: `RETURNING`

Every write that returns a row gets it from the statement itself,
`… RETURNING <the model's columns>`, never by reading it again afterwards:
`insert`, `insert_many`, `update`, `upsert` (all its forms), `restore`,
the bulk `query().update()…run()` and
[`delete_returning`](crate::orm::Select::delete_returning). What comes back
is what the server stored, with serial keys, server timestamps and any
coercion of the column type applied, and no other write can land between
the write and the read. An upsert of a model that is all key, which has
nothing to update, writes a no-op `DO UPDATE SET k = EXCLUDED.k` so the
stored row still comes back from the same statement.

The ORM names its columns rather than `RETURNING *` (which the server also
takes) because a `NUMERIC`, `JSONB` or array column has to be cast to text
to arrive readable over the native protocol.

## Upserts

`upsert` and `insert_many` return builders that run when awaited, so
`client.upsert(&row).await` works as it is, and a clash can be described
first:

```rust,no_run
# use rasterizeddb_client::prelude::*;
# #[derive(Model, Debug, Clone)] #[rdb(table = "members")]
# struct Member { #[rdb(primary_key, serial)] id: i64, #[rdb(unique)] email: String, name: String, visits: i64 }
# async fn demo(db: &Client, m: Member, batch: Vec<Member>) -> rasterizeddb_client::Result<()> {
// By another unique key: the serial id is assigned by the server.
let stored = db.upsert(&m).on_conflict(Member::email).await?;

// Choose what a clash updates.
let renamed = db.upsert(&m).on_conflict(Member::email).do_update(Member::name).await?;
let most = db.upsert(&m).do_update_all_except((Member::visits,)).await?;

// Keep the stored row: None when the key was taken.
let fresh: Option<Member> = db.upsert(&m).on_conflict(Member::email).do_nothing().await?;

// Many rows, multi-row statements, the same choices.
let inserted = db.insert_many(&batch).on_conflict(Member::email).do_nothing().await?;
let merged = db
    .insert_many(&batch)
    .on_conflict(Member::email)
    .do_update((Member::name, Member::visits))
    .await?;
# let _ = (stored, renamed, most, fresh, inserted, merged); Ok(()) }
```

* The default target is the primary key, and then every column is written
  as given, a serial key included (the upsert names its row). With another
  target, serial columns are left to the server, as an insert leaves them.
* The default update writes every column but the key, the target, serial
  and `created_at` columns; `version` and `updated_at` move as on any
  update and cannot be named in `do_update`.
* The server absorbs a clash on the **arbiter** only: under
  `on_conflict(Member::id)` a row whose *email* is taken is still a
  unique-violation error, and the statement writes nothing.
* `insert_many(…).do_nothing()` with no target skips a clash on any unique
  key, and returns only the rows inserted.
* A multi-row upsert that updates a model with a `#[rdb(version)]` column is
  refused: one statement cannot check each row's own version. Upsert the
  rows one at a time, or use `update_many`.
* Every builder shows its statement(s) with `to_sql()`.

## Many rows in one statement: `update_many`

[`update_many`](crate::orm::ModelExt::update_many) writes every row given,
by primary key, as **one** `UPDATE`, so the change commits whole or not at
all:

```sql
UPDATE t SET name = CASE id WHEN 1 THEN 'a' WHEN 2 THEN 'b' END,
             version = version + 1, updated_at = now()
 WHERE id IN (1, 2)
   AND (SELECT COUNT(*) FROM t WHERE (id = 1 AND version = 3) OR (id = 2 AND version = 5)) = 2
 RETURNING …
```

The count guard is what makes it all or nothing: if any row is gone,
soft-deleted, or (with a `#[rdb(version)]` column) at another version than
the struct holds, the guard is false for every row, nothing is updated, and
the call fails with [`Error::Conflict`](crate::Error::Conflict) naming the
rows. Rows come back as stored, in the order given; each is validated
first.

A change past [`UPDATE_MANY_MAX_BYTES`](crate::orm::UPDATE_MANY_MAX_BYTES)
of SQL is refused, not split: two statements could leave the first applied
and the second not. Update fewer rows per call, or run several calls in a
transaction to make them one change.

```rust,no_run
# use rasterizeddb_client::prelude::*;
# #[derive(Model, Debug, Clone)] struct Doc { #[rdb(primary_key)] id: i64, body: String, #[rdb(version)] version: i64 }
# async fn demo(db: &Client) -> rasterizeddb_client::Result<()> {
let mut docs = Doc::query().filter(Doc::id.is_in([1i64, 2, 3])).all(db).await?;
for d in &mut docs {
    d.body = d.body.trim().to_string();
}
match db.update_many(&docs).await {
    Ok(stored) => { let _ = stored; }
    Err(e) if e.is_conflict() => { /* someone got there first: read again */ }
    Err(e) => return Err(e),
}
# Ok(()) }
```

## Optimistic locking

A `#[rdb(version)]` integer field makes every write check that nobody
changed the row since it was read:

| call | writes | matches |
|---|---|---|
| `insert`, `insert_many` | version `1` | |
| `update` | `version = version + 1` | `WHERE <key> AND version = <the struct's>` |
| `upsert` | `1` when new; `version + 1` when it exists | the existing row only at the struct's version |
| `delete` | | `WHERE <key> AND version = <the struct's>` |
| `query().update()…run()` | `version = version + 1` (unless a `set` names it) | the filter |

A write that matches no row because the row is there at another version
fails with [`Error::Conflict`](crate::Error::Conflict) and writes nothing:
read the row again, reapply the change, retry. To tell a conflict from a
row that is gone, the ORM reads the stored version with one more
statement: a gone row stays [`Error::RowCount`](crate::Error::RowCount)
for `update` and `Ok(false)` for `delete`, as without a version.

`delete` checks the version too, so a row someone changed after you read it
is not deleted on the strength of the old copy. Delete a row by key alone
with `query().filter(…).delete(…)`, which checks nothing.

```rust,no_run
# use rasterizeddb_client::prelude::*;
#[derive(Model, Debug, Clone)]
struct Doc {
    #[rdb(primary_key)]
    id: i64,
    body: String,
    #[rdb(version)]
    version: i64,
}

# async fn demo(client: &Client) -> rasterizeddb_client::Result<()> {
let mut doc: Doc = client.find(1i64).await?.expect("there");
loop {
    doc.body.push('!');
    match client.update(&doc).await {
        Ok(stored) => {
            doc = stored;
            break;
        }
        Err(e) if e.is_conflict() => doc = client.find(1i64).await?.expect("there"),
        Err(e) => return Err(e),
    }
}
# let _ = doc; Ok(()) }
```

## Timestamps and soft delete

`#[rdb(created_at)]` and `#[rdb(updated_at)]` fields are written by the
server: `now()` in the statement, so every client agrees on one clock and a
skewed one cannot write a time in the future. An insert or upsert sets both
(the struct's values are ignored); every update, the bulk
`query().update()` included, sets `updated_at` again and never touches
`created_at`. The row that comes back holds the server's times. (This server
evaluates `now()` per call, so the two can differ by microseconds within
one insert.)

`#[rdb(soft_delete)]` on an `Option<Timestamp>` field makes deleting a
mark:

| call | does |
|---|---|
| `delete(&row)` | `UPDATE … SET deleted_at = now()` (and the version and update time) of the row while it is not deleted; `false` when it already was |
| `query()…delete(db)` | the same, for the filtered rows |
| every query, `find`, relation load, count and aggregate | leaves out rows whose mark is set |
| `.with_deleted()`, `.only_deleted()` | on a query or a relation: all rows, or only the marked ones |
| `restore(&row)` | clears the mark, returns the row |
| `force_delete(&row)`, `query()…force_delete(db)` | a real `DELETE` |
| `update(&row)` | updates the row whether it is marked or not (you hold it) |
| `upsert(&row)` | writes the row as given, the mark included: upserting a struct whose mark is `None` restores the row |

```rust,no_run
# use rasterizeddb_client::prelude::*;
#[derive(Model, Debug, Clone)]
struct Note {
    #[rdb(primary_key, serial)]
    id: i64,
    body: String,
    #[rdb(created_at)]
    created_at: Timestamp,
    #[rdb(updated_at)]
    updated_at: Timestamp,
    #[rdb(soft_delete)]
    deleted_at: Option<Timestamp>,
}

# async fn demo(db: &Client, note: Note) -> rasterizeddb_client::Result<()> {
db.delete(&note).await?;                                  // marked
let live = Note::query().count(db).await?;                // leaves it out
let bin = Note::query().only_deleted().all(db).await?;    // finds it
let back = db.restore(&bin[0]).await?;
db.force_delete(&back).await?;                            // gone
# let _ = live; Ok(()) }
```

## Partitioned tables

`#[rdb(partition_by = "…")]` on the struct makes `create_table` (and a
migrator's model sync) create the table hash-partitioned on that key, one
to three `NOT NULL` columns that every primary and unique key includes:

```rust,no_run
# use rasterizeddb_client::prelude::*;
#[derive(Model, Debug, Clone)]
#[rdb(table = "assets", partition_by = "tenant_id, group_id", partitions = 64)]
struct Asset {
    #[rdb(primary_key)]
    tenant_id: i64,
    #[rdb(primary_key)]
    group_id: i64,
    #[rdb(primary_key)]
    id: Uuid,
    body: Option<String>,
}
// CREATE TABLE IF NOT EXISTS assets (…, PRIMARY KEY (tenant_id, group_id, id))
//   PARTITION BY HASH (tenant_id, group_id) PARTITIONS 64
# fn main() {}
```

Everything else works on it unchanged: the server routes each row by its
key, and a query fixing the key (or its leading column) reads one
partition. A [`Migrator`](crate::orm::Migrator) reports a model partitioned
over a plain table as drift, and converts it only under
[`SyncPolicy::convert_partitioning`](crate::orm::SyncPolicy::convert_partitioning)
(an offline `ALTER TABLE … PARTITION BY HASH`); see the
[migrations chapter](crate::guide::migrations). Reshaping partitions
(`SPLIT`, `MERGE`, `ISOLATE KEY`, moves) is the administration API's.

## Hooks and validation

`#[rdb(hooks)]` on the struct makes the ORM call its
[`ModelHooks`](crate::orm::ModelHooks) implementation, whose methods all
default to doing nothing:

```rust,no_run
use rasterizeddb_client::prelude::*;
use rasterizeddb_client::orm::ModelHooks;

#[derive(Model, Debug, Clone)]
#[rdb(hooks)]
struct Person {
    #[rdb(primary_key, serial)]
    id: i64,
    email: String,
    frozen: bool,
    #[rdb(skip)]
    domain: String,
}

impl ModelHooks for Person {
    fn validate(&self) -> std::result::Result<(), String> {
        if self.email.contains('@') { Ok(()) } else { Err(format!("{:?} is not an email", self.email)) }
    }
    fn before_update(&self) -> rasterizeddb_client::Result<()> {
        if self.frozen {
            return Err(Error::Unsupported(format!("person {} is frozen", self.id)));
        }
        Ok(())
    }
    fn after_load(&mut self) {
        self.domain = self.email.split('@').nth(1).unwrap_or("").to_string();
    }
}
# fn main() {}
```

`validate` runs before every insert, upsert and update of a row (and each
row of `insert_many` and `update_many`); refusing is
[`Error::Invalid`](crate::Error::Invalid), and **nothing is sent**, not even
the valid rows of a batch. `before_insert`, `before_update` (an upsert runs
both) and `before_delete` can veto a write with any error. `after_load` runs
on every row the ORM reads into the model, the rows writes return
included. The hooks are synchronous and run on this side; the bulk
`query().update()` and `query().delete()` take no rows and run none.

## Queries

```rust,no_run
# use rasterizeddb_client::prelude::*;
# #[derive(Model, Debug, Clone)] #[rdb(table = "accounts")]
# struct Account { #[rdb(primary_key, serial)] id: i64, email: String, name: Option<String>, visits: i64, balance: Numeric }
# async fn demo(client: &Client) -> rasterizeddb_client::Result<()> {
let page = Account::query()
    .filter(Account::balance.gt(Numeric::new("10")?) & Account::name.is_not_null())
    .filter(Account::email.like("%@example.com") | Account::id.is_in([1i64, 2, 3]))
    .order_by(Account::visits.desc().nulls_last())
    .limit(20)
    .offset(40)
    .all(client)
    .await?;

let n = Account::query().filter(Account::visits.ge(100)).count(client).await?;
let one = Account::query().filter(Account::email.eq("ada@example.com")).one(client).await?;

// What the builder does not say, with parameters still escaped:
let raw = Account::query()
    .filter(Expr::raw("lower(email) = $1", params!["ada@example.com"]))
    .first(client)
    .await?;

// Bulk updates and deletes return what they touched; with no filter they
// refuse to run unless told every_row().
let bumped: Vec<Account> = Account::query()
    .filter(Account::name.is_null())
    .update()
    .set(Account::name, "anonymous")
    .set_raw(Account::visits, "visits + $1", params![1])
    .run(client)
    .await?;
let removed = Account::query().filter(Account::visits.eq(0)).delete(client).await?;
# let _ = (page, n, one, raw, bumped, removed); Ok(()) }
```

Every statement the builder makes can be seen first with `to_sql()`. The
terminal calls (`all`, `first`, `one`, `count`, `exists`, `delete`, `run`)
take any executor, as the `ModelExt` calls do:

```rust,no_run
# use rasterizeddb_client::prelude::*;
# #[derive(Model)] struct Account { #[rdb(primary_key, serial)] id: i64, visits: i64 }
async fn busiest<E: Executor>(db: &E) -> rasterizeddb_client::Result<Option<Account>> {
    Account::query().order_by(Account::visits.desc()).first(db).await
}
```

## Projections and aggregates

[`select`](crate::orm::Select::select) reads chosen columns and aggregates
instead of whole models: one item is read as its value, a tuple as a tuple.
[`all_as`](crate::orm::Projected::all_as) reads each row into a
`#[derive(FromRow)]` struct instead, by position.

```rust,no_run
# use rasterizeddb_client::prelude::*;
# #[derive(Model)] #[rdb(table = "sales")]
# struct Sale { #[rdb(primary_key)] id: i64, region: String, units: i32, amount: i64, price: Numeric }
#[derive(FromRow)]
struct RegionTotal { region: String, sales: i64, amount: Option<i64> }

# async fn demo(db: &Client) -> rasterizeddb_client::Result<()> {
let emails: Vec<String> = Sale::query().select(Sale::region).distinct().all(db).await?;

let (n, total, mean): (i64, Option<i64>, Option<Numeric>) = Sale::query()
    .select((count_rows(), Sale::amount.sum(), Sale::units.avg()))
    .one(db)
    .await?;

let by_region: Vec<RegionTotal> = Sale::query()
    .select((Sale::region, count_rows(), Sale::amount.sum()))
    .group_by(Sale::region)
    .having(count_rows::<Sale>().ge(10))
    .order_by(Sale::amount.sum().desc())
    .all_as(db)
    .await?;
# let _ = (emails, n, total, mean, by_region); Ok(()) }
```

| aggregate | reads as | over no rows |
|---|---|---|
| [`count_rows()`](crate::orm::count_rows), `col.count()`, `col.count_distinct()` | `i64` | 0 |
| `col.sum()` over an integer column | `Option<i64>`, as `CAST(SUM(col) AS BIGINT)` | `None` |
| `col.sum()` over `f32`/`f64`; over `Numeric` | `Option<f64>`; `Option<Numeric>` (as text) | `None` |
| `col.avg()` over an integer or `Numeric` column; over a float | `Option<Numeric>` (as text, exact); `Option<f64>` | `None` |
| `col.min()`, `col.max()` | `Option<T>` (a `Numeric` as text) | `None` |

**Why the cast.** `SUM` over a `BIGINT` is a `NUMERIC`, and the native
protocol sends a `NUMERIC` of that kind without its scale, which this client
refuses to guess. The cast goes *outside* the sum, so the server adds exact
integers and only the total is converted; a total past `i64` is the
server's error, never a wrapped number.

## Keyset pages and streams

`OFFSET` pages skip rows the server still reads, and shift when rows are
added before them. A keyset page starts *after a row's sort key* instead:
[`page`](crate::orm::Select::page) reads up to `limit` rows in the query's
order and returns a [`Page`](crate::orm::Page) with the
[`Cursor`](crate::orm::Cursor) of the next page and of the one before.

```rust,no_run
# use rasterizeddb_client::prelude::*;
use rasterizeddb_client::orm::Cursor;
# #[derive(Model)] #[rdb(table = "events")]
# struct Event { #[rdb(primary_key)] id: i64, kind: String, at: Timestamp }
# async fn demo(db: &Client, from_client: Option<&str>) -> rasterizeddb_client::Result<()> {
let q = Event::query()
    .filter(Event::kind.eq("login"))
    .order_by(Event::at.desc())
    .limit(50);
let page = match from_client {
    Some(text) => q.after(&Cursor::decode(text)?).page(db).await?,
    None => q.page(db).await?,
};
// Hand page.next_cursor (encoded) to the client for the next request.
let next: Option<String> = page.next_cursor.map(|c| c.encode()).transpose()?;
# let _ = (page.items, next); Ok(()) }
```

* The sort key is the `ORDER BY` columns, then the primary key's (in the
  last column's direction) to break ties, so every row has one place.
* When every key sorts the same way the condition is a row-value
  comparison, `(at, id) < ('…', 42)`; when directions differ it is the
  equivalent OR-chain, `kind > 'x' OR (kind = 'x' AND at < '…') OR …`.
* [`before`](crate::orm::Select::before) reads the page before a cursor
  (backwards, then put back in order).
* Keys must be `NOT NULL` columns: a NULL compares as unknown, and the rows
  holding one would never be reached. A nullable key is refused, as is an
  `offset` or an order by an expression.
* [`Cursor::encode`](crate::orm::Cursor::encode) is an opaque, URL-safe
  string of typed values; a decoded cursor is written into the next
  statement as escaped literals, so a client cannot smuggle SQL in one.

[`stream`](crate::orm::Select::stream) reads every matching row in keyset
batches, one statement each, so a table of any size is read without an
answer past the message limit. Pull rows with
[`next`](crate::orm::RowStream::next) or whole batches with
[`next_batch`](crate::orm::RowStream::next_batch); it is also a
`futures_core::Stream`.

```rust,no_run
# use rasterizeddb_client::prelude::*;
# #[derive(Model)] #[rdb(table = "events")]
# struct Event { #[rdb(primary_key)] id: i64, at: Timestamp }
# async fn demo(db: &Client) -> rasterizeddb_client::Result<()> {
let mut rows = Event::query().order_by(Event::at.asc()).stream(db, 1000);
while let Some(event) = rows.next().await {
    let event = event?;
#   let _ = event;
}
# Ok(()) }
```

Each batch is its own statement: outside a transaction, a row written
while the stream runs is seen or not depending on where it sorts.

## The same struct in the cache

A model can also live in the key/value cache, as a hash per row:
[`client.kv().repo::<Account>()`](crate::kv::KvRepo); or the cache can sit
in front of its table, read-through, with
[`client.cached::<Account>(ttl)`](crate::orm::Cached). See the
[KV chapter](crate::guide::kv).
