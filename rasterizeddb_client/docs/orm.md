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
| `references = "users(id)"` | a foreign key, with `on_delete` / `on_update` ([relations](crate::guide::relations#foreign-keys)) |
| `has_many(…)`, `has_one(…)`, `belongs_to(…)`, `many_to_many(…)`, `count_of = "…"` | a relation field, not a column ([relations](crate::guide::relations)) |

| on the struct | meaning |
|---|---|
| `table = "name"` | the table (otherwise the struct's name in snake case) |
| `renamed_from = "old"` | an earlier table name, for the migrator to rename |

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
let upserted = client.upsert(&stored).await?;        // ON CONFLICT (primary key) DO UPDATE
let existed = client.delete(&upserted).await?;
# let _ = (found, existed); Ok(()) }
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

## The same struct in the cache

A model can also live in the key/value cache, as a hash per row:
[`client.kv().repo::<Account>()`](crate::kv::KvRepo). See the
[KV chapter](crate::guide::kv).
