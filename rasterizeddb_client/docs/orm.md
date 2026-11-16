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

| on the struct | meaning |
|---|---|
| `table = "name"` | the table (otherwise the struct's name in snake case) |
| `renamed_from = "old"` | an earlier table name, for the migrator to rename |

A field's type gives its column type through [`SqlType`](crate::orm::SqlType)
(`i64` is `BIGINT`, `Option<T>` is nullable, `Vec<String>` is `TEXT[]`, …).

## Create, read, update, delete

[`ModelExt`](crate::orm::ModelExt) gives the client the calls; each is one
statement.

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

Every statement the builder makes can be seen first with `to_sql()`.

## The same struct in the cache

A model can also live in the key/value cache, as a hash per row:
[`client.kv().repo::<Account>()`](crate::kv::KvRepo). See the
[KV chapter](crate::guide::kv).
