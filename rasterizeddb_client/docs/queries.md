# Queries

## The calls

| call | parameters | returns |
|---|---|---|
| [`run`](crate::Client::run) | none (text as given) | [`Rows`](crate::Rows) |
| [`query`](crate::Client::query) / [`execute`](crate::Client::execute) | `&[Param]` | `Rows` / rows affected |
| [`query_with`](crate::Client::query_with) / [`exec`](crate::Client::exec) | `&[&dyn ToSql]` ([`params!`](crate::params)) | `Rows` / rows affected |
| [`fetch_all`](crate::Client::fetch_all) | `params!` | `Vec<T>` |
| [`fetch_one`](crate::Client::fetch_one) | `params!` | `T`, exactly one row |
| [`fetch_optional`](crate::Client::fetch_optional) | `params!` | `Option<T>`, at most one row |
| [`fetch_scalar`](crate::Client::fetch_scalar) | `params!` | the first column of exactly one row |

`T` is anything [`FromRow`](crate::FromRow): a tuple of up to twelve
[`FromSql`](crate::FromSql) types, a `Vec<Value>`, or a struct with
`#[derive(FromRow)]` (or `#[derive(Model)]`). Columns are matched by
position, and a row whose width differs from the type's is an error, so a
select list that drifted from the code is found at once.

```rust,no_run
use rasterizeddb_client::prelude::*;

#[derive(FromRow, Debug)]
struct Order {
    id: i64,
    customer: String,
    total: Numeric,
    shipped: Option<Timestamp>,
}

# async fn demo(client: &Client) -> rasterizeddb_client::Result<()> {
let open: Vec<Order> = client
    .fetch_all(
        "SELECT id, customer, total::text, shipped FROM orders WHERE shipped IS NULL AND total > $1",
        params![Numeric::new("100")?],
    )
    .await?;
let count: i64 = client.fetch_scalar("SELECT COUNT(*) FROM orders", params![]).await?;
let newest: Option<(i64, Timestamp)> = client
    .fetch_optional("SELECT id, shipped FROM orders ORDER BY shipped DESC LIMIT 1", params![])
    .await?;
# let _ = (open, count, newest); Ok(()) }
```

## Rows by hand

```rust,no_run
# use rasterizeddb_client::prelude::*;
# async fn demo(client: &Client) -> rasterizeddb_client::Result<()> {
let rows = client.query_with("SELECT id, name, score FROM players", params![]).await?;
for row in &rows {
    let id: i64 = row.get(0)?;
    let name: &str = row.get(1)?;          // borrows from `rows`
    let score: Option<f64> = row.get(2)?;  // NULL is None
    let all: Vec<Value> = row.values()?;   // whatever the types
    # let _ = (id, name, score, all);
}
# Ok(()) }
```

## Writing many rows, atomically

Each statement is atomic. Writes that belong together can go in one
statement, which needs no transaction (several statements that must commit
together go in a [transaction](crate::guide::transactions)):

```rust,no_run
# use rasterizeddb_client::prelude::*;
# async fn demo(client: &Client) -> rasterizeddb_client::Result<()> {
// One statement: all three rows or none.
client
    .exec(
        "INSERT INTO ledger (account, amount) VALUES ($1, $2), ($3, $4), ($5, $6)",
        params![1i64, -50i64, 2i64, 30i64, 3i64, 20i64],
    )
    .await?;

// An upsert, and what was stored.
let (id, hits): (i64, i64) = client
    .fetch_one(
        "INSERT INTO pages (path, hits) VALUES ($1, 1) \
         ON CONFLICT (path) DO UPDATE SET hits = pages.hits + 1 RETURNING id, hits",
        params!["/index"],
    )
    .await?;
# let _ = (id, hits); Ok(()) }
```

## Errors

| [`Error`](crate::Error) | meaning |
|---|---|
| `Transport` | the connection failed; the statement may or may not have run |
| `TransactionLost` | a transaction's connection failed or idled out; it was rolled back, unless the failure came during COMMIT |
| `Server` | the server refused the statement; its message |
| `Unsupported` | refused before sending: a second statement, transaction control, an unsafe ORM call |
| `Param` | a value that cannot be written (text with a NUL, a bad number) |
| `Column` | a cell that cannot be read as the type asked for |
| `RowCount` | `fetch_one`/`fetch_optional` got another number of rows |
| `Protocol` | an answer this client cannot read |

[`is_unique_violation`](crate::Error::is_unique_violation) and
[`is_no_rows`](crate::Error::is_no_rows) answer the two questions code most
often asks of an error.
