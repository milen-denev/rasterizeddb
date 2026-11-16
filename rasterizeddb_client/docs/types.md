# Types

## Writing values

Anything [`ToSql`](crate::ToSql) fills a `$n` placeholder. The
[`params!`](crate::params) macro makes the list:

```rust,no_run
# use rasterizeddb_client::prelude::*;
# async fn demo(client: &Client) -> rasterizeddb_client::Result<()> {
let tags = vec!["a".to_string(), "b".to_string()];
client
    .exec(
        "INSERT INTO posts (id, title, score, tags, published, created) VALUES ($1, $2, $3, $4, $5, $6)",
        params![Uuid::new_v4()?, "It's here", 4.5f64, tags, true, Timestamp::now()],
    )
    .await?;
# Ok(()) }
```

## The mapping

| Rust | written as | column types | read from tag |
|---|---|---|---|
| `bool` | `true` | `BOOLEAN` | `BOOL` |
| `i8`, `i16` | `5` | `SMALLINT` | any integer |
| `i32`, `u16` | `5` | `INTEGER` | any integer |
| `i64`, `u32`, `isize` | `5`, `(-5)` | `BIGINT` | any integer |
| `u64`, `usize` | `5` | `UBIGINT` | any integer |
| `i128` | `5` | `HUGEINT`, `NUMERIC` | any integer |
| `f32` | `0.1::real` | `REAL` | `F32`, `F64` when exact |
| `f64` | `0.1`, `'NaN'` | `DOUBLE PRECISION` | `F64`, `F32`, integers |
| [`Numeric`](crate::Numeric) | `12.50` | `NUMERIC`, `DECIMAL` | see below |
| `&str`, `String` | `'it''s'` | `TEXT`, `VARCHAR`, `CHAR` | `STRING` |
| `&[u8]`, `Vec<u8>` | `'\x00ff'` | `BYTEA` | `STRING`, `BYTEA` |
| [`Uuid`](crate::Uuid) | `'a0ee…'` | `UUID` | `UUID` |
| [`Timestamp`](crate::Timestamp), `SystemTime` | `'2026-11-16 12:00:00+00'` | `TIMESTAMPTZ`, `TIMESTAMP` | `DATETIME` |
| [`Date`](crate::Date) | `'2026-11-16'` | `DATE` | `DATE` |
| [`Time`](crate::Time) | `'13:05:09.25'` | `TIME` | `TIME` |
| [`Interval`](crate::Interval), `Duration` | `'1 mons 2 days 3 microseconds'` | `INTERVAL` | `INTERVAL` |
| [`Json`](crate::Json) | `'{"a":1}'` | `JSON`, `JSONB` | see below |
| `Vec<T>`, `&[T]`, `[T; N]` | `'{1,2,NULL}'` | `T[]` | see below |
| `Option<T>` | `NULL` when `None` | any nullable column | `NULL` is `None` |
| [`Value`](crate::Value) | its variant | any | any |

Every non-text type also reads from text, so a column selected as
`col::text` reads back as its own type: `"2026-01-02"` as a `Date`,
`"{1,2}"` as a `Vec<i64>`.

## Three types that need `::text`

The native protocol sends a cell's stored bytes and a type tag, nothing
more. For three kinds of column that is not enough to know the value:

* **`jsonb`** arrives under the text tag holding the engine's binary
  document. Reading it as [`Json`](crate::Json) is an error; select
  `col::text`.
* **Arrays** arrive under the text tag holding PostgreSQL's binary array.
  This client decodes that form too, but `col::text` is the one to rely on.
* **`NUMERIC(p, s)`** arrives as an integer scaled by `s`, and `s` is not
  sent: `12.34` in a `NUMERIC(10,2)` column and a `sum()` of `1234` are the
  same bytes. Reading one is an error naming the fix (`col::text`, or a cast
  such as `sum(x)::bigint`); an unconstrained `NUMERIC` (and `avg`) carries
  its scale and reads directly.

These are errors, never a plausible wrong value. The ORM writes its select
lists with these casts, so its models read correctly by construction.

```rust,no_run
# use rasterizeddb_client::prelude::*;
# async fn demo(client: &Client) -> rasterizeddb_client::Result<()> {
let (price, doc, tags): (Numeric, Json, Vec<String>) = client
    .fetch_one("SELECT price::text, doc::text, tags::text FROM items WHERE id = $1", params![7i64])
    .await?;
# let _ = (price, doc, tags); Ok(()) }
```

## Floats

An `f32` is written as `0.1::real`. A bare `0.1` is a `NUMERIC` literal,
which never equals the `REAL` 0.1 (that is 0.100000001490116…), so
`WHERE r = 0.1` finds nothing, as in PostgreSQL. `NaN` and the infinities
are written as `'NaN'`, `'Infinity'` and `'-Infinity'`.

## Points in time

A [`Timestamp`](crate::Timestamp) is microseconds since the Unix epoch, and
is written in UTC with an explicit `+00`. A `timestamptz` column stores that
instant whatever the server's time zone; a `timestamp` column stores the UTC
wall-clock time (PostgreSQL drops the zone there).

## Optional crates

| feature | adds |
|---|---|
| `chrono` | `DateTime<Utc>`, `NaiveDateTime`, `NaiveDate`, `NaiveTime` as parameters and results |
| `uuid` | `uuid::Uuid` as a parameter and a result |
| `serde_json` | `serde_json::Value` as a result; [`Json::from_value`](crate::Json::from_value) and [`Json::parse`](crate::Json::parse); `#[rdb(json)]` model fields; [`KvJson`](crate::kv::KvJson) |

## Enums

`#[derive(SqlEnum)]` stores an enum of unit variants as text:

```rust
use rasterizeddb_client::SqlEnum;

#[derive(SqlEnum, Debug, Clone, Copy, PartialEq)]
#[rdb(rename_all = "snake_case")]
enum Status {
    Active,
    #[rdb(rename = "on-hold")]
    OnHold,
}

assert_eq!(Status::OnHold.as_sql_str(), "on-hold");
```
