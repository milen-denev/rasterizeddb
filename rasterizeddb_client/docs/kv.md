# The key/value cache

The server keeps a Redis-compatible key/value cache, reachable from SQL as
`KV …` statements. [`client.kv()`](crate::Client::kv) gives three levels of
it. Every argument is sent as hex and values come back as bytes, so any byte
string is a key or a value.

## Typed values

```rust,no_run
# use rasterizeddb_client::prelude::*;
# use std::time::Duration;
# async fn demo(client: &Client) -> rasterizeddb_client::Result<()> {
let kv = client.kv();
kv.set_as("visits", &41i32, None).await?;
let visits: Option<i32> = kv.get_as::<i32>("visits").await?;
kv.set_as("banner", &"Sale!".to_string(), Some(Duration::from_secs(600))).await?;
# let _ = visits; Ok(()) }
```

## Typed maps and queries

A [`KvMap<K, V>`](crate::kv::KvMap) keeps its entries under `prefix:<key>`.
Keys and values are any type with a text form: strings, integers, floats,
booleans, bytes, `Uuid`, `Timestamp`, `Numeric`, `SqlEnum` enums, arrays, or
[`KvJson<T>`](crate::kv::KvJson) for a serde type.

```rust,no_run
# use rasterizeddb_client::prelude::*;
# use std::time::Duration;
# async fn demo(client: &Client) -> rasterizeddb_client::Result<()> {
let hits = client.kv().map::<String, i64>("hits");
hits.incr_by(&"/api/users".to_string(), 1).await?;

// Narrowed on the server by key, then here by typed closures.
let busy: Vec<(String, i64)> = hits
    .query()
    .prefix("/api/")                    // SCAN … MATCH, on the server
    .filter(|path, n| *n > 100 && !path.ends_with("/health"))
    .limit(50)
    .all()
    .await?;

// The whole keyspace, by key only: values are read only for keys that pass.
let found: Vec<(String, i32)> = client
    .kv()
    .query::<String, i32>()
    .filter_keys(|key| key == "Value 1")
    .all()
    .await?;

// A lease: the first caller wins until it expires.
let leases = client.kv().map::<String, Uuid>("lease");
let me = Uuid::new_v4()?;
let mine = leases.set_if_absent(&"nightly-report".to_string(), &me, Some(Duration::from_secs(300))).await?;
# let _ = (busy, found, mine); Ok(()) }
```

`filter_keys` runs before values are read; `filter` after. A cache has no
index on values, so a value filter reads every value the key filters let
through. Keys or values that do not read as `K`/`V` are an error unless
[`lenient()`](crate::kv::KvQuery::lenient) skips them.

## Models as hashes

[`KvRepo<M>`](crate::kv::KvRepo) stores a `#[derive(Model)]` struct as a hash
under `<table>:<primary key>`, one field per column. The struct is the same
one the SQL ORM maps, so a row can live in a table, in the cache, or both.

```rust,no_run
# use rasterizeddb_client::prelude::*;
# use std::time::Duration;
#[derive(Model, Debug, Clone)]
#[rdb(table = "sessions")]
struct Session {
    #[rdb(primary_key)]
    token: Uuid,
    user_id: i64,
    roles: Vec<String>,
    seen: Timestamp,
}

# async fn demo(client: &Client, s: Session) -> rasterizeddb_client::Result<()> {
let sessions = client.kv().repo::<Session>();
sessions.save_with_ttl(&s, Duration::from_secs(3600)).await?;
let again: Option<Session> = sessions.get(&[&s.token]).await?;
let theirs: Vec<Session> = sessions.filter(|x| x.user_id == 7).await?;
sessions.delete(&s).await?;
# let _ = (again, theirs); Ok(()) }
```

## Raw commands

[`Kv`](crate::kv::Kv) has the common commands on byte strings (strings,
hashes, lists, sets, sorted sets, TTLs, `SCAN`, `MGET`/`MSET`), and
[`command`](crate::kv::Kv::command) for any other.

No two commands are atomic together: the native protocol has no `MULTI`.
Where a call takes two commands (saving a model with NULL fields, setting a
TTL after a save) its documentation says so.

## A read-through cache for models

[`client.cached::<M>(ttl)`](crate::Client::cached) (or
`M::cached(&client, ttl)`) puts the cache in front of a model's table:
[`find`](crate::orm::Cached::find) answers from the cache, and on a miss
reads the table and stores the row for `ttl`. Writes through the same handle
run the ORM's statement and then remove the row's entry, so the next read
sees them.

```rust,no_run
# use rasterizeddb_client::prelude::*;
# #[derive(Model, Debug, Clone)] #[rdb(table = "profiles")]
# struct Profile { #[rdb(primary_key)] id: i64, name: String }
# async fn demo(client: &Client) -> rasterizeddb_client::Result<()> {
use std::time::Duration;

let profiles = client.cached::<Profile>(Duration::from_secs(30));
let mut p = profiles.find(7i64).await?.expect("there");   // the table, then cached
let again = profiles.find(7i64).await?;                     // the cache
p.name = "renamed".into();
profiles.update(&p).await?;                                 // UPDATE, then the entry removed
# let _ = again; Ok(()) }
```

**Staleness is bounded by the TTL, and only by it.** A write that does not
go through the handle (another process, plain SQL, a bulk
`query().update()`, a pgwire client) leaves the cached row as it was until
its TTL runs out. So does a read that races a write: a reader that read the
table just before the write and stores the row just after the write removed
the entry puts the old row back. Choose the TTL as the staleness you
accept.

Each entry is one `SET … PX`, so none is ever left without its TTL; it is
keyed `rdb_orm:<table>:<primary key>` and tagged with the model's columns,
so a struct whose columns changed reads an older entry as a miss. A row that
does not exist is not cached; soft-deleted rows read as missing, as `find`
reads them.

The cache needs nothing on the server beyond the key/value cache itself,
which the default build includes and which needs no store to be created;
a server built without it fails every call with
[`Error::Unsupported`](crate::Error::Unsupported) naming the cause. It is
not transactional, so the cached handle takes a [`Client`](crate::Client),
not a transaction.
