# Getting started

## Connecting

A [`Client`](crate::Client) talks to one server over its native protocol,
either a unix socket (the server's `--rdb-socket`) or TCP:

```rust,no_run
use rasterizeddb_client::{Client, Login, Options};
use std::time::Duration;

# async fn demo() -> rasterizeddb_client::Result<()> {
// A unix socket, under --rdb-socket-auth trust: no login exchange.
let local = Client::connect("unix:/run/rdb/rdb.sock", Options::default()).await?;

// Under --rdb-socket-auth peer: log in as this process's OS user.
let peer = Client::connect(
    "unix:/run/rdb/rdb.sock",
    Options { login: Login::Peer, ..Options::default() },
).await?;

// TCP, with more connections, a longer timeout and a larger answer limit.
let remote = Client::connect(
    "db.internal:61170",
    Options {
        connections: 4,
        timeout: Some(Duration::from_secs(60)),
        max_message_size: 64 * 1024 * 1024,
        ..Options::default()
    },
).await?;
# let _ = (local, peer, remote); Ok(()) }
```

Every connection is opened and logged in before `connect` returns, so a
wrong path or a refused login is `connect`'s error. When
[`Options::migrations`](crate::Options::migrations) is set, the migrations
run (or are verified) before it returns too: see the
[migrations chapter](crate::guide::migrations).

| option | default | meaning |
|---|---|---|
| `connections` | 1 | connections shared by every caller; each carries many statements at once |
| `timeout` | 30 s | how long a statement waits for its answer (`None`: forever) |
| `login` | `Trust` | `Trust`, `Peer` or `PeerAs(name)` |
| `max_message_size` | 8 MiB | the largest answer accepted; a larger one fails whole |
| `migrations` | `None` | a [`Migrator`](crate::orm::Migrator) to run on connect |
| `auto_migrate` | `Apply` | `Off`, `Apply` or `Verify` |

## Sharing a client

A `Client` is `Send + Sync`, and its connections are multiplexed: put it in
an `Arc` and call it from any number of tasks. Statements sent together run
side by side on the server, so await one before sending another when their
order matters.

```rust,no_run
# use rasterizeddb_client::{Client, Options, params};
# use std::sync::Arc;
# async fn demo() -> rasterizeddb_client::Result<()> {
let client = Arc::new(Client::connect("unix:/run/rdb/rdb.sock", Options::default()).await?);
let mut tasks = Vec::new();
for id in 0..100i64 {
    let client = client.clone();
    tasks.push(tokio::spawn(async move {
        client.exec("INSERT INTO hits (id) VALUES ($1)", params![id]).await
    }));
}
for task in tasks {
    task.await.expect("the task ran")?;
}
# Ok(()) }
```

## A first statement

```rust,no_run
# use rasterizeddb_client::{Client, Options, params};
# async fn demo(client: &Client) -> rasterizeddb_client::Result<()> {
client
    .run("CREATE TABLE IF NOT EXISTS notes (id BIGSERIAL, body TEXT NOT NULL, PRIMARY KEY (id))")
    .await?;
let id: i64 = client
    .fetch_scalar("INSERT INTO notes (body) VALUES ($1) RETURNING id", params!["hello"])
    .await?;
let notes: Vec<(i64, String)> = client
    .fetch_all("SELECT id, body FROM notes WHERE id >= $1 ORDER BY id", params![id])
    .await?;
# let _ = notes; Ok(()) }
```

## What the native protocol cannot do

The native protocol is the fast path for an application next to its
database. A few things it does not carry, which this client accounts for:

* **One statement per text.** A string holding two statements is refused
  before it is sent ([`Error::Unsupported`](crate::Error::Unsupported)): the
  server would run only the first statement of an `INSERT …; INSERT …`
  string and drop the rest.
* **No transactions.** `BEGIN`, `COMMIT` and the rest are refused; every
  statement commits on its own. Put writes that belong together into one
  statement (a multi-row `INSERT`, `INSERT … ON CONFLICT`, `UPDATE … WHERE`,
  `DELETE … RETURNING`), or use the server's PostgreSQL listener when you need
  a transaction block.
* **No column names or types in answers.** Columns are read by position, and
  each cell carries a type tag; see [types](crate::guide::types) for what that
  means for `jsonb`, arrays and `NUMERIC(p, s)`.
* **No bound parameters.** `$1`…`$n` are rendered into literals on this
  side, under the server's lexer's rules, so a value can never become syntax;
  see [`render`](crate::render).
* **Some statement shapes run on the PostgreSQL listener only** (as of
  this release): common table expressions (`WITH …`), `SELECT EXISTS (…)`,
  `SELECT` with no `FROM`, and `CREATE UNIQUE INDEX` (use
  [`Index::unique`](crate::admin::Index::unique), which writes the
  constraint form the native protocol reads).

A connection that fails mid-statement returns
[`Error::Transport`](crate::Error::Transport): whether that statement ran is
unknown, and nothing is sent twice. The next statement reconnects.
