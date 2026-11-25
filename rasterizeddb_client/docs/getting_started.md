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

// TCP as a role with a password (SCRAM-SHA-256), believing only the
// server's own certificate (started with --rdb-cert/--rdb-key), with more
// connections, a longer timeout and a larger answer limit.
let remote = Client::connect(
    "db.internal:61170",
    Options {
        login: Login::password("app", "<password>"),
        trust: rasterizeddb_client::pin(&["<the certificate's SHA-256 fingerprint>"])?,
        connections: 4,
        timeout: Some(Duration::from_secs(60)),
        max_message_size: 64 * 1024 * 1024,
        ..Options::default()
    },
).await?;

// Another database than the server's default, as `/kopa` in a PostgreSQL URL.
let kopa = Client::connect(
    "unix:/run/rdb/rdb.sock",
    Options { database: Some("kopa".into()), ..Options::default() },
).await?;
# let _ = (local, peer, remote, kopa); Ok(()) }
```

Every connection is opened and logged in before `connect` returns, so a
wrong path or a refused login is `connect`'s error. When
[`Options::migrations`](crate::Options::migrations) is set, the migrations
run (or are verified) before it returns too: see the
[migrations chapter](crate::guide::migrations).

**Over TCP, set `trust`.** The default, `AcceptAny`, encrypts but believes
any server: a password is safe from replay, but whoever answers in the
server's place can guess it offline from the login, and reads everything.
Pin the server's certificate ([`pin`](crate::pin)) or use a CA
([`load_ca`](crate::load_ca)).

**Timeouts end the wait, not the statement.** A statement whose `timeout`
passes on this side, or whose future is dropped, may still run to the end
on the server. Over a plain (non-shared) connection, such as a
transaction's, the write and the answer each get the whole timeout, so a
statement can take up to twice it before failing.

**The server bounds a statement itself.** At its request timeout (30 s by
default, `--rdb-request-timeout`) it stops the statement and answers with
SQLSTATE `57014`
([`Error::is_statement_timeout`](crate::Error::is_statement_timeout)), as
PostgreSQL's `statement_timeout` does: nothing of it took effect, the
connection goes on, and a transaction is failed as by any failed statement
(roll it back). The client waits [`ANSWER_GRACE`](crate::ANSWER_GRACE) (2 s)
past `timeout` for an answer, so that this answer, not a timeout of its
own, is what a caller gets when both are 30 s. To run a statement longer
than the server's limit, raise `--rdb-request-timeout`, or `SET
statement_timeout = '10min'` (or `0`, no limit) inside a transaction; and
raise `timeout` to match.

| option | default | meaning |
|---|---|---|
| `connections` | 1 | connections shared by every caller; each carries many statements at once |
| `timeout` | 30 s | how long a statement waits for its answer, plus a 2 s grace (`None`: forever) |
| `login` | `Trust` | `Trust`, `TrustAs(role)`, `Peer`, `PeerAs(name)` or `Password { user, password }` (`Login::password`). On a trust socket of a database with roles, `Trust` alone is no administrator and reaches only its own tables and what PUBLIC is granted: name a role with `TrustAs("rasterized")` |
| `database` | `None` | the database every connection uses (`None`: the server's `--pg-default-db`); a password or peer login is checked against that database's roles |
| `max_message_size` | 8 MiB | the largest answer accepted; a larger one fails whole ([`Error::is_message_too_large`](crate::Error::is_message_too_large)) and closes its connection |
| `idle_reconnect` | 20 s | reopen a connection idle this long before using it, inside the server's 30 s idle close |
| `trust` | `AcceptAny` | how a TCP connection checks the server: `AcceptAny` (encrypted, not authenticated), [`pin`](crate::pin), [`load_ca`](crate::load_ca) or `WebPki`; unix sockets have no TLS |
| `migrations` | `None` | a [`Migrator`](crate::orm::Migrator) to run on connect |
| `auto_migrate` | `Apply` | `Off`, `Apply` or `Verify` |

## Notices

What the server says about a statement without failing it (`RAISE NOTICE`
in a `DO` block, `DROP TABLE IF EXISTS` naming no table, `CREATE TABLE IF
NOT EXISTS` naming one that exists) comes with its answer,
[`Rows::notices`](crate::Rows::notices), each a [`Notice`](crate::Notice)
with its severity, SQLSTATE and message. A handler sees every one, those of
a statement that then failed included:

```no_run
# async fn demo(client: &rasterizeddb_client::Client) -> rasterizeddb_client::Result<()> {
client.set_notice_handler(|n| eprintln!("{}: {} ({})", n.severity, n.message, n.sqlstate));
let rows = client.run("DROP TABLE IF EXISTS gone").await?;
assert_eq!(rows.notices()[0].sqlstate, "00000");
# Ok(()) }
```

Each connection asks for notices when it opens. A server that predates
them refuses the request without running anything, and sends none; a
client that predates them never asks, and is sent none.

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
* **Transactions through [`begin`](crate::Client::begin).** A statement sent
  on the client commits on its own. Statements that must commit together run
  in a [`Transaction`](crate::Transaction), on a connection of its own; see
  [transactions](crate::guide::transactions). `BEGIN`, `COMMIT` and the rest
  written as statements are refused.
* **No column names or types in answers.** Columns are read by position, and
  each cell carries a type tag, its column's declared type; see
  [types](crate::guide::types) for `jsonb`, arrays and `NUMERIC(p, s)`.
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
