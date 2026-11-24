# Transactions

A [`Transaction`](crate::Transaction) runs statements that commit together
or not at all, with the same semantics as a `BEGIN … COMMIT` block on the
server's PostgreSQL listener: the server runs both through one transaction
machinery.

```rust,no_run
use rasterizeddb_client::prelude::*;

# async fn demo(client: &Client) -> rasterizeddb_client::Result<()> {
let tx = client.begin().await?;
tx.exec("UPDATE account SET balance = balance - $1 WHERE id = $2", params![10, 1]).await?;
tx.exec("UPDATE account SET balance = balance + $1 WHERE id = $2", params![10, 2]).await?;
tx.commit().await?;
# Ok(()) }
```

## Opening and ending one

| call | does |
|---|---|
| [`client.begin()`](crate::Client::begin) | `BEGIN`, at the server's default isolation (read committed) |
| [`client.begin_with(options)`](crate::Client::begin_with) | with an [`Isolation`](crate::Isolation) level, or `READ ONLY` |
| [`tx.commit()`](crate::Transaction::commit) | `COMMIT` |
| [`tx.rollback()`](crate::Transaction::rollback) | `ROLLBACK` |
| dropping `tx` | rolls back: its connection is closed, and the server rolls back a transaction whose connection ends |
| [`client.transaction(…)`](crate::Client::transaction) | runs a closure in one; see below |

Statements go through [`Executor`](crate::Executor), which `Transaction`
implements: `tx.run`, `tx.query`, `tx.query_with`, `tx.execute`, `tx.exec`,
`tx.fetch_all`, `tx.fetch_one`, `tx.fetch_optional`, `tx.fetch_scalar`. So
code written against `E: Executor` runs on a client, each statement
committing on its own, or inside a transaction, unchanged:

```rust,no_run
use rasterizeddb_client::{Executor, Result, params};

async fn transfer<E: Executor>(e: &E, from: i64, to: i64, amount: i64) -> Result<()> {
    e.exec("UPDATE account SET balance = balance - $1 WHERE id = $2", params![amount, from]).await?;
    e.exec("UPDATE account SET balance = balance + $1 WHERE id = $2", params![amount, to]).await?;
    Ok(())
}

# async fn demo(client: &rasterizeddb_client::Client) -> Result<()> {
let tx = client.begin().await?;
transfer(&tx, 1, 2, 10).await?;
tx.commit().await?;
# Ok(()) }
```

`BEGIN`, `COMMIT`, `ROLLBACK`, `SAVEPOINT` and the rest are never sent as
statements: [`Client::run`](crate::Client::run) and every executor call
refuse them with [`Error::Unsupported`](crate::Error::Unsupported). The
methods above send them.

## The closure form, and retries

```rust,no_run
# use rasterizeddb_client::prelude::*;
# async fn demo(client: &Client) -> rasterizeddb_client::Result<()> {
let new_balance: i64 = client
    .transaction(|tx| {
        Box::pin(async move {
            let balance: i64 = tx
                .fetch_scalar("SELECT balance FROM account WHERE id = $1", params![1])
                .await?;
            tx.exec("UPDATE account SET balance = $1 WHERE id = $2", params![balance - 10, 1])
                .await?;
            Ok(balance - 10)
        })
    })
    .await?;
# let _ = new_balance; Ok(()) }
```

It commits when the closure returns `Ok`, and rolls back when it returns
`Err`. When the failure was a serialization failure (`40001`) or a deadlock
(`40P01`) ([`Error::is_retryable`](crate::Error::is_retryable)), it waits a
few milliseconds and runs the closure again in a new transaction, up to
[`TransactionOptions::retries`](crate::TransactionOptions::retries) times
(default [`DEFAULT_TRANSACTION_RETRIES`](crate::DEFAULT_TRANSACTION_RETRIES),
2; set 0 for none). **So the closure may run more than once**: do what it
does outside the database after it returns. Only those two failures are
retried: the server rolled the whole transaction back, and running it again
may succeed. A COMMIT whose outcome is unknown is never retried.

A closure that swallows a statement's error and returns `Ok` does not
commit: the server has failed the transaction, and COMMIT reports the
rollback.

## Isolation

| [`Isolation`](crate::Isolation) | the server |
|---|---|
| `ReadCommitted` (the default) | a snapshot per statement; a write to a row another transaction is writing waits for it, then goes on with what it committed |
| `RepeatableRead` | one snapshot for the transaction; a write to a row another transaction changed after it fails with `40001` |
| `Serializable` | accepted, and runs as `RepeatableRead`: write skew between transactions is not detected |

`read_only: true` makes every write fail with `25006`.

## Savepoints

```rust,no_run
# use rasterizeddb_client::prelude::*;
# async fn demo(client: &Client) -> rasterizeddb_client::Result<()> {
let tx = client.begin().await?;
tx.run("INSERT INTO log (id) VALUES (1)").await?;
tx.savepoint("before_risky").await?;
if tx.run("INSERT INTO log (id) VALUES (1)").await.is_err() {
    // The failure is undone, and the transaction can go on.
    tx.rollback_to("before_risky").await?;
}
tx.release("before_risky").await?;
tx.commit().await?;
# Ok(()) }
```

[`savepoint`](crate::Transaction::savepoint),
[`rollback_to`](crate::Transaction::rollback_to) and
[`release`](crate::Transaction::release) take a plain identifier.

## Errors inside a transaction

After a statement fails, the server refuses every later one with `25P02`
until the transaction ends or rolls back to a savepoint taken before the
failure. `commit()` of such a transaction rolls it back and fails with
`25P02`.

A transaction's errors end with `(SQLSTATE xxxxx)`, read by
[`Error::sqlstate`](crate::Error::sqlstate):

| code | |
|---|---|
| `40001` | serialization failure ([`is_serialization_failure`](crate::Error::is_serialization_failure)) |
| `40P01` | deadlock victim ([`is_deadlock`](crate::Error::is_deadlock)) |
| `55P03` | a lock wait passed `SET lock_timeout` |
| `57014` | the statement ran past the statement timeout ([`is_statement_timeout`](crate::Error::is_statement_timeout)) |
| `25P02` | the transaction had already failed |
| `25006` | a write in a read-only transaction |
| `23505` | a unique or primary key ([`is_unique_violation`](crate::Error::is_unique_violation)) |

DDL inside a transaction rolls back with it (`CREATE TABLE`, `ALTER TABLE`,
`CREATE INDEX`, `DROP TABLE`, which is carried out at COMMIT, …), as on the
PostgreSQL listener. `CREATE DATABASE` and `DROP DATABASE` are refused in a
transaction (`25001`). Warnings the server raises as notices (a `BEGIN`
inside a transaction, a `RAISE NOTICE`, DDL it cannot roll back) come with
the statement's answer ([`Rows::notices`](crate::Rows::notices)) and go to
the client's notice handler
([`Client::set_notice_handler`](crate::Client::set_notice_handler)).

A statement that runs past the server's statement timeout (its request
timeout, 30 s by default, or `SET statement_timeout` inside the
transaction) is stopped and fails with `57014`
([`is_statement_timeout`](crate::Error::is_statement_timeout)). The
transaction is then failed like any other (`25P02` until it rolls back),
and its connection stays: it is not lost. `SET statement_timeout = 0`
inside a transaction lets its statements run as long as they need; give
the client a `timeout` to match.

## Connections

A transaction runs on a connection of its own, never on one of the
client's shared connections. Those are multiplexed: many tasks' statements
travel on them at once, so a transaction there would take in other tasks'
statements, and the server refuses transaction control on them. A
transaction's connection is a plain one, whose requests the server runs one
at a time, in order. Statements sent from several tasks on one
`Transaction` queue behind each other.

When a transaction ends cleanly, its connection is kept for the next one,
for up to 20 s; otherwise one is opened, and logged in, for the
transaction. Opening one costs a connect and a login: little on a unix
socket, a TLS handshake over TCP, and a SCRAM exchange with
[`Login::Password`](crate::Login::Password). The server keeps a
transaction's settings (a `SET` inside it) only until the transaction ends.

## When the connection fails

A transaction lives and dies with its connection. **It is never moved to
another connection**: the client's idle reconnect and transparent reopen
do not apply to it. A transport failure ends the transaction, and that call
and every later one fail with
[`Error::TransactionLost`](crate::Error::TransactionLost). The server rolls a
transaction back when its connection ends, so it did not commit, except
when the connection failed while COMMIT was in flight: then
[`is_commit_outcome_unknown`](crate::Error::is_commit_outcome_unknown) is
true, and whether it committed cannot be known from this side. Check the
data before doing it again.

A statement whose future is dropped before its answer comes (a
`tokio::time::timeout` around it, a cancelled task) ends the transaction
the same way: its answer would otherwise be read as the next statement's.

## The idle limit

The server closes a native connection that has been idle for 30 s
([`SERVER_IDLE_CLOSE`](crate::SERVER_IDLE_CLOSE)), and rolls back the
transaction on it, releasing its locks. Nothing keeps a transaction's
connection alive by itself: a transaction its owner forgot must not hold its
locks for ever. A call made after 30 s of quiet fails with
`TransactionLost`, saying the transaction was idle, without sending
anything. Keep transactions short, and do slow work (a remote call, user
input) before `begin()` or after `commit()`.
