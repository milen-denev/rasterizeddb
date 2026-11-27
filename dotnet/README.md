# RasterizedDB for .NET

Packages that talk to a RasterizedDB server over its native protocol (rastcp), the one
`rasterizeddb_client` speaks in Rust, and one for its message broker:

| package | what |
|---|---|
| `RasterizedDb.Data` | An ADO.NET driver (`DbConnection`, `DbCommand`, `DbDataReader`, `DbDataSource`), plus the server's administration statements and key/value cache as typed calls. .NET 8, 9 and 10. |
| `RasterizedDb.EntityFrameworkCore` | An Entity Framework Core provider on top of it. EF Core 10 on .NET 10; EF Core 9 on .NET 8 and 9. |
| `RasterizedDb.Messaging` | A client for the message broker (`rdbmq/1`): queues, topics, subscriptions. Pre-production, see [below](#messaging). .NET 8, 9 and 10. |

```csharp
// ADO.NET
await using var source = new RasterizedDbDataSource("Host=db.example.com;Username=app;Password=secret;Database=shop");
await using var connection = await source.OpenConnectionAsync();
await using var command = new RasterizedDbCommand("SELECT id, name FROM users WHERE age > @age", connection);
command.Parameters.Add("age", 30);
await using var reader = await command.ExecuteReaderAsync();
while (reader.Read())
{
    Console.WriteLine($"{reader.GetInt64(0)} {reader.GetString(1)}");
}

// EF Core
services.AddDbContext<ShopContext>(o => o.UseRasterizedDb("Host=db.example.com;Username=app;Password=secret;Database=shop"));
```

## Connecting

| keyword | default | |
|---|---|---|
| `Host`, `Port` | port 61170 | The native listener (`--rdb-addr`, `--rdb-port`). TLS 1.3. |
| `Socket` | | A unix socket's path (`--rdb-socket`) instead of `Host`. No TLS, and none needed. |
| `Database` | the server's default | |
| `Username`, `Password` | | A SCRAM-SHA-256 login, bound to the connection it runs on. |
| `Login` | `Auto` | `Trust` (no exchange), `Peer` (the OS user, on a unix socket), `Password`. `Auto` is `Password` when one is given. |
| `Role` | | On a trust connection, the role its statements run as. From RasterizedDB 1.0.2 a trust connection that names none is no administrator in a database that has roles. |
| `Trust` | `AcceptAny` | Whether the server's certificate is checked: `AcceptAny`, `WebPki`, `RootCa` (+ `Root Certificate`, a PEM file), `Pinned` (+ `Certificate Fingerprint`, SHA-256 hex). |
| `Result Format` | `Text` | See below. |
| `Command Timeout` | 30 | Seconds; 0 waits indefinitely. |
| `Timeout` | 15 | Seconds to connect and log in. |
| `Pooling`, `Maximum Pool Size`, `Minimum Pool Size` | on, 100, 0 | |
| `Keepalive` | 10 | Seconds between pings of an unused pooled connection (0: none). |
| `Idle Reconnect` | 20 | A connection quiet this long is replaced before the next statement (0: never). |
| `Connection Idle Lifetime` | 300 | Seconds an unused pooled connection is kept. |
| `Max Message Size` | 8 MiB | The largest answer accepted. |
| `Client Certificate`, `Client Key` | | PEM files of a certificate to present to a server that asks for one. |
| `Tls Engine` | `Auto` | `Platform` (SslStream) or `Managed` (BouncyCastle). |

**Over a network you do not control, do not keep `Trust=AcceptAny`.** A password login stays safe from
replay under it, but whoever answers in the server's place reads every statement and row.

The server closes a native connection that has been quiet for 30 s. The pool pings its unused
connections, and an open connection that has gone quiet is swapped for a fresh one before the next
statement, unless a transaction is open on it. Nothing is ever sent twice: a statement whose connection
fails throws, because whether it ran is unknown. A transaction left quiet for 30 s is lost and says so
(`RasterizedDbErrorKind.TransactionLost`).

## Two result formats

The native protocol answers a statement with the engine's binary cells and nothing else: no column
names. That is the fast path, and it suits code that knows what it selected. Code that reads by name
(`reader["name"]`, Dapper, `DataTable.Load`) needs the other form.

| | `Binary` | `Text` (the default) |
|---|---|---|
| statements per command | one | any number (`NextResult`) |
| column names, `GetOrdinal`, schema | no (`?column?`) | yes |
| cells | typed binary, decoded in place | PostgreSQL's text, parsed on read |
| server path | native; repeated statements are answered from a learned parse | the PostgreSQL path |
| cancelling | when asked for (below) | always |

Set it per connection string (`Result Format=Binary`) or per command (`command.ResultFormat`). The EF
Core provider picks for you: the SQL it generates runs in `Binary`; raw SQL (`FromSql`, `SqlQuery`,
`ExecuteSql`) and migrations run in `Text`.

## Cancelling a statement

A statement run with a `CancellationToken` that can be cancelled is sent with a cancel token of its
own. Cancel it and the driver asks the server to stop the statement, waits for it to say so (SQLSTATE
57014) and throws `OperationCanceledException`: the connection stays open, and so does a transaction
it holds, to be rolled back. Only if the server does not answer within five seconds is the connection
closed instead. A command on a data source (below) stops waiting at once and tells the server.

`DbCommand.Cancel()` from another thread stops a text-format statement, and a binary-format one when
`command.Cancelable = true` (a synchronous call has no token to say so). Against a server that predates
cancelable requests, a binary-format statement is not interrupted: a cancelled token closes its
connection, as it did before.

## Parameters

`$1`…`$n` (by position in the collection) or `@name`. The native protocol has no Bind message, so a
value reaches the server as the SQL literal for it, written by the driver under the server's quoting
rules: text is single-quoted with `'` doubled, bytes are a `'\x…'` literal, a negative number is
parenthesised, a NUL is refused. A value can never become syntax, and a placeholder inside a string, a
quoted name or a comment is left alone. Where the server knows the form, a statement's text and its
values travel apart, so the text is the same bytes every time.

`RasterizedDbParameter<T>` holds its value unboxed.

| .NET | SQL |
|---|---|
| `bool`, `short`, `int`, `long`, `float`, `double`, `decimal` | `boolean`, `smallint`, `integer`, `bigint`, `real`, `double precision`, `numeric` |
| `string`, `byte[]`, `Guid` | `text`, `bytea`, `uuid` |
| `DateTime` (UTC or local) / `DateTimeOffset` | `timestamp with time zone`; reads back as UTC |
| `DateTime` (unspecified) | `timestamp`; reads back unspecified |
| `DateOnly`, `TimeOnly`, `TimeSpan` | `date`, `time`, `interval` (`RasterizedDbInterval` for one holding months) |
| `T[]`, `List<T>` | an array literal; arrays read back with `GetFieldValue<T[]>` |

## Transactions

`BeginTransaction` opens the block PostgreSQL clients get: isolation levels, savepoints
(`transaction.Save("name")`), DDL that rolls back. A statement that fails inside it fails the block
(25P02) until it is rolled back. `RasterizedDbDataSource.ExecuteInTransactionAsync` runs a body again
after a serialization failure or a deadlock (`DbException.IsTransient`), and never repeats a COMMIT
whose outcome is unknown (`RasterizedDbException.CommitOutcomeUnknown`).

## Commands without a connection

```csharp
await using var command = dataSource.CreateCommand("SELECT name FROM users WHERE id = $1");
command.Parameters.AddWithValue(42);
var name = await command.ExecuteScalarAsync();
```

A command made by the data source runs on one connection all such commands share, with any number of
statements in flight at once. It is the cheapest way to run statements that each commit on their own.
Transaction control is refused there; a transaction, a temporary table or a sequence's `currval` needs
a `RasterizedDbConnection`.

## Batches

`connection.CreateBatch()` sends several statements in one round trip, as one simple query: the server
runs them as one implicit transaction, and the reader has a result per row-returning statement.

## Migrations without an ORM

```csharp
var migrator = new RasterizedDbMigrator()
    .Step("001_users", "CREATE TABLE users (id BIGSERIAL PRIMARY KEY, email TEXT NOT NULL)")
    .Step("002_unique_email", "ALTER TABLE users ADD CONSTRAINT users_email_key UNIQUE (email)")
    .Down("ALTER TABLE users DROP CONSTRAINT users_email_key");
await migrator.ApplyAsync(dataSource);   // or VerifyAsync: refuse to start with anything pending
```

Each step runs in a transaction with the row that records it. Every statement is checked before
anything runs; an applied step whose text changed is an error.

## Administration and key/value

```csharp
var admin = connection.Admin;
await admin.CreateIndexAsync(new RasterizedDbIndex("users", "email").Unique());
await admin.CreateRoleAsync(RasterizedDbRole.User("app").Password("…"));
foreach (var table in await admin.TablesAsync()) { … }

var kv = connection.Kv;
await kv.SetAsync("greeting", "hello", TimeSpan.FromMinutes(5));
var sessions = kv.Map<Guid, Session>("sessions");     // scalars as text, anything else as JSON
await sessions.SetAsync(id, session, TimeSpan.FromHours(1));
var busy = await sessions.Query().Where((_, s) => s.Requests > 100).ToListAsync();

var visitors = kv.Repository<Visitor, Guid>(v => v.Id);   // an object as a hash, a field per property
await visitors.IncrementAsync(id, nameof(Visitor.Visits));
```

Every call has a synchronous twin without the `Async` (`admin.Tables()`, `kv.Set(…)`,
`sessions.TryGet(id)`), which sends with blocking I/O on the calling thread.

## Entity Framework Core

```csharp
options.UseRasterizedDb(connectionString, o => o.EnableRetryOnFailure());
```

- Integer keys generated on insert are `serial` / `bigserial` columns; generated values come back with
  `RETURNING`.
- `SaveChanges` sends its commands together, up to `MaxBatchSize` (100) per round trip.
- Migrations emit PostgreSQL DDL. While migrating, a row in `__EFMigrationsLock` is the lock (the
  server has no advisory locks); a migration that died holding it leaves the row, and the error says
  how to clear it. Idempotent scripts wrap each migration in a `DO` block.
- Translated: comparisons, paging, joins, `Include`, grouping and aggregates, `Contains` over a list,
  the common `string` members (with `PadLeft`/`PadRight`), `Math`, date parts, `Date`, `AddDays` and
  its siblings, `TimeSpan.Total…`, `EF.Functions.Like` and `EF.Functions.ILike`, `ExecuteUpdate` /
  `ExecuteDelete`. `Regex.IsMatch(text, pattern)` is `text ~ pattern` (`~*` with
  `RegexOptions.IgnoreCase`; other options are not translated): the pattern is read by the server, in
  PostgreSQL's syntax.
- **HiLo keys.** `builder.Property(x => x.Id).UseHiLo("order_ids")` takes keys from a sequence a block
  at a time (ten, or the sequence's `IncrementsBy`): the key is set when the entity is added, and one
  `nextval` serves a block of inserts. The sequence is part of the model, so migrations create it.
- **Array columns.** A `T[]` or `List<T>` property of a mapped type is an array column (`text[]`,
  `integer[]`). `tags.Contains(x)` is `x = ANY (tags)`; `Length`, `Count` and `Any()` are
  `cardinality(tags)`; `tags[0]`, `First()` and `ElementAt(i)` are `tags[1]` and so on.
- **JSON columns.** An owned type mapped with `ToJson()` lives in one `jsonb` column, nested owned
  types and collections included. A member in a query is a path into it
  (`CAST((b."Details" ->> 'Stars') AS integer)`), and a change to part of a document is written in
  place with `jsonb_set`.
- **Scaffolding.** `dotnet ef dbcontext scaffold "<connection string>" RasterizedDb.EntityFrameworkCore`
  reads tables, views, columns, primary and unique keys, foreign keys and indexes from
  `information_schema` and `pg_indexes` (the `public` schema). `test/scaffold-e2e.sh` runs the tool
  against a real server and uses the code it writes.
- Not there yet: LINQ composed over a JSON collection or over an array's elements (`SelectMany`, a
  `Where` inside the array), spatial types.

## Messaging

`RasterizedDb.Messaging` is a client for the server's message broker. **Pre-production**: it is written
against a broker whose protocol (`rdbmq/1`) is not frozen, and has been tested against a stand-in that
speaks the frames; of the broker itself, only queue delivery, nack, redelivery and dead-lettering are
tested (with `RDB_SERVER_BIN` set).

```csharp
await using var broker = await RasterizedDbBroker.ConnectAsync("Host=mq.example.com;Port=7171;Username=app;Password=secret");
await broker.DeclareQueueAsync("jobs", new BrokerQueueConfig { AckTimeout = TimeSpan.FromMinutes(1), MaxDeliveries = 5, DeadLetter = "jobs.dead" });
await broker.ProduceAsync("jobs", payload, [new BrokerHeader("kind", "email")]);       // durable when it returns

await using var jobs = await broker.ConsumeAsync("jobs", credits: 64);
await foreach (var message in jobs)
{
    try
    {
        Handle(message.Value.Span, message.Header("kind"));
        jobs.Ack(message);
    }
    catch (TransientException)
    {
        jobs.Nack(message, TimeSpan.FromSeconds(30));      // delivered again, after the delay
    }
}
```

One connection carries every request and subscription and is safe to share. A subscription holds about
`credits` messages unread and asks for more as they are taken. The connection does not reconnect by
itself: when it is lost, pending calls and subscriptions end with a `BrokerException` whose `Code` is
null, `ConnectionLost` is raised, and the application connects and consumes again (the broker delivers
unacknowledged messages again). Topics with partitions and ordered or broadcast subscriptions
(`CreateTopicAsync`, `SubscribeAsync`), batches (`BrokerBatchBuilder`), `FetchAsync` and exchanges
(`DeclareExchangeAsync`, `BindAsync`, `PublishAsync`) are there too. `ProduceAsync(…, deliverAt: time)`
holds a message back from a queue's consumers until then, and `jobs.RenewAsync(message, extend)` keeps
a delivered message yours for longer when the work outlasts the acknowledgement timeout.

`message.DeliveryCount` says which delivery of a queue's message this is: 1 the first time, one more
after each nack or lapsed acknowledgement timeout. When it equals the queue's `MaxDeliveries` this is
the last attempt, and a handler that cannot finish should say so (log, alert) before the broker
dead-letters the message. The broker keeps the counts in memory: they start again from 1 after it
restarts, so a count is a lower bound across a restart. A broker older than delivery counts reports 1
for every message.

Not yet: compressed batches.

## Tests

`dotnet test` runs the unit tests anywhere. The live tests start a real server from the binary
`RDB_SERVER_BIN` names, and pass without doing anything when it is not set:

```
RDB_SERVER_BIN=/path/to/rasterizeddb_core dotnet test
```
