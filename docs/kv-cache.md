# Redis-Compatible Key/Value Store

Status: **implemented, in the default build** (`kv_cache` feature). Dormant
until used: nothing is allocated before the first `KV` statement or Redis
connection, and the Redis port is closed unless `--kv-port` is given.
Target: `rasterizeddb_core::core::kv_cache`.

The database can be the application's cache too. Any Redis client connects to
`--kv-port` unchanged, and the same store answers `KV …` statements on the
PostgreSQL port. So an application that already talks SQL to RasterizedDB can
cache, count, rate-limit and queue without a second server.

This is a different thing from [key/value *storage*](kv-storage.md). That
backend keeps a database's tables in an object store. This feature keeps keys:
strings, counters, hashes, lists, sets and sorted sets, with lifetimes.

## Starting it

```sh
RASTERIZEDDB_PG_PASSWORD=secret rasterizeddb_core --location /data/db \
    --protocol pgwire --kv-port 6379
redis-cli -p 6379 -a secret PING
```

| Flag | Default | Meaning |
|---|---|---|
| `--kv-port PORT` | closed | Listen for the Redis protocol |
| `--kv-addr IP` | `127.0.0.1` | Address to bind |
| `--kv-auth password\|trust` | `password` | `password` checks `AUTH` against the database's roles; `trust` asks nothing and is refused on a non-loopback address |
| `--kv-user ROLE` | `--pg-user` | The role `AUTH <password>` (no user name) is checked as |
| `--kv-max-memory SIZE` | `0` (unbounded) | Redis's `maxmemory`: `512mb`, `4gb` |
| `--kv-eviction POLICY` | `noeviction` | Redis's `maxmemory-policy` |
| `--kv-persist` | off | Save periodically and on clean shutdown |
| `--kv-save-interval-secs N` | `300` | Interval between periodic saves |

The Redis port serves the default database (`--pg-default-db`). Its numbered
databases (`SELECT 0`–`15`) belong to that database. Every database in the
catalogue has a store of its own, reachable over SQL by connecting to that
database.

The settings can be changed at runtime with `CONFIG SET maxmemory 1gb` and
`CONFIG SET maxmemory-policy allkeys-lru`.

**Authentication** uses the database's own roles: the ones `CREATE ROLE` makes
and the PostgreSQL port checks. `AUTH alice <password>` and
`HELLO 3 AUTH alice <password>` both work. Until a client authenticates it may
only send `AUTH`, `HELLO`, `QUIT` and `RESET`, and its frames are capped at
10 arguments of 16 KiB each, as Redis caps unauthenticated clients.

## From SQL

```sql
KV SET session:42 '{"user":7}' EX 3600;   -- value: OK
KV GET session:42;                          -- one row, or none
KV INCR page:/home:hits;                    -- value: bigint
KV HSET user:7 name 'Ada' plan 'pro';
KV HGETALL user:7;                          -- (field, value) rows
KV ZADD board 1500 ada 1320 bob;
KV ZRANGE board 0 9 REV WITHSCORES;         -- (member, score) rows
KV SCAN 0 MATCH 'session:*' COUNT 100;      -- (cursor, key) rows
KV INFO memory;                             -- (metric, value) rows
```

Arguments are words or single-quoted strings, with `''` for a quote inside one.
A statement runs against database 0. The columns depend only on the command and
its options, so the extended protocol (every ORM) can describe a statement
before it runs:

| Reply | Columns |
|---|---|
| integer (`INCR`, `DEL`, `HSET`, …) | `value bigint` |
| score (`ZSCORE`, `ZINCRBY`) | `value float8` |
| string or list of strings | `value text`, a row per element; a missing key is no row |
| `HGETALL`, `CONFIG GET` | `field text, value text` |
| `… WITHSCORES`, `ZPOPMIN/MAX` | `member text, score float8` |
| `SCAN`, `HSCAN`, `SSCAN`, `ZSCAN` | `cursor text` and the item columns |
| `INFO` | `metric text, value text` |

Refused from SQL, with a pointer to the Redis port, are the commands that need
a connection of their own: `MULTI`/`EXEC`, `SUBSCRIBE`, `SELECT`, `AUTH`,
`CLIENT`, `MOVE`. A blocking pop answers at once, as though its timeout had
passed. A value that is not valid UTF-8 is refused rather than sent mangled;
binary values belong on the Redis port.

A `KV` statement is **not part of a SQL transaction**. It takes effect when it
runs, and a later `ROLLBACK` does not undo it, just as a Redis command is not
undone by a database rollback. Use `MULTI`/`EXEC` on the Redis port for an
atomic group of key/value commands.

**Privileges.** The commands that administer the server rather than the data
(`CONFIG SET`/`RESETSTAT`, `SAVE`, `BGSAVE`, `FLUSHALL`) need a superuser role on
both ports, as `SET CONFIG` does in SQL. A Redis client gets `NOPERM`; a SQL
client gets the same refusal as any other privileged statement. `CONFIG GET`
and everything else is open to any role that can log in.

The statement log, `SHOW QUERIES` and `pg_stat_statements` record a `KV`
statement as its command alone (`KV SET <redacted>`). A key such as
`session:<token>` is as likely to be a secret as a value is.

## Commands

157 commands across every core type, plus `SWAPDB`, which is recognised and
answered with an error:

- **Strings:** `GET SET (NX XX GET EX PX EXAT PXAT KEEPTTL) SETNX SETEX PSETEX GETSET GETDEL GETEX
  MGET MSET MSETNX INCR DECR INCRBY DECRBY INCRBYFLOAT APPEND STRLEN GETRANGE SUBSTR
  SETRANGE SETBIT GETBIT BITCOUNT`
- **Keys:** `DEL UNLINK EXISTS TOUCH EXPIRE PEXPIRE EXPIREAT PEXPIREAT (NX XX GT LT) TTL PTTL
  EXPIRETIME PEXPIRETIME PERSIST TYPE RENAME RENAMENX COPY KEYS SCAN DBSIZE RANDOMKEY
  OBJECT MEMORY MOVE`
- **Hashes:** `HSET HSETNX HMSET HGET HMGET HGETALL HDEL HEXISTS HLEN HKEYS HVALS HINCRBY
  HINCRBYFLOAT HSTRLEN HRANDFIELD HSCAN`
- **Lists:** `LPUSH RPUSH LPUSHX RPUSHX LPOP RPOP LLEN LRANGE LINDEX LSET LREM LTRIM LINSERT LPOS
  LMOVE RPOPLPUSH BLPOP BRPOP BLMOVE BRPOPLPUSH`
- **Sets:** `SADD SREM SMEMBERS SISMEMBER SMISMEMBER SCARD SPOP SRANDMEMBER SINTER SINTERSTORE
  SINTERCARD SUNION SUNIONSTORE SDIFF SDIFFSTORE SMOVE SSCAN`
- **Sorted sets:** `ZADD (NX XX GT LT CH INCR) ZREM ZSCORE ZMSCORE ZINCRBY ZCARD ZCOUNT ZLEXCOUNT
  ZRANK ZREVRANK (WITHSCORE) ZRANGE (BYSCORE BYLEX REV LIMIT WITHSCORES) ZREVRANGE
  ZRANGEBYSCORE ZREVRANGEBYSCORE ZRANGEBYLEX ZREVRANGEBYLEX ZPOPMIN ZPOPMAX ZREMRANGEBYRANK
  ZREMRANGEBYSCORE ZREMRANGEBYLEX ZSCAN ZUNION ZINTER ZDIFF ZUNIONSTORE ZINTERSTORE
  ZDIFFSTORE (WEIGHTS AGGREGATE)`
- **Pub/Sub:** `PUBLISH SUBSCRIBE UNSUBSCRIBE PSUBSCRIBE PUNSUBSCRIBE PUBSUB`
- **Transactions:** `MULTI EXEC DISCARD WATCH UNWATCH`
- **Connection:** `PING ECHO AUTH HELLO (RESP2 and RESP3) SELECT (16 databases) QUIT RESET
  CLIENT (ID SETNAME GETNAME SETINFO INFO LIST REPLY)`
- **Server:** `INFO CONFIG (GET SET RESETSTAT) COMMAND (COUNT LIST INFO DOCS GETKEYS) SAVE BGSAVE
  LASTSAVE FLUSHDB FLUSHALL TIME`

Replies and error messages are Redis's own (`WRONGTYPE …`,
`ERR value is not an integer or out of range`, `OOM command not allowed …`), so
client libraries recognise them. `INFO` and `HELLO` report `redis_version:7.4.0`
because clients gate features on it (Sidekiq refuses anything below 6.2).
Every command such a client may then send is in the list above.

**Not implemented:** Lua scripting (`EVAL`, `FUNCTION`), streams (`XADD` …),
HyperLogLog, geo, `BITFIELD`/`BITOP`/`BITPOS`, `OBJECT FREQ` and the LFU
policies, keyspace notifications, `CLIENT TRACKING`, `WAIT`, cluster mode,
replication of the store, TLS on the Redis port, and the append-only file.
A command outside the list is answered with Redis's `ERR unknown command`.

## How it is built

**Shards.** The keyspace is 1024 shards, each a `parking_lot` read-write lock
over a hashbrown table. Redis runs one thread, so it never pays for a lock,
but it never uses a second core either. Here a read takes one shard's read
lock and a write takes one shard's write lock, and two random keys share a
shard one time in a thousand. Everything a write accounts for (bytes held,
keys with a lifetime, the version `WATCH` compares) is a plain field of the
shard, changed under its lock. No counter is shared across shards on the hot
path. A process-wide atomic touched by every write would be one cache line
every core fights over.

**One allocation per string.** A slot holds its key and a string value in one
boxed slice, so `SET` makes one allocation and `GET` touches one heap line.

**Atomicity.** Every command is atomic on its key. Commands on several keys
(`MSET`, `MGET`, `RENAME`, `LMOVE`, `SINTERSTORE`, `ZUNIONSTORE`, …) and `EXEC`
lock every shard they touch at once, in ascending shard order, so they are
atomic too and two of them cannot deadlock. The command table carries each
command's key positions, as Redis's does. That is how `EXEC` knows what to
lock. A unit test runs every command inside a transaction holding only those
shards, so a wrong key position fails a test instead of a server.

**Lifetimes.** An expiry is an absolute millisecond time on the key. A read
treats an expired key as absent. A write, or the sweeper (ten times a second,
visiting only shards that hold keys with a lifetime, and staying on a shard
while it keeps finding expired keys), removes it.

**Memory.** Each key is charged its bytes plus a fixed per-key and
per-element overhead. Structures keep running byte totals, so an `HSET` on a
million-field hash costs one insertion, not a walk. `maxmemory` is enforced per
shard, at `maxmemory / 1024`, under the shard's own lock. When a shard is full
the policy chooses a victim from a sample of five, as Redis does:
`noeviction` refuses the write with `OOM`, and `allkeys-lru`, `volatile-lru`,
`allkeys-random`, `volatile-random` and `volatile-ttl` evict. A single value
larger than a shard's whole share is still stored, into an otherwise emptied
shard, so the split never refuses what the store as a whole has room for.

**`SCAN`.** The cursor is a shard index, and a call returns whole shards until
it has `COUNT` keys. Each shard is walked under one read lock, so no key that
exists throughout a scan can be missed, whatever the table does between
calls. A call can therefore return more than `COUNT` keys, which Redis's
contract allows. `HSCAN`, `SSCAN` and `ZSCAN` return the whole structure in one
page, as Redis does for compactly encoded ones.

**Pub/Sub.** `PUBLISH` takes a read lock on the channel map and `try_send`s to
each subscriber's bounded queue. It never waits on a slow subscriber. A
subscriber whose queue fills is disconnected, which is what Redis does past
its output-buffer limit, rather than being sent a stream with holes in it.

**Blocking pops.** A push checks one relaxed counter and returns when nobody
waits. Otherwise it wakes as many waiters on the key as elements it pushed,
oldest first. A waiter that loses the race, or times out holding a wake-up,
passes the wake-up on, so an element never sits in a list while a client
sleeps on it.

**Persistence.** Off by default: it is a cache. `SAVE`, `BGSAVE` or `KV SAVE`
writes every database to `db_kv_cache.snap` beside the database's files, and
an open that finds that file loads it. The writer streams one shard at a time
under its read lock, so a save never holds more than one shard's bytes and
writers wait only on that one shard. The file is written as a sibling, fsynced,
renamed into place, and the directory is fsynced. The loader checks the CRC
over the whole file before storing a single key, so a torn snapshot is refused,
never half loaded. With `--kv-persist` the sweeper also saves every
`--kv-save-interval-secs` when anything changed, and a clean shutdown saves.
That is Redis's RDB without the AOF: a crash loses what changed since the last
save.

## Performance

Measured 2026-10-10 on one 16-core machine, with `memtier_benchmark` against
Redis 7.4 and Valkey 8.1 (the official images, `--network host`, persistence
off), each single-threaded and with `io-threads` equal to its cores. Keyspace
of 1M keys, filled before each run, random access, 50 connections per client
thread, 10 s per run. Every figure is the **median of three rounds**, with the
servers interleaved inside each round. Runs on this machine vary by up to 15%,
so the last column compares RasterizedDB's *slowest* run against the
competitor's *fastest*.

**Server on 8 cores, client on the other 8:**

| Workload | RasterizedDB | Redis | Redis io-threads | Valkey | Valkey io-threads | Ours vs best, median | worst vs their best |
|---|---|---|---|---|---|---|---|
| 32 B, 1 SET : 10 GET, no pipeline | **465k** | 182k | 295k | 174k | 393k | 1.18× | 1.15× |
| same, pipeline 16 | **3.42M** | 1.40M | 1.14M | 1.36M | 876k | 2.44× | 2.30× |
| 1 KiB, 1 : 1, no pipeline | **392k** | 167k | 281k | 167k | 336k | 1.17× | 1.10× |
| same, pipeline 16 | **1.68M** | 828k | 654k | 815k | 626k | 2.03× | 1.72× |

**Server on 4 cores, client on 12** (the client is no longer what limits an
unpipelined run):

| Workload | RasterizedDB | Redis | Redis io-threads | Valkey | Valkey io-threads | Ours vs best, median | worst vs their best |
|---|---|---|---|---|---|---|---|
| 32 B, 1 : 10, no pipeline | **402k** | 172k | 288k | 180k | 299k | 1.34× | 1.29× |
| same, pipeline 16 | **3.37M** | 1.38M | 1.37M | 1.32M | 1.30M | 2.45× | 2.40× |
| 1 KiB, 1 : 1, no pipeline | **403k** | 170k | 281k | 173k | 280k | 1.43× | 1.32× |
| same, pipeline 16 | **1.53M** | 816k | 724k | 821k | 853k | 1.79× | 1.70× |

Median latency is lower too, in every configuration. 8 cores, 32 B, no
pipeline: p50 0.71 ms against Valkey io-threads' 0.77 ms, and p99 2.85 ms
against its 4.08 ms. With pipelining the gap widens: p50 1.53 ms against
Redis's 4.29 ms.

**Where the time goes.** Unpipelined, 83% of the server's samples are in the
kernel's TCP path, and the store itself is about 3%. The win there is doing
that kernel work on every core at once. Each I/O thread runs its own
single-threaded runtime and owns its connections, so a request is read,
answered and written by one thread with no hand-off. That measured 5–13%
faster than one shared work-stealing runtime, with a lower p99. Pipelined,
user space shows: the hash probe and the shard lock, which are cache misses
into a million-key table. Redis's chain of dict entry, object and string
costs at least as many.

**Unpipelined, the client is the limit.** At 465k ops/s RasterizedDB used 5.4
of its 8 cores, and the client used all of its own. That is why the 4-core
configuration, which leaves the client room, shows a wider lead.

The harness is `memtier_benchmark --threads=N --clients=50 --ratio=1:10
--data-size=32 --key-pattern=R:R --key-maximum=1000000 --test-time=10
--pipeline=P`, run against each server in turn.
