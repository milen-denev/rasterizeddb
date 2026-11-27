# The sharding gateway

`rasterizeddb_gateway` sits between PostgreSQL clients and the shards of a
cluster. For each statement it finds the route key, works out which shard
owns it, and relays the client's bytes to a session on that shard. It does
not parse or run SQL, and it does not decode rows. The design is
`docs/sharding-plan.md` §4.9; this page is how to run it.

The gateway is never what keeps data correct. Every shard refuses a read or
write outside the ranges it owns (`RDB02`). So a stale map or a wrong
template costs a retry or an error, never wrong rows.

## Running it

```
rasterizeddb_gateway --config /etc/rasterizeddb/gateway.conf
rasterizeddb_gateway --map cluster.map --listen 0.0.0.0:6432 --auth scram
```

The configuration file has one `key = value` per line; `#` starts a
comment. A `--key=value` flag (or `--key value`) overrides the same key in
the file. A dash in a key means the same as an underscore.

| Key | Default | Meaning |
|---|---|---|
| `listen` | `0.0.0.0:6432` | Comma-separated addresses. Every worker binds each one with `SO_REUSEPORT`. |
| `workers` | `0` (one per core) | Worker threads. Each is a single-threaded runtime, and a client stays on one for its whole connection. |
| `map` | — | The shard map file, in text or binary form (`rasterizeddb_router::ShardMap`). The highest epoch seen is written to `<map>.epoch`. |
| `map_from` | — | Fetch the map from a shard (`host:port`) at start, instead of reading a file. |
| `home_shard` | lowest shard id | Where global tables, sequences, catalogue reads, session `SET`s, classification and authentication go. |
| `gateway_name` | `rasterizeddb_gateway` | The name the gateway gives shards in Hello and in `OpenSession`. |
| `tls_cert`, `tls_key` | off | Client TLS. The gateway terminates TLS, so SCRAM channel binding (`-PLUS`) works. |
| `shard_trust` | `accept_any` | How shard certificates are checked: `accept_any`, `ca:<file>` or `pinned:<sha256>[,…]`. |
| `shard_cert`, `shard_key` | off | The gateway's mTLS identity towards the shards. |
| `auth` | `scram` | `scram` (SCRAM-SHA-256, checked by the gateway) or `trust` (loopback benches only). |
| `unrouted` | `refuse` | What a statement without a route key does: `refuse` or `fan_out` (B1b, D8). |
| `unrouted_writes` | `refuse` | Only `refuse` is accepted here. `each_shard` can be set per session only. |
| `user_routing` | `off` | Per-user shards (G11): `off`, `prefer` or `require`. Anything but `off` needs a build that has the per-user-shard hook. |
| `fixed_shard` | off | Send every session to this one shard, with no routing: G0's mux floor. |
| `relay` | `off` | `tcp:<host:port>`: a pure byte relay to one address, with no parsing at all (G0's floor). |
| `max_sessions` | `100000` | Above this, a new connection gets `53300`. |
| `max_in_flight` | `4096` | Requests in flight per mux connection. |
| `max_message_bytes` | 64 MiB | The largest client message. A larger one ends the connection with `08P01`. |
| `max_batch_bytes` | 16 MiB | Pipelined extended messages without a Sync. A larger batch ends the connection with `54000`. |
| `shard_compression` | `off` | `zstd` or `zstd:LEVEL` (-7 to 22): after Hello each link to a shard asks it to compress (rshard `Compression`, 0x14), and from then on frames of at least `shard_compression_min_bytes` go compressed both ways, each only when that makes it smaller. A shard too old to know the request leaves that link uncompressed (logged). Worth it on links between machines: fan-out rows, partials and RETURNING rows compress well; on one host it costs CPU for nothing. `SHOW GATEWAY` reports it, with `shard_frames_compressed_in`/`_out` and `shard_compression_saved_bytes`. Built with the default `shard_compression` feature. |
| `shard_compression_min_bytes` | 1024 | Frames shorter than this are sent as they are. |
| `gather_max_bytes` | 256 MiB | A fan-out's gathered result. Past it the statement fails with `54000`; it is never truncated. |
| `template_cache_per_core`, `template_cache_shared` | 4096, 65536 | Sizes of the route-template caches. |
| `auth_cache_secs` | `60` | How long role keys fetched from the home shard are cached. |
| `admin` | off | `host:port` for `GET /metrics` (Prometheus text) and a plain-text dump of `SHOW GATEWAY`. |
| `native_listen`, `native_role`, `native_database` | off | The native front end (below). |
| `drain_secs` | `30` | How long a SIGTERM drain waits for open sessions. |
| `shard_timeout_ms` | `0` (none) | A request timeout towards shards. |
| `server_version` | `17.0` | What clients see in `server_version`. |
| `retries` | `3` | How many times a before-effect refusal is retried. |
| `session_pool` | `0` (off) | How many idle shard sessions each core keeps for its next clients (below). |
| `session_pool_idle_secs` | `30` | How long an idle shard session is kept before it is closed. |

### Upgrading without downtime

A new process binds the same port next to the old one (`SO_REUSEPORT`).
When the old process gets SIGTERM it stops accepting connections. Each
session that is idle then closes with `57P01`, and each session inside a
transaction closes when its transaction ends. Whatever is still open after
`drain_secs` is closed.

The only state the gateway keeps on disk is the highest map epoch it has
seen. A gateway restarted with an older map refuses to start.

## How a statement is routed

The gateway tries these in order (B1a.6):

0. **The login's pinned shard** (per-user shards, G11), decided once at
   login. A pinned session is locked to that shard; see "Per-user shards"
   below.
1. **The transaction's shard.** Once a transaction's first statement has
   gone to a shard, every later statement in that transaction goes there
   too.
2. **An explicit route key**, from `SET LOCAL rdb.route_key` (this
   transaction only) or `SET rdb.route_key` (the whole session). The SQL is
   not inspected.
   - A bare integer is an integer key: `SET rdb.route_key = 42`.
   - A quoted value names its family: `'i:42'`, `'u:<uuid>'`, `'t:<text>'`.
   - Any other quoted string is a text key.
   - `RESET rdb.route_key` clears it, and `SHOW rdb.route_key` shows it.
3. **A route template** for the statement's shape:
   - For a prepared statement, the shape is its Parse text. Every Bind is
     routed by hashing the bound parameter the template names, in text or
     binary format.
   - For a simple query, the shape is its text with literals replaced by
     `$1`, `$2`, … (`rasterizeddb_router::literals`). The template names the
     slot that holds the key.
   - Templates are cached on each core, with a shared second level behind
     those. On a miss, the gateway sends the shape to the home shard
     (`Classify`). The shard answers using its own parser, views and range
     analysis, so a statement through a view routes when its key is visible
     after the view is expanded.
   - A template that a shard contradicts with `RDB02` is dropped
     (`route_template_refuted`).
4. **No key:**
   - A shape that needs a key is refused with `0A000 "statement needs a
     route key in cluster mode"`.
   - A shape whose results can be combined exactly is decided by
     `unrouted`: refused, or sent to every shard (below).

### Transactions

- **BEGIN.** The gateway records `BEGIN` (and any `SET LOCAL` that follows
  it) and answers it itself. It sends them to the shard together with the
  transaction's first routed statement.
- **One shard per transaction.** A statement that would reach another shard
  is refused with `0A000`. This is stricter than the plan's READ COMMITTED
  allowance for read-only statements on other shards.
- **No-op transactions.** A `COMMIT` or `ROLLBACK` of a transaction that
  never routed anything is answered by the gateway.

### Session settings

A session-level `SET` (or `RESET`, `DISCARD ALL`) runs on the home shard
and on every virtual session that is already open. The gateway also records
it and replays it on every virtual session that opens later.

### Reusing shard sessions (`session_pool`)

By default a client's sessions on the shards are closed when it
disconnects, and its next connection opens new ones: one round trip and one
login on each shard it reaches. With `session_pool = N`, each core keeps up
to `N` of them instead. This is for clients that connect per request.

- A session is kept only when the client left it between statements and
  outside a transaction. The shard then puts it back as it was when opened
  (`RESET ROLE`, `DISCARD ALL`: settings, prepared statements, temporary
  tables, sequence values). A session that does not reset cleanly is closed.
- It is given only to a client with the same database, role and startup
  parameters, on the same core.
- An idle session is closed after `session_pool_idle_secs`.
- `SHOW GATEWAY` counts the reuses in `virtual_sessions_reused`.

### Prepared statements

- Parse, Bind, Describe, Execute and Close are forwarded unchanged.
- Where a prepared statement's key is, is looked up once and kept with the
  statement. It is looked up again after a schema change, or after a shard
  refused a statement the gateway had routed by a template.
- A statement parsed on one shard is parsed again on another shard the
  first time it routes there. The gateway adds that Parse itself and
  removes its reply.
- All Binds in one batch must route to the same shard. A batch that
  reaches two shards is refused with `0A000`; put a Sync between them.
- Describe goes to the shard the session last used, or to the home shard.

### COPY

`COPY … FROM STDIN` and `COPY … TO STDOUT` work on the shard the statement
routed to.

## Fan-out (`unrouted = fan_out`)

A read with no key, in autocommit, can be sent to every shard. How the
parts are combined depends on the merge the shard asked for:

| Merge | Combined how |
|---|---|
| Concat | Rows in shard order under one RowDescription; row counts summed. |
| CountSum | One `int8` per shard, summed. |
| Any | The rows of the first shard that has any (`EXISTS`, `LIMIT 1`). |
| TopN | A k-way merge by sort keys the shards computed, then `OFFSET`/`LIMIT` applied. |
| Coordinator | Sent to the home shard, which gathers the parts and merges them with the engine's own aggregation code. |

Each shard reads at its own snapshot, so one fan-out does not see a single
consistent cut of the cluster:
- Transactions that commit at the same time on different shards may appear
  in any combination.
- Every transaction is still seen whole, because none spans shards.

Other rules:
- Fan-out is refused inside a transaction block, and for pinned roles.
- Fan-out works for simple queries only. A prepared statement without a key
  is refused.
- A write with no key fans out only if the session sets
  `SET rdb.unrouted_writes = each_shard`:
  - It runs on each shard in turn, each as its own transaction, and the
    row counts are summed.
  - If it fails partway, the error names the shards where it already
    applied.

## Per-user shards

The gateway defines only the hook (`hooks::UserRouting`) and enforces what
it answers. The mapping itself comes from a separately built crate, which
passes its implementation to `rasterizeddb_gateway::run_with`.

What a pinned session gets:
- Every statement goes to the pinned shard. There is no template lookup and
  no hashing.
- `SET rdb.route_key` and `SET rdb.unrouted = fan_out` are refused with
  `42501`.
- Statements are never sent to more than one shard.
- Its virtual sessions open with `pinned = 1`, so the shard checks the role
  against its own placement list.

## Authentication

- **Method.** The gateway checks SCRAM-SHA-256 itself, against the role's
  StoredKey and ServerKey.
- **Keys.** It fetches them from the home shard (`AuthFetch`) and caches
  them for `auth_cache_secs`. It never holds a password.
- **TLS.** Over TLS it offers SCRAM-SHA-256-PLUS with
  `tls-server-end-point` channel binding.
- **Unknown roles.** The exchange runs to the end with a made-up salt, so an
  unknown role and a wrong password look the same to the client (`28P01`).
- **Shard sessions.** Sessions on shards are opened as the authenticated
  role, and the shards check grants again.

## Refusals and errors

| SQLSTATE | When |
|---|---|
| `0A000` | No route key, or one statement or batch would reach two shards. Also features not available through the gateway: LISTEN/NOTIFY, the function-call protocol, replication connections. |
| `42501` | A pinned role tried to change its routing. |
| `08006` | A shard is down, or the statement could not be classified because the home shard is down. Nothing was sent. |
| `08007` | The connection to a shard was lost after the statement was sent, so its outcome is unknown. It is not retried. Inside a transaction the client connection is closed too. |
| `57P03` | A range kept being refused while it moved, past `retries`. |
| `54000` | A fan-out result passed `gather_max_bytes`, or a batch passed `max_batch_bytes`. |
| `53300` | `max_sessions` reached. |
| `57P01` | The gateway is shutting down. |

These shard refusals are retried without the client seeing them, at most
`retries` times, and only in autocommit or on a transaction's first
statement:
- `RDB02` (the range is not owned there): the map is refreshed first.
- `RDB03` (the range moved): the map is refreshed first.
- `57P03` (fenced): the retry waits briefly first.
- A shard that lost the virtual session: a new session is opened.

## The native front end

`native_listen` accepts native-protocol (rastcp) clients. A request is
routed by the `route=` option among its `\x01SIMPLE:` options:
- `route=i<decimal>` for an integer key;
- `route=u<32 hex>` for a UUID key;
- `route=t<hex of the UTF-8>` for a text key.

A request without the option goes to the home shard.

Limits in this version:
- `auth = trust` only, running as `native_role` in `native_database`.
- Every request is its own autocommit statement.
- Native transactions still go directly to a shard.

## Observability

`SHOW GATEWAY` (through any client connection) and `GET /metrics` on
`admin` report:
- the map's epoch and digest;
- the cluster schema epoch;
- sessions;
- statements routed and fanned out;
- template hits, misses and refutations;
- refusals by kind, retries, `08006` and `08007` counts;
- bytes in and out;
- hop latency p50 and p99.

Each core counts with plain integers and publishes about once a second, so
the numbers can be up to a second old.

## The protocol to the shards

The gateway talks to shards with `rshard`, a request/response protocol over
rastcp/2's multiplexed connections. Its messages, byte encodings and
semantics are documented in `rasterizeddb_router/src/rshard.rs`, which is
the reference for the shard side (`core/shard/rshard.rs`, `backend.rs`,
`classify.rs`).

Results come back in chunks, never splitting a pgwire message. The gateway
asks for the next chunk while it writes the previous one to the client, and
for no further one until that write is done. So a client that stops reading
holds up its own session and nothing else, and the gateway holds at most
one chunk it has not taken (R3).
