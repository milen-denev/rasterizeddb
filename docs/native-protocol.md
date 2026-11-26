# The native protocol

What a client of the native listener (`--rdb-addr`/`--rdb-port`, default 61170, or `--rdb-socket`) puts
on the wire. Two clients are written against this page: [`rasterizeddb_client`](../rasterizeddb_client/README.md)
(Rust) and [`RasterizedDb.Data`](../dotnet/README.md) (.NET). The transport underneath is
[rastcp](../rastcp/README.md); the sources named in each section are the authority.

## Transport

| | TCP | unix socket |
|---|---|---|
| encryption | TLS 1.3 | none (the socket file's permissions decide who connects) |
| protocol name | ALPN `rastcp/1` or `rastcp/2` | the same eight bytes, sent first |
| channel binding | RFC 9266 `tls-exporter`: 32 bytes, label `EXPORTER-Channel-Binding`, empty context | the 32 bytes `rastcp unix socket: no exporter.` |

**`rastcp/1`** — a frame is `[u32 length][payload]`, little endian. Requests are answered one at a
time, in order. The connection can hold a transaction block, temporary tables and sequence state.

**`rastcp/2`** — a frame is `[u32 length][u32 id][u8 kind][payload]`; the length counts the payload
only. The client chooses `id`, the answer returns it, and answers come in any order. Kind 0 is a
request or its answer, 1 "the handler timed out", 2 "the handler failed" (the payload is a message).
Transaction control is refused on it (`0A000`): its requests come from unrelated callers.

The server never sends a frame nobody asked for. It closes a connection that has been quiet for its
request timeout (30 s).

## Login

Runs once per connection, before any request, in plain `[u32 length][payload]` frames on either
protocol (at most 64 KiB each). A listener under `trust` runs no exchange: the client sends nothing.

```
client: [0x01][version][u16 user length][user]            version 1
        [0x01][2][u16 user][user][u16 database][database]  version 2: names the database
server: [0x00]            let in (peer login)
        [0x01]            a password exchange follows
        [0xFF][message]   refused
```

A password login is SCRAM-SHA-256 with `p=tls-exporter` and the channel binding above:

```
client: [0x02][client-first]      p=tls-exporter,,n=<user>,r=<nonce>
server: [0x02][server-first]      r=<nonce>,s=<salt>,i=<iterations>
client: [0x03][client-final]      c=<base64(gs2 header + binding)>,r=<nonce>,p=<proof>
server: [0x00][server-final]      v=<server signature>, which the client must check
```

## A statement and its answer

A request is a statement's UTF-8 text: one statement (a second, after a `;`, is refused whole). There
is no Bind message, so values are written into the text as literals by the client.

An answer's first byte says what follows:

| byte | |
|---|---|
| 0 | done; optionally a `u64` count |
| 1 | a `u64` count of rows affected |
| 2 | rows, back to back |
| 3 | an error: its message, ending in ` (SQLSTATE xxxxx)` when the server knew a code |
| 4 | PostgreSQL backend messages (the simple-query form, below) |
| 5 | notices, then one of 0 to 3 (only on a connection that asked for them) |

A row is `[u32 payload length]`, then per column `[u64 size][u64 schema id][data][u8 type tag]`.
There are **no column names**: column *i* is item *i* of the select list. Tags and their data, all
little endian unless said: 2/3/4 `smallint`/`integer`/`bigint`; 9 unsigned 64-bit; 5 128-bit; 11/12
`real`/`double precision`; 14 text; 16 `boolean`; 17 `uuid` (16 bytes, network order); 19 `numeric`
(`i128` unscaled value, then one byte of scale); 20 `bytea`; 21 an array (PostgreSQL's binary array
framing around text elements); 23 `jsonb` and 47 `json` as JSON text; 15 `timestamptz` and 46
`timestamp` (`i64` microseconds since the Unix epoch); 29 `date` (big-endian `i32` days since
2000-01-01); 30 `time` (big-endian `i64` microseconds); 24 `interval` (big-endian microseconds, days,
months); 253 NULL (trust the tag: the data may not be empty).

## Marked requests

A request whose first byte is `0x01` is not a statement. Each was chosen so that a server without it
refuses it and runs nothing: an older server reads the `;` as two statements (answer 3), a newer one
that lacks the request answers `0A000` in the simple-query form. A client treats either as "not
supported" and carries on as before.

| request | answer | |
|---|---|---|
| `\x01DATABASE;<name>` | simple-query form | Selects the database of a connection that ran no login exchange. Only before the connection's first statement. |
| `\x01ROLE;<role>[\x1f<database>]` | simple-query form | Names the role a trust connection runs as (and optionally its database). Only before the first statement. |
| `\x01NOTICES;` | simple-query form | From here on, an answer that has notices carries them (answer 5). |
| `\x01EXEC;…` | as a statement | A statement's text and its values apart (below). |
| `\x01SIMPLE[:options];<statements>` | answer 4 | The simple-query form (below). |
| `\x01CANCEL;<token>` | simple-query form, tag `CANCEL 1` or `CANCEL 0` | Stops the statement a simple query sent with `cancel=<token>` is running, for the same user. |
| `\x01DESCRIBE;<statement>` | simple-query form | Names a statement's result columns without running it (below). |

Setup requests (`DATABASE` or `ROLE`, then `NOTICES`) go out after the login and before anything else,
on every connection a client opens, reconnects included. On `rastcp/2` they are ordinary frames with id 0.

### `\x01EXEC;` — text and values apart

```
u32 sql length, sql (UTF-8, with its $n placeholders)
u16 literal count, per literal: u32 length, bytes     parameter n is literal n-1, as the SQL literal
                                                       the client would have written; empty = unused
u16 span count, per span: u32 start, u32 end, u16 literal     where each placeholder sits in the sql
```

Spans are ascending, disjoint, on character boundaries, each naming a non-empty literal. The server
splices the literals in, so what runs is the text the client would have sent; what it gains is the
statement's shape, ready made, to answer from a parse it has learned. A client asks once whether the
server knows the form (an `EXEC` of `SELECT 1` answers 2) and otherwise sends text. A statement that
would render past 8 MiB is sent as text.

### `\x01SIMPLE;` — the simple-query form

The statements run as a PostgreSQL client's simple query does (several are one implicit transaction;
`WITH` and derived tables work) and the answer is byte 4 followed by the messages pgwire would send:
`T` RowDescription (names, type OIDs), `D` DataRow (every cell as text), `C` CommandComplete, `E`
ErrorResponse, `N` NoticeResponse, and a final `Z` ReadyForQuery whose status byte is `I`, `T` (a
block is open) or `E` (a failed block). Options go between `SIMPLE` and `;`, comma separated:
`cancel=<token>` (1 to 64 of `[A-Za-z0-9_-]`, unique per running query) and `rows=<n>` (at most *n*
rows of each result are sent; the command tag still counts them all). An option the server does not
know refuses the request.

On `rastcp/1` a block opened here, or by a native `BEGIN`, lasts until it is ended: native statements
and simple queries on the connection all run inside it. On `rastcp/2` a block must end in the request
that began it.

### `\x01DESCRIBE;` — column names for a native answer

Native rows carry no names, so a client that needs them (reading by name, a schema table) asks:

```
request:  \x01DESCRIBE;SELECT id, "Label", price AS cost FROM t WHERE id = $1
answer:   [4] T(RowDescription: id, Label, cost with their type OIDs)  Z(status)
```

The statement is sent with its `$n` placeholders as written; no values are needed. The answer is the
RowDescription pgwire sends when the same statement is prepared and described, followed by
ReadyForQuery with the connection's transaction status, and no rows. A statement that returns no rows
answers `C` with the tag `DESCRIBE` and no description, and so may one the server cannot describe (a
table that does not exist): its error is the statement's own, when it is run. A write with `RETURNING`
is described by the columns it returns.

Nothing runs: a write with `RETURNING` is as safe to describe as a `SELECT`, and an open block, failed
or not, is neither read nor changed. A client describes a statement once and keeps the names by
statement text. Against a server without the request (see the fallbacks above) there are no names,
which is what such a client had before.
