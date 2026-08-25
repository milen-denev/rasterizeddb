# Multi-Primary Replication — Design & Implementation

Status: **implemented**, off by default (`rasterizeddb_core::replication`).
Transport: `rastcp` (TLS 1.3). Target: `rasterizeddb_core`, edition 2024.

## 0. What exists

| Component | State |
|---|---|
| `replication::protocol` — frames, records, negotiation | **Done.** 12-byte header, little-endian, bounds-checked decode |
| Compressed and uncompressed exchange | **Done.** Zstd behind `replication_compression` (on by default), negotiated per link |
| `replication::hlc` — hybrid logical clock | **Done.** 48-bit ms + 16-bit counter, monotonic under clock skew |
| `replication::log` — catch-up history, applied positions | **Done.** CRC-framed, torn-tail recovery, size-bounded compaction |
| `replication::manager` — capture hooks, commit path | **Done.** One relaxed load when replication is off |
| `replication::peer` — outbound link | **Done.** Batching, linger, adaptive compression, reconnect, catch-up |
| `replication::server` — inbound listener, apply path | **Done.** Own port, own `rastcp` listener |
| Peer authentication | **Done.** Mutual certificate pinning or a cluster CA. See §4.3 |
| `replication::sql` — `REPLICATION …` / `SHOW REPLICATION …` | **Done.** See §7 |
| CLI flags | **Done.** See §6 |
| Loop prevention in a mesh | **Done.** See §3.2 |
| Conflict policies | **Done.** `abort` (default), `skip`, `last-writer-wins`. See §5 |
| Tests | 146 (unit, end-to-end over real TLS sockets, and 8 authentication tests in `rastcp`) |

**Known limits** are in §9, and they are not small: this is eventually-consistent
multi-primary replication, not consensus.

## 1. The shape

Every node accepts reads and writes. A transaction committed on one is shipped to
the others and applied there. There is no primary to fail over from, and no
promotion step when one goes away.

```text
        ┌──────────┐   commits, batched + compressed   ┌──────────┐
        │  node 1  │ ───────────────────────────────▶  │  node 2  │
        │ (primary)│ ◀───────────────────────────────  │ (primary)│
        └──────────┘                                   └──────────┘
              ▲   ╲                                     ╱   ▲
              │    ╲            ┌──────────┐           ╱    │
           clients   ────────▶  │  node 3  │  ◀───────      clients
                                │ (primary)│
                                └──────────┘
```

The intended topology is a full mesh: every node configured with every other, so
each commit reaches all of them first-hand. Relaying — forwarding what arrived
from one peer to the others — exists (`--replication-relay`) for chains and hubs,
and is off by default because in a mesh it sends every transaction *N* times over.

## 2. What travels, and why it is statements

A record is one committed transaction:

| Field | Meaning |
|---|---|
| `origin` | the node that took the write |
| `seq` | its position in that node's stream — contiguous, gap-free |
| `commit_hlc` | when it committed, on the cluster's hybrid clock |
| `statements` | the SQL that ran, in order |
| `tables` | what it wrote, for the conflict comparison |

`(origin, seq)` is the global identifier and the deduplication key.

**Why not ship pages or WAL records.** The engine's write-ahead log records byte
ranges of specific files. Two nodes that both accept writes lay their files out
differently — different insertion orders, different free-list decisions,
different compaction timing — so the same physical record means different things
on either side. There is no byte-level correspondence to ship. Statements are the
level at which two nodes agree about what happened, and applying one is a call
the executor already offers (`execute_in_txn`).

**What that costs** is determinism, and §9.1 is about the cost.

## 3. The write path

### 3.1 Capture

Two hooks, both behind one relaxed atomic load:

- `note_statement(tx, sql, table)` — called at the point a write is *known to
  have succeeded*, not when it parsed. A statement that failed is never
  replicated.
- `commit_record(tx)` / `publish(record)` — called from
  `Database::commit_transaction`, which is what the autocommit path and pgwire's
  `COMMIT` both go through.

The buffer is keyed by `TxId`, so a `BEGIN` block accumulating statements over
several round trips ships as one record and a replica cannot apply half of it.

**Savepoints make a block several ids.** A write inside a `SAVEPOINT` carries the
sub-transaction's id, not the block's, so publishing on the outer id alone would
ship what was written outside the savepoints and silently drop what was written
inside — a replica holding a subset of a transaction that committed whole, with
nothing reporting a problem. `Database::commit_transaction` therefore returns the
whole family, and `publish_transaction` consumes it:

- the family is read **at commit**, because a rolled-back savepoint already
  resolves as aborted while its parent runs, and asking afterwards would depend
  on the transaction manager keeping bookkeeping it is not obliged to keep;
- a sub-transaction that was rolled back to contributes nothing, but its buffer
  is still drained;
- statements carry a database-wide order number and are re-sorted, because two
  sub-transactions interleave and concatenating them by id would put an `UPDATE`
  before the `INSERT` it depends on.

Two details that matter for correctness:

- **The statement recorded is the one that ran.** Where the engine rewrites a
  statement — a JSON document decomposed into its shadow table — capture happens
  at the leaf, after the rewrite, so a generated UUID travels as a literal
  instead of being generated afresh on each node.
- **Foreign-key cascades are not captured** (`depth == 0` only). A replica
  running the parent statement triggers the same cascades itself; shipping both
  would apply them twice.

### 3.2 Loop prevention

The failure mode a mesh has to rule out is each node re-originating what it was
sent, so two nodes bounce a transaction forever under ever-changing sequence
numbers that deduplication cannot recognise.

`enter_apply(tx, origin)` marks a transaction as applying a peer's record;
`note_statement` sees the mark and captures nothing. This is asserted directly —
`a_replicated_transaction_is_not_echoed_back` writes on one node and checks the
other's local sequence has not moved.

### 3.3 Publish

Asynchronous by default: the commit path is a channel send per peer. The queue is
bounded and a full one *drops* rather than blocking — the record is already in
the replication log, and the drop raises a catch-up flag the sender acts on. The
alternative, blocking a commit until a dead peer's queue drains, trades bounded
catch-up work for unbounded client latency.

**Nothing is published before it is durable.** The autocommit path commits,
flushes the outcome record, and reports failure — rolling back — if that flush
did not work. Publishing inside the commit put the transaction on the wire before
that check, so a flush failure left peers holding a write this node had disowned,
with no way to recall it. The local commit and the publish are therefore separate
steps, and the sequence number is assigned by the publish: a number burned on a
transaction that then failed leaves a hole in this node's stream that peers would
wait forever to have filled.

Under `--replication-mode sync|quorum:N|all` the commit additionally waits for
acknowledgements, bounded by `ack_timeout`. The local commit has already
happened; the timeout bounds how long a slow peer can hold a client's write open.

An acknowledgement counts only when the peer reported the record as *applied* or
*already held*. One it refused — a conflict, a failed statement, a gap — does not
count, so a synchronous commit cannot report a durability the cluster does not
have; the waiter times out instead and the timeout is counted in the status view.

## 4. The wire

```text
+--------+--------+--------+--------+
| 'R' 'R'| ver    | kind   | codec  |
+--------+--------+--------+--------+
| flags  |    reserved     |
+--------+-----------------+
|            raw_len       |   payload length *before* compression
+--------------------------+
|         payload ...      |
+--------------------------+
```

`raw_len` in the header lets the receiver size its decompression target exactly
once and reject an implausible expansion before spending memory on it. Every
length field is checked against what is actually in the buffer before it is used
to reserve anything, so a malformed frame is an error and never a panic in an
accept loop.

`rastcp` supplies TLS and length-prefixed framing, so this layer does not
concern itself with either.

### 4.1 Compression

Negotiated once per connection: the caller advertises a mask of codecs it can
decode, the answerer picks from the intersection and says which. A build without
`replication_compression` advertises only `none` and interoperates fine — the
link falls back to uncompressed rather than failing.

Three modes:

| Mode | Behaviour |
|---|---|
| `off` | never; right on a loopback or a dedicated link where CPU is scarcer than bandwidth |
| `always` | above the size floor (default 512 bytes) |
| `auto` (default) | as `always`, but stops while the observed ratio shows it is not paying for itself |

A compressor context is kept per link rather than per frame — zstd's context owns
its window and hash tables, and rebuilding one per batch would put an allocation
and a table clear on every send.

Compression that *expands* a payload is discarded and the original bytes are
sent, so a frame is never larger for having tried.

### 4.2 Batching

The sender coalesces whatever has accumulated into one frame, up to a record and
byte limit, waiting at most `batch_linger` (default 500 µs) for more. This is
where the throughput comes from: one TLS record, one round trip and one
compression pass amortised over a batch rather than paid per commit. At low load
the batch is one record and latency is not traded for throughput nobody asked
for.

Records are sorted by `(origin, seq)` within a batch. Sequence numbers are
assigned at commit and queued just after, so two transactions committing
concurrently can reach the queue in the other order; the receiver would refuse
the inversion as a gap and the sender would backfill it, which converges but
costs a round trip per inversion. Measured on six concurrent writers, sorting
took gaps from 53 to 0 and batches from 73 to 61.

### 4.3 Measured

360 rows inserted by six concurrent writers, two nodes, `always:zstd:1`,
20 ms linger:

```text
captured 361, published 361, applied 361, duplicates 0
conflicts 0, failures 0, gaps 0, ack timeouts 0
61 batch(es), 60 compressed: 52439 bytes of payload went out as 12331 (76.5% smaller)
```

Roughly six transactions per batch and **76.5%** off the wire. Repetitive SQL is
what zstd is good at, and replication traffic is nothing but.

With one statement at a time — a single client, serial round trips — batches are
one record each, fall under the 512-byte floor, and go uncompressed. That is the
floor working: compressing 150 bytes costs a call and a header and saves nothing
a TLS record notices. The status view says `none compressed` rather than
reporting a negative saving.

### 4.4 Authentication

TLS gives a private channel for free and gives *identity* only when someone
configures it. Without that second half the link is encrypted against a passive
eavesdropper and open to an active one: an attacker in the network path presents
any certificate, is believed, and from then on reads and rewrites everything —
including the shared token, which travels inside the tunnel and so protects
nothing against an attacker already inside it.

`--replication-auth` picks one of three:

| Mode | What it proves | Setup |
|---|---|---|
| `insecure` | nothing | none; refused on any non-loopback listen address |
| `pinned` | each node holds the other nodes' exact certificates | one self-signed certificate per node, fingerprints distributed |
| `ca` | each node holds a certificate the cluster authority signed | one CA, a signed certificate per node |

Both authenticated modes are **mutual**: a client verifies the server it reached
*and* presents a certificate the server verifies. Half of that is not much use —
verifying the server stops you talking to an impostor, and requiring a client
certificate stops an impostor talking to you.

The token (`--replication-token`) stays available and is now what it should
always have been: a second, cheap check that catches a misconfigured node, rather
than the only thing standing between the cluster and the network.

#### Setting up pinned mode

Once per node:

```text
$ rasterizeddb_core --replication-generate-identity /data --replication-addr 10.0.0.1
wrote /data/replication-cert.pem
wrote /data/replication-key.pem (mode 0600)

fingerprint: f954bd37c0086d024b2dd4c790adc0e502b634f62c12efc324a9fbb0ab4ece98
```

Then start each node with its own certificate and every other node's
fingerprint:

```bash
rasterizeddb_core --location /data --replication --node-id 1 \
    --replication-auth pinned \
    --replication-cert /data/replication-cert.pem \
    --replication-key  /data/replication-key.pem \
    --replication-peer-fingerprint <node 2's fingerprint> \
    --replication-peer-fingerprint <node 3's fingerprint> \
    --peer 'two@10.0.0.2:61171' --peer 'three@10.0.0.3:61171'
```

The fingerprint is SHA-256 over the certificate's DER, which is the same value
`openssl x509 -fingerprint -sha256` prints, so it can be checked without this
tool. Each node also logs its own at start-up.

A wrong pin fails the handshake and says so:

```text
last error: could not connect to 10.0.0.2:61171: the server presented certificate
f954bd37…ce98, which is not among the pinned ones
```

Naming `--replication-cert` without `--replication-auth` selects `pinned`, and
naming `--replication-ca` selects `ca` — an operator who went to the trouble of
supplying certificates gets them used rather than silently running
unauthenticated.

#### Why `insecure` is refused off loopback

It is the one configuration that looks safe and is not: the traffic is
encrypted, so nothing about it appears wrong, and anyone who can intercept the
address can impersonate a peer and write to every node. `validate()` rejects the
combination outright rather than warning about it.


## 5. The apply path

Per record, in order:

1. **Own record** (origin is this node) → `Duplicate`, ignore.
2. **Already applied** (`seq <= high_water`) → `Duplicate`.
3. **Gap** (`seq > high_water + 1`, and a position exists) → refuse. A record
   cannot be applied over a hole; the statements before it may be what make it
   valid. The sender backfills from its log on seeing this.
4. **Fresh origin** (`high_water == 0`, `seq > 1`) → adopt the stream where it
   starts, and say loudly that earlier history is not present. A node joining a
   cluster whose early history is past retention would otherwise stall forever on
   a gap it cannot close. Seed such a node from a backup.
5. **Superseded** under `last-writer-wins` → `Conflict`, advance past it.
6. Otherwise: begin a transaction, mark it as an apply, run the statements,
   commit, record the position, stamp the tables.

### 5.1 Conflict policies

Two nodes writing the same key at the same time is what this topology *permits*,
so one of the two arrivals will not apply cleanly.

| Policy | Behaviour |
|---|---|
| `abort` (default) | abort the transaction and **stall that origin's stream**, loudly |
| `skip` | skip the failing statement, apply the rest, move on |
| `last-writer-wins` | drop records a strictly later local write supersedes; skip failures |

`abort` deliberately does not advance the applied position. The stream stops and
says so — a visible stall an operator can act on, rather than a transaction
silently skipped and quiet divergence afterwards. PostgreSQL's logical
replication makes the same choice.

`last-writer-wins` compares hybrid timestamps **per table, not per row**: the
engine stores no per-row commit stamp, so the finest granularity available is
"when did this node last commit anything to this table". A record older than the
local write to *every* table it touches is dropped; one that is newer for any of
them still carries information and applies. Ties are broken by node id so both
ends of a link reach the same verdict. Schema changes are never dropped this way
— a `CREATE TABLE` discarded as superseded would leave everything after it with
nowhere to go.

### 5.2 Delivery is at-least-once

The applied position is recorded *after* the transaction commits. A crash in that
window replays one transaction on restart rather than losing it. Replaying re-runs
statements that already landed, which the conflict policy then sees — usually as
a duplicate key. Losing it instead would be silent divergence, which is worse and
undetectable.

## 6. Configuration — CLI

```bash
# node 1
rasterizeddb_core --location /data --replication --node-id 1 \
    --peer 'two@10.0.0.2:61171' --peer 'three@10.0.0.3:61171'

# node 2
rasterizeddb_core --location /data --replication --node-id 2 \
    --peer 'one@10.0.0.1:61171' --peer 'three@10.0.0.3:61171'
```

| Flag | Default |
|---|---|
| `--replication` | off |
| `--node-id <N>` | unset; required, and must be unique and non-zero |
| `--peer <name@host:port>` | none; repeatable |
| `--replication-addr <IP>` | `127.0.0.1` |
| `--replication-port <PORT>` | SQL port + 1 (`61171`) |
| `--replication-cluster <NAME>` | `rasterizeddb` |
| `--replication-token <SECRET>` | unset |
| `--replication-compression <SPEC>` | `auto:zstd:1` |
| `--replication-mode <MODE>` | `async` |
| `--replication-ack-timeout-ms <MS>` | 5000 |
| `--replication-conflict <POLICY>` | `abort` |
| `--replication-batch-records <N>` | 256 |
| `--replication-batch-bytes <N>` | 1 MiB |
| `--replication-linger-us <US>` | 500 |
| `--replication-queue <N>` | 8192 |
| `--replication-relay` | off |
| `--replication-retention-mb <N>` | 256 |
| `--replication-auth <MODE>` | `insecure`, or inferred from the certificate flags |
| `--replication-cert <PATH>` / `--replication-key <PATH>` | unset |
| `--replication-ca <PATH>` | unset |
| `--replication-peer-fingerprint <HEX>` | none; repeatable |
| `--replication-generate-identity <DIR>` | writes a certificate pair, prints its fingerprint, exits |

The replication listener is a separate port from the SQL one, deliberately: a
deployment usually exposes SQL to applications and replication only to the other
nodes, and that distinction is a firewall rule, which needs a port.

## 7. Configuration — SQL

```sql
REPLICATION ENABLE
REPLICATION DISABLE

REPLICATION ADD PEER 'berlin' ADDRESS '10.0.0.2:61171'
REPLICATION ADD PEER 'tokyo'  ADDRESS '10.0.1.7:61171' WITH (COMPRESSION = 'zstd:3')
REPLICATION DROP PEER 'berlin'
REPLICATION PAUSE PEER 'berlin'
REPLICATION RESUME PEER 'berlin'
REPLICATION RESYNC PEER 'berlin' FROM 0

REPLICATION SET compression = 'auto:zstd:1'
REPLICATION SET mode = 'quorum:1'
REPLICATION SET conflict = 'last-writer-wins'
REPLICATION SET batch_linger_us = 500

SHOW REPLICATION            -- summary and a line per peer
SHOW REPLICATION STATUS     -- counters
SHOW REPLICATION PEERS
SHOW REPLICATION SETTINGS
SHOW REPLICATION KEYS       -- what SET accepts
```

Runtime changes are **not persisted**. A peer added with a statement is gone
after a restart unless the flag is added too. That is deliberate: what a node
comes up as should always be what its command line says.

`node_id` cannot change while replication is running — moving a node's identity
under a live cluster makes its stream ambiguous. The token is never echoed by
`SHOW`; it reports `(set)` or `(unset)`.

Example:

```text
$ psql -c "SHOW REPLICATION STATUS"
 node 1 of cluster 'rasterizeddb' — enabled
 listening on 127.0.0.1:7441, mode async, conflict abort, compression auto:zstd:1
 local sequence 361, clock 1787234791376.1, 1 peer(s), 0 open capture(s)
 captured 361, published 361, applied 361, duplicates 0
 conflicts 0, failures 0, gaps 0, ack timeouts 0
 61 batch(es), 60 compressed: 52439 bytes of payload went out as 12331 (76.5% smaller)
 replication log holds 56527 bytes, retention 256 MiB
 peer             address                  node   state      acked    lag      queued   codec
 two              127.0.0.1:7442           2      up         361      0        0        zstd
```

## 8. The two files

Beside the data, and neither is a source of truth for user data — the WAL and the
transaction status log remain that.

- **`db_replication.log`** — this node's committed history, so a peer that was
  down can ask for what it missed. CRC-framed records; a torn tail from a crash
  is truncated and everything before it survives; a bad checksum stops recovery
  rather than being trusted. Bounded by `--replication-retention-mb`, compacted
  by rewriting the newest half under a temp file and a rename. A peer that falls
  further behind than retention is told to resynchronise rather than served a
  gap.
- **`db_replication.state`** — how far this node has applied each origin's
  stream. Append-only `(origin, seq)` pairs, recovered as the maximum per origin,
  rewritten when it grows. A torn tail costs at most the most recent advance.

An **in-memory** database has neither file, and its log is the in-memory window
alone. It reports that window as its retention boundary, so a peer that falls
past it is told to resynchronise — the same answer a file-backed node gives when
retention has dropped what the peer wants, rather than a silently short reply.

Both files are **buffered, not flushed per commit**, with a one-second flusher and a
flush at shutdown. A commit's own durability is already paid for by the WAL;
adding a second `fsync` to the commit path to protect a catch-up cache would be a
poor trade. A crash that loses the buffered tail costs a peer its incremental
catch-up, and it resynchronises instead.

## 9. Limits

### 9.1 Statements must be deterministic

`NOW()`, `RANDOM()`, `CURRENT_TIMESTAMP` and friends are re-evaluated on each
node and produce different rows there. The capture path warns once per marker per
process. It warns rather than refuses: `NOW()` in a `WHERE` clause is usually
harmless, and rejecting the statement would be a worse answer than replicating it
and saying why it might drift. Compute the value in the client and send a
literal.

Values the *engine* generates internally are not affected — see §3.1.

### 9.2 What authentication does not cover

`pinned` and `ca` establish that the peer holds a certificate this cluster
accepts. They do not express *authorisation* beyond that: every authenticated
node is a full peer and may write anything. There are no per-node privileges,
and revoking a node means removing its fingerprint (or revoking its certificate)
and restarting the nodes that trusted it — there is no CRL or OCSP check.

### 9.3 This is not consensus

No leader election, no quorum reads, no split-brain protection. Two partitioned
halves both keep accepting writes, and the conflict policy decides what happens
when they meet. If you need a single serialisable history, this is the wrong
shape.

### 9.4 Ordering is per origin

Each node's stream is applied in the order that node committed it. Two nodes'
streams are ordered against each other only by hybrid timestamp — enough to order
causally related commits, not enough to serialise concurrent ones. That is what
multi-primary means.

### 9.5 A new node needs a base copy

Replication ships what happens from now on, not what happened before. A node
joining a cluster that already holds data must be seeded from a backup first;
otherwise it adopts the stream at whatever point it finds it (§5, step 4) and is
missing everything earlier.

### 9.6 Conflict granularity

`last-writer-wins` resolves per table, not per row (§5.1). Row-level resolution
would need a per-row commit stamp, which the storage format does not carry.
