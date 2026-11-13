# Multi-Primary Replication — Design & Implementation

Status: **implemented**, off by default (`rasterizeddb_core::replication`).
Transport: `rastcp` (TLS 1.3). Target: `rasterizeddb_core`, edition 2024.

## 0. What exists

| Component | State |
|---|---|
| `replication::protocol` — frames, records, negotiation | **Done.** 12-byte header, little-endian, bounds-checked decode |
| Compressed and uncompressed exchange | **Done.** Zstd behind `replication_compression` (on by default), negotiated per link |
| `replication::hlc` — hybrid logical clock | **Done.** 48-bit ms + 16-bit counter, monotonic under clock skew |
| `replication::log` — this node's stream, applied positions | **Done.** Written ahead of the commit through the database's write-ahead log; CRC-framed; size-bounded |
| `replication::manager` — capture hooks, commit path | **Done.** One relaxed load when replication is off |
| `replication::peer` — outbound link | **Done.** Reads the log from where the peer is; batching, linger, adaptive compression, reconnect |
| `replication::server` — inbound listener, apply path | **Done.** Own port, own `rastcp` listener; one apply at a time per origin, one sync per batch, each transaction once |
| Peer authentication | **Done.** Mutual certificate pinning or a cluster CA. See §4.4 |
| `replication::sql` — `REPLICATION …` / `SHOW REPLICATION …` | **Done.** See §7 |
| CLI flags | **Done.** See §6 |
| Loop prevention in a mesh | **Done.** See §3.2 |
| Conflict policies | **Done.** `abort` (default), `skip`, `last-writer-wins`. See §5 |
| Tests | 170 in the library (unit, end-to-end over real TLS sockets, power-cut images of both sides), and 8 authentication tests in `rastcp` |

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
| `seq` | its position in that node's stream — contiguous, gap-free, in commit order |
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
- `prepare_family(…)`, then `publish_prepared(tx)` — called from
  `Database::commit_transaction` and `Database::publish_transaction`, which is
  what the autocommit path and pgwire's `COMMIT` both go through. §3.3 says
  why there are two.

The buffer is keyed by `TxId`, so a `BEGIN` block accumulating statements over
several round trips ships as one record and a replica cannot apply half of it.

**Savepoints make a block several ids.** A write inside a `SAVEPOINT` carries the
sub-transaction's id, not the block's, so publishing on the outer id alone would
ship what was written outside the savepoints and silently drop what was written
inside — a replica holding a subset of a transaction that committed whole, with
nothing reporting a problem. `Database::commit_transaction` therefore gathers the
whole family:

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

### 3.3 Record first, commit, then release

A transaction's record is **written before the transaction commits**, and
**released to the peers only once that commit is durable**. Three failures
this order rules out, all of which happened or could be made to:

- **A committed transaction no peer ever hears of.** The record used to be
  built after the commit, from a buffer in memory, and appended to a file that
  was flushed once a second. A crash between the commit and that flush left a
  transaction that was durable here and unknown everywhere else, with both
  nodes reporting themselves in step. Now the record is in the replication log
  and described to the database's write-ahead log *before* the outcome is, and
  the commit's one sync covers both, record first. Whatever a crash leaves,
  it leaves the record without the commit (the record is voided at the next
  start) or both.
- **A transaction the peers hold and this node disowned.** Nothing is sent
  before it is durable here: the record, and the outcome it depends on. After
  a commit that waited for its sync that is immediate; after one that did not
  (`synchronous_commit = off`) it is the log writer's next round.
- **Two dependent transactions sent in the wrong order.** The sequence number
  is taken while the transaction still holds its row locks. A transaction that
  waited for one of those rows can only be numbered afterwards. Numbered after
  the locks were released, as it was, the waiter could be numbered first, and
  a peer ran the `UPDATE` before the `INSERT` it depended on: no rows matched,
  no error, different data.

A record whose transaction turned out not to commit (its sync failed, or the
node stopped first) keeps its sequence number and carries nothing: a *void*.
A peer applies it by moving past it, so a stream never has a hole for anyone
to wait on.

The cost on the commit path is one positional write and one write-ahead log
record, under a short lock, and no second `fdatasync`. A record that cannot be
written fails the commit, before anything is committed.

A statement that changed no row here (an `UPDATE` matching nothing, a schema
change) has no durable outcome to tie its record to. A schema change's record
is synced on its own account before the statement returns; a row write that
changed nothing is not, since a crash loses nothing a peer could be missing.

Under `--replication-mode sync|quorum:N|all` the commit additionally waits
until that many peers report the transaction applied, bounded by
`ack_timeout`. The local commit has already happened; the timeout bounds how
long a slow peer can hold a client's write open. A peer that refuses the
transaction has not applied it, so the wait times out and the timeout is
counted in the status view.

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

### 4.2 The sender reads the log

Each peer's sender reads this node's log from where *the peer says it is*, up
to the release mark of §3.3, and sends what it finds in one frame: up to a
record and byte limit, after waiting at most `batch_linger` (default 500 µs)
for more. One TLS record, one round trip and one compression pass are
amortised over a batch, and at low load the batch is one record.

There is no queue between the commit path and a peer. There used to be a
bounded one, with the log as its overflow, and the two paths disagreed: the
queue delivered in arrival order, an overflow dropped records and fell back to
the file, and a relayed record that was dropped was never caught up at all.
The log is in sequence order and holds everything; its newest few thousand
records are served from memory.

Every handshake and every acknowledgement carries the peer's own account of
how far it has applied each origin, and the next read starts there. So a
refused record is offered again and never passed over, and a peer that was
rewound is followed back.

A link is in one of three states, shown by `SHOW REPLICATION`:

| State | Meaning |
|---|---|
| `up` | the peer is taking what it is sent |
| `stalled` | the peer refuses the next transaction; it is offered again, less and less often, and the stream picks up by itself once the cause is gone |
| `needs reseed` | the peer needs history this node no longer holds; **nothing** is sent, because sending the rest would skip the missing part |

One transaction is one record and travels in one frame, up to 64 MiB. A
transaction whose statements could not fit is refused at commit.

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

Records of one origin are applied **one at a time, in order, under that
origin's lock**: the same record arriving on two connections (a link that
reconnected while its old request was still being served, a resync during a
push) is applied by one and reported as already held by the other. A batch
runs in a task of its own, so a transport timeout cannot cut an apply between
two statements and leave a transaction open.

Per record:

1. **Own record** (origin is this node) → `Duplicate`, ignore.
2. **Already applied** (`seq <= high_water`) → `Duplicate`.
3. **Gap** (`seq > high_water + 1`) → refuse. A record cannot be applied over
   a hole; the statements before it may be what make it valid. This holds from
   nothing as well: a stream is taken from its first transaction or not at
   all. (It used to be taken up wherever the first record arrived, which in
   practice skipped a transaction this node had just refused.)
4. **Void** (the origin's transaction did not commit) → move past it.
5. **Superseded** under `last-writer-wins` → `Conflict`, advance past it.
6. Otherwise: begin a transaction, mark it as an apply, run the statements,
   write the position (§5.2), commit.

The records of a batch share **one `fdatasync`**, and nothing is acknowledged
before it. The origin commits many writers under one sync; a peer paying one
each could never keep up, and falling behind is where the other failures
start. If the sync fails, every record waiting on it is taken back, positions
included, and offered again. A schema change is synced on its own.

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

When each table was last written is rebuilt at start from the replication log,
as far back as the log goes. It used to be kept in memory only, so after a
restart nothing was superseded and the first old record to arrive won over a
newer row.

### 5.2 Each transaction is applied once

Applying a record and noting that it was applied are two writes. Noted first,
a crash between them loses the transaction; noted after, as it was, the
transaction is applied again on restart, and a replayed `INSERT` into a table
without a unique key, or a `SET n = n + 1`, lands twice.

So the note is conditional and goes first. Before an applying transaction
commits, an *intent* is written through the write-ahead log: "this origin
moves from `prev` to `seq`, if transaction `tx` committed". The outcome is
logged after it, in the same log, and one sync covers both. After any crash
the log holds a prefix, so an outcome is never there without its intent. At
start each origin's position is its last settled one, moved along its intents
for as long as each one's transaction committed: exactly the last transaction
whose rows are there, including when the cut fell part-way through a batch.

A **schema change** is the exception to "written first": it makes itself
durable over several syncs and is not undone by an abort, so a power cut can
always leave it done and its position not (35 crash images of 160 in the test
for it). What is written ahead of it instead is a mark that it was started. A
node that stops part-way finds the mark at its next start, and when the origin
offers the record again, a statement of it that fails because its work is
already there (the table exists, the column exists, the table is already gone)
is taken as done. Only for that one record, and only after a restart found it
in flight: at any other time such a failure is a conflict.

**A sync that fails** under an applied batch is treated as a failed commit
sync is everywhere else: the write-ahead log refuses further commits until the
node is restarted, the origin's stream is halted until then, nothing of the
batch is acknowledged and nothing is undone. The restart's replay decides what
survived, and the positions, which ride the same log, come back agreeing with
the rows.

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
REPLICATION ADOPT PEER 'berlin'      -- only when it holds more of this node's stream than this node does (§8)
REPLICATION CHECK                    -- compare every table with every peer, row for row (§7.1)
REPLICATION CHECK PEER 'berlin' TABLE orders

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
 released to peers through 361, 0 committing or awaiting their sync
 captured 361, published 361, applied 361, duplicates 0
 conflicts 0, failures 0, gaps 0, ack timeouts 0
 61 batch(es), 60 compressed: 52439 bytes of payload went out as 12331 (76.5% smaller)
 replication log holds 56527 bytes, retention 256 MiB
 peer             address                  node   state      acked    lag      queued   codec
 two              127.0.0.1:7442           2      up         361      0        0        zstd
```

### 7.1 Checking that two nodes hold the same rows

`SHOW REPLICATION` compares sequence numbers: how much of each stream a node
has applied. That is not the same as holding the same rows, and the one time a
pair diverged here both nodes reported the peer up with a lag of zero.

`REPLICATION CHECK` does not take the stream's word for it. Each node reduces
every table a client made to a row count and a checksum over its rows'
**values** (order-independent, so the two nodes need not store rows in the
same order), and the two are compared. Values, not stored bytes: two nodes
that both hold a row correctly can lay it out differently, with different
bytes left under a `NULL`, a dropped column still in one node's layout, or a
`CHAR(n)` padded on one and not the other. Each live column is read by name,
a `NULL` counts as a `NULL` whatever lies under it, and a `character(n)` is
compared without its trailing spaces. Only a `character(n)`: in `text` and
`varchar` a trailing space is part of the value, and `'a'` against `'a '` is
a difference:

```text
$ psql -c "REPLICATION CHECK"
 node 1 and berlin (node 2) are in step: each has applied all the other has committed (5120 from here, 14 from there)
 3 table(s) compared: 1 DIFFERENT, 0 on one node only
 DIFFERENT  orders: 5100 rows here, 5099 on berlin
```

The comparison only counts when the two nodes are **in step**: nothing was
committed or applied on either while it read its tables, and each has applied
all the other has released. Otherwise it is tried again a few times and then
reported as `NOT in step`, since a table read while a transaction is in flight
differs for an honest reason. Run it when the nodes are quiet.

What it does not do: a table of more than two million rows is counted but not
read, because its rows are held in memory while they are hashed; and it says
which table differs, not which rows.

## 8. The files

Beside the data, all named `db_replication.*`, and none is a source of truth
for user data — the WAL and the transaction status log remain that.

- **`db_replication.log`**, then **`db_replication.log.<n>`** — this node's
  stream, and what it relays, in files that are only ever appended to.
  CRC-framed records, each a positional write described to the database's
  write-ahead log, so a commit's sync covers its record and a replay (after a
  crash, or in a restore) puts back whatever a file lost. A record of a
  transaction that wrote rows names that transaction, and counts only if it
  committed. Bounded by `--replication-retention-mb`: a file grows to an
  eighth of the budget, the next one is started, and past the budget the
  oldest files are removed whole. Nothing is ever copied or rewritten: a file
  can be rebuilt from the write-ahead log only if everything in it went
  through that log, and a log that was compacted by copying came back from a
  restore empty.
  The budget is for a peer that is away. A file that a **connected** peer has
  not been sent all of yet is kept past it, up to four times the budget: under
  a burst the origin's many writers outrun a peer applying one transaction at
  a time, and letting go of what such a peer still needed turned a few seconds
  of lag into a node to reseed.
- **`db_replication.state`** — how far each origin's stream is applied and
  durable: one checksummed 64-byte slot an origin, overwritten in place. A
  slot that does not check out stops replication from starting rather than
  being guessed at. A state file of the earlier format (pairs) is converted
  at open.
- **`db_replication.intents`** — the steps of §5.2 not yet settled into a
  slot. Emptied when nothing is waiting on it.

All three are settled at **every server start that finds them**, whether or
not replication is switched on: an entry written ahead of a commit that never
happened is decided by asking how its transaction ended, and that has to be
asked before anything can forget the answer.

**Restoring this node** from a backup brings these files back with the
tables, to the same point: the positions especially, since a position older
than the rows would have the peers' transactions applied a second time. Keep
them.

**Seeding a different node** from this one's backup must remove every
`db_replication.*` file from the copy, and from the node it will pair with
(§9.5): they describe this node's stream and positions, not the new node's.

**A node restored to an earlier point** than its peers have seen is behind its
own stream: a peer holds transactions of this node that this node no longer
has. The link reports `needs reseed` and sends nothing, because the peer would
take every new transaction numbered below what it holds as one it already
has. Reseed one side from the other; or, to keep both as they are,
`REPLICATION ADOPT PEER '<name>'` moves this node's sequence past what the
peer holds.

An **in-memory** database has no files, and its log is the in-memory window
alone. It reports that window as its retention boundary.

There is **no downgrade** to a build from before this format: an earlier build
cuts the log at the first record it does not recognise.

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
joining a cluster that already holds data must be seeded from a backup first,
with every `db_replication.*` file removed on both sides (§8). A node that
holds nothing of an origin whose log no longer starts at its first transaction
is not sent anything: the link reports `needs reseed`.

What every node does for itself at first start — its role catalogue and its
first role — is not replicated.

### 9.6 Conflict granularity

`last-writer-wins` resolves per table, not per row (§5.1). Row-level resolution
would need a per-row commit stamp, which the storage format does not carry.
