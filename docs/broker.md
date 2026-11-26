# Message broker

A message broker inside the database, in the same way as the key/value cache
(docs/kv-cache.md). It has one engine, and each wire protocol gets a port of
its own:

| Port flag | Protocol | Clients |
|---|---|---|
| `--broker-port` / `--broker-socket` | **native** (`rdbmq/1`) | `rasterizeddb_client::broker` |
| `--kafka-port` | Kafka (classic protocol) | librdkafka, Java, franz-go, kcat, … |
| `--amqp-port` | AMQP 0-9-1 | RabbitMQ clients (pika, amqplib, lapin, Java, .NET) |
| `--mqtt-port` | MQTT 3.1.1 and 5.0 | mosquitto clients, paho, rumqtt, … |

`BROKER <command>` statements on the PostgreSQL port reach the same engine.
Nothing is opened, allocated or started until one of these flags is given,
or until a `BROKER` statement first needs the engine.

Cargo features: `broker` (the engine, the native protocol and the SQL
surface), plus `broker_kafka`, `broker_amqp` and `broker_mqtt`. All four are
in the default list. A build without them contains none of this code.

**The native path is the fastest one.** The on-disk format is the native
format. A native producer builds a batch in exactly the layout it will have
on disk. The server checks it, stamps two fixed fields and appends it. A
native consumer receives those same bytes. The Kafka, AMQP and MQTT adapters
translate to and from this format at their edges, so they cost more than
native does.

---

## 1. Model

Two storage primitives sit at the bottom:

- **Topic.** A named set of `partitions`. Each partition is an append-only
  log of batches, and every message in it has a 64-bit offset. Messages are
  kept until retention removes them (by age, size or both), whether or not
  anyone has read them.
- **Subscription.** A named reader of one topic. It comes in three modes:
  - `ordered`: one committed offset per partition, with partitions shared
    out among the group's members. This is a Kafka consumer group: ordering
    holds within a partition, and a member owns its partitions.
  - `shared`: competing consumers with an ack per message. A message goes to
    one consumer, and it is redelivered on nack, on an ack timeout, or when
    its consumer disconnects. After `max_deliveries` it moves to a
    dead-letter topic. This is a work queue.
  - `broadcast`: each connection gets its own cursor, which exists only in
    memory and starts at the tail. This is plain pub/sub.

Every pattern people ask for is one of these:

| Pattern | How |
|---|---|
| 1 → 1 queue | topic (1 partition) + `shared` sub, one consumer |
| N → 1 | many producers on one topic + one consumer |
| 1 → N fan-out | one topic, N subscriptions (each sees every message) |
| N → M work queue | one topic + `shared` sub, M consumers |
| N → M ordered stream | P partitions + `ordered` sub, ≤ P active members |
| pub/sub | topic + `broadcast` subs (or a `memory` topic) |
| routing | an **exchange** (§5) maps a publish to one or more topics |

A **queue** is a single-partition topic with one `shared` subscription of the
same name. AMQP's queues are exactly this, and the native `QUEUE DECLARE`
creates the pair in one step.

### Durability, per topic

| `durability` | Acked when | Survives |
|---|---|---|
| `sync` (default) | the batch is `fdatasync`ed. Group commit: one sync covers every batch appended since the last one, across all connections | power loss |
| `async` | the batch is in the page cache; a flusher syncs every `broker.flush_ms` (default 10) | process crash; loses ≤ flush_ms on power loss |
| `memory` | appended to the in-memory tail | nothing: the tail is a bounded ring, nothing is written to disk |

## 2. On-disk layout

```
<database dir>/broker/
  meta.<gen>.log            topics, subscriptions, exchanges, bindings (framed_log)
  t<topic id>/
    p<partition>/
      <base offset 20 digits>.seg   batches, back to back
      <base offset 20 digits>.idx   sparse index: (rel offset u32, position u32)
    s<sub id>.<gen>.log      a subscription's state (framed_log): offsets or ack ranges
```

Topics and subscriptions are named on disk by number, never by name. A
rename does not touch any files, and no name can escape the directory.

### 2.1 Batch: the one format

Every value is little-endian. Every record starts on an 8-byte boundary, and
the batch's length is a multiple of 8. A reader can therefore cast the
fixed-width fields where they sit, without varints or a parsing pass.

```
Batch header, 64 bytes
 off  size  field
   0     4  magic        b"RQB1"
   4     4  length       whole batch in bytes, header included (multiple of 8)
   8     4  crc          CRC-32 (crc32fast) of header bytes 12..64
  12     2  flags        bit 0-2 codec (0 none, 1 zstd), bit 3 has_keys,
                         bit 4 has_headers, bit 5 uniform (every record the
                         same length), bit 6 transactional, bit 7 control,
                         bit 8 producer_time (base_time is the producer's)
  14     2  producer_epoch
  16     8  base_offset  STAMPED BY THE SERVER
  24     8  base_time    µs since the epoch; STAMPED BY THE SERVER unless the
                         producer set flag `producer_time` (bit 8)
  32     4  count        records in the batch (≥ 1)
  36     4  max_time_delta  µs from base_time to the latest record
  40     8  producer_id  0 = not idempotent
  48     4  base_sequence
  52     4  records_len  bytes after the header (stored: compressed size)
  56     4  raw_len      records_len before compression (= records_len if none)
  60     4  records_crc  CRC-32 of bytes 64..length (the records as stored)
```

```
Record, 8-byte aligned
   0     4  length       this record, padding included
   4     4  time_delta   µs from base_time
   8     4  key_len      u32::MAX = no key
  12     4  value_len
  16     2  header_count
  18     2  reserved
  20     …  key bytes, value bytes
            headers: (name_len u16, value_len u32, name, value) × header_count
         …  zero padding to 8
```

A record's offset is `base_offset + index`, and nothing in the record stores
it. If `uniform` is set, record *i* is at `64 + i * records[0].length`, so a
reader can seek inside a batch without walking it.

The server does three things to a batch from a native producer:

1. Checks `magic`, `length`, `count`, `records_len` and the CRC, and walks
   the record lengths once (or, for zstd, checks `raw_len` against the
   limit).
2. Writes `base_offset` and `base_time`, then recomputes the CRC. The CRC is
   the one place the server touches the bytes, and only over the batch's 64
   header bytes: see §2.2.
3. Appends the batch in a single `memcpy` into the partition's tail buffer.

A fetch returns whole batches exactly as they are stored. A consumer that
asked for offset *n* may receive a batch that starts before *n* and skips
the records ahead of it (Kafka does the same).

### 2.2 Two CRCs

`records_crc` covers the records, and `crc` covers the rest of the header,
`records_crc` included. Damage anywhere in a batch therefore fails one of the
two. Stamping `base_offset` and `base_time` changes only header bytes, so it
recomputes `crc` over 52 bytes and never rehashes the records. The server
hashes a native producer's records once, to check `records_crc`, in the same
pass that walks them. Batches the server builds itself (the Kafka, AMQP and
MQTT adapters) were hashed once when they were built and are stamped without
being checked again.

The shared crate `rasterizeddb_mq` holds this format and the wire codec
(§6). Its only dependency is `crc32fast`. The engine and
`rasterizeddb_client` both use it, so there is one copy of the layout.

### 2.3 Segments and the index

A partition's log is a run of segment files. Each file is named for the
first offset it holds, and a new one is rolled at `segment_bytes` (default
256 MiB) or `segment_ms`. `.idx` holds one 8-byte entry per
`index_interval` (4 KiB) of batches: `(offset - base u32, position u32)`. A
lookup is a binary search in the index followed by a short walk of batch
headers.

**Opening a partition.** The scan starts at the last index entry of the
newest segment and checks each batch's frame and CRC. The segment is cut at
the first batch that fails, using the torn-tail rule in
`framed_log`/`torn_tail`: a power cut can leave a partial last batch, and
nothing after it was ever acknowledged. `.idx` is rebuilt from the scan when
it runs past the batches the scan accepted.

**Retention** removes whole segments from the head, never part of one. A
subscription whose committed position has fallen behind the head resumes at
the head. It reports the skip as `offset out of range`; Kafka calls this
`auto.offset.reset`.

### 2.4 In memory

A partition keeps:

- its tail, an append buffer of `tail_bytes` (default 8 MiB). Writes are
  batched into it under the partition's mutex, which is held only for the
  copy (about 100 ns). The flusher writes the filled part with one `pwrite`
  and moves the tail on.
- `next_offset`, `written_through` and `durable_through`, the same three
  watermarks as `Wal::commit_through` / `durable_through`. A `sync` producer
  waits on `durable_through ≥ its last offset`.
- a waiter list. Fetches and pushes parked on "anything past offset *n*" are
  woken once per append round, not once per batch.

A fetch at the tail is served from the tail buffer: those bytes are hot in
cache and no syscall is needed. A fetch further back is served by `pread`
from the segment file, through the page cache.

## 3. Subscription state

- `ordered`: `commit(partition, offset, metadata)` records, written to the
  sub's framed log and compacted into a new generation when it passes 4 MiB.
  This store also backs Kafka's `OffsetCommit`.
- `shared`: every partition keeps
  - `floor`: every offset below it is acked;
  - a bitmap window over `[floor, floor + 2^20)` of offsets acked out of
    order;
  - the in-flight map `offset → (consumer, deadline, deliveries)`, held in
    memory only.

  The log stores `ACK` ranges and `DEAD` offsets. `floor` advances, and the
  bitmap slides, whenever the low end fills in. After a restart nothing is
  in flight: anything not acked is redelivered, so delivery is at least
  once.

## 4. Threads

The engine uses the thread-per-core I/O model of `kv_cache::resp::serve`.
There are `--broker-threads` I/O threads, each with its own `current_thread`
runtime, and connections are dealt round-robin. A partition is not owned by
a thread: any I/O thread appends under the partition's mutex, which is held
only for the copy. One **flusher** thread per database writes the tails and
runs the group `fdatasync`. Durable acks go out from the waiters it wakes.
Retention, ack timeouts and the delay queue run on one housekeeping timer
per database, at ten ticks a second.

## 5. Exchanges and routing

An exchange is a named routing table that maps `(routing key, headers) →
topics`:

| type | matches |
|---|---|
| `direct` | binding key == routing key |
| `fanout` | every binding |
| `topic` | AMQP words split on `.`: `*` matches one word and `#` matches zero or more |
| `headers` | `x-match all/any` over header values |

MQTT's filters (`/` separator, `+` and `#` wildcards) use the same
wildcard trie (`broker::trie`), configured with a different separator and
different wildcard tokens. A publish to an exchange appends one copy to each
matched topic, deduplicated: if several bindings name the same topic, it is
appended once. Unroutable publishes are dropped, or with `mandatory` they
are returned. An exchange can have an `alternate` exchange.

## 6. The native protocol (`rdbmq/1`)

The protocol runs over TCP, TLS or a unix socket. Each frame is

```
 len u32 (bytes after this field) | op u8 | flags u8 | reserved u16 | corr u32 | body
```

The header is 12 bytes, and `corr` comes back on the reply. Replies can
arrive in any order, so one connection can carry any number of requests at
once. Pushed deliveries carry `corr = subscription handle`.

| op | body |
|---|---|
| `HELLO` | version, client name; reply: server version, limits |
| `AUTH` | user, password: checked against the database's roles, as `kv_cache`'s AUTH does |
| `TOPIC_CREATE` / `TOPIC_DELETE` / `TOPIC_LIST` / `TOPIC_ALTER` | name, partitions, durability, retention |
| `RESOLVE` | name → topic id, partition count. Afterwards a client names a topic by its `u32` id |
| `PRODUCE` | topic id, partition (`u32::MAX` = by key hash, `u32::MAX-1` = round robin), records_crc, ack mode (0 none, 1 written, 2 durable), then a batch in §2.1 layout; reply: base offset |
| `FETCH` | topic id, partition, offset, max bytes, max wait ms; reply: stored batches |
| `SUBSCRIBE` | topic, sub name, mode, start (earliest/latest/offset/time), credits; reply: handle |
| `CREDIT` | handle, n: the server pushes up to n more messages (prefetch) |
| `ACK` / `NACK` | handle, a list of (offset, count) ranges; NACK takes a requeue delay |
| `COMMIT` | handle or (sub, partition), offset |
| `UNSUBSCRIBE` | handle |
| `EXCHANGE_DECLARE` / `BIND` / `UNBIND` / `PUBLISH` | exchange name and type, binding key, routing key + one batch |
| `QUEUE_DECLARE` | name, durability, options: a 1-partition topic plus a shared sub |
| `PING` | — |

Pushed deliveries (`DELIVER`, op 0x80) carry the handle, the partition and
then stored batches. For `shared` subscriptions they also carry a delivery
bitmap, which tells the client which records in the batch are its to ack.
Other members of the group have the rest.

## 7. Protocol adapters

- **Kafka** (`broker_kafka`) implements ApiVersions, Metadata, Produce
  (v3+), Fetch (v4+), ListOffsets, FindCoordinator,
  JoinGroup/SyncGroup/Heartbeat/LeaveGroup, OffsetCommit/OffsetFetch,
  InitProducerId (idempotent producers, which clients enable by default),
  CreateTopics, DeleteTopics, DescribeConfigs and SASL PLAIN/SCRAM. Kafka
  RecordBatch v2 is converted to and from §2.1 at the adapter. Compressed
  Kafka batches are decoded: zstd with the zstd crate already in the tree,
  gzip, snappy and lz4 with small crates behind `broker_kafka`. A Kafka topic
  is a topic, and a consumer group is an `ordered` subscription.
- **AMQP 0-9-1** (`broker_amqp`) supports exchanges, queues (topic plus
  `shared` sub), bindings, `basic.qos` (credits), ack/nack/reject, publisher
  confirms (sent at the durable watermark), mandatory/return, TTL,
  dead-letter exchanges and max-length. A durable queue is `sync`; a
  transient one is `memory`.
- **MQTT** (`broker_mqtt`) supports 3.1.1 and 5.0, QoS 0/1/2, retained
  messages, persistent sessions (a per-session queue), wills, keep-alive and
  shared subscriptions (`$share/g/filter` as a `shared` sub). QoS 0 to online
  subscribers goes through the trie straight into their connection queues
  and is not stored.

## 8. Security

- Logging in uses the database's roles, through `crate::auth::lookup_role`
  and `verify_password`. Strict tenant mode applies through
  `policy_hooks::tenancy().admit` with `protocol = "broker"`, `"kafka"`,
  `"amqp"` or `"mqtt"`.
- Creating, deleting or altering topics, exchanges and bindings needs a
  superuser, or a role with the `broker_admin` setting.
- `--broker-auth trust` is refused on anything but loopback, and on a unix
  socket that admits others (the same rule as `--kv-auth`).
- TLS uses `pgwire::tls_acceptor`, with the PostgreSQL port's certificate.

## 9. Phases

1. The engine: the batch format, segments, partitions, the flusher, topics,
   subscriptions (all three modes), exchanges, retention and the meta log.
   Then the native protocol, its server and the client in
   `rasterizeddb_client`, the flags and `BROKER` SQL.
2. The Kafka adapter.
3. The AMQP 0-9-1 adapter.
4. The MQTT adapter.
5. Replication (a `FLAG_BROKER` record on the table-data stream, as
   `FLAG_KV`), transactional publish (`BROKER PUBLISH` inside a SQL
   transaction is appended at commit) and reading a topic with `SELECT`.
6. Tests and benchmarks, run against each protocol's own load tool: native,
   then `kafka-producer-perf-test`/`rpk`, `rabbitmq-perf-test` and
   `emqtt-bench`, compared with Redpanda, RabbitMQ and EMQX.
