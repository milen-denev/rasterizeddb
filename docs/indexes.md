# Indexes

Status: **implemented**. B-tree and hash indexes over one or more columns,
declared with `CREATE INDEX`, plus an implicit index for every `PRIMARY KEY`,
`UNIQUE` column and composite unique constraint.
Target: `rasterizeddb_core::core::index`.

```sql
CREATE INDEX orders_customer ON orders (customer_id);            -- method chosen
CREATE INDEX orders_placed ON orders USING btree (placed_at);
CREATE INDEX sessions_token ON sessions USING hash (token);
CREATE INDEX orders_cust_day ON orders (customer_id, placed_at);  -- composite
CREATE INDEX IF NOT EXISTS ON orders (status);                    -- name derived
DROP INDEX orders_customer;
```

## Choosing the method

With no `USING`, the engine chooses from the column types:

| Columns | Method | Why |
|---|---|---|
| Every column is `UUID`, `JSONB`, `MACADDR`, `MACADDR8` or `CHAR` | **hash** | Nothing asks a range of a random UUID or a document; one probe, no ordered structure to keep |
| Anything else that can be ordered | **B-tree** | Answers equality *and* ranges, so it is the safe default whenever a range might be asked |

`USING btree` or `USING hash` overrides the choice. A type that cannot be
indexed by the requested method is refused with the reason. It is never
indexed approximately.

| Method | Answers | Types |
|---|---|---|
| B-tree | `=`, `IN`, `<`, `<=`, `>`, `>=`, `BETWEEN`, `LIKE 'prefix%'`, and any of these on the *leading* column of a composite index | integers, floats, `NUMERIC`, `BOOL`, `TIMESTAMP`, `DATE`, `TIME`, `UUID`, `INET`, `MACADDR`, `TEXT` |
| Hash | `=` and `IN`, on *every* column of the key | the B-tree types, plus `CHAR` and `JSONB` |

Refused, with the reason:
- **`INTERVAL`**, because `'1 day' = '24 hours'` is true with different bytes, so a
  hash would miss one by the other.
- **`TIMETZ`**, for the same reason.
- **Geometric and range types**, which have no order a lookup could keep.
- **`CHAR` as a B-tree.** The sort path and the `WHERE` evaluator disagree on
  its order today, so no order an index could keep would satisfy both.

## What is and is not built

- Plain columns, one or more, with `ASC`/`DESC`, `NULLS FIRST/LAST`, `COLLATE`
  and an operator class accepted and set aside: the index orders by the
  engine's own comparison either way. `INCLUDE`, `WITH` and `TABLESPACE` are
  accepted too.
- `CREATE UNIQUE INDEX` becomes a unique constraint, as it was already, and the
  constraint brings its implicit B-tree.
- A **partial** index (`WHERE …`) or an **expression** index (`(lower(email))`)
  is accepted and *not* built, as all index DDL was before this feature. An
  index built without its predicate or expression would answer different
  questions from the ones it was declared for. An index over a document path,
  `((doc->>'k'))`, is built: see [Documents](#documents).
- `DROP INDEX` of a name never built (a partial index, or one declared before
  indexes existed) succeeds, so existing migrations keep running.

`pg_indexes` lists every index with its `indexdef`, and `pg_index` lists the
declared ones beside the constraint indexes, so a migration tool sees what
exists.

## Documents

Two kinds of index serve `jsonb` columns.

```sql
CREATE INDEX docs_gin  ON docs USING gin (doc);                 -- jsonb_ops
CREATE INDEX docs_path ON docs USING gin (doc jsonb_path_ops);  -- path ops
CREATE INDEX docs_status ON docs ((doc->>'status'));            -- one path
```

**A `gin` index** keeps, for each document, a set of 64-bit items. The two
operator classes match PostgreSQL's:
- **`jsonb_ops`**, the default, keeps every object key and every string
  array element as a *key* item, and every scalar as a *value* item. It
  answers `@>`, `?`, `?|` and `?&`.
- **`jsonb_path_ops`** keeps one item per scalar, hashing the object keys on
  its path with it. It answers `@>` only, with fewer and more selective items.

`doc @> 'q'` intersects the lists of `q`'s items, and `?|` takes their union.
A query with no items, such as `doc @> '{}'`, cannot be narrowed, and the
table is scanned.

Two rules keep a `gin` answer a superset of the matches:
- **Every item of a contained query is an item of the document.** This is
  pinned by a property test over random documents and queries. Numbers are
  hashed by value, not spelling, exactly as containment compares them: `1`,
  `1.0` and `1e0` are one item, and every zero is the same zero. Array
  nesting never adds a path step, which is what lets `[1,2] @> 2` and
  `{"a":[1]} @> {"a":[1]}` find their documents.
- **Removal is lazy.** A common item (`"status": "shipped"`) is listed under a
  large share of the table. Vacuum therefore stops returning a forgotten slot
  rather than finding it in every list, and the lists are compacted once
  forgotten entries are numerous.

**A path index** is a B-tree over the text `->>` reads, which is always text
whatever the value is. It answers `=`, ranges and `LIKE 'prefix%'` on that
path. A missing path or a JSON `null` is SQL `NULL`, which no comparison
matches.

An index over `->` (a document rather than text) is refused. A path index
serves both `->>` and `->` spellings of the same path, but answers only `->>`
comparisons.

`pg_indexes` lists both kinds with the definition PostgreSQL would print.

## How a query uses one

A `WHERE` clause is reduced to typed `column op literal` predicates under its
`AND`/`OR` structure by the same extractor the filter index uses, with the
same literal coercion and `NUMERIC` scale handling. The planner then picks:

1. an index whose **whole key** is matched by equalities (a hash probe, or a
   B-tree point lookup), the one with the most columns first;
2. otherwise a **B-tree whose leading column** is constrained: one range scan,
   with every bound on that column within the conjunction merged into it;
3. `AND` narrows by any answerable branch, and `OR` unions its branches only
   when every branch is answerable.

The answer is a sorted list of candidate slots. The scan reads only the
pointer records at those slots. It jumps from one candidate to the next
rather than walking the pointer file, and reads exactly the records between
the first and last candidate a chunk would reach. It then checks visibility
and evaluates the whole `WHERE` clause on each candidate, exactly as without
an index. An answer of more than an eighth of the table is declined, and the
filter index or a plain scan handles it.

`UNIQUE` and `PRIMARY KEY` checks use the index over exactly their columns,
so an insert into a keyed table costs the same at a million rows as at a
thousand.

## Correctness: a superset, never a subset

An index may only ever return *too much*. A missing candidate is a row that
silently drops out of an answer. Every rule follows from that:

- **Entries are added when a version is written**, before its transaction can
  commit. No snapshot can see a version its index does not hold.
- **Entries are removed only when the value can never be read again**:
  - after an in-place overwrite is durable (the engine overwrites in place
    only when nothing can roll the write back and no other snapshot exists);
  - when vacuum reclaims a version that no snapshot can reach. The key is read
    back from the row before its space is freed. Each index keeps one bit per
    slot it holds, so vacuum finds every such version, whatever retired it:
    delete, update, or an aborted insert.
- **A version whose key cannot be built** is kept in an *unkeyed* list returned
  with every lookup.
- **A slot appended after a lookup** sampled the table's length is a
  candidate by default.
- **Keys agree with the evaluator by test, not by argument.** Each B-tree
  encoding is proven to sort exactly as the `WHERE` evaluator compares, and
  each hash encoding to be equal exactly when the evaluator says equal, by
  property tests over every supported type.
- **An end-to-end model test** runs an indexed table and an unindexed twin
  through the same random inserts, in-place and relocating updates, deletes,
  rolled-back transactions, `VACUUM` and `VACUUM FULL`, and requires every read
  to return the same rows from both.

## Lifetime

Only declarations are stored, in `<table>_schema_IDX.db`. The index lives in
memory:

- **On open**, it is rebuilt from the table by a background task. Queries scan
  until it is ready, and writes maintain it from the start.
- **`VACUUM FULL`** renumbers every slot, so the indexes are rebuilt under the
  same exclusive gate.
- **An `ALTER TABLE` that rewrites the table** carries the declarations
  across, matched by column name; an index on a dropped column goes with it.

Because the index is derived from the data the WAL already made durable, it
cannot disagree with the table after a crash. The cost is the rebuild at
open, one pass over the indexed columns, and memory: roughly 60–90 bytes per
row per index for short keys.

## Performance

Measured 2026-10-10 against PostgreSQL 17 on the same machine: 8 cores each,
`pgbench -M prepared` on 8 more, data on tmpfs for both. "Before" is the
same build without indexes; the indexed figures are the mean of two rounds.

| Query | Before | With the implicit primary-key index | PostgreSQL 17 | Gap before → after |
|---|---|---|---|---|
| Point read by primary key, 1M rows, 1 client | 2.4k/s | 12.6k/s | 48.6k/s | 12.7× → 3.9× |
| Point read by primary key, 1M rows, 16 clients | 7.9k/s | 51.9k/s | 234.7k/s | 16.8× → 4.5× |
| Primary-key range of 101 rows, 1 client | 2.8k/s | 7.7k/s | 17.9k/s | 6.4× → 2.3× |
| Primary-key range of 101 rows, 16 clients | 11.7k/s | 28.1k/s | 79.9k/s | 6.8× → 2.8× |
| `INSERT … ON CONFLICT DO UPDATE`, 100k rows, 64 clients | 3.5k/s | 16.8k/s | 120.1k/s | 44× → 7× |

Three changes produced these figures:

- **The index names the slots,** so a lookup no longer walks the table.
- **The scan reads only those slots.** The pointer iterator jumps between
  candidates and reads exactly the window from the first to the last, where
  it used to copy two whole chunks for a single read.
- **Workers are sized by the candidate count.** A scan used to start a worker
  per core even when the index named one row.

`UNIQUE` checks probe the index rather than evaluating a filter over the
whole table, which is the upsert gain.

After that, upserts were bound by locks rather than by work: every insert
into a keyed table and every update took one table-wide lock. The uniqueness
lock is now striped by key, and an update naming its row by a unique key runs
beside updates of other keys (see [MVCC](mvcc.md), "Which writers take
turns"). Measured the same way, the upsert row went from 16.9k/s to
31–35k/s, and the server is now CPU-bound rather than waiting on a lock.

Filtered queries over unindexed columns, the engine's existing strength, are
unchanged: still 4.6–9× ahead of PostgreSQL without an index there.

### Prepared statements

A prepared statement used to be parsed again on every execution. The bound
values were written into its text, and the text then went through the whole
pipeline: classified twice, its select list parsed three times, its `WHERE`
clause tokenized three times. Nearly all of that produced the same result
every time.

A prepared `SELECT` now keeps its parse (`core::rql::prepared`). That parse is
never derived, only observed:
- **First execution.** The text path runs as before while recording what it
  did. Each value's literal is located in the parsed `WHERE` clause, which
  becomes a template.
- **Second execution, with different values.** It must produce exactly that
  template rendered with its own values, and everything else it records must
  match the first.
- **From the third on,** executions write their values into the template and
  go straight to the scan.

Anything that would make the shortcut answer differently keeps a statement on
the text path:
- a value used other than as `column <op> $n` (`LIKE` turns `'ab%'` into a
  prefix test and `'ab'` into an equality, so its rewrite depends on the
  value);
- a column type whose literals a rewrite may interpret (dates, booleans,
  JSON);
- a `NULL`, or a string holding a quote;
- a rewrite around the statement (a CTE, a view, a subquery);
- a schema change since the parse was learned.

Measured the same way as above, point reads by primary key went from 16.3k
to 25.9k/s with one client, and from 72.9k to 98.7k/s with sixteen. The gap
to PostgreSQL went from 3.2× to 2.0× and 2.4×.

What remains per execution is text work around the parse: the values are
still written into the statement for logging and statistics, and the
rendered `WHERE` clause is still tokenized and compiled at the table.
