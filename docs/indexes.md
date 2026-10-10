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
  `((doc->>'k'))`, is the path index described in [JSON documents](../README.md).
- `DROP INDEX` of a name never built (a partial index, or one declared before
  indexes existed) succeeds, so existing migrations keep running.

`pg_indexes` lists every index with its `indexdef`, and `pg_index` lists the
declared ones beside the constraint indexes, so a migration tool sees what
exists.

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

Filtered queries over unindexed columns, the engine's existing strength, are
unchanged: still 4.6–9× ahead of PostgreSQL without an index there.

What remains is per-statement work outside the index. The statement text is
parsed again on every execution, even when prepared, and results pass through
the general scan machinery. That is where the next gains are.
