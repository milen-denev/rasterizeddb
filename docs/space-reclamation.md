# Space Reclamation — Design & Implementation

Status: **implemented and enabled by default** (`rasterizeddb_core::core::vacuum`), together with fixes to the
`DELETE` / `UPDATE` write path it depends on.
Target: `rasterizeddb_core`, edition 2024.

## 0. What exists

| Component | State |
|---|---|
| `Table::compact` — full in-place compaction, `VACUUM FULL` | **Done.** See §10 |
| `StorageIO::truncate` — return space to the filesystem | **Done.** File and memory providers |
| `VACUUM` / `VACUUM FULL` statement | **Done.** See §11 |
| `UNIQUE` enforcement on `INSERT` / `UPDATE` | **Done**, wired through `CREATE TABLE`. See §12 |
| Column-level in-place `UPDATE` | **Done.** See §13 |
| Snapshot-aware reclamation under MVCC | **Done.** See §17 |
| `commit_durable` reporting failure | **Done.** See §18 |
| Stable row identity across relocation and compaction | **Done.** See §5 |
| `vacuum::FreeSpace` — reclaimed extents, best-fit allocation | **Done.** `BTreeSet` keyed `(len, start)`, split-and-return |
| `vacuum::ReaderRegistry` — reader epochs | **Done.** Ticket per query, oldest-active query gates publication |
| `vacuum::sweep_pointers` — full pointer walk incl. tombstones | **Done.** Catch-up passes so a mid-sweep append is still observed |
| `vacuum::live_extents_overlapping` — soundness pass for the capped path | **Done.** See §3.3 |
| `vacuum::dead_extents` — gap derivation | **Done.** Also recovers slack from in-place shrinking updates |
| Background service, one task per table | **Done.** Shares `Table`'s drop signal with filter maintenance |
| Write-path integration | **Done.** `insert_row` and the `UPDATE` relocation path allocate from the free list |
| `Table::vacuum_now`, `Database::vacuum_all` | **Done.** Explicit runs bypass the size thresholds |
| Concurrent `DELETE` / `UPDATE` correctness | **Fixed.** See §4 |
| Row history across a relocation | **Fixed.** See §5 |
| `INSERT` reporting failure as success | **Fixed.** See §6 |
| Tests | 85 (35 unit + 50 write-path/integration) |
| Benchmarks | `benches/vacuum_benchmarks.rs`; numbers in §15 |

**Known gaps** are in §13 — row-level locking and column-level in-place updates, neither of which is
reachable without changes larger than this work.

## 1. The problem

A `DELETE` sets the `deleted` flag on a pointer record. An `UPDATE` whose row outgrows its extent tombstones
the old pointer and appends a replacement. Neither touches the row bytes, and nothing ever reuses them, so the
rows file only grows.

Measured before the change, on a table of 500 rows of ~53 bytes:

```
insert 500 → delete 450 → insert 450
rows file  26 500 → 50 350 bytes   (1.9×)
pointers      500 → 950 slots      (for 500 live rows)
```

A table that deletes and reinserts its working set doubles on disk every cycle while its live size never moves.

## 2. Design

Two halves: a background scan that decides what is dead, and a free list the write path draws from.

**Scan.** Reads every pointer record, tombstones included — `RowPointerIterator` deliberately skips tombstones,
and a tombstone is exactly what marks an extent dead. Live extents cannot overlap (each row owns its bytes
outright), so sorting them by start and walking the gaps gives the dead set exactly. That also catches slack an
in-place *shrinking* update leaves inside a row's old extent, which tombstones alone would miss.

The scan re-reads the pointer file's length after each pass, up to eight times, so a pointer appended during
the scan is still observed. Without that, a row written into a just-allocated extent could be missed and its
extent freed a second time.

**Free list.** `BTreeSet<(len, start)>`, so best fit is `range((need, 0)..).next()` — the smallest extent that
still fits, keeping large holes available for large rows. A leftover of 24 bytes or more goes back on the list
as its own extent; anything smaller stays part of the loan and reappears as an interior gap next scan, where it
is filtered out for the same reason. The list is capped at 65 536 entries, largest first.

**Write path.** `RowPointer::write_row_with_pointer_record_pos` asks the free list before it appends, and writes
into a reclaimed extent with `write_data_txn` — the same WAL-backed in-place write an update uses, in the same
transaction as the pointer record.

## 3. Why a reused extent cannot be read out from under a query

A query decodes a pointer and *then* fetches the row bytes it names. An extent must not be handed to a writer
while an in-flight query still holds a pointer into it. Two mechanisms cover the two ways that could happen.

### 3.1 Reader epochs

Every query takes a monotonically increasing ticket for its duration (`Table::read_guard`, held across the whole
query including worker fetches). A batch of candidate extents is stamped with the ticket counter as it stood
*after* the scan that found them, and published only once every still-running query holds a ticket at or beyond
that stamp.

The argument: the scan observed tombstone `T`, so `T` was committed before the scan read it, so before the
barrier. A query holding a ticket ≥ barrier started after the barrier, hence after `T` was durable, hence can
only ever have seen that row as deleted — and a deleted pointer is skipped before any fetch. A query holding a
ticket < barrier might predate `T` and blocks publication until it finishes.

If older queries do not drain within `drain_timeout` (30 s), the batch is dropped and the report comes back with
`deferred` set. Nothing is published; the next run tries again.

### 3.2 In-flight allocations

A scan racing an append could see an extent that a writer has already been handed, find no live pointer owning
it, and free it a second time. So an extent handed out is recorded in `in_flight` until a later scan actually
observes the live pointer at that position.

Candidates are filtered against `in_flight` **at publish time, not at compute time** — a writer may claim an
extent during the drain wait — and by interval subtraction rather than exact match, so a partially consumed
extent contributes only its unused remainder.

`in_flight` is normally empty, and `retire_loan` checks an atomic count before taking its lock, so the per-live-
row cost of a full scan is one relaxed load.

The list and the loans live under **one** lock, not two. With separate locks, a writer could claim an extent in
the window between publication reading the loans and installing the new list — the extent would then be both
lent out and on the list, handed to two rows that write over each other.
`publishing_concurrently_with_allocation_never_double_hands_an_extent` reproduces this; reintroducing the
two-lock ordering makes it fail within a few of its 200 iterations.

### 3.3 Memory bound, and why the capped path needs a second pass

Above 2 M live rows (`VacuumConfig::max_tracked_live_extents`) the scan stops building a map of the heap and
derives dead space from tombstones instead. That loses the in-place-update slack, which only the gap
computation can see — and it is **not** sound on its own.

A tombstone is not proof its extent is free. Once an extent has been reclaimed and written into, the tombstone
that released it is still on disk pointing at bytes a live row now owns. The gap-derived path never has this
problem — an extent a live row occupies is not a gap — but a tombstone-derived one would hand out an occupied
extent and the row that landed there would be overwritten.

So the capped path takes a second sweep (`live_extents_overlapping`) and withholds any tombstoned extent a live
row now overlaps. Conflicts are limited to extents this table has previously reclaimed, so the result stays
small even where the live map was too large to hold. Both the cap being hit and the conflicts found are logged
rather than silently reclaiming less.

`the_capped_path_never_rehands_a_reused_extent` covers this, forcing the path with
`max_tracked_live_extents = 0`. It asserts against the expected **set** of ids: checking each row's label
against its own id would not catch it, because a re-handed extent makes the pointer read another row's bytes,
so id and label move together and stay consistent with each other while the set as a whole is wrong. Removing
the second pass makes it fail with 2 960 bytes re-handed.

### 3.4 Corrupt records and failed writes

Every extent is clamped to the rows file's length, read before the pointer sweep so a row appended during the
scan lands past it. A pointer record with a nonsense `position` or `length` — a torn write, a corrupt file —
would otherwise have the write path extend the file to satisfy it: sparsely on disk, and by a `Vec::resize` in
the in-memory provider. `Extent::end` saturates for the same reason.

An extent whose write fails is given back (`release_failed_loan`). Without that the loan would shield those
bytes from every future scan and the space would be lost for the life of the process. It is not put straight
back on the list — the failed write may have left partial bytes there, and a scan is what establishes that
nothing owns them. `FreeSpace::leaked_loans()` counts these.

### 3.5 What it costs when there is nothing to do

The service skips the sweep entirely when no `DELETE` or `UPDATE` has committed since the last successful scan
and no loans are outstanding — it shares `Table`'s `write_generation` counter to tell. A read-only or
append-only table therefore pays nothing per tick, rather than re-reading its whole pointer file every 30
seconds. The sleep is taken in 250 ms slices so a dropped table's storage is released promptly instead of being
held for up to a full interval.

### 3.6 A permanently busy table

If queries always overlap the scan, every run times out waiting and nothing is ever published. Extents from an
earlier run are already safe, though — they were stamped with a barrier that has since been passed, which is a
fact about the past that a query starting now cannot undo. So a run that times out publishes the batch it is
holding from last time and quarantines its own findings for the next. Such a table reclaims one run behind
rather than never.


## 4. Fixed: concurrent `DELETE` / `UPDATE` duplicated rows

Both statements scanned for matches and then rewrote the pointer records they found, with nothing between the
two. Two statements matching the same row each acted on their own snapshot: both saw it live, both tombstoned
it, and both appended a replacement.

Measured before the fix — four concurrent `UPDATE`s of one row in a 50-row table:

```
rows matching id = 7 : 4
total rows           : 53
```

Six concurrent `DELETE`s of one row reported six rows affected.

The first fix was a per-table write lock around the *mutating* half only, with the scan left concurrent and each
record re-read under the lock. That is not enough, and a test caught it: a relocating `UPDATE` tombstones its
record and appends the replacement **past the end the other statement's scan snapshotted**, so that statement
sees only a tombstone and matches nothing at all. There is no record left for a re-read to correct. The row is
then neither deleted nor reported — the one outcome a statement must never have.

```
a_delete_follows_a_row_that_was_relocated_under_it ... FAILED (~1 run in 5)
    the row survived its DELETE
```

So `DELETE` and `UPDATE` take the lock **before** they scan. They serialize against each other outright, which
they already did for their rewrites; this extends it over the scan, the expensive half. `SELECT` and `INSERT`
are unaffected: neither rewrites an existing record, and neither takes this lock. The test is stable over 12
consecutive runs.

There is no lost update left to describe: a second statement's scan runs after the first has committed, so it
sees the row wherever it now lives.

Separately: `DELETE` held one transaction for the whole statement with no batching, while `UPDATE` broke its own
once the log was due for a checkpoint. An open transaction blocks checkpointing, so a `DELETE` over millions of
rows grew the log without bound. It now breaks on the same condition.

## 5. Fixed: a relocating `UPDATE` erased the row's history

A relocated row got a brand-new pointer, so under `enable_long_row` its `created_at` was set to the time of the
*update* and its `version` restarted at zero. `RowPointer::inherit_metadata` now carries `created_at`,
`cluster` and `version + 1` across.

`id` is carried over too, which reverses an earlier decision. Treating it as a physical address — the Postgres
`ctid` reading — leaves the engine with **no** stable handle to a row at all, because compaction (§10)
renumbers every slot. Keeping it gives a row an identity that outlives both its address and its slot, which is
what an external mapping has to key on. `Table::locate_row_by_id` resolves one.

The cost is that ids no longer ascend with file order, so the counter cannot be recovered from the last record.
`Table` persists it in a `{table}_seq.db` sidecar, reserving a block of 1 024 ahead of use — a crash then skips
ids rather than repeating one, and ids only have to be unique, never gapless. A table written before the
sidecar existed falls back to the last record, which is exact for it: its ids did still ascend.

## 6. Fixed: `INSERT` reported failure as success

`Table::insert_row` consulted the write result only to decide whether to index the row, then returned `Ok(())`
either way. A client whose insert failed was told it had landed. It now propagates, through a new
`From<RowError> for io::Error` that preserves the underlying `ErrorKind` rather than flattening everything to
`Other`.

Related, and **not** fixed: `RowPointer::new` sums column sizes into a `u32` in the short-row layout, so a row
whose columns total more than 4 GiB wraps (or panics in debug). Not reachable through SQL — the wire protocol
gives out long before — and the fix is the `enable_long_row` layout, which already widens that field. Flagged
here rather than half-addressed.

## 7. Fixed: a leaked loan lost a working set per cycle

`churn_reaches_a_steady_state` runs eight delete-and-refill cycles over a table whose live size never changes.
The rows file grew by about a cycle's worth of data every other cycle, forever:

```
sizes: 12991, 21456, 21630, 30016, 30291, 38598, 38955, 47183
```

A sweep reported finding 8 646 dead bytes and publishing 102 of them. The rest was shielded by loans that had
been taken in the previous cycle and never given back: `retire_loan` ran only for **live** pointers, and every
row written into a reclaimed extent had been deleted before the next sweep saw it. The loan outlived the row
and hid those bytes from every scan thereafter — and under this workload that is the entire working set, once
per cycle.

A loan shields an extent until a *record* owns it, and a tombstone is a record. Retiring on any pointer at that
position, live or dead, is the fix. The same test now holds flat from the third cycle:

```
sizes: 12991, 12991, 13025, 13025, 13025, 13025, 13025, 13025
```

with 200 of 200 refilled rows drawn from the free list each cycle.

## 8. Not built incrementally: pointer-slot reuse

Tombstoned **pointer records** are never reused in place, and the background service never renumbers a slot:

- Record offsets are slot addresses for the filter index (`slot = offset / ROW_POINTER_RECORD_LEN`), so
  renumbering invalidates it wholesale.
- Reusing a slot in place is no better. A reader decoding a 21-byte record while the write path rewrites it
  could pair a new row's id with the old row's position. That is unlike the single-byte tombstone flip the
  engine already does, which is benign because every other field is unchanged.

Slot reuse is safe from the filter's side — `TableFilters::on_insert` *sets* a slot rather than OR-ing into it —
so the blocker is the torn record, not the index. Both point at the same answer, which is what §10 does: an
exclusive pass that can rebuild the filter afterwards.

## 9. Configuration

| Knob | Default | Effect |
|---|---|---|
| `VACUUM_ENABLED` | `true` | Off restores append-only behaviour and skips per-query epoch registration |
| `VACUUM_INTERVAL_SECS` | `30` | Scan period |
| `VACUUM_MIN_DEAD_BYTES` | 8 MiB | Below this *and* the ratio below, a scan publishes nothing |
| `VACUUM_MIN_DEAD_RATIO` | `0.20` | For tables too small to reach the byte threshold |
| `VACUUM_DRAIN_TIMEOUT_SECS` | `30` | How long a run waits for older queries before deferring |

`VacuumConfig::max_tracked_live_extents` (2 M) is not a static: it is per-run, so a test can force the capped
path without building a table large enough to trip it.

Every value above is pinned by `the_reclamation_defaults_are_what_the_documentation_says`. It exists because
these were once deleted by an over-broad edit to `lib.rs` and rebuilt from their call sites — which give names
and types but not values, so all seven had to be guessed and three came back wrong. A wrong drain timeout or
dead-bytes threshold surfaces as a performance regression or a flaky test, never as an obvious config error.
Changing one on purpose means editing the test and this table; changing one by accident now fails.

`Table::vacuum_now()` and `Database::vacuum_all()` bypass both thresholds; they still respect the drain wait.
`Table::set_vacuum_enabled()` turns reclamation off for one table at runtime, dropping its free list.

`Table::vacuum_stats()` exposes runs, compactions, deferrals, and last/total reclaimed bytes;
`Table::reclaimable_bytes()` reports what is on the list right now. A deferred run logs at **warn** — a table
whose queries always overlap the scan never reclaims anything, and the only other symptom is a file that keeps
growing.

## 10. `VACUUM FULL`: compaction

The background service fills holes but never closes them. A heap fragmented into holes smaller than the rows
being inserted stays large, tombstoned pointer records are scanned forever, and neither file ever shrinks.
`Table::compact` addresses all three, at the cost of stopping every other operation on the table — the same
trade `VACUUM FULL` makes.

Exclusion is a `RwLock` taken shared by every read and write and exclusively by compaction. Epochs are not
enough here: they tell you when older queries have *drained*, but compaction moves live rows, so a query
starting after the drain would still be holding a pointer it is about to invalidate.

**In place, and safe to interrupt.** No temporary copy is made. Rows are visited in increasing source position
and every target is at or below its source, so a move can never overwrite bytes a later move still needs. Each
move is its own transaction, and each leaves the table *valid* rather than merely recoverable:

- A row move writes the bytes at the new offset and updates the pointer to match, together. A pointer never
  names bytes that are not its own.
- A slot move writes the record at its new slot and tombstones the old one, together. The record is live at
  exactly one slot throughout; a copy that left the source live would make the row appear twice.

A crash mid-compaction therefore loses only the batch in flight, leaving a table that is partly compacted and
entirely correct, and the next run continues from there.

Truncation happens last, after the log is checkpointed so no unreplayed record still refers to the range being
dropped. A provider that cannot truncate reports `truncation_unsupported` and keeps the tail.

Afterwards the filter is rebuilt from storage: every slot address changed, so it now indexes rows by addresses
they no longer have. `backfill_row` re-*sets* each slot rather than merging into it, so the old bits do not
survive. Slots past the new end keep stale bits, which no query reads — a scan bounds itself by the pointer
file's length — and which a later insert overwrites.

## 11. The `VACUUM` statement

`VACUUM`, `VACUUM FULL`, and either with a table name. Postgres's parenthesised option list and its
`FREEZE` / `VERBOSE` / `ANALYZE` keywords are accepted and ignored: a client sending `VACUUM (ANALYZE) users`
gets its table vacuumed rather than a syntax error. Without a table name, every table is done.

The reply follows `UPDATE` and `DELETE`: a status byte then a little-endian `u64`, here the bytes reclaimed.

## 12. `UNIQUE` enforcement

`SchemaField::is_unique` was parsed, persisted at byte 151 of the field record, and then ignored by every write
path. It is now enforced on `INSERT` and `UPDATE`.

Probing goes through the filter: a `column = value` predicate narrows the table to a few candidate slots, and
each is compared exactly, so the answer is never the filter's guess. Where the filter cannot index the column
this degrades to a scan — correct, slow, and logged.

An empty value is this engine's stand-in for `NULL`, and SQL lets a unique column hold any number of those, so
those are skipped. An `UPDATE` excludes the row being updated, which is allowed to keep the value it has.

A check that is not atomic with the write it guards is not a check: two concurrent inserts would each look, each
find nothing, and each write. So an insert into a table with a constraint takes the write lock. Tables without
one never do, and keep concurrent inserts.

`CREATE TABLE` carries the constraint through, in every spelling a client uses: inline `UNIQUE`, inline
`PRIMARY KEY`, table-level `UNIQUE (col)`, `PRIMARY KEY (col)`, and `CONSTRAINT name UNIQUE (col)`.

A **composite** key is recorded and not enforced, and says so in the log. `UNIQUE (a, b)` constrains the pair;
enforcing it per column would reject rows the constraint allows — a stricter database than the one the user
asked for, which is its own kind of wrong.

`ALTER TABLE … ADD COLUMN` rewrites the table by dropping and recreating it, so it carries the constraints
across explicitly. Without that, adding a column would quietly drop the table's uniqueness with it.

`Table::new_with_constraints` takes the column names. It is a separate constructor rather than a field on
`CreateColumnData` because that struct is built in 33 places.

## 13. Column-level in-place `UPDATE`

When every assigned column is fixed-width, the row's layout and length are unchanged and only those columns'
bytes differ. The general path re-encodes and rewrites the whole row regardless — for a wide row and a
one-column `SET`, almost all of that work produces bytes identical to the ones already on disk.

I first wrote this off as needing a filter API change: `on_insert` sets *every* column of a slot, so handing it
only the changed ones would clear the rest. That was wrong. `update_rows` already fetches the full row to build
the merged `RowWrite` — it has to, for the merge and for the uniqueness check — so the filter gets exactly the
same full payload on either path. The fast path needs nothing from it.

It declines, and falls back, whenever it cannot be sure: any assignment to a `STRING` (whose bytes live in the
variable-length region after the fixed columns, so changing one moves everything behind it), a payload whose
width does not match its column, a column that would run past the row's end, and the whole `enable_long_row`
layout, which stores a checksum over the row that this path does not have the bytes to recompute. Declining is
always safe; taking it wrongly is not.

`Table::in_place_column_updates()` counts how often it was taken, which is what
`a_fixed_width_update_writes_only_the_columns_it_changes` asserts on — the observable effects are otherwise
identical to the general path at equal length, so a test without the counter passes either way. That is how the
first version of that test was worthless.

## 14. Known gaps

**Row-level locking.** `DELETE` and `UPDATE` serialize per table. Striping the lock by row is not reachable from
here: the scan has to run under the lock (§4), and it cannot know which rows it will touch until it has run.
Making these concurrent means real MVCC — row versions and a snapshot per statement — not a finer lock.

**Uniqueness is not an index.** The probe is exact, but the filter behind it is sized for selectivity, not for
constraint checking. A unique column the filter prunes poorly makes every insert a scan.

**Composite keys are not enforced.** See §12.

**Reclamation under MVCC.** Done — see §17.

**`RowPointer::write_row_with_pointer_record_pos` takes nine positional arguments**, three of them `Option`.
No bug today, but it is the kind of signature a future mis-wiring hides in; it wants the same struct treatment
`VacuumService` got.

## 15. Measured

`cargo bench -p rasterizeddb_core --bench vacuum_benchmarks`, on tmpfs, default features:

| | median | per row |
|---|---|---|
| `sweep_50k_rows_20pct_dead` | 227 µs | ~4.5 ns |
| `insert_1k_appending` | 1.115 ms | ~1.11 µs |
| `insert_1k_from_free_list` | 1.170 ms | ~1.17 µs |
| `compact_50k_rows_half_dead` | 53.3 ms | ~2.1 µs per live row |

The same sweep under `enable_long_row`, whose pointer record is 92 bytes against the default 21:

| | median |
|---|---|
| `sweep_50k_rows_20pct_dead` (21-byte record) | 227 µs |
| `sweep_50k_rows_20pct_dead` (92-byte record) | 329 µs |

**4.38× the record width for 1.45× the time**, which says most of a sweep is per-record decode rather than bytes
read. That much holds. What does **not** hold is the extrapolation I drew from it.

I fitted a line through those two points — ~29 µs per MB marginal, ~197 µs fixed — and predicted that widening
the record from 21 to 37 bytes would cost about **10%**. MVCC then widened it to exactly 37 bytes (`xmin` and
`xmax`), so the prediction became checkable:

| record | sweep, 50k rows |
|---|---|
| 21 bytes (pre-MVCC) | 227 µs |
| **37 bytes (measured)** | **281 µs** |
| 92 bytes (`enable_long_row`) | 329 µs |

**+23.8%, not +10%.** Reproducible within 0.5%.

Two things were wrong with the estimate, and the second is the one worth remembering:

1. Part of the gap is mine rather than the record's. The sweep gained work between those measurements — the
   `is_reclaimable` predicate and the fingerprint-clearing test now run per record. So +23.8% is the
   end-to-end change, and the width alone accounts for less than that. I cannot separate them from these
   numbers, which is itself a reason not to have quoted a width-only figure.

2. **The two points were not on the same curve.** The 92-byte measurement comes from `enable_long_row`, which
   is not the same decoder — it reads eleven fields where the short layout reads five. So the "fixed" term I
   derived absorbed decoding work that only exists in one of the two builds, which inflated it and correspondingly
   understated the marginal cost of bytes. Fitting a line through two points from different code paths is not a
   model, and it read like one.

The design conclusion is unaffected, because it never rested on this number: version stamps live in the pointer
record because **compaction copies whole records, so they survive it for free**, where a positional shadow would
have to be renumbered in lockstep. That argument stands at +24% as it did at +10%. But the number was offered as
reassurance and it was wrong by a factor of two, which is worth more than the conclusion it happened not to
change.

What these say:

- **The sweep is not the cost.** 4.5 ns a row extrapolates to ~45 ms for a ten-million-row table, once every 30
  seconds, on a background task. The skip for an unchanged table (§3.5) is still worth having, but it is
  avoiding tens of milliseconds, not seconds. Nothing here argues for a longer interval.
- **Reuse costs about 5% of an insert** — roughly 55 ns a row for the `BTreeSet` lookup and the loan bookkeeping.
  That is what the table pays to stop growing. Against a delete-and-refill workload, which without this doubles
  the rows file every cycle (§1), it is not a close call. Against an insert-only table it is pure overhead, and
  the free list is empty there so the cost is a single failed lookup.
- **Compaction is ~2.1 µs per live row**, so a million-row table is a couple of seconds with the table stopped.
  That is `VACUUM FULL` territory: fine for a maintenance window, not something to run under load.

The benchmark runs on a real filesystem by default (`RASTDB_BENCH_DIR`, defaulting to /var/tmp) because the
sweep is bounded by reading the pointer file. The numbers above were taken on tmpfs, so they measure the CPU
side and flatter the sweep; the ranking between the two insert paths is unaffected, since both pay the same I/O.

## 16. Tests

`core::vacuum::tests` (28) cover gap derivation, coalescing, interval subtraction, best fit, split-and-return,
loan shielding and release, quarantine maturation, clamping and saturation, runtime disable, and epoch
ordering — plus two threaded ones, `concurrent_allocation_never_overlaps` and
`publishing_concurrently_with_allocation_never_double_hands_an_extent` (§3.2).

`core::row::write_path_tests` (37) run against a real `LocalStorageProvider`. The ones that pin down a bug this
work found:

| Test | Guards |
|---|---|
| `reinserting_after_a_delete_reuses_the_freed_space` | rows-file length *unchanged* across delete-and-refill |
| `churn_reaches_a_steady_state` | §7 — the leaked loan; asserts flat size over eight cycles |
| `concurrent_updates_do_not_duplicate_a_row` | §4 — four racing updates left four copies |
| `a_delete_follows_a_row_that_was_relocated_under_it` | §4 — the row that survived its `DELETE` |
| `concurrent_deletes_of_one_row_count_once` | §4 — six racing deletes reported six rows |
| `a_relocating_update_preserves_the_rows_history` | §5 — `created_at` rewritten by an update |
| `a_relocated_row_keeps_its_id_and_the_sequence_survives_reopen` | §5 — identity, and id reuse after reopen |
| `insert_reports_what_it_actually_wrote` | §6 — a failed insert reported success |
| `a_bad_literal_is_rejected_rather_than_zeroed` | `'abc'` stored as `0` in an integer column |
| `a_malformed_row_write_is_refused_before_anything_is_written` | encoder panicking part-way through a row |
| `the_capped_path_never_rehands_a_reused_extent` | §3.3 — 2 960 bytes handed out while live |
| `a_permanently_busy_table_still_reclaims` | §3.6 — a table that never reclaimed at all |
| `compaction_shrinks_both_files` | §10 |
| `point_lookups_still_work_after_compaction` | §10 — the filter after renumbering |
| `compaction_excludes_concurrent_readers_and_writers` | §10 — rows moving under a query |
| `a_duplicate_insert_into_a_unique_column_is_refused` | §12 |
| `concurrent_inserts_cannot_both_claim_a_unique_value` | §12 — the check/write race |
| `create_table_carries_unique_constraints` | §12 — every spelling, and composites declined |
| `unique_and_vacuum_work_through_sql` | §11 + §12 end to end through the executor |
| `a_fixed_width_update_writes_only_the_columns_it_changes` | §13 |
| `an_older_running_transaction_holds_the_version` | §17 — freeing a version an open snapshot sees |
| `an_uncommitted_or_aborted_delete_reclaims_nothing` | §17 — an aborted `DELETE` discarding its own row |
| `the_bound_is_taken_once_and_does_not_drift` | §17 — the horizon tracking the bound instead of holding it |
| `a_versioned_table_reclaims_across_a_restart` | §17 — the whole round trip, including a lost commit record |
| `the_in_place_latch_holds_begin_off` | §13 — a snapshot taken mid-overwrite |
| `reclamation_clears_filter_fingerprints_once` | §17 — a version pruned before its pointer is decoded |
| `the_reclamation_defaults_are_what_the_documentation_says` | §9 — the values, which nothing else pinned |
| `concurrent_writes_and_reclamation_keep_rows_consistent` | a torn extent, whose failure mode is otherwise silent |

Three of those were verified by mutation — reverting the fix and confirming the test fails:
`publishing_concurrently_with_allocation_never_double_hands_an_extent` (within a few of its 200 iterations),
`the_capped_path_never_rehands_a_reused_extent` (2 960 bytes re-handed), and
`a_delete_follows_a_row_that_was_relocated_under_it` (~1 run in 5 before the §4 fix, 12/12 stable after).

`benches/vacuum_benchmarks.rs` measures the sweep, the two insert paths against each other, and compaction. It
runs on a real filesystem, like `append_benchmarks`, because the sweep is bounded by reading the pointer file
and tmpfs would hide that.

## 17. Reclamation under MVCC

A tombstone stops meaning "dead" once the engine has versions. A row whose `xmax` is set is invisible to *newer*
snapshots but still visible to older ones, and freeing its extent lets a later insert overwrite bytes an open
transaction is about to read.

`VersionHorizon` is what reclamation asks instead of reading the flag:

```rust
xmax != NO_TX
  && manager.outcome(xmax) == TxOutcome::Committed
  && xmax < manager.oldest_active()
```

Each conjunct rules out a different way of being wrong. `NO_TX` means nothing deleted it. An **aborted** delete
did not happen, so the row is still live — treating an abort as a delete discards a row nobody removed, which is
the worst outcome available here. And a transaction older than `xmax` may still be able to see the version.

### The row nobody deleted, that nobody can see

`xmax` is only half the question. A rolled-back `INSERT` leaves its bytes and its pointer record durably on
disk — the WAL commits per row, while the *transaction* spans the batch — stamped with an `xmin` that then
aborted. Nothing deleted it, so `xmax` is `NO_TX` and `deleted` is false: both fields the predicate above
consults say **live**, and no later event ever changes either one. That version was leaked permanently.

So the predicate asks about the creator too:

```rust
xmin != FROZEN_TX && xmin != NO_TX
  && manager.outcome(xmin) == TxOutcome::Aborted
```

This one takes **no bound**, and the asymmetry with `xmax` is the point. A version with a committed `xmax` was
alive once, so a snapshot older than the delete can still see it and reclamation has to wait the snapshot out.
A version whose `xmin` aborted was never visible to *any* snapshot — a snapshot taken while the transaction ran
saw it as in-progress, one taken after sees it as aborted — so there is no reader to outlive and no bound to
check.

A rolled-back `UPDATE` is where both branches meet, and they have to disagree. A relocating update writes a
new version stamped `xmin = T` and tombstones the old one with `xmax = T`; when `T` aborts, the new version is
garbage and the old one is a committed row that was never removed. So the same abort frees one and protects the
other — `a_rolled_back_update_frees_the_new_version_and_keeps_the_old` asserts the two halves separately,
because a row count alone passes when both go the same way.

The two sentinel exclusions are not defensive noise. `outcome(FROZEN_TX)` short-circuits to `Committed`, so that
half is redundant today and kept for symmetry; `NO_TX` is load-bearing. `NO_TX` is `0`, and `StatusLog::outcome`
returns `Aborted` for any id past the end of its bitmap — so on a log with no bits yet, `outcome(NO_TX)` reads
**aborted**. That is the exact state a table is in the moment `enable_versioning` is called, and without the
exclusion the first sweep would hand every live pre-versioning row to reclamation at once.
`the_sentinel_transactions_are_never_read_as_aborted` pins both the guard and the `StatusLog` behaviour it
depends on, so a change to either surfaces as a failure here rather than as missing rows.

The bound is sampled **once per sweep**, not per record, and is a bound rather than a sample: nothing below
`oldest_active()` can still be running, and a transaction starting later gets a higher id, so it cannot see a
version a committed transaction below the bound deleted. That is what makes a decision taken during the sweep
still true when the sweep ends — `the_bound_is_taken_once_and_does_not_drift` pins it.

Three sites use it: `scan_pointers`, `live_extents_overlapping`, and `collect_live`. The last matters most.
Compaction *discards* what it does not collect, so a version still visible to an open snapshot has to be carried
across — the same predicate as the sweep, with a worse failure mode if it is wrong.

**`deleted` and `xmax` are not the same claim, and neither can replace the other.**

- `xmax` says *this version was removed by transaction T* — a fact about the row, which older snapshots may
  still be entitled to ignore.
- `deleted` says *this pointer slot holds no live record* — a fact about the slot.

Compaction is where they come apart. Moving a record to a lower slot writes the record at its destination and
then tombstones the source, and that tombstone is a **full copy**: same `id`, same `xmin`, same `xmax`, same
position and length, differing only in the flag. The row is alive — at another slot. So a read path that
filtered on visibility *instead of* on `deleted` would pass both copies and return the row twice, once per
compaction that ever moved it.

The scan must therefore keep honouring `deleted` as slot vacancy **in addition to** filtering by visibility.
`compaction_shrinks_both_files` and `point_lookups_still_work_after_compaction` assert exact row counts after a
compaction that moves records, so dropping the flag from the read path fails them rather than shipping.

**Compaction needs nothing else.** An earlier draft of this section claimed `compaction_gate` had to exclude
open transactions too. It does not: a snapshot pins *versions*, not positions, and compaction updates each
pointer atomically with its row move, so a later read through the updated pointer finds the right bytes. Only
the discard predicate was wrong.

**The reader epochs stay.** Under full MVCC the transaction bound arguably subsumes them — a query that could
see a version holds a snapshot below its `xmax`, so the predicate already declines — but that holds only once
every reader registers a snapshot, and it covers nothing for `deleted`-marked rows carrying no `xmax`. The
machinery already exists; running both costs nothing.

**A commit record that is not durable is a version that is never reclaimed.** `is_reclaimable` requires
`Committed`, so an outcome the engine has lost reads as `InProgress` (zero bits) or `Aborted` (past the end of
the bitmap) and reclamation declines forever. That is the safe direction and it is worth keeping: the inverted
default, where an unknown id reads as committed, frees versions that are still live and cannot be recovered
from. `a_versioned_table_reclaims_across_a_restart` covers the round trip — delete under a transaction, commit,
hold it with an older reader, release, reclaim, restart, and reclaim again.

**Reclamation owns clearing the filter's fingerprints.** A transactional `DELETE` keeps them: clearing at delete
time makes a scan prune the slot *before it decodes the pointer*, so the version goes **unreachable underneath**
the visibility rule rather than invisible through it, and an older snapshot silently loses rows it is entitled
to. Establishing that nothing can reach a version is what the sweep does, so the sweep clears them.

Exactly once per slot, or the filter's write counter would drive a checkpoint on every tick forever. The test is
one integer comparison rather than a set of every slot ever cleared: `oldest_active` only moves forward, so a
version becomes reclaimable exactly once — when the bound crosses its `xmax` — and `ReclaimContext::cleared_below`
holds the previous bound. Compaction resets it to zero, since every slot address changed and the filter was
rebuilt from storage.

Safe even when the run is later deferred: deferral is about handing out *bytes*, and clearing says only that no
snapshot sees the version. Losing the clearing to a crash costs pruning quality and never correctness — the mask
stays a superset — so it asks for a checkpoint rather than marking the sidecar dirty.

**Attaching a manager has a precondition**, and it is the one way to get silent corruption out of this.
`is_reclaimable` reads `deleted && xmax == NO_TX` as "predates versioning, so no snapshot can be older than
versioning itself" — correct for old data, wrong for a row that a `DELETE` marked with the flag while a manager
was attached. `Table::enable_versioning` documents it: `DELETE` must already stamp `xmax` first. One change, not
a switch to flip ahead of the write path.

**What was considered instead.** Letting vacuum *set* `deleted` to mean "dead for everyone", so the two read
sites stay as written. Rejected because it makes vacuum a writer: it would need the write lock that `DELETE` and
`UPDATE` hold across their scans, plus a transaction and WAL records, where today the sweep is read-only and
contends with nothing. The "snapshot-awareness in one place" benefit is real but the two sites are both in this
module, and a shared predicate buys it for nothing.

## 18. A commit that fails now says so

`StorageIO::commit_durable` returned `()`. A failed flush was logged and swallowed, so a caller was told its
write was durable when it was not — the same shape as a commit record written nowhere, and the third instance
of that pattern found in this codebase in a day.

It now returns `IOResult<()>`, and the fifteen call sites in the write path answer the question each of them
was previously ducking:

- **`write_row` and the `UPDATE` relocation path** return the error *and* give back the free-space loan. Nothing
  will ever own an extent whose pointer did not become durable, and a loan left outstanding shields those bytes
  from every future sweep (§7).
- **`DELETE`** fails the statement rather than reporting rows affected. A tombstone that is not durable is a row
  that comes back, and the difference between a failed delete and a silent one is what the caller is told.
- **`UPDATE`** likewise, including on the `UNIQUE`-violation path: the commit error is returned in preference to
  the constraint error, because a caller can retry a rejected update but can do nothing about rows it was told
  had landed.
- **Compaction aborts**, and truncates nothing. Targets sit at or below their sources, so the *next* row's move
  can land on bytes this one was supposed to have vacated — if one batch is not durable and the next is, a
  restart finds a pointer naming bytes that were overwritten. Compaction is safe to *interrupt*; it is not safe
  to continue past a failure.

`write_data` and `append_data` have no way to report anything, so they log and continue; callers that need to
know use the transactional form and commit it themselves.

**Poisoning stays distinguishable from a transient failure.** The log poisons itself when a flush fails, because
the kernel may drop the dirty pages after reporting the error — so a retry could return success while the data
is gone. That state is permanent, and a caller that retries on it never makes progress. An `io::Error` has
nowhere structural to carry the distinction, so it rides in the message behind `traits::POISONED_LOG` and
`traits::is_poisoned`. Nothing consumes it yet; `a_poisoned_commit_is_not_mistaken_for_a_transient_one` pins it
anyway, for the same reason §9's defaults are pinned — a contract nothing stands under is one a tidy-up removes.

The `WAL commit failed` log line stays alongside the returned error. It is what surfaced the truncation bug from
a live server, and an operator watching logs should not also have to be reading the client's errors.
