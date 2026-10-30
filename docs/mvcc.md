# MVCC — Design & Implementation

Two things have to be true at once for a database to be safe under concurrency:
a reader must never see a half-finished write, and a writer must never have to
wait for a reader. Locking gives the first by giving up the second. MVCC gives
both, by never overwriting a row a reader might still be looking at — an update
writes a *new* version, and each statement reads through a snapshot that decides
which versions existed when it began.

## 0. What this replaced

`BEGIN`, `COMMIT` and `ROLLBACK` were accepted and **ignored**. Each statement
committed as it ran, so an application that wrapped work in a transaction and
rolled it back had already committed every statement in it. That is the most
dangerous thing a database can do with those keywords: the application has no
way to find out, because the server reports success either way.

Everything below exists to make those three words mean what they say.

## 1. Transaction identity

`TxId` is a `u64`, monotonic and never reused, so ordering two of them answers
"which happened first" without consulting anything else. Three values are
reserved:

| id | meaning |
|---|---|
| `NO_TX` = 0 | not a transaction. As an `xmax`: nothing has removed this row |
| `FROZEN_TX` = 1 | predates versioning, or written outside any transaction. Visible to everyone |
| ≥ 2 | a real transaction |

`FROZEN_TX` is what makes the change backward-compatible in behaviour: an
unversioned table writes it, `Visibility::All` admits everything untombstoned,
and the engine does exactly what it did before.

## 2. Where the version metadata lives

In the pointer record. `RowPointer` carries `xmin` (the transaction that created
this version) and `xmax` (the one that removed it, or `NO_TX`), appended after
the existing fields so every other offset is unchanged. The default record goes
from 21 bytes to 37.

The alternative considered was a positional shadow of the pointer file, on the
theory that 76% wider records would slow every scan to serve the concurrent case.

Measured once the record was actually widened, 50k-row sweep:

| record | sweep |
|---|---|
| 21 bytes, pre-MVCC | 227 µs |
| 37 bytes, this layout | **281 µs (+23.8%)** |
| 92 bytes, `enable_long_row` | 329 µs |

**The bandwidth cost is real**, and about 2.4× what was predicted before the
change was made. That prediction — ~10% — came from fitting a line through the
21- and 92-byte points, which are not on the same curve: the 92-byte figure is
`enable_long_row`, a *different decoder* reading eleven fields where the short
layout reads five. The "fixed" term the fit derived therefore absorbed per-record
decoding work present in only one of the two builds, which inflated it and
correspondingly understated the marginal cost of bytes. Two real numbers from
different code paths with a line drawn between them read like a measurement and
were not one.

The +23.8% is also an upper bound on the width's own share rather than a clean
attribution: the sweep gained per-record work between the two measurements —
`is_reclaimable` and the fingerprint-clearing check — which cannot be separated
out from these numbers.

So the bandwidth argument does not carry the decision, and never did. What
decides it is compaction. Compaction copies whole pointer
records, so stamps carried inside one survive it for free. A shadow would have to
be renumbered atomically with the records — and unlike the filter index, which is
rebuilt from storage because storage is its authority, **version data is its own
authority**. There is nothing to rebuild it from, so a renumbering bug loses
committed data rather than costing a rebuild.

### Format break

37-byte records cannot be read from a 21-byte file. An existing database has to
be rebuilt, as it did across the `write_order` fix.

There is deliberately **no runtime detection** of the old format. The obvious
test — a length dividing by the old stride but not the new — cannot be told apart
from a current file observed mid-append, because a reader may see a partially
written record and the trailing bytes are then legitimately ignored. Roughly one
torn append in 21 would land on a multiple of 21 and get a valid table refused.
Rejecting good data to catch bad data is the wrong trade.

A sound check would be a format marker written at table creation — `{table}_seq.db`
is the natural home, since it already exists, is written at creation and dropped
with the table. It is not worth adding yet: a marker only helps tables created
*after* it exists, so every older table still looks corrupt and the
documented-rebuild story is needed regardless.

## 3. Visibility

A snapshot is a value, not a lock: taking one is the only synchronisation a
reader does, and from then on it decides visibility entirely on its own. That is
what lets readers run without blocking writers.

```
visible(deleted, xmin, xmax) =
      not deleted                       -- the slot holds a live record
  and xmin happened, from here          -- and its creation is in our past
  and xmax did not                      -- and its removal is not
```

Three details carry their weight:

**`deleted` is checked first and separately.** It is a fact about the *slot* —
this pointer record holds no live entry — where visibility is a fact about the
*row*. They come apart at compaction, which moves a record to a lower slot and
tombstones the source as a **full copy**: same id, same `xmin`, same `xmax`,
differing only in the flag. Both copies are visible to exactly the same
snapshots, so a rule consulting only the stamps would return the row once per
compaction that ever moved it.

**A transaction sees its own writes**, committed or not, or an `INSERT` followed
by a `SELECT` in the same transaction would not find the row it just wrote.

**A transaction in flight when the snapshot was taken stays invisible even after
it commits**, or rows would appear underneath a running statement.

### The fast path is per row, not per scan

A row no transaction has ever touched — `xmin == FROZEN_TX && xmax == NO_TX` — is
visible to everyone, in two integer compares with no lock. Rows that *have* been
touched pay for the full check.

It was tempting to collapse the whole scan instead: with nothing in flight, no
snapshot can disagree about any row, so `!deleted` should be the whole answer.
**That argument is wrong.** `deleted` is set by *vacuum*, not by the delete, so
between a committed `DELETE` and the next sweep a removed row still has
`deleted == false` and only its `xmax` says it is gone. A quiescent database is
when that window is widest, because nothing is running and so nothing is
sweeping. `the_rule_does_not_collapse_to_the_tombstone_bit` is the counterexample.

### Every read path has to carry the reader

A visibility rule only applies where a reader is passed in. `execute_join`
originally scanned each relation with `Table::query_row` — the variant with no
`TxId` — so a join read outside the transaction that issued it and could not see
that transaction's own writes:

```sql
BEGIN;
INSERT INTO jl …;  INSERT INTO jr …;
SELECT jl.nm, jr.id FROM jl JOIN jr ON jl.id = jr.jl_id;   -- returned nothing
```

A join matching no rows is not an error, so the statement reported success and
returned an empty result. It surfaced two layers away, in a chained
data-modifying CTE: each body materialises its `RETURNING` rows into a temporary
table **inside the statement's transaction**, and the outer statement then joins
those tables. Both temporaries held their row and the rewritten SQL was correct,
yet the outer `INSERT … SELECT … FROM a, b` inserted nothing. The CTE rewriter
was blamed for a while and was never at fault.

The rule this leaves: a read path that does not take a `TxId` is not
transaction-safe, and adding one is not optional. Both directions are pinned —
`a_join_sees_the_transactions_own_writes` and
`a_join_does_not_see_another_transactions_uncommitted_writes`. The second is
what makes the first meaningful: threading in a reader that sees everything
would satisfy the first test and break isolation.

## 4. Writes

| statement | what it does |
|---|---|
| `INSERT` | new version, `xmin` = the writer |
| `DELETE` | stamps `xmax`; the version stays readable by older snapshots |
| `UPDATE` | retires the old version with `xmax` and appends a new one with `xmin` |

A transactional `UPDATE` **always relocates**. Overwriting in place leaves
nothing to roll back to, and a snapshot older than the update would read the new
value through a version it is entitled to see unchanged. Only a write outside any
transaction may overwrite, where there is no rollback and no other snapshot.

### The filter index must not be cleared early

Clearing a slot's fingerprints makes a scan prune it *before decoding the
pointer*, so a version whose fingerprints are gone is unreachable no matter what
the visibility rule says. Under versioning the retired version is still readable
by older snapshots, so `on_delete(slot)` is skipped for a transactional `DELETE`
or relocation and left to reclamation, which knows when nothing can reach it.

This one cost an hour to find, because the stamps were provably correct and the
query still returned nothing — the pointer was never read.

### Write conflicts: first updater wins

Two transactions updating the same row would otherwise both succeed: the second
one's snapshot still shows the old version, so it relocates from the same base
and appends a second replacement. Once both commit, two versions of one row are
visible — the row duplicated and one update lost.

`Table::check_write_conflict` refuses the second with a serialization failure,
telling it to retry rather than making it wait; blocking until the first
transaction ends would need a lock manager this engine does not have. An `xmax`
belonging to an *aborted* transaction is not a conflict — that retirement never
happened — and is overwritten rather than treated as already-retired.

### Which writers take turns

Two locks per table decide which writes may run side by side.

- **The write lock** keeps a `DELETE` or `UPDATE` from acting on a row
  another one is relocating: a relocated row's new version is appended past
  the end a concurrent scan has already passed, so that scan would find only
  the tombstone and silently match nothing. `DELETE`, a general `UPDATE` and
  the aborted-write sweep take it exclusively.
- **The key locks** make a uniqueness check atomic with the write it guards.
  They are 64 stripes, and a key hashes to one of them, in the canonical form
  a hash index uses, so two values the check calls equal always share a
  stripe. An `INSERT` takes the stripes of the keys it writes, so inserts of
  different keys no longer take turns. A general `UPDATE` takes every stripe
  for its rewrite, since it learns its rows' keys only as it goes.

A **point update**, an `UPDATE` whose `WHERE` fixes every column of one
`UNIQUE` or `PRIMARY KEY` constraint and which assigns no unique column,
takes the write lock *shared* and that key's stripe *before* its scan. Every
other writer of a row that key can match holds the same stripe or the write
lock exclusively, so the scan's answer stays true until the rewrite. Before
the rewrite it also takes the stripes of the row's other unique keys. Without
them, `UPDATE … WHERE id = 1` and `UPDATE … WHERE email = 'a'` on the same row
would hold different stripes, both relocate the same version, and fork the
row. Stripes are always taken in ascending order. The one taken late is only
*tried* when it sits below one already held, and on failure the statement
starts again as a general update, having written nothing.

So point updates of different rows, the conflict half of every
`INSERT … ON CONFLICT DO UPDATE`, run in parallel. Pinned by
`compat/tests/point_update_concurrency.rs`, mutation-tested against the
other-key stripes.

## 5. Durability of outcomes

Version stamps are only meaningful next to the outcome of the transaction that
wrote them. A row stamped `xmin = 41` is either real data or the debris of a
transaction that aborted, and nothing in the row says which.

`StatusLog` records that: two bits per transaction, packed four to a byte and
indexed by id, so it is an array lookup rather than a search. PostgreSQL's
`pg_xact` under another name.

**An outcome is written to both the write-ahead log and its own file.** That is
not belt-and-braces, it is required. `Wal::checkpoint()` does not replay records
into files — it fsyncs the files their writers already wrote and then discards
the log. A participant that writes *only* to the log gets fsynced-around and
truncated. Writing only to the log lost every outcome older than the last
checkpoint, so committed rows came back as debris and a versioned database
returned nothing while holding all of it. `outcomes_survive_the_log_being_checkpointed`
pins it.

The log record is written first: crashing between the two leaves a record to
replay, whereas the other order leaves the file holding bytes that were never
logged, and an un-fsynced write can still be lost.

**An unknown id never reads as committed** — `InProgress` for zeroed bits,
`Aborted` past the end of the bitmap. Reclamation requires `Committed` to free a
version, so a lost record costs space and never data. The inverted failure would
free versions that are still live.

## 6. Reclamation

A version is reclaimable when

```
xmax != NO_TX  and  outcome(xmax) == Committed  and  xmax < oldest_active()
```

The third conjunct is the one that matters. `oldest_active()` is a *bound*, not a
sample: nothing below it can still be running, and no snapshot taken from here on
can be older than it. So a version retired by a transaction below the bound is
dead for every snapshot that will ever exist.

The reader-epoch barrier stays alongside the transaction bound. Under full MVCC
the bound arguably subsumes it, but that holds only once every reader registers a
snapshot, and it does not cover `deleted`-marked rows with no `xmax` at all.

## 7. Sessions

`BEGIN` opens a transaction on the connection; `COMMIT` and `ROLLBACK` close it.
`ReadyForQuery` reports `I` (idle), `T` (in a block) or `E` (in a failed block) —
sqlx and psql both track this, and reporting `I` throughout makes an open
transaction invisible to the driver, so `sqlx::Transaction` does not work at all.

A failure inside a block **poisons** it: every later statement is refused with
`25P02` until `COMMIT` or `ROLLBACK` closes it, and committing a poisoned block
rolls it back. Otherwise a block could commit having silently skipped the
statement that did not work. The poisoning happens wherever an error is
*reported* rather than at one chosen point, because a statement can fail at
parse, at name resolution, or in the engine.

A connection dropped mid-block rolls it back. A leaked transaction would pin the
reclamation bound forever, which looks like reclamation silently stopping.

## 8. What this costs

**Two fsyncs per autocommit write**, the WAL data commit plus the status commit.
They are separable in principle — the status byte could ride in the statement's
own WAL transaction rather than its own `log_and_commit` — but that needs the
statement's transaction threaded into `StatusLog`.

**The row-level in-place overwrite is gone on a versioned table**, and the
column-level one survives under two conditions. `execute` opens a transaction for
every autocommit statement, because a `DELETE` has to stamp an `xmax` other
snapshots can order against, so a claim testing `running.is_empty()` would
decline always — unreachable rather than merely rare, which is the failure mode
that looks like a fast path not being worth much.

The claim needs two separate things, and the second is easy to miss:

1. **No one *else* is running.** Makes the overwrite safe to *read*: no other
   snapshot can tell before from after.
2. **The writer cannot roll back.** An overwrite keeps no previous version, so a
   transaction that aborts later cannot restore what it replaced. Only a
   statement-scoped transaction qualifies — autocommit, or no transaction at all.

Condition 1 alone was implemented first and measured at `200/200` inside an
explicit block, which is what a missing condition 2 looks like:
`a_rolled_back_update_to_a_fixed_width_column_is_undone` shows the rollback
reporting success while the new value stays.

Measured through the wire, 200 updates per shape
(`cargo run -p rasterizeddb_compat --example inplace_rate`):

| shape | in-place |
|---|---|
| autocommit, fixed-width column | 200 / 200 |
| inside `BEGIN`, fixed-width | 0 / 200 |
| autocommit, string column | 0 / 200 (must be 0) |

So the path is fully reachable for autocommit traffic and correctly unreachable
where it would be wrong. The string row is the control: it confirms the counter
measures the path rather than counting updates.

**Dead-space generation goes up sharply**, because every transactional update
leaves a version behind. Reclamation stops being a nicety.

**The query cache needed re-keying, and now works.** It was briefly switched off
for every versioned table — which is every table — so `--enable-cache true` did
nothing at all and every repeat of a query re-scanned from slot 0.

The original entry held the pointers a scan found *visible*, a set only the
snapshot that built it could answer from, and the generation counter could not
police that: it bumps on writes, and what changes visibility is a **commit**,
which writes nothing.

Keying entries *by snapshot* would have been sound and useless — under
`READ COMMITTED` every statement gets a fresh snapshot, so the hit rate would be
near zero. The fix is to cache a set that does not depend on visibility at all:

> Cache the pointers matching the **predicate**, and apply the visibility test
> per pointer at serve time.

One entry then serves every snapshot, and the case that broke the old design
works by construction — a transaction that wrote before the entry was cached and
commits after is *in* the set, was merely invisible, and becomes visible.

Entries can now name versions that are dead for everyone, which reclamation does
not know about. That is safe, but by **two** mechanisms rather than one —
`VersionHorizon::is_reclaimable` has three return paths:

| branch | condition | what protects it |
|---|---|---|
| unversioned table | `deleted`, no bound consulted | the `deleted` short-circuit |
| stamped | `outcome(xmax) == Committed && xmax < oldest_active` | the lemma below |
| pre-versioning row | `xmax == NO_TX && deleted` | the `deleted` short-circuit |

The lemma, for the stamped branch: **a version is reclaimable only once its
`xmax` is committed and below `oldest_active`, which is exactly the condition
that makes it invisible to every snapshot that can still be taken.**

The other two branches involve no `xmax` at all, and nothing about the
transaction bound touches them. They are covered by `Visibility::visible`
short-circuiting on `deleted` before it consults the snapshot — a line whose
*stated* purpose is compaction leftovers. Two thirds of the reclamation surface
rests on it, which is why it now says so and why removing it fails two named
tests. For the same reason there must be no `visible_stamped(xmin, xmax)`
overload: taking `deleted` as a parameter is what makes the safe call the natural
one.

Both branches and the lemma are pinned in `visibility::reclamation_lemma`, and
the `deleted` short-circuit is mutation-verified — removing it fails three tests.

**The ordering of the visibility test and the fetch is load-bearing**, at all five
sites that do both. Reclaimed space is reused, so fetch-then-check would read
another row's bytes and only then discard them, which is one refactor away from
returning them. That is a structural property no test can express, so each site
carries a comment saying the ordering is not stylistic.

## 9. Isolation levels

Two behaviours, not four.

| requested | behaviour |
|---|---|
| `READ UNCOMMITTED` | as `READ COMMITTED` — a dirty read is permitted by the standard, not required, and this engine cannot produce one |
| `READ COMMITTED` (default) | fresh snapshot per statement |
| `REPEATABLE READ` | one snapshot for the transaction |
| `SERIALIZABLE` | as `REPEATABLE READ` — **see the caveat** |

The snapshot is pinned in the transaction manager, keyed by id, rather than
carried on the session. That way nothing between the session and the scan has to
know about snapshots: `snapshot(own)` returns the pinned one if there is one.
Pinning happens when the level is set rather than at `BEGIN`, matching
PostgreSQL — a `BEGIN` followed by a long pause does not freeze the view at the
pause.

`SERIALIZABLE` is **weaker than the name promises**. Write skew — two
transactions reading a set and each writing a row the other's predicate would
have matched — is not detected, because detecting it needs predicate locking.
First-updater-wins catches conflicts on the *same row* only. An application that
depends on true serializability should not rely on this level.

`SET TRANSACTION ISOLATION LEVEL` and `SET SESSION CHARACTERISTICS AS
TRANSACTION ...` both take effect; they used to fall through to the blanket
`SET` no-op, so asking for a stricter level silently got the default. So do
the run-time parameters: `default_transaction_isolation` and
`default_transaction_read_only` (`SET`, `SET LOCAL` inside a block, `RESET`,
`DEFAULT`; `SHOW` reads them back) set what a `BEGIN` naming no mode
inherits, and `transaction_isolation` / `transaction_read_only` inside a block
act as `SET TRANSACTION`. A value they do not take is refused with 22023.
`SERIALIZABLE` runs as `REPEATABLE READ` and `READ UNCOMMITTED` as
`READ COMMITTED`, and `SHOW` reports the level that runs. As in PostgreSQL,
`SET TRANSACTION` outside a block warns (25P01) and changes nothing; it used
to set the session default. Not yet as in PostgreSQL: a `SET` inside a block
that then rolls back is not undone, for these as for every other setting.

## 10. Savepoints

`SAVEPOINT`, `ROLLBACK TO SAVEPOINT` and `RELEASE SAVEPOINT`. They matter more
than they look: every ORM that offers nested transactions builds them out of
these, so "nested transaction support" *is* savepoint support — `sqlx`'s
`outer.begin()` emits `SAVEPOINT`/`RELEASE` on the wire.

A savepoint is a **sub-transaction** with an id of its own, so its writes can be
discarded separately. It has no independent outcome: `outcome()` resolves a
sub-transaction through its parent chain, so it commits when its parent commits
and aborts when its parent aborts. Rolling one back records it as aborted
immediately, while its parent is still undecided.

Three consequences worth stating:

- **A transaction is no longer one id.** `TransactionManager::family_of(tx)`
  gives the whole set. `Snapshot` carries the family rather than a single `own`,
  because "is this write mine?" stops being an equality test — a write made
  before a savepoint carries the outer id and must stay visible inside it, and a
  write made inside a rolled-back savepoint is its own and must not.
- **Rolling back to a savepoint un-poisons the block.** That is the reason the
  feature exists: recovering from a failed statement without losing the whole
  transaction. Without it, one failure loses everything.
- **A savepoint never qualifies for the in-place fast path**, because it is as
  rollback-able as its parent.

`RELEASE` merges a savepoint into what is below it. Nothing moves — a
sub-transaction already resolves through its parent — it only stops being
separately rollback-able.

A name may be reused: the newest shadows the older, and rolling back to it leaves
the older standing. Ending a block retires every savepoint under it, because one
left running holds `oldest_active` down for the life of the process and
reclamation quietly stops — a transaction leak that presents as a disk-space
leak.

## 11. Not built
- **Blocking on conflict.** The second updater is told to retry. PostgreSQL under
  `READ COMMITTED` waits for the first to end and then re-evaluates.
- **Uniqueness under concurrency.** The probe checks rows the statement can see,
  so two transactions can insert the same value and both commit. That is the
  pre-existing gap — the probe was never an index and never locked.

## 12. Tests

- `core::mvcc::transaction` — visibility rules, ids, the active bound
- `core::mvcc::status` — packing, reopen, an interrupted transaction
- `core::mvcc::visibility` — the tombstone/stamp distinction, compaction leftovers
- `core::wal::outcomes_survive_the_log_being_checkpointed` — §5
- `core::row::write_path_tests::a_rolled_back_update_leaves_the_old_value_readable` — §4
- `core::mvcc::transaction::savepoint_tests` and `savepoint_leaks` — §10
- `rasterizeddb_compat/tests/transactions.rs` — 32 tests driving real sessions
  through sqlx: rollback, isolation levels, poisoned blocks, disconnects,
  write conflicts, savepoints, and the driver's nested-transaction API
